from __future__ import annotations

import os
import random
import re
import time
from typing import Optional
from urllib.parse import urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from data_cleaning import clean_listings
from database import SessionLocal, clean_inactive_listings, init_db
from deduplication import mark_duplicate_listings
from listing_pipeline import process_listing_pipeline
from scraper_health import (
    STATUS_DEGRADED,
    STATUS_FAILED,
    detect_blocking,
    evaluate_source_run,
    filter_valid_scraped_rows,
    print_source_health,
)


DEFAULT_WAIT_SECONDS = 20
MIN_HTML_LENGTH = 500
MIN_SALE_PRICE_CLP = 10_000_000
BASE_URL = "https://www.yapo.cl"
EXAMPLE_URL = (
    "https://www.yapo.cl/bienes-raices-venta-de-propiedades-apartamentos/"
    "region-metropolitana-nunoa"
)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
CARD_SELECTORS = [
    "[data-testid*='ad']",
    "[data-testid*='listing']",
    "[data-testid*='Listing']",
    "[class*='AdCard']",
    "[class*='ad-card']",
    "[class*='ui-search-result']",
    "[class*='ui-search-layout__item']",
    "article",
    "li",
    "li[class*='listing']",
    "li[class*='Listing']",
    "div[class*='listing']",
    "div[class*='Listing']",
    "div[class*='card']",
    "div[class*='Card']",
]
ATTRIBUTE_SELECTORS = [
    "[class*='attribute']",
    "[class*='Attribute']",
    "[class*='feature']",
    "[class*='Feature']",
    "[class*='spec']",
    "[class*='Spec']",
    "[class*='detail']",
    "[class*='Detail']",
    "[data-testid*='attribute']",
    "[data-testid*='feature']",
    "[aria-label]",
    "li",
    "span",
]
DEBUG_EXTRACTION = os.getenv("YAPO_DEBUG_EXTRACTION", "1").lower() not in {
    "0",
    "false",
    "no",
}
DEBUG_LISTING_LIMIT = 3
RUN_STATUS_SUCCESS = "success"
RUN_STATUS_PARTIAL = "partial"
RUN_STATUS_FAILED = "failed"
DORMITORIOS_MAX = 6
BANOS_MAX = 5
DORMITORIO_CONTEXT_PATTERN = r"(?:dorm(?:itorio|itorios|s)?|habitacion(?:es)?)"
BANO_CONTEXT_PATTERN = r"(?:bano|banos)"
M2_MIN_VALUE = 10
M2_MAX_VALUE = 1000
M2_UNIT_PATTERN = r"(?:m\s*(?:2|\u00b2|\?|\s+2)|mt2|mts2?|mtrs?|metros?\s*cuadrados?)"
M2_UTIL_LABEL_PATTERN = r"(?:util(?:es)?|superficie\s+util)"
M2_TOTAL_LABEL_PATTERN = r"(?:total(?:es)?|superficie\s+total)"
M2_BUILT_LABEL_PATTERN = r"(?:construid[ao]s?|superficie\s+construid[ao])"


def iniciar_driver():
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("start-maximized")
    options.add_argument("window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")

    # headless=False para validar visualmente que cargan propiedades reales.
    driver = webdriver.Chrome(service=Service(), options=options)
    driver.execute_script("""
Object.defineProperty(navigator, 'webdriver', {
get: () => undefined
})
""")
    print("[STEALTH V2] Driver inicializado con configuración anti-detección")
    return driver


def fetch_page(url):
    driver = None

    try:
        driver = iniciar_driver()
        delay = random.uniform(3.0, 7.0)
        print(f"[PAGE DELAY V2] {delay:.2f}s")
        time.sleep(delay)
        driver.get(url)

        wait = WebDriverWait(driver, DEFAULT_WAIT_SECONDS)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(random.uniform(1.0, 2.5))
        scroll_pause = random.uniform(0.5, 1.5)
        driver.execute_script("window.scrollBy(0, 300);")
        time.sleep(scroll_pause)
        driver.execute_script("window.scrollBy(0, 600);")
        time.sleep(scroll_pause)

        # Yapo carga parte del contenido dinamicamente; esperamos y luego scrolleamos.
        delay = random.uniform(2.0, 5.0)
        print(f"[DELAY] Sleeping {delay:.2f}s")
        time.sleep(delay)
        scroll_page(driver)
        wait_for_possible_listings(driver)
        time.sleep(random.uniform(0.3, 1.2))

        return driver.page_source
    except TimeoutException as error:
        print(f"Timeout real cargando pagina con Selenium: {error}")
        return ""
    except WebDriverException as error:
        print(f"Error de red o Selenium cargando pagina: {error}")
        return ""
    except Exception as error:
        print(f"Error inesperado cargando pagina: {error}")
        return ""
    finally:
        if driver is not None:
            driver.quit()


def parse_listings(html):
    if not is_html_processable(html):
        return []

    soup = BeautifulSoup(html, "html.parser")
    content_root = soup.select_one("main") or soup
    cards = find_listing_cards(content_root)
    listings = []
    seen_links = set()

    for card in cards:
        time.sleep(random.uniform(0.05, 0.2))
        listing = parse_listing_card(card)
        title = clean_text(listing["titulo"])
        price = listing["precio"]
        link = listing["link"]
        raw_text = clean_text(listing["raw_text"])
        normalized_price = normalize_price(price)

        if not link:
            print("Listing descartado por falta de URL")
            continue

        if not title or not price:
            continue

        if normalized_price is None:
            continue

        if link in seen_links:
            continue

        if is_navigation_link(link, title):
            continue

        if not is_valid_sale_price(normalized_price):
            continue

        attributes_text = " ".join(value for value in [title, raw_text] if value)
        compact_attributes = extract_compact_attributes(attributes_text)
        m2_construidos = extract_m2_from_text(attributes_text)
        comuna = extract_comuna(attributes_text)
        dormitorios = first_not_none(
            extract_dormitorios(attributes_text),
            compact_attributes.get("dormitorios"),
        )
        banos = first_not_none(
            extract_banos(attributes_text),
            compact_attributes.get("banos"),
        )
        estacionamientos = first_not_none(
            extract_number_near_keywords(
                attributes_text,
                ["estacionamiento", "estacionamientos", "estac"],
            ),
            compact_attributes.get("estacionamientos"),
        )
        quality_flags = []

        if not has_valid_m2(m2_construidos):
            quality_flags.append("missing_m2")

            if not has_strong_partial_listing(
                title=title,
                link=link,
                normalized_price=normalized_price,
                comuna=comuna,
                dormitorios=dormitorios,
                banos=banos,
            ):
                print("Listing descartado por falta de m2")
                continue

        parsed_listing = {
            "titulo": title,
            "precio_texto": price,
            "precio_clp": normalized_price["precio_clp"],
            "precio_uf": normalized_price["precio_uf"],
            "url": link,
            "link": link,
            "source_listing_id": extract_source_listing_id(link),
            "comuna": comuna,
            "m2_construidos": m2_construidos,
            "m2_terreno": None,
            "m2_util": None,
            "m2_total": None,
            "dormitorios": dormitorios,
            "banos": banos,
            "estacionamientos": estacionamientos,
            "fecha_publicacion": None,
            "quality": "low" if quality_flags else "standard",
            "quality_flags": quality_flags,
            "raw_text": raw_text,
        }

        debug_listing_extraction(len(listings), parsed_listing, raw_text)

        seen_links.add(link)
        listings.append(parsed_listing)

    complete_listings = [
        listing
        for listing in listings
        if "missing_m2" not in listing.get("quality_flags", [])
    ]
    partial_listings = [
        listing
        for listing in listings
        if "missing_m2" in listing.get("quality_flags", [])
    ]

    return clean_listings(complete_listings) + partial_listings


def scrape(url):
    html = fetch_page(url)
    block_detected = detect_blocking(html, url)

    if not is_html_processable(html):
        health = evaluate_source_run(
            source="yapo",
            raw_rows=0,
            valid_rows=0,
            block_detected=block_detected,
        )
        run_status = apply_run_status_to_health(health)
        print_source_health(health)
        return {
            "items": [],
            "health": health_as_dict(health, run_status),
            "rejected_rows": [],
        }

    try:
        raw_results = parse_raw_listings(html)
        parsed_results = parse_listings(html)
        filtered_results, rejected_rows = filter_valid_scraped_rows(
            "yapo",
            parsed_results,
        )
        health = evaluate_source_run(
            source="yapo",
            raw_rows=len(raw_results),
            valid_rows=len(filtered_results),
            block_detected=block_detected,
        )
        run_status = apply_run_status_to_health(health)
        print(f"Cantidad original: {len(raw_results)}")
        print(f"Cantidad filtrada: {len(filtered_results)}")
        print_source_health(health)

        if rejected_rows:
            print(f"Listings rechazados por validacion: {len(rejected_rows)}")

        return {
            "items": filtered_results,
            "health": health_as_dict(health, run_status),
            "rejected_rows": rejected_rows,
        }
    except Exception as error:
        print(f"Error parseando listings: {error}")
        health = evaluate_source_run(
            source="yapo",
            raw_rows=0,
            valid_rows=0,
            block_detected=block_detected,
        )
        health.status = STATUS_FAILED
        health.reasons.append(f"parse error: {error}")
        run_status = apply_run_status_to_health(health)
        print_source_health(health)
        return {
            "items": [],
            "health": health_as_dict(health, run_status),
            "rejected_rows": [],
        }


def apply_run_status_to_health(health):
    run_status = get_run_status(health)

    if run_status == RUN_STATUS_PARTIAL and health.status == STATUS_FAILED:
        health.status = STATUS_DEGRADED

    return run_status


def health_as_dict(health, run_status: str | None = None) -> dict:
    health_dict = health.as_dict()
    health_dict["run_status"] = run_status or get_run_status(health)
    return health_dict


def get_run_status(health) -> str:
    if isinstance(health, dict):
        valid_rows = int(health.get("valid_rows") or 0)
        status = health.get("status")
        block_detected = bool(health.get("block_detected"))
    else:
        valid_rows = int(health.valid_rows or 0)
        status = health.status
        block_detected = bool(health.block_detected)

    if valid_rows == 0:
        return RUN_STATUS_FAILED

    if block_detected or status in {STATUS_FAILED, STATUS_DEGRADED}:
        return RUN_STATUS_PARTIAL

    return RUN_STATUS_SUCCESS


def scrape_and_save(url):
    init_db()
    run_result = scrape(url)
    items = run_result["items"]
    health = run_result["health"]
    run_status = health.get("run_status") or get_run_status(health)
    saved_count = 0
    updated_count = 0

    if run_status == RUN_STATUS_FAILED:
        print("WARNING: YAPO scrape FAILED. No valid rows will be saved from this run.")
        inactive_count = clean_inactive_listings()
        print(f"Listings marcados inactivos por last_seen vencido: {inactive_count}")
        return {
            "source": "yapo",
            "status": health["status"],
            "run_status": run_status,
            "health": health,
            "saved": 0,
            "updated": 0,
            "inserted_count": 0,
            "updated_count": 0,
            "raw_rows": health["raw_rows"],
            "valid_rows": health["valid_rows"],
        }

    if run_status == RUN_STATUS_PARTIAL:
        print("WARNING: YAPO scrape PARTIAL/DEGRADED. Saving validated rows.")

    with SessionLocal() as db:
        for item in items:
            try:
                listing = save_listing(db, item)

                if listing is None:
                    continue

                if getattr(listing, "_was_created", False):
                    saved_count += 1
                else:
                    updated_count += 1
            except Exception as error:
                db.rollback()
                print(f"Error guardando listing: {error}")

    inactive_count = clean_inactive_listings()
    duplicate_result = mark_duplicate_listings()
    print(f"Listings marcados inactivos por last_seen vencido: {inactive_count}")
    print(
        "Duplicados de propiedad marcados: "
        f"{duplicate_result['duplicates']} en {duplicate_result['groups']} grupos"
    )
    print(f"Total scrapeados: {len(items)}")
    print(f"Total guardados: {saved_count}")
    print(f"Total actualizados: {updated_count}")
    return {
        "source": "yapo",
        "status": health["status"],
        "run_status": run_status,
        "health": health,
        "saved": saved_count,
        "updated": updated_count,
        "inserted_count": saved_count,
        "updated_count": updated_count,
        "raw_rows": health["raw_rows"],
        "valid_rows": health["valid_rows"],
        "property_duplicates": duplicate_result,
    }


def save_listing(db, item):
    listing_data = normalize_listing_item(item)
    listing_data["fuente"] = "yapo"
    listing_data["precio_texto"] = item.get("precio_texto")
    listing_data["precio_clp"] = item.get("precio_clp")
    listing_data["precio_uf"] = item.get("precio_uf")
    return process_listing_pipeline(db, listing_data, source="scraper")


def normalize_listing_item(item):
    link = normalize_listing_url(clean_text(item.get("link") or item.get("url"))) or None
    url = normalize_listing_url(clean_text(item.get("url") or item.get("link"))) or None
    title = clean_text(item.get("titulo")) or None
    raw_text = clean_text(item.get("raw_text") or item.get("texto")) or None
    attributes_text = " ".join(value for value in [title, raw_text] if value)
    source_listing_id = clean_text(
        item.get("source_listing_id") or extract_source_listing_id(url)
    ) or None

    return {
        "source_listing_id": source_listing_id,
        "url": url,
        "link": link,
        "titulo": title,
        "comuna": clean_text(item.get("comuna")) or extract_comuna(attributes_text),
        "lat": float_or_none(item.get("lat")),
        "lon": float_or_none(item.get("lon")),
        "m2_construidos": first_not_none(
            positive_or_none(item.get("m2_construidos")),
            extract_m2_from_text(attributes_text),
        ),
        "m2_terreno": positive_or_none(item.get("m2_terreno")),
        "m2_util": positive_or_none(item.get("m2_util")),
        "m2_total": positive_or_none(item.get("m2_total")),
        "dormitorios": bounded_positive_int_or_none(
            item.get("dormitorios"),
            DORMITORIOS_MAX,
        )
        or extract_dormitorios(attributes_text),
        "banos": bounded_positive_int_or_none(item.get("banos"), BANOS_MAX)
        or extract_banos(attributes_text),
        "estacionamientos": non_negative_int_or_none(item.get("estacionamientos"))
        if item.get("estacionamientos") is not None
        else extract_number_near_keywords(
            attributes_text,
            ["estacionamiento", "estacionamientos", "estac"],
        ),
        "fecha_publicacion": item.get("fecha_publicacion"),
    }


def parse_raw_listings(html):
    if not is_html_processable(html):
        return []

    soup = BeautifulSoup(html, "html.parser")
    content_root = soup.select_one("main") or soup
    cards = find_listing_cards(content_root)
    raw_listings = []
    seen_links = set()

    for card in cards:
        time.sleep(random.uniform(0.05, 0.2))
        listing = parse_listing_card(card)
        link = listing["link"]

        if not link or link in seen_links:
            continue

        if is_navigation_link(link, listing["titulo"]):
            continue

        seen_links.add(link)
        raw_listings.append(listing)

    return raw_listings


def scroll_page(driver) -> None:
    for scroll_y in (700, 1500, 2600, 3800):
        driver.execute_script("window.scrollTo(0, arguments[0]);", scroll_y)
        delay = random.uniform(2.0, 5.0)
        print(f"[DELAY] Sleeping {delay:.2f}s")
        time.sleep(delay)


def wait_for_possible_listings(driver) -> None:
    try:
        wait = WebDriverWait(driver, DEFAULT_WAIT_SECONDS)
        wait.until(
            lambda active_driver: page_has_listing_signal(active_driver.page_source)
        )
    except TimeoutException:
        print("Contenido lento: primer timeout detectando cards. Intentando segundo scroll.")
        scroll_page(driver)
        time.sleep(random.uniform(4.0, 9.0))

        try:
            wait = WebDriverWait(driver, DEFAULT_WAIT_SECONDS)
            wait.until(
                lambda active_driver: page_has_listing_signal(active_driver.page_source)
            )
            print("Contenido cargado tras segundo intento.")
            time.sleep(random.uniform(0.3, 1.2))
        except TimeoutException:
            html = driver.page_source or ""

            if html_contains_price(html):
                print("Contenido cargado pero no detectado como cards. Se intentara fallback por precios.")
            else:
                print("Timeout real: no se detectaron cards ni precios en el HTML.")


def find_listing_cards(content_root):
    cards_by_link = {}

    for selector in CARD_SELECTORS:
        for candidate in content_root.select(selector):
            if not looks_like_listing_container(candidate):
                continue

            link = extract_link(candidate)

            if not link:
                continue

            text = clean_text(candidate.get_text(" ", strip=True))
            current = cards_by_link.get(link)

            if current is None or len(text) < len(clean_text(current.get_text(" ", strip=True))):
                cards_by_link[link] = candidate

    if cards_by_link:
        return list(cards_by_link.values())

    return fallback_listing_anchors(content_root)


def looks_like_listing_container(candidate) -> bool:
    text = clean_text(candidate.get_text(" ", strip=True))

    if not text or len(text) < 35:
        return False

    if len(text) > 2500:
        return False

    if not extract_price(text):
        return False

    listing_links = [
        link
        for link in candidate.find_all("a", href=True)
        if is_real_yapo_listing_url(urljoin(BASE_URL, link.get("href", "")))
    ]

    if getattr(candidate, "name", None) == "a" and is_real_yapo_listing_url(
        urljoin(BASE_URL, candidate.get("href", ""))
    ):
        listing_links.append(candidate)

    if not listing_links:
        return False

    if len({normalize_listing_url(urljoin(BASE_URL, link.get("href", ""))) for link in listing_links}) > 2:
        return False

    return not is_navigation_link(
        normalize_listing_url(urljoin(BASE_URL, listing_links[0].get("href", ""))),
        text,
    )


def fallback_listing_anchors(content_root):
    anchors = []
    seen_links = set()

    for link in content_root.select("a[href]"):
        absolute_url = normalize_listing_url(urljoin(BASE_URL, link.get("href", "")))
        text = build_anchor_context_text(link)

        if not absolute_url or absolute_url in seen_links:
            continue

        if not is_real_yapo_listing_url(absolute_url):
            continue

        if is_navigation_link(absolute_url, text):
            continue

        if not extract_price(text):
            continue

        seen_links.add(absolute_url)
        anchors.append(link)

    return anchors


def build_anchor_context_text(link) -> str:
    values = [link.get_text(" ", strip=True)]

    for parent in link.parents:
        if getattr(parent, "name", None) in {"main", "body", "html"}:
            break

        parent_text = clean_text(parent.get_text(" ", strip=True))

        if parent_text and len(parent_text) <= 1500:
            values.append(parent_text)
            break

    return clean_text(" ".join(value for value in values if value))


def is_html_processable(html) -> bool:
    if not html:
        print("HTML vacio: no se procesara.")
        return False

    if len(html.strip()) < MIN_HTML_LENGTH:
        print("HTML demasiado corto: posible error de red o bloqueo.")
        return False

    normalized_text = normalize_text(BeautifulSoup(html, "html.parser").get_text(" ", strip=True))
    blocking_texts = [
        "sin resultados",
        "no se encontraron resultados",
        "no encontramos resultados",
        "error",
        "err_internet_disconnected",
        "site cannot be reached",
        "no internet",
    ]

    if any(text in normalized_text for text in blocking_texts):
        print("HTML indica sin resultados o error: no se procesara.")
        return False

    return True


def html_contains_price(html) -> bool:
    return extract_price(BeautifulSoup(html or "", "html.parser").get_text(" ", strip=True)) is not None


def page_has_listing_signal(html) -> bool:
    if not html:
        return False

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    if not html_contains_price(html):
        return False

    return any(
        is_real_yapo_listing_url(urljoin(BASE_URL, link.get("href", "")))
        for link in soup.select("a[href]")
    ) or extract_m2_from_text(text) is not None


def parse_listing_card(card):
    text = build_anchor_context_text(card) if getattr(card, "name", None) == "a" else clean_text(card.get_text(" ", strip=True))
    structured_text = extract_structured_text(card, text)

    return {
        "titulo": extract_title(card, text),
        "precio": extract_price(structured_text),
        "link": extract_link(card),
        "raw_text": structured_text,
    }


def extract_link(card) -> Optional[str]:
    links = card.find_all("a", href=True)

    if getattr(card, "name", None) == "a":
        links = [card]

    for link in links:
        href = link.get("href")

        if not href:
            continue

        absolute_url = urljoin(BASE_URL, href)
        title = clean_text(link.get_text(" ", strip=True))

        if is_real_yapo_listing_url(absolute_url) and not is_navigation_link(absolute_url, title):
            return normalize_listing_url(absolute_url)

    return None


def normalize_listing_url(url) -> Optional[str]:
    url = clean_text(url)

    if not url:
        return None

    parsed_url = urlparse(url)
    normalized_path = parsed_url.path.rstrip("/") or parsed_url.path

    return urlunparse(
        (
            parsed_url.scheme,
            parsed_url.netloc.lower(),
            normalized_path,
            "",
            "",
            "",
        )
    )


def extract_title(card, text) -> Optional[str]:
    selectors = [
        "h1",
        "h2",
        "h3",
        "[class*='title']",
        "[class*='Title']",
    ]

    for selector in selectors:
        element = card.select_one(selector)
        if element:
            title = clean_text(element.get_text(" ", strip=True))
            if is_valid_title(title):
                return title

    price = extract_price(text)
    if price:
        title = clean_text(text.split(price, 1)[0])
        if is_valid_title(title):
            return title

    return None


def extract_price(text) -> Optional[str]:
    patterns = [
        r"(?:UF|U\.F\.|CLF)\s*([\d.,]+)",
        r"([\d.,]+)\s*(?:UF|U\.F\.|CLF)\b",
        r"(\$\s*[\d.,]+)",
        r"(?:CLP|CH\$)\s*([\d.,]+)",
        r"([\d.,]+)\s*(?:pesos|clp)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text or "", flags=re.IGNORECASE)

        if not match:
            continue

        matched_text = clean_text(match.group(0))
        normalized_match = normalize_text(matched_text)

        if "$" in matched_text or "clp" in normalized_match or "peso" in normalized_match:
            return matched_text if "$" in matched_text else f"$ {match.group(1)}"

        return matched_text if "uf" in normalized_match else f"UF {match.group(1)}"

    return None


def normalize_price(price_text):
    if not price_text:
        return None

    normalized_text = normalize_text(price_text)

    if "uf" in normalized_text:
        amount = parse_uf_amount(price_text)

        if amount is None:
            return None

        return {
            "precio_clp": None,
            "precio_uf": amount,
        }

    if "$" in price_text:
        amount = parse_clp_amount(price_text)

        if amount is None:
            return None

        return {
            "precio_clp": amount,
            "precio_uf": None,
        }

    return None


def parse_uf_amount(value):
    amount_text = re.sub(r"(?i)uf", "", value or "")
    amount_text = re.sub(r"[^\d.,]", "", amount_text)

    if not amount_text:
        return None

    amount_text = amount_text.replace(".", "").replace(",", ".")

    try:
        return float(amount_text)
    except ValueError:
        return None


def parse_clp_amount(value):
    digits = re.sub(r"[^\d]", "", value or "")

    if not digits:
        return None

    return int(digits)


def extract_comuna(text):
    known_comunas = [
        "Nunoa",
        "Ñuñoa",
        "Providencia",
        "Santiago",
        "Las Condes",
        "La Reina",
        "Macul",
        "Vitacura",
        "La Florida",
        "San Miguel",
        "Recoleta",
        "Independencia",
        "Penalolen",
        "Peñalolén",
    ]
    normalized_text = normalize_text(text)

    for comuna in known_comunas:
        if normalize_text(comuna) in normalized_text:
            return display_comuna_name(comuna)

    extra_comunas = ["Lo Barnechea", "La Dehesa", "Maipu", "Maipú"]
    for comuna in extra_comunas:
        if normalize_text(comuna) in normalized_text:
            return display_comuna_name(comuna)

    return None


def display_comuna_name(comuna):
    aliases = {
        "nunoa": "Ñuñoa",
        "penalolen": "Peñalolén",
    }
    return aliases.get(normalize_text(comuna), comuna)


def extract_number_near_keywords(text, keywords):
    normalized_text = normalize_text(text)
    is_dormitorio = any("dorm" in normalize_text(keyword) for keyword in keywords)
    is_habitacion = any(
        "habitacion" in normalize_text(keyword) for keyword in keywords
    )
    is_bano = any("bano" in normalize_text(keyword) for keyword in keywords)

    if is_dormitorio or is_habitacion:
        return extract_dormitorios(normalized_text)

    if is_bano:
        return extract_banos(normalized_text)

    for keyword in keywords:
        normalized_keyword = normalize_text(keyword)
        patterns = [
            rf"(\d+)\s*{re.escape(normalized_keyword)}",
            rf"{re.escape(normalized_keyword)}\s*(\d+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, normalized_text)
            if match:
                value = int(match.group(1))
                return value if value > 0 else None

    fallback_patterns = []

    if is_dormitorio:
        fallback_patterns.extend(
            [
                r"(\d+)\s*(?:d|dorm|dorms|hab|habitacion|habitaciones)\b",
                r"(?:dormitorios?|habitaciones?)\s*:?\s*(\d+)",
            ]
        )

    if is_bano:
        fallback_patterns.extend(
            [
                r"(\d+)\s*(?:b|ban|bano|banos|baño|baños)\b",
                r"(?:banos?|baños?|bathrooms?)\s*:?\s*(\d+)",
            ]
        )

    for pattern in fallback_patterns:
        match = re.search(pattern, normalized_text)

        if match:
            value = int(match.group(1))
            return value if value > 0 else None

    return None


def extract_dormitorios(text):
    return extract_bounded_context_number(
        text,
        DORMITORIO_CONTEXT_PATTERN,
        DORMITORIOS_MAX,
        BANO_CONTEXT_PATTERN,
    )


def extract_banos(text):
    return extract_bounded_context_number(
        text,
        BANO_CONTEXT_PATTERN,
        BANOS_MAX,
        DORMITORIO_CONTEXT_PATTERN,
    )


def extract_bounded_context_number(
    text,
    keyword_pattern,
    max_value,
    previous_context_pattern=None,
):
    normalized_text = normalize_text(text)
    patterns = [
        rf"\b(?P<value>\d{{1,2}})\s*(?P<keyword>{keyword_pattern})\b",
        rf"\b(?P<keyword>{keyword_pattern})\s*(?:[:=.-]\s*)?(?P<value>\d{{1,2}})\b",
    ]
    candidates = []

    for pattern_index, pattern in enumerate(patterns):
        for match in re.finditer(pattern, normalized_text):
            value = int(match.group("value"))

            if not 0 < value <= max_value:
                continue

            if pattern_index == 0 and belongs_to_previous_context(
                normalized_text,
                match.start("value"),
                previous_context_pattern,
            ):
                continue

            keyword = match.group("keyword")
            value_span = match.span("value")
            keyword_span = match.span("keyword")
            gap = max(
                0,
                max(value_span[0], keyword_span[0])
                - min(value_span[1], keyword_span[1]),
            )
            candidates.append(
                (-gap, len(keyword), -pattern_index, -match.start(), value)
            )

    if not candidates:
        return None

    return max(candidates)[-1]


def belongs_to_previous_context(text, value_start, previous_context_pattern):
    if not previous_context_pattern:
        return False

    prefix = text[max(0, value_start - 30):value_start]
    match = re.search(rf"(?:{previous_context_pattern})\s*(?:[:=.-]\s*)?$", prefix)

    if not match:
        return False

    text_before_keyword = prefix[:match.start()]
    return re.search(r"\d+\s*$", text_before_keyword) is None


def extract_structured_text(card, fallback_text):
    values = [fallback_text]

    for selector in ATTRIBUTE_SELECTORS:
        for element in card.select(selector):
            texts = [
                element.get_text(" ", strip=True),
                element.get("aria-label"),
                element.get("title"),
                element.get("data-testid"),
            ]
            values.extend(clean_text(text) for text in texts if text)

    return clean_text(" ".join(value for value in values if value))


def parse_decimal_number(value):
    amount_text = clean_text(value).replace(".", "").replace(",", ".")

    try:
        return float(amount_text)
    except ValueError:
        return None


def extract_source_listing_id(url):
    if not url:
        return None

    path = urlparse(url).path
    numeric_ids = re.findall(r"\d{5,}", path)

    if numeric_ids:
        return numeric_ids[-1]

    slug = clean_text(path.rstrip("/").split("/")[-1])
    return slug or None


def is_valid_sale_price(normalized_price) -> bool:
    return has_valid_price(
        normalized_price["precio_clp"],
        normalized_price["precio_uf"],
    )


def has_valid_price(precio_clp, precio_uf):
    if precio_uf is not None:
        return precio_uf > 0

    if precio_clp is not None:
        return precio_clp >= MIN_SALE_PRICE_CLP

    return False


def has_valid_m2(m2_construidos):
    return m2_construidos is not None and m2_construidos > 0


def extract_quality_fields(item):
    return {
        "comuna": item.get("comuna"),
        "m2_construidos": item.get("m2_construidos"),
        "dormitorios": item.get("dormitorios"),
        "banos": item.get("banos"),
        "estacionamientos": item.get("estacionamientos"),
    }


def has_quality_signal(
    comuna=None,
    m2_construidos=None,
    dormitorios=None,
    banos=None,
    estacionamientos=None,
):
    return any(
        value is not None
        for value in [
            comuna,
            m2_construidos,
            dormitorios,
            banos,
            estacionamientos,
        ]
    )


def positive_or_none(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    if number <= 0:
        return None

    if number.is_integer():
        return int(number)

    return number


def positive_int_or_none(value):
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None

    return number if number > 0 else None


def bounded_positive_int_or_none(value, max_value):
    number = positive_int_or_none(value)

    if number is None:
        return None

    return number if number <= max_value else None


def non_negative_int_or_none(value):
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None

    return number if number >= 0 else None


def first_not_none(*values):
    for value in values:
        if value is not None:
            return value

    return None


def float_or_none(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def is_valid_title(title) -> bool:
    normalized_title = normalize_text(title)
    invalid_titles = {
        "venta",
        "arriendo",
        "arriendo de temporada",
        "proyectos nuevos",
        "departamentos",
        "casas",
        "contactar ahora",
        "anadir a favoritos",
        "añadir a favoritos",
        "compara este anuncio",
    }

    if not title or len(title) < 10:
        return False

    return normalized_title not in invalid_titles


def is_navigation_link(link, title) -> bool:
    normalized_link = (link or "").lower()
    normalized_title = normalize_text(title)
    ignored_link_parts = [
        "publicar",
        "ayuda",
        "seguridad",
        "favoritos",
        "login",
        "notificaciones",
        "autos-usados",
        "empleos",
        "marketplace",
        "consejos",
        "centro-de-ayuda",
    ]
    ignored_titles = {
        "venta",
        "arriendo",
        "arriendo de temporada",
        "proyectos nuevos",
        "departamentos",
        "casas",
        "publicar aviso",
    }

    if any(part in normalized_link for part in ignored_link_parts):
        return True

    if normalized_title in ignored_titles:
        return True

    if "yapo.cl" in normalized_link and not is_real_yapo_listing_url(normalized_link):
        return True

    return False


def is_real_yapo_listing_url(url) -> bool:
    parsed = urlparse(url or "")
    path = parsed.path.lower()

    return (
        "yapo.cl" in parsed.netloc.lower()
        and "bienes-raices" in path
        and re.search(r"/\d{5,}$", path) is not None
    )


def clean_text(value) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_text(value) -> str:
    replacements = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ñ": "n",
        "Ã¡": "a",
        "Ã©": "e",
        "Ã­": "i",
        "Ã³": "o",
        "Ãº": "u",
        "Ã±": "n",
    }
    normalized = (value or "").lower()

    for original, replacement in replacements.items():
        normalized = normalized.replace(original, replacement)

    return normalized


def has_strong_partial_listing(
    title,
    link,
    normalized_price,
    comuna,
    dormitorios=None,
    banos=None,
):
    return (
        is_valid_title(title)
        and bool(link)
        and normalized_price is not None
        and bool(comuna)
        and (dormitorios is not None or banos is not None)
    )


def debug_listing_extraction(index, parsed_listing, raw_text):
    if not DEBUG_EXTRACTION or index >= DEBUG_LISTING_LIMIT:
        return

    print("DEBUG YAPO LISTING")
    print(f"- raw_text: {raw_text[:500]}")
    print(
        "- parsed: "
        f"title={parsed_listing.get('titulo')!r}, "
        f"price={parsed_listing.get('precio_texto')!r}, "
        f"comuna={parsed_listing.get('comuna')!r}, "
        f"m2={parsed_listing.get('m2_construidos')!r}, "
        f"dormitorios={parsed_listing.get('dormitorios')!r}, "
        f"banos={parsed_listing.get('banos')!r}, "
        f"link={parsed_listing.get('link')!r}, "
        f"quality={parsed_listing.get('quality')!r}"
    )


def extract_m2_from_text(text):
    normalized_text = normalize_text(text)
    label_pattern = (
        rf"(?:{M2_UTIL_LABEL_PATTERN}|{M2_TOTAL_LABEL_PATTERN}|"
        rf"{M2_BUILT_LABEL_PATTERN}|superficie|sup\.?)"
    )
    patterns = [
        rf"(?P<label>{label_pattern})\s*(?:de)?\s*:?\s*"
        rf"(?P<value>\d+(?:[.,]\d+)?)\s*(?P<unit>{M2_UNIT_PATTERN})?",
        rf"(?P<unit>{M2_UNIT_PATTERN})\s*:?\s*"
        rf"(?P<value>\d+(?:[.,]\d+)?)",
        rf"(?P<value>\d+(?:[.,]\d+)?)\s*(?P<unit>{M2_UNIT_PATTERN})\s*"
        rf"(?P<label>{label_pattern})?",
    ]
    candidates = []

    for pattern_index, pattern in enumerate(patterns):
        for match in re.finditer(pattern, normalized_text, flags=re.IGNORECASE):
            if pattern_index == 0 and label_belongs_to_previous_m2(
                normalized_text,
                match.start("label"),
            ):
                continue

            if pattern_index == 1 and unit_belongs_to_previous_m2_value(
                normalized_text,
                match.start("unit"),
            ):
                continue

            amount = parse_decimal_number(match.group("value"))

            if not is_valid_m2_amount(amount):
                continue

            label = match.groupdict().get("label") or ""
            unit = match.groupdict().get("unit") or ""

            if not label and not unit:
                continue

            candidates.append(
                (
                    m2_context_score(label),
                    1 if unit else 0,
                    -pattern_index,
                    -match.start(),
                    amount,
                )
            )

    if not candidates:
        return None

    return max(candidates)[-1]


def label_belongs_to_previous_m2(text, label_start):
    prefix = text[max(0, label_start - 25):label_start]
    return re.search(
        rf"\d+(?:[.,]\d+)?\s*{M2_UNIT_PATTERN}\s*$",
        prefix,
        flags=re.IGNORECASE,
    ) is not None


def unit_belongs_to_previous_m2_value(text, unit_start):
    prefix = text[max(0, unit_start - 20):unit_start]
    return re.search(r"\d+(?:[.,]\d+)?\s*$", prefix) is not None


def is_valid_m2_amount(amount):
    return amount is not None and M2_MIN_VALUE <= amount <= M2_MAX_VALUE


def m2_context_score(label):
    normalized_label = normalize_text(label)

    if re.search(M2_UTIL_LABEL_PATTERN, normalized_label):
        return 4

    if re.search(M2_BUILT_LABEL_PATTERN, normalized_label):
        return 3

    if re.search(M2_TOTAL_LABEL_PATTERN, normalized_label):
        return 1

    return 2


def extract_compact_attributes(text):
    m2_unit = r"(?:m\s*(?:2|\u00b2|\?|\s+2)|mts?|mt2|mtrs?|metros?)"
    m2_match = re.search(
        rf"\b\d+(?:[.,]\d+)?\s*{m2_unit}\b",
        clean_text(text),
        flags=re.IGNORECASE,
    )

    if not m2_match:
        return {}

    tail = clean_text(text)[m2_match.end():]
    tail = re.split(
        r"\b(?:contactar|compara|anadir|favoritos?|publicado|ver|whatsapp)\b",
        tail,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]

    return {
        "dormitorios": extract_dormitorios(tail),
        "estacionamientos": extract_number_near_keywords(
            tail,
            ["estacionamiento", "estacionamientos", "estac"],
        ),
        "banos": extract_banos(tail),
    }


if __name__ == "__main__":
    scrape_and_save(EXAMPLE_URL)
