from __future__ import annotations

import json
import random
import re
import time
from typing import Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from data_cleaning import clean_listings
from scraper_health import (
    STATUS_DEGRADED,
    STATUS_FAILED,
    detect_blocking,
    evaluate_source_run,
    filter_valid_scraped_rows,
    print_source_health,
)


DEFAULT_TIMEOUT_SECONDS = 20
MIN_POST_LOAD_WAIT_SECONDS = 5
MAX_POST_LOAD_WAIT_SECONDS = 10
SCROLL_PAUSE_RANGE_SECONDS = (0.7, 1.8)
FUENTE = "portalinmobiliario"
BASE_URL = "https://www.portalinmobiliario.com"
MERCADOLIBRE_API_HOST = "api.mercadolibre.com"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
ATTRIBUTE_SELECTORS = [
    ".ui-search-card-attributes__attribute",
    ".ui-pdp-highlighted-specs-res__icon-label",
    ".andes-table__column",
    "[class*='attribute']",
    "[class*='Attribute']",
    "[class*='feature']",
    "[class*='Feature']",
    "[class*='spec']",
    "[class*='Spec']",
    "[class*='detail']",
    "[class*='Detail']",
    "[aria-label]",
    "li",
    "span",
]
LAST_BLOCK_DETECTED = False
LAST_RAW_ROWS = 0


def fetch_search_page(url: str) -> str:
    return fetch_search_json_from_browser(url)


def fetch_search_json_from_browser(url: str) -> list[dict]:
    global LAST_BLOCK_DETECTED
    global LAST_RAW_ROWS

    LAST_BLOCK_DETECTED = False
    LAST_RAW_ROWS = 0
    driver = iniciar_driver()

    try:
        driver.get(url)
        wait_for_page_ready(driver)
        wait_for_document_complete(driver)
        human_pause(MIN_POST_LOAD_WAIT_SECONDS, MAX_POST_LOAD_WAIT_SECONDS)

        if is_login_page(driver.current_url, driver.page_source):
            print("LOGIN DETECTADO - SCRAPING BLOQUEADO")
            LAST_BLOCK_DETECTED = True
            return []

        move_mouse_like_human(driver)
        scroll_page(driver)
        human_pause(2, 4)

        if is_login_page(driver.current_url, driver.page_source):
            print("LOGIN DETECTADO - SCRAPING BLOQUEADO")
            LAST_BLOCK_DETECTED = True
            return []

        payloads = extract_search_payloads_from_network(driver)

        for payload in payloads:
            LAST_RAW_ROWS = len(payload.get("results", []))
            listings = map_api_results_to_listings(payload)

            if listings:
                print(f"JSON INTERCEPTADO: {payload.get('_source_url')}")
                return listings

        print("POSIBLE BLOQUEO")
        LAST_BLOCK_DETECTED = detect_blocking(driver.page_source, driver.current_url)
        return []
    finally:
        driver.quit()


def iniciar_driver():
    options = Options()
    options.add_argument(f"--user-agent={USER_AGENT}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-infobars")
    options.add_argument("--lang=es-CL,es")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-notifications")
    options.add_argument("--window-size=1366,900")
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_experimental_option(
        "prefs",
        {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
            "intl.accept_languages": "es-CL,es,en-US,en",
        },
    )

    driver = webdriver.Chrome(service=Service(), options=options)
    driver.execute_cdp_cmd("Network.enable", {})
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => false
                });
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['es-CL', 'es', 'en-US', 'en']
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                window.chrome = window.chrome || { runtime: {} };
            """,
        },
    )
    return driver


def wait_for_page_ready(driver) -> None:
    wait = WebDriverWait(driver, DEFAULT_TIMEOUT_SECONDS)
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))


def wait_for_document_complete(driver) -> None:
    wait = WebDriverWait(driver, DEFAULT_TIMEOUT_SECONDS)
    wait.until(
        lambda active_driver: active_driver.execute_script(
            "return document.readyState"
        )
        == "complete"
    )


def scroll_page(driver) -> None:
    page_height = driver.execute_script("return document.body.scrollHeight")
    current_position = 0

    while current_position < page_height:
        current_position += random.randint(260, 520)
        driver.execute_script("window.scrollTo(0, arguments[0]);", current_position)
        human_pause(*SCROLL_PAUSE_RANGE_SECONDS)
        move_mouse_like_human(driver)
        page_height = driver.execute_script("return document.body.scrollHeight")

        if current_position >= min(page_height, 4200):
            break

    driver.execute_script("window.scrollBy(0, arguments[0]);", -random.randint(180, 420))
    human_pause(0.8, 1.8)
    driver.execute_script("window.scrollBy(0, arguments[0]);", random.randint(280, 640))
    human_pause(*SCROLL_PAUSE_RANGE_SECONDS)


def human_pause(min_seconds: float, max_seconds: float) -> None:
    time.sleep(random.uniform(min_seconds, max_seconds))


def move_mouse_like_human(driver) -> None:
    try:
        body = driver.find_element(By.TAG_NAME, "body")
        actions = ActionChains(driver)
        actions.move_to_element_with_offset(
            body,
            random.randint(80, 600),
            random.randint(80, 500),
        )
        actions.pause(random.uniform(0.2, 0.8))
        actions.move_by_offset(random.randint(-40, 40), random.randint(-30, 30))
        actions.perform()
    except WebDriverException:
        pass


def extract_search_payloads_from_network(driver) -> list[dict]:
    payloads = []

    for entry in driver.get_log("performance"):
        try:
            message = json.loads(entry["message"])["message"]
        except (KeyError, json.JSONDecodeError):
            continue

        if message.get("method") != "Network.responseReceived":
            continue

        params = message.get("params", {})
        response = params.get("response", {})
        response_url = response.get("url", "")

        if not looks_like_search_api_response(response_url, response):
            continue

        payload = get_response_json(driver, params.get("requestId"))

        if payload is None:
            continue

        payload["_source_url"] = response_url
        payloads.append(payload)

    return payloads


def looks_like_search_api_response(url: str, response: dict) -> bool:
    normalized_url = (url or "").lower()
    mime_type = (response.get("mimeType") or "").lower()

    if MERCADOLIBRE_API_HOST not in normalized_url:
        return False

    if "json" not in mime_type:
        return False

    search_markers = [
        "/sites/mlc/search",
        "/search",
        "search?",
        "category",
        "real_estate",
    ]
    return any(marker in normalized_url for marker in search_markers)


def get_response_json(driver, request_id: Optional[str]) -> Optional[dict]:
    if not request_id:
        return None

    try:
        body = driver.execute_cdp_cmd(
            "Network.getResponseBody",
            {"requestId": request_id},
        )
    except WebDriverException:
        return None

    if body.get("base64Encoded"):
        return None

    try:
        payload = json.loads(body.get("body") or "")
    except json.JSONDecodeError:
        return None

    if isinstance(payload, dict) and isinstance(payload.get("results"), list):
        return payload

    return None


def map_api_results_to_listings(payload: dict) -> list[dict]:
    listings = [map_api_item_to_listing(item) for item in payload.get("results", [])]
    return clean_listings(listings)


def map_api_item_to_listing(item: dict) -> dict:
    price = positive_number_or_none(item.get("price"))
    currency_id = item.get("currency_id")
    attributes = attributes_by_id(item.get("attributes", []))
    address = item.get("address") or {}
    location = item.get("location") or {}
    permalink = item.get("permalink")
    m2_construidos = first_number_attribute(
        attributes,
        [
            "COVERED_AREA",
            "TOTAL_AREA",
            "PROPERTY_SIZE",
            "CONSTRUCTED_AREA",
            "BUILT_AREA",
            "LIVING_AREA",
            "SURFACE_TOTAL",
            "SURFACE_COVERED",
        ],
    )

    return {
        "fuente": FUENTE,
        "source_listing_id": item.get("id"),
        "url": permalink,
        "titulo": item.get("title"),
        "precio_texto": format_api_price(price, currency_id),
        "precio_clp": price if currency_id == "CLP" else None,
        "precio_uf": price if currency_id == "CLF" else None,
        "comuna": (
            address.get("city_name")
            or value_name(attributes, "CITY")
            or value_name(attributes, "LOCATION")
        ),
        "m2": m2_construidos,
        "m2_construidos": m2_construidos,
        "m2_terreno": first_number_attribute(attributes, ["TOTAL_AREA", "LAND_AREA"]),
        "m2_util": first_number_attribute(attributes, ["COVERED_AREA"]),
        "m2_total": first_number_attribute(attributes, ["TOTAL_AREA"]),
        "dormitorios": first_number_attribute(
            attributes,
            ["BEDROOMS", "ROOMS", "BEDROOM", "DORMITORIES"],
        ),
        "banos": first_number_attribute(
            attributes,
            ["FULL_BATHROOMS", "BATHROOMS", "BATHROOM", "HALF_BATHROOMS"],
        ),
        "lat": location.get("latitude"),
        "lon": location.get("longitude"),
    }


def attributes_by_id(attributes: list[dict]) -> dict:
    return {
        attribute.get("id"): attribute
        for attribute in attributes
        if attribute.get("id")
    }


def value_name(attributes: dict, attribute_id: str) -> Optional[str]:
    attribute = attributes.get(attribute_id) or {}
    return attribute.get("value_name")


def first_number_attribute(attributes: dict, attribute_ids: list[str]) -> Optional[float]:
    for attribute_id in attribute_ids:
        attribute = attributes.get(attribute_id)

        if not attribute:
            continue

        value = attribute.get("value_struct", {}).get("number")
        if value is None:
            value = parse_number(attribute.get("value_name"))

        value = positive_number_or_none(value)

        if value is not None:
            return value

    return None


def parse_number(value: Optional[str]) -> Optional[float]:
    match = re.search(r"\d+(?:[.,]\d+)?", value or "")

    if not match:
        return None

    try:
        return positive_number_or_none(match.group(0).replace(".", "").replace(",", "."))
    except ValueError:
        return None


def format_api_price(price, currency_id: Optional[str]) -> Optional[str]:
    if price is None:
        return None

    if currency_id == "CLF":
        return f"UF {price}"

    if currency_id == "CLP":
        return f"$ {price}"

    return str(price)


def scrape_search_url(url: str) -> list[dict]:
    return scrape_search_url_with_status(url)["items"]


def scrape_search_url_with_status(url: str) -> dict:
    try:
        data = fetch_search_page(url)
    except Exception as error:
        print(f"Error descargando datos JSON: {error}")
        health = evaluate_source_run(
            source=FUENTE,
            raw_rows=0,
            valid_rows=0,
            block_detected=True,
        )
        health.status = STATUS_FAILED
        health.reasons.append(f"download error: {error}")
        print_source_health(health)
        return {"items": [], "health": health.as_dict(), "rejected_rows": []}

    try:
        parsed_listings = parse_search_results(data)
        listings, rejected_rows = filter_valid_scraped_rows(FUENTE, parsed_listings)
    except Exception as error:
        print(f"Error parseando resultados: {error}")
        health = evaluate_source_run(
            source=FUENTE,
            raw_rows=0,
            valid_rows=0,
            block_detected=LAST_BLOCK_DETECTED,
        )
        health.status = STATUS_FAILED
        health.reasons.append(f"parse error: {error}")
        print_source_health(health)
        return {"items": [], "health": health.as_dict(), "rejected_rows": []}

    if not listings:
        print("POSIBLE BLOQUEO")
        print_no_listings_debug(data, url)

    raw_rows = LAST_RAW_ROWS or len(parsed_listings)
    health = evaluate_source_run(
        source=FUENTE,
        raw_rows=raw_rows,
        valid_rows=len(listings),
        block_detected=LAST_BLOCK_DETECTED,
    )
    print_source_health(health)

    if rejected_rows:
        print(f"Listings rechazados por validacion: {len(rejected_rows)}")

    if health.status == STATUS_FAILED:
        print("WARNING: PORTALINMOBILIARIO scrape FAILED. No rows should be ingested.")
        return {"items": [], "health": health.as_dict(), "rejected_rows": rejected_rows}

    if health.status == STATUS_DEGRADED:
        print("WARNING: PORTALINMOBILIARIO scrape DEGRADED. Use only validated rows.")

    return {"items": listings, "health": health.as_dict(), "rejected_rows": rejected_rows}


def print_no_listings_debug(data, current_url: Optional[str]) -> None:
    print("NO SE ENCONTRARON LISTINGS")
    debug_text = json.dumps(data, ensure_ascii=False) if not isinstance(data, str) else data
    print((debug_text or "")[:2000])
    print(f"URL actual: {current_url or 'desconocida'}")


def is_login_page(url: Optional[str], html: Optional[str]) -> bool:
    normalized_url = (url or "").lower()

    if "login" in normalized_url or "registration" in normalized_url:
        return True

    page_text = BeautifulSoup(html or "", "html.parser").get_text(" ", strip=True)
    normalized_text = normalize_for_matching(page_text)
    return "soy nuevo" in normalized_text or "ya tengo cuenta" in normalized_text


def parse_search_results(html) -> list[dict]:
    if isinstance(html, list):
        listings = []
        seen_urls = set()

        for listing in html:
            url = listing.get("url")

            if not is_valid_listing(listing):
                continue

            if url and url in seen_urls:
                continue

            if url:
                seen_urls.add(url)

            listings.append(listing)

        return clean_listings(listings)

    soup = BeautifulSoup(html or "", "html.parser")
    cards = find_listing_cards(soup)
    listings = []
    seen_urls = set()

    for card in cards:
        listing = parse_listing_card(card)
        url = listing["url"]

        if not is_valid_listing(listing):
            continue

        if url and url in seen_urls:
            continue

        if url:
            seen_urls.add(url)

        listings.append(listing)

    return clean_listings(listings)


def find_listing_cards(soup: BeautifulSoup) -> list:
    selectors = [
        "li.ui-search-layout__item",
        "div.ui-search-result",
        "div.poly-card",
        "article",
    ]

    for selector in selectors:
        cards = soup.select(selector)
        if cards:
            return cards

    # Limitacion: PortalInmobiliario/MercadoLibre cambia nombres de clases y a
    # veces renderiza parte del listado con JS. Este fallback captura anchors
    # plausibles, pero no garantiza todos los atributos visibles en navegador.
    return [
        link
        for link in soup.find_all("a", href=True)
        if looks_like_listing_url(link.get("href"))
    ]


def parse_listing_card(card) -> dict:
    text = clean_text(card.get_text(" ", strip=True))
    structured_text = extract_structured_text(card, text)
    url = extract_url(card)
    title = extract_title(card)
    price_text = extract_price_text(card, structured_text)
    normalized_price = normalize_price(price_text)
    m2_construidos = extract_m2_from_text(structured_text)

    return {
        "fuente": FUENTE,
        "source_listing_id": extract_source_listing_id(url),
        "url": url,
        "titulo": title,
        "precio_texto": price_text,
        "precio_clp": normalized_price["precio_clp"] if normalized_price else None,
        "precio_uf": normalized_price["precio_uf"] if normalized_price else None,
        "comuna": extract_comuna(card, structured_text),
        "m2": m2_construidos,
        "m2_construidos": m2_construidos,
        "m2_terreno": None,
        "m2_util": None,
        "m2_total": None,
        "dormitorios": extract_int_near_keywords(
            structured_text,
            ["dormitorio", "dormitorios", "dorm"],
        ),
        "banos": extract_int_near_keywords(
            structured_text,
            ["bano", "banos", "baño", "baños"],
        ),
    }


def extract_url(card) -> Optional[str]:
    link = card if getattr(card, "name", None) == "a" else card.find("a", href=True)

    if not link:
        return None

    href = link.get("href")
    if not href:
        return None

    return normalize_url(href)


def normalize_url(href: str) -> Optional[str]:
    href = clean_text(href)

    if not href:
        return None

    return urljoin(BASE_URL, href)


def looks_like_listing_url(href: Optional[str]) -> bool:
    if not href:
        return False

    normalized_href = href.lower()
    return (
        "portalinmobiliario.com" in normalized_href
        or "/mlc-" in normalized_href
        or "/propiedades/" in normalized_href
    )


def extract_source_listing_id(url: Optional[str]) -> Optional[str]:
    if not url:
        return None

    path = urlparse(url).path
    mlc_match = re.search(r"MLC[-_]?(\d+)", path, flags=re.IGNORECASE)

    if mlc_match:
        return f"MLC-{mlc_match.group(1)}"

    numeric_ids = re.findall(r"\d{5,}", path)
    if numeric_ids:
        return numeric_ids[-1]

    return None


def extract_price_text(card, full_text: str) -> Optional[str]:
    selectors = [
        ".andes-money-amount",
        ".andes-money-amount__currency-symbol",
        ".andes-money-amount__fraction",
        "[class*='price']",
        "[class*='Price']",
        "[aria-label*='pesos']",
        "[aria-label*='UF']",
    ]

    for selector in selectors:
        element = card.select_one(selector)
        if element:
            price_text = clean_text(element.get_text(" ", strip=True))
            price_text = enrich_price_with_symbol(element, price_text)
            if price_text:
                return price_text

    patterns = [
        r"(?:UF|U\.F\.|CLF)\s*([\d.,]+)",
        r"([\d.,]+)\s*(?:UF|U\.F\.|CLF)\b",
        r"(\$\s*[\d.,]+)",
        r"(?:CLP|CH\$)\s*([\d.,]+)",
        r"([\d.,]+)\s*(?:pesos|clp)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, full_text or "", flags=re.IGNORECASE)

        if not match:
            continue

        matched_text = clean_text(match.group(0))
        normalized_match = normalize_for_matching(matched_text)

        if "$" in matched_text or "clp" in normalized_match or "peso" in normalized_match:
            return matched_text if "$" in matched_text else f"$ {match.group(1)}"

        return matched_text if "uf" in normalized_match else f"UF {match.group(1)}"

    return None


def enrich_price_with_symbol(element, price_text: str) -> str:
    if not price_text:
        return price_text

    parent_text = clean_text(element.parent.get_text(" ", strip=True)) if element.parent else ""
    combined_text = f"{parent_text} {price_text}"

    if "UF" in combined_text.upper() and "UF" not in price_text.upper():
        return f"UF {price_text}"

    if "$" in combined_text and "$" not in price_text:
        return f"$ {price_text}"

    return price_text


def normalize_price(price_text: Optional[str]) -> Optional[dict]:
    if not price_text:
        return None

    normalized_text = normalize_for_matching(price_text)

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


def parse_uf_amount(value: Optional[str]) -> Optional[float]:
    amount_text = re.sub(r"(?i)uf", "", value or "")
    amount_text = re.sub(r"[^\d.,]", "", amount_text)

    if not amount_text:
        return None

    amount_text = amount_text.replace(".", "").replace(",", ".")

    try:
        amount = float(amount_text)
    except ValueError:
        return None

    return amount if amount > 0 else None


def parse_clp_amount(value: Optional[str]) -> Optional[int]:
    digits = re.sub(r"[^\d]", "", value or "")

    if not digits:
        return None

    amount = int(digits)
    return amount if amount > 0 else None


def extract_int_near_keywords(text: str, keywords: list[str]) -> Optional[int]:
    normalized_text = normalize_for_matching(text)
    is_dormitorio = any("dorm" in normalize_for_matching(keyword) for keyword in keywords)
    is_bano = any("bano" in normalize_for_matching(keyword) for keyword in keywords)

    for keyword in keywords:
        normalized_keyword = normalize_for_matching(keyword)
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


def extract_m2_from_text(text: str) -> Optional[float]:
    patterns = [
        r"(\d+(?:[.,]\d+)?)\s*m(?:2|²|Â²|Ã‚Â²)\b",
        r"(?:m(?:2|²|Â²|Ã‚Â²)|metros?\s*cuadrados?|superficie|sup\.?|construid[ao]s?|cubiert[ao]s?|util(?:es)?|útil(?:es)?)\s*:?\s*(\d+(?:[.,]\d+)?)",
        r"(\d+(?:[.,]\d+)?)\s*(?:m(?:2|²|Â²|Ã‚Â²)|metros?)\s*(?:construid[ao]s?|cubiert[ao]s?|util(?:es)?|útil(?:es)?|totales?)",
        r"(\d+(?:[.,]\d+)?)\s*m(?:2|²|Â²)",
        r"(\d+(?:[.,]\d+)?)\s*metros?\s*cuadrados?",
    ]

    for pattern in patterns:
        match = re.search(pattern, text or "", flags=re.IGNORECASE)
        if match:
            amount_text = match.group(1).replace(".", "").replace(",", ".")

            try:
                amount = float(amount_text)
            except ValueError:
                return None

            return amount if amount > 0 else None

    return None


def extract_structured_text(card, fallback_text: str) -> str:
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


def extract_comuna(card, full_text: str) -> Optional[str]:
    selectors = [
        "[class*='location']",
        "[class*='Location']",
        "[class*='address']",
        "[class*='Address']",
    ]

    for selector in selectors:
        element = card.select_one(selector)
        if element:
            location_text = clean_text(element.get_text(" ", strip=True))
            comuna = comuna_from_location_text(location_text)
            if comuna:
                return comuna

    return comuna_from_location_text(full_text) or extract_known_comuna(full_text)


def extract_title(card) -> Optional[str]:
    selectors = [
        "h1",
        "h2",
        "h3",
        "a[title]",
        "[class*='title']",
        "[class*='Title']",
    ]

    for selector in selectors:
        element = card.select_one(selector)
        if not element:
            continue

        title = element.get("title") or element.get_text(" ", strip=True)
        title = clean_text(title)
        if title and not looks_like_price_or_attribute(title):
            return title

    link = card if getattr(card, "name", None) == "a" else card.find("a")
    if link:
        title = clean_text(link.get("title") or link.get_text(" ", strip=True))
        return title or None

    return None


def comuna_from_location_text(text: str) -> Optional[str]:
    if not text:
        return None

    parts = [clean_text(part) for part in re.split(r",|\|", text) if clean_text(part)]

    for part in parts:
        if looks_like_location_candidate(part):
            return part

    return None


def extract_known_comuna(text: str) -> Optional[str]:
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
        "Lo Barnechea",
        "La Dehesa",
        "Puente Alto",
        "Maipu",
        "Maipú",
    ]
    normalized_text = normalize_for_matching(text)

    for comuna in known_comunas:
        if normalize_for_matching(comuna) in normalized_text:
            return display_comuna_name(comuna)

    return None


def display_comuna_name(comuna: str) -> str:
    aliases = {
        "nunoa": "Ñuñoa",
        "penalolen": "Peñalolén",
        "maipu": "Maipú",
    }
    return aliases.get(normalize_for_matching(comuna), comuna)


def looks_like_location_candidate(text: str) -> bool:
    if not text:
        return False

    if looks_like_price_or_attribute(text):
        return False

    normalized_text = normalize_for_matching(text)
    ignored_words = ["publicado", "vende", "inmobiliaria", "corredora", "nuevo"]
    return not any(word in normalized_text for word in ignored_words)


def looks_like_price_or_attribute(text: str) -> bool:
    normalized_text = normalize_for_matching(text)
    attribute_words = [
        "dorm",
        "bano",
        "banos",
        "baño",
        "baños",
        "m2",
        "metro",
        "uf",
        "$",
    ]
    return any(word in normalized_text for word in attribute_words)


def has_minimum_listing_signal(listing: dict) -> bool:
    return any(
        listing.get(field)
        for field in [
            "url",
            "titulo",
            "precio_texto",
            "precio_clp",
            "precio_uf",
        ]
    )


def is_valid_listing(listing: dict) -> bool:
    url = clean_text(listing.get("url"))
    title = clean_text(listing.get("titulo"))

    if not is_valid_listing_url(url):
        return False

    if listing.get("precio_clp") is None and listing.get("precio_uf") is None:
        return False

    if not has_valid_m2(listing.get("m2_construidos")):
        return False

    if not is_real_listing_title(title):
        return False

    return looks_like_property_listing(listing)


def is_valid_listing_url(url: str) -> bool:
    if not url:
        return False

    normalized_url = url.lower()
    blocked_parts = [
        "login",
        "registration",
        "mercadolibre.com/jms",
    ]

    return not any(part in normalized_url for part in blocked_parts)


def is_real_listing_title(title: str) -> bool:
    if not title:
        return False

    normalized_title = normalize_for_matching(title)
    invalid_titles = {
        "soy nuevo",
        "ya tengo cuenta",
        "ingresar",
        "crear cuenta",
        "iniciar sesion",
        "iniciar sesión",
    }

    return normalized_title not in invalid_titles


def looks_like_property_listing(listing: dict) -> bool:
    text = " ".join(
        clean_text(str(value))
        for value in [
            listing.get("titulo"),
            listing.get("precio_texto"),
            listing.get("url"),
            listing.get("comuna"),
            listing.get("m2"),
            listing.get("m2_construidos"),
        ]
        if value is not None
    )
    normalized_text = normalize_for_matching(text)
    property_signals = [
        "uf",
        "$",
        "m2",
        "metro",
        "dorm",
        "bano",
        "banos",
        "departamento",
        "casa",
        "propiedad",
    ]

    return any(signal in normalized_text for signal in property_signals)


def has_valid_m2(m2_construidos) -> bool:
    return m2_construidos is not None and m2_construidos > 0


def positive_number_or_none(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    if number <= 0:
        return None

    if number.is_integer():
        return int(number)

    return number


def clean_text(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_for_matching(value: Optional[str]) -> str:
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


if __name__ == "__main__":
    example_url = "https://www.portalinmobiliario.com/venta/departamento/metropolitana"
    run_result = scrape_search_url_with_status(example_url)
    results = run_result["items"]

    print(f"Resultados encontrados: {len(results)}")
    for result in results[:5]:
        print(result)
