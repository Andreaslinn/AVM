from __future__ import annotations

import random
import time
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup

from database import SessionLocal, clean_inactive_listings, init_db
from deduplication import mark_duplicate_listings
from scraper_health import (
    STATUS_FAILED,
    detect_blocking,
    evaluate_source_run,
    filter_valid_scraped_rows,
    print_source_health,
)
from scraper_yapo import (
    EXAMPLE_URL,
    BASE_URL as YAPO_BASE_URL,
    apply_run_status_to_health,
    clean_text,
    extract_source_listing_id,
    get_run_status,
    health_as_dict,
    is_html_processable,
    is_navigation_link,
    normalize_listing_url,
    normalize_price,
    parse_listing_card,
    parse_listings,
    parse_raw_listings,
    save_listing,
)


REQUEST_TIMEOUT_SECONDS = 25
DEFAULT_PAGES = 4
AJAX_BASE_URL = (
    "https://www.yapo.cl/chile-es/ajax/"
    "bienes-raices-venta-de-propiedades-apartamentos"
)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "es-CL,es;q=0.9",
    "Accept": "text/html",
    "Referer": "https://www.yapo.cl/",
}


def build_url(comuna, page):
    query = quote(f"keyword.{normalize_comuna_slug(comuna)}")
    return f"{AJAX_BASE_URL}?issearchresult=1&q={query}&page={page}&list=searchresult"


def fetch_listings_page(comuna, page):
    url = build_url(comuna, page)
    print(f"[API SCRAPER] Fetching {url}")
    session = requests.Session()

    try:
        response = session.get(
            url,
            headers=HEADERS,
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException as error:
        print(f"[API SCRAPER] Error: {error}")
        return ""

    print(f"[DEBUG] HTML length: {len(response.text)}")
    soup = BeautifulSoup(response.text, "html.parser")
    valid_links = find_property_links(soup)
    print(f"[DEBUG] Found {len(valid_links)} property links")
    return response.text


def scrape_comuna(comuna, pages=DEFAULT_PAGES):
    html_pages = []
    block_detected = False

    for page in range(1, pages + 1):
        html = fetch_listings_page(comuna, page)

        if html:
            html_pages.append(html)
            block_detected = block_detected or detect_blocking(html, f"{comuna}:{page}")

        time.sleep(random.uniform(1.5, 3.5))

    combined_html = combine_html_pages(html_pages)
    return build_scrape_result(combined_html, block_detected, comuna=comuna)


def scrape(comuna="nunoa", pages=DEFAULT_PAGES):
    return scrape_comuna(comuna, pages=pages)


def scrape_and_save(comuna="nunoa", pages=DEFAULT_PAGES):
    init_db()
    run_result = scrape_comuna(comuna, pages=pages)
    items = run_result["items"]
    health = run_result["health"]
    run_status = health.get("run_status") or get_run_status(health)
    saved_count = 0
    updated_count = 0

    if run_status == "failed":
        print("WARNING: YAPO API scrape FAILED. No valid rows will be saved from this run.")
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

    if run_status == "partial":
        print("WARNING: YAPO API scrape PARTIAL/DEGRADED. Saving validated rows.")

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


def build_scrape_result(html, block_detected=False, comuna=None):
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

        if not parsed_results:
            parsed_results = parse_api_listings(html, comuna=comuna)
            raw_results = raw_results or parsed_results

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
        print(f"Error parseando listings API: {error}")
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


def parse_api_listings(html, comuna=None):
    soup = BeautifulSoup(html, "html.parser")
    links = find_property_links(soup)
    print(f"[DEBUG] Found {len(links)} property links")
    listings = []
    seen_links = set()

    for link_tag in links:
        try:
            card = link_tag.parent or link_tag
            listing = parse_listing_card(card)
            title = clean_text(listing.get("titulo")) or extract_api_title(card)
            price = listing.get("precio") or extract_api_price(card)
            link = normalize_api_url(clean_text(link_tag.get("href"))) or extract_api_link(card)
            raw_text = clean_text(listing.get("raw_text")) or clean_text(
                card.get_text(" ", strip=True)
            )
            normalized_price = normalize_price(price)

            if not link or link in seen_links:
                continue

            if is_navigation_link(link, title):
                continue

            if not title or normalized_price is None:
                continue

            seen_links.add(link)
            listings.append(
                {
                    "titulo": title,
                    "precio_texto": price,
                    "precio_clp": normalized_price["precio_clp"],
                    "precio_uf": normalized_price["precio_uf"],
                    "url": link,
                    "link": link,
                    "source_listing_id": extract_source_listing_id(link),
                    "comuna": comuna,
                    "raw_text": raw_text,
                    "texto": raw_text,
                }
            )
        except Exception as error:
            print(f"[PARSE ERROR] {error}")

    return listings


def extract_api_title(card):
    title_tag = card.find("span") or card.find("h3")
    return clean_text(title_tag.get_text(strip=True)) if title_tag else None


def extract_api_price(card):
    price_text = card.find(string=lambda value: value and ("$" in value or "UF" in value))
    return clean_text(price_text) if price_text else None


def extract_api_link(card):
    link = card if getattr(card, "name", None) == "a" else card.select_one("a[href]")

    if link is None:
        return None

    href = normalize_api_url(link.get("href"))
    return href if href and is_property_link(href) else None


def find_property_links(soup):
    links_by_href = {}

    for link in soup.find_all("a", href=True):
        href = normalize_api_url(link.get("href"))

        if not href:
            continue

        if "/bienes-raices-venta-de-propiedades" not in href:
            continue

        links_by_href.setdefault(href, link)

    return list(links_by_href.values())


def combine_html_pages(html_pages):
    if not html_pages:
        return ""

    bodies = []
    for html in html_pages:
        soup = BeautifulSoup(html, "html.parser")
        body = soup.select_one("body") or soup
        bodies.append(str(body))

    return "<html><body>" + "\n".join(bodies) + "</body></html>"


def normalize_comuna_slug(comuna):
    return clean_text(comuna).lower().replace("ñ", "n").replace("Ã±", "n").replace(" ", "-")


def normalize_api_url(href):
    if not href:
        return None

    return normalize_listing_url(urljoin(YAPO_BASE_URL, href))


def is_property_link(href):
    parsed = normalize_api_url(href)
    if not parsed:
        return False

    return "yapo.cl" in parsed and "bienes-raices" in parsed


if __name__ == "__main__":
    scrape_and_save("nunoa")
