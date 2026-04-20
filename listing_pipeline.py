from __future__ import annotations

import re
from datetime import date, datetime
from urllib.parse import urlparse, urlunparse

from sqlalchemy import select

from database import DEMO_MODE
from data_cleaning import clean_listing
from deduplication import generate_fingerprint, listing_matches_data, representative_score
from models import Listing, PriceHistory, Property
from scraper_health import validate_scraped_listing


MIN_SALE_PRICE_CLP = 10_000_000


def process_listing_pipeline(db, listing_input, source):
    """Single controlled write path for the listings table."""
    if DEMO_MODE:
        raise RuntimeError("Ingesta de listings desactivada en DEMO_MODE; la DB demo es de solo lectura.")

    try:
        if source == "geocoding":
            listing = process_geocoding_update(db, listing_input)
        else:
            listing_data = normalize_listing_item(listing_input)
            listing = process_listing_upsert(db, listing_data, source)

        db.commit()

        if listing is not None:
            db.refresh(listing)

        return listing
    except Exception:
        db.rollback()
        raise


def process_geocoding_update(db, listing_input):
    listing_id = listing_input.get("id") or listing_input.get("listing_id")
    listing = db.get(Listing, listing_id) if listing_id is not None else None

    if listing is None:
        print(f"[PIPELINE] Listing not found for geocoding update: {listing_id}")
        return None

    lat = float_or_none(listing_input.get("lat"))
    lon = float_or_none(listing_input.get("lon"))

    if lat is None or lon is None:
        print(f"[PIPELINE] Geocoding update skipped for listing {listing_id}")
        return None

    listing.lat = lat
    listing.lon = lon
    db.add(listing)
    return listing


def process_listing_upsert(db, listing_data, source):
    listing_data = clean_listing(listing_data)

    if listing_data is None:
        print("Listing descartado por validacion de calidad")
        return None

    if source == "scraper":
        validation_source = listing_data.get("fuente") or "scraper"
        is_valid_row, validation_errors = validate_scraped_listing(
            validation_source,
            listing_data,
        )

        if not is_valid_row:
            print(
                "Listing descartado por validacion central: "
                + "; ".join(validation_errors)
            )
            return None

    precio_clp = positive_or_none(listing_data.get("precio_clp"))
    precio_uf = positive_or_none(listing_data.get("precio_uf"))

    if source == "scraper" and listing_data.get("url") is None:
        print("Listing descartado por falta de URL")
        return None

    if source == "scraper" and listing_data.get("titulo") is None:
        return None

    if not has_valid_price(precio_clp, precio_uf, source=source):
        return None

    if not has_valid_m2(listing_data.get("m2_construidos")):
        print("Listing descartado por falta de m2")
        return None

    fuente = listing_data.get("fuente") or source
    fingerprint = generate_fingerprint(listing_data)
    listing_data["property_fingerprint"] = fingerprint
    listing = find_existing_listing(db, listing_data)

    was_created = listing is None
    old_precio_clp = listing.precio_clp if listing is not None else None
    old_precio_uf = listing.precio_uf if listing is not None else None

    property_record = resolve_property(db, listing_data, source)

    if listing is None:
        listing = Listing(
            property_id=property_record.id if property_record is not None else None,
            fuente=fuente,
            source_listing_id=listing_data.get("source_listing_id"),
            url=listing_data.get("url"),
            link=listing_data.get("link"),
            status=listing_data.get("status") or default_status_for_source(source),
        )
        db.add(listing)
        print(f"nuevo listing insertado: {listing_data.get('link')}")
    else:
        print(f"listing ya existente actualizado: {listing_data.get('link')}")

    if property_record is not None:
        listing.property_id = property_record.id

    listing.source_listing_id = (
        listing_data.get("source_listing_id") or listing.source_listing_id
    )
    listing.url = listing_data.get("url") or listing.url
    listing.link = listing_data.get("link") or listing.link
    listing.status = listing_data.get("status") or default_status_for_source(source)
    listing.last_seen = datetime.now()
    listing.titulo = listing_data.get("titulo") or listing.titulo
    listing.precio_clp = precio_clp
    listing.precio_uf = precio_uf
    listing.comuna = listing_data.get("comuna")
    listing.lat = listing_data.get("lat")
    listing.lon = listing_data.get("lon")
    listing.m2_construidos = listing_data.get("m2_construidos")
    listing.m2_terreno = listing_data.get("m2_terreno")
    listing.m2_util = listing_data.get("m2_util")
    listing.m2_total = listing_data.get("m2_total")
    listing.dormitorios = listing_data.get("dormitorios")
    listing.banos = listing_data.get("banos")
    listing.estacionamientos = listing_data.get("estacionamientos")
    listing.fecha_publicacion = listing_data.get("fecha_publicacion")
    listing.fecha_captura = date.today()
    listing.property_fingerprint = fingerprint

    price_changed = old_precio_clp != precio_clp or old_precio_uf != precio_uf

    if not was_created and price_changed:
        print(f"precio actualizado en listing existente: {listing_data.get('link')}")

    db.flush()
    listing._was_created = was_created
    add_price_history_if_needed(
        db,
        listing,
        precio_clp_anterior=old_precio_clp,
        precio_uf_anterior=old_precio_uf,
        precio_clp_nuevo=precio_clp,
        precio_uf_nuevo=precio_uf,
        was_created=was_created,
    )
    return listing


def normalize_listing_item(item):
    item = dict(item or {})
    link = normalize_listing_url(clean_text(item.get("link") or item.get("url"))) or None
    url = normalize_listing_url(clean_text(item.get("url") or item.get("link"))) or None
    title = clean_text(item.get("titulo")) or None
    source_listing_id = clean_text(
        item.get("source_listing_id") or extract_source_listing_id(url)
    ) or None

    return {
        **item,
        "fuente": item.get("fuente"),
        "source_listing_id": source_listing_id,
        "url": url,
        "link": link,
        "titulo": title,
        "comuna": clean_text(item.get("comuna")) or None,
        "lat": float_or_none(item.get("lat")),
        "lon": float_or_none(item.get("lon")),
        "precio_texto": item.get("precio_texto"),
        "precio_clp": positive_or_none(item.get("precio_clp")),
        "precio_uf": positive_or_none(item.get("precio_uf")),
        "m2_construidos": positive_or_none(item.get("m2_construidos")),
        "m2_terreno": positive_or_none(item.get("m2_terreno")),
        "m2_util": positive_or_none(item.get("m2_util")),
        "m2_total": positive_or_none(item.get("m2_total")),
        "dormitorios": positive_int_or_none(item.get("dormitorios")),
        "banos": positive_int_or_none(item.get("banos")),
        "estacionamientos": non_negative_int_or_none(item.get("estacionamientos")),
        "fecha_publicacion": item.get("fecha_publicacion"),
        "status": item.get("status"),
    }


def resolve_property(db, listing_data, source):
    if source != "app":
        return None

    property_data = {
        "comuna": listing_data.get("comuna"),
        "lat": listing_data.get("lat"),
        "lon": listing_data.get("lon"),
        "m2_construidos": listing_data.get("m2_construidos"),
        "m2_terreno": listing_data.get("m2_terreno"),
        "m2_util": listing_data.get("m2_util"),
        "m2_total": listing_data.get("m2_total"),
        "dormitorios": listing_data.get("dormitorios"),
        "banos": listing_data.get("banos"),
        "estacionamientos": listing_data.get("estacionamientos"),
    }
    statement = select(Property).filter_by(**property_data)
    property_record = db.execute(statement).scalar_one_or_none()

    if property_record is not None:
        return property_record

    property_record = Property(**property_data)
    db.add(property_record)
    db.flush()
    return property_record


def find_existing_listing(db, listing_data):
    """Find an existing listing before insert, preserving DB unique constraints."""
    exact_match = find_existing_listing_by_identity(db, listing_data)

    if exact_match is not None:
        return exact_match

    return find_existing_listing_by_structure(db, listing_data)


def find_existing_listing_by_identity(db, listing_data):
    fuente = listing_data.get("fuente")
    link = normalize_listing_url(listing_data.get("link"))
    url = normalize_listing_url(listing_data.get("url"))
    source_listing_id = listing_data.get("source_listing_id")

    if fuente and link:
        listing = db.execute(
            select(Listing).where(
                Listing.fuente == fuente,
                Listing.link == link,
            )
        ).scalar_one_or_none()

        if listing is not None:
            return listing

    if fuente and url:
        listing = db.execute(
            select(Listing).where(
                Listing.fuente == fuente,
                Listing.url == url,
            )
        ).scalar_one_or_none()

        if listing is not None:
            return listing

    if fuente and source_listing_id:
        return db.execute(
            select(Listing).where(
                Listing.fuente == fuente,
                Listing.source_listing_id == source_listing_id,
            )
        ).scalar_one_or_none()

    return None


def find_existing_listing_by_structure(db, listing_data):
    """Find an equivalent listing using fingerprint and structural tolerance."""
    fingerprint = listing_data.get("property_fingerprint") or generate_fingerprint(
        listing_data
    )
    candidates = []

    if fingerprint:
        candidates.extend(
            db.execute(
                select(Listing).where(Listing.property_fingerprint == fingerprint)
            )
            .scalars()
            .all()
        )

    candidates.extend(find_structural_candidates(db, listing_data))
    unique_candidates = unique_listings_by_id(candidates)
    matching_candidates = [
        candidate
        for candidate in unique_candidates
        if listing_matches_data(candidate, listing_data)
    ]

    if not matching_candidates:
        return None

    return max(matching_candidates, key=representative_score)


def find_structural_candidates(db, listing_data):
    comuna = listing_data.get("comuna")
    m2 = listing_data.get("m2_construidos")

    if not comuna or m2 is None or m2 <= 0:
        return []

    min_m2 = m2 * 0.88
    max_m2 = m2 * 1.12
    statement = select(Listing).where(
        Listing.comuna == comuna,
        Listing.m2_construidos.is_not(None),
        Listing.m2_construidos >= min_m2,
        Listing.m2_construidos <= max_m2,
    )
    return list(db.execute(statement).scalars().all())


def unique_listings_by_id(listings):
    unique = {}

    for listing in listings:
        unique[listing.id] = listing

    return list(unique.values())


def add_price_history_if_needed(
    db,
    listing,
    precio_clp_anterior,
    precio_uf_anterior,
    precio_clp_nuevo,
    precio_uf_nuevo,
    was_created=False,
):
    price_changed = (
        precio_clp_anterior != precio_clp_nuevo
        or precio_uf_anterior != precio_uf_nuevo
    )

    if not was_created and not price_changed:
        return

    statement = (
        select(PriceHistory)
        .where(PriceHistory.listing_id == listing.id)
        .order_by(PriceHistory.fecha_cambio.desc(), PriceHistory.id.desc())
        .limit(1)
    )
    last_price = db.execute(statement).scalar_one_or_none()

    if (
        last_price is not None
        and last_price.precio_clp_nuevo == precio_clp_nuevo
        and last_price.precio_uf_nuevo == precio_uf_nuevo
        and last_price.fecha_captura == listing.fecha_captura
    ):
        return

    db.add(
        PriceHistory(
            listing_id=listing.id,
            precio_clp=precio_clp_nuevo,
            precio_uf=precio_uf_nuevo,
            precio_clp_anterior=precio_clp_anterior,
            precio_uf_anterior=precio_uf_anterior,
            precio_clp_nuevo=precio_clp_nuevo,
            precio_uf_nuevo=precio_uf_nuevo,
            fecha_captura=listing.fecha_captura,
            fecha_cambio=datetime.now(),
        )
    )
    print("price history registrado")


def default_status_for_source(source):
    if source == "app":
        return "appraisal_result"

    return "active"


def has_valid_price(precio_clp, precio_uf, source):
    if precio_uf is not None:
        return precio_uf > 0

    if precio_clp is not None:
        if source == "scraper":
            return precio_clp >= MIN_SALE_PRICE_CLP
        return precio_clp > 0

    return False


def has_valid_m2(m2_construidos):
    return m2_construidos is not None and m2_construidos > 0


def normalize_listing_url(url):
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


def extract_source_listing_id(url):
    if not url:
        return None

    match = re.search(r"(MLC[-_]?\d+|\d{5,})", url, flags=re.IGNORECASE)
    return match.group(1) if match else None


def clean_text(value):
    return re.sub(r"\s+", " ", value or "").strip()


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


def non_negative_int_or_none(value):
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None

    return number if number >= 0 else None


def float_or_none(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
