from __future__ import annotations

import argparse
import time
import unicodedata

import requests
from sqlalchemy import or_, select

from database import SessionLocal
from listing_pipeline import process_listing_pipeline
from micro_location import extract_micro_location_match
from models import Listing


NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "tasador-simple-avm/1.0"
REQUEST_TIMEOUT_SECONDS = 20
RATE_LIMIT_SECONDS = 1
COMUNA_COORDS = {
    "nunoa": (-33.4543, -70.5936),
    "providencia": (-33.4263, -70.6170),
    "las condes": (-33.4080, -70.5670),
    "vitacura": (-33.3850, -70.5650),
    "santiago": (-33.4489, -70.6693),
    "macul": (-33.4870, -70.5990),
}


def run_geocoding(limit=None, force=False):
    updated_count = 0
    skipped_count = 0
    failed_count = 0

    with SessionLocal() as db:
        listings = get_listings_to_geocode(db, limit=limit, force=force)

        for listing in listings:
            micro_location = extract_micro_location_match(listing.titulo)

            if micro_location is not None:
                key = micro_location["key"]
                lat, lon = micro_location["coords"]
                updated_listing = process_listing_pipeline(
                    db,
                    {"listing_id": listing.id, "lat": lat, "lon": lon},
                    source="geocoding",
                )

                if updated_listing is not None:
                    updated_count += 1
                print(
                    f"[MICRO-LOC] Listing {listing.id} matched '{key}' "
                    f"→ lat={lat}, lon={lon}"
                )
                continue

            print(f"[MICRO-LOC] Listing {listing.id} fallback to comuna")
            fixed_coords = get_fixed_comuna_coords(listing.comuna)

            if fixed_coords is not None:
                lat, lon = fixed_coords
                updated_listing = process_listing_pipeline(
                    db,
                    {"listing_id": listing.id, "lat": lat, "lon": lon},
                    source="geocoding",
                )

                if updated_listing is not None:
                    updated_count += 1
                print(
                    f"[GEOCODE] Listing {listing.id} fallback FIXED comuna "
                    f"→ lat={lat}, lon={lon}"
                )
                continue

            query = build_geocoding_query(listing)
            if not query:
                skipped_count += 1
                print(f"[GEOCODE] Listing {listing.id} skipped: missing comuna")
                continue

            result = geocode_query(query)

            if result is None:
                failed_count += 1
                print(f"[GEOCODE] Listing {listing.id} not found: {query}")
                time.sleep(RATE_LIMIT_SECONDS)
                continue

            lat, lon = result
            updated_listing = process_listing_pipeline(
                db,
                {"listing_id": listing.id, "lat": lat, "lon": lon},
                source="geocoding",
            )

            if updated_listing is not None:
                updated_count += 1
            print(f"[GEOCODE] Listing {listing.id} → lat={lat}, lon={lon}")

            time.sleep(RATE_LIMIT_SECONDS)

    print()
    print("[GEOCODE] Finished")
    print(f"[GEOCODE] Updated: {updated_count}")
    print(f"[GEOCODE] Skipped: {skipped_count}")
    print(f"[GEOCODE] Failed: {failed_count}")


def get_listings_to_geocode(db, limit=None, force=False):
    filters = [Listing.comuna.is_not(None)]

    if not force:
        filters.append(or_(Listing.lat.is_(None), Listing.lon.is_(None)))

    statement = select(Listing).where(*filters).order_by(Listing.id)

    if limit is not None:
        statement = statement.limit(limit)

    return list(db.execute(statement).scalars().all())


def get_listings_missing_coordinates(db, limit=None):
    return get_listings_to_geocode(db, limit=limit, force=False)


def build_geocoding_query(listing):
    comuna = clean_text(listing.comuna)

    if not comuna:
        return None

    return f"{comuna}, Santiago, Chile"


def get_fixed_comuna_coords(comuna):
    comuna_normalizada = normalize_comuna(comuna)

    if not comuna_normalizada:
        return None

    return COMUNA_COORDS.get(comuna_normalizada)


def normalize_comuna(comuna):
    comuna = clean_text(comuna)

    if not comuna:
        return None

    comuna = comuna.lower()
    comuna = unicodedata.normalize("NFKD", comuna)
    return "".join(char for char in comuna if not unicodedata.combining(char))


def clean_text(value):
    if value is None:
        return None

    value = str(value).strip()
    return value or None


def geocode_query(query):
    params = {
        "q": query,
        "format": "json",
        "limit": 1,
    }
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }

    try:
        response = requests.get(
            NOMINATIM_URL,
            params=params,
            headers=headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        print(f"[GEOCODE] Request failed: {query} ({exc})")
        return None
    except ValueError as exc:
        print(f"[GEOCODE] Invalid response: {query} ({exc})")
        return None

    if not payload:
        return None

    first_result = payload[0]

    try:
        return float(first_result["lat"]), float(first_result["lon"])
    except (KeyError, TypeError, ValueError):
        return None


def parse_args():
    parser = argparse.ArgumentParser(
        description="Populate missing listing coordinates using Nominatim."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum listings to geocode in this run.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Geocode listings even if they already have lat/lon.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_geocoding(limit=args.limit, force=args.force)
