from __future__ import annotations

from datetime import datetime
from typing import Iterable, Optional

from sqlalchemy import select

from database import DEMO_MODE, SessionLocal, init_db
from main import normalize_comuna
from models import Listing


M2_MATCH_TOLERANCE = 0.10
ROOM_MATCH_TOLERANCE = 1
ROUND_M2_TO = 5
TITLE_FINGERPRINT_WORDS = 6


def rounded_m2(value: Optional[float], step: int = ROUND_M2_TO) -> Optional[int]:
    if value is None or value <= 0:
        return None

    return int(round(float(value) / step) * step)


def small_int_or_unknown(value: Optional[int]) -> str:
    if value is None:
        return "x"

    try:
        return str(int(value))
    except (TypeError, ValueError):
        return "x"


def build_property_fingerprint(listing: Listing) -> Optional[str]:
    """Coarse property fingerprint. It is a candidate key, not a hard identity."""
    return generate_fingerprint(
        {
            "comuna": listing.comuna,
            "m2_construidos": listing.m2_construidos,
            "dormitorios": listing.dormitorios,
            "banos": listing.banos,
            "estacionamientos": listing.estacionamientos,
            "titulo": listing.titulo,
        }
    )


def generate_fingerprint(listing_data: dict) -> Optional[str]:
    """Stable structural fingerprint independent from price, coordinates and time."""
    comuna = normalize_comuna(listing_data.get("comuna"))
    m2_bucket = rounded_m2(listing_data.get("m2_construidos"))

    if not comuna or m2_bucket is None:
        return None

    parts = [
        comuna,
        f"m2:{m2_bucket}",
        f"d:{small_int_or_unknown(listing_data.get('dormitorios'))}",
        f"b:{small_int_or_unknown(listing_data.get('banos'))}",
        f"p:{small_int_or_unknown(listing_data.get('estacionamientos'))}",
    ]
    title_key = normalized_title_key(listing_data.get("titulo"))

    if title_key:
        parts.append(f"t:{title_key}")

    return "|".join(parts)


def normalized_title_key(title: Optional[str]) -> Optional[str]:
    normalized = normalize_comuna(title)

    if not normalized:
        return None

    words = [
        word
        for word in normalized.split()
        if len(word) > 2 and word not in {"venta", "vende", "depto", "departamento"}
    ]

    if not words:
        return None

    return "-".join(words[:TITLE_FINGERPRINT_WORDS])


def listing_matches_data(listing: Listing, listing_data: dict) -> bool:
    """Property equivalence check for pre-insert matching."""
    if normalize_comuna(listing.comuna) != normalize_comuna(listing_data.get("comuna")):
        return False

    if not values_within_ratio(
        listing.m2_construidos,
        listing_data.get("m2_construidos"),
        M2_MATCH_TOLERANCE,
    ):
        return False

    if not room_values_compatible(listing.dormitorios, listing_data.get("dormitorios")):
        return False

    if not room_values_compatible(listing.banos, listing_data.get("banos")):
        return False

    return parking_values_compatible(
        listing.estacionamientos,
        listing_data.get("estacionamientos"),
    )


def values_within_ratio(left, right, tolerance_ratio: float) -> bool:
    if left is None or right is None:
        return False

    left = float(left)
    right = float(right)

    if left <= 0 or right <= 0:
        return False

    return abs(left - right) <= max(left, right) * tolerance_ratio


def room_values_compatible(left, right) -> bool:
    if left is None or right is None:
        return False

    try:
        return abs(int(left) - int(right)) <= ROOM_MATCH_TOLERANCE
    except (TypeError, ValueError):
        return False


def parking_values_compatible(left, right) -> bool:
    """Parking helps avoid bad merges when both sources publish it."""
    if left is None or right is None:
        return True

    return room_values_compatible(left, right)


def same_property(left: Listing, right: Listing) -> bool:
    """Conservative property-level duplicate rule. Price is intentionally ignored."""
    if normalize_comuna(left.comuna) != normalize_comuna(right.comuna):
        return False

    if not values_within_ratio(
        left.m2_construidos,
        right.m2_construidos,
        M2_MATCH_TOLERANCE,
    ):
        return False

    if not room_values_compatible(left.dormitorios, right.dormitorios):
        return False

    if not room_values_compatible(left.banos, right.banos):
        return False

    return parking_values_compatible(left.estacionamientos, right.estacionamientos)


def representative_score(listing: Listing) -> tuple:
    """Prefer the richest, freshest listing as the group representative."""
    data_points = sum(
        value is not None
        for value in [
            listing.precio_clp,
            listing.precio_uf,
            listing.lat,
            listing.lon,
            listing.m2_construidos,
            listing.dormitorios,
            listing.banos,
            listing.estacionamientos,
        ]
    )
    has_source_id = 1 if listing.source_listing_id else 0
    has_link = 1 if (listing.link or listing.url) else 0
    last_seen = listing.last_seen or datetime.min
    return (data_points, has_source_id, has_link, last_seen, -(listing.id or 0))


def choose_representative(group: list[Listing]) -> Listing:
    return max(group, key=representative_score)


def add_to_duplicate_groups(groups: list[list[Listing]], listing: Listing) -> None:
    for group in groups:
        if any(same_property(listing, existing) for existing in group):
            group.append(listing)
            return

    groups.append([listing])


def eligible_for_property_dedup(listing: Listing) -> bool:
    return (
        listing.status == "active"
        and normalize_comuna(listing.comuna) != ""
        and listing.m2_construidos is not None
        and listing.m2_construidos > 0
        and listing.dormitorios is not None
        and listing.banos is not None
    )


def group_property_duplicates(listings: Iterable[Listing]) -> list[list[Listing]]:
    groups_by_comuna: dict[str, list[list[Listing]]] = {}

    for listing in sorted(listings, key=lambda item: (normalize_comuna(item.comuna), item.m2_construidos or 0, item.id or 0)):
        comuna = normalize_comuna(listing.comuna)
        groups = groups_by_comuna.setdefault(comuna, [])
        add_to_duplicate_groups(groups, listing)

    return [group for groups in groups_by_comuna.values() for group in groups]


def mark_duplicate_listings(db=None) -> dict:
    """Recalculate property-level duplicate flags without deleting any row."""
    owns_session = db is None

    if DEMO_MODE:
        if owns_session:
            with SessionLocal() as read_db:
                total = read_db.execute(select(Listing)).scalars().all()
                active_eligible = [
                    listing for listing in total if eligible_for_property_dedup(listing)
                ]
                duplicate_count = sum(1 for listing in total if listing.is_duplicate)
                return {
                    "groups": 0,
                    "duplicates": duplicate_count,
                    "eligible": len(active_eligible),
                    "total": len(total),
                }

        return {
            "groups": 0,
            "duplicates": 0,
            "eligible": 0,
            "total": 0,
        }

    if owns_session:
        init_db()
        db = SessionLocal()

    try:
        listings = list(db.execute(select(Listing)).scalars().all())
        active_eligible = []

        for listing in listings:
            listing.property_fingerprint = build_property_fingerprint(listing)

            if listing.status == "active":
                listing.is_duplicate = False
                listing.duplicate_group_id = None

                if eligible_for_property_dedup(listing):
                    active_eligible.append(listing)

        duplicate_count = 0
        group_count = 0

        for group in group_property_duplicates(active_eligible):
            if len(group) < 2:
                continue

            representative = choose_representative(group)
            group_id = f"property-{representative.id}"
            group_count += 1

            for listing in group:
                listing.duplicate_group_id = group_id
                listing.is_duplicate = listing.id != representative.id

                if listing.is_duplicate:
                    duplicate_count += 1

        db.commit()

        return {
            "groups": group_count,
            "duplicates": duplicate_count,
            "eligible": len(active_eligible),
            "total": len(listings),
        }
    finally:
        if owns_session:
            db.close()


def is_representative_filter(model=Listing):
    """SQLAlchemy expression for analytics queries: active, non-duplicate rows only."""
    return (model.is_duplicate.is_(False)) | (model.is_duplicate.is_(None))


if __name__ == "__main__":
    result = mark_duplicate_listings()
    print(
        "Property deduplication complete: "
        f"{result['duplicates']} duplicates in {result['groups']} groups"
    )
