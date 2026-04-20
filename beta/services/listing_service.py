from __future__ import annotations

from database import SessionLocal, init_db
from deduplication import mark_duplicate_listings
from listing_pipeline import process_listing_pipeline


def initialize_app_data() -> None:
    init_db()
    mark_duplicate_listings()


def save_listing(property_data: dict, precio_clp: int) -> tuple[int | None, int]:
    with SessionLocal() as db:
        listing_input = {
            **property_data,
            "fuente": "tasador_app",
            "source_listing_id": None,
            "url": None,
            "link": None,
            "status": "appraisal_result",
            "precio_clp": precio_clp,
            "precio_uf": None,
            "m2_total": property_data.get("m2_terreno"),
            "fecha_publicacion": None,
        }
        listing = process_listing_pipeline(
            db,
            listing_input,
            source="app",
        )
        return listing.property_id, listing.id
