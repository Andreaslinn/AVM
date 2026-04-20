from __future__ import annotations

from sqlalchemy import func, or_, select

from deduplication import is_representative_filter
from models import Listing


MIN_ACTIVE_LISTINGS = 50
MIN_PER_COMUNA = 10
LOW_DATA_WARNING = "WARNING: System operating in LOW DATA MODE. Results may be unstable."

low_data_mode = True


def get_data_sufficiency(db) -> dict:
    """Return a simple global data sufficiency assessment for AVM guardrails."""
    global low_data_mode

    rows = db.execute(
        select(Listing.comuna, func.count(Listing.id))
        .where(
            Listing.status == "active",
            is_representative_filter(Listing),
            Listing.comuna.is_not(None),
            Listing.m2_construidos.is_not(None),
            Listing.m2_construidos > 0,
            or_(
                Listing.precio_clp.is_not(None),
                Listing.precio_uf.is_not(None),
            ),
        )
        .group_by(Listing.comuna)
    ).all()

    per_comuna = {
        (comuna or "Sin comuna"): int(count)
        for comuna, count in rows
    }
    total_active = sum(per_comuna.values())
    insufficient_comunas = {
        comuna: count
        for comuna, count in per_comuna.items()
        if count < MIN_PER_COMUNA
    }
    most_comunas_insufficient = bool(per_comuna) and (
        len(insufficient_comunas) > len(per_comuna) / 2
    )

    reasons = []

    if total_active < MIN_ACTIVE_LISTINGS:
        reasons.append(
            f"active representative listings below threshold ({total_active}/{MIN_ACTIVE_LISTINGS})"
        )

    if most_comunas_insufficient:
        reasons.append(
            f"most comunas below threshold ({len(insufficient_comunas)}/{len(per_comuna)})"
        )

    if not per_comuna:
        reasons.append("no active representative listings available")

    low_data_mode = bool(reasons)

    return {
        "low_data_mode": low_data_mode,
        "total_active_listings": total_active,
        "total_comunas": len(per_comuna),
        "per_comuna": per_comuna,
        "insufficient_comunas": insufficient_comunas,
        "min_active_listings": MIN_ACTIVE_LISTINGS,
        "min_per_comuna": MIN_PER_COMUNA,
        "reasons": reasons,
    }


def print_low_data_warning(assessment: dict | None = None) -> None:
    if assessment is None:
        active = low_data_mode
    else:
        active = assessment.get("low_data_mode", False)

    if active:
        print(LOW_DATA_WARNING)
