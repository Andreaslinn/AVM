from __future__ import annotations

from numbers import Number
from typing import Any


# Valor fijo y explicito para mantener esta capa pura y sin dependencias externas.
UF_TO_CLP = 38_000

# Rango conservador para departamentos en venta.
MIN_M2_CONSTRUIDOS = 20
MAX_M2_CONSTRUIDOS = 500

# Umbrales ajustables para detectar outliers evidentes de precio por m2.
MIN_PRECIO_M2_CLP = 500_000
MAX_PRECIO_M2_CLP = 10_000_000

UNKNOWN_COMUNAS = {"", "desconocida"}

CRITICAL_ISSUES = {
    "missing_price",
    "missing_m2",
    "invalid_m2_range",
    "invalid_precio_m2",
}

NON_CRITICAL_ISSUES = {
    "missing_basic_attributes",
    "unknown_comuna",
}


def get_precio_clp_safe(listing: Any) -> float | None:
    """Return a valid CLP price, converting UF when needed."""
    precio_clp = _get_positive_number(listing, "precio_clp")
    if precio_clp is not None:
        return precio_clp

    precio_uf = _get_positive_number(listing, "precio_uf")
    if precio_uf is not None:
        return precio_uf * UF_TO_CLP

    return None


def calculate_precio_m2(listing: Any) -> float | None:
    """Calculate CLP price per built square meter when inputs are usable."""
    precio_clp = get_precio_clp_safe(listing)
    m2_construidos = _get_positive_number(listing, "m2_construidos")

    if precio_clp is None or m2_construidos is None:
        return None

    return precio_clp / m2_construidos


def is_precio_m2_valid(value: Any) -> bool:
    """Return True when CLP/m2 is inside a reasonable apartment-sale range."""
    precio_m2 = _to_number(value)
    if precio_m2 is None:
        return False

    return MIN_PRECIO_M2_CLP <= precio_m2 <= MAX_PRECIO_M2_CLP


def get_listing_quality_issues(listing: Any) -> list[str]:
    """Return critical quality issues that make a scraped listing unreliable."""
    issues = []

    if get_precio_clp_safe(listing) is None:
        issues.append("missing_price")

    m2_construidos = _get_positive_number(listing, "m2_construidos")
    if m2_construidos is None:
        issues.append("missing_m2")
    elif not _is_m2_in_reasonable_range(m2_construidos):
        issues.append("invalid_m2_range")

    if not _has_basic_attributes(listing):
        issues.append("missing_basic_attributes")

    if _is_unknown_comuna(_get_attr(listing, "comuna")):
        issues.append("unknown_comuna")

    precio_m2 = calculate_precio_m2(listing)
    if precio_m2 is None or not is_precio_m2_valid(precio_m2):
        issues.append("invalid_precio_m2")

    return issues


def is_listing_usable(listing: Any) -> bool:
    """Return True when the listing has no critical quality issues."""
    issues = get_listing_quality_issues(listing)
    return not any(issue in CRITICAL_ISSUES for issue in issues)


def is_listing_high_quality(listing: Any) -> bool:
    """Return True only when the listing has no quality issues at all."""
    return not get_listing_quality_issues(listing)


def _get_attr(listing: Any, field_name: str) -> Any:
    """Read values from plain objects and dict-like rows without coupling."""
    if isinstance(listing, dict):
        return listing.get(field_name)

    return getattr(listing, field_name, None)


def _get_positive_number(listing: Any, field_name: str) -> float | None:
    value = _to_number(_get_attr(listing, field_name))
    if value is None or value <= 0:
        return None

    return value


def _to_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, Number):
        return float(value)

    return None


def _is_m2_in_reasonable_range(value: float) -> bool:
    return MIN_M2_CONSTRUIDOS <= value <= MAX_M2_CONSTRUIDOS


def _has_basic_attributes(listing: Any) -> bool:
    return (
        _get_positive_number(listing, "dormitorios") is not None
        or _get_positive_number(listing, "banos") is not None
    )


def _is_unknown_comuna(value: Any) -> bool:
    if value is None:
        return True

    if not isinstance(value, str):
        return False

    return value.strip().lower() in UNKNOWN_COMUNAS
