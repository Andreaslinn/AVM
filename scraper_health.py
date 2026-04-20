from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse


STATUS_OK = "OK"
STATUS_DEGRADED = "DEGRADED"
STATUS_FAILED = "FAILED"
VALIDATION_RATE_DEGRADED_THRESHOLD = 0.50

EXPECTED_MIN_VALID_ROWS = {
    "yapo": 10,
    "portalinmobiliario": 10,
}

BLOCKING_PATTERNS = [
    "captcha",
    "access denied",
    "acceso denegado",
    "bloqueado",
    "blocked",
    "robot",
    "verifica que eres humano",
    "verificar que eres humano",
    "soy nuevo",
    "ya tengo cuenta",
    "iniciar sesion",
    "iniciar sesión",
    "login",
    "registration",
    "err_internet_disconnected",
    "site cannot be reached",
]

NAVIGATION_URL_PARTS = [
    "login",
    "registration",
    "publicar",
    "ayuda",
    "seguridad",
    "favoritos",
    "notificaciones",
    "jms",
    "search?",
]


@dataclass
class SourceRunHealth:
    source: str
    raw_rows: int
    valid_rows: int
    block_detected: bool = False
    expected_min_valid: int = 10
    reasons: list[str] = field(default_factory=list)
    status: str = STATUS_OK

    @property
    def validation_rate(self) -> float:
        if self.raw_rows <= 0:
            return 0

        return self.valid_rows / self.raw_rows

    def as_dict(self) -> dict:
        return {
            "source": self.source,
            "status": self.status,
            "raw_rows": self.raw_rows,
            "valid_rows": self.valid_rows,
            "validation_rate": self.validation_rate,
            "block_detected": self.block_detected,
            "expected_min_valid": self.expected_min_valid,
            "reasons": list(self.reasons),
        }


def evaluate_source_run(
    source: str,
    raw_rows: int,
    valid_rows: int,
    block_detected: bool = False,
    expected_min_valid: Optional[int] = None,
) -> SourceRunHealth:
    source_key = normalize_source(source)
    expected_min_valid = expected_min_valid or EXPECTED_MIN_VALID_ROWS.get(source_key, 10)
    health = SourceRunHealth(
        source=source_key,
        raw_rows=raw_rows,
        valid_rows=valid_rows,
        block_detected=block_detected,
        expected_min_valid=expected_min_valid,
    )

    if block_detected:
        health.status = STATUS_FAILED
        health.reasons.append("blocking/login/captcha detected")

    if raw_rows == 0:
        health.status = STATUS_FAILED
        health.reasons.append("raw rows = 0")

    if health.status != STATUS_FAILED:
        if health.validation_rate < VALIDATION_RATE_DEGRADED_THRESHOLD:
            health.status = STATUS_DEGRADED
            health.reasons.append(
                f"validation rate below {VALIDATION_RATE_DEGRADED_THRESHOLD:.0%}"
            )

        if valid_rows < expected_min_valid:
            health.status = STATUS_DEGRADED
            health.reasons.append(
                f"valid rows below expected minimum ({valid_rows} < {expected_min_valid})"
            )

    if not health.reasons:
        health.reasons.append("source run within expected thresholds")

    return health


def print_source_health(health: SourceRunHealth) -> None:
    print(f"SOURCE: {health.source.upper()}")
    print(f"STATUS: {health.status}")
    print(f"RAW: {health.raw_rows}")
    print(f"VALID: {health.valid_rows}")
    print(f"VALIDATION RATE: {health.validation_rate:.1%}")
    print(f"BLOCK DETECTED: {'yes' if health.block_detected else 'no'}")
    print(f"REASON: {'; '.join(health.reasons)}")


def detect_blocking(text: Optional[str], current_url: Optional[str] = None) -> bool:
    combined = f"{current_url or ''} {text or ''}".lower()
    return any(pattern in combined for pattern in BLOCKING_PATTERNS)


def validate_scraped_listing(source: str, listing: dict) -> tuple[bool, list[str]]:
    reasons = []
    source_key = normalize_source(source)
    url = clean_text(listing.get("url") or listing.get("link"))

    if not url:
        reasons.append("missing link/url")
    elif not is_real_property_listing_link(source_key, url):
        reasons.append("not a real property listing link")

    if not has_valid_price(listing):
        reasons.append("missing or invalid price")

    if not clean_text(listing.get("comuna")):
        reasons.append("missing comuna")

    if not has_usable_surface(listing):
        reasons.append("missing usable surface area")

    if is_navigation_or_category_link(source_key, url):
        reasons.append("navigation/search/category link")

    return not reasons, reasons


def filter_valid_scraped_rows(source: str, rows: list[dict]) -> tuple[list[dict], list[dict]]:
    valid_rows = []
    rejected_rows = []

    for row in rows:
        is_valid, reasons = validate_scraped_listing(source, row)

        if is_valid:
            valid_rows.append(row)
            continue

        rejected = dict(row)
        rejected["_validation_errors"] = reasons
        rejected_rows.append(rejected)

    return valid_rows, rejected_rows


def is_real_property_listing_link(source: str, url: str) -> bool:
    normalized_url = url.lower()
    parsed = urlparse(url)
    path = parsed.path.lower()

    if source == "yapo":
        return (
            "yapo.cl" in parsed.netloc
            and "bienes-raices" in path
            and re.search(r"/\d{5,}$", path) is not None
        )

    if source == "portalinmobiliario":
        return (
            "portalinmobiliario.com" in normalized_url
            or re.search(r"/mlc[-_]?\d+", path, flags=re.IGNORECASE) is not None
            or "/propiedades/" in path
        )

    return bool(parsed.scheme and parsed.netloc)


def is_navigation_or_category_link(source: str, url: str) -> bool:
    normalized_url = (url or "").lower()

    if any(part in normalized_url for part in NAVIGATION_URL_PARTS):
        return True

    parsed = urlparse(url or "")
    path = parsed.path.strip("/").lower()

    if source == "yapo":
        return bool(path) and re.search(r"/\d{5,}$", f"/{path}") is None

    if source == "portalinmobiliario":
        category_paths = {
            "venta",
            "arriendo",
            "venta/departamento",
            "venta/casa",
            "venta/departamento/metropolitana",
        }
        return path in category_paths

    return False


def has_valid_price(listing: dict) -> bool:
    return is_positive_number(listing.get("precio_clp")) or is_positive_number(
        listing.get("precio_uf")
    )


def has_usable_surface(listing: dict) -> bool:
    value = listing.get("m2_construidos") or listing.get("m2")
    return is_positive_number(value)


def is_positive_number(value) -> bool:
    try:
        return value is not None and float(value) > 0
    except (TypeError, ValueError):
        return False


def normalize_source(source: str) -> str:
    return clean_text(source).lower()


def clean_text(value) -> str:
    return str(value or "").strip()
