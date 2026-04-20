from __future__ import annotations

import re
import unicodedata
from statistics import median
from typing import Iterable, Optional


DEFAULT_UF_TO_CLP = 37_000
MIN_M2 = 10
MAX_M2 = 1_000
PRICE_OUTLIER_IQR_MULTIPLIER = 1.5
MIN_PRICE_UF_PER_M2 = 5
MAX_PRICE_UF_PER_M2 = 300
DEDUP_M2_TOLERANCE_RATIO = 0.10
DEDUP_ROOM_TOLERANCE = 1
MAX_DORMITORIOS = 6
MAX_BANOS = 5
MAX_ESTACIONAMIENTOS = 6

COMUNA_ALIASES = {
    "nunoa": "Ñuñoa",
    "nunoa santiago": "Ñuñoa",
    "providencia": "Providencia",
    "las condes": "Las Condes",
    "santiago": "Santiago",
    "santiago centro": "Santiago",
    "vitacura": "Vitacura",
    "la reina": "La Reina",
    "macul": "Macul",
    "la florida": "La Florida",
    "san miguel": "San Miguel",
    "recoleta": "Recoleta",
    "independencia": "Independencia",
    "penalolen": "Peñalolén",
    "penalolen santiago": "Peñalolén",
    "puente alto": "Puente Alto",
    "lo barnechea": "Lo Barnechea",
    "la dehesa": "Lo Barnechea",
    "maipu": "Maipú",
}

TEXT_FIELDS = (
    "titulo",
    "descripcion",
    "precio_texto",
    "comuna",
    "url",
    "link",
    "source_listing_id",
    "fuente",
)

NUMERIC_FIELDS = (
    "precio_clp",
    "precio_uf",
    "m2",
    "m2_construidos",
    "m2_terreno",
    "m2_util",
    "m2_total",
    "dormitorios",
    "banos",
    "estacionamientos",
    "lat",
    "lon",
)


def clean_listings(
    listings: Iterable[dict],
    uf_to_clp: float = DEFAULT_UF_TO_CLP,
    remove_outliers: bool = True,
    deduplicate: bool = False,
) -> list[dict]:
    """Return validated, normalized listings ready for comparable valuation."""
    normalized = []

    for raw_listing in listings:
        cleaned_listing = clean_listing(raw_listing, uf_to_clp=uf_to_clp)

        if cleaned_listing is None:
            continue

        normalized.append(cleaned_listing)

    if remove_outliers:
        normalized = remove_price_outliers(normalized)

    if deduplicate:
        normalized = deduplicate_listings(normalized)

    return normalized


def clean_listing(listing: dict, uf_to_clp: float = DEFAULT_UF_TO_CLP) -> Optional[dict]:
    """Normalize one listing and return None when critical valuation fields are invalid."""
    cleaned = dict(listing or {})

    clean_text_fields(cleaned)
    normalize_numeric_fields(cleaned)
    enrich_missing_features(cleaned)
    standardize_comuna(cleaned)
    normalize_area_fields(cleaned)
    normalize_price_fields(cleaned, uf_to_clp=uf_to_clp)

    if not is_valid_listing(cleaned):
        return None

    return cleaned


def is_valid_listing(listing: dict) -> bool:
    """A valuation comparable needs at least price and usable built area."""
    precio_uf = listing.get("precio_uf")
    m2_construidos = listing.get("m2_construidos")

    if precio_uf is None or precio_uf <= 0:
        return False

    if m2_construidos is None:
        return False

    if m2_construidos < MIN_M2 or m2_construidos > MAX_M2:
        return False

    return True


def clean_text_fields(listing: dict) -> None:
    """Trim text, collapse whitespace, and keep empty strings as None."""
    for field in TEXT_FIELDS:
        if field not in listing:
            continue

        value = clean_text(listing.get(field))
        listing[field] = value or None


def normalize_numeric_fields(listing: dict) -> None:
    """Convert numeric-looking strings to numbers and invalid/zero values to None."""
    for field in NUMERIC_FIELDS:
        if field not in listing:
            continue

        if field in {"lat", "lon"}:
            listing[field] = float_or_none(listing.get(field))
        elif field == "estacionamientos":
            listing[field] = non_negative_int_or_none(listing.get(field))
        elif field in {"dormitorios", "banos"}:
            listing[field] = positive_int_or_none(listing.get(field))
        else:
            listing[field] = positive_number_or_none(listing.get(field))


def enrich_missing_features(listing: dict) -> None:
    """Infer missing room/parking attributes from title and description text."""
    inferred = extract_features_from_text(
        listing.get("titulo"),
        first_not_none(
            listing.get("descripcion"),
            listing.get("description"),
            listing.get("raw_text"),
            listing.get("texto"),
        ),
    )

    for field in ("dormitorios", "banos", "estacionamientos"):
        if listing.get(field) is not None:
            continue

        value = inferred.get(field)

        if value is None:
            continue

        confidence = inferred.get("confidence", {}).get(field) or "low"

        if confidence == "low":
            print(
                f"[DATA ENRICHMENT] {field} candidate = {value} "
                f"(LOW confidence, not applied)"
            )
            continue

        listing[field] = value
        print(
            f"[DATA ENRICHMENT] {field} inferred = {value} "
            f"({confidence.upper()} confidence)"
        )


def extract_features_from_text(title, description) -> dict:
    text = normalize_for_matching(
        " ".join(value for value in [title, description] if value)
    )

    if not text:
        return {
            "dormitorios": None,
            "banos": None,
            "estacionamientos": None,
            "confidence": {
                "dormitorios": None,
                "banos": None,
                "estacionamientos": None,
            },
        }

    features = {
        "dormitorios": None,
        "banos": None,
        "estacionamientos": None,
        "confidence": {
            "dormitorios": None,
            "banos": None,
            "estacionamientos": None,
        },
    }

    compact_match = re.search(
        r"\b([1-6])\s*d\s*/?\s*([1-5])\s*b(?:\s*/?\s*([0-6])\s*e)?\b",
        text,
    )

    if compact_match:
        set_inferred_feature(
            features,
            "dormitorios",
            int(compact_match.group(1)),
            "medium",
        )
        set_inferred_feature(features, "banos", int(compact_match.group(2)), "medium")

        if compact_match.group(3) is not None:
            set_inferred_feature(
                features,
                "estacionamientos",
                int(compact_match.group(3)),
                "medium",
            )

    compact_b_e_match = re.search(r"\b([1-5])\s*b\s*/?\s*([0-6])\s*e\b", text)

    if compact_b_e_match:
        if features["banos"] is None:
            set_inferred_feature(
                features,
                "banos",
                int(compact_b_e_match.group(1)),
                "medium",
            )

        if features["estacionamientos"] is None:
            set_inferred_feature(
                features,
                "estacionamientos",
                int(compact_b_e_match.group(2)),
                "medium",
            )

    if features["dormitorios"] is None:
        value, confidence = extract_bounded_feature(
            text,
            r"(?:dormitorios?|dorms?|dorm|habitaciones?|hab)",
            MAX_DORMITORIOS,
        )
        set_inferred_feature(features, "dormitorios", value, confidence)

    if features["banos"] is None:
        value, confidence = extract_bounded_feature(
            text,
            r"(?:banos?|bano)",
            MAX_BANOS,
            previous_context_pattern=r"(?:dormitorios?|dorms?|dorm|habitaciones?|hab)",
        )
        set_inferred_feature(features, "banos", value, confidence)

    if features["estacionamientos"] is None:
        value, confidence = extract_bounded_feature(
            text,
            r"(?:estacionamientos?|estac\.?|est)",
            MAX_ESTACIONAMIENTOS,
            allow_zero=True,
            previous_context_pattern=(
                r"(?:dormitorios?|dorms?|dorm|habitaciones?|hab|banos?|bano)"
            ),
            low_context_pattern=r"(?:opcion|opcional|posibilidad)",
        )
        set_inferred_feature(features, "estacionamientos", value, confidence)

    if features["estacionamientos"] is None:
        value, confidence = extract_low_confidence_parking(text)
        set_inferred_feature(features, "estacionamientos", value, confidence)

    return features


def set_inferred_feature(
    features: dict,
    field: str,
    value: Optional[int],
    confidence: Optional[str],
) -> None:
    if value is None:
        return

    features[field] = value
    features["confidence"][field] = confidence or "low"


def extract_bounded_feature(
    text: str,
    keyword_pattern: str,
    max_value: int,
    allow_zero: bool = False,
    previous_context_pattern: Optional[str] = None,
    low_context_pattern: Optional[str] = None,
) -> tuple[Optional[int], Optional[str]]:
    min_value = 0 if allow_zero else 1
    patterns = [
        rf"\b(\d{{1,2}})\s*(?:{keyword_pattern})\b",
        rf"\b(?:{keyword_pattern})\s*(?::|=|-)?\s*(\d{{1,2}})\b",
    ]
    candidates = []

    for pattern_index, pattern in enumerate(patterns):
        for match in re.finditer(pattern, text):
            value = int(match.group(1))

            if not min_value <= value <= max_value:
                continue

            if pattern_index == 0 and belongs_to_previous_feature(
                text,
                match.start(1),
                previous_context_pattern,
            ):
                continue

            if pattern_index == 1 and keyword_belongs_to_previous_value(
                text,
                match.start(),
                match.group(0),
            ):
                continue

            confidence = (
                "low"
                if has_low_confidence_context(text, match, low_context_pattern)
                else "high"
            )
            candidates.append((pattern_index, -match.start(), value, confidence))

    if not candidates:
        return None, None

    best_match = max(candidates)
    return best_match[-2], best_match[-1]


def has_low_confidence_context(text: str, match, low_context_pattern: Optional[str]) -> bool:
    if not low_context_pattern:
        return False

    context = text[max(0, match.start() - 30):match.end() + 30]
    return re.search(low_context_pattern, context) is not None


def extract_low_confidence_parking(text: str) -> tuple[Optional[int], Optional[str]]:
    patterns = [
        r"\b(?:opcion|opcional|posibilidad)(?:\s+de)?\s*(\d{1,2})?\s*"
        r"(?:estacionamientos?|estac\.?)\b",
        r"\b(?:estacionamientos?|estac\.?)\s*(?:opcional|disponible|con\s+opcion)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)

        if not match:
            continue

        raw_value = match.group(1) if match.lastindex else None
        value = int(raw_value) if raw_value is not None else 1

        if 0 <= value <= MAX_ESTACIONAMIENTOS:
            return value, "low"

    return None, None


def keyword_belongs_to_previous_value(
    text: str,
    keyword_start: int,
    matched_text: str,
) -> bool:
    if re.search(r"[:=-]", matched_text):
        return False

    prefix = text[max(0, keyword_start - 12):keyword_start]
    return re.search(r"\d+\s*$", prefix) is not None


def belongs_to_previous_feature(
    text: str,
    value_start: int,
    previous_context_pattern: Optional[str],
) -> bool:
    if not previous_context_pattern:
        return False

    prefix = text[max(0, value_start - 30):value_start]
    match = re.search(
        rf"(?:{previous_context_pattern})\s*(?:incluidos?|incluye|:|=|-)?\s*$",
        prefix,
    )

    if not match:
        return False

    text_before_keyword = prefix[:match.start()]
    return re.search(r"\d+\s*$", text_before_keyword) is None


def extract_m2_from_text(text: str) -> tuple[int | None, str]:
    """
    Extrae m2 desde texto libre.
    Retorna:
    - valor m2
    - nivel de confianza ("high", "medium", "low")
    """
    cleaned_text = clean_text(text).lower()
    normalized_text = normalize_for_matching(cleaned_text)

    if not normalized_text:
        return None, "low"

    patterns_high = [
        r"(\d{2,4})\s?m2",
        r"(\d{2,4})\s?m²",
        r"(\d{2,4})\s?mts2",
        r"(\d{2,4})\s?metros cuadrados",
    ]
    patterns_medium = [
        r"superficie\s?:?\s?(\d{2,4})",
        r"construidos?\s?:?\s?(\d{2,4})",
        r"(\d{2,4})\s?m\b",
    ]
    patterns_low = [
        r"\b(\d{2,4})\b",
    ]

    high_value = find_m2_pattern_value(cleaned_text, normalized_text, patterns_high)
    if high_value is not None:
        return high_value, "high"

    medium_value = find_m2_pattern_value(cleaned_text, normalized_text, patterns_medium)
    if medium_value is not None:
        return medium_value, "medium"

    for pattern in patterns_low:
        for match in re.finditer(pattern, normalized_text, flags=re.IGNORECASE):
            if is_embedded_in_larger_number(normalized_text, match):
                continue

            value = safe_m2_value(match.group(1), min_value=20, max_value=500)

            if value is None:
                continue

            if has_nearby_numeric_context(normalized_text, match):
                continue

            if has_price_or_year_context(normalized_text, match):
                continue

            return value, "low"

    return None, "low"


def find_m2_pattern_value(
    raw_text: str,
    normalized_text: str,
    patterns: list[str],
) -> Optional[int]:
    for pattern in patterns:
        for text in (raw_text, normalized_text):
            match = re.search(pattern, text, flags=re.IGNORECASE)

            if not match:
                continue

            if is_embedded_in_larger_number(text, match):
                continue

            value = safe_m2_value(match.group(1))

            if value is not None:
                return value

    return None


def safe_m2_value(
    value,
    min_value: int = MIN_M2,
    max_value: int = MAX_M2,
) -> Optional[int]:
    try:
        numeric_value = int(value)
    except (TypeError, ValueError):
        return None

    if numeric_value < 10 or numeric_value > 1000:
        return None

    if numeric_value < min_value or numeric_value > max_value:
        return None

    return numeric_value


def has_nearby_numeric_context(text: str, match) -> bool:
    context = text[max(0, match.start() - 25):match.end() + 25]
    numbers = re.findall(r"\d+", context)
    return len(numbers) > 1


def is_embedded_in_larger_number(text: str, match) -> bool:
    start = match.start(1)
    end = match.end(1)
    previous_is_digit = start > 0 and text[start - 1].isdigit()
    next_is_digit = end < len(text) and text[end].isdigit()
    return previous_is_digit or next_is_digit


def has_price_or_year_context(text: str, match) -> bool:
    context = text[max(0, match.start() - 25):match.end() + 25]

    if re.search(r"(?:\$|clp|uf|precio|valor)", context):
        return True

    value = int(match.group(1))
    if 1900 <= value <= 2099:
        return True

    return False


def normalize_area_fields(listing: dict) -> None:
    """Use m2_construidos as canonical area, falling back to m2 only when needed."""
    m2_construidos = first_not_none(
        listing.get("m2_construidos"),
        listing.get("m2"),
        listing.get("m2_util"),
        listing.get("m2_total"),
    )

    if m2_construidos is None:
        combined_text = " ".join(
            value
            for value in (
                listing.get("titulo"),
                listing.get("title"),
                listing.get("descripcion"),
                listing.get("description"),
                listing.get("raw_text"),
                listing.get("texto"),
            )
            if value
        )
        m2, confidence = extract_m2_from_text(combined_text)

        if m2:
            m2_construidos = m2
            listing["m2_source"] = "text_extraction"
            listing["m2_confidence"] = confidence
            print(f"[M2 EXTRACTION] Found {m2}m2 ({confidence}) from text")

    listing["m2_construidos"] = positive_number_or_none(m2_construidos)
    listing["m2"] = listing["m2_construidos"]


def normalize_price_fields(listing: dict, uf_to_clp: float = DEFAULT_UF_TO_CLP) -> None:
    """Convert CLP or text price to UF and keep CLP if it is available."""
    precio_clp = positive_number_or_none(listing.get("precio_clp"))
    precio_uf = positive_number_or_none(listing.get("precio_uf"))

    if precio_clp is None and precio_uf is None:
        parsed_price = parse_price_text(listing.get("precio_texto"))
        if parsed_price is not None:
            precio_clp = parsed_price.get("precio_clp")
            precio_uf = parsed_price.get("precio_uf")

    if precio_uf is None and precio_clp is not None and uf_to_clp > 0:
        precio_uf = precio_clp / uf_to_clp

    if precio_clp is None and precio_uf is not None and uf_to_clp > 0:
        precio_clp = precio_uf * uf_to_clp

    listing["precio_clp"] = int(precio_clp) if precio_clp is not None else None
    listing["precio_uf"] = precio_uf


def standardize_comuna(listing: dict) -> None:
    """Map scraped comuna variants into one display name."""
    comuna = listing.get("comuna")
    normalized = normalize_for_matching(comuna)

    if not normalized:
        listing["comuna"] = None
        return

    listing["comuna"] = COMUNA_ALIASES.get(normalized, title_case_comuna(comuna))


def remove_price_outliers(
    listings: list[dict],
    iqr_multiplier: float = PRICE_OUTLIER_IQR_MULTIPLIER,
) -> list[dict]:
    """Remove impossible UF/m2 values, then IQR outliers inside each comuna."""
    plausible_listings = [
        listing
        for listing in listings
        if has_plausible_price_per_m2(listing)
    ]
    grouped = group_by_comuna(plausible_listings)
    filtered = []

    for comuna_listings in grouped.values():
        if len(comuna_listings) < 4:
            filtered.extend(comuna_listings)
            continue

        ratios = [
            price_per_m2_uf(listing)
            for listing in comuna_listings
            if price_per_m2_uf(listing) is not None
        ]

        lower_bound, upper_bound = iqr_bounds(ratios, multiplier=iqr_multiplier)

        if lower_bound is None or upper_bound is None:
            filtered.extend(comuna_listings)
            continue

        filtered.extend(
            listing
            for listing in comuna_listings
            if lower_bound <= price_per_m2_uf(listing) <= upper_bound
        )

    return filtered


def has_plausible_price_per_m2(listing: dict) -> bool:
    """Guard against parsing errors such as extra zeros or wrong currency capture."""
    ratio = price_per_m2_uf(listing)

    if ratio is None:
        return False

    return MIN_PRICE_UF_PER_M2 <= ratio <= MAX_PRICE_UF_PER_M2


def deduplicate_listings(
    listings: list[dict],
    m2_tolerance_ratio: float = DEDUP_M2_TOLERANCE_RATIO,
) -> list[dict]:
    """In-memory dedup for exact scrape batches. Price is intentionally ignored."""
    unique = []

    for listing in sorted(listings, key=dedupe_sort_key):
        duplicate_index = find_duplicate_index(
            unique,
            listing,
            m2_tolerance_ratio=m2_tolerance_ratio,
        )

        if duplicate_index is None:
            unique.append(listing)
            continue

        if completeness_score(listing) > completeness_score(unique[duplicate_index]):
            unique[duplicate_index] = merge_listing_records(unique[duplicate_index], listing)

    return unique


def find_duplicate_index(
    candidates: list[dict],
    listing: dict,
    m2_tolerance_ratio: float,
) -> Optional[int]:
    for index, candidate in enumerate(candidates):
        if not are_probable_duplicates(
            candidate,
            listing,
            m2_tolerance_ratio=m2_tolerance_ratio,
        ):
            continue

        return index

    return None


def are_probable_duplicates(
    left: dict,
    right: dict,
    m2_tolerance_ratio: float = DEDUP_M2_TOLERANCE_RATIO,
) -> bool:
    """Return True when two listings look like the same property; never use price."""
    if normalize_for_matching(left.get("comuna")) != normalize_for_matching(right.get("comuna")):
        return False

    return (
        values_are_similar(
            left.get("m2_construidos"),
            right.get("m2_construidos"),
            m2_tolerance_ratio,
        )
        and room_values_are_compatible(left.get("dormitorios"), right.get("dormitorios"))
        and room_values_are_compatible(left.get("banos"), right.get("banos"))
    )


def values_are_similar(left, right, tolerance_ratio: float) -> bool:
    left = positive_number_or_none(left)
    right = positive_number_or_none(right)

    if left is None or right is None:
        return False

    tolerance = max(left, right) * tolerance_ratio
    return abs(left - right) <= tolerance


def room_values_are_compatible(left, right) -> bool:
    if left is None or right is None:
        return False

    try:
        return abs(int(left) - int(right)) <= DEDUP_ROOM_TOLERANCE
    except (TypeError, ValueError):
        return False


def merge_listing_records(existing: dict, new: dict) -> dict:
    """Keep existing identity fields while filling missing values from the richer duplicate."""
    merged = dict(existing)

    for key, value in new.items():
        if merged.get(key) is None and value is not None:
            merged[key] = value

    return merged


def completeness_score(listing: dict) -> int:
    fields = (
        "url",
        "link",
        "titulo",
        "comuna",
        "precio_uf",
        "precio_clp",
        "m2_construidos",
        "dormitorios",
        "banos",
        "estacionamientos",
        "lat",
        "lon",
    )
    return sum(1 for field in fields if listing.get(field) is not None)


def dedupe_sort_key(listing: dict) -> tuple:
    """Prefer records with stronger source identity, then richer data."""
    has_url = 1 if listing.get("url") or listing.get("link") else 0
    has_source_id = 1 if listing.get("source_listing_id") else 0
    return (-has_source_id, -has_url, -completeness_score(listing))


def price_per_m2_uf(listing: dict) -> Optional[float]:
    precio_uf = positive_number_or_none(listing.get("precio_uf"))
    m2 = positive_number_or_none(listing.get("m2_construidos"))

    if precio_uf is None or m2 is None:
        return None

    return precio_uf / m2


def iqr_bounds(values: list[float], multiplier: float) -> tuple[Optional[float], Optional[float]]:
    values = sorted(value for value in values if value is not None)

    if len(values) < 4:
        return None, None

    q1 = percentile(values, 0.25)
    q3 = percentile(values, 0.75)

    if q1 is None or q3 is None:
        return None, None

    iqr = q3 - q1

    if iqr <= 0:
        center = median(values)
        return center * 0.5, center * 1.5

    return q1 - multiplier * iqr, q3 + multiplier * iqr


def percentile(sorted_values: list[float], percent: float) -> Optional[float]:
    if not sorted_values:
        return None

    position = (len(sorted_values) - 1) * percent
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(sorted_values) - 1)
    upper_weight = position - lower_index
    lower_weight = 1 - upper_weight

    return (
        sorted_values[lower_index] * lower_weight
        + sorted_values[upper_index] * upper_weight
    )


def group_by_comuna(listings: list[dict]) -> dict[str, list[dict]]:
    grouped = {}

    for listing in listings:
        key = listing.get("comuna") or "sin_comuna"
        grouped.setdefault(key, []).append(listing)

    return grouped


def parse_price_text(value: Optional[str]) -> Optional[dict]:
    text = clean_text(value)

    if not text:
        return None

    normalized = normalize_for_matching(text)

    if "uf" in normalized:
        amount = positive_number_or_none(re.sub(r"(?i)uf", "", text))
        return {"precio_clp": None, "precio_uf": amount} if amount is not None else None

    if "$" in text or "clp" in normalized:
        amount = positive_number_or_none(re.sub(r"(?i)clp", "", text))
        return {"precio_clp": int(amount), "precio_uf": None} if amount is not None else None

    return None


def clean_text(value) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def title_case_comuna(value: Optional[str]) -> Optional[str]:
    text = clean_text(value)
    return text.title() if text else None


def normalize_for_matching(value: Optional[str]) -> str:
    normalized = unicodedata.normalize("NFKD", clean_text(value).lower())
    without_accents = "".join(
        char for char in normalized if not unicodedata.combining(char)
    )
    return re.sub(r"\s+", " ", without_accents).strip()


def positive_number_or_none(value):
    if value is None or value == "":
        return None

    if isinstance(value, str):
        value = value.replace(".", "").replace(",", ".")
        value = re.sub(r"[^\d.-]", "", value)

    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    return number if number > 0 else None


def positive_int_or_none(value):
    number = positive_number_or_none(value)
    return int(number) if number is not None else None


def non_negative_int_or_none(value):
    if value is None or value == "":
        return None

    try:
        number = int(float(str(value).replace(",", ".")))
    except (TypeError, ValueError):
        return None

    return number if number >= 0 else None


def float_or_none(value):
    if value is None or value == "":
        return None

    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def first_not_none(*values):
    for value in values:
        if value is not None:
            return value

    return None
