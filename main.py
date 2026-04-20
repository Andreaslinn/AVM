from datetime import date
import unicodedata


def positive_or_none(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    return number if number > 0 else None


def non_negative_or_none(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    return number if number >= 0 else None


def current_age_or_none(ano_construccion):
    try:
        year = int(ano_construccion)
    except (TypeError, ValueError):
        return None

    if year <= 0:
        return None

    return max(date.today().year - year, 0)


def normalize_comuna(comuna):
    normalized = str(comuna or "").strip().lower()

    for encoding in ("latin1", "cp1252"):
        try:
            normalized = normalized.encode(encoding).decode("utf-8")
            break
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue

    replacements = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ñ": "n",
        "ã¡": "a",
        "ã©": "e",
        "ã­": "i",
        "ã³": "o",
        "ãº": "u",
        "ã±": "n",
        "ã‘": "n",
        "ãƒâ€˜": "n",
        "ãƒâ±": "n",
    }

    for original, replacement in replacements.items():
        normalized = normalized.replace(original, replacement)

    normalized = unicodedata.normalize("NFKD", normalized)
    return "".join(
        character for character in normalized
        if not unicodedata.combining(character)
    )


def calcular_tasacion(
    comuna,
    m2_construidos,
    m2_terreno,
    dormitorios,
    banos,
    estacionamientos,
    piscina,
    ano_construccion,
):
    valor_ubicacion = {
        "vitacura": 20000000,
        "nunoa": 15000000,
        "puente alto": 10000000,
    }.get(normalize_comuna(comuna), 12000000)

    m2_construidos = positive_or_none(m2_construidos)
    m2_terreno = positive_or_none(m2_terreno)
    dormitorios = positive_or_none(dormitorios)
    banos = positive_or_none(banos)
    estacionamientos = non_negative_or_none(estacionamientos)

    valor_superficie = 0
    if m2_construidos is not None:
        valor_superficie += m2_construidos * 650000
    if m2_terreno is not None:
        valor_superficie += m2_terreno * 120000

    valor_extras = 0
    if dormitorios is not None:
        valor_extras += dormitorios * 3000000
    if banos is not None:
        valor_extras += banos * 2500000
    if estacionamientos is not None:
        valor_extras += estacionamientos * 2000000
    if piscina:
        valor_extras += 10000000

    antiguedad = current_age_or_none(ano_construccion)
    descuento = antiguedad * 500000 if antiguedad is not None else 0

    return valor_ubicacion + valor_superficie + valor_extras - descuento
