from __future__ import annotations

import unicodedata


LOCATION_MAP = {
    "metro nuble": (-33.456, -70.613),
    "nuble": (-33.456, -70.613),
    "metro \u00f1uble": (-33.456, -70.613),
    "plaza egana": (-33.454, -70.575),
    "egana": (-33.454, -70.575),
    "irarrazaval": (-33.455, -70.625),
    "irarr\u00e1zaval": (-33.455, -70.625),
    "san eugenio": (-33.459, -70.620),
    "zanartu": (-33.460, -70.615),
    "los leones": (-33.420, -70.610),
}


def extract_micro_location(titulo):
    match = extract_micro_location_match(titulo)

    if match is None:
        return None

    return match["coords"]


def extract_micro_location_match(titulo):
    if not titulo:
        return None

    titulo_normalizado = normalize_text(titulo)
    matches = []

    for key, coords in LOCATION_MAP.items():
        key_normalizado = normalize_text(key)
        key_words = key_normalizado.split()

        if any(word in titulo_normalizado for word in key_words):
            matches.append(
                {
                    "key": key,
                    "coords": coords,
                    "match_length": len(key_normalizado),
                }
            )

    if not matches:
        return None

    best_match = max(matches, key=lambda match: match["match_length"])
    return {
        "key": best_match["key"],
        "coords": best_match["coords"],
    }


def normalize_text(value):
    value = str(value).lower()
    value = unicodedata.normalize("NFKD", value)
    return "".join(char for char in value if not unicodedata.combining(char))
