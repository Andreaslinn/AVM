from math import asin, cos, exp, radians, sin, sqrt
from statistics import median
import time

try:
    import requests
except Exception:
    requests = None

from sqlalchemy import or_, select

from database import DEMO_MODE
from data_quality import is_listing_usable
from data_sufficiency import get_data_sufficiency, print_low_data_warning
from deduplication import is_representative_filter
from models import Listing


UF_TO_CLP = 37_000
UF_CACHE_TTL_SECONDS = 12 * 3600
_uf_cache = {
    "valor": None,
    "timestamp": 0,
}
M2_RANGE_RATIO = 0.15
MIN_SCORE = 0.4
RELAXED_MIN_SCORE = 0.25
TOP_K_COMPARABLES = 10
MAX_CANDIDATES = 150
MIN_M2 = 10
MAX_M2 = 1_000
MIN_PRICE_M2_UF = 5
MAX_PRICE_M2_UF = 300
DISTANCE_FULL_SCORE_KM = 0.5
DISTANCE_MAX_KM = 10
DISTANCE_SCALE_KM = 2.0
DISTANCE_HARD_CAP_KM = 5
MIN_CONFIDENCE_COMPARABLES = 3
TARGET_CONFIDENCE_COMPARABLES = 10
MIN_REQUIRED_COMPARABLES = 5
MIN_COMPARABLES = 5
LOW_DATA_MIN_SCORE = 0.55
LOW_DATA_MIN_REQUIRED_COMPARABLES = 6
LOW_DATA_MIN_AVG_SCORE = 0.55
PRICE_M2_MEDIAN_OUTLIER_RATIO = 0.50
DIFFERENT_COMUNA_SCORE_PENALTY = 0.15
DIFFERENT_SEGMENT_SCORE_PENALTY = 0.10
ADJACENT_CLUSTER_WEIGHT_PENALTY = 0.90
FAR_CLUSTER_WEIGHT_PENALTY = 0.75
MIN_WEIGHT_THRESHOLD = 0.02
_DISTANCE_SKIP_LOGGED = False
MICRO_LOC_KEYWORDS = {
    "metro": 1.03,
    "plaza": 1.02,
    "avenida": 1.01,
    "centro": 1.01,
}
PROPERTY_SEGMENTS = {
    "small_apartment": (MIN_M2, 50),
    "standard_apartment": (50, 100),
    "large_apartment": (100, MAX_M2),
}

WEIGHTS = {
    "m2_construidos": 0.35,
    "dormitorios": 0.15,
    "banos": 0.15,
    "estacionamientos": 0.10,
    "distancia": 0.25,
}


def get_uf_actual():
    if DEMO_MODE:
        return _uf_cache["valor"] or UF_TO_CLP

    try:
        if requests is None:
            if _uf_cache["valor"] is not None:
                return _uf_cache["valor"]
            return None

        now = time.time()

        if (
            _uf_cache["valor"] is not None
            and (now - _uf_cache["timestamp"]) < UF_CACHE_TTL_SECONDS
        ):
            return _uf_cache["valor"]

        res = requests.get("https://mindicador.cl/api/uf", timeout=5)
        data = res.json()

        if "serie" in data and len(data["serie"]) > 0:
            valor = data["serie"][0].get("valor")
        else:
            if _uf_cache["valor"] is not None:
                return _uf_cache["valor"]
            return None

        if valor is None:
            if _uf_cache["valor"] is not None:
                return _uf_cache["valor"]
            return None

        _uf_cache["valor"] = valor
        _uf_cache["timestamp"] = now

        return valor
    except Exception as e:
        # print(f"[UF API ERROR] {e}")
        if _uf_cache["valor"] is not None:
            return _uf_cache["valor"]
        return None


def obtener_precio_clp(listing):
    if listing.precio_uf is not None and listing.precio_uf > 0:
        uf_actual = get_uf_actual()
        if uf_actual:
            return listing.precio_uf * uf_actual
        return listing.precio_uf * UF_TO_CLP

    if listing.precio_clp is not None and listing.precio_clp > 0:
        return listing.precio_clp

    return None


def obtener_precio_uf(listing):
    if listing.precio_uf is not None and listing.precio_uf > 0:
        return listing.precio_uf

    if listing.precio_clp is not None and listing.precio_clp > 0:
        return listing.precio_clp / UF_TO_CLP

    return None


def _valor_numerico_positivo(valor):
    return valor is not None and valor > 0


def superficie_en_rango_sano(valor):
    return _valor_numerico_positivo(valor) and MIN_M2 <= valor <= MAX_M2


def _aplicar_filtro_rango_opcional(filters, columna, valor_objetivo, minimo=0):
    if valor_objetivo is None:
        return

    filters.append(
        or_(
            columna.is_(None),
            columna.between(max(valor_objetivo - 1, minimo), valor_objetivo + 1),
        )
    )


def get_property_segment(m2_construidos):
    if m2_construidos is None:
        return "standard_apartment"

    if m2_construidos < 50:
        return "small_apartment"

    if m2_construidos <= 100:
        return "standard_apartment"

    return "large_apartment"


def get_allowed_segments(target_segment, allow_adjacent_segments=False):
    if not allow_adjacent_segments:
        return [target_segment]

    adjacent = {
        "small_apartment": ["standard_apartment"],
        "standard_apartment": ["small_apartment", "large_apartment"],
        "large_apartment": ["standard_apartment"],
    }
    return [target_segment] + adjacent.get(target_segment, [])


def build_segment_filter(segments):
    segment_filters = []

    for segment in segments:
        min_m2, max_m2 = PROPERTY_SEGMENTS[segment]

        if segment == "small_apartment":
            segment_filters.append(Listing.m2_construidos < max_m2)
        elif segment == "large_apartment":
            segment_filters.append(Listing.m2_construidos > min_m2)
        else:
            segment_filters.append(
                Listing.m2_construidos.between(min_m2, max_m2)
            )

    return or_(*segment_filters)


def buscar_comparables(
    db,
    comuna,
    m2_construidos,
    dormitorios=None,
    banos=None,
    estacionamientos=None,
    m2_range_ratio=M2_RANGE_RATIO,
    max_candidates=MAX_CANDIDATES,
    exclude_listing_id=None,
    allow_adjacent_segments=False,
):
    if not superficie_en_rango_sano(m2_construidos):
        return []

    target_segment = get_property_segment(m2_construidos)
    allowed_segments = get_allowed_segments(
        target_segment,
        allow_adjacent_segments=allow_adjacent_segments,
    )
    print(f"[SEGMENT] Using segment: {target_segment}")

    min_m2_construidos = m2_construidos * (1 - m2_range_ratio)
    max_m2_construidos = m2_construidos * (1 + m2_range_ratio)

    filters = [
        Listing.status == "active",
        is_representative_filter(Listing),
        Listing.m2_construidos.is_not(None),
        Listing.m2_construidos >= MIN_M2,
        Listing.m2_construidos <= MAX_M2,
        Listing.m2_construidos >= min_m2_construidos,
        Listing.m2_construidos <= max_m2_construidos,
        or_(
            Listing.precio_clp.is_not(None),
            Listing.precio_uf.is_not(None),
        ),
    ]
    filters.append(build_segment_filter(allowed_segments))

    if comuna:
        filters.append(Listing.comuna == comuna)

    if exclude_listing_id is not None:
        filters.append(Listing.id != exclude_listing_id)

    _aplicar_filtro_rango_opcional(filters, Listing.dormitorios, dormitorios)
    _aplicar_filtro_rango_opcional(filters, Listing.banos, banos)
    _aplicar_filtro_rango_opcional(
        filters,
        Listing.estacionamientos,
        estacionamientos,
        minimo=0,
    )

    statement = select(Listing).where(*filters).limit(max_candidates)
    comparables = list(db.execute(statement).scalars().all())
    comparables_filtrados = [
        comparable for comparable in comparables if is_listing_usable(comparable)
    ]

    print(f"Comparables antes filtro calidad: {len(comparables)}")
    print(f"Comparables despues filtro calidad: {len(comparables_filtrados)}")

    return comparables_filtrados


def calcular_score_variable(valor_objetivo, valor_comparable):
    if valor_objetivo is None:
        return None

    if valor_comparable is None:
        return None

    if valor_objetivo <= 0:
        return None

    diferencia = abs(valor_objetivo - valor_comparable)
    score = 1 - (diferencia / max(valor_objetivo, 1))
    return max(min(score, 1), 0)


def calcular_distancia_km(lat1, lon1, lat2, lon2):
    return haversine(lat1, lon1, lat2, lon2)


def haversine(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return None

    try:
        lat1 = float(lat1)
        lon1 = float(lon1)
        lat2 = float(lat2)
        lon2 = float(lon2)
    except (TypeError, ValueError):
        return None

    radio_tierra_km = 6371
    delta_lat = radians(lat2 - lat1)
    delta_lon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
    haversine = (
        sin(delta_lat / 2) ** 2
        + cos(lat1) * cos(lat2) * sin(delta_lon / 2) ** 2
    )
    return 2 * radio_tierra_km * asin(sqrt(haversine))


def calcular_score_distancia(comparable, property_data):
    distancia_km = calcular_distancia_km(
        property_data.get("lat"),
        property_data.get("lon"),
        comparable.lat,
        comparable.lon,
    )

    if distancia_km is None:
        return None

    if distancia_km <= DISTANCE_FULL_SCORE_KM:
        return 1

    if distancia_km >= DISTANCE_MAX_KM:
        return 0

    score = 1 - (
        (distancia_km - DISTANCE_FULL_SCORE_KM)
        / (DISTANCE_MAX_KM - DISTANCE_FULL_SCORE_KM)
    )
    return max(min(score, 1), 0)


def _objetivo_tiene_distancia(property_data):
    return property_data.get("lat") is not None and property_data.get("lon") is not None


def calcular_score_comparable(comparable, property_data):
    componentes = {
        "m2_construidos": calcular_score_variable(
            property_data.get("m2_construidos"),
            comparable.m2_construidos,
        ),
        "dormitorios": calcular_score_variable(
            property_data.get("dormitorios"),
            comparable.dormitorios,
        ),
        "banos": calcular_score_variable(
            property_data.get("banos"),
            comparable.banos,
        ),
        "estacionamientos": calcular_score_variable(
            property_data.get("estacionamientos"),
            comparable.estacionamientos,
        ),
    }

    if _objetivo_tiene_distancia(property_data):
        componentes["distancia"] = calcular_score_distancia(comparable, property_data)

    puntaje_ponderado = 0
    peso_total = 0

    for campo, score in componentes.items():
        valor_objetivo = property_data.get(campo)

        if campo != "distancia" and valor_objetivo is None:
            continue

        peso = WEIGHTS[campo]
        peso_total += peso
        puntaje_ponderado += (score or 0) * peso

    if peso_total <= 0:
        return 0

    return max(min(puntaje_ponderado / peso_total, 1), 0)


def score(listing, sujeto):
    return calcular_score_comparable(listing, sujeto)


def comparable_tiene_superficie_similar(comparable, property_data) -> bool:
    m2_objetivo = property_data.get("m2_construidos")
    comparable_m2 = comparable.m2_construidos

    if not superficie_en_rango_sano(m2_objetivo):
        return False

    if not superficie_en_rango_sano(comparable_m2):
        return False

    return abs(comparable_m2 - m2_objetivo) / m2_objetivo <= 0.30


def comparable_tiene_rooms_similares(comparable, property_data) -> bool:
    for field in ("dormitorios", "banos"):
        target_value = property_data.get(field)
        comparable_value = getattr(comparable, field)

        if target_value is None or comparable_value is None:
            continue

        if abs(comparable_value - target_value) > 1:
            return False

    return True


def penalizar_score_por_comuna(score_value, comparable, property_data):
    target_comuna = property_data.get("comuna")

    if not target_comuna or not comparable.comuna:
        return score_value

    if comparable.comuna == target_comuna:
        return score_value

    log_filter("Penalized comparable due to different comuna")
    return max(score_value - DIFFERENT_COMUNA_SCORE_PENALTY, 0)


def penalizar_score_por_segmento(score_value, comparable, property_data):
    target_segment = get_property_segment(property_data.get("m2_construidos"))
    comparable_segment = get_property_segment(comparable.m2_construidos)

    if comparable_segment == target_segment:
        return score_value

    print(
        "[SEGMENT] Adjacent segment comparable penalized: "
        f"{comparable_segment}"
    )
    return max(score_value - DIFFERENT_SEGMENT_SCORE_PENALTY, 0)


def log_filter(message: str) -> None:
    print(f"[FILTER] {message}")


def calcular_percentil(valores, percentil):
    if not valores:
        return None

    valores_ordenados = sorted(valores)
    posicion = (len(valores_ordenados) - 1) * percentil
    indice_inferior = int(posicion)
    indice_superior = min(indice_inferior + 1, len(valores_ordenados) - 1)
    peso_superior = posicion - indice_inferior
    peso_inferior = 1 - peso_superior

    return (
        valores_ordenados[indice_inferior] * peso_inferior
        + valores_ordenados[indice_superior] * peso_superior
    )


def calcular_mediana_ponderada(valores_ponderados):
    if not valores_ponderados:
        return None

    valores_ordenados = sorted(valores_ponderados, key=lambda item: item["valor"])
    peso_total = sum(item["peso"] for item in valores_ordenados)

    if peso_total <= 0:
        return median([item["valor"] for item in valores_ordenados])

    punto_medio = peso_total / 2
    peso_acumulado = 0

    for item in valores_ordenados:
        peso_acumulado += item["peso"]

        if peso_acumulado >= punto_medio:
            return item["valor"]

    return valores_ordenados[-1]["valor"]


def calcular_percentil_ponderado(valores_ponderados, percentil):
    if not valores_ponderados:
        return None

    valores_ordenados = sorted(valores_ponderados, key=lambda item: item["valor"])
    peso_total = sum(max(item["peso"], 0) for item in valores_ordenados)

    if peso_total <= 0:
        return calcular_percentil([item["valor"] for item in valores_ordenados], percentil)

    objetivo = peso_total * percentil
    peso_acumulado = 0

    for item in valores_ordenados:
        peso_acumulado += max(item["peso"], 0)

        if peso_acumulado >= objetivo:
            return item["valor"]

    return valores_ordenados[-1]["valor"]


def aplicar_pesos_comparables(comparables, property_data):
    if not comparables:
        return []

    comparables_con_peso = []
    comparables_filtrados_por_peso = []

    for index, comparable in enumerate(comparables, start=1):
        peso_base = calcular_peso_comparable(comparable, property_data)
        comparable_id = comparable.get("listing_id") or index

        if peso_base < MIN_WEIGHT_THRESHOLD:
            print(
                f"[FILTER] Removed comparable {comparable_id} "
                f"due to low weight ({peso_base:.3f})"
            )
            comparables_filtrados_por_peso.append((comparable, peso_base))
            continue

        comparables_con_peso.append((comparable, peso_base))

    if (
        len(comparables_con_peso) < MIN_COMPARABLES
        and comparables_filtrados_por_peso
    ):
        print("[FILTER] Too few comparables, relaxing threshold")
        faltantes = MIN_COMPARABLES - len(comparables_con_peso)
        comparables_reincluidos = sorted(
            comparables_filtrados_por_peso,
            key=lambda item: item[0].get(
                "base_similarity_score",
                item[0].get("score") or 0,
            ),
            reverse=True,
        )[:faltantes]
        comparables_con_peso.extend(comparables_reincluidos)

    if not comparables_con_peso:
        return []

    comparables_filtrados = [
        comparable for comparable, _peso in comparables_con_peso
    ]
    pesos_base = [peso for _comparable, peso in comparables_con_peso]
    pesos_amplificados = [peso**1.2 for peso in pesos_base]
    peso_total = sum(pesos_amplificados)

    if peso_total <= 0:
        peso_normalizado = 1 / len(comparables_filtrados)
        pesos_normalizados = [peso_normalizado for _ in comparables_filtrados]
    else:
        pesos_normalizados = [peso / peso_total for peso in pesos_amplificados]

    comparables_ponderados = []

    for index, (comparable, weight) in enumerate(
        zip(comparables_filtrados, pesos_normalizados),
        start=1,
    ):
        comparable_ponderado = dict(comparable)
        comparable_ponderado["weight"] = weight
        comparable_id = comparable_ponderado.get("listing_id") or index
        print(f"[WEIGHT] Comparable {comparable_id} weight = {weight:.2f}")
        comparables_ponderados.append(comparable_ponderado)

    return comparables_ponderados


def calcular_peso_comparable(comparable, property_data):
    m2_weight = calcular_peso_m2(
        comparable.get("m2_construidos") or comparable.get("m2"),
        property_data.get("m2_construidos"),
    )
    comuna_weight = calcular_peso_comuna(
        comparable.get("comuna"),
        property_data.get("comuna"),
    )
    dormitorios_weight = calcular_peso_atributo(
        comparable.get("dormitorios"),
        property_data.get("dormitorios"),
    )
    banos_weight = calcular_peso_atributo(
        comparable.get("banos"),
        property_data.get("banos"),
    )

    base_weight = (
        m2_weight
        * comuna_weight
        * dormitorios_weight
        * banos_weight
        * max(comparable.get("score") or 0, 0.01)
    )
    base_weight *= calcular_boost_micro_location(comparable)
    log_distance_weighting_skipped()
    final_weight = base_weight * comparable.get("cluster_penalty", 1)

    return final_weight


def calcular_boost_micro_location(comparable):
    titulo = (comparable.get("titulo") or "").lower()
    boost = 1.0

    for keyword, value in MICRO_LOC_KEYWORDS.items():
        if keyword in titulo:
            boost *= value

    boost = min(boost, 1.05)
    comparable_id = comparable.get("listing_id") or "unknown"
    print(f"[MICRO-LOC] Comparable {comparable_id} boost={boost:.2f}")
    return boost


def log_distance_weighting_skipped():
    global _DISTANCE_SKIP_LOGGED

    if _DISTANCE_SKIP_LOGGED:
        return

    print("[DISTANCE] Skipped due to inconsistent spatial data")
    _DISTANCE_SKIP_LOGGED = True


def calcular_penalizacion_distancia(comparable, property_data):
    comparable_id = comparable.get("listing_id") or "unknown"
    distancia_km = comparable.get("distancia_km")

    if distancia_km is None:
        distancia_km = calcular_distancia_km(
            property_data.get("lat"),
            property_data.get("lon"),
            comparable.get("lat"),
            comparable.get("lon"),
        )

    if distancia_km is None:
        print(
            f"[DISTANCE] Comparable {comparable_id} "
            "distance=N/A penalty=1.000"
        )
        return 1

    distance_penalty = exp(-distancia_km / DISTANCE_SCALE_KM)

    if distancia_km > DISTANCE_HARD_CAP_KM:
        distance_penalty *= 0.5

    print(
        f"[DISTANCE] Comparable {comparable_id} "
        f"distance={distancia_km:.2f}km penalty={distance_penalty:.3f}"
    )
    return distance_penalty


def calcular_peso_m2(comparable_m2, target_m2):
    if comparable_m2 is None or target_m2 is None or target_m2 <= 0:
        return 0.7

    size_diff_ratio = abs(comparable_m2 - target_m2) / target_m2
    size_similarity = 1 - (size_diff_ratio * 1.2)
    return max(0, min(size_similarity, 1))


def calcular_peso_comuna(comparable_comuna, target_comuna):
    if not comparable_comuna or not target_comuna:
        return 0.8

    return 1 if comparable_comuna == target_comuna else 0.75


def calcular_peso_atributo(comparable_value, target_value):
    if comparable_value is None or target_value is None:
        return 0.8

    difference = abs(comparable_value - target_value)

    if difference == 0:
        return 1

    if difference == 1:
        return 0.75

    return 0.4


def calcular_promedio_ponderado_precio_m2(comparables):
    if not comparables:
        return None

    print("[AGGREGATION] Using trimmed weighted average")
    comparables = trim_comparables_by_precio_m2(comparables)
    weighted_sum = 0
    total_weight = 0

    for comparable in comparables:
        precio_m2 = comparable.get("precio_m2")
        weight = comparable.get("weight")

        if precio_m2 is None:
            continue

        if weight is None:
            weight = comparable.get("score") or 0

        if weight <= 0:
            continue

        weighted_sum += precio_m2 * weight
        total_weight += weight

    if total_weight <= 0:
        precios_m2 = [
            comparable["precio_m2"]
            for comparable in comparables
            if comparable.get("precio_m2") is not None
        ]
        return sum(precios_m2) / len(precios_m2) if precios_m2 else None

    return weighted_sum / total_weight


def trim_comparables_by_precio_m2(comparables, trim_ratio=0.10):
    comparables_validos = [
        comparable
        for comparable in comparables
        if comparable.get("precio_m2") is not None
    ]

    if len(comparables_validos) < 5:
        return comparables_validos

    comparables_ordenados = sorted(
        comparables_validos,
        key=lambda comparable: comparable["precio_m2"],
    )
    trim_count = max(1, int(len(comparables_ordenados) * trim_ratio))

    if len(comparables_ordenados) - (trim_count * 2) < 3:
        return comparables_ordenados

    return comparables_ordenados[trim_count:-trim_count]


def precio_m2_en_rango_sano(precio_m2_clp):
    if precio_m2_clp is None or precio_m2_clp <= 0:
        return False

    precio_m2_uf = precio_m2_clp / UF_TO_CLP
    return MIN_PRICE_M2_UF <= precio_m2_uf <= MAX_PRICE_M2_UF


def filtrar_precios_sanos(comparables_validos):
    comparables_filtrados = []

    for comparable in comparables_validos:
        if precio_m2_en_rango_sano(comparable.get("precio_m2")):
            comparables_filtrados.append(comparable)
            continue

        log_filter("Removed comparable due to invalid price/m2")

    return comparables_filtrados


def filtrar_outliers_mediana_precio_m2(comparables_validos):
    if len(comparables_validos) < 3:
        return comparables_validos

    precios_m2 = [
        comparable["precio_m2"]
        for comparable in comparables_validos
        if comparable.get("precio_m2") is not None
    ]

    if len(precios_m2) < 3:
        return comparables_validos

    mediana_precio_m2 = median(precios_m2)

    if mediana_precio_m2 <= 0:
        return comparables_validos

    limite_minimo = mediana_precio_m2 * (1 - PRICE_M2_MEDIAN_OUTLIER_RATIO)
    limite_maximo = mediana_precio_m2 * (1 + PRICE_M2_MEDIAN_OUTLIER_RATIO)
    comparables_filtrados = []
    removed_count = 0

    for comparable in comparables_validos:
        precio_m2 = comparable.get("precio_m2")

        if limite_minimo <= precio_m2 <= limite_maximo:
            comparables_filtrados.append(comparable)
            continue

        removed_count += 1

    if len(comparables_filtrados) < 3:
        return comparables_validos

    for _ in range(removed_count):
        log_filter("Removed comparable due to price/m2 outlier")

    return comparables_filtrados


def filtrar_outliers_iqr(comparables_validos):
    if len(comparables_validos) < 3:
        return comparables_validos

    precios_m2 = [comparable["precio_m2"] for comparable in comparables_validos]
    p25 = calcular_percentil(precios_m2, 0.25)
    p75 = calcular_percentil(precios_m2, 0.75)

    if p25 is None or p75 is None:
        return comparables_validos

    iqr = p75 - p25

    if iqr <= 0:
        return comparables_validos

    limite_minimo = p25 - 1.5 * iqr
    limite_maximo = p75 + 1.5 * iqr

    comparables_filtrados = [
        comparable
        for comparable in comparables_validos
        if limite_minimo <= comparable["precio_m2"] <= limite_maximo
    ]

    if len(comparables_filtrados) < 3:
        return comparables_validos

    removed_count = len(comparables_validos) - len(comparables_filtrados)

    for _ in range(removed_count):
        log_filter("Removed comparable due to IQR price/m2 outlier")

    return comparables_filtrados


def preparar_comparable(comparable, property_data, min_score=MIN_SCORE):
    precio_clp = obtener_precio_clp(comparable)

    if precio_clp is None:
        return None

    if not superficie_en_rango_sano(comparable.m2_construidos):
        return None

    if not comparable_tiene_superficie_similar(comparable, property_data):
        log_filter("Removed comparable due to m2 range mismatch")
        return None

    if not comparable_tiene_rooms_similares(comparable, property_data):
        log_filter("Removed comparable due to dormitorios/banos mismatch")
        return None

    score = calcular_score_comparable(comparable, property_data)
    score = penalizar_score_por_comuna(score, comparable, property_data)
    score = penalizar_score_por_segmento(score, comparable, property_data)

    if score < min_score:
        return None

    precio_m2 = precio_clp / comparable.m2_construidos

    if precio_m2 <= 0:
        return None

    if not precio_m2_en_rango_sano(precio_m2):
        log_filter("Removed comparable due to invalid price/m2")
        return None

    distancia_km = calcular_distancia_km(
        property_data.get("lat"),
        property_data.get("lon"),
        comparable.lat,
        comparable.lon,
    )

    return {
        "listing": comparable,
        "listing_id": comparable.id,
        "fuente": comparable.fuente,
        "source_listing_id": comparable.source_listing_id,
        "link": comparable.link or comparable.url,
        "url": comparable.url,
        "titulo": comparable.titulo,
        "comuna": comparable.comuna,
        "precio": precio_clp,
        "precio_clp": precio_clp,
        "precio_uf": comparable.precio_uf,
        "m2": comparable.m2_construidos,
        "precio_m2": precio_m2,
        "m2_construidos": comparable.m2_construidos,
        "dormitorios": comparable.dormitorios,
        "banos": comparable.banos,
        "estacionamientos": comparable.estacionamientos,
        "lat": comparable.lat,
        "lon": comparable.lon,
        "score": score,
        "distancia_km": distancia_km,
    }


def seleccionar_top_comparables(comparables, top_k=TOP_K_COMPARABLES):
    comparables_ordenados = sorted(
        comparables,
        key=lambda comparable: comparable["score"],
        reverse=True,
    )

    if len(comparables_ordenados) <= top_k:
        return comparables_ordenados

    return comparables_ordenados[:top_k]


def calcular_score_promedio(comparables):
    if not comparables:
        return 0

    return sum(comparable["score"] for comparable in comparables) / len(comparables)


def calcular_confianza(comparables, score_promedio):
    if len(comparables) < MIN_CONFIDENCE_COMPARABLES:
        return 0

    factor_cantidad = min(len(comparables) / TARGET_CONFIDENCE_COMPARABLES, 1)
    precios_m2 = [comparable["precio_m2"] for comparable in comparables]
    promedio = sum(precios_m2) / len(precios_m2)

    if promedio <= 0:
        factor_dispersion = 0
    else:
        desviacion = (
            sum((precio_m2 - promedio) ** 2 for precio_m2 in precios_m2)
            / len(precios_m2)
        ) ** 0.5
        coeficiente_variacion = desviacion / promedio
        factor_dispersion = max(1 - min(coeficiente_variacion / 0.35, 1), 0)

    confianza = (
        score_promedio * 0.50
        + factor_cantidad * 0.25
        + factor_dispersion * 0.25
    )
    return max(min(confianza, 1), 0)


def etiqueta_confianza(confidence_score, cantidad_comparables=0):
    if confidence_score is None:
        return "low"

    if confidence_score >= 0.75 and cantidad_comparables >= 8:
        return "high"

    if confidence_score >= 0.50 and cantidad_comparables >= MIN_REQUIRED_COMPARABLES:
        return "medium"

    return "low"


def degradar_confianza(confidence):
    if confidence == "high":
        return "medium"

    if confidence == "medium":
        return "low"

    return "low"


def aplicar_cluster_precio_m2(comparables_validos, property_data):
    if len(comparables_validos) < MIN_CONFIDENCE_COMPARABLES:
        return comparables_validos

    price_m2_values = [
        comparable["precio_m2"]
        for comparable in comparables_validos
        if comparable.get("precio_m2") is not None
    ]

    if len(price_m2_values) < MIN_CONFIDENCE_COMPARABLES:
        return comparables_validos

    p33 = calcular_percentil(price_m2_values, 0.33)
    p66 = calcular_percentil(price_m2_values, 0.66)

    if p33 is None or p66 is None:
        return comparables_validos

    target_cluster = obtener_cluster_objetivo(comparables_validos, property_data, p33, p66)
    print(f"[CLUSTER] Target cluster: {target_cluster}")

    comparables_clusterizados = []

    for comparable in comparables_validos:
        comparable_clusterizado = dict(comparable)
        comp_cluster = asignar_cluster_precio_m2(
            comparable.get("precio_m2"),
            p33,
            p66,
        )
        penalty = calcular_penalizacion_cluster(target_cluster, comp_cluster)
        comparable_clusterizado["price_m2_cluster"] = comp_cluster
        comparable_clusterizado["cluster_penalty"] = penalty
        comparable_id = comparable_clusterizado.get("listing_id") or "unknown"
        print(
            f"[CLUSTER] Comparable {comparable_id} "
            f"cluster={comp_cluster} penalty={penalty}"
        )
        comparables_clusterizados.append(comparable_clusterizado)

    return comparables_clusterizados


def obtener_precio_m2_objetivo(property_data):
    m2_objetivo = property_data.get("m2_construidos")

    if m2_objetivo is None or m2_objetivo <= 0:
        return None

    precio_clp = property_data.get("precio_clp")

    if precio_clp is None and property_data.get("precio_uf") is not None:
        precio_clp = property_data["precio_uf"] * UF_TO_CLP

    if precio_clp is None or precio_clp <= 0:
        return None

    return precio_clp / m2_objetivo


def obtener_cluster_objetivo(comparables_validos, property_data, p33, p66):
    target_price_m2 = obtener_precio_m2_objetivo(property_data)

    if target_price_m2 is not None:
        return asignar_cluster_precio_m2(target_price_m2, p33, p66)

    comparable_mas_similar = max(
        comparables_validos,
        key=lambda comparable: comparable.get("score") or 0,
        default=None,
    )

    if comparable_mas_similar is None:
        return "mid_cluster"

    return asignar_cluster_precio_m2(
        comparable_mas_similar.get("precio_m2"),
        p33,
        p66,
    )


def asignar_cluster_precio_m2(price_m2, p33, p66):
    if price_m2 is None:
        return "mid_cluster"

    if price_m2 <= p33:
        return "low_cluster"

    if price_m2 <= p66:
        return "mid_cluster"

    return "high_cluster"


def adjacent_price_m2_clusters(cluster):
    adjacent = {
        "low_cluster": ["mid_cluster"],
        "mid_cluster": ["low_cluster", "high_cluster"],
        "high_cluster": ["mid_cluster"],
    }
    return adjacent.get(cluster, [])


def calcular_penalizacion_cluster(target_cluster, comp_cluster):
    if target_cluster == comp_cluster:
        return 1.0

    if comp_cluster in adjacent_price_m2_clusters(target_cluster):
        return ADJACENT_CLUSTER_WEIGHT_PENALTY

    return FAR_CLUSTER_WEIGHT_PENALTY


def preparar_comparables_validos(candidatos, property_data, min_score=MIN_SCORE):
    comparables_validos = []

    for candidato in candidatos:
        comparable_preparado = preparar_comparable(
            candidato,
            property_data,
            min_score=min_score,
        )

        if comparable_preparado is None:
            continue

        comparables_validos.append(comparable_preparado)

    return aplicar_cluster_precio_m2(comparables_validos, property_data)


def _resultado_insuficiente(
    reason,
    candidatos=0,
    comparables_validos=0,
    low_data_mode=False,
    data_sufficiency=None,
):
    print("STATUS: INSUFFICIENT DATA")
    print("No reliable market valuation available")
    print(f"Reason: {reason}")
    return {
        "valuation_status": "insufficient_data",
        "estimated_value": None,
        "valor_estimado": None,
        "estimated_price": None,
        "precio_m2": None,
        "rango_min": None,
        "rango_max": None,
        "min_price": None,
        "max_price": None,
        "confidence": "low",
        "confidence_score": 0,
        "cantidad_comparables": 0,
        "score_promedio": 0,
        "comparables": [],
        "reason": reason,
        "candidatos": candidatos,
        "comparables_validos": comparables_validos,
        "low_data_mode": low_data_mode,
        "data_sufficiency": data_sufficiency,
    }


def calcular_tasacion_comparables(db, property_data):
    m2_objetivo = property_data.get("m2_construidos")
    data_sufficiency = get_data_sufficiency(db)
    low_data = data_sufficiency["low_data_mode"]
    required_comparables = (
        LOW_DATA_MIN_REQUIRED_COMPARABLES
        if low_data
        else MIN_REQUIRED_COMPARABLES
    )
    min_score = LOW_DATA_MIN_SCORE if low_data else MIN_SCORE

    print_low_data_warning(data_sufficiency)

    def insufficient(reason, candidatos=0, comparables_validos=0):
        return _resultado_insuficiente(
            reason,
            candidatos=candidatos,
            comparables_validos=comparables_validos,
            low_data_mode=low_data,
            data_sufficiency=data_sufficiency,
        )

    if not superficie_en_rango_sano(m2_objetivo):
        return insufficient("subject surface area is missing or outside valid range")

    candidatos = buscar_comparables(
        db,
        property_data.get("comuna"),
        m2_objetivo,
        property_data.get("dormitorios"),
        property_data.get("banos"),
        property_data.get("estacionamientos"),
    )
    print(f"Comparables encontrados: {len(candidatos)}")

    if len(candidatos) < required_comparables:
        print("[SEGMENT] Not enough comparables in segment; allowing adjacent segment")
        candidatos_fallback = buscar_comparables(
            db,
            property_data.get("comuna"),
            m2_objetivo,
            property_data.get("dormitorios"),
            property_data.get("banos"),
            property_data.get("estacionamientos"),
            allow_adjacent_segments=True,
        )
        if len(candidatos_fallback) > len(candidatos):
            candidatos = candidatos_fallback
            print(f"Comparables con segmento adyacente: {len(candidatos)}")

    if len(candidatos) < required_comparables:
        return insufficient(
            "not enough comparable candidates",
            candidatos=len(candidatos),
        )

    comparables_validos = preparar_comparables_validos(
        candidatos,
        property_data,
        min_score=min_score,
    )

    if not low_data and len(comparables_validos) < required_comparables:
        comparables_relajados = preparar_comparables_validos(
            candidatos,
            property_data,
            min_score=RELAXED_MIN_SCORE,
        )

        if len(comparables_relajados) > len(comparables_validos):
            print(
                "Filtro de score relajado: "
                f"{MIN_SCORE:.2f} -> {RELAXED_MIN_SCORE:.2f}"
            )
            comparables_validos = comparables_relajados

    if len(comparables_validos) < 5:
        return None

    if len(comparables_validos) < required_comparables:
        return insufficient(
            "not enough reliable comparables after scoring",
            candidatos=len(candidatos),
            comparables_validos=len(comparables_validos),
        )

    comparables_top = seleccionar_top_comparables(comparables_validos)
    comparables_filtrados = seleccionar_top_comparables(
        filtrar_outliers_iqr(
            filtrar_outliers_mediana_precio_m2(
                filtrar_precios_sanos(comparables_top)
            )
        )
    )

    if len(comparables_filtrados) < required_comparables:
        return insufficient(
            "not enough reliable comparables after price filtering",
            candidatos=len(candidatos),
            comparables_validos=len(comparables_filtrados),
        )

    score_promedio = calcular_score_promedio(comparables_filtrados)
    print(f"Comparables usados: {len(comparables_filtrados)}")
    print(f"Score promedio: {score_promedio:.3f}")

    if low_data and score_promedio < LOW_DATA_MIN_AVG_SCORE:
        return insufficient(
            "borderline comparable quality under low data mode",
            candidatos=len(candidatos),
            comparables_validos=len(comparables_filtrados),
        )

    comparables_filtrados = aplicar_pesos_comparables(
        comparables_filtrados,
        property_data,
    )

    precios_m2_debug = []
    for index, comparable in enumerate(comparables_filtrados, start=1):
        precio_m2 = comparable.get("precio_m2")
        precios_m2_debug.append(precio_m2)

        print(f"Comparable {index}:")
        print(f"precio_clp: {comparable.get('precio_clp') or comparable.get('precio')}")
        print(f"m2: {comparable.get('m2_construidos') or comparable.get('m2')}")
        print(f"precio_m2: {precio_m2}")
        print(f"comuna: {comparable.get('comuna')}")
        print(f"score: {comparable.get('score')}")

    mediana_precio_m2_debug = median(precios_m2_debug) if precios_m2_debug else None
    print(f"Precio/m2 comparables: {precios_m2_debug}")
    print(f"Mediana precio/m2: {mediana_precio_m2_debug}")
    print(f"Cantidad comparables: {len(comparables_filtrados)}")

    valores_ponderados = [
        {
            "valor": comparable["precio_m2"],
            "peso": comparable["weight"],
        }
        for comparable in comparables_filtrados
    ]
    precio_m2_estimado = calcular_promedio_ponderado_precio_m2(comparables_filtrados)

    if precio_m2_estimado is None:
        return insufficient(
            "unable to compute trimmed weighted average price",
            candidatos=len(candidatos),
            comparables_validos=len(comparables_filtrados),
        )

    p25_precio_m2 = calcular_percentil_ponderado(valores_ponderados, 0.25)
    p75_precio_m2 = calcular_percentil_ponderado(valores_ponderados, 0.75)

    valor_estimado = precio_m2_estimado * m2_objetivo
    rango_min = p25_precio_m2 * m2_objetivo if p25_precio_m2 is not None else None
    rango_max = p75_precio_m2 * m2_objetivo if p75_precio_m2 is not None else None
    confidence_score = calcular_confianza(comparables_filtrados, score_promedio)
    confidence = etiqueta_confianza(confidence_score, len(comparables_filtrados))

    if low_data:
        confidence = degradar_confianza(confidence)

    return {
        "valuation_status": "market_comparable",
        "valor_estimado": valor_estimado,
        "estimated_value": valor_estimado,
        "estimated_price": valor_estimado,
        "precio_m2": precio_m2_estimado,
        "rango_min": rango_min,
        "rango_max": rango_max,
        "min_price": rango_min,
        "max_price": rango_max,
        "confidence": confidence,
        "confidence_score": confidence_score,
        "cantidad_comparables": len(comparables_filtrados),
        "score_promedio": score_promedio,
        "comparables": comparables_filtrados,
        "reason": "market valuation based on reliable comparables",
        "low_data_mode": low_data,
        "data_sufficiency": data_sufficiency,
    }
