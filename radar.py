from __future__ import annotations

from datetime import date
import time
from typing import Optional

from sqlalchemy import select

from comparables import (
    MAX_CANDIDATES,
    MIN_SCORE,
    RELAXED_MIN_SCORE,
    aplicar_pesos_comparables,
    calcular_confianza,
    calcular_promedio_ponderado_precio_m2,
    calcular_percentil_ponderado,
    calcular_score_promedio,
    buscar_comparables,
    filtrar_outliers_mediana_precio_m2,
    filtrar_outliers_iqr,
    filtrar_precios_sanos,
    get_uf_actual,
    obtener_precio_clp,
    preparar_comparables_validos,
    seleccionar_top_comparables,
)
from database import DEMO_MODE, SessionLocal, init_db
from data_sufficiency import (
    MIN_ACTIVE_LISTINGS,
    get_data_sufficiency,
    print_low_data_warning,
)
from deduplication import is_representative_filter, mark_duplicate_listings
from models import Listing, PriceHistory


DEFAULT_TOP_LIMIT = 20
DEMO_RADAR_MAX_LISTINGS = 40
DEMO_RADAR_MAX_COMPARABLES = 30
DEMO_RADAR_TIME_BUDGET_SECONDS = 6.0
MIN_RADAR_DATASET_SIZE = 10
MIN_UNDERVALUATION = 0.05
LOW_DATA_MIN_UNDERVALUATION = 0.10
OUTLIER_DISCOUNT_THRESHOLD = 0.30
MIN_MARKET_CONFIDENCE = 0.25
MIN_COMPARABLES = 3
MIN_SEGMENT_COMPARABLES = 5
LOW_DATA_MIN_COMPARABLES = 5
LOW_DATA_MIN_SCORE = 0.55
PRICE_DROP_SIGNAL_THRESHOLD = 0.03
LONG_TIME_ON_MARKET_DAYS = 60
STALE_TIME_ON_MARKET_DAYS = 120
SEGMENT_M2_RANGE_RATIO = 0.15
DORMITORIOS_FALLBACK_SCORE_PENALTY = 0.90


def run_radar(limit: int = DEFAULT_TOP_LIMIT) -> list[dict]:
    """Open a DB session and return the top investment opportunities."""
    init_db()
    if not DEMO_MODE:
        mark_duplicate_listings()

    with SessionLocal() as db:
        return detectar_oportunidades(db, limit=limit)


def get_top_opportunities(limit: int = DEFAULT_TOP_LIMIT) -> list[dict]:
    """Return explainable, auditable radar opportunities."""
    return run_radar(limit=limit)


def get_radar_ready_count() -> int:
    """Count active, non-duplicate listings that can enter radar analysis."""
    init_db()
    if not DEMO_MODE:
        mark_duplicate_listings()

    with SessionLocal() as db:
        return len(obtener_listings_candidatos(db))


def get_best_opportunity(budget, limit=3) -> list[dict]:
    """Return the strongest active purchase opportunities within a budget."""
    if budget is None or budget <= 0:
        raise ValueError("budget must be a positive number")

    init_db()
    if not DEMO_MODE:
        mark_duplicate_listings()
    opportunities = []

    with SessionLocal() as db:
        data_sufficiency = get_data_sufficiency(db)
        low_data = data_sufficiency["low_data_mode"]

        _ = get_uf_actual()

        for listing in obtener_listings_candidatos(db):
            listing_price = obtener_precio_clp(listing)

            if listing_price is None or listing_price > budget:
                continue

            opportunity = analizar_listing(db, listing, low_data_mode=low_data)

            if opportunity is None:
                continue

            opportunities.append(opportunity)

    return ordenar_oportunidades_radar(opportunities)[:limit]


def confidence_rank(confidence_level: Optional[str]) -> int:
    return {
        "low": 0,
        "medium": 1,
        "high": 2,
    }.get(confidence_level or "low", 0)


def detectar_oportunidades(db, limit: int = DEFAULT_TOP_LIMIT) -> list[dict]:
    """Analyze active listings and return the strongest undervaluation signals."""
    oportunidades = []
    candidatos = obtener_listings_candidatos(db)
    if DEMO_MODE:
        candidatos = candidatos[:DEMO_RADAR_MAX_LISTINGS]
    data_sufficiency = get_data_sufficiency(db)
    low_data = data_sufficiency["low_data_mode"]

    print_low_data_warning(data_sufficiency)

    if len(candidatos) < MIN_RADAR_DATASET_SIZE:
        print("Not enough data to run opportunity radar reliably")
        return []

    if low_data and len(candidatos) < MIN_ACTIVE_LISTINGS:
        print("Not enough data to run opportunity radar reliably")
        return []

    _ = get_uf_actual()

    started_at = time.monotonic()
    for listing in candidatos:
        if (
            DEMO_MODE
            and time.monotonic() - started_at > DEMO_RADAR_TIME_BUDGET_SECONDS
        ):
            print("[RADAR][DEMO] Time budget reached; returning partial result")
            break

        oportunidad = analizar_listing(db, listing, low_data_mode=low_data)

        if oportunidad is None:
            continue

        oportunidades.append(oportunidad)

    return ordenar_oportunidades_radar(oportunidades)[:limit]


def ordenar_oportunidades_radar(oportunidades: list[dict]) -> list[dict]:
    normales = [
        oportunidad
        for oportunidad in oportunidades
        if not oportunidad.get("is_outlier")
    ]
    outliers = [
        oportunidad
        for oportunidad in oportunidades
        if oportunidad.get("is_outlier")
    ]

    normales.sort(key=calcular_ranking_radar, reverse=True)
    outliers.sort(key=calcular_ranking_radar, reverse=True)

    return normales + outliers


def calcular_ranking_radar(oportunidad: dict) -> float:
    discount = oportunidad.get("discount")

    if discount is None:
        discount = oportunidad.get("undervaluation")

    if discount is None:
        discount = (oportunidad.get("descuento_porcentual") or 0) / 100

    return discount * (oportunidad.get("confidence_score") or 0)


def obtener_listings_candidatos(db) -> list[Listing]:
    """Only listings with usable price and m2 can be valued by comparables."""
    statement = (
        select(Listing)
        .where(
            Listing.status == "active",
            is_representative_filter(Listing),
            Listing.comuna.is_not(None),
            Listing.m2_construidos.is_not(None),
            Listing.m2_construidos > 0,
        )
        .where(
            (Listing.precio_clp.is_not(None)) | (Listing.precio_uf.is_not(None))
        )
        .order_by(Listing.last_seen.desc(), Listing.id.desc())
    )
    return list(db.execute(statement).scalars().all())


def analizar_listing(db, listing: Listing, low_data_mode: bool = False) -> Optional[dict]:
    """Estimate market value, compare against ask price, and build radar signals."""
    listing_price = obtener_precio_clp(listing)

    if listing_price is None or listing_price <= 0:
        return None

    property_data = property_data_from_listing(listing)

    if property_data["m2_construidos"] is None:
        return None

    valuation = estimar_valor_mercado(
        db,
        listing,
        property_data,
        low_data_mode=low_data_mode,
    )

    if valuation is None:
        return None

    market_value = valuation["estimated_price"]

    if market_value is None or market_value <= 0:
        return None

    undervaluation = (market_value - listing_price) / market_value

    min_undervaluation = (
        LOW_DATA_MIN_UNDERVALUATION
        if low_data_mode
        else MIN_UNDERVALUATION
    )

    if undervaluation < min_undervaluation:
        return None

    is_outlier = undervaluation > OUTLIER_DISCOUNT_THRESHOLD
    signals = calcular_signals(db, listing, valuation, undervaluation)
    missing_penalty = calcular_penalizacion_datos_faltantes(listing)
    missing_fields_pct = calcular_porcentaje_campos_faltantes(listing)
    radar_confidence_score = valuation["confidence_score"]
    confidence_level = calcular_confidence_level_desde_score(
        radar_confidence_score,
        is_outlier=is_outlier,
    )

    if is_outlier:
        print("[RADAR] Outlier detected: discount > 30%")

    valuation_for_result = {
        **valuation,
        "confidence_score": radar_confidence_score,
    }
    market_context = calcular_contexto_mercado(valuation["comparables"])
    comparables_resumen = resumir_comparables(
        valuation["comparables"],
        listing_price,
    )
    opportunity_score = calcular_opportunity_score(
        undervaluation=undervaluation,
        confidence_score=radar_confidence_score,
        price_drop_signal=signals["price_drop_pct"],
        time_on_market_days=signals["time_on_market_days"],
        missing_data_penalty=missing_penalty,
    )

    opportunity = {
        "listing_id": listing.id,
        "source_listing_id": listing.source_listing_id,
        "fuente": listing.fuente,
        "titulo": listing.titulo,
        "comuna": listing.comuna,
        "url": listing.url or listing.link,
        "link": getattr(listing, "link", None) or getattr(listing, "url", None),
        "m2": listing.m2_construidos,
        "listing_price": listing_price,
        "precio_publicado": listing_price,
        "market_value": market_value,
        "estimated_value": market_value,
        "valor_estimado": market_value,
        "undervaluation": undervaluation,
        "discount": undervaluation,
        "discount_pct": undervaluation * 100,
        "descuento_porcentual": undervaluation * 100,
        "opportunity_score": opportunity_score,
        "confidence_score": radar_confidence_score,
        "confidence_level": confidence_level,
        "confidence": confidence_level,
        "is_outlier": is_outlier,
        "outlier_reason": "high_discount_vs_market" if is_outlier else None,
        "min_price": valuation["min_price"],
        "max_price": valuation["max_price"],
        "precio_promedio_comparables": market_context["precio_promedio_comparables"],
        "precio_min_comparables": market_context["precio_min_comparables"],
        "precio_max_comparables": market_context["precio_max_comparables"],
        "price_drop_pct": signals["price_drop_pct"],
        "time_on_market_days": signals["time_on_market_days"],
        "missing_data_penalty": missing_penalty,
        "porcentaje_campos_faltantes": missing_fields_pct,
        "penalizacion_total": missing_penalty,
        "comparable_count": valuation["comparable_count"],
        "numero_comparables": valuation["comparable_count"],
        "score_promedio_comparables": valuation["score_promedio"],
        "comparables_resumen": comparables_resumen,
        "explanation_text": construir_explanation_text(
            comuna=listing.comuna,
            undervaluation=undervaluation,
            comparable_count=valuation["comparable_count"],
            confidence_level=confidence_level,
            missing_fields_pct=missing_fields_pct,
            market_context=market_context,
        ),
        "low_data_mode": low_data_mode,
        "reason": construir_reason(
            undervaluation=undervaluation,
            signals=signals,
            missing_data_penalty=missing_penalty,
            valuation=valuation_for_result,
        ),
    }
    opportunity["price_evolution"] = compute_price_evolution(listing, db)
    opportunity["score_breakdown"] = calcular_investment_score(opportunity)
    opportunity["investment_score"] = opportunity["score_breakdown"]["total"]
    opportunity["veredicto"] = generar_veredicto(opportunity["investment_score"])
    opportunity["label"] = generar_label(opportunity["investment_score"])
    opportunity["legal_profile"] = simulate_legal_risk(opportunity)
    agregar_metricas_inversion(opportunity)
    print(
        f"[INVESTMENT SCORE] Listing {listing.id} -> "
        f"score: {opportunity['investment_score']}"
    )
    return opportunity


def estimar_valor_mercado(
    db,
    listing: Listing,
    property_data: dict,
    low_data_mode: bool = False,
) -> Optional[dict]:
    """Use the comparable model while excluding the listing being analyzed."""
    candidatos = buscar_comparables(
        db,
        property_data.get("comuna"),
        property_data.get("m2_construidos"),
        property_data.get("dormitorios"),
        property_data.get("banos"),
        property_data.get("estacionamientos"),
        max_candidates=DEMO_RADAR_MAX_COMPARABLES if DEMO_MODE else MAX_CANDIDATES,
        exclude_listing_id=listing.id,
    )

    min_score = LOW_DATA_MIN_SCORE if low_data_mode else MIN_SCORE
    min_comparables = LOW_DATA_MIN_COMPARABLES if low_data_mode else MIN_COMPARABLES

    if len(candidatos) < min_comparables:
        print("[SEGMENT] Not enough comparables in segment; allowing adjacent segment")
        candidatos_fallback = buscar_comparables(
            db,
            property_data.get("comuna"),
            property_data.get("m2_construidos"),
            property_data.get("dormitorios"),
            property_data.get("banos"),
            property_data.get("estacionamientos"),
            max_candidates=DEMO_RADAR_MAX_COMPARABLES if DEMO_MODE else MAX_CANDIDATES,
            exclude_listing_id=listing.id,
            allow_adjacent_segments=True,
        )

        if len(candidatos_fallback) > len(candidatos):
            candidatos = candidatos_fallback

    print(f"Comparables totales: {len(candidatos)}")
    print("Segment mode: strict dormitorios")
    candidatos_strict = filtrar_comparables_por_segmento(
        candidatos,
        property_data,
        allow_adjacent_dormitorios=False,
    )
    print(f"[SEGMENT] strict dormitorios -> {len(candidatos_strict)} comparables")

    candidatos_segmentados = candidatos_strict
    usar_fallback_dormitorios = False

    if len(candidatos_strict) >= MIN_SEGMENT_COMPARABLES:
        candidatos_segmentados = candidatos_strict
    else:
        print("Fallback dormitorios activado")
        candidatos_relaxed = filtrar_comparables_por_segmento(
            candidatos,
            property_data,
            allow_adjacent_dormitorios=True,
        )
        print(
            f"[SEGMENT] relaxed +/-1 dormitorios -> "
            f"{len(candidatos_relaxed)} comparables"
        )

        if len(candidatos_relaxed) >= MIN_SEGMENT_COMPARABLES:
            candidatos_segmentados = candidatos_relaxed
            usar_fallback_dormitorios = True
        else:
            candidatos_sin_dormitorios = candidatos
            print(
                f"[SEGMENT] fallback sin dormitorios -> "
                f"{len(candidatos_sin_dormitorios)} comparables"
            )

            if len(candidatos_sin_dormitorios) >= MIN_COMPARABLES:
                candidatos_segmentados = candidatos_sin_dormitorios
                usar_fallback_dormitorios = True

    if len(candidatos) > 20 and len(candidatos_segmentados) == 0:
        candidatos_segmentados = candidatos
        usar_fallback_dormitorios = True
        print(
            "[SEGMENT] forced fallback sin dormitorios -> "
            f"{len(candidatos_segmentados)} comparables"
        )

    print(f"Comparables segmentados: {len(candidatos_segmentados)}")
    print(f"[SEGMENT] final selected -> {len(candidatos_segmentados)} comparables")

    if len(candidatos_segmentados) < MIN_COMPARABLES:
        print(
            f"[RADAR] Listing {listing.id}: insufficient_segment_data "
            f"({len(candidatos_segmentados)} comparables)"
        )
        return None

    comparables_validos = preparar_comparables_validos(
        candidatos_segmentados,
        property_data,
        min_score=min_score,
    )

    if not low_data_mode and len(comparables_validos) < min_comparables:
        comparables_relajados = preparar_comparables_validos(
            candidatos_segmentados,
            property_data,
            min_score=RELAXED_MIN_SCORE,
        )

        if len(comparables_relajados) > len(comparables_validos):
            comparables_validos = comparables_relajados

    if usar_fallback_dormitorios:
        comparables_validos = aplicar_penalizacion_fallback_dormitorios(
            comparables_validos,
            property_data,
        )

    if len(comparables_validos) < min_comparables:
        return None

    comparables_top = seleccionar_top_comparables(comparables_validos)
    comparables_filtrados = seleccionar_top_comparables(
        filtrar_outliers_iqr(
            filtrar_outliers_mediana_precio_m2(
                filtrar_precios_sanos(comparables_top)
            )
        )
    )

    if len(comparables_filtrados) < min_comparables:
        return None

    comparables_filtrados = aplicar_pesos_comparables(
        comparables_filtrados,
        property_data,
    )
    valores_ponderados = [
        {
            "valor": comparable["precio_m2"],
            "peso": comparable["weight"],
        }
        for comparable in comparables_filtrados
    ]
    precio_m2_estimado = calcular_promedio_ponderado_precio_m2(comparables_filtrados)

    if precio_m2_estimado is None:
        return None

    m2_objetivo = property_data["m2_construidos"]
    p25_precio_m2 = calcular_percentil_ponderado(valores_ponderados, 0.25)
    p75_precio_m2 = calcular_percentil_ponderado(valores_ponderados, 0.75)
    estimated_price = precio_m2_estimado * m2_objetivo
    min_price = p25_precio_m2 * m2_objetivo if p25_precio_m2 is not None else None
    max_price = p75_precio_m2 * m2_objetivo if p75_precio_m2 is not None else None
    score_promedio = calcular_score_promedio(comparables_filtrados)
    confidence_score = calcular_confianza(comparables_filtrados, score_promedio)

    if confidence_score < MIN_MARKET_CONFIDENCE:
        return None

    return {
        "estimated_price": estimated_price,
        "precio_m2": precio_m2_estimado,
        "min_price": min_price,
        "max_price": max_price,
        "confidence_score": confidence_score,
        "score_promedio": score_promedio,
        "comparable_count": len(comparables_filtrados),
        "comparables": comparables_filtrados,
    }


def property_data_from_listing(listing: Listing) -> dict:
    return {
        "comuna": listing.comuna,
        "lat": listing.lat,
        "lon": listing.lon,
        "m2_construidos": listing.m2_construidos,
        "m2_terreno": listing.m2_terreno,
        "dormitorios": listing.dormitorios,
        "banos": listing.banos,
        "estacionamientos": listing.estacionamientos,
    }


def filtrar_comparables_por_segmento(
    comparables: list[Listing],
    property_data: dict,
    allow_adjacent_dormitorios: bool = False,
) -> list[Listing]:
    """Keep only radar comparables that match the target segment tightly."""
    return [
        comparable
        for comparable in comparables
        if comparable_pertenece_al_segmento(
            comparable,
            property_data,
            allow_adjacent_dormitorios=allow_adjacent_dormitorios,
        )
    ]


def comparable_pertenece_al_segmento(
    comparable: Listing,
    property_data: dict,
    allow_adjacent_dormitorios: bool = False,
) -> bool:
    target_m2 = property_data.get("m2_construidos")
    comparable_m2 = comparable.m2_construidos

    if target_m2 is None or comparable_m2 is None or target_m2 <= 0:
        return False

    m2_diff_ratio = abs(comparable_m2 - target_m2) / target_m2
    if m2_diff_ratio > SEGMENT_M2_RANGE_RATIO:
        return False

    if not dormitorios_en_segmento(
        property_data.get("dormitorios"),
        comparable.dormitorios,
        allow_adjacent=allow_adjacent_dormitorios,
    ):
        return False

    if not atributo_en_rango_segmento(
        property_data.get("banos"),
        comparable.banos,
        max_diff=1,
        optional=True,
    ):
        return False

    return True


def dormitorios_en_segmento(
    target_value,
    comparable_value,
    allow_adjacent: bool = False,
) -> bool:
    if target_value is None or comparable_value is None:
        return False

    max_diff = 1 if allow_adjacent else 0
    return abs(comparable_value - target_value) <= max_diff


def aplicar_penalizacion_fallback_dormitorios(
    comparables_validos: list[dict],
    property_data: dict,
) -> list[dict]:
    target_dormitorios = property_data.get("dormitorios")

    if target_dormitorios is None:
        return comparables_validos

    comparables_penalizados = []
    for comparable in comparables_validos:
        comparable_dormitorios = comparable.get("dormitorios")

        if comparable_dormitorios == target_dormitorios:
            comparables_penalizados.append(comparable)
            continue

        comparable_penalizado = dict(comparable)
        comparable_penalizado["score"] = (
            (comparable_penalizado.get("score") or 0)
            * DORMITORIOS_FALLBACK_SCORE_PENALTY
        )
        comparable_penalizado["segment_fallback_dormitorios"] = True
        comparables_penalizados.append(comparable_penalizado)

    return comparables_penalizados


def atributo_en_rango_segmento(
    target_value,
    comparable_value,
    max_diff: int = 1,
    optional: bool = False,
) -> bool:
    if target_value is None or comparable_value is None:
        return optional

    return abs(comparable_value - target_value) <= max_diff


def calcular_signals(db, listing: Listing, valuation: dict, undervaluation: float) -> dict:
    price_drop_pct = calcular_caida_precio(db, listing)
    time_on_market_days = calcular_dias_en_mercado(db, listing)

    return {
        "undervaluation": undervaluation,
        "price_drop_pct": price_drop_pct,
        "time_on_market_days": time_on_market_days,
        "confidence_score": valuation["confidence_score"],
    }


def calcular_caida_precio(db, listing: Listing) -> float:
    """Compare first known price against current price to detect seller pressure."""
    current_price = obtener_precio_clp(listing)

    if current_price is None or current_price <= 0:
        return 0

    history = obtener_historial_precios(db, listing.id)

    if not history:
        return 0

    first_price = precio_historial_clp(history[0])

    if first_price is None or first_price <= 0 or first_price <= current_price:
        return 0

    return max((first_price - current_price) / first_price, 0)


def calcular_dias_en_mercado(db, listing: Listing) -> Optional[int]:
    """Use publication date when available, otherwise earliest captured price date."""
    start_date = listing.fecha_publicacion

    if start_date is None:
        history = obtener_historial_precios(db, listing.id)
        if history:
            start_date = history[0].fecha_captura

    if start_date is None:
        start_date = listing.fecha_captura

    if start_date is None:
        return None

    return max((date.today() - start_date).days, 0)


def obtener_historial_precios(db, listing_id: int) -> list[PriceHistory]:
    statement = (
        select(PriceHistory)
        .where(PriceHistory.listing_id == listing_id)
        .order_by(PriceHistory.fecha_captura.asc(), PriceHistory.id.asc())
    )
    return list(db.execute(statement).scalars().all())


def precio_historial_clp(price_history: PriceHistory) -> Optional[float]:
    if price_history.precio_clp_nuevo is not None and price_history.precio_clp_nuevo > 0:
        return price_history.precio_clp_nuevo

    if price_history.precio_clp is not None and price_history.precio_clp > 0:
        return price_history.precio_clp

    if price_history.precio_uf_nuevo is not None and price_history.precio_uf_nuevo > 0:
        return price_history.precio_uf_nuevo * 37_000

    if price_history.precio_uf is not None and price_history.precio_uf > 0:
        return price_history.precio_uf * 37_000

    return None


def compute_price_evolution(listing: Listing, db) -> dict:
    """Resume historial de precio sin afectar tasacion, comparables ni scoring."""
    current_price = obtener_precio_clp(listing)
    history = obtener_historial_precios(db, listing.id)

    def normalize_date(value):
        if value is None:
            return None
        if hasattr(value, "date"):
            return value.date()
        return value

    def valid_price(value):
        if value is None:
            return None
        try:
            price = float(value)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None
        return price

    price_points = []
    for row in history:
        price = valid_price(precio_historial_clp(row))
        if price is None:
            continue

        point_date = normalize_date(row.fecha_cambio) or normalize_date(
            row.fecha_captura
        )
        price_points.append((point_date, price))

    current_price = valid_price(current_price)
    if current_price is not None:
        price_points.append((date.today(), current_price))

    price_points.sort(key=lambda item: (item[0] is None, item[0] or date.max))
    prices = [price for _, price in price_points]

    all_time_high = max(prices) if prices else current_price
    all_time_low = min(prices) if prices else current_price

    price_drop_from_peak_pct = None
    if all_time_high is not None and all_time_high > 0 and current_price is not None:
        price_drop_from_peak_pct = max(
            (all_time_high - current_price) / all_time_high,
            0,
        )

    price_range_pct = None
    if all_time_high is not None and all_time_high > 0 and all_time_low is not None:
        price_range_pct = max((all_time_high - all_time_low) / all_time_high, 0)

    price_changes = 0
    last_change_date = None
    previous_distinct_price = None
    previous_price = None

    for point_date, price in price_points:
        if previous_price is None:
            previous_price = price
            continue

        if price != previous_price:
            price_changes += 1
            previous_distinct_price = previous_price
            last_change_date = point_date
            previous_price = price

    last_price_change_days = None
    if last_change_date is not None:
        last_price_change_days = max((date.today() - last_change_date).days, 0)

    trend = "stable"
    if (
        current_price is not None
        and previous_distinct_price is not None
        and current_price != previous_distinct_price
    ):
        trend = "down" if current_price < previous_distinct_price else "up"

    start_dates = [
        normalize_date(getattr(listing, "first_seen", None)),
        normalize_date(getattr(listing, "fecha_publicacion", None)),
    ]
    if history:
        start_dates.append(normalize_date(history[0].fecha_captura))

    valid_start_dates = [start_date for start_date in start_dates if start_date]
    days_on_market = None
    if valid_start_dates:
        days_on_market = max((date.today() - min(valid_start_dates)).days, 0)

    return {
        "days_on_market": days_on_market,
        "all_time_high": all_time_high,
        "all_time_low": all_time_low,
        "current_price": current_price,
        "price_drop_from_peak_pct": price_drop_from_peak_pct,
        "price_range_pct": price_range_pct,
        "price_changes": price_changes,
        "last_price_change_days": last_price_change_days,
        "trend": trend,
    }


def calcular_penalizacion_datos_faltantes(listing: Listing) -> float:
    """Missing non-critical fields reduce conviction without discarding the deal."""
    missing_count, total_fields = contar_campos_faltantes(listing)
    return min(missing_count * 0.04, 0.24)


def calcular_porcentaje_campos_faltantes(listing: Listing) -> float:
    missing_count, total_fields = contar_campos_faltantes(listing)

    if total_fields <= 0:
        return 0

    return missing_count / total_fields * 100


def contar_campos_faltantes(listing: Listing) -> tuple[int, int]:
    fields = [
        listing.comuna,
        listing.dormitorios,
        listing.banos,
        listing.estacionamientos,
        listing.lat,
        listing.lon,
    ]
    missing_count = sum(1 for value in fields if value is None)
    return missing_count, len(fields)


def calcular_confidence_level(
    comparable_count: int,
    missing_fields_pct: float,
) -> str:
    many_missing_fields = missing_fields_pct >= 40
    low_missing_fields = missing_fields_pct <= 20

    if comparable_count < 3 or many_missing_fields:
        return "low"

    if 3 <= comparable_count <= 5:
        return "medium"

    if comparable_count > 5 and low_missing_fields:
        return "high"

    return "medium"


def calcular_confidence_level_desde_score(
    confidence_score: float,
    is_outlier: bool = False,
) -> str:
    if is_outlier:
        return "low"

    if confidence_score >= 0.8:
        return "high"

    if confidence_score >= 0.65:
        return "medium"

    return "low"


def simulate_legal_risk(opportunity):
    score = 30
    legal_flags = []

    numero_comparables = (
        opportunity.get("numero_comparables")
        or opportunity.get("comparable_count")
        or 0
    )
    confianza = (
        opportunity.get("confianza")
        or opportunity.get("confidence_score")
        or 0
    )
    porcentaje_campos_faltantes = (
        opportunity.get("porcentaje_campos_faltantes")
        or 0
    )

    if confianza <= 1:
        confianza *= 100

    if numero_comparables < 3:
        score += 25
        legal_flags.append("Muy pocos comparables")
    elif numero_comparables < 6:
        score += 15
        legal_flags.append("Pocos comparables")

    if confianza < 60:
        score += 20
        legal_flags.append("Baja confianza en datos")
    elif confianza < 75:
        score += 10

    if porcentaje_campos_faltantes > 30:
        score += 20
        legal_flags.append("Datos incompletos")
    elif porcentaje_campos_faltantes > 15:
        score += 10
        legal_flags.append("Datos incompletos")

    if numero_comparables < 3 and confianza < 60:
        legal_flags.append("Alta incertidumbre en valorización")

    if porcentaje_campos_faltantes > 30 and confianza < 70:
        legal_flags.append("Información insuficiente para evaluación confiable")

    if numero_comparables >= 8 and confianza >= 80:
        legal_flags.append("Buena base de comparables y alta confiabilidad")

    legal_risk_score = min(score, 100)

    if legal_risk_score < 40:
        legal_risk_level = "Bajo"
        legal_summary = (
            f"No se detectan riesgos estructurales. Basado en {numero_comparables} "
            "comparables con buena consistencia de datos."
        )
    elif legal_risk_score < 70:
        legal_risk_level = "Medio"
        legal_summary = (
            f"Información parcial ({numero_comparables} comparables). "
            "Se recomienda validación adicional antes de invertir."
        )
    else:
        legal_risk_level = "Alto"
        legal_summary = (
            f"Alta incertidumbre: solo {numero_comparables} comparables y datos "
            "incompletos. Validar exhaustivamente."
        )

    return {
        "legal_risk_score": legal_risk_score,
        "legal_risk_level": legal_risk_level,
        "legal_flags": legal_flags,
        "legal_summary": legal_summary,
    }


def calcular_investment_score(opportunity):
    descuento_porcentual = opportunity.get("descuento_porcentual")

    if descuento_porcentual is None:
        descuento = opportunity.get("discount") or opportunity.get("undervaluation") or 0
    else:
        descuento = descuento_porcentual / 100

    discount_score = min(max(descuento, 0) / 0.30, 1.0)
    confidence_score = max(min(opportunity.get("confidence_score", 0) or 0, 1), 0)
    numero_comparables = (
        opportunity.get("numero_comparables")
        or opportunity.get("comparable_count")
        or 0
    )
    comparables_score = min(numero_comparables / 10, 1.0)
    missing_score_penalty = 0
    missing_confidence_factor = 1.0
    flags = []

    risk_penalty = 0
    if opportunity.get("is_outlier"):
        risk_penalty += 0.3

    missing = opportunity.get("porcentaje_campos_faltantes", 0) or 0
    missing_pct = missing if missing > 1 else missing * 100
    if missing > 1:
        missing = missing / 100
    risk_penalty += missing
    risk_score = max(0, 1 - risk_penalty)

    if missing_pct > 30:
        missing_score_penalty = 20
        missing_confidence_factor = 0.8
        flags.append("Datos incompletos afectan confiabilidad")
    elif missing_pct > 15:
        missing_score_penalty = 10
        missing_confidence_factor = 0.9
        flags.append("Datos incompletos afectan confiabilidad")

    if missing_score_penalty:
        confidence_score *= missing_confidence_factor
        print(f"[RISK] Missing data penalty applied -> -{missing_score_penalty} score")

    score = (
        discount_score * 0.4
        + confidence_score * 0.3
        + comparables_score * 0.2
        + risk_score * 0.1
    )
    total_score = max(0, int(score * 100) - missing_score_penalty)
    return {
        "total": total_score,
        "discount": discount_score * 100,
        "confidence": confidence_score * 100,
        "comparables": comparables_score * 100,
        "risk": risk_score * 100,
        "missing_data_penalty": missing_score_penalty,
        "flags": flags,
    }


def generar_veredicto(score):
    if score >= 75:
        return "Comprar"

    if score >= 55:
        return "Interesante"

    return "Evitar"


def generar_label(score):
    if score >= 75:
        return "🟢 Alta oportunidad"

    if score >= 55:
        return "🟡 Revisar"

    return "🔴 Riesgoso"


def calcular_appreciation(comuna):
    if comuna in ["Vitacura", "Las Condes"]:
        return 0.035

    if comuna in ["Providencia", "Ñuñoa"]:
        return 0.03

    return 0.02


def agregar_metricas_inversion(opportunity):
    yield_value = opportunity.get("yield", 0) or 0
    appreciation = calcular_appreciation(opportunity.get("comuna"))
    roi = yield_value + appreciation

    opportunity["roi"] = roi * 100
    opportunity["appreciation"] = appreciation * 100


def calcular_contexto_mercado(comparables: list[dict]) -> dict:
    precios = [
        comparable.get("precio") or comparable.get("precio_clp")
        for comparable in comparables
    ]
    precios = [precio for precio in precios if precio is not None and precio > 0]

    if not precios:
        return {
            "precio_promedio_comparables": None,
            "precio_min_comparables": None,
            "precio_max_comparables": None,
        }

    return {
        "precio_promedio_comparables": sum(precios) / len(precios),
        "precio_min_comparables": min(precios),
        "precio_max_comparables": max(precios),
    }


def resumir_comparables(
    comparables: list[dict],
    target_price: float,
    limit: int = 3,
) -> list[dict]:
    resumen = []

    for comparable in comparables[:limit]:
        precio = comparable.get("precio") or comparable.get("precio_clp")
        resumen.append(
            {
                "precio": precio,
                "m2_construidos": comparable.get("m2_construidos")
                or comparable.get("m2"),
                "comuna": comparable.get("comuna"),
                "diferencia_precio_vs_target": calcular_diferencia_precio_pct(
                    precio,
                    target_price,
                ),
            }
        )

    return resumen


def calcular_diferencia_precio_pct(precio_comparable, target_price) -> Optional[float]:
    if precio_comparable is None or target_price is None or target_price <= 0:
        return None

    return (precio_comparable - target_price) / target_price * 100


def construir_explanation_text(
    comuna: Optional[str],
    undervaluation: float,
    comparable_count: int,
    confidence_level: str,
    missing_fields_pct: float,
    market_context: dict,
) -> str:
    comuna_text = comuna or "la zona"
    min_price = format_price_short(market_context.get("precio_min_comparables"))
    max_price = format_price_short(market_context.get("precio_max_comparables"))
    confidence_text = {
        "low": "baja",
        "medium": "media",
        "high": "alta",
    }.get(confidence_level, confidence_level)

    if missing_fields_pct >= 40:
        confidence_reason = "varios datos faltantes"
    elif comparable_count <= 5:
        confidence_reason = "un numero acotado de comparables"
    else:
        confidence_reason = "buena cobertura de datos y comparables"

    return (
        f"Esta propiedad esta un {undervaluation:.0%} bajo el valor estimado. "
        f"Se comparo con {comparable_count} propiedades similares en {comuna_text} "
        f"con precios entre {min_price} y {max_price}. "
        f"Tiene confianza {confidence_text} debido a {confidence_reason}."
    )


def format_price_short(value) -> str:
    if value is None:
        return "N/D"

    if value >= 1_000_000:
        return f"{value / 1_000_000:.0f}M"

    return f"{value:,.0f}".replace(",", ".")


def calcular_opportunity_score(
    undervaluation: float,
    confidence_score: float,
    price_drop_signal: float,
    time_on_market_days: Optional[int],
    missing_data_penalty: float,
) -> float:
    """Secondary score for prioritizing similarly undervalued listings."""
    price_drop_bonus = min(price_drop_signal, 0.20)

    if time_on_market_days is None:
        time_bonus = 0
    elif time_on_market_days >= STALE_TIME_ON_MARKET_DAYS:
        time_bonus = 0.06
    elif time_on_market_days >= LONG_TIME_ON_MARKET_DAYS:
        time_bonus = 0.03
    else:
        time_bonus = 0

    score = (
        undervaluation * 0.65
        + confidence_score * 0.25
        + price_drop_bonus * 0.20
        + time_bonus
        - missing_data_penalty
    )
    return max(min(score, 1), 0)


def construir_reason(
    undervaluation: float,
    signals: dict,
    missing_data_penalty: float,
    valuation: dict,
) -> str:
    reasons = [
        f"{undervaluation:.1%} bajo valor estimado",
        f"{valuation['comparable_count']} comparables",
        f"confianza {valuation['confidence_score']:.0%}",
    ]

    if signals["price_drop_pct"] >= PRICE_DROP_SIGNAL_THRESHOLD:
        reasons.append(f"baja de precio {signals['price_drop_pct']:.1%}")

    if signals["time_on_market_days"] is not None:
        reasons.append(f"{signals['time_on_market_days']} dias en mercado")

    if missing_data_penalty > 0:
        reasons.append(f"penalizacion por datos faltantes {missing_data_penalty:.0%}")

    return "; ".join(reasons)


def print_top_opportunities(limit: int = DEFAULT_TOP_LIMIT) -> None:
    """Small CLI helper for manual runs: python radar.py."""
    opportunities = run_radar(limit=limit)

    for rank, opportunity in enumerate(opportunities, start=1):
        print(
            f"{rank}. Listing #{opportunity['listing_id']} | "
            f"{opportunity['comuna'] or 'Sin comuna'} | "
            f"undervaluation={opportunity['undervaluation']:.1%} | "
            f"confidence={opportunity['confidence_score']:.0%} | "
            f"{opportunity['reason']} | "
            f"{opportunity['url'] or ''}"
        )


if __name__ == "__main__":
    print_top_opportunities()
