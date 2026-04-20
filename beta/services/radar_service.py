from __future__ import annotations

from radar import (
    MIN_RADAR_DATASET_SIZE,
    get_best_opportunity as radar_get_best_opportunity,
    get_radar_ready_count as radar_get_radar_ready_count,
    get_top_opportunities as radar_get_top_opportunities,
)


RADAR_INSUFFICIENT_DATA_MESSAGE = (
    "No hay suficientes datos u oportunidades disponibles para el radar."
)

RADAR_RESULT_DEFAULTS = {
    "opportunities": [],
    "ready_count": 0,
    "total_count": 0,
    "low_data_mode": False,
}


def validate_or_normalize_radar_result(radar_result):
    """Protege el contrato entre radar_service, app.py y audit_runner."""
    if not isinstance(radar_result, dict):
        print(
            "[RADAR_SERVICE][WARNING] radar_result no es dict; "
            "normalizando contrato"
        )
        radar_result = {}
    else:
        radar_result = dict(radar_result)

    for key, default_value in RADAR_RESULT_DEFAULTS.items():
        if key not in radar_result:
            print(
                f"[RADAR_SERVICE][WARNING] radar_result missing '{key}'; "
                "usando default seguro"
            )
            radar_result[key] = (
                list(default_value)
                if isinstance(default_value, list)
                else default_value
            )

    if not isinstance(radar_result["opportunities"], list):
        print(
            "[RADAR_SERVICE][WARNING] radar_result 'opportunities' no es lista; "
            "usando lista vacia"
        )
        radar_result["opportunities"] = []

    if "status" not in radar_result:
        print(
            "[RADAR_SERVICE][WARNING] radar_result missing 'status'; "
            "deduciendo status seguro"
        )
        radar_result["status"] = (
            "ok" if radar_result["opportunities"] else "insufficient_data"
        )

    try:
        radar_result["ready_count"] = int(radar_result["ready_count"] or 0)
    except (TypeError, ValueError):
        print(
            "[RADAR_SERVICE][WARNING] radar_result 'ready_count' invalido; "
            "usando 0"
        )
        radar_result["ready_count"] = 0

    try:
        radar_result["total_count"] = int(radar_result["total_count"] or 0)
    except (TypeError, ValueError):
        print(
            "[RADAR_SERVICE][WARNING] radar_result 'total_count' invalido; "
            "usando 0"
        )
        radar_result["total_count"] = 0

    if not isinstance(radar_result["low_data_mode"], bool):
        print(
            "[RADAR_SERVICE][WARNING] radar_result 'low_data_mode' invalido; "
            "usando False"
        )
        radar_result["low_data_mode"] = False

    return radar_result


def _build_radar_result(opportunities, ready_count):
    """Contrato unico esperado por app.py y audit_runner."""
    normalized_opportunities = opportunities if isinstance(opportunities, list) else []

    try:
        normalized_ready_count = int(ready_count or 0)
    except (TypeError, ValueError):
        normalized_ready_count = 0

    low_data_mode = normalized_ready_count < MIN_RADAR_DATASET_SIZE
    status = "ok" if normalized_opportunities else "insufficient_data"

    result = {
        "status": status,
        "opportunities": normalized_opportunities,
        "ready_count": normalized_ready_count,
        "total_count": len(normalized_opportunities),
        "low_data_mode": low_data_mode,
    }

    if status == "insufficient_data":
        result["message"] = RADAR_INSUFFICIENT_DATA_MESSAGE

    return validate_or_normalize_radar_result(result)


def get_investment_opportunities(limit=20):
    print("[RADAR_SERVICE] Fetching investment opportunities")
    ready_count = radar_get_radar_ready_count()
    opportunities = radar_get_top_opportunities(limit=limit)
    return _build_radar_result(opportunities, ready_count)


def get_best_opportunity(presupuesto, limit=3):
    return radar_get_best_opportunity(presupuesto, limit=limit)


def get_min_dataset_size():
    return MIN_RADAR_DATASET_SIZE
