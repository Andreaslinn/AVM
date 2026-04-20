from __future__ import annotations

from comparables import calcular_tasacion_comparables


def get_valuation(db, property_data: dict):
    """
    Wrapper de calcular_tasacion_comparables.
    Punto único de entrada para tasación.
    """
    print("[VALUATION_SERVICE] Running valuation")

    result = calcular_tasacion_comparables(db, property_data)

    if result is None:
        return {"status": "insufficient_data"}

    if isinstance(result, dict):
        if "status" not in result:
            result["status"] = "ok"

        return result

    return {
        "status": "error",
        "raw_result": result,
    }
