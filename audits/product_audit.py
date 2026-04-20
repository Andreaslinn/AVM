"""Product-level audit checks for opportunity outputs.

This module does not change scoring or valuation behavior. It inspects the
already-produced opportunity payload and flags inconsistencies a human tester
would want to review before acting on the result.
"""

from __future__ import annotations


ALL_CHECKS = []


def register_check(func):
    ALL_CHECKS.append(func)
    return func


def _safe_number(value, default=None):
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _first_not_none(*values):
    for value in values:
        if value is not None:
            return value
    return None


def _get_score(opportunity):
    return _safe_number(
        _first_not_none(
            opportunity.get("investment_score"),
            opportunity.get("score"),
        )
    )


def _get_discount(opportunity):
    discount = _safe_number(
        _first_not_none(
            opportunity.get("discount"),
            opportunity.get("undervaluation"),
        )
    )
    if discount is not None:
        return discount

    discount_pct = _safe_number(
        _first_not_none(
            opportunity.get("discount_pct"),
            opportunity.get("descuento_porcentual"),
        )
    )
    if discount_pct is None:
        return None

    return discount_pct / 100


def _get_confidence(opportunity):
    confidence = _safe_number(
        _first_not_none(
            opportunity.get("confianza"),
            opportunity.get("confidence_score"),
        )
    )
    if confidence is None:
        return None
    return max(0, min(confidence, 1))


def _get_comparables(opportunity):
    value = _safe_number(
        _first_not_none(
            opportunity.get("numero_comparables"),
            opportunity.get("comparable_count"),
            opportunity.get("comparables"),
        )
    )
    return int(value) if value is not None else None


def _get_missing_pct(opportunity):
    return _safe_number(opportunity.get("porcentaje_campos_faltantes"), 0) or 0


def _get_price(opportunity):
    listing = opportunity.get("listing")
    return _safe_number(
        _first_not_none(
            opportunity.get("precio_publicado"),
            opportunity.get("listing_price"),
            opportunity.get("precio"),
            getattr(listing, "precio_clp", None),
        )
    )


def _get_m2(opportunity):
    return _safe_number(
        _first_not_none(
            opportunity.get("m2"),
            opportunity.get("m2_construidos"),
            opportunity.get("surface"),
        )
    )


def _get_verdict(opportunity):
    verdict = _first_not_none(
        opportunity.get("veredicto"),
        opportunity.get("decision"),
    )
    return str(verdict).strip().lower() if verdict else ""


def _get_text_payload(opportunity):
    fields = [
        opportunity.get("thesis"),
        opportunity.get("explanation_text"),
        opportunity.get("reason"),
        opportunity.get("summary"),
    ]
    flags = opportunity.get("risk_flags") or []
    if isinstance(flags, str):
        fields.append(flags)
    elif isinstance(flags, (list, tuple)):
        fields.extend(str(flag) for flag in flags)

    return " ".join(str(field) for field in fields if field).lower()


def _issue(message):
    return str(message)


def _empty_result():
    return {
        "warnings": [],
        "critical_issues": [],
    }


def audit_opportunity(opportunity):
    """Audit a single opportunity using all registered product checks."""
    opportunity = opportunity if hasattr(opportunity, "get") else {}
    warnings = []
    critical_issues = []

    for check in ALL_CHECKS:
        result = check(opportunity)

        if not result:
            continue

        warnings.extend(result.get("warnings", []))
        critical_issues.extend(result.get("critical_issues", []))

    severity = "high" if critical_issues else "medium" if warnings else "low"

    return {
        "listing_id": opportunity.get("listing_id"),
        "severity": severity,
        "warnings": warnings,
        "critical_issues": critical_issues,
    }


@register_check
def check_score_vs_data(opportunity):
    result = _empty_result()
    score = _get_score(opportunity)
    confidence = _get_confidence(opportunity)
    missing = _get_missing_pct(opportunity)

    if score is not None and score > 70 and missing > 50:
        result["critical_issues"].append("Score alto con baja calidad de datos")
    elif score is not None and score >= 75 and confidence is not None and confidence < 0.55:
        result["critical_issues"].append(
            _issue("Score alto con datos débiles. Revisar antes de presentarlo como señal fuerte.")
        )
    elif score is not None and score >= 60 and missing > 25:
        result["warnings"].append(
            _issue("Score atractivo, pero la ficha tiene datos incompletos relevantes.")
        )

    return result


@register_check
def check_discount_vs_comparables(opportunity):
    result = _empty_result()
    discount = _get_discount(opportunity)
    comparables = _get_comparables(opportunity)

    if discount is not None and discount >= 0.15 and (comparables is None or comparables < 4):
        result["critical_issues"].append(
            _issue("Descuento alto con pocos comparables. La oportunidad puede estar sobreestimada.")
        )
    elif discount is not None and discount >= 0.10 and comparables is not None and comparables < 7:
        result["warnings"].append(
            _issue("Descuento interesante, pero el respaldo de comparables es limitado.")
        )

    return result


@register_check
def check_negotiation(opportunity):
    result = _empty_result()
    confidence = _get_confidence(opportunity)
    missing = _get_missing_pct(opportunity)
    price_evolution = opportunity.get("price_evolution") or {}
    if not hasattr(price_evolution, "get"):
        price_evolution = {}

    days_on_market = _safe_number(price_evolution.get("days_on_market"))
    price_changes = _safe_number(price_evolution.get("price_changes"), 0) or 0
    price_drop_pct = _safe_number(price_evolution.get("price_drop_from_peak_pct"))
    has_negotiation_signal = (
        (days_on_market is not None and days_on_market > 25)
        or price_changes >= 2
        or (price_drop_pct is not None and price_drop_pct >= 0.08)
    )

    if has_negotiation_signal and (missing > 50 or confidence is not None and confidence < 0.5):
        result["warnings"].append(
            _issue("Hay señales de negociación, pero la confiabilidad del dato es baja.")
        )

    return result


@register_check
def check_narrative_vs_data(opportunity):
    result = _empty_result()
    score = _get_score(opportunity)
    confidence = _get_confidence(opportunity)
    comparables = _get_comparables(opportunity)
    text_payload = _get_text_payload(opportunity)
    positive_words = ("strong", "sólida", "solida", "alta confianza", "fuerte", "comprar")
    risk_words = ("riesgo", "pocos comparables", "baja confianza", "datos incompletos")

    if any(word in text_payload for word in positive_words):
        if confidence is not None and confidence < 0.5:
            result["critical_issues"].append(
                _issue("La narrativa suena positiva, pero la confianza del modelo es baja.")
            )
        if comparables is not None and comparables < 4:
            result["critical_issues"].append(
                _issue("La narrativa suena positiva, pero hay muy pocos comparables.")
            )

    if any(word in text_payload for word in risk_words) and score is not None and score >= 75:
        result["warnings"].append(
            _issue("El texto menciona riesgos, pero el score aparece como muy alto.")
        )

    return result


@register_check
def check_market_depth(opportunity):
    result = _empty_result()
    comparables = _get_comparables(opportunity)

    if comparables is None:
        result["warnings"].append(
            _issue("No se informa cantidad de comparables. Falta profundidad de mercado.")
        )
    elif comparables < 3:
        result["critical_issues"].append(
            _issue("Muy baja profundidad de mercado. No conviene confiar en el resultado sin revisión manual.")
        )
    elif comparables < 6:
        result["warnings"].append(_issue("Profundidad de mercado limitada. Usar con cautela."))

    return result


@register_check
def check_price_anomalies(opportunity):
    result = _empty_result()
    price = _get_price(opportunity)
    m2 = _get_m2(opportunity)

    if price is None or price <= 0:
        result["critical_issues"].append(_issue("Precio publicado ausente o inválido."))
    if m2 is None or m2 <= 0:
        result["warnings"].append(
            _issue("Superficie ausente o inválida. No se puede validar precio por m2.")
        )
    if price and m2 and m2 > 0:
        price_m2 = price / m2
        if price_m2 < 300_000:
            result["critical_issues"].append(
                _issue("Precio por m2 anormalmente bajo. Puede haber error de moneda, superficie o scraping.")
            )
        elif price_m2 > 10_000_000:
            result["critical_issues"].append(
                _issue("Precio por m2 anormalmente alto. Puede haber error de precio o superficie.")
            )

    return result


@register_check
def check_contradictions(opportunity):
    result = _empty_result()
    score = _get_score(opportunity)
    discount = _get_discount(opportunity)
    verdict = _get_verdict(opportunity)

    if verdict:
        if any(word in verdict for word in ("comprar", "strong")) and score is not None and score < 55:
            result["critical_issues"].append(
                _issue("El veredicto es positivo, pero el score no lo respalda.")
            )
        if any(word in verdict for word in ("evitar", "risk")) and score is not None and score >= 70:
            result["critical_issues"].append(
                _issue("El veredicto es negativo, pero el score aparece alto.")
            )
    if discount is not None and discount < 0 and score is not None and score >= 70:
        result["critical_issues"].append(
            _issue("El score es alto aunque la propiedad no muestra descuento.")
        )

    return result


def run_product_audit(opportunities):
    """Run product audit over multiple opportunities."""
    opportunities = opportunities or []
    audits = [audit_opportunity(opportunity) for opportunity in opportunities]
    problematic = [
        audit
        for audit in audits
        if audit["warnings"] or audit["critical_issues"]
    ]

    return {
        "total_opportunities": len(audits),
        "total_warnings": sum(len(audit["warnings"]) for audit in audits),
        "total_critical": sum(len(audit["critical_issues"]) for audit in audits),
        "problematic_opportunities": problematic,
        "audits": audits,
    }
