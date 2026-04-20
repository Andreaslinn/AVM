from __future__ import annotations


def first_not_none(*values):
    for value in values:
        if value is not None:
            return value

    return None


def radar_clamp(value, minimum=0, maximum=1):
    return max(min(value, maximum), minimum)


def get_radar_discount(opportunity):
    discount = first_not_none(
        opportunity.get("discount"),
        opportunity.get("undervaluation"),
    )

    if discount is not None:
        return discount

    discount_pct = first_not_none(
        opportunity.get("discount_pct"),
        opportunity.get("descuento_porcentual"),
    )

    if discount_pct is None:
        return 0

    return discount_pct / 100


def get_property_type(opportunity):
    return first_not_none(
        opportunity.get("tipo_propiedad"),
        opportunity.get("property_type"),
        opportunity.get("tipo"),
        "Departamento",
    )


def get_property_m2(opportunity):
    return first_not_none(
        opportunity.get("m2_construidos"),
        opportunity.get("m2"),
        opportunity.get("surface"),
    )


def get_comparable_count(opportunity):
    return first_not_none(
        opportunity.get("numero_comparables"),
        opportunity.get("comparable_count"),
        0,
    )


def get_analysis_comparables(opportunity, target_price=None):
    raw_comparables = (
        opportunity.get("comparables")
        or opportunity.get("comparables_resumen")
        or []
    )
    rows = []

    for index, comparable in enumerate(raw_comparables, start=1):
        precio = first_not_none(
            comparable.get("precio_clp"),
            comparable.get("precio"),
            comparable.get("listing_price"),
        )
        m2 = first_not_none(
            comparable.get("m2_construidos"),
            comparable.get("m2"),
        )
        precio_m2 = comparable.get("precio_m2")

        if precio_m2 is None and precio is not None and m2 is not None and m2 > 0:
            precio_m2 = precio / m2

        diferencia = comparable.get("diferencia_precio_vs_target")
        if diferencia is None and precio is not None and target_price:
            diferencia = (precio - target_price) / target_price * 100

        rows.append(
            {
                "#": index,
                "precio_clp": precio,
                "m2": m2,
                "precio_m2": precio_m2,
                "diferencia_vs_target": diferencia,
                "comuna": comparable.get("comuna"),
            }
        )

    return rows


def build_risk_report_context(opportunity):
    precio_publicado = first_not_none(
        opportunity.get("precio_publicado"),
        opportunity.get("listing_price"),
    )
    valor_estimado = first_not_none(
        opportunity.get("valor_estimado"),
        opportunity.get("estimated_value"),
        opportunity.get("market_value"),
    )
    descuento = get_radar_discount(opportunity)
    confianza = radar_clamp(
        first_not_none(opportunity.get("confianza"), opportunity.get("confidence_score"), 0)
    )
    comparables = get_comparable_count(opportunity)
    score = opportunity.get("investment_score", 0) or 0
    legal_profile = opportunity.get("legal_profile") or {}
    legal_score = legal_profile.get("legal_risk_score")
    legal_level = legal_profile.get("legal_risk_level") or "Sin datos"
    legal_flags = legal_profile.get("legal_flags") or []
    missing_pct = opportunity.get("porcentaje_campos_faltantes", 0) or 0
    property_type = get_property_type(opportunity)
    comuna = opportunity.get("comuna") or "Sin comuna"
    m2 = get_property_m2(opportunity)
    comparables_rows = get_analysis_comparables(opportunity, precio_publicado)

    veredicto = get_risk_report_veredicto(score)
    thesis = get_risk_report_thesis(
        descuento,
        confianza,
        comparables,
        legal_level,
        missing_pct,
    )
    valuation_score, market_support_score, risk_score = get_risk_report_scores(
        descuento,
        comparables,
        confianza,
        legal_score,
        missing_pct,
    )
    risk_flags = get_risk_report_flags(
        legal_flags,
        missing_pct,
        comparables,
        confianza,
    )
    catalysts = get_risk_report_catalysts(
        descuento,
        comparables,
        confianza,
        legal_level,
    )

    return {
        "precio_publicado": precio_publicado,
        "valor_estimado": valor_estimado,
        "descuento": descuento,
        "confianza": confianza,
        "comparables": comparables,
        "score": score,
        "legal_score": legal_score,
        "legal_level": legal_level,
        "missing_pct": missing_pct,
        "property_type": property_type,
        "comuna": comuna,
        "m2": m2,
        "comparables_rows": comparables_rows,
        "veredicto": veredicto,
        "thesis": thesis,
        "valuation_score": valuation_score,
        "market_support_score": market_support_score,
        "risk_score": risk_score,
        "risk_flags": risk_flags,
        "catalysts": catalysts,
    }


def get_risk_report_veredicto(score):
    if score >= 75:
        return "COMPRAR"

    if score >= 55:
        return "REVISAR"

    return "EVITAR"


def get_risk_report_thesis(descuento, confianza, comparables, legal_level, missing_pct):
    if descuento >= 0.15 and confianza >= 0.65 and comparables >= 5:
        return "Activo subvalorado con buen respaldo de mercado."

    if legal_level == "Alto" or missing_pct > 30 or confianza < 0.55:
        return "Oportunidad con incertidumbre relevante."

    if descuento >= 0.10:
        return "Descuento atractivo, sujeto a validación de datos y antecedentes."

    return "Oportunidad defensiva: requiere comparar contra alternativas cercanas."


def get_risk_report_scores(descuento, comparables, confianza, legal_score, missing_pct):
    valuation_score = min(max(descuento, 0) / 0.30, 1) * 100
    market_support_score = (
        min((comparables or 0) / 10, 1) * 50
        + confianza * 50
    )
    legal_risk_penalty = (legal_score or 0) * 0.5
    missing_penalty = min(missing_pct, 50)
    risk_score = max(0, 100 - legal_risk_penalty - missing_penalty)
    return valuation_score, market_support_score, risk_score


def get_risk_report_flags(legal_flags, missing_pct, comparables, confianza):
    risk_flags = list(legal_flags)
    if missing_pct > 30:
        risk_flags.append("Datos incompletos sobre 30%.")
    elif missing_pct > 15:
        risk_flags.append("Datos parcialmente incompletos.")
    if comparables < 5:
        risk_flags.append("Pocos comparables para sostener convicción.")
    if confianza < 0.55:
        risk_flags.append("Confianza baja del modelo.")
    return risk_flags


def get_risk_report_catalysts(descuento, comparables, confianza, legal_level):
    catalysts = []
    if descuento > 0.15:
        catalysts.append("Descuento superior a 15% frente al valor estimado.")
    if comparables >= 5:
        catalysts.append("Base comparable suficiente para una revisión inicial.")
    if confianza >= 0.65:
        catalysts.append("Buena confianza relativa del modelo.")
    if legal_level == "Bajo":
        catalysts.append("Legal risk preliminar bajo.")
    return catalysts
