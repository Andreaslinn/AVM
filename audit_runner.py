from __future__ import annotations

import contextlib
import io
import json
import re
import traceback
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from statistics import mean, median
from typing import Any, Callable

from sqlalchemy import func, or_, select
from sqlalchemy.orm import joinedload

from data_quality import (
    calculate_precio_m2,
    get_listing_quality_issues,
    is_listing_usable,
    is_precio_m2_valid,
)
from database import SessionLocal
from deduplication import is_representative_filter
from evaluation_benchmark import run_benchmark
from models import Listing, PriceHistory, Property
from radar import get_radar_ready_count, get_top_opportunities


REPORTS_DIR = Path("reports")
MARKDOWN_REPORT_PATH = REPORTS_DIR / "system_audit_report.md"
RAW_REPORT_PATH = REPORTS_DIR / "system_audit_raw.json"
BENCHMARK_SAMPLE_SIZE = 100
RADAR_LIMIT = 20
MIN_COMUNA_RECORDS = 3

_RADAR_READY_CACHE: int | None = None
_RADAR_OPPORTUNITIES_CACHE: list[dict[str, Any]] | None = None
_RADAR_LOGS_CACHE = ""
_BENCHMARK_LOGS_CACHE = ""


def main() -> None:
    REPORTS_DIR.mkdir(exist_ok=True)

    audit = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "sections": {},
        "findings": [],
        "critical": [],
        "warnings": [],
        "info": [],
        "actions": [],
    }

    section_plan: list[tuple[str, str, Callable[[], dict[str, Any]]]] = [
        ("database_health", "Salud de base de datos", audit_database_health),
        ("data_quality", "Calidad de datos", audit_data_quality),
        ("coverage_by_comuna", "Cobertura por comuna", audit_coverage_by_comuna),
        ("valuation_engine", "Evaluación del motor", audit_valuation_engine),
        ("radar", "Auditoría de radar", audit_radar),
        ("service_contracts", "Verificación de contratos de services", audit_service_contracts),
        ("integration_flows", "Validación de flujos del sistema", audit_integration_flows),
        ("system_logs", "Auditoría de Logs del Sistema", audit_system_logs),
        ("legal_risk", "Auditoría de legal risk", audit_legal_risk),
        ("consistency_checks", "Consistency checks", audit_consistency_checks),
    ]

    for key, title, runner in section_plan:
        audit["sections"][key] = run_section(key, title, runner)

    collect_findings(audit)
    audit["system_health"] = evaluate_system_health(audit)
    write_json(RAW_REPORT_PATH, audit)
    write_markdown(MARKDOWN_REPORT_PATH, audit)

    print(f"Reporte markdown generado: {MARKDOWN_REPORT_PATH.resolve()}")
    print(
        "Resumen auditoría: "
        f"{len(audit['warnings'])} warnings, {len(audit['critical'])} críticos"
    )


def run_section(
    key: str,
    title: str,
    runner: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    try:
        data = runner()
        return {
            "key": key,
            "title": title,
            "status": data.pop("status", "OK"),
            "data": data,
            "error": None,
        }
    except Exception as exc:
        return {
            "key": key,
            "title": title,
            "status": "Falló",
            "data": {},
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            },
        }


def audit_database_health() -> dict[str, Any]:
    with SessionLocal() as db:
        total_properties = scalar_count(db, Property)
        total_listings = scalar_count(db, Listing)
        total_price_history = scalar_count(db, PriceHistory)
        status_counts = dict(
            db.execute(
                select(Listing.status, func.count())
                .group_by(Listing.status)
                .order_by(func.count().desc())
            ).all()
        )
        total_active = status_counts.get("active", 0)
        total_inactive = total_listings - total_active
        total_representatives = db.scalar(
            select(func.count()).select_from(Listing).where(is_representative_filter(Listing))
        )
        duplicates_by_source_link = [
            {
                "fuente": row.fuente,
                "link": row.link,
                "count": row.count,
            }
            for row in db.execute(
                select(Listing.fuente, Listing.link, func.count().label("count"))
                .where(Listing.link.is_not(None))
                .group_by(Listing.fuente, Listing.link)
                .having(func.count() > 1)
                .order_by(func.count().desc())
                .limit(25)
            ).all()
        ]
        duplicates_by_fingerprint = [
            {
                "property_fingerprint": row.property_fingerprint,
                "count": row.count,
            }
            for row in db.execute(
                select(Listing.property_fingerprint, func.count().label("count"))
                .where(Listing.property_fingerprint.is_not(None))
                .group_by(Listing.property_fingerprint)
                .having(func.count() > 1)
                .order_by(func.count().desc())
                .limit(25)
            ).all()
        ]
        listings = load_all_listings(db)

    no_price = 0
    no_m2 = 0
    no_comuna = 0
    no_lat_lon = 0
    precio_m2_valid = 0
    precio_m2_invalid = 0
    precio_m2_not_calculable = 0

    for listing in listings:
        if listing.precio_clp is None and listing.precio_uf is None:
            no_price += 1

        if listing.m2_construidos is None or listing.m2_construidos <= 0:
            no_m2 += 1

        if not listing.comuna:
            no_comuna += 1

        if listing.lat is None or listing.lon is None:
            no_lat_lon += 1

        precio_m2 = calculate_precio_m2(listing)
        if precio_m2 is None:
            precio_m2_not_calculable += 1
        elif is_precio_m2_valid(precio_m2):
            precio_m2_valid += 1
        else:
            precio_m2_invalid += 1

    return {
        "total_properties": total_properties,
        "total_listings": total_listings,
        "total_price_history": total_price_history,
        "total_active_listings": total_active,
        "total_inactive_listings": total_inactive,
        "total_representatives": total_representatives or 0,
        "status_counts": status_counts,
        "duplicates_by_source_link_count": len(duplicates_by_source_link),
        "duplicates_by_source_link_examples": duplicates_by_source_link,
        "duplicates_by_property_fingerprint_count": len(duplicates_by_fingerprint),
        "duplicates_by_property_fingerprint_examples": duplicates_by_fingerprint,
        "listings_without_price": no_price,
        "listings_without_m2_construidos": no_m2,
        "listings_without_comuna": no_comuna,
        "listings_without_lat_lon": no_lat_lon,
        "precio_m2": {
            "valid": precio_m2_valid,
            "invalid": precio_m2_invalid,
            "not_calculable": precio_m2_not_calculable,
        },
    }


def audit_data_quality() -> dict[str, Any]:
    with SessionLocal() as db:
        listings = load_all_listings(db)

    issue_counter: Counter[str] = Counter()
    usable_count = 0
    outliers_precio_m2 = []

    field_counters = {
        "dormitorios": 0,
        "banos": 0,
        "estacionamientos": 0,
        "ano_construccion": 0,
        "piscina": 0,
        "lat_lon": 0,
    }

    examples = []

    for listing in listings:
        if listing.dormitorios is not None:
            field_counters["dormitorios"] += 1

        if listing.banos is not None:
            field_counters["banos"] += 1

        if listing.estacionamientos is not None:
            field_counters["estacionamientos"] += 1

        property_obj = listing.property
        if property_obj and property_obj.ano_construccion is not None:
            field_counters["ano_construccion"] += 1

        if property_obj and property_obj.piscina is not None:
            field_counters["piscina"] += 1

        if listing.lat is not None and listing.lon is not None:
            field_counters["lat_lon"] += 1

        issues = get_listing_quality_issues(listing)
        issue_counter.update(issues)

        if is_listing_usable(listing):
            usable_count += 1

        if issues and len(examples) < 10:
            examples.append(
                {
                    "listing_id": listing.id,
                    "comuna": listing.comuna,
                    "issues": issues,
                    "url": listing.url or listing.link,
                }
            )

        precio_m2 = calculate_precio_m2(listing)
        if precio_m2 is not None and not is_precio_m2_valid(precio_m2):
            outliers_precio_m2.append(
                {
                    "listing_id": listing.id,
                    "comuna": listing.comuna,
                    "precio_m2": precio_m2,
                    "precio_clp": listing.precio_clp,
                    "precio_uf": listing.precio_uf,
                    "m2_construidos": listing.m2_construidos,
                    "url": listing.url or listing.link,
                }
            )

    total = len(listings)

    return {
        "total_listings_checked": total,
        "usable_listings": usable_count,
        "usable_ratio": ratio(usable_count, total),
        "field_coverage": {
            field: {
                "count": count,
                "ratio": ratio(count, total),
            }
            for field, count in field_counters.items()
        },
        "issue_counts": dict(issue_counter.most_common()),
        "top_quality_issues": issue_counter.most_common(10),
        "quality_issue_examples": examples,
        "precio_m2_outliers_count": len(outliers_precio_m2),
        "precio_m2_outlier_examples": sorted(
            outliers_precio_m2,
            key=lambda item: item["precio_m2"],
            reverse=True,
        )[:20],
    }


def audit_coverage_by_comuna() -> dict[str, Any]:
    with SessionLocal() as db:
        listings = load_all_listings(db)

    grouped: dict[str, list[Listing]] = {}
    for listing in listings:
        comuna = listing.comuna or "Sin comuna"
        grouped.setdefault(comuna, []).append(listing)

    coverage = []
    skipped_low_volume = []

    for comuna, comuna_listings in grouped.items():
        active = [listing for listing in comuna_listings if listing.status == "active"]
        precios_m2 = [
            precio_m2
            for precio_m2 in (calculate_precio_m2(listing) for listing in comuna_listings)
            if precio_m2 is not None
        ]

        if len(comuna_listings) < MIN_COMUNA_RECORDS:
            skipped_low_volume.append(
                {
                    "comuna": comuna,
                    "total_listings": len(comuna_listings),
                    "active_listings": len(active),
                }
            )
            continue

        coverage.append(
            {
                "comuna": comuna,
                "total_listings": len(comuna_listings),
                "active_listings": len(active),
                "precio_m2_count": len(precios_m2),
                "precio_m2_median": median(precios_m2) if precios_m2 else None,
                "precio_m2_min": min(precios_m2) if precios_m2 else None,
                "precio_m2_max": max(precios_m2) if precios_m2 else None,
                "dormitorios_coverage": ratio(
                    count_present(comuna_listings, "dormitorios"),
                    len(comuna_listings),
                ),
                "banos_coverage": ratio(
                    count_present(comuna_listings, "banos"),
                    len(comuna_listings),
                ),
                "estacionamientos_coverage": ratio(
                    count_present(comuna_listings, "estacionamientos"),
                    len(comuna_listings),
                ),
                "geo_coverage": ratio(
                    sum(
                        1
                        for listing in comuna_listings
                        if listing.lat is not None and listing.lon is not None
                    ),
                    len(comuna_listings),
                ),
            }
        )

    coverage.sort(key=lambda item: item["active_listings"], reverse=True)

    return {
        "minimum_records": MIN_COMUNA_RECORDS,
        "comuna_count": len(coverage),
        "skipped_low_volume_count": len(skipped_low_volume),
        "coverage": coverage,
        "skipped_low_volume_examples": skipped_low_volume[:20],
    }


def audit_valuation_engine() -> dict[str, Any]:
    global _BENCHMARK_LOGS_CACHE

    buffer = io.StringIO()

    with contextlib.redirect_stdout(buffer):
        benchmark = run_benchmark(sample_size=BENCHMARK_SAMPLE_SIZE)

    _BENCHMARK_LOGS_CACHE = buffer.getvalue()

    by_comuna = benchmark.get("by_comuna") or {}
    by_m2_range = benchmark.get("by_m2_range") or {}
    combined_groups = []

    for source, groups in (("comuna", by_comuna), ("m2_range", by_m2_range)):
        for group_name, metrics in groups.items():
            combined_groups.append({"source": source, "group": group_name, **metrics})

    best_groups = sorted(
        combined_groups,
        key=lambda item: (
            none_last(item.get("median_error_pct")),
            none_last(item.get("average_error_pct")),
        ),
    )[:5]
    worst_groups = sorted(
        combined_groups,
        key=lambda item: (
            none_last(item.get("median_error_pct")),
            none_last(item.get("average_error_pct")),
        ),
        reverse=True,
    )[:5]

    return {
        "benchmark": benchmark,
        "average_error_pct": (benchmark.get("overall") or {}).get("average_error_pct"),
        "median_error_pct": (benchmark.get("overall") or {}).get("median_error_pct"),
        "evaluated_count": benchmark.get("evaluated_count"),
        "skipped_count": benchmark.get("skipped_count"),
        "by_comuna": by_comuna,
        "by_m2_range": by_m2_range,
        "best_5_groups": best_groups,
        "worst_5_groups": worst_groups,
        "worst_individual_cases": (
            benchmark.get("results")
            or benchmark.get("biggest_errors")
            or []
        )[:10],
        "worst_individual_cases_source": (
            "results"
            if benchmark.get("results")
            else "biggest_errors"
            if benchmark.get("biggest_errors")
            else "not_available"
        ),
        "captured_stdout": _BENCHMARK_LOGS_CACHE,
    }


def audit_radar() -> dict[str, Any]:
    ready_count = load_radar_ready_count()
    opportunities = load_radar_opportunities()
    scores = [
        opportunity.get("investment_score")
        for opportunity in opportunities
        if opportunity.get("investment_score") is not None
    ]
    discounts = [
        get_discount_pct(opportunity)
        for opportunity in opportunities
        if get_discount_pct(opportunity) is not None
    ]

    return {
        "listings_ready_for_radar": ready_count,
        "requested_limit": RADAR_LIMIT,
        "opportunities_generated": len(opportunities),
        "average_investment_score": mean(scores) if scores else None,
        "average_discount_pct": mean(discounts) if discounts else None,
        "top_opportunities": [
            serialize_opportunity(opportunity) for opportunity in opportunities
        ],
    }


def audit_service_contracts() -> dict[str, Any]:
    """
    Verifica que los services devuelvan estructuras válidas.
    Detecta errores de integración como missing keys o tipos incorrectos.
    """
    result = {
        "status": "ok",
        "errors": [],
        "warnings": [],
    }

    try:
        from services import radar_service

        radar_result = radar_service.get_investment_opportunities(limit=5)

        if not isinstance(radar_result, dict):
            result["errors"].append("radar_service no devuelve dict")
        else:
            if "status" not in radar_result:
                result["errors"].append("radar_service missing 'status'")

            if "opportunities" not in radar_result:
                result["errors"].append("radar_service missing 'opportunities'")

            if "opportunities" in radar_result and not isinstance(
                radar_result["opportunities"],
                list,
            ):
                result["errors"].append("radar_service 'opportunities' no es lista")

            if "ready_count" not in radar_result:
                result["warnings"].append("radar_service missing 'ready_count'")

    except Exception as e:
        result["errors"].append(f"radar_service crash: {str(e)}")

    try:
        from database import SessionLocal
        from services import valuation_service

        with SessionLocal() as db:
            dummy_property = {
                "m2_construidos": 60,
                "comuna": "nunoa",
                "dormitorios": 2,
                "banos": 1,
            }

            valuation_result = valuation_service.get_valuation(db, dummy_property)

            if not isinstance(valuation_result, dict):
                result["errors"].append("valuation_service no devuelve dict")
            elif "status" not in valuation_result:
                result["errors"].append("valuation_service missing 'status'")

    except Exception as e:
        result["errors"].append(f"valuation_service crash: {str(e)}")

    if result["errors"]:
        result["status"] = "error"
    elif result["warnings"]:
        result["status"] = "warning"

    return result


def audit_integration_flows() -> dict[str, Any]:
    """
    Simula flujos reales del sistema:
    valuation → radar → risk
    Detecta errores de integración entre services.
    """
    result = {
        "status": "ok",
        "errors": [],
        "warnings": [],
    }
    radar_result = None

    try:
        from database import SessionLocal
        from services import risk_analysis_service, valuation_service

        with SessionLocal() as db:
            dummy_property = {
                "m2_construidos": 60,
                "comuna": "nunoa",
                "dormitorios": 2,
                "banos": 1,
            }

            valuation_result = valuation_service.get_valuation(db, dummy_property)

            if not isinstance(valuation_result, dict):
                result["errors"].append("valuation_result no es dict")
            else:
                try:
                    ctx = risk_analysis_service.build_risk_report_context(valuation_result)
                    if not isinstance(ctx, dict):
                        result["errors"].append("risk context no es dict")
                except Exception as e:
                    result["errors"].append(f"risk_analysis_service crash: {str(e)}")

    except Exception as e:
        result["errors"].append(f"valuation→risk flow crash: {str(e)}")

    try:
        from services import radar_service, risk_analysis_service

        radar_result = radar_service.get_investment_opportunities(limit=5)

        if isinstance(radar_result, dict) and "opportunities" in radar_result:
            opportunities = radar_result["opportunities"]

            if len(opportunities) > 0:
                try:
                    ctx = risk_analysis_service.build_risk_report_context(opportunities[0])
                    if not isinstance(ctx, dict):
                        result["errors"].append("risk context desde radar no es dict")
                except Exception as e:
                    result["errors"].append(f"risk crash desde radar: {str(e)}")
            else:
                result["warnings"].append("radar sin opportunities para test")

        else:
            result["errors"].append("radar_result inválido en integración")

    except Exception as e:
        result["errors"].append(f"radar→risk flow crash: {str(e)}")

    try:
        if "status" in radar_result:
            if radar_result["status"] not in ["ok", "warning", "error", "insufficient_data"]:
                result["warnings"].append("status inesperado en radar_result")
    except Exception:
        pass

    if result["errors"]:
        result["status"] = "error"
    elif result["warnings"]:
        result["status"] = "warning"

    return result


def audit_system_logs() -> dict[str, Any]:
    load_radar_ready_count()
    load_radar_opportunities()

    combined_logs = "\n".join(
        log_text
        for log_text in (_BENCHMARK_LOGS_CACHE, _RADAR_LOGS_CACHE)
        if log_text
    )
    parsed_metrics = parse_system_logs(combined_logs)
    automatic_findings = build_log_findings(parsed_metrics)

    return {
        "raw_logs": combined_logs,
        "parsed_metrics": parsed_metrics,
        "automatic_findings": automatic_findings,
    }


def audit_legal_risk() -> dict[str, Any]:
    opportunities = load_radar_opportunities()
    level_counter: Counter[str] = Counter()
    flag_counter: Counter[str] = Counter()
    scores = []
    missing_profile = []
    invalid_scores = []
    high_risk_examples = []
    inconsistencies = []

    for opportunity in opportunities:
        profile = opportunity.get("legal_profile")
        listing_id = opportunity.get("listing_id")

        if not profile:
            missing_profile.append(listing_id)
            continue

        score = profile.get("legal_risk_score")
        level = profile.get("legal_risk_level") or "Sin nivel"
        flags = profile.get("legal_flags") or []
        confidence_pct = get_confidence_pct(opportunity)
        comparables = get_comparable_count(opportunity)

        level_counter.update([level])
        flag_counter.update(flags)

        if score is None or score < 0 or score > 100:
            invalid_scores.append(
                {
                    "listing_id": listing_id,
                    "legal_risk_score": score,
                }
            )
        else:
            scores.append(score)

        if level == "Alto" or (score is not None and score >= 70):
            high_risk_examples.append(serialize_opportunity(opportunity))

        if level == "Bajo" and confidence_pct is not None and confidence_pct < 60:
            inconsistencies.append(
                finding(
                    "warning",
                    "legal_low_with_low_confidence",
                    "Legal risk bajo con confianza menor a 60.",
                    listing_id=listing_id,
                    details={
                        "confidence_pct": confidence_pct,
                        "legal_risk_score": score,
                    },
                )
            )

        if level == "Bajo" and comparables is not None and comparables < 3:
            inconsistencies.append(
                finding(
                    "warning",
                    "legal_low_with_few_comparables",
                    "Legal risk bajo con menos de 3 comparables.",
                    listing_id=listing_id,
                    details={
                        "numero_comparables": comparables,
                        "legal_risk_score": score,
                    },
                )
            )

        if (
            level == "Alto"
            and comparables is not None
            and comparables >= 8
            and confidence_pct is not None
            and confidence_pct >= 80
        ):
            inconsistencies.append(
                finding(
                    "info",
                    "legal_high_despite_strong_data",
                    "Legal risk alto con alta cobertura y alta confianza.",
                    listing_id=listing_id,
                    details={
                        "numero_comparables": comparables,
                        "confidence_pct": confidence_pct,
                        "legal_risk_score": score,
                    },
                )
            )

    return {
        "opportunities_checked": len(opportunities),
        "score_distribution": {
            "count": len(scores),
            "min": min(scores) if scores else None,
            "max": max(scores) if scores else None,
            "average": mean(scores) if scores else None,
            "median": median(scores) if scores else None,
        },
        "level_counts": dict(level_counter),
        "flag_counts": dict(flag_counter.most_common()),
        "top_flags": flag_counter.most_common(10),
        "high_risk_examples": high_risk_examples[:10],
        "missing_legal_profile": missing_profile,
        "invalid_scores": invalid_scores,
        "inconsistencies": inconsistencies,
    }


def audit_consistency_checks() -> dict[str, Any]:
    opportunities = load_radar_opportunities()
    issues = []

    for opportunity in opportunities:
        listing_id = opportunity.get("listing_id")
        confidence_pct = get_confidence_pct(opportunity)
        comparables = get_comparable_count(opportunity)
        missing_pct = opportunity.get("porcentaje_campos_faltantes")
        discount_pct = get_discount_pct(opportunity)
        investment_score = opportunity.get("investment_score")
        legal_profile = opportunity.get("legal_profile") or {}
        legal_score = legal_profile.get("legal_risk_score")
        legal_level = legal_profile.get("legal_risk_level")
        score_promedio = opportunity.get("score_promedio_comparables")
        valuation_status = opportunity.get("valuation_status")

        if confidence_pct is not None and confidence_pct > 80 and comparables is not None and comparables < 3:
            issues.append(
                finding(
                    "warning",
                    "high_confidence_with_few_comparables",
                    "Confianza > 80 con menos de 3 comparables.",
                    listing_id=listing_id,
                    details={
                        "confidence_pct": confidence_pct,
                        "numero_comparables": comparables,
                    },
                )
            )

        if is_strong_opportunity(opportunity) and missing_pct is not None and missing_pct > 30:
            issues.append(
                finding(
                    "warning",
                    "strong_opportunity_with_missing_data",
                    "Opportunity fuerte con más de 30% de campos faltantes.",
                    listing_id=listing_id,
                    details={
                        "investment_score": investment_score,
                        "descuento_porcentual": discount_pct,
                        "porcentaje_campos_faltantes": missing_pct,
                    },
                )
            )

        if (
            discount_pct is not None
            and discount_pct > 40
            and confidence_pct is not None
            and confidence_pct < 60
        ):
            issues.append(
                finding(
                    "critical",
                    "extreme_discount_with_low_confidence",
                    "Descuento porcentual > 40 con confianza baja.",
                    listing_id=listing_id,
                    details={
                        "descuento_porcentual": discount_pct,
                        "confidence_pct": confidence_pct,
                    },
                )
            )

        if (
            valuation_status is not None
            and valuation_status != "market_comparable"
            and is_strong_opportunity(opportunity)
        ):
            issues.append(
                finding(
                    "warning",
                    "strong_opportunity_non_market_comparable_status",
                    "Opportunity fuerte con valuation_status distinto de market_comparable.",
                    listing_id=listing_id,
                    details={
                        "valuation_status": valuation_status,
                        "investment_score": investment_score,
                    },
                )
            )

        if (
            investment_score is not None
            and investment_score >= 75
            and (legal_level == "Alto" or (legal_score is not None and legal_score >= 70))
        ):
            issues.append(
                finding(
                    "warning",
                    "high_investment_score_with_high_legal_risk",
                    "Investment score alto con legal risk alto.",
                    listing_id=listing_id,
                    details={
                        "investment_score": investment_score,
                        "legal_risk_score": legal_score,
                        "legal_risk_level": legal_level,
                    },
                )
            )

        if legal_level == "Bajo" and confidence_pct is not None and confidence_pct < 60:
            issues.append(
                finding(
                    "warning",
                    "legal_low_with_low_confidence",
                    "Legal risk bajo con confianza menor a 60.",
                    listing_id=listing_id,
                    details={
                        "confidence_pct": confidence_pct,
                        "legal_risk_score": legal_score,
                    },
                )
            )

        if (
            comparables is not None
            and comparables >= 8
            and score_promedio is not None
            and score_promedio < 0.55
        ):
            issues.append(
                finding(
                    "info",
                    "many_comparables_with_low_average_score",
                    "Comparables altos pero score promedio bajo.",
                    listing_id=listing_id,
                    details={
                        "numero_comparables": comparables,
                        "score_promedio_comparables": score_promedio,
                    },
                )
            )

    return {
        "checks_run": len(opportunities),
        "issues": issues,
        "issue_counts_by_severity": dict(Counter(issue["severity"] for issue in issues)),
        "radar_ready_count": load_radar_ready_count(),
    }


def collect_findings(audit: dict[str, Any]) -> None:
    findings = audit["findings"]

    for key, section in audit["sections"].items():
        if section["status"] == "Falló":
            findings.append(
                finding(
                    "critical",
                    "section_failed",
                    f"{section['title']}: sección falló.",
                    section=key,
                    details=section.get("error"),
                )
            )

    db_data = get_section_data(audit, "database_health")
    if db_data:
        if db_data.get("total_active_listings", 0) == 0:
            findings.append(
                finding("critical", "no_active_listings", "No hay listings activos.")
            )
        if db_data.get("listings_without_price", 0) > 0:
            findings.append(
                finding(
                    "warning",
                    "listings_without_price",
                    "Existen listings sin precio.",
                    details={"count": db_data["listings_without_price"]},
                )
            )
        if db_data.get("listings_without_m2_construidos", 0) > 0:
            findings.append(
                finding(
                    "warning",
                    "listings_without_m2",
                    "Existen listings sin m2_construidos.",
                    details={"count": db_data["listings_without_m2_construidos"]},
                )
            )
        if db_data.get("duplicates_by_source_link_count", 0) > 0:
            findings.append(
                finding(
                    "warning",
                    "duplicate_source_link",
                    "Se detectaron duplicados por fuente + link.",
                    details={"groups": db_data["duplicates_by_source_link_count"]},
                )
            )

    quality_data = get_section_data(audit, "data_quality")
    if quality_data:
        usable_ratio = quality_data.get("usable_ratio", 0)
        if usable_ratio < 0.5:
            findings.append(
                finding(
                    "warning",
                    "low_usable_listing_ratio",
                    "Baja proporción de listings usables.",
                    details={"usable_ratio": usable_ratio},
                )
            )
        if quality_data.get("precio_m2_outliers_count", 0) > 0:
            findings.append(
                finding(
                    "warning",
                    "precio_m2_outliers",
                    "Existen outliers de precio/m2.",
                    details={"count": quality_data["precio_m2_outliers_count"]},
                )
            )

    benchmark_data = get_section_data(audit, "valuation_engine")
    if benchmark_data:
        evaluated_count = benchmark_data.get("evaluated_count") or 0
        median_error = benchmark_data.get("median_error_pct")
        if evaluated_count == 0:
            findings.append(
                finding("critical", "benchmark_no_results", "Benchmark no evaluó casos.")
            )
        elif median_error is not None and median_error > 25:
            findings.append(
                finding(
                    "warning",
                    "high_median_model_error",
                    "Error mediano del motor sobre 25%.",
                    details={"median_error_pct": median_error},
                )
            )

    radar_data = get_section_data(audit, "radar")
    if radar_data:
        if radar_data.get("listings_ready_for_radar", 0) == 0:
            findings.append(
                finding("critical", "radar_no_ready_listings", "Radar sin listings listos.")
            )
        if radar_data.get("opportunities_generated", 0) == 0:
            findings.append(
                finding("warning", "radar_no_opportunities", "Radar no generó oportunidades.")
            )

    service_contracts_data = get_section_data(audit, "service_contracts")
    if service_contracts_data:
        for message in service_contracts_data.get("errors", []):
            findings.append(
                finding(
                    "critical",
                    "service_contract_error",
                    message,
                    section="service_contracts",
                )
            )

        for message in service_contracts_data.get("warnings", []):
            findings.append(
                finding(
                    "warning",
                    "service_contract_warning",
                    message,
                    section="service_contracts",
                )
            )

    integration_flows_data = get_section_data(audit, "integration_flows")
    if integration_flows_data:
        for message in integration_flows_data.get("errors", []):
            findings.append(
                finding(
                    "critical",
                    "integration_flow_error",
                    message,
                    section="integration_flows",
                )
            )

        for message in integration_flows_data.get("warnings", []):
            findings.append(
                finding(
                    "warning",
                    "integration_flow_warning",
                    message,
                    section="integration_flows",
                )
            )

    legal_data = get_section_data(audit, "legal_risk")
    if legal_data:
        if legal_data.get("missing_legal_profile"):
            findings.append(
                finding(
                    "critical",
                    "missing_legal_profile",
                    "Hay oportunidades sin legal_profile.",
                    details=legal_data["missing_legal_profile"],
                )
            )
        if legal_data.get("invalid_scores"):
            findings.append(
                finding(
                    "critical",
                    "invalid_legal_scores",
                    "Hay legal_risk_score fuera de rango.",
                    details=legal_data["invalid_scores"],
                )
            )
        findings.extend(legal_data.get("inconsistencies", []))

    log_data = get_section_data(audit, "system_logs")
    if log_data:
        findings.extend(log_data.get("automatic_findings", []))

    consistency_data = get_section_data(audit, "consistency_checks")
    if consistency_data:
        findings.extend(consistency_data.get("issues", []))

    audit["critical"] = [item for item in findings if item["severity"] == "critical"]
    audit["warnings"] = [item for item in findings if item["severity"] == "warning"]
    audit["info"] = [item for item in findings if item["severity"] == "info"]
    audit["actions"] = build_actions(audit)


def evaluate_system_health(audit_results: dict[str, Any]) -> dict[str, Any]:
    """
    Analiza todos los resultados del audit y genera:
    - severidad por categoría
    - score global
    - estado general del sistema
    """
    severity = {
        "critical": [],
        "high": [],
        "medium": [],
        "low": [],
    }

    def add(level: str, message: Any) -> None:
        text = str(message)
        if text and text not in severity[level]:
            severity[level].append(text)

    def classify_message(message: str, default_level: str = "low") -> None:
        normalized = message.lower()

        if (
            "crash" in normalized
            or "missing 'status'" in normalized
            or "no devuelve dict" in normalized
            or "radar_result inválido" in normalized
            or "section_failed" in normalized
        ):
            add("critical", message)
        elif (
            "missing m2" in normalized
            or "sin m2" in normalized
            or "comparables < 3" in normalized
            or "menos de 3" in normalized
            or "error del modelo > 30%" in normalized
            or "error mediano del motor sobre 30%" in normalized
        ):
            add("high", message)
        elif (
            "lat/lon" in normalized
            or "cobertura baja" in normalized
            or "baja proporción" in normalized
            or "baja proporcion" in normalized
            or "missing_basic_attributes" in normalized
        ):
            add("medium", message)
        elif default_level in severity:
            add(default_level, message)
        else:
            add("low", message)

    for key, section in audit_results.get("sections", {}).items():
        if section.get("status") == "Falló":
            add("critical", f"{key}: sección falló")
        for message in (section.get("data") or {}).get("errors", []):
            classify_message(str(message), "critical")
        for message in (section.get("data") or {}).get("warnings", []):
            classify_message(str(message), "low")

    for item in audit_results.get("findings", []):
        code = item.get("code") or "finding"
        message = item.get("message") or ""
        text = f"{code}: {message}"
        default_level = {
            "critical": "critical",
            "warning": "low",
            "info": "low",
        }.get(item.get("severity"), "low")
        classify_message(text, default_level)

    db_data = get_section_data(audit_results, "database_health") or {}
    missing_m2 = db_data.get("listings_without_m2_construidos") or 0
    missing_lat_lon = db_data.get("listings_without_lat_lon") or 0
    if missing_m2 > 0:
        add("high", f"missing m2: {missing_m2} listings sin m2_construidos")
    if missing_lat_lon > 0:
        add("medium", f"missing lat/lon: {missing_lat_lon} listings sin coordenadas")

    benchmark_data = get_section_data(audit_results, "valuation_engine") or {}
    median_error = benchmark_data.get("median_error_pct")
    average_error = benchmark_data.get("average_error_pct")
    if median_error is not None and median_error > 30:
        add("high", f"error del modelo > 30%: mediana {median_error:.1f}%")
    if average_error is not None and average_error > 30:
        add("high", f"error del modelo > 30%: promedio {average_error:.1f}%")

    score = 100
    score -= 40 * len(severity["critical"])
    score -= 20 * len(severity["high"])
    score -= 10 * len(severity["medium"])
    score -= 5 * len(severity["low"])
    score = max(score, 0)

    if severity["critical"]:
        system_status = "CRITICAL"
    elif severity["high"]:
        system_status = "UNSTABLE"
    elif severity["medium"]:
        system_status = "WARNING"
    else:
        system_status = "HEALTHY"

    return {
        "system_status": system_status,
        "score": score,
        "severity": severity,
    }


def build_actions(audit: dict[str, Any]) -> list[str]:
    actions = []

    if audit["critical"]:
        actions.append("Revisar hallazgos críticos antes de usar resultados comerciales.")

    if audit["warnings"]:
        actions.append("Priorizar warnings repetidos en calidad de datos y radar.")

    legal_data = get_section_data(audit, "legal_risk")
    if legal_data and legal_data.get("high_risk_examples"):
        actions.append("Validar manualmente oportunidades con legal risk alto.")

    coverage_data = get_section_data(audit, "coverage_by_comuna")
    if coverage_data and coverage_data.get("skipped_low_volume_count", 0) > 0:
        actions.append("Aumentar cobertura en comunas con bajo volumen de registros.")

    if not actions:
        actions.append("Mantener monitoreo periódico del benchmark y legal risk.")

    return actions


def load_all_listings(db: Any) -> list[Listing]:
    return list(
        db.execute(
            select(Listing)
            .options(joinedload(Listing.property))
            .order_by(Listing.id)
        )
        .scalars()
        .all()
    )


def serialize_opportunity(opportunity: dict[str, Any]) -> dict[str, Any]:
    return {
        "listing_id": opportunity.get("listing_id"),
        "comuna": opportunity.get("comuna"),
        "precio": first_not_none(
            opportunity.get("listing_price"),
            opportunity.get("precio_publicado"),
        ),
        "valor_estimado": first_not_none(
            opportunity.get("valor_estimado"),
            opportunity.get("estimated_value"),
            opportunity.get("market_value"),
        ),
        "descuento_porcentual": get_discount_pct(opportunity),
        "confianza": get_confidence_pct(opportunity),
        "numero_comparables": get_comparable_count(opportunity),
        "score_promedio_comparables": opportunity.get("score_promedio_comparables"),
        "porcentaje_campos_faltantes": opportunity.get("porcentaje_campos_faltantes"),
        "penalizacion_total": opportunity.get("penalizacion_total"),
        "investment_score": opportunity.get("investment_score"),
        "valuation_status": opportunity.get("valuation_status"),
        "legal_profile": opportunity.get("legal_profile"),
        "url": opportunity.get("url") or opportunity.get("link"),
    }


def finding(
    severity: str,
    code: str,
    message: str,
    *,
    listing_id: Any = None,
    section: str | None = None,
    details: Any = None,
) -> dict[str, Any]:
    return {
        "severity": severity,
        "code": code,
        "message": message,
        "listing_id": listing_id,
        "section": section,
        "details": details,
    }


def is_strong_opportunity(opportunity: dict[str, Any]) -> bool:
    investment_score = opportunity.get("investment_score")
    discount_pct = get_discount_pct(opportunity)

    if investment_score is not None:
        return investment_score >= 75

    return discount_pct is not None and discount_pct >= 20


def get_confidence_pct(opportunity: dict[str, Any]) -> float | None:
    confidence = first_not_none(
        opportunity.get("confianza"),
        opportunity.get("confidence_score"),
    )

    if confidence is None:
        return None

    return confidence * 100 if confidence <= 1 else confidence


def get_discount_pct(opportunity: dict[str, Any]) -> float | None:
    discount = first_not_none(
        opportunity.get("descuento_porcentual"),
        opportunity.get("discount_pct"),
    )

    if discount is not None:
        return discount

    raw_discount = first_not_none(
        opportunity.get("discount"),
        opportunity.get("undervaluation"),
    )

    if raw_discount is None:
        return None

    return raw_discount * 100 if abs(raw_discount) <= 1 else raw_discount


def get_comparable_count(opportunity: dict[str, Any]) -> int | None:
    return first_not_none(
        opportunity.get("numero_comparables"),
        opportunity.get("comparable_count"),
    )


def first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value

    return None


def count_present(items: list[Any], field_name: str) -> int:
    return sum(1 for item in items if getattr(item, field_name, None) is not None)


def ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0


def none_last(value: Any) -> float:
    if value is None:
        return float("inf")

    return float(value)


def parse_system_logs(log_text: str) -> dict[str, Any]:
    lines = log_text.splitlines()
    comparables_before = [
        int(match.group(1))
        for match in re.finditer(r"Comparables totales:\s*(\d+)", log_text)
    ]
    comparables_segmented = [
        int(match.group(1))
        for match in re.finditer(r"Comparables segmentados:\s*(\d+)", log_text)
    ]
    paired_counts = list(zip(comparables_before, comparables_segmented))
    total_listings_processed = max(len(comparables_before), len(comparables_segmented))
    insufficient_segment_data_count = log_text.count("insufficient_segment_data")
    fallback_events = (
        log_text.count("Fallback dormitorios activado")
        + log_text.count("allowing adjacent segment")
        + log_text.count("[FILTER] Too few comparables, relaxing threshold")
    )
    final_result_count = log_text.count("[INVESTMENT SCORE]")
    lost_all_after_segmentation = [
        {"before": before, "segmented": segmented}
        for before, segmented in paired_counts
        if before > 0 and segmented == 0
    ]
    high_before_zero_after = [
        {"before": before, "segmented": segmented}
        for before, segmented in paired_counts
        if before > 20 and segmented == 0
    ]
    listings_with_lt3_segmented = [
        {"before": before, "segmented": segmented}
        for before, segmented in paired_counts
        if segmented < 3
    ]
    affected_ratio = ratio(len(lost_all_after_segmentation), total_listings_processed)

    return {
        "total_log_lines": len(lines),
        "total_log_characters": len(log_text),
        "insufficient_segment_data_count": insufficient_segment_data_count,
        "insufficient_segment_data_ratio": ratio(
            insufficient_segment_data_count,
            total_listings_processed,
        ),
        "total_listings_processed": total_listings_processed,
        "listings_without_comparables_after_segmentation": len(lost_all_after_segmentation),
        "listings_with_lt3_segmented_comparables": len(listings_with_lt3_segmented),
        "listings_with_gt20_before_and_zero_after": len(high_before_zero_after),
        "lost_all_after_segmentation_ratio": affected_ratio,
        "fallback_events": fallback_events,
        "final_result_count": final_result_count,
        "final_result_ratio": ratio(final_result_count, total_listings_processed),
        "average_comparables_before_segmentation": (
            mean(comparables_before) if comparables_before else None
        ),
        "average_comparables_segmented": (
            mean(comparables_segmented) if comparables_segmented else None
        ),
        "event_counts": {
            "CLUSTER": log_text.count("[CLUSTER]"),
            "WEIGHT": log_text.count("[WEIGHT]"),
            "AGGREGATION": log_text.count("[AGGREGATION]"),
        },
        "comparables_before_samples": comparables_before[:100],
        "comparables_segmented_samples": comparables_segmented[:100],
        "critical_segmentation_patterns": high_before_zero_after[:25],
    }


def build_log_findings(parsed_metrics: dict[str, Any]) -> list[dict[str, Any]]:
    findings = []
    affected_ratio = parsed_metrics.get("lost_all_after_segmentation_ratio") or 0
    insufficient_ratio = parsed_metrics.get("insufficient_segment_data_ratio") or 0
    high_before_zero_after = (
        parsed_metrics.get("listings_with_gt20_before_and_zero_after") or 0
    )
    fallback_events = parsed_metrics.get("fallback_events") or 0
    avg_segmented = parsed_metrics.get("average_comparables_segmented") or 0
    final_result_ratio = parsed_metrics.get("final_result_ratio") or 0
    event_counts = parsed_metrics.get("event_counts") or {}

    if affected_ratio > 0.5:
        findings.append(
            finding(
                "critical",
                "segmentation_loses_comparables_over_50pct",
                "Más de 50% de listings pierde todos los comparables tras segmentación.",
                section="system_logs",
                details={"affected_ratio": affected_ratio},
            )
        )
    elif affected_ratio > 0.3:
        findings.append(
            finding(
                "warning",
                "segmentation_loses_comparables_over_30pct",
                "Más de 30% de listings pierde todos los comparables tras segmentación.",
                section="system_logs",
                details={"affected_ratio": affected_ratio},
            )
        )

    if high_before_zero_after > 0:
        findings.append(
            finding(
                "critical",
                "segmentation_eliminating_valid_data",
                "Segmentación está eliminando datos válidos.",
                section="system_logs",
                details={"count": high_before_zero_after},
            )
        )

    if insufficient_ratio > 0.7:
        findings.append(
            finding(
                "critical",
                "radar_not_functional",
                "Radar no funcional: insufficient_segment_data supera 70% de listings procesados.",
                section="system_logs",
                details={"insufficient_segment_data_ratio": insufficient_ratio},
            )
        )

    if fallback_events >= 3 and (avg_segmented < 3 or insufficient_ratio > 0.3):
        findings.append(
            finding(
                "warning",
                "fallback_not_effective",
                "Fallback no efectivo.",
                section="system_logs",
                details={
                    "fallback_events": fallback_events,
                    "average_comparables_segmented": avg_segmented,
                    "insufficient_segment_data_ratio": insufficient_ratio,
                },
            )
        )

    if (
        (event_counts.get("CLUSTER", 0) > 0 or event_counts.get("WEIGHT", 0) > 0)
        and final_result_ratio < 0.2
    ):
        findings.append(
            finding(
                "warning",
                "pipeline_loses_data_before_scoring",
                "Pipeline pierde datos antes de scoring.",
                section="system_logs",
                details={
                    "event_counts": event_counts,
                    "final_result_ratio": final_result_ratio,
                },
            )
        )

    if (parsed_metrics.get("insufficient_segment_data_count") or 0) > 0:
        findings.append(
            finding(
                "info",
                "insufficient_segment_data_events",
                "Se detectaron eventos insufficient_segment_data en logs del radar.",
                section="system_logs",
                details={
                    "count": parsed_metrics.get("insufficient_segment_data_count"),
                },
            )
        )

    return findings


def get_section_data(
    audit: dict[str, Any],
    section_key: str,
    nested_key: str | None = None,
) -> Any:
    section = audit.get("sections", {}).get(section_key) or {}
    data = section.get("data") or {}

    if nested_key is None:
        return data

    return data.get(nested_key)


def write_json(path: Path, audit: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(audit, ensure_ascii=False, indent=2, default=json_default),
        encoding="utf-8",
    )


def write_markdown(path: Path, audit: dict[str, Any]) -> None:
    lines = [
        "# System Audit Report",
        "",
        f"Generado: {audit['generated_at']}",
        "",
        "## Resumen ejecutivo",
        "",
        f"- Secciones auditadas: {len(audit['sections'])}",
        f"- Críticos: {len(audit['critical'])}",
        f"- Warnings: {len(audit['warnings'])}",
        f"- Info: {len(audit['info'])}",
        f"- Reporte raw: `{RAW_REPORT_PATH.as_posix()}`",
        "",
    ]

    append_findings(lines, "Hallazgos críticos", audit["critical"], "Sin críticos.")
    append_findings(lines, "Warnings", audit["warnings"], "Sin warnings.")
    append_findings(lines, "Info", audit["info"], "Sin hallazgos informativos.")
    append_actions(lines, audit["actions"])

    for section in audit["sections"].values():
        lines.extend(render_section_markdown(section))

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def append_findings(
    lines: list[str],
    title: str,
    items: list[dict[str, Any]],
    empty: str,
) -> None:
    lines.extend([f"## {title}", ""])

    if not items:
        lines.extend([empty, ""])
        return

    for item in items:
        listing = f" listing={item['listing_id']}" if item.get("listing_id") else ""
        lines.append(f"- `{item['code']}`{listing}: {item['message']}")

    lines.append("")


def append_actions(lines: list[str], actions: list[str]) -> None:
    lines.extend(["## Acciones sugeridas", ""])

    for action in actions:
        lines.append(f"- {action}")

    lines.append("")


def render_section_markdown(section: dict[str, Any]) -> list[str]:
    lines = [
        f"## {section['title']}",
        "",
        f"Estado: **{section['status']}**",
        "",
    ]

    if section.get("error"):
        error = section["error"]
        lines.extend(
            [
                f"- Exception: `{error['type']}`",
                f"- Mensaje: {error['message']}",
                "",
                "<details>",
                "<summary>Traceback</summary>",
                "",
                "```text",
                error["traceback"],
                "```",
                "",
                "</details>",
                "",
            ]
        )
        return lines

    lines.extend(render_data_summary(section["key"], section.get("data") or {}))
    lines.append("")
    return lines


def render_data_summary(section_key: str, data: dict[str, Any]) -> list[str]:
    if section_key == "database_health":
        precio_m2 = data.get("precio_m2", {})
        return [
            f"- Properties: {data.get('total_properties', 0)}",
            f"- Listings: {data.get('total_listings', 0)}",
            f"- Active listings: {data.get('total_active_listings', 0)}",
            f"- Inactive listings: {data.get('total_inactive_listings', 0)}",
            f"- Representatives: {data.get('total_representatives', 0)}",
            f"- Conteo por status: {format_mapping(data.get('status_counts', {}))}",
            f"- Duplicados fuente+link: {data.get('duplicates_by_source_link_count', 0)}",
            f"- Duplicados fingerprint: {data.get('duplicates_by_property_fingerprint_count', 0)}",
            f"- Sin precio: {data.get('listings_without_price', 0)}",
            f"- Sin m2: {data.get('listings_without_m2_construidos', 0)}",
            f"- Sin comuna: {data.get('listings_without_comuna', 0)}",
            f"- Sin lat/lon: {data.get('listings_without_lat_lon', 0)}",
            f"- Precio/m2 válido: {precio_m2.get('valid', 0)}",
            f"- Precio/m2 inválido: {precio_m2.get('invalid', 0)}",
        ]

    if section_key == "data_quality":
        lines = [
            f"- Listings revisados: {data.get('total_listings_checked', 0)}",
            f"- Listings usables: {data.get('usable_listings', 0)}",
            f"- Ratio usable: {format_pct(data.get('usable_ratio'))}",
            f"- Top issues: {format_pairs(data.get('top_quality_issues', []))}",
            f"- Outliers precio/m2: {data.get('precio_m2_outliers_count', 0)}",
            "- Cobertura de campos:",
        ]
        for field, metrics in (data.get("field_coverage") or {}).items():
            lines.append(
                f"  - {field}: {metrics.get('count', 0)} "
                f"({format_pct(metrics.get('ratio'))})"
            )
        return lines

    if section_key == "coverage_by_comuna":
        lines = [
            f"- Mínimo de registros por comuna: {data.get('minimum_records')}",
            f"- Comunas incluidas: {data.get('comuna_count', 0)}",
            f"- Comunas omitidas por bajo volumen: {data.get('skipped_low_volume_count', 0)}",
            "- Top comunas:",
        ]
        for item in (data.get("coverage") or [])[:10]:
            lines.append(
                f"  - {item['comuna']}: activos={item['active_listings']}, "
                f"total={item['total_listings']}, "
                f"mediana m2={format_number(item['precio_m2_median'])}, "
                f"geo={format_pct(item['geo_coverage'])}"
            )
        return lines

    if section_key == "valuation_engine":
        return [
            f"- Evaluados: {data.get('evaluated_count')}",
            f"- Saltados: {data.get('skipped_count')}",
            f"- Error promedio: {format_pct(data.get('average_error_pct'), already_pct=True)}",
            f"- Error mediano: {format_pct(data.get('median_error_pct'), already_pct=True)}",
            f"- Mejores 5 grupos: {format_group_list(data.get('best_5_groups', []))}",
            f"- Peores 5 grupos: {format_group_list(data.get('worst_5_groups', []))}",
            f"- Fuente peores casos individuales: {data.get('worst_individual_cases_source')}",
        ]

    if section_key == "radar":
        return [
            f"- Listings listos para radar: {data.get('listings_ready_for_radar')}",
            f"- Oportunidades generadas: {data.get('opportunities_generated')}",
            f"- Score inversión promedio: {format_number(data.get('average_investment_score'))}",
            f"- Descuento promedio: {format_pct(data.get('average_discount_pct'), already_pct=True)}",
        ]

    if section_key == "legal_risk":
        return [
            f"- Opportunities revisadas: {data.get('opportunities_checked')}",
            f"- Distribución score: {format_mapping(data.get('score_distribution', {}))}",
            f"- Conteo niveles: {format_mapping(data.get('level_counts', {}))}",
            f"- Top flags: {format_pairs(data.get('top_flags', []))}",
            f"- Casos altos: {len(data.get('high_risk_examples', []))}",
            f"- Inconsistencias: {len(data.get('inconsistencies', []))}",
        ]

    if section_key == "consistency_checks":
        return [
            f"- Opportunities revisadas: {data.get('checks_run')}",
            f"- Issues detectados: {len(data.get('issues', []))}",
            f"- Por severidad: {format_mapping(data.get('issue_counts_by_severity', {}))}",
            f"- Radar ready count observado: {data.get('radar_ready_count')}",
        ]

    return ["```json", json.dumps(data, ensure_ascii=False, indent=2), "```"]


def scalar_count(db: Any, model: Any) -> int:
    return db.scalar(select(func.count()).select_from(model)) or 0


def load_radar_ready_count() -> int:
    global _RADAR_READY_CACHE

    if _RADAR_READY_CACHE is None:
        ready_count, captured_logs = capture_stdout(get_radar_ready_count)
        _RADAR_READY_CACHE = ready_count
        append_radar_logs(captured_logs)

    return _RADAR_READY_CACHE


def load_radar_opportunities() -> list[dict[str, Any]]:
    global _RADAR_OPPORTUNITIES_CACHE

    if _RADAR_OPPORTUNITIES_CACHE is None:
        opportunities, captured_logs = capture_stdout(
            get_top_opportunities,
            limit=RADAR_LIMIT,
        )
        _RADAR_OPPORTUNITIES_CACHE = opportunities
        append_radar_logs(captured_logs)

    return _RADAR_OPPORTUNITIES_CACHE


def capture_stdout(func: Callable[..., Any], *args: Any, **kwargs: Any) -> tuple[Any, str]:
    buffer = io.StringIO()

    with contextlib.redirect_stdout(buffer):
        result = func(*args, **kwargs)

    return result, buffer.getvalue()


def append_radar_logs(log_text: str) -> None:
    global _RADAR_LOGS_CACHE

    if not log_text:
        return

    if _RADAR_LOGS_CACHE:
        _RADAR_LOGS_CACHE += "\n"

    _RADAR_LOGS_CACHE += log_text


def format_mapping(mapping: dict[str, Any]) -> str:
    if not mapping:
        return "Sin datos"

    return ", ".join(f"{key}: {value}" for key, value in mapping.items())


def format_pairs(pairs: list[Any]) -> str:
    if not pairs:
        return "Sin datos"

    return ", ".join(f"{key}: {value}" for key, value in pairs)


def format_group_list(groups: list[dict[str, Any]]) -> str:
    if not groups:
        return "Sin datos"

    return "; ".join(
        f"{group.get('source')}:{group.get('group')} "
        f"med={format_pct(group.get('median_error_pct'), already_pct=True)}"
        for group in groups
    )


def format_pct(value: Any, already_pct: bool = False) -> str:
    if value is None:
        return "Sin datos"

    if already_pct:
        return f"{value:.1f}%"

    return f"{value * 100:.1f}%"


def format_number(value: Any) -> str:
    if value is None:
        return "Sin datos"

    return f"{value:.1f}"


def json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()

    return str(value)


def write_markdown(path: Path, audit: dict[str, Any]) -> None:
    general_status = get_general_status(audit)
    total_checks = count_total_checks(audit)
    lines = [
        "# System Audit Report",
        "",
        "| Campo | Valor |",
        "| --- | --- |",
        f"| Fecha/hora | {audit['generated_at']} |",
        f"| Estado general | **{general_status}** |",
        f"| Hallazgos críticos | {len(audit['critical'])} |",
        f"| Warnings | {len(audit['warnings'])} |",
        f"| Checks ejecutados | {total_checks} |",
        f"| Reporte raw | `{RAW_REPORT_PATH.as_posix()}` |",
        "",
        "## Resumen ejecutivo",
        "",
    ]

    lines.extend(build_executive_summary(audit))
    lines.append("")

    append_system_health(lines, audit)
    append_findings(lines, "Hallazgos críticos", audit["critical"], "Sin críticos.")
    append_findings(lines, "Warnings", audit["warnings"], "Sin warnings.")
    append_findings(lines, "Infos", audit["info"], "Sin hallazgos informativos.")
    append_actions(lines, audit["actions"])

    for section in audit["sections"].values():
        lines.extend(render_section_markdown(section))

    append_global_diagnosis(lines, audit)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def get_general_status(audit: dict[str, Any]) -> str:
    if audit["critical"]:
        return "CRITICAL"

    if audit["warnings"]:
        return "WARNING"

    return "OK"


def count_total_checks(audit: dict[str, Any]) -> int:
    total = 0

    for section in audit.get("sections", {}).values():
        data = section.get("data") or {}
        if "checks_run" in data:
            total += data.get("checks_run") or 0
        elif "opportunities_checked" in data:
            total += data.get("opportunities_checked") or 0
        elif "total_listings_checked" in data:
            total += data.get("total_listings_checked") or 0
        elif "total_listings" in data:
            total += data.get("total_listings") or 0
        else:
            total += 1

    return total


def build_executive_summary(audit: dict[str, Any]) -> list[str]:
    db_data = get_section_data(audit, "database_health")
    quality_data = get_section_data(audit, "data_quality")
    benchmark_data = get_section_data(audit, "valuation_engine")
    radar_data = get_section_data(audit, "radar")
    legal_data = get_section_data(audit, "legal_risk")
    summary = [
        f"- Estado general: **{get_general_status(audit)}**.",
        (
            "- Salud de datos: "
            f"{db_data.get('total_active_listings', 0)} activos, "
            f"{format_pct(quality_data.get('usable_ratio'))} usables, "
            f"{db_data.get('listings_without_price', 0)} sin precio, "
            f"{db_data.get('listings_without_m2_construidos', 0)} sin m2."
        ),
        (
            "- Benchmark: "
            f"{benchmark_data.get('evaluated_count', 'Sin datos')} evaluados, "
            f"error mediano {format_pct(benchmark_data.get('median_error_pct'), already_pct=True)}."
        ),
        (
            "- Radar: "
            f"{radar_data.get('listings_ready_for_radar', 0)} listings listos, "
            f"{radar_data.get('opportunities_generated', 0)} oportunidades generadas."
        ),
        (
            "- Legal risk: "
            f"{format_mapping(legal_data.get('level_counts', {}))}; "
            f"{len(legal_data.get('inconsistencies', []))} inconsistencias legales."
        ),
    ]

    if audit["critical"]:
        summary.append(
            f"- Principal crítico: `{audit['critical'][0]['code']}` - {audit['critical'][0]['message']}"
        )

    if audit["warnings"]:
        summary.append(
            f"- Principal warning: `{audit['warnings'][0]['code']}` - {audit['warnings'][0]['message']}"
        )

    return summary[:10]


def append_system_health(lines: list[str], audit: dict[str, Any]) -> None:
    health = audit.get("system_health") or {}
    severity = health.get("severity") or {}

    lines.extend(
        [
            "## System Health",
            "",
            f"Status: **{health.get('system_status', 'UNKNOWN')}**  ",
            f"Score: **{health.get('score', 0)} / 100**",
            "",
            f"Critical issues: {len(severity.get('critical', []))}  ",
            f"High issues: {len(severity.get('high', []))}  ",
            f"Medium issues: {len(severity.get('medium', []))}  ",
            f"Low issues: {len(severity.get('low', []))}",
            "",
        ]
    )

    for level, title in (
        ("critical", "Critical"),
        ("high", "High"),
        ("medium", "Medium"),
        ("low", "Low"),
    ):
        items = severity.get(level) or []
        if not items:
            continue

        lines.extend([f"### {title}", ""])
        for item in items[:10]:
            lines.append(f"- {item}")
        if len(items) > 10:
            lines.append(f"- ... {len(items) - 10} adicionales")
        lines.append("")


def append_findings(
    lines: list[str],
    title: str,
    items: list[dict[str, Any]],
    empty: str,
) -> None:
    lines.extend([f"## {title}", ""])

    if not items:
        lines.extend([empty, ""])
        return

    for item in items:
        listing = f" listing={item['listing_id']}" if item.get("listing_id") else ""
        section = f" sección={item['section']}" if item.get("section") else ""
        lines.append(f"- `{item['code']}`{listing}{section}: {item['message']}")

    lines.append("")


def render_section_markdown(section: dict[str, Any]) -> list[str]:
    lines = [
        f"## {section['title']}",
        "",
        f"Estado: **{section['status']}**",
        "",
    ]

    if section.get("error"):
        error = section["error"]
        lines.extend(
            [
                f"- Sección: `{section['title']}`",
                f"- Exception: `{error['type']}`",
                f"- Mensaje: {error['message']}",
                "",
                "<details>",
                "<summary>Stacktrace corto</summary>",
                "",
                "```text",
                short_traceback(error.get("traceback", "")),
                "```",
                "",
                "</details>",
                "",
            ]
        )
        return lines

    lines.extend(render_data_summary(section["key"], section.get("data") or {}))
    lines.append("")
    return lines


def render_data_summary(section_key: str, data: dict[str, Any]) -> list[str]:
    if section_key == "database_health":
        precio_m2 = data.get("precio_m2", {})
        return [
            f"- Properties: {data.get('total_properties', 0)}",
            f"- Listings: {data.get('total_listings', 0)}",
            f"- Active listings: {data.get('total_active_listings', 0)}",
            f"- Inactive listings: {data.get('total_inactive_listings', 0)}",
            f"- Representatives: {data.get('total_representatives', 0)}",
            f"- Conteo por status: {format_mapping(data.get('status_counts', {}))}",
            f"- Duplicados fuente+link: {data.get('duplicates_by_source_link_count', 0)}",
            f"- Duplicados fingerprint: {data.get('duplicates_by_property_fingerprint_count', 0)}",
            f"- Sin precio: {data.get('listings_without_price', 0)}",
            f"- Sin m2: {data.get('listings_without_m2_construidos', 0)}",
            f"- Sin comuna: {data.get('listings_without_comuna', 0)}",
            f"- Sin lat/lon: {data.get('listings_without_lat_lon', 0)}",
            f"- Precio/m2 válido: {precio_m2.get('valid', 0)}",
            f"- Precio/m2 inválido: {precio_m2.get('invalid', 0)}",
        ]

    if section_key == "data_quality":
        lines = [
            f"- Listings revisados: {data.get('total_listings_checked', 0)}",
            f"- Listings usables: {data.get('usable_listings', 0)}",
            f"- Ratio usable: {format_pct(data.get('usable_ratio'))}",
            f"- Top issues: {format_pairs(data.get('top_quality_issues', []))}",
            f"- Outliers precio/m2: {data.get('precio_m2_outliers_count', 0)}",
            "- Cobertura de campos:",
        ]
        for field, metrics in (data.get("field_coverage") or {}).items():
            lines.append(
                f"  - {field}: {metrics.get('count', 0)} "
                f"({format_pct(metrics.get('ratio'))})"
            )
        return lines

    if section_key == "coverage_by_comuna":
        lines = [
            f"- Mínimo de registros por comuna: {data.get('minimum_records')}",
            f"- Comunas incluidas: {data.get('comuna_count', 0)}",
            f"- Comunas omitidas por bajo volumen: {data.get('skipped_low_volume_count', 0)}",
            "",
            "| Comuna | Activos | Total | Mediana precio/m2 | Min precio/m2 | Max precio/m2 | Dorm. | Baños | Estac. | Geo |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
        for item in (data.get("coverage") or [])[:20]:
            lines.append(
                f"| {escape_table(item['comuna'])} "
                f"| {item['active_listings']} "
                f"| {item['total_listings']} "
                f"| {format_number(item['precio_m2_median'])} "
                f"| {format_number(item['precio_m2_min'])} "
                f"| {format_number(item['precio_m2_max'])} "
                f"| {format_pct(item['dormitorios_coverage'])} "
                f"| {format_pct(item['banos_coverage'])} "
                f"| {format_pct(item['estacionamientos_coverage'])} "
                f"| {format_pct(item['geo_coverage'])} |"
            )
        return lines

    if section_key == "valuation_engine":
        lines = [
            f"- Evaluados: {data.get('evaluated_count')}",
            f"- Saltados: {data.get('skipped_count')}",
            f"- Error promedio: {format_pct(data.get('average_error_pct'), already_pct=True)}",
            f"- Error mediano: {format_pct(data.get('median_error_pct'), already_pct=True)}",
            f"- Mejores 5 grupos: {format_group_list(data.get('best_5_groups', []))}",
            f"- Peores 5 grupos: {format_group_list(data.get('worst_5_groups', []))}",
            f"- Fuente peores casos individuales: {data.get('worst_individual_cases_source')}",
            "",
            "| Peor caso | Listing | Comuna | M2 | Precio real | Precio predicho | Error abs. | URL |",
            "| ---: | ---: | --- | ---: | ---: | ---: | ---: | --- |",
        ]
        for index, item in enumerate(data.get("worst_individual_cases", [])[:10], start=1):
            lines.append(
                f"| {index} "
                f"| {item.get('listing_id', '')} "
                f"| {escape_table(item.get('comuna'))} "
                f"| {format_number(item.get('m2_construidos'))} "
                f"| {format_number(item.get('real_price'))} "
                f"| {format_number(item.get('predicted_price'))} "
                f"| {format_pct(item.get('absolute_error_pct'), already_pct=True)} "
                f"| {format_link(item.get('url'))} |"
            )
        return lines

    if section_key == "radar":
        lines = [
            f"- Listings listos para radar: {data.get('listings_ready_for_radar')}",
            f"- Oportunidades generadas: {data.get('opportunities_generated')}",
            f"- Score inversión promedio: {format_number(data.get('average_investment_score'))}",
            f"- Descuento promedio: {format_pct(data.get('average_discount_pct'), already_pct=True)}",
            "",
            "| Rank | Listing | Comuna | Precio | Valor estimado | Descuento | Confianza | Comparables | Inv. score | Legal risk |",
            "| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
        for index, item in enumerate(data.get("top_opportunities", [])[:20], start=1):
            legal = item.get("legal_profile") or {}
            lines.append(
                f"| {index} "
                f"| {item.get('listing_id', '')} "
                f"| {escape_table(item.get('comuna'))} "
                f"| {format_number(item.get('precio'))} "
                f"| {format_number(item.get('valor_estimado'))} "
                f"| {format_pct(item.get('descuento_porcentual'), already_pct=True)} "
                f"| {format_pct(item.get('confianza'), already_pct=True)} "
                f"| {item.get('numero_comparables', '')} "
                f"| {format_number(item.get('investment_score'))} "
                f"| {escape_table(legal.get('legal_risk_level'))} ({format_number(legal.get('legal_risk_score'))}) |"
            )
        return lines

    if section_key == "system_logs":
        metrics = data.get("parsed_metrics") or {}
        event_counts = metrics.get("event_counts") or {}
        findings = data.get("automatic_findings") or []
        lines = [
            f"- Total líneas de log capturadas: {metrics.get('total_log_lines', 0)}",
            f"- Total caracteres capturados: {metrics.get('total_log_characters', 0)}",
            f"- insufficient_segment_data: {metrics.get('insufficient_segment_data_count', 0)}",
            f"- % listings afectados: {format_pct(metrics.get('lost_all_after_segmentation_ratio'))}",
            f"- Promedio comparables antes: {format_number(metrics.get('average_comparables_before_segmentation'))}",
            f"- Promedio comparables segmentados: {format_number(metrics.get('average_comparables_segmented'))}",
            f"- Eventos fallback: {metrics.get('fallback_events', 0)}",
            f"- Resultados finales radar: {metrics.get('final_result_count', 0)}",
            "",
            "| Evento | Conteo |",
            "| --- | ---: |",
            f"| CLUSTER | {event_counts.get('CLUSTER', 0)} |",
            f"| WEIGHT | {event_counts.get('WEIGHT', 0)} |",
            f"| AGGREGATION | {event_counts.get('AGGREGATION', 0)} |",
            "",
            "### Hallazgos automáticos",
            "",
        ]

        if not findings:
            lines.append("- Sin hallazgos automáticos en logs.")
        else:
            for item in findings:
                lines.append(
                    f"- **{item.get('severity', '').upper()}** `{item.get('code')}`: "
                    f"{item.get('message')}"
                )

        return lines

    if section_key == "service_contracts":
        status = "error" if data.get("errors") else "warning" if data.get("warnings") else "ok"
        lines = [
            f"- Estado contrato: {status}",
            f"- Errores: {len(data.get('errors', []))}",
            f"- Warnings: {len(data.get('warnings', []))}",
        ]

        if data.get("errors"):
            lines.extend(["", "### Errores", ""])
            for message in data.get("errors", []):
                lines.append(f"- {message}")

        if data.get("warnings"):
            lines.extend(["", "### Warnings", ""])
            for message in data.get("warnings", []):
                lines.append(f"- {message}")

        return lines

    if section_key == "integration_flows":
        status = "error" if data.get("errors") else "warning" if data.get("warnings") else "ok"
        lines = [
            f"- Estado integración: {status}",
            f"- Errores: {len(data.get('errors', []))}",
            f"- Warnings: {len(data.get('warnings', []))}",
        ]

        if data.get("errors"):
            lines.extend(["", "### Errores", ""])
            for message in data.get("errors", []):
                lines.append(f"- {message}")

        if data.get("warnings"):
            lines.extend(["", "### Warnings", ""])
            for message in data.get("warnings", []):
                lines.append(f"- {message}")

        return lines

    if section_key == "legal_risk":
        level_counts = data.get("level_counts", {})
        score_distribution = data.get("score_distribution", {})
        return [
            f"- Opportunities revisadas: {data.get('opportunities_checked')}",
            f"- Conteo niveles: {format_mapping(level_counts)}",
            f"- Top flags: {format_pairs(data.get('top_flags', []))}",
            f"- Casos altos: {len(data.get('high_risk_examples', []))}",
            f"- Inconsistencias: {len(data.get('inconsistencies', []))}",
            "",
            "| Nivel | Count |",
            "| --- | ---: |",
            f"| Bajo | {level_counts.get('Bajo', 0)} |",
            f"| Medio | {level_counts.get('Medio', 0)} |",
            f"| Alto | {level_counts.get('Alto', 0)} |",
            "",
            "| Score legal | Valor |",
            "| --- | ---: |",
            f"| Count | {score_distribution.get('count', 0)} |",
            f"| Min | {format_number(score_distribution.get('min'))} |",
            f"| Max | {format_number(score_distribution.get('max'))} |",
            f"| Promedio | {format_number(score_distribution.get('average'))} |",
            f"| Mediana | {format_number(score_distribution.get('median'))} |",
        ]

    if section_key == "consistency_checks":
        lines = [
            f"- Opportunities revisadas: {data.get('checks_run')}",
            f"- Issues detectados: {len(data.get('issues', []))}",
            f"- Por severidad: {format_mapping(data.get('issue_counts_by_severity', {}))}",
            f"- Radar ready count observado: {data.get('radar_ready_count')}",
            "",
            "| Severidad | Código | Listing | Mensaje |",
            "| --- | --- | ---: | --- |",
        ]
        for item in data.get("issues", []):
            lines.append(
                f"| {item.get('severity')} "
                f"| `{item.get('code')}` "
                f"| {item.get('listing_id') or ''} "
                f"| {escape_table(item.get('message'))} |"
            )
        return lines

    return ["```json", json.dumps(data, ensure_ascii=False, indent=2), "```"]


def append_global_diagnosis(lines: list[str], audit: dict[str, Any]) -> None:
    lines.extend(["## Diagnóstico global del sistema", ""])
    status = get_general_status(audit)

    if status == "CRITICAL":
        lines.append(
            "El sistema requiere revisión antes de usar sus resultados para decisiones externas. "
            "Hay inconsistencias o fallas críticas que pueden afectar confianza operativa."
        )
    elif status == "WARNING":
        lines.append(
            "El sistema está operativo, pero presenta señales de riesgo que deben monitorearse "
            "antes de escalar uso comercial o automatizar decisiones."
        )
    else:
        lines.append(
            "El sistema no presenta hallazgos críticos ni warnings en esta auditoría. "
            "Mantener monitoreo periódico."
        )

    lines.extend(["", "## Top 5 acciones recomendadas", ""])
    for action in build_top_actions(audit):
        lines.append(f"- {action}")
    lines.append("")


def build_top_actions(audit: dict[str, Any]) -> list[str]:
    actions = []

    if audit["critical"]:
        actions.append("Resolver primero los hallazgos críticos listados arriba.")

    if audit["warnings"]:
        actions.append("Agrupar warnings por código y atacar los de mayor frecuencia.")

    benchmark_data = get_section_data(audit, "valuation_engine")
    if benchmark_data.get("median_error_pct") is not None:
        actions.append("Revisar los peores casos del benchmark y validar patrones por comuna/m2.")

    radar_data = get_section_data(audit, "radar")
    if radar_data.get("opportunities_generated", 0) > 0:
        actions.append("Revisar manualmente las top oportunidades con mayor descuento y baja confianza.")

    legal_data = get_section_data(audit, "legal_risk")
    if legal_data.get("high_risk_examples"):
        actions.append("Priorizar validación legal manual de oportunidades con risk alto.")

    actions.extend(audit.get("actions", []))

    deduped = []
    for action in actions:
        if action not in deduped:
            deduped.append(action)

    return deduped[:5]


def short_traceback(traceback_text: str, max_lines: int = 12) -> str:
    lines = traceback_text.strip().splitlines()
    if len(lines) <= max_lines:
        return traceback_text.strip()

    return "\n".join(lines[-max_lines:])


def escape_table(value: Any) -> str:
    if value is None:
        return ""

    return str(value).replace("|", "\\|").replace("\n", " ")


def format_link(url: Any) -> str:
    if not url:
        return ""

    return f"[link]({url})"


if __name__ == "__main__":
    main()
