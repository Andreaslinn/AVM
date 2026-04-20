from __future__ import annotations

import argparse
import random
from statistics import mean, median
from typing import Optional

from sqlalchemy import select

from comparables import (
    M2_RANGE_RATIO,
    MIN_SCORE,
    RELAXED_MIN_SCORE,
    calcular_confianza,
    calcular_mediana_ponderada,
    calcular_percentil_ponderado,
    calcular_score_promedio,
    filtrar_outliers_iqr,
    filtrar_precios_sanos,
    obtener_precio_clp,
    preparar_comparables_validos,
    seleccionar_top_comparables,
    superficie_en_rango_sano,
)
from database import SessionLocal, init_db
from data_sufficiency import get_data_sufficiency, print_low_data_warning
from deduplication import is_representative_filter, mark_duplicate_listings, same_property
from models import Listing


DEFAULT_LIMIT = None
DEFAULT_HOLDOUT_FRACTION = 0.25
DEFAULT_RANDOM_SEED = 42
MIN_EVALUATIONS = 10
STRICT_MIN_REQUIRED_COMPARABLES = 5
RELAXED_MIN_REQUIRED_COMPARABLES = 3
WORST_LIMIT = 10
BEST_LIMIT = 10
STRICT_MODE = "STRICT"
RELAXED_MODE = "RELAXED"


def evaluate_model(
    limit: Optional[int] = DEFAULT_LIMIT,
    holdout_fraction: float = DEFAULT_HOLDOUT_FRACTION,
    random_seed: int = DEFAULT_RANDOM_SEED,
) -> dict:
    """Evaluate AVM performance with a train/test holdout split."""
    init_db()
    mark_duplicate_listings()

    with SessionLocal() as db:
        data_sufficiency = get_data_sufficiency(db)
        print_low_data_warning(data_sufficiency)
        listings = get_evaluation_listings(db, limit=limit)

    train_listings, test_listings = split_train_test(
        listings,
        holdout_fraction=holdout_fraction,
        random_seed=random_seed,
    )
    strict_report = run_holdout_evaluation(
        train_listings=train_listings,
        test_listings=test_listings,
        total_candidates=len(listings),
        random_seed=random_seed,
        holdout_fraction=holdout_fraction,
        mode=STRICT_MODE,
        min_required_comparables=STRICT_MIN_REQUIRED_COMPARABLES,
    )

    relaxed_report = None

    if strict_report["coverage"]["evaluated_listings"] < MIN_EVALUATIONS:
        relaxed_report = run_holdout_evaluation(
            train_listings=train_listings,
            test_listings=test_listings,
            total_candidates=len(listings),
            random_seed=random_seed,
            holdout_fraction=holdout_fraction,
            mode=RELAXED_MODE,
            min_required_comparables=RELAXED_MIN_REQUIRED_COMPARABLES,
        )

    final_report = relaxed_report or strict_report

    return {
        **final_report,
        "evaluation_mode": final_report["mode"],
        "low_data_mode": data_sufficiency["low_data_mode"],
        "data_sufficiency": data_sufficiency,
        "strict_results": strict_report,
        "relaxed_results": relaxed_report,
    }


def run_holdout_evaluation(
    train_listings: list[Listing],
    test_listings: list[Listing],
    total_candidates: int,
    random_seed: int,
    holdout_fraction: float,
    mode: str,
    min_required_comparables: int,
) -> dict:
    """Evaluate one mode against a fixed train/test split."""
    predictions = []
    skipped_low_data = 0
    skipped_invalid = 0

    for test_listing in test_listings:
        prediction = evaluate_listing_holdout(
            test_listing,
            train_listings,
            mode=mode,
            min_required_comparables=min_required_comparables,
        )

        if prediction is None:
            skipped_invalid += 1
            continue

        if prediction.get("skipped_reason") == "low_data":
            skipped_low_data += 1
            continue

        predictions.append(prediction)

    return build_evaluation_report(
        predictions=predictions,
        total_candidates=total_candidates,
        train_count=len(train_listings),
        test_count=len(test_listings),
        skipped_low_data=skipped_low_data,
        skipped_invalid=skipped_invalid,
        random_seed=random_seed,
        holdout_fraction=holdout_fraction,
        mode=mode,
        min_required_comparables=min_required_comparables,
    )


def get_evaluation_listings(db, limit: Optional[int] = None) -> list[Listing]:
    """Use active listings with enough observed data for holdout evaluation."""
    statement = (
        select(Listing)
        .where(
            Listing.status == "active",
            is_representative_filter(Listing),
            Listing.m2_construidos.is_not(None),
            Listing.m2_construidos > 0,
        )
        .where(
            (Listing.precio_clp.is_not(None)) | (Listing.precio_uf.is_not(None))
        )
        .order_by(Listing.id.asc())
    )

    if limit is not None:
        statement = statement.limit(limit)

    return list(db.execute(statement).scalars().all())


def split_train_test(
    listings: list[Listing],
    holdout_fraction: float,
    random_seed: int,
) -> tuple[list[Listing], list[Listing]]:
    """Create a reproducible holdout split."""
    if not listings:
        return [], []

    holdout_fraction = min(max(holdout_fraction, 0.05), 0.50)
    shuffled = sorted(listings, key=lambda listing: listing.id)
    random.Random(random_seed).shuffle(shuffled)
    test_size = max(1, int(round(len(shuffled) * holdout_fraction)))
    test_listings = shuffled[:test_size]
    train_listings = shuffled[test_size:]
    return train_listings, test_listings


def evaluate_listing_holdout(
    test_listing: Listing,
    train_listings: list[Listing],
    mode: str,
    min_required_comparables: int,
) -> Optional[dict]:
    """Estimate one test listing using train listings only."""
    actual_price = obtener_precio_clp(test_listing)

    if actual_price is None or actual_price <= 0:
        return None

    valuation = estimate_from_train_only(
        test_listing,
        train_listings,
        mode=mode,
        min_required_comparables=min_required_comparables,
    )

    if valuation is None:
        return {"skipped_reason": "low_data"}

    estimated_price = valuation["estimated_price"]
    absolute_error = abs(estimated_price - actual_price)
    signed_error = estimated_price - actual_price
    percentage_error = absolute_error / actual_price
    signed_percentage_error = signed_error / actual_price

    return {
        "listing_id": test_listing.id,
        "fuente": test_listing.fuente,
        "titulo": test_listing.titulo,
        "comuna": test_listing.comuna,
        "size_bucket": size_bucket(test_listing.m2_construidos),
        "url": test_listing.url or test_listing.link,
        "actual_price": actual_price,
        "estimated_price": estimated_price,
        "absolute_error": absolute_error,
        "signed_error": signed_error,
        "percentage_error": percentage_error,
        "signed_percentage_error": signed_percentage_error,
        "confidence_score": valuation["confidence_score"],
        "comparable_count": valuation["comparable_count"],
        "min_price": valuation["min_price"],
        "max_price": valuation["max_price"],
        "evaluation_mode": mode,
    }


def estimate_from_train_only(
    test_listing: Listing,
    train_listings: list[Listing],
    mode: str,
    min_required_comparables: int,
) -> Optional[dict]:
    """Run comparable valuation using only TRAIN listings."""
    property_data = property_data_from_listing(test_listing)
    candidates = [
        candidate
        for candidate in train_listings
        if is_valid_train_comparable(
            candidate,
            test_listing,
            property_data,
            mode=mode,
        )
    ]

    comparables_validos = preparar_comparables_validos(
        candidates,
        property_data,
        min_score=MIN_SCORE,
    )

    if len(comparables_validos) < min_required_comparables:
        relaxed = preparar_comparables_validos(
            candidates,
            property_data,
            min_score=RELAXED_MIN_SCORE,
        )

        if len(relaxed) > len(comparables_validos):
            comparables_validos = relaxed

    if len(comparables_validos) < min_required_comparables:
        return None

    comparables_top = seleccionar_top_comparables(comparables_validos)
    comparables_filtrados = seleccionar_top_comparables(
        filtrar_outliers_iqr(filtrar_precios_sanos(comparables_top))
    )

    if len(comparables_filtrados) < min_required_comparables:
        return None

    valores_ponderados = [
        {
            "valor": comparable["precio_m2"],
            "peso": comparable["score"],
        }
        for comparable in comparables_filtrados
    ]
    precio_m2_estimado = calcular_mediana_ponderada(valores_ponderados)

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

    return {
        "estimated_price": estimated_price,
        "min_price": min_price,
        "max_price": max_price,
        "confidence_score": confidence_score,
        "comparable_count": len(comparables_filtrados),
    }


def is_valid_train_comparable(
    candidate: Listing,
    test_listing: Listing,
    property_data: dict,
    mode: str,
) -> bool:
    """Prevent leakage and duplicate comparables from entering the prediction."""
    if candidate.status != "active":
        return False

    if candidate.is_duplicate:
        return False

    if candidate.id == test_listing.id:
        return False

    if has_same_source_identity(candidate, test_listing):
        return False

    if is_probable_duplicate(candidate, test_listing, mode=mode):
        return False

    if property_data.get("comuna") and candidate.comuna != property_data["comuna"]:
        return False

    if not superficie_en_rango_sano(candidate.m2_construidos):
        return False

    m2_objetivo = property_data.get("m2_construidos")

    if not superficie_en_rango_sano(m2_objetivo):
        return False

    min_m2 = m2_objetivo * (1 - M2_RANGE_RATIO)
    max_m2 = m2_objetivo * (1 + M2_RANGE_RATIO)

    if not min_m2 <= candidate.m2_construidos <= max_m2:
        return False

    return obtener_precio_clp(candidate) is not None


def has_same_source_identity(left: Listing, right: Listing) -> bool:
    left_urls = {clean_identity(left.url), clean_identity(left.link)} - {None}
    right_urls = {clean_identity(right.url), clean_identity(right.link)} - {None}

    if left_urls.intersection(right_urls):
        return True

    if left.source_listing_id and right.source_listing_id:
        return left.source_listing_id == right.source_listing_id

    return False


def is_probable_duplicate(left: Listing, right: Listing, mode: str = STRICT_MODE) -> bool:
    return same_property(left, right)


def values_are_close(left, right, tolerance_ratio: float) -> bool:
    if left is None or right is None:
        return False

    left = float(left)
    right = float(right)
    tolerance = max(abs(left), abs(right), 1) * tolerance_ratio
    return abs(left - right) <= tolerance


def titles_are_similar(left: Optional[str], right: Optional[str]) -> bool:
    left = normalize_text(left)
    right = normalize_text(right)

    if not left or not right:
        return False

    return left == right or left in right or right in left


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


def build_evaluation_report(
    predictions: list[dict],
    total_candidates: int,
    train_count: int,
    test_count: int,
    skipped_low_data: int,
    skipped_invalid: int,
    random_seed: int,
    holdout_fraction: float,
    mode: str,
    min_required_comparables: int,
) -> dict:
    """Build global, coverage, and grouped metrics."""
    ranked_worst = sorted(
        predictions,
        key=lambda prediction: prediction["percentage_error"],
        reverse=True,
    )
    ranked_best = sorted(
        predictions,
        key=lambda prediction: prediction["percentage_error"],
    )

    return {
        "mode": mode,
        "global_metrics": calculate_metrics(predictions),
        "coverage": {
            "total_candidates": total_candidates,
            "train_listings": train_count,
            "test_listings": test_count,
            "evaluated_listings": len(predictions),
            "skipped_low_data": skipped_low_data,
            "skipped_invalid": skipped_invalid,
            "holdout_fraction": holdout_fraction,
            "random_seed": random_seed,
            "min_required_comparables": min_required_comparables,
        },
        "by_commune": grouped_metrics(predictions, "comuna"),
        "by_size_bucket": grouped_metrics(predictions, "size_bucket"),
        "worst_predictions": ranked_worst[:WORST_LIMIT],
        "best_predictions": ranked_best[:BEST_LIMIT],
    }


def calculate_metrics(predictions: list[dict]) -> dict:
    if not predictions:
        return empty_metrics()

    absolute_errors = [prediction["absolute_error"] for prediction in predictions]
    percentage_errors = [prediction["percentage_error"] for prediction in predictions]
    comparable_counts = [prediction["comparable_count"] for prediction in predictions]

    return {
        "count": len(predictions),
        "mae": mean(absolute_errors),
        "mape": mean(percentage_errors),
        "median_absolute_error": median(absolute_errors),
        "median_percentage_error": median(percentage_errors),
        "avg_comparable_count": mean(comparable_counts),
        "median_comparable_count": median(comparable_counts),
    }


def empty_metrics() -> dict:
    return {
        "count": 0,
        "mae": None,
        "mape": None,
        "median_absolute_error": None,
        "median_percentage_error": None,
        "avg_comparable_count": None,
        "median_comparable_count": None,
    }


def grouped_metrics(predictions: list[dict], key: str) -> dict:
    groups = {}

    for prediction in predictions:
        group_key = prediction.get(key) or "Sin dato"
        groups.setdefault(group_key, []).append(prediction)

    return {
        group_key: calculate_metrics(group_predictions)
        for group_key, group_predictions in sorted(groups.items())
    }


def size_bucket(m2) -> str:
    if m2 is None:
        return "Sin dato"

    if m2 < 60:
        return "small <60"

    if m2 <= 100:
        return "medium 60-100"

    return "large >100"


def clean_identity(value: Optional[str]) -> Optional[str]:
    if not value:
        return None

    return str(value).strip().rstrip("/").lower()


def normalize_text(value: Optional[str]) -> str:
    return str(value or "").strip().lower()


def format_clp(value) -> str:
    if value is None:
        return "N/A"

    return f"${value:,.0f}".replace(",", ".")


def format_pct(value) -> str:
    if value is None:
        return "N/A"

    return f"{value * 100:.1f}%"


def format_number(value) -> str:
    if value is None:
        return "N/A"

    return f"{value:.1f}"


def print_report(report: dict) -> None:
    print("EVALUATION MODE")
    print(f"- {mode_label(report['evaluation_mode'])}")
    print(f"- LOW DATA MODE: {str(report.get('low_data_mode', False)).upper()}")

    if report["evaluation_mode"] == RELAXED_MODE:
        print("WARNING: Low data environment. Metrics may be unstable.")

    if report.get("low_data_mode"):
        print_low_data_warning(report.get("data_sufficiency"))
        data_sufficiency = report["data_sufficiency"]
        print(
            "- active representative listings: "
            f"{data_sufficiency['total_active_listings']}/"
            f"{data_sufficiency['min_active_listings']}"
        )

    print()
    print_single_report("STRICT RESULTS", report["strict_results"])

    if report.get("relaxed_results") is not None:
        print()
        print_single_report("RELAXED RESULTS", report["relaxed_results"])


def print_single_report(title: str, report: dict) -> None:
    print(title)
    print("GLOBAL METRICS")
    print_metrics(report["global_metrics"])
    print()

    coverage = report["coverage"]
    print("COVERAGE")
    print(f"- total candidates: {coverage['total_candidates']}")
    print(f"- train listings: {coverage['train_listings']}")
    print(f"- test listings: {coverage['test_listings']}")
    print(f"- evaluated listings: {coverage['evaluated_listings']}")
    print(f"- skipped listings (low data): {coverage['skipped_low_data']}")
    print(f"- skipped listings (invalid): {coverage['skipped_invalid']}")
    print(f"- min required comparables: {coverage['min_required_comparables']}")
    print(f"- holdout fraction: {coverage['holdout_fraction']:.0%}")
    print(f"- random seed: {coverage['random_seed']}")
    print()

    print("BREAKDOWN BY COMMUNE")
    print_grouped_metrics(report["by_commune"])
    print()

    print("BREAKDOWN BY SIZE BUCKET")
    print_grouped_metrics(report["by_size_bucket"])
    print()

    print_prediction_table("WORST PREDICTIONS", report["worst_predictions"])
    print()
    print_prediction_table("BEST PREDICTIONS", report["best_predictions"])


def mode_label(mode: str) -> str:
    if mode == STRICT_MODE:
        return "STRICT (no leakage, high confidence)"

    return "RELAXED (low data fallback)"


def print_metrics(metrics: dict) -> None:
    print(f"- sample size: {metrics['count']}")
    print(f"- MAE: {format_clp(metrics['mae'])}")
    print(f"- MAPE: {format_pct(metrics['mape'])}")
    print(f"- Median Absolute Error: {format_clp(metrics['median_absolute_error'])}")
    print(f"- Median Percentage Error: {format_pct(metrics['median_percentage_error'])}")
    print(f"- Avg comparables per prediction: {format_number(metrics['avg_comparable_count'])}")
    print(f"- Median comparables per prediction: {format_number(metrics['median_comparable_count'])}")


def print_grouped_metrics(grouped: dict) -> None:
    if not grouped:
        print("- no evaluated predictions")
        return

    for group_name, metrics in grouped.items():
        print(
            f"- {group_name}: "
            f"n={metrics['count']}, "
            f"MAE={format_clp(metrics['mae'])}, "
            f"MAPE={format_pct(metrics['mape'])}, "
            f"median_error={format_clp(metrics['median_absolute_error'])}, "
            f"avg_comps={format_number(metrics['avg_comparable_count'])}"
        )


def print_prediction_table(title: str, predictions: list[dict]) -> None:
    print(title)

    if not predictions:
        print("- no predictions available")
        return

    for index, prediction in enumerate(predictions, start=1):
        print(
            f"- {index}. Listing #{prediction['listing_id']} | "
            f"{prediction['comuna'] or 'Sin comuna'} | "
            f"{prediction['size_bucket']} | "
            f"actual={format_clp(prediction['actual_price'])} | "
            f"estimated={format_clp(prediction['estimated_price'])} | "
            f"error={format_clp(prediction['absolute_error'])} "
            f"({format_pct(prediction['percentage_error'])}) | "
            f"comparables={prediction['comparable_count']} | "
            f"confidence={format_pct(prediction['confidence_score'])} | "
            f"{prediction['url'] or ''}"
        )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Holdout evaluation for the comparable-based AVM."
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--holdout", type=float, default=DEFAULT_HOLDOUT_FRACTION)
    parser.add_argument("--seed", type=int, default=DEFAULT_RANDOM_SEED)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print_report(
        evaluate_model(
            limit=args.limit,
            holdout_fraction=args.holdout,
            random_seed=args.seed,
        )
    )
