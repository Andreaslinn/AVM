from __future__ import annotations

from statistics import mean, median

from sqlalchemy import func, or_, select

from comparables import obtener_precio_clp
from database import SessionLocal
from deduplication import is_representative_filter
from models import Listing
from radar import estimar_valor_mercado, property_data_from_listing


M2_BUCKETS = {
    "small": "small <50m2",
    "medium": "medium 50-100m2",
    "large": "large >100m2",
}


def run_benchmark(sample_size=100) -> dict:
    """Benchmark AVM accuracy against real listing prices without modifying the DB."""
    with SessionLocal() as db:
        candidates = get_benchmark_sample(db, sample_size=sample_size)
        results = []

        for listing in candidates:
            real_price = obtener_precio_clp(listing)

            if real_price is None or real_price <= 0:
                continue

            property_data = property_data_from_listing(listing)
            valuation = estimar_valor_mercado(
                db,
                listing,
                property_data,
                low_data_mode=False,
            )

            if valuation is None:
                continue

            predicted_price = valuation.get("estimated_price")

            if predicted_price is None or predicted_price <= 0:
                continue

            absolute_error_pct = abs(predicted_price - real_price) / real_price * 100
            signed_error_pct = (predicted_price - real_price) / real_price * 100

            results.append(
                {
                    "listing_id": listing.id,
                    "comuna": listing.comuna,
                    "m2_construidos": listing.m2_construidos,
                    "m2_range": get_m2_range(listing.m2_construidos),
                    "real_price": real_price,
                    "predicted_price": predicted_price,
                    "absolute_error_pct": absolute_error_pct,
                    "signed_error_pct": signed_error_pct,
                    "comparable_count": valuation.get("comparable_count"),
                    "confidence_score": valuation.get("confidence_score"),
                    "url": listing.url or listing.link,
                }
            )

    report = build_benchmark_report(
        results=results,
        sample_size=sample_size,
        candidate_count=len(candidates),
    )
    print_benchmark_report(report)
    return report


def get_benchmark_sample(db, sample_size: int) -> list[Listing]:
    statement = (
        select(Listing)
        .where(
            Listing.status == "active",
            is_representative_filter(Listing),
            Listing.comuna.is_not(None),
            Listing.m2_construidos.is_not(None),
            Listing.m2_construidos > 0,
            or_(
                Listing.precio_clp.is_not(None),
                Listing.precio_uf.is_not(None),
            ),
        )
        .order_by(func.random())
        .limit(sample_size)
    )
    return list(db.execute(statement).scalars().all())


def build_benchmark_report(
    results: list[dict],
    sample_size: int,
    candidate_count: int,
) -> dict:
    overall_errors = [result["absolute_error_pct"] for result in results]
    by_comuna = group_error_metrics(results, "comuna")
    by_m2_range = group_error_metrics(results, "m2_range")

    return {
        "sample_size_requested": sample_size,
        "candidate_count": candidate_count,
        "evaluated_count": len(results),
        "skipped_count": candidate_count - len(results),
        "overall": {
            "average_error_pct": mean(overall_errors) if overall_errors else None,
            "median_error_pct": median(overall_errors) if overall_errors else None,
        },
        "by_comuna": by_comuna,
        "by_m2_range": by_m2_range,
        "best_comuna": pick_group(by_comuna, best=True),
        "worst_comuna": pick_group(by_comuna, best=False),
        "biggest_errors": sorted(
            results,
            key=lambda result: result["absolute_error_pct"],
            reverse=True,
        )[:5],
    }


def group_error_metrics(results: list[dict], group_key: str) -> dict:
    grouped = {}

    for result in results:
        key = result.get(group_key) or "Sin dato"
        grouped.setdefault(key, []).append(result)

    metrics = {}

    for key, group_results in grouped.items():
        errors = [result["absolute_error_pct"] for result in group_results]
        metrics[key] = {
            "count": len(group_results),
            "average_error_pct": mean(errors),
            "median_error_pct": median(errors),
        }

    return metrics


def pick_group(group_metrics: dict, best: bool) -> dict | None:
    if not group_metrics:
        return None

    key, metrics = sorted(
        group_metrics.items(),
        key=lambda item: (item[1]["median_error_pct"], item[1]["average_error_pct"]),
        reverse=not best,
    )[0]

    return {"group": key, **metrics}


def get_m2_range(m2_construidos) -> str:
    if m2_construidos is None:
        return "Sin dato"

    if m2_construidos < 50:
        return M2_BUCKETS["small"]

    if m2_construidos <= 100:
        return M2_BUCKETS["medium"]

    return M2_BUCKETS["large"]


def print_benchmark_report(report: dict) -> None:
    print("=" * 72)
    print("AVM BENCHMARK REPORT")
    print("=" * 72)
    print(f"Sample requested: {report['sample_size_requested']}")
    print(f"Candidate listings sampled: {report['candidate_count']}")
    print(f"Evaluated listings: {report['evaluated_count']}")
    print(f"Skipped listings: {report['skipped_count']}")
    print()

    overall = report["overall"]
    print("Overall model error")
    print(f"- Average absolute error: {format_pct(overall['average_error_pct'])}")
    print(f"- Median absolute error: {format_pct(overall['median_error_pct'])}")
    print()

    print("Error by comuna")
    print_group_metrics(report["by_comuna"])
    print()

    print("Error by m2 range")
    print_group_metrics(report["by_m2_range"])
    print()

    if report["best_comuna"]:
        best = report["best_comuna"]
        print(
            "Best comuna: "
            f"{best['group']} ({format_pct(best['median_error_pct'])} median error, "
            f"n={best['count']})"
        )

    if report["worst_comuna"]:
        worst = report["worst_comuna"]
        print(
            "Worst comuna: "
            f"{worst['group']} ({format_pct(worst['median_error_pct'])} median error, "
            f"n={worst['count']})"
        )
    print()

    print("Top 5 biggest errors")
    for rank, result in enumerate(report["biggest_errors"], start=1):
        print(
            f"{rank}. Listing #{result['listing_id']} | "
            f"{result['comuna']} | {result['m2_construidos']:g} m2 | "
            f"real={format_clp(result['real_price'])} | "
            f"pred={format_clp(result['predicted_price'])} | "
            f"error={format_pct(result['absolute_error_pct'])}"
        )
    print("=" * 72)


def print_group_metrics(group_metrics: dict) -> None:
    for group, metrics in sorted(
        group_metrics.items(),
        key=lambda item: item[1]["median_error_pct"],
    ):
        print(
            f"- {group}: median={format_pct(metrics['median_error_pct'])}, "
            f"avg={format_pct(metrics['average_error_pct'])}, "
            f"n={metrics['count']}"
        )


def format_pct(value) -> str:
    if value is None:
        return "N/D"

    return f"{value:.1f}%"


def format_clp(value) -> str:
    if value is None:
        return "N/D"

    return f"${value:,.0f}".replace(",", ".")


if __name__ == "__main__":
    run_benchmark()
