import argparse
import random
import time
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from sqlalchemy import func, select

from database import SessionLocal, clean_inactive_listings, init_db
from data_sufficiency import get_data_sufficiency
from deduplication import is_representative_filter, mark_duplicate_listings
from models import Listing
from scraper_yapo import scrape_and_save


DEFAULT_COMUNAS = [
    "nunoa",
    "providencia",
    "las-condes",
    "la-reina",
    "santiago",
    "macul",
    "penalolen",
    "vitacura",
]

BASE_SEARCH_URL = (
    "https://www.yapo.cl/bienes-raices-venta-de-propiedades-apartamentos/"
    "region-metropolitana-{comuna}"
)
PAGE_QUERY_PARAM = "o"
DEFAULT_ITERATIONS = 6
DEFAULT_MAX_PAGES = 4
MIN_PAGES = 3
MAX_PAGES = 5
REQUEST_DELAY_SECONDS = (2, 5)
COMUNA_DELAY_SECONDS = (3, 7)
ITERATION_DELAY_SECONDS = (15, 40)
TEST_DELAY_SECONDS = (0.1, 0.3)
DUPLICATE_STOP_RATIO = 0.80
MAX_CONSECUTIVE_FAILED_RUNS = 5


@dataclass
class IterationSummary:
    raw_scraped: int = 0
    valid_listings: int = 0
    inserted: int = 0
    updated: int = 0
    failed_runs: int = 0
    degraded_runs: int = 0
    stopped_pages: int = 0


def build_comuna_url(comuna_slug: str) -> str:
    return BASE_SEARCH_URL.format(comuna=comuna_slug.strip().lower())


def build_page_url(base_url: str, page: int) -> str:
    if page <= 1:
        return base_url

    parsed = urlparse(base_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query[PAGE_QUERY_PARAM] = str(page)
    return urlunparse(parsed._replace(query=urlencode(query)))


def random_sleep(delay_range: tuple[float, float], label: str, enabled: bool = True) -> None:
    delay = random.uniform(*delay_range)
    print(f"Esperando {delay:.1f}s {label}")

    if enabled:
        time.sleep(delay)


def run_scraper(
    iterations: int = DEFAULT_ITERATIONS,
    comunas: list[str] | None = None,
    max_pages: int = DEFAULT_MAX_PAGES,
    test_mode: bool = False,
) -> None:
    init_db()
    comunas = comunas or DEFAULT_COMUNAS
    max_pages = min(max(max_pages, MIN_PAGES), MAX_PAGES)
    consecutive_failures = 0

    for iteration in range(1, iterations + 1):
        print("==============================")
        print(f"ITERATION {iteration}/{iterations}")
        summary = IterationSummary()

        for comuna_index, comuna_slug in enumerate(comunas, start=1):
            if consecutive_failures > MAX_CONSECUTIVE_FAILED_RUNS:
                print("Scraping halted: consecutive failures exceeded threshold")
                print_iteration_summary(iteration, summary)
                return

            scrape_comuna(
                comuna_slug=comuna_slug,
                max_pages=max_pages,
                summary=summary,
                test_mode=test_mode,
                consecutive_failures_ref={"count": consecutive_failures},
            )

            consecutive_failures = getattr(summary, "_consecutive_failures", consecutive_failures)

            if consecutive_failures > MAX_CONSECUTIVE_FAILED_RUNS:
                print("Scraping halted: consecutive failures exceeded threshold")
                print_iteration_summary(iteration, summary)
                return

            if comuna_index < len(comunas):
                random_sleep(
                    TEST_DELAY_SECONDS if test_mode else COMUNA_DELAY_SECONDS,
                    "antes de la siguiente comuna",
                    enabled=not test_mode,
                )

        inactive_count = clean_inactive_listings()
        duplicate_result = mark_duplicate_listings()
        data_sufficiency = get_current_data_sufficiency()
        print_iteration_summary(
            iteration,
            summary,
            inactive_count=inactive_count,
            duplicate_result=duplicate_result,
            data_sufficiency=data_sufficiency,
        )

        if iteration < iterations:
            random_sleep(
                TEST_DELAY_SECONDS if test_mode else ITERATION_DELAY_SECONDS,
                "antes de la siguiente iteracion",
                enabled=not test_mode,
            )

    print("Scraper finalizado correctamente")


def scrape_comuna(
    comuna_slug: str,
    max_pages: int,
    summary: IterationSummary,
    test_mode: bool,
    consecutive_failures_ref: dict,
) -> None:
    base_url = build_comuna_url(comuna_slug)
    print(f"COMUNA: {comuna_slug}")

    for page in range(1, max_pages + 1):
        url = build_page_url(base_url, page)
        print(f"Scrapeando pagina {page}/{max_pages}: {url}")

        try:
            run_result = scrape_and_save(url)
        except Exception as error:
            print(f"Error scrapeando {url}: {error}")
            summary.failed_runs += 1
            consecutive_failures_ref["count"] += 1
            summary._consecutive_failures = consecutive_failures_ref["count"]
            log_consecutive_failures(consecutive_failures_ref["count"])

            if consecutive_failures_ref["count"] > MAX_CONSECUTIVE_FAILED_RUNS:
                return

            continue

        update_summary_from_run(summary, run_result)

        run_status = get_run_status(run_result)

        if run_status == "failed":
            print(f"RUN FALLIDO: {url}")
            consecutive_failures_ref["count"] += 1
            summary._consecutive_failures = consecutive_failures_ref["count"]
            log_consecutive_failures(consecutive_failures_ref["count"])

            if consecutive_failures_ref["count"] > MAX_CONSECUTIVE_FAILED_RUNS:
                return

            continue

        if run_status == "partial":
            print(f"RUN PARCIAL/DEGRADADO: {url}")

        consecutive_failures_ref["count"] = 0
        summary._consecutive_failures = 0

        if should_stop_comuna_page(run_result):
            summary.stopped_pages += 1
            break

        if page < max_pages:
            random_sleep(
                TEST_DELAY_SECONDS if test_mode else REQUEST_DELAY_SECONDS,
                "antes de la siguiente pagina",
                enabled=not test_mode,
            )


def log_consecutive_failures(count: int) -> None:
    action = (
        "se omite la pagina y se continua"
        if count <= MAX_CONSECUTIVE_FAILED_RUNS
        else "se excedio el umbral; se detendra el scraper"
    )
    print(
        "Fallas consecutivas: "
        f"{count}/{MAX_CONSECUTIVE_FAILED_RUNS} "
        f"({action})"
    )


def update_summary_from_run(summary: IterationSummary, run_result: dict) -> None:
    summary.raw_scraped += int(run_result.get("raw_rows") or 0)
    summary.valid_listings += int(run_result.get("valid_rows") or 0)
    summary.inserted += int(
        run_result.get("inserted_count", run_result.get("saved")) or 0
    )
    summary.updated += int(
        run_result.get("updated_count", run_result.get("updated")) or 0
    )

    run_status = get_run_status(run_result)

    if run_status == "failed":
        summary.failed_runs += 1

    if run_status == "partial":
        summary.degraded_runs += 1


def get_run_status(run_result: dict) -> str:
    run_status = run_result.get("run_status")

    if run_status in {"success", "partial", "failed"}:
        return run_status

    valid_rows = int(run_result.get("valid_rows") or 0)

    if valid_rows == 0:
        return "failed"

    if run_result.get("status") in {"FAILED", "DEGRADED"}:
        return "partial"

    return "success"


def should_stop_comuna_page(run_result: dict) -> bool:
    valid_rows = int(run_result.get("valid_rows") or 0)
    inserted = int(run_result.get("inserted_count", run_result.get("saved")) or 0)
    updated = int(run_result.get("updated_count", run_result.get("updated")) or 0)
    touched_rows = inserted + updated

    if valid_rows == 0:
        print("Stop temprano: pagina sin listings validos")
        return True

    if touched_rows > 0 and updated / touched_rows > DUPLICATE_STOP_RATIO:
        print("Stop temprano: duplicados repetidos sobre 80%")
        return True

    return False


def get_current_data_sufficiency() -> dict:
    with SessionLocal() as db:
        return get_data_sufficiency(db)


def count_active_listings() -> int:
    with SessionLocal() as db:
        return int(
            db.execute(
                select(func.count(Listing.id)).where(Listing.status == "active")
            ).scalar()
            or 0
        )


def count_active_representatives() -> int:
    with SessionLocal() as db:
        return int(
            db.execute(
                select(func.count(Listing.id)).where(
                    Listing.status == "active",
                    is_representative_filter(Listing),
                )
            ).scalar()
            or 0
        )


def print_iteration_summary(
    iteration: int,
    summary: IterationSummary,
    inactive_count: int = 0,
    duplicate_result: dict | None = None,
    data_sufficiency: dict | None = None,
) -> None:
    duplicate_result = duplicate_result or {"duplicates": 0, "groups": 0}
    active_listings = count_active_listings()
    active_representatives = count_active_representatives()
    low_data_mode = bool(data_sufficiency and data_sufficiency.get("low_data_mode"))

    print("------------------------------")
    print(f"ITERATION {iteration} SUMMARY")
    print(f"- raw scraped: {summary.raw_scraped}")
    print(f"- valid listings: {summary.valid_listings}")
    print(f"- inserted: {summary.inserted}")
    print(f"- updated: {summary.updated}")
    print(f"- failed runs: {summary.failed_runs}")
    print(f"- degraded runs: {summary.degraded_runs}")
    print(f"- early page stops: {summary.stopped_pages}")
    print(f"- inactive marked: {inactive_count}")
    print(
        "- property duplicates: "
        f"{duplicate_result.get('duplicates', 0)} in "
        f"{duplicate_result.get('groups', 0)} groups"
    )
    print(f"- total active listings (DB): {active_listings}")
    print(f"- active representatives: {active_representatives}")
    print(f"- LOW DATA MODE: {str(low_data_mode).upper()}")


def parse_comunas(value: str | None) -> list[str] | None:
    if not value:
        return None

    return [
        comuna.strip().lower()
        for comuna in value.split(",")
        if comuna.strip()
    ]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Runner multi-comuna para poblar la base con listings de Yapo."
    )
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    parser.add_argument("--pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument(
        "--comunas",
        type=str,
        default=None,
        help="Lista separada por comas. Ej: nunoa,providencia,macul",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Ejecuta 2 iteraciones con pausas simuladas para validar el runner.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_scraper(
        iterations=2 if args.test else args.iterations,
        comunas=parse_comunas(args.comunas),
        max_pages=args.pages,
        test_mode=args.test,
    )
