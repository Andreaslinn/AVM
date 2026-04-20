from __future__ import annotations

import argparse
import io
from contextlib import redirect_stdout
from statistics import mean, stdev

from evaluation_benchmark import run_benchmark


DEFAULT_RUNS = 10
DEFAULT_SAMPLE_SIZE = 100


def run_benchmark_iterations(runs=DEFAULT_RUNS, sample_size=DEFAULT_SAMPLE_SIZE):
    results = []

    for run_number in range(1, runs + 1):
        print(f"Running benchmark {run_number}/{runs}...")

        with redirect_stdout(io.StringIO()):
            report = run_benchmark(sample_size=sample_size)

        overall = report.get("overall", {})
        average_error = overall.get("average_error_pct")
        median_error = overall.get("median_error_pct")

        if average_error is None or median_error is None:
            print(f"Run {run_number}: skipped, no valid benchmark result")
            continue

        results.append(
            {
                "run": run_number,
                "average_error_pct": average_error,
                "median_error_pct": median_error,
                "evaluated_count": report.get("evaluated_count", 0),
                "skipped_count": report.get("skipped_count", 0),
            }
        )

        print(
            f"Run {run_number}: "
            f"avg={format_pct(average_error)}, "
            f"median={format_pct(median_error)}, "
            f"evaluated={report.get('evaluated_count', 0)}"
        )

    print_benchmark_summary(results, runs)
    return results


def print_benchmark_summary(results, requested_runs):
    print("-" * 40)
    print("BENCHMARK SUMMARY")
    print("-" * 40)
    print(f"Runs: {len(results)}")

    if len(results) != requested_runs:
        print(f"Requested runs: {requested_runs}")

    if not results:
        print("No valid benchmark results.")
        print("-" * 40)
        return

    average_errors = [result["average_error_pct"] for result in results]
    median_errors = [result["median_error_pct"] for result in results]
    best_run = min(results, key=lambda result: result["average_error_pct"])
    worst_run = max(results, key=lambda result: result["average_error_pct"])

    print()
    print(f"Average of avg error: {format_pct(mean(average_errors))}")
    print(f"Average of median error: {format_pct(mean(median_errors))}")
    print()
    print(f"Best run (lowest avg): {format_pct(best_run['average_error_pct'])}")
    print(f"Worst run (highest avg): {format_pct(worst_run['average_error_pct'])}")

    if len(results) > 1:
        print()
        print(f"Std dev avg error: {format_pct(stdev(average_errors))}")
        print(f"Std dev median error: {format_pct(stdev(median_errors))}")

    print("-" * 40)


def format_pct(value):
    if value is None:
        return "N/D"

    return f"{value:.1f}%"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run AVM benchmark multiple times and summarize results."
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=DEFAULT_RUNS,
        help=f"Number of benchmark iterations. Default: {DEFAULT_RUNS}",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help=f"Listings sampled per run. Default: {DEFAULT_SAMPLE_SIZE}",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_benchmark_iterations(runs=args.runs, sample_size=args.sample_size)
