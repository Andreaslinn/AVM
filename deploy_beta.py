import shutil
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
BETA_DIR = PROJECT_ROOT / "beta"

FILES_TO_COPY = [
    "app.py",
    "audit_runner.py",
    "comparables.py",
    "database.py",
    "data_quality.py",
    "data_sufficiency.py",
    "deduplication.py",
    "evaluation.py",
    "evaluation_benchmark.py",
    "geocoding.py",
    "listing_pipeline.py",
    "micro_location.py",
    "models.py",
    "radar.py",
    "scraper_health.py",
    "scraper_yapo.py",
    "tracking.py",
    "users.json",
    "__init__.py",
]

DIRS_TO_COPY = [
    "services",
]

DB_NAME = "tasador.db"


def copy_file(src, dst):
    if src.exists():
        shutil.copy2(src, dst)


def copy_dir(src, dst):
    if src.exists():
        resolved_dst = dst.resolve()
        resolved_beta = BETA_DIR.resolve()

        if resolved_beta not in resolved_dst.parents and resolved_dst != resolved_beta:
            raise ValueError(f"Refusing to overwrite path outside beta: {resolved_dst}")

        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(
            src,
            dst,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.log", "*.tmp"),
        )


def deploy():
    BETA_DIR.mkdir(exist_ok=True)

    for file_name in FILES_TO_COPY:
        src = PROJECT_ROOT / file_name
        dst = BETA_DIR / file_name
        copy_file(src, dst)

    for dir_name in DIRS_TO_COPY:
        src = PROJECT_ROOT / dir_name
        dst = BETA_DIR / dir_name
        copy_dir(src, dst)

    db_src = PROJECT_ROOT / DB_NAME
    db_dst = BETA_DIR / DB_NAME
    if not db_src.exists():
        print("ERROR: tasador.db no encontrado")
        return
    copy_file(db_src, db_dst)

    print("Beta deployed successfully")


if __name__ == "__main__":
    deploy()
