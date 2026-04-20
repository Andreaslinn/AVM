from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv()

from database import DATABASE_URL  # noqa: E402
from models import Listing, PriceHistory, Property  # noqa: E402


BATCH_SIZE = 100


def main() -> None:
    sqlite_engine = create_engine("sqlite:///tasador.db")
    postgres_engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    SQLiteSession = sessionmaker(autocommit=False, autoflush=False, bind=sqlite_engine)
    PostgresSession = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=postgres_engine,
    )

    counters = {
        "migrated_properties": 0,
        "skipped_properties": 0,
        "migrated_listings": 0,
        "skipped_listings": 0,
        "migrated_price_history": 0,
        "skipped_price_history": 0,
        "failed_properties": 0,
        "failed_listings": 0,
        "failed_price_history": 0,
    }

    with SQLiteSession() as sqlite_session, PostgresSession() as postgres_session:
        migrate_properties(sqlite_session, postgres_session, counters)
        migrate_listings(sqlite_session, postgres_session, counters)
        migrate_price_history(sqlite_session, postgres_session, counters)

    print_summary(counters)


def migrate_properties(sqlite_session, postgres_session, counters: dict[str, int]) -> None:
    properties = list(sqlite_session.execute(select(Property).order_by(Property.id)).scalars())

    for index, source in enumerate(properties, start=1):
        try:
            exists = postgres_session.get(Property, source.id)
            if exists:
                counters["skipped_properties"] += 1
                continue

            postgres_session.add(copy_model(source, Property))
            counters["migrated_properties"] += 1

            if index % BATCH_SIZE == 0:
                postgres_session.commit()
        except Exception as exc:
            postgres_session.rollback()
            counters["failed_properties"] += 1
            print(f"[ERROR] Property id={getattr(source, 'id', None)}: {exc}")

    postgres_session.commit()


def migrate_listings(sqlite_session, postgres_session, counters: dict[str, int]) -> None:
    listings = list(sqlite_session.execute(select(Listing).order_by(Listing.id)).scalars())

    for index, source in enumerate(listings, start=1):
        try:
            exists_by_id = postgres_session.get(Listing, source.id)
            exists_by_source_link = None

            if source.fuente is not None and source.link is not None:
                exists_by_source_link = postgres_session.execute(
                    select(Listing).where(
                        Listing.fuente == source.fuente,
                        Listing.link == source.link,
                    )
                ).scalar_one_or_none()

            if exists_by_id or exists_by_source_link:
                counters["skipped_listings"] += 1
                continue

            postgres_session.add(copy_model(source, Listing))
            counters["migrated_listings"] += 1

            if index % BATCH_SIZE == 0:
                postgres_session.commit()
        except Exception as exc:
            postgres_session.rollback()
            counters["failed_listings"] += 1
            print(f"[ERROR] Listing id={getattr(source, 'id', None)}: {exc}")

    postgres_session.commit()


def migrate_price_history(
    sqlite_session,
    postgres_session,
    counters: dict[str, int],
) -> None:
    rows = list(
        sqlite_session.execute(select(PriceHistory).order_by(PriceHistory.id)).scalars()
    )

    for index, source in enumerate(rows, start=1):
        try:
            exists = postgres_session.get(PriceHistory, source.id)
            if exists:
                counters["skipped_price_history"] += 1
                continue

            listing_exists = postgres_session.get(Listing, source.listing_id)
            if not listing_exists:
                counters["skipped_price_history"] += 1
                print(
                    "[SKIP] PriceHistory "
                    f"id={source.id}: listing_id={source.listing_id} no existe en Postgres"
                )
                continue

            postgres_session.add(copy_model(source, PriceHistory))
            counters["migrated_price_history"] += 1

            if index % BATCH_SIZE == 0:
                postgres_session.commit()
        except Exception as exc:
            postgres_session.rollback()
            counters["failed_price_history"] += 1
            print(f"[ERROR] PriceHistory id={getattr(source, 'id', None)}: {exc}")

    postgres_session.commit()


def copy_model(source, model_class):
    values = {
        column.name: getattr(source, column.name)
        for column in model_class.__table__.columns
    }
    return model_class(**values)


def print_summary(counters: dict[str, int]) -> None:
    print()
    print("=" * 72)
    print("SQLite -> Postgres migration summary")
    print("=" * 72)
    print(f"migrated_properties: {counters['migrated_properties']}")
    print(f"skipped_properties: {counters['skipped_properties']}")
    print(f"failed_properties: {counters['failed_properties']}")
    print()
    print(f"migrated_listings: {counters['migrated_listings']}")
    print(f"skipped_listings: {counters['skipped_listings']}")
    print(f"failed_listings: {counters['failed_listings']}")
    print()
    print(f"migrated_price_history: {counters['migrated_price_history']}")
    print(f"skipped_price_history: {counters['skipped_price_history']}")
    print(f"failed_price_history: {counters['failed_price_history']}")
    print("=" * 72)


if __name__ == "__main__":
    main()
