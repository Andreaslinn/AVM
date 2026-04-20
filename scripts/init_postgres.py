from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import inspect


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv()

from database import DATABASE_URL, engine  # noqa: E402
from models import Base  # noqa: E402


def main() -> None:
    inspector_before = inspect(engine)
    existing_before = set(inspector_before.get_table_names())

    Base.metadata.create_all(bind=engine)

    inspector_after = inspect(engine)
    existing_after = set(inspector_after.get_table_names())

    created_tables = sorted(existing_after - existing_before)
    already_existing_tables = sorted(existing_before & existing_after)

    print(f"DATABASE_URL: {DATABASE_URL}")
    print()
    print("Tablas creadas:")
    if created_tables:
        for table_name in created_tables:
            print(f"- {table_name}")
    else:
        print("- Ninguna")

    print()
    print("Tablas ya existentes:")
    if already_existing_tables:
        for table_name in already_existing_tables:
            print(f"- {table_name}")
    else:
        print("- Ninguna")


if __name__ == "__main__":
    main()
