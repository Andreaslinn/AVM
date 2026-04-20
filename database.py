import os
import re
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "tasador.db")
DEMO_MODE = os.getenv("DEMO_MODE", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

if DEMO_MODE:
    os.environ.setdefault("DISABLE_SCRAPING", "1")


def _sqlite_database_url(db_path: str) -> str:
    if DEMO_MODE:
        normalized_path = Path(db_path).as_posix()
        return f"sqlite:///file:{normalized_path}?mode=ro&uri=true"

    return f"sqlite:///{db_path}"


DATABASE_URL = (
    _sqlite_database_url(DB_PATH)
    if DEMO_MODE
    else os.environ.get("DATABASE_URL") or _sqlite_database_url(DB_PATH)
)
MIN_VALID_M2_CONSTRUIDOS = 10
MAX_VALID_M2_CONSTRUIDOS = 1000
MIN_VALID_UF_M2 = 5
MAX_VALID_UF_M2 = 300
UF_TO_CLP = 37_000

if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
    )
else:
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


PROPERTY_COLUMNS = {
    "id": "INTEGER PRIMARY KEY",
    "comuna": "VARCHAR",
    "lat": "FLOAT",
    "lon": "FLOAT",
    "m2_construidos": "FLOAT",
    "m2_terreno": "FLOAT",
    "m2_util": "FLOAT",
    "m2_total": "FLOAT",
    "dormitorios": "INTEGER",
    "banos": "INTEGER",
    "estacionamientos": "INTEGER",
    "piscina": "BOOLEAN",
    "ano_construccion": "INTEGER",
}

LISTING_COLUMNS = {
    "id": "INTEGER PRIMARY KEY",
    "property_id": "INTEGER",
    "fuente": "VARCHAR NOT NULL",
    "source_listing_id": "VARCHAR",
    "url": "VARCHAR",
    "link": "VARCHAR",
    "status": "VARCHAR NOT NULL DEFAULT 'active'",
    "titulo": "VARCHAR",
    "custom_name": "TEXT",
    "comuna": "VARCHAR",
    "lat": "FLOAT",
    "lon": "FLOAT",
    "precio_clp": "INTEGER",
    "precio_uf": "FLOAT",
    "m2_construidos": "FLOAT",
    "m2_terreno": "FLOAT",
    "m2_util": "FLOAT",
    "m2_total": "FLOAT",
    "dormitorios": "INTEGER",
    "banos": "INTEGER",
    "estacionamientos": "INTEGER",
    "fecha_publicacion": "DATE",
    "fecha_captura": "DATE NOT NULL",
    "last_seen": "DATETIME",
    "property_fingerprint": "VARCHAR",
    "is_duplicate": "BOOLEAN NOT NULL DEFAULT 0",
    "duplicate_group_id": "VARCHAR",
}

PRICE_HISTORY_COLUMNS = {
    "id": "INTEGER PRIMARY KEY",
    "listing_id": "INTEGER NOT NULL",
    "precio_clp": "INTEGER",
    "precio_uf": "FLOAT",
    "precio_clp_anterior": "INTEGER",
    "precio_uf_anterior": "FLOAT",
    "precio_clp_nuevo": "INTEGER",
    "precio_uf_nuevo": "FLOAT",
    "fecha_captura": "DATE NOT NULL",
    "fecha_cambio": "DATETIME NOT NULL",
}


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    if DEMO_MODE:
        return

    import models

    Base.metadata.create_all(bind=engine)
    migrate_sqlite_schema()
    Base.metadata.create_all(bind=engine)


def migrate_sqlite_schema():
    with engine.begin() as connection:
        migrate_properties_table(connection)
        migrate_listings_table(connection)
        migrate_price_history_table(connection)
        backfill_listing_links(connection)
        deduplicate_listing_links(connection)
        backfill_listing_snapshot_columns(connection)
        cleanup_zero_sentinel_values(connection)
        mark_invalid_active_listings(connection)
        backfill_initial_price_history(connection)
        create_indexes(connection)


def migrate_properties_table(connection):
    if not table_exists(connection, "properties"):
        return

    columns = table_columns(connection, "properties")
    must_rebuild = has_required_columns(columns, PROPERTY_COLUMNS) or any(
        columns[column_name]["notnull"]
        for column_name in columns
        if column_name != "id"
    )

    if must_rebuild:
        rebuild_table(
            connection,
            table_name="properties",
            columns=PROPERTY_COLUMNS,
            copy_expressions={
                "id": "id",
                "comuna": "NULLIF(comuna, 'Desconocida')",
                "lat": "lat",
                "lon": "lon",
                "m2_construidos": "NULLIF(m2_construidos, 0)",
                "m2_terreno": "NULLIF(m2_terreno, 0)",
                "m2_util": "NULLIF(m2_util, 0)",
                "m2_total": "NULLIF(m2_total, 0)",
                "dormitorios": "NULLIF(dormitorios, 0)",
                "banos": "NULLIF(banos, 0)",
                "estacionamientos": "NULLIF(estacionamientos, 0)",
                "piscina": "piscina",
                "ano_construccion": "NULLIF(ano_construccion, 0)",
            },
        )


def migrate_listings_table(connection):
    if not table_exists(connection, "listings"):
        return

    columns = table_columns(connection, "listings")
    must_rebuild = has_required_columns(columns, LISTING_COLUMNS)
    url_expression = "COALESCE(url, link)" if "link" in columns else "url"
    link_expression = "COALESCE(link, url)" if "link" in columns else "url"
    last_seen_expression = (
        "COALESCE(last_seen, fecha_captura, DATETIME('now'))"
        if "last_seen" in columns
        else "COALESCE(fecha_captura, DATETIME('now'))"
    )
    property_fingerprint_expression = (
        "property_fingerprint" if "property_fingerprint" in columns else "NULL"
    )
    is_duplicate_expression = (
        "COALESCE(is_duplicate, 0)" if "is_duplicate" in columns else "0"
    )
    duplicate_group_id_expression = (
        "duplicate_group_id" if "duplicate_group_id" in columns else "NULL"
    )

    for nullable_column in ("property_id", "precio_clp", "precio_uf"):
        if nullable_column in columns and columns[nullable_column]["notnull"]:
            must_rebuild = True

    if "status" in columns and columns["status"]["notnull"] == 0:
        must_rebuild = True

    if must_rebuild:
        rebuild_table(
            connection,
            table_name="listings",
            columns=LISTING_COLUMNS,
            copy_expressions={
                "id": "id",
                "property_id": "property_id",
                "fuente": "COALESCE(fuente, 'desconocida')",
                "source_listing_id": "source_listing_id",
                "url": url_expression,
                "link": link_expression,
                "status": "COALESCE(status, 'active')",
                "titulo": "titulo",
                "custom_name": "custom_name",
                "comuna": "NULLIF(comuna, 'Desconocida')",
                "lat": "lat",
                "lon": "lon",
                "precio_clp": "NULLIF(precio_clp, 0)",
                "precio_uf": "NULLIF(precio_uf, 0)",
                "m2_construidos": "NULLIF(m2_construidos, 0)",
                "m2_terreno": "NULLIF(m2_terreno, 0)",
                "m2_util": "NULLIF(m2_util, 0)",
                "m2_total": "NULLIF(m2_total, 0)",
                "dormitorios": "NULLIF(dormitorios, 0)",
                "banos": "NULLIF(banos, 0)",
                "estacionamientos": "NULLIF(estacionamientos, 0)",
                "fecha_publicacion": "fecha_publicacion",
                "fecha_captura": "COALESCE(fecha_captura, DATE('now'))",
                "last_seen": last_seen_expression,
                "property_fingerprint": property_fingerprint_expression,
                "is_duplicate": is_duplicate_expression,
                "duplicate_group_id": duplicate_group_id_expression,
            },
        )


def migrate_price_history_table(connection):
    if not table_exists(connection, "price_history"):
        return

    columns = table_columns(connection, "price_history")
    must_rebuild = has_required_columns(columns, PRICE_HISTORY_COLUMNS)
    precio_clp_nuevo_expression = (
        "NULLIF(COALESCE(precio_clp_nuevo, precio_clp), 0)"
        if "precio_clp_nuevo" in columns
        else "NULLIF(precio_clp, 0)"
    )
    precio_uf_nuevo_expression = (
        "NULLIF(COALESCE(precio_uf_nuevo, precio_uf), 0)"
        if "precio_uf_nuevo" in columns
        else "NULLIF(precio_uf, 0)"
    )
    fecha_cambio_expression = (
        "COALESCE(fecha_cambio, fecha_captura, DATETIME('now'))"
        if "fecha_cambio" in columns
        else "COALESCE(fecha_captura, DATETIME('now'))"
    )

    if "precio_clp" in columns and columns["precio_clp"]["notnull"]:
        must_rebuild = True

    if must_rebuild:
        rebuild_table(
            connection,
            table_name="price_history",
            columns=PRICE_HISTORY_COLUMNS,
            copy_expressions={
                "id": "id",
                "listing_id": "listing_id",
                "precio_clp": "NULLIF(precio_clp, 0)",
                "precio_uf": "NULLIF(precio_uf, 0)",
                "precio_clp_anterior": "NULLIF(precio_clp_anterior, 0)",
                "precio_uf_anterior": "NULLIF(precio_uf_anterior, 0)",
                "precio_clp_nuevo": precio_clp_nuevo_expression,
                "precio_uf_nuevo": precio_uf_nuevo_expression,
                "fecha_captura": "COALESCE(fecha_captura, DATE('now'))",
                "fecha_cambio": fecha_cambio_expression,
            },
        )


def rebuild_table(connection, table_name, columns, copy_expressions):
    temp_table = f"{table_name}_new"
    connection.exec_driver_sql(f"DROP TABLE IF EXISTS {temp_table}")
    connection.exec_driver_sql(
        f"CREATE TABLE {temp_table} ({format_column_definitions(columns)})"
    )

    source_columns = table_columns(connection, table_name)
    insert_columns = []
    select_expressions = []

    for column_name in columns:
        expression = copy_expressions.get(column_name)

        if expression is None:
            continue

        if not expression_can_be_used(expression, source_columns):
            expression = default_expression_for(columns[column_name])

        insert_columns.append(column_name)
        select_expressions.append(expression)

    connection.exec_driver_sql(
        f"""
        INSERT INTO {temp_table} ({", ".join(insert_columns)})
        SELECT {", ".join(select_expressions)}
        FROM {table_name}
        """
    )
    connection.exec_driver_sql(f"DROP TABLE {table_name}")
    connection.exec_driver_sql(f"ALTER TABLE {temp_table} RENAME TO {table_name}")


def backfill_listing_snapshot_columns(connection):
    if not table_exists(connection, "listings") or not table_exists(connection, "properties"):
        return

    connection.exec_driver_sql(
        """
        UPDATE listings
        SET
            comuna = COALESCE(listings.comuna, (
                SELECT properties.comuna
                FROM properties
                WHERE properties.id = listings.property_id
            )),
            m2_construidos = COALESCE(listings.m2_construidos, (
                SELECT properties.m2_construidos
                FROM properties
                WHERE properties.id = listings.property_id
            )),
            m2_terreno = COALESCE(listings.m2_terreno, (
                SELECT properties.m2_terreno
                FROM properties
                WHERE properties.id = listings.property_id
            )),
            dormitorios = COALESCE(listings.dormitorios, (
                SELECT properties.dormitorios
                FROM properties
                WHERE properties.id = listings.property_id
            )),
            banos = COALESCE(listings.banos, (
                SELECT properties.banos
                FROM properties
                WHERE properties.id = listings.property_id
            )),
            estacionamientos = COALESCE(listings.estacionamientos, (
                SELECT properties.estacionamientos
                FROM properties
                WHERE properties.id = listings.property_id
            ))
        WHERE property_id IS NOT NULL
        """
    )


def cleanup_zero_sentinel_values(connection):
    cleanup_map = {
        "properties": [
            "m2_construidos",
            "m2_terreno",
            "m2_util",
            "m2_total",
            "dormitorios",
            "banos",
            "estacionamientos",
            "ano_construccion",
        ],
        "listings": [
            "precio_clp",
            "precio_uf",
            "m2_construidos",
            "m2_terreno",
            "m2_util",
            "m2_total",
            "dormitorios",
            "banos",
            "estacionamientos",
        ],
        "price_history": [
            "precio_clp",
            "precio_uf",
            "precio_clp_anterior",
            "precio_uf_anterior",
            "precio_clp_nuevo",
            "precio_uf_nuevo",
        ],
    }

    for table_name, column_names in cleanup_map.items():
        if not table_exists(connection, table_name):
            continue

        existing_columns = table_columns(connection, table_name)
        assignments = [
            f"{column_name} = NULLIF({column_name}, 0)"
            for column_name in column_names
            if column_name in existing_columns
        ]

        if not assignments:
            continue

        conditions = [
            f"{column_name} = 0"
            for column_name in column_names
            if column_name in existing_columns
        ]
        connection.exec_driver_sql(
            f"""
            UPDATE {table_name}
            SET {", ".join(assignments)}
            WHERE {" OR ".join(conditions)}
            """
        )


def mark_invalid_active_listings(connection):
    if not table_exists(connection, "listings"):
        return

    columns = table_columns(connection, "listings")
    required_columns = {
        "status",
        "m2_construidos",
        "precio_clp",
        "precio_uf",
    }

    if not required_columns.issubset(columns):
        return

    connection.exec_driver_sql(
        f"""
        UPDATE listings
        SET status = 'invalid_data'
        WHERE status = 'active'
        AND (
            m2_construidos IS NULL
            OR m2_construidos < {MIN_VALID_M2_CONSTRUIDOS}
            OR m2_construidos > {MAX_VALID_M2_CONSTRUIDOS}
            OR NOT (
                (
                    precio_clp IS NOT NULL
                    AND precio_clp > 0
                    AND (precio_clp / m2_construidos / {UF_TO_CLP})
                        BETWEEN {MIN_VALID_UF_M2} AND {MAX_VALID_UF_M2}
                )
                OR (
                    precio_uf IS NOT NULL
                    AND precio_uf > 0
                    AND (precio_uf / m2_construidos)
                        BETWEEN {MIN_VALID_UF_M2} AND {MAX_VALID_UF_M2}
                )
            )
        )
        """
    )


def clean_inactive_listings(days_threshold=7):
    cutoff = datetime.now() - timedelta(days=days_threshold)

    with engine.begin() as connection:
        if not table_exists(connection, "listings"):
            return 0

        columns = table_columns(connection, "listings")

        if "last_seen" not in columns or "status" not in columns:
            return 0

        result = connection.exec_driver_sql(
            """
            UPDATE listings
            SET status = 'inactive'
            WHERE status = 'active'
            AND (
                last_seen IS NULL
                OR last_seen < ?
            )
            """,
            (cutoff,),
        )
        return result.rowcount or 0


def backfill_listing_links(connection):
    if not table_exists(connection, "listings"):
        return

    columns = table_columns(connection, "listings")

    if "link" not in columns or "url" not in columns:
        return

    connection.exec_driver_sql(
        """
        UPDATE listings
        SET
            link = COALESCE(NULLIF(link, ''), NULLIF(url, '')),
            url = COALESCE(NULLIF(url, ''), NULLIF(link, ''))
        WHERE link IS NULL
        OR link = ''
        OR url IS NULL
        OR url = ''
        """
    )


def deduplicate_listing_links(connection):
    if not table_exists(connection, "listings"):
        return

    columns = table_columns(connection, "listings")

    if "link" not in columns:
        return

    duplicate_rows = connection.exec_driver_sql(
        """
        SELECT fuente, link, MIN(id) AS keeper_id
        FROM listings
        WHERE link IS NOT NULL
        AND link != ''
        GROUP BY fuente, link
        HAVING COUNT(*) > 1
        """
    ).fetchall()

    for fuente, link, keeper_id in duplicate_rows:
        duplicate_ids = [
            row[0]
            for row in connection.exec_driver_sql(
                """
                SELECT id
                FROM listings
                WHERE fuente = ?
                AND link = ?
                AND id != ?
                """,
                (fuente, link, keeper_id),
            ).fetchall()
        ]

        for duplicate_id in duplicate_ids:
            connection.exec_driver_sql(
                """
                UPDATE price_history
                SET listing_id = ?
                WHERE listing_id = ?
                """,
                (keeper_id, duplicate_id),
            )
            connection.exec_driver_sql(
                """
                DELETE FROM listings
                WHERE id = ?
                """,
                (duplicate_id,),
            )


def backfill_initial_price_history(connection):
    if not table_exists(connection, "listings") or not table_exists(connection, "price_history"):
        return

    connection.exec_driver_sql(
        """
        INSERT INTO price_history (
            listing_id,
            precio_clp,
            precio_uf,
            precio_clp_anterior,
            precio_uf_anterior,
            precio_clp_nuevo,
            precio_uf_nuevo,
            fecha_captura,
            fecha_cambio
        )
        SELECT
            listings.id,
            listings.precio_clp,
            listings.precio_uf,
            NULL,
            NULL,
            listings.precio_clp,
            listings.precio_uf,
            listings.fecha_captura,
            COALESCE(listings.fecha_captura, DATETIME('now'))
        FROM listings
        WHERE (listings.precio_clp IS NOT NULL OR listings.precio_uf IS NOT NULL)
        AND NOT EXISTS (
            SELECT 1
            FROM price_history
            WHERE price_history.listing_id = listings.id
        )
        """
    )


def create_indexes(connection):
    index_statements = [
        "CREATE INDEX IF NOT EXISTS ix_properties_comuna ON properties (comuna)",
        "CREATE INDEX IF NOT EXISTS ix_listings_property_id ON listings (property_id)",
        "CREATE INDEX IF NOT EXISTS ix_listings_fuente ON listings (fuente)",
        "CREATE INDEX IF NOT EXISTS ix_listings_source_listing_id ON listings (source_listing_id)",
        "CREATE INDEX IF NOT EXISTS ix_listings_url ON listings (url)",
        "CREATE INDEX IF NOT EXISTS ix_listings_link ON listings (link)",
        "CREATE INDEX IF NOT EXISTS ix_listings_status ON listings (status)",
        "CREATE INDEX IF NOT EXISTS ix_listings_last_seen ON listings (last_seen)",
        "CREATE INDEX IF NOT EXISTS ix_listings_comuna ON listings (comuna)",
        "CREATE INDEX IF NOT EXISTS ix_listings_property_fingerprint ON listings (property_fingerprint)",
        "CREATE INDEX IF NOT EXISTS ix_listings_is_duplicate ON listings (is_duplicate)",
        "CREATE INDEX IF NOT EXISTS ix_listings_duplicate_group_id ON listings (duplicate_group_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_listing_source_listing_id ON listings (fuente, source_listing_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_listing_fuente_link ON listings (fuente, link) WHERE link IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS ix_price_history_listing_id ON price_history (listing_id)",
    ]

    for statement in index_statements:
        connection.exec_driver_sql(statement)


def table_exists(connection, table_name):
    row = connection.exec_driver_sql(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def table_columns(connection, table_name):
    return {
        row[1]: {
            "type": row[2],
            "notnull": bool(row[3]),
            "default": row[4],
            "primary_key": bool(row[5]),
        }
        for row in connection.exec_driver_sql(f"PRAGMA table_info({table_name})")
    }


def has_required_columns(existing_columns, expected_columns):
    return any(column_name not in existing_columns for column_name in expected_columns)


def format_column_definitions(columns):
    return ", ".join(
        f"{column_name} {column_definition}"
        for column_name, column_definition in columns.items()
    )


def expression_can_be_used(expression, source_columns):
    expression_without_literals = re.sub(r"'[^']*'", "", expression)
    tokens = (
        expression_without_literals.replace("(", " ")
        .replace(")", " ")
        .replace(",", " ")
        .split()
    )
    sql_keywords = {"COALESCE", "NULLIF", "DATE", "DATETIME", "NOW"}

    for token in tokens:
        if token.upper() in sql_keywords or token.isnumeric():
            continue

        if token in source_columns:
            continue

        if token.startswith("0"):
            continue

        return False

    return True


def default_expression_for(column_definition):
    if "NOT NULL" in column_definition and "DEFAULT 'active'" in column_definition:
        return "'active'"

    if "NOT NULL" in column_definition and "DATE" in column_definition:
        return "DATE('now')"

    if "NOT NULL" in column_definition:
        return "'desconocida'"

    return "NULL"
