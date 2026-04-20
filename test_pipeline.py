import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

sys.path.append(str(Path(__file__).resolve().parent))

from database import Base
from listing_pipeline import process_listing_pipeline
from models import Listing


@pytest.fixture()
def db_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )
    TestingSessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
    )
    Base.metadata.create_all(bind=engine)

    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)


def make_listing_input(
    *,
    comuna="Ñuñoa",
    m2=70,
    dormitorios=2,
    banos=2,
    estacionamientos=1,
    precio_clp=145_000_000,
    source_id="100001",
    url=None,
    titulo=None,
):
    url = url or f"https://www.yapo.cl/bienes-raices/departamento-test/{source_id}"
    titulo = titulo or f"Departamento en venta {comuna} {m2} m2"

    return {
        "fuente": "yapo",
        "source_listing_id": source_id,
        "url": url,
        "link": url,
        "titulo": titulo,
        "comuna": comuna,
        "precio_clp": precio_clp,
        "precio_uf": None,
        "m2_construidos": m2,
        "m2_terreno": None,
        "m2_util": None,
        "m2_total": m2,
        "dormitorios": dormitorios,
        "banos": banos,
        "estacionamientos": estacionamientos,
        "lat": None,
        "lon": None,
        "fecha_publicacion": None,
    }


def count_listings(db):
    return len(db.execute(select(Listing)).scalars().all())


def get_only_listing(db):
    return db.execute(select(Listing)).scalar_one()


def test_basic_deduplication_updates_existing_listing(db_session):
    first = make_listing_input(
        source_id="100001",
        precio_clp=145_000_000,
        titulo="Departamento en venta Ñuñoa 70 m2",
    )
    second = make_listing_input(
        source_id="100002",
        precio_clp=150_000_000,
        titulo="Departamento en venta Ñuñoa 70 m2",
    )

    created = process_listing_pipeline(db_session, first, source="scraper")
    updated = process_listing_pipeline(db_session, second, source="scraper")

    assert created is not None
    assert updated is not None
    assert created.id == updated.id
    assert count_listings(db_session) == 1

    listing = get_only_listing(db_session)
    assert listing.precio_clp == 150_000_000
    assert listing.source_listing_id == "100002"


def test_m2_tolerance_updates_existing_listing(db_session):
    first = make_listing_input(
        source_id="200001",
        m2=70,
        titulo="Departamento en venta Ñuñoa",
    )
    second = make_listing_input(
        source_id="200002",
        m2=72,
        titulo="Departamento en venta Ñuñoa",
    )

    created = process_listing_pipeline(db_session, first, source="scraper")
    updated = process_listing_pipeline(db_session, second, source="scraper")

    assert created is not None
    assert updated is not None
    assert created.id == updated.id
    assert count_listings(db_session) == 1

    listing = get_only_listing(db_session)
    assert listing.m2_construidos == 72


def test_different_comuna_creates_distinct_listings(db_session):
    nunoa = make_listing_input(
        comuna="Ñuñoa",
        source_id="300001",
        m2=70,
        titulo="Departamento en venta Ñuñoa 70 m2",
    )
    providencia = make_listing_input(
        comuna="Providencia",
        source_id="300002",
        m2=70,
        titulo="Departamento en venta Providencia 70 m2",
        url="https://www.yapo.cl/bienes-raices/departamento-test/300002",
    )

    first = process_listing_pipeline(db_session, nunoa, source="scraper")
    second = process_listing_pipeline(db_session, providencia, source="scraper")

    assert first is not None
    assert second is not None
    assert first.id != second.id
    assert count_listings(db_session) == 2


def test_listing_without_m2_is_blocked(db_session):
    listing_input = make_listing_input(
        source_id="400001",
        m2=None,
        titulo="Departamento en venta Ñuñoa sin superficie",
    )

    result = process_listing_pipeline(db_session, listing_input, source="scraper")

    assert result is None
    assert count_listings(db_session) == 0
