from sqlalchemy import Boolean, Column, Date, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship

from database import Base


class Property(Base):
    __tablename__ = "properties"

    id = Column(Integer, primary_key=True, index=True)
    comuna = Column(String, nullable=True, index=True)
    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)
    m2_construidos = Column(Float, nullable=True)
    m2_terreno = Column(Float, nullable=True)
    m2_util = Column(Float, nullable=True)
    m2_total = Column(Float, nullable=True)
    dormitorios = Column(Integer, nullable=True)
    banos = Column(Integer, nullable=True)
    estacionamientos = Column(Integer, nullable=True)
    piscina = Column(Boolean, nullable=True)
    ano_construccion = Column(Integer, nullable=True)

    listings = relationship("Listing", back_populates="property")


class Listing(Base):
    __tablename__ = "listings"
    __table_args__ = (
        UniqueConstraint(
            "fuente",
            "source_listing_id",
            name="uq_listing_source_listing_id",
        ),
        UniqueConstraint(
            "fuente",
            "link",
            name="uq_listing_fuente_link",
        ),
    )

    id = Column(Integer, primary_key=True, index=True)
    property_id = Column(Integer, ForeignKey("properties.id"), nullable=True, index=True)

    fuente = Column(String, nullable=False, index=True)
    source_listing_id = Column(String, nullable=True, index=True)
    url = Column(String, nullable=True, index=True)
    link = Column(String, nullable=True, index=True)
    status = Column(String, nullable=False, default="active", index=True)
    titulo = Column(String, nullable=True)

    comuna = Column(String, nullable=True, index=True)
    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)

    precio_clp = Column(Integer, nullable=True)
    precio_uf = Column(Float, nullable=True)

    m2_construidos = Column(Float, nullable=True)
    m2_terreno = Column(Float, nullable=True)
    m2_util = Column(Float, nullable=True)
    m2_total = Column(Float, nullable=True)

    dormitorios = Column(Integer, nullable=True)
    banos = Column(Integer, nullable=True)
    estacionamientos = Column(Integer, nullable=True)

    fecha_publicacion = Column(Date, nullable=True)
    fecha_captura = Column(Date, nullable=False)
    last_seen = Column(DateTime, nullable=True, index=True)
    property_fingerprint = Column(String, nullable=True, index=True)
    is_duplicate = Column(Boolean, nullable=False, default=False, index=True)
    duplicate_group_id = Column(String, nullable=True, index=True)

    property = relationship("Property", back_populates="listings")
    price_history = relationship(
        "PriceHistory",
        back_populates="listing",
        cascade="all, delete-orphan",
        order_by="PriceHistory.fecha_captura",
    )


class PriceHistory(Base):
    __tablename__ = "price_history"

    id = Column(Integer, primary_key=True, index=True)
    listing_id = Column(Integer, ForeignKey("listings.id"), nullable=False, index=True)
    precio_clp = Column(Integer, nullable=True)
    precio_uf = Column(Float, nullable=True)
    precio_clp_anterior = Column(Integer, nullable=True)
    precio_uf_anterior = Column(Float, nullable=True)
    precio_clp_nuevo = Column(Integer, nullable=True)
    precio_uf_nuevo = Column(Float, nullable=True)
    fecha_captura = Column(Date, nullable=False)
    fecha_cambio = Column(DateTime, nullable=False)

    listing = relationship("Listing", back_populates="price_history")
