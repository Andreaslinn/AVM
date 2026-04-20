from datetime import date, datetime
from html import escape
import json
import os
from pathlib import Path
import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv

load_dotenv()


def env_flag(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "on"}


DEMO_MODE = env_flag("DEMO_MODE", True)
os.environ["DEMO_MODE"] = "1" if DEMO_MODE else "0"

from database import DB_PATH, SessionLocal, engine
from data_sufficiency import LOW_DATA_WARNING, MIN_ACTIVE_LISTINGS
from services.listing_service import initialize_app_data, save_listing as save_property_listing
from services import radar_service, risk_analysis_service, valuation_service
from tracking import log_event

st.set_page_config(page_title="Tasador Inmobiliario", layout="wide")

if "page" not in st.session_state:
    st.session_state.page = "tracking"


def nav_button(label, key, target):
    if st.session_state.page == target:
        st.button(f"[{label}]", key=key, use_container_width=True)
    else:
        if st.button(label, key=key, use_container_width=True):
            st.session_state.page = target


def render_nav():
    cols = st.columns(3)

    with cols[0]:
        nav_button("Tasar propiedad", "nav_tasar", "tasar")

    with cols[1]:
        nav_button("Oportunidades", "nav_radar", "radar")

    with cols[2]:
        nav_button("Tracking", "nav_tracking", "tracking")

    st.markdown("---")

def check_login():
    USERS_FILE = Path("users.json")

    if not USERS_FILE.exists():
        st.error("users.json no encontrado")
        return False

    users = json.loads(USERS_FILE.read_text(encoding="utf-8"))

    st.title("Acceso restringido")
    username = st.text_input("Usuario")
    password = st.text_input("Contraseña", type="password")

    if st.button("Entrar"):
        if username in users and users[username] == password:
            st.session_state["user"] = username
            if username == "Andy":
                st.session_state["is_admin"] = True
            else:
                st.session_state["is_admin"] = False
            return True

        st.error("Credenciales incorrectas")

    return False


def log_usage(user):
    log_event(user, "open_app", skip_tracking=st.session_state.get("is_admin", False))


def track(event, metadata=None):
    user = st.session_state.get("user", "anonymous")
    log_event(
        user,
        event,
        metadata,
        skip_tracking=st.session_state.get("is_admin", False),
    )


if "user" not in st.session_state:
    if not check_login():
        st.stop()
    else:
        log_usage(st.session_state["user"])
        st.session_state["usage_logged"] = True
        st.rerun()


if "usage_logged" not in st.session_state:
    log_usage(st.session_state["user"])
    st.session_state["usage_logged"] = True


st.info("Beta privada — análisis automatizado con fines informativos. No reemplaza asesoría profesional.")

if DEMO_MODE:
    st.caption(f"Modo demo: usando snapshot `{DB_PATH}`. Scraping y escrituras desactivadas.")


if st.session_state.get("is_admin"):
    st.caption("Modo admin (no tracking)")


if "dashboard_logged" not in st.session_state:
    track("view_dashboard")
    st.session_state["dashboard_logged"] = True


COMUNA_COORDENADAS = {
    "Ñuñoa": (-33.456, -70.597),
    "Providencia": (-33.426, -70.617),
    "Las Condes": (-33.408, -70.567),
    "Santiago": (-33.448, -70.669),
    "Vitacura": (-33.386, -70.573),
    "La Reina": (-33.441, -70.535),
    "Macul": (-33.487, -70.599),
    "La Florida": (-33.522, -70.598),
    "San Miguel": (-33.496, -70.651),
    "Recoleta": (-33.406, -70.642),
    "Independencia": (-33.414, -70.665),
    "Peñalolén": (-33.486, -70.533),
    "Puente Alto": (-33.616, -70.575),
}


def geocode_simple(comuna, direccion=None):
    return COMUNA_COORDENADAS.get(comuna, (None, None))


def format_clp(value):
    if value is None:
        return "Sin precio"

    return f"${value:,.0f}".replace(",", ".")


def is_positive_number(value):
    return value is not None and value > 0


def first_not_none(*values):
    for value in values:
        if value is not None:
            return value

    return None


def safe_float(value, default=None):
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def get_listing_source_listing_id(listing):
    if not listing:
        return None
    if hasattr(listing, "get"):
        return listing.get("source_listing_id")
    return getattr(listing, "source_listing_id", None)


def init_saved_listings_table():
    if DEMO_MODE:
        return

    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS saved_listings (
                id INTEGER PRIMARY KEY,
                listing_id VARCHAR NOT NULL UNIQUE,
                precio_guardado FLOAT,
                score_guardado FLOAT,
                saved_at VARCHAR NOT NULL
            )
            """
        )
        columns = {
            row[1]
            for row in connection.exec_driver_sql("PRAGMA table_info(saved_listings)").fetchall()
        }
        if "precio_guardado" not in columns:
            connection.exec_driver_sql("ALTER TABLE saved_listings ADD COLUMN precio_guardado FLOAT")
        if "score_guardado" not in columns:
            connection.exec_driver_sql("ALTER TABLE saved_listings ADD COLUMN score_guardado FLOAT")
        if "saved_at" not in columns:
            connection.exec_driver_sql("ALTER TABLE saved_listings ADD COLUMN saved_at VARCHAR")
        if "precio" in columns:
            connection.exec_driver_sql(
                "UPDATE saved_listings SET precio_guardado = COALESCE(precio_guardado, precio)"
            )
        if "score" in columns:
            connection.exec_driver_sql(
                "UPDATE saved_listings SET score_guardado = COALESCE(score_guardado, score)"
            )
        connection.exec_driver_sql(
            "UPDATE saved_listings SET saved_at = COALESCE(saved_at, DATETIME('now'))"
        )
        columns = {
            row[1]
            for row in connection.exec_driver_sql("PRAGMA table_info(saved_listings)").fetchall()
        }
        legacy_columns = {"precio", "score"} & columns
        if legacy_columns:
            connection.exec_driver_sql("DROP TABLE IF EXISTS saved_listings_clean")
            connection.exec_driver_sql(
                """
                CREATE TABLE IF NOT EXISTS saved_listings_clean (
                    id INTEGER PRIMARY KEY,
                    listing_id VARCHAR NOT NULL UNIQUE,
                    precio_guardado FLOAT,
                    score_guardado FLOAT,
                    saved_at VARCHAR NOT NULL
                )
                """
            )
            connection.exec_driver_sql(
                """
                INSERT OR IGNORE INTO saved_listings_clean (
                    id,
                    listing_id,
                    precio_guardado,
                    score_guardado,
                    saved_at
                )
                SELECT
                    id,
                    listing_id,
                    precio_guardado,
                    score_guardado,
                    COALESCE(saved_at, DATETIME('now'))
                FROM saved_listings
                """
            )
            connection.exec_driver_sql("DROP TABLE saved_listings")
            connection.exec_driver_sql(
                "ALTER TABLE saved_listings_clean RENAME TO saved_listings"
            )


def save_listing(opportunity, precio=None, score=None):
    if DEMO_MODE:
        return False

    if hasattr(opportunity, "get"):
        opportunity = opportunity or {}
        listing = opportunity.get("listing")
        listing_id = first_not_none(
            opportunity.get("source_listing_id"),
            get_listing_source_listing_id(listing),
            opportunity.get("listing_id"),
            opportunity.get("id"),
            getattr(listing, "id", None),
        )
        precio, score = get_opportunity_price_score(opportunity)
    else:
        listing_id = opportunity

    if not listing_id:
        return False

    init_saved_listings_table()
    with engine.begin() as connection:
        existing = connection.exec_driver_sql(
            "SELECT id FROM saved_listings WHERE listing_id = ?",
            (str(listing_id),),
        ).fetchone()
        if existing:
            return False

        connection.exec_driver_sql(
            """
            INSERT INTO saved_listings (listing_id, precio_guardado, score_guardado, saved_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                str(listing_id),
                safe_float(precio),
                safe_float(score),
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        return True


def get_saved_listings():
    if not DEMO_MODE:
        init_saved_listings_table()

    with engine.begin() as connection:
        saved_table_exists = connection.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'saved_listings'"
        ).fetchone()
        if not saved_table_exists:
            return []

        rows = connection.exec_driver_sql(
            """
            SELECT id, listing_id, precio_guardado, score_guardado, saved_at
            FROM saved_listings
            ORDER BY saved_at DESC
            """
        ).fetchall()

    return [
        {
            "id": row[0],
            "listing_id": row[1],
            "precio_guardado": row[2],
            "score_guardado": row[3],
            "saved_at": row[4],
        }
        for row in rows
    ]


def remove_saved_listing(listing_id):
    if DEMO_MODE:
        return False

    if not listing_id:
        return False

    init_saved_listings_table()
    with engine.begin() as connection:
        result = connection.exec_driver_sql(
            "DELETE FROM saved_listings WHERE listing_id = ?",
            (str(listing_id),),
        )

    return result.rowcount > 0


def get_opportunity_price_score(opportunity):
    opportunity = opportunity or {}
    listing = opportunity.get("listing")
    precio = first_not_none(
        opportunity.get("precio_publicado"),
        opportunity.get("listing_price"),
        opportunity.get("precio"),
        getattr(listing, "precio_clp", None),
    )
    score = first_not_none(
        opportunity.get("investment_score"),
        opportunity.get("score"),
    )

    return safe_float(precio), safe_float(score)


def calcular_precio_promedio_m2(comparables_usados):
    if not comparables_usados:
        return None

    precios_m2 = [comparable["precio_m2"] for comparable in comparables_usados]
    return sum(precios_m2) / len(precios_m2)


def calcular_superficie_promedio(comparables_usados):
    superficies = [
        comparable["listing"].m2_construidos
        for comparable in comparables_usados
        if is_positive_number(comparable["listing"].m2_construidos)
    ]

    if not superficies:
        return None

    return sum(superficies) / len(superficies)


def calcular_desviacion_estimada(comparables_usados):
    precios_m2 = [comparable["precio_m2"] for comparable in comparables_usados]

    if len(precios_m2) < 2:
        return None

    promedio = sum(precios_m2) / len(precios_m2)

    if promedio <= 0:
        return None

    varianza = sum((precio_m2 - promedio) ** 2 for precio_m2 in precios_m2) / len(
        precios_m2
    )
    desviacion = varianza ** 0.5
    return desviacion / promedio


def calcular_promedio_atributo(comparables_usados, atributo):
    valores = [
        getattr(comparable["listing"], atributo)
        for comparable in comparables_usados
        if getattr(comparable["listing"], atributo) is not None
    ]

    if not valores:
        return None

    return sum(valores) / len(valores)


def limitar_impacto(value, minimo=-0.12, maximo=0.12):
    return max(min(value, maximo), minimo)


def calcular_factores_tasacion(property_data, comparables_usados):
    superficie_promedio = calcular_superficie_promedio(comparables_usados)
    dormitorios_promedio = calcular_promedio_atributo(comparables_usados, "dormitorios")
    ano_actual = date.today().year
    ano_construccion = property_data.get("ano_construccion")
    antiguedad = (
        max(ano_actual - ano_construccion, 0)
        if ano_construccion is not None and ano_construccion > 0
        else None
    )

    if superficie_promedio and is_positive_number(property_data.get("m2_construidos")):
        impacto_superficie = limitar_impacto(
            ((property_data.get("m2_construidos") / superficie_promedio) - 1) * 0.35
        )
    else:
        impacto_superficie = 0

    if len(comparables_usados) >= 5:
        impacto_ubicacion = 0.08
    elif comparables_usados:
        impacto_ubicacion = 0.03
    else:
        impacto_ubicacion = -0.05

    if dormitorios_promedio and property_data.get("dormitorios") is not None:
        impacto_dormitorios = limitar_impacto(
            (property_data.get("dormitorios") - dormitorios_promedio) * 0.03
        )
    else:
        impacto_dormitorios = 0

    if antiguedad is None:
        impacto_antiguedad = 0
        detalle_antiguedad = "Antiguedad sin dato."
    elif antiguedad <= 5:
        impacto_antiguedad = 0.06
        detalle_antiguedad = f"Propiedad con {antiguedad} anos aproximados."
    elif antiguedad <= 15:
        impacto_antiguedad = 0.02
        detalle_antiguedad = f"Propiedad con {antiguedad} anos aproximados."
    elif antiguedad <= 30:
        impacto_antiguedad = -0.03
        detalle_antiguedad = f"Propiedad con {antiguedad} anos aproximados."
    else:
        impacto_antiguedad = -0.08
        detalle_antiguedad = f"Propiedad con {antiguedad} anos aproximados."

    return [
        {
            "nombre": "Superficie",
            "impacto": impacto_superficie,
            "detalle": "Comparada con propiedades similares.",
        },
        {
            "nombre": "Ubicación",
            "impacto": impacto_ubicacion,
            "detalle": f"Basado en comparables disponibles en {property_data['comuna']}.",
        },
        {
            "nombre": "Dormitorios",
            "impacto": impacto_dormitorios,
            "detalle": "Ajuste frente al promedio de comparables.",
        },
        {
            "nombre": "Antigüedad",
            "impacto": impacto_antiguedad,
            "detalle": detalle_antiguedad,
        },
    ]


def format_m2(value):
    if value is None:
        return "Sin datos"

    return f"{value:,.0f} m²".replace(",", ".")


def format_percent(value):
    if value is None:
        return "Sin datos"

    return f"±{value * 100:.1f}%"


def format_impact(value):
    sign = "+" if value >= 0 else ""
    return f"{sign}{value * 100:.0f}%"


def interpretar_resultado(precio_resultado_m2, precio_promedio_m2):
    if not precio_promedio_m2 or precio_promedio_m2 <= 0:
        return {
            "estado": "En mercado",
            "clase": "neutral",
            "descripcion": "No hay suficientes comparables para clasificar el valor con mayor precisión.",
        }

    diferencia = (precio_resultado_m2 / precio_promedio_m2) - 1

    if diferencia < -0.08:
        return {
            "estado": "Bajo mercado",
            "clase": "low",
            "descripcion": "El valor estimado está por debajo del promedio observado en propiedades similares.",
        }

    if diferencia > 0.08:
        return {
            "estado": "Sobre mercado",
            "clase": "high",
            "descripcion": "El valor estimado está por encima del promedio de los comparables disponibles.",
        }

    return {
        "estado": "En mercado",
        "clase": "neutral",
        "descripcion": "El valor estimado se encuentra dentro del rango esperado para comparables similares.",
    }


def mostrar_interpretacion_resultado(interpretacion):
    st.markdown(
        f"""
        <div class="interpretation-card interpretation-inline {interpretacion["clase"]}">
            <div class="interpretation-copy">{interpretacion["descripcion"]}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def generar_resumen_automatico(interpretacion, factores_tasacion):
    factores_ordenados = sorted(
        factores_tasacion,
        key=lambda factor: abs(factor["impacto"]),
        reverse=True,
    )
    factores_principales = [factor["nombre"].lower() for factor in factores_ordenados[:2]]

    if len(factores_principales) == 2:
        factores_texto = f"{factores_principales[0]} y {factores_principales[1]}"
    elif factores_principales:
        factores_texto = factores_principales[0]
    else:
        factores_texto = "los comparables disponibles"

    return (
        f"La propiedad se encuentra {interpretacion['estado'].lower()} según los comparables disponibles. "
        f"Los factores que más influyen en esta estimación son {factores_texto}. "
        "La lectura combina precio por m², similitud y consistencia de mercado."
    )


def mostrar_resumen_automatico(resumen):
    st.markdown(
        f"""
        <div class="auto-summary auto-summary-inline">
            <div class="auto-summary-title">Resumen automático</div>
            <div class="auto-summary-copy">{resumen}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def mostrar_metric_card(titulo, valor, descripcion="", icono=""):
    titulo_completo = f"{icono} {titulo}" if icono else titulo
    descripcion_html = (
        f'<div class="key-metric-copy">{descripcion}</div>' if descripcion else ""
    )
    st.markdown(
        f"""
        <div class="key-metric-card">
            <div class="key-metric-value">{valor}</div>
            <div class="key-metric-title">{titulo_completo}</div>
            {descripcion_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def obtener_tipologia_dominante(comparables_usados):
    conteos = {}

    for comparable_data in comparables_usados:
        comparable = comparable_data["listing"]

        if comparable.dormitorios is None and comparable.banos is None:
            continue

        dormitorios = comparable.dormitorios if comparable.dormitorios is not None else "sin dato"
        banos = comparable.banos if comparable.banos is not None else "sin dato"
        tipologia = f"{dormitorios} dorm / {banos} baños"
        conteos[tipologia] = conteos.get(tipologia, 0) + 1

    if not conteos:
        return "Sin datos"

    return max(conteos.items(), key=lambda item: item[1])[0]


def mostrar_resumen_comparables(comparables_usados):
    superficies = [
        comparable_data["listing"].m2_construidos
        for comparable_data in comparables_usados
        if comparable_data["listing"].m2_construidos
        and comparable_data["listing"].m2_construidos > 0
    ]
    precios = [
        comparable_data["precio_clp"]
        for comparable_data in comparables_usados
        if comparable_data["precio_clp"] and comparable_data["precio_clp"] > 0
    ]

    cantidad = len(comparables_usados)
    rango_superficie = (
        f"{format_m2(min(superficies))} - {format_m2(max(superficies))}"
        if superficies
        else "Sin datos"
    )
    rango_precios = (
        f"{format_clp(min(precios))} - {format_clp(max(precios))}"
        if precios
        else "Sin datos"
    )
    tipologia = obtener_tipologia_dominante(comparables_usados)

    st.markdown(
        f"""
        <div class="comparables-summary">
            <div class="comparables-summary-header">
                <div>
                    <div class="section-title">Resumen de comparables</div>
                    <div class="section-copy">Vista simplificada de la muestra usada para sostener el cálculo.</div>
                </div>
                <span class="summary-status">Visualización simple</span>
            </div>
            <div class="comparables-summary-grid">
                <div class="summary-item">
                    <div class="summary-label">Comparables analizados</div>
                    <div class="summary-value">{escape(str(cantidad))}</div>
                </div>
                <div class="summary-item">
                    <div class="summary-label">Rango de superficie</div>
                    <div class="summary-value">{escape(rango_superficie)}</div>
                </div>
                <div class="summary-item">
                    <div class="summary-label">Rango de precios</div>
                    <div class="summary-value">{escape(rango_precios)}</div>
                </div>
                <div class="summary-item">
                    <div class="summary-label">Tipología dominante</div>
                    <div class="summary-value">{escape(tipologia)}</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def format_clp_corto(value):
    if value is None:
        return "Sin precio"

    if value >= 1_000_000:
        return f"${value / 1_000_000:.0f} MM"

    return format_clp(value)


def scale_value(value, min_value, max_value, min_position, max_position):
    if max_value == min_value:
        return (min_position + max_position) / 2

    ratio = (value - min_value) / (max_value - min_value)
    return min_position + ratio * (max_position - min_position)


def mostrar_grafico_posicion(property_data, valor, comparables_usados):
    puntos = []

    for comparable_data in comparables_usados:
        comparable = comparable_data["listing"]

        if not comparable.m2_construidos or comparable.m2_construidos <= 0:
            continue

        puntos.append(
            {
                "m2": comparable.m2_construidos,
                "precio": comparable_data["precio_clp"],
                "score": comparable_data["score"],
            }
        )

    puntos.append(
        {
            "m2": property_data["m2_construidos"],
            "precio": valor,
            "score": 1,
            "target": True,
        }
    )

    if len(puntos) < 2:
        st.info("No hay suficientes datos para graficar el posicionamiento.")
        return

    min_m2 = min(punto["m2"] for punto in puntos)
    max_m2 = max(punto["m2"] for punto in puntos)
    min_precio = min(punto["precio"] for punto in puntos)
    max_precio = max(punto["precio"] for punto in puntos)
    width = 680
    height = 320
    left = 56
    right = 28
    top = 28
    bottom = 52
    plot_width = width - left - right
    plot_height = height - top - bottom
    circles = []

    for punto in puntos:
        x = scale_value(punto["m2"], min_m2, max_m2, left, left + plot_width)
        y = scale_value(
            punto["precio"],
            min_precio,
            max_precio,
            top + plot_height,
            top,
        )

        if punto.get("target"):
            circles.append(
                f"""
                <circle cx="{x:.1f}" cy="{y:.1f}" r="9" class="scatter-target" />
                <text x="{x + 13:.1f}" y="{y - 10:.1f}" class="scatter-label">Propiedad</text>
                """
            )
        else:
            radius = 4 + min(punto["score"], 1) * 3
            circles.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{radius:.1f}" class="scatter-point" />'
            )

    st.markdown(
        f"""
        <div class="scatter-card">
            <div class="scatter-header">
                <div>
                    <div class="section-title">Posición frente al mercado</div>
                    <div class="section-copy">Superficie versus precio de comparables.</div>
                </div>
                <div class="scatter-legend">
                    <span><i class="legend-dot market"></i> Comparables</span>
                    <span><i class="legend-dot target"></i> Propiedad</span>
                </div>
            </div>
            <svg viewBox="0 0 {width} {height}" class="scatter-svg" role="img">
                <line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" class="axis-line" />
                <line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top + plot_height}" class="axis-line" />
                <text x="{left}" y="{height - 18}" class="axis-label">{format_m2(min_m2)}</text>
                <text x="{left + plot_width - 46}" y="{height - 18}" class="axis-label">{format_m2(max_m2)}</text>
                <text x="8" y="{top + plot_height}" class="axis-label">{format_clp_corto(min_precio)}</text>
                <text x="8" y="{top + 6}" class="axis-label">{format_clp_corto(max_precio)}</text>
                {"".join(circles)}
            </svg>
        </div>
        """,
        unsafe_allow_html=True,
    )


def mostrar_factor_tasacion(factor):
    impacto = factor["impacto"]
    bar_width = min(abs(impacto) / 0.12 * 100, 100)
    bar_class = "positive" if impacto >= 0 else "negative"

    st.markdown(
        f"""
        <div class="factor-row">
            <div class="factor-name">{factor["nombre"]}</div>
            <div class="factor-track">
                <div class="factor-bar {bar_class}" style="width: {bar_width}%;"></div>
            </div>
            <div class="factor-impact {bar_class}">{format_impact(impacto)}</div>
            <div class="factor-detail">
                {factor["detalle"]}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def calcular_distancia_texto(comparable):
    if comparable.lat is None or comparable.lon is None:
        return "Sin dato"

    return "Ubicación disponible"


def calcular_similitud_visual(score):
    if score >= 0.85:
        return "alta"

    if score >= 0.65:
        return "media"

    return "baja"


def mostrar_tabla_comparables(comparables_usados):
    filas = []

    for ranking, comparable_data in enumerate(
        sorted(
            comparables_usados,
            key=lambda item: item["score"],
            reverse=True,
        ),
        start=1,
    ):
        comparable = comparable_data["listing"]
        similitud = calcular_similitud_visual(comparable_data["score"])
        filas.append(
            {
                "titulo": f"Comparable #{comparable.id} - {comparable.fuente}",
                "precio": format_clp(comparable_data["precio_clp"]),
                "m2": f"{comparable.m2_construidos:g} m²"
                if comparable.m2_construidos
                else "Sin dato",
                "dormitorios": comparable.dormitorios
                if comparable.dormitorios is not None
                else "Sin dato",
                "distancia": calcular_distancia_texto(comparable),
                "similitud": similitud,
                "ranking": ranking,
                "destacado": ranking <= 3,
            }
        )

    table_rows = []
    for fila in filas[:10]:
        row_class = ' class="similar-row"' if fila["destacado"] else ""
        similitud = escape(str(fila["similitud"]))
        badge_label = escape(str(fila["similitud"].capitalize()))
        table_rows.append(
            f"""
            <tr{row_class}>
                <td>
                    <strong>{escape(str(fila["titulo"]))}</strong>
                    <div class="table-subtitle">Ranking #{escape(str(fila["ranking"]))}</div>
                </td>
                <td>{escape(str(fila["precio"]))}</td>
                <td>{escape(str(fila["m2"]))}</td>
                <td>{escape(str(fila["dormitorios"]))}</td>
                <td>{escape(str(fila["distancia"]))}</td>
                <td><span class="relevance-badge {similitud}">{badge_label}</span></td>
            </tr>
            """
        )

    rows_html = "\n".join(table_rows)
    table_html = f"""
    <div class="comparables-table-wrap">
        <table class="comparables-table">
            <thead>
                <tr>
                    <th>Título</th>
                    <th>Precio</th>
                    <th>m²</th>
                    <th>Dormitorios</th>
                    <th>Distancia</th>
                    <th>Relevancia</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
    """

    st.markdown(table_html, unsafe_allow_html=True)


def mostrar_tabla_comparables(comparables_usados):
    filas = []

    for ranking, comparable_data in enumerate(
        sorted(
            comparables_usados,
            key=lambda item: item["score"],
            reverse=True,
        )[:15],
        start=1,
    ):
        comparable = comparable_data.get("listing")
        url = comparable_data.get("url")
        comuna = comparable_data.get("comuna")
        precio = first_not_none(
            comparable_data.get("precio"),
            comparable_data.get("precio_clp"),
        )
        m2 = first_not_none(
            comparable_data.get("m2"),
            comparable_data.get("m2_construidos"),
        )
        precio_m2 = comparable_data.get("precio_m2")
        score = comparable_data.get("score", 0)

        if comparable is not None:
            url = first_not_none(url, comparable.url, comparable.link)
            comuna = first_not_none(comuna, comparable.comuna)
            m2 = first_not_none(m2, comparable.m2_construidos)

        filas.append(
            {
                "#": ranking,
                "precio": format_clp(precio),
                "m2": f"{m2:g}" if m2 else "Sin dato",
                "precio/m2": format_clp(precio_m2),
                "score": round(score, 3),
                "comuna": comuna or "Sin dato",
                "link": url or "",
            }
        )

    st.dataframe(
        filas,
        hide_index=True,
        use_container_width=True,
        column_config={
            "link": st.column_config.LinkColumn("Link"),
            "score": st.column_config.NumberColumn("Score", format="%.3f"),
        },
    )


def mostrar_card_comparable(comparable_data):
    comparable = comparable_data["listing"]
    titulo = f"Comparable #{comparable.id} - {comparable.fuente}"
    precio = format_clp(comparable_data["precio_clp"])
    m2 = f"{comparable.m2_construidos:g} m²" if comparable.m2_construidos else "Sin m²"
    dormitorios = (
        f"{comparable.dormitorios} dormitorios"
        if comparable.dormitorios is not None
        else "Dormitorios sin dato"
    )

    st.markdown(
        f"""
        <div class="comparable-card">
            <div class="comparable-title">{titulo}</div>
            <div class="comparable-price">{precio}</div>
            <div class="comparable-meta">
                <span>{m2}</span>
                <span>{dormitorios}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def format_radar_rows(opportunities):
    rows = []

    for opportunity in opportunities:
        rows.append(
            {
                "estimated_value": format_clp(opportunity.get("estimated_value")),
                "listing_price": format_clp(opportunity.get("listing_price")),
                "discount": f"{opportunity.get('discount', 0):.1%}",
                "comuna": opportunity.get("comuna"),
                "m2": opportunity.get("m2"),
                "link": opportunity.get("link") or opportunity.get("url"),
                "reason": opportunity.get("reason"),
            }
        )

    return rows


def get_radar_discount(opportunity):
    discount = first_not_none(
        opportunity.get("discount"),
        opportunity.get("undervaluation"),
    )

    if discount is not None:
        return discount

    discount_pct = first_not_none(
        opportunity.get("discount_pct"),
        opportunity.get("descuento_porcentual"),
    )

    if discount_pct is None:
        return 0

    return discount_pct / 100


def get_radar_confidence(opportunity):
    return first_not_none(
        opportunity.get("confidence"),
        opportunity.get("confidence_level"),
        "low",
    )


def radar_discount_badge(discount):
    if discount > 0.30:
        return "", "Posible anomalía"

    if discount >= 0.20:
        return "", "Descuento relevante"

    return "", "Descuento moderado"


def radar_confidence_badge(confidence):
    labels = {
        "high": ("", "Alta confianza"),
        "medium": ("", "Media confianza"),
        "low": ("", "Baja confianza"),
    }
    return labels.get(confidence or "low", labels["low"])


def sort_radar_opportunities(opportunities):
    normales = [
        opportunity
        for opportunity in opportunities
        if not opportunity.get("is_outlier")
    ]
    outliers = [
        opportunity
        for opportunity in opportunities
        if opportunity.get("is_outlier")
    ]

    normales.sort(key=radar_opportunity_rank, reverse=True)
    outliers.sort(key=radar_opportunity_rank, reverse=True)

    return normales + outliers


def radar_opportunity_rank(opportunity):
    return get_radar_discount(opportunity) * (
        opportunity.get("confidence_score") or 0
    )


def confidence_gauge_title(value):
    if value > 75:
        return "Alta confianza"

    if value >= 50:
        return "Confianza media"

    return "Baja confianza"


def build_confidence_gauge(value):
    value = max(0, min(value or 0, 100))
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=value,
            title={"text": confidence_gauge_title(value), "font": {"size": 14}},
            number={"suffix": "%", "font": {"size": 24}},
            gauge={
                "axis": {"range": [0, 100], "tickwidth": 0, "tickfont": {"size": 10}},
                "bar": {"color": "rgba(0,0,0,0)"},
                "bgcolor": "rgba(0,0,0,0)",
                "borderwidth": 0,
                "steps": [
                    {"range": [0, 50], "color": "#ff6b6b"},
                    {"range": [50, 75], "color": "#ffd166"},
                    {"range": [75, 100], "color": "#06d6a0"},
                ],
                "threshold": {
                    "line": {"color": "#f5f5f5", "width": 4},
                    "thickness": 0.75,
                    "value": value,
                },
            },
        )
    )
    fig.update_layout(
        height=200,
        margin={"l": 12, "r": 12, "t": 36, "b": 8},
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "#e6e6e6"},
    )
    return fig


def render_confidence_gauge(score):
    score = max(0, min(score or 0, 1))
    fig = build_confidence_gauge(score * 100)
    st.plotly_chart(fig, use_container_width=True)


def render_animated_gauge(score, listing_id):
    score = max(0, min(score or 0, 1))
    target_value = score * 100
    frame_count = 6
    values = [
        target_value * index / (frame_count - 1)
        for index in range(frame_count)
    ]
    placeholder = st.empty()

    for index, value in enumerate(values):
        placeholder.plotly_chart(
            build_confidence_gauge(value),
            use_container_width=True,
            key=f"gauge_{listing_id}_{index}",
        )
        time.sleep(0.05)


def mostrar_radar_cards(opportunities):
    for index, opportunity in enumerate(sort_radar_opportunities(opportunities)):
        discount = get_radar_discount(opportunity)
        discount_icon, discount_label = radar_discount_badge(discount)
        confidence = get_radar_confidence(opportunity)
        confidence_icon, confidence_label = radar_confidence_badge(confidence)
        confidence_score = max(
            0,
            min(opportunity.get("confidence_score") or 0, 1),
        )
        listing_price = first_not_none(
            opportunity.get("listing_price"),
            opportunity.get("precio_publicado"),
        )
        estimated_value = first_not_none(
            opportunity.get("estimated_value"),
            opportunity.get("valor_estimado"),
            opportunity.get("market_value"),
        )
        comuna = opportunity.get("comuna") or "Sin comuna"
        m2 = opportunity.get("m2")
        dormitorios = opportunity.get("dormitorios")
        comparable_count = first_not_none(
            opportunity.get("comparable_count"),
            opportunity.get("numero_comparables"),
            0,
        )
        time_on_market_days = opportunity.get("time_on_market_days")
        link = opportunity.get("link") or opportunity.get("url")
        property_bits = [escape(str(comuna))]

        if m2 is not None:
            property_bits.append(escape(format_m2(m2)))

        if dormitorios is not None:
            property_bits.append(f"{escape(str(dormitorios))} dormitorios")

        context_lines = [
            f"Basado en {escape(str(comparable_count))} comparables"
        ]

        if time_on_market_days is not None:
            context_lines.append(
                f"{escape(str(time_on_market_days))} días en mercado"
            )

        outlier_warning = ""
        if opportunity.get("is_outlier") is True:
            outlier_warning = """
            <div style="margin-top:12px;padding:10px 12px;border:1px solid #ef4444;border-radius:8px;color:#fecaca;background:#2a1214;">
                ⚠️ Posible anomalía de mercado
            </div>
            """

        outlier_warning = ""

        with st.container():
            st.markdown(
                f"""
                <div style="padding:18px;border:1px solid #2a303b;border-radius:8px;background:#151922;">
                    <div style="display:flex;gap:12px;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;">
                        <div>
                            <div style="font-size:28px;font-weight:800;line-height:1.15;color:#f5f5f5;">
                                {discount_icon} -{discount * 100:.1f}% bajo mercado
                            </div>
                            <div style="margin-top:6px;color:#c0c6d0;font-size:14px;">
                                {escape(discount_label)}
                            </div>
                        </div>
                        <div style="font-size:16px;font-weight:700;color:#e6e6e6;">
                            {confidence_icon} {escape(confidence_label)}
                        </div>
                    </div>
                    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-top:18px;">
                        <div>
                            <div style="color:#9ca3af;font-size:13px;">Precio</div>
                            <div style="font-size:20px;font-weight:700;color:#f5f5f5;">{escape(format_clp(listing_price))}</div>
                        </div>
                        <div>
                            <div style="color:#9ca3af;font-size:13px;">Valor estimado</div>
                            <div style="font-size:20px;font-weight:700;color:#f5f5f5;">{escape(format_clp(estimated_value))}</div>
                        </div>
                    </div>
                    <div style="margin-top:14px;color:#e6e6e6;">
                        {" | ".join(property_bits)}
                    </div>
                    <div style="margin-top:10px;color:#9ca3af;">
                        {"<br>".join(context_lines)}
                    </div>
                    {outlier_warning}
                </div>
                """,
                unsafe_allow_html=True,
            )

            listing_id = opportunity.get("source_listing_id")
            save_key = listing_id or f"missing_source_{index}"
            if st.button("⭐ Guardar", key=f"save_radar_listing_{save_key}"):
                if not listing_id:
                    st.error("No se puede guardar: falta source_listing_id")
                    return
                if save_listing(listing_id, listing_price, opportunity.get("investment_score")):
                    st.success("Oportunidad guardada")
                else:
                    st.info("Esta oportunidad ya estaba guardada")

            render_animated_gauge(confidence_score, save_key)
            st.caption(f"Confianza: {confidence_score:.0%}")

            if opportunity.get("is_outlier") is True:
                st.warning("Posible anomalía de mercado")

            if link:
                st.markdown(f"[🔗 Ver publicación]({link})")

        st.divider()


def radar_clamp(value, minimum=0, maximum=1):
    return max(min(value, maximum), minimum)


def avg_or_zero(values):
    values = [value for value in values if value is not None]
    return sum(values) / len(values) if values else 0


def format_dashboard_percent(value):
    return f"{value * 100:.1f}%"


def get_dashboard_m2_price(opportunity):
    estimated_value = first_not_none(
        opportunity.get("estimated_value"),
        opportunity.get("valor_estimado"),
        opportunity.get("market_value"),
    )
    m2 = opportunity.get("m2")

    if estimated_value is None or m2 is None or m2 <= 0:
        return None

    return estimated_value / m2


def get_dashboard_m2_range(opportunity):
    m2 = opportunity.get("m2")
    min_price = opportunity.get("min_price")
    max_price = opportunity.get("max_price")

    if m2 is None or m2 <= 0 or min_price is None or max_price is None:
        return None

    return min_price / m2, max_price / m2


def get_dashboard_dispersion(opportunity):
    market_avg = opportunity.get("precio_promedio_comparables")
    market_min = opportunity.get("precio_min_comparables")
    market_max = opportunity.get("precio_max_comparables")

    if market_avg and market_min is not None and market_max is not None:
        return (market_max - market_min) / market_avg

    m2_range = get_dashboard_m2_range(opportunity)
    m2_price = get_dashboard_m2_price(opportunity)

    if not m2_range or not m2_price:
        return None

    return (m2_range[1] - m2_range[0]) / m2_price


def calculate_radar_dashboard_metrics(opportunities):
    count = len(opportunities)
    avg_discount = avg_or_zero(
        [get_radar_discount(opportunity) for opportunity in opportunities]
    )
    avg_confidence = avg_or_zero(
        [
            radar_clamp(opportunity.get("confidence_score") or 0)
            for opportunity in opportunities
        ]
    )
    outlier_ratio = (
        sum(1 for opportunity in opportunities if opportunity.get("is_outlier")) / count
        if count
        else 0
    )
    low_confidence_ratio = (
        sum(1 for opportunity in opportunities if get_radar_confidence(opportunity) == "low")
        / count
        if count
        else 0
    )
    avg_m2_price = avg_or_zero(
        [get_dashboard_m2_price(opportunity) for opportunity in opportunities]
    )
    avg_dispersion = avg_or_zero(
        [get_dashboard_dispersion(opportunity) for opportunity in opportunities]
    )
    global_score = radar_clamp(
        (avg_confidence * 70)
        + (min(avg_discount / 0.30, 1) * 30)
        - (outlier_ratio * 25),
        0,
        100,
    )

    return {
        "score": global_score,
        "avg_discount": avg_discount,
        "avg_confidence": avg_confidence,
        "outlier_ratio": outlier_ratio,
        "low_confidence_ratio": low_confidence_ratio,
        "avg_m2_price": avg_m2_price,
        "avg_dispersion": avg_dispersion,
        "count": count,
    }


def radar_market_interpretation(metrics):
    score = metrics["score"]

    if score >= 75 and metrics["outlier_ratio"] < 0.20:
        return "Mercado ineficiente con senales de alta calidad"

    if score >= 55:
        return "Mercado moderadamente ineficiente - oportunidades selectivas"

    if score >= 35:
        return "Mercado con senales mixtas - revisar riesgo"

    return "Mercado poco concluyente - priorizar validacion"


def render_radar_kpi(label, value, helper=""):
    helper_html = (
        f'<div style="margin-top:5px;color:#7f8794;font-size:12px;">{escape(helper)}</div>'
        if helper
        else ""
    )
    st.markdown(
        f"""
        <div style="padding:14px;border:1px solid #2a303b;border-radius:8px;background:#11151d;">
            <div style="color:#9ca3af;font-size:12px;text-transform:uppercase;letter-spacing:.08em;">{escape(label)}</div>
            <div style="margin-top:8px;color:#f5f5f5;font-size:22px;font-weight:800;">{escape(str(value))}</div>
            {helper_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_radar_dashboard_header(opportunities):
    metrics = calculate_radar_dashboard_metrics(opportunities)
    interpretation = radar_market_interpretation(metrics)

    st.markdown(
        f"""
        <div style="margin-top:8px;margin-bottom:18px;padding:20px;border:1px solid #3a414f;border-radius:8px;background:#11151d;">
            <div style="display:flex;justify-content:space-between;gap:16px;align-items:flex-start;flex-wrap:wrap;">
                <div>
                    <div style="color:#9ca3af;font-size:12px;text-transform:uppercase;letter-spacing:.12em;">Risk report</div>
                    <div style="margin-top:6px;color:#f5f5f5;font-size:30px;font-weight:900;">Radar de oportunidades</div>
                    <div style="margin-top:8px;color:#c0c6d0;font-size:15px;">{escape(interpretation)}</div>
                </div>
                <div style="min-width:130px;text-align:center;padding:14px;border:1px solid #2a303b;border-radius:8px;background:#151922;">
                    <div style="color:#9ca3af;font-size:12px;text-transform:uppercase;letter-spacing:.08em;">Score global</div>
                    <div style="color:#f5f5f5;font-size:36px;font-weight:900;line-height:1;">{metrics["score"]:.0f}</div>
                    <div style="color:#7f8794;font-size:12px;">/ 100</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    kpi_cols = st.columns(5)
    with kpi_cols[0]:
        render_radar_kpi("Descuento promedio", format_dashboard_percent(metrics["avg_discount"]))
    with kpi_cols[1]:
        render_radar_kpi("Oportunidades", metrics["count"])
    with kpi_cols[2]:
        render_radar_kpi("Low confidence", format_dashboard_percent(metrics["low_confidence_ratio"]))
    with kpi_cols[3]:
        render_radar_kpi("Precio promedio m2", format_clp(metrics["avg_m2_price"]))
    with kpi_cols[4]:
        render_radar_kpi("Dispersion", format_dashboard_percent(metrics["avg_dispersion"]))


def build_dashboard_confidence_gauge(value):
    value = max(0, min(value or 0, 100))
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=value,
            title={"text": confidence_gauge_title(value), "font": {"size": 12}},
            number={"suffix": "%", "font": {"size": 20}},
            gauge={
                "axis": {"range": [0, 100], "tickwidth": 0, "tickfont": {"size": 9}},
                "bar": {"color": "rgba(0,0,0,0)"},
                "bgcolor": "rgba(0,0,0,0)",
                "borderwidth": 0,
                "steps": [
                    {"range": [0, 50], "color": "#ff6b6b"},
                    {"range": [50, 75], "color": "#ffd166"},
                    {"range": [75, 100], "color": "#06d6a0"},
                ],
                "threshold": {
                    "line": {"color": "#f5f5f5", "width": 4},
                    "thickness": 0.75,
                    "value": value,
                },
            },
        )
    )
    fig.update_layout(
        height=150,
        margin={"l": 8, "r": 8, "t": 30, "b": 0},
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "#e6e6e6"},
    )
    return fig


def render_dashboard_gauge(score, listing_id):
    score = max(0, min(score or 0, 1))
    target_value = score * 100
    frame_count = 6
    values = [
        target_value * index / (frame_count - 1)
        for index in range(frame_count)
    ]
    placeholder = st.empty()

    for index, value in enumerate(values):
        placeholder.plotly_chart(
            build_dashboard_confidence_gauge(value),
            use_container_width=True,
            key=f"risk_gauge_{listing_id}_{index}",
        )
        time.sleep(0.05)


def radar_confidence_badge_html(confidence):
    styles = {
        "high": ("HIGH CONFIDENCE", "#06d6a0", "#0f2b24"),
        "medium": ("MEDIUM", "#ffd166", "#2d2714"),
        "low": ("LOW", "#ff6b6b", "#2a1214"),
    }
    label, color, background = styles.get(confidence or "low", styles["low"])
    return (
        f'<span style="display:inline-block;margin-right:8px;margin-top:8px;'
        f'padding:5px 9px;border:1px solid {color};border-radius:8px;'
        f'background:{background};color:{color};font-size:11px;font-weight:800;'
        f'letter-spacing:.08em;">{label}</span>'
    )


def radar_outlier_badge_html(is_outlier):
    if not is_outlier:
        return ""

    return (
        '<span style="display:inline-block;margin-right:8px;margin-top:8px;'
        'padding:5px 9px;border:1px solid #ff6b6b;border-radius:8px;'
        'background:#2a1214;color:#ffb4b4;font-size:11px;font-weight:800;'
        'letter-spacing:.08em;">OUTLIER</span>'
    )


def radar_micro_comment(opportunity, discount, confidence):
    if opportunity.get("is_outlier"):
        return "Descuento extremo: validar precio, metraje y publicacion."

    if confidence == "high":
        return "Senal consistente para revision prioritaria."

    if discount >= 0.20:
        return "Descuento atractivo con riesgo moderado."

    return "Oportunidad acotada: revisar alternativas cercanas."


def render_radar_listing_card(opportunity, index):
    discount = get_radar_discount(opportunity)
    confidence = get_radar_confidence(opportunity)
    confidence_score = radar_clamp(opportunity.get("confidence_score") or 0)
    listing_price = first_not_none(
        opportunity.get("listing_price"),
        opportunity.get("precio_publicado"),
    )
    estimated_value = first_not_none(
        opportunity.get("estimated_value"),
        opportunity.get("valor_estimado"),
        opportunity.get("market_value"),
    )
    comuna = opportunity.get("comuna") or "Sin comuna"
    m2 = opportunity.get("m2")
    dormitorios = opportunity.get("dormitorios")
    comparable_count = first_not_none(
        opportunity.get("comparable_count"),
        opportunity.get("numero_comparables"),
        0,
    )
    time_on_market_days = opportunity.get("time_on_market_days")
    link = opportunity.get("link") or opportunity.get("url")
    m2_range = get_dashboard_m2_range(opportunity)
    border_color = "#ff6b6b" if opportunity.get("is_outlier") else "#2a303b"
    property_bits = [escape(str(comuna))]

    if m2 is not None:
        property_bits.append(escape(format_m2(m2)))

    if dormitorios is not None:
        property_bits.append(f"{escape(str(dormitorios))} dormitorios")

    range_text = "Rango precio/m2 no disponible"
    if m2_range:
        range_text = f"Rango precio/m2: {format_clp(m2_range[0])} - {format_clp(m2_range[1])}"

    days_text = ""
    if time_on_market_days is not None:
        days_text = f"<li>{escape(str(time_on_market_days))} dias en mercado</li>"

    badges_html = (
        radar_confidence_badge_html(confidence)
        + radar_outlier_badge_html(opportunity.get("is_outlier"))
    )

    with st.container():
        st.markdown(
            f"""
            <div style="padding:18px 18px 12px;border:1px solid {border_color};border-radius:8px;background:#151922;">
                <div style="display:flex;justify-content:space-between;gap:16px;align-items:flex-start;flex-wrap:wrap;">
                    <div>
                        <div style="color:#9ca3af;font-size:12px;text-transform:uppercase;letter-spacing:.1em;">Precio publicado</div>
                        <div style="margin-top:4px;color:#f5f5f5;font-size:26px;font-weight:900;">{escape(format_clp(listing_price))}</div>
                        <div style="margin-top:8px;color:#c0c6d0;font-size:14px;">{" | ".join(property_bits)}</div>
                    </div>
                    <div style="text-align:right;">
                        <div style="color:#ff6b6b;font-size:30px;font-weight:900;line-height:1;">-{discount * 100:.1f}%</div>
                        <div style="margin-top:4px;color:#9ca3af;font-size:12px;text-transform:uppercase;letter-spacing:.08em;">bajo mercado</div>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        listing_id = first_not_none(opportunity.get("listing_id"), index)
        render_dashboard_gauge(confidence_score, listing_id)

        st.markdown(
            f"""
            <div style="padding:0 18px 18px;border-left:1px solid {border_color};border-right:1px solid {border_color};border-bottom:1px solid {border_color};border-radius:0 0 8px 8px;background:#151922;">
                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin-bottom:12px;">
                    <div>
                        <div style="color:#9ca3af;font-size:12px;">Valor estimado</div>
                        <div style="color:#f5f5f5;font-size:20px;font-weight:800;">{escape(format_clp(estimated_value))}</div>
                    </div>
                    <div>
                        <div style="color:#9ca3af;font-size:12px;">Confianza</div>
                        <div style="color:#f5f5f5;font-size:20px;font-weight:800;">{confidence_score:.0%}</div>
                    </div>
                </div>
                <div>{badges_html}</div>
                <ul style="margin:14px 0 0 18px;padding:0;color:#c0c6d0;line-height:1.65;">
                    <li>Basado en {escape(str(comparable_count))} comparables</li>
                    <li>{escape(range_text)}</li>
                    {days_text}
                    <li>{escape(radar_micro_comment(opportunity, discount, confidence))}</li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

        if opportunity.get("is_outlier") is True:
            st.warning("Posible anomalia de mercado")

        if link:
            st.markdown(f"[Ver publicacion]({link})")

    st.divider()


def mostrar_radar_dashboard(opportunities):
    ordered_opportunities = sort_radar_opportunities(opportunities)
    render_radar_dashboard_header(ordered_opportunities)
    st.markdown(
        """
        <div style="margin:22px 0 12px;color:#f5f5f5;font-size:18px;font-weight:800;">
            Reporte de oportunidades
        </div>
        """,
        unsafe_allow_html=True,
    )

    for index, opportunity in enumerate(ordered_opportunities):
        render_radar_listing_card(opportunity, index)


def opportunity_analysis_score(opportunity):
    score = opportunity.get("opportunity_score")

    if score is None:
        score = radar_opportunity_rank(opportunity)

    return radar_clamp(score) * 100


def opportunity_analysis_text(opportunity):
    confidence = get_radar_confidence(opportunity)

    if opportunity.get("is_outlier"):
        return "Descuento extremo con riesgo alto: validar datos antes de decidir."

    if confidence == "high":
        return "Oportunidad consistente: buen descuento con soporte comparable."

    if confidence == "medium":
        return "Oportunidad interesante: requiere revision de comparables y rango."

    return "Senal debil: usar como alerta, no como decision automatica."


def render_analysis_gauge(score, listing_id):
    fig = build_confidence_gauge(score * 100)
    fig.update_layout(height=260, margin={"l": 16, "r": 16, "t": 46, "b": 8})
    st.plotly_chart(
        fig,
        use_container_width=True,
        key=f"analysis_gauge_{listing_id}",
    )


def render_analysis_risk_flags(opportunity):
    risks = []

    if get_radar_confidence(opportunity) == "low":
        risks.append("Low confidence")

    if opportunity.get("is_outlier"):
        risks.append("Outlier")

    missing_fields_pct = opportunity.get("porcentaje_campos_faltantes")
    if missing_fields_pct is not None and missing_fields_pct > 0:
        risks.append(f"Datos faltantes: {missing_fields_pct:.0f}%")

    if not risks:
        st.success("Sin flags criticos visibles.")
        return

    for risk in risks:
        st.warning(risk)


def format_market_range(opportunity):
    min_price = opportunity.get("min_price")
    max_price = opportunity.get("max_price")

    if min_price is None or max_price is None:
        return "Sin rango disponible"

    return f"{format_clp(min_price)} - {format_clp(max_price)}"


def analysis_option_label(opportunity, index):
    comuna = opportunity.get("comuna") or "Sin comuna"
    discount = get_radar_discount(opportunity)
    price = first_not_none(
        opportunity.get("listing_price"),
        opportunity.get("precio_publicado"),
    )
    return f"{index + 1}. {comuna} | -{discount * 100:.1f}% | {format_clp(price)}"


def get_analysis_comparables(opportunity, target_price=None):
    raw_comparables = (
        opportunity.get("comparables")
        or opportunity.get("comparables_resumen")
        or []
    )
    rows = []

    for index, comparable in enumerate(raw_comparables, start=1):
        precio = first_not_none(
            comparable.get("precio_clp"),
            comparable.get("precio"),
            comparable.get("listing_price"),
        )
        m2 = first_not_none(
            comparable.get("m2_construidos"),
            comparable.get("m2"),
        )
        precio_m2 = comparable.get("precio_m2")

        if precio_m2 is None and precio is not None and m2 is not None and m2 > 0:
            precio_m2 = precio / m2

        diferencia = comparable.get("diferencia_precio_vs_target")
        if diferencia is None and precio is not None and target_price:
            diferencia = (precio - target_price) / target_price * 100

        rows.append(
            {
                "#": index,
                "precio_clp": precio,
                "m2": m2,
                "precio_m2": precio_m2,
                "diferencia_vs_target": diferencia,
                "comuna": comparable.get("comuna"),
            }
        )

    return rows


def render_analysis_comparables_table(comparables):
    if not comparables:
        st.info("No hay detalle de comparables disponible para esta oportunidad.")
        return

    table_rows = []
    for row in comparables:
        diferencia = row.get("diferencia_vs_target")
        table_rows.append(
            {
                "#": row["#"],
                "precio_clp": format_clp(row.get("precio_clp")),
                "m2": row.get("m2") or "Sin dato",
                "precio_m2": format_clp(row.get("precio_m2")),
                "diferencia_vs_target": (
                    f"{diferencia:.1f}%" if diferencia is not None else "Sin dato"
                ),
                "comuna": row.get("comuna") or "Sin dato",
            }
        )

    st.dataframe(table_rows, hide_index=True, use_container_width=True)


def render_price_m2_distribution(opportunity, comparables, listing_id):
    prices_m2 = [
        row.get("precio_m2")
        for row in comparables
        if row.get("precio_m2") is not None
    ]

    if not prices_m2:
        st.info("No hay suficientes precios/m2 para graficar distribucion.")
        return

    target_m2_price = get_dashboard_m2_price(opportunity)
    fig = go.Figure()
    fig.add_trace(
        go.Histogram(
            x=prices_m2,
            marker_color="#8bd3c7",
            opacity=0.72,
            name="Comparables",
        )
    )

    if target_m2_price is not None:
        fig.add_vline(
            x=target_m2_price,
            line_width=3,
            line_dash="dash",
            line_color="#ff6b6b",
            annotation_text="Target",
        )

    fig.update_layout(
        height=260,
        margin={"l": 12, "r": 12, "t": 32, "b": 24},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": "#e6e6e6"},
        xaxis_title="Precio/m2",
        yaxis_title="Frecuencia",
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True, key=f"dist_m2_{listing_id}")


def render_price_evolution(opportunity, comparables, listing_id):
    history = opportunity.get("price_history") or opportunity.get("historial_precios")
    empty_message = opportunity.get(
        "empty_price_history_message",
        "No hay evolucion de precio disponible.",
    )
    labels = []
    values = []

    if history:
        for point in history:
            labels.append(
                first_not_none(
                    point.get("fecha"),
                    point.get("date"),
                    point.get("fecha_captura"),
                    len(labels) + 1,
                )
            )
            values.append(
                first_not_none(
                    point.get("precio_clp"),
                    point.get("precio"),
                    point.get("value"),
                )
            )
    else:
        market_min = opportunity.get("precio_min_comparables")
        market_avg = opportunity.get("precio_promedio_comparables")
        market_max = opportunity.get("precio_max_comparables")
        listing_price = first_not_none(
            opportunity.get("listing_price"),
            opportunity.get("precio_publicado"),
        )
        labels = ["Min comparables", "Promedio", "Max comparables", "Listado"]
        values = [market_min, market_avg, market_max, listing_price]

    points = [
        (label, value)
        for label, value in zip(labels, values)
        if value is not None
    ]

    if len(points) < 2:
        st.info(empty_message)
        return

    fig = go.Figure(
        go.Scatter(
            x=[point[0] for point in points],
            y=[point[1] for point in points],
            mode="lines+markers",
            line={"color": "#ffd166", "width": 3},
            marker={"size": 8, "color": "#ffd166"},
        )
    )
    fig.update_layout(
        height=250,
        margin={"l": 12, "r": 12, "t": 32, "b": 24},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": "#e6e6e6"},
        yaxis_title="Precio CLP",
        xaxis_title="Referencia temporal / proxy",
    )
    st.plotly_chart(fig, use_container_width=True, key=f"evol_price_{listing_id}")


def render_market_range_bar(opportunity, listing_price, estimated_value):
    min_price = opportunity.get("min_price")
    max_price = opportunity.get("max_price")

    if min_price is None or max_price is None or max_price <= min_price:
        st.info("Sin rango suficiente para visualizar mercado.")
        return

    scale_min = min(min_price, listing_price or min_price, estimated_value or min_price)
    scale_max = max(max_price, listing_price or max_price, estimated_value or max_price)

    if scale_max <= scale_min:
        return

    range_left = (min_price - scale_min) / (scale_max - scale_min) * 100
    range_width = (max_price - min_price) / (scale_max - scale_min) * 100
    listing_position = (
        ((listing_price - scale_min) / (scale_max - scale_min) * 100)
        if listing_price is not None
        else None
    )
    estimated_position = (
        ((estimated_value - scale_min) / (scale_max - scale_min) * 100)
        if estimated_value is not None
        else None
    )
    listing_marker = ""
    estimated_marker = ""

    if listing_position is not None:
        listing_marker = (
            f'<span style="position:absolute;left:{listing_position:.1f}%;top:-7px;'
            'width:3px;height:26px;background:#ff6b6b;border-radius:2px;"></span>'
        )

    if estimated_position is not None:
        estimated_marker = (
            f'<span style="position:absolute;left:{estimated_position:.1f}%;top:-7px;'
            'width:3px;height:26px;background:#06d6a0;border-radius:2px;"></span>'
        )

    st.markdown(
        f"""
        <div style="margin-top:12px;padding:16px;border:1px solid #2a303b;border-radius:8px;background:#151922;">
            <div style="color:#f5f5f5;font-weight:800;margin-bottom:14px;">Rango de mercado</div>
            <div style="position:relative;height:12px;border-radius:8px;background:#2a303b;">
                <span style="position:absolute;left:{range_left:.1f}%;width:{range_width:.1f}%;height:12px;border-radius:8px;background:#8bd3c7;"></span>
                {listing_marker}
                {estimated_marker}
            </div>
            <div style="display:flex;justify-content:space-between;margin-top:12px;color:#9ca3af;font-size:12px;">
                <span>Publicado: {escape(format_clp(listing_price))}</span>
                <span>Mercado: {escape(format_clp(min_price))} - {escape(format_clp(max_price))}</span>
                <span>Estimado: {escape(format_clp(estimated_value))}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def build_dynamic_analysis_text(opportunity, comparable_count, confidence_score):
    dispersion = get_dashboard_dispersion(opportunity)
    confidence = get_radar_confidence(opportunity)
    dispersion_text = (
        f" La dispersion estimada entre comparables es {dispersion * 100:.1f}%."
        if dispersion is not None
        else " La dispersion no esta disponible con los datos actuales."
    )
    risk_text = (
        " El descuento es extremo y debe revisarse como posible anomalia."
        if opportunity.get("is_outlier")
        else ""
    )

    return (
        f"Basado en {comparable_count} comparables similares, "
        f"con confianza {confidence} ({confidence_score:.0%})."
        f"{dispersion_text}{risk_text}"
    )


def build_analysis_detail_context(opportunity, index=0):
    listing_id = first_not_none(opportunity.get("listing_id"), f"analysis_{index}")
    score = first_not_none(
        opportunity.get("investment_score"),
        opportunity_analysis_score(opportunity),
    )
    score_breakdown = opportunity.get("score_breakdown") or {}
    label = opportunity.get("label") or get_investment_recommendation(
        get_radar_discount(opportunity),
        radar_clamp(opportunity.get("confidence_score") or 0),
    )
    confidence_score = radar_clamp(opportunity.get("confidence_score") or 0)
    discount = get_radar_discount(opportunity)
    listing_price = first_not_none(
        opportunity.get("listing_price"),
        opportunity.get("precio_publicado"),
    )
    estimated_value = first_not_none(
        opportunity.get("estimated_value"),
        opportunity.get("valor_estimado"),
        opportunity.get("market_value"),
    )
    comparable_count = first_not_none(
        opportunity.get("comparable_count"),
        opportunity.get("numero_comparables"),
        0,
    )
    roi = opportunity.get("roi", 0) or 0
    appreciation = opportunity.get("appreciation", 0) or 0
    yield_value = opportunity.get("yield")
    if yield_value is None and listing_price is not None and listing_price > 0:
        rent_monthly = listing_price / 300
        yield_value = (rent_monthly * 12) / listing_price
    yield_pct = yield_value * 100 if yield_value is not None and yield_value <= 1 else (yield_value or 0)
    missing_fields_pct = opportunity.get("porcentaje_campos_faltantes", 0) or 0
    analysis_comparables = get_analysis_comparables(
        opportunity,
        target_price=listing_price,
    )
    return {
        "listing_id": listing_id,
        "score": score,
        "score_breakdown": score_breakdown,
        "label": label,
        "confidence_score": confidence_score,
        "discount": discount,
        "listing_price": listing_price,
        "estimated_value": estimated_value,
        "comparable_count": comparable_count,
        "roi": roi,
        "appreciation": appreciation,
        "yield_pct": yield_pct,
        "missing_fields_pct": missing_fields_pct,
        "analysis_comparables": analysis_comparables,
    }


def render_analysis_score_section(ctx):
    # --- METRICS SECTION ---
    st.metric("Investment Score", f"{ctx['score']:.0f}/100")
    st.markdown(f"### {ctx['label']}")
    render_animated_gauge(ctx["score"] / 100, f"investment_{ctx['listing_id']}")


def render_analysis_roi_section(ctx):
    # --- METRICS SECTION ---
    roi_cols = st.columns(3)
    with roi_cols[0]:
        st.metric("ROI estimado", f"{ctx['roi']:.1f}%")
    with roi_cols[1]:
        st.metric("Yield", f"{ctx['yield_pct']:.1f}%")
    with roi_cols[2]:
        st.metric("Apreciación", f"{ctx['appreciation']:.1f}%")


def render_analysis_score_breakdown_section(ctx):
    # --- METRICS SECTION ---
    st.markdown("**Breakdown del score**")
    score_components = [
        ("Descuento", ctx["score_breakdown"].get("discount")),
        ("Confianza", ctx["score_breakdown"].get("confidence")),
        ("Comparables", ctx["score_breakdown"].get("comparables")),
        ("Riesgo", ctx["score_breakdown"].get("risk")),
    ]

    for component_label, value in score_components:
        if value is None:
            continue

        value = max(0, min(value, 100))
        st.caption(f"{component_label}: {value:.0f}/100")
        st.progress(value / 100)


def render_analysis_matrix_section(opportunity, ctx):
    # --- METRICS SECTION ---
    render_risk_return_matrix([opportunity], key_suffix=f"_{ctx['listing_id']}")
    render_discount_comparables_matrix([opportunity], key_suffix=f"_{ctx['listing_id']}")


def render_analysis_confidence_section(ctx):
    # --- METRICS SECTION ---
    st.metric("Confianza", f"{ctx['confidence_score'] * 100:.0f}%")
    st.caption(
        "Basado en cantidad de comparables, similitud y calidad de datos."
    )
    st.markdown("**Componentes de confianza:**")
    st.markdown(f"- Comparables: {ctx['comparable_count']}")
    st.markdown(f"- Datos faltantes: {ctx['missing_fields_pct']}%")
    st.info(
        "La confianza es una estimación basada en datos disponibles y no garantiza el valor real."
    )


def render_analysis_summary_section(opportunity, ctx):
    # --- SUMMARY SECTION ---
    st.markdown(
        f"""
        <div style="margin-top:12px;padding:20px;border:1px solid #3a414f;border-radius:8px;background:#11151d;">
            <div style="color:#9ca3af;font-size:12px;text-transform:uppercase;letter-spacing:.12em;">Analisis de oportunidad</div>
            <div style="display:flex;justify-content:space-between;gap:18px;align-items:flex-start;flex-wrap:wrap;margin-top:8px;">
                <div>
                    <div style="color:#f5f5f5;font-size:30px;font-weight:900;">Score {ctx['score']:.0f}/100</div>
                    <div style="margin-top:8px;color:#c0c6d0;font-size:15px;">{escape(opportunity_analysis_text(opportunity))}</div>
                </div>
                <div style="text-align:right;">
                    <div style="color:#ff6b6b;font-size:32px;font-weight:900;">-{ctx['discount'] * 100:.1f}%</div>
                    <div style="color:#9ca3af;font-size:12px;text-transform:uppercase;letter-spacing:.08em;">bajo mercado</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_analysis_market_metrics_section(opportunity, ctx):
    # --- METRICS SECTION ---
    breakdown_cols = st.columns(4)
    with breakdown_cols[0]:
        render_radar_kpi("Precio listado", format_clp(ctx["listing_price"]))
    with breakdown_cols[1]:
        render_radar_kpi("Valor estimado", format_clp(ctx["estimated_value"]))
    with breakdown_cols[2]:
        render_radar_kpi("Descuento", format_dashboard_percent(ctx["discount"]))
    with breakdown_cols[3]:
        render_radar_kpi("Rango mercado", format_market_range(opportunity))

    render_market_range_bar(opportunity, ctx["listing_price"], ctx["estimated_value"])


def render_analysis_explanation_section(opportunity, ctx):
    # --- SUMMARY SECTION ---
    st.markdown(
        f"""
        <div style="margin-top:16px;padding:16px;border:1px solid #2a303b;border-radius:8px;background:#151922;">
            <div style="color:#f5f5f5;font-size:18px;font-weight:800;">Explicacion</div>
            <div style="margin-top:8px;color:#c0c6d0;line-height:1.6;">
                {escape(build_dynamic_analysis_text(opportunity, ctx['comparable_count'], ctx['confidence_score']))}
                <br>
                {escape(opportunity.get("explanation_text") or opportunity.get("reason") or "")}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_analysis_comparables_section(ctx):
    # --- METRICS SECTION ---
    st.markdown("**Comparables**")
    render_analysis_comparables_table(ctx["analysis_comparables"])


def render_analysis_visuals_section(opportunity, ctx):
    # --- DEBUG SECTION ---
    chart_cols = st.columns(2)
    with chart_cols[0]:
        st.markdown("**Distribucion precio/m2**")
        render_price_m2_distribution(
            opportunity,
            ctx["analysis_comparables"],
            ctx["listing_id"],
        )
    with chart_cols[1]:
        st.markdown("**Evolucion de precio**")
        render_price_evolution(
            opportunity,
            ctx["analysis_comparables"],
            ctx["listing_id"],
        )


def render_analysis_risk_section(opportunity):
    # --- RISK SECTION ---
    st.markdown("**Riesgos**")
    render_analysis_risk_flags(opportunity)


def render_analysis_actions_section(opportunity):
    # --- ACTIONS SECTION ---
    link = opportunity.get("link") or opportunity.get("url")
    if link:
        st.markdown(f"[Ver publicacion]({link})")


def mostrar_analisis_detallado(opportunity, index=0):
    ctx = build_analysis_detail_context(opportunity, index=index)

    render_analysis_score_section(ctx)
    render_analysis_roi_section(ctx)
    render_analysis_score_breakdown_section(ctx)
    render_analysis_matrix_section(opportunity, ctx)
    render_analysis_confidence_section(ctx)
    render_analysis_summary_section(opportunity, ctx)
    render_analysis_market_metrics_section(opportunity, ctx)
    render_analysis_explanation_section(opportunity, ctx)
    render_analysis_comparables_section(ctx)
    render_analysis_visuals_section(opportunity, ctx)
    render_analysis_risk_section(opportunity)
    render_analysis_actions_section(opportunity)


def get_investment_recommendation(discount, confidence_score):
    if discount > 0.25 and confidence_score > 0.6:
        return "Buena inversion"

    if discount > 0.15:
        return "Interesante pero revisar"

    return "Baja oportunidad"


def generar_resumen_inversion(opportunity):
    bullets = []
    descuento = get_radar_discount(opportunity)
    confidence_score = radar_clamp(opportunity.get("confidence_score") or 0)
    numero_comparables = first_not_none(
        opportunity.get("numero_comparables"),
        opportunity.get("comparable_count"),
        0,
    )

    if descuento > 0.25:
        bullets.append("Descuento alto vs mercado")
    elif descuento > 0.15:
        bullets.append("Descuento moderado")

    if confidence_score > 0.75:
        bullets.append("Alta confianza")
    elif confidence_score > 0.5:
        bullets.append("Confianza media -> validar")
    else:
        bullets.append("Baja confianza")

    if numero_comparables >= 8:
        bullets.append("Comparables suficientes")
    else:
        bullets.append("Pocos comparables")

    if opportunity.get("is_outlier"):
        bullets.append("Posible anomalia en precio")

    return bullets


def get_investment_metrics(opportunity):
    precio_listado = first_not_none(
        opportunity.get("listing_price"),
        opportunity.get("precio_publicado"),
    )

    if precio_listado is None or precio_listado <= 0:
        rent_monthly = None
        yield_pct = 0
    else:
        rent_monthly = precio_listado / 300
        yield_pct = ((rent_monthly * 12) / precio_listado) * 100

    appreciation = opportunity.get("appreciation")
    if appreciation is None:
        comuna = opportunity.get("comuna")
        if comuna in ["Vitacura", "Las Condes"]:
            appreciation = 3.5
        elif comuna in ["Providencia", "Ñuñoa"]:
            appreciation = 3.0
        else:
            appreciation = 2.0

    backend_roi = opportunity.get("roi")
    if backend_roi is None or backend_roi <= appreciation + 0.05:
        roi = yield_pct + appreciation
    else:
        roi = backend_roi

    return {
        "precio_listado": precio_listado,
        "rent_monthly": rent_monthly,
        "yield_pct": yield_pct,
        "appreciation": appreciation,
        "roi": roi,
    }


def generar_texto_inversion(opportunity):
    metrics = get_investment_metrics(opportunity)
    discount = get_radar_discount(opportunity)
    confidence_score = radar_clamp(opportunity.get("confidence_score") or 0)
    comparables = first_not_none(
        opportunity.get("numero_comparables"),
        opportunity.get("comparable_count"),
        0,
    )
    frases = []

    if discount > 0.25:
        frases.append("presenta descuento significativo frente al mercado")
    elif discount > 0.15:
        frases.append("presenta descuento moderado")
    else:
        frases.append("muestra una brecha de precio acotada")

    if confidence_score > 0.75:
        frases.append("la senal cuenta con alta confianza")
    elif confidence_score > 0.5:
        frases.append("requiere validacion adicional por confianza media")
    else:
        frases.append("la confianza es baja y debe tratarse como alerta preliminar")

    if metrics["roi"] >= 8:
        frases.append("el perfil de retorno es atractivo")
    elif metrics["roi"] >= 6:
        frases.append("el retorno esperado es competitivo")
    else:
        frases.append("el retorno esperado es limitado")

    if comparables < 5:
        frases.append("la base comparativa es limitada")
    else:
        frases.append("la base comparable entrega soporte razonable")

    if opportunity.get("is_outlier"):
        frases.append("existe posible anomalia de mercado")

    return "Esta oportunidad " + ", ".join(frases) + "."


def render_score_breakdown(score_breakdown):
    st.markdown("**Breakdown del score**")
    components = [
        ("Descuento (40%)", score_breakdown.get("discount")),
        ("Confianza (30%)", score_breakdown.get("confidence")),
        ("Comparables (20%)", score_breakdown.get("comparables")),
        ("Riesgo (10%)", score_breakdown.get("risk")),
    ]

    for label, value in components:
        if value is None:
            continue

        value = max(0, min(value, 100))
        st.caption(f"{label}: {value:.0f}/100")
        st.progress(value / 100)


def render_investment_risk_flags(opportunity):
    flags = []
    confidence_score = radar_clamp(opportunity.get("confidence_score") or 0)
    comparables = first_not_none(
        opportunity.get("numero_comparables"),
        opportunity.get("comparable_count"),
        0,
    )
    missing = opportunity.get("porcentaje_campos_faltantes", 0) or 0

    if opportunity.get("is_outlier"):
        flags.append("Outlier: posible anomalia de precio")

    if confidence_score < 0.55:
        flags.append("Baja confianza")

    if missing > 0:
        flags.append(f"Datos faltantes: {missing:.0f}%")

    if comparables < 5:
        flags.append("Pocos comparables")

    if not flags:
        st.success("Sin flags criticos visibles.")
        return

    for flag in flags:
        st.warning(flag)


def build_legal_ownership_context(opportunity):
    legal_profile = opportunity.get("legal_profile") or {}
    score = legal_profile.get("legal_risk_score", 0) or 0
    level = legal_profile.get("legal_risk_level") or "Sin datos"
    flags = legal_profile.get("legal_flags") or []
    summary = legal_profile.get("legal_summary") or "Sin resumen legal disponible."
    missing = opportunity.get("porcentaje_campos_faltantes", 0) or 0
    comparables = first_not_none(
        opportunity.get("numero_comparables"),
        opportunity.get("comparable_count"),
        0,
    )
    confidence_score = radar_clamp(
        opportunity.get("confianza") or opportunity.get("confidence_score") or 0
    )

    if confidence_score <= 1:
        confidence_pct = confidence_score * 100
    else:
        confidence_pct = confidence_score

    if missing <= 15:
        data_consistency = "Alta"
    elif missing <= 30:
        data_consistency = "Media"
    else:
        data_consistency = "Baja"

    if comparables >= 6:
        comparable_coverage = "Amplia"
    elif comparables >= 3:
        comparable_coverage = "Parcial"
    else:
        comparable_coverage = "Limitada"

    if confidence_pct >= 75:
        model_confidence = "Alta"
    elif confidence_pct >= 60:
        model_confidence = "Media"
    else:
        model_confidence = "Baja"

    return {
        "score": score,
        "level": level,
        "flags": flags,
        "summary": summary,
        "missing": missing,
        "comparables": comparables,
        "confidence_pct": confidence_pct,
        "data_consistency": data_consistency,
        "comparable_coverage": comparable_coverage,
        "model_confidence": model_confidence,
    }


def render_legal_risk_score_section(ctx):
    # --- METRICS SECTION ---
    st.metric(
        label="Legal Risk",
        value=f"{ctx['score']}/100",
        delta=ctx["level"],
    )
    if ctx["score"] < 40:
        st.success("🟢 Riesgo legal bajo")
    elif ctx["score"] < 70:
        st.warning("🟡 Riesgo legal medio")
    else:
        st.error("🔴 Riesgo legal alto")


def render_legal_risk_factors_section(ctx):
    # --- METRICS SECTION ---
    factor_cols = st.columns(3)
    with factor_cols[0]:
        st.metric("Consistencia de datos", ctx["data_consistency"], f"{ctx['missing']:.0f}% faltante")
    with factor_cols[1]:
        st.metric("Cobertura comparables", ctx["comparable_coverage"], f"{ctx['comparables']} comps")
    with factor_cols[2]:
        st.metric("Confianza modelo", ctx["model_confidence"], f"{ctx['confidence_pct']:.0f}%")


def render_legal_risk_flags_section(ctx):
    # --- RISK SECTION ---
    if ctx["flags"]:
        for flag in ctx["flags"]:
            st.warning(flag)
    else:
        st.info("Sin flags legales visibles.")


def render_legal_report_cta_section(ctx, listing_id):
    # --- ACTIONS SECTION ---
    st.markdown("### 🔓 Reporte Legal Completo")
    st.caption("Dominio, hipotecas y gravámenes.")
    st.button(
        "Desbloquear reporte completo (próximamente)",
        key=f"legal_cta_{listing_id}",
    )

def render_legal_ownership_risk(opportunity, listing_id):
    ctx = build_legal_ownership_context(opportunity)

    with st.container():
        st.markdown("---")
        st.markdown("#### 🔒 Legal & Ownership Risk")
        render_legal_risk_score_section(ctx)
        render_legal_risk_factors_section(ctx)
        render_legal_risk_flags_section(ctx)
        render_legal_report_cta_section(ctx, listing_id)


def mostrar_inversion_simple(opportunities):
    for index, opportunity in enumerate(opportunities):
        precio_listado = first_not_none(
            opportunity.get("listing_price"),
            opportunity.get("precio_publicado"),
        )
        estimated_value = first_not_none(
            opportunity.get("estimated_value"),
            opportunity.get("valor_estimado"),
            opportunity.get("market_value"),
        )
        descuento = get_radar_discount(opportunity)
        confidence_score = radar_clamp(opportunity.get("confidence_score") or 0)
        comuna = opportunity.get("comuna") or "Sin comuna"
        link = opportunity.get("link") or opportunity.get("url")

        if precio_listado is None or precio_listado <= 0:
            rent_monthly = None
            yield_pct = None
        else:
            rent_monthly = precio_listado / 300
            rent_yearly = rent_monthly * 12
            yield_pct = rent_yearly / precio_listado

        recomendacion = get_investment_recommendation(
            descuento,
            confidence_score,
        )

        with st.container():
            st.markdown(
                f"""
                <div style="padding:18px;border:1px solid #2a303b;border-radius:8px;background:#151922;">
                    <div style="display:flex;justify-content:space-between;gap:16px;align-items:flex-start;flex-wrap:wrap;">
                        <div>
                            <div style="color:#9ca3af;font-size:12px;text-transform:uppercase;letter-spacing:.1em;">Inversion #{index + 1}</div>
                            <div style="margin-top:5px;color:#f5f5f5;font-size:24px;font-weight:900;">{escape(comuna)}</div>
                            <div style="margin-top:6px;color:#c0c6d0;">{escape(recomendacion)}</div>
                        </div>
                        <div style="text-align:right;">
                            <div style="color:#06d6a0;font-size:28px;font-weight:900;">{format_dashboard_percent(yield_pct or 0)}</div>
                            <div style="color:#9ca3af;font-size:12px;text-transform:uppercase;letter-spacing:.08em;">yield estimado</div>
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            metric_cols = st.columns(5)
            with metric_cols[0]:
                render_radar_kpi("Precio listado", format_clp(precio_listado))
            with metric_cols[1]:
                render_radar_kpi("Arriendo estimado", format_clp(rent_monthly))
            with metric_cols[2]:
                render_radar_kpi("Yield", format_dashboard_percent(yield_pct or 0))
            with metric_cols[3]:
                render_radar_kpi("Descuento", format_dashboard_percent(descuento))
            with metric_cols[4]:
                render_radar_kpi("Confianza", f"{confidence_score:.0%}")

            if estimated_value is not None:
                st.caption(f"Valor estimado AVM: {format_clp(estimated_value)}")

            st.markdown("**Análisis rápido**")
            for item in generar_resumen_inversion(opportunity):
                st.markdown(f"- {item}")

            if link:
                st.markdown(f"[Ver publicacion]({link})")

        st.divider()


def get_veredicto_color(veredicto):
    if veredicto in ("Comprar", "🟢 Comprar"):
        return "#06d6a0"

    if veredicto in ("Interesante", "Revisar", "🟡 Revisar"):
        return "#ffd166"

    return "#ff6b6b"


def generar_veredicto_frontend(score, confidence_score, opportunity=None):
    confidence_pct = confidence_score * 100

    if score >= 75 and confidence_pct >= 75:
        veredicto = "Comprar"

    elif score >= 60:
        veredicto = "Revisar"

    else:
        veredicto = "Evitar"

    if opportunity is None:
        return veredicto

    comparables = get_comparable_count(opportunity)
    missing = opportunity.get("porcentaje_campos_faltantes", 0) or 0

    if confidence_score < 0.50:
        return "Revisar" if score >= 75 else "Evitar"

    if veredicto == "Comprar" and (
        opportunity.get("is_outlier")
        or comparables < 5
        or missing >= 20
    ):
        return "Revisar"

    return veredicto


def generar_label_veredicto(veredicto):
    labels = {
        "Comprar": "🟢 Comprar",
        "Revisar": "🟡 Revisar",
        "Evitar": "🔴 Evitar",
    }
    return labels.get(veredicto, "🔴 Evitar")


def risk_level_from_score(score, confidence_score):
    if score >= 75 and confidence_score >= 0.75:
        return "Low Risk"

    if score >= 60 and confidence_score >= 0.55:
        return "Medium Risk"

    return "High Risk"


def get_property_type(opportunity):
    return first_not_none(
        opportunity.get("tipo_propiedad"),
        opportunity.get("property_type"),
        opportunity.get("tipo"),
        "Departamento",
    )


def get_property_m2(opportunity):
    return first_not_none(
        opportunity.get("m2_construidos"),
        opportunity.get("m2"),
        opportunity.get("surface"),
    )


def get_comparable_count(opportunity):
    return first_not_none(
        opportunity.get("numero_comparables"),
        opportunity.get("comparable_count"),
        0,
    )


def get_property_summary(opportunity):
    comuna = opportunity.get("comuna") or "Sin comuna"
    m2 = get_property_m2(opportunity)
    dormitorios = first_not_none(
        opportunity.get("dormitorios"),
        opportunity.get("bedrooms"),
    )
    banos = first_not_none(
        opportunity.get("banos"),
        opportunity.get("baños"),
        opportunity.get("bathrooms"),
    )
    details = [comuna]

    if m2:
        details.append(f"{m2} m2")

    if dormitorios:
        details.append(f"{dormitorios}D")

    if banos:
        details.append(f"{banos}B")

    return " | ".join(str(detail) for detail in details)


def generar_mini_narrativa_inversion(opportunity):
    discount = get_radar_discount(opportunity)
    confidence_score = radar_clamp(opportunity.get("confidence_score") or 0)
    comparables = get_comparable_count(opportunity)
    missing = opportunity.get("porcentaje_campos_faltantes", 0) or 0
    frases = []

    if discount >= 0.25:
        frases.append("Propiedad bajo mercado con descuento relevante.")
    elif discount >= 0.15:
        frases.append("Descuento moderado frente al rango comparable.")
    else:
        frases.append("Brecha de precio acotada frente al mercado.")

    if comparables >= 8:
        frases.append("Soporte comparable suficiente para una primera lectura.")
    elif comparables >= 5:
        frases.append("Soporte moderado de comparables.")
    else:
        frases.append("Base comparable limitada; requiere validacion.")

    if missing > 0:
        frases.append("Existen datos incompletos que elevan el riesgo.")
    elif confidence_score < 0.55:
        frases.append("La confianza es baja y conviene revisar manualmente.")
    else:
        frases.append("Senal interpretable para priorizacion inicial.")

    return frases[:3]


def generar_tesis_inversion(opportunity):
    discount = get_radar_discount(opportunity)
    confidence_score = radar_clamp(opportunity.get("confidence_score") or 0)
    comparables = get_comparable_count(opportunity)

    if discount >= 0.25 and confidence_score >= 0.65 and comparables >= 5:
        return "Descuento atractivo con soporte comparable suficiente."

    if discount >= 0.15 and comparables >= 5:
        return "Precio bajo mercado con evidencia moderada."

    if comparables < 5:
        return "Senal preliminar con base comparable limitada."

    return "Oportunidad acotada; priorizar validacion antes de avanzar."


def generar_linea_riesgo_inversion(opportunity):
    risks = get_resumen_riesgos_inversion(opportunity)

    if risks == ["Sin alertas criticas"]:
        return "Riesgo operativo acotado con los datos disponibles."

    return "Riesgo: " + " | ".join(risks)


def get_resumen_riesgos_inversion(opportunity):
    risks = []
    comparables = get_comparable_count(opportunity)
    missing = opportunity.get("porcentaje_campos_faltantes", 0) or 0

    if missing > 0:
        risks.append(f"Datos faltantes: {missing:.0f}%")

    if comparables < 5:
        risks.append("Pocos comparables")

    if opportunity.get("is_outlier"):
        risks.append("Outlier de descuento")

    if not risks:
        risks.append("Sin alertas criticas")

    return risks


def render_investment_quick_panel(opportunity, index, listing_id):
    discount = get_radar_discount(opportunity)
    confidence_score = radar_clamp(opportunity.get("confidence_score") or 0)
    comparables = get_comparable_count(opportunity)
    score = opportunity.get("investment_score", 0) or 0
    link = opportunity.get("link")
    listing = opportunity.get("listing")
    precio_clp = opportunity.get("precio_publicado")
    precio_uf = getattr(listing, "precio_uf", None)
    veredicto = generar_veredicto_frontend(score, confidence_score, opportunity)
    border_color = get_veredicto_color(veredicto)
    score_color = "#9ca3af" if veredicto == "Revisar" else border_color

    st.markdown(
        f"""
        <div style="padding:20px;border:1px solid {border_color};border-radius:8px;background:#151922;">
            <div style="display:flex;justify-content:space-between;gap:18px;align-items:flex-start;flex-wrap:wrap;">
                <div>
                    <div style="color:#9ca3af;font-size:12px;text-transform:uppercase;letter-spacing:.12em;">Decision #{index + 1}</div>
                    <div style="margin-top:8px;color:{border_color};font-size:40px;font-weight:950;line-height:1;">{escape(veredicto.upper())}</div>
                    <div style="margin-top:10px;color:#c0c6d0;font-size:15px;">{escape(get_property_summary(opportunity))}</div>
                </div>
                <div style="text-align:right;">
                    <div style="color:{score_color};font-size:26px;font-weight:850;line-height:1;">{score:.0f}</div>
                    <div style="color:#9ca3af;font-size:11px;text-transform:uppercase;letter-spacing:.08em;">score</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if precio_uf and precio_uf > 0:
        st.markdown(f"### {precio_uf:,.0f} UF".replace(",", "."))

        if precio_clp:
            precio_clp_text = f"${precio_clp:,.0f}".replace(",", ".")
            st.caption(f"≈ {precio_clp_text} (UF actual)")

    elif precio_clp and precio_clp > 0:
        precio_clp_text = f"${precio_clp:,.0f}".replace(",", ".")
        st.markdown(f"### {precio_clp_text}")

        from comparables import get_uf_actual

        uf_actual = get_uf_actual()

        if uf_actual:
            precio_uf_ref = precio_clp / uf_actual
            st.caption(f"≈ {precio_uf_ref:,.0f} UF".replace(",", "."))

    else:
        st.markdown("### Precio no disponible")

    kpi_cols = st.columns(3)
    with kpi_cols[0]:
        render_radar_kpi("Descuento", format_dashboard_percent(discount))
    with kpi_cols[1]:
        render_radar_kpi("Confianza", f"{confidence_score:.0%}")
    with kpi_cols[2]:
        render_radar_kpi("Comparables", comparables)

    pe = opportunity.get("price_evolution", {})

    if pe:
        days = pe.get("days_on_market")
        changes = pe.get("price_changes")
        trend = pe.get("trend")
        last_change = pe.get("last_price_change_days")

        st.markdown("**Dinámica de precio**")

        lines = []

        if days is not None:
            if days < 7:
                lines.append("🆕 Recién publicada")
            elif days > 60:
                lines.append(f"⏱ {days} días en mercado")
            else:
                lines.append(f"{days} días en mercado")

        if changes and changes > 0:
            lines.append(f"🔄 {changes} cambios de precio")

        if last_change is not None:
            if last_change < 7:
                lines.append("📉 Ajuste reciente")
            else:
                lines.append(f"Último cambio hace {last_change} días")

        if trend == "down":
            lines.append("⬇️ Tendencia bajista")
        elif trend == "up":
            lines.append("⬆️ Tendencia alcista")

        for line in lines:
            st.caption(line)

    missing_pct = opportunity.get("porcentaje_campos_faltantes", 0) or 0
    if missing_pct > 0:
        st.caption(f"⚠️ Datos incompletos ({missing_pct:.0f}%)")

    report_url = f"?report_id={listing_id}"
    st.markdown(
        f'<a href="{report_url}" target="_blank">Ver Risk Report</a>',
        unsafe_allow_html=True,
    )

    source_listing_id = opportunity.get("source_listing_id")
    save_key = source_listing_id or f"missing_source_{listing_id}"
    if st.button("⭐ Guardar", key=f"save_listing_{save_key}"):
        if not source_listing_id:
            st.error("No se puede guardar: falta source_listing_id")
            return
        if save_listing(source_listing_id, precio_clp, score):
            st.success("Oportunidad guardada")
        else:
            st.info("Esta oportunidad ya estaba guardada")

    st.markdown(
        f'<a href="{report_url}" target="_blank">Abrir Risk Report V2</a>',
        unsafe_allow_html=True,
    )

    if link:
        if hasattr(st, "link_button"):
            st.link_button("\U0001f517 Ver publicacion", link)
        else:
            safe_link = escape(str(link), quote=True)
            st.markdown(
                f'<a href="{safe_link}" target="_blank">\U0001f517 Ver publicacion</a>',
                unsafe_allow_html=True,
            )

    st.button(
        "🔒 Informe Legal (Próximamente)",
        key=f"legal_report_soon_{listing_id}",
        disabled=True,
    )
    st.caption("Acceso a información del CBR: dominio, hipotecas y riesgos legales.")


def get_veredicto_estrategia_perfil(veredicto, opportunity):
    confidence_score = radar_clamp(opportunity.get("confidence_score") or 0)

    if veredicto == "Comprar":
        perfil = "Balanceado" if confidence_score < 0.85 else "Conservador"
        return "Comprar con validacion", perfil

    if veredicto == "Revisar":
        return "Esperar", "Balanceado"

    return "Evitar", "Agresivo"


def render_analyst_block(title, bullets):
    bullet_html = "".join(
        f"<li style='margin-bottom:6px;'>{escape(str(item))}</li>"
        for item in bullets
        if item
    )
    st.markdown(
        f"""
        <div style="height:100%;padding:16px;border:1px solid #2a303b;border-radius:8px;background:#11151d;">
            <div style="color:#9ca3af;font-size:12px;text-transform:uppercase;letter-spacing:.12em;">{escape(title)}</div>
            <ul style="margin:12px 0 0 18px;padding:0;color:#c0c6d0;line-height:1.45;">{bullet_html}</ul>
        </div>
        """,
        unsafe_allow_html=True,
    )


def build_risk_report_blocks(opportunity):
    discount = get_radar_discount(opportunity)
    confidence_score = radar_clamp(opportunity.get("confidence_score") or 0)
    comparables = get_comparable_count(opportunity)
    missing = opportunity.get("porcentaje_campos_faltantes", 0) or 0
    dispersion = get_dashboard_dispersion(opportunity)

    valuation = [
        (
            "Precio bajo mercado"
            if discount >= 0.15
            else "Precio cercano al rango de mercado"
        ),
        (
            "Consistencia AVM favorable"
            if confidence_score >= 0.65
            else "Consistencia AVM por validar"
        ),
    ]
    data_quality = [
        f"Comparables: {comparables}",
        f"Datos faltantes: {missing:.0f}%",
        f"Confianza: {confidence_score:.0%}",
    ]
    risk_factors = []

    if comparables < 5:
        risk_factors.append("Pocos comparables")
    if dispersion is not None and dispersion > 0.30:
        risk_factors.append(f"Alta dispersion: {dispersion * 100:.1f}%")
    if missing > 0:
        risk_factors.append("Datos incompletos")
    if opportunity.get("is_outlier"):
        risk_factors.append("Descuento extremo / outlier")
    if not risk_factors:
        risk_factors.append("Riesgos operativos acotados")

    return valuation, data_quality, risk_factors



def render_risk_report_header(ctx):
    # --- SUMMARY SECTION ---
    with st.container():
        st.markdown("## Investment Risk Report")
        st.caption(
            f"{ctx['comuna']} | {ctx['property_type'] or 'Propiedad'}"
            + (f" | {ctx['m2']} m2" if ctx["m2"] else "")
        )
        header_cols = st.columns([2, 1])
        with header_cols[0]:
            st.metric("Precio publicado", format_clp(ctx["precio_publicado"]))
        with header_cols[1]:
            st.caption(f"Fecha: {date.today().isoformat()}")

    st.markdown("---")


def render_risk_report_hero(ctx):
    # --- METRICS SECTION ---
    hero_cols = st.columns(2)
    with hero_cols[0]:
        st.markdown("### Decisión")
        st.markdown(
            f"""
            <div style="padding:18px;border:1px solid {get_veredicto_color(ctx['veredicto'].title())};border-radius:8px;background:#151922;">
                <div style="color:#9ca3af;font-size:12px;text-transform:uppercase;letter-spacing:.12em;">Investment Score</div>
                <div style="margin-top:8px;color:#f5f5f5;font-size:56px;font-weight:950;line-height:1;">{ctx['score']:.0f}</div>
                <div style="margin-top:10px;color:#f5f5f5;font-size:24px;font-weight:900;">{escape(ctx['veredicto'])}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with hero_cols[1]:
        st.markdown("### Métricas clave")
        metric_cols = st.columns(2)
        with metric_cols[0]:
            st.metric("Precio publicado", format_clp(ctx["precio_publicado"]))
            st.metric("Descuento", format_dashboard_percent(ctx["descuento"]))
            st.metric("Comparables", ctx["comparables"])
        with metric_cols[1]:
            st.metric("Valor estimado", format_clp(ctx["valor_estimado"]))
            st.metric("Confianza", f"{ctx['confianza']:.0%}")
            st.metric(
                "Legal Risk",
                f"{ctx['legal_score']}/100" if ctx["legal_score"] is not None else "Sin datos",
                ctx["legal_level"],
            )


def render_risk_report_thesis(ctx):
    # --- SUMMARY SECTION ---
    st.markdown("### Tesis")
    st.info(ctx["thesis"])


def render_risk_report_score_breakdown(ctx):
    # --- METRICS SECTION ---
    st.markdown("### Score Breakdown")
    score_cols = st.columns(3)
    with score_cols[0]:
        st.metric("Valuation", f"{ctx['valuation_score']:.0f}/100")
        st.caption("Basado en descuento frente al valor estimado.")
    with score_cols[1]:
        st.metric("Market Support", f"{ctx['market_support_score']:.0f}/100")
        st.caption("Combina comparables disponibles y confianza del modelo.")
    with score_cols[2]:
        st.metric("Risk", f"{ctx['risk_score']:.0f}/100")
        st.caption("Penaliza legal risk y datos faltantes.")


def render_risk_report_comparables(ctx):
    # --- METRICS SECTION ---
    st.markdown("### Comparables")
    if ctx["comparables_rows"]:
        table_rows = []
        for row in ctx["comparables_rows"][:5]:
            diferencia = row.get("diferencia_vs_target")
            table_rows.append(
                {
                    "comuna": row.get("comuna") or "Sin comuna",
                    "m2": row.get("m2"),
                    "precio": format_clp(row.get("precio_clp")),
                    "precio/m2": format_clp(row.get("precio_m2")),
                    "diferencia %": (
                        f"{diferencia:.1f}%" if diferencia is not None else "Sin dato"
                    ),
                }
            )
        st.table(table_rows)
    else:
        st.info(f"Resumen disponible: {ctx['comparables']} comparables usados por el radar.")


def render_risk_report_price_dynamics(opportunity):
    # --- METRICS SECTION ---
    st.markdown("### Dinamica de precio")
    pe = opportunity.get("price_evolution", {})

    if not isinstance(pe, dict) or not pe:
        st.info("Sin datos suficientes para analizar evolucion de precio")
        return

    current_price = pe.get("current_price")
    all_time_high = pe.get("all_time_high")
    all_time_low = pe.get("all_time_low")
    days_on_market = pe.get("days_on_market")

    if current_price is None and all_time_high is None and all_time_low is None:
        st.info("Sin datos suficientes para analizar evolucion de precio")
        return

    primary_cols = st.columns(4)
    with primary_cols[0]:
        st.metric(
            "Tiempo en mercado",
            f"{days_on_market} dias" if days_on_market is not None else "Sin datos",
        )
    with primary_cols[1]:
        st.metric("Precio actual", format_clp(current_price))
    with primary_cols[2]:
        st.metric("Maximo historico", format_clp(all_time_high))
    with primary_cols[3]:
        st.metric("Minimo historico", format_clp(all_time_low))

    secondary_cols = st.columns(4)
    with secondary_cols[0]:
        drop_from_peak = pe.get("price_drop_from_peak_pct")
        st.metric(
            "Variacion desde peak",
            format_dashboard_percent(drop_from_peak)
            if drop_from_peak is not None
            else "Sin datos",
        )
    with secondary_cols[1]:
        price_range = pe.get("price_range_pct")
        st.metric(
            "Rango historico",
            format_dashboard_percent(price_range)
            if price_range is not None
            else "Sin datos",
        )
    with secondary_cols[2]:
        st.metric("Cambios de precio", pe.get("price_changes", 0))
    with secondary_cols[3]:
        last_change_days = pe.get("last_price_change_days")
        st.metric(
            "Ultimo cambio",
            f"{last_change_days} dias" if last_change_days is not None else "Sin datos",
        )

    insights = []
    if days_on_market is not None and days_on_market < 7:
        insights.append("Propiedad recien publicada")
    if days_on_market is not None and days_on_market > 60:
        insights.append("Tiempo en mercado elevado")
    if pe.get("price_changes", 0) >= 2:
        insights.append("El vendedor ha ajustado el precio multiples veces")
    if drop_from_peak is not None and drop_from_peak > 0.1:
        insights.append("Reduccion significativa desde el precio inicial")
    if pe.get("trend") == "down":
        insights.append("Tendencia bajista reciente")

    if insights:
        st.info(". ".join(insights[:2]) + ".")
    else:
        st.caption("Sin senales relevantes de cambio de precio con la informacion disponible.")


def render_risk_report_risks(ctx):
    # --- RISK SECTION ---
    st.markdown("### Riesgos")
    if ctx["risk_flags"]:
        for flag in ctx["risk_flags"]:
            st.warning(flag)
    else:
        st.success("Sin riesgos críticos visibles en los datos disponibles.")


def render_risk_report_catalysts(ctx):
    # --- ACTIONS SECTION ---
    st.markdown("### Catalizadores")
    if ctx["catalysts"]:
        for catalyst in ctx["catalysts"]:
            st.success(catalyst)
    else:
        st.info("No se detectan catalizadores fuertes con la información actual.")


def render_risk_report_bottom_line(ctx):
    # --- ACTIONS SECTION ---
    st.markdown("### Bottom Line")
    st.markdown(
        f"""
        <div style="padding:18px;border:1px solid {get_veredicto_color(ctx['veredicto'].title())};border-radius:8px;background:#151922;">
            <div style="color:#9ca3af;font-size:12px;text-transform:uppercase;letter-spacing:.12em;">Veredicto</div>
            <div style="margin-top:8px;color:#f5f5f5;font-size:34px;font-weight:950;">{escape(ctx['veredicto'])}</div>
            <div style="margin-top:6px;color:#c0c6d0;">Score {ctx['score']:.0f}/100, descuento {ctx['descuento'] * 100:.1f}%, confianza {ctx['confianza']:.0%}.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_risk_report(opportunity, listing_id):
    ctx = risk_analysis_service.build_risk_report_context(opportunity)

    render_risk_report_header(ctx)
    render_risk_report_hero(ctx)
    render_risk_report_thesis(ctx)
    render_risk_report_score_breakdown(ctx)
    render_risk_report_comparables(ctx)
    render_risk_report_price_dynamics(opportunity)
    render_risk_report_risks(ctx)
    render_risk_report_catalysts(ctx)
    render_risk_report_bottom_line(ctx)


def render_risk_report_v2(*args, **kwargs):
    raise NotImplementedError("Risk Report V2 ahora funciona exclusivamente como HTML externo")

def test_risk_report_v2():
    st.write("Test visual Risk Report V2")

    casos = [
        (
            "1. opportunity completa",
            {
                "listing_id": "qa_completa",
                "precio_publicado": 120000000,
                "valor_estimado": 145000000,
                "m2": 62,
                "dormitorios": 2,
                "banos": 2,
                "estacionamientos": 1,
                "comuna": "Providencia",
                "tipo_propiedad": "Departamento",
                "investment_score": 78,
                "confianza": 0.82,
                "numero_comparables": 9,
                "discount": 0.17,
                "precio_promedio_comparables": 2300000,
                "legal_profile": {
                    "legal_risk_score": 12,
                    "legal_risk_level": "Bajo",
                    "legal_flags": [],
                },
                "price_evolution": {
                    "days_on_market": 34,
                    "price_drop_from_peak_pct": 0.06,
                    "price_changes": 1,
                },
                "link": "https://example.com/qa-completa",
            },
        ),
        (
            "2. sin precio",
            {
                "listing_id": "qa_sin_precio",
                "m2": 58,
                "dormitorios": 2,
                "banos": 1,
                "comuna": "Ñuñoa",
                "investment_score": 55,
                "confianza": 0.64,
                "numero_comparables": 6,
                "precio_promedio_comparables": 2100000,
            },
        ),
        (
            "3. sin m2",
            {
                "listing_id": "qa_sin_m2",
                "precio_publicado": 98000000,
                "dormitorios": 1,
                "banos": 1,
                "comuna": "Santiago",
                "investment_score": 48,
                "confianza": 0.52,
                "numero_comparables": 4,
            },
        ),
        (
            "4. sin comparables",
            {
                "listing_id": "qa_sin_comparables",
                "precio_publicado": 135000000,
                "m2": 70,
                "dormitorios": 3,
                "banos": 2,
                "comuna": "Las Condes",
                "investment_score": 62,
                "confianza": 0.58,
                "precio_promedio_comparables": 2400000,
            },
        ),
        (
            "5. con datos corruptos",
            {
                "listing_id": "qa_corruptos",
                "precio_publicado": "precio_invalido",
                "valor_estimado": "valor_invalido",
                "m2": "metros_invalidos",
                "dormitorios": None,
                "banos": "dos",
                "estacionamientos": object(),
                "comuna": None,
                "tipo_propiedad": 123,
                "investment_score": "score_invalido",
                "confianza": "confianza_invalida",
                "numero_comparables": "comparables_invalidos",
                "discount": "descuento_invalido",
                "precio_promedio_comparables": "promedio_invalido",
                "legal_profile": "perfil_legal_invalido",
                "score_breakdown": "breakdown_invalido",
                "price_evolution": "evolucion_invalida",
                "risk_flags": None,
            },
        ),
    ]

    for index, (nombre, opportunity) in enumerate(casos, start=1):
        st.write(nombre)
        open_risk_report_v2(opportunity, f"qa_risk_report_v2_{index}")
        st.success("Render OK")


def generate_risk_report_html(opportunity):
    """Build a standalone HTML document for st.components.v1.html."""
    from services.risk_analysis_service import build_risk_report_context

    opportunity = opportunity if hasattr(opportunity, "get") else {}
    try:
        ctx = build_risk_report_context(opportunity or {})
    except Exception:
        ctx = {}
    if not isinstance(ctx, dict):
        ctx = {}

    def safe_number(value, default=0):
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def safe_pct(value):
        if value is None:
            return "N/A"
        return f"{safe_number(value, 0) * 100:.1f}%"

    def display_clp(value):
        value = safe_number(value, 0)
        if value <= 0:
            return "N/A"
        return format_clp(value)

    def display_number(value, suffix=""):
        value = safe_number(value, 0)
        if value <= 0:
            return "N/A"
        return f"{value:.0f}{suffix}"

    def display_pct(value):
        if value is None:
            return "N/A"
        return f"{safe_number(value, 0):.0%}"

    def score_color(value):
        value = safe_number(value, 0)
        if value >= 70:
            return "green"
        if value >= 45:
            return "amber"
        return "red"

    def discount_status(value):
        value = safe_number(value, 0)
        if value > 0.15:
            return "beat"
        if value >= 0.05:
            return "caution"
        return "miss"

    def comparables_status(value):
        value = safe_number(value, 0)
        if value >= 8:
            return "beat"
        if value >= 4:
            return "caution"
        return "miss"

    def confidence_status(value):
        value = safe_number(value, 0)
        if value >= 0.75:
            return "beat"
        if value >= 0.5:
            return "caution"
        return "miss"

    def missing_data_status(value):
        value = safe_number(value, 0)
        if value > 40:
            return "miss"
        if value >= 20:
            return "caution"
        return "beat"

    def status_color(status):
        if status in ("beat", "green"):
            return "green"
        if status in ("caution", "amber"):
            return "amber"
        return "red"

    def metric_card(label, value, detail="", status="neutral"):
        return f"""
        <div class="metric-card card-hover {escape(status)}">
          <div class="metric-label">{escape(str(label))}</div>
          <div class="metric-value">{escape(str(value))}</div>
          <div class="metric-detail {escape(status)}">{escape(str(detail))}</div>
        </div>
        """

    def analysis_card(label, value, detail, status):
        status_copy = {
            "beat": "FAVORABLE",
            "green": "FAVORABLE",
            "caution": "CAUTION",
            "amber": "CAUTION",
            "miss": "WEAK",
            "red": "WEAK",
        }.get(status, str(status).upper())
        return f"""
        <div class="analysis-card card-hover">
          <div class="analysis-label">{escape(str(label))}</div>
          <div class="analysis-value">{escape(str(value))}</div>
          <div class="analysis-detail">{escape(str(detail))}</div>
          <div class="pill {escape(status)}">{escape(status_copy)}</div>
        </div>
        """

    def breakdown_row(label, value, weight, color):
        value = max(0, min(safe_number(value, 0), 100))
        return f"""
        <div class="score-row">
          <div class="score-row-head">
            <span>{escape(label)} <span class="muted mono">({escape(weight)})</span></span>
            <span class="mono {escape(color)}">{value:.0f} / 100</span>
          </div>
          <div class="bar-track">
            <div class="bar-fill {escape(color)}" style="width:{value:.0f}%"></div>
          </div>
        </div>
        """

    listing = opportunity.get("listing")
    listing_id = (
        opportunity.get("listing_id")
        or opportunity.get("id")
        or getattr(listing, "id", None)
        or "sin_id"
    )

    precio_clp = safe_number(ctx.get("precio_publicado"), 0)
    valor_estimado = safe_number(ctx.get("valor_estimado"), 0)
    precio_uf = safe_number(
        opportunity.get("precio_uf") or getattr(listing, "precio_uf", None),
        0,
    )
    if not precio_uf and precio_clp:
        try:
            from comparables import get_uf_actual

            uf_actual = safe_number(get_uf_actual(), 0)
            if uf_actual > 0:
                precio_uf = precio_clp / uf_actual
        except Exception:
            precio_uf = 0
    m2 = safe_number(ctx.get("m2"), 0)
    dormitorios = first_not_none(
        opportunity.get("dormitorios"),
        opportunity.get("bedrooms"),
        "N/A",
    )
    banos = first_not_none(
        opportunity.get("banos"),
        opportunity.get("baños"),
        opportunity.get("bathrooms"),
        "N/A",
    )
    comuna = ctx.get("comuna") or opportunity.get("comuna") or "Sin comuna"
    property_type = ctx.get("property_type") or "Propiedad"
    score = max(0, min(safe_number(ctx.get("score"), 0), 100))
    descuento = safe_number(ctx.get("descuento"), 0)
    confianza = max(0, min(safe_number(ctx.get("confianza"), 0), 1))
    comparables = int(safe_number(ctx.get("comparables"), 0))
    valuation_score = max(0, min(safe_number(ctx.get("valuation_score"), 0), 100))
    market_support_score = max(0, min(safe_number(ctx.get("market_support_score"), 0), 100))
    risk_score = max(0, min(safe_number(ctx.get("risk_score"), 0), 100))
    thesis = ctx.get("thesis") or "Sin tesis disponible."
    risk_flags = ctx.get("risk_flags") or []
    if isinstance(risk_flags, str):
        risk_flags = [risk_flags]
    elif not isinstance(risk_flags, (list, tuple)):
        risk_flags = [str(risk_flags)]
    risk_flags = list(dict.fromkeys(str(flag) for flag in risk_flags if flag))
    negative_keywords = (
        "baja",
        "bajo",
        "pocos",
        "poco",
        "incompleto",
        "incompleta",
        "faltante",
        "faltantes",
        "riesgo",
        "outlier",
        "legal",
        "incertidumbre",
        "insuficiente",
        "débil",
        "debil",
    )
    positive_keywords = (
        "alta confianza",
        "base sólida",
        "base solida",
        "descuento significativo",
        "favorable",
        "sólida",
        "solida",
    )
    risk_flags = [
        flag
        for flag in risk_flags
        if any(keyword in flag.lower() for keyword in negative_keywords)
        and not any(keyword in flag.lower() for keyword in positive_keywords)
    ]

    strengths = []
    if comparables >= 8:
        strengths.append("Base sólida de comparables")
    if confianza >= 0.8:
        strengths.append("Alta confianza del modelo")
    if descuento >= 0.15:
        strengths.append("Descuento significativo")

    missing_pct = safe_number(ctx.get("missing_pct"), 0)
    if missing_pct > 50:
        data_quality_label = "Low Reliability"
    elif missing_pct > 25:
        data_quality_label = "Medium Reliability"
    else:
        data_quality_label = "High Reliability"

    precio_total_promedio = safe_number(opportunity.get("precio_promedio_comparables"), 0)
    precio_m2_mercado = None
    if precio_total_promedio and m2 and m2 > 0:
        precio_m2_mercado = precio_total_promedio / m2
        if precio_m2_mercado > 10000000:
            precio_m2_mercado = None
    price_m2 = precio_clp / m2 if precio_clp and m2 and m2 > 0 else 0
    price_m2_delta = None
    if price_m2 and precio_m2_mercado:
        price_m2_delta = (price_m2 - precio_m2_mercado) / precio_m2_mercado

    if score >= 75:
        score_label = "Strong Signal"
        score_status = "green"
        score_hex = "#22c55e"
    elif score >= 55:
        score_label = "Needs Review"
        score_status = "amber"
        score_hex = "#f59e0b"
    else:
        score_label = "High Risk"
        score_status = "red"
        score_hex = "#ef4444"

    veredicto = ctx.get("veredicto") or "SIN DEFINIR"
    arc_length = 251.2
    dash_offset = arc_length * (1 - score / 100)
    needle_angle = -90 + (score * 1.8)
    price_text = f"≈ {precio_uf:,.0f} UF".replace(",", ".") if precio_uf else display_clp(precio_clp)
    clp_text = display_clp(precio_clp)
    market_delta_text = (
        f"{price_m2_delta * 100:+.1f}%"
        if price_m2_delta is not None
        else "N/A"
    )
    if price_m2_delta is None:
        market_delta_status = "amber"
    elif price_m2_delta < 0:
        market_delta_status = "green"
    elif price_m2_delta <= 0.05:
        market_delta_status = "amber"
    else:
        market_delta_status = "red"
    legal_level = ctx.get("legal_level") or "Sin datos"

    valuation_cards = "".join(
        [
            analysis_card("Precio publicado", clp_text, "Precio observado en la publicación.", "beat" if precio_clp else "caution"),
            analysis_card("Valor estimado", display_clp(valor_estimado), "Referencia de valor del radar.", "beat" if valor_estimado else "caution"),
            analysis_card("Descuento", safe_pct(descuento), "Brecha contra valor estimado.", discount_status(descuento)),
            analysis_card("Precio/m2 activo", display_clp(price_m2), "Precio unitario calculado.", "beat" if price_m2 else "caution"),
            analysis_card("Precio/m2 mercado", display_clp(precio_m2_mercado), "Promedio de comparables ajustado por superficie.", "beat" if precio_m2_mercado else "caution"),
            analysis_card("Vs mercado", market_delta_text, "Diferencia del activo contra mercado.", "beat" if price_m2_delta is not None and price_m2_delta < 0 else "caution"),
        ]
    )
    market_cards = "".join(
        [
            analysis_card("Comparables", comparables, "Profundidad de mercado disponible.", comparables_status(comparables)),
            analysis_card("Confianza", display_pct(confianza), "Confianza reportada por el modelo.", confidence_status(confianza)),
            analysis_card("Comuna", comuna, "Mercado local objetivo.", "beat"),
            analysis_card("Tipo", property_type, "Segmento de propiedad.", "beat"),
            analysis_card("Superficie", display_number(m2, " m2"), "Base para comparabilidad.", "beat" if m2 else "miss"),
            analysis_card("Dorm/Baños", f"{dormitorios}D / {banos}B", "Atributos principales.", "beat"),
        ]
    )
    risk_cards = "".join(
        [
            analysis_card("Legal risk", legal_level if legal_level != "Sin datos" else "N/A", "Perfil legal disponible.", "beat" if legal_level != "Sin datos" else "caution"),
            analysis_card("Data quality", data_quality_label, "Confiabilidad según datos faltantes.", missing_data_status(missing_pct)),
            analysis_card("Score riesgo", f"{risk_score:.0f}/100", "Subscore de riesgo.", score_color(risk_score)),
            analysis_card("Datos faltantes", f"{missing_pct:.0f}%", "Completitud del input.", missing_data_status(missing_pct)),
            analysis_card("Flags", len(risk_flags), "Riesgos levantados por el radar.", "beat" if not risk_flags else "caution"),
            analysis_card("Signal flag", score_label, "Lectura institucional del score.", score_status),
        ]
    )

    price_evolution = opportunity.get("price_evolution") or {}
    if not hasattr(price_evolution, "get"):
        price_evolution = {}
    days_on_market = price_evolution.get("days_on_market")
    price_changes = safe_number(price_evolution.get("price_changes", 0), 0)
    price_drop_pct = price_evolution.get("price_drop_from_peak_pct")
    days_on_market_num = safe_number(days_on_market, 0)
    price_drop_pct_num = safe_number(price_drop_pct, 0)

    if days_on_market and days_on_market_num > 25 and price_changes >= 2:
        seller_profile = "Flexible"
    elif days_on_market and days_on_market_num > 10:
        seller_profile = "Neutral"
    else:
        seller_profile = "Rigid"

    negotiation_raw_score = 0
    if days_on_market:
        negotiation_raw_score += min(days_on_market_num * 1.5, 40)
    if price_changes:
        negotiation_raw_score += price_changes * 15
    if price_drop_pct:
        negotiation_raw_score += min(price_drop_pct_num * 100, 30)
    negotiation_score = min(negotiation_raw_score, 100)

    if negotiation_score >= 70:
        strategy = "Aggressive"
        strategy_status = "green"
    elif negotiation_score >= 40:
        strategy = "Moderate"
        strategy_status = "amber"
    else:
        strategy = "Wait"
        strategy_status = "red"

    if missing_pct > 50 or comparables < 4 or confianza < 0.5:
        negotiation_confidence = "Low"
        negotiation_confidence_status = "red"
    elif missing_pct > 25 or comparables < 7 or confianza < 0.75:
        negotiation_confidence = "Medium"
        negotiation_confidence_status = "amber"
    else:
        negotiation_confidence = "High"
        negotiation_confidence_status = "green"

    if days_on_market_num > 25 and price_changes >= 2:
        explanation = "El vendedor muestra señales de presión comercial."
    elif days_on_market_num > 10 and price_changes < 2:
        explanation = "Existe margen de negociación, pero no hay evidencia fuerte de urgencia."
    else:
        explanation = "No hay señales claras de flexibilidad del vendedor."
    if negotiation_confidence == "Low":
        explanation += " Esta lectura tiene baja confiabilidad por calidad de datos o soporte insuficiente."

    if strategy == "Aggressive" and negotiation_confidence != "Low":
        suggested_tactic = "Intentar una oferta agresiva sustentada en comparables y tiempo en mercado."
    elif strategy == "Moderate":
        suggested_tactic = "Probar una oferta moderada y evaluar la reacción del vendedor."
    elif strategy == "Wait":
        suggested_tactic = "Esperar mayor evidencia de presión comercial o un nuevo ajuste de precio."
    else:
        suggested_tactic = "Validar manualmente antes de negociar."

    negotiation_warning = ""
    if missing_pct > 50:
        negotiation_warning = "⚠️ Baja confiabilidad de la estrategia por datos incompletos."

    signals = []
    if days_on_market:
        signals.append(f"{days_on_market_num:.0f} días en mercado")
    if price_changes:
        signals.append(f"{price_changes:.0f} cambios de precio")
    if price_drop_pct:
        signals.append(f"{round(price_drop_pct_num * 100)}% caída desde peak")

    negotiation_cards = "".join(
        [
            analysis_card("Seller Profile", seller_profile, "Lectura de flexibilidad observada.", strategy_status),
            analysis_card("Negotiation Score", f"{negotiation_score:.0f}/100", "Score basado en tiempo, cambios y caídas.", strategy_status),
            analysis_card("Strategy", strategy, "Postura sugerida de negociación.", strategy_status),
            analysis_card("Negotiation Confidence", negotiation_confidence, "Confiabilidad según datos y soporte.", negotiation_confidence_status),
            analysis_card("Suggested Tactic", suggested_tactic, "Acción recomendada para negociar.", strategy_status if negotiation_confidence != "Low" else "red"),
            analysis_card("Days on Market", f"{days_on_market_num:.0f}" if days_on_market else "N/A", "Tiempo publicado.", "beat" if days_on_market_num > 25 else "caution" if days_on_market_num > 10 else "miss"),
            analysis_card("Price Changes", f"{price_changes:.0f}" if price_changes else "N/A", "Cambios observados de precio.", "beat" if price_changes >= 2 else "caution" if price_changes else "miss"),
            analysis_card("Drop from Peak", f"{round(price_drop_pct_num * 100)}%" if price_drop_pct else "N/A", "Caída acumulada desde máximo.", "beat" if price_drop_pct_num >= 0.10 else "caution" if price_drop_pct else "miss"),
        ]
    )
    signal_items = "".join(
        f"""<li><span class="catalyst-marker">▸</span><span>{escape(str(signal))}</span></li>"""
        for signal in signals
    ) or '<li><span class="catalyst-marker">▸</span><span>N/A</span></li>'

    risk_items = "".join(
        f"""<li><span class="risk-marker">▸</span><span>{escape(str(flag))}</span></li>"""
        for flag in risk_flags
    ) or '<li><span class="risk-marker">▸</span><span>N/A</span></li>'
    catalyst_items = "".join(
        f"""<li><span class="catalyst-marker">▸</span><span>{escape(str(strength))}</span></li>"""
        for strength in strengths
    ) or '<li><span class="catalyst-marker">▸</span><span>N/A</span></li>'
    generated_date = date.today().isoformat()

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{escape(str(comuna))} | Real Estate Risk Report</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
:root{{
  color-scheme:dark;
  --base:#08090d;
  --surface:#0e1018;
  --card:#12141c;
  --border:#1e2030;
  --accent:#6366f1;
  --green:#22c55e;
  --red:#ef4444;
  --amber:#f59e0b;
  --text:#e2e4eb;
  --muted:#8b90a0;
}}
body{{background:var(--base);color:var(--text);font-family:DM Sans,Inter,Segoe UI,Arial,sans-serif;line-height:1.5}}
body::before{{content:"";position:fixed;inset:0;pointer-events:none;background:radial-gradient(circle at 50% -10%,rgba(99,102,241,.18),transparent 34%),linear-gradient(180deg,rgba(255,255,255,.02),transparent 180px);z-index:-1}}
.mono{{font-family:JetBrains Mono,Consolas,monospace}}
.topbar{{position:sticky;top:0;z-index:50;background:rgba(8,9,13,.92);backdrop-filter:blur(16px);border-bottom:1px solid var(--border);box-shadow:0 10px 30px rgba(0,0,0,.22)}}
.topbar-inner{{max-width:1180px;margin:0 auto;padding:14px 24px;display:flex;justify-content:space-between;align-items:center;gap:24px}}
.brand{{display:flex;align-items:center;gap:14px}}
.badge{{border:1px solid rgba(99,102,241,.35);background:rgba(99,102,241,.12);color:var(--accent);border-radius:8px;padding:6px 10px;font-weight:800;letter-spacing:.08em}}
.muted{{color:var(--muted)}}
.green{{color:var(--green)}}.amber{{color:var(--amber)}}.red{{color:var(--red)}}
main{{max-width:1180px;margin:0 auto;padding:38px 24px 56px}}
.report-kicker{{display:flex;align-items:center;gap:14px;margin-bottom:8px}}
.line{{height:1px;flex:1;background:linear-gradient(90deg,rgba(99,102,241,.45),transparent)}}
.line.right{{background:linear-gradient(270deg,rgba(99,102,241,.45),transparent)}}
.kicker-text{{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.24em}}
h1{{text-align:center;font-size:40px;letter-spacing:-.03em;color:white;margin-top:4px}}
.subtitle{{text-align:center;color:var(--muted);font-size:13px;margin-top:8px}}
.grid-main{{display:grid;grid-template-columns:1fr 2fr;gap:28px;margin-top:34px;margin-bottom:34px}}
.card,.metric-card,.analysis-card{{background:linear-gradient(180deg,#141722,#10121a);border:1px solid var(--border);border-radius:16px;box-shadow:0 18px 50px rgba(0,0,0,.18)}}
.gauge-card{{padding:30px;display:flex;flex-direction:column;align-items:center;text-align:center;position:relative;overflow:hidden}}
.gauge-card::after{{content:"";position:absolute;inset:auto 26px 0;height:1px;background:linear-gradient(90deg,transparent,{score_hex},transparent);opacity:.55}}
.section-card{{padding:24px;margin-bottom:28px}}
.card-hover{{transition:all .22s ease}}
.card-hover:hover{{background:#181a26;border-color:#2a2d42;transform:translateY(-1px)}}
.eyebrow{{font-size:11px;letter-spacing:.2em;text-transform:uppercase;color:var(--muted);margin-bottom:14px}}
.score-pill{{border-radius:999px;padding:7px 16px;margin-top:12px;font-weight:800;font-size:13px;letter-spacing:.02em}}
.score-pill.green{{background:rgba(34,197,94,.12);border:1px solid rgba(34,197,94,.35)}}
.score-pill.amber{{background:rgba(245,158,11,.12);border:1px solid rgba(245,158,11,.35)}}
.score-pill.red{{background:rgba(239,68,68,.12);border:1px solid rgba(239,68,68,.35)}}
.kpi-grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px}}
.metric-card{{padding:18px;position:relative;overflow:hidden;min-height:122px}}
.metric-card::before{{content:"";position:absolute;left:0;top:0;bottom:0;width:3px;background:var(--muted);opacity:.75}}
.metric-card.green::before{{background:var(--green)}}.metric-card.amber::before{{background:var(--amber)}}.metric-card.red::before{{background:var(--red)}}
.metric-label,.analysis-label{{font-size:10px;text-transform:uppercase;letter-spacing:.18em;color:var(--muted);margin-bottom:8px}}
.metric-value{{font-family:JetBrains Mono,Consolas,monospace;color:white;font-size:25px;font-weight:850;word-break:break-word;line-height:1.08}}
.metric-detail,.analysis-detail{{font-size:12px;color:var(--muted);margin-top:5px}}
.metric-detail.green{{color:var(--green)}}.metric-detail.amber{{color:var(--amber)}}.metric-detail.red{{color:var(--red)}}
.section-title{{font-size:13px;text-transform:uppercase;letter-spacing:.2em;color:var(--muted);margin:4px 0 16px;display:flex;align-items:center;gap:10px}}
.dot{{width:8px;height:8px;border-radius:999px;background:var(--accent)}}
.dot.green{{background:var(--green)}}.dot.amber{{background:var(--amber)}}.dot.red{{background:var(--red)}}
.analysis-grid{{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:12px}}
.analysis-card{{padding:16px;border-radius:14px;min-height:145px}}
.analysis-value{{font-family:JetBrains Mono,Consolas,monospace;color:white;font-size:18px;font-weight:850;margin-bottom:5px;word-break:break-word;line-height:1.15}}
.pill{{display:inline-block;margin-top:10px;border-radius:999px;padding:3px 8px;font-size:10px;font-family:JetBrains Mono,Consolas,monospace;font-weight:700}}
.pill.beat,.pill.green{{background:rgba(34,197,94,.12);color:var(--green)}}
.pill.caution,.pill.amber{{background:rgba(245,158,11,.12);color:var(--amber)}}
.pill.miss,.pill.red{{background:rgba(239,68,68,.12);color:var(--red)}}
.breakdown{{padding:24px;margin-bottom:28px}}
.score-row{{margin-bottom:18px}}
.score-row-head{{display:flex;justify-content:space-between;font-size:14px;margin-bottom:8px}}
.bar-track{{height:10px;background:var(--surface);border-radius:999px;overflow:hidden}}
.bar-fill{{height:100%;border-radius:999px}}
.bar-fill.green{{background:linear-gradient(90deg,var(--green),rgba(34,197,94,.55))}}
.bar-fill.amber{{background:linear-gradient(90deg,var(--amber),rgba(245,158,11,.55))}}
.bar-fill.red{{background:linear-gradient(90deg,var(--red),rgba(239,68,68,.55))}}
.two-col{{display:grid;grid-template-columns:1fr 1fr;gap:22px;margin-bottom:28px}}
ul.clean{{list-style:none;display:grid;gap:12px}}
ul.clean li{{display:flex;gap:10px;font-size:14px;color:#c7cad3}}
.catalyst-marker{{color:var(--green)}}.risk-marker{{color:var(--red)}}
.bottom{{padding:30px;text-align:center;background:linear-gradient(180deg,#151823,var(--surface));border:1px solid var(--border);border-radius:16px;box-shadow:0 24px 70px rgba(0,0,0,.22)}}
.verdict{{display:inline-block;margin-top:10px;border:1px solid {score_hex};background:color-mix(in srgb,{score_hex} 14%, transparent);color:white;border-radius:999px;padding:9px 20px;font-weight:850;letter-spacing:.02em}}
.footer{{border-top:1px solid var(--border);margin-top:34px;padding-top:18px;text-align:center;color:#5d6472;font-size:12px}}
@media(max-width:900px){{.grid-main,.two-col{{grid-template-columns:1fr}}.kpi-grid{{grid-template-columns:repeat(2,1fr)}}.analysis-grid{{grid-template-columns:repeat(2,1fr)}}h1{{font-size:32px}}}}
@media(max-width:560px){{.topbar-inner{{align-items:flex-start;flex-direction:column}}.kpi-grid,.analysis-grid{{grid-template-columns:1fr}}main{{padding:28px 16px}}}}
</style>
</head>
<body>
<header class="topbar">
  <div class="topbar-inner">
    <div class="brand">
      <div class="badge mono">RE</div>
      <div>
        <div>{escape(str(property_type))}</div>
        <div class="muted mono" style="font-size:12px">{escape(str(comuna))} · {escape(display_number(m2, " m2"))} · {escape(str(dormitorios))}D/{escape(str(banos))}B</div>
      </div>
    </div>
    <div style="text-align:right">
      <div class="mono" style="font-size:22px;font-weight:800;color:white">{escape(price_text)}</div>
      <div class="muted mono" style="font-size:12px">{escape(clp_text)}</div>
    </div>
  </div>
</header>

<main>
  <section>
    <div class="report-kicker"><div class="line"></div><div class="kicker-text mono">Real Estate Risk Report</div><div class="line right"></div></div>
    <h1>{escape(str(comuna))} <span style="color:var(--accent)">{escape(str(property_type))}</span></h1>
    <div class="subtitle mono">Generated {generated_date} · Listing {escape(str(listing_id))} · Standalone HTML report</div>
  </section>

  <section class="grid-main">
    <div class="card gauge-card">
      <div class="eyebrow">Investment Score</div>
      <svg viewBox="0 0 200 120" width="230" aria-label="Investment score gauge">
        <defs>
          <filter id="softGlow" x="-40%" y="-40%" width="180%" height="180%">
            <feGaussianBlur stdDeviation="3" result="blur"/>
            <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
          </filter>
        </defs>
        <path d="M 20 100 A 80 80 0 0 1 180 100" fill="none" stroke="#24283a" stroke-width="14" stroke-linecap="round"/>
        <path d="M 20 100 A 80 80 0 0 1 180 100" fill="none" stroke="{score_hex}" stroke-width="14" stroke-linecap="round" stroke-dasharray="{arc_length}" stroke-dashoffset="{dash_offset:.1f}" filter="url(#softGlow)"/>
        <g transform="rotate({needle_angle:.1f} 100 100)">
          <line x1="100" y1="100" x2="100" y2="25" stroke="#f8fafc" stroke-width="2.5" stroke-linecap="round"/>
        </g>
        <circle cx="100" cy="100" r="6" fill="{score_hex}" stroke="#f8fafc" stroke-width="2"/>
        <text x="100" y="86" text-anchor="middle" fill="white" font-family="JetBrains Mono,Consolas,monospace" font-size="30" font-weight="800">{score:.0f}</text>
        <text x="100" y="104" text-anchor="middle" fill="#8b90a0" font-family="JetBrains Mono,Consolas,monospace" font-size="9">/100</text>
        <text x="20" y="116" text-anchor="middle" fill="#ef4444" font-family="JetBrains Mono,Consolas,monospace" font-size="8" font-weight="700">HIGH RISK</text>
        <text x="100" y="116" text-anchor="middle" fill="#f59e0b" font-family="JetBrains Mono,Consolas,monospace" font-size="8" font-weight="700">REVIEW</text>
        <text x="180" y="116" text-anchor="middle" fill="#22c55e" font-family="JetBrains Mono,Consolas,monospace" font-size="8" font-weight="700">STRONG</text>
      </svg>
      <div class="score-pill {score_status}">{escape(score_label)}</div>
      <p class="muted" style="font-size:12px;margin-top:12px">Signal flag based on the radar score.</p>
    </div>

    <div class="kpi-grid">
      {metric_card("Descuento", safe_pct(descuento), "Brecha contra valor estimado", status_color(discount_status(descuento)))}
      {metric_card("Comparables", comparables, "Muestras usadas por el radar", status_color(comparables_status(comparables)))}
      {metric_card("Confianza", display_pct(confianza), "Confianza del modelo", status_color(confidence_status(confianza)))}
      {metric_card("Precio/m2 vs mercado", market_delta_text, "Activo contra promedio", market_delta_status)}
      {metric_card("Precio/m2 activo", display_clp(price_m2), "Precio unitario", "green" if price_m2 else "amber")}
      {metric_card("Precio/m2 mercado", display_clp(precio_m2_mercado), "Promedio comparables", "green" if precio_m2_mercado else "amber")}
      {metric_card("Superficie", display_number(m2, " m2"), "Base física", "green" if m2 else "red")}
      {metric_card("Dorm/Baños", f"{dormitorios}D / {banos}B", "Programa principal", "green")}
    </div>
  </section>

  <section class="section-card card">
    <h2 class="section-title"><span class="dot"></span>Valuation Analysis</h2>
    <div class="analysis-grid">{valuation_cards}</div>
  </section>

  <section class="section-card card">
    <h2 class="section-title"><span class="dot green"></span>Market Support</h2>
    <div class="analysis-grid">{market_cards}</div>
  </section>

  <section class="section-card card">
    <h2 class="section-title"><span class="dot amber"></span>Risk Analysis</h2>
    <div class="analysis-grid">{risk_cards}</div>
  </section>

  <section class="section-card card">
    <h2 class="section-title"><span class="dot {strategy_status}"></span>Negotiation Strategy</h2>
    <div class="analysis-grid">{negotiation_cards}</div>
    <div class="two-col" style="margin:18px 0 0">
      <div class="card section-card" style="margin-bottom:0;border-color:rgba(99,102,241,.24)">
        <h2 class="section-title"><span class="dot"></span>Signals</h2>
        <ul class="clean">{signal_items}</ul>
      </div>
      <div class="card section-card" style="margin-bottom:0;border-color:rgba(99,102,241,.24)">
        <h2 class="section-title"><span class="dot {strategy_status}"></span>Explanation</h2>
        <p class="muted" style="font-size:14px;line-height:1.6">{escape(explanation)}</p>
        {f'<p class="red" style="font-size:13px;line-height:1.6;margin-top:12px;font-weight:700">{escape(negotiation_warning)}</p>' if negotiation_warning else ''}
      </div>
    </div>
  </section>

  <section class="card breakdown">
    <h2 class="section-title"><span class="dot"></span>Score Breakdown</h2>
    {breakdown_row("Valuation", valuation_score, "35%", score_color(valuation_score))}
    {breakdown_row("Market Support", market_support_score, "35%", score_color(market_support_score))}
    {breakdown_row("Risk", risk_score, "30%", score_color(risk_score))}
    <div style="border-top:1px solid var(--border);margin-top:18px;padding-top:16px;display:flex;justify-content:space-between;gap:16px;align-items:center">
      <span class="muted">Weighted Composite</span>
      <span class="mono" style="font-size:20px;font-weight:800;color:var(--accent)">Score: {score:.0f} / 100</span>
    </div>
  </section>

  <section class="two-col">
    <div class="card section-card" style="border-color:rgba(34,197,94,.24)">
      <h2 class="section-title" style="color:var(--green)"><span class="dot green"></span>Catalysts</h2>
      <ul class="clean">{catalyst_items}</ul>
    </div>
    <div class="card section-card" style="border-color:rgba(239,68,68,.24)">
      <h2 class="section-title" style="color:var(--red)"><span class="dot red"></span>Risks</h2>
      <ul class="clean">{risk_items}</ul>
    </div>
  </section>

  <section class="bottom">
    <div class="eyebrow">The Bottom Line</div>
    <div class="verdict">{escape(score_label)}</div>
    <p class="muted" style="max-width:760px;margin:16px auto 0;font-size:14px">{escape(str(thesis))}</p>
    <p class="muted" style="max-width:760px;margin:12px auto 0;font-size:12px;font-style:italic">Reporte informativo generado a partir de radar inmobiliario, AVM y señales de riesgo. No constituye asesoría financiera, legal ni recomendación de inversión.</p>
  </section>

  <footer class="footer mono">
    Real Estate Risk Report · Generated {generated_date} · Listing {escape(str(listing_id))}
  </footer>
</main>
</body>
</html>"""

    return html


def load_opportunity_by_id(listing_id):
    with engine.begin() as connection:
        listing = connection.exec_driver_sql(
            """
            SELECT
                id,
                source_listing_id,
                titulo,
                comuna,
                precio_clp,
                precio_uf,
                m2_construidos,
                m2_terreno,
                dormitorios,
                banos,
                estacionamientos,
                fecha_publicacion,
                link,
                url
            FROM listings
            WHERE source_listing_id = ?
               OR CAST(id AS TEXT) = ?
            LIMIT 1
            """,
            (str(listing_id), str(listing_id)),
        ).fetchone()

    if not listing:
        return {"listing_id": listing_id}

    return {
        "listing_id": first_not_none(listing[1], listing[0]),
        "id": listing[0],
        "source_listing_id": listing[1],
        "titulo": listing[2],
        "comuna": listing[3],
        "precio_publicado": listing[4],
        "listing_price": listing[4],
        "precio_uf": listing[5],
        "m2": listing[6],
        "m2_construidos": listing[6],
        "m2_terreno": listing[7],
        "dormitorios": listing[8],
        "banos": listing[9],
        "estacionamientos": listing[10],
        "fecha_publicacion": listing[11],
        "link": first_not_none(listing[12], listing[13]),
        "url": first_not_none(listing[13], listing[12]),
    }


def open_risk_report_v2(opportunity, listing_id):
    return f"?report_id={listing_id}"


def get_risk_report_file(opportunity, listing_id):
    report_opportunity = dict(opportunity or {})
    if listing_id is not None:
        report_opportunity["listing_id"] = listing_id

    return None


def open_risk_report(opportunity, listing_id):
    if "show_risk_report" not in st.session_state:
        st.session_state["show_risk_report"] = False

    st.session_state["show_risk_report"] = True

    for file in Path(".").glob("risk_report_*.html"):
        try:
            file.unlink()
        except Exception:
            pass

    if not st.session_state["show_risk_report"]:
        return

    with st.expander("Risk Report", expanded=True):
        render_risk_report(opportunity, listing_id)


def render_investment_discount_quality_matrix(opportunity, listing_id):
    discount = get_radar_discount(opportunity) * 100
    comparables = first_not_none(
        opportunity.get("numero_comparables"),
        opportunity.get("comparable_count"),
        0,
    )
    score = opportunity.get("investment_score", 0) or 0
    color = "#ff6b6b" if opportunity.get("is_outlier") else "#06d6a0"
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=[discount],
            y=[comparables],
            mode="markers",
            marker={
                "size": max(score / 2, 10),
                "color": color,
                "opacity": 0.85,
                "line": {"width": 1, "color": "#f5f5f5"},
            },
            text=[
                f"{opportunity.get('comuna') or 'Sin comuna'}<br>"
                f"Descuento: {discount:.1f}%<br>"
                f"Comparables: {comparables}<br>"
                f"Score: {score}/100"
            ],
            hoverinfo="text",
        )
    )
    fig.add_vline(x=15, line_dash="dash", line_color="#ffd166")
    fig.add_hline(y=5, line_dash="dash", line_color="#8bd3c7")
    fig.update_layout(
        title="Descuento vs Calidad",
        height=320,
        margin={"l": 12, "r": 12, "t": 46, "b": 34},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": "#e6e6e6"},
        xaxis_title="Descuento (%)",
        yaxis_title="Numero de comparables",
        showlegend=False,
    )
    fig.update_xaxes(range=[0, max(20, discount * 1.25)], gridcolor="#2a303b")
    fig.update_yaxes(range=[0, max(8, comparables * 1.4)], gridcolor="#2a303b")
    st.plotly_chart(
        fig,
        use_container_width=True,
        key=f"investment_discount_quality_{listing_id}",
    )


def find_opportunity_by_id(opportunities, active_id):
    for index, opportunity in enumerate(opportunities):
        raw_listing_id = first_not_none(opportunity.get("listing_id"), "listing")
        listing_id = f"{raw_listing_id}_{index}"
        if active_id == listing_id or active_id == raw_listing_id:
            return opportunity

    return None


def build_current_opportunity_snapshots(opportunities):
    snapshots = {}
    for index, opportunity in enumerate(opportunities):
        raw_listing_id = first_not_none(opportunity.get("listing_id"), "listing")
        listing_id = f"{raw_listing_id}_{index}"
        precio, score = get_opportunity_price_score(opportunity)
        snapshot = {
            "listing_id": listing_id,
            "precio": precio,
            "score": score,
            "opportunity": opportunity,
        }
        snapshots[listing_id] = snapshot
        snapshots[str(raw_listing_id)] = snapshot

    return snapshots


def render_portfolio_header(items):
    total = len(items)
    precios = [
        safe_float(item.get("precio_guardado"))
        for item in items
        if safe_float(item.get("precio_guardado")) is not None
    ]
    descuentos = [
        safe_float(item.get("descuento"))
        for item in items
        if safe_float(item.get("descuento")) is not None
    ]
    strong_signals = sum(
        1
        for item in items
        if (safe_float(item.get("score_guardado")) or 0) >= 70
    )

    precio_promedio = sum(precios) / len(precios) if precios else None
    descuento_promedio = sum(descuentos) / len(descuentos) if descuentos else None

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Propiedades", total)
    col2.metric("Precio prom.", format_clp(precio_promedio) if precio_promedio else "Sin dato")
    col3.metric(
        "Desc. prom.",
        f"{descuento_promedio * 100:.1f}%" if descuento_promedio is not None else "Sin dato",
    )
    col4.metric("Señales fuertes", strong_signals)


def render_portfolio_performance(items):
    changes = []
    for item in items:
        precio_guardado = safe_float(item.get("precio_guardado"))
        precio_actual = safe_float(item.get("precio_actual"))
        if precio_guardado and precio_actual:
            changes.append((precio_actual - precio_guardado) / precio_guardado)

    change_pct = sum(changes) / len(changes) if changes else None

    st.subheader("Performance")
    st.metric(
        "Cambio promedio",
        f"{change_pct * 100:+.1f}%" if change_pct is not None else "Sin dato",
    )


def compute_status(item):
    precio_guardado = safe_float(item.get("precio_guardado"))
    precio_actual = safe_float(item.get("precio_actual"))
    score_guardado = safe_float(item.get("score_guardado"))
    score_actual = safe_float(item.get("score_actual"))

    precio_baja = precio_guardado is not None and precio_actual is not None and precio_actual < precio_guardado
    score_sube = score_guardado is not None and score_actual is not None and score_actual > score_guardado
    precio_sin_cambios = precio_guardado is None or precio_actual is None or precio_actual == precio_guardado
    score_sin_cambios = score_guardado is None or score_actual is None or score_actual == score_guardado

    if precio_baja or score_sube:
        return "Improving"
    if precio_sin_cambios and score_sin_cambios:
        return "Stable"
    return "Deteriorating"


def render_top_movers(items):
    scored_items = [
        item
        for item in items
        if safe_float(item.get("score_actual")) is not None
    ]

    best = max(scored_items, key=lambda item: safe_float(item.get("score_actual")), default=None)
    worst = min(scored_items, key=lambda item: safe_float(item.get("score_actual")), default=None)

    improved_items = [
        item
        for item in items
        if safe_float(item.get("score_actual")) is not None
        and safe_float(item.get("score_guardado")) is not None
    ]
    most_improved = max(
        improved_items,
        key=lambda item: safe_float(item.get("score_actual")) - safe_float(item.get("score_guardado")),
        default=None,
    )

    col1, col2, col3 = st.columns(3)
    col1.metric("Mejor actual", best.get("listing_id") if best else "Sin dato")
    col2.metric("Mayor mejora", most_improved.get("listing_id") if most_improved else "Sin dato")
    col3.metric("Mayor riesgo", worst.get("listing_id") if worst else "Sin dato")


def render_portfolio_card(item):
    listing_id = item.get("listing_id")
    opportunity = item.get("opportunity") or {
        "listing_id": listing_id,
        "precio_publicado": item.get("precio_actual") or item.get("precio_guardado"),
        "investment_score": item.get("score_actual") or item.get("score_guardado"),
        "comuna": item.get("comuna") or "Sin comuna",
    }
    opportunity["listing_id"] = listing_id

    comuna = opportunity.get("comuna") or item.get("comuna") or "Sin comuna"
    m2 = first_not_none(opportunity.get("m2"), opportunity.get("m2_construidos"), item.get("m2"))
    precio_guardado = safe_float(item.get("precio_guardado"))
    precio_actual = safe_float(item.get("precio_actual"))
    score_guardado = safe_float(item.get("score_guardado"))
    score_actual = safe_float(item.get("score_actual"))
    status = compute_status(item)

    st.markdown(f"### {comuna} · {format_m2(m2) if m2 else 'Sin m2'}")
    st.write(
        "Precio: "
        f"{format_clp(precio_guardado) if precio_guardado else 'Sin dato'} → "
        f"{format_clp(precio_actual) if precio_actual else 'No disponible'}"
    )
    score_guardado_text = f"{score_guardado:.0f}" if score_guardado is not None else "Sin dato"
    score_actual_text = f"{score_actual:.0f}" if score_actual is not None else "No disponible"
    st.write(f"Score: {score_guardado_text} → {score_actual_text}")

    if status == "Improving":
        st.success("Improving")
    elif status == "Stable":
        st.warning("Stable")
    else:
        st.error("Deteriorating")

    col1, col2, col3 = st.columns(3)
    with col1:
        report_url = f"?report_id={listing_id}"
        st.markdown(
            f'<a href="{report_url}" target="_blank">Ver Risk Report</a>',
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            f'<a href="{report_url}" target="_blank">Risk Report V2</a>',
            unsafe_allow_html=True,
        )
    with col3:
        if st.button("❌ Quitar", key=f"portfolio_remove_{listing_id}"):
            remove_saved_listing(listing_id)
            st.rerun()

    st.markdown("---")


def mostrar_tracking_portfolio(items):
    st.title("📊 Portfolio de Oportunidades")

    if not items:
        st.info("No tienes propiedades guardadas")
        return

    render_portfolio_header(items)
    render_portfolio_performance(items)
    render_top_movers(items)

    st.subheader("Posiciones")

    for item in items:
        render_portfolio_card(item)


def mostrar_tracking(opportunities=None):
    opportunities = opportunities or []
    saved_listings = get_saved_listings()
    st.markdown("## 📊 Mis oportunidades")
    modo_avanzado = st.toggle("Vista Portafolio", key="tracker_portfolio_toggle")
    if modo_avanzado:
        mostrar_tracking_portfolio(saved_listings)
        return

    if not saved_listings:
        st.info("Aún no tienes oportunidades guardadas.")
        return

    current_snapshots = build_current_opportunity_snapshots(opportunities)

    for saved in saved_listings:
        listing_id = saved["listing_id"]
        current = current_snapshots.get(saved["listing_id"], {})
        precio_guardado = safe_float(saved.get("precio_guardado"))
        score_guardado = safe_float(saved.get("score_guardado"))
        precio_actual = safe_float(current.get("precio"))
        score_actual = safe_float(current.get("score"))
        opportunity = current.get("opportunity") or {
            "listing_id": listing_id,
            "precio_publicado": precio_guardado,
            "investment_score": score_guardado,
            "score": score_guardado,
            "comuna": "Sin comuna",
        }
        if not opportunity.get("precio_publicado") and precio_guardado:
            opportunity["precio_publicado"] = precio_guardado
        if opportunity.get("investment_score") is None and score_guardado is not None:
            opportunity["investment_score"] = score_guardado
        opportunity["listing_id"] = listing_id
        if precio_actual is None:
            precio_actual = safe_float(opportunity.get("precio_publicado"))
        if score_actual is None:
            score_actual = safe_float(opportunity.get("investment_score"))
        comuna = opportunity.get("comuna") or "Sin comuna"
        m2 = first_not_none(opportunity.get("m2"), opportunity.get("m2_construidos"))

        price_change_pct = None
        if precio_guardado and precio_actual:
            price_change_pct = (precio_actual - precio_guardado) / precio_guardado

        score_change = None
        if score_guardado is not None and score_actual is not None:
            score_change = score_actual - score_guardado

        tags = []
        if price_change_pct is not None and price_change_pct < 0:
            tags.append("🔥 Bajó precio")
        if score_change is not None and score_change > 0:
            tags.append("⬆️ Mejor oportunidad")
        if not tags:
            tags.append("Sin cambios")

        with st.container():
            st.markdown(f"### {comuna} · `{listing_id}`")
            st.caption(f"Superficie: {format_m2(m2) if m2 else 'Sin dato'} | Guardado: {saved['saved_at']}")

            cols = st.columns(5)
            with cols[0]:
                st.metric("Precio actual", format_clp(precio_actual) if precio_actual else "No disponible")
            with cols[1]:
                st.metric("Precio anterior", format_clp(precio_guardado) if precio_guardado else "Sin dato")
            with cols[2]:
                if price_change_pct is None:
                    st.metric("Cambio precio", "Sin dato")
                else:
                    st.metric("Cambio precio", f"{price_change_pct * 100:+.1f}%")
            with cols[3]:
                if score_actual is None:
                    st.metric("Score actual", "No disponible")
                else:
                    score_delta = f"{score_change:+.0f}" if score_change is not None else None
                    st.metric("Score actual", f"{score_actual:.0f}", score_delta)
            with cols[4]:
                st.metric("Score anterior", f"{score_guardado:.0f}" if score_guardado is not None else "Sin dato")

            st.caption(" · ".join(tags))

            action_cols = st.columns(3)
            with action_cols[0]:
                report_url = f"?report_id={listing_id}"
                st.markdown(
                    f'<a href="{report_url}" target="_blank">Ver Risk Report</a>',
                    unsafe_allow_html=True,
                )
            with action_cols[1]:
                st.markdown(
                    f'<a href="{report_url}" target="_blank">Risk Report V2</a>',
                    unsafe_allow_html=True,
                )
            with action_cols[2]:
                if st.button("❌ Quitar", key=f"remove_{listing_id}"):
                    remove_saved_listing(listing_id)
                    st.rerun()

            st.divider()


def mostrar_inversion(opportunities):
    selected_report = st.session_state.get("selected_investment_report")

    for index, opportunity in enumerate(opportunities):
        link = opportunity.get("link") or opportunity.get("url")
        raw_listing_id = first_not_none(opportunity.get("listing_id"), "listing")
        listing_id = f"{raw_listing_id}_{index}"
        view_listing_key = f"view_listing_logged_{listing_id}"

        if view_listing_key not in st.session_state:
            track(
                "view_listing",
                {
                    "listing_id": listing_id,
                    "comuna": opportunity.get("comuna"),
                },
            )
            st.session_state[view_listing_key] = True

        with st.container():
            render_investment_quick_panel(opportunity, index, listing_id)
            render_legal_ownership_risk(opportunity, listing_id)

            if link:
                st.markdown(f"[Ver publicacion]({link})")

        st.divider()

    active_id = st.session_state.get("active_risk_report")
    if active_id:
        selected_opportunity = find_opportunity_by_id(opportunities, active_id)
        if selected_opportunity:
            st.markdown("## Risk Report")
            if st.button("Cerrar Reporte", key=f"close_risk_report_{active_id}"):
                st.session_state.pop("active_risk_report", None)
                return
            open_risk_report(selected_opportunity, active_id)
        else:
            st.session_state.pop("active_risk_report", None)
            active_id = None

    selected_report = st.session_state.get("selected_investment_report", selected_report)
    if selected_report and not active_id:
        st.markdown("## Risk Report")
        selected_listing_id = selected_report["listing_id"]
        selected_opportunity = selected_report["opportunity"]
        report_url = f"?report_id={selected_listing_id}"
        st.markdown(
            f'<a href="{report_url}" target="_blank">Abrir Risk Report V2</a>',
            unsafe_allow_html=True,
        )

        render_risk_report(
            selected_opportunity,
            selected_listing_id,
        )


def render_risk_return_matrix(opportunities, key_suffix=""):
    if not opportunities:
        return

    x_roi = []
    y_confidence = []
    sizes = []
    colors = []
    labels = []

    for opportunity in opportunities:
        investment_score = opportunity.get("investment_score", 0) or 0
        roi = opportunity.get("roi", 0) or 0
        confidence = (opportunity.get("confidence_score", 0) or 0) * 100
        comuna = opportunity.get("comuna") or "Sin comuna"
        veredicto = opportunity.get("veredicto") or "Evitar"

        x_roi.append(roi)
        y_confidence.append(confidence)
        sizes.append(max(investment_score / 2, 8))
        colors.append(get_veredicto_color(veredicto))
        labels.append(
            f"{comuna}<br>"
            f"ROI: {roi:.1f}%<br>"
            f"Confianza: {confidence:.0f}%<br>"
            f"Score: {investment_score}/100<br>"
            f"Veredicto: {veredicto}"
        )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=x_roi,
            y=y_confidence,
            mode="markers",
            marker={
                "size": sizes,
                "color": colors,
                "opacity": 0.82,
                "line": {"width": 1, "color": "#f5f5f5"},
            },
            text=labels,
            hoverinfo="text",
        )
    )
    fig.add_vline(
        x=6,
        line_width=2,
        line_dash="dash",
        line_color="#ffd166",
        annotation_text="ROI 6%",
    )
    fig.add_hline(
        y=70,
        line_width=2,
        line_dash="dash",
        line_color="#8bd3c7",
        annotation_text="Confianza 70%",
    )
    fig.update_layout(
        title="Risk vs Return",
        height=360,
        margin={"l": 12, "r": 12, "t": 46, "b": 34},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": "#e6e6e6"},
        xaxis_title="ROI estimado (%)",
        yaxis_title="Confianza (%)",
        showlegend=False,
    )
    fig.update_xaxes(gridcolor="#2a303b", zerolinecolor="#3a414f")
    fig.update_yaxes(range=[0, 100], gridcolor="#2a303b", zerolinecolor="#3a414f")
    st.plotly_chart(
        fig,
        use_container_width=True,
        key=f"risk_return_matrix{key_suffix}",
    )


def render_discount_comparables_matrix(opportunities, key_suffix=""):
    if not opportunities:
        return

    x_discount = []
    y_comparables = []
    sizes = []
    colors = []
    labels = []

    for opportunity in opportunities:
        discount = get_radar_discount(opportunity) * 100
        comparables = first_not_none(
            opportunity.get("numero_comparables"),
            opportunity.get("comparable_count"),
            0,
        )
        investment_score = opportunity.get("investment_score", 0) or 0
        comuna = opportunity.get("comuna") or "Sin comuna"
        veredicto = opportunity.get("veredicto") or "Evitar"

        x_discount.append(discount)
        y_comparables.append(comparables)
        sizes.append(max(investment_score / 2, 8))
        colors.append(get_veredicto_color(veredicto))
        labels.append(
            f"{comuna}<br>"
            f"Descuento: {discount:.1f}%<br>"
            f"Comparables: {comparables}<br>"
            f"Score: {investment_score}/100<br>"
            f"Veredicto: {veredicto}"
        )

    max_discount = max(max(x_discount, default=15), 20)
    max_comparables = max(max(y_comparables, default=5), 8)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=x_discount,
            y=y_comparables,
            mode="markers",
            marker={
                "size": sizes,
                "color": colors,
                "opacity": 0.82,
                "line": {"width": 1, "color": "#f5f5f5"},
            },
            text=labels,
            hoverinfo="text",
        )
    )
    fig.add_vline(
        x=15,
        line_width=2,
        line_dash="dash",
        line_color="#ffd166",
        annotation_text="15% descuento",
    )
    fig.add_hline(
        y=5,
        line_width=2,
        line_dash="dash",
        line_color="#8bd3c7",
        annotation_text="5 comparables",
    )
    fig.add_annotation(
        x=max_discount * 0.82,
        y=max_comparables * 0.86,
        text="Oportunidad real",
        showarrow=False,
        font={"color": "#06d6a0"},
    )
    fig.add_annotation(
        x=max_discount * 0.82,
        y=max_comparables * 0.18,
        text="Posible error",
        showarrow=False,
        font={"color": "#ff6b6b"},
    )
    fig.add_annotation(
        x=max_discount * 0.18,
        y=max_comparables * 0.86,
        text="Estable",
        showarrow=False,
        font={"color": "#8bd3c7"},
    )
    fig.add_annotation(
        x=max_discount * 0.18,
        y=max_comparables * 0.18,
        text="Irrelevante",
        showarrow=False,
        font={"color": "#9ca3af"},
    )
    fig.update_layout(
        title="Descuento vs Comparables",
        height=360,
        margin={"l": 12, "r": 12, "t": 46, "b": 34},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": "#e6e6e6"},
        xaxis_title="Descuento (%)",
        yaxis_title="Numero de comparables",
        showlegend=False,
    )
    fig.update_xaxes(
        range=[0, max_discount * 1.08],
        gridcolor="#2a303b",
        zerolinecolor="#3a414f",
    )
    fig.update_yaxes(
        range=[0, max_comparables * 1.15],
        gridcolor="#2a303b",
        zerolinecolor="#3a414f",
    )
    st.plotly_chart(
        fig,
        use_container_width=True,
        key=f"discount_comparables_matrix{key_suffix}",
    )


def get_opportunity_listing_price(opportunity):
    return first_not_none(
        opportunity.get("precio_listado"),
        opportunity.get("listing_price"),
        opportunity.get("precio_publicado"),
        0,
    )


def filtrar_oportunidades_radar(
    opportunities,
    min_score,
    max_price,
    min_conf,
    selected_comuna,
):
    filtradas = []

    for opportunity in opportunities:
        precio_listado = get_opportunity_listing_price(opportunity)

        if opportunity.get("investment_score", 0) < min_score:
            continue

        if precio_listado > max_price:
            continue

        if (opportunity.get("confidence_score", 0) * 100) < min_conf:
            continue

        if selected_comuna != "Todas" and opportunity.get("comuna") != selected_comuna:
            continue

        filtradas.append(opportunity)

    return filtradas


def confidence_label_es(confidence_level):
    return {
        "low": "baja",
        "medium": "media",
        "high": "alta",
    }.get(confidence_level or "low", "baja")


def render_price_chart(listing_id):
    with engine.begin() as connection:
        rows = connection.exec_driver_sql(
            """
            SELECT
                fecha_captura AS fecha,
                COALESCE(precio_clp_nuevo, precio_clp) AS precio
            FROM price_history
            WHERE listing_id = ?
            ORDER BY fecha_captura ASC, fecha_cambio ASC
            """,
            (listing_id,),
        ).fetchall()

    data = [
        {"fecha": row[0], "precio": row[1]}
        for row in rows
        if row[0] is not None and row[1] is not None
    ]

    if not data:
        st.caption("Sin historial de precios")
        return

    df = pd.DataFrame(data)
    df["fecha"] = pd.to_datetime(df["fecha"])
    df = df.sort_values("fecha")

    st.line_chart(df.set_index("fecha")["precio"])


def calcular_estimacion_relajada(db, property_data):
    from comparables import buscar_comparables, obtener_precio_clp

    m2_objetivo = safe_float(property_data.get("m2_construidos"))
    if not is_positive_number(m2_objetivo):
        return None

    comparables_relajados = buscar_comparables(
        db,
        property_data.get("comuna"),
        m2_objetivo,
        dormitorios=None,
        banos=None,
        estacionamientos=None,
        m2_range_ratio=0.50,
        max_candidates=50,
        allow_adjacent_segments=True,
    )

    if not comparables_relajados:
        comparables_relajados = buscar_comparables(
            db,
            None,
            m2_objetivo,
            dormitorios=None,
            banos=None,
            estacionamientos=None,
            m2_range_ratio=0.50,
            max_candidates=50,
            allow_adjacent_segments=True,
        )

    precios_m2 = []
    for comparable in comparables_relajados:
        precio = obtener_precio_clp(comparable)
        superficie = safe_float(getattr(comparable, "m2_construidos", None))
        if is_positive_number(precio) and is_positive_number(superficie):
            precios_m2.append(precio / superficie)

    if not precios_m2:
        return None

    precio_m2_promedio = sum(precios_m2) / len(precios_m2)
    return {
        "valor_estimado": precio_m2_promedio * m2_objetivo,
        "precio_m2_promedio": precio_m2_promedio,
        "comparables": len(precios_m2),
    }


if not DEMO_MODE:
    initialize_app_data()

st.markdown(
    """
    <style>
        :root {
            --color-bg: #0e1117;
            --color-surface: #151922;
            --color-surface-soft: #1a1f2a;
            --color-surface-muted: #11151d;
            --color-border: #2a303b;
            --color-border-strong: #3a414f;
            --color-text: #e6e6e6;
            --color-heading: #f5f5f5;
            --color-muted: #9ca3af;
            --color-muted-strong: #c0c6d0;
            --color-accent: #8bd3c7;
            --color-accent-bg: #142520;
            --color-accent-soft: #19322c;
            --color-accent-border: #2d5c52;
            --color-positive: #8bd3c7;
            --color-positive-bg: #142520;
            --color-positive-soft: #19322c;
            --color-positive-border: #2d5c52;
            --color-warning: #d7bd7f;
            --color-warning-bg: #282416;
            --color-warning-soft: #332d1b;
            --color-warning-border: #5b4d28;
            --color-danger: #d98b8b;
            --color-danger-bg: #2a1a1d;
            --color-danger-soft: #3a2226;
            --color-info-bg: #172131;
            --color-info-border: #2f4058;
            --radius-card: 8px;
            --radius-pill: 8px;
            --shadow-card: 0 0 0 rgba(0, 0, 0, 0);
            --shadow-soft: 0 0 0 rgba(0, 0, 0, 0);
            --space-card: 20px;
            --space-sm: 10px;
            --space-md: 14px;
            --space-lg: 22px;
            --font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            --text-xs: 0.78rem;
            --text-sm: 0.84rem;
            --text-md: 0.9rem;
            --text-lg: 1.02rem;
            --text-xl: 1.35rem;
            --text-hero: 4.8rem;
        }

        html, body, [class*="stApp"] {
            background: var(--color-bg);
            color: var(--color-text);
            font-family: var(--font-family);
        }

        .stApp,
        [data-testid="stAppViewContainer"],
        [data-testid="stHeader"] {
            background: var(--color-bg);
        }

        [data-testid="stSidebar"],
        [data-testid="stToolbar"],
        [data-testid="stDecoration"] {
            background: var(--color-bg);
        }

        .block-container {
            max-width: 1180px;
            padding-top: 2.35rem;
            padding-bottom: 3.5rem;
        }

        h1, h2, h3 {
            letter-spacing: 0;
        }

        div[data-testid="stVerticalBlockBorderWrapper"] {
            border: 1px solid var(--color-border);
            border-radius: var(--radius-card);
            box-shadow: var(--shadow-card);
            background: var(--color-surface);
            padding: 2px;
            transition: transform 160ms ease, box-shadow 160ms ease, border-color 160ms ease;
        }

        div[data-testid="stVerticalBlockBorderWrapper"]:hover {
            border-color: var(--color-border);
            box-shadow: none;
        }

        div[data-testid="stMetric"] {
            background: var(--color-surface);
            border: 1px solid var(--color-border);
            border-radius: var(--radius-card);
            padding: var(--space-card);
            box-shadow: var(--shadow-soft);
        }

        div[data-testid="stMetricLabel"] p {
            color: var(--color-muted);
            font-size: var(--text-sm);
        }

        div[data-testid="stMetricValue"] {
            color: var(--color-heading);
            font-weight: 750;
        }

        div.stButton > button {
            border-radius: var(--radius-card);
            border: 1px solid var(--color-accent-border);
            background: var(--color-accent-bg);
            color: var(--color-accent);
            font-weight: 750;
            min-height: 38px;
            font-size: 0.88rem;
            box-shadow: var(--shadow-soft);
            transition: transform 140ms ease, box-shadow 140ms ease, background 140ms ease;
        }

        div.stButton > button:hover {
            border-color: var(--color-accent);
            background: var(--color-accent-soft);
            color: var(--color-accent);
            transform: none;
            box-shadow: none;
        }

        div.stButton > button:active {
            transform: translateY(0);
            box-shadow: none;
        }

        label,
        [data-testid="stWidgetLabel"],
        [data-testid="stMarkdownContainer"] p {
            color: var(--color-text);
        }

        [data-testid="stWidgetLabel"] {
            min-height: auto;
            margin-bottom: 2px;
        }

        [data-testid="stWidgetLabel"] p {
            color: var(--color-muted);
            font-size: 0.76rem;
            font-weight: 700;
            line-height: 1.2;
        }

        div[data-testid="stSelectbox"],
        div[data-testid="stNumberInput"],
        div[data-testid="stCheckbox"],
        div[data-testid="stRadio"] {
            margin-bottom: 0.35rem;
        }

        div[data-baseweb="select"] > div,
        div[data-baseweb="input"] input,
        div[data-baseweb="base-input"],
        div[data-testid="stNumberInput"] input {
            background: var(--color-surface-muted);
            border-color: var(--color-border);
            color: var(--color-text);
            min-height: 34px;
            font-size: 0.88rem;
        }

        div[data-baseweb="select"] > div,
        div[data-baseweb="base-input"] {
            min-height: 34px;
        }

        div[data-testid="stNumberInput"] input {
            padding: 4px 10px;
        }

        div[data-baseweb="select"] span,
        div[data-testid="stNumberInput"] input {
            color: var(--color-text);
        }

        div[data-testid="stAlert"] {
            background: var(--color-surface-soft);
            border: 1px solid var(--color-border);
            color: var(--color-text);
        }

        div[data-testid="stAlert"] p {
            color: var(--color-muted-strong);
        }

        div[data-baseweb="select"] > div:hover,
        div[data-baseweb="input"] input:hover,
        div[data-testid="stNumberInput"] input:hover {
            border-color: var(--color-border-strong);
        }

        div[data-baseweb="popover"],
        div[data-baseweb="menu"] {
            background: var(--color-surface);
            border: 1px solid var(--color-border);
            color: var(--color-text);
        }

        div[role="option"],
        div[role="listbox"] {
            background: var(--color-surface);
            color: var(--color-text);
        }

        div[role="option"]:hover {
            background: var(--color-surface-soft);
        }

        div[data-testid="stRadio"] label,
        div[data-testid="stCheckbox"] label,
        div[data-testid="stRadio"] p,
        div[data-testid="stCheckbox"] p {
            color: var(--color-text);
        }

        div[data-testid="stRadio"] [role="radiogroup"] {
            background: var(--color-surface-muted);
            border: 1px solid var(--color-border);
            border-radius: var(--radius-card);
            padding: 5px 8px;
        }

        div[data-testid="stCheckbox"] {
            background: var(--color-surface-muted);
            border: 1px solid var(--color-border);
            border-radius: var(--radius-card);
            padding: 5px 8px;
        }

        input,
        textarea,
        select {
            caret-color: var(--color-text);
        }

        .eyebrow {
            color: var(--color-muted-strong);
            font-size: var(--text-md);
            font-weight: 700;
            letter-spacing: 0;
            text-transform: uppercase;
            margin-bottom: 4px;
        }

        .page-title {
            color: var(--color-heading);
            font-size: 2.18rem;
            line-height: 1.12;
            font-weight: 800;
            margin: 0 0 var(--space-sm) 0;
        }

        .page-subtitle {
            color: var(--color-muted-strong);
            font-size: 0.98rem;
            line-height: 1.55;
            max-width: 760px;
            margin-bottom: 1.85rem;
        }

        .section-title {
            color: var(--color-heading);
            font-size: var(--text-lg);
            font-weight: 750;
            margin-bottom: 6px;
        }

        .section-copy {
            color: var(--color-muted);
            font-size: var(--text-md);
            line-height: 1.5;
            margin-bottom: var(--space-lg);
        }

        .input-group-label {
            color: var(--color-muted-strong);
            font-size: 0.72rem;
            font-weight: 850;
            letter-spacing: 0;
            text-transform: uppercase;
            margin: 12px 0 6px;
        }

        .input-helper {
            color: var(--color-muted);
            font-size: 0.82rem;
            line-height: 1.35;
            margin-bottom: 10px;
        }

        .comparable-card {
            background: var(--color-surface);
            border: 1px solid var(--color-border);
            border-radius: var(--radius-card);
            box-shadow: var(--shadow-soft);
            padding: 14px var(--space-card);
            margin-bottom: var(--space-md);
            transition: transform 160ms ease, box-shadow 160ms ease, border-color 160ms ease;
        }

        .comparable-card:hover {
            transform: translateY(-2px);
            border-color: var(--color-border-strong);
            box-shadow: none;
        }

        .comparable-title {
            font-weight: 750;
            color: var(--color-text);
            margin-bottom: 6px;
        }

        .comparable-price {
            font-size: var(--text-lg);
            font-weight: 800;
            color: var(--color-positive);
            margin-bottom: var(--space-sm);
        }

        .comparable-meta {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            color: var(--color-muted);
            font-size: 0.9rem;
        }

        .status-note {
            color: var(--color-muted);
            font-size: 0.88rem;
            margin-top: var(--space-md);
        }

        .result-hero {
            text-align: center;
            min-height: 260px;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            padding: 52px 34px 46px;
            margin: 10px 0 28px;
            border: 1px solid var(--color-border-strong);
            border-radius: var(--radius-card);
            background: var(--color-surface-muted);
            animation: resultFadeIn 360ms ease both;
        }

        .result-layer-stack {
            display: grid;
            gap: 22px;
            margin: 10px 0 30px;
        }

        .result-layer-stack .result-hero {
            min-height: 255px;
            margin: 0;
        }

        .insight-layer {
            border: 1px solid var(--color-border);
            border-radius: var(--radius-card);
            background: var(--color-surface);
            padding: 18px 20px;
            animation: resultFadeIn 420ms ease both;
        }

        .insight-header {
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 10px;
        }

        .insight-label {
            color: var(--color-muted);
            font-size: var(--text-xs);
            font-weight: 850;
            text-transform: uppercase;
        }

        .result-label {
            color: var(--color-muted);
            font-size: var(--text-xs);
            font-weight: 750;
            text-transform: uppercase;
            margin-bottom: 14px;
        }

        .result-value {
            color: var(--color-heading);
            font-size: var(--text-hero);
            line-height: 1;
            font-weight: 850;
            margin-bottom: 14px;
        }

        .market-state-badge {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            border-radius: var(--radius-pill);
            border: 1px solid var(--color-border);
            background: var(--color-surface-soft);
            color: var(--color-muted-strong);
            font-size: 0.78rem;
            font-weight: 850;
            letter-spacing: 0;
            line-height: 1;
            padding: 7px 10px;
            margin-bottom: 0;
            text-transform: uppercase;
        }

        .market-state-badge.neutral {
            color: var(--color-positive);
            background: var(--color-positive-bg);
            border-color: var(--color-positive-border);
        }

        .market-state-badge.low {
            color: var(--color-muted-strong);
            background: var(--color-info-bg);
            border-color: var(--color-info-border);
        }

        .market-state-badge.high {
            color: var(--color-warning);
            background: var(--color-warning-bg);
            border-color: var(--color-warning-border);
        }

        .result-support {
            display: flex;
            justify-content: center;
            gap: 28px;
            flex-wrap: wrap;
            color: var(--color-muted-strong);
            font-size: 0.86rem;
            font-weight: 450;
        }

        .result-support strong {
            color: var(--color-text);
            font-weight: 650;
        }

        .layer-heading {
            margin: 32px 0 14px;
        }

        .layer-heading .section-copy {
            margin-bottom: 0;
        }

        .interpretation-card {
            border-radius: var(--radius-card);
            padding: 14px var(--space-card);
            margin: -4px 0 var(--space-lg);
            border: 1px solid var(--color-border);
            animation: resultFadeIn 420ms ease both;
            transition: transform 160ms ease, box-shadow 160ms ease;
        }

        .interpretation-card:hover {
            transform: translateY(-1px);
            box-shadow: var(--shadow-soft);
        }

        .interpretation-card.interpretation-inline {
            margin: 0 18px 0;
            border-radius: var(--radius-card);
            box-shadow: none;
        }

        .interpretation-card.low {
            background: var(--color-info-bg);
            border-color: var(--color-info-border);
        }

        .interpretation-card.neutral {
            background: var(--color-positive-bg);
            border-color: var(--color-positive-border);
        }

        .interpretation-card.high {
            background: var(--color-warning-bg);
            border-color: var(--color-warning-border);
        }

        .interpretation-copy {
            color: var(--color-muted-strong);
            font-size: 0.9rem;
            line-height: 1.4;
        }

        .auto-summary {
            background: var(--color-surface);
            border: 1px solid var(--color-border);
            border-radius: var(--radius-card);
            padding: 16px var(--space-card);
            margin-bottom: var(--space-lg);
            animation: resultFadeIn 480ms ease both;
            transition: transform 160ms ease, box-shadow 160ms ease, border-color 160ms ease;
        }

        .auto-summary:hover {
            transform: translateY(-1px);
            border-color: var(--color-border-strong);
            box-shadow: var(--shadow-soft);
        }

        .auto-summary.auto-summary-inline {
            margin: 12px 18px 18px;
            box-shadow: none;
        }

        .insight-layer .auto-summary.auto-summary-inline {
            margin: 12px 0 0;
            background: var(--color-surface-soft);
        }

        .auto-summary-title {
            color: var(--color-text);
            font-size: 0.95rem;
            font-weight: 850;
            margin-bottom: 5px;
        }

        .auto-summary-copy {
            color: var(--color-muted-strong);
            font-size: var(--text-md);
            line-height: 1.45;
        }

        .key-metric-card {
            min-height: 102px;
            background: var(--color-surface);
            border: 1px solid var(--color-border);
            border-radius: var(--radius-card);
            box-shadow: var(--shadow-soft);
            padding: 18px 16px 16px;
            margin-bottom: var(--space-lg);
            transition: transform 160ms ease, box-shadow 160ms ease, border-color 160ms ease;
        }

        .key-metric-card:hover {
            transform: none;
            border-color: var(--color-border);
            box-shadow: none;
        }

        .key-metric-title {
            color: var(--color-muted);
            font-size: 0.72rem;
            font-weight: 800;
            text-transform: uppercase;
            line-height: 1.25;
            margin-top: 8px;
            margin-bottom: 0;
        }

        .key-metric-value {
            color: var(--color-heading);
            font-size: 1.68rem;
            line-height: 1.05;
            font-weight: 850;
            margin-bottom: 0;
        }

        .key-metric-copy {
            color: var(--color-muted);
            font-size: 0.78rem;
            line-height: 1.25;
            margin-top: 5px;
        }

        .comparables-summary {
            background: var(--color-surface);
            border: 1px solid var(--color-border);
            border-radius: var(--radius-card);
            box-shadow: var(--shadow-soft);
            padding: 18px var(--space-card);
            margin: 8px 0 28px;
            animation: resultFadeIn 360ms ease both;
            transition: transform 160ms ease, box-shadow 160ms ease, border-color 160ms ease;
        }

        .comparables-summary:hover {
            transform: none;
            border-color: var(--color-border);
            box-shadow: none;
        }

        .comparables-summary-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: var(--space-md);
            margin-bottom: var(--space-md);
        }

        .comparables-summary-header .section-copy {
            margin-bottom: 0;
        }

        .summary-status {
            display: inline-flex;
            align-items: center;
            border: 1px solid var(--color-info-border);
            border-radius: var(--radius-pill);
            background: var(--color-info-bg);
            color: var(--color-muted-strong);
            font-size: var(--text-xs);
            font-weight: 800;
            padding: 5px 8px;
            white-space: nowrap;
        }

        .comparables-summary-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 12px;
        }

        .summary-item {
            background: var(--color-surface-soft);
            border: 1px solid var(--color-border);
            border-radius: var(--radius-card);
            padding: 14px;
            min-height: 92px;
        }

        .summary-label {
            color: var(--color-muted);
            font-size: var(--text-xs);
            font-weight: 800;
            text-transform: uppercase;
            margin-bottom: 8px;
        }

        .summary-value {
            color: var(--color-heading);
            font-size: 1.05rem;
            line-height: 1.25;
            font-weight: 850;
        }

        .scatter-card {
            background: var(--color-surface);
            border: 1px solid var(--color-border);
            border-radius: var(--radius-card);
            box-shadow: var(--shadow-soft);
            padding: var(--space-card);
            margin: 4px 0 var(--space-lg);
            transition: transform 160ms ease, box-shadow 160ms ease, border-color 160ms ease;
        }

        .scatter-card:hover {
            transform: translateY(-1px);
            border-color: var(--color-border-strong);
            box-shadow: none;
        }

        .scatter-header {
            display: flex;
            justify-content: space-between;
            gap: var(--space-md);
            align-items: flex-start;
            margin-bottom: 4px;
        }

        .scatter-header .section-copy {
            margin-bottom: 0;
        }

        .scatter-legend {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            color: var(--color-muted);
            font-size: var(--text-xs);
            white-space: nowrap;
        }

        .legend-dot {
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 999px;
            margin-right: 4px;
        }

        .legend-dot.market {
            background: var(--color-muted);
        }

        .legend-dot.target {
            background: var(--color-accent);
        }

        .scatter-svg {
            width: 100%;
            height: auto;
            display: block;
            margin-top: 6px;
        }

        .axis-line {
            stroke: var(--color-border-strong);
            stroke-width: 1;
        }

        .axis-label {
            fill: var(--color-muted);
            font-size: 11px;
            font-weight: 650;
        }

        .scatter-point {
            fill: var(--color-muted);
            opacity: 0.72;
            transition: opacity 140ms ease, transform 140ms ease;
        }

        .scatter-target {
            fill: var(--color-accent);
            stroke: var(--color-surface);
            stroke-width: 3;
            filter: none;
        }

        .scatter-label {
            fill: var(--color-accent);
            font-size: 12px;
            font-weight: 850;
        }

        .factor-row {
            display: grid;
            grid-template-columns: minmax(120px, 0.9fr) minmax(160px, 1.8fr) minmax(56px, auto);
            align-items: center;
            gap: 14px;
            border: 1px solid var(--color-border);
            border-radius: var(--radius-card);
            padding: 14px 16px;
            margin-bottom: 10px;
            background: var(--color-surface);
            transition: transform 160ms ease, box-shadow 160ms ease, border-color 160ms ease;
        }

        .factor-row:hover {
            transform: none;
            border-color: var(--color-border);
            box-shadow: var(--shadow-soft);
        }

        .factor-name {
            color: var(--color-text);
            font-size: 0.9rem;
            font-weight: 800;
            line-height: 1.2;
        }

        .factor-detail {
            grid-column: 1 / -1;
            color: var(--color-muted);
            font-size: 0.76rem;
            line-height: 1.3;
            margin-top: -4px;
        }

        .factor-impact {
            font-size: 0.9rem;
            font-weight: 850;
            white-space: nowrap;
            text-align: right;
            font-variant-numeric: tabular-nums;
        }

        .factor-impact.positive {
            color: var(--color-muted-strong);
        }

        .factor-impact.negative {
            color: var(--color-muted);
        }

        .factor-track {
            height: 7px;
            background: var(--color-surface-muted);
            border-radius: var(--radius-pill);
            overflow: hidden;
            border: 1px solid var(--color-border);
        }

        .factor-bar {
            height: 100%;
            border-radius: var(--radius-pill);
            animation: growBar 520ms ease both;
            transform-origin: left center;
        }

        .factor-bar.positive {
            background: var(--color-accent);
        }

        .factor-bar.negative {
            background: var(--color-muted);
        }

        .comparables-table-wrap {
            border: 1px solid var(--color-border);
            border-radius: var(--radius-card);
            overflow: hidden;
            background: var(--color-surface);
        }

        .comparables-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }

        .comparables-table th {
            background: var(--color-surface-muted);
            color: var(--color-muted);
            font-size: var(--text-xs);
            font-weight: 800;
            text-transform: uppercase;
            text-align: left;
            padding: 12px 14px;
            border-bottom: 1px solid var(--color-border);
        }

        .comparables-table td {
            color: var(--color-text);
            padding: 13px 14px;
            border-bottom: 1px solid var(--color-border);
            vertical-align: top;
        }

        .comparables-table tbody tr {
            transition: background 120ms ease, transform 120ms ease;
        }

        .comparables-table tbody tr:hover {
            background: var(--color-surface-soft);
            transform: translateX(2px);
        }

        .comparables-table tbody tr.similar-row {
            background: var(--color-positive-bg);
        }

        .comparables-table tbody tr.similar-row:hover {
            background: var(--color-positive-soft);
        }

        .table-subtitle {
            color: var(--color-muted);
            font-size: var(--text-xs);
            margin-top: 3px;
        }

        .relevance-badge {
            display: inline-flex;
            align-items: center;
            border-radius: var(--radius-pill);
            padding: 4px 8px;
            font-size: 0.76rem;
            font-weight: 800;
            border: 1px solid transparent;
            transition: transform 140ms ease;
        }

        .relevance-badge:hover {
            transform: scale(1.03);
        }

        .relevance-badge.alta {
            color: var(--color-positive);
            background: var(--color-positive-soft);
            border-color: var(--color-positive-border);
        }

        .relevance-badge.media {
            color: var(--color-warning);
            background: var(--color-warning-soft);
            border-color: var(--color-warning-border);
        }

        .relevance-badge.baja {
            color: var(--color-muted);
            background: var(--color-surface-soft);
            border-color: var(--color-border);
        }

        @keyframes resultFadeIn {
            from {
                opacity: 0;
                transform: translateY(8px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        @keyframes growBar {
            from {
                transform: scaleX(0);
            }
            to {
                transform: scaleX(1);
            }
        }

        @media (prefers-reduced-motion: reduce) {
            *, *::before, *::after {
                animation-duration: 0.01ms !important;
                animation-iteration-count: 1 !important;
                scroll-behavior: auto !important;
                transition-duration: 0.01ms !important;
            }
        }

        @media (max-width: 900px) {
            .comparables-summary-grid {
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }

            .factor-row {
                grid-template-columns: minmax(100px, 0.9fr) minmax(120px, 1.4fr) minmax(54px, auto);
            }
        }

        @media (max-width: 560px) {
            .comparables-summary-header {
                display: block;
            }

            .summary-status {
                margin-top: 10px;
            }

            .comparables-summary-grid {
                grid-template-columns: 1fr;
            }

            .factor-row {
                grid-template-columns: 1fr;
                gap: 8px;
            }

            .factor-impact {
                text-align: left;
            }
        }
    </style>
    """,
    unsafe_allow_html=True,
)


def render_tasar():
    st.markdown(
        """
        <div class="eyebrow">Tasación inmobiliaria</div>
        <div class="page-title">Motor de Tasación Inmobiliaria</div>
        <div class="page-subtitle">
            El valor se calcula en base a comparables reales del mercado.
        </div>
        """,
        unsafe_allow_html=True,
    )
    
    input_col, result_col = st.columns([0.82, 1.58], gap="large")
    
    with input_col:
        with st.container(border=True):
            st.markdown(
                """
                <div class="section-title">Datos de la propiedad</div>
                <div class="input-helper">Completa los campos clave para estimar el valor.</div>
                """,
                unsafe_allow_html=True,
            )
    
            st.markdown('<div class="input-group-label">Ubicación</div>', unsafe_allow_html=True)
            comuna = st.selectbox("Comuna", list(COMUNA_COORDENADAS.keys()))
            direccion = st.text_input(
                "Dirección",
                placeholder="Opcional, por ahora se usa como referencia interna",
            )
            usar_coordenadas_manuales = st.checkbox("Usar lat/lon manual para pruebas")
    
            lat_manual = None
            lon_manual = None
    
            if usar_coordenadas_manuales:
                geo_col_a, geo_col_b = st.columns(2)
                with geo_col_a:
                    lat_manual = st.number_input(
                        "Latitud",
                        value=COMUNA_COORDENADAS.get(comuna, (0.0, 0.0))[0],
                        format="%.6f",
                    )
                with geo_col_b:
                    lon_manual = st.number_input(
                        "Longitud",
                        value=COMUNA_COORDENADAS.get(comuna, (0.0, 0.0))[1],
                        format="%.6f",
                    )
    
            st.markdown('<div class="input-group-label">Superficie</div>', unsafe_allow_html=True)
            surface_col_a, surface_col_b = st.columns(2)
            with surface_col_a:
                m2_construidos = st.number_input("Construida (m²)", min_value=1, value=100)
            with surface_col_b:
                m2_terreno = st.number_input("Terreno (m²)", min_value=0, value=200)
    
            st.markdown('<div class="input-group-label">Características</div>', unsafe_allow_html=True)
            detail_col_a, detail_col_b = st.columns(2)
            with detail_col_a:
                dormitorios = st.number_input("Dormitorios", min_value=1, value=3)
                estacionamientos = st.number_input("Estacionamientos", min_value=0, value=1)
            with detail_col_b:
                banos = st.number_input("Baños", min_value=1, value=2)
                ano_construccion = st.number_input("Año", min_value=1800, value=2015)
    
            piscina = st.checkbox("¿Tiene piscina?")
            permitir_estimacion_heuristica = st.checkbox(
                "Permitir estimación heurística si no hay comparables suficientes",
                value=False,
                help=(
                    "Solo para demo. No debe tratarse como una valoración AVM de mercado."
                ),
            )
            calcular = st.button("Calcular valor estimado", use_container_width=True)
    
    with result_col:
        if not calcular:
            with st.container(border=True):
                st.markdown(
                    """
                    <div class="section-title">Resultados</div>
                    <div class="section-copy">Completa los datos y calcula una tasación para ver el valor estimado.</div>
                    """,
                    unsafe_allow_html=True,
                )
                st.info("Los resultados aparecerán aquí.")
        else:
            lat_aproximada, lon_aproximada = geocode_simple(comuna, direccion)
            lat_objetivo = float(lat_manual) if usar_coordenadas_manuales else lat_aproximada
            lon_objetivo = float(lon_manual) if usar_coordenadas_manuales else lon_aproximada
    
            property_data = {
                "comuna": comuna,
                "lat": lat_objetivo,
                "lon": lon_objetivo,
                "m2_construidos": float(m2_construidos),
                "m2_terreno": float(m2_terreno),
                "dormitorios": int(dormitorios),
                "banos": int(banos),
                "estacionamientos": int(estacionamientos),
                "piscina": bool(piscina),
                "ano_construccion": int(ano_construccion),
            }
            track(
                "click_calculate_valuation",
                {
                    "comuna": comuna,
                    "m2_construidos": float(m2_construidos),
                    "dormitorios": int(dormitorios),
                    "banos": int(banos),
                },
            )
    
            with SessionLocal() as db:
                resultado_tasacion = valuation_service.get_valuation(
                    db,
                    {
                        **property_data,
                        "allow_heuristic": permitir_estimacion_heuristica,
                    },
                )
    
            if (
                resultado_tasacion is None
                or resultado_tasacion.get("valuation_status") == "insufficient_data"
            ):
                if resultado_tasacion and resultado_tasacion.get("low_data_mode"):
                    st.warning(LOW_DATA_WARNING)
                st.warning("STATUS: INSUFFICIENT DATA")
                st.info("No reliable market valuation available.")
                with SessionLocal() as db:
                    estimacion_relajada = calcular_estimacion_relajada(db, property_data)
                if estimacion_relajada:
                    st.warning("⚠️ Estimación con baja confianza")
                    st.metric(
                        "Valor estimado",
                        format_clp(estimacion_relajada["valor_estimado"]),
                    )
                    st.write("Basado en comparables menos estrictos")
                    st.caption(
                        f"Precio/m² promedio: {format_clp(estimacion_relajada['precio_m2_promedio'])} | "
                        f"Comparables relajados: {estimacion_relajada['comparables']}"
                    )
                st.markdown(
                    f"""
                    <div class="status-note">
                        Razón: {escape((resultado_tasacion or {}).get("reason", "Datos insuficientes"))}
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                st.stop()
    
            if resultado_tasacion.get("valuation_status") == "heuristic_estimate":
                valor = resultado_tasacion["valor_estimado"]
                comparables_usados = []
                rango_minimo = resultado_tasacion["rango_min"]
                rango_maximo = resultado_tasacion["rango_max"]
                precio_resultado_m2 = resultado_tasacion["precio_m2"]
            else:
                valor = resultado_tasacion["valor_estimado"]
                comparables_usados = resultado_tasacion.get("comparables", [])
                rango_minimo = first_not_none(resultado_tasacion.get("rango_min"), valor * 0.95)
                rango_maximo = first_not_none(resultado_tasacion.get("rango_max"), valor * 1.05)
                precio_resultado_m2 = first_not_none(
                    resultado_tasacion.get("precio_m2"),
                    valor / property_data["m2_construidos"],
                )
            valuation_status = resultado_tasacion.get("valuation_status", "market_comparable")
            confidence_label = resultado_tasacion.get("confidence", "low")
            low_data_mode = resultado_tasacion.get("low_data_mode", False)
            result_value_label = (
                "Estimacion heuristica"
                if valuation_status == "heuristic_estimate"
                else "Valor estimado AVM"
            )
            result_section_copy = (
                "Estimacion heuristica explicita, basada en datos limitados."
                if valuation_status == "heuristic_estimate"
                else "Estimacion calculada con comparables confiables de mercado."
            )
    
            if DEMO_MODE:
                property_id, listing_id = None, "demo"
            else:
                property_id, listing_id = save_property_listing(property_data, int(valor))
            precio_promedio_m2 = calcular_precio_promedio_m2(comparables_usados)
            superficie_promedio = calcular_superficie_promedio(comparables_usados)
            desviacion_estimada = calcular_desviacion_estimada(comparables_usados)
            factores_tasacion = calcular_factores_tasacion(
                property_data,
                comparables_usados,
            )
            score_promedio_comparables = (
                sum(comparable["score"] for comparable in comparables_usados)
                / len(comparables_usados)
                if comparables_usados
                else None
            )
            interpretacion = interpretar_resultado(precio_resultado_m2, precio_promedio_m2)
            resumen_automatico = generar_resumen_automatico(
                interpretacion,
                factores_tasacion,
            )
    
            with st.container(border=True):
                st.markdown(
                    f"""
                    <div class="section-title">Resultados de tasación</div>
                    <div class="section-copy">{result_section_copy}</div>
                    """,
                    unsafe_allow_html=True,
                )
    
                if valuation_status == "heuristic_estimate":
                    if low_data_mode:
                        st.warning(LOW_DATA_WARNING)
                    st.warning("STATUS: HEURISTIC ESTIMATE")
                    st.warning(resultado_tasacion["warning"])
                else:
                    if low_data_mode:
                        st.warning(LOW_DATA_WARNING)
                    st.success(
                        f"STATUS: MARKET COMPARABLE AVM | CONFIDENCE: {confidence_label.upper()}"
                    )
    
                st.markdown(
                    f"""
                    <div class="result-layer-stack">
                        <section class="result-hero">
                            <div class="result-label">{result_value_label}</div>
                            <div class="result-value">{format_clp(valor)}</div>
                            <div class="result-support">
                                <span>Rango estimado: <strong>{format_clp(rango_minimo)} - {format_clp(rango_maximo)}</strong></span>
                                <span>Precio por m²: <strong>{format_clp(precio_resultado_m2)}</strong></span>
                                <span>Confianza: <strong>{confidence_label.upper()}</strong></span>
                            </div>
                        </section>
                        <section class="insight-layer">
                            <div class="insight-header">
                                <span class="market-state-badge {interpretacion["clase"]}">{escape(interpretacion["estado"])}</span>
                                <span class="insight-label">Insight</span>
                            </div>
                            <div class="interpretation-copy">{escape(interpretacion["descripcion"])}</div>
                            <div class="auto-summary auto-summary-inline">
                                <div class="auto-summary-title">Resumen automático</div>
                                <div class="auto-summary-copy">{escape(resumen_automatico)}</div>
                            </div>
                        </section>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
    
                st.markdown(
                    """
                    <div class="layer-heading">
                        <div class="section-title">Métricas clave</div>
                        <div class="section-copy">Contexto rápido para leer la estimación.</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
    
                metric_cols = st.columns(4)
                with metric_cols[0]:
                    mostrar_metric_card(
                        "Precio promedio zona",
                        format_clp(precio_promedio_m2)
                        if precio_promedio_m2
                        else "Sin datos",
                    )
                with metric_cols[1]:
                    mostrar_metric_card(
                        "Cantidad de comparables",
                        str(len(comparables_usados)),
                    )
                with metric_cols[2]:
                    mostrar_metric_card(
                        "Superficie promedio",
                        format_m2(superficie_promedio),
                    )
                with metric_cols[3]:
                    mostrar_metric_card(
                        "Desviación",
                        format_percent(desviacion_estimada),
                    )

                mostrar_resumen_comparables(comparables_usados)

                st.markdown(
                    """
                    <div class="layer-heading">
                        <div class="section-title">Factores de tasación</div>
                        <div class="section-copy">Principales variables que explican la estimación.</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                for factor in factores_tasacion:
                    mostrar_factor_tasacion(factor)
    
                if comparables_usados:
                    st.markdown(
                        """
                        <div class="layer-heading">
                            <div class="section-title">Comparables utilizados</div>
                            <div class="section-copy">Publicaciones usadas directamente para calcular esta tasación.</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                    st.caption(
                        f"Comparables usados: {len(comparables_usados)}"
                        + (
                            f" | Score promedio: {score_promedio_comparables:.3f}"
                            if score_promedio_comparables is not None
                            else ""
                        )
                    )
                    mostrar_tabla_comparables(comparables_usados)
    
                st.markdown(
                    f"""
                    <div class="status-note">
                        Guardado en base de datos: property #{property_id}, listing #{listing_id}
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
    
            if not comparables_usados:
                with st.container(border=True):
                    st.warning("No se encontraron comparables válidos para mostrar.")
    
    
    if "feedback_enviado" not in st.session_state:
        st.session_state["feedback_enviado"] = False
    
    with st.expander("Feedback beta"):
        confianza = st.slider(
            "¿Confías en el valor estimado que muestra el radar?",
            1,
            5,
        )
    
        info_faltante = st.text_area(
            "¿Qué información te faltó para tomar una decisión?"
        )
    
        pago = st.radio(
            "¿Pagarías por un informe completo con datos del Conservador de Bienes Raíces?",
            ["Sí", "No", "Tal vez"],
        )
    
        if st.button("Enviar feedback") and not st.session_state["feedback_enviado"]:
            track("click_send_feedback", {"pago": pago, "confianza": confianza})
            import json
            from datetime import datetime
    
            feedback = {
                "timestamp": datetime.utcnow().isoformat(),
                "confianza": confianza,
                "info_faltante": info_faltante,
                "pago": pago,
            }
    
            if DEMO_MODE:
                st.session_state["feedback_enviado"] = True
                st.success("Gracias, tu feedback fue registrado para esta sesión demo.")
            else:
                try:
                    with open("feedback_log.jsonl", "a", encoding="utf-8") as f:
                        f.write(json.dumps(feedback, ensure_ascii=False) + "\n")
    
                    st.session_state["feedback_enviado"] = True
                    st.success("Gracias, tu feedback fue registrado.")
    
                except Exception as e:
                    st.error(f"Error al guardar el feedback: {e}")
    
        if st.session_state["feedback_enviado"]:
            st.info("Ya enviaste feedback en esta sesión.")
    
    



def render_radar():
    st.title("Oportunidades")

    with st.container(border=True):
        st.markdown(
            """
            <div class="section-title">Radar de oportunidades</div>
            <div class="section-copy">Busca propiedades activas, representativas y con datos suficientes.</div>
            """,
            unsafe_allow_html=True,
        )
        radar_limit = st.number_input(
            "Cantidad de oportunidades",
            min_value=5,
            max_value=50,
            value=20,
            step=5,
        )
        modo = st.selectbox(
            "Modo de visualización",
            ["Radar", "Análisis", "Inversión", "Tracking"],
        )
    
        mode_event_map = {
            "Radar": "view_radar",
            "Análisis": "view_analysis_mode",
            "Inversión": "view_investment_mode",
            "Tracking": "view_tracking_mode",
        }
        mode_event = mode_event_map.get(modo)
        if mode_event is None:
            if "Invers" in modo:
                mode_event = "view_investment_mode"
            else:
                mode_event = "view_analysis_mode"
        if mode_event:
            mode_logged_key = f"{mode_event}_logged"
            if mode_logged_key not in st.session_state:
                track(mode_event)
                st.session_state[mode_logged_key] = True
    
        if st.button("Ejecutar radar", use_container_width=True):
            track("click_run_radar", {"limit": int(radar_limit)})
            if DEMO_MODE:
                st.info(
                    "Modo demo: radar activo sobre la DB snapshot, sin scraping ni escrituras."
                )
            st.session_state["radar_result"] = radar_service.get_investment_opportunities(limit=int(radar_limit))
    
        radar_result = st.session_state.get("radar_result")
    
        if modo == "Tracking":
            tracking_opportunities = []
            if radar_result and radar_result.get("opportunities"):
                tracking_opportunities = sort_radar_opportunities(
                    radar_result.get("opportunities", [])
                )
            mostrar_tracking(tracking_opportunities)
        elif radar_result:
            radar_status = radar_result.get("status")
            radar_opportunities = radar_result.get("opportunities", [])
    
            if radar_status == "insufficient_data":
                if radar_result.get("low_data_mode"):
                    st.warning(LOW_DATA_WARNING)
                st.warning(
                    radar_result.get(
                        "message",
                        "No hay suficientes datos u oportunidades disponibles para el radar.",
                    )
                )
                minimum_required = (
                    MIN_ACTIVE_LISTINGS
                    if radar_result.get("low_data_mode")
                    else radar_service.get_min_dataset_size()
                )
                st.caption(
                    f"Listings listos para radar: {radar_result.get('ready_count', 0)} "
                    f"/ minimo recomendado: {minimum_required}"
                )
            elif not radar_opportunities:
                st.info("No se encontraron oportunidades con descuento suficiente.")
            else:
                opportunities = sort_radar_opportunities(radar_opportunities)
    
                if radar_result.get("low_data_mode"):
                    st.warning(LOW_DATA_WARNING)
                st.caption(
                    f"Listings listos para radar: {radar_result.get('ready_count', 0)} | "
                    f"Oportunidades: {len(opportunities)}"
                )
    
                st.markdown("**Filtros**")
                filter_cols = st.columns(4)
                with filter_cols[0]:
                    min_score = st.slider(
                        "Investment Score mínimo",
                        0,
                        100,
                        50,
                        key="radar_min_investment_score",
                    )
                with filter_cols[1]:
                    max_price = st.number_input(
                        "Precio máximo",
                        value=500_000_000,
                        key="radar_max_price",
                    )
                with filter_cols[2]:
                    min_conf = st.slider(
                        "Confianza mínima (%)",
                        0,
                        100,
                        50,
                        key="radar_min_confidence",
                    )
                with filter_cols[3]:
                    comunas = sorted(
                        {
                            opportunity.get("comuna")
                            for opportunity in opportunities
                            if opportunity.get("comuna")
                        }
                    )
                    selected_comuna = st.selectbox(
                        "Comuna",
                        ["Todas"] + comunas,
                        key="radar_comuna_filter",
                    )
                filter_payload = {
                    "min_score": min_score,
                    "max_price": max_price,
                    "min_conf": min_conf,
                    "comuna": selected_comuna,
                }
                if st.session_state.get("last_radar_filter") != filter_payload:
                    track("apply_filter", filter_payload)
                    st.session_state["last_radar_filter"] = filter_payload
    
                filtradas = filtrar_oportunidades_radar(
                    opportunities,
                    min_score,
                    max_price,
                    min_conf,
                    selected_comuna,
                )
                st.caption(
                    f"Oportunidades filtradas: {len(filtradas)} / {len(opportunities)}"
                )
    
                if not filtradas:
                    st.info("No hay oportunidades que cumplan los filtros seleccionados.")
    
                if modo == "Radar":
                    mostrar_radar_cards(filtradas)
                elif modo == "Inversión":
                    mostrar_inversion(filtradas)
                else:
                    render_risk_return_matrix(filtradas)
                    render_discount_comparables_matrix(filtradas)
                    for index, opportunity in enumerate(filtradas):
                        with st.expander(
                            analysis_option_label(opportunity, index),
                            expanded=index == 0,
                        ):
                            mostrar_analisis_detallado(opportunity, index)
    
    
    with st.container(border=True):
        st.markdown(
            """
            <div class="section-title">Buscar mejores oportunidades con presupuesto</div>
            <div class="section-copy">Encuentra propiedades activas bajo un monto máximo de compra.</div>
            """,
            unsafe_allow_html=True,
        )
        presupuesto = st.number_input(
            "Presupuesto",
            min_value=0,
            value=150_000_000,
            step=10_000_000,
            format="%d",
        )
    
        if st.button("Buscar oportunidades", use_container_width=True):
            track("click_budget_search", {"presupuesto": float(presupuesto)})
            if presupuesto <= 0:
                st.warning("Ingresa un presupuesto mayor a cero.")
            else:
                if DEMO_MODE:
                    st.info(
                        "Modo demo: búsqueda activa sobre la DB snapshot, sin scraping ni escrituras."
                    )
                opportunities = radar_service.get_best_opportunity(float(presupuesto))
    
                if not opportunities:
                    st.info(
                        "No encontramos oportunidades activas bajo ese presupuesto. "
                        "Prueba con un monto mayor o actualiza la snapshot demo antes del deploy."
                    )
                else:
                    for opportunity in opportunities:
                        confidence_level = opportunity.get("confidence_level") or "low"
    
                        if confidence_level == "low":
                            st.warning(
                                "Resultado con baja confianza: revisa los datos antes de decidir."
                            )
    
                        with st.container(border=True):
                            st.subheader(opportunity.get("comuna") or "Sin comuna")
                            metric_cols = st.columns(4)
    
                            metric_cols[0].metric(
                                "Precio publicado",
                                format_clp(opportunity.get("precio_publicado")),
                            )
                            metric_cols[1].metric(
                                "Valor estimado",
                                format_clp(opportunity.get("valor_estimado")),
                            )
                            metric_cols[2].metric(
                                "Descuento",
                                f"{opportunity.get('descuento_porcentual', 0):.1f}%",
                            )
                            metric_cols[3].metric(
                                "Confianza",
                                confidence_label_es(confidence_level).title(),
                            )
    
                            st.write(opportunity.get("explanation_text") or "")
    
                            link = opportunity.get("link") or opportunity.get("url")
                            if link:
                                st.link_button("Ver publicación", link)

def render_tracking():
    saved = get_saved_listings()

    if not saved:
        st.info("No tienes propiedades guardadas todavía")
    else:
        hoy = date.today()
        portfolio_items = []

        for saved_listing in saved:
            listing_id = saved_listing["listing_id"]
            listing = None
            price_history = []

            with engine.begin() as connection:
                listing = connection.exec_driver_sql(
                    """
                    SELECT
                        id,
                        titulo,
                        precio_clp,
                        fecha_publicacion,
                        comuna,
                        link,
                        url,
                        m2_construidos,
                        dormitorios,
                        banos,
                        custom_name
                    FROM listings
                    WHERE source_listing_id = ?
                       OR CAST(id AS TEXT) = ?
                    LIMIT 1
                    """,
                    (str(listing_id), str(listing_id)),
                ).fetchone()

                if listing:
                    history_rows = connection.exec_driver_sql(
                        """
                        SELECT
                            fecha_captura,
                            precio_clp,
                            precio_clp_nuevo
                        FROM price_history
                        WHERE listing_id = ?
                        ORDER BY fecha_captura ASC, fecha_cambio ASC
                        """,
                        (listing[0],),
                    ).fetchall()

                    price_history = [
                        {
                            "fecha": row[0],
                            "precio_clp": first_not_none(row[2], row[1]),
                        }
                        for row in history_rows
                    ]

            if not listing:
                portfolio_items.append(
                    {
                        "listing_id": listing_id,
                        "available": False,
                        "titulo": f"Listing {listing_id}",
                        "delta": None,
                        "dias_en_mercado": None,
                    }
                )
                continue

            titulo = listing[1] or f"Listing {listing_id}"
            precio = first_not_none(listing[2], saved_listing.get("precio_guardado"))
            fecha_publicacion = listing[3]
            comuna = listing[4]
            link = first_not_none(listing[5], listing[6])
            m2 = listing[7]
            dormitorios = listing[8]
            banos = listing[9]
            custom_name = listing[10]

            if isinstance(fecha_publicacion, str):
                try:
                    fecha_publicacion = date.fromisoformat(fecha_publicacion[:10])
                except ValueError:
                    fecha_publicacion = None
            elif isinstance(fecha_publicacion, datetime):
                fecha_publicacion = fecha_publicacion.date()

            dias_en_mercado = (
                (hoy - fecha_publicacion).days
                if fecha_publicacion
                else None
            )

            opportunity = {
                "listing_id": listing_id,
                "titulo": titulo,
                "comuna": comuna,
                "listing_price": precio,
                "precio_publicado": precio,
                "price_history": price_history,
                "empty_price_history_message": "📊 Aún no hay historial de precios (necesita más tiempo de seguimiento)",
                "link": link,
            }

            precios_historial = [
                safe_float(point.get("precio_clp"))
                for point in price_history
                if safe_float(point.get("precio_clp")) is not None
            ]
            precio_inicial = precios_historial[0] if precios_historial else None
            precio_actual = safe_float(precio)
            delta = None
            if precio_inicial and precio_actual:
                delta = ((precio_actual - precio_inicial) / precio_inicial) * 100

            portfolio_items.append(
                {
                    "listing_id": listing_id,
                    "db_listing_id": listing[0],
                    "available": True,
                    "titulo": titulo,
                    "precio": precio,
                    "comuna": comuna,
                    "m2": m2,
                    "dormitorios": dormitorios,
                    "banos": banos,
                    "custom_name": custom_name,
                    "link": link,
                    "dias_en_mercado": dias_en_mercado,
                    "delta": delta,
                    "price_history": price_history,
                    "opportunity": opportunity,
                }
            )

        portfolio_items = sorted(
            portfolio_items,
            key=lambda item: item["delta"] if item["delta"] is not None else 999,
        )

        if len(portfolio_items) == 0:
            st.info("No hay posiciones en tracking")
            st.stop()

        total = len(saved)
        opportunities = sum(
            1
            for item in portfolio_items
            if item.get("delta") is not None
            and item.get("delta") <= -5
            and item.get("dias_en_mercado") is not None
            and item.get("dias_en_mercado") >= 10
        )
        watching = sum(
            1
            for item in portfolio_items
            if item.get("delta") is not None
            and item.get("delta") <= -3
            and not (
                item.get("delta") <= -5
                and item.get("dias_en_mercado") is not None
                and item.get("dias_en_mercado") >= 10
            )
        )
        neutral = total - opportunities - watching

        st.markdown("## 📈 Portfolio")
        cols = st.columns(4)
        cols[0].metric("Total", total)
        cols[1].metric("Oportunidades", opportunities)
        cols[2].metric("Observación", watching)
        cols[3].metric("Sin señal", neutral)

        header_cols = st.columns([3, 2, 2, 2, 2, 2])
        header_cols[0].caption("Propiedad")
        header_cols[1].caption("Precio")
        header_cols[2].caption("Delta")
        header_cols[3].caption("Días")
        header_cols[4].caption("Estado")
        header_cols[5].caption("Acciones")

        for item in portfolio_items:
            listing_id = item["listing_id"]
            cols = st.columns([3, 2, 2, 2, 2, 2])

            if not item.get("available"):
                cols[0].write(f"Listing {listing_id}")
                cols[1].write("No disponible")
                cols[2].write("-")
                cols[3].write("-")
                cols[4].write("HOLD")
                cols[5].markdown("[link](#)")
                continue

            precio = item.get("precio")
            comuna = item.get("comuna")
            m2 = item.get("m2")
            custom_name = item.get("custom_name")
            link = item.get("link")
            dias_en_mercado = item.get("dias_en_mercado")
            delta = item.get("delta")

            title_comuna = comuna or "Sin comuna"
            title_m2 = f"{m2:g}" if safe_float(m2) is not None else "Sin dato"
            fallback_name = f"{title_comuna} · {title_m2}m²"
            name = custom_name or fallback_name
            name_input = cols[0].text_input(
                "Nombre",
                value=name if custom_name else "",
                key=f"name_{listing_id}",
                placeholder=fallback_name,
                label_visibility="collapsed",
            )
            normalized_name = name_input.strip() or None
            if normalized_name != custom_name:
                if DEMO_MODE:
                    st.caption("Modo demo: nombre no guardado en la snapshot")
                else:
                    with engine.begin() as connection:
                        connection.exec_driver_sql(
                            """
                            UPDATE listings
                            SET custom_name = ?
                            WHERE source_listing_id = ?
                               OR CAST(id AS TEXT) = ?
                            """,
                            (normalized_name, str(listing_id), str(listing_id)),
                        )
                    custom_name = normalized_name
                    name = custom_name or fallback_name

            if delta is not None and delta <= -5:
                estado = "BUY"
            elif delta is not None and delta <= -3:
                estado = "WATCH"
            else:
                estado = "HOLD"

            if delta is None:
                delta_str = "-"
            elif delta < 0:
                delta_str = f"↓ {abs(delta):.1f}%"
            elif delta > 0:
                delta_str = f"↑ {delta:.1f}%"
            else:
                delta_str = "0%"

            dias_text = str(dias_en_mercado) if dias_en_mercado is not None else "-"

            cols[0].write(name)
            cols[1].write(format_clp(precio))
            cols[2].write(delta_str)
            cols[3].write(dias_text)
            cols[4].write(estado)
            url = link or "#"
            cols[5].markdown(f"[link]({url})")


query_params = st.query_params

if "report_id" in query_params:
    listing_id = query_params["report_id"]
    opportunity = load_opportunity_by_id(listing_id)
    html = generate_risk_report_html(opportunity)
    st.components.v1.html(html, height=1000, scrolling=True)
    st.stop()


render_nav()

if st.session_state.page == "tasar":
    render_tasar()
elif st.session_state.page == "radar":
    render_radar()
elif st.session_state.page == "tracking":
    render_tracking()
