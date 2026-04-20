from datetime import date
from html import escape
import os
from pathlib import Path
import time

import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv

from database import SessionLocal
from data_sufficiency import LOW_DATA_WARNING, MIN_ACTIVE_LISTINGS
from services.listing_service import initialize_app_data, save_listing
from services import radar_service, risk_analysis_service, valuation_service


st.set_page_config(page_title="Tasador Inmobiliario", layout="wide")

load_dotenv()

try:
    beta_password = st.secrets["BETA_PASSWORD"]
except Exception:
    beta_password = os.environ.get("BETA_PASSWORD")

if "autenticado" not in st.session_state:
    st.session_state["autenticado"] = False

if not st.session_state["autenticado"]:
    st.title("Acceso restringido")

    password_input = st.text_input("Password", type="password")

    if st.button("Acceder"):
        if password_input == beta_password:
            st.session_state["autenticado"] = True
            st.rerun()
        else:
            st.error("Password incorrecta")

    st.stop()


st.info("Beta privada — Los análisis son automatizados y no reemplazan asesoría profesional. El perfil legal es preliminar y no proviene del Conservador de Bienes Raíces.")


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
        return "🔴", "Posible anomalía"

    if discount >= 0.20:
        return "🟡", "Descuento relevante"

    return "🟢", "Descuento moderado"


def radar_confidence_badge(confidence):
    labels = {
        "high": ("🟢", "Alta confianza"),
        "medium": ("🟡", "Media confianza"),
        "low": ("🔴", "Baja confianza"),
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

            listing_id = first_not_none(opportunity.get("listing_id"), index)
            render_animated_gauge(confidence_score, listing_id)
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
        st.info("No hay evolucion de precio disponible.")
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
        elif comuna in ["Providencia", "Ñuñoa", "Ã‘uÃ±oa"]:
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
    st.caption(ctx["summary"])
    st.markdown("### 🔓 Reporte Legal Completo")
    st.caption(
        "Historial de propiedad, hipotecas, gravámenes y validación legal completa "
        "para reducir riesgos antes de comprar."
    )
    st.warning(
        "Este análisis es preliminar. El reporte completo elimina incertidumbre "
        "legal antes de invertir."
    )
    st.info("Función futura: disponible próximamente.")
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

    st.caption(generar_tesis_inversion(opportunity))
    st.caption(generar_linea_riesgo_inversion(opportunity))

    if st.button("Ver Risk Report", key=f"risk_report_{listing_id}"):
        open_risk_report(opportunity, listing_id)

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
            <div style="margin-top:10px;color:#c0c6d0;">{escape(ctx['thesis'])}</div>
            <div style="margin-top:6px;color:#c0c6d0;">Score {ctx['score']:.0f}/100, descuento {ctx['descuento'] * 100:.1f}%, confianza {ctx['confianza']:.0%}.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption("Este análisis es automatizado y no reemplaza asesoría profesional.")


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


def generate_risk_report_html(opportunity):
    ctx = risk_analysis_service.build_risk_report_context(opportunity)
    precio_publicado = ctx["precio_publicado"]
    valor_estimado = ctx["valor_estimado"]
    descuento = ctx["descuento"]
    confianza = ctx["confianza"]
    comparables = ctx["comparables"]
    score = ctx["score"]
    legal_score = ctx["legal_score"]
    legal_level = ctx["legal_level"]
    property_type = ctx["property_type"]
    comuna = ctx["comuna"]
    m2 = ctx["m2"]
    comparables_rows = ctx["comparables_rows"]
    veredicto = ctx["veredicto"]
    thesis = ctx["thesis"]
    valuation_score = ctx["valuation_score"]
    market_support_score = ctx["market_support_score"]
    risk_score = ctx["risk_score"]
    risk_flags = ctx["risk_flags"]
    catalysts = ctx["catalysts"]

    comparable_rows_html = ""
    if comparables_rows:
        for row in comparables_rows[:5]:
            diferencia = row.get("diferencia_vs_target")
            diferencia_text = (
                f"{diferencia:.1f}%" if diferencia is not None else "Sin dato"
            )
            comparable_rows_html += f"""
                <tr>
                    <td>{escape(str(row.get("comuna") or "Sin comuna"))}</td>
                    <td>{escape(str(row.get("m2") or "Sin dato"))}</td>
                    <td>{escape(format_clp(row.get("precio_clp")))}</td>
                    <td>{escape(format_clp(row.get("precio_m2")))}</td>
                    <td>{escape(diferencia_text)}</td>
                </tr>
            """
    else:
        comparable_rows_html = f"""
            <tr>
                <td colspan="5">Resumen disponible: {escape(str(comparables))} comparables usados por el radar.</td>
            </tr>
        """

    risk_items = "".join(
        f"<li>{escape(str(flag))}</li>"
        for flag in risk_flags
    ) or "<li>Sin riesgos críticos visibles en los datos disponibles.</li>"
    catalyst_items = "".join(
        f"<li>{escape(str(catalyst))}</li>"
        for catalyst in catalysts
    ) or "<li>No se detectan catalizadores fuertes con la información actual.</li>"
    legal_score_text = f"{legal_score}/100" if legal_score is not None else "Sin datos"
    property_line = f"{comuna} | {property_type or 'Propiedad'}"
    if m2:
        property_line += f" | {m2} m2"

    html = f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Investment Risk Report</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #0b0f14;
      --panel: #11151d;
      --panel-2: #151922;
      --border: #2a303b;
      --text: #f5f5f5;
      --muted: #9ca3af;
      --soft: #c0c6d0;
      --green: #06d6a0;
      --yellow: #ffd166;
      --red: #ff6b6b;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: Inter, Segoe UI, Arial, sans-serif;
      line-height: 1.45;
    }}
    .page {{
      max-width: 1180px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}
    .header, .card {{
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--panel);
      padding: 20px;
    }}
    .header-top, .grid-2, .grid-3, .metric-grid {{
      display: grid;
      gap: 16px;
    }}
    .header-top {{ grid-template-columns: 1fr auto; align-items: start; }}
    .grid-2 {{ grid-template-columns: repeat(2, minmax(0, 1fr)); margin-top: 18px; }}
    .grid-3 {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
    .metric-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    h1, h2, h3 {{ margin: 0; }}
    h1 {{ font-size: 34px; letter-spacing: 0; }}
    h2 {{ margin-top: 28px; margin-bottom: 12px; font-size: 20px; }}
    .muted {{ color: var(--muted); }}
    .soft {{ color: var(--soft); }}
    .eyebrow {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .12em;
      font-weight: 800;
    }}
    .price {{
      margin-top: 10px;
      font-size: 28px;
      font-weight: 900;
    }}
    .score {{
      font-size: 72px;
      line-height: 1;
      font-weight: 950;
    }}
    .verdict {{
      margin-top: 12px;
      font-size: 28px;
      font-weight: 950;
    }}
    .metric {{
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--panel-2);
      padding: 14px;
    }}
    .metric .label {{ color: var(--muted); font-size: 12px; }}
    .metric .value {{ margin-top: 6px; font-size: 22px; font-weight: 850; }}
    .info {{
      border-left: 4px solid #5ea0ff;
      background: #101827;
      padding: 14px 16px;
      border-radius: 8px;
      color: var(--soft);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      overflow: hidden;
      border-radius: 8px;
      border: 1px solid var(--border);
    }}
    th, td {{
      padding: 12px;
      border-bottom: 1px solid var(--border);
      text-align: left;
    }}
    th {{ color: var(--muted); background: #0f141c; font-size: 12px; }}
    ul {{ margin: 0; padding-left: 20px; }}
    li {{ margin: 6px 0; color: var(--soft); }}
    .risk li::marker {{ color: var(--red); }}
    .catalyst li::marker {{ color: var(--green); }}
    .bottom {{
      border-color: var(--green);
      background: var(--panel-2);
    }}
    .disclaimer {{ margin-top: 12px; color: var(--muted); font-size: 13px; }}
    @media (max-width: 760px) {{
      .header-top, .grid-2, .grid-3, .metric-grid {{ grid-template-columns: 1fr; }}
      .score {{ font-size: 56px; }}
    }}
  </style>
</head>
<body>
  <main class="page">
    <section class="header">
      <div class="header-top">
        <div>
          <div class="eyebrow">Investment Risk Report</div>
          <h1>{escape(property_line)}</h1>
          <div class="soft">Reporte automatizado basado en radar, AVM y perfil legal preliminar.</div>
        </div>
        <div class="muted">Fecha: {date.today().isoformat()}</div>
      </div>
      <div class="price">Precio publicado: {escape(format_clp(precio_publicado))}</div>
    </section>

    <section class="grid-2">
      <div class="card">
        <div class="eyebrow">Investment Score</div>
        <div class="score">{score:.0f}</div>
        <div class="verdict">{escape(veredicto)}</div>
      </div>
      <div class="card">
        <div class="metric-grid">
          <div class="metric"><div class="label">Precio publicado</div><div class="value">{escape(format_clp(precio_publicado))}</div></div>
          <div class="metric"><div class="label">Valor estimado</div><div class="value">{escape(format_clp(valor_estimado))}</div></div>
          <div class="metric"><div class="label">Descuento</div><div class="value">{descuento * 100:.1f}%</div></div>
          <div class="metric"><div class="label">Confianza</div><div class="value">{confianza:.0%}</div></div>
          <div class="metric"><div class="label">Comparables</div><div class="value">{comparables}</div></div>
          <div class="metric"><div class="label">Legal Risk</div><div class="value">{escape(legal_score_text)} · {escape(legal_level)}</div></div>
        </div>
      </div>
    </section>

    <h2>Tesis</h2>
    <div class="info">{escape(thesis)}</div>

    <h2>Score Breakdown</h2>
    <section class="grid-3">
      <div class="metric"><div class="label">Valuation</div><div class="value">{valuation_score:.0f}/100</div><div class="muted">Basado en descuento.</div></div>
      <div class="metric"><div class="label">Market Support</div><div class="value">{market_support_score:.0f}/100</div><div class="muted">Comparables + confianza.</div></div>
      <div class="metric"><div class="label">Risk</div><div class="value">{risk_score:.0f}/100</div><div class="muted">Legal risk + datos faltantes.</div></div>
    </section>

    <h2>Comparables</h2>
    <table>
      <thead>
        <tr><th>Comuna</th><th>M2</th><th>Precio</th><th>Precio/m2</th><th>Diferencia %</th></tr>
      </thead>
      <tbody>{comparable_rows_html}</tbody>
    </table>

    <section class="grid-2">
      <div>
        <h2>Riesgos</h2>
        <div class="card"><ul class="risk">{risk_items}</ul></div>
      </div>
      <div>
        <h2>Catalizadores</h2>
        <div class="card"><ul class="catalyst">{catalyst_items}</ul></div>
      </div>
    </section>

    <h2>Bottom Line</h2>
    <section class="card bottom">
      <div class="eyebrow">Veredicto</div>
      <div class="verdict">{escape(veredicto)}</div>
      <div class="soft">{escape(thesis)}</div>
      <div class="soft">Score {score:.0f}/100, descuento {descuento * 100:.1f}%, confianza {confianza:.0%}.</div>
      <div class="disclaimer">Este análisis es automatizado y no reemplaza asesoría profesional.</div>
    </section>
  </main>
</body>
</html>"""

    footer_text = (
        "Beta privada — Los análisis son automatizados y no reemplazan asesoría "
        "profesional. El perfil legal es preliminar y no proviene del Conservador "
        "de Bienes Raíces."
    )
    footer_html = f"""
    <div style="margin-top:40px; padding:12px; font-size:12px; color:#888; text-align:center; border-top:1px solid #333;">
        {footer_text}
    </div>
    """

    if footer_text not in html:
        html = html.replace("</body>", footer_html + "</body>")

    return html


def open_risk_report(opportunity, listing_id):
    html_content = generate_risk_report_html(opportunity)

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
        import streamlit.components.v1 as components

        components.html(html_content, height=800, scrolling=True)


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


def mostrar_inversion(opportunities):
    selected_report = st.session_state.get("selected_investment_report")

    for index, opportunity in enumerate(opportunities):
        link = opportunity.get("link") or opportunity.get("url")
        raw_listing_id = first_not_none(opportunity.get("listing_id"), "listing")
        listing_id = f"{raw_listing_id}_{index}"

        with st.container():
            render_investment_quick_panel(opportunity, index, listing_id)
            render_legal_ownership_risk(opportunity, listing_id)

            if link:
                st.markdown(f"[Ver publicacion]({link})")

        st.divider()

    selected_report = st.session_state.get("selected_investment_report", selected_report)
    if selected_report:
        st.markdown("## Risk Report")
        render_risk_report(
            selected_report["opportunity"],
            selected_report["listing_id"],
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



def render_analysis():
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
                st.markdown(
                    f"""
                    <div class="status-note">
                        Razón: {escape(resultado_tasacion["reason"])}
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

            property_id, listing_id = save_listing(property_data, int(valor))
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
                            <div class="section-copy">Publicaciones usadas directamente para calcular esta tasaciÃ³n.</div>
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


def render_radar():
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
        if st.button("Ejecutar radar", use_container_width=True):
            st.session_state["radar_result"] = radar_service.get_investment_opportunities(limit=int(radar_limit))

        radar_result = st.session_state.get("radar_result")

        if radar_result:
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

                mostrar_radar_cards(filtradas)
                mostrar_inversion(filtradas)
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
            if presupuesto <= 0:
                st.warning("Ingresa un presupuesto mayor a cero.")
            else:
                opportunities = radar_service.get_best_opportunity(float(presupuesto))

                if not opportunities:
                    st.info(
                        "No encontramos oportunidades activas bajo ese presupuesto. "
                        "Prueba con un monto mayor o vuelve a correr el scraper."
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


def tasador_page():
    render_analysis()


def radar_page():
    render_radar()


def tracking_page():
    st.title("📈 Tracking")
    st.info("Próximamente: seguimiento de propiedades")


pages = [
    st.Page(tasador_page, title="🏠 Tasar propiedad", default=True),
    st.Page(radar_page, title="🔎 Oportunidades"),
    st.Page(tracking_page, title="📈 Tracking"),
]

pg = st.navigation(pages, position="top")
pg.run()
