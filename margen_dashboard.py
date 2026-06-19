"""
margen_dashboard.py — Dashboard PyG Margen Operativo DGM.

Uso:
    streamlit run margen_dashboard.py
    (desde el directorio dgm_bi/)
"""

from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ── Configuración de página ───────────────────────────────────────────────────

st.set_page_config(
    page_title="DGM — Margen Operativo",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Login ─────────────────────────────────────────────────────────────────────

if not st.session_state.get("authenticated"):
    _pwd = st.text_input("🔒 Código de acceso", type="password", key="pwd")
    if _pwd == "anibal2026!":
        st.session_state["authenticated"] = True
        st.rerun()
    else:
        if _pwd:
            st.error("Código incorrecto.")
        st.stop()

# ── Paleta y estilos ──────────────────────────────────────────────────────────

TRANSP = "rgba(0,0,0,0)"
GRID   = "rgba(255,255,255,0.07)"
TEXT   = "#c8e6c0"
GREEN  = "#5a9e47"
RED    = "#c44040"
ORANGE = "#e07a30"
GOLD   = "#e0a020"
BLUE   = "#3b9dc4"

MESES_ES = {
    "01": "Ene", "02": "Feb", "03": "Mar", "04": "Abr",
    "05": "May", "06": "Jun", "07": "Jul", "08": "Ago",
    "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dic",
}

st.markdown("""
<style>
/* ── KPI cards ──────────────────────────────────────────────────────────── */
.kpi-card {
    background: linear-gradient(160deg, #1b381b 0%, #224022 100%);
    border: 1px solid rgba(90,158,71,0.2);
    border-top: 3px solid var(--kpi-accent, #5a9e47);
    border-radius: 12px;
    padding: 0.85rem 0.75rem 0.8rem;
    text-align: center;
    box-shadow: 0 3px 10px rgba(0,0,0,0.35);
}
.kpi-label { font-size: 0.6rem; color: #7ab87a; font-weight: 700;
             text-transform: uppercase; letter-spacing: .11em; margin-top: 1px; }
.kpi-value { font-size: 1.85rem; font-weight: 800; color: #f0f8ed;
             line-height: 1.1; margin: 4px 0 2px; }
.kpi-sub   { font-size: 0.65rem; color: #5f9a5f; min-height: 1em; }

/* ── P&L table ───────────────────────────────────────────────────────────── */
.pnl-wrap { overflow-x: auto; max-height: 65vh; overflow-y: auto; margin-top: 0.5rem; }
.pnl-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.75rem;
    font-family: monospace;
}
.pnl-table thead th {
    background: #1a2e1a;
    color: #7ab87a;
    padding: 5px 10px;
    text-align: right;
    white-space: nowrap;
    position: sticky;
    top: 0;
    z-index: 3;
    font-weight: 600;
    font-size: 0.65rem;
    letter-spacing: .04em;
    border-bottom: 1px solid rgba(90,158,71,0.25);
}
.pnl-table thead th:first-child {
    text-align: left;
    min-width: 260px;
    max-width: 260px;
    position: sticky;
    left: 0;
    z-index: 4;
    background: #1a2e1a;
}
.pnl-table td {
    padding: 3px 10px;
    white-space: nowrap;
    text-align: right;
    border-bottom: 1px solid rgba(255,255,255,0.03);
}
.pnl-table td:first-child {
    text-align: left;
    position: sticky;
    left: 0;
    z-index: 1;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 260px;
}

/* row types */
.row-grupo   td { background:#1e3d1e; color:#f0f8ed; font-weight:700;
                  border-top:1px solid rgba(90,158,71,0.3); }
.row-grupo   td:first-child { background:#1e3d1e; }
.row-concepto td { background:#182a18; color:#c8e6c0; }
.row-concepto td:first-child { background:#182a18; padding-left:18px; }
.row-detalle  td { background:#131e13; color:#8ab88a; font-size:0.7rem; }
.row-detalle  td:first-child { background:#131e13; padding-left:32px; }
.row-margen   td { background:#0f1a0f; color:#f0f8ed; font-weight:700;
                   border-top:2px solid rgba(90,158,71,0.5);
                   border-bottom:2px solid rgba(90,158,71,0.3); }
.row-margen   td:first-child { background:#0f1a0f; }
.row-pct      td { background:#0f1a0f; color:#7ab87a; font-size:0.72rem; }
.row-pct      td:first-child { background:#0f1a0f; }

.neg { color: #c44040 !important; font-weight: 600; }
.pos { color: #5a9e47 !important; }

/* misc */
h2 { border-bottom: 2px solid #2e4e2e; padding-bottom: .3rem; }
footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Carga de datos ────────────────────────────────────────────────────────────

DATA_PATH = Path(__file__).parent / "margen_2026.xlsx"

@st.cache_data
def load_data():
    return {
        "bsas":      pd.read_excel(DATA_PATH, sheet_name="Bs.As."),
        "salta":     pd.read_excel(DATA_PATH, sheet_name="Salta"),
        "bsas_mov":  pd.read_excel(DATA_PATH, sheet_name="Bs.As. - movimientos"),
        "salta_mov": pd.read_excel(DATA_PATH, sheet_name="Salta - movimientos"),
    }

data = load_data()

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### DGM — Margen Operativo")
    st.markdown("---")
    page = st.radio("Unidad de Negocios", ["Buenos Aires", "Salta"])
    st.markdown("---")
    years = st.multiselect(
        "Año", ["2024", "2025", "2026"],
        default=["2025", "2026"],
    )
    st.markdown("---")
    show_detalle = st.checkbox("Mostrar detalle por contraparte", value=False)
    top_n = 10
    if show_detalle:
        top_n = st.slider("Contrapartes por concepto (top N)", 3, 20, 10)

# ── Selección de datos ────────────────────────────────────────────────────────

if page == "Buenos Aires":
    df_sum = data["bsas"]
    df_mov = data["bsas_mov"]
else:
    df_sum = data["salta"]
    df_mov = data["salta_mov"]

all_months = [c for c in df_sum.columns if c not in ["Grupo", "Concepto"]]
months = [m for m in all_months if m[:4] in years] if years else []

if not months:
    st.warning("Seleccioná al menos un año en el filtro.")
    st.stop()

# ── KPI helpers ───────────────────────────────────────────────────────────────

def fmt_ars_m(v):
    """Formato ARS en millones con separador argentino."""
    m = v / 1_000_000
    s = f"{abs(m):,.1f}M".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"-${s}" if v < 0 else f"${s}"

def kpi(col, label, value_str, sub="", accent=GREEN):
    col.markdown(f"""
    <div class="kpi-card" style="--kpi-accent:{accent}">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value_str}</div>
        <div class="kpi-sub">{sub}</div>
    </div>""", unsafe_allow_html=True)

# ── Cálculo de totales ────────────────────────────────────────────────────────

ventas_m    = df_sum[df_sum["Grupo"] == "1.Ventas"][months].sum()
rrhh_m      = df_sum[df_sum["Grupo"] == "2.RRHH"][months].sum()
egresos_m   = df_sum[df_sum["Grupo"] == "3.Egresos"][months].sum()
margen_m    = ventas_m - rrhh_m - egresos_m
margen_pct_m = (margen_m / ventas_m.replace(0, float("nan")) * 100)

ventas_t   = ventas_m.sum()
rrhh_t     = rrhh_m.sum()
egresos_t  = egresos_m.sum()
costos_t   = rrhh_t + egresos_t
margen_t   = ventas_t - costos_t
margen_pct = margen_t / ventas_t * 100 if ventas_t else 0

# ── Header + KPIs ─────────────────────────────────────────────────────────────

periodo_label = f"{min(years)} – {max(years)}" if years else ""
st.markdown(f"## {page}")

c1, c2, c3, c4 = st.columns(4)
kpi(c1, "Ventas Netas", fmt_ars_m(ventas_t), f"Período {periodo_label}", GREEN)
kpi(c2, "Costos Totales", fmt_ars_m(costos_t),
    f"RRHH {fmt_ars_m(rrhh_t)}  ·  Egresos {fmt_ars_m(egresos_t)}", ORANGE)
kpi(c3, "Margen Operativo", fmt_ars_m(margen_t),
    "Ventas − RRHH − Egresos", GOLD if margen_t >= 0 else RED)
kpi(c4, "Margen %", f"{margen_pct:.1f}%",
    "sobre Ventas netas", GREEN if margen_pct >= 0 else RED)

st.markdown("<br>", unsafe_allow_html=True)

# ── P&L Table builder ─────────────────────────────────────────────────────────

def cell(v, bold=False):
    if pd.isna(v) or v == 0:
        return "<td>—</td>"
    s = f"{abs(v):,.0f}".replace(",", ".")
    cls = "neg" if v < 0 else ""
    sign = "-" if v < 0 else ""
    inner = f"<b>{sign}{s}</b>" if bold else f"{sign}{s}"
    return f'<td class="{cls}">{inner}</td>'

def cell_pct(v, bold=False):
    if pd.isna(v):
        return "<td>—</td>"
    cls = "neg" if v < 0 else "pos"
    inner = f"<b>{v:.1f}%</b>" if bold else f"{v:.1f}%"
    return f'<td class="{cls}">{inner}</td>'

def build_pnl_html(df_sum, df_mov, months, show_detalle, top_n):
    rows = []

    # ── Header ──────────────────────────────────────────────────────────────
    def month_label(m):
        year, mon = m.split("-")
        return f"{MESES_ES.get(mon, mon)}<br>{year}"

    hdr = '<th>Concepto / Cuenta</th>' + "".join(
        f"<th>{month_label(m)}</th>" for m in months
    )
    rows.append(f"<tr>{hdr}</tr>")

    # Pre-build detalle pivot once
    if show_detalle:
        df_mov_f = df_mov[df_mov["Mes"].isin(months)].copy()
        det_piv = (
            df_mov_f
            .groupby(["Concepto", "RazonSocial", "Mes"])["Importe"]
            .sum()
            .reset_index()
            .pivot_table(
                index=["Concepto", "RazonSocial"],
                columns="Mes",
                values="Importe",
                aggfunc="sum",
            )
            .reindex(columns=months)
        )
    else:
        det_piv = None

    # ── Grupos ──────────────────────────────────────────────────────────────
    grupo_labels = {"1.Ventas": "Ventas", "2.RRHH": "RRHH", "3.Egresos": "Egresos"}

    for grupo_key in ["1.Ventas", "2.RRHH", "3.Egresos"]:
        gdf = df_sum[df_sum["Grupo"] == grupo_key]
        gsums = gdf[months].sum()

        # Grupo header
        gcells = "".join(cell(v, bold=True) for v in gsums)
        rows.append(
            f'<tr class="row-grupo">'
            f'<td>{grupo_labels[grupo_key]}</td>'
            f'{gcells}</tr>'
        )

        # Concepto rows
        for _, row in gdf.iterrows():
            concepto = row["Concepto"]
            # Strip sort prefix (e.g. "00-", "01-", "02-")
            label = concepto.split("-", 1)[1].strip() if "-" in concepto else concepto
            vals = [row.get(m, 0) for m in months]
            ccells = "".join(cell(v) for v in vals)
            rows.append(
                f'<tr class="row-concepto">'
                f'<td title="{concepto}">{label}</td>'
                f'{ccells}</tr>'
            )

            # Detalle rows (if enabled and data exists)
            if show_detalle and det_piv is not None:
                lvl0 = det_piv.index.get_level_values(0)
                if concepto in lvl0:
                    sub = det_piv.xs(concepto, level="Concepto").copy()
                    sub = sub.fillna(0)
                    sub["_total"] = sub.abs().sum(axis=1)
                    sub = sub.sort_values("_total", ascending=False).drop("_total", axis=1)
                    sub = sub.head(top_n)
                    for razon, drow in sub.iterrows():
                        dcells = "".join(cell(v) for v in drow)
                        label_d = str(razon)[:45] + ("…" if len(str(razon)) > 45 else "")
                        rows.append(
                            f'<tr class="row-detalle">'
                            f'<td title="{razon}">{label_d}</td>'
                            f'{dcells}</tr>'
                        )

    # ── Margen rows ─────────────────────────────────────────────────────────
    v_m = df_sum[df_sum["Grupo"] == "1.Ventas"][months].sum()
    r_m = df_sum[df_sum["Grupo"] == "2.RRHH"][months].sum()
    e_m = df_sum[df_sum["Grupo"] == "3.Egresos"][months].sum()
    mg  = v_m - r_m - e_m
    mg_pct = (mg / v_m.replace(0, float("nan")) * 100)

    mcells = "".join(cell(v, bold=True) for v in mg)
    rows.append(
        f'<tr class="row-margen"><td>MARGEN OPERATIVO</td>{mcells}</tr>'
    )
    pctcells = "".join(cell_pct(v, bold=True) for v in mg_pct)
    rows.append(
        f'<tr class="row-pct"><td>Margen %</td>{pctcells}</tr>'
    )

    thead = f"<thead>{rows[0]}</thead>"
    tbody = "<tbody>" + "".join(rows[1:]) + "</tbody>"
    return f'<div class="pnl-wrap"><table class="pnl-table">{thead}{tbody}</table></div>'

st.markdown("### Estado de Resultados")
pnl_html = build_pnl_html(df_sum, df_mov, months, show_detalle, top_n)
st.markdown(pnl_html, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Gráfico de evolución mensual ──────────────────────────────────────────────

st.markdown("---")
st.markdown("### Evolución mensual")

chart_mode = st.radio(
    "Ver como",
    ["ARS (millones)", "Margen %"],
    horizontal=True,
    label_visibility="collapsed",
)

fig = go.Figure()

x_labels = [f"{MESES_ES.get(m[5:], m[5:])} {m[:4]}" for m in months]

if chart_mode == "ARS (millones)":
    traces = [
        ("Ventas",   ventas_m.values / 1e6,   GREEN,  "solid",  2.5, 6, False),
        ("RRHH",     rrhh_m.values / 1e6,     RED,    "dot",    2.0, 5, False),
        ("Egresos",  egresos_m.values / 1e6,  ORANGE, "dash",   2.0, 5, False),
        ("Margen",   margen_m.values / 1e6,   GOLD,   "solid",  3.0, 7, True),
    ]
    ytitle = "ARS (millones)"
    yformat = None
else:
    traces = [
        ("Margen %", margen_pct_m.values, GOLD, "solid", 3.0, 7, True),
    ]
    ytitle = "Margen (%)"
    yformat = ".1f"

for name, y, color, dash, width, msize, fill in traces:
    trace_kwargs = dict(
        x=x_labels,
        y=y,
        name=name,
        mode="lines+markers",
        line=dict(color=color, width=width, shape="spline", dash=dash),
        marker=dict(color=color, size=msize, line=dict(color="#0e1a0e", width=1.5)),
        fill="tozeroy" if fill else "none",
        hovertemplate=f"<b>%{{x}}</b><br>{name}: %{{y:,.1f}}<extra></extra>",
    )
    if fill:
        trace_kwargs["fillcolor"] = "rgba(224,160,32,0.07)"
    fig.add_trace(go.Scatter(**trace_kwargs))

fig.add_hline(
    y=0,
    line=dict(color="rgba(255,255,255,0.15)", width=1, dash="dot"),
)

fig.update_layout(
    paper_bgcolor=TRANSP,
    plot_bgcolor=TRANSP,
    margin=dict(l=0, r=0, t=10, b=0),
    height=380,
    xaxis=dict(
        showgrid=False,
        color=TEXT,
        tickfont=dict(size=10),
        tickangle=-35,
    ),
    yaxis=dict(
        title=ytitle,
        title_font=dict(color=TEXT, size=11),
        showgrid=True,
        gridcolor=GRID,
        color=TEXT,
        zeroline=False,
        tickformat=yformat,
    ),
    legend=dict(
        orientation="h",
        x=0,
        y=1.12,
        font=dict(color=TEXT, size=12),
        bgcolor=TRANSP,
    ),
    hoverlabel=dict(bgcolor="#1a2e1a", font_color="#e8f5e2"),
)

st.plotly_chart(fig, use_container_width=True)

# ── Footer ────────────────────────────────────────────────────────────────────

st.markdown("---")
st.caption("DGM — Margen Operativo 2024–2026  •  fuente: margen_2026.xlsx")
