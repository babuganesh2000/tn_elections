"""
Tamil Nadu Elections Intelligence App – Home
2001-2026 · DuckDB Analytics · ECI Data Model
"""
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils.db import get_connection, get_data_status, get_overview_stats, get_alliance_trend, get_turnout_trend, query
from utils.app_guard import stop_if_data_unverified
from utils.ui import inject_enterprise_theme, render_demo_banner, render_masthead, render_sidebar_branding

st.set_page_config(page_title="TN Election 2026 · Modeled Election Dashboard", page_icon="🗳️", layout="wide", initial_sidebar_state="expanded")

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,700;0,800;1,700&family=IBM+Plex+Sans:wght@300;400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap');
html,body,[class*="css"]{font-family:'IBM Plex Sans',sans-serif;}
#MainMenu,footer{visibility:hidden;}
section[data-testid="stSidebar"]{background:#07101a;border-right:1px solid #102030;}
section[data-testid="stSidebar"] *{color:#c8d8e8 !important;}
.block-container{padding-top:1.5rem;}
.hero-eyebrow{font-family:'IBM Plex Mono',monospace;font-size:.72rem;color:#2a7fc0;letter-spacing:.2em;text-transform:uppercase;margin-bottom:.4rem;}
.hero-title{font-family:'Playfair Display',Georgia,serif;font-size:3rem;font-weight:800;color:#e8f4fd;line-height:1.12;letter-spacing:-.02em;margin:0;}
.hero-accent{color:#1e9be0;}
.hero-desc{color:#7090a8;font-size:.95rem;line-height:1.75;margin-top:.9rem;max-width:640px;}
.accent-line{width:56px;height:3px;background:linear-gradient(90deg,#1e6fc0,#0cd4c4);border-radius:2px;margin:1rem 0 1.2rem;}
.kpi-card{background:linear-gradient(135deg,#07131f 0%,#0e2035 100%);border:1px solid #163350;border-radius:12px;padding:1.1rem 1.4rem;position:relative;overflow:hidden;}
.kpi-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,#1e6fc0,#0cd4c4);}
.kpi-label{font-family:'IBM Plex Mono',monospace;font-size:.65rem;color:#3a7aaa;letter-spacing:.12em;text-transform:uppercase;margin-bottom:.3rem;}
.kpi-value{font-family:'IBM Plex Mono',monospace;font-size:1.9rem;font-weight:500;color:#e0f0ff;}
.kpi-sub{font-size:.72rem;color:#3a6a8a;margin-top:.2rem;}
.section-hdr{font-family:'IBM Plex Mono',monospace;font-size:.65rem;color:#1e6fc0;letter-spacing:.18em;text-transform:uppercase;border-bottom:1px solid #102030;padding-bottom:.4rem;margin-bottom:.9rem;}
.insight-card{background:#071929;border:1px solid #102a40;border-left:3px solid #1e6fc0;border-radius:0 8px 8px 0;padding:.9rem 1.1rem;font-size:.85rem;color:#7090a8;line-height:1.7;margin-bottom:.7rem;}
.insight-card strong{color:#a8c8e0;}
.fact-tag{display:inline-block;background:#071520;border:1px solid #1a3a55;color:#4a9acc;font-family:'IBM Plex Mono',monospace;font-size:.63rem;border-radius:4px;padding:.1rem .45rem;margin:.15rem;}
.timeline-year{font-family:'IBM Plex Mono',monospace;font-weight:500;font-size:.8rem;color:#5a8aaa;}
.note-box{background:#04111e;border:1px solid #0e2233;border-radius:8px;padding:.8rem 1rem;font-size:.75rem;color:#3a6080;line-height:1.6;margin-top:1.5rem;}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)
inject_enterprise_theme()

@st.cache_resource(show_spinner=False)
def load_db():
    return get_connection()
con = load_db()
stop_if_data_unverified()
if get_data_status().get("mode") == "demo":
    render_demo_banner()

# ── SIDEBAR ──────────────────────────────────────────────────────────────────
with st.sidebar:
    render_sidebar_branding("TN Election 2026", "Modeled election dashboard · Simulation, forecast, and pre-election intelligence")

# ── HERO ─────────────────────────────────────────────────────────────────────
# ── KPI ROW ──────────────────────────────────────────────────────────────────
stats = get_overview_stats(con)
cols = st.columns(6)
kpis = [
    ("Assembly Seats", "234", "All TN constituencies"),
    ("Elections Covered", "5", "2001 to 2021 + 2026"),
    ("2026 Candidates", f"{stats['total_2026_candidates']:,}", "Confirmed contesting"),
    ("2026 Blocs", "4", "DMK, AIADMK, NTK, TVK"),
    ("Avg Turnout 2021", f"{stats['avg_turnout_2021']}%", "Statewide average"),
    ("2026 Swing Seats", f"{stats['swing_seats_2026']}", "Margin <15 strength pts"),
]
for col, (label, val, sub) in zip(cols, kpis):
    with col:
        st.markdown(f'<div class="kpi-card"><div class="kpi-label">{label}</div><div class="kpi-value">{val}</div><div class="kpi-sub">{sub}</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── 20-YEAR ALLIANCE TREND ────────────────────────────────────────────────────
left, right = st.columns([1.3, 1])

with left:
    st.markdown('<div class="section-hdr">20-Year Power Swing · Alliance Seats Won (2001–2021)</div>', unsafe_allow_html=True)
    trend = get_alliance_trend(con)
    # Pivot
    pivot = trend.pivot_table(index="election_year", columns="bloc", values="seats", fill_value=0).reset_index()

    fig = go.Figure()
    BLOC_COLORS = {"DMK Alliance": "#D40000", "AIADMK": "#00AA44", "NDA / AIADMK Allied": "#FF7722", "Others": "#446688"}
    for bloc in ["DMK Alliance", "AIADMK", "NDA / AIADMK Allied", "Others"]:
        if bloc in pivot.columns:
            fig.add_trace(go.Bar(name=bloc, x=pivot["election_year"], y=pivot[bloc],
                marker_color=BLOC_COLORS[bloc],
                text=pivot[bloc], textposition="inside",
                hovertemplate=f"<b>{bloc}</b><br>Year: %{{x}}<br>Seats: %{{y}}<extra></extra>"))
    fig.add_hline(y=117, line_dash="dot", line_color="#446688", annotation_text="Majority 117", annotation_font_size=10)
    fig.update_layout(barmode="stack", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font_color="#c8d8e8", xaxis=dict(tickvals=[2001,2006,2011,2016,2021], gridcolor="#102030"),
        yaxis=dict(gridcolor="#102030", title="Seats Won", range=[0,240]),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)), margin=dict(l=0,r=0,t=10,b=0), height=340)
    st.plotly_chart(fig, use_container_width=True)

    # Narrative
    st.markdown("""<div class="insight-card">
<strong>2001:</strong> AIADMK sweeps 196 seats — Jayalalithaa's comeback after DMK's corruption-hit term.<br>
<strong>2006:</strong> DMK alliance wins 163 seats — anti-incumbency flips TN; Karunanidhi becomes CM.<br>
<strong>2011:</strong> AIADMK wins 203 seats — highest ever; Jayalalithaa tsunami against DMK.<br>
<strong>2016:</strong> AIADMK retains with 136 seats — reduced majority; DMK closes the gap at 98.<br>
<strong>2021:</strong> DMK alliance wins 159 seats — Stalin becomes CM; AIADMK without Jayalalithaa collapses.
</div>""", unsafe_allow_html=True)

with right:
    st.markdown('<div class="section-hdr">Turnout Trend by Region · 2001–2021</div>', unsafe_allow_html=True)
    tdf = get_turnout_trend(con)
    REGION_CLR = {"Chennai":"#00d4ff","North":"#7eb8f7","South":"#ff9f43","West":"#26de81","Central":"#a29bfe","Delta":"#fd9644"}
    fig2 = go.Figure()
    for reg in tdf["region"].unique():
        sub = tdf[tdf["region"]==reg]
        fig2.add_trace(go.Scatter(x=sub["election_year"], y=sub["avg_turnout"], name=reg,
            mode="lines+markers", line=dict(width=2.5, color=REGION_CLR.get(reg,"#aaa")),
            marker=dict(size=7), hovertemplate=f"<b>{reg}</b><br>%{{x}}: %{{y:.1f}}%<extra></extra>"))
    fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font_color="#c8d8e8", xaxis=dict(tickvals=[2001,2006,2011,2016,2021], gridcolor="#102030"),
        yaxis=dict(gridcolor="#102030", title="Avg Turnout %", range=[55,90]),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10)), margin=dict(l=0,r=0,t=10,b=0), height=220)
    st.plotly_chart(fig2, use_container_width=True)

    st.markdown('<div class="section-hdr" style="margin-top:1rem;">2026 Key Stories</div>', unsafe_allow_html=True)
    stories = [
        ("🎬", "TVK Statewide Entry", "Vijay’s TVK is modeled as fielding candidates across all 234 constituencies, with high-visibility contests in Perambur and Tiruchirappalli East."),
        ("⚖️", "Four-Bloc Contest", "The 2026 model is built around DMK Alliance, AIADMK Alliance, NTK, and TVK as the four statewide competing blocs."),
        ("🔴", "DMK Alliance Defense", "The DMK-led alliance carries incumbency into 2026 while distributing seats among allies across Chennai, Delta, and central Tamil Nadu."),
        ("🟢", "AIADMK Alliance Counter", "The AIADMK-led alliance is modeled as the main opposition bloc, with allied parties taking a distributed set of nomination slots."),
    ]
    for icon, title, desc in stories:
        st.markdown(f'<div class="insight-card"><strong>{icon} {title}</strong><br>{desc}</div>', unsafe_allow_html=True)

# ── 2026 SEAT PROJECTION ─────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown('<div class="section-hdr">2026 Pre-Election Strength Scores · By Region</div>', unsafe_allow_html=True)

swing_df = query(con, """
    SELECT dc.region,
           ROUND(AVG(sw.dmk_strength_score),1) AS dmk_avg,
           ROUND(AVG(sw.aiadmk_strength_score),1) AS aiadmk_avg,
           COUNT(CASE WHEN sw.swing_seat_flag THEN 1 END) AS swing_seats,
           COUNT(*) AS total_seats
    FROM fact_2026_swing sw JOIN dim_constituency dc ON sw.constituency_id=dc.constituency_id
    GROUP BY dc.region ORDER BY dmk_avg DESC
""")

fig3 = go.Figure()
fig3.add_trace(go.Bar(name="DMK Alliance Strength", x=swing_df["region"], y=swing_df["dmk_avg"],
    marker_color="#D40000", text=swing_df["dmk_avg"].map(lambda x: f"{x:.0f}"), textposition="outside"))
fig3.add_trace(go.Bar(name="AIADMK Strength", x=swing_df["region"], y=swing_df["aiadmk_avg"],
    marker_color="#00AA44", text=swing_df["aiadmk_avg"].map(lambda x: f"{x:.0f}"), textposition="outside"))
fig3.update_layout(barmode="group", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font_color="#c8d8e8", xaxis=dict(gridcolor="#102030"),
    yaxis=dict(gridcolor="#102030", title="Strength Score (0–100)", range=[0,110]),
    legend=dict(bgcolor="rgba(0,0,0,0)"), margin=dict(l=0,r=0,t=10,b=0), height=260)
st.plotly_chart(fig3, use_container_width=True)

# ── SEAT TYPE BREAKDOWN ───────────────────────────────────────────────────────
st.markdown('<div class="section-hdr">Constituency Breakdown · Seat Type × Region</div>', unsafe_allow_html=True)
breakdown = query(con, "SELECT region, type, COUNT(*) AS seats FROM dim_constituency GROUP BY region, type ORDER BY region, type")
pivot_b = breakdown.pivot_table(index="region", columns="type", values="seats", fill_value=0).reset_index()
pivot_b["Total"] = pivot_b.get("GEN",0) + pivot_b.get("SC",0) + pivot_b.get("ST",0)
st.dataframe(pivot_b.rename(columns={"region":"Region"}), use_container_width=True, hide_index=True,
    column_config={"Total": st.column_config.NumberColumn(format="%d")})
