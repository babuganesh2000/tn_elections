"""Page 6: 2026 Election Intelligence — swing map, seat projections, key watchlist"""
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.db import get_connection, get_data_status, get_swing_analysis, get_forecast_2026, query
from utils.app_guard import stop_if_data_unverified
from utils.ui import inject_enterprise_theme, render_demo_banner, render_masthead, render_sidebar_branding

st.set_page_config(page_title="2026 Intelligence · TN Election 2026 · Modeled Election Dashboard", page_icon="🔮", layout="wide")
st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,700;0,800;1,700&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap');
html,body,[class*="css"]{font-family:'IBM Plex Sans',sans-serif;}
#MainMenu,footer{visibility:hidden;}
.pg-title{font-family:'Playfair Display',serif;font-size:2.1rem;font-weight:800;color:#e8f4fd;margin:0;}
.sec{font-family:'IBM Plex Mono',monospace;font-size:.65rem;color:#1e6fc0;letter-spacing:.18em;text-transform:uppercase;border-bottom:1px solid #102030;padding-bottom:.35rem;margin-bottom:.8rem;}
div[data-testid="metric-container"]{background:linear-gradient(135deg,#071520,#0e2035);border:1px solid #163350;border-radius:10px;padding:.75rem;}
div[data-testid="metric-container"] label{color:#3a7aaa!important;font-size:.65rem!important;letter-spacing:.08em!important;text-transform:uppercase!important;font-family:'IBM Plex Mono',monospace!important;}
div[data-testid="metric-container"] [data-testid="stMetricValue"]{color:#e0f0ff!important;font-size:1.5rem!important;font-weight:700!important;font-family:'IBM Plex Mono',monospace!important;}
.story-card{border-radius:10px;padding:1rem 1.2rem;margin-bottom:1rem;border-left-width:4px;border-left-style:solid;}
.insight{background:#071929;border:1px solid #102a40;border-left:3px solid #1e6fc0;border-radius:0 8px 8px 0;padding:.75rem 1rem;font-size:.82rem;color:#7090a8;line-height:1.65;margin:.6rem 0;}
.badge{display:inline-block;border-radius:4px;padding:.15rem .5rem;font-size:.65rem;font-family:'IBM Plex Mono',monospace;margin:.1rem;}
.hdr2{font-family:'IBM Plex Mono',monospace;font-size:.75rem;color:#a0c8e8;margin:.8rem 0 .3rem;}
</style>""", unsafe_allow_html=True)
inject_enterprise_theme()

@st.cache_resource(show_spinner=False)
def load_db(): return get_connection()
con = load_db()
stop_if_data_unverified()
if get_data_status().get("mode") == "demo":
    render_demo_banner()

with st.sidebar:
    render_sidebar_branding("TN Election 2026", "Modeled election dashboard · 2026 strategy")
all_c = query(con, "SELECT constituency_id,constituency_name,district,type,region FROM dim_constituency ORDER BY constituency_name")

def _apply_constituency_filters(df, region="All", district="All", seat_type="All", constituency="All"):
    out = df.copy()
    if region not in {"All", None, ""}:
        out = out[out["region"] == region]
    if district not in {"All", None, ""}:
        out = out[out["district"] == district]
    if seat_type not in {"All", None, ""}:
        out = out[out["type"] == seat_type]
    if constituency not in {"All", None, ""}:
        out = out[out["constituency_name"] == constituency]
    return out

def _valid_or_default(value, options, fallback="All"):
    if value in options:
        return value
    if fallback in options:
        return fallback
    return options[0] if options else fallback

region_key = "e26_region"
district_key = "e26_district"
type_key = "e26_type"
name_key = "e26_name"

raw_region = st.session_state.get(region_key, "All")
raw_district = st.session_state.get(district_key, "All")
raw_type = st.session_state.get(type_key, "All")
raw_name = st.session_state.get(name_key, "All")

region_options = ["All"] + sorted(
    _apply_constituency_filters(all_c, district=raw_district, seat_type=raw_type, constituency=raw_name)["region"].dropna().unique().tolist()
)
region_filter = _valid_or_default(raw_region, region_options)

district_options = ["All"] + sorted(
    _apply_constituency_filters(all_c, region=region_filter, seat_type=raw_type, constituency=raw_name)["district"].dropna().unique().tolist()
)
district_filter = _valid_or_default(raw_district, district_options)

seat_type_options = ["All"] + sorted(
    _apply_constituency_filters(all_c, region=region_filter, district=district_filter, constituency=raw_name)["type"].dropna().unique().tolist()
)
seat_type_filter = _valid_or_default(raw_type, seat_type_options)

constituency_options = ["All"] + sorted(
    _apply_constituency_filters(all_c, region=region_filter, district=district_filter, seat_type=seat_type_filter)["constituency_name"].dropna().unique().tolist()
)
constituency_filter = _valid_or_default(raw_name, constituency_options)

filtered_constituencies = _apply_constituency_filters(all_c, region=region_filter, district=district_filter, seat_type=seat_type_filter, constituency=constituency_filter)

st.markdown('<div class="sec">2026 Filters</div>', unsafe_allow_html=True)
f1, f2, f3, f4, f5, f6 = st.columns([1, 1, 1, 1.4, 1, 1])
with f1:
    region_filter = st.selectbox("Region", region_options, index=region_options.index(region_filter), key=region_key)
with f2:
    district_filter = st.selectbox("District", district_options, index=district_options.index(district_filter), key=district_key)
with f3:
    seat_type_filter = st.selectbox("Seat Type", seat_type_options, index=seat_type_options.index(seat_type_filter), key=type_key)
with f4:
    constituency_filter = st.selectbox("Select Constituency", constituency_options, index=constituency_options.index(constituency_filter), key=name_key)
with f5:
    min_tvk = st.slider("Min TVK factor %", 0, 20, 0)
with f6:
    show_swing_only = st.checkbox("Swing seats only", False)

filtered_constituencies = _apply_constituency_filters(all_c, region=region_filter, district=district_filter, seat_type=seat_type_filter, constituency=constituency_filter)
st.caption(f"{len(filtered_constituencies)} constituencies in current filter")

# ── SUMMARY METRICS ─────────────────────────────────────────────────────────
swing_df = get_swing_analysis(con)
contest_df = get_forecast_2026(con)
if region_filter != "All": swing_df = swing_df[swing_df["region"]==region_filter]
if region_filter != "All": contest_df = contest_df[contest_df["region"]==region_filter]
if district_filter != "All":
    swing_df = swing_df[swing_df["district"] == district_filter]
    contest_df = contest_df[contest_df["district"] == district_filter]
if seat_type_filter != "All":
    swing_df = swing_df[swing_df["type"] == seat_type_filter]
    contest_df = contest_df[contest_df["type"] == seat_type_filter]
if constituency_filter != "All":
    swing_df = swing_df[swing_df["constituency_name"] == constituency_filter]
    contest_df = contest_df[contest_df["constituency_name"] == constituency_filter]
if min_tvk > 0: swing_df = swing_df[swing_df["tvk_factor_pct"]>=min_tvk]
if min_tvk > 0:
    allowed_ids = swing_df["constituency_id"].unique().tolist()
    contest_df = contest_df[contest_df["constituency_id"].isin(allowed_ids)]
if show_swing_only: swing_df = swing_df[swing_df["swing_seat_flag"]==True]

if show_swing_only:
    allowed_ids = swing_df["constituency_id"].unique().tolist()
    contest_df = contest_df[contest_df["constituency_id"].isin(allowed_ids)]

leaders = (
    contest_df.sort_values(["constituency_name", "forecast_win_prob"], ascending=[True, False])
    .drop_duplicates("constituency_name")
    .copy()
)
leaders["projected_winner"] = leaders["bloc_name"]
leaders["competitiveness"] = 1 - leaders["forecast_win_prob"]

seat_projection = (
    contest_df.groupby("bloc_name", as_index=False)["forecast_win_prob"].sum()
    .rename(columns={"forecast_win_prob": "expected_seats"})
    .sort_values("expected_seats", ascending=False)
)
projection_map = {row["bloc_name"]: row["expected_seats"] for _, row in seat_projection.iterrows()}

dmk_proj = projection_map.get("DMK Alliance", 0.0)
aiadmk_proj = projection_map.get("AIADMK Alliance", 0.0)
ntk_proj = projection_map.get("NTK", 0.0)
tvk_proj = projection_map.get("TVK", 0.0)
swing_count = int((leaders["forecast_win_prob"] < 0.45).sum())
avg_ntk = swing_df["ntk_vote_split_pct"].mean()
avg_tvk = swing_df["tvk_factor_pct"].mean()
safe_dmk = int(((contest_df["bloc_name"] == "DMK Alliance") & (contest_df["forecast_win_prob"] >= 0.8)).sum())
safe_aiadmk = int(((contest_df["bloc_name"] == "AIADMK Alliance") & (contest_df["forecast_win_prob"] >= 0.8)).sum())

m1,m2,m3,m4,m5,m6 = st.columns(6)
with m1: st.metric("DMK Expected Seats", f"{dmk_proj:.0f}")
with m2: st.metric("AIADMK Expected Seats", f"{aiadmk_proj:.0f}")
with m3: st.metric("NTK Expected Seats", f"{ntk_proj:.1f}")
with m4: st.metric("TVK Expected Seats", f"{tvk_proj:.1f}")
with m5: st.metric("Toss-up Seats", str(swing_count))
with m6: st.metric("Avg TVK Factor", f"{avg_tvk:.1f}%")
st.markdown("<br>", unsafe_allow_html=True)

st.markdown('<div class="sec">Forecast Model — Seat Probability Engine</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="insight"><strong>How the model works:</strong> Each constituency has one modeled contestant row per '
    '2026 bloc or alliance slot. The app converts bloc strength into candidate-level win probabilities, then sums '
    'those probabilities statewide into expected seats. Historical winners remain fixed facts; 2026 remains forecast-only.</div>',
    unsafe_allow_html=True,
)

left_model, right_model = st.columns([1.05, 1])
with left_model:
    band_counts = contest_df.groupby(["bloc_name", "forecast_band"], as_index=False).size()
    fig_band = go.Figure(
        px.bar(
            band_counts,
            x="forecast_band",
            y="size",
            color="bloc_name",
            barmode="group",
            color_discrete_map={"DMK Alliance":"#D40000","AIADMK Alliance":"#00AA44","NTK":"#8b8b8b","TVK":"#9B59B6","Others":"#446688"},
        )
    )
    fig_band.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#c8d8e8",
        xaxis=dict(gridcolor="#102030"),
        yaxis=dict(gridcolor="#102030", title="Seats"),
        margin=dict(l=0, r=0, t=10, b=0),
        height=300,
    )
    st.plotly_chart(fig_band, use_container_width=True)

with right_model:
    fig_prob = go.Figure(
        go.Bar(
            x=seat_projection["bloc_name"],
            y=seat_projection["expected_seats"],
            marker_color=[{"DMK Alliance":"#D40000","AIADMK Alliance":"#00AA44","NTK":"#8b8b8b","TVK":"#9B59B6"}.get(v, "#446688") for v in seat_projection["bloc_name"]],
            text=seat_projection["expected_seats"].round(1),
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Expected seats: %{y:.1f}<extra></extra>",
        )
    )
    fig_prob.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#c8d8e8",
        xaxis=dict(gridcolor="#102030", title="Bloc"),
        yaxis=dict(gridcolor="#102030", title="Expected Seats"),
        margin=dict(l=0, r=0, t=10, b=0),
        height=300,
    )
    st.plotly_chart(fig_prob, use_container_width=True)

f1, f2, f3, f4 = st.columns(4)
with f1: st.metric("Safe DMK", str(safe_dmk))
with f2: st.metric("Safe AIADMK", str(safe_aiadmk))
with f3: st.metric("DMK Majority Chance", "High" if dmk_proj >= 117 else "Open")
with f4: st.metric("AIADMK Majority Chance", "High" if aiadmk_proj >= 117 else "Open")
st.markdown("<br>", unsafe_allow_html=True)

# ── KEY STORIES ─────────────────────────────────────────────────────────────
st.markdown('<div class="sec">The 5 Big Stories of 2026</div>', unsafe_allow_html=True)
stories = [
    ("#1a0d00","#e8a030","🎬 Vijay's TVK — Wildcard Entry",
     "TVK is modeled as a statewide entrant contesting all 234 seats, with Vijay himself in Perambur and Tiruchirappalli East. Its presence matters most in seats where anti-incumbent and anti-establishment votes are fragmented.",
     "Perambur · Tiruchirappalli East · Statewide slate"),
    ("#071a0d","#30e890","⚖️ Four-Bloc Contest",
     "The 2026 model is not a simple two-way race. It assumes DMK Alliance and AIADMK Alliance distribute seats among allies, while NTK and TVK independently field candidates in all 234 constituencies.",
     "DMK Alliance · AIADMK Alliance · NTK · TVK"),
    ("#0d0718","#9070e0","🏛️ BJP's Urban Push",
     "Within the AIADMK-led bloc, BJP and other partners are modeled as concentrated alliance components rather than standalone statewide fronts. Their influence is strongest in urban and alliance-assigned seats.",
     "Alliance-assigned seats · Urban pockets"),
    ("#1a0707","#e03030","🔴 DMK's Incumbent Risk",
     "The DMK-led bloc enters 2026 with incumbency, stronger urban and delta positioning, and alliance seat-sharing. The model treats this as the leading edge in Chennai, Delta, and parts of Central Tamil Nadu.",
     "Incumbency · Delta · Chennai"),
    ("#071522","#30a0e8","📊 NTK — The Spoiler Factor",
     "Seeman's NTK is modeled as a full-coverage statewide force with candidates in all 234 constituencies. Its main effect is not outright seat conversion but vote fragmentation in close races.",
     "234-seat contest · Independent statewide slate"),
]

for bg,border,title,body,tags in stories:
    tag_html = " ".join([f'<span class="badge" style="background:#07111e;border:1px solid #1a3a55;color:#4a8aaa;">{t}</span>' for t in tags.split(" · ")])
    st.markdown(f"""<div class="story-card" style="background:{bg};border-color:{border};">
<p class="hdr2" style="color:{border};">{title}</p>
<p style="font-size:.83rem;color:#8090a0;line-height:1.65;margin:.3rem 0 .5rem;">{body}</p>
{tag_html}
</div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── SWING SEAT MAP ───────────────────────────────────────────────────────────
st.markdown('<div class="sec">Strength Score Map — DMK vs AIADMK by Constituency</div>', unsafe_allow_html=True)
TN_DISTRICT_COORDS = {
    "Ariyalur":(11.14,79.08),"Chennai":(13.08,80.27),"Chengalpattu":(12.69,80.00),
    "Coimbatore":(11.00,76.97),"Cuddalore":(11.75,79.77),"Dharmapuri":(12.13,78.16),
    "Dindigul":(10.36,77.97),"Erode":(11.34,77.73),"Kallakurichi":(11.74,78.96),
    "Kanchipuram":(12.84,79.70),"Kanyakumari":(8.09,77.54),"Karur":(10.96,78.08),
    "Krishnagiri":(12.52,78.22),"Madurai":(9.93,78.12),"Mayiladuthurai":(11.10,79.65),
    "Nagapattinam":(10.76,79.84),"Namakkal":(11.22,78.17),"Nilgiris":(11.49,76.73),
    "Perambalur":(11.23,78.88),"Pudukkottai":(10.38,78.82),"Ramanathapuram":(9.37,78.83),
    "Ranipet":(12.93,79.33),"Salem":(11.67,78.15),"Sivaganga":(9.84,78.48),
    "Tenkasi":(8.96,77.32),"Thanjavur":(10.79,79.14),"Theni":(10.01,77.48),
    "Thiruvallur":(13.14,79.91),"Thiruvarur":(10.77,79.64),"Thoothukudi":(8.79,77.99),
    "Tirunelveli":(8.73,77.70),"Tirupathur":(12.50,78.56),"Tiruppur":(11.10,77.34),
    "Tiruchirappalli":(10.79,78.69),"Tiruvannamalai":(12.23,79.07),"Vellore":(12.92,79.13),
    "Villupuram":(11.94,79.49),"Virudhunagar":(9.58,77.96),
}

map_agg = query(con, f"""
    SELECT dc.district, dc.region,
           ROUND(AVG(sw.dmk_strength_score),1) AS dmk_avg,
           ROUND(AVG(sw.aiadmk_strength_score),1) AS aiadmk_avg,
           ROUND(AVG(sw.tvk_factor_pct),1) AS tvk_avg,
           COUNT(CASE WHEN sw.swing_seat_flag THEN 1 END) AS swing_seats,
           COUNT(*) AS total_seats
    FROM fact_2026_swing sw JOIN dim_constituency dc ON sw.constituency_id=dc.constituency_id
    {"WHERE dc.region='" + region_filter + "'" if region_filter != 'All' else ''}
    GROUP BY dc.district, dc.region ORDER BY dc.district
""")
map_agg["lat"] = map_agg["district"].map(lambda d: TN_DISTRICT_COORDS.get(d,(10.5,78.5))[0])
map_agg["lon"] = map_agg["district"].map(lambda d: TN_DISTRICT_COORDS.get(d,(10.5,78.5))[1])
map_agg["dominant"] = map_agg.apply(lambda r: "DMK Alliance" if r["dmk_avg"]>=r["aiadmk_avg"] else "AIADMK", axis=1)
map_agg["margin"] = abs(map_agg["dmk_avg"]-map_agg["aiadmk_avg"])
map_agg["is_swing"] = map_agg["margin"] < 10

fig_map = go.Figure()
for dom, color in [("DMK Alliance","#D40000"),("AIADMK","#00AA44")]:
    sub = map_agg[map_agg["dominant"]==dom]
    fig_map.add_trace(go.Scattergeo(
        lat=sub["lat"], lon=sub["lon"], name=dom, mode="markers+text",
        marker=dict(size=sub["total_seats"]*2.2, color=color, opacity=0.7,
                    symbol=["diamond" if s else "circle" for s in sub["is_swing"]],
                    line=dict(width=1.5, color="white")),
        text=sub["district"].str[:7], textposition="top center",
        textfont=dict(size=8, color="#c8d8e8"),
        customdata=sub[["district","dmk_avg","aiadmk_avg","tvk_avg","swing_seats","total_seats"]].values,
        hovertemplate="<b>%{customdata[0]}</b><br>DMK strength: %{customdata[1]}<br>AIADMK strength: %{customdata[2]}<br>TVK avg: %{customdata[3]}%<br>Swing seats: %{customdata[4]}/%{customdata[5]}<extra></extra>"))

fig_map.update_layout(
    geo=dict(scope="asia", projection_type="mercator", center=dict(lat=10.8,lon=78.5),
             lataxis_range=[7.5,14], lonaxis_range=[76,81.5],
             bgcolor="rgba(0,0,0,0)", landcolor="#0e1e2e", showland=True,
             oceancolor="#04111e", showocean=True,
             coastlinecolor="#1e3a5a", showcoastlines=True,
             countrycolor="#1e3a5a", showcountries=True),
    paper_bgcolor="rgba(0,0,0,0)", font_color="#c8d8e8",
    legend=dict(bgcolor="rgba(0,0,0,0)", x=0.02, y=0.98),
    margin=dict(l=0,r=0,t=10,b=0), height=460)
st.plotly_chart(fig_map, use_container_width=True)
st.markdown("<p style='font-size:.72rem;color:#3a6080;text-align:center;'>Circle = strong seat. Diamond = swing seat (&lt;10 pt margin). Bubble size = number of Assembly segments. Hover for scores.</p>", unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

# ── SWING SEAT ANALYSIS TABLE ─────────────────────────────────────────────────
st.markdown('<div class="sec">Constituency-Level Swing Intelligence</div>', unsafe_allow_html=True)

disp = leaders.merge(
    swing_df[["constituency_id","dmk_strength_score","aiadmk_strength_score","ntk_vote_split_pct","tvk_factor_pct","bjp_factor_pct","confidence_score"]],
    on="constituency_id",
    how="left",
)[["constituency_name","district","region","type","candidate_name","party_abbr","bloc_name","forecast_win_prob","forecast_band","dmk_strength_score","aiadmk_strength_score","ntk_vote_split_pct","tvk_factor_pct","bjp_factor_pct","confidence_score"]].copy()
disp["bloc_name"] = disp["bloc_name"].map({"DMK Alliance":"DMK Alliance","AIADMK Alliance":"AIADMK Alliance","NTK":"NTK","TVK":"TVK"}).fillna(disp["bloc_name"])

st.dataframe(
    disp.rename(columns={"constituency_name":"Constituency","district":"District","region":"Region","type":"Type",
                          "candidate_name":"Projected Candidate","party_abbr":"Party","bloc_name":"Projected Bloc",
                          "forecast_win_prob":"Win Prob",
                          "dmk_strength_score":"DMK","aiadmk_strength_score":"AIADMK",
                          "ntk_vote_split_pct":"NTK %","tvk_factor_pct":"TVK %","bjp_factor_pct":"BJP %",
                          "forecast_band":"Band","confidence_score":"Confidence"}),
    use_container_width=True, hide_index=True,
    column_config={
        "DMK": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f"),
        "AIADMK": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f"),
        "Win Prob": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
        "Confidence": st.column_config.NumberColumn(format="%.2f"),
    })

# ── TVK IMPACT SCATTER ─────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown('<div class="sec">TVK Impact vs Seat Competitiveness — Where Vijay\'s Party Can Tip Results</div>', unsafe_allow_html=True)
st.markdown('<div class="insight">The X-axis shows TVK\'s expected vote draw and the Y-axis shows DMK win probability. <strong>Mid-probability seats with high TVK factor are the places where Vijay\'s party can most materially tilt the forecast.</strong></div>', unsafe_allow_html=True)

tvk_plot = leaders.merge(
    swing_df[["constituency_id","tvk_factor_pct","ntk_vote_split_pct","dmk_strength_score","aiadmk_strength_score"]],
    on="constituency_id",
    how="left",
)
fig_tvk = px.scatter(tvk_plot, x="tvk_factor_pct", y="forecast_win_prob",
    color="projected_winner", hover_name="constituency_name",
    hover_data={"district":True,"dmk_strength_score":True,"aiadmk_strength_score":True,"candidate_name":True},
    color_discrete_map={"DMK Alliance":"#D40000","AIADMK Alliance":"#00AA44","NTK":"#8b8b8b","TVK":"#9B59B6"},
    size="ntk_vote_split_pct", size_max=16,
    labels={"tvk_factor_pct":"TVK Factor %","forecast_win_prob":"Leader Win Probability","projected_winner":"Projected Winner"})
fig_tvk.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",font_color="#c8d8e8",
    xaxis=dict(gridcolor="#102030",title="TVK Vote Factor %"),
    yaxis=dict(gridcolor="#102030",title="Leader Win Probability"),
    legend=dict(bgcolor="rgba(0,0,0,0)"),margin=dict(l=0,r=0,t=10,b=0),height=380)
st.plotly_chart(fig_tvk, use_container_width=True)

# ── DISCLAIMER ─────────────────────────────────────────────────────────────
st.markdown("""<div style='background:#04111e;border:1px solid #0e2233;border-radius:8px;padding:.8rem 1rem;font-size:.75rem;color:#3a6080;line-height:1.6;margin-top:1rem;'>
<strong style='color:#3a7aaa;'>📋 Model Note:</strong> 2026 strength scores are modelled from historical DMK/AIADMK win patterns (2001–2021), demographic trends, and regional base calibration. 
They are <em>not</em> live polling data. Win probabilities are produced by converting constituency strength margins into forecast odds through a logistic curve. 
TVK and NTK factors are scenario inputs based on prior vote-split patterns and the modeled 2026 environment.
</div>""", unsafe_allow_html=True)
