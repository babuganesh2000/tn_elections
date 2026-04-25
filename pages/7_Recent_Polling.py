"""Page 7: Recent Polling Analysis - post-April 23 survey read."""
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.app_guard import stop_if_data_unverified
from utils.db import get_connection, get_data_status, get_forecast_2026, get_swing_analysis, query
from utils.ui import inject_enterprise_theme, render_demo_banner, render_sidebar_branding


st.set_page_config(
    page_title="Recent Polling Analysis · TN Election 2026 · Modeled Election Dashboard",
    page_icon="📡",
    layout="wide",
)

st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');
html,body,[class*="css"]{font-family:'IBM Plex Sans',sans-serif;}
#MainMenu,footer{visibility:hidden;}
.sec{font-family:'IBM Plex Mono',monospace;font-size:.65rem;color:#1e6fc0;letter-spacing:.18em;text-transform:uppercase;border-bottom:1px solid #102030;padding-bottom:.35rem;margin-bottom:.8rem;}
.poll-note{background:#071929;border:1px solid #102a40;border-left:3px solid #1e6fc0;border-radius:0 8px 8px 0;padding:.8rem 1rem;font-size:.84rem;color:#b8d1e6;line-height:1.65;margin:.6rem 0;}
.source-card{background:#071929;border:1px solid #102a40;border-radius:10px;padding:.8rem 1rem;margin-bottom:.7rem;color:#b8d1e6;font-size:.82rem;line-height:1.6;}
.source-card strong{color:#f0f8ff;}
.source-card a{color:#9fd7ff;text-decoration:none;}
.callout{background:linear-gradient(135deg,#07131f,#0e2035);border:1px solid #163350;border-radius:10px;padding:1rem 1.1rem;color:#c8d8e8;line-height:1.65;}
.callout h4{margin:.1rem 0 .4rem;color:#f0f8ff;font-size:1rem;}
div[data-testid="metric-container"]{background:linear-gradient(135deg,#071520,#0e2035);border:1px solid #163350;border-radius:10px;padding:.75rem;}
div[data-testid="metric-container"] label{color:#9bbdda!important;font-size:.68rem!important;letter-spacing:.08em!important;text-transform:uppercase!important;font-family:'IBM Plex Mono',monospace!important;}
div[data-testid="metric-container"] [data-testid="stMetricValue"]{color:#f4f9ff!important;font-size:1.55rem!important;font-weight:700!important;font-family:'IBM Plex Mono',monospace!important;}
</style>
""",
    unsafe_allow_html=True,
)
inject_enterprise_theme()


@st.cache_resource(show_spinner=False)
def load_db():
    return get_connection()


con = load_db()
stop_if_data_unverified()
if get_data_status().get("mode") == "demo":
    render_demo_banner()

with st.sidebar:
    render_sidebar_branding("TN Election 2026", "Recent polling analysis · Survey spread and turnout shock")
    st.divider()
    st.caption("Last updated from public sources checked on 25 Apr 2026.")


POLL_ROWS = [
    {
        "pollster": "IANS-Matrize",
        "field_date": "15 Mar 2026",
        "sample_size": 17410,
        "dmk_vote": 37.5,
        "aiadmk_vote": 39.5,
        "tvk_vote": 14.5,
        "ntk_vote": 11.0,
        "dmk_seat_low": None,
        "dmk_seat_high": None,
        "aiadmk_seat_low": None,
        "aiadmk_seat_high": None,
        "tvk_seat_low": None,
        "tvk_seat_high": None,
        "source": "Wikipedia polling table",
    },
    {
        "pollster": "News18-Vote Vibe",
        "field_date": "23 Mar 2026",
        "sample_size": 7992,
        "dmk_vote": 40.0,
        "aiadmk_vote": 38.0,
        "tvk_vote": 15.0,
        "ntk_vote": 7.0,
        "dmk_seat_low": None,
        "dmk_seat_high": None,
        "aiadmk_seat_low": None,
        "aiadmk_seat_high": None,
        "tvk_seat_low": None,
        "tvk_seat_high": None,
        "source": "Wikipedia polling table",
    },
    {
        "pollster": "Agni News Agency",
        "field_date": "23 Mar 2026",
        "sample_size": 101643,
        "dmk_vote": 44.9,
        "aiadmk_vote": 38.5,
        "tvk_vote": 9.7,
        "ntk_vote": 6.9,
        "dmk_seat_low": None,
        "dmk_seat_high": None,
        "aiadmk_seat_low": None,
        "aiadmk_seat_high": None,
        "tvk_seat_low": None,
        "tvk_seat_high": None,
        "source": "Wikipedia polling table",
    },
    {
        "pollster": "Lok Poll",
        "field_date": "1 Apr 2026",
        "sample_size": 117000,
        "dmk_vote": 40.1,
        "aiadmk_vote": 29.0,
        "tvk_vote": 23.9,
        "ntk_vote": 4.9,
        "dmk_seat_low": 181,
        "dmk_seat_high": 189,
        "aiadmk_seat_low": 38,
        "aiadmk_seat_high": 42,
        "tvk_seat_low": 8,
        "tvk_seat_high": 10,
        "source": "Lok Poll / NDTV / Moneycontrol",
    },
    {
        "pollster": "Poll Tracker",
        "field_date": "2 Apr 2026",
        "sample_size": None,
        "dmk_vote": 42.7,
        "aiadmk_vote": None,
        "tvk_vote": 19.2,
        "ntk_vote": 5.1,
        "dmk_seat_low": 172,
        "dmk_seat_high": 178,
        "aiadmk_seat_low": 46,
        "aiadmk_seat_high": 52,
        "tvk_seat_low": 6,
        "tvk_seat_high": 12,
        "source": "NDTV summary of Poll Tracker",
    },
    {
        "pollster": "News18-Vote Vibe",
        "field_date": "6 Apr 2026",
        "sample_size": 24943,
        "dmk_vote": 39.0,
        "aiadmk_vote": 41.0,
        "tvk_vote": 12.0,
        "ntk_vote": 8.0,
        "dmk_seat_low": None,
        "dmk_seat_high": None,
        "aiadmk_seat_low": None,
        "aiadmk_seat_high": None,
        "tvk_seat_low": None,
        "tvk_seat_high": None,
        "source": "Wikipedia polling table",
    },
    {
        "pollster": "Spick Media",
        "field_date": "17 Apr 2026",
        "sample_size": 126801,
        "dmk_vote": 37.47,
        "aiadmk_vote": 38.85,
        "tvk_vote": 14.81,
        "ntk_vote": 8.47,
        "dmk_seat_low": None,
        "dmk_seat_high": None,
        "aiadmk_seat_low": None,
        "aiadmk_seat_high": None,
        "tvk_seat_low": None,
        "tvk_seat_high": None,
        "source": "Wikipedia polling table",
    },
    {
        "pollster": "Junior Vikatan",
        "field_date": "18 Apr 2026",
        "sample_size": 93600,
        "dmk_vote": 37.5,
        "aiadmk_vote": 33.63,
        "tvk_vote": 24.71,
        "ntk_vote": 4.16,
        "dmk_seat_low": None,
        "dmk_seat_high": None,
        "aiadmk_seat_low": None,
        "aiadmk_seat_high": None,
        "tvk_seat_low": None,
        "tvk_seat_high": None,
        "source": "Wikipedia polling table",
    },
    {
        "pollster": "Dinamalar",
        "field_date": "20 Apr 2026",
        "sample_size": 25691,
        "dmk_vote": 32.0,
        "aiadmk_vote": 36.0,
        "tvk_vote": 23.0,
        "ntk_vote": 9.0,
        "dmk_seat_low": None,
        "dmk_seat_high": None,
        "aiadmk_seat_low": None,
        "aiadmk_seat_high": None,
        "tvk_seat_low": None,
        "tvk_seat_high": None,
        "source": "Wikipedia polling table",
    },
]


polls = pd.DataFrame(POLL_ROWS)
polls["dmk_margin_vs_aiadmk"] = polls["dmk_vote"] - polls["aiadmk_vote"]
polls["tvk_high_share"] = polls["tvk_vote"] >= 20

forecast = get_forecast_2026(con)
swing = get_swing_analysis(con)
model_projection = (
    forecast.groupby("bloc_name", as_index=False)["forecast_win_prob"]
    .sum()
    .rename(columns={"forecast_win_prob": "expected_seats"})
    .sort_values("expected_seats", ascending=False)
)
model_map = {row["bloc_name"]: row["expected_seats"] for _, row in model_projection.iterrows()}
polls_with_dmk = polls[polls["dmk_vote"].notna()]
polls_with_tvk = polls[polls["tvk_vote"].notna()]

OFFICIAL_SIR = {
    "pre_sir_electors": 64114587,
    "draft_after_enumeration": 54376756,
    "final_roll_electors": 56707380,
    "claims_period_additions": 2753000,
    "claims_period_deletions": 423000,
    "poll_day_electorate_rounded": 57300000,
    "official_turnout_pct": 84.69,
    "official_male_turnout_pct": 83.57,
    "official_female_turnout_pct": 85.76,
    "official_third_gender_turnout_pct": 60.49,
}
OFFICIAL_SIR["enumeration_net_reduction"] = (
    OFFICIAL_SIR["pre_sir_electors"] - OFFICIAL_SIR["draft_after_enumeration"]
)
OFFICIAL_SIR["final_net_reduction"] = (
    OFFICIAL_SIR["pre_sir_electors"] - OFFICIAL_SIR["final_roll_electors"]
)
OFFICIAL_SIR["final_net_reduction_pct"] = (
    OFFICIAL_SIR["final_net_reduction"] / OFFICIAL_SIR["pre_sir_electors"] * 100
)
OFFICIAL_SIR["estimated_votes_polled"] = round(
    OFFICIAL_SIR["poll_day_electorate_rounded"] * OFFICIAL_SIR["official_turnout_pct"] / 100
)

st.markdown('<div class="sec">Recent Polling Read · After 23 April Voting</div>', unsafe_allow_html=True)
st.markdown(
    """
<div class="poll-note">
Tamil Nadu voted on <strong>23 April 2026</strong>. Public pre-poll surveys broadly agreed on one direction:
DMK-led forces had the clearest path to government. They disagreed sharply on the size of that lead, especially
because TVK's vote share was estimated anywhere from the low teens to nearly a quarter of the electorate.
The record turnout after polling day makes this a volatility page, not a result prediction page.
</div>
""",
    unsafe_allow_html=True,
)

m1, m2, m3, m4, m5 = st.columns(5)
with m1:
    st.metric("Polling Day", "23 Apr")
with m2:
    st.metric("ECI Turnout", "84.69%")
with m3:
    st.metric("DMK Vote Range", f"{polls_with_dmk['dmk_vote'].min():.1f}-{polls_with_dmk['dmk_vote'].max():.1f}%")
with m4:
    st.metric("AIADMK Vote Range", f"{polls['aiadmk_vote'].min():.1f}-{polls['aiadmk_vote'].max():.1f}%")
with m5:
    st.metric("TVK Vote Range", f"{polls_with_tvk['tvk_vote'].min():.1f}-{polls_with_tvk['tvk_vote'].max():.1f}%")

left, right = st.columns([1.15, 1])
with left:
    st.markdown('<div class="sec">Vote Share Spread Across Polls</div>', unsafe_allow_html=True)
    fig_vote = go.Figure()
    colors = {
        "DMK / SPA": "#D40000",
        "AIADMK / NDA": "#00AA44",
        "TVK": "#9B59B6",
        "NTK": "#8b8b8b",
    }
    series = [
        ("DMK / SPA", "dmk_vote"),
        ("AIADMK / NDA", "aiadmk_vote"),
        ("TVK", "tvk_vote"),
        ("NTK", "ntk_vote"),
    ]
    for label, col in series:
        fig_vote.add_trace(
            go.Scatter(
                x=polls["field_date"],
                y=polls[col],
                mode="lines+markers",
                name=label,
                line=dict(width=2.5, color=colors[label]),
                marker=dict(size=8),
                connectgaps=True,
                hovertemplate=f"<b>{label}</b><br>%{{x}}: %{{y:.2f}}%<extra></extra>",
            )
        )
    fig_vote.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#c8d8e8",
        xaxis=dict(gridcolor="#102030"),
        yaxis=dict(gridcolor="#102030", title="Vote share %", range=[0, 50]),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
        margin=dict(l=0, r=0, t=10, b=0),
        height=380,
    )
    st.plotly_chart(fig_vote, use_container_width=True)

with right:
    st.markdown('<div class="sec">What Changed After April 23</div>', unsafe_allow_html=True)
    st.markdown(
        """
<div class="callout">
<h4>Turnout is the new uncertainty layer</h4>
The Election Commission's provisional poll-participation release put Tamil Nadu turnout at 84.69%, the state's
highest Assembly poll participation since Independence. That can mean stronger party machinery, higher
anti-incumbency mobilisation, youth mobilisation for TVK, or all three at once.
</div>
""",
        unsafe_allow_html=True,
    )
    st.markdown(
        """
<div class="poll-note">
The cleanest analytical read is this: DMK still entered polling day with the broadest survey advantage, but TVK's
high-variance vote share makes constituency-level conversion much harder than statewide vote share implies.
</div>
""",
        unsafe_allow_html=True,
    )

st.markdown('<div class="sec">Seat Projection Ranges Published Publicly</div>', unsafe_allow_html=True)
seat_polls = polls[polls["dmk_seat_low"].notna()].copy()
seat_polls["dmk_mid"] = (seat_polls["dmk_seat_low"] + seat_polls["dmk_seat_high"]) / 2
seat_polls["aiadmk_mid"] = (seat_polls["aiadmk_seat_low"] + seat_polls["aiadmk_seat_high"]) / 2
seat_polls["tvk_mid"] = (seat_polls["tvk_seat_low"] + seat_polls["tvk_seat_high"]) / 2

fig_seats = go.Figure()
for label, low_col, high_col, color in [
    ("DMK / SPA", "dmk_seat_low", "dmk_seat_high", "#D40000"),
    ("AIADMK / NDA", "aiadmk_seat_low", "aiadmk_seat_high", "#00AA44"),
    ("TVK", "tvk_seat_low", "tvk_seat_high", "#9B59B6"),
]:
    mid = (seat_polls[low_col] + seat_polls[high_col]) / 2
    err = (seat_polls[high_col] - seat_polls[low_col]) / 2
    fig_seats.add_trace(
        go.Bar(
            name=label,
            x=seat_polls["pollster"],
            y=mid,
            error_y=dict(type="data", array=err, visible=True, color="#c8d8e8"),
            marker_color=color,
            text=[f"{int(lo)}-{int(hi)}" for lo, hi in zip(seat_polls[low_col], seat_polls[high_col])],
            textposition="outside",
            hovertemplate=f"<b>{label}</b><br>%{{x}}<br>Seats: %{{text}}<extra></extra>",
        )
    )
fig_seats.add_hline(y=118, line_dash="dot", line_color="#9dbbd6", annotation_text="Majority 118")
fig_seats.update_layout(
    barmode="group",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font_color="#c8d8e8",
    xaxis=dict(gridcolor="#102030"),
    yaxis=dict(gridcolor="#102030", title="Seats", range=[0, 210]),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
    margin=dict(l=0, r=0, t=10, b=0),
    height=360,
)
st.plotly_chart(fig_seats, use_container_width=True)

st.markdown('<div class="sec">Model vs Polling Read</div>', unsafe_allow_html=True)
model_left, model_right = st.columns([1, 1])
with model_left:
    model_display = pd.DataFrame(
        [
            {"Bloc": "DMK Alliance", "Expected Seats": model_map.get("DMK Alliance", 0)},
            {"Bloc": "AIADMK Alliance", "Expected Seats": model_map.get("AIADMK Alliance", 0)},
            {"Bloc": "NTK", "Expected Seats": model_map.get("NTK", 0)},
            {"Bloc": "TVK", "Expected Seats": model_map.get("TVK", 0)},
        ]
    )
    fig_model = go.Figure(
        go.Bar(
            x=model_display["Bloc"],
            y=model_display["Expected Seats"],
            marker_color=["#D40000", "#00AA44", "#8b8b8b", "#9B59B6"],
            text=model_display["Expected Seats"].round(1),
            textposition="outside",
        )
    )
    fig_model.add_hline(y=118, line_dash="dot", line_color="#9dbbd6")
    fig_model.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#c8d8e8",
        xaxis=dict(gridcolor="#102030"),
        yaxis=dict(gridcolor="#102030", title="Expected seats", range=[0, 210]),
        margin=dict(l=0, r=0, t=10, b=0),
        height=310,
    )
    st.plotly_chart(fig_model, use_container_width=True)
with model_right:
    swing_summary = pd.DataFrame(
        [
            {"Signal": "Swing seats in local model", "Value": int(swing["swing_seat_flag"].sum())},
            {"Signal": "Avg TVK factor in local model", "Value": round(float(swing["tvk_factor_pct"].mean()), 1)},
            {"Signal": "Avg NTK split in local model", "Value": round(float(swing["ntk_vote_split_pct"].mean()), 1)},
            {"Signal": "Polls with TVK at 20%+", "Value": int(polls["tvk_high_share"].sum())},
        ]
    )
    st.dataframe(swing_summary, use_container_width=True, hide_index=True)
    st.markdown(
        """
<div class="poll-note">
The app's existing forecast remains a scenario model. This page does not overwrite it with poll numbers; it exposes
where the public polling environment is wider or narrower than the local model assumptions.
</div>
""",
        unsafe_allow_html=True,
    )

st.markdown('<div class="sec">Polling Table</div>', unsafe_allow_html=True)
table = polls[
    [
        "pollster",
        "field_date",
        "sample_size",
        "dmk_vote",
        "aiadmk_vote",
        "dmk_margin_vs_aiadmk",
        "tvk_vote",
        "ntk_vote",
        "source",
    ]
].rename(
    columns={
        "pollster": "Pollster",
        "field_date": "Date",
        "sample_size": "Sample",
        "dmk_vote": "DMK / SPA %",
        "aiadmk_vote": "AIADMK / NDA %",
        "dmk_margin_vs_aiadmk": "DMK Margin",
        "tvk_vote": "TVK %",
        "ntk_vote": "NTK %",
        "source": "Source",
    }
)
st.dataframe(
    table,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Sample": st.column_config.NumberColumn(format="%d"),
        "DMK / SPA %": st.column_config.NumberColumn(format="%.2f"),
        "AIADMK / NDA %": st.column_config.NumberColumn(format="%.2f"),
        "DMK Margin": st.column_config.NumberColumn(format="%.2f"),
        "TVK %": st.column_config.NumberColumn(format="%.2f"),
        "NTK %": st.column_config.NumberColumn(format="%.2f"),
    },
)

st.markdown('<div class="sec">SIR vs Turnout Effect</div>', unsafe_allow_html=True)
st.markdown(
    f"""
<div class="poll-note">
This section separates two questions: did more people vote, or did turnout percentage rise because SIR cleaned the
electoral-roll denominator? The official state-level SIR roll moved from
<strong>{OFFICIAL_SIR["pre_sir_electors"]:,}</strong> electors before SIR to
<strong>{OFFICIAL_SIR["final_roll_electors"]:,}</strong> in the final roll, a net reduction of
<strong>{OFFICIAL_SIR["final_net_reduction_pct"]:.1f}%</strong>. The ECI poll-day release reported
<strong>{OFFICIAL_SIR["official_turnout_pct"]:.2f}%</strong> provisional turnout.
</div>
""",
    unsafe_allow_html=True,
)

sir_m1, sir_m2, sir_m3, sir_m4, sir_m5 = st.columns(5)
with sir_m1:
    st.metric("Pre-SIR Roll", f"{OFFICIAL_SIR['pre_sir_electors'] / 10_000_000:.2f} cr")
with sir_m2:
    st.metric("Draft After SIR", f"{OFFICIAL_SIR['draft_after_enumeration'] / 10_000_000:.2f} cr")
with sir_m3:
    st.metric("Final Roll", f"{OFFICIAL_SIR['final_roll_electors'] / 10_000_000:.2f} cr")
with sir_m4:
    st.metric("Net Roll Change", f"-{OFFICIAL_SIR['final_net_reduction_pct']:.1f}%")
with sir_m5:
    st.metric("Estimated Votes", f"{OFFICIAL_SIR['estimated_votes_polled'] / 10_000_000:.2f} cr")

sir_left, sir_right = st.columns([1.05, 1])
with sir_left:
    fig_sir = go.Figure(
        go.Waterfall(
            name="SIR roll movement",
            orientation="v",
            measure=["absolute", "relative", "relative", "total"],
            x=["Pre-SIR roll", "Enumeration cleanup", "Claims net add", "Final roll"],
            y=[
                OFFICIAL_SIR["pre_sir_electors"],
                -OFFICIAL_SIR["enumeration_net_reduction"],
                OFFICIAL_SIR["claims_period_additions"] - OFFICIAL_SIR["claims_period_deletions"],
                OFFICIAL_SIR["final_roll_electors"],
            ],
            connector={"line": {"color": "#9dbbd6"}},
            increasing={"marker": {"color": "#39c58d"}},
            decreasing={"marker": {"color": "#ff6b6b"}},
            totals={"marker": {"color": "#2a7fff"}},
            text=[
                f"{OFFICIAL_SIR['pre_sir_electors'] / 10_000_000:.2f} cr",
                f"-{OFFICIAL_SIR['enumeration_net_reduction'] / 100000:.1f} lakh",
                f"+{(OFFICIAL_SIR['claims_period_additions'] - OFFICIAL_SIR['claims_period_deletions']) / 100000:.1f} lakh",
                f"{OFFICIAL_SIR['final_roll_electors'] / 10_000_000:.2f} cr",
            ],
            textposition="outside",
        )
    )
    fig_sir.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#c8d8e8",
        yaxis=dict(gridcolor="#102030", title="Electors"),
        margin=dict(l=0, r=0, t=10, b=0),
        height=340,
    )
    st.plotly_chart(fig_sir, use_container_width=True)

with sir_right:
    st.markdown(
        """
<div class="callout">
<h4>How to read the correlation</h4>
If SIR reduction is high and turnout percentage rises, check whether actual votes polled rose too. A high
percentage with flat votes polled is mostly a denominator effect. A high percentage with higher votes polled is
real mobilisation plus roll cleanup.
</div>
""",
        unsafe_allow_html=True,
    )
    st.markdown(
        """
<div class="poll-note">
Use constituency-level data where possible. District aggregation is useful for scanning, but AC-level analysis is
better because SIR cleanup and turnout can vary sharply inside the same district.
</div>
""",
        unsafe_allow_html=True,
    )

st.markdown('<div class="sec">Optional ECINET / CEO CSV Correlation</div>', unsafe_allow_html=True)
st.markdown(
    """
<div class="poll-note">
Upload a district or constituency CSV to compute the correlation directly. Expected columns:
<strong>unit</strong>, <strong>electors_2021</strong>, <strong>votes_polled_2021</strong>,
<strong>pre_sir_electors_2025</strong>, <strong>final_electors_2026</strong>,
<strong>votes_polled_2026</strong>. Optional columns: <strong>region</strong>,
<strong>turnout_pct_2021</strong>, <strong>turnout_pct_2026</strong>.
</div>
""",
    unsafe_allow_html=True,
)
uploaded_sir = st.file_uploader("Upload SIR turnout CSV", type=["csv"], key="sir_turnout_csv")

if uploaded_sir is not None:
    sir_df = pd.read_csv(uploaded_sir)
    sir_df.columns = [c.strip().lower() for c in sir_df.columns]
    required_cols = {
        "unit",
        "electors_2021",
        "votes_polled_2021",
        "pre_sir_electors_2025",
        "final_electors_2026",
        "votes_polled_2026",
    }
    missing_cols = sorted(required_cols.difference(sir_df.columns))
    if missing_cols:
        st.error(f"Missing required columns: {', '.join(missing_cols)}")
    else:
        for col in required_cols.difference({"unit"}):
            sir_df[col] = pd.to_numeric(sir_df[col], errors="coerce")
        if "turnout_pct_2021" not in sir_df.columns:
            sir_df["turnout_pct_2021"] = 100 * sir_df["votes_polled_2021"] / sir_df["electors_2021"]
        if "turnout_pct_2026" not in sir_df.columns:
            sir_df["turnout_pct_2026"] = 100 * sir_df["votes_polled_2026"] / sir_df["final_electors_2026"]
        sir_df["sir_net_change_pct"] = (
            100 * (sir_df["final_electors_2026"] - sir_df["pre_sir_electors_2025"])
            / sir_df["pre_sir_electors_2025"]
        )
        sir_df["turnout_change_pp"] = sir_df["turnout_pct_2026"] - sir_df["turnout_pct_2021"]
        sir_df["votes_polled_change_pct"] = (
            100 * (sir_df["votes_polled_2026"] - sir_df["votes_polled_2021"])
            / sir_df["votes_polled_2021"]
        )
        sir_df["electorate_change_pct"] = (
            100 * (sir_df["final_electors_2026"] - sir_df["electors_2021"])
            / sir_df["electors_2021"]
        )
        clean_sir = sir_df.dropna(
            subset=["sir_net_change_pct", "turnout_change_pp", "votes_polled_change_pct"]
        )
        corr_turnout = clean_sir["sir_net_change_pct"].corr(clean_sir["turnout_change_pp"])
        corr_votes = clean_sir["sir_net_change_pct"].corr(clean_sir["votes_polled_change_pct"])

        cm1, cm2, cm3 = st.columns(3)
        with cm1:
            st.metric("Rows Analysed", f"{len(clean_sir):,}")
        with cm2:
            st.metric("SIR vs Turnout Corr", f"{corr_turnout:.2f}" if pd.notna(corr_turnout) else "N/A")
        with cm3:
            st.metric("SIR vs Votes Corr", f"{corr_votes:.2f}" if pd.notna(corr_votes) else "N/A")

        sc1, sc2 = st.columns(2)
        with sc1:
            fig_corr = go.Figure(
                go.Scatter(
                    x=clean_sir["sir_net_change_pct"],
                    y=clean_sir["turnout_change_pp"],
                    mode="markers",
                    text=clean_sir["unit"],
                    marker=dict(
                        size=(clean_sir["final_electors_2026"] / clean_sir["final_electors_2026"].max() * 24) + 6,
                        color="#2a7fff",
                        opacity=0.72,
                        line=dict(width=1, color="#c8d8e8"),
                    ),
                    hovertemplate="<b>%{text}</b><br>SIR net change: %{x:.2f}%<br>Turnout change: %{y:.2f} pp<extra></extra>",
                )
            )
            fig_corr.add_hline(y=0, line_dash="dot", line_color="#9dbbd6")
            fig_corr.add_vline(x=0, line_dash="dot", line_color="#9dbbd6")
            fig_corr.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#c8d8e8",
                xaxis=dict(gridcolor="#102030", title="SIR net roll change %"),
                yaxis=dict(gridcolor="#102030", title="Turnout change, 2021 to 2026 (pp)"),
                margin=dict(l=0, r=0, t=10, b=0),
                height=360,
            )
            st.plotly_chart(fig_corr, use_container_width=True)
        with sc2:
            fig_votes = go.Figure(
                go.Scatter(
                    x=clean_sir["sir_net_change_pct"],
                    y=clean_sir["votes_polled_change_pct"],
                    mode="markers",
                    text=clean_sir["unit"],
                    marker=dict(
                        size=(clean_sir["final_electors_2026"] / clean_sir["final_electors_2026"].max() * 24) + 6,
                        color="#39c58d",
                        opacity=0.72,
                        line=dict(width=1, color="#c8d8e8"),
                    ),
                    hovertemplate="<b>%{text}</b><br>SIR net change: %{x:.2f}%<br>Votes polled change: %{y:.2f}%<extra></extra>",
                )
            )
            fig_votes.add_hline(y=0, line_dash="dot", line_color="#9dbbd6")
            fig_votes.add_vline(x=0, line_dash="dot", line_color="#9dbbd6")
            fig_votes.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#c8d8e8",
                xaxis=dict(gridcolor="#102030", title="SIR net roll change %"),
                yaxis=dict(gridcolor="#102030", title="Votes polled change, 2021 to 2026 %"),
                margin=dict(l=0, r=0, t=10, b=0),
                height=360,
            )
            st.plotly_chart(fig_votes, use_container_width=True)

        st.dataframe(
            clean_sir[
                [
                    "unit",
                    "sir_net_change_pct",
                    "electorate_change_pct",
                    "turnout_pct_2021",
                    "turnout_pct_2026",
                    "turnout_change_pp",
                    "votes_polled_change_pct",
                ]
            ].sort_values("turnout_change_pp", ascending=False),
            use_container_width=True,
            hide_index=True,
            column_config={
                "sir_net_change_pct": st.column_config.NumberColumn("SIR Net Change %", format="%.2f"),
                "electorate_change_pct": st.column_config.NumberColumn("Electorate Change %", format="%.2f"),
                "turnout_pct_2021": st.column_config.NumberColumn("Turnout 2021 %", format="%.2f"),
                "turnout_pct_2026": st.column_config.NumberColumn("Turnout 2026 %", format="%.2f"),
                "turnout_change_pp": st.column_config.NumberColumn("Turnout Change pp", format="%.2f"),
                "votes_polled_change_pct": st.column_config.NumberColumn("Votes Polled Change %", format="%.2f"),
            },
        )
else:
    model_electorate = query(
        con,
        """
        SELECT dc.region,
               SUM(CASE WHEN fv.election_year=2021 THEN fv.total_electors ELSE 0 END) AS electors_2021,
               SUM(CASE WHEN fv.election_year=2026 THEN fv.total_electors ELSE 0 END) AS modeled_electors_2026,
               AVG(CASE WHEN fv.election_year=2021 THEN fv.turnout_pct END) AS turnout_2021
        FROM fact_voter_demographics fv
        JOIN dim_constituency dc ON fv.constituency_id=dc.constituency_id
        WHERE fv.election_year IN (2021, 2026)
        GROUP BY dc.region
        ORDER BY dc.region
        """,
    )
    model_electorate["modeled_electorate_change_pct"] = (
        100 * (model_electorate["modeled_electors_2026"] - model_electorate["electors_2021"])
        / model_electorate["electors_2021"]
    )
    st.markdown(
        """
<div class="poll-note">
No ECINET/CEO SIR CSV is loaded yet. The table below is only the app's modeled electorate baseline by region,
not official SIR deletion data. Upload official AC or district data to activate the actual correlation charts.
</div>
""",
        unsafe_allow_html=True,
    )
    st.dataframe(
        model_electorate,
        use_container_width=True,
        hide_index=True,
        column_config={
            "electors_2021": st.column_config.NumberColumn("Electors 2021", format="%d"),
            "modeled_electors_2026": st.column_config.NumberColumn("Modeled Electors 2026", format="%d"),
            "turnout_2021": st.column_config.NumberColumn("Turnout 2021 %", format="%.2f"),
            "modeled_electorate_change_pct": st.column_config.NumberColumn("Modeled Electorate Change %", format="%.2f"),
        },
    )

st.markdown('<div class="sec">Source Notes</div>', unsafe_allow_html=True)
st.markdown(
    """
<div class="source-card">
<strong>Lok Poll and Poll Tracker:</strong> NDTV summarized two April pre-poll surveys. Lok Poll put DMK at
181-189 seats and TVK at 8-10; Poll Tracker put DMK at 172-178 and TVK at 6-12.
<br><a href="https://www.ndtv.com/india-news/dmk-nda-or-tvk-who-has-edge-in-2026-tamil-nadu-assembly-polls-what-surveys-found-11310123" target="_blank">NDTV survey summary</a>
</div>
<div class="source-card">
<strong>Lok Poll methodology:</strong> Moneycontrol/PTI reported Lok Poll's 117,000 respondent sample and published
the 40.1 / 29.0 / 23.9 / 4.9 vote-share split.
<br><a href="https://www.moneycontrol.com/elections/assembly-election/tamil-nadu/lok-poll-survey-projects-comfortable-victory-for-dmk-led-alliance-with-40-vote-share-article-13877146.html" target="_blank">Moneycontrol/PTI report</a>
</div>
<div class="source-card">
<strong>Official turnout:</strong> The Election Commission press release published by PIB recorded Tamil Nadu's
provisional poll participation at 84.69%: male 83.57%, female 85.76%, and third gender 60.49%. It notes that
district-wise and Assembly-constituency approximate turnout figures are available in the ECINET app.
<br><a href="https://www.pib.gov.in/PressReleasePage.aspx?PRID=2255010" target="_blank">PIB / Election Commission press release</a>
</div>
<div class="source-card">
<strong>Polling-day context:</strong> Indian Express reported roughly 84.6% turnout, the highest Assembly turnout
in Tamil Nadu since 1952, and highlighted Chennai's strong turnout.
<br><a href="https://indianexpress.com/article/political-pulse/tamil-nadu-polling-turnout-2026-record-assembly-elections-10652401/" target="_blank">Indian Express turnout report</a>
</div>
<div class="source-card">
<strong>Broader polling list:</strong> The Wikipedia election page aggregates multiple Tamil Nadu 2026 opinion polls,
including March and April vote-share estimates. Treat it as an index and verify primary links before using numbers in formal reporting.
<br><a href="https://en.wikipedia.org/wiki/2026_Tamil_Nadu_Legislative_Assembly_election#Opinion_polls" target="_blank">Wikipedia opinion-poll table</a>
</div>
""",
    unsafe_allow_html=True,
)
