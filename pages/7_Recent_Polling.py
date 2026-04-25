"""Page 7: Recent Polling Analysis - post-April 23 survey read."""
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.app_guard import stop_if_data_unverified
from utils.db import get_connection, get_data_status, get_forecast_2026, get_swing_analysis
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
