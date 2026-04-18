"""Page 1: Constituency Explorer — TN district map + 5-election trend + photos"""
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.db import (get_connection, get_data_status, get_all_constituencies, get_constituency_results,
                      get_constituency_nominations_2026, get_constituency_trend, get_constituency_forecast,
                      get_constituency_swing, get_youth_pct_per_constituency, query)
from utils.app_guard import stop_if_data_unverified
from utils.image_cache import find_local_image
from utils.ui import inject_enterprise_theme, render_demo_banner, render_masthead, render_sidebar_branding
from utils.wiki import get_wikipedia_url

st.set_page_config(page_title="Constituency Explorer · TN Election 2026 · Modeled Election Dashboard", page_icon="🗺️", layout="wide")
st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700;800&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap');
html,body,[class*="css"]{font-family:'IBM Plex Sans',sans-serif;}
#MainMenu,footer{visibility:hidden;}
.pg-title{font-family:'Playfair Display',serif;font-size:2.1rem;font-weight:800;color:#e8f4fd;margin:0;}
.sec{font-family:'IBM Plex Mono',monospace;font-size:.65rem;color:#1e6fc0;letter-spacing:.18em;text-transform:uppercase;border-bottom:1px solid #102030;padding-bottom:.35rem;margin-bottom:.8rem;}
div[data-testid="metric-container"]{background:linear-gradient(135deg,#071520,#0e2035);border:1px solid #163350;border-radius:10px;padding:.75rem;}
div[data-testid="metric-container"] label{color:#3a7aaa!important;font-size:.65rem!important;letter-spacing:.08em!important;text-transform:uppercase!important;font-family:'IBM Plex Mono',monospace!important;}
div[data-testid="metric-container"] [data-testid="stMetricValue"]{color:#e0f0ff!important;font-size:1.5rem!important;font-weight:700!important;font-family:'IBM Plex Mono',monospace!important;}
.swing-box{border-radius:8px;padding:.8rem 1rem;font-size:.82rem;line-height:1.6;margin:.5rem 0;}
</style>""", unsafe_allow_html=True)
inject_enterprise_theme()

# TN District centroids for map
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

@st.cache_resource(show_spinner=False)
def load_db(): return get_connection()
con = load_db()
stop_if_data_unverified()
data_status = get_data_status()
if data_status.get("mode") == "demo":
    render_demo_banner()

PARTY_CLR = {"DMK":"#D40000","INC":"#19AAED","VCK":"#FF6600","CPM":"#CC0000","CPI":"#AA0000",
             "MDMK":"#880000","IUML":"#009900","TMC(M)":"#3399FF","AIADMK":"#00AA44",
             "PMK":"#FF9900","BJP":"#FF7722","NTK":"#888888","DMDK":"#884400","IND":"#446688",
             "TVK":"#9B59B6","AMMK":"#226622"}

# ── SIDEBAR ──
with st.sidebar:
    render_sidebar_branding("TN Election 2026", "Modeled election dashboard · Constituency performance")
all_c = get_all_constituencies(con)

def _apply_constituency_filters(df, region="All", district="All", seat_type="All", constituency="All"):
    out = df.copy()
    if region not in {"All", None, ""}:
        out = out[out["region"] == region]
    if district not in {"All", None, ""}:
        out = out[out["district"] == district]
    if seat_type not in {"All", None, ""}:
        out = out[out["type"] == seat_type]
    if constituency not in {"All", None, "", "—"}:
        out = out[out["constituency_name"] == constituency]
    return out

def _valid_or_default(value, options, fallback="All"):
    if value in options:
        return value
    if fallback in options:
        return fallback
    return options[0] if options else fallback

region_key = "ce_region"
district_key = "ce_district"
type_key = "ce_type"
name_key = "ce_name"
year_key = "ce_year"

query_constituency = st.query_params.get("constituency", "All")
if query_constituency and query_constituency != st.session_state.get(name_key):
    st.session_state[name_key] = query_constituency

raw_region = st.session_state.get(region_key, "All")
raw_district = st.session_state.get(district_key, "All")
raw_type = st.session_state.get(type_key, "All")
raw_name = st.session_state.get(name_key, "All")

region_options = ["All"] + sorted(
    _apply_constituency_filters(all_c, district=raw_district, seat_type=raw_type, constituency=raw_name)["region"].dropna().unique().tolist()
)
sel_region = _valid_or_default(raw_region, region_options)

district_options = ["All"] + sorted(
    _apply_constituency_filters(all_c, region=sel_region, seat_type=raw_type, constituency=raw_name)["district"].dropna().unique().tolist()
)
sel_district = _valid_or_default(raw_district, district_options)

seat_type_options = ["All"] + sorted(
    _apply_constituency_filters(all_c, region=sel_region, district=sel_district, constituency=raw_name)["type"].dropna().unique().tolist()
)
sel_type = _valid_or_default(raw_type, seat_type_options)

constituency_options = ["All"] + sorted(
    _apply_constituency_filters(all_c, region=sel_region, district=sel_district, seat_type=sel_type)["constituency_name"].dropna().unique().tolist()
)
sel_name = _valid_or_default(raw_name, constituency_options)

filtered = _apply_constituency_filters(all_c, region=sel_region, district=sel_district, seat_type=sel_type, constituency=sel_name)

st.markdown('<div class="sec">Constituency Filters</div>', unsafe_allow_html=True)
f1, f2, f3, f4, f5 = st.columns([1, 1, 1, 1.4, 1])
with f1:
    sel_region = st.selectbox("Region", region_options, index=region_options.index(sel_region), key=region_key)
with f2:
    sel_district = st.selectbox("District", district_options, index=district_options.index(sel_district), key=district_key)
with f3:
    sel_type = st.selectbox("Seat Type", seat_type_options, index=seat_type_options.index(sel_type), key=type_key)
with f4:
    sel_name = st.selectbox("Select Constituency", constituency_options, index=constituency_options.index(sel_name), key=name_key)
with f5:
    hist_year = st.selectbox("Historical Year", [2021, 2016, 2011, 2006, 2001], index=0, key=year_key)

filtered = _apply_constituency_filters(all_c, region=sel_region, district=sel_district, seat_type=sel_type, constituency=sel_name)
st.caption(f"{len(filtered)} constituencies in current filter")

# ── CONSTITUENCY DETAIL ────────────────────────────────────────────────────────
if sel_name and sel_name not in {"—", "All"}:
    const_row = all_c[all_c["constituency_name"]==sel_name].iloc[0]
    cid = int(const_row["constituency_id"])

    m1,m2,m3,m4 = st.columns(4)
    with m1: st.metric("Constituency", sel_name)
    with m2: st.metric("District", const_row["district"])
    with m3: st.metric("Seat Type", const_row["type"])
    with m4: st.metric("Region", const_row["region"])

    tab_hist, tab_trend, tab_2026, tab_swing, tab_demo = st.tabs(
        [f"📜 {hist_year} Results", "📈 20-Year Trend", "🗳️ 2026 Forecast", "🔮 2026 Swing", "👥 Demographics"])

    with tab_hist:
        st.markdown(f'<div class="sec">{hist_year} Results – {sel_name}</div>', unsafe_allow_html=True)
        res = get_constituency_results(con, cid, hist_year)
        if res.empty:
            st.info("No data for this year.")
        else:
            winner = res[res["winner_flag"]==True].iloc[0]
            w1,w2,w3,w4 = st.columns(4)
            with w1: st.metric("Winner", winner["candidate_name"])
            with w2: st.metric("Party", winner["party_abbr"])
            with w3: st.metric("Votes", f"{int(winner['votes']):,}")
            margin = winner.get("margin_votes")
            with w4: st.metric("Margin", f"{int(margin):,}" if margin and not pd.isna(margin) else "—")

            # Photo if celebrity
            celeb_rows = res[res["celebrity_flag"]==True]
            if not celeb_rows.empty:
                for _,cr in celeb_rows.iterrows():
                    if cr.get("photo_url"):
                        pcol1,pcol2 = st.columns([1,4])
                        with pcol1:
                            local_image = find_local_image(cr["candidate_id"])
                            st.image(str(local_image) if local_image else cr["photo_url"], width=90)
                        with pcol2:
                            wiki_url = get_wikipedia_url(cr["candidate_name"])
                            st.markdown(f"**{cr['candidate_name']}** ({cr['party_abbr']})<br><small style='color:#5c8aaa;'>Celebrity · {cr['party_name'] if 'party_name' in cr else ''}</small><br><a href='{wiki_url}' target='_blank' style='color:#9fd7ff;text-decoration:none;'>🌐 Wikipedia profile</a>", unsafe_allow_html=True)

            colors = [PARTY_CLR.get(p,"#446688") for p in res["party_abbr"]]
            fig = go.Figure(go.Bar(y=res["candidate_name"], x=res["vote_share"], orientation="h",
                marker_color=colors, text=[f"{v:.1f}%" for v in res["vote_share"]], textposition="outside",
                hovertemplate="<b>%{y}</b><br>%{x:.1f}%<extra></extra>"))
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                font_color="#c8d8e8",xaxis=dict(gridcolor="#102030",title="Vote Share %"),
                yaxis=dict(autorange="reversed"),margin=dict(l=0,r=60,t=10,b=0),
                height=max(250,len(res)*34))
            st.plotly_chart(fig, use_container_width=True)

    with tab_trend:
        st.markdown(f'<div class="sec">20-Year Winner Trend – {sel_name}</div>', unsafe_allow_html=True)
        trend_data = get_constituency_trend(con, cid)
        if trend_data.empty:
            st.info("No trend data.")
        else:
            fig_t = go.Figure()
            colors_t = [PARTY_CLR.get(p,"#446688") for p in trend_data["party_abbr"]]
            fig_t.add_trace(go.Bar(x=trend_data["election_year"], y=trend_data["vote_share"],
                marker_color=colors_t, text=trend_data["party_abbr"], textposition="outside",
                customdata=trend_data[["candidate_name","margin_votes","turnout_pct"]].values,
                hovertemplate="<b>%{text}</b><br>Year: %{x}<br>Vote Share: %{y:.1f}%<br>Winner: %{customdata[0]}<br>Margin: %{customdata[1]:,}<br>Turnout: %{customdata[2]:.1f}%<extra></extra>"))
            fig_t.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                font_color="#c8d8e8",xaxis=dict(tickvals=[2001,2006,2011,2016,2021],gridcolor="#102030"),
                yaxis=dict(gridcolor="#102030",title="Winner Vote Share %"),
                margin=dict(l=0,r=0,t=10,b=0),height=280)
            st.plotly_chart(fig_t, use_container_width=True)
            st.dataframe(trend_data.rename(columns={"election_year":"Year","party_abbr":"Party","alliance_name":"Alliance","votes":"Votes","vote_share":"Vote %","margin_votes":"Margin","turnout_pct":"Turnout %"}),
                use_container_width=True, hide_index=True)

    with tab_2026:
        st.markdown(f'<div class="sec">2026 Forecast – {sel_name}</div>', unsafe_allow_html=True)
        noms = get_constituency_nominations_2026(con, cid)
        forecast = get_constituency_forecast(con, cid)
        if noms.empty: st.info("No 2026 data.")
        else:
            contesting = noms[noms["final_contesting_flag"]==True]
            st.markdown(f"**{len(contesting)} confirmed contesting** | {len(noms)} total nominations filed")
            if not forecast.empty:
                lead = forecast.iloc[0]
                f1, f2, f3 = st.columns(3)
                with f1: st.metric("Forecast Leader", lead["candidate_name"])
                with f2: st.metric("Bloc", lead["bloc_name"])
                with f3: st.metric("Win Probability", f"{lead['forecast_win_prob'] * 100:.1f}%")
                st.dataframe(
                    forecast[["candidate_name","party_abbr","bloc_name","forecast_win_prob","forecast_band"]].rename(
                        columns={
                            "candidate_name":"Candidate",
                            "party_abbr":"Party",
                            "bloc_name":"Bloc",
                            "forecast_win_prob":"Win Prob",
                            "forecast_band":"Band",
                        }
                    ),
                    use_container_width=True,
                    hide_index=True,
                    column_config={"Win Prob": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f")},
                )
                st.divider()
            celebs = noms[noms["celebrity_flag"]==True]
            if not celebs.empty:
                st.markdown("⭐ **Celebrity Candidates**")
                for _,cr in celebs.iterrows():
                    pc1,pc2 = st.columns([1,5])
                    with pc1:
                        if cr.get("photo_url"):
                            local_image = find_local_image(cr["candidate_id"])
                            st.image(str(local_image) if local_image else cr["photo_url"], width=80)
                    with pc2:
                        wiki_url = get_wikipedia_url(cr["candidate_name"])
                        st.markdown(f"**{cr['candidate_name']}** · {cr['party_abbr']} ({cr['alliance_name']})")
                        st.markdown(f"<small style='color:#5c8aaa;'>{cr.get('celebrity_type','politician')} · {'✅ Contesting' if cr['final_contesting_flag'] else '⏳ Pending'}</small>", unsafe_allow_html=True)
                        affidavit_url = cr.get("affidavit_url")
                        if data_status.get("mode") == "demo" or pd.isna(affidavit_url) or affidavit_url in (None, ""):
                            st.markdown(f"[🌐 Wikipedia]({wiki_url}) · `Affidavit unavailable in demo`")
                        else:
                            st.markdown(f"[🌐 Wikipedia]({wiki_url}) · [📄 Affidavit]({affidavit_url})")
                st.divider()
            disp = noms[["candidate_name","gender","party_abbr","alliance_name","forecast_win_prob","forecast_band","nomination_status","scrutiny_status","withdrawal_status","final_contesting_flag"]]
            st.dataframe(disp.rename(columns={"candidate_name":"Candidate","gender":"G","party_abbr":"Party","alliance_name":"Alliance","nomination_status":"Nom","scrutiny_status":"Scrutiny","withdrawal_status":"Withdrawal","final_contesting_flag":"✅"}),
                use_container_width=True, hide_index=True,
                column_config={"forecast_win_prob": st.column_config.ProgressColumn("Win Prob", min_value=0, max_value=1, format="%.2f"),
                               "forecast_band": "Band",
                               "✅": st.column_config.CheckboxColumn()})

    with tab_swing:
        st.markdown(f'<div class="sec">2026 Swing Intelligence – {sel_name}</div>', unsafe_allow_html=True)
        sw = get_constituency_swing(con, cid)
        if sw.empty: st.info("No swing data.")
        else:
            r = sw.iloc[0]
            bg = "#1a0808" if r["expected_winner_2026"]=="DMK Alliance" else "#081a08"
            border = "#D40000" if r["expected_winner_2026"]=="DMK Alliance" else "#00AA44"
            st.markdown(f"""<div class="swing-box" style="background:{bg};border:1px solid {border};border-left:4px solid {border};">
<strong style="color:#e0f0ff;font-size:1rem;">{'⚠️ Swing Seat' if r['swing_seat_flag'] else '🔵 '+r['expected_winner_2026']}</strong><br>
<span style="color:#7090a8;">{r['analysis_note']}</span>
</div>""", unsafe_allow_html=True)

            s1,s2,s3,s4,s5 = st.columns(5)
            with s1: st.metric("DMK Strength", f"{r['dmk_strength_score']}/100")
            with s2: st.metric("AIADMK Strength", f"{r['aiadmk_strength_score']}/100")
            with s3: st.metric("NTK Split %", f"{r['ntk_vote_split_pct']}%")
            with s4: st.metric("TVK Factor %", f"{r['tvk_factor_pct']}%")
            with s5: st.metric("BJP Base %", f"{r['bjp_factor_pct']}%")

            fig_sw = go.Figure(go.Bar(
                x=["DMK Alliance","AIADMK","NTK","TVK","BJP"],
                y=[r["dmk_strength_score"],r["aiadmk_strength_score"],r["ntk_vote_split_pct"],r["tvk_factor_pct"],r["bjp_factor_pct"]],
                marker_color=["#D40000","#00AA44","#888","#9B59B6","#FF7722"],
                text=[f"{v:.1f}" for v in [r["dmk_strength_score"],r["aiadmk_strength_score"],r["ntk_vote_split_pct"],r["tvk_factor_pct"],r["bjp_factor_pct"]]],
                textposition="outside"))
            fig_sw.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                font_color="#c8d8e8",yaxis=dict(gridcolor="#102030",title="Score / %"),
                margin=dict(l=0,r=0,t=10,b=0),height=260)
            st.plotly_chart(fig_sw, use_container_width=True)

    with tab_demo:
        st.markdown(f'<div class="sec">Voter Demographics – {sel_name}</div>', unsafe_allow_html=True)
        demo = query(con, f"""SELECT election_year,total_electors,male_electors,female_electors,
            age_18_19,age_20_29,age_30_39,age_40_49,age_50_59,age_60_plus,turnout_pct
            FROM fact_voter_demographics WHERE constituency_id={cid} ORDER BY election_year""")
        if demo.empty: st.info("No demographic data.")
        else:
            sel_dy = st.select_slider("Year", demo["election_year"].tolist(), value=2021)
            dr = demo[demo["election_year"]==sel_dy].iloc[0]
            d1,d2,d3,d4 = st.columns(4)
            with d1: st.metric("Total Electors", f"{int(dr['total_electors']):,}")
            with d2: st.metric("Male", f"{int(dr['male_electors']):,}")
            with d3: st.metric("Female", f"{int(dr['female_electors']):,}")
            tp = dr["turnout_pct"]
            with d4: st.metric("Turnout", f"{tp:.1f}%" if tp and not pd.isna(tp) else "TBD (2026)")
            labels=["18–19","20–29","30–39","40–49","50–59","60+"]
            vals=[int(dr[c]) for c in ["age_18_19","age_20_29","age_30_39","age_40_49","age_50_59","age_60_plus"]]
            fig_d = go.Figure(go.Bar(x=labels,y=vals,
                marker_color=["#00d4ff","#0099ff","#2e6da4","#1a4a6e","#0d2d45","#071a2a"],
                text=[f"{v:,}" for v in vals],textposition="outside"))
            fig_d.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                font_color="#c8d8e8",yaxis=dict(gridcolor="#102030",title="Electors"),
                margin=dict(l=0,r=0,t=10,b=0),height=260)
            st.plotly_chart(fig_d, use_container_width=True)

# ── CONSTITUENCY TABLE ───────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown('<div class="sec">All Constituencies in Current Filter</div>', unsafe_allow_html=True)
youth = get_youth_pct_per_constituency(con, 2021)
year_winners = query(con, f"""
    SELECT dconst.constituency_name,
           dc.candidate_name AS winner_name,
           dp.party_abbr AS winner_party,
           fr.margin_votes,
           fr.vote_share AS winner_vote_share
    FROM fact_results fr
    JOIN dim_constituency dconst ON fr.constituency_id=dconst.constituency_id
    JOIN dim_candidate dc ON fr.candidate_id=dc.candidate_id
    JOIN dim_party dp ON fr.party_id=dp.party_id
    WHERE fr.election_year={hist_year} AND fr.winner_flag=true
""")
merged = (
    filtered
    .merge(youth[["constituency_name","youth_pct","turnout_pct"]], on="constituency_name", how="left")
    .merge(year_winners, on="constituency_name", how="left")
)
merged["Open"] = merged["constituency_name"].apply(lambda name: f"?constituency={name.replace(' ', '%20')}")

st.dataframe(
    merged[["constituency_name","Open","district","type","region","winner_name","winner_party","winner_vote_share","margin_votes","youth_pct","turnout_pct"]].rename(
        columns={
            "constituency_name":"Constituency",
            "district":"District",
            "type":"Type",
            "region":"Region",
            "winner_name":f"{hist_year} Winner",
            "winner_party":"Party",
            "winner_vote_share":"Vote %",
            "margin_votes":"Margin",
            "youth_pct":"Youth % (18-29)",
            "turnout_pct":"Turnout 2021 %",
        }
    ),
    use_container_width=True,
    hide_index=True,
    column_config={
        "Open": st.column_config.LinkColumn("Open", display_text="Open"),
        "Vote %": st.column_config.NumberColumn(format="%.2f"),
        "Margin": st.column_config.NumberColumn(format="%d"),
        "Youth % (18-29)": st.column_config.ProgressColumn(min_value=0, max_value=35, format="%.1f%%"),
        "Turnout 2021 %": st.column_config.ProgressColumn(min_value=50, max_value=90, format="%.1f%%"),
    },
)
