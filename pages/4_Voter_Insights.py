"""Page 4: Voter Insights — Demographics, Youth Influence Score, gender, margins"""
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.db import (get_connection, get_data_status, get_youth_pct_per_constituency, get_youth_turnout_scatter,
                      get_gender_ratio_by_region, get_top_winning_margins, get_closest_contests, query)
from utils.app_guard import stop_if_data_unverified
from utils.ui import inject_enterprise_theme, render_demo_banner, render_masthead, render_sidebar_branding

st.set_page_config(page_title="Voter Insights · TN Election 2026 · Modeled Election Dashboard", page_icon="📊", layout="wide")
st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700;800&family=IBM+Plex+Sans:wght@400;500&family=IBM+Plex+Mono:wght@400;500&display=swap');
html,body,[class*="css"]{font-family:'IBM Plex Sans',sans-serif;}
#MainMenu,footer{visibility:hidden;}
.pg-title{font-family:'Playfair Display',serif;font-size:2.1rem;font-weight:800;color:#e8f4fd;margin:0;}
.sec{font-family:'IBM Plex Mono',monospace;font-size:.65rem;color:#1e6fc0;letter-spacing:.18em;text-transform:uppercase;border-bottom:1px solid #102030;padding-bottom:.35rem;margin-bottom:.8rem;}
div[data-testid="metric-container"]{background:linear-gradient(135deg,#071520,#0e2035);border:1px solid #163350;border-radius:10px;padding:.75rem;}
div[data-testid="metric-container"] label{color:#3a7aaa!important;font-size:.65rem!important;letter-spacing:.08em!important;text-transform:uppercase!important;font-family:'IBM Plex Mono',monospace!important;}
div[data-testid="metric-container"] [data-testid="stMetricValue"]{color:#e0f0ff!important;font-size:1.5rem!important;font-weight:700!important;font-family:'IBM Plex Mono',monospace!important;}
.formula{background:#071929;border:1px solid #102a40;border-radius:8px;padding:.7rem 1rem;font-family:'IBM Plex Mono',monospace;font-size:.8rem;color:#4a9acc;margin:.5rem 0;}
.insight{background:#071929;border:1px solid #102a40;border-left:3px solid #1e6fc0;border-radius:0 8px 8px 0;padding:.7rem 1rem;font-size:.82rem;color:#7090a8;line-height:1.65;margin:.5rem 0;}
</style>""", unsafe_allow_html=True)
inject_enterprise_theme()

REGION_CLR = {"Chennai":"#00d4ff","North":"#7eb8f7","South":"#ff9f43","West":"#26de81","Central":"#a29bfe","Delta":"#fd9644"}

@st.cache_resource(show_spinner=False)
def load_db(): return get_connection()
con = load_db()
stop_if_data_unverified()
if get_data_status().get("mode") == "demo":
    render_demo_banner()

with st.sidebar:
    render_sidebar_branding("TN Election 2026", "Modeled election dashboard · Electorate analytics")
    st.divider()
    sel_year = st.select_slider("Election Year", [2001,2006,2011,2016,2021], value=2021)
    st.divider()
    st.markdown("<small style='color:#3a6a8e;'>ECI electoral roll patterns</small>", unsafe_allow_html=True)

agg = query(con, f"""SELECT SUM(total_electors) AS te,
    ROUND(100.0*SUM(female_electors)/SUM(total_electors),2) AS fp,
    ROUND(100.0*SUM(age_18_19+age_20_29)/SUM(total_electors),2) AS yp,
    ROUND(AVG(turnout_pct),1) AS at
    FROM fact_voter_demographics WHERE election_year={sel_year}""")
r = agg.iloc[0]
m1,m2,m3,m4 = st.columns(4)
with m1: st.metric("Total Electorate", f"{int(r['te']):,}")
with m2: st.metric("Female Electors", f"{r['fp']:.1f}%")
with m3: st.metric("Youth (18–29)", f"{r['yp']:.1f}%")
tp = r['at']
with m4: st.metric("Avg Turnout", f"{tp:.1f}%" if tp and not pd.isna(tp) else "N/A")
st.markdown("<br>", unsafe_allow_html=True)

tab1,tab2,tab3,tab4,tab5 = st.tabs(["📊 Age Distribution","⚡ Youth Influence","♀ Gender Split","🏆 Top Margins","🔥 Close Contests"])

with tab1:
    st.markdown('<div class="sec">State-Wide Age Distribution</div>', unsafe_allow_html=True)
    age_agg = query(con, f"""SELECT SUM(age_18_19) AS a18,SUM(age_20_29) AS a29,SUM(age_30_39) AS a39,
        SUM(age_40_49) AS a49,SUM(age_50_59) AS a59,SUM(age_60_plus) AS a60
        FROM fact_voter_demographics WHERE election_year={sel_year}""")
    ar = age_agg.iloc[0]
    labels = ["18–19","20–29","30–39","40–49","50–59","60+"]
    vals = [int(ar[c]) for c in ["a18","a29","a39","a49","a59","a60"]]
    total_v = sum(vals)
    pcts = [100*v/max(1,total_v) for v in vals]
    fig = go.Figure(go.Bar(x=labels, y=pcts,
        marker_color=["#00d4ff","#0099ff","#2e6da4","#1a4a6e","#0d2d45","#071a2a"],
        text=[f"{p:.1f}%" for p in pcts], textposition="outside",
        customdata=vals,
        hovertemplate="Age %{x}<br>Share: %{y:.2f}%<br>Count: %{customdata:,}<extra></extra>"))
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",font_color="#c8d8e8",
        xaxis=dict(gridcolor="#102030"),yaxis=dict(gridcolor="#102030",title="% of Electorate"),
        margin=dict(l=0,r=0,t=10,b=0),height=300)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown('<div class="sec" style="margin-top:1rem;">Youth % Trend by Region — 2001–2021</div>', unsafe_allow_html=True)
    yt = query(con, """SELECT fv.election_year, dc.region,
        ROUND(100.0*SUM(fv.age_18_19+fv.age_20_29)/SUM(fv.total_electors),2) AS youth_pct
        FROM fact_voter_demographics fv JOIN dim_constituency dc ON fv.constituency_id=dc.constituency_id
        WHERE fv.election_year<=2021
        GROUP BY fv.election_year,dc.region ORDER BY fv.election_year""")
    fig2 = go.Figure()
    for reg in yt["region"].unique():
        sub = yt[yt["region"]==reg]
        fig2.add_trace(go.Scatter(x=sub["election_year"],y=sub["youth_pct"],name=reg,mode="lines+markers",
            line=dict(width=2.5,color=REGION_CLR.get(reg,"#aaa")),marker=dict(size=7)))
    fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",font_color="#c8d8e8",
        xaxis=dict(tickvals=[2001,2006,2011,2016,2021],gridcolor="#102030"),
        yaxis=dict(gridcolor="#102030",title="Youth % (18–29)"),
        legend=dict(bgcolor="rgba(0,0,0,0)"),margin=dict(l=0,r=0,t=10,b=0),height=260)
    st.plotly_chart(fig2, use_container_width=True)
    st.markdown('<div class="insight"><strong>Key insight:</strong> Youth share is declining across all regions — from ~28–32% in 2001 to ~22–26% in 2021 as TN\'s population ages. Chennai retains the highest youth density due to urban in-migration and large college populations. South TN shows slower youth decline — rural demographics.</div>', unsafe_allow_html=True)

with tab2:
    st.markdown('<div class="sec">Youth Influence Score — Constituency Ranking</div>', unsafe_allow_html=True)
    st.markdown('<div class="formula">Youth Influence Score (YIS) = Youth % × Turnout % ÷ 100</div>', unsafe_allow_html=True)
    st.markdown('<div class="insight">YIS measures <strong>mobilised youth voting power</strong>. High YIS = youth both exist AND voted. Low YIS despite high youth % = untapped mobilisation opportunity — critical insight for party campaign targeting in 2026.</div>', unsafe_allow_html=True)

    sc = get_youth_turnout_scatter(con, sel_year)
    if not sc.empty:
        fig_s = px.scatter(sc, x="youth_pct", y="turnout_pct", color="region",
            size="youth_influence_score",
            hover_name="constituency_name",
            hover_data={"district":True,"youth_influence_score":":.2f","region":False},
            color_discrete_map=REGION_CLR, size_max=22,
            labels={"youth_pct":"Youth % (18–29)","turnout_pct":"Turnout %","region":"Region"})
        fig_s.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
            font_color="#c8d8e8",
            xaxis=dict(gridcolor="#102030"),yaxis=dict(gridcolor="#102030"),
            legend=dict(bgcolor="rgba(0,0,0,0)"),margin=dict(l=0,r=0,t=10,b=0),height=420)
        st.plotly_chart(fig_s, use_container_width=True)

        c1,c2 = st.columns(2)
        with c1:
            st.markdown('<div class="sec">Top 15 — Highest YIS</div>', unsafe_allow_html=True)
            st.dataframe(sc.head(15)[["constituency_name","district","region","youth_pct","turnout_pct","youth_influence_score"]].rename(
                columns={"constituency_name":"Constituency","district":"District","region":"Region",
                         "youth_pct":"Youth %","turnout_pct":"Turnout %","youth_influence_score":"YIS"}),
                use_container_width=True, hide_index=True,
                column_config={"YIS":st.column_config.ProgressColumn(min_value=0,max_value=28,format="%.2f")})
        with c2:
            st.markdown('<div class="sec">Bottom 15 — Lowest YIS (latent potential)</div>', unsafe_allow_html=True)
            st.dataframe(sc.tail(15)[["constituency_name","district","region","youth_pct","turnout_pct","youth_influence_score"]].rename(
                columns={"constituency_name":"Constituency","district":"District","region":"Region",
                         "youth_pct":"Youth %","turnout_pct":"Turnout %","youth_influence_score":"YIS"}),
                use_container_width=True, hide_index=True,
                column_config={"YIS":st.column_config.ProgressColumn(min_value=0,max_value=28,format="%.2f")})

with tab3:
    st.markdown('<div class="sec">Gender Distribution by Region</div>', unsafe_allow_html=True)
    gdf = get_gender_ratio_by_region(con)
    fig_g = go.Figure()
    fig_g.add_trace(go.Bar(name="Female",x=gdf["region"],y=gdf["female_pct"],
        marker_color="#ff6b9d",text=[f"{v:.2f}%" for v in gdf["female_pct"]],textposition="outside"))
    fig_g.add_trace(go.Bar(name="Male",x=gdf["region"],y=100-gdf["female_pct"],
        marker_color="#2e6da4",text=[f"{100-v:.2f}%" for v in gdf["female_pct"]],textposition="outside"))
    fig_g.update_layout(barmode="stack",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
        font_color="#c8d8e8",xaxis=dict(gridcolor="#102030"),
        yaxis=dict(gridcolor="#102030",title="%",range=[0,105]),
        legend=dict(bgcolor="rgba(0,0,0,0)"),margin=dict(l=0,r=0,t=10,b=0),height=300)
    st.plotly_chart(fig_g, use_container_width=True)
    st.markdown('<div class="insight">TN\'s female voter registration is remarkably balanced — Delta region slightly leads at 50.06% female, reflecting matrilineal social patterns. This near-parity is significant: female voter turnout in TN consistently matches or exceeds male turnout, unlike most Indian states.</div>', unsafe_allow_html=True)
    st.dataframe(gdf.rename(columns={"region":"Region","total_male":"Male","total_female":"Female",
        "total_third":"Third Gender","female_pct":"Female %"}),
        use_container_width=True, hide_index=True,
        column_config={"Male":st.column_config.NumberColumn(format="%d"),
                       "Female":st.column_config.NumberColumn(format="%d"),
                       "Female %":st.column_config.NumberColumn(format="%.2f%%")})

with tab4:
    st.markdown(f'<div class="sec">Top 20 Winning Margins — {sel_year}</div>', unsafe_allow_html=True)
    st.markdown('<div class="insight">Large margins signal <strong>incumbent entrenchment or wave elections</strong>. 2011 saw the highest average margins (AIADMK wave). 2016 saw compressed margins as DMK recovered. Use alongside swing scores for 2026 targeting.</div>', unsafe_allow_html=True)
    mdf = get_top_winning_margins(con, sel_year)
    if not mdf.empty:
        fig_m = go.Figure(go.Bar(
            y=mdf["constituency_name"], x=mdf["margin_votes"], orientation="h",
            marker_color="#26de81",
            text=[f"{int(v):,}" for v in mdf["margin_votes"]], textposition="outside",
            customdata=mdf[["candidate_name","party_abbr","district"]].values,
            hovertemplate="<b>%{y}</b> (%{customdata[2]})<br>Winner: %{customdata[0]} (%{customdata[1]})<br>Margin: %{x:,}<extra></extra>"))
        fig_m.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
            font_color="#c8d8e8",
            xaxis=dict(gridcolor="#102030",title="Winning Margin (Votes)"),
            yaxis=dict(autorange="reversed"),
            margin=dict(l=0,r=80,t=10,b=0),height=520)
        st.plotly_chart(fig_m, use_container_width=True)

with tab5:
    st.markdown(f'<div class="sec">20 Closest Contests — {sel_year}</div>', unsafe_allow_html=True)
    st.markdown('<div class="insight">Seats with margins under 5,000 votes are <strong>prime swing targets</strong> in 2026. These constituencies are where NTK vote splits, TVK entries, or BJP consolidation can flip the result. Cross-reference with 2026 Swing Intelligence page.</div>', unsafe_allow_html=True)
    cdf = get_closest_contests(con, sel_year)
    if not cdf.empty:
        fig_c = go.Figure(go.Bar(
            y=cdf["constituency_name"], x=cdf["margin_votes"], orientation="h",
            marker_color="#ff6b6b",
            text=[f"{int(v):,}" for v in cdf["margin_votes"]], textposition="outside",
            customdata=cdf[["candidate_name","party_abbr","district"]].values,
            hovertemplate="<b>%{y}</b> (%{customdata[2]})<br>Winner: %{customdata[0]} (%{customdata[1]})<br>Margin: %{x:,}<extra></extra>"))
        fig_c.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
            font_color="#c8d8e8",
            xaxis=dict(gridcolor="#102030",title="Winning Margin (Votes)"),
            yaxis=dict(autorange="reversed"),
            margin=dict(l=0,r=80,t=10,b=0),height=520)
        st.plotly_chart(fig_c, use_container_width=True)
