"""Shared enterprise UI theme for the Streamlit app."""

import streamlit as st


ENTERPRISE_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=Space+Grotesk:wght@500;700&display=swap');

:root{
  --bg:#0f2236;
  --bg-soft:#16304a;
  --panel:#1a3957;
  --panel-2:#21486a;
  --border:#4c7093;
  --border-soft:rgba(186,211,234,.26);
  --text:#f6fbff;
  --muted:#c2d7eb;
  --muted-2:#9dbbd6;
  --brand:#2a7fff;
  --brand-2:#1fc4ff;
  --brand-3:#9be8ff;
  --success:#39c58d;
  --shadow:0 18px 48px rgba(8,20,34,.22);
}

html, body, [class*="css"], [data-testid="stAppViewContainer"]{
  font-family:'Manrope',sans-serif;
  color:var(--text);
}

[data-testid="stAppViewContainer"]{
  background:
    radial-gradient(circle at 12% 0%, rgba(81,156,255,.28), transparent 30%),
    radial-gradient(circle at 88% 10%, rgba(54,205,255,.18), transparent 26%),
    linear-gradient(180deg, #15314d 0%, #122b43 34%, #10263d 100%);
}

[data-testid="stHeader"]{
  background:rgba(21,49,77,.72);
  border-bottom:1px solid rgba(186,211,234,.16);
  backdrop-filter: blur(12px);
  visibility: visible !important;
}

section[data-testid="stSidebar"]{
  background:
    linear-gradient(180deg, rgba(28,58,88,.98) 0%, rgba(19,42,64,.98) 100%);
  border-right:1px solid rgba(186,211,234,.18);
}

section[data-testid="stSidebar"] *{
  color:var(--text) !important;
}

#MainMenu, footer{visibility:hidden;}
[data-testid="collapsedControl"]{
  display:flex !important;
  visibility:visible !important;
}
.block-container{padding-top:1.2rem;padding-bottom:2rem;max-width:1360px;}

.tn-shell{
  position:relative;
  overflow:hidden;
  background:
    linear-gradient(135deg, rgba(255,255,255,.09) 0%, rgba(255,255,255,.03) 100%),
    linear-gradient(135deg, rgba(38,78,116,.95) 0%, rgba(24,51,79,.96) 65%);
  border:1px solid rgba(186,211,234,.18);
  border-radius:18px;
  box-shadow:var(--shadow);
  padding:.7rem .9rem .75rem .9rem;
  margin-bottom:.55rem;
}

.tn-shell::before{
  content:'';
  position:absolute;
  inset:0;
  background:
    linear-gradient(90deg, rgba(255,255,255,.08), transparent 24%),
    radial-gradient(circle at top right, rgba(122,226,255,.12), transparent 22%);
  pointer-events:none;
}

.tn-masthead{
  display:block;
}

.tn-brand{
  display:flex;
  align-items:center;
  gap:.7rem;
}

.tn-brand-mark{
  width:52px;
  height:52px;
  border-radius:16px;
  display:grid;
  place-items:center;
  position:relative;
  background:
    linear-gradient(180deg, rgba(255,255,255,.26), rgba(255,255,255,.07)),
    linear-gradient(145deg, #2a70b2 0%, #1c4f7d 55%, #173c5c 100%);
  border:1px solid rgba(187,220,247,.34);
  box-shadow:
    inset 0 1px 0 rgba(255,255,255,.14),
    0 12px 28px rgba(0,0,0,.28);
}

.tn-brand-mark::before{
  content:'';
  position:absolute;
  width:28px;
  height:28px;
  border-radius:50%;
  background:
    radial-gradient(circle at 30% 30%, rgba(255,255,255,.88), rgba(255,255,255,.12) 36%, transparent 37%),
    linear-gradient(180deg, #53b4ff 0%, #1f7dff 100%);
  box-shadow:0 0 0 4px rgba(18,48,77,.88);
}

.tn-brand-mark::after{
  content:'';
  position:absolute;
  width:7px;
  height:18px;
  border-radius:999px;
  background:linear-gradient(180deg, #d8efff 0%, #7bd0ff 100%);
  box-shadow:0 0 0 1px rgba(255,255,255,.08);
}

.tn-brand-copy{display:flex;flex-direction:column;gap:.28rem;}
.tn-kicker{
  font-size:.52rem;
  letter-spacing:.1em;
  text-transform:uppercase;
  color:var(--brand-3);
  font-weight:700;
}
.tn-title{
  font-family:'Space Grotesk',sans-serif;
  font-size:1.28rem;
  line-height:1.08;
  letter-spacing:-.04em;
  margin:0;
  color:var(--text);
}
.tn-title span{
  display:inline;
  color:#a7dbff;
}
.tn-desc{
  color:var(--muted);
  font-size:.78rem;
  line-height:1.35;
  max-width:980px;
  margin:.45rem 0 0 0;
}

.tn-chip-row{display:flex;flex-wrap:wrap;gap:.38rem;margin-top:.55rem;}
.tn-chip{
  display:inline-flex;
  align-items:center;
  gap:.45rem;
  padding:.22rem .48rem;
  border-radius:999px;
  background:rgba(255,255,255,.10);
  border:1px solid rgba(186,211,234,.20);
  color:#eef7ff;
  font-size:.62rem;
  font-weight:700;
}

.page-subtitle{
  color:var(--muted);
  margin-top:.35rem;
  margin-bottom:1rem;
  font-size:.96rem;
}

.pg-title, .hero-title{
  font-family:'Space Grotesk',sans-serif !important;
  color:var(--text) !important;
  letter-spacing:-.035em !important;
}

.hero-eyebrow, .section-hdr, .sec, .kpi-label, .hdr2{
  font-family:'Manrope',sans-serif !important;
  font-weight:800 !important;
  letter-spacing:.16em !important;
  color:var(--brand-3) !important;
}

.hero-desc, .insight, .insight-card, .note-box, .formula{
  color:var(--muted) !important;
}

.accent-line{
  width:96px !important;
  height:4px !important;
  border-radius:999px !important;
  background:linear-gradient(90deg, var(--brand), var(--brand-2), var(--brand-3)) !important;
  box-shadow:0 0 24px rgba(23,183,255,.24);
}

.kpi-card,
div[data-testid="metric-container"],
.insight-card,
.formula,
.note-box,
.story-card,
.celeb-card{
  background:
    linear-gradient(180deg, rgba(255,255,255,.11), rgba(255,255,255,.03)),
    linear-gradient(180deg, rgba(31,66,99,.94), rgba(22,49,76,.98)) !important;
  border:1px solid rgba(186,211,234,.20) !important;
  border-radius:18px !important;
  box-shadow:var(--shadow);
}

.kpi-card::before{
  height:3px !important;
  background:linear-gradient(90deg, var(--brand), var(--brand-2), var(--brand-3)) !important;
}

div[data-testid="metric-container"] label,
.kpi-label{
  color:#8fb9d8 !important;
}

div[data-testid="metric-container"] [data-testid="stMetricValue"],
.kpi-value{
  color:#f4f9ff !important;
}

.fact-tag, .badge, .tag{
  background:rgba(255,255,255,.10) !important;
  border:1px solid rgba(186,211,234,.22) !important;
  color:#f0f7ff !important;
  border-radius:999px !important;
  font-family:'Manrope',sans-serif !important;
  font-weight:700 !important;
}

.tn-sidecard{
  margin-bottom:1rem;
  padding:1rem;
  border-radius:18px;
  background:
    linear-gradient(180deg, rgba(255,255,255,.12), rgba(255,255,255,.03)),
    linear-gradient(180deg, rgba(35,73,108,.94), rgba(21,47,72,.98));
  border:1px solid rgba(186,211,234,.20);
  box-shadow:var(--shadow);
}
.tn-sidebrand{
  display:flex;
  align-items:center;
  gap:.8rem;
}
.tn-sidebrand .tn-brand-mark{
  width:46px;
  height:46px;
  border-radius:14px;
}
.tn-sidebrand .tn-brand-mark::before{
  width:25px;
  height:25px;
  box-shadow:0 0 0 4px rgba(18,48,77,.88);
}
.tn-sidebrand .tn-brand-mark::after{
  width:6px;
  height:15px;
}
.tn-side-title{
  font-family:'Space Grotesk',sans-serif;
  font-size:1rem;
  line-height:1.05;
  margin:0;
}
.tn-side-meta{
  margin-top:.2rem;
  color:var(--muted);
  font-size:.76rem;
  line-height:1.45;
}

.stSelectbox label, .stTextInput label, .stCheckbox label, .stSlider label, .stRadio label{
  color:#cfe2f3 !important;
  font-weight:700 !important;
}

.stTabs [data-baseweb="tab-list"]{
  gap:.4rem;
}
.stTabs [data-baseweb="tab"]{
  background:rgba(255,255,255,.08);
  border:1px solid rgba(186,211,234,.16);
  border-radius:14px 14px 0 0;
}

.stDataFrame, .stTable, [data-testid="stMetric"], [data-testid="stMarkdownContainer"] .insight, [data-testid="stMarkdownContainer"] .formula{
  color:var(--text);
}

div[data-baseweb="select"] > div,
div[data-baseweb="input"] > div{
  background:rgba(10,27,43,.72) !important;
  border:1px solid rgba(186,211,234,.18) !important;
}

.stSelectbox label, .stTextInput label, .stCheckbox label, .stSlider label, .stRadio label{
  color:#eef6ff !important;
  font-weight:700 !important;
}
</style>
"""


def inject_enterprise_theme():
    st.markdown(ENTERPRISE_CSS, unsafe_allow_html=True)


def render_sidebar_branding(title="TN Election 2026", subtitle="Modeled election dashboard · Simulation, forecast, and pre-election intelligence"):
    st.markdown(
        f"""
        <div class="tn-sidecard">
          <div class="tn-sidebrand">
            <div class="tn-brand-mark"></div>
            <div>
              <p class="tn-side-title">{title}</p>
              <div class="tn-side-meta">{subtitle}</div>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_masthead(title, subtitle, chips=None, status_label="Platform", status_value="Modeled Election Dashboard", status_meta="Simulation, forecast, and pre-election intelligence model."):
    return None


def render_demo_banner():
    return None
