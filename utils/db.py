"""
db.py – DuckDB connection manager and analytics engine
"""
import json
import os
from pathlib import Path

import duckdb
import pandas as pd

PARQUET_DIR = Path(__file__).parent.parent / "data" / "parquet"
SOURCE_METADATA_PATH = PARQUET_DIR / "source_metadata.json"
REQUIRED_TABLES = [
    "dim_constituency",
    "dim_constituency_scd",
    "dim_party",
    "dim_candidate",
    "fact_results",
    "fact_constituency_contest",
    "fact_nominations_2026",
    "fact_voter_demographics",
    "dim_celebrity_constituency",
    "fact_2026_swing",
]
SYNTHETIC_FINGERPRINTS = [
    "Anchetty",
    "Ganesh Nagar",
    "Harbour South",
    "Kolathur North",
    "Nagercoil North",
]

def get_connection():
    con = duckdb.connect(":memory:")
    _register_views(con)
    return con

def _register_views(con):
    for t in REQUIRED_TABLES:
        p = PARQUET_DIR / f"{t}.parquet"
        if p.exists():
            con.execute(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_parquet('{p}')")

def query(con, sql):
    return con.execute(sql).df()


def _read_source_metadata():
    if not SOURCE_METADATA_PATH.exists():
        return None

    with SOURCE_METADATA_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def get_data_status():
    if os.getenv("TN_ELECTIONS_ALLOW_DEMO_DATA") == "1":
        return {
            "ready": True,
            "mode": "demo",
            "summary": "Demo dataset enabled.",
            "details": (
                "The app is running with demo/synthetic data because "
                "TN_ELECTIONS_ALLOW_DEMO_DATA=1 is set."
            ),
        }

    missing_tables = [
        table for table in REQUIRED_TABLES if not (PARQUET_DIR / f"{table}.parquet").exists()
    ]
    metadata = _read_source_metadata()

    if missing_tables:
        return {
            "ready": False,
            "mode": "missing",
            "summary": "Election data is not loaded.",
            "details": (
                "The app refuses to render incomplete datasets. "
                f"Missing Parquet files: {', '.join(missing_tables)}."
            ),
        }

    if metadata and metadata.get("mode") == "official":
        return {
            "ready": True,
            "mode": "official",
            "summary": "Official dataset is loaded.",
            "details": metadata.get("description", ""),
        }

    if metadata and metadata.get("mode") == "modeled":
        return {
            "ready": True,
            "mode": "modeled",
            "summary": "Modeled dataset is loaded.",
            "details": metadata.get("description", ""),
        }

    if metadata and metadata.get("mode") == "wikipedia":
        return {
            "ready": True,
            "mode": "wikipedia",
            "summary": "Wikipedia-derived dataset is loaded.",
            "details": metadata.get("description", ""),
        }

    synthetic_hits = []
    constituency_path = PARQUET_DIR / "dim_constituency.parquet"
    if constituency_path.exists():
        con = duckdb.connect(":memory:")
        try:
            names = {
                row[0]
                for row in con.execute(
                    f"SELECT constituency_name FROM read_parquet('{constituency_path}')"
                ).fetchall()
            }
            synthetic_hits = sorted(set(SYNTHETIC_FINGERPRINTS).intersection(names))
        finally:
            con.close()

    if synthetic_hits:
        details = (
            "Synthetic/demo constituency records were detected: "
            f"{', '.join(synthetic_hits)}. "
            "Rebuild the Parquet dataset with `python generate_data.py` or "
            "`python import_wikipedia_data.py`."
        )
    else:
        details = (
            "No official source manifest was found for the current Parquet files. "
            "Rebuild the Parquet dataset with `python import_wikipedia_data.py` "
            "or commit a ready `data/parquet/` folder to the repository."
        )

    return {
        "ready": False,
        "mode": metadata.get("mode", "unknown") if metadata else "unknown",
        "summary": "The current dataset is not verified as official.",
        "details": details,
    }

# ─── OVERVIEW ──────────────────────────────────────────────────────────────
def get_overview_stats(con):
    r = lambda sql: con.execute(sql).fetchone()[0]
    return {
        "total_seats": 234,
        "total_parties": r("SELECT COUNT(DISTINCT party_id) FROM dim_party WHERE party_id<=17"),
        "total_2026_candidates": r("SELECT COUNT(*) FROM fact_nominations_2026 WHERE final_contesting_flag=true"),
        "celebrity_candidates": r("SELECT COUNT(*) FROM dim_candidate WHERE celebrity_flag=true"),
        "avg_turnout_2021": round(r("SELECT AVG(turnout_pct) FROM fact_voter_demographics WHERE election_year=2021 AND turnout_pct IS NOT NULL"),1),
        "swing_seats_2026": r("SELECT COUNT(*) FROM fact_2026_swing WHERE swing_seat_flag=true"),
        "elections_covered": 5,
    }

def get_all_constituencies(con):
    return query(con,"SELECT constituency_id,constituency_name,district,type,region FROM dim_constituency ORDER BY constituency_name")

# ─── CONSTITUENCY DETAIL ─────────────────────────────────────────────────
def get_constituency_results(con, constituency_id, year):
    return query(con, f"""
        SELECT dc.candidate_id, dc.candidate_name, dc.celebrity_flag, dc.photo_url,
               dp.party_abbr, dp.party_name, fcc.bloc_name AS alliance_name,
               fcc.votes, fcc.vote_share, fcc.rank, fcc.winner_flag,
               fcc.margin_votes, fcc.turnout_pct
        FROM fact_constituency_contest fcc
        JOIN dim_candidate dc ON fcc.candidate_id=dc.candidate_id
        JOIN dim_party dp ON fcc.party_id=dp.party_id
        WHERE fcc.constituency_id={constituency_id}
          AND fcc.election_year={year}
          AND fcc.record_type='historical_result'
        ORDER BY fcc.rank
    """)

def get_constituency_nominations_2026(con, constituency_id):
    return query(con, f"""
        SELECT dc.candidate_id, dc.candidate_name, dc.gender, dc.celebrity_flag, dc.celebrity_type, dc.photo_url,
               dp.party_abbr, dp.party_name, fcc.bloc_name AS alliance_name,
               fn.nomination_status, fn.scrutiny_status, fn.withdrawal_status,
               fn.final_contesting_flag, fn.affidavit_url, fn.source_date,
               fcc.forecast_win_prob, fcc.forecast_band
        FROM fact_nominations_2026 fn
        JOIN dim_candidate dc ON fn.candidate_id=dc.candidate_id
        JOIN dim_party dp ON fn.party_id=dp.party_id
        LEFT JOIN fact_constituency_contest fcc
            ON fn.constituency_id=fcc.constituency_id
           AND fn.candidate_id=fcc.candidate_id
           AND fcc.election_year=2026
           AND fcc.record_type='forecast_2026'
        WHERE fn.constituency_id={constituency_id}
        ORDER BY fn.final_contesting_flag DESC, fcc.forecast_win_prob DESC NULLS LAST, dc.celebrity_flag DESC
    """)

def get_constituency_swing(con, constituency_id):
    return query(con, f"SELECT * FROM fact_2026_swing WHERE constituency_id={constituency_id}")

# ─── MULTI-YEAR TREND ────────────────────────────────────────────────────
def get_constituency_trend(con, constituency_id):
    """Winner party trend across all 5 elections for a constituency."""
    return query(con, f"""
        SELECT fcc.election_year, dc.candidate_name, dp.party_abbr, fcc.bloc_name AS alliance_name,
               fcc.votes, fcc.vote_share, fcc.margin_votes, fcc.turnout_pct
        FROM fact_constituency_contest fcc
        JOIN dim_candidate dc ON fcc.candidate_id=dc.candidate_id
        JOIN dim_party dp ON fcc.party_id=dp.party_id
        WHERE fcc.constituency_id={constituency_id}
          AND fcc.winner_flag=true
          AND fcc.record_type='historical_result'
        ORDER BY fcc.election_year
    """)

def get_constituency_forecast(con, constituency_id):
    return query(con, f"""
        SELECT fcc.election_year, fcc.constituency_id, dc.candidate_name, dp.party_abbr,
               dp.party_name, fcc.bloc_name, fcc.forecast_win_prob, fcc.forecast_band,
               dc.celebrity_flag, dc.celebrity_type, dc.photo_url
        FROM fact_constituency_contest fcc
        JOIN dim_candidate dc ON fcc.candidate_id=dc.candidate_id
        JOIN dim_party dp ON fcc.party_id=dp.party_id
        WHERE fcc.constituency_id={constituency_id}
          AND fcc.election_year=2026
          AND fcc.record_type='forecast_2026'
        ORDER BY fcc.forecast_win_prob DESC, dc.celebrity_flag DESC
    """)

def get_forecast_2026(con):
    return query(con, """
        SELECT fcc.constituency_id, dcst.constituency_name, dcst.district, dcst.region, dcst.type,
               cand.candidate_name, cand.celebrity_flag, cand.photo_url,
               dp.party_abbr, dp.party_name, fcc.bloc_name, fcc.forecast_win_prob, fcc.forecast_band
        FROM fact_constituency_contest fcc
        JOIN dim_constituency dcst ON fcc.constituency_id=dcst.constituency_id
        JOIN dim_candidate cand ON fcc.candidate_id=cand.candidate_id
        JOIN dim_party dp ON fcc.party_id=dp.party_id
        WHERE fcc.election_year=2026
          AND fcc.record_type='forecast_2026'
        ORDER BY dcst.constituency_name, fcc.forecast_win_prob DESC
    """)

# ─── PARTY / ALLIANCE ────────────────────────────────────────────────────
def get_party_seat_share(con, year):
    return query(con, f"""
        SELECT dp.party_abbr, dp.party_name, dp.alliance_name, COUNT(*) AS seats_won
        FROM fact_results fr JOIN dim_party dp ON fr.party_id=dp.party_id
        WHERE fr.winner_flag=true AND fr.election_year={year}
        GROUP BY dp.party_abbr,dp.party_name,dp.alliance_name
        ORDER BY seats_won DESC
    """)

def get_alliance_trend(con):
    """Seats won by alliance across all 5 elections."""
    return query(con, """
        SELECT fr.election_year,
               CASE WHEN dp.alliance_name='INDIA Alliance' THEN 'DMK Alliance'
                    WHEN dp.alliance_name='NDA' THEN 'NDA / AIADMK Allied'
                    WHEN dp.party_abbr IN ('AIADMK','AMMK','AIADMK(P)') THEN 'AIADMK'
                    ELSE 'Others' END AS bloc,
               COUNT(*) AS seats
        FROM fact_results fr JOIN dim_party dp ON fr.party_id=dp.party_id
        WHERE fr.winner_flag=true
        GROUP BY fr.election_year, bloc
        ORDER BY fr.election_year, seats DESC
    """)

def get_party_vote_share_trend(con):
    return query(con, """
        SELECT fr.election_year, dp.party_abbr, dp.alliance_name,
               ROUND(AVG(fr.vote_share),2) AS avg_vote_share,
               SUM(fr.votes) AS total_votes
        FROM fact_results fr JOIN dim_party dp ON fr.party_id=dp.party_id
        WHERE dp.party_id IN (1,2,3,4,10,11)
        GROUP BY fr.election_year,dp.party_abbr,dp.alliance_name
        ORDER BY fr.election_year, total_votes DESC
    """)

# ─── ANALYTICS ───────────────────────────────────────────────────────────
def get_youth_pct_per_constituency(con, year=2021):
    return query(con, f"""
        SELECT dc.constituency_name, dc.district, dc.region, fv.total_electors,
               fv.age_18_19+fv.age_20_29 AS youth_electors,
               ROUND(100.0*(fv.age_18_19+fv.age_20_29)/NULLIF(fv.total_electors,0),2) AS youth_pct,
               fv.turnout_pct
        FROM fact_voter_demographics fv
        JOIN dim_constituency dc ON fv.constituency_id=dc.constituency_id
        WHERE fv.election_year={year} ORDER BY youth_pct DESC
    """)

def get_youth_turnout_scatter(con, year=2021):
    return query(con, f"""
        SELECT dc.constituency_name, dc.district, dc.region,
               ROUND(100.0*(fv.age_18_19+fv.age_20_29)/NULLIF(fv.total_electors,0),2) AS youth_pct,
               fv.turnout_pct,
               ROUND((100.0*(fv.age_18_19+fv.age_20_29)/NULLIF(fv.total_electors,0))*fv.turnout_pct/100,2) AS youth_influence_score
        FROM fact_voter_demographics fv
        JOIN dim_constituency dc ON fv.constituency_id=dc.constituency_id
        WHERE fv.election_year={year} AND fv.turnout_pct IS NOT NULL
        ORDER BY youth_influence_score DESC
    """)

def get_gender_ratio_by_region(con):
    return query(con, """
        SELECT dc.region, SUM(fv.male_electors) AS total_male,
               SUM(fv.female_electors) AS total_female,
               SUM(fv.third_gender_electors) AS total_third,
               ROUND(100.0*SUM(fv.female_electors)/NULLIF(SUM(fv.total_electors),0),2) AS female_pct
        FROM fact_voter_demographics fv
        JOIN dim_constituency dc ON fv.constituency_id=dc.constituency_id
        WHERE fv.election_year=2021
        GROUP BY dc.region ORDER BY female_pct DESC
    """)

def get_top_winning_margins(con, year, n=20):
    return query(con, f"""
        SELECT dc2.constituency_name, dc2.district, dcand.candidate_name,
               dp.party_abbr, fr.votes, fr.margin_votes, fr.turnout_pct
        FROM fact_results fr
        JOIN dim_constituency dc2 ON fr.constituency_id=dc2.constituency_id
        JOIN dim_candidate dcand ON fr.candidate_id=dcand.candidate_id
        JOIN dim_party dp ON fr.party_id=dp.party_id
        WHERE fr.winner_flag=true AND fr.election_year={year} AND fr.margin_votes IS NOT NULL
        ORDER BY fr.margin_votes DESC LIMIT {n}
    """)

def get_closest_contests(con, year, n=20):
    return query(con, f"""
        SELECT dc2.constituency_name, dc2.district, dcand.candidate_name,
               dp.party_abbr, fr.votes, fr.margin_votes, fr.turnout_pct
        FROM fact_results fr
        JOIN dim_constituency dc2 ON fr.constituency_id=dc2.constituency_id
        JOIN dim_candidate dcand ON fr.candidate_id=dcand.candidate_id
        JOIN dim_party dp ON fr.party_id=dp.party_id
        WHERE fr.winner_flag=true AND fr.election_year={year} AND fr.margin_votes IS NOT NULL
        ORDER BY fr.margin_votes ASC LIMIT {n}
    """)

def get_celebrity_constituencies(con):
    return query(con, """
        SELECT dcc.election_year, dc.constituency_id, dc.constituency_name, dc.district, dc.region,
               dcc.celebrity_candidate_name, dcc.celebrity_type, dcc.celebrity_reason
        FROM dim_celebrity_constituency dcc
        JOIN dim_constituency dc ON dcc.constituency_id=dc.constituency_id
        WHERE dcc.celebrity_flag=true ORDER BY dcc.election_year DESC, dc.constituency_name
    """)

def get_candidate_search(con, name_q="", party="", constituency_id=None):
    filters=["1=1"]
    if name_q: filters.append(f"LOWER(dc.candidate_name) LIKE '%{name_q.lower()}%'")
    if party and party!="All": filters.append(f"dp.party_abbr='{party}'")
    if constituency_id: filters.append(f"fn.constituency_id={constituency_id}")
    return query(con, f"""
        SELECT dc.candidate_name, dc.gender, dc.celebrity_flag, dc.celebrity_type,
               dc.profile_notes, dc.photo_url,
               dp.party_abbr,
               CASE
                   WHEN fn.party_id IN (1,3,5,6,7,8,9,13) THEN 'DMK Alliance'
                   WHEN fn.party_id IN (2,4,10,14,16) THEN 'AIADMK Alliance'
                   WHEN fn.party_id = 11 THEN 'NTK'
                   WHEN fn.party_id = 17 THEN 'TVK'
                   ELSE 'Others'
               END AS alliance_name,
               const.constituency_name, const.district,
               fn.final_contesting_flag, fn.affidavit_url
        FROM fact_nominations_2026 fn
        JOIN dim_candidate dc ON fn.candidate_id=dc.candidate_id
        JOIN dim_party dp ON fn.party_id=dp.party_id
        JOIN dim_constituency const ON fn.constituency_id=const.constituency_id
        WHERE {' AND '.join(filters)}
        ORDER BY dc.celebrity_flag DESC, dc.candidate_name
        LIMIT 200
    """)

def get_swing_analysis(con):
    return query(con, """
        SELECT sw.*, dc.constituency_name, dc.district, dc.region, dc.type
        FROM fact_2026_swing sw
        JOIN dim_constituency dc ON sw.constituency_id=dc.constituency_id
        ORDER BY sw.confidence_score ASC
    """)

def get_district_map_data(con, year=2021):
    """Aggregated district stats for choropleth map."""
    return query(con, f"""
        SELECT dc.district,
               COUNT(DISTINCT dc.constituency_id) AS total_seats,
               SUM(CASE WHEN fr.winner_flag AND dp.alliance_name='INDIA Alliance' THEN 1 ELSE 0 END) AS dmk_seats,
               SUM(CASE WHEN fr.winner_flag AND dp.party_abbr='AIADMK' THEN 1 ELSE 0 END) AS aiadmk_seats,
               ROUND(AVG(CASE WHEN fv.election_year={year} THEN fv.turnout_pct END),1) AS avg_turnout,
               ROUND(AVG(CASE WHEN fv.election_year={year} THEN 100.0*(fv.age_18_19+fv.age_20_29)/NULLIF(fv.total_electors,0) END),1) AS avg_youth_pct
        FROM dim_constituency dc
        LEFT JOIN fact_results fr ON dc.constituency_id=fr.constituency_id AND fr.election_year={year} AND fr.winner_flag=true
        LEFT JOIN dim_party dp ON fr.party_id=dp.party_id
        LEFT JOIN fact_voter_demographics fv ON dc.constituency_id=fv.constituency_id
        GROUP BY dc.district ORDER BY dc.district
    """)

def get_turnout_trend(con):
    return query(con, """
        SELECT fv.election_year, dc.region,
               ROUND(AVG(fv.turnout_pct),2) AS avg_turnout
        FROM fact_voter_demographics fv
        JOIN dim_constituency dc ON fv.constituency_id=dc.constituency_id
        WHERE fv.turnout_pct IS NOT NULL
        GROUP BY fv.election_year,dc.region ORDER BY fv.election_year,dc.region
    """)
