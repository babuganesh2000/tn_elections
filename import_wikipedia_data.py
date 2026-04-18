"""
Import Tamil Nadu constituency data from Wikipedia into the app Parquet schema.

This importer is constituency-centric:
- pulls the current 234-seat list from Wikipedia
- fetches each constituency page
- extracts historical result rows for 2001/2006/2011/2016/2021 where available
- extracts 2026 candidate lists where available

The remaining supporting tables that are not available from Wikipedia
(demographics, swing model, celebrity tags) are generated from local helper logic.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

from generate_data import (
    CELEB_SEATS,
    CONSTITUENCIES,
    PARTIES,
    build_dim_constituency_scd,
    build_fact_2026_swing,
    build_fact_constituency_contest,
    build_fact_voter_demographics,
)

LIST_URL = "https://en.wikipedia.org/wiki/List_of_constituencies_of_the_Tamil_Nadu_Legislative_Assembly"
BASE_URL = "https://en.wikipedia.org"
PARQUET_DIR = Path("data/parquet")
METADATA_PATH = PARQUET_DIR / "source_metadata.json"
TIMEOUT = 30
TARGET_YEARS = [2001, 2006, 2011, 2016, 2021]

DISTRICT_TO_REGION = {
    "Chennai": "Chennai",
    "Chengalpattu": "South",
    "Kancheepuram": "North",
    "Tiruvallur": "North",
    "Thiruvallur": "North",
    "Ranipet": "North",
    "Vellore": "North",
    "Tirupattur": "North",
    "Tirupathur": "North",
    "Tiruvannamalai": "North",
    "Viluppuram": "Central",
    "Villupuram": "Central",
    "Kallakurichi": "Central",
    "Salem": "West",
    "Namakkal": "West",
    "Erode": "West",
    "The Nilgiris": "West",
    "Nilgiris": "West",
    "Coimbatore": "West",
    "Tiruppur": "West",
    "Krishnagiri": "West",
    "Dharmapuri": "West",
    "Madurai": "South",
    "Theni": "South",
    "Virudhunagar": "South",
    "Sivaganga": "South",
    "Dindigul": "South",
    "Tenkasi": "South",
    "Tirunelveli": "South",
    "Thoothukudi": "South",
    "Kanniyakumari": "South",
    "Kanyakumari": "South",
    "Karur": "Central",
    "Tiruchirappalli": "Central",
    "Perambalur": "Central",
    "Ariyalur": "Central",
    "Pudukkottai": "Central",
    "Cuddalore": "Delta",
    "Nagapattinam": "Delta",
    "Mayiladuthurai": "Delta",
    "Thanjavur": "Delta",
    "Tiruvarur": "Delta",
    "Thiruvarur": "Delta",
    "Puducherry": "Delta",
}

STATIC_PARTIES = {
    1: ("Dravida Munnetra Kazhagam", "DMK", "INDIA Alliance", 2021),
    2: ("All India Anna Dravida Munnetra Kazhagam", "AIADMK", "Independent", 2021),
    3: ("Indian National Congress", "INC", "INDIA Alliance", 2021),
    4: ("Bharatiya Janata Party", "BJP", "NDA", 2021),
    5: ("Viduthalai Chiruthaigal Katchi", "VCK", "INDIA Alliance", 2021),
    6: ("Communist Party of India (Marxist)", "CPM", "INDIA Alliance", 2021),
    7: ("Communist Party of India", "CPI", "INDIA Alliance", 2021),
    8: ("Marumalarchi Dravida Munnetra Kazhagam", "MDMK", "INDIA Alliance", 2021),
    9: ("Indian Union Muslim League", "IUML", "INDIA Alliance", 2021),
    10: ("Pattali Makkal Katchi", "PMK", "NDA", 2021),
    11: ("Naam Tamilar Katchi", "NTK", "Independent", 2021),
    12: ("All India Majlis-e-Ittehadul Muslimeen", "AIMIM", "Independent", 2021),
    13: ("Tamil Maanila Congress", "TMC(M)", "INDIA Alliance", 2021),
    14: ("Desiya Murpokku Dravida Kazhagam", "DMDK", "Independent", 2016),
    15: ("Independent", "IND", "Independent", 2001),
    16: ("Amma Makkal Munnetra Kazhagam", "AMMK", "Independent", 2021),
    17: ("Tamilaga Vettri Kazhagam", "TVK", "Independent", 2026),
    18: ("Makkal Needhi Maiam", "MNM", "Independent", 2019),
    19: ("MGR Anna Dravida Munnetra Kazhagam", "MGRK", "Independent", 2001),
}

PARTY_ALIASES = {
    "DMK": 1,
    "Dravida Munnetra Kazhagam": 1,
    "AIADMK": 2,
    "AIADMK+": 2,
    "All India Anna Dravida Munnetra Kazhagam": 2,
    "INC": 3,
    "Indian National Congress": 3,
    "Congress": 3,
    "BJP": 4,
    "Bharatiya Janata Party": 4,
    "VCK": 5,
    "CPM": 6,
    "CPI": 7,
    "MDMK": 8,
    "IUML": 9,
    "PMK": 10,
    "NTK": 11,
    "Naam Tamilar Katchi": 11,
    "AIMIM": 12,
    "TMC(M)": 13,
    "DMDK": 14,
    "Independent": 15,
    "IND": 15,
    "AMMK": 16,
    "TVK": 17,
    "Tamilaga Vettri Kazhagam": 17,
    "MNM": 18,
    "MGRK": 19,
}


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "tn-elections-importer/1.0 (research use)",
        }
    )
    return session


def _clean_text(value: str) -> str:
    text = re.sub(r"\[[^\]]+\]", "", str(value or ""))
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", _clean_text(text).lower())


def _parse_number(value):
    text = _clean_text(value).replace(",", "")
    text = text.replace("%", "")
    if text in {"", "New", "-", "—"}:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    num = float(match.group(0))
    return int(num) if num.is_integer() else num


def _table_matrix(table: Tag):
    matrix = []
    pending = {}
    for row_idx, tr in enumerate(table.find_all("tr")):
        row = []
        col_idx = 0

        while (row_idx, col_idx) in pending:
            value, remaining = pending.pop((row_idx, col_idx))
            row.append(value)
            if remaining > 1:
                pending[(row_idx + 1, col_idx)] = (value, remaining - 1)
            col_idx += 1

        for cell in tr.find_all(["th", "td"]):
            while (row_idx, col_idx) in pending:
                value, remaining = pending.pop((row_idx, col_idx))
                row.append(value)
                if remaining > 1:
                    pending[(row_idx + 1, col_idx)] = (value, remaining - 1)
                col_idx += 1

            text = _clean_text(cell.get_text(" ", strip=True))
            rowspan = int(cell.get("rowspan", 1))
            colspan = int(cell.get("colspan", 1))
            for offset in range(colspan):
                row.append(text)
                if rowspan > 1:
                    pending[(row_idx + 1, col_idx + offset)] = (text, rowspan - 1)
            col_idx += colspan

        while (row_idx, col_idx) in pending:
            value, remaining = pending.pop((row_idx, col_idx))
            row.append(value)
            if remaining > 1:
                pending[(row_idx + 1, col_idx)] = (value, remaining - 1)
            col_idx += 1

        if any(cell for cell in row):
            matrix.append(row)

    width = max((len(row) for row in matrix), default=0)
    return [row + [""] * (width - len(row)) for row in matrix]


def _normalize_party_name(raw_party: str) -> str:
    text = _clean_text(raw_party)
    text = text.replace("AIADMK+", "AIADMK+")
    return text


def _derive_abbr(party_name: str) -> str:
    letters = re.findall(r"[A-Z]", party_name)
    if letters:
        return "".join(letters)[:10]
    words = [w for w in re.split(r"[\s()/.-]+", party_name) if w]
    return "".join(word[:1].upper() for word in words)[:10] or "OTH"


def _alliance_for_party(party_name: str) -> str:
    pid = PARTY_ALIASES.get(party_name)
    if pid and pid in STATIC_PARTIES:
        return STATIC_PARTIES[pid][2]
    if party_name in {"DMK", "INC", "VCK", "CPM", "CPI", "MDMK", "IUML", "TMC(M)"}:
        return "INDIA Alliance"
    if party_name in {"AIADMK", "BJP", "PMK"}:
        return "NDA"
    return "Independent"


def _party_registry():
    registry = {
        party_id: {
            "party_id": party_id,
            "party_name": vals[0],
            "party_abbr": vals[1],
            "alliance_name": vals[2],
            "alliance_year": vals[3],
        }
        for party_id, vals in STATIC_PARTIES.items()
    }
    next_id = max(registry) + 1
    return registry, next_id


def _candidate_registry_from_celebrities():
    from generate_data import CELEBRITIES

    records = []
    next_id = 1
    for nm, gn, cf, ct, notes, photo in CELEBRITIES:
        records.append(
            {
                "candidate_id": next_id,
                "candidate_name": nm,
                "gender": gn,
                "celebrity_flag": cf,
                "celebrity_type": ct,
                "profile_notes": notes,
                "photo_url": photo,
            }
        )
        next_id += 1
    return records, next_id


def _load_master_constituencies(session: requests.Session):
    response = session.get(LIST_URL, timeout=TIMEOUT)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    current_header = soup.find(id="Current_list_of_constituencies")
    if not current_header:
        raise RuntimeError("Could not locate current constituency list on Wikipedia.")

    if current_header.name in {"h2", "h3", "h4"}:
        heading = current_header
    else:
        heading = current_header.find_parent(["h2", "h3", "h4"])
    if not heading:
        raise RuntimeError("Could not locate the heading wrapper for the constituency list.")
    table = heading.find_next("table")
    if not table:
        raise RuntimeError("Could not locate the constituency table on Wikipedia.")
    rows = []
    link_map = {}
    table_rows = _table_matrix(table)
    id_lookup = {row[0]: row for row in CONSTITUENCIES}

    for row_idx, row in enumerate(table_rows[1:], start=1):
        if len(row) < 8:
            continue
        number = _parse_number(row[0])
        if not isinstance(number, int):
            continue
        tr = table.find_all("tr")[row_idx]
        raw_cells = tr.find_all(["td", "th"])
        constituency_link = raw_cells[1].find("a") if len(raw_cells) > 1 else None
        constituency_name = _clean_text(row[1])
        reserved = _clean_text(row[2])
        district = _clean_text(row[5])
        ctype = "ST" if reserved == "ST" else "SC" if reserved == "SC" else "GEN"
        region = DISTRICT_TO_REGION.get(district, id_lookup.get(number, [None, None, None, None, "Central"])[4])

        rows.append(
            {
                "constituency_id": number,
                "constituency_name": constituency_name,
                "district": district,
                "type": ctype,
                "region": region,
            }
        )
        if constituency_link and constituency_link.get("href"):
            link_map[number] = urljoin(BASE_URL, constituency_link["href"])

    df = pd.DataFrame(rows).sort_values("constituency_id").reset_index(drop=True)
    return df, link_map


def _extract_section_table(soup: BeautifulSoup, section_id: str) -> Tag | None:
    marker = soup.find(id=section_id)
    if not marker:
        return None
    container = marker.parent if marker.parent and marker.parent.name == "div" else marker
    node = container.find_next_sibling()
    while node:
        if isinstance(node, Tag) and node.name in {"h2", "h3", "h4"}:
            break
        if isinstance(node, Tag) and node.name == "table":
            return node
        node = node.find_next_sibling()
    return None


def _parse_result_rows(table: Tag, year: int):
    rows = []
    turnout_pct = None
    for tr in table.select("tr"):
        cells = [_clean_text(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
        if len(cells) < 2:
            continue
        label = cells[0]
        if label == "Party":
            continue
        if label.startswith(str(year)) or label.startswith("Bye-election"):
            continue
        if label == "Turnout":
            turnout_pct = _parse_number(cells[2] if len(cells) > 2 else cells[1])
            continue
        if label in {"Margin of victory", "Rejected ballots", "Registered electors"}:
            continue
        if label == "":
            if len(cells) < 5:
                continue
            party = _normalize_party_name(cells[1])
            candidate = cells[2]
            votes = _parse_number(cells[3])
            vote_share = _parse_number(cells[4])
        else:
            if len(cells) < 4:
                continue
            party = _normalize_party_name(cells[0])
            candidate = cells[1]
            votes = _parse_number(cells[2])
            vote_share = _parse_number(cells[3])
        if party == "NOTA" or candidate == "NOTA":
            continue
        if not party or not candidate or votes is None or vote_share is None:
            continue
        if isinstance(votes, (int, float)) and votes < 100:
            continue
        rows.append(
            {
                "party_name": party,
                "candidate_name": candidate,
                "votes": votes,
                "vote_share": vote_share,
            }
        )
    rows = [row for row in rows if row["candidate_name"] != "NOTA"]
    rows.sort(key=lambda row: (row["votes"] or 0), reverse=True)
    if rows and len(rows) > 1 and rows[0]["votes"] and rows[1]["votes"]:
        margin = int(rows[0]["votes"] - rows[1]["votes"])
    else:
        margin = None
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
        row["winner_flag"] = index == 1
        row["margin_votes"] = margin if index == 1 else None
        row["turnout_pct"] = turnout_pct
    return rows


def _parse_2026_rows(table: Tag):
    rows = []
    for tr in table.select("tr"):
        cells = [_clean_text(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
        if len(cells) < 2:
            continue
        if cells[0] in {"Party", "Margin of victory", "Turnout", "Rejected ballots", "Registered electors", "NOTA"}:
            continue
        if cells[0].startswith("2026 "):
            continue
        if cells[0] == "":
            if len(cells) < 3:
                continue
            party = _normalize_party_name(cells[1])
            candidate = cells[2]
        else:
            party = _normalize_party_name(cells[0])
            candidate = cells[1]
        if (
            not party
            or not candidate
            or candidate == "NOTA"
            or candidate == "Swing"
            or party.startswith("gain from")
        ):
            continue
        rows.append({"party_name": party, "candidate_name": candidate})
    return rows


def _register_party(party_registry, next_party_id, party_name):
    normalized = _normalize_party_name(party_name)
    existing = PARTY_ALIASES.get(normalized)
    if existing:
        return existing, next_party_id

    for pid, row in party_registry.items():
        if _slug(row["party_name"]) == _slug(normalized) or row["party_abbr"] == normalized:
            PARTY_ALIASES[normalized] = pid
            return pid, next_party_id

    pid = next_party_id
    next_party_id += 1
    party_registry[pid] = {
        "party_id": pid,
        "party_name": normalized,
        "party_abbr": _derive_abbr(normalized),
        "alliance_name": _alliance_for_party(normalized),
        "alliance_year": 2026,
    }
    PARTY_ALIASES[normalized] = pid
    return pid, next_party_id


def _register_candidate(candidate_records, candidate_lookup, next_candidate_id, candidate_name):
    normalized = _clean_text(candidate_name)
    existing = candidate_lookup.get(_slug(normalized))
    if existing:
        return existing, next_candidate_id

    candidate_records.append(
        {
            "candidate_id": next_candidate_id,
            "candidate_name": normalized,
            "gender": None,
            "celebrity_flag": False,
            "celebrity_type": None,
            "profile_notes": "Imported from Wikipedia constituency page.",
            "photo_url": None,
        }
    )
    candidate_lookup[_slug(normalized)] = next_candidate_id
    return next_candidate_id, next_candidate_id + 1


def _build_celebrity_table():
    return pd.DataFrame(
        [
            {
                "constituency_id": row[0],
                "election_year": row[1],
                "celebrity_flag": row[2],
                "celebrity_reason": row[3],
                "celebrity_candidate_name": row[4],
                "celebrity_type": row[5],
            }
            for row in CELEB_SEATS
        ]
    )


def _write_metadata():
    metadata = {
        "mode": "wikipedia",
        "description": (
            "Wikipedia-derived constituency warehouse rebuilt from the Tamil Nadu Legislative Assembly list and constituency pages. "
            "Historical seat results and 2026 candidate lists come from Wikipedia pages; supporting demographics and swing tables remain locally modeled."
        ),
        "source": LIST_URL,
    }
    METADATA_PATH.write_text(json.dumps(metadata, indent=2), encoding="utf-8")


def main():
    session = _session()
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    dim_constituency, link_map = _load_master_constituencies(session)

    party_registry, next_party_id = _party_registry()
    candidate_records, next_candidate_id = _candidate_registry_from_celebrities()
    candidate_lookup = {_slug(row["candidate_name"]): row["candidate_id"] for row in candidate_records}
    fact_results_rows = []
    nominations_rows = []

    for _, constituency in dim_constituency.iterrows():
        cid = int(constituency["constituency_id"])
        page_url = link_map.get(cid)
        if not page_url:
            continue

        response = session.get(page_url, timeout=TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        for year in TARGET_YEARS:
            table = _extract_section_table(soup, str(year))
            if not table:
                continue
            parsed_rows = _parse_result_rows(table, year)
            for row in parsed_rows:
                party_id, next_party_id = _register_party(party_registry, next_party_id, row["party_name"])
                candidate_id, next_candidate_id = _register_candidate(
                    candidate_records, candidate_lookup, next_candidate_id, row["candidate_name"]
                )
                fact_results_rows.append(
                    {
                        "election_year": year,
                        "constituency_id": cid,
                        "candidate_id": candidate_id,
                        "party_id": party_id,
                        "votes": row["votes"],
                        "vote_share": row["vote_share"],
                        "rank": row["rank"],
                        "winner_flag": row["winner_flag"],
                        "margin_votes": row["margin_votes"],
                        "turnout_pct": row["turnout_pct"],
                    }
                )

        table_2026 = _extract_section_table(soup, "2026")
        if table_2026:
            for row in _parse_2026_rows(table_2026):
                party_id, next_party_id = _register_party(party_registry, next_party_id, row["party_name"])
                candidate_id, next_candidate_id = _register_candidate(
                    candidate_records, candidate_lookup, next_candidate_id, row["candidate_name"]
                )
                nominations_rows.append(
                    {
                        "constituency_id": cid,
                        "candidate_id": candidate_id,
                        "party_id": party_id,
                        "nomination_status": "Filed",
                        "scrutiny_status": "Accepted",
                        "withdrawal_status": "Not Withdrawn",
                        "final_contesting_flag": True,
                        "affidavit_url": None,
                        "source_date": "2026-04-17",
                    }
                )

    dim_party = pd.DataFrame(sorted(party_registry.values(), key=lambda row: row["party_id"]))
    dim_candidate = pd.DataFrame(candidate_records).drop_duplicates(subset=["candidate_name"], keep="first")
    fact_results = pd.DataFrame(fact_results_rows)
    fact_nominations_2026 = pd.DataFrame(nominations_rows)

    if fact_results.empty:
        raise RuntimeError("No historical result rows were parsed from Wikipedia.")

    dim_constituency.to_parquet(PARQUET_DIR / "dim_constituency.parquet", index=False)
    dim_party.to_parquet(PARQUET_DIR / "dim_party.parquet", index=False)
    dim_candidate.to_parquet(PARQUET_DIR / "dim_candidate.parquet", index=False)
    fact_results.to_parquet(PARQUET_DIR / "fact_results.parquet", index=False)
    fact_nominations_2026.to_parquet(PARQUET_DIR / "fact_nominations_2026.parquet", index=False)

    fact_voter_demographics = build_fact_voter_demographics(dim_constituency)
    dim_celebrity_constituency = _build_celebrity_table()
    dim_celebrity_constituency.to_parquet(PARQUET_DIR / "dim_celebrity_constituency.parquet", index=False)
    fact_2026_swing = build_fact_2026_swing(dim_constituency)
    dim_constituency_scd = build_dim_constituency_scd(dim_constituency)
    build_fact_constituency_contest(dim_constituency_scd, fact_results, fact_nominations_2026, fact_2026_swing)

    _write_metadata()
    print(f"Wrote Wikipedia-derived dataset to {PARQUET_DIR}")


if __name__ == "__main__":
    main()
