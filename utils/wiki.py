"""Helpers for Wikipedia profile links used in the UI."""

from urllib.parse import quote


WIKIPEDIA_PAGE_OVERRIDES = {
    "M.K. Stalin": "M._K._Stalin",
    "Udhayanidhi Stalin": "Udhayanidhi_Stalin",
    "Edappadi K. Palaniswami": "Edappadi_K._Palaniswami",
    "O. Panneerselvam": "O._Panneerselvam",
    "Seeman": "Seeman_(politician)",
    "Vijay": "Vijay_(actor)",
    "Durai Murugan": "Durai_Murugan",
    "Sarathkumar": "R._Sarathkumar",
    "Khushbu Sundar": "Khushbu",
    "Tamilisai Soundararajan": "Tamilisai_Soundararajan",
    "Anbumani Ramadoss": "Anbumani_Ramadoss",
    "Kamal Haasan": "Kamal_Haasan",
    "Jayalalithaa": "J._Jayalalithaa",
    "M. Karunanidhi": "M._Karunanidhi",
}


def get_wikipedia_url(candidate_name):
    if not candidate_name:
        return None

    page_name = WIKIPEDIA_PAGE_OVERRIDES.get(candidate_name, candidate_name.replace(" ", "_"))
    return f"https://en.wikipedia.org/wiki/{quote(page_name, safe='():,_._')}"
