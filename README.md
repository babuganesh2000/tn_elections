# TN Election 2026

Streamlit dashboard for Tamil Nadu assembly election history, constituency intelligence, and 2026 modeling.

The current repo is set up to deploy with committed Parquet data and optionally refresh that dataset from Wikipedia.

Supported dataset modes in the code:
- `wikipedia` via `import_wikipedia_data.py`
- `modeled` via `generate_data.py`

The app entrypoint is `Home.py`.

## Current Pages

- `Home.py`
- `pages/1_Constituency_Explorer.py`
- `pages/4_Voter_Insights.py`
- `pages/6_Election_2026.py`

## Repo Layout

```text
tn_elections/
├── Home.py
├── pages/
├── utils/
├── assets/
├── data/
│   └── parquet/
├── generate_data.py
├── import_wikipedia_data.py
├── requirements.txt
└── .streamlit/config.toml
```

## Local Run

```powershell
pip install -r requirements.txt
python import_wikipedia_data.py
streamlit run Home.py
```

If you prefer the fully modeled dataset instead of Wikipedia-derived data:

```powershell
python generate_data.py
streamlit run Home.py
```

## Streamlit Cloud

Use `Home.py` as the main file path.

Recommended deployment approach:

1. Commit `data/parquet/` to the repository so the app does not depend on runtime scraping or generation.
2. Commit `.streamlit/config.toml`.
3. Set the Streamlit Cloud main file path to `Home.py`.
4. Ensure `requirements.txt` is present at repo root.

If `data/parquet/` is not committed, Streamlit Cloud will start without a dataset unless you add an explicit startup step outside the app.

## Data Notes

- `import_wikipedia_data.py` rebuilds constituency history from Wikipedia constituency pages and the master assembly constituency list.
- `generate_data.py` produces a fully modeled dataset for local testing.

The app reads `data/parquet/source_metadata.json` to determine whether the dataset is `official`, `wikipedia`, or `modeled`.
