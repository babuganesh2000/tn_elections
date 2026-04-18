"""Streamlit guardrails for verified election data."""

import streamlit as st

from utils.db import get_data_status


def stop_if_data_unverified():
    status = get_data_status()
    if status["ready"]:
        return status

    st.error(status["summary"])
    st.warning(status["details"])
    st.markdown(
        """
`Expected next step`

Either commit `data/parquet/` to the repository or rebuild it locally with
`python import_wikipedia_data.py`.
"""
    )
    st.stop()
