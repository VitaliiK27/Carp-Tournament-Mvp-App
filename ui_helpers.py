from datetime import timedelta

import pandas as pd
import streamlit as st

from db import parse_datetime


def render_mobile_cards(df: pd.DataFrame, title_col: str, fields: list[str], empty_message: str):
    if df.empty:
        st.info(empty_message)
        return

    for _, row in df.iterrows():
        body = "".join([f"<div><b>{field}:</b> {row[field]}</div>" for field in fields if field in df.columns])
        st.markdown(
            f"""
            <div class="mobile-card">
                <div style="font-size:1.05rem;font-weight:700;margin-bottom:0.35rem;">{row[title_col]}</div>
                {body}
            </div>
            """,
            unsafe_allow_html=True,
        )


def build_tournament_date_options(start_at: str, end_at: str):
    start_date = parse_datetime(start_at).date()
    end_date = parse_datetime(end_at).date()
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates
