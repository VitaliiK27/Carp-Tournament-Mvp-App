from datetime import datetime

import streamlit as st

from db import FISH_TYPES, add_catch, format_db_datetime, get_period_count, get_period_number, get_teams_df
from ui_helpers import build_tournament_date_options


def render_add_fish_page(active_meta: dict, active_tournament_id: int):
    st.subheader(f"Додати зважування — {active_meta['name']}")
    teams_df = get_teams_df(active_tournament_id)

    if teams_df.empty:
        st.warning("Спочатку додай хоча б одну команду")
        return

    team_options = {row["name"]: int(row["id"]) for _, row in teams_df.iterrows()}
    period_count = get_period_count(active_meta["start_at"], active_meta["end_at"], int(active_meta["period_hours"]))
    date_options = build_tournament_date_options(active_meta["start_at"], active_meta["end_at"])
    st.markdown(
        f"""
        <div class="mobile-card">
            <div><b>Старт:</b> {active_meta['start_at']}</div>
            <div><b>Фініш:</b> {active_meta['end_at']}</div>
            <div><b>Періодів:</b> {period_count}</div>
            <div><b>Один період:</b> {active_meta['period_hours']} год</div>
            <div><b>Мін. вага:</b> {active_meta['min_weight']} кг</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.form("add_catch_form", clear_on_submit=True):
        team_name = st.selectbox("Команда", list(team_options.keys()))
        date_value = st.selectbox("Дата", date_options, format_func=lambda value: value.strftime("%d-%m-%Y"))
        time_value = st.time_input("Час")
        fish_type = st.selectbox("Вид", FISH_TYPES)
        weight = st.number_input("Вага (кг)", min_value=0.001, step=0.001, format="%.3f")
        submitted = st.form_submit_button("Зберегти зважування")

        if submitted:
            caught_at = format_db_datetime(datetime.combine(date_value, time_value))
            period = get_period_number(caught_at, active_meta["start_at"], active_meta["end_at"], int(active_meta["period_hours"]))
            if period is None:
                st.error("Час зважування має бути в межах турніру")
            elif float(weight) < float(active_meta["min_weight"]):
                st.error(f"Риба менша за мінімально допустиму вагу: {active_meta['min_weight']} кг")
            else:
                add_catch(active_tournament_id, team_options[team_name], caught_at, int(period), fish_type, float(weight))
                st.success(f"Зважування додано в період {period}")
                st.rerun()
