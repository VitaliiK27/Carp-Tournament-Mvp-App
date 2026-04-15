from datetime import datetime

import streamlit as st

from db import (
    FISH_TYPES,
    add_catch,
    delete_catch,
    format_db_datetime,
    get_catches_df,
    get_period_count,
    get_period_number,
    get_teams_df,
    parse_datetime,
    update_catch,
)
from ui_helpers import build_tournament_date_options

FLASH_MESSAGE_KEY = "quick_weighing_flash"


def _build_catch_label(row) -> str:
    return f"{row['caught_at']} | {row['fish_type']} | {float(row['weight']):.3f} кг | період {int(row['period'])}"


def _save_flash_message(message: str):
    st.session_state[FLASH_MESSAGE_KEY] = message


def _render_flash_message():
    message = st.session_state.pop(FLASH_MESSAGE_KEY, None)
    if message:
        st.success(message)


def render_quick_weighing_page(active_meta: dict, active_tournament_id: int):
    st.subheader(f"Зважування — {active_meta['name']}")
    _render_flash_message()

    teams_df = get_teams_df(active_tournament_id)
    if teams_df.empty:
        st.warning("Спочатку додай хоча б одну команду")
        return

    team_options = {row["name"]: int(row["id"]) for _, row in teams_df.iterrows()}
    team_names = list(team_options.keys())
    date_options = build_tournament_date_options(active_meta["start_at"], active_meta["end_at"])
    period_count = get_period_count(active_meta["start_at"], active_meta["end_at"], int(active_meta["period_hours"]))

    selected_team_name = st.selectbox("Команда", team_names, key="quick_weighing_team")
    selected_team_id = team_options[selected_team_name]

    st.markdown(
        f"""
        <div class="mobile-card">
            <div><b>Активний турнір:</b> {active_meta['name']}</div>
            <div><b>Команда:</b> {selected_team_name}</div>
            <div><b>Періодів:</b> {period_count}</div>
            <div><b>Мін. вага:</b> {active_meta['min_weight']} кг</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("### Додати зважування")
    fish_type = st.selectbox("Вид риби", FISH_TYPES, key="new_catch_fish_type")
    weight = st.number_input(
        "Вага (кг)",
        min_value=0.001,
        value=None,
        step=0.001,
        format="%.3f",
        placeholder="Напр. 3.500",
        key="new_catch_weight",
    )
    date_value = st.selectbox(
        "Дата зважування",
        date_options,
        format_func=lambda value: value.strftime("%d-%m-%Y"),
        key="new_catch_date",
    )
    time_value = st.time_input("Час зважування", step=1800, key="new_catch_time")

    if st.button("Зберегти зважування", key="save_new_catch", use_container_width=True):
        if weight is None:
            st.error("Вкажи вагу риби")
        else:
            caught_at = format_db_datetime(datetime.combine(date_value, time_value))
            period = get_period_number(
                caught_at,
                active_meta["start_at"],
                active_meta["end_at"],
                int(active_meta["period_hours"]),
            )
            if period is None:
                st.error("Час зважування має бути в межах турніру")
            elif float(weight) < float(active_meta["min_weight"]):
                st.error(f"Риба менша за мінімально допустиму вагу: {active_meta['min_weight']} кг")
            else:
                add_catch(active_tournament_id, selected_team_id, caught_at, int(period), fish_type, float(weight))
                _save_flash_message(
                    f"Зважування збережено: {selected_team_name}, {float(weight):.3f} кг, період {period}"
                )
                st.rerun()

    catches_df = get_catches_df(active_tournament_id)
    team_catches = catches_df[catches_df["team"] == selected_team_name].copy()
    total_team_weight = float(team_catches["weight"].astype(float).sum()) if not team_catches.empty else 0.0

    st.markdown(f"**Загальна вага команди:** {total_team_weight:.3f} кг")

    st.markdown(f"### Зважування по {selected_team_name}")
    if team_catches.empty:
        st.info("По цій команді ще немає зважувань")
        return

    display_df = team_catches[["caught_at", "fish_type", "weight", "period"]].rename(
        columns={
            "caught_at": "Час",
            "fish_type": "Вид риби",
            "weight": "Вага (кг)",
            "period": "Період",
        }
    )
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    catch_labels = {int(row["id"]): _build_catch_label(row) for _, row in team_catches.iterrows()}
    selected_catch_id = st.selectbox(
        "Зважування для редагування",
        list(catch_labels.keys()),
        format_func=lambda catch_id: catch_labels[catch_id],
        key=f"selected_catch_{selected_team_id}",
    )
    selected_catch = team_catches.loc[team_catches["id"] == selected_catch_id].iloc[0]
    selected_catch_dt = parse_datetime(selected_catch["caught_at"])

    try:
        edit_date_index = date_options.index(selected_catch_dt.date())
    except ValueError:
        edit_date_index = 0

    current_fish_type = selected_catch["fish_type"] if selected_catch["fish_type"] in FISH_TYPES else FISH_TYPES[0]
    edit_fish_type_index = FISH_TYPES.index(current_fish_type)

    st.markdown("#### Редагувати зважування")
    edit_date = st.selectbox(
        "Дата зважування",
        date_options,
        index=edit_date_index,
        format_func=lambda value: value.strftime("%d-%m-%Y"),
        key=f"edit_catch_date_{selected_catch_id}",
    )
    edit_time = st.time_input(
        "Час зважування",
        value=selected_catch_dt.time().replace(second=0, microsecond=0),
        step=1800,
        key=f"edit_catch_time_{selected_catch_id}",
    )
    edit_fish_type = st.selectbox(
        "Вид риби",
        FISH_TYPES,
        index=edit_fish_type_index,
        key=f"edit_catch_fish_type_{selected_catch_id}",
    )
    edit_weight = st.number_input(
        "Вага (кг)",
        min_value=0.001,
        step=0.001,
        format="%.3f",
        value=float(selected_catch["weight"]),
        key=f"edit_catch_weight_{selected_catch_id}",
    )
    save_col, delete_col = st.columns(2)
    edit_submitted = save_col.button("Зберегти зміни", key=f"save_catch_{selected_catch_id}", use_container_width=True)
    delete_submitted = delete_col.button(
        "Видалити зважування",
        key=f"delete_catch_{selected_catch_id}",
        use_container_width=True,
    )

    if edit_submitted:
        caught_at = format_db_datetime(datetime.combine(edit_date, edit_time))
        period = get_period_number(
            caught_at,
            active_meta["start_at"],
            active_meta["end_at"],
            int(active_meta["period_hours"]),
        )
        if period is None:
            st.error("Час зважування має бути в межах турніру")
        elif float(edit_weight) < float(active_meta["min_weight"]):
            st.error(f"Риба менша за мінімально допустиму вагу: {active_meta['min_weight']} кг")
        else:
            update_catch(int(selected_catch_id), caught_at, int(period), edit_fish_type, float(edit_weight))
            _save_flash_message(
                f"Зважування оновлено: {selected_team_name}, {float(edit_weight):.3f} кг, період {period}"
            )
            st.rerun()
    elif delete_submitted:
        delete_catch(int(selected_catch_id))
        _save_flash_message(f"Зважування команди {selected_team_name} видалено")
        st.rerun()
