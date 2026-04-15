from datetime import datetime

import streamlit as st

from db import MAX_TOP_N, PERIOD_OPTIONS, TOURNAMENT_TYPES, create_tournament, format_db_datetime, get_tournaments_df, parse_datetime, update_tournament

EDIT_TOURNAMENT_STATE_KEYS = {
    "name": "edit_tournament_name",
    "type": "edit_tournament_type",
    "top_n": "edit_tournament_top_n",
    "start_date": "edit_tournament_start_date",
    "start_time": "edit_tournament_start_time",
    "period_hours": "edit_tournament_period_hours",
    "end_date": "edit_tournament_end_date",
    "end_time": "edit_tournament_end_time",
    "min_weight": "edit_tournament_min_weight",
}
CREATE_TOURNAMENT_STATE_KEYS = {
    "name": "create_tournament_name",
    "type": "create_tournament_type",
    "top_n": "create_tournament_top_n",
    "start_date": "create_tournament_start_date",
    "start_time": "create_tournament_start_time",
    "period_hours": "create_tournament_period_hours",
    "end_date": "create_tournament_end_date",
    "end_time": "create_tournament_end_time",
    "min_weight": "create_tournament_min_weight",
}


def sync_edit_tournament_form(meta: dict | None):
    if not meta:
        return

    loaded_id = st.session_state.get("edit_tournament_loaded_id")
    tournament_id = int(meta["id"])
    if loaded_id == tournament_id:
        return

    start_at = parse_datetime(meta["start_at"])
    end_at = parse_datetime(meta["end_at"])
    st.session_state[EDIT_TOURNAMENT_STATE_KEYS["name"]] = meta["name"]
    st.session_state[EDIT_TOURNAMENT_STATE_KEYS["type"]] = meta["tournament_type"]
    st.session_state[EDIT_TOURNAMENT_STATE_KEYS["top_n"]] = int(meta["top_n"])
    st.session_state[EDIT_TOURNAMENT_STATE_KEYS["start_date"]] = start_at.date()
    st.session_state[EDIT_TOURNAMENT_STATE_KEYS["start_time"]] = start_at.time()
    st.session_state[EDIT_TOURNAMENT_STATE_KEYS["period_hours"]] = int(meta["period_hours"])
    st.session_state[EDIT_TOURNAMENT_STATE_KEYS["end_date"]] = end_at.date()
    st.session_state[EDIT_TOURNAMENT_STATE_KEYS["end_time"]] = end_at.time()
    st.session_state[EDIT_TOURNAMENT_STATE_KEYS["min_weight"]] = float(meta["min_weight"])
    st.session_state["edit_tournament_loaded_id"] = tournament_id


def sync_create_tournament_form():
    start_date_key = CREATE_TOURNAMENT_STATE_KEYS["start_date"]
    end_date_key = CREATE_TOURNAMENT_STATE_KEYS["end_date"]
    if start_date_key in st.session_state and end_date_key in st.session_state:
        if st.session_state[end_date_key] < st.session_state[start_date_key]:
            st.session_state[end_date_key] = st.session_state[start_date_key]


def render_tournaments_page(active_meta: dict | None, active_tournament_id: int | None):
    st.subheader("Керування турнірами")
    sync_edit_tournament_form(active_meta)
    sync_create_tournament_form()
    creation_success_message = st.session_state.pop("tournament_created_success", None)
    if creation_success_message:
        st.success(creation_success_message, icon="✅")

    name = st.text_input("Назва змагання", key=CREATE_TOURNAMENT_STATE_KEYS["name"])
    tournament_type = st.selectbox(
        "Тип змагання",
        options=list(TOURNAMENT_TYPES.keys()),
        format_func=lambda x: TOURNAMENT_TYPES[x],
        key=CREATE_TOURNAMENT_STATE_KEYS["type"],
    )
    top_n = st.number_input(
        "Скільки крупних риб враховувати",
        min_value=1,
        max_value=MAX_TOP_N,
        value=5,
        step=1,
        key=CREATE_TOURNAMENT_STATE_KEYS["top_n"],
    )
    col_a, col_b = st.columns(2)
    with col_a:
        start_date = st.date_input("Дата старту", key=CREATE_TOURNAMENT_STATE_KEYS["start_date"])
        start_time = st.time_input("Час старту", key=CREATE_TOURNAMENT_STATE_KEYS["start_time"], step=3600)
        period_hours = st.selectbox(
            "Тривалість одного періоду (год)",
            PERIOD_OPTIONS,
            index=1,
            key=CREATE_TOURNAMENT_STATE_KEYS["period_hours"],
        )
    with col_b:
        end_date = st.date_input(
            "Дата завершення",
            min_value=start_date,
            key=CREATE_TOURNAMENT_STATE_KEYS["end_date"],
        )
        end_time = st.time_input("Час завершення", key=CREATE_TOURNAMENT_STATE_KEYS["end_time"], step=3600)
        min_weight = st.number_input(
            "Мінімальна вага риби (кг)",
            min_value=0.0,
            step=0.1,
            value=1.0,
            format="%.3f",
            key=CREATE_TOURNAMENT_STATE_KEYS["min_weight"],
        )
    submitted = st.button(
        "Створити нове змагання",
        key="create_tournament_submit",
        type="primary",
        use_container_width=True,
    )
    if submitted:
        if not name.strip():
            st.error("Вкажи назву змагання")
        else:
            start_at = format_db_datetime(datetime.combine(start_date, start_time))
            end_at = format_db_datetime(datetime.combine(end_date, end_time))
            if end_at <= start_at:
                st.error("Дата і час завершення мають бути пізніше за старт")
            else:
                create_tournament(name, tournament_type, int(top_n), start_at, end_at, int(period_hours), float(min_weight))
                st.session_state["tournament_created_success"] = "Турнір успішно створено. Нове змагання стало активним."
                st.rerun()

    if active_meta is not None and active_tournament_id is not None:
        st.markdown("### Редагування обраного турніру")
        with st.form("edit_tournament_form"):
            edit_name = st.text_input("Назва змагання", key=EDIT_TOURNAMENT_STATE_KEYS["name"])
            edit_tournament_type = st.selectbox(
                "Тип змагання",
                options=list(TOURNAMENT_TYPES.keys()),
                format_func=lambda x: TOURNAMENT_TYPES[x],
                key=EDIT_TOURNAMENT_STATE_KEYS["type"],
            )
            edit_top_n = st.number_input(
                "Скільки крупних риб враховувати",
                min_value=1,
                max_value=MAX_TOP_N,
                step=1,
                key=EDIT_TOURNAMENT_STATE_KEYS["top_n"],
            )
            edit_col_a, edit_col_b = st.columns(2)
            with edit_col_a:
                edit_start_date = st.date_input("Дата старту", key=EDIT_TOURNAMENT_STATE_KEYS["start_date"])
                edit_start_time = st.time_input("Час старту", key=EDIT_TOURNAMENT_STATE_KEYS["start_time"], step=3600)
                edit_period_hours = st.selectbox(
                    "Тривалість одного періоду (год)",
                    PERIOD_OPTIONS,
                    key=EDIT_TOURNAMENT_STATE_KEYS["period_hours"],
                )
            with edit_col_b:
                edit_end_date = st.date_input("Дата завершення", key=EDIT_TOURNAMENT_STATE_KEYS["end_date"])
                edit_end_time = st.time_input("Час завершення", key=EDIT_TOURNAMENT_STATE_KEYS["end_time"], step=3600)
                edit_min_weight = st.number_input(
                    "Мінімальна вага риби (кг)",
                    min_value=0.0,
                    step=0.1,
                    format="%.3f",
                    key=EDIT_TOURNAMENT_STATE_KEYS["min_weight"],
                )
            edit_submitted = st.form_submit_button("Зберегти зміни")
            if edit_submitted:
                if not edit_name.strip():
                    st.error("Вкажи назву змагання")
                else:
                    edit_start_at = format_db_datetime(datetime.combine(edit_start_date, edit_start_time))
                    edit_end_at = format_db_datetime(datetime.combine(edit_end_date, edit_end_time))
                    if edit_end_at <= edit_start_at:
                        st.error("Дата і час завершення мають бути пізніше за старт")
                    else:
                        update_tournament(
                            active_tournament_id,
                            edit_name,
                            edit_tournament_type,
                            int(edit_top_n),
                            edit_start_at,
                            edit_end_at,
                            int(edit_period_hours),
                            float(edit_min_weight),
                        )
                        st.success("Дані турніру оновлено")
                        st.rerun()

    st.markdown("### Останні 3 змагання")
    tournaments_df = get_tournaments_df().copy()
    if tournaments_df.empty:
        st.info("Ще немає жодного змагання")
        return

    tournaments_df["tournament_type"] = tournaments_df["tournament_type"].map(TOURNAMENT_TYPES)
    tournaments_df["is_active"] = tournaments_df["is_active"].map({True: "Так", False: "Ні"})
    tournaments_df = tournaments_df.rename(
        columns={
            "name": "Назва",
            "tournament_type": "Тип",
            "top_n": "Крупних риб",
            "start_at": "Старт",
            "end_at": "Фініш",
            "period_hours": "Період (год)",
            "min_weight": "Мін. вага",
            "is_active": "Активний",
            "created_at": "Створено",
        }
    )
    st.dataframe(
        tournaments_df[["Назва", "Тип", "Крупних риб", "Старт", "Фініш", "Період (год)", "Мін. вага", "Активний", "Створено"]],
        use_container_width=True,
    )
