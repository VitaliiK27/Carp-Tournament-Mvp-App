import streamlit as st

from db import ZONE_OPTIONS, add_team, get_teams_df, update_team

EDIT_TEAM_STATE_KEYS = {
    "selected_id": "edit_team_selected_id",
    "name": "edit_team_name",
    "sector": "edit_team_sector",
    "zone": "edit_team_zone",
}


def sync_edit_team_form(teams_df, selected_team_id: int):
    loaded_id = st.session_state.get("edit_team_loaded_id")
    if loaded_id == selected_team_id:
        return

    selected_team = teams_df[teams_df["id"] == selected_team_id]
    if selected_team.empty:
        return

    team = selected_team.iloc[0]
    st.session_state[EDIT_TEAM_STATE_KEYS["selected_id"]] = selected_team_id
    st.session_state[EDIT_TEAM_STATE_KEYS["name"]] = team["name"]
    st.session_state[EDIT_TEAM_STATE_KEYS["sector"]] = str(team["sector"])
    st.session_state[EDIT_TEAM_STATE_KEYS["zone"]] = team["zone"]
    st.session_state["edit_team_loaded_id"] = selected_team_id


def render_teams_page(active_meta: dict, active_tournament_id: int):
    st.subheader(f"Команди — {active_meta['name']}")

    with st.expander("➕ Додати команду", expanded=True):
        with st.form("add_team_form", clear_on_submit=True):
            name = st.text_input("Назва команди")
            sector = st.text_input("Сектор")
            zone = st.selectbox("Зона", ZONE_OPTIONS)
            submitted = st.form_submit_button("Додати команду")
            if submitted:
                if not name.strip() or not sector.strip():
                    st.error("Вкажи назву команди та сектор")
                else:
                    try:
                        add_team(active_tournament_id, name, sector, zone)
                        st.success(f"Команду '{name}' додано")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Не вдалося додати команду: {exc}")

    teams_df = get_teams_df(active_tournament_id)

    if not teams_df.empty:
        team_options = {
            f"{row['name']} (сектор {row['sector']}, зона {row['zone']})": int(row["id"])
            for _, row in teams_df.iterrows()
        }
        selected_label = st.selectbox("Команда для редагування", list(team_options.keys()))
        selected_team_id = team_options[selected_label]
        sync_edit_team_form(teams_df, selected_team_id)

        with st.form("edit_team_form"):
            edit_name = st.text_input("Назва команди", key=EDIT_TEAM_STATE_KEYS["name"])
            edit_sector = st.text_input("Сектор", key=EDIT_TEAM_STATE_KEYS["sector"])
            edit_zone = st.selectbox("Зона", ZONE_OPTIONS, key=EDIT_TEAM_STATE_KEYS["zone"])
            edit_submitted = st.form_submit_button("Зберегти зміни")
            if edit_submitted:
                if not edit_name.strip() or not edit_sector.strip():
                    st.error("Вкажи назву команди та сектор")
                else:
                    try:
                        update_team(selected_team_id, edit_name, edit_sector, edit_zone)
                        st.success("Дані команди оновлено")
                        st.session_state["edit_team_loaded_id"] = None
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Не вдалося оновити команду: {exc}")

    st.markdown("### Список команд")
    st.dataframe(teams_df.drop(columns=["id"]) if not teams_df.empty else teams_df, use_container_width=True, height=360)
