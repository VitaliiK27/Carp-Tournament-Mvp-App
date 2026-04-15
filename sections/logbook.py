import streamlit as st

from db import delete_catch, get_catches_df


def render_logbook_page(active_meta: dict, active_tournament_id: int):
    st.subheader(f"Усі зважування — {active_meta['name']}")
    catches_df = get_catches_df(active_tournament_id)
    if catches_df.empty:
        st.info("Поки що немає жодного зважування")
        return

    st.dataframe(catches_df, use_container_width=True, height=420)
    with st.expander("🗑️ Видалити запис"):
        catch_id = st.selectbox("ID запису", catches_df["id"].tolist())
        if st.button("Видалити вибране зважування"):
            delete_catch(int(catch_id))
            st.success("Запис видалено")
            st.rerun()
