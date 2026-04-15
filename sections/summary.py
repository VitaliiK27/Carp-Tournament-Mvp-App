import streamlit as st

from db import build_big_fish, build_period_zone_winners, build_podium, build_results, build_zone_winners


def render_summary_page(active_meta: dict, active_tournament_id: int):
    st.subheader(f"Підсумки — {active_meta['name']}")
    top_n_df, total_df, combo_df = build_results(active_tournament_id)
    top_n_value = int(active_meta.get("top_n", 5))

    zone_source = total_df if active_meta["tournament_type"] == "combo" else top_n_df
    zone_col = "Загальна вага" if active_meta["tournament_type"] == "combo" else f"Заг. вага по {top_n_value}"
    zone_df = build_zone_winners(zone_source, zone_col)
    big_fish_df = build_big_fish(active_tournament_id)
    podium_df = build_podium(top_n_df, total_df, combo_df, active_meta["tournament_type"], top_n_value)

    summary_tabs = st.tabs(["Зони", "Big Fish", "Подіум", "Номінація періодів"])

    with summary_tabs[0]:
        st.dataframe(zone_df, use_container_width=True, height=320)
    with summary_tabs[1]:
        st.dataframe(big_fish_df, use_container_width=True, height=220)
    with summary_tabs[2]:
        st.dataframe(podium_df, use_container_width=True, height=220)
    with summary_tabs[3]:
        st.caption("Ця номінація показується окремо і не впливає на загальний результат турніру")
        period_zone_df = build_period_zone_winners(active_tournament_id)
        if period_zone_df.empty:
            st.info("Поки що немає даних по цій номінації")
        else:
            st.dataframe(period_zone_df, use_container_width=True, height=420)
