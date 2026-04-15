import streamlit as st

from db import build_results
from pdf_utils import build_results_pdf


def render_results_page(active_meta: dict, active_tournament_id: int):
    st.subheader(f"Таблиці результатів — {active_meta['name']}")
    top_n_df, total_df, combo_df = build_results(active_tournament_id)
    top_n_value = int(active_meta.get("top_n", 5))
    pdf_bytes = build_results_pdf(active_tournament_id)

    c1, c2 = st.columns([1, 1])
    with c1:
        st.download_button(
            "⬇️ Експорт у PDF",
            data=pdf_bytes,
            file_name=f"results_{active_meta['name'].replace(' ', '_')}.pdf",
            mime="application/pdf",
            use_container_width=True,
        )
    with c2:
        st.code("Додай до URL: ?mode=tablo", language=None)

    result_tabs = st.tabs([f"{top_n_value} крупних", "Загальна вага", "Залік"] if active_meta["tournament_type"] == "combo" else [f"{top_n_value} крупних"])

    with result_tabs[0]:
        if top_n_df.empty:
            st.info("Поки що немає даних")
        else:
            st.dataframe(top_n_df, use_container_width=True, height=420)

    if active_meta["tournament_type"] == "combo":
        with result_tabs[1]:
            st.dataframe(total_df, use_container_width=True, height=420)
        with result_tabs[2]:
            st.dataframe(combo_df, use_container_width=True, height=420)
