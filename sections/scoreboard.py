import streamlit as st

from db import build_big_fish, build_live_scoreboard, build_period_zone_winners
from ui_helpers import render_mobile_cards


def render_scoreboard_page(active_meta: dict, active_tournament_id: int, scoreboard_mode: bool):
    st.subheader(f"Мобільне табло — {active_meta['name']}")
    if scoreboard_mode:
        st.caption("Режим табло активний. Для звичайного інтерфейсу відкрий додаток без ?mode=tablo")
        st.button("Оновити табло", use_container_width=True)
    else:
        st.caption("Окремий режим табло: відкрий додаток з параметром ?mode=tablo")

    board_df, zone_df, top_n_value = build_live_scoreboard(active_tournament_id)
    big_fish_df = build_big_fish(active_tournament_id)
    period_zone_df = build_period_zone_winners(active_tournament_id)

    st.markdown("### Лідери зон")
    render_mobile_cards(zone_df, "Зона", ["Команда", "Показник"], "Поки що немає лідерів по зонах")

    st.markdown("### Подіум серед переможців зон")
    if active_meta["tournament_type"] == "combo":
        render_mobile_cards(
            board_df,
            "Команда",
            [f"Місце {top_n_value} риб", "Місце загальна", "Сума балів", f"Заг. вага по {top_n_value}", "Загальна вага"],
            "Поки що немає результатів",
        )
    else:
        render_mobile_cards(
            board_df,
            "Команда",
            ["Сектор", "Зона", f"Заг. вага по {top_n_value}", "1 риба"],
            "Поки що немає результатів",
        )

    st.markdown("### Big Fish")
    render_mobile_cards(big_fish_df, "Команда", ["Вага", "Час", "Вид"], "Big Fish ще не визначено")

    st.markdown("### Найбільша риба періоду в зоні")
    render_mobile_cards(period_zone_df, "Команда", ["Період", "Зона", "Найбільша риба"], "Поки що немає даних по періодах")
