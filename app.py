import streamlit as st

from db import (
    TOURNAMENT_TYPES,
    get_active_tournament_id,
    get_tournament_meta,
    get_tournaments_df,
    init_db,
    set_active_tournament,
)
from sections.quick_weighing import render_quick_weighing_page
from sections.results import render_results_page
from sections.scoreboard import render_scoreboard_page
from sections.summary import render_summary_page
from sections.teams import render_teams_page
from sections.tournaments import render_tournaments_page

PAGE_OPTIONS = [
    "Турніри",
    "Команди",
    "Зважування",
    "Результати",
    "Мобільне табло",
    "Підсумки",
]


def render_global_styles(scoreboard_mode: bool):
    st.markdown(
        """
        <style>
        section[data-testid="stSidebar"] {
            display: none !important;
        }
        button[kind="header"], [data-testid="collapsedControl"] {
            display: none !important;
        }
        .block-container {
            padding-top: 1rem;
            padding-bottom: 4rem;
            padding-left: 0.8rem;
            padding-right: 0.8rem;
            max-width: 1200px;
        }
        .mobile-card {
            border: 1px solid rgba(128,128,128,0.25);
            border-radius: 14px;
            padding: 0.9rem;
            margin-bottom: 0.75rem;
            background: rgba(255,255,255,0.02);
        }
        .form-success-banner {
            margin-top: 0.75rem;
            padding: 0.95rem 1rem;
            border-radius: 14px;
            border: 1px solid rgba(50, 122, 74, 0.28);
            background: linear-gradient(180deg, rgba(235, 248, 238, 0.98), rgba(221, 242, 228, 0.98));
            color: #1f5130;
            box-shadow: 0 8px 18px rgba(50, 122, 74, 0.12);
            font-weight: 600;
        }
        div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlockBorderWrapper"] {
            background: linear-gradient(180deg, rgba(247,248,244,0.96), rgba(237,239,232,0.96));
            border: 1px solid rgba(62, 74, 52, 0.16);
            border-radius: 18px;
            box-shadow: 0 10px 28px rgba(46, 56, 36, 0.08);
        }
        div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlockBorderWrapper"] [data-testid="stSegmentedControl"] button {
            border-radius: 10px;
        }
        div[data-testid="stDataFrame"] {
            overflow-x: auto;
        }
        div[data-testid="stSegmentedControl"] {
            margin-top: 0.35rem;
        }
        div[data-testid="stSegmentedControl"] button {
            min-height: 2.8rem;
        }
        @media (max-width: 768px) {
            .block-container {
                padding-left: 0.55rem;
                padding-right: 0.55rem;
            }
            h1 {
                font-size: 1.55rem !important;
            }
            h2 {
                font-size: 1.25rem !important;
            }
            h3 {
                font-size: 1.05rem !important;
            }
            .stButton > button, .stDownloadButton > button {
                width: 100%;
                border-radius: 10px;
                min-height: 2.8rem;
            }
            .form-success-banner {
                margin-top: 0.85rem;
                padding: 1rem 0.9rem;
                font-size: 0.98rem;
                line-height: 1.45;
            }
            div[data-testid="stNumberInput"] input,
            div[data-testid="stTextInput"] input,
            div[data-testid="stTextArea"] textarea {
                font-size: 16px !important;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if scoreboard_mode:
        st.markdown(
            """
            <style>
            .block-container {
                padding-top: 0.6rem;
                max-width: 980px;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )


def render_navigation(scoreboard_mode: bool):
    if scoreboard_mode:
        return "Мобільне табло"

    nav_container = st.container(border=True)
    with nav_container:
        st.caption("Навігація")
        tournaments_df = get_tournaments_df()
        if not tournaments_df.empty:
            tournament_labels = {
                f"{row['name']} ({TOURNAMENT_TYPES.get(row['tournament_type'], row['tournament_type'])})": int(row["id"])
                for _, row in tournaments_df.iterrows()
            }
            active_tournament_id = get_active_tournament_id()
            labels = list(tournament_labels.keys())
            values = list(tournament_labels.values())
            default_index = values.index(active_tournament_id) if active_tournament_id in values else 0
            selected_label = st.selectbox(
                "Активний турнір",
                labels,
                index=default_index,
                key="top_nav_tournament",
            )
            selected_tournament_id = tournament_labels[selected_label]
            if selected_tournament_id != active_tournament_id:
                set_active_tournament(selected_tournament_id)
                st.rerun()
        else:
            st.caption("Спочатку створи турнір, тоді з'явиться вибір активного турніру.")

        current_page = st.session_state.get("main_page", PAGE_OPTIONS[0])
        if current_page not in PAGE_OPTIONS:
            current_page = PAGE_OPTIONS[0]

        page = st.segmented_control(
            "Розділ",
            PAGE_OPTIONS,
            default=current_page,
            key="main_page",
            width="stretch",
            label_visibility="collapsed",
        )
    return page or PAGE_OPTIONS[0]


def render_page(page: str, active_tournament_id: int | None, active_meta: dict | None, scoreboard_mode: bool):
    if page == "Турніри":
        render_tournaments_page(active_meta, active_tournament_id)
        return

    if active_tournament_id is None or active_meta is None:
        st.warning("Спочатку створи турнір")
        return

    if page == "Команди":
        render_teams_page(active_meta, active_tournament_id)
    elif page == "Зважування":
        render_quick_weighing_page(active_meta, active_tournament_id)
    elif page == "Результати":
        render_results_page(active_meta, active_tournament_id)
    elif page == "Мобільне табло":
        render_scoreboard_page(active_meta, active_tournament_id, scoreboard_mode)
    elif page == "Підсумки":
        render_summary_page(active_meta, active_tournament_id)


st.set_page_config(
    page_title="Коропові змагання",
    page_icon="🎣",
    layout="wide",
    initial_sidebar_state="collapsed",
)
init_db()

query_params = st.query_params
scoreboard_mode = str(query_params.get("mode", "")).lower() in ["tablo", "scoreboard", "tv"]

render_global_styles(scoreboard_mode)

st.title("🎣 Коропові змагання")
st.caption("Створи турнір, додай команди і в бій! НХНЛ!")

page = render_navigation(scoreboard_mode)
active_tournament_id = get_active_tournament_id()
active_meta = get_tournament_meta(active_tournament_id) if active_tournament_id else None

render_page(page, active_tournament_id, active_meta, scoreboard_mode)
