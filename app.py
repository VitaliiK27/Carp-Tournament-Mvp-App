import sqlite3
from contextlib import closing
from datetime import datetime

import pandas as pd
import streamlit as st

DB_PATH = "carp_tournament.db"
FISH_TYPES = ["Короп", "Амур", "Інше"]
TOURNAMENT_TYPES = {
    "topN": "N крупних риб",
    "combo": "N крупних + загальна вага",
}
MAX_TOP_N = 15
PERIOD_OPTIONS = [6, 12, 18, 24]
ZONE_OPTIONS = ["A", "B", "C", "D"]


# ---------- DB ----------
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def ensure_column(conn, table_name: str, column_name: str, column_def: str):
    cols = pd.read_sql_query(f"PRAGMA table_info({table_name})", conn)["name"].tolist()
    if column_name not in cols:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")


def init_db():
    with closing(get_conn()) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tournaments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                tournament_type TEXT NOT NULL,
                top_n INTEGER NOT NULL DEFAULT 5,
                start_at TEXT,
                end_at TEXT,
                period_hours INTEGER NOT NULL DEFAULT 12,
                min_weight REAL NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS teams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id INTEGER,
                name TEXT NOT NULL,
                sector TEXT NOT NULL,
                zone TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS catches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id INTEGER,
                team_id INTEGER NOT NULL,
                caught_at TEXT NOT NULL,
                period INTEGER NOT NULL,
                fish_type TEXT NOT NULL,
                weight REAL NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

        ensure_column(conn, "tournaments", "top_n", "INTEGER NOT NULL DEFAULT 5")
        ensure_column(conn, "tournaments", "start_at", "TEXT")
        ensure_column(conn, "tournaments", "end_at", "TEXT")
        ensure_column(conn, "tournaments", "period_hours", "INTEGER NOT NULL DEFAULT 12")
        ensure_column(conn, "tournaments", "min_weight", "REAL NOT NULL DEFAULT 0")
        ensure_column(conn, "tournaments", "is_active", "INTEGER NOT NULL DEFAULT 1")

        ensure_column(conn, "teams", "tournament_id", "INTEGER")
        ensure_column(conn, "catches", "tournament_id", "INTEGER")

        cur.execute("UPDATE tournaments SET top_n = COALESCE(top_n, 5)")
        cur.execute("UPDATE tournaments SET period_hours = COALESCE(period_hours, 12)")
        cur.execute("UPDATE tournaments SET min_weight = COALESCE(min_weight, 0)")
        cur.execute("UPDATE tournaments SET is_active = COALESCE(is_active, 1)")
        cur.execute("UPDATE tournaments SET start_at = COALESCE(start_at, created_at)")
        cur.execute("UPDATE tournaments SET end_at = COALESCE(end_at, created_at)")

        try:
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_team_per_tournament
                ON teams (tournament_id, name)
                """
            )
        except Exception:
            pass

        conn.commit()


def create_tournament(
    name: str,
    tournament_type: str,
    top_n: int,
    start_at: str,
    end_at: str,
    period_hours: int,
    min_weight: float,
):
    top_n = max(1, min(int(top_n), MAX_TOP_N))
    period_hours = max(1, int(period_hours))
    min_weight = max(0.0, float(min_weight))

    with closing(get_conn()) as conn:
        cur = conn.cursor()
        now = datetime.now().isoformat()

        cur.execute("UPDATE tournaments SET is_active = 0")
        cur.execute(
            """
            INSERT INTO tournaments (
                name, tournament_type, top_n, start_at, end_at,
                period_hours, min_weight, is_active, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
            """,
            (name.strip(), tournament_type, top_n, start_at, end_at, period_hours, min_weight, now),
        )
        conn.commit()

    prune_old_tournaments()


def update_tournament(
    tournament_id: int,
    name: str,
    tournament_type: str,
    top_n: int,
    start_at: str,
    end_at: str,
    period_hours: int,
    min_weight: float,
):
    top_n = max(1, min(int(top_n), MAX_TOP_N))
    period_hours = max(1, int(period_hours))
    min_weight = max(0.0, float(min_weight))

    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE tournaments
            SET name = ?, tournament_type = ?, top_n = ?, start_at = ?, end_at = ?,
                period_hours = ?, min_weight = ?
            WHERE id = ?
            """,
            (name.strip(), tournament_type, top_n, start_at, end_at, period_hours, min_weight, int(tournament_id)),
        )
        conn.commit()


def prune_old_tournaments():
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        ids = pd.read_sql_query(
            "SELECT id FROM tournaments ORDER BY datetime(created_at) DESC",
            conn,
        )["id"].tolist()

        to_delete = ids[3:]
        for tournament_id in to_delete:
            cur.execute("DELETE FROM catches WHERE tournament_id = ?", (int(tournament_id),))
            cur.execute("DELETE FROM teams WHERE tournament_id = ?", (int(tournament_id),))
            cur.execute("DELETE FROM tournaments WHERE id = ?", (int(tournament_id),))

        conn.commit()


def get_tournaments_df() -> pd.DataFrame:
    with closing(get_conn()) as conn:
        return pd.read_sql_query(
            """
            SELECT
                id,
                name,
                tournament_type,
                top_n,
                start_at,
                end_at,
                period_hours,
                min_weight,
                is_active,
                created_at
            FROM tournaments
            ORDER BY datetime(created_at) DESC
            """,
            conn,
        )


def get_active_tournament_id():
    with closing(get_conn()) as conn:
        df = pd.read_sql_query(
            """
            SELECT id
            FROM tournaments
            WHERE is_active = 1
            ORDER BY datetime(created_at) DESC
            LIMIT 1
            """,
            conn,
        )
    if df.empty:
        return None
    return int(df.iloc[0]["id"])


def set_active_tournament(tournament_id: int):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("UPDATE tournaments SET is_active = 0")
        cur.execute("UPDATE tournaments SET is_active = 1 WHERE id = ?", (int(tournament_id),))
        conn.commit()


def get_tournament_meta(tournament_id: int):
    with closing(get_conn()) as conn:
        df = pd.read_sql_query(
            "SELECT * FROM tournaments WHERE id = ?",
            conn,
            params=(int(tournament_id),),
        )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def add_team(tournament_id: int, name: str, sector: str, zone: str):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO teams (tournament_id, name, sector, zone, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                int(tournament_id),
                name.strip(),
                str(sector).strip(),
                zone.strip().upper(),
                datetime.now().isoformat(),
            ),
        )
        conn.commit()


def update_team(team_id: int, name: str, sector: str, zone: str):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE teams
            SET name = ?, sector = ?, zone = ?
            WHERE id = ?
            """,
            (name.strip(), str(sector).strip(), zone.strip().upper(), int(team_id)),
        )
        conn.commit()


def delete_team(team_id: int):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM catches WHERE team_id = ?", (int(team_id),))
        cur.execute("DELETE FROM teams WHERE id = ?", (int(team_id),))
        conn.commit()


def get_teams_df(tournament_id: int) -> pd.DataFrame:
    with closing(get_conn()) as conn:
        return pd.read_sql_query(
            """
            SELECT id, name, sector, zone
            FROM teams
            WHERE tournament_id = ?
            ORDER BY
                CASE
                    WHEN trim(sector) GLOB '[0-9]*' THEN CAST(trim(sector) AS INTEGER)
                    ELSE 999999
                END,
                sector,
                name
            """,
            conn,
            params=(int(tournament_id),),
        )


def get_period_number(caught_at: str, start_at: str, end_at: str, period_hours: int):
    caught_dt = datetime.strptime(caught_at, "%Y-%m-%d %H:%M")
    start_dt = datetime.strptime(start_at, "%Y-%m-%d %H:%M")
    end_dt = datetime.strptime(end_at, "%Y-%m-%d %H:%M")

    if caught_dt < start_dt or caught_dt > end_dt:
        return None

    delta_hours = (caught_dt - start_dt).total_seconds() / 3600
    return int(delta_hours // int(period_hours)) + 1


def get_period_count(start_at: str, end_at: str, period_hours: int):
    start_dt = datetime.strptime(start_at, "%Y-%m-%d %H:%M")
    end_dt = datetime.strptime(end_at, "%Y-%m-%d %H:%M")
    total_hours = max(0, (end_dt - start_dt).total_seconds() / 3600)
    return max(1, int((total_hours + int(period_hours) - 1) // int(period_hours)))


def add_catch(tournament_id: int, team_id: int, caught_at: str, period: int, fish_type: str, weight: float):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO catches (tournament_id, team_id, caught_at, period, fish_type, weight, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(tournament_id),
                int(team_id),
                caught_at,
                int(period),
                fish_type,
                float(weight),
                datetime.now().isoformat(),
            ),
        )
        conn.commit()


def update_catch(catch_id: int, team_id: int, caught_at: str, period: int, fish_type: str, weight: float):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE catches
            SET team_id = ?, caught_at = ?, period = ?, fish_type = ?, weight = ?
            WHERE id = ?
            """,
            (int(team_id), caught_at, int(period), fish_type, float(weight), int(catch_id)),
        )
        conn.commit()


def delete_catch(catch_id: int):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM catches WHERE id = ?", (int(catch_id),))
        conn.commit()


def get_catches_df(tournament_id: int) -> pd.DataFrame:
    with closing(get_conn()) as conn:
        return pd.read_sql_query(
            """
            SELECT
                c.id,
                c.tournament_id,
                t.id AS team_id,
                t.name AS team,
                t.sector,
                t.zone,
                c.caught_at,
                c.period,
                c.fish_type,
                ROUND(c.weight, 3) AS weight
            FROM catches c
            JOIN teams t ON t.id = c.team_id
            WHERE c.tournament_id = ?
            ORDER BY datetime(c.caught_at) DESC, c.id DESC
            """,
            conn,
            params=(int(tournament_id),),
        )


def get_big_fish_row(catches_df: pd.DataFrame):
    if catches_df.empty:
        return None
    return catches_df.sort_values(["weight", "caught_at"], ascending=[False, True]).iloc[0]


def add_places(df: pd.DataFrame, score_col: str, place_col: str):
    df = df.sort_values([score_col, "Команда"], ascending=[False, True]).reset_index(drop=True)
    df[place_col] = range(1, len(df) + 1)
    return df


def build_results(tournament_id: int):
    meta = get_tournament_meta(tournament_id)
    top_n_value = int(meta.get("top_n", 5)) if meta else 5
    min_weight_value = float(meta.get("min_weight", 0)) if meta else 0.0

    with closing(get_conn()) as conn:
        teams = pd.read_sql_query(
            """
            SELECT id, name, sector, zone
            FROM teams
            WHERE tournament_id = ?
            ORDER BY
                CASE
                    WHEN trim(sector) GLOB '[0-9]*' THEN CAST(trim(sector) AS INTEGER)
                    ELSE 999999
                END,
                sector,
                name
            """,
            conn,
            params=(int(tournament_id),),
        )
        catches = pd.read_sql_query(
            """
            SELECT id, team_id, weight, fish_type, caught_at, period
            FROM catches
            WHERE tournament_id = ?
            """,
            conn,
            params=(int(tournament_id),),
        )

    fish_columns = [f"{i} риба" for i in range(1, top_n_value + 1)]
    empty_top_n = pd.DataFrame(columns=["Сектор", "Команда", "Зона", *fish_columns, f"Заг. вага по {top_n_value}"])
    empty_total = pd.DataFrame(columns=["Сектор", "Команда", "Зона", "Загальна вага"])
    empty_combo = pd.DataFrame(columns=["Сектор", "Команда", "Зона", f"Місце {top_n_value} риб", "Місце загальна", "Сума балів"])

    if teams.empty:
        return empty_top_n, empty_total, empty_combo

    if min_weight_value > 0:
        catches = catches[catches["weight"] >= min_weight_value].copy()

    big_fish_row = get_big_fish_row(catches)
    catches_wo_big_fish = catches.copy()
    if big_fish_row is not None:
        catches_wo_big_fish = catches_wo_big_fish[catches_wo_big_fish["id"] != big_fish_row["id"]].copy()

    top_n_rows = []
    total_rows = []

    for _, team in teams.iterrows():
        team_catches = catches_wo_big_fish[catches_wo_big_fish["team_id"] == team["id"]].copy()
        weights = sorted(team_catches["weight"].tolist(), reverse=True)

        top_n_weights = weights[:top_n_value]
        while len(top_n_weights) < top_n_value:
            top_n_weights.append(0.0)

        row = {
            "Сектор": team["sector"],
            "Команда": team["name"],
            "Зона": team["zone"],
        }
        for idx in range(top_n_value):
            row[f"{idx + 1} риба"] = round(top_n_weights[idx], 3)
        row[f"Заг. вага по {top_n_value}"] = round(sum(top_n_weights), 3)
        top_n_rows.append(row)

        total_rows.append(
            {
                "Сектор": team["sector"],
                "Команда": team["name"],
                "Зона": team["zone"],
                "Загальна вага": round(sum(weights), 3),
            }
        )

    top_n_df = pd.DataFrame(top_n_rows)
    total_df = pd.DataFrame(total_rows)
    top_n_score_col = f"Заг. вага по {top_n_value}"
    top_n_place_col = f"Місце {top_n_value} риб"

    if meta and meta["tournament_type"] == "combo":
        top_n_places = add_places(
            top_n_df[["Сектор", "Команда", "Зона", top_n_score_col]].copy(),
            top_n_score_col,
            top_n_place_col,
        )
        total_places = add_places(
            total_df[["Сектор", "Команда", "Зона", "Загальна вага"]].copy(),
            "Загальна вага",
            "Місце загальна",
        )
        combo_df = top_n_places[["Сектор", "Команда", "Зона", top_n_place_col]].merge(
            total_places[["Команда", "Місце загальна"]],
            on="Команда",
            how="left",
        )
        combo_df["Сума балів"] = combo_df[top_n_place_col] + combo_df["Місце загальна"]
        combo_df = combo_df.sort_values(
            ["Сума балів", top_n_place_col, "Місце загальна", "Команда"],
            ascending=[True, True, True, True],
        ).reset_index(drop=True)
    else:
        combo_df = empty_combo

    return top_n_df, total_df, combo_df


def build_zone_winners(results_df: pd.DataFrame, score_col: str) -> pd.DataFrame:
    if results_df.empty:
        return pd.DataFrame(columns=["Зона", "Команда", "Показник"])

    zone_rows = []
    for zone, group in results_df.groupby("Зона"):
        winner = group.sort_values([score_col, "Команда"], ascending=[False, True]).iloc[0]
        zone_rows.append({"Зона": zone, "Команда": winner["Команда"], "Показник": winner[score_col]})

    return pd.DataFrame(zone_rows).sort_values("Зона")


def build_period_zone_winners(tournament_id: int) -> pd.DataFrame:
    meta = get_tournament_meta(tournament_id)
    if not meta:
        return pd.DataFrame(columns=["Період", "Зона", "Команда", "Найбільша риба"])

    catches = get_catches_df(tournament_id)
    if catches.empty:
        return pd.DataFrame(columns=["Період", "Зона", "Команда", "Найбільша риба"])

    min_weight_value = float(meta.get("min_weight", 0))
    if min_weight_value > 0:
        catches = catches[catches["weight"] >= min_weight_value].copy()

    if catches.empty:
        return pd.DataFrame(columns=["Період", "Зона", "Команда", "Найбільша риба"])

    winners = []
    grouped = catches.groupby(["period", "zone"], dropna=False)
    for (period, zone), group in grouped:
        winner = group.sort_values(["weight", "caught_at", "team"], ascending=[False, True, True]).iloc[0]
        winners.append(
            {
                "Період": int(period),
                "Зона": zone,
                "Команда": winner["team"],
                "Найбільша риба": winner["weight"],
            }
        )

    if not winners:
        return pd.DataFrame(columns=["Період", "Зона", "Команда", "Найбільша риба"])

    return pd.DataFrame(winners).sort_values(["Період", "Зона"]).reset_index(drop=True)


def build_big_fish(tournament_id: int) -> pd.DataFrame:
    catches = get_catches_df(tournament_id)
    if catches.empty:
        return pd.DataFrame(columns=["Команда", "Вага", "Час", "Вид"])

    max_row = get_big_fish_row(catches)
    return pd.DataFrame(
        [
            {
                "Команда": max_row["team"],
                "Вага": max_row["weight"],
                "Час": max_row["caught_at"],
                "Вид": max_row["fish_type"],
            }
        ]
    )


def build_podium(top_n_df: pd.DataFrame, combo_df: pd.DataFrame, tournament_type: str, top_n_value: int) -> pd.DataFrame:
    if tournament_type == "combo":
        if combo_df.empty:
            return pd.DataFrame(columns=["Місце", "Команда", "Бали"])
        podium = combo_df.head(3).copy().reset_index(drop=True)
        podium.insert(0, "Місце", ["1 місце", "2 місце", "3 місце"][: len(podium)])
        return podium[["Місце", "Команда", "Сума балів"]].rename(columns={"Сума балів": "Бали"})

    score_col = f"Заг. вага по {top_n_value}"
    if top_n_df.empty:
        return pd.DataFrame(columns=["Місце", "Команда", "Вага"])

    podium = top_n_df.sort_values([score_col, "1 риба", "Команда"], ascending=[False, False, True]).head(3).copy().reset_index(drop=True)
    podium.insert(0, "Місце", ["1 місце", "2 місце", "3 місце"][: len(podium)])
    return podium[["Місце", "Команда", score_col]].rename(columns={score_col: "Вага"})


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


def build_live_scoreboard(tournament_id: int):
    meta = get_tournament_meta(tournament_id)
    top_n_df, total_df, combo_df = build_results(tournament_id)
    top_n_value = int(meta.get("top_n", 5)) if meta else 5
    score_col = f"Заг. вага по {top_n_value}"

    if meta and meta["tournament_type"] == "combo":
        board_df = combo_df.copy()
        if not board_df.empty:
            board_df = board_df.merge(total_df[["Команда", "Загальна вага"]], on="Команда", how="left")
            board_df = board_df.merge(top_n_df[["Команда", score_col]], on="Команда", how="left")
        return board_df.head(10), top_n_value

    board_df = top_n_df.sort_values([score_col, "1 риба", "Команда"], ascending=[False, False, True]).copy()
    return board_df.head(10), top_n_value


def seed_demo_data():
    if not get_tournaments_df().empty:
        return

    create_tournament("Демо турнір", "combo", 5, "2026-03-27 06:00", "2026-03-29 06:00", 12, 1.0)
    tournament_id = get_active_tournament_id()

    demo_teams = [
        ("CarpHub", "1", "A"),
        ("GoldFish", "2", "A"),
        ("Бос", "3", "B"),
        ("TekoFish", "4", "B"),
    ]
    for team in demo_teams:
        add_team(tournament_id, *team)

    teams_df = get_teams_df(tournament_id)
    team_map = {row["name"]: int(row["id"]) for _, row in teams_df.iterrows()}

    demo_rows = [
        ("Бос", "2026-03-27 10:59", "Короп", 10.02),
        ("Бос", "2026-03-27 07:30", "Короп", 3.41),
        ("GoldFish", "2026-03-27 11:20", "Короп", 14.73),
        ("GoldFish", "2026-03-27 13:05", "Короп", 10.44),
        ("GoldFish", "2026-03-28 08:10", "Амур", 8.225),
        ("CarpHub", "2026-03-27 12:15", "Короп", 17.01),
        ("CarpHub", "2026-03-28 09:42", "Короп", 14.50),
        ("CarpHub", "2026-03-28 10:05", "Короп", 12.91),
        ("TekoFish", "2026-03-28 11:33", "Короп", 13.07),
        ("TekoFish", "2026-03-28 11:55", "Короп", 12.00),
    ]

    meta = get_tournament_meta(tournament_id)
    for team_name, caught_at, fish_type, weight in demo_rows:
        period = get_period_number(caught_at, meta["start_at"], meta["end_at"], int(meta["period_hours"]))
        add_catch(tournament_id, team_map[team_name], caught_at, int(period), fish_type, float(weight))


# ---------- UI ----------
st.set_page_config(page_title="Карпові змагання", page_icon="🎣", layout="wide")
init_db()

st.markdown(
    """
    <style>
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
    div[data-testid="stDataFrame"] {
        overflow-x: auto;
    }
    @media (max-width: 768px) {
        .block-container {
            padding-left: 0.55rem;
            padding-right: 0.55rem;
        }
        h1 { font-size: 1.55rem !important; }
        h2 { font-size: 1.25rem !important; }
        h3 { font-size: 1.05rem !important; }

        .stButton > button,
        .stDownloadButton > button {
            width: 100%;
            border-radius: 10px;
            min-height: 2.8rem;
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

st.title("🎣 MVP: облік результатів карпових змагань")
st.caption(
    "Підтримує кілька змагань, зберігає останні 3 турніри, Big Fish окремо від командного заліку, "
    "а при створенні задаються N крупних риб, старт/фініш, тривалість періоду та мінімальна вага"
)

with st.sidebar:
    st.header("Турніри")

    if st.button("Заповнити демо-дані"):
        seed_demo_data()
        st.success("Демо-дані додано")
        st.rerun()

    tournaments_df = get_tournaments_df()
    if not tournaments_df.empty:
        tournament_labels = {
            f"{row['name']} ({TOURNAMENT_TYPES.get(row['tournament_type'], row['tournament_type'])})": int(row["id"])
            for _, row in tournaments_df.iterrows()
        }
        active_tournament_id = get_active_tournament_id()
        default_index = list(tournament_labels.values()).index(active_tournament_id) if active_tournament_id in tournament_labels.values() else 0

        selected_label = st.selectbox("Активний турнір", list(tournament_labels.keys()), index=default_index)
        selected_tournament_id = tournament_labels[selected_label]

        if selected_tournament_id != active_tournament_id:
            set_active_tournament(selected_tournament_id)
            st.rerun()

    st.markdown("---")
    page = st.radio(
        "Розділ",
        [
            "Турніри",
            "Команди",
            "Швидке зважування",
            "Додати рибу",
            "Результати",
            "Мобільне табло",
            "Підсумки",
            "Журнал зважувань",
        ],
    )

active_tournament_id = get_active_tournament_id()
active_meta = get_tournament_meta(active_tournament_id) if active_tournament_id else None


if page == "Турніри":
    st.subheader("Керування турнірами")

    with st.form("add_tournament_form", clear_on_submit=True):
        name = st.text_input("Назва змагання")
        tournament_type = st.selectbox(
            "Тип змагання",
            options=list(TOURNAMENT_TYPES.keys()),
            format_func=lambda x: TOURNAMENT_TYPES[x],
        )
        top_n = st.number_input("Скільки крупних риб враховувати", min_value=1, max_value=MAX_TOP_N, value=5, step=1)

        col_a, col_b = st.columns(2)
        with col_a:
            start_date = st.date_input("Дата старту")
            start_time = st.time_input("Час старту")
            period_hours = st.selectbox("Тривалість одного періоду (год)", PERIOD_OPTIONS, index=1)
        with col_b:
            end_date = st.date_input("Дата завершення")
            end_time = st.time_input("Час завершення")
            min_weight = st.number_input("Мінімальна вага риби (кг)", min_value=0.0, step=0.1, value=1.0, format="%.3f")

        submitted = st.form_submit_button("Створити нове змагання")
        if submitted:
            if not name.strip():
                st.error("Вкажи назву змагання")
            else:
                start_at = datetime.combine(start_date, start_time).strftime("%Y-%m-%d %H:%M")
                end_at = datetime.combine(end_date, end_time).strftime("%Y-%m-%d %H:%M")

                if end_at <= start_at:
                    st.error("Дата і час завершення мають бути пізніше за старт")
                else:
                    create_tournament(name, tournament_type, int(top_n), start_at, end_at, int(period_hours), float(min_weight))
                    st.success("Нове змагання створено. Воно стало активним")
                    st.rerun()

    if active_meta:
        with st.expander("✏️ Редагувати активний турнір"):
            active_start_dt = datetime.strptime(active_meta["start_at"], "%Y-%m-%d %H:%M")
            active_end_dt = datetime.strptime(active_meta["end_at"], "%Y-%m-%d %H:%M")
            current_period = int(active_meta.get("period_hours", 12))

            with st.form("edit_tournament_form"):
                edit_name = st.text_input("Назва змагання", value=active_meta["name"])
                edit_type = st.selectbox(
                    "Тип змагання",
                    options=list(TOURNAMENT_TYPES.keys()),
                    index=list(TOURNAMENT_TYPES.keys()).index(active_meta["tournament_type"]),
                    format_func=lambda x: TOURNAMENT_TYPES[x],
                )
                edit_top_n = st.number_input(
                    "Скільки крупних риб враховувати",
                    min_value=1,
                    max_value=MAX_TOP_N,
                    value=int(active_meta.get("top_n", 5)),
                    step=1,
                )

                ec1, ec2 = st.columns(2)
                with ec1:
                    edit_start_date = st.date_input("Дата старту", value=active_start_dt.date(), key="edit_start_date")
                    edit_start_time = st.time_input("Час старту", value=active_start_dt.time(), key="edit_start_time")
                    edit_period_hours = st.selectbox(
                        "Тривалість одного періоду (год)",
                        PERIOD_OPTIONS,
                        index=PERIOD_OPTIONS.index(current_period) if current_period in PERIOD_OPTIONS else 1,
                    )
                with ec2:
                    edit_end_date = st.date_input("Дата завершення", value=active_end_dt.date(), key="edit_end_date")
                    edit_end_time = st.time_input("Час завершення", value=active_end_dt.time(), key="edit_end_time")
                    edit_min_weight = st.number_input(
                        "Мінімальна вага риби (кг)",
                        min_value=0.0,
                        step=0.1,
                        value=float(active_meta.get("min_weight", 0)),
                        format="%.3f",
                        key="edit_min_weight",
                    )

                save_tournament = st.form_submit_button("Зберегти зміни турніру")
                if save_tournament:
                    start_at = datetime.combine(edit_start_date, edit_start_time).strftime("%Y-%m-%d %H:%M")
                    end_at = datetime.combine(edit_end_date, edit_end_time).strftime("%Y-%m-%d %H:%M")

                    if end_at <= start_at:
                        st.error("Дата і час завершення мають бути пізніше за старт")
                    else:
                        update_tournament(
                            active_tournament_id,
                            edit_name,
                            edit_type,
                            int(edit_top_n),
                            start_at,
                            end_at,
                            int(edit_period_hours),
                            float(edit_min_weight),
                        )
                        st.success("Турнір оновлено")
                        st.rerun()

    st.markdown("### Останні 3 змагання")
    tournaments_df = get_tournaments_df().copy()
    if tournaments_df.empty:
        st.info("Ще немає жодного змагання")
    else:
        tournaments_df["tournament_type"] = tournaments_df["tournament_type"].map(TOURNAMENT_TYPES)
        tournaments_df["is_active"] = tournaments_df["is_active"].map({1: "Так", 0: "Ні"})
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

elif active_tournament_id is None:
    st.warning("Спочатку створи турнір")

elif page == "Команди":
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
                    except sqlite3.IntegrityError:
                        st.error("У цьому турнірі команда з такою назвою вже існує")

    teams_df = get_teams_df(active_tournament_id)
    st.markdown("### Список команд")
    st.dataframe(teams_df.drop(columns=["id"]) if not teams_df.empty else teams_df, use_container_width=True, height=360)

    if not teams_df.empty:
        with st.expander("✏️ Редагувати або видалити команду"):
            team_choice = st.selectbox("Команда", teams_df["name"].tolist())
            selected_team = teams_df[teams_df["name"] == team_choice].iloc[0]

            with st.form("edit_team_form"):
                edit_team_name = st.text_input("Назва команди", value=selected_team["name"])
                edit_team_sector = st.text_input("Сектор", value=str(selected_team["sector"]))
                edit_team_zone = st.selectbox("Зона", ZONE_OPTIONS, index=ZONE_OPTIONS.index(selected_team["zone"]))

                c1, c2 = st.columns(2)
                with c1:
                    save_team = st.form_submit_button("Зберегти команду")
                with c2:
                    delete_team_btn = st.form_submit_button("Видалити команду")

                if save_team:
                    try:
                        update_team(int(selected_team["id"]), edit_team_name, edit_team_sector, edit_team_zone)
                        st.success("Команду оновлено")
                        st.rerun()
                    except sqlite3.IntegrityError:
                        st.error("У цьому турнірі команда з такою назвою вже існує")

                if delete_team_btn:
                    delete_team(int(selected_team["id"]))
                    st.success("Команду видалено")
                    st.rerun()

elif page == "Швидке зважування":
    st.subheader(f"Швидке зважування — {active_meta['name']}")
    teams_df = get_teams_df(active_tournament_id)

    if teams_df.empty:
        st.warning("Спочатку додай хоча б одну команду")
    else:
        team_options = {row["name"]: int(row["id"]) for _, row in teams_df.iterrows()}
        period_count = get_period_count(active_meta["start_at"], active_meta["end_at"], int(active_meta["period_hours"]))

        st.markdown(
            f"""
            <div class="mobile-card">
                <div><b>Активний турнір:</b> {active_meta['name']}</div>
                <div><b>Періодів:</b> {period_count}</div>
                <div><b>Мін. вага:</b> {active_meta['min_weight']} кг</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        with st.form("quick_catch_form", clear_on_submit=True):
            team_name = st.selectbox("Команда", list(team_options.keys()))
            fish_type = st.selectbox("Вид риби", FISH_TYPES)
            weight = st.number_input("Вага (кг)", min_value=0.001, step=0.001, format="%.3f")
            date_value = st.date_input("Дата зважування")
            time_value = st.time_input("Час зважування")
            submitted = st.form_submit_button("Швидко зберегти")

            if submitted:
                caught_at = datetime.combine(date_value, time_value).strftime("%Y-%m-%d %H:%M")
                period = get_period_number(caught_at, active_meta["start_at"], active_meta["end_at"], int(active_meta["period_hours"]))

                if period is None:
                    st.error("Час зважування має бути в межах турніру")
                elif float(weight) < float(active_meta["min_weight"]):
                    st.error(f"Риба менша за мінімально допустиму вагу: {active_meta['min_weight']} кг")
                else:
                    add_catch(active_tournament_id, team_options[team_name], caught_at, int(period), fish_type, float(weight))
                    st.success(f"Збережено. Команда: {team_name}, вага: {weight} кг, період: {period}")
                    st.rerun()

        recent_df = get_catches_df(active_tournament_id).head(5)
        st.markdown("### Останні зважування")
        render_mobile_cards(recent_df, "team", ["sector", "zone", "weight", "period", "caught_at"], "Поки що немає зважувань")

elif page == "Додати рибу":
    st.subheader(f"Додати зважування — {active_meta['name']}")
    teams_df = get_teams_df(active_tournament_id)

    if teams_df.empty:
        st.warning("Спочатку додай хоча б одну команду")
    else:
        team_options = {row["name"]: int(row["id"]) for _, row in teams_df.iterrows()}
        period_count = get_period_count(active_meta["start_at"], active_meta["end_at"], int(active_meta["period_hours"]))

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
            date_value = st.date_input("Дата")
            time_value = st.time_input("Час")
            fish_type = st.selectbox("Вид", FISH_TYPES)
            weight = st.number_input("Вага (кг)", min_value=0.001, step=0.001, format="%.3f")
            submitted = st.form_submit_button("Зберегти зважування")

            if submitted:
                caught_at = datetime.combine(date_value, time_value).strftime("%Y-%m-%d %H:%M")
                period = get_period_number(caught_at, active_meta["start_at"], active_meta["end_at"], int(active_meta["period_hours"]))

                if period is None:
                    st.error("Час зважування має бути в межах турніру")
                elif float(weight) < float(active_meta["min_weight"]):
                    st.error(f"Риба менша за мінімально допустиму вагу: {active_meta['min_weight']} кг")
                else:
                    add_catch(active_tournament_id, team_options[team_name], caught_at, int(period), fish_type, float(weight))
                    st.success(f"Зважування додано в період {period}")
                    st.rerun()

elif page == "Результати":
    st.subheader(f"Таблиці результатів — {active_meta['name']}")
    top_n_df, total_df, combo_df = build_results(active_tournament_id)
    top_n_value = int(active_meta.get("top_n", 5))

    result_tabs = st.tabs(
        [f"{top_n_value} крупних", "Загальна вага", "Залік"]
        if active_meta["tournament_type"] == "combo"
        else [f"{top_n_value} крупних"]
    )

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

elif page == "Мобільне табло":
    st.subheader(f"Мобільне табло — {active_meta['name']}")
    board_df, top_n_value = build_live_scoreboard(active_tournament_id)
    big_fish_df = build_big_fish(active_tournament_id)
    period_zone_df = build_period_zone_winners(active_tournament_id)

    st.markdown("### Топ команд")
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

elif page == "Підсумки":
    st.subheader(f"Підсумки — {active_meta['name']}")
    top_n_df, total_df, combo_df = build_results(active_tournament_id)
    top_n_value = int(active_meta.get("top_n", 5))

    zone_source = total_df if active_meta["tournament_type"] == "combo" else top_n_df
    zone_col = "Загальна вага" if active_meta["tournament_type"] == "combo" else f"Заг. вага по {top_n_value}"

    zone_df = build_zone_winners(zone_source, zone_col)
    big_fish_df = build_big_fish(active_tournament_id)
    podium_df = build_podium(top_n_df, combo_df, active_meta["tournament_type"], top_n_value)

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

elif page == "Журнал зважувань":
    st.subheader(f"Усі зважування — {active_meta['name']}")
    catches_df = get_catches_df(active_tournament_id)

    if catches_df.empty:
        st.info("Поки що немає жодного зважування")
    else:
        st.dataframe(catches_df, use_container_width=True, height=420)

        with st.expander("✏️ Редагувати або видалити запис"):
            catch_id = st.selectbox("ID запису", catches_df["id"].tolist())
            selected_catch = catches_df[catches_df["id"] == catch_id].iloc[0]

            teams_df = get_teams_df(active_tournament_id)
            team_names = teams_df["name"].tolist()
            current_team_index = team_names.index(selected_catch["team"]) if selected_catch["team"] in team_names else 0
            catch_dt = datetime.strptime(selected_catch["caught_at"], "%Y-%m-%d %H:%M")

            with st.form("edit_catch_form"):
                edit_team_name = st.selectbox("Команда", team_names, index=current_team_index)
                edit_date = st.date_input("Дата", value=catch_dt.date(), key="edit_catch_date")
                edit_time = st.time_input("Час", value=catch_dt.time(), key="edit_catch_time")

                fish_index = FISH_TYPES.index(selected_catch["fish_type"]) if selected_catch["fish_type"] in FISH_TYPES else 0
                edit_fish_type = st.selectbox("Вид", FISH_TYPES, index=fish_index)
                edit_weight = st.number_input(
                    "Вага (кг)",
                    min_value=0.001,
                    step=0.001,
                    format="%.3f",
                    value=float(selected_catch["weight"]),
                    key="edit_catch_weight",
                )

                c1, c2 = st.columns(2)
                with c1:
                    save_catch = st.form_submit_button("Зберегти запис")
                with c2:
                    delete_catch_btn = st.form_submit_button("Видалити запис")

                if save_catch:
                    caught_at = datetime.combine(edit_date, edit_time).strftime("%Y-%m-%d %H:%M")
                    period = get_period_number(caught_at, active_meta["start_at"], active_meta["end_at"], int(active_meta["period_hours"]))

                    if period is None:
                        st.error("Час зважування має бути в межах турніру")
                    elif float(edit_weight) < float(active_meta["min_weight"]):
                        st.error(f"Риба менша за мінімально допустиму вагу: {active_meta['min_weight']} кг")
                    else:
                        team_id = int(teams_df[teams_df["name"] == edit_team_name].iloc[0]["id"])
                        update_catch(int(catch_id), team_id, caught_at, int(period), edit_fish_type, float(edit_weight))
                        st.success("Запис оновлено")
                        st.rerun()

                if delete_catch_btn:
                    delete_catch(int(catch_id))
                    st.success("Запис видалено")
                    st.rerun()