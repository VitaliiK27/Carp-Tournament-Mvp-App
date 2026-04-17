import os
from datetime import datetime

import pandas as pd
import psycopg
import streamlit as st

FISH_TYPES = ["Короп", "Амур"]
TOURNAMENT_TYPES = {
    "topN": "N крупних риб",
    "combo": "N крупних + загальна вага",
}
MAX_TOP_N = 15
PERIOD_OPTIONS = [6, 12, 18, 24]
ZONE_OPTIONS = ["A", "B", "C", "D"]
DB_DATETIME_FORMAT = "%Y-%m-%d %H:%M"
DISPLAY_DATETIME_FORMAT = "%d-%m-%Y %H:%M"
DISPLAY_DATETIME_SQL = "DD-MM-YYYY HH24:MI"
DISPLAY_TIMESTAMP_SQL = "DD-MM-YYYY HH24:MI:SS"


def parse_datetime(value: str) -> datetime:
    for fmt in (DISPLAY_DATETIME_FORMAT, DB_DATETIME_FORMAT):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported datetime format: {value}")


def format_db_datetime(value: datetime) -> str:
    return value.strftime(DB_DATETIME_FORMAT)


def get_database_url() -> str:
    if "DATABASE_URL" in st.secrets:
        return st.secrets["DATABASE_URL"]
    env_url = os.getenv("DATABASE_URL")
    if env_url:
        return env_url
    raise RuntimeError(
        "Не знайдено DATABASE_URL. Додай його в .streamlit/secrets.toml або в змінні середовища."
    )


@st.cache_resource
def get_conn():
    return psycopg.connect(get_database_url(), autocommit=False)


def run_query(sql: str, params=None, fetch: bool = False):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        if fetch:
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return rows, columns
    conn.commit()
    return None


def query_df(sql: str, params=None) -> pd.DataFrame:
    rows, columns = run_query(sql, params=params, fetch=True)
    return pd.DataFrame(rows, columns=columns)


def init_db():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tournaments (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                tournament_type TEXT NOT NULL,
                top_n INTEGER NOT NULL DEFAULT 5,
                start_at TIMESTAMP NOT NULL,
                end_at TIMESTAMP NOT NULL,
                period_hours INTEGER NOT NULL DEFAULT 12,
                min_weight NUMERIC(10,3) NOT NULL DEFAULT 0,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS teams (
                id BIGSERIAL PRIMARY KEY,
                tournament_id BIGINT NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                sector TEXT NOT NULL,
                zone TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                CONSTRAINT teams_unique_per_tournament UNIQUE (tournament_id, name)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS catches (
                id BIGSERIAL PRIMARY KEY,
                tournament_id BIGINT NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
                team_id BIGINT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
                caught_at TIMESTAMP NOT NULL,
                period INTEGER NOT NULL,
                fish_type TEXT NOT NULL,
                weight NUMERIC(10,3) NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_tournaments_created_at ON tournaments(created_at DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_teams_tournament_id ON teams(tournament_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_catches_tournament_id ON catches(tournament_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_catches_team_id ON catches(team_id)"
        )
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

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("UPDATE tournaments SET is_active = FALSE")
        cur.execute(
            """
            INSERT INTO tournaments (
                name, tournament_type, top_n, start_at, end_at,
                period_hours, min_weight, is_active
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
            """,
            (
                name.strip(),
                tournament_type,
                top_n,
                start_at,
                end_at,
                period_hours,
                min_weight,
            ),
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

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tournaments
            SET
                name = %s,
                tournament_type = %s,
                top_n = %s,
                start_at = %s,
                end_at = %s,
                period_hours = %s,
                min_weight = %s
            WHERE id = %s
            """,
            (
                name.strip(),
                tournament_type,
                top_n,
                start_at,
                end_at,
                period_hours,
                min_weight,
                int(tournament_id),
            ),
        )
        conn.commit()


def delete_tournament(tournament_id: int):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT is_active FROM tournaments WHERE id = %s", (int(tournament_id),))
        row = cur.fetchone()
        if row is None:
            conn.commit()
            return

        was_active = bool(row[0])
        cur.execute("DELETE FROM tournaments WHERE id = %s", (int(tournament_id),))

        if was_active:
            cur.execute(
                """
                SELECT id
                FROM tournaments
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            next_row = cur.fetchone()
            cur.execute("UPDATE tournaments SET is_active = FALSE")
            if next_row is not None:
                cur.execute("UPDATE tournaments SET is_active = TRUE WHERE id = %s", (int(next_row[0]),))

        conn.commit()


def prune_old_tournaments():
    ids_df = query_df("SELECT id FROM tournaments ORDER BY created_at DESC")
    ids = ids_df["id"].tolist() if not ids_df.empty else []
    to_delete = ids[3:]
    if not to_delete:
        return

    conn = get_conn()
    with conn.cursor() as cur:
        for tournament_id in to_delete:
            cur.execute("DELETE FROM tournaments WHERE id = %s", (int(tournament_id),))
        conn.commit()


def get_tournaments_df() -> pd.DataFrame:
    return query_df(
        """
        SELECT
            id,
            name,
            tournament_type,
            top_n,
            TO_CHAR(start_at, 'DD-MM-YYYY HH24:MI') AS start_at,
            TO_CHAR(end_at, 'DD-MM-YYYY HH24:MI') AS end_at,
            period_hours,
            ROUND(min_weight::numeric, 3) AS min_weight,
            is_active,
            TO_CHAR(created_at, 'DD-MM-YYYY HH24:MI:SS') AS created_at
        FROM tournaments
        ORDER BY created_at DESC
        """
    )


def get_active_tournament_id():
    df = query_df(
        "SELECT id FROM tournaments WHERE is_active = TRUE ORDER BY created_at DESC LIMIT 1"
    )
    if df.empty:
        return None
    return int(df.iloc[0]["id"])


def set_active_tournament(tournament_id: int):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("UPDATE tournaments SET is_active = FALSE")
        cur.execute("UPDATE tournaments SET is_active = TRUE WHERE id = %s", (int(tournament_id),))
        conn.commit()


def get_tournament_meta(tournament_id: int):
    df = query_df(
        """
        SELECT
            id,
            name,
            tournament_type,
            top_n,
            TO_CHAR(start_at, 'DD-MM-YYYY HH24:MI') AS start_at,
            TO_CHAR(end_at, 'DD-MM-YYYY HH24:MI') AS end_at,
            period_hours,
            ROUND(min_weight::numeric, 3) AS min_weight,
            is_active,
            TO_CHAR(created_at, 'DD-MM-YYYY HH24:MI:SS') AS created_at
        FROM tournaments
        WHERE id = %s
        """,
        (int(tournament_id),),
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def add_team(tournament_id: int, name: str, sector: str, zone: str):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO teams (tournament_id, name, sector, zone)
            VALUES (%s, %s, %s, %s)
            """,
            (int(tournament_id), name.strip(), str(sector).strip(), zone.strip().upper()),
        )
        conn.commit()


def update_team(team_id: int, name: str, sector: str, zone: str):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE teams
            SET
                name = %s,
                sector = %s,
                zone = %s
            WHERE id = %s
            """,
            (name.strip(), str(sector).strip(), zone.strip().upper(), int(team_id)),
        )
        conn.commit()


def get_period_number(caught_at: str, start_at: str, end_at: str, period_hours: int):
    caught_dt = parse_datetime(caught_at)
    start_dt = parse_datetime(start_at)
    end_dt = parse_datetime(end_at)
    if caught_dt < start_dt or caught_dt > end_dt:
        return None
    delta_hours = (caught_dt - start_dt).total_seconds() / 3600
    return int(delta_hours // period_hours) + 1


def get_period_count(start_at: str, end_at: str, period_hours: int):
    start_dt = parse_datetime(start_at)
    end_dt = parse_datetime(end_at)
    total_hours = max(0, (end_dt - start_dt).total_seconds() / 3600)
    return max(1, int((total_hours + period_hours - 1) // period_hours))


def add_catch(
    tournament_id: int,
    team_id: int,
    caught_at: str,
    period: int,
    fish_type: str,
    weight: float,
):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO catches (tournament_id, team_id, caught_at, period, fish_type, weight)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (int(tournament_id), int(team_id), caught_at, int(period), fish_type, float(weight)),
        )
        conn.commit()


def update_catch(
    catch_id: int,
    caught_at: str,
    period: int,
    fish_type: str,
    weight: float,
):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE catches
            SET
                caught_at = %s,
                period = %s,
                fish_type = %s,
                weight = %s
            WHERE id = %s
            """,
            (caught_at, int(period), fish_type, float(weight), int(catch_id)),
        )
        conn.commit()


def delete_catch(catch_id: int):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM catches WHERE id = %s", (int(catch_id),))
        conn.commit()


def get_teams_df(tournament_id: int) -> pd.DataFrame:
    return query_df(
        """
        SELECT id, name, sector, zone
        FROM teams
        WHERE tournament_id = %s
        ORDER BY
            CASE WHEN sector ~ '^[0-9]+$' THEN sector::INTEGER ELSE 999999 END,
            sector,
            name
        """,
        (int(tournament_id),),
    )


def get_catches_df(tournament_id: int) -> pd.DataFrame:
    return query_df(
        """
        SELECT
            c.id,
            c.tournament_id,
            t.name AS team,
            t.sector,
            t.zone,
            TO_CHAR(c.caught_at, 'DD-MM-YYYY HH24:MI') AS caught_at,
            c.period,
            c.fish_type,
            ROUND(c.weight::numeric, 3) AS weight
        FROM catches c
        JOIN teams t ON t.id = c.team_id
        WHERE c.tournament_id = %s
        ORDER BY c.caught_at DESC, c.id DESC
        """,
        (int(tournament_id),),
    )


def get_big_fish_row(catches_df: pd.DataFrame):
    if catches_df.empty:
        return None
    ranked = catches_df.copy()
    ranked["_caught_at_dt"] = ranked["caught_at"].map(parse_datetime)
    return ranked.sort_values(["weight", "_caught_at_dt"], ascending=[False, True]).iloc[0]


def add_places(df: pd.DataFrame, score_col: str, place_col: str):
    df = df.sort_values([score_col, "Команда"], ascending=[False, True]).reset_index(drop=True)
    df[place_col] = range(1, len(df) + 1)
    return df


def build_results(tournament_id: int):
    meta = get_tournament_meta(tournament_id)
    top_n_value = int(meta.get("top_n", 5)) if meta else 5
    min_weight_value = float(meta.get("min_weight", 0)) if meta else 0

    teams = query_df(
        """
        SELECT id, name, sector, zone
        FROM teams
        WHERE tournament_id = %s
        ORDER BY
            CASE WHEN sector ~ '^[0-9]+$' THEN sector::INTEGER ELSE 999999 END,
            sector,
            name
        """,
        (int(tournament_id),),
    )
    catches = query_df(
        """
        SELECT id, team_id, weight, fish_type, TO_CHAR(caught_at, 'DD-MM-YYYY HH24:MI') AS caught_at, period
        FROM catches
        WHERE tournament_id = %s
        """,
        (int(tournament_id),),
    )

    fish_columns = [f"{i} риба" for i in range(1, top_n_value + 1)]
    empty_top5 = pd.DataFrame(
        columns=["Сектор", "Команда", "Зона", *fish_columns, f"Заг. вага по {top_n_value}"]
    )
    empty_total = pd.DataFrame(columns=["Сектор", "Команда", "Зона", "Загальна вага"])
    empty_combo = pd.DataFrame(
        columns=["Сектор", "Команда", "Зона", f"Місце {top_n_value} риб", "Місце загальна", "Сума балів"]
    )

    if teams.empty:
        return empty_top5, empty_total, empty_combo

    if min_weight_value > 0:
        catches = catches[catches["weight"].astype(float) >= min_weight_value].copy()

    big_fish_row = get_big_fish_row(catches)
    catches_wo_big_fish = catches.copy()
    if big_fish_row is not None:
        catches_wo_big_fish = catches_wo_big_fish[catches_wo_big_fish["id"] != big_fish_row["id"]].copy()

    top_n_rows = []
    total_rows = []
    for _, team in teams.iterrows():
        team_catches = catches_wo_big_fish[catches_wo_big_fish["team_id"] == team["id"]].copy()
        weights = sorted(team_catches["weight"].astype(float).tolist(), reverse=True)
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
        catches = catches[catches["weight"].astype(float) >= min_weight_value].copy()

    if catches.empty:
        return pd.DataFrame(columns=["Період", "Зона", "Команда", "Найбільша риба"])

    winners = []
    grouped = catches.groupby(["period", "zone"], dropna=False)
    for (period, zone), group in grouped:
        ranked = group.copy()
        ranked["_caught_at_dt"] = ranked["caught_at"].map(parse_datetime)
        winner = ranked.sort_values(["weight", "_caught_at_dt", "team"], ascending=[False, True, True]).iloc[0]
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


def build_live_scoreboard(tournament_id: int):
    meta = get_tournament_meta(tournament_id)
    top_n_df, total_df, combo_df = build_results(tournament_id)
    top_n_value = int(meta.get("top_n", 5)) if meta else 5
    score_col = f"Заг. вага по {top_n_value}"
    zone_source = total_df if meta and meta["tournament_type"] == "combo" else top_n_df
    zone_col = "Загальна вага" if meta and meta["tournament_type"] == "combo" else score_col
    zone_df = build_zone_winners(zone_source, zone_col)

    if meta and meta["tournament_type"] == "combo":
        winner_teams = zone_df["Команда"].tolist() if not zone_df.empty else []
        board_df = combo_df[combo_df["Команда"].isin(winner_teams)].copy() if winner_teams else combo_df.head(0).copy()
        if not board_df.empty:
            board_df = board_df.merge(total_df[["Команда", "Загальна вага"]], on="Команда", how="left")
            board_df = board_df.merge(top_n_df[["Команда", score_col]], on="Команда", how="left")
            board_df = board_df.sort_values(
                ["Сума балів", f"Місце {top_n_value} риб", "Місце загальна", "Команда"],
                ascending=[True, True, True, True],
            ).reset_index(drop=True)
        return board_df.head(3), zone_df, top_n_value

    winner_teams = zone_df["Команда"].tolist() if not zone_df.empty else []
    board_df = top_n_df[top_n_df["Команда"].isin(winner_teams)].copy() if winner_teams else top_n_df.head(0).copy()
    if not board_df.empty:
        board_df = board_df.sort_values([score_col, "1 риба", "Команда"], ascending=[False, False, True]).reset_index(drop=True)
    return board_df.head(3), zone_df, top_n_value


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


def build_podium(
    top_n_df: pd.DataFrame,
    total_df: pd.DataFrame,
    combo_df: pd.DataFrame,
    tournament_type: str,
    top_n_value: int,
) -> pd.DataFrame:
    zone_source = total_df if tournament_type == "combo" else top_n_df
    zone_col = "Загальна вага" if tournament_type == "combo" else f"Заг. вага по {top_n_value}"
    zone_winners_df = build_zone_winners(zone_source, zone_col)
    winner_teams = zone_winners_df["Команда"].tolist() if not zone_winners_df.empty else []

    if tournament_type == "combo":
        if combo_df.empty or not winner_teams:
            return pd.DataFrame(columns=["Місце", "Команда", "Бали"])
        top_n_place_col = f"Місце {top_n_value} риб"
        podium = combo_df[combo_df["Команда"].isin(winner_teams)].copy()
        podium = podium.sort_values(
            ["Сума балів", top_n_place_col, "Місце загальна", "Команда"],
            ascending=[True, True, True, True],
        ).head(3).reset_index(drop=True)
        podium.insert(0, "Місце", ["1 місце", "2 місце", "3 місце"][: len(podium)])
        return podium[["Місце", "Команда", "Сума балів"]].rename(columns={"Сума балів": "Бали"})

    score_col = f"Заг. вага по {top_n_value}"
    first_fish_col = "1 риба"
    if top_n_df.empty or not winner_teams:
        return pd.DataFrame(columns=["Місце", "Команда", "Вага"])
    podium = top_n_df[top_n_df["Команда"].isin(winner_teams)].copy()
    podium = podium.sort_values([score_col, first_fish_col, "Команда"], ascending=[False, False, True]).head(3)
    podium = podium.reset_index(drop=True)
    podium.insert(0, "Місце", ["1 місце", "2 місце", "3 місце"][: len(podium)])
    return podium[["Місце", "Команда", score_col]].rename(columns={score_col: "Вага"})


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

    teams = get_teams_df(tournament_id)
    team_map = {row["name"]: int(row["id"]) for _, row in teams.iterrows()}
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
