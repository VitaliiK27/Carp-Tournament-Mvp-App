import io
import os
from contextlib import closing
from datetime import datetime

import pandas as pd
import psycopg
import streamlit as st
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas

FISH_TYPES = ["Короп", "Амур", "Інше"]
TOURNAMENT_TYPES = {
    "topN": "N крупних риб",
    "combo": "N крупних + загальна вага",
}
MAX_TOP_N = 15
PERIOD_OPTIONS = [6, 12, 18, 24]
ZONE_OPTIONS = ["A", "B", "C", "D"]
PDF_FONT_NAME = "DejaVuSans"
PDF_FONT_BOLD_NAME = "DejaVuSans-Bold"
PDF_FONT_REGULAR_PATH = "fonts/DejaVuSans.ttf"
PDF_FONT_BOLD_PATH = "fonts/DejaVuSans-Bold.ttf"


# ---------- DB ----------
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


def prune_old_tournaments():
    ids_df = query_df(
        "SELECT id FROM tournaments ORDER BY created_at DESC"
    )
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
            TO_CHAR(start_at, 'YYYY-MM-DD HH24:MI') AS start_at,
            TO_CHAR(end_at, 'YYYY-MM-DD HH24:MI') AS end_at,
            period_hours,
            ROUND(min_weight::numeric, 3) AS min_weight,
            is_active,
            TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') AS created_at
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
            TO_CHAR(start_at, 'YYYY-MM-DD HH24:MI') AS start_at,
            TO_CHAR(end_at, 'YYYY-MM-DD HH24:MI') AS end_at,
            period_hours,
            ROUND(min_weight::numeric, 3) AS min_weight,
            is_active,
            TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') AS created_at
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


def get_period_number(caught_at: str, start_at: str, end_at: str, period_hours: int):
    caught_dt = datetime.strptime(caught_at, "%Y-%m-%d %H:%M")
    start_dt = datetime.strptime(start_at, "%Y-%m-%d %H:%M")
    end_dt = datetime.strptime(end_at, "%Y-%m-%d %H:%M")
    if caught_dt < start_dt or caught_dt > end_dt:
        return None
    delta_hours = (caught_dt - start_dt).total_seconds() / 3600
    return int(delta_hours // period_hours) + 1


def get_period_count(start_at: str, end_at: str, period_hours: int):
    start_dt = datetime.strptime(start_at, "%Y-%m-%d %H:%M")
    end_dt = datetime.strptime(end_at, "%Y-%m-%d %H:%M")
    total_hours = max(0, (end_dt - start_dt).total_seconds() / 3600)
    return max(1, int((total_hours + period_hours - 1) // period_hours))


def add_catch(tournament_id: int, team_id: int, caught_at: str, period: int, fish_type: str, weight: float):
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
            TO_CHAR(c.caught_at, 'YYYY-MM-DD HH24:MI') AS caught_at,
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
    return catches_df.sort_values(["weight", "caught_at"], ascending=[False, True]).iloc[0]


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
        SELECT id, team_id, weight, fish_type, TO_CHAR(caught_at, 'YYYY-MM-DD HH24:MI') AS caught_at, period
        FROM catches
        WHERE tournament_id = %s
        """,
        (int(tournament_id),),
    )

    fish_columns = [f"{i} риба" for i in range(1, top_n_value + 1)]
    empty_top5 = pd.DataFrame(columns=["Сектор", "Команда", "Зона", *fish_columns, f"Заг. вага по {top_n_value}"])
    empty_total = pd.DataFrame(columns=["Сектор", "Команда", "Зона", "Загальна вага"])
    empty_combo = pd.DataFrame(columns=["Сектор", "Команда", "Зона", f"Місце {top_n_value} риб", "Місце загальна", "Сума балів"])

    if teams.empty:
        return empty_top5, empty_total, empty_combo

    if min_weight_value > 0:
        catches = catches[catches["weight"].astype(float) >= min_weight_value].copy()

    big_fish_row = get_big_fish_row(catches)
    catches_wo_big_fish = catches.copy()
    if big_fish_row is not None:
        catches_wo_big_fish = catches_wo_big_fish[catches_wo_big_fish["id"] != big_fish_row["id"]].copy()

    topN_rows = []
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
        topN_rows.append(row)

        total_rows.append(
            {
                "Сектор": team["sector"],
                "Команда": team["name"],
                "Зона": team["zone"],
                "Загальна вага": round(sum(weights), 3),
            }
        )

    topN_df = pd.DataFrame(topN_rows)
    total_df = pd.DataFrame(total_rows)
    top_n_score_col = f"Заг. вага по {top_n_value}"
    top_n_place_col = f"Місце {top_n_value} риб"

    if meta and meta["tournament_type"] == "combo":
        topN_places = add_places(topN_df[["Сектор", "Команда", "Зона", top_n_score_col]].copy(), top_n_score_col, top_n_place_col)
        total_places = add_places(total_df[["Сектор", "Команда", "Зона", "Загальна вага"]].copy(), "Загальна вага", "Місце загальна")
        combo_df = topN_places[["Сектор", "Команда", "Зона", top_n_place_col]].merge(
            total_places[["Команда", "Місце загальна"]],
            on="Команда",
            how="left",
        )
        combo_df["Сума балів"] = combo_df[top_n_place_col] + combo_df["Місце загальна"]
        combo_df = combo_df.sort_values(["Сума балів", top_n_place_col, "Місце загальна", "Команда"], ascending=[True, True, True, True]).reset_index(drop=True)
    else:
        combo_df = empty_combo

    return topN_df, total_df, combo_df


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
    topN_df, total_df, combo_df = build_results(tournament_id)
    top_n_value = int(meta.get("top_n", 5)) if meta else 5
    score_col = f"Заг. вага по {top_n_value}"

    if meta and meta["tournament_type"] == "combo":
        board_df = combo_df.copy()
        if not board_df.empty:
            board_df = board_df.merge(total_df[["Команда", "Загальна вага"]], on="Команда", how="left")
            board_df = board_df.merge(topN_df[["Команда", score_col]], on="Команда", how="left")
        return board_df.head(10), top_n_value

    board_df = topN_df.sort_values([score_col, "1 риба", "Команда"], ascending=[False, False, True]).copy()
    return board_df.head(10), top_n_value


def build_big_fish(tournament_id: int) -> pd.DataFrame:
    catches = get_catches_df(tournament_id)
    if catches.empty:
        return pd.DataFrame(columns=["Команда", "Вага", "Час", "Вид"])

    max_row = get_big_fish_row(catches)
    return pd.DataFrame([
        {
            "Команда": max_row["team"],
            "Вага": max_row["weight"],
            "Час": max_row["caught_at"],
            "Вид": max_row["fish_type"],
        }
    ])


def build_podium(topN_df: pd.DataFrame, combo_df: pd.DataFrame, tournament_type: str, top_n_value: int) -> pd.DataFrame:
    if tournament_type == "combo":
        if combo_df.empty:
            return pd.DataFrame(columns=["Місце", "Команда", "Бали"])
        podium = combo_df.head(3).copy().reset_index(drop=True)
        podium.insert(0, "Місце", ["1 місце", "2 місце", "3 місце"][: len(podium)])
        return podium[["Місце", "Команда", "Сума балів"]].rename(columns={"Сума балів": "Бали"})

    score_col = f"Заг. вага по {top_n_value}"
    first_fish_col = "1 риба"
    if topN_df.empty:
        return pd.DataFrame(columns=["Місце", "Команда", "Вага"])
    podium = topN_df.sort_values([score_col, first_fish_col, "Команда"], ascending=[False, False, True]).head(3).copy().reset_index(drop=True)
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


def register_pdf_fonts():
    registered = pdfmetrics.getRegisteredFontNames()
    if PDF_FONT_NAME not in registered:
        pdfmetrics.registerFont(TTFont(PDF_FONT_NAME, PDF_FONT_REGULAR_PATH))
    if PDF_FONT_BOLD_NAME not in registered:
        pdfmetrics.registerFont(TTFont(PDF_FONT_BOLD_NAME, PDF_FONT_BOLD_PATH))


def _fit_text(text: str, font_name: str, font_size: int, max_width: float) -> str:
    original = str(text)
    fitted = original
    while fitted and stringWidth(fitted, font_name, font_size) > max_width:
        fitted = fitted[:-1]
    if fitted != original:
        fitted = fitted.rstrip()
        return (fitted[:-3] + "...") if len(fitted) > 3 else "..."
    return fitted


def _draw_simple_table(pdf: canvas.Canvas, title: str, df: pd.DataFrame, y: float, page_width: float, page_height: float):
    left = 15 * mm
    usable = page_width - 30 * mm
    pdf.setFont(PDF_FONT_BOLD_NAME, 11)
    pdf.drawString(left, y, title)
    y -= 7 * mm

    if df.empty:
        pdf.setFont(PDF_FONT_NAME, 9)
        pdf.drawString(left, y, "Немає даних")
        return y - 10 * mm

    max_cols = min(len(df.columns), 6)
    shown = list(df.columns[:max_cols])
    table_df = df[shown].copy().head(12)
    col_w = usable / max_cols
    row_h = 7 * mm

    pdf.setFillColor(colors.HexColor("#EAEAEA"))
    pdf.rect(left, y - row_h, usable, row_h, fill=1, stroke=0)
    pdf.setFillColor(colors.black)
    pdf.setFont(PDF_FONT_BOLD_NAME, 8)
    for idx, col in enumerate(shown):
        pdf.drawString(left + idx * col_w + 2, y - 5 * mm, _fit_text(col, PDF_FONT_BOLD_NAME, 8, col_w - 4))
    y -= row_h

    pdf.setFont(PDF_FONT_NAME, 8)
    for _, row in table_df.iterrows():
        if y < 20 * mm:
            pdf.showPage()
            pdf.setFont(PDF_FONT_NAME, 8)
            y = page_height - 20 * mm
        for idx, col in enumerate(shown):
            value = row[col]
            text = _fit_text(value, PDF_FONT_NAME, 8, col_w - 4)
            pdf.drawString(left + idx * col_w + 2, y - 5 * mm, str(text))
        pdf.setStrokeColor(colors.HexColor("#DDDDDD"))
        pdf.line(left, y - row_h, left + usable, y - row_h)
        y -= row_h

    return y - 5 * mm


def build_results_pdf(tournament_id: int) -> bytes:
    register_pdf_fonts()
    meta = get_tournament_meta(tournament_id)
    topN_df, total_df, combo_df = build_results(tournament_id)
    top_n_value = int(meta.get("top_n", 5))
    big_fish_df = build_big_fish(tournament_id)
    period_zone_df = build_period_zone_winners(tournament_id)
    zone_source = total_df if meta["tournament_type"] == "combo" else topN_df
    zone_col = "Загальна вага" if meta["tournament_type"] == "combo" else f"Заг. вага по {top_n_value}"
    zone_df = build_zone_winners(zone_source, zone_col)
    podium_df = build_podium(topN_df, combo_df, meta["tournament_type"], top_n_value)

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    page_width, page_height = A4

    pdf.setTitle(f"Результати - {meta['name']}")
    pdf.setFont(PDF_FONT_BOLD_NAME, 16)
    pdf.drawString(15 * mm, page_height - 20 * mm, meta["name"])
    pdf.setFont(PDF_FONT_NAME, 10)
    pdf.drawString(15 * mm, page_height - 28 * mm, f"Тип: {TOURNAMENT_TYPES.get(meta['tournament_type'], meta['tournament_type'])}")
    pdf.drawString(15 * mm, page_height - 34 * mm, f"Період турніру: {meta['start_at']} - {meta['end_at']}")
    pdf.drawString(15 * mm, page_height - 40 * mm, f"Крупних риб: {top_n_value} | Період: {meta['period_hours']} год | Мін. вага: {meta['min_weight']} кг")

    y = page_height - 52 * mm
    y = _draw_simple_table(pdf, f"Таблиця: {top_n_value} крупних риб", topN_df, y, page_width, page_height)
    if meta["tournament_type"] == "combo":
        y = _draw_simple_table(pdf, "Таблиця: загальна вага", total_df, y, page_width, page_height)
        y = _draw_simple_table(pdf, "Залік по сумі місць", combo_df, y, page_width, page_height)
    y = _draw_simple_table(pdf, "Подіум", podium_df, y, page_width, page_height)
    y = _draw_simple_table(pdf, "Big Fish", big_fish_df, y, page_width, page_height)

    pdf.showPage()
    y = page_height - 20 * mm
    y = _draw_simple_table(pdf, "Переможці зон", zone_df, y, page_width, page_height)
    _draw_simple_table(pdf, "Окрема номінація: найбільша риба періоду в зоні", period_zone_df, y, page_width, page_height)

    pdf.save()
    buffer.seek(0)
    return buffer.getvalue()


# ---------- UI ----------
st.set_page_config(page_title="Карпові змагання", page_icon="🎣", layout="wide")
init_db()

query_params = st.query_params
scoreboard_mode = str(query_params.get("mode", "")).lower() in ["tablo", "scoreboard", "tv"]
if scoreboard_mode:
    st.markdown(
        """
        <style>
        section[data-testid="stSidebar"] {display:none !important;}
        .block-container {padding-top:0.6rem; max-width: 980px;}
        </style>
        """,
        unsafe_allow_html=True,
    )

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
st.caption("Supabase Postgres версія. Додай DATABASE_URL у secrets для роботи в Streamlit Cloud.")

if scoreboard_mode:
    tournaments_df = get_tournaments_df()
    page = "Мобільне табло"
    selected_tournament_id = get_active_tournament_id()
else:
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
        else:
            selected_tournament_id = None

        st.markdown("---")
        page = st.radio(
            "Розділ",
            ["Турніри", "Команди", "Швидке зважування", "Додати рибу", "Результати", "Мобільне табло", "Підсумки", "Журнал зважувань"],
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

    st.markdown("### Останні 3 змагання")
    tournaments_df = get_tournaments_df().copy()
    if tournaments_df.empty:
        st.info("Ще немає жодного змагання")
    else:
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
        st.dataframe(tournaments_df[["Назва", "Тип", "Крупних риб", "Старт", "Фініш", "Період (год)", "Мін. вага", "Активний", "Створено"]], use_container_width=True)

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
                    except Exception as e:
                        st.error(f"Не вдалося додати команду: {e}")

    teams_df = get_teams_df(active_tournament_id)
    st.markdown("### Список команд")
    st.dataframe(teams_df.drop(columns=["id"]) if not teams_df.empty else teams_df, use_container_width=True, height=360)

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
    topN_df, total_df, combo_df = build_results(active_tournament_id)
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
        if topN_df.empty:
            st.info("Поки що немає даних")
        else:
            st.dataframe(topN_df, use_container_width=True, height=420)

    if active_meta["tournament_type"] == "combo":
        with result_tabs[1]:
            st.dataframe(total_df, use_container_width=True, height=420)
        with result_tabs[2]:
            st.dataframe(combo_df, use_container_width=True, height=420)

elif page == "Мобільне табло":
    st.subheader(f"Мобільне табло — {active_meta['name']}")
    if scoreboard_mode:
        st.caption("Режим табло активний. Для звичайного інтерфейсу відкрий додаток без ?mode=tablo")
        st.button("Оновити табло", use_container_width=True)
    else:
        current_url = st.query_params.to_dict() if hasattr(st.query_params, 'to_dict') else {}
        st.caption("Окремий режим табло: відкрий додаток з параметром ?mode=tablo")
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
    topN_df, total_df, combo_df = build_results(active_tournament_id)
    top_n_value = int(active_meta.get("top_n", 5))

    zone_source = total_df if active_meta["tournament_type"] == "combo" else topN_df
    zone_col = "Загальна вага" if active_meta["tournament_type"] == "combo" else f"Заг. вага по {top_n_value}"
    zone_df = build_zone_winners(zone_source, zone_col)
    big_fish_df = build_big_fish(active_tournament_id)
    podium_df = build_podium(topN_df, combo_df, active_meta["tournament_type"], top_n_value)

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
        with st.expander("🗑️ Видалити запис"):
            catch_id = st.selectbox("ID запису", catches_df["id"].tolist())
            if st.button("Видалити вибране зважування"):
                delete_catch(int(catch_id))
                st.success("Запис видалено")
                st.rerun()
