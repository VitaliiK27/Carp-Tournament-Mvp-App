import io

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas

from db import TOURNAMENT_TYPES, build_big_fish, build_period_zone_winners, build_podium, build_results, build_zone_winners, get_tournament_meta

PDF_FONT_NAME = "DejaVuSans"
PDF_FONT_BOLD_NAME = "DejaVuSans-Bold"
PDF_FONT_REGULAR_PATH = "fonts/DejaVuSans.ttf"
PDF_FONT_BOLD_PATH = "fonts/DejaVuSans-Bold.ttf"


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


def _draw_landscape_results_table(pdf: canvas.Canvas, title: str, df: pd.DataFrame, page_width: float, page_height: float):
    left = 10 * mm
    top = page_height - 15 * mm
    usable = page_width - 20 * mm
    row_h = 6.5 * mm
    header_font_size = 7
    body_font_size = 7

    pdf.setFont(PDF_FONT_BOLD_NAME, 13)
    pdf.drawString(left, top, title)
    y = top - 8 * mm

    if df.empty:
        pdf.setFont(PDF_FONT_NAME, 9)
        pdf.drawString(left, y, "Немає даних")
        return

    shown = list(df.columns)
    table_df = df[shown].copy()
    col_w = usable / len(shown)

    def draw_header(current_y: float) -> float:
        pdf.setFillColor(colors.HexColor("#EAEAEA"))
        pdf.rect(left, current_y - row_h, usable, row_h, fill=1, stroke=0)
        pdf.setFillColor(colors.black)
        pdf.setFont(PDF_FONT_BOLD_NAME, header_font_size)
        for idx, col in enumerate(shown):
            pdf.drawString(
                left + idx * col_w + 2,
                current_y - 4.5 * mm,
                _fit_text(col, PDF_FONT_BOLD_NAME, header_font_size, col_w - 4),
            )
        return current_y - row_h

    y = draw_header(y)
    pdf.setFont(PDF_FONT_NAME, body_font_size)
    for _, row in table_df.iterrows():
        if y < 15 * mm:
            pdf.showPage()
            pdf.setPageSize((page_width, page_height))
            pdf.setFont(PDF_FONT_BOLD_NAME, 13)
            pdf.drawString(left, top, title)
            y = draw_header(top - 8 * mm)
            pdf.setFont(PDF_FONT_NAME, body_font_size)

        for idx, col in enumerate(shown):
            value = row[col]
            text = _fit_text(value, PDF_FONT_NAME, body_font_size, col_w - 4)
            pdf.drawString(left + idx * col_w + 2, y - 4.5 * mm, str(text))
        pdf.setStrokeColor(colors.HexColor("#DDDDDD"))
        pdf.line(left, y - row_h, left + usable, y - row_h)
        y -= row_h


def build_results_pdf(tournament_id: int) -> bytes:
    register_pdf_fonts()
    meta = get_tournament_meta(tournament_id)
    top_n_df, total_df, combo_df = build_results(tournament_id)
    top_n_value = int(meta.get("top_n", 5))
    big_fish_df = build_big_fish(tournament_id)
    period_zone_df = build_period_zone_winners(tournament_id)
    zone_source = total_df if meta["tournament_type"] == "combo" else top_n_df
    zone_col = "Загальна вага" if meta["tournament_type"] == "combo" else f"Заг. вага по {top_n_value}"
    zone_df = build_zone_winners(zone_source, zone_col)
    podium_df = build_podium(top_n_df, total_df, combo_df, meta["tournament_type"], top_n_value)

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
    y = _draw_simple_table(pdf, "Подіум", podium_df, y, page_width, page_height)
    y = _draw_simple_table(pdf, "Big Fish", big_fish_df, y, page_width, page_height)

    landscape_width, landscape_height = landscape(A4)
    pdf.showPage()
    pdf.setPageSize((landscape_width, landscape_height))
    _draw_landscape_results_table(pdf, f"Таблиця: {top_n_value} крупних риб", top_n_df, landscape_width, landscape_height)

    pdf.showPage()
    pdf.setPageSize(A4)
    page_width, page_height = A4
    y = page_height - 20 * mm
    if meta["tournament_type"] == "combo":
        y = _draw_simple_table(pdf, "Таблиця: загальна вага", total_df, y, page_width, page_height)
        y = _draw_simple_table(pdf, "Залік по сумі місць", combo_df, y, page_width, page_height)
    y = _draw_simple_table(pdf, "Переможці зон", zone_df, y, page_width, page_height)
    _draw_simple_table(pdf, "Окрема номінація: найбільша риба періоду в зоні", period_zone_df, y, page_width, page_height)

    pdf.save()
    buffer.seek(0)
    return buffer.getvalue()
