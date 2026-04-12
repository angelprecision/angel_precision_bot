# ap/weekly_report.py — Angel Precision Weekly Performance PDF Report
# =============================================================================
# Generates a professional dark-themed PDF report per client per week.
# Uses ReportLab with DM Sans font (downloaded from Google Fonts at runtime).
# All DB queries use ap.db conn() + run_with_retry with %s placeholders.
# Python 3.13 compatible — ast.parse() clean.
# =============================================================================

from __future__ import annotations

import logging
import os
import re
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from reportlab.lib import colors
from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
)

log = logging.getLogger("ap.weekly_report")

# ── Theme colours ─────────────────────────────────────────────────────────────
BG_COLOR = HexColor("#0a0a0f")
SURFACE_COLOR = HexColor("#1a1a26")
ACCENT_TEAL = HexColor("#01696F")
TEXT_COLOR = HexColor("#e8e8f0")
TEXT_MUTED = HexColor("#9898a8")
GREEN = HexColor("#00e676")
RED = HexColor("#ff5252")
AMBER = HexColor("#ffb300")

# ── Font setup ────────────────────────────────────────────────────────────────
FONT_DIR = Path("/tmp/fonts")
FONT_URL = (
    "https://github.com/google/fonts/raw/main/ofl/dmsans/"
    "DMSans%5Bopsz%2Cwght%5D.ttf"
)
_FONT_REGISTERED = False
FONT_NAME = "Helvetica"
FONT_NAME_BOLD = "Helvetica-Bold"


def _ensure_font() -> None:
    """Download DM Sans from Google Fonts and register it. Fall back to Helvetica."""
    global _FONT_REGISTERED, FONT_NAME, FONT_NAME_BOLD
    if _FONT_REGISTERED:
        return
    _FONT_REGISTERED = True
    try:
        FONT_DIR.mkdir(parents=True, exist_ok=True)
        font_path = FONT_DIR / "DMSans.ttf"
        if not font_path.exists():
            log.info("Downloading DM Sans from Google Fonts …")
            urllib.request.urlretrieve(FONT_URL, str(font_path))
        pdfmetrics.registerFont(TTFont("DMSans", str(font_path)))
        FONT_NAME = "DMSans"
        # Variable font — same file for bold (ReportLab renders at normal weight)
        FONT_NAME_BOLD = "DMSans"
        log.info("DM Sans font registered successfully")
    except Exception as exc:
        log.warning("Could not load DM Sans, falling back to Helvetica: %s", exc)
        FONT_NAME = "Helvetica"
        FONT_NAME_BOLD = "Helvetica-Bold"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sanitize(text: str) -> str:
    """Sanitize a string for use in filenames."""
    return re.sub(r"[^a-zA-Z0-9_\-]", "_", text)


def _fmt_pnl(value: float | None) -> str:
    if value is None:
        return "$0.00"
    return f"${value:+,.2f}" if value != 0 else "$0.00"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "0.0%"
    return f"{value:+.1f}%"


def _pnl_color(value: float) -> HexColor:
    if value > 0:
        return GREEN
    if value < 0:
        return RED
    return TEXT_COLOR


def _safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b else default


def _get_spy_return(week_start: str, week_end: str) -> float | None:
    """Fetch SPY weekly return via yfinance. Returns percentage or None."""
    try:
        import yfinance as yf

        spy = yf.Ticker("SPY")
        hist = spy.history(start=week_start, end=week_end)
        if hist.empty or len(hist) < 2:
            return None
        return (hist["Close"].iloc[-1] / hist["Close"].iloc[0] - 1) * 100
    except Exception as exc:
        log.warning("Failed to fetch SPY data: %s", exc)
        return None


def _max_consecutive_losses(trades: list[dict]) -> int:
    streak = max_streak = 0
    for t in trades:
        pnl = float(t.get("realized_pnl") or 0)
        if pnl < 0:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    return max_streak


def _hold_time_str(minutes: float | None) -> str:
    if minutes is None:
        return "—"
    if minutes < 60:
        return f"{minutes:.0f}m"
    hours = minutes / 60
    if hours < 24:
        return f"{hours:.1f}h"
    return f"{hours / 24:.1f}d"


# ── Styles ────────────────────────────────────────────────────────────────────

def _build_styles() -> dict[str, ParagraphStyle]:
    """Build paragraph styles for the dark-themed PDF."""
    _ensure_font()
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "APTitle",
            parent=base["Title"],
            fontName=FONT_NAME_BOLD,
            fontSize=28,
            leading=34,
            textColor=TEXT_COLOR,
            spaceAfter=4,
        ),
        "subtitle": ParagraphStyle(
            "APSubtitle",
            parent=base["Normal"],
            fontName=FONT_NAME,
            fontSize=12,
            leading=16,
            textColor=TEXT_MUTED,
            spaceAfter=20,
        ),
        "heading": ParagraphStyle(
            "APHeading",
            parent=base["Heading1"],
            fontName=FONT_NAME_BOLD,
            fontSize=16,
            leading=22,
            textColor=ACCENT_TEAL,
            spaceBefore=18,
            spaceAfter=10,
        ),
        "body": ParagraphStyle(
            "APBody",
            parent=base["Normal"],
            fontName=FONT_NAME,
            fontSize=10,
            leading=14,
            textColor=TEXT_COLOR,
            spaceAfter=6,
        ),
        "body_muted": ParagraphStyle(
            "APBodyMuted",
            parent=base["Normal"],
            fontName=FONT_NAME,
            fontSize=9,
            leading=13,
            textColor=TEXT_MUTED,
            spaceAfter=4,
        ),
        "kpi_value": ParagraphStyle(
            "APKPIValue",
            parent=base["Normal"],
            fontName=FONT_NAME_BOLD,
            fontSize=22,
            leading=26,
            textColor=TEXT_COLOR,
            alignment=1,  # center
        ),
        "kpi_label": ParagraphStyle(
            "APKPILabel",
            parent=base["Normal"],
            fontName=FONT_NAME,
            fontSize=9,
            leading=12,
            textColor=TEXT_MUTED,
            alignment=1,
        ),
        "footer": ParagraphStyle(
            "APFooter",
            parent=base["Normal"],
            fontName=FONT_NAME,
            fontSize=7,
            leading=10,
            textColor=TEXT_MUTED,
        ),
    }


# ── PDF sections ──────────────────────────────────────────────────────────────

def _build_cover(
    story: list,
    styles: dict,
    client_name: str,
    week_start: str,
    week_end: str,
    total_pnl: float,
) -> None:
    """Section 1: Cover page."""
    story.append(Spacer(1, 1.8 * inch))
    story.append(
        Paragraph("ANGEL PRECISION", styles["subtitle"])
    )
    story.append(
        Paragraph("Weekly Performance Report", styles["title"])
    )
    story.append(Spacer(1, 0.3 * inch))
    story.append(
        Paragraph(f"Client: {client_name}", styles["body"])
    )
    story.append(
        Paragraph(f"Period: {week_start}  —  {week_end}", styles["body"])
    )
    story.append(Spacer(1, 0.5 * inch))
    pnl_color = "#00e676" if total_pnl >= 0 else "#ff5252"
    story.append(
        Paragraph(
            f'<font color="{pnl_color}" size="20">'
            f"Week P&amp;L: {_fmt_pnl(total_pnl)}</font>",
            styles["body"],
        )
    )
    story.append(PageBreak())


def _build_kpi_table(
    story: list,
    styles: dict,
    trades: list[dict],
    total_pnl: float,
) -> None:
    """Section 2: Performance summary KPI cards."""
    story.append(Paragraph("Performance Summary", styles["heading"]))

    wins = [t for t in trades if float(t.get("realized_pnl") or 0) > 0]
    losses = [t for t in trades if float(t.get("realized_pnl") or 0) < 0]
    win_rate = _safe_div(len(wins), len(trades)) * 100
    avg_win = (
        sum(float(t.get("realized_pnl") or 0) for t in wins) / len(wins)
        if wins
        else 0.0
    )
    avg_loss = (
        sum(float(t.get("realized_pnl") or 0) for t in losses) / len(losses)
        if losses
        else 0.0
    )
    gross_wins = sum(float(t.get("realized_pnl") or 0) for t in wins)
    gross_losses = abs(
        sum(float(t.get("realized_pnl") or 0) for t in losses)
    )
    profit_factor = _safe_div(gross_wins, gross_losses)

    # Max drawdown (cumulative PnL peak-to-trough)
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    for t in trades:
        cumulative += float(t.get("realized_pnl") or 0)
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd

    kpi_data = [
        ("Total P&L", _fmt_pnl(total_pnl)),
        ("Win Rate", f"{win_rate:.1f}%"),
        ("Trades", str(len(trades))),
        ("Avg Win", _fmt_pnl(avg_win)),
        ("Avg Loss", _fmt_pnl(avg_loss)),
        ("Profit Factor", f"{profit_factor:.2f}"),
        ("Max Drawdown", _fmt_pnl(max_dd)),
    ]

    # Build a 2-row table: values on top, labels below (4 columns per row)
    val_cells: list[Any] = []
    lbl_cells: list[Any] = []
    for label, value in kpi_data:
        val_cells.append(Paragraph(value, styles["kpi_value"]))
        lbl_cells.append(Paragraph(label, styles["kpi_label"]))

    # Pad to multiple of 4
    while len(val_cells) % 4 != 0:
        val_cells.append(Paragraph("", styles["kpi_value"]))
        lbl_cells.append(Paragraph("", styles["kpi_label"]))

    rows = []
    for i in range(0, len(val_cells), 4):
        rows.append(val_cells[i : i + 4])
        rows.append(lbl_cells[i : i + 4])

    col_w = (letter[0] - 2 * 54) / 4
    kpi_table = Table(rows, colWidths=[col_w] * 4)
    kpi_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), SURFACE_COLOR),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("TOPPADDING", (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("ROUNDEDCORNERS", [6, 6, 6, 6]),
            ]
        )
    )
    story.append(kpi_table)
    story.append(Spacer(1, 0.3 * inch))


def _build_trade_log(
    story: list,
    styles: dict,
    trades: list[dict],
) -> None:
    """Section 3: Trade log table."""
    story.append(Paragraph("Trade Log", styles["heading"]))

    if not trades:
        story.append(
            Paragraph(
                "No completed trades this week.",
                styles["body_muted"],
            )
        )
        return

    header = [
        "Date",
        "Ticker",
        "Side",
        "Tier",
        "Entry",
        "Exit",
        "P&L",
        "Hold",
        "Exit Reason",
    ]
    header_cells = [
        Paragraph(f'<font color="#e8e8f0"><b>{h}</b></font>', styles["body"])
        for h in header
    ]
    data_rows = [header_cells]

    for t in trades:
        entry_ts = t.get("entry_ts") or ""
        date_str = str(entry_ts)[:10] if entry_ts else "—"
        pnl = float(t.get("realized_pnl") or 0)
        pnl_hex = "#00e676" if pnl > 0 else "#ff5252" if pnl < 0 else "#e8e8f0"
        hold_min = t.get("hold_minutes")
        if hold_min is not None:
            hold_min = float(hold_min)

        row = [
            Paragraph(date_str, styles["body_muted"]),
            Paragraph(str(t.get("underlying") or "—"), styles["body"]),
            Paragraph(str(t.get("direction") or "—"), styles["body"]),
            Paragraph(str(t.get("tier") or "—"), styles["body_muted"]),
            Paragraph(f"${float(t.get('avg_fill') or 0):.2f}", styles["body"]),
            Paragraph(f"${float(t.get('exit_price') or 0):.2f}", styles["body"]),
            Paragraph(
                f'<font color="{pnl_hex}">{_fmt_pnl(pnl)}</font>',
                styles["body"],
            ),
            Paragraph(_hold_time_str(hold_min), styles["body_muted"]),
            Paragraph(str(t.get("exit_reason") or "—"), styles["body_muted"]),
        ]
        data_rows.append(row)

    col_widths = [62, 48, 42, 30, 50, 50, 62, 36, 84]
    table = Table(data_rows, colWidths=col_widths, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                # Header
                ("BACKGROUND", (0, 0), (-1, 0), ACCENT_TEAL),
                ("TEXTCOLOR", (0, 0), (-1, 0), TEXT_COLOR),
                # Body rows alternate
                (
                    "ROWBACKGROUNDS",
                    (0, 1),
                    (-1, -1),
                    [SURFACE_COLOR, BG_COLOR],
                ),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("ALIGN", (4, 0), (6, -1), "RIGHT"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LINEBELOW", (0, 0), (-1, 0), 1, ACCENT_TEAL),
                (
                    "LINEBELOW",
                    (0, 1),
                    (-1, -2),
                    0.25,
                    HexColor("#2a2a36"),
                ),
            ]
        )
    )
    story.append(table)
    story.append(Spacer(1, 0.3 * inch))


def _build_pattern_breakdown(
    story: list,
    styles: dict,
    trades: list[dict],
) -> None:
    """Section 4: Pattern / setup breakdown."""
    story.append(Paragraph("Pattern Breakdown", styles["heading"]))

    patterns: dict[str, list[dict]] = {}
    for t in trades:
        pat = str(t.get("pattern") or "Unknown")
        patterns.setdefault(pat, []).append(t)

    if not patterns:
        story.append(
            Paragraph("No pattern data available.", styles["body_muted"])
        )
        return

    header = ["Pattern", "Trades", "Wins", "Win Rate", "Total P&L"]
    header_cells = [
        Paragraph(f'<font color="#e8e8f0"><b>{h}</b></font>', styles["body"])
        for h in header
    ]
    data_rows = [header_cells]

    sorted_patterns = sorted(
        patterns.items(),
        key=lambda x: sum(float(t.get("realized_pnl") or 0) for t in x[1]),
        reverse=True,
    )
    for pat, pat_trades in sorted_patterns:
        wins = sum(
            1 for t in pat_trades if float(t.get("realized_pnl") or 0) > 0
        )
        wr = _safe_div(wins, len(pat_trades)) * 100
        total = sum(float(t.get("realized_pnl") or 0) for t in pat_trades)
        pnl_hex = "#00e676" if total > 0 else "#ff5252" if total < 0 else "#e8e8f0"
        data_rows.append(
            [
                Paragraph(pat, styles["body"]),
                Paragraph(str(len(pat_trades)), styles["body"]),
                Paragraph(str(wins), styles["body"]),
                Paragraph(f"{wr:.1f}%", styles["body"]),
                Paragraph(
                    f'<font color="{pnl_hex}">{_fmt_pnl(total)}</font>',
                    styles["body"],
                ),
            ]
        )

    table = Table(data_rows, colWidths=[130, 55, 50, 65, 80])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), ACCENT_TEAL),
                ("TEXTCOLOR", (0, 0), (-1, 0), TEXT_COLOR),
                (
                    "ROWBACKGROUNDS",
                    (0, 1),
                    (-1, -1),
                    [SURFACE_COLOR, BG_COLOR],
                ),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("ALIGN", (1, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LINEBELOW", (0, 0), (-1, 0), 1, ACCENT_TEAL),
            ]
        )
    )
    story.append(table)
    story.append(Spacer(1, 0.3 * inch))


def _build_spy_comparison(
    story: list,
    styles: dict,
    trades: list[dict],
    week_start: str,
    week_end: str,
) -> None:
    """Section 5: SPY comparison."""
    story.append(Paragraph("SPY Comparison", styles["heading"]))

    spy_return = _get_spy_return(week_start, week_end)

    # Client weekly return as sum of realized PnL
    total_pnl = sum(float(t.get("realized_pnl") or 0) for t in trades)

    if spy_return is not None:
        spy_hex = "#00e676" if spy_return >= 0 else "#ff5252"
        story.append(
            Paragraph(
                f'SPY Weekly Return: <font color="{spy_hex}">'
                f"{_fmt_pct(spy_return)}</font>",
                styles["body"],
            )
        )
    else:
        story.append(
            Paragraph(
                "SPY data unavailable for this period.",
                styles["body_muted"],
            )
        )

    client_hex = "#00e676" if total_pnl >= 0 else "#ff5252"
    story.append(
        Paragraph(
            f'Client Week P&amp;L: <font color="{client_hex}">'
            f"{_fmt_pnl(total_pnl)}</font>",
            styles["body"],
        )
    )

    if spy_return is not None:
        alpha = total_pnl  # absolute dollar PnL vs SPY percentage — note in text
        story.append(Spacer(1, 0.1 * inch))
        story.append(
            Paragraph(
                f"Note: Client P&amp;L is absolute dollar-based; "
                f"SPY return is percentage-based. Direct comparison "
                f"is directional only.",
                styles["body_muted"],
            )
        )

    story.append(Spacer(1, 0.3 * inch))


def _build_risk_metrics(
    story: list,
    styles: dict,
    trades: list[dict],
) -> None:
    """Section 6: Risk metrics."""
    story.append(Paragraph("Risk Metrics", styles["heading"]))

    consec_losses = _max_consecutive_losses(trades)

    # Max drawdown (cumulative PnL peak-to-trough)
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    for t in trades:
        cumulative += float(t.get("realized_pnl") or 0)
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd

    # Capital utilization — approximate from number of trades
    # (full utilization metric would require position sizing data)
    trades_per_day = _safe_div(len(trades), 5)  # 5 trading days

    metrics = [
        ("Max Drawdown (P&L)", _fmt_pnl(max_dd)),
        ("Max Consecutive Losses", str(consec_losses)),
        ("Avg Trades / Day", f"{trades_per_day:.1f}"),
    ]

    data_rows = []
    for label, value in metrics:
        data_rows.append(
            [
                Paragraph(label, styles["body"]),
                Paragraph(f"<b>{value}</b>", styles["body"]),
            ]
        )

    table = Table(data_rows, colWidths=[250, 150])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), SURFACE_COLOR),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ("LEFTPADDING", (0, 0), (-1, -1), 10),
                ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                ("LINEBELOW", (0, 0), (-1, -2), 0.25, HexColor("#2a2a36")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    story.append(table)
    story.append(Spacer(1, 0.3 * inch))


def _build_outlook(
    story: list,
    styles: dict,
) -> None:
    """Section 7: Next week outlook."""
    story.append(Paragraph("Next Week Outlook", styles["heading"]))
    story.append(
        Paragraph(
            "System is monitoring active signals across all tracked tickers. "
            "Current score floor: 60. Only setups scoring above the tier "
            "threshold will be executed.",
            styles["body"],
        )
    )
    story.append(
        Paragraph(
            "Risk controls remain active: per-position sizing, max daily "
            "loss limits, and kill-switch thresholds are enforced.",
            styles["body_muted"],
        )
    )
    story.append(Spacer(1, 0.3 * inch))


# ── Page background callback ─────────────────────────────────────────────────

def _dark_bg(canvas_obj: canvas.Canvas, doc: Any) -> None:
    """Draw dark background and footer on every page."""
    canvas_obj.saveState()
    w, h = letter
    canvas_obj.setFillColor(BG_COLOR)
    canvas_obj.rect(0, 0, w, h, fill=1, stroke=0)

    # Footer
    canvas_obj.setFillColor(TEXT_MUTED)
    canvas_obj.setFont(FONT_NAME, 7)
    canvas_obj.drawString(54, 28, "Angel Precision — Confidential")
    canvas_obj.drawRightString(
        w - 54, 28, f"Page {doc.page}"
    )
    canvas_obj.restoreState()


# ── Main class ────────────────────────────────────────────────────────────────


class APWeeklyReporter:
    """Generate weekly PDF performance reports for Angel Precision clients."""

    TRADE_QUERY = """
        SELECT underlying, direction, tier, score, avg_fill, exit_price,
               realized_pnl, entry_ts, exit_ts, exit_reason, pattern,
               EXTRACT(EPOCH FROM (exit_ts - entry_ts))/60 as hold_minutes
        FROM positions
        WHERE client_id = %s
          AND status IN ('CLOSED','STOPPED','TAKEN_PROFIT','EXPIRED')
          AND entry_ts >= %s AND entry_ts < %s
        ORDER BY entry_ts ASC
    """

    def __init__(self, supabase_client: Any = None) -> None:
        self._sb = supabase_client

    def _fetch_trades(
        self,
        client_id: str,
        week_start: str,
        week_end: str,
    ) -> list[dict]:
        """Fetch closed trades for a client in the given week using ap.db."""
        try:
            from ap.db import conn, run_with_retry
        except ImportError:
            log.warning("ap.db not available — returning empty trade list")
            return []

        def _query() -> list[dict]:
            with conn() as c:
                c.execute(
                    self.TRADE_QUERY,
                    (client_id, week_start, week_end),
                )
                columns = [desc[0] for desc in c.description]
                rows = c.fetchall()
                return [dict(zip(columns, row)) for row in rows]

        try:
            return run_with_retry(_query)
        except Exception as exc:
            log.error("Failed to fetch trades for %s: %s", client_id, exc)
            return []

    def generate(
        self,
        client_id: str,
        client_name: str,
        week_start: str,
        week_end: str,
        output_dir: str = "/tmp/reports",
        trades: list[dict] | None = None,
    ) -> str:
        """
        Generate a weekly PDF report for one client.

        Parameters
        ----------
        client_id : str
            Client identifier (e.g. email).
        client_name : str
            Display name for the report.
        week_start : str
            ISO date string for week start (inclusive), e.g. "2026-04-07".
        week_end : str
            ISO date string for week end (exclusive), e.g. "2026-04-14".
        output_dir : str
            Directory to write the PDF to.
        trades : list[dict] | None
            Pre-fetched trade rows. If None, fetches from DB.

        Returns
        -------
        str
            Path to the generated PDF file.
        """
        _ensure_font()

        os.makedirs(output_dir, exist_ok=True)
        safe_id = _sanitize(client_id)
        filename = f"ap_report_{safe_id}_{week_start}.pdf"
        pdf_path = os.path.join(output_dir, filename)

        if trades is None:
            trades = self._fetch_trades(client_id, week_start, week_end)

        total_pnl = sum(float(t.get("realized_pnl") or 0) for t in trades)
        styles = _build_styles()

        doc = SimpleDocTemplate(
            pdf_path,
            pagesize=letter,
            title="Angel Precision — Weekly Performance Report",
            author="Perplexity Computer",
            leftMargin=54,
            rightMargin=54,
            topMargin=54,
            bottomMargin=54,
        )

        story: list[Any] = []

        # 1. Cover
        _build_cover(story, styles, client_name, week_start, week_end, total_pnl)

        if trades:
            # 2. Performance Summary KPIs
            _build_kpi_table(story, styles, trades, total_pnl)

            # 3. Trade Log
            _build_trade_log(story, styles, trades)

            # 4. Pattern Breakdown
            _build_pattern_breakdown(story, styles, trades)

            # 5. SPY Comparison
            _build_spy_comparison(story, styles, trades, week_start, week_end)

            # 6. Risk Metrics
            _build_risk_metrics(story, styles, trades)
        else:
            # Zero-trade week
            story.append(Paragraph("Performance Summary", styles["heading"]))
            story.append(
                Paragraph(
                    "No completed trades this week. "
                    "The system was either not active or all positions "
                    "remain open.",
                    styles["body_muted"],
                )
            )
            story.append(Spacer(1, 0.3 * inch))

        # 7. Outlook (always shown)
        _build_outlook(story, styles)

        doc.build(
            story,
            onFirstPage=_dark_bg,
            onLaterPages=_dark_bg,
        )

        log.info("Generated weekly report: %s", pdf_path)
        return pdf_path

    def generate_all(
        self,
        client_ids: list[str],
        week_start: str,
        week_end: str | None = None,
        output_dir: str = "/tmp/reports",
    ) -> list[str]:
        """
        Generate weekly reports for multiple clients.

        Parameters
        ----------
        client_ids : list[str]
            List of client identifiers.
        week_start : str
            ISO date for week start.
        week_end : str | None
            ISO date for week end. If None, defaults to week_start + 7 days.
        output_dir : str
            Output directory.

        Returns
        -------
        list[str]
            Paths to all generated PDF files.
        """
        if week_end is None:
            ws = datetime.strptime(week_start, "%Y-%m-%d")
            we = ws + timedelta(days=7)
            week_end = we.strftime("%Y-%m-%d")

        paths: list[str] = []
        for cid in client_ids:
            try:
                # Use client_id as name if no separate name lookup
                path = self.generate(
                    client_id=cid,
                    client_name=cid,
                    week_start=week_start,
                    week_end=week_end,
                    output_dir=output_dir,
                )
                paths.append(path)
            except Exception as exc:
                log.error("Failed to generate report for %s: %s", cid, exc)
        return paths


# ── Convenience for standalone / demo usage ───────────────────────────────────

def generate_sample_pdf(output_path: str) -> str:
    """Generate a sample PDF with mock data for demonstration."""
    mock_trades = [
        {
            "underlying": "AAPL",
            "direction": "LONG",
            "tier": "A+",
            "score": 92,
            "avg_fill": 3.45,
            "exit_price": 4.80,
            "realized_pnl": 135.00,
            "entry_ts": "2026-04-07 10:15:00",
            "exit_ts": "2026-04-07 14:30:00",
            "exit_reason": "TAKEN_PROFIT",
            "pattern": "breakout_pullback",
            "hold_minutes": 255,
        },
        {
            "underlying": "TSLA",
            "direction": "SHORT",
            "tier": "A",
            "score": 87,
            "avg_fill": 5.20,
            "exit_price": 4.10,
            "realized_pnl": -110.00,
            "entry_ts": "2026-04-08 09:45:00",
            "exit_ts": "2026-04-08 11:20:00",
            "exit_reason": "STOPPED",
            "pattern": "mean_reversion",
            "hold_minutes": 95,
        },
        {
            "underlying": "NVDA",
            "direction": "LONG",
            "tier": "A+",
            "score": 94,
            "avg_fill": 8.10,
            "exit_price": 10.55,
            "realized_pnl": 245.00,
            "entry_ts": "2026-04-09 10:00:00",
            "exit_ts": "2026-04-10 15:50:00",
            "exit_reason": "TAKEN_PROFIT",
            "pattern": "breakout_pullback",
            "hold_minutes": 1790,
        },
    ]

    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    reporter = APWeeklyReporter()
    # Use generate with pre-supplied trades so no DB is needed
    path = reporter.generate(
        client_id="demo@angelprecision.com",
        client_name="Demo Client",
        week_start="2026-04-07",
        week_end="2026-04-14",
        output_dir=output_dir or "/tmp",
        trades=mock_trades,
    )

    # Rename to requested output path if different
    if os.path.abspath(path) != os.path.abspath(output_path):
        os.rename(path, output_path)
        return output_path
    return path


if __name__ == "__main__":
    import sys

    out = generate_sample_pdf(
        sys.argv[1] if len(sys.argv) > 1 else "/tmp/sample_weekly_report.pdf"
    )
    print(f"Sample report generated: {out}")
