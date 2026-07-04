# ap/flatline_alarm.py — P0: zero-submission trading-day alarm
# =============================================================================
# WHY THIS MODULE EXISTS
# ──────────────────────
# Every major production incident of Jun–Jul 2026 shared one symptom: the bot
# silently stopped submitting orders on trading days, and no one was paged.
#   • 2026-06-29 → 2026-07-02: 137/137 live entry orders EXPIRED, zero broker
#     submissions across three sessions. Detected days later by manual SQL.
#   • Earlier: paper pipeline dead for four sessions (486 frozen queue rows).
#   • Earlier: Jason's 54 overnight setups expired with zero submissions.
# A single scheduled check — "is today a trading day, and has anything been
# submitted?" — would have caught every one of these within an hour of open.
#
# Additionally the system currently has NO market holiday awareness: on
# 2026-07-03 (Independence Day observed, NYSE closed) scanners generated and
# processed signals all afternoon against dead quotes. This module is the
# single authoritative trading-calendar source; other modules may import
# is_trading_day() from here.
#
# WHAT THIS DOES
# ──────────────
# run_flatline_check() classifies the current session per execution mode:
#   OK                              submissions > 0
#   FLATLINE_SIGNALS_NO_SUBMISSIONS signals flowed but nothing reached broker
#                                   (the Jun-29..Jul-02 failure shape) — P0
#   FLATLINE_NO_SIGNALS             nothing even entered the pipeline — P0
#   SKIPPED_NOT_TRADING_DAY / SKIPPED_BEFORE_CHECKPOINT
# and (unless dry_run) posts a Discord alert via DISCORD_WEBHOOK_URL.
#
# DESIGN
# ──────
# • Read-only against orders/ap_signals. No mutations, no DDL, no state.
# • Dedup is owned by the invoking schedule (Render Cron at 10:30 + 13:00 ET
#   recommended), not by hidden in-process memory that dies on restart.
# • Fail-loud: a DB error during the check produces CHECK_ERROR (and alerts),
#   never a silent OK. An alarm that can silently fail is worse than none.
# • Calendar: static NYSE full-closure list per year + MARKET_HOLIDAYS_EXTRA
#   env (comma-separated YYYY-MM-DD) for ad-hoc closures (e.g. mourning days).
# =============================================================================

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from ap.logger import get_logger

log = get_logger("ap.flatline_alarm")

ET = ZoneInfo("America/New_York")

# NYSE full-closure holidays. Extend per-year; unknown years fall back to
# weekday-only logic plus MARKET_HOLIDAYS_EXTRA (fail-open to "trading day" so
# the alarm still runs rather than silently never firing).
NYSE_HOLIDAYS: dict[int, frozenset[str]] = {
    2026: frozenset({
        "2026-01-01",  # New Year's Day
        "2026-01-19",  # Martin Luther King Jr. Day
        "2026-02-16",  # Washington's Birthday
        "2026-04-03",  # Good Friday
        "2026-05-25",  # Memorial Day
        "2026-06-19",  # Juneteenth
        "2026-07-03",  # Independence Day (observed — Jul 4 is a Saturday)
        "2026-09-07",  # Labor Day
        "2026-11-26",  # Thanksgiving Day
        "2026-12-25",  # Christmas Day
    }),
    2027: frozenset({
        "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26",
        "2027-05-31", "2027-06-18", "2027-07-05", "2027-09-06",
        "2027-11-25", "2027-12-24",
    }),
}

DEFAULT_CHECKPOINT = dt_time(10, 30)  # ET; first check one hour after open


def _extra_holidays() -> frozenset[str]:
    raw = os.getenv("MARKET_HOLIDAYS_EXTRA", "")
    return frozenset(s.strip() for s in raw.split(",") if s.strip())


def is_trading_day(d: date) -> bool:
    """Weekday and not an NYSE full-closure holiday. Single source of truth."""
    if d.weekday() >= 5:
        return False
    iso = d.isoformat()
    if iso in _extra_holidays():
        return False
    year_set = NYSE_HOLIDAYS.get(d.year)
    if year_set is None:
        # Unknown year: fail toward "trading day" so the alarm still runs.
        log.warning("flatline_alarm: no NYSE calendar for %s — weekday-only logic", d.year)
        return True
    return iso not in year_set


@dataclass
class FlatlineResult:
    checked_at_et: str
    trading_day: bool
    state_by_mode: dict[str, str] = field(default_factory=dict)
    counts: dict[str, dict[str, int]] = field(default_factory=dict)
    alerts_sent: list[str] = field(default_factory=list)
    skipped_reason: Optional[str] = None
    error: Optional[str] = None

    @property
    def worst_state(self) -> str:
        order = ("CHECK_ERROR", "FLATLINE_SIGNALS_NO_SUBMISSIONS",
                 "FLATLINE_NO_SIGNALS", "OK")
        states = set(self.state_by_mode.values())
        if self.error:
            return "CHECK_ERROR"
        for s in order:
            if s in states:
                return s
        return self.skipped_reason or "SKIPPED"

    def to_dict(self) -> dict[str, Any]:
        return {
            "checked_at_et": self.checked_at_et,
            "trading_day": self.trading_day,
            "worst_state": self.worst_state,
            "state_by_mode": self.state_by_mode,
            "counts": self.counts,
            "alerts_sent": self.alerts_sent,
            "skipped_reason": self.skipped_reason,
            "error": self.error,
        }


def _default_counts_fn(session_date: date) -> dict[str, dict[str, int]]:
    """Per-execution-mode counts for the ET session date. Read-only.

    NOTE on execution_mode NULLs: 137 historical FILLED entries carry NULL
    execution_mode (pre-#130 backfill gap). New orders always carry it, but we
    bucket NULL as 'unknown' rather than dropping rows — an alarm must never
    lose data to a join.
    """
    from ap.db import conn, run_with_retry

    def _q() -> dict[str, dict[str, int]]:
        out: dict[str, dict[str, int]] = {}
        with conn() as c:
            c.execute(
                """
                SELECT COALESCE(execution_mode, 'unknown') AS mode,
                       COUNT(*) FILTER (WHERE submitted_ts IS NOT NULL) AS submissions,
                       COUNT(*) FILTER (WHERE status = 'FILLED')        AS fills,
                       COUNT(*)                                          AS orders_created
                FROM orders
                WHERE (created_ts AT TIME ZONE 'America/New_York')::date = %s
                   OR (submitted_ts AT TIME ZONE 'America/New_York')::date = %s
                GROUP BY 1
                """,
                (session_date, session_date),
            )
            for row in c.fetchall():
                mode, submissions, fills, created = row[0], row[1], row[2], row[3]
                out.setdefault(mode, {}).update(
                    submissions=int(submissions or 0),
                    fills=int(fills or 0),
                    orders_created=int(created or 0),
                )
            c.execute(
                """
                SELECT COUNT(*) FROM ap_signals
                WHERE (created_at AT TIME ZONE 'America/New_York')::date = %s
                """,
                (session_date,),
            )
            signals_today = int((c.fetchone() or [0])[0] or 0)
        for mode in list(out) or []:
            out[mode]["signals_global"] = signals_today
        if not out:
            out["live"] = {"submissions": 0, "fills": 0, "orders_created": 0,
                           "signals_global": signals_today}
            out["paper"] = {"submissions": 0, "fills": 0, "orders_created": 0,
                            "signals_global": signals_today}
        return out

    return run_with_retry(_q)


def _classify(counts: dict[str, int]) -> str:
    if counts.get("submissions", 0) > 0:
        return "OK"
    if counts.get("signals_global", 0) > 0 or counts.get("orders_created", 0) > 0:
        return "FLATLINE_SIGNALS_NO_SUBMISSIONS"
    return "FLATLINE_NO_SIGNALS"


def _send_alert(state_by_mode: dict[str, str], counts: dict[str, dict[str, int]],
                now_et: datetime) -> list[str]:
    from ap.discord_reporter import send_discord_webhook

    webhook = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    sent: list[str] = []
    flat = {m: s for m, s in state_by_mode.items() if s.startswith("FLATLINE")}
    if not flat:
        return sent
    lines = [f"🚨 **FLATLINE ALARM** — {now_et.strftime('%Y-%m-%d %H:%M ET')}",
             "Trading day, zero broker submissions:"]
    for mode, state in sorted(flat.items()):
        c = counts.get(mode, {})
        lines.append(
            f"• **{mode}** — {state} | signals={c.get('signals_global', 0)} "
            f"orders_created={c.get('orders_created', 0)} submissions=0"
        )
    lines.append("Runbook: check /admin/trade_flow_status, rejection reasons "
                 "in ap_signals.context_notes, and Render deploy status.")
    if send_discord_webhook(webhook, {"content": "\n".join(lines)}):
        sent.append("discord")
    else:
        log.error("flatline_alarm: FLATLINE detected but Discord alert FAILED "
                  "to send — states=%s", flat)
    return sent


def run_flatline_check(
    *,
    now_et: Optional[datetime] = None,
    checkpoint: dt_time = DEFAULT_CHECKPOINT,
    dry_run: bool = False,
    counts_fn: Callable[[date], dict[str, dict[str, int]]] = _default_counts_fn,
) -> FlatlineResult:
    """Run one flatline classification pass. Never raises."""
    now = now_et or datetime.now(ET)
    res = FlatlineResult(checked_at_et=now.isoformat(), trading_day=False)

    if not is_trading_day(now.date()):
        res.skipped_reason = "SKIPPED_NOT_TRADING_DAY"
        return res
    res.trading_day = True
    if now.time() < checkpoint:
        res.skipped_reason = "SKIPPED_BEFORE_CHECKPOINT"
        return res

    try:
        counts = counts_fn(now.date())
    except Exception as exc:  # fail-loud, never silent-OK
        log.exception("flatline_alarm: counts query failed")
        res.error = f"counts_query_failed: {exc}"
        if not dry_run:
            res.alerts_sent = _send_alert(
                {"system": "FLATLINE_SIGNALS_NO_SUBMISSIONS"},
                {"system": {"signals_global": -1, "orders_created": -1}},
                now,
            )
        return res

    res.counts = counts
    res.state_by_mode = {mode: _classify(c) for mode, c in counts.items()
                         if mode != "unknown" or c.get("submissions", 0) > 0}
    if not dry_run:
        res.alerts_sent = _send_alert(res.state_by_mode, counts, now)
    log.info("flatline_alarm: %s", res.to_dict())
    return res
