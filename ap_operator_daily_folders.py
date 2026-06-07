# =============================================================================
# ap_operator_daily_folders.py  —  PR #91 Daily Operator Folders
# =============================================================================
# Build one durable JSON artifact per trading day, one row per date in the
# operator_daily_folders Supabase table. Each section is independent:
# if any section fails, the day still saves — the failure is recorded in
# section_errors and the section appears in missing_sections.
#
# DESIGN RULES (from PR91 spec):
#
#   1. Supabase is the durable source of truth. Disk is optional fallback
#      / export.
#   2. No fake zeros. If a section fails, surface that — do NOT write
#      total_signals=0 as if confirmed.
#   3. Each section JSON includes: date, generated_at, data_source,
#      query_window, counts, items/rows, section_errors.
#   4. Official live totals only count PR90 Tradier Exit Proof Lock
#      eligible rows — never estimated/legacy/midpoint.
#   5. No trading logic changes. No scanner/scoring/sizing/exit/entry
#      logic touched. Archive/reporting only.
#
# Public functions:
#   build_daily_operator_folder(date_value, *, conn_factory=None,
#                                disk_export=False, disk_root=None)
#   collect_<section>(date_value, *, conn_factory=None)
#   build_daily_summary(sections)
#   write_disk_export(folder, disk_root)
#
# All collectors return a dict with the metadata envelope. None / NULL is
# returned for missing values — NEVER faked.
# =============================================================================

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date as _date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional

log = logging.getLogger("ap.operator_daily_folders")

# ---------------------------------------------------------------------------
# Section catalogue (must match the migration column list)
# ---------------------------------------------------------------------------

SECTION_NAMES: tuple[str, ...] = (
    "command_center",
    "scanner_truth",
    "intelligence_truth",
    "execution_truth",
    "live_execution_journal",
    "trade_volume_funnel",
    "missed_winner_report",
    "client_parity",
    "ghost_impact",
    "score_regression",
    "ticker_truth",
    "client_balances",
    "rejected_setups",
    "failed_orders",
    "closed_trades",
)

# ---------------------------------------------------------------------------
# Date / ISO-week helpers
# ---------------------------------------------------------------------------

def _coerce_date(d: Any) -> _date:
    """Accept a date, datetime, or ISO 'YYYY-MM-DD' string."""
    if isinstance(d, _date) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, str):
        s = d.strip()
        if "T" in s:
            s = s.split("T", 1)[0]
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    raise TypeError(f"_coerce_date: unsupported {type(d).__name__}")


def iso_week_string(d: Any) -> str:
    """Return ISO week label 'YYYY-Www' for the given date."""
    dt = _coerce_date(d)
    iy, iw, _ = dt.isocalendar()
    return f"{iy:04d}-W{iw:02d}"


def iso_week_bounds(iso_week: str) -> tuple[_date, _date]:
    """Return (week_start_monday, week_end_sunday) for 'YYYY-Www'."""
    iso_week = iso_week.strip()
    year_part, week_part = iso_week.split("-W")
    iy = int(year_part)
    iw = int(week_part)
    # ISO weeks: Monday is day 1
    monday = datetime.strptime(f"{iy}-W{iw:02d}-1", "%G-W%V-%u").date()
    sunday = monday + timedelta(days=6)
    return monday, sunday


def day_window_utc(d: Any) -> tuple[str, str]:
    """Return ISO start/end timestamps (UTC) for the given calendar day."""
    dt = _coerce_date(d)
    start = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start.isoformat(), end.isoformat()


# ---------------------------------------------------------------------------
# Section envelope
# ---------------------------------------------------------------------------

def _envelope(
    *,
    date_value: _date,
    data_source: str,
    counts: Optional[dict[str, Any]] = None,
    items: Optional[list[Any]] = None,
    section_errors: Optional[dict[str, str]] = None,
    extra: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Build the canonical per-section dict shape pinned by the PR spec."""
    start_iso, end_iso = day_window_utc(date_value)
    payload: dict[str, Any] = {
        "date":           date_value.isoformat(),
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "data_source":    data_source,
        "query_window":   {"start": start_iso, "end": end_iso},
        "counts":         dict(counts or {}),
        "items":          list(items or []),
        "section_errors": dict(section_errors or {}),
    }
    if extra:
        for k, v in extra.items():
            if k not in payload:
                payload[k] = v
    return payload


def _failed_section(date_value: _date, exc: BaseException) -> dict[str, Any]:
    """Build a *partial* section envelope when the collector raised.
    No counts are populated — we never fake zeros."""
    return _envelope(
        date_value=date_value,
        data_source="unavailable",
        counts=None,        # NOT {} of zeros — left empty
        items=[],
        section_errors={"error": f"{type(exc).__name__}: {exc}"},
    )


# ---------------------------------------------------------------------------
# Optional DB-conn factory
# ---------------------------------------------------------------------------

def _default_conn_factory() -> Optional[Callable[[], Any]]:
    """Try to import the real Supabase/Postgres connection factory.

    Returns None if ap.db is unavailable (test/dev without psycopg2),
    which causes each collector to record a partial section_error
    rather than crash.
    """
    try:
        from ap.db import conn  # type: ignore
        return conn
    except Exception:
        return None


def _run_query(
    conn_factory: Optional[Callable[[], Any]],
    sql: str,
    args: Optional[list[Any]] = None,
) -> list[dict[str, Any]]:
    """Execute a read-only SELECT and return a list of dicts.
    Caller is responsible for catching exceptions and recording section_errors.
    """
    if conn_factory is None:
        raise RuntimeError("no_conn_factory")
    with conn_factory() as cur:
        cur.execute(sql, args or [])
        desc = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            if isinstance(r, dict):
                out.append(r)
            else:
                out.append(dict(zip(desc, r)))
        return out


# ---------------------------------------------------------------------------
# Section collectors
# ---------------------------------------------------------------------------
# Each collector tries to call an existing builder/route function from the
# codebase. If that fails, it falls back to a direct SQL SELECT (when a
# stable schema exists). If THAT fails, it records section_errors and
# returns a partial envelope — never a fake zero envelope.
# ---------------------------------------------------------------------------

def collect_command_center(date_value: Any, *, conn_factory=None) -> dict[str, Any]:
    """Daily snapshot of the operator command-center counters."""
    d = _coerce_date(date_value)
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    start_iso, end_iso = day_window_utc(d)
    counts: dict[str, Any] = {}
    errors: dict[str, str] = {}
    try:
        if cf is None:
            raise RuntimeError("no_conn_factory")
        rows = _run_query(cf, """
            SELECT
                COUNT(*) FILTER (WHERE kind='ENTRY')        AS entry_orders,
                COUNT(*) FILTER (WHERE kind='EXIT')         AS exit_orders,
                COUNT(*) FILTER (WHERE status='FILLED')     AS filled_orders,
                COUNT(*) FILTER (WHERE status='SUBMITTED')  AS submitted_orders,
                COUNT(*) FILTER (WHERE status IN ('CANCELED','CANCELLED','REJECTED','FAILED'))
                                                            AS failed_orders
            FROM orders
            WHERE created_ts >= %s::timestamptz
              AND created_ts <  %s::timestamptz
        """, [start_iso, end_iso])
        if rows:
            counts.update({k: int(v) if v is not None else None for k, v in rows[0].items()})
    except Exception as e:
        errors["command_center_counts"] = f"{type(e).__name__}: {e}"
    return _envelope(
        date_value=d,
        data_source="supabase.orders",
        counts=counts,
        items=[],
        section_errors=errors,
    )


def collect_scanner_truth(date_value: Any, *, conn_factory=None) -> dict[str, Any]:
    """Daily scanner outcomes — created opportunities per scanner."""
    d = _coerce_date(date_value)
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    start_iso, end_iso = day_window_utc(d)
    counts: dict[str, Any] = {}
    items: list[Any] = []
    errors: dict[str, str] = {}
    try:
        if cf is None:
            raise RuntimeError("no_conn_factory")
        rows = _run_query(cf, """
            SELECT scanner_name,
                   COUNT(*)                                         AS opportunities,
                   COUNT(*) FILTER (WHERE opportunity_status='FILLED') AS filled,
                   COUNT(*) FILTER (WHERE block_reason IS NOT NULL) AS blocked
            FROM client_signal_opportunities
            WHERE created_ts >= %s::timestamptz
              AND created_ts <  %s::timestamptz
            GROUP BY scanner_name
            ORDER BY opportunities DESC
        """, [start_iso, end_iso])
        items = [dict(r) for r in rows]
        counts["scanners_seen"] = len(items)
        counts["total_opportunities"] = sum(int(r.get("opportunities") or 0) for r in items)
    except Exception as e:
        errors["scanner_truth_rollup"] = f"{type(e).__name__}: {e}"
    return _envelope(
        date_value=d,
        data_source="supabase.client_signal_opportunities",
        counts=counts,
        items=items,
        section_errors=errors,
    )


def collect_intelligence_truth(date_value: Any, *, conn_factory=None) -> dict[str, Any]:
    """Daily summary of intelligence/pattern decisions."""
    d = _coerce_date(date_value)
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    start_iso, end_iso = day_window_utc(d)
    counts: dict[str, Any] = {}
    items: list[Any] = []
    errors: dict[str, str] = {}
    try:
        if cf is None:
            raise RuntimeError("no_conn_factory")
        rows = _run_query(cf, """
            SELECT pattern,
                   COUNT(*) AS count,
                   AVG(score)::float AS avg_score
            FROM client_signal_opportunities
            WHERE created_ts >= %s::timestamptz
              AND created_ts <  %s::timestamptz
              AND pattern IS NOT NULL
            GROUP BY pattern
            ORDER BY count DESC
        """, [start_iso, end_iso])
        items = [dict(r) for r in rows]
        counts["patterns_seen"] = len(items)
    except Exception as e:
        errors["intelligence_truth_rollup"] = f"{type(e).__name__}: {e}"
    return _envelope(
        date_value=d,
        data_source="supabase.client_signal_opportunities",
        counts=counts,
        items=items,
        section_errors=errors,
    )


def collect_execution_truth(date_value: Any, *, conn_factory=None) -> dict[str, Any]:
    """Per-client execution stats for the day."""
    d = _coerce_date(date_value)
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    start_iso, end_iso = day_window_utc(d)
    counts: dict[str, Any] = {}
    items: list[Any] = []
    errors: dict[str, str] = {}
    try:
        if cf is None:
            raise RuntimeError("no_conn_factory")
        rows = _run_query(cf, """
            SELECT client_id,
                   COUNT(*)                                                AS orders,
                   COUNT(*) FILTER (WHERE status='FILLED')                 AS filled,
                   COUNT(*) FILTER (WHERE status IN ('CANCELED','CANCELLED','REJECTED','FAILED'))
                                                                            AS failed,
                   COUNT(*) FILTER (WHERE execution_mode='live')           AS live_orders
            FROM orders
            WHERE created_ts >= %s::timestamptz
              AND created_ts <  %s::timestamptz
            GROUP BY client_id
            ORDER BY orders DESC
        """, [start_iso, end_iso])
        items = [dict(r) for r in rows]
        counts["clients_active"] = len(items)
    except Exception as e:
        errors["execution_truth_rollup"] = f"{type(e).__name__}: {e}"
    return _envelope(
        date_value=d,
        data_source="supabase.orders",
        counts=counts,
        items=items,
        section_errors=errors,
    )


def collect_live_execution_journal(date_value: Any, *, conn_factory=None) -> dict[str, Any]:
    """Reuse PR90 journal builder for the same day window. This is the
    source of truth for official live trades."""
    d = _coerce_date(date_value)
    start_iso, end_iso = day_window_utc(d)
    errors: dict[str, str] = {}
    counts: dict[str, Any] = {}
    items: list[Any] = []
    extra: dict[str, Any] = {}
    try:
        from ap.operator.live_execution_journal import build_journal
        report = build_journal(
            start=start_iso,
            end=end_iso,
            conn_factory=conn_factory,
        )
        trades = report.get("trades") or []
        items = trades
        # Counts come from PR90 summary if present
        summary = report.get("summary") or {}
        counts.update({
            "total_trades":                  len(trades),
            "official_live_trades":          int(summary.get("official_live_trades")
                                                or sum(1 for t in trades if t.get("official_live_performance_eligible"))),
            "unofficial_unreconciled_trades": int(summary.get("unofficial_unreconciled_trades")
                                                or 0),
            "missing_exit_fill_truth":       int(summary.get("missing_exit_fill_truth") or 0),
        })
        extra["summary"]      = summary
        extra["action_items"] = report.get("action_items") or []
        extra["data_quality"] = report.get("data_quality") or {}
    except Exception as e:
        errors["live_execution_journal"] = f"{type(e).__name__}: {e}"
    return _envelope(
        date_value=d,
        data_source="ap.operator.live_execution_journal",
        counts=counts,
        items=items,
        section_errors=errors,
        extra=extra,
    )


def collect_trade_volume_funnel(date_value: Any, *, conn_factory=None) -> dict[str, Any]:
    """Funnel: opportunities -> admitted -> orders -> filled."""
    d = _coerce_date(date_value)
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    start_iso, end_iso = day_window_utc(d)
    counts: dict[str, Any] = {}
    errors: dict[str, str] = {}
    try:
        if cf is None:
            raise RuntimeError("no_conn_factory")
        rows = _run_query(cf, """
            SELECT
                (SELECT COUNT(*) FROM client_signal_opportunities
                   WHERE created_ts >= %s::timestamptz AND created_ts < %s::timestamptz)        AS total_signals,
                (SELECT COUNT(*) FROM client_signal_opportunities
                   WHERE created_ts >= %s::timestamptz AND created_ts < %s::timestamptz
                     AND block_reason IS NULL)                                                  AS admitted_signals,
                (SELECT COUNT(*) FROM client_signal_opportunities
                   WHERE created_ts >= %s::timestamptz AND created_ts < %s::timestamptz
                     AND block_reason IS NOT NULL)                                              AS rejected_signals,
                (SELECT COUNT(*) FROM orders
                   WHERE created_ts >= %s::timestamptz AND created_ts < %s::timestamptz
                     AND kind='ENTRY')                                                          AS orders_created,
                (SELECT COUNT(*) FROM orders
                   WHERE created_ts >= %s::timestamptz AND created_ts < %s::timestamptz
                     AND kind='ENTRY' AND broker_order_id IS NOT NULL)                          AS broker_submitted,
                (SELECT COUNT(*) FROM orders
                   WHERE created_ts >= %s::timestamptz AND created_ts < %s::timestamptz
                     AND kind='ENTRY' AND status='FILLED')                                      AS orders_filled,
                (SELECT COUNT(*) FROM orders
                   WHERE created_ts >= %s::timestamptz AND created_ts < %s::timestamptz
                     AND kind='ENTRY'
                     AND status IN ('CANCELED','CANCELLED','REJECTED','FAILED'))                AS orders_failed
        """, [start_iso, end_iso] * 7)
        if rows:
            for k, v in rows[0].items():
                counts[k] = int(v) if v is not None else None
    except Exception as e:
        errors["trade_volume_funnel"] = f"{type(e).__name__}: {e}"
    return _envelope(
        date_value=d,
        data_source="supabase.client_signal_opportunities+orders",
        counts=counts,
        items=[],
        section_errors=errors,
    )


def collect_missed_winner_report(date_value: Any, *, conn_factory=None) -> dict[str, Any]:
    """Opportunities blocked at preflight that later moved favorably."""
    d = _coerce_date(date_value)
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    start_iso, end_iso = day_window_utc(d)
    counts: dict[str, Any] = {}
    items: list[Any] = []
    errors: dict[str, str] = {}
    try:
        if cf is None:
            raise RuntimeError("no_conn_factory")
        rows = _run_query(cf, """
            SELECT canonical_signal_id, symbol, direction, pattern, score, miss_stage,
                   miss_reason, block_reason
            FROM client_signal_opportunities
            WHERE created_ts >= %s::timestamptz
              AND created_ts <  %s::timestamptz
              AND (miss_reason IS NOT NULL OR block_reason IS NOT NULL)
            ORDER BY score DESC NULLS LAST
            LIMIT 200
        """, [start_iso, end_iso])
        items = [dict(r) for r in rows]
        counts["misses_logged"] = len(items)
    except Exception as e:
        errors["missed_winner_report"] = f"{type(e).__name__}: {e}"
    return _envelope(
        date_value=d,
        data_source="supabase.client_signal_opportunities",
        counts=counts,
        items=items,
        section_errors=errors,
    )


def collect_client_parity(date_value: Any, *, conn_factory=None) -> dict[str, Any]:
    """Per-client parity: opportunities vs filled orders."""
    d = _coerce_date(date_value)
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    start_iso, end_iso = day_window_utc(d)
    counts: dict[str, Any] = {}
    items: list[Any] = []
    errors: dict[str, str] = {}
    try:
        if cf is None:
            raise RuntimeError("no_conn_factory")
        rows = _run_query(cf, """
            SELECT client_id,
                   COUNT(*) FILTER (WHERE opportunity_status IS NOT NULL) AS opportunities,
                   COUNT(*) FILTER (WHERE opportunity_status='FILLED')    AS filled,
                   COUNT(*) FILTER (WHERE block_reason IS NOT NULL)       AS blocked
            FROM client_signal_opportunities
            WHERE created_ts >= %s::timestamptz
              AND created_ts <  %s::timestamptz
            GROUP BY client_id
            ORDER BY client_id
        """, [start_iso, end_iso])
        items = [dict(r) for r in rows]
        discrepancies = 0
        # A "discrepancy" = a client with opportunities but zero fills AND
        # at least one other client in the same group filled. This is a
        # cheap heuristic; the canonical parity check lives elsewhere and
        # is referenced by client_breakdown in the journal.
        any_filled = any(int(r.get("filled") or 0) > 0 for r in items)
        if any_filled:
            for r in items:
                if int(r.get("opportunities") or 0) > 0 and int(r.get("filled") or 0) == 0:
                    discrepancies += 1
        counts["clients_seen"] = len(items)
        counts["client_discrepancy_count"] = discrepancies
    except Exception as e:
        errors["client_parity"] = f"{type(e).__name__}: {e}"
    return _envelope(
        date_value=d,
        data_source="supabase.client_signal_opportunities",
        counts=counts,
        items=items,
        section_errors=errors,
    )


def collect_ghost_impact(date_value: Any, *, conn_factory=None) -> dict[str, Any]:
    """Ghost = order created but never broker-submitted. High signal for
    bugs in the submit pipeline."""
    d = _coerce_date(date_value)
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    start_iso, end_iso = day_window_utc(d)
    counts: dict[str, Any] = {}
    items: list[Any] = []
    errors: dict[str, str] = {}
    try:
        if cf is None:
            raise RuntimeError("no_conn_factory")
        rows = _run_query(cf, """
            SELECT local_order_id, client_id, symbol, status, last_error
            FROM orders
            WHERE created_ts >= %s::timestamptz
              AND created_ts <  %s::timestamptz
              AND kind='ENTRY'
              AND broker_order_id IS NULL
            ORDER BY created_ts DESC
            LIMIT 200
        """, [start_iso, end_iso])
        items = [dict(r) for r in rows]
        counts["ghost_count"] = len(items)
    except Exception as e:
        errors["ghost_impact"] = f"{type(e).__name__}: {e}"
    return _envelope(
        date_value=d,
        data_source="supabase.orders",
        counts=counts,
        items=items,
        section_errors=errors,
    )


def collect_score_regression(date_value: Any, *, conn_factory=None) -> dict[str, Any]:
    """Score buckets and fill rates — useful for spotting score drift."""
    d = _coerce_date(date_value)
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    start_iso, end_iso = day_window_utc(d)
    counts: dict[str, Any] = {}
    items: list[Any] = []
    errors: dict[str, str] = {}
    try:
        if cf is None:
            raise RuntimeError("no_conn_factory")
        rows = _run_query(cf, """
            SELECT
                CASE
                    WHEN score >= 85 THEN '85+'
                    WHEN score >= 78 THEN '78-84'
                    WHEN score >= 70 THEN '70-77'
                    ELSE '<70'
                END AS bucket,
                COUNT(*) AS opportunities,
                COUNT(*) FILTER (WHERE opportunity_status='FILLED') AS filled
            FROM client_signal_opportunities
            WHERE created_ts >= %s::timestamptz
              AND created_ts <  %s::timestamptz
            GROUP BY bucket
            ORDER BY bucket
        """, [start_iso, end_iso])
        items = [dict(r) for r in rows]
        counts["buckets"] = len(items)
    except Exception as e:
        errors["score_regression"] = f"{type(e).__name__}: {e}"
    return _envelope(
        date_value=d,
        data_source="supabase.client_signal_opportunities",
        counts=counts,
        items=items,
        section_errors=errors,
    )


def collect_ticker_truth(date_value: Any, *, conn_factory=None) -> dict[str, Any]:
    """Per-symbol activity for the day."""
    d = _coerce_date(date_value)
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    start_iso, end_iso = day_window_utc(d)
    counts: dict[str, Any] = {}
    items: list[Any] = []
    errors: dict[str, str] = {}
    try:
        if cf is None:
            raise RuntimeError("no_conn_factory")
        rows = _run_query(cf, """
            SELECT symbol,
                   COUNT(*) AS opportunities,
                   COUNT(*) FILTER (WHERE opportunity_status='FILLED') AS filled,
                   COUNT(*) FILTER (WHERE block_reason IS NOT NULL)    AS blocked
            FROM client_signal_opportunities
            WHERE created_ts >= %s::timestamptz
              AND created_ts <  %s::timestamptz
            GROUP BY symbol
            ORDER BY opportunities DESC
            LIMIT 500
        """, [start_iso, end_iso])
        items = [dict(r) for r in rows]
        counts["symbols_seen"] = len(items)
    except Exception as e:
        errors["ticker_truth"] = f"{type(e).__name__}: {e}"
    return _envelope(
        date_value=d,
        data_source="supabase.client_signal_opportunities",
        counts=counts,
        items=items,
        section_errors=errors,
    )


def collect_client_balances(date_value: Any, *, conn_factory=None) -> dict[str, Any]:
    """End-of-day client equity / cash if tracked. Best-effort only."""
    d = _coerce_date(date_value)
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    counts: dict[str, Any] = {}
    items: list[Any] = []
    errors: dict[str, str] = {}
    try:
        if cf is None:
            raise RuntimeError("no_conn_factory")
        # Try the canonical members table first.
        rows = _run_query(cf, """
            SELECT client_id, account_equity, cash, updated_ts
            FROM members
            WHERE approved = TRUE
              AND subscription_active = TRUE
            ORDER BY client_id
        """)
        items = [dict(r) for r in rows]
        counts["clients_reporting"] = len(items)
    except Exception as e:
        errors["client_balances"] = f"{type(e).__name__}: {e}"
    return _envelope(
        date_value=d,
        data_source="supabase.members",
        counts=counts,
        items=items,
        section_errors=errors,
    )


def collect_rejected_setups(date_value: Any, *, conn_factory=None) -> dict[str, Any]:
    """Preflight-rejected setups grouped by reason."""
    d = _coerce_date(date_value)
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    start_iso, end_iso = day_window_utc(d)
    counts: dict[str, Any] = {}
    items: list[Any] = []
    errors: dict[str, str] = {}
    try:
        if cf is None:
            raise RuntimeError("no_conn_factory")
        rows = _run_query(cf, """
            SELECT block_reason AS reason, COUNT(*) AS count
            FROM client_signal_opportunities
            WHERE created_ts >= %s::timestamptz
              AND created_ts <  %s::timestamptz
              AND block_reason IS NOT NULL
            GROUP BY block_reason
            ORDER BY count DESC
        """, [start_iso, end_iso])
        items = [dict(r) for r in rows]
        counts["total_rejected"] = sum(int(r.get("count") or 0) for r in items)
        counts["distinct_reasons"] = len(items)
    except Exception as e:
        errors["rejected_setups"] = f"{type(e).__name__}: {e}"
    return _envelope(
        date_value=d,
        data_source="supabase.client_signal_opportunities",
        counts=counts,
        items=items,
        section_errors=errors,
    )


def collect_failed_orders(date_value: Any, *, conn_factory=None) -> dict[str, Any]:
    """Orders that failed at any stage today, grouped by last_error."""
    d = _coerce_date(date_value)
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    start_iso, end_iso = day_window_utc(d)
    counts: dict[str, Any] = {}
    items: list[Any] = []
    errors: dict[str, str] = {}
    try:
        if cf is None:
            raise RuntimeError("no_conn_factory")
        rows = _run_query(cf, """
            SELECT local_order_id, client_id, symbol, status, last_error, created_ts
            FROM orders
            WHERE created_ts >= %s::timestamptz
              AND created_ts <  %s::timestamptz
              AND status IN ('CANCELED','CANCELLED','REJECTED','FAILED')
            ORDER BY created_ts DESC
            LIMIT 500
        """, [start_iso, end_iso])
        items = [dict(r) for r in rows]
        counts["total_failed"] = len(items)
    except Exception as e:
        errors["failed_orders"] = f"{type(e).__name__}: {e}"
    return _envelope(
        date_value=d,
        data_source="supabase.orders",
        counts=counts,
        items=items,
        section_errors=errors,
    )


def collect_closed_trades(date_value: Any, *, conn_factory=None) -> dict[str, Any]:
    """Trades closed today from proof_trades — wins/losses + win_rate."""
    d = _coerce_date(date_value)
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    start_iso, end_iso = day_window_utc(d)
    counts: dict[str, Any] = {}
    items: list[Any] = []
    errors: dict[str, str] = {}
    try:
        if cf is None:
            raise RuntimeError("no_conn_factory")
        rows = _run_query(cf, """
            SELECT id, local_order_id, client_id, symbol, direction,
                   entry_option_price, exit_fill_price, option_pnl_pct,
                   exit_reason, closed_at, execution_mode,
                   broker_reconciled, synthetic_entry,
                   entry_price_source, exit_price_source,
                   broker_entry_order_id, broker_exit_order_id,
                   broker_entry_filled_qty, broker_exit_filled_qty
            FROM proof_trades
            WHERE closed_at >= %s::timestamptz
              AND closed_at <  %s::timestamptz
            ORDER BY closed_at DESC
            LIMIT 500
        """, [start_iso, end_iso])
        items = [dict(r) for r in rows]
        wins = sum(1 for r in items if (r.get("option_pnl_pct") or 0) > 0)
        losses = sum(1 for r in items if (r.get("option_pnl_pct") or 0) < 0)
        counts["closed_trades"] = len(items)
        counts["wins"]          = wins
        counts["losses"]        = losses
        counts["win_rate"]      = round(100.0 * wins / len(items), 2) if items else None
    except Exception as e:
        errors["closed_trades"] = f"{type(e).__name__}: {e}"
    return _envelope(
        date_value=d,
        data_source="supabase.proof_trades",
        counts=counts,
        items=items,
        section_errors=errors,
    )


# ---------------------------------------------------------------------------
# Section dispatch
# ---------------------------------------------------------------------------

_COLLECTORS: dict[str, Callable[..., dict[str, Any]]] = {
    "command_center":         collect_command_center,
    "scanner_truth":          collect_scanner_truth,
    "intelligence_truth":     collect_intelligence_truth,
    "execution_truth":        collect_execution_truth,
    "live_execution_journal": collect_live_execution_journal,
    "trade_volume_funnel":    collect_trade_volume_funnel,
    "missed_winner_report":   collect_missed_winner_report,
    "client_parity":          collect_client_parity,
    "ghost_impact":           collect_ghost_impact,
    "score_regression":       collect_score_regression,
    "ticker_truth":           collect_ticker_truth,
    "client_balances":        collect_client_balances,
    "rejected_setups":        collect_rejected_setups,
    "failed_orders":          collect_failed_orders,
    "closed_trades":          collect_closed_trades,
}


# ---------------------------------------------------------------------------
# Summary builder
# ---------------------------------------------------------------------------

def build_daily_summary(sections: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Roll the per-section counts into a flat daily summary.

    A section that failed (data_source == 'unavailable' OR has any
    section_errors) is NOT counted toward totals — its slot is left
    null/unavailable per the No-Fake-Zeros rule.

    Official live totals are pulled exclusively from the
    live_execution_journal section, which already enforces the PR90
    Tradier Exit Proof Lock.
    """
    def _ok(sec_name: str) -> bool:
        s = sections.get(sec_name) or {}
        if not s:
            return False
        if s.get("data_source") == "unavailable":
            return False
        if s.get("section_errors"):
            return False
        return True

    def _count(sec_name: str, key: str) -> Optional[int]:
        if not _ok(sec_name):
            return None
        v = (sections.get(sec_name) or {}).get("counts", {}).get(key)
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    def _count_float(sec_name: str, key: str) -> Optional[float]:
        if not _ok(sec_name):
            return None
        v = (sections.get(sec_name) or {}).get("counts", {}).get(key)
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    # Top failure stage/reason from failed_orders (if available)
    top_failure_stage: Optional[str] = None
    top_failure_reason: Optional[str] = None
    if _ok("failed_orders"):
        fo_items = (sections.get("failed_orders") or {}).get("items") or []
        if fo_items:
            # Group by status as stage and last_error as reason
            reasons: dict[str, int] = {}
            stages: dict[str, int] = {}
            for it in fo_items:
                s = (it.get("status") or "UNKNOWN")
                r = (it.get("last_error") or "unknown")
                stages[s] = stages.get(s, 0) + 1
                reasons[r] = reasons.get(r, 0) + 1
            if stages:
                top_failure_stage = max(stages.items(), key=lambda kv: kv[1])[0]
            if reasons:
                top_failure_reason = max(reasons.items(), key=lambda kv: kv[1])[0]

    # Top drop stage from rejected_setups
    top_drop_stage: Optional[str] = None
    if _ok("rejected_setups"):
        rs_items = (sections.get("rejected_setups") or {}).get("items") or []
        if rs_items:
            top_drop_stage = (rs_items[0] or {}).get("reason")

    return {
        # Volume funnel
        "total_signals":                     _count("trade_volume_funnel", "total_signals"),
        "admitted_signals":                  _count("trade_volume_funnel", "admitted_signals"),
        "rejected_signals":                  _count("trade_volume_funnel", "rejected_signals"),
        "orders_created":                    _count("trade_volume_funnel", "orders_created"),
        "broker_submitted":                  _count("trade_volume_funnel", "broker_submitted"),
        "orders_filled":                     _count("trade_volume_funnel", "orders_filled"),
        "orders_failed":                     _count("trade_volume_funnel", "orders_failed"),
        # Official live trades — sourced ONLY from PR90 journal section
        "official_live_trades":              _count("live_execution_journal", "official_live_trades"),
        "unofficial_unreconciled_trades":    _count("live_execution_journal", "unofficial_unreconciled_trades"),
        "missing_exit_fill_truth":           _count("live_execution_journal", "missing_exit_fill_truth"),
        # Closed-trade stats
        "closed_trades":                     _count("closed_trades", "closed_trades"),
        "wins":                              _count("closed_trades", "wins"),
        "losses":                            _count("closed_trades", "losses"),
        "win_rate":                          _count_float("closed_trades", "win_rate"),
        # Failure / drop stages
        "top_drop_stage":                    top_drop_stage,
        "top_failure_stage":                 top_failure_stage,
        "top_failure_reason":                top_failure_reason,
        # Parity
        "client_discrepancy_count":          _count("client_parity", "client_discrepancy_count"),
    }


# ---------------------------------------------------------------------------
# Daily folder builder
# ---------------------------------------------------------------------------

@dataclass
class DailyFolder:
    folder_date:   _date
    iso_week:      str
    generated_at:  str
    source_status: str
    sections:      dict[str, dict[str, Any]] = field(default_factory=dict)
    summary:       dict[str, Any]            = field(default_factory=dict)
    section_errors: dict[str, str]           = field(default_factory=dict)
    missing_sections: list[str]              = field(default_factory=list)

    def to_db_row(self) -> dict[str, Any]:
        """Build the upsert payload for operator_daily_folders."""
        row = {
            "folder_date":      self.folder_date.isoformat(),
            "iso_week":         self.iso_week,
            "generated_at":     self.generated_at,
            "source_status":    self.source_status,
            "summary":          self.summary,
            "section_errors":   self.section_errors,
            "missing_sections": self.missing_sections,
        }
        for sec in SECTION_NAMES:
            row[sec] = self.sections.get(sec) or {}
        return row


def build_daily_operator_folder(
    date_value: Any,
    *,
    conn_factory=None,
    disk_export: bool = False,
    disk_root: Optional[str] = None,
) -> DailyFolder:
    """Build the full operator folder for a date.

    Each section is collected independently. A section failure does NOT
    fail the whole day; instead it is recorded in section_errors +
    missing_sections, and source_status flips to 'partial'.

    No write to Supabase is performed here — caller upserts via
    upsert_daily_folder(...). This keeps the builder pure for tests.
    """
    d = _coerce_date(date_value)
    iso = iso_week_string(d)
    generated_at = datetime.now(timezone.utc).isoformat()

    sections: dict[str, dict[str, Any]] = {}
    section_errors: dict[str, str] = {}
    missing_sections: list[str] = []

    for name, fn in _COLLECTORS.items():
        try:
            sec = fn(d, conn_factory=conn_factory)
            sections[name] = sec
            # A collector that succeeded may still have recorded internal
            # section_errors. If so, treat the section as partial.
            if sec.get("section_errors"):
                section_errors[name] = ";".join(
                    f"{k}={v}" for k, v in sec["section_errors"].items()
                )
                if name not in missing_sections:
                    # Still present, but partial — keep it in sections but
                    # also surface in missing_sections so callers know.
                    missing_sections.append(name)
        except Exception as e:
            # Collector itself threw — replace with the partial envelope,
            # never with a fake zeros envelope.
            sections[name] = _failed_section(d, e)
            section_errors[name] = f"{type(e).__name__}: {e}"
            if name not in missing_sections:
                missing_sections.append(name)
            log.warning("daily folder section %s failed for %s: %s", name, d, e)

    summary = build_daily_summary(sections)
    source_status = "ok" if not missing_sections else "partial"

    folder = DailyFolder(
        folder_date=d,
        iso_week=iso,
        generated_at=generated_at,
        source_status=source_status,
        sections=sections,
        summary=summary,
        section_errors=section_errors,
        missing_sections=missing_sections,
    )

    if disk_export:
        try:
            write_disk_export(folder, disk_root or _default_disk_root())
        except Exception as e:
            # Disk export failure must not break the rest of the pipeline.
            log.warning("disk export failed for %s: %s", d, e)
            folder.section_errors["__disk_export__"] = f"{type(e).__name__}: {e}"

    return folder


# ---------------------------------------------------------------------------
# Upsert / fetch
# ---------------------------------------------------------------------------

def upsert_daily_folder(
    folder: DailyFolder,
    *,
    conn_factory=None,
) -> bool:
    """Upsert into operator_daily_folders. Returns True on success, False
    if Supabase is unavailable (in which case disk_export should already
    have run as the partial fallback)."""
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    if cf is None:
        return False
    row = folder.to_db_row()
    cols = list(row.keys())
    placeholders = ",".join(["%s"] * len(cols))
    update_set = ",".join(
        f"{c}=EXCLUDED.{c}" for c in cols if c != "folder_date"
    )
    sql = (
        f"INSERT INTO operator_daily_folders ({','.join(cols)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT (folder_date) DO UPDATE SET {update_set}, updated_at=NOW()"
    )
    # JSON-encode any dict/list columns to be tolerant of psycopg2 + jsonb.
    args: list[Any] = []
    for c in cols:
        v = row[c]
        if isinstance(v, (dict, list)):
            args.append(json.dumps(v, default=str))
        else:
            args.append(v)
    try:
        with cf() as cur:
            cur.execute(sql, args)
        return True
    except Exception as e:
        log.error("upsert_daily_folder failed for %s: %s", folder.folder_date, e)
        return False


def fetch_daily_folder(
    date_value: Any,
    *,
    conn_factory=None,
) -> Optional[dict[str, Any]]:
    """Fetch a stored daily folder row by date. Returns None if missing."""
    d = _coerce_date(date_value)
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    if cf is None:
        return None
    try:
        with cf() as cur:
            cur.execute(
                "SELECT * FROM operator_daily_folders WHERE folder_date=%s",
                [d.isoformat()],
            )
            desc = [x[0] for x in cur.description] if cur.description else []
            row = cur.fetchone()
            if not row:
                return None
            if isinstance(row, dict):
                return dict(row)
            return dict(zip(desc, row))
    except Exception as e:
        log.warning("fetch_daily_folder failed for %s: %s", d, e)
        return None


# ---------------------------------------------------------------------------
# Disk export (optional, fallback only)
# ---------------------------------------------------------------------------

def _default_disk_root() -> str:
    return os.environ.get("OPERATOR_DAILY_DISK_ROOT", "/home/user/workspace/data")


def write_disk_export(folder: DailyFolder, disk_root: str) -> str:
    """Write the daily folder out to data/daily/YYYY-MM-DD/*.json.
    Best-effort; raises only on truly broken filesystem state."""
    day_str = folder.folder_date.isoformat()
    target = Path(disk_root) / "daily" / day_str
    target.mkdir(parents=True, exist_ok=True)

    for name in SECTION_NAMES:
        sec = folder.sections.get(name) or {}
        (target / f"{name}.json").write_text(
            json.dumps(sec, indent=2, default=str), encoding="utf-8"
        )
    (target / "summary.json").write_text(
        json.dumps({
            "date":             day_str,
            "iso_week":         folder.iso_week,
            "generated_at":     folder.generated_at,
            "source_status":    folder.source_status,
            "summary":          folder.summary,
            "section_errors":   folder.section_errors,
            "missing_sections": folder.missing_sections,
        }, indent=2, default=str),
        encoding="utf-8",
    )
    return str(target)


def read_disk_export(date_value: Any, disk_root: Optional[str] = None) -> Optional[dict[str, Any]]:
    """Read back a disk-exported daily folder. Used only as a fallback
    when Supabase is unavailable."""
    d = _coerce_date(date_value)
    root = Path(disk_root or _default_disk_root()) / "daily" / d.isoformat()
    if not root.exists():
        return None
    out: dict[str, Any] = {
        "folder_date":    d.isoformat(),
        "iso_week":       iso_week_string(d),
        "sections":       {},
    }
    try:
        sj = (root / "summary.json")
        if sj.exists():
            summary_blob = json.loads(sj.read_text(encoding="utf-8"))
            for k in ("generated_at", "source_status", "summary",
                      "section_errors", "missing_sections", "iso_week"):
                if k in summary_blob:
                    out[k] = summary_blob[k]
        for name in SECTION_NAMES:
            p = root / f"{name}.json"
            if p.exists():
                out["sections"][name] = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        out.setdefault("section_errors", {})["__disk_read__"] = f"{type(e).__name__}: {e}"
    return out


__all__ = [
    "SECTION_NAMES",
    "DailyFolder",
    "build_daily_operator_folder",
    "upsert_daily_folder",
    "fetch_daily_folder",
    "build_daily_summary",
    "write_disk_export",
    "read_disk_export",
    "iso_week_string",
    "iso_week_bounds",
    "day_window_utc",
    "collect_command_center",
    "collect_scanner_truth",
    "collect_intelligence_truth",
    "collect_execution_truth",
    "collect_live_execution_journal",
    "collect_trade_volume_funnel",
    "collect_missed_winner_report",
    "collect_client_parity",
    "collect_ghost_impact",
    "collect_score_regression",
    "collect_ticker_truth",
    "collect_client_balances",
    "collect_rejected_setups",
    "collect_failed_orders",
    "collect_closed_trades",
]
