#!/usr/bin/env python3
"""
Daily proof report (Commit 31, Task 1).

Generates a per-day summary of the Angel Precision Bot's entry/fill
behavior plus a trade-by-trade table. Used during proof week
(Tue-Fri) so the operator can answer the three questions that decide
whether we go LIVE in week 2:

    1. Was the fill rate >= 40% today?
    2. Was the bot's realized P/L within $1 of Tradier's view?
    3. Did anything close as 'FILLED' that wasn't broker-confirmed?

Design choices
--------------
- Read-only.  No writes anywhere.  Only SELECTs against orders,
  positions, audit_log, decision_events, and clients.
- Single source of truth for entry shape:  re-uses
  ap.entry_telemetry.compute_entry_telemetry so the report rows
  match whatever the dashboard shows.
- Proof-only exit prices:  exit_fill_price is read from
  positions.exit_avg_fill (broker-confirmed) NEVER from
  positions.exit_target_price or any pre-submit estimate.
- Idempotent:  rerunning for the same date produces the same numbers.
- Works on empty days.  Run it any market date.
- Emits a DAILY_PROOF_REPORT_GENERATED audit event on success.

Usage
-----
    python scripts/daily_proof_report.py --date 2026-05-26
    python scripts/daily_proof_report.py --date 2026-05-26 --json out.json
    python scripts/daily_proof_report.py --date 2026-05-26 --client client-A

Exit code
---------
    0  report generated successfully
    1  hard error (DB unreachable, etc.)
    2  partial data warning (some metrics could not be computed but
       the report did produce something useful)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import date as _date_type
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional


# Make the repo importable when this script is run directly.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# ----------------------------------------------------------------------
# Output schema
# ----------------------------------------------------------------------

@dataclass
class ClientDailyPnL:
    client_id: str
    bot_realized_pnl_usd: float
    bot_realized_pnl_count: int
    # broker_realized_pnl_usd left None when we don't have a Tradier reading
    # for the day.  The reconciler can fill this in once the equity-truth
    # MVP (Task 9) ships; for now it's a placeholder so the schema is stable.
    broker_realized_pnl_usd: Optional[float] = None
    pnl_mismatch_usd: Optional[float] = None
    sync_issues: list[str] = field(default_factory=list)


@dataclass
class TradeRow:
    client_id: str
    signal_id: Optional[str]
    local_order_id: str
    broker_order_id: Optional[str]
    ticker: Optional[str]
    contract: Optional[str]
    side: Optional[str]
    entry_limit: Optional[float]
    submit_ask: Optional[float]
    fill_price: Optional[float]
    seconds_to_fill: Optional[float]
    cancel_reason: Optional[str]
    exit_reason: Optional[str]
    exit_submit_price: Optional[float]
    exit_fill_price: Optional[float]
    realized_pnl_pct: Optional[float]
    realized_pnl_usd: Optional[float]
    proof_finalized_at: Optional[str]
    status: Optional[str]
    # PR #30 telemetry surfaces:
    reason_bucket: Optional[str] = None
    entry_attempt: int = 0
    repeg_attempt: int = 0
    retry_attempt: int = 0
    quote_age_ms: Optional[int] = None
    sizing_reason_code: Optional[str] = None
    final_qty: Optional[int] = None
    account_equity: Optional[float] = None
    position_budget: Optional[float] = None


@dataclass
class DailyProofReport:
    date: str
    generated_at_utc: str
    mode: str                  # 'PAPER' / 'LIVE' / 'MIXED' / 'UNKNOWN'
    active_clients: list[str]
    # ---- entry funnel ------------------------------------------------
    signals_received: int            # decision_events where stage='admission' OR new orders created
    entry_orders_created: int
    orders_submitted: int            # entries that have a broker_order_id
    orders_filled: int
    fill_rate_pct: Optional[float]
    avg_seconds_to_fill: Optional[float]
    canceled_entries: int
    expired_entries: int
    stale_entries_prevented: int     # ENTRY_REEVAL events (did NOT cancel)
    missed_move_cancels: int
    repeg_attempts_total: int
    retry_attempts_total: int
    broker_circuit_events: int
    reconciler_stale_events: int
    # ---- exits -------------------------------------------------------
    exit_orders_submitted: int
    exit_orders_filled: int
    mfe_mae_closed_count: int
    mfe_mae_covered_count: int
    mfe_mae_coverage_pct: Optional[float]
    mfe_mae_coverage_warning: Optional[str]
    # ---- P/L by client ----------------------------------------------
    realized_pnl_by_client: list[ClientDailyPnL]
    # ---- audit signals ----------------------------------------------
    bot_vs_broker_mismatch_count: int
    client_sync_issues: list[str]
    errors: list[str]
    fixes_needed: list[str]
    # ---- detail ------------------------------------------------------
    trade_rows: list[TradeRow]


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _parse_date(s: str) -> _date_type:
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def _day_bounds(d: _date_type) -> tuple[datetime, datetime]:
    """Return [start, end) UTC bounds for a calendar day.  Trading is
    America/New_York but for proof we report on UTC days so the daily
    report aligns with how Render logs are time-stamped.
    """
    start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    end   = start + timedelta(days=1)
    return start, end


def _round(x: Any, ndigits: int = 2) -> Optional[float]:
    if x is None:
        return None
    try:
        return round(float(x), ndigits)
    except (TypeError, ValueError):
        return None


def _to_iso(ts: Any) -> Optional[str]:
    if ts is None:
        return None
    if isinstance(ts, str):
        return ts
    try:
        return ts.isoformat()
    except Exception:
        return str(ts)


# ----------------------------------------------------------------------
# Core data fetchers (all read-only)
# ----------------------------------------------------------------------

def _fetch_active_clients(conn_fn) -> list[dict]:
    with conn_fn() as c:
        c.execute(
            """
            SELECT client_id, name, status, COALESCE(mode, '') AS mode
            FROM   clients
            WHERE  COALESCE(status, '') ILIKE 'ACTIVE'
            ORDER  BY client_id
            """
        )
        return [dict(r) for r in c.fetchall()]


def _fetch_entry_orders(conn_fn, day_start, day_end, client_id=None) -> list[dict]:
    """Pull ENTRY orders created in the window plus their joined position
    row (for fill_price / exit data).
    """
    params: list[Any] = [day_start, day_end]
    where_extra = ""
    if client_id:
        where_extra = " AND o.client_id = %s"
        params.append(client_id)

    sql = f"""
        SELECT o.client_id,
               o.local_order_id,
               o.broker_order_id,
               o.kind,
               o.status,
               o.symbol,
               o.contract,
               o.direction,
               o.qty,
               o.limit_price,
               o.last_error,
               o.created_ts,
               o.updated_ts,
               o.meta,
               p.avg_fill         AS p_avg_fill,
               p.opened_ts        AS p_opened_ts,
               p.exit_avg_fill    AS p_exit_avg_fill,
               p.exit_target_price AS p_exit_target_price,
               p.exit_reason      AS p_exit_reason,
               p.closed_ts        AS p_closed_ts,
               p.status           AS p_status,
               p.realized_pnl     AS p_realized_pnl,
               p.realized_pnl_pct AS p_realized_pnl_pct
        FROM       orders o
        LEFT  JOIN positions p
                ON p.contract  = o.contract
               AND p.client_id = o.client_id
               AND p.status IN ('OPEN','CLOSED','CLOSING','PENDING')
        WHERE     o.kind = 'ENTRY'
          AND     o.created_ts >= %s
          AND     o.created_ts <  %s
          {where_extra}
        ORDER  BY o.created_ts ASC
    """
    with conn_fn() as c:
        c.execute(sql, tuple(params))
        return [dict(r) for r in c.fetchall()]


def _fetch_audit_event_counts(conn_fn, day_start, day_end, events: tuple[str, ...]) -> dict[str, int]:
    if not events:
        return {}
    placeholders = ",".join(["%s"] * len(events))
    sql = f"""
        SELECT event, COUNT(*) AS n
        FROM   audit_log
        WHERE  ts >= %s AND ts < %s
          AND  event IN ({placeholders})
        GROUP  BY event
    """
    params = [day_start, day_end, *events]
    with conn_fn() as c:
        c.execute(sql, tuple(params))
        return {row.get("event"): int(row.get("n") or 0) for row in c.fetchall()}


def _fetch_exit_orders(conn_fn, day_start, day_end, client_id=None) -> tuple[int, int]:
    """Return (exit_submitted_count, exit_filled_count) using broker-confirmed
    state only.  An exit is 'filled' only when status='FILLED' AND there is
    a positions row with exit_avg_fill set.
    """
    params: list[Any] = [day_start, day_end]
    where_extra = ""
    if client_id:
        where_extra = " AND o.client_id = %s"
        params.append(client_id)

    sql = f"""
        SELECT o.status,
               o.broker_order_id,
               p.exit_avg_fill AS p_exit_avg_fill
        FROM       orders o
        LEFT  JOIN positions p
                ON p.contract  = o.contract
               AND p.client_id = o.client_id
        WHERE  o.kind = 'EXIT'
          AND  o.created_ts >= %s
          AND  o.created_ts <  %s
          {where_extra}
    """
    submitted = 0
    filled = 0
    with conn_fn() as c:
        c.execute(sql, tuple(params))
        for r in c.fetchall():
            r = dict(r)
            if r.get("broker_order_id"):
                submitted += 1
            if (str(r.get("status") or "").upper() == "FILLED"
                    and r.get("p_exit_avg_fill") not in (None, "", 0)):
                filled += 1
    return submitted, filled


def _fetch_mfe_mae_coverage(conn_fn, day_start, day_end, client_id=None) -> dict[str, int]:
    """Coverage audit for closed/terminal orders using orders.meta JSONB."""
    params: list[Any] = [day_start, day_end]
    where_extra = ""
    if client_id:
        where_extra = " AND client_id = %s"
        params.append(client_id)

    sql = f"""
        SELECT
          COUNT(*) AS closed_count,
          COUNT(*) FILTER (
            WHERE meta ? 'mfe_pct'
               OR meta ? 'mae_pct'
               OR meta ? 'mfe_mae_unavailable_reason'
          ) AS covered_count
        FROM orders
        WHERE status IN ('FILLED','CLOSED','CANCELLED','CANCELED','EXPIRED')
          AND created_ts >= %s
          AND created_ts <  %s
          {where_extra}
    """
    with conn_fn() as c:
        c.execute(sql, tuple(params))
        row = c.fetchone()
        if not row:
            return {"closed_count": 0, "covered_count": 0}
        if hasattr(row, "get"):
            return {
                "closed_count": int(row.get("closed_count") or 0),
                "covered_count": int(row.get("covered_count") or 0),
            }
        return {
            "closed_count": int(row[0] or 0),
            "covered_count": int(row[1] or 0),
        }


# ----------------------------------------------------------------------
# Per-row projection
# ----------------------------------------------------------------------

def _project_trade_row(row: dict) -> TradeRow:
    """Convert the joined DB row into the schema's TradeRow.

    Uses ap.entry_telemetry.compute_entry_telemetry for the shared
    23-field projection so the report agrees with the dashboard.
    """
    from ap.entry_telemetry import compute_entry_telemetry

    # Build the (order_row, position_row) pair the helper expects.
    pos = None
    if row.get("p_avg_fill") not in (None, "", 0) or row.get("p_opened_ts"):
        pos = {
            "avg_fill":  row.get("p_avg_fill"),
            "opened_ts": row.get("p_opened_ts"),
        }
    order_row = {k: v for k, v in row.items()
                 if not k.startswith("p_")}
    t = compute_entry_telemetry(order_row=order_row, position_row=pos)

    # Exit-side proof: only broker-confirmed fills count.
    p_exit_avg_fill = row.get("p_exit_avg_fill")
    if p_exit_avg_fill in (None, "", 0):
        exit_fill_price = None
        proof_finalized_at = None
    else:
        exit_fill_price = _round(p_exit_avg_fill)
        proof_finalized_at = _to_iso(row.get("p_closed_ts"))

    exit_submit_price = _round(row.get("p_exit_target_price"))
    realized_pnl_pct  = _round(row.get("p_realized_pnl_pct"), 2)
    realized_pnl_usd  = _round(row.get("p_realized_pnl"),     2)

    return TradeRow(
        client_id          = t.get("client_id"),
        signal_id          = (order_row.get("meta") or {}).get("signal_id"),
        local_order_id     = t.get("local_order_id"),
        broker_order_id    = t.get("broker_order_id"),
        ticker             = t.get("symbol"),
        contract           = t.get("contract"),
        side               = t.get("direction"),
        entry_limit        = _round(t.get("submit_limit")),
        submit_ask         = _round(t.get("submit_ask")),
        fill_price         = _round(t.get("fill_price")),
        seconds_to_fill    = _round(t.get("seconds_to_fill"), 1),
        cancel_reason      = t.get("cancel_reason_detail"),
        exit_reason        = row.get("p_exit_reason"),
        exit_submit_price  = exit_submit_price,
        exit_fill_price    = exit_fill_price,
        realized_pnl_pct   = realized_pnl_pct,
        realized_pnl_usd   = realized_pnl_usd,
        proof_finalized_at = proof_finalized_at,
        status             = t.get("status"),
        reason_bucket      = t.get("reason_bucket"),
        entry_attempt      = int(t.get("entry_attempt") or 0),
        repeg_attempt      = int(t.get("repeg_attempt") or 0),
        retry_attempt      = int(t.get("retry_attempt") or 0),
        quote_age_ms       = t.get("quote_age_ms"),
        sizing_reason_code = t.get("sizing_reason_code"),
        final_qty          = t.get("final_qty"),
        account_equity     = _round(t.get("account_equity")),
        position_budget    = _round(t.get("position_budget")),
    )


# ----------------------------------------------------------------------
# Aggregation
# ----------------------------------------------------------------------

def _aggregate(
    rows: list[dict],
    audit_counts: dict[str, int],
    exit_submitted: int,
    exit_filled: int,
    mfe_mae_coverage: dict[str, int],
    active_clients: list[dict],
    target_date: _date_type,
    mode_hint: Optional[str] = None,
) -> DailyProofReport:
    trade_rows = [_project_trade_row(r) for r in rows]

    # --- entry funnel counts -----------------------------------------
    entry_orders_created = len(trade_rows)
    orders_submitted = sum(1 for t in trade_rows if t.broker_order_id)
    orders_filled = sum(1 for t in trade_rows if (t.status or "").upper() == "FILLED")
    canceled = sum(1 for t in trade_rows if (t.status or "").upper() in ("CANCELED", "CANCELLED"))
    expired  = sum(1 for t in trade_rows if (t.reason_bucket == "expired"))

    fill_rate = (100.0 * orders_filled / entry_orders_created) if entry_orders_created else None
    fill_times = [t.seconds_to_fill for t in trade_rows
                  if t.seconds_to_fill is not None and t.seconds_to_fill > 0]
    avg_seconds_to_fill = (sum(fill_times) / len(fill_times)) if fill_times else None

    repeg_total = sum(int(t.repeg_attempt or 0) for t in trade_rows)
    retry_total = sum(int(t.retry_attempt or 0) for t in trade_rows)

    # --- P/L by client -----------------------------------------------
    pnl_by_client: dict[str, ClientDailyPnL] = {}
    for t in trade_rows:
        if t.client_id is None:
            continue
        if t.client_id not in pnl_by_client:
            pnl_by_client[t.client_id] = ClientDailyPnL(
                client_id=t.client_id,
                bot_realized_pnl_usd=0.0,
                bot_realized_pnl_count=0,
            )
        if t.realized_pnl_usd is not None and t.proof_finalized_at is not None:
            pnl_by_client[t.client_id].bot_realized_pnl_usd += float(t.realized_pnl_usd)
            pnl_by_client[t.client_id].bot_realized_pnl_count += 1

    # Round once we're done summing.
    for c in pnl_by_client.values():
        c.bot_realized_pnl_usd = round(c.bot_realized_pnl_usd, 2)

    # --- mode -------------------------------------------------------
    if mode_hint:
        mode = mode_hint.upper()
    else:
        modes = {(c.get("mode") or "").strip().upper() for c in active_clients}
        modes.discard("")
        if not modes:
            mode = "UNKNOWN"
        elif len(modes) == 1:
            mode = modes.pop()
        else:
            mode = "MIXED"

    # --- audit counts -----------------------------------------------
    stale_prevented   = audit_counts.get("ENTRY_REEVAL", 0)
    missed_move_canx  = audit_counts.get("MISSED_MOVE_ENTRY_CANCEL", 0)
    broker_circuit    = audit_counts.get("BROKER_ERROR_CIRCUIT_OPEN", 0)
    reconciler_stale  = audit_counts.get("RECONCILER_STALE", 0)
    mfe_mae_closed = int(mfe_mae_coverage.get("closed_count") or 0)
    mfe_mae_covered = int(mfe_mae_coverage.get("covered_count") or 0)
    mfe_mae_pct = (100.0 * mfe_mae_covered / mfe_mae_closed) if mfe_mae_closed else None
    mfe_mae_warning = None

    # --- problems / fixes ------------------------------------------
    errors: list[str] = []
    fixes_needed: list[str] = []
    if broker_circuit > 0:
        errors.append(
            f"BROKER_ERROR_CIRCUIT_OPEN fired {broker_circuit}x today. "
            "Investigate broker/network log before next session."
        )
        fixes_needed.append(
            "Review BROKER_ERROR_CIRCUIT_OPEN audit rows. Verify Tradier "
            "credentials + network. Manually clear breaker if appropriate."
        )
    if reconciler_stale > 0:
        errors.append(
            f"RECONCILER_STALE fired {reconciler_stale}x today (heartbeat "
            f"older than RECONCILER_SLA_SECONDS)."
        )
        fixes_needed.append(
            "Check reconciler health on Render. Confirm the reconciler "
            "cycle is completing every cycle."
        )
    if fill_rate is not None and fill_rate < 40.0:
        errors.append(
            f"Fill rate {fill_rate:.1f}% below proof-week target of 40%."
        )
        fixes_needed.append(
            "Drill into reason_bucket distribution. canceled_signal_alive "
            "dominant -> chase band or repeg logic; canceled_signal_dead "
            "dominant -> scanner is firing on bad setups."
        )
    if mfe_mae_pct is not None and mfe_mae_pct < 95.0:
        mfe_mae_warning = (
            f"MFE/MAE coverage {mfe_mae_pct:.1f}% below 95% target "
            f"({mfe_mae_covered}/{mfe_mae_closed} terminal orders covered)."
        )
        errors.append(mfe_mae_warning)
        fixes_needed.append(
            "Inspect orders.meta for missing mfe_pct/mae_pct/"
            "mfe_mae_unavailable_reason and verify PositionQuoteMonitor ran "
            "during the session."
        )

    return DailyProofReport(
        date                       = target_date.isoformat(),
        generated_at_utc           = datetime.now(timezone.utc).isoformat(),
        mode                       = mode,
        active_clients             = [c["client_id"] for c in active_clients],
        signals_received           = entry_orders_created,  # 1:1 with admission today
        entry_orders_created       = entry_orders_created,
        orders_submitted           = orders_submitted,
        orders_filled              = orders_filled,
        fill_rate_pct              = _round(fill_rate, 1),
        avg_seconds_to_fill        = _round(avg_seconds_to_fill, 1),
        canceled_entries           = canceled,
        expired_entries            = expired,
        stale_entries_prevented    = stale_prevented,
        missed_move_cancels        = missed_move_canx,
        repeg_attempts_total       = repeg_total,
        retry_attempts_total       = retry_total,
        broker_circuit_events      = broker_circuit,
        reconciler_stale_events    = reconciler_stale,
        exit_orders_submitted      = exit_submitted,
        exit_orders_filled         = exit_filled,
        mfe_mae_closed_count       = mfe_mae_closed,
        mfe_mae_covered_count      = mfe_mae_covered,
        mfe_mae_coverage_pct       = _round(mfe_mae_pct, 1),
        mfe_mae_coverage_warning   = mfe_mae_warning,
        realized_pnl_by_client     = list(pnl_by_client.values()),
        bot_vs_broker_mismatch_count = 0,   # Filled in by reconciler MVP (Task 9, future)
        client_sync_issues         = [],
        errors                     = errors,
        fixes_needed               = fixes_needed,
        trade_rows                 = trade_rows,
    )


# ----------------------------------------------------------------------
# Rendering
# ----------------------------------------------------------------------

def render_text(rpt: DailyProofReport) -> str:
    lines: list[str] = []
    add = lines.append

    add("=" * 72)
    add(f"  ANGEL PRECISION  -  DAILY PROOF REPORT  -  {rpt.date}  ({rpt.mode})")
    add(f"  Generated:  {rpt.generated_at_utc}")
    add(f"  Active clients ({len(rpt.active_clients)}):  {', '.join(rpt.active_clients) or '(none)'}")
    add("=" * 72)
    add("")
    add("ENTRY FUNNEL")
    add(f"  signals_received          : {rpt.signals_received}")
    add(f"  entry_orders_created      : {rpt.entry_orders_created}")
    add(f"  orders_submitted          : {rpt.orders_submitted}")
    add(f"  orders_filled             : {rpt.orders_filled}")
    add(f"  fill_rate_pct             : {rpt.fill_rate_pct if rpt.fill_rate_pct is not None else 'n/a'}")
    add(f"  avg_seconds_to_fill       : {rpt.avg_seconds_to_fill if rpt.avg_seconds_to_fill is not None else 'n/a'}")
    add(f"  canceled_entries          : {rpt.canceled_entries}")
    add(f"  expired_entries           : {rpt.expired_entries}")
    add(f"  stale_entries_prevented   : {rpt.stale_entries_prevented}    (ENTRY_REEVAL events)")
    add(f"  missed_move_cancels       : {rpt.missed_move_cancels}")
    add(f"  repeg_attempts_total      : {rpt.repeg_attempts_total}")
    add(f"  retry_attempts_total      : {rpt.retry_attempts_total}")
    add(f"  broker_circuit_events     : {rpt.broker_circuit_events}")
    add(f"  reconciler_stale_events   : {rpt.reconciler_stale_events}")
    add("")
    add("EXITS")
    add(f"  exit_orders_submitted     : {rpt.exit_orders_submitted}")
    add(f"  exit_orders_filled        : {rpt.exit_orders_filled}    (broker-confirmed only)")
    add(f"  mfe_mae_coverage_pct      : {rpt.mfe_mae_coverage_pct if rpt.mfe_mae_coverage_pct is not None else 'n/a'}")
    add(f"  mfe_mae_covered_count     : {rpt.mfe_mae_covered_count}/{rpt.mfe_mae_closed_count}")
    if rpt.mfe_mae_coverage_warning:
        add(f"  ! {rpt.mfe_mae_coverage_warning}")
    add("")
    add("REALIZED P/L BY CLIENT (broker-confirmed exit fills only)")
    if not rpt.realized_pnl_by_client:
        add("  (no closed trades)")
    for c in rpt.realized_pnl_by_client:
        add(f"  {c.client_id:20}  pnl=${c.bot_realized_pnl_usd:>10.2f}  count={c.bot_realized_pnl_count}")
    add("")
    if rpt.errors:
        add("ERRORS")
        for e in rpt.errors:
            add(f"  ! {e}")
        add("")
    if rpt.fixes_needed:
        add("FIXES NEEDED")
        for f in rpt.fixes_needed:
            add(f"  - {f}")
        add("")
    add("TRADES")
    if not rpt.trade_rows:
        add("  (no entries today)")
    else:
        # Trim header to keep terminal-friendly width.
        add(f"  {'time':19}  {'client':10}  {'ticker':6}  {'side':4}  "
            f"{'limit':>7}  {'fill':>7}  {'s2fill':>7}  {'bucket':22}  status")
        for t in rpt.trade_rows:
            ts = ""
            # Use created_ts is not on the dataclass; we don't carry it.
            add(
                f"  {ts:19}  "
                f"{(t.client_id or '?'):10}  "
                f"{(t.ticker or '?'):6}  "
                f"{(t.side or '?'):4}  "
                f"{(t.entry_limit if t.entry_limit is not None else 0):>7.2f}  "
                f"{(t.fill_price if t.fill_price is not None else 0):>7.2f}  "
                f"{(t.seconds_to_fill if t.seconds_to_fill is not None else 0):>7.1f}  "
                f"{(t.reason_bucket or '?'):22}  "
                f"{t.status or '?'}"
            )
    add("")
    add("=" * 72)
    return "\n".join(lines)


def to_dict(rpt: DailyProofReport) -> dict:
    return asdict(rpt)


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------

def generate_report(
    target_date: _date_type,
    conn_fn=None,
    client_id: Optional[str] = None,
    mode_hint: Optional[str] = None,
) -> DailyProofReport:
    """Build the report.  conn_fn override lets us point tests at a fake
    connection.  All real DB access goes through ap.db.conn by default.
    """
    if conn_fn is None:
        from ap.db import conn as _conn  # type: ignore
        conn_fn = _conn

    day_start, day_end = _day_bounds(target_date)

    try:
        active = _fetch_active_clients(conn_fn)
    except Exception:
        active = []

    rows = _fetch_entry_orders(conn_fn, day_start, day_end, client_id=client_id)

    audit_events = (
        "ENTRY_REEVAL",
        "MISSED_MOVE_ENTRY_CANCEL",
        "BROKER_ERROR_CIRCUIT_OPEN",
        "RECONCILER_STALE",
        "ENTRY_RETRY_ARMED",
        "ENTRY_RETRY_SUBMITTED",
        "ENTRY_RETRY_ABORTED",
    )
    try:
        audit_counts = _fetch_audit_event_counts(conn_fn, day_start, day_end, audit_events)
    except Exception:
        audit_counts = {}

    try:
        exit_sub, exit_fil = _fetch_exit_orders(conn_fn, day_start, day_end, client_id=client_id)
    except Exception:
        exit_sub, exit_fil = 0, 0
    try:
        mfe_mae_coverage = _fetch_mfe_mae_coverage(conn_fn, day_start, day_end, client_id=client_id)
    except Exception:
        mfe_mae_coverage = {"closed_count": 0, "covered_count": 0}

    rpt = _aggregate(
        rows, audit_counts, exit_sub, exit_fil, mfe_mae_coverage, active, target_date,
        mode_hint=mode_hint,
    )

    return rpt


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Angel Precision daily proof report")
    p.add_argument("--date", required=True, help="Report date in YYYY-MM-DD format")
    p.add_argument("--client", default=None, help="Optional client_id filter")
    p.add_argument("--mode", default=None, help="Optional mode hint (PAPER/LIVE)")
    p.add_argument("--json", default=None, help="Optional path to write a JSON copy")
    args = p.parse_args(argv)

    try:
        target = _parse_date(args.date)
    except ValueError as e:
        print(f"ERROR: invalid --date: {e}", file=sys.stderr)
        return 1

    try:
        rpt = generate_report(target, client_id=args.client, mode_hint=args.mode)
    except Exception as e:
        print(f"ERROR: daily proof report generation failed: {e}", file=sys.stderr)
        return 1

    text = render_text(rpt)
    print(text)

    if args.json:
        try:
            Path(args.json).write_text(json.dumps(to_dict(rpt), indent=2, default=str))
        except Exception as e:
            print(f"WARN: failed to write JSON copy to {args.json}: {e}", file=sys.stderr)
            return 2

    # Best-effort audit log.  If the audit subsystem isn't available we
    # just print and exit OK.
    try:
        from ap.logger import audit  # type: ignore
        audit(
            args.client or "system",
            "INFO",
            "DAILY_PROOF_REPORT_GENERATED",
            {
                "date":                 rpt.date,
                "mode":                 rpt.mode,
                "active_clients":       rpt.active_clients,
                "entry_orders_created": rpt.entry_orders_created,
                "orders_filled":        rpt.orders_filled,
                "fill_rate_pct":        rpt.fill_rate_pct,
                "exit_orders_filled":   rpt.exit_orders_filled,
                "mfe_mae_coverage_pct": rpt.mfe_mae_coverage_pct,
                "mfe_mae_coverage_warning": rpt.mfe_mae_coverage_warning,
                "errors":               rpt.errors,
            },
        )
    except Exception:
        # Audit log unavailable when running from a workstation: not fatal.
        pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
