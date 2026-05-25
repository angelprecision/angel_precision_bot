#!/usr/bin/env python3
"""
Stale-entry / late-fill audit (Commit 31, Task 6).

Read-only audit over canceled / expired / acknowledged ENTRY orders in a
window.  Surfaces:

  - Orders that sat past the adaptive-autocancel ceilings (>90s normal /
    >120s A+) before terminating.
  - ACKNOWLEDGED orders older than RECONCILER_SLA_SECONDS that NEVER got
    a terminal status (the SMCI-style 2-hour-acked bug).
  - Cancels whose reason normalizes to one of the canonical buckets,
    plus whether a post-cancel retry was armed / submitted / aborted.
  - Fill events whose seconds_to_fill exceeded the configured ceiling
    (would normally be 'expired' but in case a slow fill landed late).

Reads only.  Writes nothing.

Usage
-----
    python scripts/stale_entry_audit.py --date 2026-05-26
    python scripts/stale_entry_audit.py --hours 48
    python scripts/stale_entry_audit.py --hours 24 --client client-A
    python scripts/stale_entry_audit.py --hours 24 --json out.json

Exit code
---------
    0  audit ran; either no problems or only warnings
    1  audit found at least one CRITICAL late-fill / acked-stuck row
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# Reason-code taxonomy for stale-entry findings (callout strings the
# dashboard / SQL queries can grep against).
REASON_ENTRY_ACK_TIMEOUT          = "ENTRY_ACK_TIMEOUT"
REASON_MISSED_MOVE_CANCEL         = "MISSED_MOVE_CANCEL"
REASON_SIGNAL_DEAD_CANCEL         = "SIGNAL_DEAD_CANCEL"
REASON_SPREAD_WIDENED_CANCEL      = "SPREAD_WIDENED_CANCEL"
REASON_UNDERLYING_THESIS_BROKEN   = "UNDERLYING_THESIS_BROKEN"
REASON_RETRY_SUBMITTED            = "RETRY_SUBMITTED"
REASON_RETRY_BLOCKED_RISK         = "RETRY_BLOCKED_RISK"
REASON_RETRY_BLOCKED_SYMBOL_LOCK  = "RETRY_BLOCKED_SYMBOL_LOCK"
REASON_RETRY_BLOCKED_STALE_PRICE  = "RETRY_BLOCKED_STALE_PRICE"


@dataclass
class StaleFinding:
    local_order_id: str
    client_id: str
    ticker: Optional[str]
    contract: Optional[str]
    direction: Optional[str]
    score: Optional[float]
    status: Optional[str]
    age_at_terminal_secs: Optional[float]
    entry_limit: Optional[float]
    submit_ask: Optional[float]
    cancel_reason_norm: Optional[str]   # normalized via ap.post_cancel_retry._normalize_reason
    cancel_reason_raw: Optional[str]    # raw last_error / meta.cancel_reason_detail
    reason_code: str                    # one of the REASON_* above
    severity: str                       # 'CRITICAL' / 'WARN' / 'INFO'
    retry_armed: bool
    retry_submitted: bool
    retry_aborted_reason: Optional[str]
    created_ts: Optional[str]
    updated_ts: Optional[str]
    notes: list[str] = field(default_factory=list)


@dataclass
class AuditSummary:
    window_start_utc: str
    window_end_utc: str
    client_filter: Optional[str]
    total_orders_inspected: int
    critical_count: int
    warn_count: int
    info_count: int
    by_reason: dict[str, int]
    findings: list[StaleFinding]


# ----------------------------------------------------------------------
# Thresholds (mirror order_monitor / Phase 2)
# ----------------------------------------------------------------------

_DEFAULT_ENTRY_MAX_AGE_NORMAL = int(os.getenv("ENTRY_MAX_AGE_NORMAL",      "90"))
_DEFAULT_ENTRY_MAX_AGE_APLUS  = int(os.getenv("ENTRY_MAX_AGE_APLUS",       "120"))
_DEFAULT_APLUS_THRESHOLD      = float(os.getenv("ENTRY_APLUS_SCORE_THRESHOLD", "85"))
_DEFAULT_ACK_STUCK_SECS       = int(os.getenv("RECONCILER_SLA_SECONDS",    "120"))


def _age_seconds(a: Any, b: Any) -> Optional[float]:
    try:
        if a is None or b is None:
            return None
        if isinstance(a, str):
            a = datetime.fromisoformat(a.replace("Z", "+00:00"))
        if isinstance(b, str):
            b = datetime.fromisoformat(b.replace("Z", "+00:00"))
        return (b - a).total_seconds()
    except Exception:
        return None


def _classify(order: dict, retry_events: dict[str, dict]) -> StaleFinding:
    """Build a StaleFinding for a single order row + its retry event
    map (keyed by local_order_id)."""
    meta = order.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}

    score = None
    try:
        if meta.get("score") is not None:
            score = float(meta["score"])
    except (TypeError, ValueError):
        pass

    age_secs = _age_seconds(order.get("created_ts"), order.get("updated_ts"))
    cancel_reason_raw = (
        order.get("last_error")
        or meta.get("cancel_reason_detail")
        or meta.get("last_repeg_reason")
    )

    # Normalize via the canonical helper so we get the same tokens the
    # post-cancel-retry engine sees.
    try:
        from ap.post_cancel_retry import _normalize_reason
        cancel_reason_norm = _normalize_reason(cancel_reason_raw) if cancel_reason_raw else None
    except Exception:
        cancel_reason_norm = str(cancel_reason_raw or "").lower() or None

    status = (order.get("status") or "").upper()

    # Pick a reason_code + severity from what we have.
    notes: list[str] = []
    if status in ("ACK", "ACKNOWLEDGED", "SUBMITTED", "NEW", "PENDING_FILL"):
        # Not terminal yet, age_secs is "still running".  Flag the SMCI
        # case: order has been ACKED for > ack-stuck threshold without a
        # FILL or CANCEL.
        age_now = _age_seconds(order.get("created_ts"),
                               datetime.now(timezone.utc).isoformat())
        if age_now is not None and age_now > _DEFAULT_ACK_STUCK_SECS:
            severity = "CRITICAL"
            reason_code = REASON_ENTRY_ACK_TIMEOUT
            notes.append(
                f"order has been {status} for {age_now:.0f}s "
                f"(> RECONCILER_SLA_SECONDS={_DEFAULT_ACK_STUCK_SECS}s) "
                "without terminal status - SMCI-style late-fill risk"
            )
        else:
            severity = "INFO"
            reason_code = REASON_ENTRY_ACK_TIMEOUT
            notes.append(f"order still {status}; age={age_now}s")
        age_at_terminal = None
    elif status in ("CANCELED", "CANCELLED", "EXPIRED"):
        age_at_terminal = age_secs
        # Map to reason_code by normalized cancel reason.
        if cancel_reason_norm in ("missed_move",):
            reason_code = REASON_MISSED_MOVE_CANCEL
            severity = "WARN"
        elif cancel_reason_norm in ("thesis_invalid", "stale_thesis_call", "stale_thesis_put"):
            reason_code = REASON_UNDERLYING_THESIS_BROKEN
            severity = "INFO"
        elif cancel_reason_norm in ("spread_wide",):
            reason_code = REASON_SPREAD_WIDENED_CANCEL
            severity = "WARN"
        elif cancel_reason_norm in ("runaway_quote", "runaway_quote_at_submit"):
            reason_code = REASON_SIGNAL_DEAD_CANCEL
            severity = "INFO"
        elif cancel_reason_norm in (
            "entry_max_age_normal_reached", "entry_max_age_aplus_reached", "stale_entry_timeout"
        ):
            reason_code = REASON_ENTRY_ACK_TIMEOUT
            # If the order sat past the ceiling for its tier, that's
            # expected behavior; flag CRITICAL only if it went *well*
            # past the ceiling (rare; would indicate hung cancel).
            is_aplus = score is not None and score >= _DEFAULT_APLUS_THRESHOLD
            ceiling = _DEFAULT_ENTRY_MAX_AGE_APLUS if is_aplus else _DEFAULT_ENTRY_MAX_AGE_NORMAL
            if age_at_terminal is not None and age_at_terminal > 2 * ceiling:
                severity = "CRITICAL"
                notes.append(
                    f"age_at_terminal={age_at_terminal:.0f}s is more than 2x "
                    f"the {'A+' if is_aplus else 'normal'} ceiling ({ceiling}s); "
                    "investigate cancel path latency"
                )
            else:
                severity = "INFO"
        else:
            reason_code = REASON_SIGNAL_DEAD_CANCEL
            severity = "INFO"
    else:
        # FILLED, REJECTED, etc.  Out of scope for stale-entry audit, but
        # we still surface very slow fills.
        age_at_terminal = age_secs
        is_aplus = score is not None and score >= _DEFAULT_APLUS_THRESHOLD
        ceiling = _DEFAULT_ENTRY_MAX_AGE_APLUS if is_aplus else _DEFAULT_ENTRY_MAX_AGE_NORMAL
        if status == "FILLED" and age_at_terminal is not None and age_at_terminal > ceiling:
            reason_code = REASON_ENTRY_ACK_TIMEOUT
            severity = "WARN"
            notes.append(
                f"FILLED at age {age_at_terminal:.0f}s exceeds "
                f"{'A+' if is_aplus else 'normal'} ceiling {ceiling}s; "
                "fill probably late vs spec"
            )
        else:
            reason_code = REASON_ENTRY_ACK_TIMEOUT
            severity = "INFO"

    # Retry event correlation
    rev = retry_events.get(order.get("local_order_id"), {})
    retry_armed = bool(rev.get("armed"))
    retry_submitted = bool(rev.get("submitted"))
    retry_aborted_reason = rev.get("abort_reason")
    if retry_submitted:
        # Overrides reason_code for visibility - we recovered.
        notes.append("retry was armed and submitted")
        if reason_code in (REASON_SIGNAL_DEAD_CANCEL, REASON_MISSED_MOVE_CANCEL):
            reason_code = REASON_RETRY_SUBMITTED
            severity = "INFO"
    elif retry_armed and not retry_submitted:
        notes.append(
            f"retry armed but never submitted "
            f"(abort_reason={retry_aborted_reason or 'unknown'})"
        )
        if retry_aborted_reason:
            ar = retry_aborted_reason.upper()
            if ar in ("RISK_GATE_BLOCKED", "POSITIONS_FULL", "DAILY_TRADE_CAP"):
                reason_code = REASON_RETRY_BLOCKED_RISK
            elif ar in ("SYMBOL_LOCKED",):
                reason_code = REASON_RETRY_BLOCKED_SYMBOL_LOCK
            elif ar in ("RUNAWAY_QUOTE_AT_SUBMIT",):
                reason_code = REASON_RETRY_BLOCKED_STALE_PRICE

    return StaleFinding(
        local_order_id      = order.get("local_order_id"),
        client_id           = order.get("client_id"),
        ticker              = order.get("symbol") or meta.get("ticker"),
        contract            = order.get("contract"),
        direction           = order.get("direction"),
        score               = score,
        status              = status,
        age_at_terminal_secs= round(age_at_terminal, 1) if age_at_terminal is not None else None,
        entry_limit         = order.get("limit_price"),
        submit_ask          = meta.get("submit_ask"),
        cancel_reason_norm  = cancel_reason_norm,
        cancel_reason_raw   = str(cancel_reason_raw)[:240] if cancel_reason_raw else None,
        reason_code         = reason_code,
        severity            = severity,
        retry_armed         = retry_armed,
        retry_submitted     = retry_submitted,
        retry_aborted_reason= retry_aborted_reason,
        created_ts          = str(order.get("created_ts")) if order.get("created_ts") else None,
        updated_ts          = str(order.get("updated_ts")) if order.get("updated_ts") else None,
        notes               = notes,
    )


# ----------------------------------------------------------------------
# Fetchers
# ----------------------------------------------------------------------

def _fetch_orders_in_window(conn_fn, start, end, client_filter=None) -> list[dict]:
    params: list[Any] = [start, end]
    where_extra = ""
    if client_filter:
        where_extra = " AND client_id = %s"
        params.append(client_filter)
    sql = f"""
        SELECT client_id, local_order_id, broker_order_id, symbol, contract,
               direction, status, qty, limit_price, last_error,
               created_ts, updated_ts, meta
        FROM   orders
        WHERE  kind = 'ENTRY'
          AND  created_ts >= %s
          AND  created_ts <  %s
          {where_extra}
        ORDER  BY created_ts ASC
    """
    with conn_fn() as c:
        c.execute(sql, tuple(params))
        return [dict(r) for r in c.fetchall()]


def _fetch_retry_events(conn_fn, start, end, client_filter=None) -> dict[str, dict]:
    """Return {local_order_id: {armed, submitted, abort_reason}} by reading
    decision_events from the stage='post_cancel_retry' branch."""
    params: list[Any] = [start, end]
    where_extra = ""
    if client_filter:
        where_extra = " AND client_id = %s"
        params.append(client_filter)

    sql = f"""
        SELECT client_id, local_order_id, decision, reason_code, ts
        FROM   decision_events
        WHERE  stage = 'post_cancel_retry'
          AND  ts >= %s AND ts < %s
          {where_extra}
        ORDER  BY ts ASC
    """
    out: dict[str, dict] = {}
    try:
        with conn_fn() as c:
            c.execute(sql, tuple(params))
            for row in c.fetchall():
                row = dict(row)
                oid = row.get("local_order_id")
                if not oid:
                    continue
                slot = out.setdefault(oid, {})
                dec = (row.get("decision") or "").upper()
                if dec == "ARM":
                    slot["armed"] = True
                elif dec == "SUBMIT":
                    slot["submitted"] = True
                elif dec == "ABORT":
                    slot["abort_reason"] = row.get("reason_code")
    except Exception:
        # decision_events table may not exist on every deploy; the audit
        # still runs, retry rows will just be empty.
        pass
    return out


# ----------------------------------------------------------------------
# Top-level
# ----------------------------------------------------------------------

def run_audit(
    start: datetime, end: datetime,
    client_filter: Optional[str] = None,
    conn_fn=None,
) -> AuditSummary:
    if conn_fn is None:
        from ap.db import conn as _conn  # type: ignore
        conn_fn = _conn

    orders = _fetch_orders_in_window(conn_fn, start, end, client_filter=client_filter)
    retry_events = _fetch_retry_events(conn_fn, start, end, client_filter=client_filter)

    findings: list[StaleFinding] = []
    for o in orders:
        findings.append(_classify(o, retry_events))

    by_reason: dict[str, int] = {}
    for f in findings:
        by_reason[f.reason_code] = by_reason.get(f.reason_code, 0) + 1

    crit = sum(1 for f in findings if f.severity == "CRITICAL")
    warn = sum(1 for f in findings if f.severity == "WARN")
    info = sum(1 for f in findings if f.severity == "INFO")

    return AuditSummary(
        window_start_utc       = start.isoformat(),
        window_end_utc         = end.isoformat(),
        client_filter          = client_filter,
        total_orders_inspected = len(orders),
        critical_count         = crit,
        warn_count             = warn,
        info_count             = info,
        by_reason              = by_reason,
        findings               = findings,
    )


def render_text(s: AuditSummary) -> str:
    lines: list[str] = []
    add = lines.append
    add("=" * 72)
    add(f"  ANGEL PRECISION - STALE ENTRY / LATE FILL AUDIT")
    add(f"  Window:  {s.window_start_utc}  ->  {s.window_end_utc}")
    if s.client_filter:
        add(f"  Client:  {s.client_filter}")
    add(f"  Orders inspected: {s.total_orders_inspected}")
    add(f"  CRITICAL={s.critical_count}  WARN={s.warn_count}  INFO={s.info_count}")
    add("=" * 72)
    add("")
    if s.by_reason:
        add("BY REASON CODE")
        for code, n in sorted(s.by_reason.items(), key=lambda kv: -kv[1]):
            add(f"  {code:32}  {n}")
        add("")
    # Only print findings that aren't pure INFO; INFO is fine for the JSON.
    interesting = [f for f in s.findings if f.severity in ("CRITICAL", "WARN")]
    if interesting:
        add("INTERESTING FINDINGS (CRITICAL / WARN)")
        for f in interesting:
            sym = "!!" if f.severity == "CRITICAL" else "??"
            add(
                f"  {sym} {f.local_order_id:14}  {(f.client_id or '?'):12}  "
                f"{(f.ticker or '?'):6}  {(f.status or '?'):10}  "
                f"age={f.age_at_terminal_secs}s  "
                f"reason={f.reason_code}"
            )
            for n in f.notes:
                add(f"        - {n}")
    else:
        add("No CRITICAL or WARN findings in window. Stale-entry protection looks healthy.")
    add("")
    add("=" * 72)
    return "\n".join(lines)


def _parse_args(argv):
    p = argparse.ArgumentParser(description="Stale-entry / late-fill audit")
    grp = p.add_mutually_exclusive_group(required=True)
    grp.add_argument("--date",  help="Audit window = full UTC day  (YYYY-MM-DD)")
    grp.add_argument("--hours", type=int, help="Audit window = last N hours")
    p.add_argument("--client", default=None, help="Optional client_id filter")
    p.add_argument("--json",   default=None, help="Optional path for JSON copy")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)

    if args.date:
        try:
            d = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print(f"ERROR: bad --date: {args.date!r}", file=sys.stderr)
            return 1
        start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        end   = start + timedelta(days=1)
    else:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=int(args.hours))

    try:
        summary = run_audit(start, end, client_filter=args.client)
    except Exception as e:
        print(f"ERROR: audit failed: {e}", file=sys.stderr)
        return 1

    print(render_text(summary))

    if args.json:
        try:
            Path(args.json).write_text(json.dumps(asdict(summary), indent=2, default=str))
        except Exception as e:
            print(f"WARN: failed to write JSON copy: {e}", file=sys.stderr)

    return 1 if summary.critical_count > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
