# ap/closed_position_price_audit.py
# =============================================================================
# P0: detect and alert on positions that reached a terminal CLOSED state with
# quantity_remaining == 0 but no recorded exit economics (exit_ts, exit_price,
# or realized_pnl).
#
# Incident context
# -----------------
# 2026-08-28, jasoncosby1@gmail.com, BAC260904C00062000 (live): the canonical
# exit engine attempted to sell 2 contracts 56 times over ~44 minutes and was
# rejected every time by the broker ("Sell order is for more shares than your
# current long position"). The standing protective stop later removed in
# PR #544 had already reserved/closed the position out from under the
# canonical exit path. The position record ended up status='CLOSED',
# quantity_remaining=0 -- but with exit_ts, exit_price, and realized_pnl all
# NULL. The client's real P&L on that trade was never recorded anywhere.
#
# ap/manual_close_reconciliation.py already exists to finalize externally
# closed positions from exact broker fill evidence (detect_manual_closes),
# and it always requires fill-price evidence before it will finalize a
# position -- so it should not itself be capable of producing this gap. The
# scope of this module is deliberately narrower and independent of that
# machinery: it is a read-only invariant check that catches ANY position
# that reaches this state, regardless of which code path produced it, so the
# gap is never silent again.
#
# This module NEVER touches the broker, NEVER mutates positions, NEVER
# blocks or delays an exit. It only observes and alerts, exactly like
# ap/reconciler_heartbeat.py.
# =============================================================================

from __future__ import annotations

from typing import Optional

from ap.logger import get_logger
from ap.db import conn, run_with_retry

log = get_logger("ap.closed_position_price_audit")

DEFAULT_LOOKBACK_DAYS = 45


def find_unpriced_closures(client_id: str, lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> list[dict]:
    """Return CLOSED, zero-quantity positions missing exit economics.

    A position qualifies only when ALL of the following hold:
      - status = 'CLOSED' (terminal)
      - quantity_remaining = 0 (broker truth agrees nothing is open)
      - at least one of exit_ts / exit_price / realized_pnl is NULL

    Read-only. Never mutates the row, never calls the broker.
    """
    if not client_id or not isinstance(lookback_days, int) or lookback_days <= 0:
        return []

    def _query():
        with conn() as c:
            c.execute(
                """
                SELECT id, client_id, contract, direction, qty,
                       quantity_remaining, entry_price, entry_ts,
                       exit_ts, exit_price, realized_pnl,
                       execution_mode, close_source, updated_at
                FROM positions
                WHERE client_id = %s
                  AND UPPER(TRIM(COALESCE(status, ''))) = 'CLOSED'
                  AND COALESCE(quantity_remaining, 0) = 0
                  AND (exit_ts IS NULL OR exit_price IS NULL OR realized_pnl IS NULL)
                  AND updated_at >= NOW() - (%s || ' days')::interval
                ORDER BY updated_at DESC
                """,
                (client_id, str(lookback_days)),
            )
            rows = c.fetchall()
            return [dict(r) for r in (rows or [])]

    try:
        return run_with_retry(_query) or []
    except Exception as exc:
        log.error(
            "[%s] UNPRICED_CLOSURE_AUDIT_QUERY_FAILED lookback_days=%d err=%s: %s",
            client_id, lookback_days, type(exc).__name__, exc,
        )
        return []


def audit_unpriced_closures(client_id: str, lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> int:
    """Log a CRITICAL marker for every unpriced closure found. Returns the count.

    Alert only -- see module docstring. Safe to call every reconciler cycle;
    it re-alerts on every call for as long as the row remains unpriced, which
    is intentional (this is a data-integrity gap, not a transient condition,
    and it must not be easy to miss in the logs).
    """
    rows = find_unpriced_closures(client_id, lookback_days=lookback_days)
    for row in rows:
        missing = [
            field for field, val in (
                ("exit_ts", row.get("exit_ts")),
                ("exit_price", row.get("exit_price")),
                ("realized_pnl", row.get("realized_pnl")),
            )
            if val is None
        ]
        log.critical(
            "[%s] UNPRICED_POSITION_CLOSURE position_id=%s contract=%s "
            "direction=%s qty=%s execution_mode=%s close_source=%s "
            "entry_price=%s entry_ts=%s missing_fields=%s updated_at=%s "
            "-- position closed with quantity_remaining=0 but no recorded "
            "exit economics; client P&L for this trade is not captured. "
            "Requires manual reconciliation against broker trade history.",
            client_id, row.get("id"), row.get("contract"),
            row.get("direction"), row.get("qty"), row.get("execution_mode"),
            row.get("close_source"), row.get("entry_price"), row.get("entry_ts"),
            ",".join(missing), row.get("updated_at"),
        )
    return len(rows)
