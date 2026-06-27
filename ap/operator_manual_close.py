"""
ap/operator_manual_close.py
─────────────────────────────────────────────────────────────────────────────
PR: feat/operator-manual-close-endpoint

Business logic for the POST /admin/operator/manual-close endpoint.

Kept separate from app.py so it can be unit-tested independently.
app.py imports and calls execute_operator_manual_close().

PURPOSE
  When Angel closes a position manually via Tradier, the bot records a
  RECONCILER_AUTO_CLOSE with a market-quote exit_price — not the true fill.
  This endpoint backfills the true fill price, correct PnL, correct
  close_source, and cancels any stale pending exit orders.

CALLED FROM
  app.py → POST /admin/operator/manual-close

REQUIRES
  - Migration: migrations/2026_06_26_operator_audit_log.sql applied
  - HMAC auth via @require_hmac (handled in app.py)
─────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import logging
from typing import Any, Optional

log = logging.getLogger("ap.operator_manual_close")


# ── Schema-safe column helpers ────────────────────────────────────────────────

def _col(row: dict, *names: str, default=None):
    """Return first matching key from a RealDictRow."""
    for n in names:
        if n in row:
            return row[n]
    return default


# ── Position fetch ─────────────────────────────────────────────────────────────

def _fetch_position(db_conn, position_id: str, client_id: str) -> Optional[dict]:
    db_conn.execute(
        """
        SELECT id, client_id, status, contract,
               qty, avg_fill, cost_basis,
               entry_ts, entry_option_price,
               exit_price, exit_reason, close_source
        FROM   positions
        WHERE  id        = %s
          AND  client_id = %s
        LIMIT 1
        """,
        (position_id, client_id),
    )
    row = db_conn.fetchone()
    if not row:
        return None
    return dict(row)


# ── PnL calculation ────────────────────────────────────────────────────────────

def _calculate_pnl(pos: dict, true_fill_price: float) -> tuple[float, float]:
    """
    Realized PnL and PnL% from the true fill price.
    Options: per-share price × 100 shares/contract × qty contracts.
    cost_basis is stored as total dollars; fall back to avg_fill × qty × 100.
    """
    qty = int(pos.get("qty") or 1)

    cost_basis = pos.get("cost_basis")
    avg_fill   = pos.get("avg_fill")
    entry_opt  = pos.get("entry_option_price")

    if cost_basis:
        entry_cost = float(cost_basis)
    elif avg_fill:
        entry_cost = float(avg_fill) * qty * 100
    elif entry_opt:
        entry_cost = float(entry_opt) * qty * 100
    else:
        log.warning("no entry cost data for position %s — PnL will be approximate", pos["id"])
        entry_cost = 0.0

    exit_proceeds = float(true_fill_price) * qty * 100
    realized_pnl  = exit_proceeds - entry_cost
    realized_pnl_pct = (realized_pnl / entry_cost * 100) if entry_cost else 0.0
    return round(realized_pnl, 2), round(realized_pnl_pct, 4)


# ── Position update ────────────────────────────────────────────────────────────

def _update_position(
    db_conn,
    position_id: str,
    client_id: str,
    true_fill_price: float,
    reason: str,
    realized_pnl: float,
    realized_pnl_pct: float,
) -> int:
    db_conn.execute(
        """
        UPDATE positions
        SET    exit_price         = %s,
               exit_reason        = %s,
               close_source       = 'operator_manual_close',
               close_confidence   = 'HIGH',
               status             = 'CLOSED',
               realized_pnl       = %s,
               realized_pnl_pct   = %s,
               quantity_remaining = 0,
               exit_in_flight     = false,
               exit_ts            = COALESCE(exit_ts, NOW()),
               updated_at         = NOW()
        WHERE  id        = %s
          AND  client_id = %s
        """,
        (true_fill_price, reason, realized_pnl, realized_pnl_pct,
         position_id, client_id),
    )
    return db_conn.rowcount


# ── Cancel stale exit orders ───────────────────────────────────────────────────

def _cancel_pending_exit_orders(db_conn, client_id: str, contract: str) -> int:
    db_conn.execute(
        """
        UPDATE orders
        SET    status     = 'CANCELED',
               last_error = 'operator_manual_close_superseded',
               updated_ts = NOW()
        WHERE  client_id  = %s
          AND  contract   = %s
          AND  kind       = 'EXIT'
          AND  status NOT IN ('FILLED', 'CANCELED', 'REJECTED')
        """,
        (client_id, contract),
    )
    return db_conn.rowcount


# ── Audit log ─────────────────────────────────────────────────────────────────

def _write_audit_log(
    db_conn,
    *,
    client_id: str,
    position_id: str,
    contract: str,
    true_fill_price: float,
    tradier_order_id: Optional[str],
    reason: str,
    realized_pnl: float,
    realized_pnl_pct: float,
    operator_note: Optional[str],
) -> None:
    db_conn.execute(
        """
        INSERT INTO operator_audit_log
          (event_type, client_id, position_id, contract,
           true_fill_price, tradier_order_id, reason,
           realized_pnl, realized_pnl_pct, operator_note, created_at)
        VALUES
          ('manual_close', %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """,
        (
            client_id, position_id, contract,
            true_fill_price, tradier_order_id, reason,
            realized_pnl, realized_pnl_pct, operator_note,
        ),
    )


# ── Public entrypoint ──────────────────────────────────────────────────────────

def execute_operator_manual_close(
    *,
    position_id: str,
    client_id: str,
    true_fill_price: float,
    tradier_order_id: Optional[str] = None,
    reason: str = "operator_manual_close",
    operator_note: Optional[str] = None,
) -> dict[str, Any]:
    """
    Execute a manual close with the true fill price.

    Returns a result dict safe to return directly as JSON.
    Raises on DB error — caller is responsible for rollback.

    Called from app.py POST /admin/operator/manual-close.
    """
    from ap.db import conn, run_with_retry

    def _fn():
        with conn() as c:
            pos = _fetch_position(c, position_id, client_id)
            if not pos:
                return {
                    "ok": False,
                    "error": f"position {position_id} not found for {client_id}",
                    "status_code": 404,
                }

            contract = pos["contract"]
            already_closed = (pos.get("status") or "").upper() == "CLOSED"
            realized_pnl, realized_pnl_pct = _calculate_pnl(pos, true_fill_price)

            positions_updated = _update_position(
                c, position_id, client_id,
                true_fill_price, reason, realized_pnl, realized_pnl_pct,
            )
            orders_canceled = _cancel_pending_exit_orders(c, client_id, contract)
            _write_audit_log(
                c,
                client_id=client_id,
                position_id=position_id,
                contract=contract,
                true_fill_price=true_fill_price,
                tradier_order_id=tradier_order_id,
                reason=reason,
                realized_pnl=realized_pnl,
                realized_pnl_pct=realized_pnl_pct,
                operator_note=operator_note,
            )

            log.info(
                "manual_close ok | client=%s position=%s contract=%s "
                "exit_price=%s pnl=%s (%s%%) orders_canceled=%d was_already_closed=%s",
                client_id, position_id, contract,
                true_fill_price, realized_pnl, realized_pnl_pct,
                orders_canceled, already_closed,
            )

            return {
                "ok":                True,
                "status_code":       200,
                "position_id":       position_id,
                "contract":          contract,
                "exit_price":        true_fill_price,
                "realized_pnl":      realized_pnl,
                "realized_pnl_pct":  realized_pnl_pct,
                "positions_updated": positions_updated,
                "orders_canceled":   orders_canceled,
                "was_already_closed": already_closed,
            }

    return run_with_retry(_fn)
