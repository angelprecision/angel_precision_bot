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
  This endpoint is REPAIR-ONLY: it backfills the true fill price on an
  already-closed position, corrects PnL, scopes stale EXIT-order cleanup to
  the position's execution_mode, and writes an audit log.

CALLED FROM
  app.py → POST /admin/operator/manual-close

REQUIRES
  - Migration: migrations/2026_06_26_operator_audit_log.sql applied
  - Admin auth via @_require_admin (handled in app.py)
─────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from ap.db import conn, run_with_retry

log = logging.getLogger("ap.operator_manual_close")

_ALLOWED_REPAIR_CLOSE_SOURCES = frozenset({
    "RECONCILER_AUTO_CLOSE",
    "broker_fallback_auto_close",
})

_ACTIVE_EXIT_STATUSES = (
    "NEW",
    "PROCESSING",
    "CREATED",
    "WATCHING",
    "PENDING_TRIGGER",
    "SUBMITTED",
    "ACKNOWLEDGED",
    "PARTIAL_FILL",
    "PARTIALLY_FILLED",
)


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


def _audit_log_exists(db_conn) -> bool:
    db_conn.execute("SELECT to_regclass('public.operator_audit_log')")
    row = db_conn.fetchone()
    if isinstance(row, dict):
        val = next(iter(row.values()), None)
    elif isinstance(row, (tuple, list)):
        val = row[0] if row else None
    else:
        val = row
    return bool(val)


def _derive_execution_mode(db_conn, *, position_id: str, client_id: str, contract: str) -> Optional[str]:
    db_conn.execute(
        """
        SELECT execution_mode
        FROM   orders
        WHERE  client_id = %s
          AND  contract  = %s
          AND  kind      = 'ENTRY'
          AND (
                position_id = %s
             OR filled_ts IS NOT NULL
             OR COALESCE(filled_qty, 0) > 0
             OR status IN ('FILLED', 'PARTIAL_FILL', 'PARTIALLY_FILLED', 'OPEN')
          )
        ORDER BY updated_ts DESC NULLS LAST, created_ts DESC NULLS LAST
        LIMIT 1
        """,
        (client_id, contract, position_id),
    )
    row = db_conn.fetchone()
    if not row:
        return None
    if isinstance(row, dict):
        mode = row.get("execution_mode")
    else:
        mode = row[0]
    mode = str(mode or "").strip().lower()
    return mode or None


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
        log.warning(
            "no entry cost data for position %s — PnL will be approximate",
            pos.get("id", "<unknown>"),
        )
        return 0.0, 0.0

    exit_proceeds = float(true_fill_price) * qty * 100
    realized_pnl  = exit_proceeds - entry_cost
    realized_pnl_pct = (realized_pnl / entry_cost * 100) if entry_cost else 0.0
    return round(realized_pnl, 2), round(realized_pnl_pct, 4)


# ── Position update ────────────────────────────────────────────────────────────

def _position_is_repair_eligible(pos: dict) -> bool:
    return (
        str(pos.get("status") or "").upper() == "CLOSED"
        and str(pos.get("close_source") or "") in _ALLOWED_REPAIR_CLOSE_SOURCES
    )


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
        WHERE  id           = %s
          AND  client_id    = %s
          AND  status       = 'CLOSED'
          AND  close_source IN ('RECONCILER_AUTO_CLOSE', 'broker_fallback_auto_close')
        """,
        (true_fill_price, reason, realized_pnl, realized_pnl_pct,
         position_id, client_id),
    )
    return db_conn.rowcount


# ── Cancel stale exit orders ───────────────────────────────────────────────────

def _count_pending_exit_orders(db_conn, client_id: str, contract: str, execution_mode: Optional[str]) -> int:
    db_conn.execute(
        """
        SELECT COUNT(*)
        FROM   orders
        WHERE  client_id  = %s
          AND  contract   = %s
          AND  kind       = 'EXIT'
          AND  status IN (
               'NEW',
               'PROCESSING',
               'CREATED',
               'WATCHING',
               'PENDING_TRIGGER',
               'SUBMITTED',
               'ACKNOWLEDGED',
               'PARTIAL_FILL',
               'PARTIALLY_FILLED'
          )
          AND (
                (%s IS NOT NULL AND execution_mode = %s)
             OR execution_mode IS NULL
          )
        """,
        (client_id, contract, execution_mode, execution_mode),
    )
    row = db_conn.fetchone()
    if isinstance(row, dict):
        return int(next(iter(row.values()), 0) or 0)
    if isinstance(row, (tuple, list)):
        return int(row[0] or 0)
    return int(row or 0)


def _cancel_pending_exit_orders(db_conn, client_id: str, contract: str, execution_mode: Optional[str]) -> int:
    db_conn.execute(
        """
        UPDATE orders
        SET    status     = 'CANCELED',
               last_error = 'operator_manual_close_superseded',
               updated_ts = NOW()
        WHERE  client_id  = %s
          AND  contract   = %s
          AND  kind       = 'EXIT'
          AND  status IN (
               'NEW',
               'PROCESSING',
               'CREATED',
               'WATCHING',
               'PENDING_TRIGGER',
               'SUBMITTED',
               'ACKNOWLEDGED',
               'PARTIAL_FILL',
               'PARTIALLY_FILLED'
          )
          AND (
                (%s IS NOT NULL AND execution_mode = %s)
             OR execution_mode IS NULL
          )
        """,
        (client_id, contract, execution_mode, execution_mode),
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
    dry_run: bool = False,
    confirmed_broker_flat: bool = False,
) -> dict[str, Any]:
    """
    Execute a manual close with the true fill price.

    Returns a result dict safe to return directly as JSON.
    Raises on DB error — caller is responsible for rollback.

    Called from app.py POST /admin/operator/manual-close.
    """
    def _fn():
        with conn() as c:
            if true_fill_price <= 0:
                return {
                    "ok": False,
                    "error": "true_fill_price_must_be_positive",
                    "status_code": 400,
                }
            if not _audit_log_exists(c):
                return {
                    "ok": False,
                    "error": "operator_audit_log_missing",
                    "status_code": 503,
                }
            pos = _fetch_position(c, position_id, client_id)
            if not pos:
                return {
                    "ok": False,
                    "error": f"position {position_id} not found for {client_id}",
                    "status_code": 404,
                }

            contract = pos["contract"]
            execution_mode = _derive_execution_mode(
                c, position_id=position_id, client_id=client_id, contract=contract
            )
            already_closed = (pos.get("status") or "").upper() == "CLOSED"
            realized_pnl, realized_pnl_pct = _calculate_pnl(pos, true_fill_price)
            orders_cancellable = _count_pending_exit_orders(
                c, client_id, contract, execution_mode
            )

            if not _position_is_repair_eligible(pos):
                if confirmed_broker_flat and operator_note and tradier_order_id:
                    return {
                        "ok": False,
                        "error": "override_not_supported_repair_only_endpoint",
                        "status_code": 409,
                        "position_status": pos.get("status"),
                        "close_source": pos.get("close_source"),
                    }
                return {
                    "ok": False,
                    "error": "repair_only_closed_position_required",
                    "status_code": 409,
                    "position_status": pos.get("status"),
                    "close_source": pos.get("close_source"),
                }

            if dry_run:
                return {
                    "ok": True,
                    "dry_run": True,
                    "status_code": 200,
                    "position_id": position_id,
                    "client_id": client_id,
                    "contract": contract,
                    "execution_mode": execution_mode,
                    "position_status": pos.get("status"),
                    "close_source": pos.get("close_source"),
                    "current_exit_price": pos.get("exit_price"),
                    "projected_exit_price": true_fill_price,
                    "realized_pnl": realized_pnl,
                    "realized_pnl_pct": realized_pnl_pct,
                    "active_exit_orders": orders_cancellable,
                    "operator_audit_log_exists": True,
                    "would_update_close_source": "operator_manual_close",
                    "would_set_reason": reason,
                }

            positions_updated = _update_position(
                c, position_id, client_id,
                true_fill_price, reason, realized_pnl, realized_pnl_pct,
            )
            if positions_updated != 1:
                return {
                    "ok": False,
                    "error": "position_update_conflict",
                    "status_code": 409,
                    "position_id": position_id,
                    "client_id": client_id,
                }
            orders_canceled = _cancel_pending_exit_orders(c, client_id, contract, execution_mode)
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
                "execution_mode":    execution_mode,
                "was_already_closed": already_closed,
            }

    return run_with_retry(_fn)
