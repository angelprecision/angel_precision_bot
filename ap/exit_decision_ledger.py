"""
ap/exit_decision_ledger.py
==============================================================================
Best-effort audit trail for every exit decision cycle.

Records the full context at each step:
  quote seen → peak recorded → decision fired → order submitted
  → broker ack/reject → fill → DB closed

Non-fatal: every write is wrapped in try/except. A ledger failure
must NEVER block a live exit.

Usage (from exit engine, post evaluate_exit):
    _ledger_exit_decision(pos, decision, client_id=self.client_id)

Usage (from order state machine / fill monitor, on status change):
    record_exit_order_event(
        position_id, local_order_id, broker_order_id,
        event_type="EXIT_FILLED", fill_qty=2, fill_price=2.34,
        client_id=email,
    )
==============================================================================
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("ap.exit_decision_ledger")

# ---------------------------------------------------------------------------
# Internal DB writer — uses the same conn/run_with_retry pattern as the rest
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(val, default: float = 0.0) -> Optional[float]:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return default


def _write(row: dict) -> bool:
    """Insert one row into exit_decision_ledger. Returns True on success."""
    try:
        from ap.db import conn, run_with_retry
        from psycopg2.extras import Json

        row = dict(row)
        if isinstance(row.get("metadata"), dict):
            row["metadata"] = Json(row["metadata"])
        if isinstance(row.get("payload"), dict):
            row["payload"] = Json(row["payload"])

        cols = list(row.keys())
        vals = list(row.values())
        placeholders = ", ".join(["%s"] * len(cols))
        col_str = ", ".join(cols)
        sql = f"INSERT INTO exit_decision_ledger ({col_str}) VALUES ({placeholders})"

        def _fn():
            with conn() as c:
                c.execute(sql, vals)

        run_with_retry(_fn)
        return True
    except Exception as exc:
        log.debug("exit_decision_ledger write failed (non-fatal): %s", exc)
        return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def record_exit_decision(
    pos: Any,
    decision: Any,
    *,
    client_id: str = "",
    metadata: Optional[dict] = None,
) -> bool:
    """
    Record one exit decision cycle. Call this immediately after evaluate_exit().
    Safe to call on every cycle (HOLD decisions are also recorded when P&L > 5%).

    Parameters
    ----------
    pos      : ManagedPosition — the position being evaluated
    decision : ExitDecision    — result of evaluate_exit()
    client_id: str             — client email
    metadata : dict            — any extra context (event_context, stage, etc.)
    """
    try:
        pnl = _safe_float(getattr(pos, "option_pnl_pct", None), 0.0)
        peak = _safe_float(getattr(pos, "peak_pnl_pct", None), 0.0)

        # Only record when position is interesting — skip pure hold at zero
        if not decision.should_act and (pnl or 0) < 0.03 and (peak or 0) < 0.05:
            return False

        row = {
            "created_at":               _now_iso(),
            "event_type":               "EXIT_DECISION",
            "client_id":                client_id or str(getattr(pos, "client_id", "") or ""),
            "position_id":              str(getattr(pos, "position_id", "") or ""),
            "signal_id":                str(getattr(pos, "signal_id", "") or ""),
            "ticker":                   str(getattr(pos, "ticker", "") or ""),
            "contract":                 str(getattr(pos, "option_symbol", "") or getattr(pos, "contract", "") or ""),
            "side":                     str(getattr(pos, "side", "") or ""),
            "quantity_remaining":       int(getattr(pos, "quantity_remaining", 0) or 0),
            "scale_outs_done":          int(getattr(pos, "scale_outs_done", 0) or 0),
            "entry_price":              _safe_float(getattr(pos, "entry_price", None)),
            "current_option_price":     _safe_float(getattr(pos, "current_option_price", None)),
            "current_bid":              _safe_float(getattr(pos, "current_bid", None)),
            "current_ask":              _safe_float(getattr(pos, "current_ask", None)),
            "current_underlying":       _safe_float(getattr(pos, "current_underlying", None)),
            "option_pnl_pct":           pnl,
            "peak_pnl_pct":             peak,
            "max_profit_seen":          _safe_float(getattr(pos, "max_profit_seen", None)),
            "touched_profit":           bool(getattr(pos, "touched_profit", False)),
            "quote_state":              str(getattr(pos, "quote_state", "") or ""),
            "exit_in_flight":           bool(getattr(pos, "exit_in_flight", False)),
            "decision_action":          str(getattr(decision, "action", "HOLD") or "HOLD"),
            "decision_qty":             int(getattr(decision, "quantity", 0) or 0),
            "decision_reason":          str(getattr(decision, "reason", "") or ""),
            "decision_reason_code":     str(getattr(decision, "reason_code", "") or ""),
            "decision_urgency":         str(getattr(decision, "urgency", "") or ""),
            "decision_pnl_pct":         _safe_float(getattr(decision, "pnl_pct", None)),
            "metadata":                 metadata or {},
            "payload":                  {},
        }
        return _write(row)
    except Exception as exc:
        log.debug("record_exit_decision failed (non-fatal): %s", exc)
        return False


def record_exit_order_event(
    position_id: str,
    local_order_id: str,
    broker_order_id: Optional[str] = None,
    *,
    event_type: str = "EXIT_ORDER_EVENT",
    client_id: str = "",
    ticker: str = "",
    contract: str = "",
    broker_status: str = "",
    fill_qty: int = 0,
    fill_price: Optional[float] = None,
    error: Optional[str] = None,
    metadata: Optional[dict] = None,
) -> bool:
    """
    Record a broker-level exit order event.
    Call from fill_monitor, OSM, or reconciler when exit order status changes.

    Events: EXIT_SUBMITTED, EXIT_ACK, EXIT_FILLED, EXIT_REJECTED,
            EXIT_CANCELED, EXIT_STALE, EXIT_PARTIAL_FILL
    """
    try:
        row = {
            "created_at":               _now_iso(),
            "event_type":               event_type,
            "client_id":                client_id,
            "position_id":              str(position_id or ""),
            "ticker":                   ticker,
            "contract":                 contract,
            "local_order_id":           str(local_order_id or ""),
            "broker_order_id":          str(broker_order_id or ""),
            "broker_status":            broker_status,
            "fill_qty":                 int(fill_qty or 0),
            "fill_price":               _safe_float(fill_price),
            "error":                    error,
            "metadata":                 metadata or {},
            "payload":                  {},
        }
        return _write(row)
    except Exception as exc:
        log.debug("record_exit_order_event failed (non-fatal): %s", exc)
        return False
