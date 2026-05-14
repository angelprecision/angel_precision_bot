# ap/exit_decision_ledger.py
# =============================================================================
# Exit Decision Ledger
# =============================================================================
# Purpose
# -------
# A production-safe, best-effort ledger for recording what the exit system saw,
# decided, submitted, acknowledged, filled, or failed to close.
#
# Why this exists
# ---------------
# The most expensive failure mode for Angel Precision is not simply "a trade lost".
# It is: a trade went green, the system saw it, but we cannot prove whether the
# exit decision fired, whether the broker accepted it, whether it filled, or
# whether DB truth updated correctly.
#
# This module gives every position an audit trail from quote -> decision -> order
# -> fill. It is intentionally non-fatal: ledger failure must NEVER block exits.
# =============================================================================

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("ap.exit_decision_ledger")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _safe_bool(value: Any) -> bool:
    try:
        return bool(value)
    except Exception:
        return False


def _get(obj: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        try:
            value = getattr(obj, name, None)
            if value is not None and value != "":
                return value
        except Exception:
            pass
        try:
            if isinstance(obj, dict) and obj.get(name) not in (None, ""):
                return obj.get(name)
        except Exception:
            pass
    return default


def _json_dumps(payload: dict[str, Any]) -> str:
    def _default(value: Any):
        if isinstance(value, datetime):
            return value.isoformat()
        try:
            return str(value)
        except Exception:
            return None

    return json.dumps(payload, default=_default, sort_keys=True)


@dataclass
class ExitLedgerEvent:
    event_type: str
    client_id: str = ""
    position_id: str = ""
    signal_id: str = ""
    ticker: str = ""
    contract: str = ""
    side: str = ""
    quantity_remaining: int = 0
    scale_outs_done: int = 0

    entry_price: float = 0.0
    current_option_price: float = 0.0
    current_bid: float = 0.0
    current_ask: float = 0.0
    current_underlying: float = 0.0
    option_pnl_pct: float = 0.0
    peak_pnl_pct: float = 0.0
    max_profit_seen: float = 0.0
    touched_profit: bool = False

    quote_state: str = ""
    quote_opt_age_sec: float = 0.0
    quote_und_age_sec: float = 0.0
    last_quote_update_ts: str = ""

    decision_action: str = ""
    decision_qty: int = 0
    decision_reason: str = ""
    decision_reason_code: str = ""
    decision_urgency: str = ""
    decision_pnl_pct: float = 0.0

    exit_in_flight: bool = False
    pending_exit_action: str = ""
    pending_exit_reason: str = ""
    pending_exit_qty: int = 0
    pending_exit_local_order_id: str = ""
    pending_exit_broker_order_id: str = ""

    broker_order_id: str = ""
    local_order_id: str = ""
    broker_status: str = ""
    fill_qty: int = 0
    fill_price: float = 0.0
    error: str = ""

    latency_quote_to_decision_ms: Optional[float] = None
    latency_decision_to_submit_ms: Optional[float] = None
    latency_submit_to_ack_ms: Optional[float] = None
    latency_submit_to_fill_ms: Optional[float] = None

    created_at: str = field(default_factory=_utc_now_iso)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


def build_exit_ledger_event(
    *,
    event_type: str,
    pos: Any = None,
    decision: Any = None,
    client_id: str = "",
    local_order_id: str = "",
    broker_order_id: str = "",
    broker_status: str = "",
    fill_qty: Any = 0,
    fill_price: Any = 0.0,
    error: Any = "",
    metadata: Optional[dict[str, Any]] = None,
) -> ExitLedgerEvent:
    """Build a normalized event from a ManagedPosition + optional decision/order data."""
    pos = pos or {}
    decision = decision or {}

    entry_price = _safe_float(_get(pos, "entry_price", "entryprice"), 0.0)
    current_option_price = _safe_float(_get(pos, "current_option_price", "currentoptionprice"), 0.0)
    option_pnl_pct = 0.0
    if entry_price > 0 and current_option_price > 0:
        option_pnl_pct = (current_option_price - entry_price) / entry_price
    else:
        option_pnl_pct = _safe_float(_get(decision, "pnl_pct", "pnlpct"), 0.0)

    last_quote_ts = _get(
        pos,
        "last_option_quote_update_ts",
        "lastoptionquoteupdatets",
        "last_quote_update_ts",
        "lastquoteupdatets",
        default="",
    )
    if isinstance(last_quote_ts, datetime):
        last_quote_ts = last_quote_ts.isoformat()
    else:
        last_quote_ts = str(last_quote_ts or "")

    return ExitLedgerEvent(
        event_type=str(event_type or "UNKNOWN"),
        client_id=str(client_id or _get(pos, "client_id", "clientid", default="") or ""),
        position_id=str(_get(pos, "position_id", "positionid", "id", default="") or ""),
        signal_id=str(_get(pos, "signal_id", "signalid", default="") or ""),
        ticker=str(_get(pos, "ticker", "underlying", default="") or "").upper(),
        contract=str(_get(pos, "option_symbol", "optionsymbol", "contract", default="") or "").upper(),
        side=str(_get(pos, "side", "direction", default="") or "").upper(),
        quantity_remaining=_safe_int(_get(pos, "quantity_remaining", "quantityremaining", "qty_remaining"), 0),
        scale_outs_done=_safe_int(_get(pos, "scale_outs_done", "scaleoutsdone"), 0),
        entry_price=entry_price,
        current_option_price=current_option_price,
        current_bid=_safe_float(_get(pos, "current_bid", "currentbid"), 0.0),
        current_ask=_safe_float(_get(pos, "current_ask", "currentask"), 0.0),
        current_underlying=_safe_float(_get(pos, "current_underlying", "currentunderlying"), 0.0),
        option_pnl_pct=option_pnl_pct,
        peak_pnl_pct=_safe_float(_get(pos, "peak_pnl_pct", "peakpnlpct"), 0.0),
        max_profit_seen=_safe_float(_get(pos, "max_profit_seen", "maxprofitseen"), 0.0),
        touched_profit=_safe_bool(_get(pos, "touched_profit", "touchedprofit")),
        quote_state=str(_get(pos, "quote_state", "quotestate", default="") or ""),
        quote_opt_age_sec=_safe_float(_get(pos, "quote_opt_age_sec", "quoteoptagesec"), 0.0),
        quote_und_age_sec=_safe_float(_get(pos, "quote_und_age_sec", "quoteundagesec"), 0.0),
        last_quote_update_ts=last_quote_ts,
        decision_action=str(_get(decision, "action", default="") or ""),
        decision_qty=_safe_int(_get(decision, "quantity", "qty"), 0),
        decision_reason=str(_get(decision, "reason", default="") or ""),
        decision_reason_code=str(_get(decision, "reason_code", "reasoncode", default="") or ""),
        decision_urgency=str(_get(decision, "urgency", default="") or ""),
        decision_pnl_pct=_safe_float(_get(decision, "pnl_pct", "pnlpct"), option_pnl_pct),
        exit_in_flight=_safe_bool(_get(pos, "exit_in_flight", "exitinflight")),
        pending_exit_action=str(_get(pos, "pending_exit_action", "pendingexitaction", default="") or ""),
        pending_exit_reason=str(_get(pos, "pending_exit_reason", "pendingexitreason", default="") or ""),
        pending_exit_qty=_safe_int(_get(pos, "pending_exit_qty", "pendingexitqty"), 0),
        pending_exit_local_order_id=str(_get(pos, "pending_exit_local_order_id", "pendingexitlocalorderid", default="") or ""),
        pending_exit_broker_order_id=str(_get(pos, "pending_exit_broker_order_id", "pendingexitbrokerorderid", default="") or ""),
        broker_order_id=str(broker_order_id or _get(pos, "pending_exit_broker_order_id", default="") or ""),
        local_order_id=str(local_order_id or _get(pos, "pending_exit_local_order_id", default="") or ""),
        broker_status=str(broker_status or ""),
        fill_qty=_safe_int(fill_qty, 0),
        fill_price=_safe_float(fill_price, 0.0),
        error=str(error or ""),
        metadata=metadata or {},
    )


def record_exit_event(event: ExitLedgerEvent | dict[str, Any]) -> bool:
    """
    Persist an exit ledger event to Postgres when available.

    Returns True when the row was written. Returns False if the DB layer/table is
    unavailable. This function must never raise into trading code.
    """
    try:
        if isinstance(event, ExitLedgerEvent):
            row = event.to_row()
        else:
            row = dict(event or {})

        try:
            from ap.db import conn, run_with_retry
        except Exception as exc:
            log.debug("exit ledger DB unavailable: %s", exc)
            return False

        payload_json = _json_dumps(row)
        metadata_json = _json_dumps(row.get("metadata") or {})

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    INSERT INTO exit_decision_ledger (
                        created_at, event_type, client_id, position_id, signal_id,
                        ticker, contract, side, quantity_remaining, scale_outs_done,
                        entry_price, current_option_price, current_bid, current_ask,
                        current_underlying, option_pnl_pct, peak_pnl_pct,
                        max_profit_seen, touched_profit, quote_state,
                        quote_opt_age_sec, quote_und_age_sec, last_quote_update_ts,
                        decision_action, decision_qty, decision_reason,
                        decision_reason_code, decision_urgency, decision_pnl_pct,
                        exit_in_flight, pending_exit_action, pending_exit_reason,
                        pending_exit_qty, pending_exit_local_order_id,
                        pending_exit_broker_order_id, local_order_id, broker_order_id,
                        broker_status, fill_qty, fill_price, error, metadata, payload
                    ) VALUES (
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,
                        %s,%s,%s,
                        %s,%s,%s,%s,%s::jsonb,%s::jsonb
                    )
                    """,
                    (
                        row.get("created_at") or _utc_now_iso(),
                        row.get("event_type"), row.get("client_id"), row.get("position_id"), row.get("signal_id"),
                        row.get("ticker"), row.get("contract"), row.get("side"), int(row.get("quantity_remaining") or 0), int(row.get("scale_outs_done") or 0),
                        float(row.get("entry_price") or 0), float(row.get("current_option_price") or 0), float(row.get("current_bid") or 0), float(row.get("current_ask") or 0),
                        float(row.get("current_underlying") or 0), float(row.get("option_pnl_pct") or 0), float(row.get("peak_pnl_pct") or 0),
                        float(row.get("max_profit_seen") or 0), bool(row.get("touched_profit")), row.get("quote_state"),
                        float(row.get("quote_opt_age_sec") or 0), float(row.get("quote_und_age_sec") or 0), row.get("last_quote_update_ts") or None,
                        row.get("decision_action"), int(row.get("decision_qty") or 0), row.get("decision_reason"),
                        row.get("decision_reason_code"), row.get("decision_urgency"), float(row.get("decision_pnl_pct") or 0),
                        bool(row.get("exit_in_flight")), row.get("pending_exit_action"), row.get("pending_exit_reason"),
                        int(row.get("pending_exit_qty") or 0), row.get("pending_exit_local_order_id"),
                        row.get("pending_exit_broker_order_id"), row.get("local_order_id"), row.get("broker_order_id"),
                        row.get("broker_status"), int(row.get("fill_qty") or 0), float(row.get("fill_price") or 0), row.get("error"),
                        metadata_json, payload_json,
                    ),
                )

        run_with_retry(_fn)
        return True
    except Exception as exc:
        log.debug("exit ledger write failed: %s", exc, exc_info=False)
        return False


def record_exit_decision(pos: Any, decision: Any, *, client_id: str = "", metadata: Optional[dict[str, Any]] = None) -> bool:
    """Convenience helper for evaluate_exit() / submit paths."""
    event = build_exit_ledger_event(
        event_type="EXIT_DECISION",
        pos=pos,
        decision=decision,
        client_id=client_id,
        metadata=metadata,
    )
    return record_exit_event(event)


def record_exit_order_event(
    *,
    event_type: str,
    pos: Any,
    decision: Any = None,
    client_id: str = "",
    local_order_id: str = "",
    broker_order_id: str = "",
    broker_status: str = "",
    fill_qty: Any = 0,
    fill_price: Any = 0.0,
    error: Any = "",
    metadata: Optional[dict[str, Any]] = None,
) -> bool:
    """Convenience helper for submit/ack/fill/reject events."""
    event = build_exit_ledger_event(
        event_type=event_type,
        pos=pos,
        decision=decision,
        client_id=client_id,
        local_order_id=local_order_id,
        broker_order_id=broker_order_id,
        broker_status=broker_status,
        fill_qty=fill_qty,
        fill_price=fill_price,
        error=error,
        metadata=metadata,
    )
    return record_exit_event(event)
