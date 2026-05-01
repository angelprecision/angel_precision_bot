"""
ap.exit_autonomous_recovery
===========================
Conservative autonomous recovery helper for APExitEngine quarantine/stale in-flight states.

Safety rules
------------
1. Never clear an exit quarantine by time alone.
2. Never authorize replacement if a matching live broker exit order is found.
3. Prefer fill/close truth when broker/DB evidence exists.
4. On ambiguous matching live exits, try broker cancel first and only unlock after
   cancel proof or terminal broker confirmation.
5. If broker truth is ambiguous, alert/no-op.
6. Quote staleness is reported as a kill-switch signal for new entries, not a
   reason to guess exit truth.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("ap.exit_autonomous_recovery")

OPEN_BROKER_STATUSES = {"open", "pending", "accepted", "submitted", "queued", "working", "acknowledged", "partially_filled"}
TERMINAL_BROKER_STATUSES = {"filled", "canceled", "cancelled", "rejected", "expired"}
CANCEL_CONFIRMED_STATUSES = {"canceled", "cancelled", "rejected", "expired"}
QUOTE_STALE_WARN_SEC = int(os.getenv("EXIT_RECOVERY_QUOTE_STALE_SEC", "30"))


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _norm_contract(value: Any) -> str:
    return _norm(value).upper().replace(" ", "")


def _status(raw: dict) -> str:
    return _norm(raw.get("status") or raw.get("Status") or raw.get("state") or raw.get("order_status")).lower()


def _broker_order_id(raw: dict) -> str:
    return _norm(raw.get("broker_order_id") or raw.get("order_id") or raw.get("id") or raw.get("orderId"))


def _contract(raw: dict) -> str:
    return _norm_contract(raw.get("contract") or raw.get("symbol") or raw.get("option_symbol") or raw.get("instrument"))


def _qty(raw: dict) -> int:
    for key in ("qty", "quantity", "order_qty", "remaining_qty", "remaining_quantity", "filled_qty", "filled_quantity", "exec_quantity"):
        try:
            val = raw.get(key)
            if val not in (None, ""):
                return abs(int(float(val)))
        except Exception:
            pass
    return 0


def _is_exit_like(raw: dict) -> bool:
    text = " ".join(str(raw.get(k) or "") for k in (
        "side", "action", "instruction", "order_action", "transaction_type", "trade_action", "type", "description", "memo", "notes"
    )).lower()
    compact = text.replace("_", "").replace("-", "").replace(" ", "")
    return "selltoclose" in compact or compact == "stc" or "sell to close" in text or "sell" in text


def _dt_age_seconds(dt: Any) -> Optional[float]:
    if not dt:
        return None
    try:
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return max(0.0, (_now() - dt).total_seconds())
    except Exception:
        return None


def _list_open_orders(broker: Any) -> list[dict]:
    for method_name in ("list_open_orders", "get_open_orders", "list_orders", "orders"):
        method = getattr(broker, method_name, None)
        if not callable(method):
            continue
        try:
            try:
                result = method(status="open")
            except TypeError:
                result = method()
            if result is None:
                continue
            if isinstance(result, dict):
                for key in ("orders", "data", "results", "items"):
                    if isinstance(result.get(key), list):
                        return [dict(x) for x in result[key] if isinstance(x, dict)]
                return [result]
            if isinstance(result, list):
                return [dict(x) for x in result if isinstance(x, dict)]
        except Exception as exc:
            log.debug("broker.%s failed during autonomous recovery: %s", method_name, exc)
    return []


def _get_order(broker: Any, broker_order_id: str) -> Optional[dict]:
    if not broker_order_id:
        return None
    method = getattr(broker, "get_order", None)
    if not callable(method):
        return None
    try:
        raw = method(broker_order_id)
        return dict(raw) if isinstance(raw, dict) else None
    except Exception as exc:
        log.debug("broker.get_order(%s) failed: %s", broker_order_id, exc)
        return None


def _cancel_order_with_proof(broker: Any, broker_order_id: str) -> tuple[bool, dict]:
    """Attempt broker cancel and verify terminal/canceled status."""
    if not broker_order_id:
        return False, {"error": "missing_broker_order_id"}
    cancel = getattr(broker, "cancel_order", None)
    if not callable(cancel):
        return False, {"error": "broker_cancel_order_missing"}
    try:
        raw = cancel(broker_order_id)
        raw = dict(raw) if isinstance(raw, dict) else {"raw": raw}
    except Exception as exc:
        return False, {"error": str(exc), "broker_order_id": broker_order_id}

    status = _status(raw)
    ok_flag = bool(raw.get("ok"))
    confirmed = _get_order(broker, broker_order_id)
    confirmed_status = _status(confirmed or {}) if confirmed else status
    if ok_flag and (not confirmed_status or confirmed_status in CANCEL_CONFIRMED_STATUSES):
        raw["confirmed_status"] = confirmed_status
        return True, raw
    if confirmed_status in CANCEL_CONFIRMED_STATUSES:
        raw["confirmed_status"] = confirmed_status
        return True, raw
    raw["confirmed_status"] = confirmed_status
    return False, raw


def _position_contract(pos: Any) -> str:
    return _norm_contract(getattr(pos, "option_symbol", "") or getattr(pos, "contract", "") or getattr(pos, "symbol", ""))


def _position_id(pos: Any) -> str:
    return _norm(getattr(pos, "position_id", "") or getattr(pos, "id", ""))


def _pending_identity(pos: Any) -> tuple[str, str]:
    return (
        _norm(getattr(pos, "pending_exit_local_order_id", "")),
        _norm(getattr(pos, "pending_exit_broker_order_id", "")),
    )


def quote_health(pos: Any, *, stale_sec: int = QUOTE_STALE_WARN_SEC) -> dict:
    option_age = _dt_age_seconds(getattr(pos, "last_option_quote_update_ts", None) or getattr(pos, "last_quote_update_ts", None))
    underlying_age = _dt_age_seconds(getattr(pos, "last_underlying_quote_update_ts", None) or getattr(pos, "last_quote_update_ts", None))
    option_missing = getattr(pos, "last_option_quote_missing_ts", None) is not None
    underlying_missing = getattr(pos, "last_underlying_quote_missing_ts", None) is not None
    return {
        "option_quote_age_sec": option_age,
        "underlying_quote_age_sec": underlying_age,
        "option_quote_stale": option_age is None or option_age > stale_sec or bool(option_missing),
        "underlying_quote_stale": underlying_age is None or underlying_age > stale_sec or bool(underlying_missing),
        "stale_sec": stale_sec,
    }


@dataclass
class RecoveryAction:
    action: str
    reason: str
    position_id: str = ""
    local_order_id: str = ""
    broker_order_id: str = ""
    details: dict = field(default_factory=dict)


def recover_exit_position(pos: Any, *, broker: Any, exit_engine: Any = None, osm: Any = None) -> RecoveryAction:
    pid = _position_id(pos)
    local_id, pending_broker_id = _pending_identity(pos)
    contract = _position_contract(pos)
    qh = quote_health(pos)

    if not broker or not pid:
        return RecoveryAction("NOOP", "missing_broker_or_position_id", pid, local_id, pending_broker_id, {"quote_health": qh})

    # Exact broker identity path.
    if pending_broker_id:
        raw = _get_order(broker, pending_broker_id)
        if raw:
            st = _status(raw)
            if st in OPEN_BROKER_STATUSES:
                if exit_engine and hasattr(exit_engine, "set_pending_exit_order"):
                    exit_engine.set_pending_exit_order(
                        pid,
                        local_order_id=local_id,
                        broker_order_id=pending_broker_id,
                        qty=int(getattr(pos, "pending_exit_qty", 0) or 0),
                        reason="autonomous_recovery_confirmed_broker_open_exit",
                    )
                return RecoveryAction("CONFIRMED_OPEN", "broker_order_still_open", pid, local_id, pending_broker_id, {"status": st, "quote_health": qh})
            if st == "filled":
                filled_qty = _qty(raw) or int(getattr(pos, "pending_exit_qty", 0) or 0)
                fill_price = None
                for key in ("avg_fill_price", "average_fill_price", "fill_price", "filled_avg_price", "price"):
                    try:
                        if raw.get(key) not in (None, ""):
                            fill_price = float(raw.get(key))
                            break
                    except Exception:
                        pass
                if exit_engine and hasattr(exit_engine, "mark_position_closed"):
                    exit_engine.mark_position_closed(
                        pid,
                        reason="AUTONOMOUS_RECOVERY_BROKER_FILLED",
                        qty_filled=filled_qty,
                        fill_price=fill_price,
                        local_order_id=local_id,
                        broker_order_id=pending_broker_id,
                        cumulative_filled=filled_qty,
                        reconciled=True,
                    )
                return RecoveryAction("MARKED_CLOSED", "broker_order_filled", pid, local_id, pending_broker_id, {"status": st, "filled_qty": filled_qty, "quote_health": qh})
            if st in TERMINAL_BROKER_STATUSES:
                if exit_engine and hasattr(exit_engine, "mark_exit_replacement_safe"):
                    exit_engine.mark_exit_replacement_safe(
                        pid,
                        reason=f"autonomous_recovery_broker_terminal_{st}",
                        local_order_id=local_id,
                        broker_order_id=pending_broker_id,
                        reconciled=True,
                    )
                elif exit_engine and hasattr(exit_engine, "clear_exit_in_flight"):
                    exit_engine.clear_exit_in_flight(
                        pid,
                        reason=f"autonomous_recovery_broker_terminal_{st}",
                        local_order_id=local_id,
                        broker_order_id=pending_broker_id,
                        reconciled=True,
                    )
                return RecoveryAction("REPLACEMENT_SAFE", "broker_order_terminal", pid, local_id, pending_broker_id, {"status": st, "quote_health": qh})
            return RecoveryAction("NOOP", "broker_order_ambiguous_status", pid, local_id, pending_broker_id, {"status": st, "quote_health": qh})

    # Missing broker id: scan open orders for matching exit order.
    open_orders = _list_open_orders(broker)
    matches = []
    for raw in open_orders:
        if contract and _contract(raw) != contract:
            continue
        if not _is_exit_like(raw):
            continue
        st = _status(raw)
        if st and st not in OPEN_BROKER_STATUSES:
            continue
        bid = _broker_order_id(raw)
        if bid:
            matches.append((bid, raw))

    if len(matches) == 1:
        recovered_broker_id, raw = matches[0]
        if exit_engine and hasattr(exit_engine, "set_pending_exit_order"):
            exit_engine.set_pending_exit_order(
                pid,
                local_order_id=local_id,
                broker_order_id=recovered_broker_id,
                qty=int(getattr(pos, "pending_exit_qty", 0) or _qty(raw) or 0),
                reason="autonomous_recovery_matched_live_exit_order",
            )
        return RecoveryAction("RECOVERED_BROKER_ID", "matched_single_live_exit_order", pid, local_id, recovered_broker_id, {"contract": contract, "quote_health": qh})

    if len(matches) > 1:
        cancel_results = []
        all_canceled = True
        for bid, raw in matches:
            ok, proof = _cancel_order_with_proof(broker, bid)
            cancel_results.append({"broker_order_id": bid, "ok": ok, "proof": proof})
            if not ok:
                all_canceled = False
        if all_canceled and exit_engine and hasattr(exit_engine, "mark_exit_replacement_safe"):
            exit_engine.mark_exit_replacement_safe(
                pid,
                reason="autonomous_recovery_multiple_live_exit_orders_canceled",
                local_order_id=local_id,
                broker_order_id="",
                reconciled=True,
            )
            return RecoveryAction("REPLACEMENT_SAFE", "multiple_live_exit_orders_canceled_with_proof", pid, local_id, "", {"match_count": len(matches), "cancel_results": cancel_results, "quote_health": qh})
        return RecoveryAction("NOOP", "multiple_live_exit_orders_cancel_not_proven", pid, local_id, "", {"match_count": len(matches), "cancel_results": cancel_results, "quote_health": qh})

    # Negative proof: no matching open sell-to-close order currently at broker.
    if exit_engine and hasattr(exit_engine, "mark_exit_replacement_safe"):
        exit_engine.mark_exit_replacement_safe(
            pid,
            reason="autonomous_recovery_no_matching_live_exit_order",
            local_order_id=local_id,
            broker_order_id="",
            reconciled=True,
        )
        return RecoveryAction("REPLACEMENT_SAFE", "no_matching_live_exit_order", pid, local_id, "", {"contract": contract, "quote_health": qh})

    return RecoveryAction("NOOP", "no_replacement_hook_available", pid, local_id, "", {"contract": contract, "quote_health": qh})


def recover_exit_engine(exit_engine: Any, *, broker: Any, osm: Any = None, max_positions: int = 10) -> list[RecoveryAction]:
    if exit_engine is None or broker is None:
        return []
    try:
        if hasattr(exit_engine, "active_positions"):
            positions = list(exit_engine.active_positions())
        else:
            positions = [p for p in getattr(exit_engine, "_positions", []) if not getattr(p, "closed", False)]
    except Exception:
        positions = []

    actions: list[RecoveryAction] = []
    for pos in positions[:max_positions]:
        if not (getattr(pos, "exit_identity_quarantine", False) or getattr(pos, "last_callback_identity_missing", False) or getattr(pos, "exit_in_flight", False)):
            continue
        try:
            actions.append(recover_exit_position(pos, broker=broker, exit_engine=exit_engine, osm=osm))
        except Exception as exc:
            log.exception("autonomous recovery failed for pos=%s: %s", _position_id(pos), exc)
            actions.append(RecoveryAction("ERROR", str(exc), _position_id(pos)))
    return actions


def scan_quote_staleness(exit_engine: Any, *, stale_sec: int = QUOTE_STALE_WARN_SEC) -> list[dict]:
    """Return active positions whose option/underlying quote health is stale."""
    out = []
    if exit_engine is None:
        return out
    try:
        positions = list(exit_engine.active_positions()) if hasattr(exit_engine, "active_positions") else list(getattr(exit_engine, "_positions", []))
    except Exception:
        positions = []
    for pos in positions:
        if getattr(pos, "closed", False):
            continue
        qh = quote_health(pos, stale_sec=stale_sec)
        if qh.get("option_quote_stale") or qh.get("underlying_quote_stale"):
            out.append({
                "position_id": _position_id(pos),
                "ticker": getattr(pos, "ticker", ""),
                "contract": _position_contract(pos),
                **qh,
            })
    return out


__all__ = ["RecoveryAction", "recover_exit_position", "recover_exit_engine", "scan_quote_staleness", "quote_health"]
