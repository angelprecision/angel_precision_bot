"""
ap.exit_autonomous_recovery
===========================
Conservative autonomous recovery helper for APExitEngine quarantine/stale in-flight states.

This module is intentionally NOT a strategy layer and NOT a blind retry layer.
It exists to close the gap when the broker reconciler is missing/dead/delayed.

Safety rules
------------
1. Never clear an exit quarantine by time alone.
2. Never authorize replacement if a matching live broker exit order is found.
3. Prefer fill/close truth when broker/DB evidence exists.
4. Only call mark_exit_replacement_safe() after negative broker proof.
5. If broker truth is ambiguous, alert/no-op.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("ap.exit_autonomous_recovery")

OPEN_BROKER_STATUSES = {"open", "pending", "accepted", "submitted", "queued", "working", "acknowledged", "partially_filled"}
TERMINAL_BROKER_STATUSES = {"filled", "canceled", "cancelled", "rejected", "expired"}


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
    for key in ("qty", "quantity", "order_qty", "remaining_qty", "remaining_quantity", "filled_qty", "filled_quantity"):
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


def _position_contract(pos: Any) -> str:
    return _norm_contract(getattr(pos, "option_symbol", "") or getattr(pos, "contract", "") or getattr(pos, "symbol", ""))


def _position_id(pos: Any) -> str:
    return _norm(getattr(pos, "position_id", "") or getattr(pos, "id", ""))


def _pending_identity(pos: Any) -> tuple[str, str]:
    return (
        _norm(getattr(pos, "pending_exit_local_order_id", "")),
        _norm(getattr(pos, "pending_exit_broker_order_id", "")),
    )


@dataclass
class RecoveryAction:
    action: str
    reason: str
    position_id: str = ""
    local_order_id: str = ""
    broker_order_id: str = ""
    details: dict = field(default_factory=dict)


def recover_exit_position(pos: Any, *, broker: Any, exit_engine: Any = None, osm: Any = None) -> RecoveryAction:
    """Attempt one conservative autonomous recovery for a single position.

    Returns a RecoveryAction describing the outcome. This function only mutates
    the exit engine when broker evidence is strong enough.
    """
    pid = _position_id(pos)
    local_id, pending_broker_id = _pending_identity(pos)
    contract = _position_contract(pos)

    if not broker or not pid:
        return RecoveryAction("NOOP", "missing_broker_or_position_id", pid, local_id, pending_broker_id)

    # 1) If we have a broker id, ask broker for that exact order first.
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
                return RecoveryAction("CONFIRMED_OPEN", "broker_order_still_open", pid, local_id, pending_broker_id, {"status": st})
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
                return RecoveryAction("MARKED_CLOSED", "broker_order_filled", pid, local_id, pending_broker_id, {"status": st, "filled_qty": filled_qty})
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
                return RecoveryAction("REPLACEMENT_SAFE", "broker_order_terminal", pid, local_id, pending_broker_id, {"status": st})
            return RecoveryAction("NOOP", "broker_order_ambiguous_status", pid, local_id, pending_broker_id, {"status": st})

    # 2) Missing broker id: look for a live sell-to-close order matching contract.
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
        return RecoveryAction("RECOVERED_BROKER_ID", "matched_single_live_exit_order", pid, local_id, recovered_broker_id, {"contract": contract})

    if len(matches) > 1:
        return RecoveryAction("NOOP", "multiple_live_exit_order_matches_manual_review", pid, local_id, "", {"match_count": len(matches), "contract": contract})

    # 3) Negative proof: no matching open sell-to-close order currently at broker.
    # Allow exactly one replacement through the identity-bound exit engine hook.
    if exit_engine and hasattr(exit_engine, "mark_exit_replacement_safe"):
        exit_engine.mark_exit_replacement_safe(
            pid,
            reason="autonomous_recovery_no_matching_live_exit_order",
            local_order_id=local_id,
            broker_order_id="",
            reconciled=True,
        )
        return RecoveryAction("REPLACEMENT_SAFE", "no_matching_live_exit_order", pid, local_id, "", {"contract": contract})

    return RecoveryAction("NOOP", "no_replacement_hook_available", pid, local_id, "", {"contract": contract})


def recover_exit_engine(exit_engine: Any, *, broker: Any, osm: Any = None, max_positions: int = 10) -> list[RecoveryAction]:
    """Recover quarantined/stale in-flight positions directly from broker truth."""
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


__all__ = ["RecoveryAction", "recover_exit_position", "recover_exit_engine"]
