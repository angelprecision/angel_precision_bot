"""
ap.exit_autonomous_recovery
===========================
Conservative autonomous recovery helper for APExitEngine quarantine/stale in-flight states.

Safety rules
------------
1. Never clear an exit quarantine by time alone.
2. Never authorize replacement if ANY matching live broker exit order is found.
3. Prefer fill/close truth when broker/DB evidence exists.
4. On ambiguous matching live exits, try broker cancel first and only unlock after
   cancel proof or terminal broker confirmation.
5. If broker truth is ambiguous, alert/no-op.
6. Quote staleness is reported as a kill-switch signal for new entries, not a
   reason to guess exit truth.
"""

from __future__ import annotations

import logging
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from ap.utils import parse_aware_utc_timestamp

log = logging.getLogger("ap.exit_autonomous_recovery")

OPEN_BROKER_STATUSES = {"open", "pending", "accepted", "submitted", "queued", "working", "acknowledged", "partially_filled"}
TERMINAL_BROKER_STATUSES = {"filled", "canceled", "cancelled", "rejected", "expired"}
CANCEL_CONFIRMED_STATUSES = {"canceled", "cancelled", "rejected", "expired"}
QUOTE_STALE_WARN_SEC = int(os.getenv("EXIT_RECOVERY_QUOTE_STALE_SEC", "30"))
CANCEL_PROOF_RETRIES = int(os.getenv("EXIT_RECOVERY_CANCEL_RETRIES", "3"))
CANCEL_PROOF_DELAY_SEC = float(os.getenv("EXIT_RECOVERY_CANCEL_DELAY_SEC", "1.0"))


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
                if isinstance(val, bool):
                    continue
                parsed = float(val)
                if math.isfinite(parsed) and parsed > 0 and parsed.is_integer():
                    return int(parsed)
        except Exception:
            pass
    return 0


def _filled_qty(raw: dict) -> int:
    """Read only explicit broker execution quantity; never requested qty."""
    for key in (
        "filled_qty",
        "filled_quantity",
        "cumulative_filled_qty",
        "exec_quantity",
    ):
        if key not in raw:
            continue
        value = raw.get(key)
        if value in (None, "") or isinstance(value, bool):
            return 0
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return 0
        if not math.isfinite(parsed) or parsed <= 0 or not parsed.is_integer():
            return 0
        return int(parsed)
    return 0


_BROKER_FILL_TIMESTAMP_KEYS = (
    "filled_ts",
    "filled_at",
    "fill_ts",
    "last_fill_date",
    "transaction_date",
)


def _broker_fill_timestamp(raw: dict) -> datetime | None:
    """Return explicit broker fill time; never manufacture recovery time."""
    for key in _BROKER_FILL_TIMESTAMP_KEYS:
        if key in raw:
            return parse_aware_utc_timestamp(raw.get(key))
    return None


def _position_value(pos: Any, name: str, default: Any = None) -> Any:
    if isinstance(pos, dict):
        return pos.get(name, default)
    return getattr(pos, name, default)


def _position_remaining(pos: Any) -> Optional[int]:
    raw = _position_value(pos, "quantity_remaining")
    if raw in (None, "") or isinstance(raw, bool):
        return None
    try:
        parsed = float(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or parsed <= 0 or not parsed.is_integer():
        return None
    return int(parsed)


def _broker_position_qty(raw: dict) -> Optional[int]:
    """Read broker position quantity without converting malformed scalars."""
    for key in ("quantity", "qty", "long_quantity", "short_quantity"):
        if key not in raw:
            continue
        value = raw.get(key)
        if value in (None, "") or isinstance(value, bool):
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(parsed) or not parsed.is_integer():
            return None
        return abs(int(parsed))
    return None


def _is_exit_like(raw: dict) -> bool:
    text = " ".join(str(raw.get(k) or "") for k in (
        "side", "action", "instruction", "order_action", "transaction_type", "trade_action", "type", "description", "memo", "notes"
    )).lower()
    compact = text.replace("_", "").replace("-", "").replace(" ", "")
    return "selltoclose" in compact or compact == "stc" or "sell to close" in text


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
                log.warning("broker.%s returned None during autonomous recovery", method_name)
                continue
            if isinstance(result, dict):
                for key in ("orders", "data", "results", "items"):
                    if isinstance(result.get(key), list):
                        return [dict(x) for x in result[key] if isinstance(x, dict)]
                return [result]
            if isinstance(result, list):
                return [dict(x) for x in result if isinstance(x, dict)]
        except Exception as exc:
            log.warning("broker.%s failed during autonomous recovery: %s", method_name, exc)
    return []


def _get_order(broker: Any, broker_order_id: str) -> Optional[dict]:
    if not broker_order_id:
        return None
    method = getattr(broker, "get_order", None)
    if not callable(method):
        log.warning("broker.get_order missing during autonomous recovery")
        return None
    try:
        raw = method(broker_order_id)
        if isinstance(raw, dict):
            return dict(raw)
        log.warning("broker.get_order(%s) returned non-dict payload: %r", broker_order_id, raw)
        return None
    except Exception as exc:
        log.warning("broker.get_order(%s) failed: %s", broker_order_id, exc)
        return None


def _matching_open_exit_orders(broker: Any, contract: str, *, exclude_broker_id: str = "") -> list[tuple[str, dict]]:
    matches: list[tuple[str, dict]] = []
    for raw in _list_open_orders(broker):
        if contract and _contract(raw) != contract:
            continue
        if not _is_exit_like(raw):
            continue
        st = _status(raw)
        if st and st not in OPEN_BROKER_STATUSES:
            continue
        bid = _broker_order_id(raw)
        if not bid or (exclude_broker_id and bid == exclude_broker_id):
            continue
        matches.append((bid, raw))
    return matches


def _cancel_order_with_proof(
    broker: Any,
    broker_order_id: str,
    *,
    max_retries: int = CANCEL_PROOF_RETRIES,
    retry_delay: float = CANCEL_PROOF_DELAY_SEC,
) -> tuple[bool, dict]:
    """Attempt broker cancel and wait briefly for terminal/canceled proof."""
    if not broker_order_id:
        return False, {"error": "missing_broker_order_id"}
    cancel = getattr(broker, "cancel_order", None)
    if not callable(cancel):
        return False, {"error": "broker_cancel_order_missing", "broker_order_id": broker_order_id}
    try:
        raw = cancel(broker_order_id)
        raw = dict(raw) if isinstance(raw, dict) else {"raw": raw}
    except Exception as exc:
        return False, {"error": str(exc), "broker_order_id": broker_order_id}

    status_val = _status(raw)
    ok_flag = bool(raw.get("ok"))
    confirmed_status = status_val
    confirmed_payload: Optional[dict] = None

    for attempt in range(max(1, int(max_retries))):
        confirmed = _get_order(broker, broker_order_id)
        confirmed_payload = confirmed
        confirmed_status = _status(confirmed or {}) if confirmed else status_val
        if confirmed_status in CANCEL_CONFIRMED_STATUSES:
            raw["confirmed_status"] = confirmed_status
            raw["confirmation_attempts"] = attempt + 1
            return True, raw
        if attempt < max_retries - 1:
            time.sleep(max(0.0, float(retry_delay)))

    # If broker accepted cancel but status has not propagated, do NOT unlock.
    raw["confirmed_status"] = confirmed_status
    raw["confirmation_attempts"] = max_retries
    raw["confirmed_payload"] = confirmed_payload
    raw["ok_flag"] = ok_flag
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


def _mark_replacement_safe(exit_engine: Any, pid: str, *, reason: str, local_id: str, broker_id: str, details: dict) -> RecoveryAction:
    if exit_engine and hasattr(exit_engine, "mark_exit_replacement_safe"):
        exit_engine.mark_exit_replacement_safe(
            pid,
            reason=reason,
            local_order_id=local_id,
            broker_order_id=broker_id,
            reconciled=True,
        )
        return RecoveryAction("REPLACEMENT_SAFE", reason, pid, local_id, broker_id, details)
    if exit_engine and hasattr(exit_engine, "clear_exit_in_flight"):
        exit_engine.clear_exit_in_flight(
            pid,
            reason=reason,
            local_order_id=local_id,
            broker_order_id=broker_id,
            reconciled=True,
        )
        return RecoveryAction("CLEARED_IN_FLIGHT", reason, pid, local_id, broker_id, details)
    return RecoveryAction("NOOP", "no_replacement_or_clear_hook_available", pid, local_id, broker_id, details)


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
                        qty=_qty({"qty": getattr(pos, "pending_exit_qty", 0)}),
                        reason="autonomous_recovery_confirmed_broker_open_exit",
                    )
                return RecoveryAction("CONFIRMED_OPEN", "broker_order_still_open", pid, local_id, pending_broker_id, {"status": st, "quote_health": qh})
            if st == "filled":
                filled_qty = _filled_qty(raw)
                fill_price = None
                for key in ("avg_fill_price", "average_fill_price", "fill_price", "filled_avg_price"):
                    try:
                        if raw.get(key) not in (None, ""):
                            candidate = float(raw.get(key))
                            if not math.isfinite(candidate) or candidate <= 0:
                                continue
                            fill_price = candidate
                            break
                    except Exception:
                        pass
                if filled_qty <= 0 or fill_price is None:
                    return RecoveryAction(
                        "NOOP",
                        "autonomous_recovery_broker_filled_exact_economics_missing",
                        pid,
                        local_id,
                        pending_broker_id,
                        {
                            "status": st,
                            "filled_qty": filled_qty,
                            "fill_price": fill_price,
                            "quote_health": qh,
                            "requires_exact_exit_fill": True,
                        },
                    )
                fill_ts = _broker_fill_timestamp(raw)
                entry_ts = parse_aware_utc_timestamp(
                    _position_value(pos, "opened_at")
                    or _position_value(pos, "entry_ts")
                )
                if fill_ts is None or entry_ts is None or fill_ts < entry_ts:
                    return RecoveryAction(
                        "NOOP",
                        "autonomous_recovery_broker_filled_timestamp_unproven",
                        pid,
                        local_id,
                        pending_broker_id,
                        {
                            "status": st,
                            "filled_qty": filled_qty,
                            "fill_price": fill_price,
                            "filled_ts": fill_ts.isoformat() if fill_ts else None,
                            "entry_ts": entry_ts.isoformat() if entry_ts else None,
                            "quote_health": qh,
                            "requires_aware_broker_fill_timestamp": True,
                        },
                    )
                remaining_qty = _position_remaining(pos)
                if remaining_qty is None or filled_qty != remaining_qty:
                    return RecoveryAction(
                        "NOOP",
                        "autonomous_recovery_broker_filled_quantity_mismatch",
                        pid,
                        local_id,
                        pending_broker_id,
                        {
                            "status": st,
                            "filled_qty": filled_qty,
                            "remaining_qty": remaining_qty,
                            "fill_price": fill_price,
                            "filled_ts": fill_ts.isoformat(),
                            "quote_health": qh,
                            "requires_exact_remaining_coverage": True,
                        },
                    )
                mark = getattr(exit_engine, "mark_position_closed", None) if exit_engine else None
                if not callable(mark):
                    return RecoveryAction(
                        "NOOP",
                        "autonomous_recovery_broker_filled_close_hook_missing",
                        pid,
                        local_id,
                        pending_broker_id,
                        {"status": st, "quote_health": qh},
                    )
                mark_result = mark(
                    pid,
                    reason="AUTONOMOUS_RECOVERY_BROKER_FILLED",
                    qty_filled=filled_qty,
                    fill_price=fill_price,
                    local_order_id=local_id,
                    broker_order_id=pending_broker_id,
                    cumulative_filled=filled_qty,
                    broker_exit_order_id=pending_broker_id,
                    broker_exit_filled_qty=filled_qty,
                    broker_exit_fill_ts=fill_ts,
                    reconciled=True,
                )
                if mark_result is False:
                    return RecoveryAction(
                        "NOOP",
                        "autonomous_recovery_broker_filled_close_rejected",
                        pid,
                        local_id,
                        pending_broker_id,
                        {"status": st, "quote_health": qh},
                    )
                return RecoveryAction("MARKED_CLOSED", "broker_order_filled", pid, local_id, pending_broker_id, {"status": st, "filled_qty": filled_qty, "quote_health": qh})
            if st in TERMINAL_BROKER_STATUSES:
                # CRITICAL safety: terminal status for the old pending id is NOT enough.
                # Scan broker for a different live exit on the same contract before allowing replacement.
                other_matches = _matching_open_exit_orders(broker, contract, exclude_broker_id=pending_broker_id)
                if len(other_matches) == 1:
                    other_bid, other_raw = other_matches[0]
                    if exit_engine and hasattr(exit_engine, "set_pending_exit_order"):
                        exit_engine.set_pending_exit_order(
                            pid,
                            local_order_id=local_id,
                            broker_order_id=other_bid,
                            qty=_qty(
                                {
                                    "qty": getattr(pos, "pending_exit_qty", 0)
                                    or _qty(other_raw)
                                }
                            ),
                            reason="autonomous_recovery_found_different_open_exit",
                        )
                    return RecoveryAction("CONFIRMED_OPEN", "different_broker_exit_still_open", pid, local_id, other_bid, {"old_status": st, "contract": contract, "quote_health": qh})
                if len(other_matches) > 1:
                    return RecoveryAction("NOOP", "multiple_different_open_exits_block_replacement", pid, local_id, pending_broker_id, {"old_status": st, "matches": [m[0] for m in other_matches], "quote_health": qh})
                return _mark_replacement_safe(
                    exit_engine,
                    pid,
                    reason=f"autonomous_recovery_broker_terminal_{st}",
                    local_id=local_id,
                    broker_id=pending_broker_id,
                    details={"status": st, "quote_health": qh},
                )
            return RecoveryAction("NOOP", "broker_order_ambiguous_status", pid, local_id, pending_broker_id, {"status": st, "quote_health": qh})

    # Missing broker id: scan open orders for matching exit order.
    matches = _matching_open_exit_orders(broker, contract)

    if len(matches) == 1:
        recovered_broker_id, raw = matches[0]
        if exit_engine and hasattr(exit_engine, "set_pending_exit_order"):
            exit_engine.set_pending_exit_order(
                pid,
                local_order_id=local_id,
                broker_order_id=recovered_broker_id,
                qty=_qty(
                    {
                        "qty": getattr(pos, "pending_exit_qty", 0)
                        or _qty(raw)
                    }
                ),
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
        if all_canceled:
            return _mark_replacement_safe(
                exit_engine,
                pid,
                reason="autonomous_recovery_multiple_live_exit_orders_canceled",
                local_id=local_id,
                broker_id="",
                details={"match_count": len(matches), "cancel_results": cancel_results, "quote_health": qh},
            )
        return RecoveryAction("NOOP", "multiple_live_exit_orders_cancel_not_proven", pid, local_id, "", {"match_count": len(matches), "cancel_results": cancel_results, "quote_health": qh})

    # Negative proof: no matching open sell-to-close order currently at broker.
    # A flat broker position is not an EXIT fill record: it carries neither an
    # authoritative price nor an exact filled quantity/order identity. Hold for
    # the reconciler/manual-close path rather than manufacturing a close.
    try:
        _broker_positions = broker.list_positions() if hasattr(broker, "list_positions") else []
        _contract_held = False
        _position_truth_unknown = False
        for p in (_broker_positions or []):
            if str(p.get("symbol") or p.get("contract") or "").upper() != str(contract or "").upper():
                continue
            _position_qty = _broker_position_qty(p)
            if _position_qty is None:
                _position_truth_unknown = True
                break
            if _position_qty != 0:
                _contract_held = True
        if _position_truth_unknown:
            return RecoveryAction(
                "NOOP",
                "autonomous_recovery_broker_position_quantity_unusable",
                pid,
                local_id,
                "",
                {
                    "contract": contract,
                    "quote_health": qh,
                    "requires_exact_exit_fill": True,
                },
            )
        if not _contract_held and contract:
            return RecoveryAction(
                "NOOP",
                "autonomous_recovery_contract_flat_requires_exact_exit_fill",
                pid, local_id, "",
                {
                    "contract": contract,
                    "quote_health": qh,
                    "source": "negative_proof_position_check",
                    "requires_exact_exit_fill": True,
                },
            )
    except Exception as _bp_exc:
        log.debug("exit_autonomous_recovery: broker position check failed (non-fatal): %s", _bp_exc)

    return _mark_replacement_safe(
        exit_engine,
        pid,
        reason="autonomous_recovery_no_matching_live_exit_order",
        local_id=local_id,
        broker_id="",
        details={"contract": contract, "quote_health": qh},
    )


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
