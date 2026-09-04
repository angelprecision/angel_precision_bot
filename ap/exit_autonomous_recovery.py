"""
ap.exit_autonomous_recovery
===========================
Conservative autonomous recovery helper for APExitEngine quarantine/stale in-flight states.

Safety rules
------------
1. Never clear an exit quarantine by time alone.
2. Never authorize replacement if ANY matching live broker exit order is found.
3. Prefer fill/close truth when broker/DB evidence exists.
4. This containment path never cancels or authorizes a replacement from a
   partial or negative lookup; canonical reconciliation owns that decision.
5. If broker truth is ambiguous, alert/no-op.
6. Quote staleness is reported as a kill-switch signal for new entries, not a
   reason to guess exit truth.
"""

from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from ap.exit_safety import (
    _normalize_contract,
    is_valid_exact_occ_contract,
    resolve_exit_broker_truth,
)

log = logging.getLogger("ap.exit_autonomous_recovery")

OPEN_BROKER_STATUSES = {"open", "pending", "accepted", "submitted", "queued", "working", "acknowledged", "partially_filled"}
TERMINAL_BROKER_STATUSES = {"filled", "canceled", "cancelled", "rejected", "expired"}
CANCEL_CONFIRMED_STATUSES = {"canceled", "cancelled", "rejected", "expired"}
BROKER_FLAT_RECONCILIATION_PENDING = "BROKER_FLAT_CLOSE_PENDING"
QUOTE_STALE_WARN_SEC = int(os.getenv("EXIT_RECOVERY_QUOTE_STALE_SEC", "30"))
CANCEL_PROOF_RETRIES = int(os.getenv("EXIT_RECOVERY_CANCEL_RETRIES", "3"))
CANCEL_PROOF_DELAY_SEC = float(os.getenv("EXIT_RECOVERY_CANCEL_DELAY_SEC", "1.0"))


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _norm_contract(value: Any) -> str:
    return _normalize_contract(value)


def _status(raw: dict) -> str:
    return _norm(raw.get("status") or raw.get("Status") or raw.get("state") or raw.get("order_status")).lower()


def _broker_order_id(raw: dict) -> str:
    return _norm(raw.get("broker_order_id") or raw.get("order_id") or raw.get("id") or raw.get("orderId"))


def _contract(raw: dict) -> str:
    records = [raw]
    nested = raw.get("raw") if isinstance(raw, dict) else None
    if isinstance(nested, dict):
        records.append(nested)
    exact_values: set[str] = set()
    underlying_values: set[str] = set()
    invalid = False
    for record in records:
        for key in (
            "option_symbol",
            "optionSymbol",
            "contract",
            "instrument",
            "option_contract",
            "optionContract",
            "symbol",
        ):
            if key not in record or record.get(key) in (None, ""):
                continue
            value = record.get(key)
            normalized = _norm_contract(value)
            if is_valid_exact_occ_contract(value):
                exact_values.add(normalized)
            elif key in {"symbol", "instrument"} and re.fullmatch(r"[A-Z0-9.]{1,6}", normalized):
                underlying_values.add(normalized)
            else:
                invalid = True
    if len(exact_values) != 1 or invalid:
        return ""
    exact = next(iter(exact_values))
    root = exact[:-15]
    if any(value != root for value in underlying_values):
        return ""
    return exact


def _strict_qty(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        quantity = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if (
        not quantity.is_finite()
        or quantity != quantity.to_integral_value()
        or quantity < 0
    ):
        return None
    return int(quantity)


def _qty(raw: dict) -> Optional[int]:
    for key in ("qty", "quantity", "order_qty", "remaining_qty", "remaining_quantity", "filled_qty", "filled_quantity", "exec_quantity"):
        if key not in raw or raw.get(key) in (None, ""):
            continue
        return _strict_qty(raw.get(key))
    return None


def _is_exit_like(raw: dict) -> bool:
    values: list[str] = []
    records = [raw]
    nested = raw.get("raw") if isinstance(raw, dict) else None
    if isinstance(nested, dict):
        records.append(nested)
    for record in records:
        values.extend(
            str(record.get(key) or "").strip().lower()
            for key in (
                "side",
                "action",
                "instruction",
                "order_action",
                "transaction_type",
                "trade_action",
                "position_effect",
            )
        )
    compact = {re.sub(r"[\s_-]+", "", value) for value in values if value}
    if compact.intersection({"selltoclose", "stc"}):
        return True
    return "sell" in compact and bool(compact.intersection({"close", "closing"}))


def _has_structured_order_direction(raw: dict) -> bool:
    records = [raw]
    nested = raw.get("raw") if isinstance(raw, dict) else None
    if isinstance(nested, dict):
        records.append(nested)
    return any(
        str(record.get(key) or "").strip()
        for record in records
        for key in (
            "side",
            "action",
            "instruction",
            "order_action",
            "transaction_type",
            "trade_action",
            "position_effect",
        )
    )


def _has_order_instrument_identity(raw: dict) -> bool:
    """Accept valid option or underlying identities in account-wide order pages."""
    records = [raw]
    nested = raw.get("raw") if isinstance(raw, dict) else None
    if isinstance(nested, dict):
        records.append(nested)
    saw_identity = False
    for record in records:
        for key in (
            "option_symbol",
            "optionSymbol",
            "contract",
            "instrument",
            "option_contract",
            "optionContract",
            "symbol",
        ):
            if key not in record or record.get(key) in (None, ""):
                continue
            value = record.get(key)
            if is_valid_exact_occ_contract(value):
                saw_identity = True
                continue
            if key in {"symbol", "instrument"} and re.fullmatch(r"[A-Z0-9.]{1,6}", _norm_contract(value)):
                saw_identity = True
                continue
            return False
    return saw_identity


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


def _coerce_order_row(raw: Any) -> Optional[dict]:
    if not isinstance(raw, dict):
        return None
    if isinstance(raw.get("order"), dict):
        row = dict(raw["order"])
        for key in ("status", "state", "order_status"):
            if key not in row and raw.get(key) not in (None, ""):
                row[key] = raw[key]
        return row
    return dict(raw)


def _explicit_order_error(raw: Any) -> bool:
    row = _coerce_order_row(raw)
    if row is None:
        return True
    if _status(row) in {"error", "failed", "failure", "unavailable"}:
        return True
    return any(
        key in row and row.get(key) not in (None, "")
        for key in ("error", "errors", "message", "reason")
    )


def _looks_like_order_row(row: dict) -> bool:
    """Require a recognizable broker order before accepting a row."""
    if not isinstance(row, dict) or not _broker_order_id(row):
        return False
    if _status(row) not in OPEN_BROKER_STATUSES | TERMINAL_BROKER_STATUSES:
        return False
    if not _has_structured_order_direction(row) or not _has_order_instrument_identity(row):
        return False
    quantity = _qty(row)
    return quantity is not None and quantity > 0


def _parse_order_payload(payload: Any) -> tuple[str, list[dict]]:
    if payload is None:
        return "unavailable", []
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        if _explicit_order_error(payload):
            return "malformed", []
        node = payload.get("orders")
        if node is None:
            for key in ("data", "results", "items"):
                if key in payload:
                    node = payload[key]
                    break
        if node is None:
            # A single normalized order row is also accepted.
            row = _coerce_order_row(payload)
            return ("available", [row]) if row is not None and _looks_like_order_row(row) else ("malformed", [])
        if isinstance(node, dict):
            if not node:
                return "malformed", []
            node = node.get("order")
        if node is None or node == "null":
            return "malformed", []
        rows = node if isinstance(node, list) else [node]
    else:
        return "malformed", []

    if not isinstance(rows, list):
        return "malformed", []
    normalized: list[dict] = []
    for raw in rows:
        row = _coerce_order_row(raw)
        if row is None or not _looks_like_order_row(row):
            return "malformed", []
        normalized.append(row)
    return "available", normalized


def _list_open_orders_truth(broker: Any) -> tuple[str, list[dict]]:
    """Return order-list truth without laundering lookup failure into an empty page."""
    for method_name in ("list_orders", "list_open_orders", "get_open_orders", "orders"):
        method = getattr(broker, method_name, None)
        if not callable(method):
            continue
        try:
            try:
                result = method(status="open")
            except TypeError:
                result = method()
        except Exception as exc:
            log.warning("broker.%s failed during autonomous recovery: %s", method_name, exc)
            return "unavailable", []
        state, rows = _parse_order_payload(result)
        if state != "available":
            log.warning("broker.%s returned %s during autonomous recovery", method_name, state)
        return state, rows

    # Legacy adapters may expose only the raw endpoint.  This fallback is kept
    # after the authoritative list-orders interface so current TradierBroker
    # pagination/includeTags/error propagation remains in force.
    raw_get_declared = getattr(type(broker), "_get", None)
    account_value = str(
        getattr(getattr(broker, "cfg", None), "account_id", None) or ""
    ).strip()
    if callable(raw_get_declared) and account_value:
        try:
            result = broker._get(f"/v1/accounts/{account_value}/orders")
        except Exception as exc:
            log.warning("broker._get orders failed during autonomous recovery: %s", exc)
            return "unavailable", []
        state, rows = _parse_order_payload(result)
        if state != "available":
            log.warning("broker._get orders returned %s during autonomous recovery", state)
        return state, rows
    return "unavailable", []


def _list_open_orders(broker: Any) -> list[dict]:
    # Compatibility wrapper for callers that only need rows.  Recovery paths
    # use _list_open_orders_truth so an empty list is never negative proof.
    _state, rows = _list_open_orders_truth(broker)
    return rows


def _filled_order_quantity(raw: dict) -> Optional[int]:
    for key in (
        "filled_qty",
        "filled_quantity",
        "exec_quantity",
        "exec_qty",
        "quantity_filled",
        "quantity",
    ):
        if key in raw and raw.get(key) not in (None, ""):
            return _strict_qty(raw.get(key))
    return None


def _extract_fill_price(raw: dict) -> Optional[float]:
    # A submitted limit price is not execution economics.
    for key in (
        "avg_fill_price",
        "average_fill_price",
        "fill_price",
        "last_fill_price",
        "filled_avg_price",
    ):
        value = raw.get(key)
        if value in (None, "") or isinstance(value, bool):
            continue
        try:
            price = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            continue
        if price.is_finite() and price > 0:
            return float(price)
    return None


def _get_order_truth(broker: Any, broker_order_id: str) -> tuple[str, Optional[dict]]:
    """Fetch one order while preserving unavailable/malformed truth as HOLD."""
    requested_id = _norm(broker_order_id)
    if not requested_id:
        return "unavailable", None
    method = getattr(broker, "get_order", None)
    if not callable(method):
        return "unavailable", None
    try:
        raw = method(requested_id)
    except Exception as exc:
        log.warning("broker.get_order(%s) failed during autonomous recovery: %s", requested_id, exc)
        return "unavailable", None
    row = _coerce_order_row(raw)
    if row is None or _explicit_order_error(row):
        return "malformed", None
    if _broker_order_id(row) != requested_id:
        return "identity_unproven", None
    if not _status(row):
        return "malformed", None
    if not _has_structured_order_direction(row):
        return "malformed", None
    if not _contract(row) or not _is_exit_like(row):
        return "identity_unproven", None
    order_qty = _qty(row)
    if order_qty is None or order_qty <= 0:
        return "malformed", None
    if _status(row) == "filled":
        filled_qty = _filled_order_quantity(row)
        fill_price = _extract_fill_price(row)
        if filled_qty is None or filled_qty <= 0 or fill_price is None:
            return "malformed", None
        row["_recovery_fill_qty"] = filled_qty
        row["_recovery_fill_price"] = fill_price
    return "available", row


def _matching_open_exit_orders_with_truth(
    broker: Any,
    contract: str,
    *,
    exclude_broker_id: str = "",
) -> tuple[str, list[tuple[str, dict]]]:
    if not is_valid_exact_occ_contract(contract):
        return "identity_unproven", []
    state, rows = _list_open_orders_truth(broker)
    if state != "available":
        return state, []
    matches: list[tuple[str, dict]] = []
    for row in rows:
        if not _is_exit_like(row):
            continue
        row_contract = _contract(row)
        if not row_contract:
            return "identity_unproven", []
        if row_contract != contract:
            continue
        status = _status(row)
        if not status:
            return "malformed", []
        if status not in OPEN_BROKER_STATUSES:
            continue
        broker_id = _broker_order_id(row)
        if not broker_id:
            return "identity_unproven", []
        if exclude_broker_id and broker_id == exclude_broker_id:
            continue
        matches.append((broker_id, row))
    return "available", matches


def _matching_open_exit_orders(broker: Any, contract: str, *, exclude_broker_id: str = "") -> list[tuple[str, dict]]:
    _state, matches = _matching_open_exit_orders_with_truth(
        broker, contract, exclude_broker_id=exclude_broker_id
    )
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
    exact: set[str] = set()
    underlying: set[str] = set()
    invalid = False
    for name in ("option_symbol", "contract", "symbol", "instrument"):
        value = getattr(pos, name, "")
        if value in (None, ""):
            continue
        normalized = _norm_contract(value)
        if is_valid_exact_occ_contract(value):
            exact.add(normalized)
        elif name in {"symbol", "instrument"} and re.fullmatch(r"[A-Z0-9.]{1,6}", normalized):
            underlying.add(normalized)
        else:
            invalid = True
    if invalid or len(exact) != 1:
        return ""
    contract = next(iter(exact))
    if any(value != contract[:-15] for value in underlying):
        return ""
    return contract


def _position_id(pos: Any) -> str:
    return _norm(getattr(pos, "position_id", "") or getattr(pos, "id", ""))


def _pending_identity(pos: Any) -> tuple[str, str]:
    return (
        _norm(getattr(pos, "pending_exit_local_order_id", "")),
        _norm(getattr(pos, "pending_exit_broker_order_id", "")),
    )


def _position_remaining(pos: Any) -> Optional[int]:
    for name in ("quantity_remaining", "contracts", "quantity"):
        value = getattr(pos, name, None)
        if value not in (None, ""):
            return _strict_qty(value)
    return None


def _order_identity_mismatch(raw: dict, pos: Any) -> str:
    expected_client = _norm(getattr(pos, "client_id", "")).lower()
    expected_mode = _norm(getattr(pos, "execution_mode", "")).lower()
    records = [raw]
    nested = raw.get("raw") if isinstance(raw, dict) else None
    if isinstance(nested, dict):
        records.append(nested)
    for record in records:
        for key in ("client_id", "clientId", "email"):
            value = _norm(record.get(key)).lower()
            if value and expected_client and value != expected_client:
                return "client"
        for key in ("execution_mode", "executionMode", "mode"):
            value = _norm(record.get(key)).lower()
            if value and expected_mode and value != expected_mode:
                return "execution_mode"
    return ""


def _order_has_explicit_owner(raw: dict) -> bool:
    records = [raw]
    nested = raw.get("raw") if isinstance(raw, dict) else None
    if isinstance(nested, dict):
        records.append(nested)
    has_client = any(
        _norm(record.get(key))
        for record in records
        for key in ("client_id", "clientId", "email")
    )
    has_mode = any(
        _norm(record.get(key)).lower() in {"live", "paper"}
        for record in records
        for key in ("execution_mode", "executionMode", "mode")
    )
    return bool(has_client and has_mode)


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


def _string_identity_attr(obj: Any, name: str) -> str:
    value = getattr(obj, name, None) if obj is not None else None
    return value.strip() if isinstance(value, str) else ""


def _recovery_engine_identity(exit_engine: Any) -> tuple[str, str]:
    client_id = (
        _string_identity_attr(exit_engine, "_email")
        or _string_identity_attr(exit_engine, "client_id")
    )
    mode = ""
    master_control = getattr(exit_engine, "master_control", None) if exit_engine is not None else None
    candidate = getattr(master_control, "mode", None) if master_control is not None else None
    if isinstance(candidate, str):
        mode = candidate.strip().lower()
    return client_id, mode


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


def _hold(
    reason: str,
    pid: str = "",
    local_id: str = "",
    broker_id: str = "",
    details: Optional[dict] = None,
) -> RecoveryAction:
    return RecoveryAction("NOOP", reason, pid, local_id, broker_id, details or {})


def _broker_position_state(broker: Any, *, client_id: str, contract: str) -> str:
    if not is_valid_exact_occ_contract(contract):
        return "identity_unproven"
    try:
        truth = resolve_exit_broker_truth(
            broker=broker,
            client_id=client_id,
            contract=contract,
        )
    except Exception as exc:
        log.warning("broker position truth failed during autonomous recovery: %s", exc)
        return "unavailable"
    if truth.get("is_fresh_exact") is not True:
        snapshot_status = str((truth.get("audit") or {}).get("snapshot_status") or "")
        if snapshot_status == "broker_positions_malformed":
            return "malformed"
        if snapshot_status == "contract_identity_unproven":
            return "identity_unproven"
        return "unavailable"
    quantity = truth.get("broker_truth_open_qty")
    if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity < 0:
        return "malformed"
    return "held" if quantity > 0 else "flat"


def _pending_order_quantity(pos: Any, raw: dict) -> Optional[int]:
    order_qty = _qty(raw)
    if order_qty is not None and order_qty > 0:
        return order_qty
    position_qty = _strict_qty(getattr(pos, "pending_exit_qty", None))
    return position_qty if position_qty is not None and position_qty > 0 else None


def recover_exit_position(
    pos: Any,
    *,
    broker: Any,
    exit_engine: Any = None,
    osm: Any = None,
    expected_client_id: Optional[str] = None,
    expected_execution_mode: Optional[str] = None,
) -> RecoveryAction:
    pid = _position_id(pos)
    local_id, pending_broker_id = _pending_identity(pos)
    contract = _position_contract(pos)
    qh = quote_health(pos)
    position_client = _norm(getattr(pos, "client_id", ""))
    position_mode = _norm(getattr(pos, "execution_mode", "")).lower()

    if not broker or not pid:
        return _hold("missing_broker_or_position_id", pid, local_id, pending_broker_id, {"quote_health": qh})
    if not position_client or position_mode not in {"live", "paper"}:
        return _hold(
            "recovery_position_identity_unproven",
            pid,
            local_id,
            pending_broker_id,
            {"client_id": position_client, "execution_mode": position_mode, "quote_health": qh},
        )
    engine_client, engine_mode = _recovery_engine_identity(exit_engine)
    expected_client = _norm(expected_client_id) if expected_client_id is not None else engine_client
    expected_mode = _norm(expected_execution_mode).lower() if expected_execution_mode is not None else engine_mode
    if expected_client and position_client.lower() != expected_client.lower():
        return _hold(
            "recovery_client_identity_mismatch",
            pid,
            local_id,
            pending_broker_id,
            {"client_id": position_client, "expected_client_id": expected_client, "quote_health": qh},
        )
    if expected_mode and position_mode != expected_mode:
        return _hold(
            "recovery_execution_mode_mismatch",
            pid,
            local_id,
            pending_broker_id,
            {"execution_mode": position_mode, "expected_execution_mode": expected_mode, "quote_health": qh},
        )
    if not contract:
        return _hold("broker_contract_identity_unproven", pid, local_id, pending_broker_id, {"quote_health": qh})

    # Exact broker identity path.
    if pending_broker_id:
        order_state, raw = _get_order_truth(broker, pending_broker_id)
        if order_state != "available" or raw is None:
            return _hold(
                f"broker_order_truth_{order_state}",
                pid,
                local_id,
                pending_broker_id,
                {"contract": contract, "quote_health": qh},
            )
        if _contract(raw) != contract:
            return _hold(
                "broker_contract_identity_unproven",
                pid,
                local_id,
                pending_broker_id,
                {"contract": contract, "broker_contract": _contract(raw), "quote_health": qh},
            )
        identity_mismatch = _order_identity_mismatch(raw, pos)
        if identity_mismatch:
            return _hold(
                f"broker_order_{identity_mismatch}_identity_mismatch",
                pid,
                local_id,
                pending_broker_id,
                {"contract": contract, "identity_field": identity_mismatch, "quote_health": qh},
            )

        status = _status(raw)
        if status in OPEN_BROKER_STATUSES:
            pending_qty = _pending_order_quantity(pos, raw)
            if pending_qty is None:
                return _hold(
                    "broker_order_quantity_unproven",
                    pid,
                    local_id,
                    pending_broker_id,
                    {"status": status, "contract": contract, "quote_health": qh},
                )
            if exit_engine and hasattr(exit_engine, "set_pending_exit_order"):
                exit_engine.set_pending_exit_order(
                    pid,
                    local_order_id=local_id,
                    broker_order_id=pending_broker_id,
                    qty=pending_qty,
                    reason="autonomous_recovery_confirmed_broker_open_exit",
                )
            return RecoveryAction("CONFIRMED_OPEN", "broker_order_still_open", pid, local_id, pending_broker_id, {"status": status, "quote_health": qh})

        if status == "filled":
            filled_qty = raw.get("_recovery_fill_qty")
            fill_price = raw.get("_recovery_fill_price")
            remaining = _position_remaining(pos)
            if (
                not isinstance(filled_qty, int)
                or isinstance(filled_qty, bool)
                or filled_qty <= 0
                or fill_price is None
                or remaining is None
                or remaining <= 0
                or filled_qty > remaining
            ):
                return _hold(
                    "broker_order_fill_quantity_unproven",
                    pid,
                    local_id,
                    pending_broker_id,
                    {
                        "status": status,
                        "filled_qty": filled_qty,
                        "local_remaining": remaining,
                        "contract": contract,
                        "quote_health": qh,
                    },
                )
            if filled_qty == remaining:
                if not (exit_engine and hasattr(exit_engine, "mark_position_closed")):
                    return _hold("broker_order_fill_close_hook_missing", pid, local_id, pending_broker_id, {"quote_health": qh})
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
                return RecoveryAction(
                    "MARKED_CLOSED",
                    "broker_order_filled",
                    pid,
                    local_id,
                    pending_broker_id,
                    {"status": status, "filled_qty": filled_qty, "fill_price": fill_price, "quote_health": qh},
                )
            if not (exit_engine and hasattr(exit_engine, "note_partial_exit_fill")):
                return _hold("broker_order_partial_fill_hook_missing", pid, local_id, pending_broker_id, {"quote_health": qh})
            exit_engine.note_partial_exit_fill(
                pid,
                qty_filled=filled_qty,
                fill_price=fill_price,
                local_order_id=local_id,
                broker_order_id=pending_broker_id,
                cumulative_filled=filled_qty,
            )
            return RecoveryAction(
                "PARTIAL_FILL_APPLIED",
                "broker_order_partial_fill",
                pid,
                local_id,
                pending_broker_id,
                {"status": status, "filled_qty": filled_qty, "fill_price": fill_price, "quote_health": qh},
            )

        if status in TERMINAL_BROKER_STATUSES:
            # A terminal old id is not negative proof.  We only adopt a
            # different exact open order; replacement authorization remains a
            # separate follow-up with complete pagination/ownership proof.
            order_state, other_matches = _matching_open_exit_orders_with_truth(
                broker, contract, exclude_broker_id=pending_broker_id
            )
            if order_state != "available":
                return _hold(
                    f"broker_order_truth_{order_state}",
                    pid,
                    local_id,
                    pending_broker_id,
                    {"old_status": status, "contract": contract, "quote_health": qh},
                )
            for other_id, other_raw in other_matches:
                mismatch = _order_identity_mismatch(other_raw, pos)
                if mismatch:
                    return _hold(
                        f"broker_order_{mismatch}_identity_mismatch",
                        pid,
                        local_id,
                        pending_broker_id,
                        {"broker_order_id": other_id, "identity_field": mismatch, "quote_health": qh},
                    )
            if len(other_matches) == 1:
                other_id, other_raw = other_matches[0]
                pending_qty = _pending_order_quantity(pos, other_raw)
                if pending_qty is None:
                    return _hold("broker_order_quantity_unproven", pid, local_id, pending_broker_id, {"quote_health": qh})
                if exit_engine and hasattr(exit_engine, "set_pending_exit_order"):
                    exit_engine.set_pending_exit_order(
                        pid,
                        local_order_id=local_id,
                        broker_order_id=other_id,
                        qty=pending_qty,
                        reason="autonomous_recovery_found_different_open_exit",
                    )
                return RecoveryAction("CONFIRMED_OPEN", "different_broker_exit_still_open", pid, local_id, other_id, {"old_status": status, "contract": contract, "quote_health": qh})
            if len(other_matches) > 1:
                return _hold(
                    "multiple_different_open_exits_block_replacement",
                    pid,
                    local_id,
                    pending_broker_id,
                    {"old_status": status, "matches": [item[0] for item in other_matches], "quote_health": qh},
                )
            position_state = _broker_position_state(
                broker, client_id=position_client, contract=contract
            )
            if position_state == "flat":
                return _hold(
                    "broker_flat_requires_exact_external_fill",
                    pid,
                    local_id,
                    pending_broker_id,
                    {"old_status": status, "contract": contract, "quote_health": qh},
                )
            if position_state != "held":
                return _hold(
                    f"broker_position_truth_{position_state}",
                    pid,
                    local_id,
                    pending_broker_id,
                    {"old_status": status, "contract": contract, "quote_health": qh},
                )
            return _hold(
                "replacement_authorization_deferred",
                pid,
                local_id,
                pending_broker_id,
                {"old_status": status, "contract": contract, "broker_position_state": position_state, "quote_health": qh},
            )
        return _hold("broker_order_ambiguous_status", pid, local_id, pending_broker_id, {"status": status, "quote_health": qh})

    # Missing broker id: exact order-list ownership is not complete enough for
    # autonomous adoption in this containment change.  We can still recognize
    # a single explicitly fenced order, but never cancel or unlock on a
    # non-paginated negative result.
    order_state, matches = _matching_open_exit_orders_with_truth(broker, contract)
    if order_state != "available":
        return _hold(f"broker_order_truth_{order_state}", pid, local_id, "", {"contract": contract, "quote_health": qh})
    for broker_id, raw in matches:
        mismatch = _order_identity_mismatch(raw, pos)
        if mismatch:
            return _hold(
                f"broker_order_{mismatch}_identity_mismatch",
                pid,
                local_id,
                "",
                {"broker_order_id": broker_id, "identity_field": mismatch, "quote_health": qh},
            )
        # Without explicit broker-side client/mode ownership, a same-contract
        # row could belong to another worker/account.  Leave it for canonical
        # reconciliation rather than adopting it here.
        if not _order_has_explicit_owner(raw):
            return _hold(
                "broker_order_owner_unproven",
                pid,
                local_id,
                broker_id,
                {"contract": contract, "quote_health": qh},
            )
    if len(matches) == 1:
        recovered_id, raw = matches[0]
        pending_qty = _pending_order_quantity(pos, raw)
        if pending_qty is None:
            return _hold("broker_order_quantity_unproven", pid, local_id, recovered_id, {"quote_health": qh})
        if exit_engine and hasattr(exit_engine, "set_pending_exit_order"):
            exit_engine.set_pending_exit_order(
                pid,
                local_order_id=local_id,
                broker_order_id=recovered_id,
                qty=pending_qty,
                reason="autonomous_recovery_matched_live_exit_order",
            )
        return RecoveryAction("RECOVERED_BROKER_ID", "matched_single_live_exit_order", pid, local_id, recovered_id, {"contract": contract, "quote_health": qh})
    if len(matches) > 1:
        return _hold(
            "multiple_live_exit_orders_replacement_deferred",
            pid,
            local_id,
            "",
            {"match_count": len(matches), "quote_health": qh},
        )

    position_state = _broker_position_state(
        broker, client_id=position_client, contract=contract
    )
    if position_state == "flat":
        return _hold(
            "broker_flat_requires_exact_external_fill",
            pid,
            local_id,
            "",
            {"contract": contract, "quote_health": qh},
        )
    if position_state != "held":
        return _hold(
            f"broker_position_truth_{position_state}",
            pid,
            local_id,
            "",
            {"contract": contract, "quote_health": qh},
        )
    return _hold(
        "replacement_authorization_deferred",
        pid,
        local_id,
        "",
        {"contract": contract, "broker_position_state": position_state, "quote_health": qh},
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

    expected_client_id, expected_execution_mode = _recovery_engine_identity(exit_engine)
    actions: list[RecoveryAction] = []
    for pos in positions[:max_positions]:
        if not (
            getattr(pos, "exit_identity_quarantine", False)
            or getattr(pos, "last_callback_identity_missing", False)
            or getattr(pos, "exit_in_flight", False)
            or getattr(pos, "protective_monitoring_state", "") == BROKER_FLAT_RECONCILIATION_PENDING
            or getattr(pos, "broker_flat_reconciliation_pending", False)
        ):
            continue
        try:
            actions.append(
                recover_exit_position(
                    pos,
                    broker=broker,
                    exit_engine=exit_engine,
                    osm=osm,
                    expected_client_id=expected_client_id or None,
                    expected_execution_mode=expected_execution_mode or None,
                )
            )
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
