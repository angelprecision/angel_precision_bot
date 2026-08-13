"""Fail-closed authority for replaying durably FILLED ENTRY handoffs.

A historical broker order whose terminal state is FILLED proves only that an
execution happened in the past. It does not prove that the option position is
still open now. This module establishes the missing present-tense authority
boundary used by ``ap.fill_monitor`` before any recovery can recreate a local
position or hand ownership back to the exit engine.

The helper is intentionally read-only. It never submits/cancels broker orders
and never mutates orders, positions, proof_trades, or queue state.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime, timezone
from typing import Any


OCC_EXPIRATION_RE = re.compile(r"(\d{6})[CP]\d{8}$")
VALID_EXECUTION_MODES = frozenset({"live", "paper"})
NONACTIONABLE_RECOVERY_DISPOSITIONS = frozenset(
    {
        "HISTORICAL_FILLED_RECOVERY_EXPIRED_CONTRACT",
        "HISTORICAL_FILLED_RECOVERY_NO_CURRENT_BROKER_POSITION",
    }
)
TERMINAL_POSITION_STATUSES = frozenset(
    {
        "CLOSED",
        "CLOSED_REPAIR",
        "EXPIRED",
        "STOPPED",
        "TAKEN_PROFIT",
        "ERROR",
        "CANCELED",
        "CANCELLED",
    }
)


def _normalize_contract(value: Any) -> str:
    return str(value or "").upper().replace(" ", "").strip()


def _positive_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    if not math.isfinite(parsed) or parsed <= 0 or not parsed.is_integer():
        return 0
    return int(parsed)


def _position_value(position: Any, name: str, default: Any = None) -> Any:
    if isinstance(position, dict):
        return position.get(name, default)
    return getattr(position, name, default)


def _position_id(position: Any) -> str:
    return str(
        _position_value(position, "id", "")
        or _position_value(position, "position_id", "")
        or ""
    ).strip()


def _position_is_terminal(position: Any) -> bool:
    status = str(_position_value(position, "status", "") or "").strip().upper()
    if status in TERMINAL_POSITION_STATUSES:
        return True
    if bool(_position_value(position, "closed", False)):
        return True
    remaining = _position_value(position, "quantity_remaining", None)
    if remaining is not None:
        return _positive_int(remaining) <= 0
    return False


def _position_remaining_qty(position: Any) -> int:
    remaining = _position_value(position, "quantity_remaining", None)
    if remaining is not None:
        return _positive_int(remaining)
    for key in ("qty", "quantity"):
        parsed = _positive_int(_position_value(position, key, None))
        if parsed > 0:
            return parsed
    return 0


def _occ_expiration(contract: str) -> date | None:
    compact = _normalize_contract(contract)
    match = OCC_EXPIRATION_RE.search(compact)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%y%m%d").date()
    except (TypeError, ValueError):
        return None


def _load_existing_position(pm: Any, order: dict) -> Any:
    if pm is None:
        return None
    for method_name, identity in (
        ("get_position_by_local_order", order.get("local_order_id")),
        ("get_position_by_broker_order", order.get("broker_order_id")),
    ):
        if not identity:
            continue
        method = getattr(pm, method_name, None)
        if not callable(method):
            continue
        found = method(identity)
        if found:
            return found
    return None


def _result(disposition: str, reason_code: str, **extra: Any) -> dict:
    return {
        "disposition": disposition,
        "reason_code": reason_code,
        **extra,
    }


def evaluate_filled_entry_recovery_authority(
    *,
    pm: Any,
    broker: Any,
    order: dict,
    today: date | None = None,
) -> dict:
    """Prove that a durably FILLED ENTRY still represents current risk.

    Dispositions:
      ``ACTIVE_EXISTING``
        An exact current broker position exists and an exact active canonical
        local position already exists. Ownership repair may continue.

      ``ACTIVE_RECREATE``
        An exact current broker position exists, no local position exists, and
        its quantity exactly matches the durable filled quantity. The caller
        may recreate the canonical local position, but this result grants no
        broker submit/cancel authority.

      ``NONACTIONABLE``
        The contract is expired or the authoritative broker positions endpoint
        proves the contract is not currently held. The caller should persist a
        terminal recovery disposition and stop replaying the historical fill.

      ``HOLD``
        Present-tense truth is unavailable, malformed, or contradictory. The
        caller must make zero money-path mutations and retry/escalate safely.
    """
    if not isinstance(order, dict):
        return _result("HOLD", "FILLED_ENTRY_RECOVERY_ORDER_INVALID")

    client_id = str(order.get("client_id") or "").strip()
    execution_mode = str(order.get("execution_mode") or "").strip().lower()
    contract = _normalize_contract(order.get("contract") or order.get("symbol"))
    local_order_id = str(order.get("local_order_id") or "").strip()
    broker_order_id = str(order.get("broker_order_id") or "").strip()

    if not client_id or not local_order_id or not broker_order_id or not contract:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_IDENTITY_UNPROVEN",
            client_id=client_id,
            execution_mode=execution_mode,
            contract=contract,
        )
    if execution_mode not in VALID_EXECUTION_MODES:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_EXECUTION_MODE_UNPROVEN",
            execution_mode=execution_mode,
            contract=contract,
        )

    expiration = _occ_expiration(contract)
    if expiration is None:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_CONTRACT_EXPIRATION_UNPROVEN",
            contract=contract,
        )
    current_date = today or datetime.now(timezone.utc).date()
    if expiration < current_date:
        return _result(
            "NONACTIONABLE",
            "HISTORICAL_FILLED_RECOVERY_EXPIRED_CONTRACT",
            contract=contract,
            expiration=expiration.isoformat(),
            current_date=current_date.isoformat(),
        )

    # Reuse the repository's strict Tradier position-shape parser. This path
    # raises on transport/auth/malformed payloads instead of laundering them as
    # an empty account, which is essential before declaring historical risk gone.
    try:
        from ap.manual_close_reconciliation import (
            fetch_authoritative_broker_positions,
        )

        broker_positions = fetch_authoritative_broker_positions(broker)
    except Exception as exc:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_UNAVAILABLE",
            contract=contract,
            exception_type=type(exc).__name__,
            exception=str(exc),
        )

    matches: list[dict] = []
    for row in broker_positions:
        if not isinstance(row, dict):
            continue
        row_contract = _normalize_contract(
            row.get("option_symbol") or row.get("contract") or row.get("symbol")
        )
        if row_contract == contract:
            matches.append(row)

    if not matches:
        return _result(
            "NONACTIONABLE",
            "HISTORICAL_FILLED_RECOVERY_NO_CURRENT_BROKER_POSITION",
            contract=contract,
            broker_position_count=0,
        )
    if len(matches) != 1:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_BROKER_POSITION_AMBIGUOUS",
            contract=contract,
            broker_position_count=len(matches),
        )

    broker_qty = _positive_int(matches[0].get("quantity") or matches[0].get("qty"))
    if broker_qty <= 0:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_BROKER_QUANTITY_UNPROVEN",
            contract=contract,
        )

    try:
        existing = _load_existing_position(pm, order)
    except Exception as exc:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_LOOKUP_FAILED",
            contract=contract,
            broker_qty=broker_qty,
            exception_type=type(exc).__name__,
            exception=str(exc),
        )

    if existing:
        position_id = _position_id(existing)
        position_client = str(_position_value(existing, "client_id", "") or "").strip()
        position_mode = str(
            _position_value(existing, "execution_mode", "") or ""
        ).strip().lower()
        position_contract = _normalize_contract(
            _position_value(existing, "contract", "")
            or _position_value(existing, "option_symbol", "")
        )

        if (
            not position_id
            or position_client != client_id
            or position_mode != execution_mode
            or position_contract != contract
        ):
            return _result(
                "HOLD",
                "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_IDENTITY_CONFLICT",
                contract=contract,
                position_id=position_id,
                position_client_id=position_client,
                position_execution_mode=position_mode,
                position_contract=position_contract,
                broker_qty=broker_qty,
            )
        if _position_is_terminal(existing):
            return _result(
                "HOLD",
                "FILLED_ENTRY_RECOVERY_LOCAL_TERMINAL_BROKER_PRESENT_CONFLICT",
                contract=contract,
                position_id=position_id,
                broker_qty=broker_qty,
                position_status=str(
                    _position_value(existing, "status", "") or ""
                ).strip().upper(),
            )

        local_qty = _position_remaining_qty(existing)
        if local_qty <= 0 or local_qty != broker_qty:
            return _result(
                "HOLD",
                "FILLED_ENTRY_RECOVERY_POSITION_QUANTITY_CONFLICT",
                contract=contract,
                position_id=position_id,
                broker_qty=broker_qty,
                local_qty=local_qty,
            )
        return _result(
            "ACTIVE_EXISTING",
            "FILLED_ENTRY_RECOVERY_CURRENT_POSITION_PROVEN",
            contract=contract,
            position_id=position_id,
            broker_qty=broker_qty,
            local_qty=local_qty,
        )

    durable_filled_qty = _positive_int(order.get("filled_qty"))
    if durable_filled_qty <= 0 or durable_filled_qty != broker_qty:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_RECREATE_QUANTITY_UNPROVEN",
            contract=contract,
            broker_qty=broker_qty,
            durable_filled_qty=durable_filled_qty,
        )

    return _result(
        "ACTIVE_RECREATE",
        "FILLED_ENTRY_RECOVERY_CURRENT_POSITION_PROVEN",
        contract=contract,
        broker_qty=broker_qty,
        durable_filled_qty=durable_filled_qty,
    )
