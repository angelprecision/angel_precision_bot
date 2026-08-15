"""Read-only current-risk authority for interrupted FILLED ENTRY handoffs.

An historical broker order in ``FILLED`` state proves that an execution
happened in the past.  It does not prove that the option position is still
open.  This module is the narrow, mutation-free authority boundary used by
``ap.fill_monitor`` before it repairs a terminal FILLED ENTRY handoff.

The module only reads broker positions and local position state.  It never
submits, cancels, changes an order or position, writes proof, or changes queue
state.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime, timezone
from typing import Any


OCC_RE = re.compile(r"[A-Z0-9.]{1,6}\d{6}[CP]\d{8}")
OCC_EXPIRATION_RE = re.compile(r"([A-Z0-9.]{1,6})(\d{6})[CP]\d{8}")
VALID_EXECUTION_MODES = frozenset({"live", "paper"})
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
_UNSET = object()


def _normalize_contract(value: Any) -> str:
    return str(value or "").upper().replace(" ", "").strip()


def is_valid_occ_contract(value: Any) -> bool:
    contract = _normalize_contract(value)
    return bool(OCC_RE.fullmatch(contract))


def _positive_integral(value: Any) -> int:
    if value is None or isinstance(value, bool):
        return 0
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    if not math.isfinite(parsed) or parsed <= 0 or not parsed.is_integer():
        return 0
    return int(parsed)


def _positive_finite(value: Any) -> float:
    if value is None or isinstance(value, bool):
        return 0.0
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    if not math.isfinite(parsed) or parsed <= 0:
        return 0.0
    return parsed


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


def _position_contract(position: Any) -> str:
    return _normalize_contract(
        _position_value(position, "contract", "")
        or _position_value(position, "option_symbol", "")
        or _position_value(position, "symbol", "")
    )


def _position_is_terminal(position: Any) -> bool:
    status = str(_position_value(position, "status", "") or "").strip().upper()
    if status in TERMINAL_POSITION_STATUSES:
        return True
    if bool(_position_value(position, "closed", False)):
        return True
    remaining = _position_value(position, "quantity_remaining", None)
    if remaining is not None:
        return _positive_integral(remaining) <= 0
    return False


def _position_quantity(position: Any) -> int:
    for key in ("quantity_remaining", "qty", "quantity", "contracts"):
        value = _positive_integral(_position_value(position, key, None))
        if value > 0:
            return value
    return 0


def _result(disposition: str, reason_code: str, **extra: Any) -> dict:
    return {
        "disposition": disposition,
        "reason_code": reason_code,
        **extra,
    }


def _occ_expiration(contract: str) -> date | None:
    match = OCC_EXPIRATION_RE.fullmatch(_normalize_contract(contract))
    if not match:
        return None
    try:
        return datetime.strptime(match.group(2), "%y%m%d").date()
    except (TypeError, ValueError):
        return None


def fetch_current_broker_positions(broker: Any) -> list[dict]:
    """Fetch and structurally validate one current broker position snapshot.

    ``fetch_authoritative_broker_positions`` is the current-main read-only
    broker-position adapter.  This wrapper additionally rejects malformed
    authoritative rows instead of silently treating malformed truth as an
    empty account.  Equity rows are allowed and ignored by the exact-OCC
    matcher; option rows must have an exact OCC symbol and positive integral
    quantity.
    """
    from ap.manual_close_reconciliation import fetch_authoritative_broker_positions

    rows = fetch_authoritative_broker_positions(broker)
    if not isinstance(rows, list):
        raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_MALFORMED")

    normalized: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_ROW_MALFORMED")

        option_symbol = row.get("option_symbol") or row.get("contract")
        symbol = option_symbol or row.get("symbol")
        contract = _normalize_contract(symbol)
        if not contract:
            raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_IDENTITY_MISSING")

        # A symbol-only non-OCC row can be an equity position and is not part
        # of the option-risk snapshot.  Explicit option_symbol/contract rows
        # must always be exact OCC rows.
        explicit_option = option_symbol is not None and str(option_symbol).strip() != ""
        if not is_valid_occ_contract(contract):
            if explicit_option:
                raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_OCC_INVALID")
            continue

        quantity = _positive_integral(row.get("quantity") or row.get("qty"))
        if quantity <= 0:
            raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_QUANTITY_INVALID")
        normalized.append({
            "symbol": contract,
            "quantity": quantity,
            "raw": dict(row),
        })
    return normalized


def _append_unique(candidates: list[Any], value: Any) -> None:
    if not value:
        return
    value_id = _position_id(value)
    if value_id:
        if any(_position_id(existing) == value_id for existing in candidates):
            return
    elif any(existing is value for existing in candidates):
        return
    candidates.append(value)


def _load_local_candidates(pm: Any, order: dict) -> tuple[list[Any], str | None]:
    """Read local rows that could own this exact local/broker identity."""
    if pm is None:
        return [], "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_LOOKUP_FAILED"

    candidates: list[Any] = []
    try:
        get_active = getattr(pm, "get_active_positions", None)
        if callable(get_active):
            active_rows = get_active()
            if not isinstance(active_rows, (list, tuple)):
                return [], "FILLED_ENTRY_RECOVERY_LOCAL_POSITIONS_MALFORMED"
            for row in active_rows:
                if not isinstance(row, (dict,)) and not hasattr(row, "__dict__"):
                    return [], "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_ROW_MALFORMED"
                row_id = _position_id(row)
                row_local = str(_position_value(row, "local_order_id", "") or "").strip()
                row_broker = str(_position_value(row, "broker_order_id", "") or "").strip()
                if (
                    (order.get("position_id") and row_id == str(order.get("position_id")).strip())
                    or (row_local and row_local == str(order.get("local_order_id") or "").strip())
                    or (row_broker and row_broker == str(order.get("broker_order_id") or "").strip())
                ):
                    _append_unique(candidates, row)
    except Exception:
        return [], "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_LOOKUP_FAILED"

    # These exact identity lookups also see terminal rows and rows omitted by
    # get_active_positions(), which is required to distinguish a local
    # terminal + broker-open contradiction from a safe recreate.
    lookup_specs = []
    position_id = str(order.get("position_id") or "").strip()
    if position_id:
        lookup_specs.append(("get_position", position_id))
    lookup_specs.extend(
        [
            ("get_position_by_local_order", str(order.get("local_order_id") or "").strip()),
            ("get_position_by_broker_order", str(order.get("broker_order_id") or "").strip()),
        ]
    )
    for method_name, identity in lookup_specs:
        if not identity:
            continue
        method = getattr(pm, method_name, None)
        if not callable(method):
            continue
        try:
            found = method(identity)
        except Exception:
            return [], "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_LOOKUP_FAILED"
        _append_unique(candidates, found)
    return candidates, None


def evaluate_filled_entry_recovery_authority(
    *,
    pm: Any,
    broker: Any = None,
    order: dict,
    runtime_execution_mode: str | None = None,
    expected_client_id: str | None = None,
    broker_positions: Any = _UNSET,
    broker_positions_error: Exception | str | None = None,
    today: date | None = None,
) -> dict:
    """Classify an interrupted terminal FILLED ENTRY handoff.

    ``ACTIVE_EXISTING`` permits repair against one exact active local
    position.  ``ACTIVE_RECREATE`` permits local recreation only when the
    current broker quantity exactly equals durable filled quantity.  The two
    non-active dispositions grant no position or owner mutation authority.
    """
    if not isinstance(order, dict):
        return _result("HOLD", "FILLED_ENTRY_RECOVERY_ORDER_INVALID", retryable=False)

    client_id = str(order.get("client_id") or "").strip()
    expected_client = str(expected_client_id or "").strip()
    local_order_id = str(order.get("local_order_id") or "").strip()
    broker_order_id = str(order.get("broker_order_id") or "").strip()
    execution_mode = order.get("execution_mode")
    contract = _normalize_contract(order.get("contract"))
    kind = str(order.get("kind") or "")
    status = str(order.get("status") or "")

    if expected_client and client_id != expected_client:
        return _result("HOLD", "FILLED_ENTRY_RECOVERY_CLIENT_MISMATCH", retryable=False)
    if not client_id or not local_order_id or not broker_order_id:
        return _result("HOLD", "FILLED_ENTRY_RECOVERY_IDENTITY_UNPROVEN", retryable=False)
    if broker_order_id.upper() in {
        "0", "N/A", "NA", "NONE", "NULL", "UNKNOWN", "UNDEFINED", "NIL", "TRUE", "FALSE",
    }:
        return _result("HOLD", "FILLED_ENTRY_RECOVERY_BROKER_ORDER_ID_UNPROVEN", retryable=False)
    if kind != "ENTRY" or status != "FILLED":
        return _result("HOLD", "FILLED_ENTRY_RECOVERY_ORDER_STATE_INVALID", retryable=False)
    if execution_mode not in VALID_EXECUTION_MODES:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_EXECUTION_MODE_UNPROVEN",
            execution_mode=execution_mode,
            retryable=False,
        )

    runtime_mode = runtime_execution_mode
    if runtime_mode not in VALID_EXECUTION_MODES:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_RUNTIME_MODE_UNPROVEN",
            execution_mode=execution_mode,
            retryable=False,
        )
    if runtime_mode != execution_mode:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_MODE_CONFLICT",
            execution_mode=execution_mode,
            runtime_execution_mode=runtime_mode,
            retryable=False,
        )
    if not is_valid_occ_contract(contract):
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_CONTRACT_INVALID",
            contract=contract,
            retryable=False,
        )

    durable_qty = _positive_integral(order.get("filled_qty"))
    durable_price = _positive_finite(order.get("fill_price"))
    if durable_qty <= 0:
        return _result("HOLD", "FILLED_ENTRY_RECOVERY_QUANTITY_INVALID", retryable=False)
    if durable_price <= 0:
        return _result("HOLD", "FILLED_ENTRY_RECOVERY_FILL_PRICE_INVALID", retryable=False)

    expiration = _occ_expiration(contract)
    current_date = today or datetime.now(timezone.utc).date()
    if expiration is None:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_CONTRACT_EXPIRATION_UNPROVEN",
            contract=contract,
            retryable=False,
        )
    if expiration < current_date:
        return _result(
            "NONACTIONABLE",
            "HISTORICAL_FILLED_RECOVERY_EXPIRED_CONTRACT",
            contract=contract,
            expiration=expiration.isoformat(),
            current_date=current_date.isoformat(),
            retryable=False,
        )

    if broker_positions_error is not None:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_UNAVAILABLE",
            contract=contract,
            exception=str(broker_positions_error),
            retryable=True,
        )

    if broker_positions is _UNSET:
        try:
            broker_positions = fetch_current_broker_positions(broker)
        except Exception as exc:
            return _result(
                "HOLD",
                "FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_UNAVAILABLE",
                contract=contract,
                exception_type=type(exc).__name__,
                exception=str(exc),
                retryable=True,
            )
    if not isinstance(broker_positions, list):
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_MALFORMED",
            contract=contract,
            retryable=True,
        )

    matches = []
    for row in broker_positions:
        if not isinstance(row, dict):
            return _result(
                "HOLD",
                "FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_MALFORMED",
                contract=contract,
                retryable=True,
            )
        row_contract = _normalize_contract(
            row.get("option_symbol") or row.get("contract") or row.get("symbol")
        )
        if not row_contract:
            return _result(
                "HOLD",
                "FILLED_ENTRY_RECOVERY_BROKER_POSITION_IDENTITY_MISSING",
                contract=contract,
                retryable=True,
            )
        explicit_option = any(
            row.get(key) not in (None, "")
            for key in ("option_symbol", "contract")
        )
        if not is_valid_occ_contract(row_contract):
            if explicit_option:
                return _result(
                    "HOLD",
                    "FILLED_ENTRY_RECOVERY_BROKER_POSITION_OCC_INVALID",
                    contract=contract,
                    retryable=True,
                )
            # Non-option rows (for example equity positions) are outside
            # this exact OCC risk domain and are safely ignored.
            continue
        if row_contract == contract:
            row_qty = _positive_integral(row.get("quantity") or row.get("qty"))
            if row_qty <= 0:
                return _result(
                    "HOLD",
                    "FILLED_ENTRY_RECOVERY_BROKER_QUANTITY_UNPROVEN",
                    contract=contract,
                    retryable=True,
                )
            matches.append((row, row_qty))

    if not matches:
        return _result(
            "NONACTIONABLE",
            "HISTORICAL_FILLED_RECOVERY_NO_CURRENT_BROKER_POSITION",
            contract=contract,
            broker_position_count=0,
            retryable=False,
        )
    if len(matches) != 1:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_BROKER_POSITION_AMBIGUOUS",
            contract=contract,
            broker_position_count=len(matches),
            retryable=True,
        )
    broker_qty = matches[0][1]

    candidates, lookup_error = _load_local_candidates(pm, order)
    if lookup_error:
        return _result(
            "HOLD",
            lookup_error,
            contract=contract,
            broker_qty=broker_qty,
            retryable=True,
        )

    durable_position_id = str(order.get("position_id") or "").strip()
    if len(candidates) > 1:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_AMBIGUOUS",
            contract=contract,
            broker_qty=broker_qty,
            candidate_count=len(candidates),
            retryable=False,
        )

    if candidates:
        position = candidates[0]
        position_id = _position_id(position)
        position_client = str(_position_value(position, "client_id", "") or "").strip()
        position_mode = _position_value(position, "execution_mode", "")
        position_contract = _position_contract(position)
        row_local = str(_position_value(position, "local_order_id", "") or "").strip()
        row_broker = str(_position_value(position, "broker_order_id", "") or "").strip()

        if (
            not position_id
            or (durable_position_id and position_id != durable_position_id)
            or position_client != client_id
            or position_mode != execution_mode
            or position_contract != contract
            or (row_local and row_local != local_order_id)
            or (row_broker and row_broker != broker_order_id)
        ):
            return _result(
                "HOLD",
                "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_IDENTITY_CONFLICT",
                contract=contract,
                position_id=position_id,
                retryable=False,
            )
        if _position_is_terminal(position):
            return _result(
                "HOLD",
                "FILLED_ENTRY_RECOVERY_LOCAL_TERMINAL_BROKER_PRESENT_CONFLICT",
                contract=contract,
                position_id=position_id,
                broker_qty=broker_qty,
                retryable=False,
            )
        local_qty = _position_quantity(position)
        if local_qty <= 0 or local_qty != broker_qty:
            return _result(
                "HOLD",
                "FILLED_ENTRY_RECOVERY_POSITION_QUANTITY_CONFLICT",
                contract=contract,
                position_id=position_id,
                broker_qty=broker_qty,
                local_qty=local_qty,
                retryable=True,
            )
        return _result(
            "ACTIVE_EXISTING",
            "FILLED_ENTRY_RECOVERY_CURRENT_POSITION_PROVEN",
            contract=contract,
            position_id=position_id,
            existing_position=position,
            broker_qty=broker_qty,
            local_qty=local_qty,
            retryable=True,
        )

    if durable_position_id:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_MISSING_DURABLE_IDENTITY",
            contract=contract,
            position_id=durable_position_id,
            broker_qty=broker_qty,
            retryable=True,
        )
    if durable_qty != broker_qty:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_RECREATE_QUANTITY_UNPROVEN",
            contract=contract,
            broker_qty=broker_qty,
            durable_filled_qty=durable_qty,
            retryable=True,
        )
    return _result(
        "ACTIVE_RECREATE",
        "FILLED_ENTRY_RECOVERY_CURRENT_POSITION_PROVEN",
        contract=contract,
        broker_qty=broker_qty,
        durable_filled_qty=durable_qty,
        durable_fill_price=durable_price,
        retryable=True,
    )


__all__ = [
    "VALID_EXECUTION_MODES",
    "fetch_current_broker_positions",
    "evaluate_filled_entry_recovery_authority",
    "is_valid_occ_contract",
]
