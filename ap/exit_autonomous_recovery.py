"""
ap.exit_autonomous_recovery
===========================
Conservative autonomous recovery helper for APExitEngine quarantine/stale in-flight states.

Safety rules
------------
1. Never clear an exit quarantine by time alone.
2. Never authorize replacement if ANY matching live broker exit order is found.
3. Prefer fill/close truth when broker/DB evidence exists.
4. On ambiguous matching live exits, do not mutate broker or durable state;
   identity ambiguity is a zero-mutation hold.
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

log = logging.getLogger("ap.exit_autonomous_recovery")

OPEN_BROKER_STATUSES = {"open", "pending", "accepted", "submitted", "queued", "working", "acknowledged", "partially_filled"}
TERMINAL_BROKER_STATUSES = {"filled", "canceled", "cancelled", "rejected", "expired"}
CANCEL_CONFIRMED_STATUSES = {"canceled", "cancelled", "rejected", "expired"}
QUOTE_STALE_WARN_SEC = int(os.getenv("EXIT_RECOVERY_QUOTE_STALE_SEC", "30"))
CANCEL_PROOF_RETRIES = int(os.getenv("EXIT_RECOVERY_CANCEL_RETRIES", "3"))
CANCEL_PROOF_DELAY_SEC = float(os.getenv("EXIT_RECOVERY_CANCEL_DELAY_SEC", "1.0"))
STALE_EXIT_CANCEL_LIVENESS_META_KEY = "stale_exit_cancel_liveness"
STALE_EXIT_CANCEL_MAX_ATTEMPTS = max(
    1, int(os.getenv("ORDER_STALE_EXIT_CANCEL_MAX_ATTEMPTS", "2"))
)
STALE_EXIT_RECOVERY_AGE_SECONDS = max(
    0, int(os.getenv("ORDER_TIMEOUT_EXIT_PENDING", "45"))
)


class _BrokerSnapshotUnavailable(RuntimeError):
    """A broker order snapshot was not available as authoritative truth."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _order_monitor_alive(order_monitor: Any) -> bool:
    """
    PR #423 Patch 3: single-cancellation-owner guarantee.

    #423 gives APOrderMonitor its own narrow stale-EXIT watchdog cancel
    authority (ap/order_monitor.py _handle_stale_exit). Before that PR,
    this module's ambiguous-multi-match branch (see below) was the only
    place that could independently cancel a stale exit order, so there was
    no dual-ownership risk. Now there is: if the order monitor is alive and
    already working the exact same stale exit, this module must defer to
    it rather than issue a second independent cancel.

    Returns False (i.e. "assume no owner, act independently") for any
    monitor reference that doesn't look like a real running APOrderMonitor
    — a missing/None monitor is not evidence that one is alive elsewhere.
    """
    if order_monitor is None:
        return False
    try:
        thread = getattr(order_monitor, "_thread", None)
        return bool(thread is not None and thread.is_alive())
    except Exception:
        return False


def _order_row(osm: Any, local_order_id: str) -> dict:
    """Read one durable OSM order snapshot, fail-closed."""
    if not osm or not local_order_id:
        return {}
    getter = getattr(osm, "get_order", None)
    if not callable(getter):
        return {}
    try:
        row = getter(local_order_id)
        if row is None:
            return {}
        try:
            row = dict(row)
        except Exception:
            return {}
        return row
    except Exception:
        return {}


def _order_meta(osm: Any, local_order_id: str) -> dict:
    """Read one durable OSM order metadata snapshot, fail-closed."""
    row = _order_row(osm, local_order_id)
    if not row:
        return {}
    raw_meta = row.get("meta") or {}
    if isinstance(raw_meta, str):
        import json
        try:
            raw_meta = json.loads(raw_meta) if raw_meta.strip() else {}
        except Exception:
            raw_meta = {}
    return dict(raw_meta) if isinstance(raw_meta, dict) else {}


def _metadata_from_order_row(order: dict) -> dict:
    raw_meta = (order or {}).get("meta") or {}
    if isinstance(raw_meta, str):
        import json
        try:
            raw_meta = json.loads(raw_meta) if raw_meta.strip() else {}
        except Exception:
            raw_meta = {}
    return dict(raw_meta) if isinstance(raw_meta, dict) else {}


def _read_stale_exit_cancel_liveness(
    osm: Any, local_order_id: str, broker_order_id: str, *, order: Optional[dict] = None,
) -> dict:
    """Load the exact broker-order cancel-attempt fence from durable OSM meta."""
    broker_order_id = _norm(broker_order_id)
    if order is None:
        metadata = _order_meta(osm, local_order_id)
    else:
        metadata = _metadata_from_order_row(order)
    payload = metadata.get(STALE_EXIT_CANCEL_LIVENESS_META_KEY) or {}
    if not isinstance(payload, dict):
        return {"attempt": 0, "broker_order_id": "", "updated_at": ""}
    if _norm(payload.get("broker_order_id")) != broker_order_id:
        return {"attempt": 0, "broker_order_id": "", "updated_at": ""}
    try:
        attempt = max(0, int(payload.get("attempt", 0) or 0))
    except (TypeError, ValueError, OverflowError):
        attempt = 0
    return {
        "attempt": attempt,
        "broker_order_id": broker_order_id,
        "updated_at": _norm(payload.get("updated_at")),
    }


def _persist_stale_exit_cancel_attempt(
    osm: Any,
    local_order_id: str,
    broker_order_id: str,
    attempt: int,
    execution_mode: str = "",
) -> bool:
    """Persist one exact-order cancel attempt before issuing the broker DELETE.

    This money-path fence deliberately does not fall back to the generic
    ``update_order_meta`` merge.  The caller may have read an older snapshot,
    so only the OSM's row-locked monotonic writer can authorize the DELETE.
    """
    if not osm or not local_order_id or not broker_order_id:
        return False
    persister = getattr(osm, "persist_stale_exit_cancel_attempt", None)
    if not callable(persister):
        return False
    try:
        attempt_i = int(attempt)
        if attempt_i <= 0:
            return False
        mode = str(execution_mode or "").strip().lower()
        # Production APOrderStateMachine requires the exact mode fence.  Keep
        # old three-argument test doubles compatible without weakening that
        # production contract; a real four-argument writer with no mode fails
        # closed above rather than guessing LIVE/PAPER.
        import inspect

        try:
            parameters = inspect.signature(persister).parameters.values()
            supports_mode = any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD
                or parameter.kind == inspect.Parameter.VAR_POSITIONAL
                or parameter.name == "execution_mode"
                for parameter in parameters
            )
        except Exception:
            supports_mode = True
        if supports_mode:
            if mode not in {"live", "paper"}:
                return False
            return bool(
                persister(local_order_id, _norm(broker_order_id), attempt_i, mode)
            )
        return bool(persister(local_order_id, _norm(broker_order_id), attempt_i))
    except Exception as exc:
        log.error(
            "durable stale-exit cancel-attempt persist failed | local=%s broker=%s: %s",
            local_order_id, broker_order_id, exc,
        )
        return False


def _stale_exit_cancel_retry_after_seconds() -> Optional[int]:
    """Use OrderMonitor's configured retry interval for autonomous retries."""
    try:
        from ap.order_monitor import STALE_EXIT_CANCEL_RETRY_AFTER_SECONDS

        return max(0, int(STALE_EXIT_CANCEL_RETRY_AFTER_SECONDS))
    except Exception as exc:
        log.error(
            "unable to read OrderMonitor stale-exit cancel retry interval: %s",
            exc,
        )
        return None


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _norm_contract(value: Any) -> str:
    return _norm(value).upper().replace(" ", "")


_BROKER_STATUS_KEYS = ("status", "Status", "state", "order_status")
_BROKER_ORDER_ID_KEYS = ("broker_order_id", "order_id", "id", "orderId")
# Tradier's ``symbol`` is the underlying root (for example ``SPY``), while
# ``option_symbol`` is the OCC contract identity.  They are deliberately not
# aliases and must never be compared as if they represented the same field.
_BROKER_CONTRACT_KEYS = ("contract", "option_symbol", "instrument")
_BROKER_UNDERLYING_KEYS = ("symbol",)
_BROKER_ORDER_QTY_KEYS = ("qty", "quantity", "order_qty")
_BROKER_REMAINING_QTY_KEYS = ("remaining_qty", "remaining_quantity")
_BROKER_FILLED_QTY_KEYS = ("filled_qty", "filled_quantity", "exec_quantity")


def _strict_nonnegative_quantity(value: Any) -> Optional[int]:
    """Parse broker quantities without bool, sign, fraction, or whitespace coercion."""
    if isinstance(value, bool) or value is None:
        return None
    if type(value) is int:
        return value if value >= 0 else None
    if type(value) is float:
        if not math.isfinite(value) or not value.is_integer() or value < 0:
            return None
        return int(value)
    if isinstance(value, str):
        if value != value.strip() or not value or not value.isdigit():
            return None
        return int(value)
    return None


def _strict_consistent_text(
    raw: dict,
    keys: tuple[str, ...],
    *,
    field_name: str,
    normalize,
) -> str:
    values = []
    for key in keys:
        if key not in raw or raw.get(key) is None:
            continue
        value = raw.get(key)
        if not isinstance(value, str) or not value or value != value.strip():
            raise ValueError(f"malformed broker {field_name} field: {key}")
        values.append(normalize(value))
    if values and any(value != values[0] for value in values[1:]):
        raise ValueError(f"conflicting broker {field_name} fields")
    return values[0] if values else ""


def _strict_provider_order_id(value: Any, *, field_name: str = "order identity") -> str:
    """Normalize provider IDs without bool/fraction/malformed coercion."""
    if isinstance(value, bool) or value is None:
        raise ValueError(f"malformed broker {field_name} field")
    if type(value) is int:
        if value < 0:
            raise ValueError(f"malformed broker {field_name} field")
        return str(value)
    if type(value) is float:
        if not math.isfinite(value) or not value.is_integer() or value < 0:
            raise ValueError(f"malformed broker {field_name} field")
        return str(int(value))
    if isinstance(value, str):
        if not value or value != value.strip():
            raise ValueError(f"malformed broker {field_name} field")
        return value
    raise ValueError(f"malformed broker {field_name} field")


def _strict_consistent_provider_id(
    raw: dict,
    keys: tuple[str, ...],
    *,
    field_name: str,
) -> str:
    values = []
    for key in keys:
        if key not in raw or raw.get(key) is None:
            continue
        values.append(
            _strict_provider_order_id(raw.get(key), field_name=field_name)
        )
    if values and any(value != values[0] for value in values[1:]):
        raise ValueError(f"conflicting broker {field_name} fields")
    return values[0] if values else ""


def _strict_quantity_group(
    raw: dict,
    keys: tuple[str, ...],
    *,
    field_name: str,
) -> Optional[int]:
    values = []
    for key in keys:
        if key not in raw or raw.get(key) is None:
            continue
        parsed = _strict_nonnegative_quantity(raw.get(key))
        if parsed is None:
            raise ValueError(f"malformed broker {field_name} field: {key}")
        values.append(parsed)
    if values and any(value != values[0] for value in values[1:]):
        raise ValueError(f"conflicting broker {field_name} fields")
    return values[0] if values else None


def _validate_broker_order_payload(
    raw: Any,
    *,
    expected_broker_order_id: str = "",
    expected_contract: str = "",
) -> dict:
    """Validate broker authority before any recovery mutation is permitted."""
    if not isinstance(raw, dict) or not raw:
        raise ValueError("broker order payload is empty or not an object")

    status = _strict_consistent_text(
        raw,
        _BROKER_STATUS_KEYS,
        field_name="status",
        normalize=lambda value: value.lower(),
    )
    if not status:
        raise ValueError("broker order status is missing")

    broker_order_id = _strict_consistent_provider_id(
        raw,
        _BROKER_ORDER_ID_KEYS,
        field_name="order identity",
    )
    expected_id = ""
    if expected_broker_order_id not in (None, ""):
        expected_id = _strict_provider_order_id(
            expected_broker_order_id,
            field_name="expected order identity",
        )
    if expected_id and not broker_order_id:
        raise ValueError("broker order identity is missing")
    if broker_order_id and expected_id and broker_order_id != expected_id:
        raise ValueError("broker order identity does not match requested order")

    contract = _strict_consistent_text(
        raw,
        _BROKER_CONTRACT_KEYS,
        field_name="contract",
        normalize=lambda value: _norm_contract(value),
    )
    _strict_consistent_text(
        raw,
        _BROKER_UNDERLYING_KEYS,
        field_name="underlying symbol",
        normalize=lambda value: _norm_contract(value),
    )
    expected_contract_norm = _norm_contract(expected_contract)
    if expected_contract_norm and not contract:
        raise ValueError("broker order contract is missing")
    if contract and expected_contract_norm and contract != expected_contract_norm:
        raise ValueError("broker order contract does not match durable position")

    order_qty = _strict_quantity_group(
        raw, _BROKER_ORDER_QTY_KEYS, field_name="order quantity"
    )
    remaining_qty = _strict_quantity_group(
        raw, _BROKER_REMAINING_QTY_KEYS, field_name="remaining quantity"
    )
    filled_qty = _strict_quantity_group(
        raw, _BROKER_FILLED_QTY_KEYS, field_name="filled quantity"
    )

    if status == "filled":
        if order_qty is not None and filled_qty is not None and order_qty != filled_qty:
            raise ValueError("filled broker order quantity authorities disagree")
        authoritative_filled_qty = filled_qty if filled_qty is not None else order_qty
        if authoritative_filled_qty is None or authoritative_filled_qty <= 0:
            raise ValueError("filled broker order quantity is missing or non-positive")
        if remaining_qty is not None and remaining_qty != 0:
            raise ValueError("filled broker order has non-zero remaining quantity")

    return dict(raw)


def _strict_filled_order_quantity(raw: dict) -> Optional[int]:
    """Return a proven positive filled quantity, never a local fallback."""
    try:
        order_qty = _strict_quantity_group(
            raw, _BROKER_ORDER_QTY_KEYS, field_name="order quantity"
        )
        filled_qty = _strict_quantity_group(
            raw, _BROKER_FILLED_QTY_KEYS, field_name="filled quantity"
        )
        remaining_qty = _strict_quantity_group(
            raw, _BROKER_REMAINING_QTY_KEYS, field_name="remaining quantity"
        )
    except ValueError:
        return None
    if order_qty is not None and filled_qty is not None and order_qty != filled_qty:
        return None
    quantity = filled_qty if filled_qty is not None else order_qty
    if quantity is None or quantity <= 0 or (remaining_qty is not None and remaining_qty != 0):
        return None
    return quantity


def _status(raw: dict) -> str:
    return _norm(raw.get("status") or raw.get("Status") or raw.get("state") or raw.get("order_status")).lower()


def _broker_order_id(raw: dict) -> str:
    try:
        return _strict_consistent_provider_id(
            raw,
            _BROKER_ORDER_ID_KEYS,
            field_name="order identity",
        )
    except ValueError:
        return ""


def _contract(raw: dict) -> str:
    for key in _BROKER_CONTRACT_KEYS:
        value = raw.get(key)
        if value not in (None, ""):
            return _norm_contract(value)
    # Keep legacy non-option/equity discovery usable, but never treat this
    # fallback as satisfying an expected OCC option contract in validation.
    return _norm_contract(raw.get("symbol"))


def _qty(raw: dict) -> int:
    for keys in (
        _BROKER_ORDER_QTY_KEYS,
        _BROKER_REMAINING_QTY_KEYS,
        _BROKER_FILLED_QTY_KEYS,
    ):
        try:
            quantity = _strict_quantity_group(raw, keys, field_name="quantity")
        except ValueError:
            return 0
        if quantity is not None:
            return quantity
    return 0


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


def _normalize_broker_rows(raw: Any, *, container_keys: tuple[str, ...]) -> Optional[list[dict]]:
    """Normalize a broker collection without turning malformed truth into empty truth."""
    if isinstance(raw, list):
        if any(not isinstance(item, dict) or not item for item in raw):
            return None
        return [dict(item) for item in raw]

    if isinstance(raw, dict):
        for key in container_keys:
            if key not in raw:
                continue
            node = raw.get(key)
            if node in (None, "null"):
                return []
            if isinstance(node, list):
                if any(not isinstance(item, dict) or not item for item in node):
                    return None
                return [dict(item) for item in node]
            if isinstance(node, dict):
                for child_key in ("order", "orders", "position", "positions", "items", "results", "data"):
                    if child_key not in node:
                        continue
                    child = node.get(child_key)
                    if child in (None, "null"):
                        return []
                    if isinstance(child, dict):
                        return [dict(child)] if child else []
                    if isinstance(child, list):
                        if any(not isinstance(item, dict) or not item for item in child):
                            return None
                        return [dict(item) for item in child]
                    return None
                return [] if not node else None
            return None

        return [dict(raw)] if raw else None

    return None


def _list_open_orders(broker: Any) -> tuple[bool, list[dict]]:
    """Return ``(available, orders)``; unavailable is never represented by ``[]``."""
    found_method = False
    for method_name in ("list_open_orders", "get_open_orders", "list_orders", "orders"):
        method = getattr(broker, method_name, None)
        if not callable(method):
            continue
        found_method = True
        try:
            try:
                result = method(status="open")
            except TypeError:
                result = method()
            rows = _normalize_broker_rows(
                result,
                container_keys=("orders", "data", "results", "items"),
            )
            if rows is None:
                log.warning("broker.%s returned malformed data during autonomous recovery", method_name)
                continue
            try:
                rows = [_validate_broker_order_payload(row) for row in rows]
            except ValueError as exc:
                log.warning(
                    "broker.%s returned contradictory or malformed order authority: %s",
                    method_name,
                    exc,
                )
                continue
            if not all(
                _broker_order_id(row) and _contract(row) and _status(row)
                for row in rows
            ):
                log.warning("broker.%s returned structurally incomplete order data during autonomous recovery", method_name)
                continue
            return True, rows
        except Exception as exc:
            log.warning("broker.%s failed during autonomous recovery: %s", method_name, exc)
    if not found_method:
        log.warning("broker has no open-order query during autonomous recovery")
    return False, []


def _get_order(
    broker: Any,
    broker_order_id: str,
    *,
    expected_contract: str = "",
) -> Optional[dict]:
    if not broker_order_id:
        raise _BrokerSnapshotUnavailable("missing broker order id")
    method = getattr(broker, "get_order", None)
    if not callable(method):
        log.warning("broker.get_order missing during autonomous recovery")
        raise _BrokerSnapshotUnavailable("broker.get_order unavailable")
    try:
        raw = method(broker_order_id)
    except Exception as exc:
        log.warning("broker.get_order(%s) failed: %s", broker_order_id, exc)
        raise _BrokerSnapshotUnavailable(
            f"broker.get_order failed for {broker_order_id}"
        ) from exc
    try:
        return _validate_broker_order_payload(
            raw,
            expected_broker_order_id=broker_order_id,
            expected_contract=expected_contract,
        )
    except ValueError as exc:
        log.warning(
            "broker.get_order(%s) returned unavailable or contradictory payload: %r (%s)",
            broker_order_id,
            raw,
            exc,
        )
        raise _BrokerSnapshotUnavailable(
            f"broker.get_order returned malformed payload for {broker_order_id}"
        )


def _get_order_for_contract(
    broker: Any,
    broker_order_id: str,
    contract: str,
) -> Optional[dict]:
    """Keep the legacy two-argument GET seam while fencing contract identity."""
    expected_contract = _norm_contract(contract)
    if not expected_contract:
        raise _BrokerSnapshotUnavailable("missing expected broker contract")
    try:
        raw = _get_order(broker, broker_order_id)
        return _validate_broker_order_payload(
            raw,
            expected_broker_order_id=broker_order_id,
            expected_contract=expected_contract,
        )
    except (_BrokerSnapshotUnavailable, ValueError) as exc:
        raise _BrokerSnapshotUnavailable(
            f"broker.get_order exact identity authority unavailable for {broker_order_id}"
        ) from exc


def _matching_open_exit_orders(
    broker: Any, contract: str, *, exclude_broker_id: str = "",
) -> tuple[bool, list[tuple[str, dict]]]:
    matches: list[tuple[str, dict]] = []
    available, rows = _list_open_orders(broker)
    if not available:
        return False, matches
    for raw in rows:
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
    return True, matches


def _list_broker_positions(broker: Any) -> tuple[bool, list[dict]]:
    """Return ``(available, positions)``; an unavailable snapshot is not flat."""
    method = getattr(broker, "list_positions", None)
    if not callable(method):
        log.warning("broker has no position query during autonomous recovery")
        return False, []
    try:
        rows = _normalize_broker_rows(
            method(),
            container_keys=("positions", "data", "results", "items"),
        )
        if rows is None:
            log.warning("broker.list_positions returned malformed data during autonomous recovery")
            return False, []
        for row in rows:
            try:
                _strict_consistent_text(
                    row,
                    _BROKER_CONTRACT_KEYS,
                    field_name="contract",
                    normalize=lambda value: _norm_contract(value),
                )
                if not _contract(row):
                    raise ValueError("missing broker position contract")
                quantity = _strict_quantity_group(
                    row,
                    ("quantity", "qty", "position_qty", "long_quantity"),
                    field_name="position quantity",
                )
                if quantity is None:
                    raise ValueError("missing broker position quantity")
            except (TypeError, ValueError, OverflowError) as exc:
                log.warning(
                    "broker.list_positions returned contradictory or malformed quantity/contract: %s",
                    exc,
                )
                return False, []
        return True, rows
    except Exception as exc:
        log.warning("broker.list_positions failed during autonomous recovery: %s", exc)
        return False, []


def _cancel_order_with_proof(
    broker: Any,
    broker_order_id: str,
    *,
    expected_contract: str = "",
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
        try:
            confirmed = (
                _get_order_for_contract(broker, broker_order_id, expected_contract)
                if expected_contract
                else _get_order(broker, broker_order_id)
            )
        except _BrokerSnapshotUnavailable:
            confirmed = None
        confirmed_payload = confirmed
        # The cancel response is only a request acknowledgement.  A fresh
        # exact GET is the terminal proof; an unavailable GET must never fall
        # back to the response's optimistic ``canceled`` status.
        confirmed_status = _status(confirmed or {}) if confirmed else ""
        if confirmed_status in CANCEL_CONFIRMED_STATUSES:
            raw["confirmed_status"] = confirmed_status
            raw["confirmation_attempts"] = attempt + 1
            raw["confirmed_payload"] = dict(confirmed) if isinstance(confirmed, dict) else confirmed
            return True, raw
        if attempt < max_retries - 1:
            time.sleep(max(0.0, float(retry_delay)))

    # If broker accepted cancel but status has not propagated, do NOT unlock.
    raw["confirmed_status"] = confirmed_status
    raw["cancel_response_status"] = status_val
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


def _position_close_confirmed(pos: Any) -> bool:
    """Verify the real exit engine mutated the position to closed state."""
    if not all(hasattr(pos, attr) for attr in ("closed", "quantity_remaining", "exit_in_flight")):
        return False
    if not bool(getattr(pos, "closed", False)) or bool(getattr(pos, "exit_in_flight", False)):
        return False
    try:
        return int(float(getattr(pos, "quantity_remaining", 0) or 0)) == 0
    except (TypeError, ValueError, OverflowError):
        return False


def _strict_managed_quantity_remaining(pos: Any) -> Optional[int]:
    """Read the managed remainder without coercing ambiguous local state."""
    value = getattr(pos, "quantity_remaining", None)
    if type(value) is not int or value <= 0:
        return None
    return value


_OSM_EXIT_ACTIVE_STATUSES = {
    "EXIT_REQUESTED",
    "EXIT_SUBMITTED",
    "EXIT_ACKNOWLEDGED",
    "EXIT_PARTIAL_FILL",
}
_CUMULATIVE_FILL_KEYS = ("filled_qty", "filled_quantity", "exec_quantity")


def _strict_durable_quantity(
    row: dict,
    keys: tuple[str, ...],
    *,
    positive: bool,
) -> Optional[int]:
    """Read a durable integer quantity without compatibility coercion."""
    values = []
    for key in keys:
        if key not in row:
            continue
        value = row.get(key)
        if type(value) is not int or isinstance(value, bool):
            return None
        if value < 0 or (positive and value <= 0):
            return None
        values.append(value)
    if not values or any(value != values[0] for value in values[1:]):
        return None
    return values[0]


def _exact_osm_exit_generation(
    osm: Any,
    *,
    local_id: str,
    broker_id: str,
    position_id: str,
    client_id: str,
    execution_mode: str,
) -> tuple[Optional[dict], Optional[int], str]:
    """Return the exact active durable EXIT row and its requested quantity."""
    row = _order_row(osm, local_id)
    if not row:
        return None, None, "durable_osm_exit_row_unavailable"
    if _norm(row.get("local_order_id")) != _norm(local_id):
        return None, None, "durable_osm_exit_local_identity_mismatch"
    if _norm(row.get("broker_order_id")) != _norm(broker_id):
        return None, None, "durable_osm_exit_broker_identity_mismatch"
    if _norm(row.get("position_id")) != _norm(position_id):
        return None, None, "durable_osm_exit_position_identity_mismatch"
    if _norm(row.get("client_id")) != _norm(client_id):
        return None, None, "durable_osm_exit_client_identity_mismatch"
    if str(row.get("execution_mode") or "") != str(execution_mode or ""):
        return None, None, "durable_osm_exit_execution_mode_mismatch"
    if _norm(row.get("kind")).upper() != "EXIT":
        return None, None, "durable_osm_exit_kind_mismatch"
    if _norm(row.get("status")).upper() not in _OSM_EXIT_ACTIVE_STATUSES:
        return None, None, "durable_osm_exit_generation_not_active"
    requested_qty = _strict_durable_quantity(
        row,
        ("qty", "quantity"),
        positive=True,
    )
    if requested_qty is None:
        return None, None, "durable_osm_exit_requested_quantity_unavailable"
    return row, requested_qty, "ok"


def _strict_cumulative_fill_authority(
    payload: dict,
) -> tuple[Optional[int], bool]:
    """Return (cumulative fill, present) from a broker snapshot."""
    values = []
    present = False
    for key in _CUMULATIVE_FILL_KEYS:
        if key not in payload:
            continue
        present = True
        parsed = _strict_nonnegative_quantity(payload.get(key))
        if parsed is None:
            return None, True
        values.append(parsed)
    if not present or any(value != values[0] for value in values[1:]):
        return None, present
    return values[0], True


def _strict_durable_cumulative_fill_authority(
    row: dict,
) -> tuple[Optional[int], bool]:
    """Return (durable cumulative fill, present) from an OSM row."""
    values = []
    present = False
    for key in _CUMULATIVE_FILL_KEYS:
        if key not in row:
            continue
        present = True
        value = row.get(key)
        if type(value) is not int or isinstance(value, bool) or value < 0:
            return None, True
        values.append(value)
    if not present or any(value != values[0] for value in values[1:]):
        return None, present
    return values[0], True


def _terminal_cumulative_fill_authority(
    broker_payload: dict,
    osm_row: dict,
) -> tuple[Optional[int], str]:
    """Resolve terminal fill quantity from broker or durable OSM evidence.

    A broker cumulative value greater than the durable OSM watermark is a
    monotonic late fill during the cancel window, not automatically a
    contradiction.  Callers must reconcile that delta through canonical exit
    accounting before granting a replacement.
    """
    broker_qty, broker_present = _strict_cumulative_fill_authority(broker_payload)
    osm_qty, osm_present = _strict_durable_cumulative_fill_authority(osm_row)
    if broker_present and broker_qty is None:
        return None, "broker_cumulative_fill_malformed"
    if osm_present and osm_qty is None:
        return None, "durable_osm_cumulative_fill_malformed"
    if not broker_present and not osm_present:
        return None, "terminal_cumulative_fill_missing"
    if broker_present and osm_present and broker_qty != osm_qty:
        if broker_qty > osm_qty:
            return broker_qty, "broker_snapshot_late_fill"
        return None, "terminal_cumulative_fill_conflict"
    if broker_present:
        return broker_qty, "broker_snapshot"
    return osm_qty, "durable_osm"


def _reconcile_terminal_late_fill(
    *,
    pos: Any,
    exit_engine: Any,
    osm: Any,
    local_id: str,
    broker_id: str,
    position_id: str,
    terminal_row: dict,
    requested_qty: int,
    broker_qty: int,
) -> tuple[bool, str, dict]:
    """Apply broker-minus-OSM terminal fill before replacement authorization."""
    durable_qty, durable_present = _strict_durable_cumulative_fill_authority(terminal_row)
    if not durable_present or durable_qty is None:
        return False, "durable_osm_cumulative_fill_unavailable", {}
    if broker_qty <= durable_qty:
        return False, "terminal_late_fill_delta_not_positive", {
            "broker_cumulative_filled": broker_qty,
            "durable_osm_cumulative_filled": durable_qty,
        }
    if broker_qty > requested_qty:
        return False, "terminal_late_fill_exceeds_requested_quantity", {
            "broker_cumulative_filled": broker_qty,
            "durable_osm_cumulative_filled": durable_qty,
            "osm_requested_qty": requested_qty,
        }

    delta = broker_qty - durable_qty
    before_remaining = _strict_managed_quantity_remaining(pos)
    transition = getattr(osm, "transition", None)
    partial_fill_hook = getattr(exit_engine, "note_partial_exit_fill", None)
    if not callable(transition) or not callable(partial_fill_hook):
        return False, "canonical_late_fill_owner_unavailable", {
            "late_fill_delta": delta,
            "broker_cumulative_filled": broker_qty,
            "durable_osm_cumulative_filled": durable_qty,
        }

    try:
        transition_ok = bool(
            transition(
                local_id,
                "EXIT_PARTIAL_FILL",
                broker_order_id=broker_id,
                filled_qty=broker_qty,
                position_id=position_id,
            )
        )
    except Exception as exc:
        return False, "canonical_late_fill_osm_transition_failed", {
            "late_fill_delta": delta,
            "broker_cumulative_filled": broker_qty,
            "durable_osm_cumulative_filled": durable_qty,
            "error": type(exc).__name__,
        }

    if not transition_ok:
        refreshed_row = _order_row(osm, local_id)
        refreshed_qty, refreshed_present = _strict_durable_cumulative_fill_authority(
            refreshed_row
        )
        transition_ok = bool(
            _norm(refreshed_row.get("local_order_id")) == _norm(local_id)
            and _norm(refreshed_row.get("broker_order_id")) == _norm(broker_id)
            and _norm(refreshed_row.get("position_id")) == _norm(position_id)
            and _norm(refreshed_row.get("status")).upper() == "EXIT_PARTIAL_FILL"
            and refreshed_present
            and refreshed_qty == broker_qty
        )
    if not transition_ok:
        return False, "canonical_late_fill_osm_transition_unconfirmed", {
            "late_fill_delta": delta,
            "broker_cumulative_filled": broker_qty,
            "durable_osm_cumulative_filled": durable_qty,
        }

    try:
        # The durable OSM transition normally invokes this hook itself.  The
        # explicit call is idempotent and covers recovery doubles/registries
        # that do not wire the OSM hook; prior_cumulative_filled prevents a
        # missed in-memory watermark from applying the whole broker total.
        partial_fill_hook(
            position_id,
            qty_filled=delta,
            local_order_id=local_id,
            broker_order_id=broker_id,
            cumulative_filled=broker_qty,
            prior_cumulative_filled=durable_qty,
        )
    except Exception as exc:
        return False, "canonical_late_fill_accounting_failed", {
            "late_fill_delta": delta,
            "broker_cumulative_filled": broker_qty,
            "durable_osm_cumulative_filled": durable_qty,
            "error": type(exc).__name__,
        }

    refreshed_row = _order_row(osm, local_id)
    refreshed_qty, refreshed_present = _strict_durable_cumulative_fill_authority(
        refreshed_row
    )
    if not refreshed_present or refreshed_qty != broker_qty:
        return False, "canonical_late_fill_durable_quantity_unconfirmed", {
            "late_fill_delta": delta,
            "broker_cumulative_filled": broker_qty,
            "durable_osm_cumulative_filled": refreshed_qty,
        }
    if bool(getattr(pos, "closed", False)):
        return False, "canonical_late_fill_closed_position_unexpected", {
            "late_fill_delta": delta,
            "broker_cumulative_filled": broker_qty,
            "durable_osm_cumulative_filled": durable_qty,
        }
    if before_remaining is not None:
        after_remaining = _strict_managed_quantity_remaining(pos)
        if after_remaining != before_remaining - delta:
            return False, "canonical_late_fill_position_delta_unconfirmed", {
                "late_fill_delta": delta,
                "quantity_remaining_before": before_remaining,
                "quantity_remaining_after": after_remaining,
            }

    return True, "broker_snapshot_late_fill_reconciled", {
        "late_fill_delta": delta,
        "broker_cumulative_filled": broker_qty,
        "durable_osm_cumulative_filled": durable_qty,
        "quantity_remaining_before": before_remaining,
        "quantity_remaining_after": (
            _strict_managed_quantity_remaining(pos)
            if before_remaining is not None else None
        ),
    }


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


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _exit_age_seconds(pos: Any, osm: Any, local_id: str) -> Optional[float]:
    """Use the same stale-age authority as the monitor, without inventing age."""
    for attr in ("last_exit_signal_ts", "last_callback_identity_missing_ts"):
        age = _dt_age_seconds(getattr(pos, attr, None))
        if age is not None:
            return age
    row = _order_row(osm, local_id)
    for key in ("submitted_ts", "created_ts"):
        age = _dt_age_seconds(_parse_timestamp(row.get(key)))
        if age is not None:
            return age
    return None


def _durable_osm_terminal_row(
    osm: Any, *, local_id: str, broker_id: str, position_id: str,
) -> bool:
    if not osm or not local_id:
        return False
    getter = getattr(osm, "get_order", None)
    if not callable(getter):
        return False
    try:
        row = getter(local_id)
        row = dict(row) if row is not None else {}
    except Exception:
        return False
    row_local = _norm(row.get("local_order_id") or local_id)
    row_broker = _norm(row.get("broker_order_id"))
    row_position = _norm(row.get("position_id"))
    row_status = _norm(row.get("status")).upper()
    return (
        row_local == _norm(local_id)
        and (not broker_id or row_broker == _norm(broker_id))
        and (not position_id or row_position == _norm(position_id))
        and row_status in {"CANCELED", "CANCELLED", "EXPIRED", "REJECTED"}
    )


def _mark_replacement_safe(
    exit_engine: Any,
    pid: str,
    *,
    osm: Any,
    reason: str,
    local_id: str,
    broker_id: str,
    details: dict,
    replacement_qty: int = 0,
) -> RecoveryAction:
    """Commit replacement authority only after the exact durable OSM fence."""
    if not osm or not local_id:
        return RecoveryAction(
            "NOOP", "durable_osm_cancel_identity_unproven", pid, local_id, broker_id, details,
        )
    if not exit_engine:
        return RecoveryAction(
            "NOOP", "missing_exit_engine_durable_handoff", pid, local_id, broker_id, details,
        )
    mark = getattr(exit_engine, "mark_exit_replacement_safe", None)
    finalize = getattr(exit_engine, "finalize_exit_replacement_safe", None)
    revoke = getattr(exit_engine, "revoke_exit_replacement_safe", None)
    clear = getattr(exit_engine, "clear_exit_in_flight", None)
    transition = getattr(osm, "transition", None)
    if not all(callable(fn) for fn in (mark, finalize, revoke, clear, transition)):
        return RecoveryAction(
            "NOOP", "durable_replacement_fence_unavailable", pid, local_id, broker_id, details,
        )

    force_reconciled = not bool(broker_id)
    try:
        staged = bool(
            mark(
                pid,
                reason=reason,
                local_order_id=local_id,
                broker_order_id=broker_id,
                replacement_qty=replacement_qty,
                defer_attempt=True,
                reconciled=force_reconciled,
            )
        )
    except Exception as exc:
        log.error("autonomous replacement staging failed for pos=%s: %s", pid, exc)
        staged = False
    if not staged:
        return RecoveryAction(
            "NOOP", "replacement_grant_not_staged", pid, local_id, broker_id, details,
        )

    try:
        transition_ok = bool(
            transition(
                local_id,
                "CANCELED",
                broker_order_id=broker_id or None,
                position_id=pid,
                last_error=reason,
            )
        )
    except Exception as exc:
        log.error("autonomous durable OSM CANCELED transition failed for order=%s: %s", local_id, exc)
        transition_ok = False

    if not transition_ok:
        transition_ok = _durable_osm_terminal_row(
            osm, local_id=local_id, broker_id=broker_id, position_id=pid,
        )
    if not transition_ok:
        try:
            revoke(
                pid,
                reason="autonomous_osm_cancel_transition_not_durable",
                local_order_id=local_id,
                broker_order_id=broker_id,
                force=force_reconciled,
            )
        except Exception as exc:
            log.error("autonomous replacement revoke failed for pos=%s: %s", pid, exc)
        return RecoveryAction(
            "NOOP", "durable_osm_cancel_unproven", pid, local_id, broker_id, details,
        )

    try:
        finalized = bool(
            finalize(
                pid,
                reason="autonomous_osm_cancel_durable_success",
                local_order_id=local_id,
                broker_order_id=broker_id,
                reconciled=force_reconciled,
            )
        )
    except Exception as exc:
        log.error("autonomous replacement finalize failed for pos=%s: %s", pid, exc)
        finalized = False
    if not finalized:
        # The durable STAGED record is intentionally retained.  Revoking here
        # would reopen the process-death seam between exact OSM terminality and
        # replacement lifecycle persistence; restart recovery can safely retry
        # the same old generation without exposing a replacement submit.
        return RecoveryAction(
            "NOOP", "replacement_generation_commit_failed_staged_retained", pid, local_id, broker_id, details,
        )

    try:
        clear(
            pid,
            reason=reason,
            local_order_id=local_id,
            broker_order_id=broker_id,
            reconciled=force_reconciled,
        )
    except Exception as exc:
        log.error("autonomous exact clear failed for pos=%s: %s", pid, exc)
        return RecoveryAction(
            "NOOP", "exact_exit_clear_failed", pid, local_id, broker_id, details,
        )
    committed_details = dict(details or {})
    committed_details["durable_osm_transition"] = "CANCELED"
    committed_details["replacement_generation_committed"] = True
    return RecoveryAction("REPLACEMENT_SAFE", reason, pid, local_id, broker_id, committed_details)


def _recover_known_open_exit_when_monitor_unavailable(
    pos: Any,
    *,
    broker: Any,
    exit_engine: Any,
    osm: Any,
    local_id: str,
    broker_id: str,
    status: str,
    quote_health_payload: dict,
    age_seconds: Optional[float] = None,
) -> RecoveryAction:
    """Bounded exact cancel fallback for a stale known broker-owned exit."""
    if age_seconds is None:
        age_seconds = _exit_age_seconds(pos, osm, local_id)
    if age_seconds is None or age_seconds < STALE_EXIT_RECOVERY_AGE_SECONDS:
        return RecoveryAction(
            "CONFIRMED_OPEN",
            "broker_order_still_open_monitor_unavailable_age_unproven",
            _position_id(pos), local_id, broker_id,
            {
                "status": status,
                "quote_health": quote_health_payload,
                "recovery_owner": "autonomous_recovery",
                "age_seconds": age_seconds,
                "stale_age_required": STALE_EXIT_RECOVERY_AGE_SECONDS,
            },
        )
    if status == "partially_filled":
        return RecoveryAction(
            "CONFIRMED_OPEN",
            "partial_fill_requires_canonical_fill_monitor",
            _position_id(pos), local_id, broker_id,
            {"status": status, "quote_health": quote_health_payload},
        )

    liveness = _read_stale_exit_cancel_liveness(osm, local_id, broker_id)
    prior_attempt = int(liveness.get("attempt", 0) or 0)
    if prior_attempt >= STALE_EXIT_CANCEL_MAX_ATTEMPTS:
        return RecoveryAction(
            "NOOP",
            "autonomous_exit_cancel_attempts_exhausted",
            _position_id(pos), local_id, broker_id,
            {
                "status": status,
                "cancel_attempt": prior_attempt,
                "cancel_max_attempts": STALE_EXIT_CANCEL_MAX_ATTEMPTS,
                "quote_health": quote_health_payload,
            },
        )
    if prior_attempt > 0:
        updated_at = _parse_timestamp(liveness.get("updated_at"))
        retry_after = _stale_exit_cancel_retry_after_seconds()
        if updated_at is None:
            return RecoveryAction(
                "NOOP",
                "autonomous_exit_cancel_retry_marker_timestamp_invalid",
                _position_id(pos), local_id, broker_id,
                {
                    "status": status,
                    "cancel_attempt": prior_attempt,
                    "cancel_max_attempts": STALE_EXIT_CANCEL_MAX_ATTEMPTS,
                    "updated_at": liveness.get("updated_at"),
                    "quote_health": quote_health_payload,
                },
            )
        if retry_after is None:
            return RecoveryAction(
                "NOOP",
                "autonomous_exit_cancel_retry_interval_unavailable",
                _position_id(pos), local_id, broker_id,
                {
                    "status": status,
                    "cancel_attempt": prior_attempt,
                    "cancel_max_attempts": STALE_EXIT_CANCEL_MAX_ATTEMPTS,
                    "quote_health": quote_health_payload,
                },
            )
        elapsed = (_now() - updated_at).total_seconds()
        if elapsed < retry_after:
            return RecoveryAction(
                "NOOP",
                "autonomous_exit_cancel_retry_not_due",
                _position_id(pos), local_id, broker_id,
                {
                    "status": status,
                    "cancel_attempt": prior_attempt,
                    "cancel_max_attempts": STALE_EXIT_CANCEL_MAX_ATTEMPTS,
                    "elapsed_sec": elapsed,
                    "retry_after_sec": retry_after,
                    "quote_health": quote_health_payload,
                },
            )

        # The first GET established that the order is currently open.  Once
        # the retry interval has elapsed, require another fresh recognized
        # live proof before consuming the next durable cancel attempt.
        try:
            fresh_raw = _get_order_for_contract(
                broker, broker_id, _position_contract(pos)
            )
        except _BrokerSnapshotUnavailable:
            fresh_raw = None
        fresh_status = _status(fresh_raw or {})
        if fresh_status == "partially_filled":
            return RecoveryAction(
                "CONFIRMED_OPEN",
                "partial_fill_requires_canonical_fill_monitor",
                _position_id(pos), local_id, broker_id,
                {
                    "status": status,
                    "fresh_status": fresh_status,
                    "cancel_attempt": prior_attempt,
                    "retry_after_sec": retry_after,
                    "quote_health": quote_health_payload,
                },
            )
        if fresh_status not in OPEN_BROKER_STATUSES:
            return RecoveryAction(
                "NOOP",
                "autonomous_exit_cancel_retry_working_proof_missing",
                _position_id(pos), local_id, broker_id,
                {
                    "status": status,
                    "fresh_status": fresh_status,
                    "cancel_attempt": prior_attempt,
                    "cancel_max_attempts": STALE_EXIT_CANCEL_MAX_ATTEMPTS,
                    "elapsed_sec": elapsed,
                    "retry_after_sec": retry_after,
                    "quote_health": quote_health_payload,
                },
            )
        status = fresh_status
    next_attempt = prior_attempt + 1
    if not _persist_stale_exit_cancel_attempt(
        osm,
        local_id,
        broker_id,
        next_attempt,
        execution_mode=str(getattr(pos, "execution_mode", "") or ""),
    ):
        return RecoveryAction(
            "NOOP",
            "autonomous_cancel_attempt_durability_unconfirmed",
            _position_id(pos), local_id, broker_id,
            {"status": status, "cancel_attempt": next_attempt, "quote_health": quote_health_payload},
        )
    ok, proof = _cancel_order_with_proof(
        broker,
        broker_id,
        expected_contract=_position_contract(pos),
    )
    details = {
        "status": status,
        "cancel_attempt": next_attempt,
        "cancel_max_attempts": STALE_EXIT_CANCEL_MAX_ATTEMPTS,
        "cancel_proof": proof,
        "quote_health": quote_health_payload,
        "recovery_owner": "autonomous_recovery",
    }
    if not ok:
        return RecoveryAction(
            "NOOP", "autonomous_cancel_not_proven", _position_id(pos), local_id, broker_id, details,
        )
    terminal_row, terminal_requested_qty, terminal_osm_reason = _exact_osm_exit_generation(
        osm,
        local_id=local_id,
        broker_id=broker_id,
        position_id=_position_id(pos),
        client_id=_norm(getattr(pos, "client_id", "")),
        execution_mode=str(getattr(pos, "execution_mode", "") or ""),
    )
    if terminal_row is None or terminal_requested_qty is None:
        details.update(
            {
                "error": terminal_osm_reason,
                "broker_mutation_blocked": True,
                "replacement_blocked": True,
            }
        )
        return RecoveryAction(
            "NOOP",
            "autonomous_recovery_terminal_cancel_osm_generation_unavailable",
            _position_id(pos),
            local_id,
            broker_id,
            details,
        )
    confirmed_payload = proof.get("confirmed_payload") if isinstance(proof, dict) else None
    terminal_filled_qty, terminal_fill_source = _terminal_cumulative_fill_authority(
        confirmed_payload if isinstance(confirmed_payload, dict) else {},
        terminal_row,
    )
    details.update(
        {
            "terminal_filled_qty": terminal_filled_qty,
            "terminal_fill_source": terminal_fill_source,
            "osm_requested_qty": terminal_requested_qty,
        }
    )
    if terminal_fill_source == "broker_snapshot_late_fill":
        late_fill_ok, late_fill_reason, late_fill_details = _reconcile_terminal_late_fill(
            pos=pos,
            exit_engine=exit_engine,
            osm=osm,
            local_id=local_id,
            broker_id=broker_id,
            position_id=_position_id(pos),
            terminal_row=terminal_row,
            requested_qty=terminal_requested_qty,
            broker_qty=terminal_filled_qty,
        )
        details.update(late_fill_details)
        if not late_fill_ok:
            details.update(
                {
                    "error": late_fill_reason,
                    "broker_mutation_blocked": True,
                    "replacement_blocked": True,
                }
            )
            return RecoveryAction(
                "NOOP",
                "autonomous_recovery_terminal_late_fill_reconciliation_failed",
                _position_id(pos),
                local_id,
                broker_id,
                details,
            )
        details["terminal_fill_source"] = late_fill_reason
    if terminal_filled_qty is None:
        details.update(
            {
                "error": terminal_fill_source,
                "broker_mutation_blocked": True,
                "replacement_blocked": True,
            }
        )
        return RecoveryAction(
            "NOOP",
            "autonomous_recovery_terminal_cancel_fill_quantity_unproven",
            _position_id(pos),
            local_id,
            broker_id,
            details,
        )
    if terminal_filled_qty > terminal_requested_qty:
        details.update(
            {
                "broker_mutation_blocked": True,
                "replacement_blocked": True,
            }
        )
        return RecoveryAction(
            "NOOP",
            "autonomous_recovery_terminal_cancel_fill_exceeds_osm_qty",
            _position_id(pos),
            local_id,
            broker_id,
            details,
        )
    replacement_qty = terminal_requested_qty - terminal_filled_qty
    if replacement_qty <= 0:
        details.update(
            {
                "broker_mutation_blocked": True,
                "replacement_blocked": True,
            }
        )
        return RecoveryAction(
            "NOOP",
            "autonomous_recovery_terminal_cancel_fill_exhausts_osm_qty",
            _position_id(pos),
            local_id,
            broker_id,
            details,
        )
    return _mark_replacement_safe(
        exit_engine,
        _position_id(pos),
        osm=osm,
        reason="autonomous_recovery_stale_known_exit_canceled",
        local_id=local_id,
        broker_id=broker_id,
        details=details,
        replacement_qty=replacement_qty,
    )


def _durable_replacement_lifecycle(pos: Any) -> tuple[Optional[dict], str]:
    """Read the canonical replacement lifecycle without inventing authority."""
    raw = getattr(pos, "exit_retry_liveness", None)
    if not isinstance(raw, dict) or "state" not in raw:
        return None, "absent"
    try:
        from ap_exit_engine import _parse_exit_retry_liveness_namespace

        lifecycle, error = _parse_exit_retry_liveness_namespace(raw)
    except Exception as exc:
        return None, f"parser_unavailable:{exc}"
    if lifecycle is None:
        return None, error
    if lifecycle.get("state") not in {
        "STAGED",
        "REPLACEMENT_PENDING",
        "REPLACEMENT_OWNED_BY_NEW_GENERATION",
    }:
        return None, "inactive"
    if (
        lifecycle.get("position_id") != _position_id(pos)
        or lifecycle.get("client_id") != _norm(getattr(pos, "client_id", ""))
        or lifecycle.get("execution_mode") != str(getattr(pos, "execution_mode", "") or "")
    ):
        return None, "position_client_mode_fence_mismatch"
    if not _norm(getattr(pos, "client_id", "")):
        return None, "missing_client_id"
    if str(getattr(pos, "execution_mode", "") or "") not in {"live", "paper"}:
        return None, "invalid_execution_mode"
    return lifecycle, "ok"


def _active_osm_exit_row(osm: Any, position_id: str) -> dict:
    """Find the exact newer active OSM EXIT generation, if one is durable."""
    if not osm or not position_id:
        return {}
    for method_name in ("_get_active_exit_order", "get_active_exit_order"):
        method = getattr(osm, method_name, None)
        if not callable(method):
            continue
        try:
            row = method(position_id)
            return dict(row) if row else {}
        except Exception:
            continue
    return {}


def _replacement_row_is_exact(
    row: dict,
    *,
    local_id: str,
    broker_id: str,
    position_id: str,
    client_id: str,
    execution_mode: str,
    active: bool = False,
    filled: bool = False,
) -> bool:
    if not isinstance(row, dict):
        return False
    status = _norm(row.get("status")).upper()
    if active:
        valid_status = status in {
            "EXIT_REQUESTED",
            "EXIT_SUBMITTED",
            "EXIT_ACKNOWLEDGED",
            "EXIT_PARTIAL_FILL",
        }
    elif filled:
        valid_status = status == "EXIT_FILLED"
    else:
        valid_status = status in {"CANCELED", "CANCELLED", "EXPIRED", "REJECTED"}
    return bool(
        _norm(row.get("local_order_id")) == _norm(local_id)
        and _norm(row.get("broker_order_id")) == _norm(broker_id)
        and _norm(row.get("position_id")) == _norm(position_id)
        and _norm(row.get("client_id")) == _norm(client_id)
        and str(row.get("execution_mode") or "") == str(execution_mode or "")
        and _norm(row.get("kind")).upper() == "EXIT"
        and valid_status
    )


def _strict_replacement_row_qty(row: dict) -> Optional[int]:
    value = row.get("qty") if row.get("qty") is not None else row.get("quantity")
    if isinstance(value, bool) or value is None:
        return None
    if type(value) is int:
        return value if value > 0 else None
    if isinstance(value, str) and value.isdigit():
        parsed = int(value)
        return parsed if parsed > 0 else None
    return None


def _strict_replacement_filled_qty(payload: dict) -> Optional[int]:
    """Read broker/OSM cumulative fill quantity without numeric coercion."""
    if not isinstance(payload, dict):
        return None
    value = next(
        (
            payload.get(key)
            for key in ("filled_qty", "filled_quantity", "exec_quantity")
            if key in payload and payload.get(key) is not None
        ),
        None,
    )
    if isinstance(value, bool) or value is None:
        return None
    if type(value) is int:
        return value if value >= 0 else None
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _recover_durable_replacement_pending(
    pos: Any,
    *,
    broker: Any,
    exit_engine: Any,
    osm: Any,
    order_monitor: Any = None,
) -> Optional[RecoveryAction]:
    """Recover a durable replacement obligation after process restart.

    This path is deliberately proof-only: it performs GET/list reads and
    restores one in-memory reservation. The broker POST remains owned by the
    normal exit-engine/OSM submit boundary.
    """
    lifecycle, lifecycle_reason = _durable_replacement_lifecycle(pos)
    if lifecycle is None:
        if lifecycle_reason not in {"absent", "inactive"}:
            return RecoveryAction(
                "NOOP",
                "replacement_lifecycle_invalid_fail_closed",
                _position_id(pos),
                details={"lifecycle_error": lifecycle_reason, "replacement_blocked": True},
            )
        return None

    pid = _position_id(pos)
    client_id = _norm(getattr(pos, "client_id", ""))
    execution_mode = str(getattr(pos, "execution_mode", "") or "")
    contract = _position_contract(pos)
    qh = quote_health(pos)
    state = lifecycle["state"]
    replacement_qty = lifecycle["replace_quantity"]
    old_local = lifecycle["old_local_order_id"]
    old_broker = lifecycle["old_broker_order_id"]
    try:
        quantity_remaining = int(getattr(pos, "quantity_remaining", 0) or 0)
    except (TypeError, ValueError, OverflowError):
        quantity_remaining = 0
    if replacement_qty > quantity_remaining:
        return RecoveryAction(
            "NOOP",
            "replacement_quantity_exceeds_durable_position_remainder",
            pid,
            old_local,
            old_broker,
            {"replacement_qty": replacement_qty, "quantity_remaining": quantity_remaining},
        )

    old_row = _order_row(osm, old_local)
    old_row_terminal = _replacement_row_is_exact(
        old_row,
        local_id=old_local,
        broker_id=old_broker,
        position_id=pid,
        client_id=client_id,
        execution_mode=execution_mode,
        active=False,
    )
    old_row_staged_active = (
        state == "STAGED"
        and _replacement_row_is_exact(
            old_row,
            local_id=old_local,
            broker_id=old_broker,
            position_id=pid,
            client_id=client_id,
            execution_mode=execution_mode,
            active=True,
        )
    )
    if not old_row_terminal and not old_row_staged_active:
        return RecoveryAction(
            "NOOP",
            "replacement_old_generation_not_durably_terminal",
            pid,
            old_local,
            old_broker,
            {"replacement_blocked": True, "lifecycle_state": state, "quote_health": qh},
        )

    try:
        old_raw = _get_order_for_contract(broker, old_broker, contract)
    except _BrokerSnapshotUnavailable as exc:
        return RecoveryAction(
            "NOOP",
            "replacement_old_generation_broker_proof_unavailable",
            pid,
            old_local,
            old_broker,
            {"replacement_blocked": True, "error": str(exc), "quote_health": qh},
        )
    old_status = _status(old_raw)
    if old_status == "filled":
        return RecoveryAction(
            "NOOP",
            "replacement_old_generation_filled_requires_canonical_fill_reconciliation",
            pid,
            old_local,
            old_broker,
            {"status": old_status, "replacement_blocked": True, "quote_health": qh},
        )
    if old_status in OPEN_BROKER_STATUSES:
        if state == "REPLACEMENT_OWNED_BY_NEW_GENERATION":
            return RecoveryAction(
                "NOOP",
                "replacement_old_generation_still_open",
                pid,
                old_local,
                old_broker,
                {"status": old_status, "replacement_blocked": True, "quote_health": qh},
            )
        if callable(getattr(exit_engine, "set_pending_exit_order", None)):
            exit_engine.set_pending_exit_order(
                pid,
                local_order_id=old_local,
                broker_order_id=old_broker,
                qty=_strict_replacement_row_qty(old_row) or replacement_qty,
                reason="durable_replacement_old_generation_still_open",
            )
        return RecoveryAction(
            "CONFIRMED_OPEN",
            "replacement_old_generation_still_open",
            pid,
            old_local,
            old_broker,
            {"status": old_status, "quote_health": qh, "replacement_blocked": True},
        )
    if old_status not in CANCEL_CONFIRMED_STATUSES:
        return RecoveryAction(
            "NOOP",
            "replacement_old_generation_broker_status_unrecognized",
            pid,
            old_local,
            old_broker,
            {"status": old_status, "replacement_blocked": True, "quote_health": qh},
        )

    old_requested_qty = _strict_replacement_row_qty(old_row)
    old_filled_qty, old_fill_source = _terminal_cumulative_fill_authority(
        old_raw,
        old_row,
    )
    if old_requested_qty is None or old_filled_qty is None:
        return RecoveryAction(
            "NOOP",
            "autonomous_recovery_terminal_cancel_fill_quantity_unproven",
            pid,
            old_local,
            old_broker,
            {
                "status": old_status,
                "error": (
                    "replacement_requested_quantity_unavailable"
                    if old_requested_qty is None
                    else old_fill_source
                ),
                "replacement_blocked": True,
                "quote_health": qh,
            },
        )
    if old_filled_qty > old_requested_qty:
        return RecoveryAction(
            "NOOP",
            "autonomous_recovery_terminal_cancel_fill_exceeds_osm_qty",
            pid,
            old_local,
            old_broker,
            {
                "status": old_status,
                "filled_qty": old_filled_qty,
                "osm_requested_qty": old_requested_qty,
                "replacement_blocked": True,
                "quote_health": qh,
            },
        )
    expected_replacement_qty = old_requested_qty - old_filled_qty
    if expected_replacement_qty != replacement_qty:
        return RecoveryAction(
            "NOOP",
            "autonomous_recovery_terminal_cancel_replacement_qty_mismatch",
            pid,
            old_local,
            old_broker,
            {
                "status": old_status,
                "filled_qty": old_filled_qty,
                "osm_requested_qty": old_requested_qty,
                "replacement_qty": replacement_qty,
                "expected_replacement_qty": expected_replacement_qty,
                "replacement_blocked": True,
                "quote_health": qh,
            },
        )

    # STAGED means the old broker order is terminal but the exact OSM fence
    # still needs to be completed. Re-run that handoff before restoring the
    # one-shot pending authority.
    if state == "STAGED":
        return _mark_replacement_safe(
            exit_engine,
            pid,
            osm=osm,
            reason="restart_recovery_staged_replacement_terminal_proof",
            local_id=old_local,
            broker_id=old_broker,
            details={
                "status": old_status,
                "quote_health": qh,
                "restart_recovery": True,
                "terminal_filled_qty": old_filled_qty,
                "terminal_fill_source": old_fill_source,
            },
            replacement_qty=replacement_qty,
        )

    if state == "REPLACEMENT_OWNED_BY_NEW_GENERATION":
        new_local = lifecycle.get("new_local_order_id") or ""
        new_broker = lifecycle.get("new_broker_order_id") or ""
        new_row = _order_row(osm, new_local)
        row_broker = new_broker or _norm(new_row.get("broker_order_id"))
        if _replacement_row_is_exact(
            new_row,
            local_id=new_local,
            broker_id=row_broker,
            position_id=pid,
            client_id=client_id,
            execution_mode=execution_mode,
            filled=True,
        ):
            row_filled = _strict_replacement_filled_qty(new_row)
            if row_filled != replacement_qty:
                return RecoveryAction(
                    "NOOP",
                    "replacement_owned_generation_filled_quantity_mismatch",
                    pid,
                    new_local,
                    row_broker,
                    {
                        "replacement_qty": replacement_qty,
                        "row_filled_qty": row_filled,
                        "replacement_blocked": True,
                    },
                )
            try:
                new_raw = _get_order_for_contract(broker, row_broker, contract)
            except _BrokerSnapshotUnavailable as exc:
                return RecoveryAction(
                    "NOOP",
                    "replacement_owned_generation_fill_proof_unavailable",
                    pid,
                    new_local,
                    row_broker,
                    {"error": str(exc), "replacement_blocked": True, "quote_health": qh},
                )
            if _status(new_raw) != "filled":
                return RecoveryAction(
                    "NOOP",
                    "replacement_owned_generation_fill_status_unrecognized",
                    pid,
                    new_local,
                    row_broker,
                    {"status": _status(new_raw), "replacement_blocked": True, "quote_health": qh},
                )
            broker_filled_raw = _strict_replacement_filled_qty(new_raw)
            broker_fill_keys_present = any(
                key in new_raw and new_raw.get(key) is not None
                for key in ("filled_qty", "filled_quantity", "exec_quantity")
            )
            if broker_fill_keys_present and broker_filled_raw is None:
                return RecoveryAction(
                    "NOOP",
                    "replacement_owned_generation_broker_fill_quantity_invalid",
                    pid,
                    new_local,
                    row_broker,
                    {"replacement_blocked": True, "quote_health": qh},
                )
            broker_filled = row_filled if broker_filled_raw is None else broker_filled_raw
            if broker_filled != replacement_qty:
                return RecoveryAction(
                    "NOOP",
                    "replacement_owned_generation_broker_fill_quantity_mismatch",
                    pid,
                    new_local,
                    row_broker,
                    {
                        "replacement_qty": replacement_qty,
                        "broker_filled_qty": broker_filled,
                        "replacement_blocked": True,
                    },
                )
            consume = getattr(exit_engine, "consume_replacement_lifecycle_after_fill", None)
            if not callable(consume):
                return RecoveryAction(
                    "NOOP",
                    "replacement_fill_lifecycle_consumer_unavailable",
                    pid,
                    new_local,
                    row_broker,
                    {"replacement_blocked": True, "quote_health": qh},
                )
            try:
                consumed = bool(
                    consume(
                        pid,
                        local_order_id=new_local,
                        broker_order_id=row_broker,
                        filled_qty=broker_filled,
                    )
                )
            except Exception as exc:
                log.error("replacement fill lifecycle consumption failed for pos=%s: %s", pid, exc)
                consumed = False
            if not consumed:
                return RecoveryAction(
                    "NOOP",
                    "replacement_fill_lifecycle_consumption_unconfirmed",
                    pid,
                    new_local,
                    row_broker,
                    {"replacement_blocked": True, "quote_health": qh},
                )
            return RecoveryAction(
                "REPLACEMENT_FILL_CONSUMED",
                "durable_replacement_fill_released_liveness",
                pid,
                new_local,
                row_broker,
                {
                    "replacement_qty": replacement_qty,
                    "filled_qty": broker_filled,
                    "quote_health": qh,
                    "duplicate_submit_blocked": False,
                },
            )
        if not _replacement_row_is_exact(
            new_row,
            local_id=new_local,
            broker_id=row_broker,
            position_id=pid,
            client_id=client_id,
            execution_mode=execution_mode,
            active=True,
        ):
            return RecoveryAction(
                "NOOP",
                "replacement_owned_generation_reconciliation_required",
                pid,
                new_local,
                new_broker,
                {"old_local_order_id": old_local, "old_broker_order_id": old_broker, "replacement_blocked": True},
            )
        if _strict_replacement_row_qty(new_row) != replacement_qty:
            return RecoveryAction(
                "NOOP",
                "replacement_owned_generation_quantity_mismatch",
                pid,
                new_local,
                row_broker,
                {"replacement_qty": replacement_qty, "row_qty": _strict_replacement_row_qty(new_row), "replacement_blocked": True},
            )
        if callable(getattr(exit_engine, "set_pending_exit_order", None)):
            exit_engine.set_pending_exit_order(
                pid,
                local_order_id=new_local,
                broker_order_id=row_broker,
                qty=replacement_qty,
                reason="durable_replacement_owned_generation_restored",
            )
        return RecoveryAction(
            "REPLACEMENT_OWNED",
            "durable_replacement_new_generation_already_owned",
            pid,
            new_local,
            row_broker,
            {"replacement_qty": replacement_qty, "quote_health": qh, "duplicate_submit_blocked": True},
        )

    # A crash can leave a durable OSM reservation before the broker POST (or
    # before its broker identity is copied back). Treat that exact newer row
    # as the existing replacement generation; never reserve a second one.
    reserved_row = _active_osm_exit_row(osm, pid)
    if reserved_row:
        reserved_local = _norm(reserved_row.get("local_order_id"))
        reserved_broker = _norm(reserved_row.get("broker_order_id"))
        if (
            not reserved_local
            or not _replacement_row_is_exact(
                reserved_row,
                local_id=reserved_local,
                broker_id=reserved_broker,
                position_id=pid,
                client_id=client_id,
                execution_mode=execution_mode,
                active=True,
            )
            or _strict_replacement_row_qty(reserved_row) != replacement_qty
        ):
            return RecoveryAction(
                "NOOP",
                "replacement_reserved_generation_owner_unproven",
                pid,
                reserved_local,
                reserved_broker,
                {"replacement_blocked": True, "quote_health": qh},
            )
        if not callable(getattr(exit_engine, "set_pending_exit_order", None)):
            return RecoveryAction(
                "NOOP",
                "replacement_reserved_generation_restore_unavailable",
                pid,
                reserved_local,
                reserved_broker,
                {"replacement_blocked": True, "quote_health": qh},
            )
        exit_engine.set_pending_exit_order(
            pid,
            local_order_id=reserved_local,
            broker_order_id=reserved_broker,
            qty=replacement_qty,
            reason="durable_replacement_reserved_generation_restored",
        )
        if reserved_broker:
            lifecycle_after, _ = _durable_replacement_lifecycle(pos)
            if not lifecycle_after or lifecycle_after.get("state") != "REPLACEMENT_OWNED_BY_NEW_GENERATION":
                return RecoveryAction(
                    "NOOP",
                    "replacement_reserved_generation_ownership_persist_unconfirmed",
                    pid,
                    reserved_local,
                    reserved_broker,
                    {"replacement_blocked": True, "quote_health": qh},
            )
            return RecoveryAction(
                "REPLACEMENT_OWNED",
                "durable_replacement_reserved_generation_reconciled",
                pid,
                reserved_local,
                reserved_broker,
                {"replacement_qty": replacement_qty, "quote_health": qh, "duplicate_submit_blocked": True},
            )
        return RecoveryAction(
            "REPLACEMENT_RESERVED",
            "durable_replacement_reserved_before_broker_identity",
            pid,
            reserved_local,
            "",
            {"replacement_qty": replacement_qty, "quote_health": qh, "duplicate_submit_blocked": True},
        )

    open_orders_available, matches = _matching_open_exit_orders(
        broker, contract, exclude_broker_id=old_broker,
    )
    if not open_orders_available:
        return RecoveryAction(
            "NOOP",
            "replacement_new_generation_open_order_query_unavailable",
            pid,
            old_local,
            old_broker,
            {"replacement_blocked": True, "quote_health": qh},
        )
    if len(matches) > 1:
        return RecoveryAction(
            "NOOP",
            "replacement_new_generation_identity_ambiguous",
            pid,
            old_local,
            old_broker,
            {"matches": [bid for bid, _ in matches], "replacement_blocked": True, "quote_health": qh},
        )
    if len(matches) == 1:
        new_broker, new_raw = matches[0]
        broker_qty = _qty(new_raw)
        if broker_qty not in (0, replacement_qty):
            return RecoveryAction(
                "NOOP",
                "replacement_new_generation_quantity_mismatch",
                pid,
                old_local,
                new_broker,
                {"replacement_qty": replacement_qty, "broker_qty": broker_qty, "replacement_blocked": True},
            )
        new_row = _active_osm_exit_row(osm, pid)
        new_local = _norm(new_row.get("local_order_id"))
        if not _replacement_row_is_exact(
            new_row,
            local_id=new_local,
            broker_id=new_broker,
            position_id=pid,
            client_id=client_id,
            execution_mode=execution_mode,
            active=True,
        ) or _strict_replacement_row_qty(new_row) != replacement_qty:
            return RecoveryAction(
                "NOOP",
                "replacement_new_generation_osm_owner_unproven",
                pid,
                new_local,
                new_broker,
                {"replacement_blocked": True, "quote_health": qh},
            )
        if callable(getattr(exit_engine, "set_pending_exit_order", None)):
            exit_engine.set_pending_exit_order(
                pid,
                local_order_id=new_local,
                broker_order_id=new_broker,
                qty=replacement_qty,
                reason="durable_replacement_new_generation_found",
            )
        lifecycle_after, _ = _durable_replacement_lifecycle(pos)
        if not lifecycle_after or lifecycle_after.get("state") != "REPLACEMENT_OWNED_BY_NEW_GENERATION":
            return RecoveryAction(
                "NOOP",
                "replacement_new_generation_ownership_persist_unconfirmed",
                pid,
                new_local,
                new_broker,
                {"replacement_blocked": True, "quote_health": qh},
            )
        return RecoveryAction(
            "REPLACEMENT_OWNED",
            "durable_replacement_new_generation_reconciled",
            pid,
            new_local,
            new_broker,
            {"replacement_qty": replacement_qty, "quote_health": qh, "duplicate_submit_blocked": True},
        )

    positions_available, broker_positions = _list_broker_positions(broker)
    if not positions_available:
        return RecoveryAction(
            "NOOP",
            "replacement_pending_position_query_unavailable",
            pid,
            old_local,
            old_broker,
            {"replacement_blocked": True, "quote_health": qh},
        )
    held = any(_contract(row) == contract and _qty(row) > 0 for row in broker_positions)
    if not held:
        return RecoveryAction(
            "NOOP",
            "replacement_pending_broker_contract_not_held",
            pid,
            old_local,
            old_broker,
            {"replacement_blocked": True, "quote_health": qh},
        )
    revalidate = getattr(exit_engine, "revalidate_durable_replacement_pending", None)
    if not callable(revalidate) or not revalidate(
        pid,
        old_local_order_id=old_local,
        old_broker_order_id=old_broker,
        replacement_qty=replacement_qty,
    ):
        return RecoveryAction(
            "NOOP",
            "replacement_pending_runtime_revalidation_failed",
            pid,
            old_local,
            old_broker,
            {"replacement_blocked": True, "quote_health": qh},
        )
    return RecoveryAction(
        "REPLACEMENT_PENDING",
        "durable_replacement_pending_revalidated",
        pid,
        old_local,
        old_broker,
        {"replacement_qty": replacement_qty, "quote_health": qh, "one_shot_submit": True},
    )


def recover_exit_position(
    pos: Any, *, broker: Any, exit_engine: Any = None, osm: Any = None,
    order_monitor: Any = None,
) -> RecoveryAction:
    pid = _position_id(pos)
    local_id, pending_broker_id = _pending_identity(pos)
    contract = _position_contract(pos)
    qh = quote_health(pos)

    if not broker or not pid:
        return RecoveryAction("NOOP", "missing_broker_or_position_id", pid, local_id, pending_broker_id, {"quote_health": qh})

    terminal_exact_broker_id = ""
    terminal_broker_snapshot: Optional[dict] = None
    terminal_replacement_qty: Optional[int] = None
    terminal_late_fill_details: dict = {}

    # Exact broker identity path.
    if pending_broker_id:
        # APExitEngine.set_pending_exit_order() refreshes its in-memory signal
        # timestamp while it adopts the exact broker identity.  Preserve the
        # stale-age proof from before that reconciliation so a dead monitor can
        # still take the bounded autonomous handoff.
        pre_reconciliation_age_seconds = _exit_age_seconds(pos, osm, local_id)
        try:
            raw = _get_order_for_contract(broker, pending_broker_id, contract)
        except _BrokerSnapshotUnavailable as exc:
            return RecoveryAction(
                "NOOP",
                "autonomous_recovery_exact_order_query_unavailable",
                pid,
                local_id,
                pending_broker_id,
                {
                    "quote_health": qh,
                    "broker_truth_unavailable": True,
                    "exact_order_query_available": False,
                    "broker_mutation_blocked": True,
                    "replacement_blocked": True,
                    "error": str(exc),
                },
            )
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
                if _order_monitor_alive(order_monitor):
                    return RecoveryAction(
                        "CONFIRMED_OPEN",
                        "broker_order_still_open",
                        pid,
                        local_id,
                        pending_broker_id,
                        {
                            "status": st,
                            "quote_health": qh,
                            "recovery_owner": "order_monitor_stale_exit",
                        },
                    )
                return _recover_known_open_exit_when_monitor_unavailable(
                    pos,
                    broker=broker,
                    exit_engine=exit_engine,
                    osm=osm,
                    local_id=local_id,
                    broker_id=pending_broker_id,
                    status=st,
                    quote_health_payload=qh,
                    age_seconds=pre_reconciliation_age_seconds,
                )
            if st == "filled":
                filled_qty = _strict_filled_order_quantity(raw)
                if filled_qty is None:
                    return RecoveryAction(
                        "NOOP",
                        "autonomous_recovery_filled_quantity_unproven",
                        pid,
                        local_id,
                        pending_broker_id,
                        {
                            "status": st,
                            "quote_health": qh,
                            "broker_truth_unavailable": False,
                            "broker_authority_malformed": True,
                            "broker_mutation_blocked": True,
                            "replacement_blocked": True,
                        },
                    )
                fill_price = None
                for key in ("avg_fill_price", "average_fill_price", "fill_price", "filled_avg_price", "price"):
                    try:
                        value = raw.get(key)
                        if value in (None, "") or isinstance(value, bool):
                            continue
                        if isinstance(value, str) and value != value.strip():
                            continue
                        parsed = float(value)
                        if math.isfinite(parsed) and parsed > 0:
                            if fill_price is not None and parsed != fill_price:
                                fill_price = None
                                break
                            fill_price = parsed
                    except Exception:
                        pass

                managed_remaining = _strict_managed_quantity_remaining(pos)
                if managed_remaining is None:
                    return RecoveryAction(
                        "NOOP",
                        "autonomous_recovery_managed_position_remainder_unproven",
                        pid,
                        local_id,
                        pending_broker_id,
                        {
                            "status": st,
                            "filled_qty": filled_qty,
                            "quote_health": qh,
                            "broker_mutation_blocked": True,
                            "replacement_blocked": True,
                        },
                    )

                osm_row, requested_qty, osm_reason = _exact_osm_exit_generation(
                    osm,
                    local_id=local_id,
                    broker_id=pending_broker_id,
                    position_id=pid,
                    client_id=_norm(getattr(pos, "client_id", "")),
                    execution_mode=str(getattr(pos, "execution_mode", "") or ""),
                )
                if osm_row is None or requested_qty is None:
                    return RecoveryAction(
                        "NOOP",
                        "autonomous_recovery_exact_osm_exit_generation_unavailable",
                        pid,
                        local_id,
                        pending_broker_id,
                        {
                            "status": st,
                            "filled_qty": filled_qty,
                            "quantity_remaining": managed_remaining,
                            "quote_health": qh,
                            "error": osm_reason,
                            "broker_mutation_blocked": True,
                            "position_mutation_blocked": True,
                            "replacement_blocked": True,
                        },
                    )
                if filled_qty != requested_qty:
                    return RecoveryAction(
                        "NOOP",
                        "autonomous_recovery_broker_filled_qty_osm_requested_qty_mismatch",
                        pid,
                        local_id,
                        pending_broker_id,
                        {
                            "status": st,
                            "filled_qty": filled_qty,
                            "osm_requested_qty": requested_qty,
                            "quantity_remaining": managed_remaining,
                            "quote_health": qh,
                            "broker_mutation_blocked": True,
                            "position_mutation_blocked": True,
                            "replacement_blocked": True,
                        },
                    )

                if filled_qty > managed_remaining:
                    return RecoveryAction(
                        "NOOP",
                        "autonomous_recovery_filled_quantity_exceeds_managed_remainder",
                        pid,
                        local_id,
                        pending_broker_id,
                        {
                            "status": st,
                            "filled_qty": filled_qty,
                            "quantity_remaining": managed_remaining,
                            "quote_health": qh,
                            "position_mutation_blocked": True,
                            "replacement_blocked": True,
                        },
                    )

                # A broker order can be FILLED while representing only a
                # scale-out tranche.  Route that exact cumulative fill through
                # the canonical accounting hook; mark_position_closed() is a
                # full-position authority and unconditionally zeros the local
                # remainder once invoked.
                if filled_qty < managed_remaining:
                    partial_fill_hook = getattr(exit_engine, "note_partial_exit_fill", None) if exit_engine else None
                    if not callable(partial_fill_hook):
                        return RecoveryAction(
                            "NOOP",
                            "autonomous_recovery_partial_fill_hook_unavailable",
                            pid,
                            local_id,
                            pending_broker_id,
                            {
                                "status": st,
                                "filled_qty": filled_qty,
                                "quantity_remaining": managed_remaining,
                                "quote_health": qh,
                                "position_mutation_blocked": True,
                                "replacement_blocked": True,
                            },
                        )
                    try:
                        partial_fill_hook(
                            pid,
                            qty_filled=filled_qty,
                            fill_price=fill_price,
                            local_order_id=local_id,
                            broker_order_id=pending_broker_id,
                            cumulative_filled=filled_qty,
                        )
                    except Exception as _partial_exc:
                        log.warning(
                            "exit_autonomous_recovery: broker-filled partial accounting failed: %s",
                            _partial_exc,
                        )
                        return RecoveryAction(
                            "NOOP",
                            "autonomous_recovery_broker_filled_partial_accounting_failed",
                            pid,
                            local_id,
                            pending_broker_id,
                            {
                                "status": st,
                                "filled_qty": filled_qty,
                                "quantity_remaining": managed_remaining,
                                "quote_health": qh,
                                "replacement_blocked": True,
                            },
                        )

                    remaining_after = _strict_managed_quantity_remaining(pos)
                    tranche_completed = (
                        remaining_after is not None
                        and not bool(getattr(pos, "closed", False))
                        and not bool(getattr(pos, "exit_in_flight", False))
                        and type(getattr(pos, "pending_exit_qty", None)) is int
                        and getattr(pos, "pending_exit_qty", None) == 0
                    )
                    if not tranche_completed:
                        return RecoveryAction(
                            "NOOP",
                            "autonomous_recovery_broker_filled_partial_accounting_unconfirmed",
                            pid,
                            local_id,
                            pending_broker_id,
                            {
                                "status": st,
                                "filled_qty": filled_qty,
                                "quantity_remaining": remaining_after,
                                "quote_health": qh,
                                "position_mutation_blocked": True,
                                "replacement_blocked": True,
                            },
                        )
                    return RecoveryAction(
                        "CONFIRMED_OPEN",
                        "broker_order_filled_partial_position",
                        pid,
                        local_id,
                        pending_broker_id,
                        {
                            "status": st,
                            "filled_qty": filled_qty,
                            "quantity_remaining_before": managed_remaining,
                            "quantity_remaining": remaining_after,
                            "exit_tranche_completed": True,
                            "full_position_proof": False,
                            "quote_health": qh,
                        },
                    )

                close_hook = getattr(exit_engine, "mark_position_closed", None) if exit_engine else None
                if callable(close_hook):
                    try:
                        close_hook(
                            pid,
                            reason="AUTONOMOUS_RECOVERY_BROKER_FILLED",
                            qty_filled=filled_qty,
                            fill_price=fill_price,
                            local_order_id=local_id,
                            broker_order_id=pending_broker_id,
                            cumulative_filled=filled_qty,
                            reconciled=True,
                            economic_pending=fill_price is None,
                        )
                    except Exception as _close_exc:
                        log.warning(
                            "exit_autonomous_recovery: broker-filled close failed: %s",
                            _close_exc,
                        )
                        return RecoveryAction(
                            "NOOP",
                            "autonomous_recovery_broker_filled_close_failed",
                            pid,
                            local_id,
                            pending_broker_id,
                            {
                                "status": st,
                                "filled_qty": filled_qty,
                                "quote_health": qh,
                                "replacement_blocked": True,
                            },
                        )
                else:
                    return RecoveryAction(
                        "NOOP",
                        "autonomous_recovery_broker_filled_close_hook_unavailable",
                        pid,
                        local_id,
                        pending_broker_id,
                        {
                            "status": st,
                            "filled_qty": filled_qty,
                            "quote_health": qh,
                            "replacement_blocked": True,
                        },
                    )
                if not _position_close_confirmed(pos):
                    return RecoveryAction(
                        "NOOP",
                        "autonomous_recovery_broker_filled_close_unconfirmed",
                        pid,
                        local_id,
                        pending_broker_id,
                        {
                            "status": st,
                            "filled_qty": filled_qty,
                            "quote_health": qh,
                            "replacement_blocked": True,
                        },
                    )
                return RecoveryAction("MARKED_CLOSED", "broker_order_filled", pid, local_id, pending_broker_id, {"status": st, "filled_qty": filled_qty, "quote_health": qh})
            if st in TERMINAL_BROKER_STATUSES:
                # CRITICAL safety: terminal status for the old pending id is NOT enough.
                # Scan broker for a different live exit on the same contract before allowing replacement.
                open_orders_available, other_matches = _matching_open_exit_orders(
                    broker, contract, exclude_broker_id=pending_broker_id,
                )
                if not open_orders_available:
                    return RecoveryAction(
                        "NOOP",
                        "autonomous_recovery_open_order_query_unavailable",
                        pid,
                        local_id,
                        pending_broker_id,
                        {
                            "status": st,
                            "contract": contract,
                            "quote_health": qh,
                            "broker_truth_unavailable": True,
                            "open_order_query_available": False,
                            "broker_mutation_blocked": True,
                            "replacement_blocked": True,
                        },
                    )
                if len(other_matches) == 1:
                    other_bid, other_raw = other_matches[0]
                    if exit_engine and hasattr(exit_engine, "set_pending_exit_order"):
                        exit_engine.set_pending_exit_order(
                            pid,
                            local_order_id=local_id,
                            broker_order_id=other_bid,
                            qty=int(_qty(other_raw) or getattr(pos, "pending_exit_qty", 0) or 0),
                            reason="autonomous_recovery_found_different_open_exit",
                        )
                    return RecoveryAction("CONFIRMED_OPEN", "different_broker_exit_still_open", pid, local_id, other_bid, {"old_status": st, "contract": contract, "quote_health": qh})
                if len(other_matches) > 1:
                    return RecoveryAction("NOOP", "multiple_different_open_exits_block_replacement", pid, local_id, pending_broker_id, {"old_status": st, "matches": [m[0] for m in other_matches], "quote_health": qh})
                terminal_exact_broker_id = pending_broker_id
                terminal_broker_snapshot = dict(raw)
                # Do not authorize replacement from terminal status plus zero
                # other orders alone.  Fall through to the shared negative-
                # proof path so an authoritative flat position closes and an
                # unavailable position snapshot blocks all mutation.
            else:
                return RecoveryAction("NOOP", "broker_order_ambiguous_status", pid, local_id, pending_broker_id, {"status": st, "quote_health": qh})

    # No exact live exit remains: scan open orders for a matching exit order.
    open_orders_available, matches = _matching_open_exit_orders(broker, contract)

    if not open_orders_available:
        return RecoveryAction(
            "NOOP",
            "autonomous_recovery_open_order_query_unavailable",
            pid,
            local_id,
            pending_broker_id,
            {
                "contract": contract,
                "quote_health": qh,
                "broker_truth_unavailable": True,
                "open_order_query_available": False,
                "broker_mutation_blocked": True,
                "replacement_blocked": True,
            },
        )

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
        # P0 safety invariant:
        # Contract/side matching is discovery evidence only. It is NOT
        # sufficient authority to mutate broker orders. When more than one
        # live exit matches the position contract, there is no provable
        # one-to-one mapping between the local OSM generation and any broker
        # order. Therefore autonomous recovery must perform ZERO broker
        # mutations regardless of order-monitor health.
        match_details = [
            {
                "broker_order_id": bid,
                "status": _status(raw),
                "qty": _qty(raw),
            }
            for bid, raw in matches
        ]

        log.error(
            "ambiguous live exit identity — refusing broker mutation | "
            "position_id=%s local_order_id=%s contract=%s matches=%s",
            pid,
            local_id,
            contract,
            [m["broker_order_id"] for m in match_details],
        )

        return RecoveryAction(
            "NOOP",
            "multiple_live_exit_orders_identity_ambiguous",
            pid,
            local_id,
            "",
            {
                "contract": contract,
                "match_count": len(matches),
                "matches": match_details,
                "quote_health": qh,
                "recovery_owner": "none_identity_ambiguous",
                "broker_mutation_blocked": True,
                "replacement_blocked": True,
            },
        )

    # Negative proof: no matching open sell-to-close order currently at broker.
    # Before marking replacement safe, verify the contract is still held.
    # If position is flat at the broker (exit filled, callback dropped), close it
    # instead of spawning a duplicate sell-to-close that Tradier will reject.
    positions_available, broker_positions = _list_broker_positions(broker)
    if not positions_available:
        return RecoveryAction(
            "NOOP",
            "autonomous_recovery_position_query_unavailable",
            pid,
            local_id,
            pending_broker_id,
            {
                "contract": contract,
                "quote_health": qh,
                "broker_truth_unavailable": True,
                "open_order_query_available": True,
                "position_query_available": False,
                "broker_mutation_blocked": True,
                "replacement_blocked": True,
            },
        )

    def _broker_position_quantity(row: dict) -> float:
        for key in ("quantity", "qty", "position_qty", "long_quantity"):
            if key in row:
                try:
                    return float(row.get(key) or 0)
                except (TypeError, ValueError, OverflowError):
                    return 0.0
        return 0.0

    _contract_held = any(
        _contract(p) == contract and _broker_position_quantity(p) != 0
        for p in broker_positions
    )

    if terminal_exact_broker_id:
        terminal_row, terminal_requested_qty, terminal_osm_reason = _exact_osm_exit_generation(
            osm,
            local_id=local_id,
            broker_id=terminal_exact_broker_id,
            position_id=pid,
            client_id=_norm(getattr(pos, "client_id", "")),
            execution_mode=str(getattr(pos, "execution_mode", "") or ""),
        )
        if terminal_row is None or terminal_requested_qty is None:
            return RecoveryAction(
                "NOOP",
                "autonomous_recovery_terminal_cancel_osm_generation_unavailable",
                pid,
                local_id,
                terminal_exact_broker_id,
                {
                    "contract": contract,
                    "quote_health": qh,
                    "error": terminal_osm_reason,
                    "broker_mutation_blocked": True,
                    "replacement_blocked": True,
                },
            )
        terminal_filled_qty, terminal_fill_source = _terminal_cumulative_fill_authority(
            terminal_broker_snapshot or {},
            terminal_row,
        )
        if terminal_fill_source == "broker_snapshot_late_fill":
            late_fill_ok, late_fill_reason, terminal_late_fill_details = _reconcile_terminal_late_fill(
                pos=pos,
                exit_engine=exit_engine,
                osm=osm,
                local_id=local_id,
                broker_id=terminal_exact_broker_id,
                position_id=pid,
                terminal_row=terminal_row,
                requested_qty=terminal_requested_qty,
                broker_qty=terminal_filled_qty,
            )
            if not late_fill_ok:
                return RecoveryAction(
                    "NOOP",
                    "autonomous_recovery_terminal_late_fill_reconciliation_failed",
                    pid,
                    local_id,
                    terminal_exact_broker_id,
                    {
                        "contract": contract,
                        "quote_health": qh,
                        "error": late_fill_reason,
                        **terminal_late_fill_details,
                        "broker_mutation_blocked": True,
                        "replacement_blocked": True,
                    },
                )
            terminal_fill_source = late_fill_reason
        if terminal_filled_qty is None:
            return RecoveryAction(
                "NOOP",
                "autonomous_recovery_terminal_cancel_fill_quantity_unproven",
                pid,
                local_id,
                terminal_exact_broker_id,
                {
                    "contract": contract,
                    "quote_health": qh,
                    "error": terminal_fill_source,
                    "osm_requested_qty": terminal_requested_qty,
                    "broker_mutation_blocked": True,
                    "replacement_blocked": True,
                },
            )
        if terminal_filled_qty > terminal_requested_qty:
            return RecoveryAction(
                "NOOP",
                "autonomous_recovery_terminal_cancel_fill_exceeds_osm_qty",
                pid,
                local_id,
                terminal_exact_broker_id,
                {
                    "contract": contract,
                    "quote_health": qh,
                    "filled_qty": terminal_filled_qty,
                    "osm_requested_qty": terminal_requested_qty,
                    "broker_mutation_blocked": True,
                    "replacement_blocked": True,
                },
            )
        terminal_replacement_qty = terminal_requested_qty - terminal_filled_qty
        if _contract_held and terminal_replacement_qty <= 0:
            return RecoveryAction(
                "NOOP",
                "autonomous_recovery_terminal_cancel_fill_exhausts_osm_qty_position_held",
                pid,
                local_id,
                terminal_exact_broker_id,
                {
                    "contract": contract,
                    "quote_health": qh,
                    "filled_qty": terminal_filled_qty,
                    "osm_requested_qty": terminal_requested_qty,
                    "broker_mutation_blocked": True,
                    "replacement_blocked": True,
                },
            )

    if not _contract_held and contract:
        # Position is flat at broker — exit filled but callback was dropped.
        # Use the actual mark_position_closed contract and verify the engine
        # performed the close before claiming economic completion.
        close_qty = (
            terminal_filled_qty
            if terminal_exact_broker_id
            else (
                getattr(pos, "pending_exit_qty", 0)
                or getattr(pos, "contracts", 0)
                or getattr(pos, "quantity_remaining", 0)
            )
        )
        try:
            close_qty = int(float(close_qty or 0))
        except (TypeError, ValueError, OverflowError):
            return RecoveryAction(
                "NOOP",
                "autonomous_recovery_broker_flat_close_quantity_unavailable",
                pid,
                local_id,
                "",
                {
                    "contract": contract,
                    "quote_health": qh,
                    "source": "negative_proof_position_check",
                    "terminal_fill_source": (
                        terminal_fill_source if terminal_exact_broker_id else ""
                    ),
                    "broker_truth_unavailable": False,
                    "open_order_query_available": True,
                    "position_query_available": True,
                    "replacement_blocked": True,
                },
            )
        close_hook = getattr(exit_engine, "mark_position_closed", None) if exit_engine else None
        if not callable(close_hook):
            return RecoveryAction(
                "NOOP",
                "autonomous_recovery_broker_flat_close_hook_unavailable",
                pid,
                local_id,
                "",
                {
                    "contract": contract,
                    "quote_health": qh,
                    "source": "negative_proof_position_check",
                    "terminal_fill_source": (
                        terminal_fill_source if terminal_exact_broker_id else ""
                    ),
                    "broker_truth_unavailable": False,
                    "open_order_query_available": True,
                    "position_query_available": True,
                    "replacement_blocked": True,
                },
            )
        try:
            close_hook(
                pid,
                reason="AUTONOMOUS_RECOVERY_BROKER_FLAT",
                qty_filled=close_qty,
                fill_price=None,
                local_order_id=local_id,
                broker_order_id="",
                cumulative_filled=close_qty,
                reconciled=True,
                economic_pending=True,
            )
        except Exception as _close_exc:
            log.warning("exit_autonomous_recovery: broker-flat close failed: %s", _close_exc)
            return RecoveryAction(
                "NOOP",
                "autonomous_recovery_broker_flat_close_failed",
                pid,
                local_id,
                "",
                {
                    "contract": contract,
                    "quote_health": qh,
                    "source": "negative_proof_position_check",
                    "terminal_fill_source": (
                        terminal_fill_source if terminal_exact_broker_id else ""
                    ),
                    "broker_truth_unavailable": False,
                    "open_order_query_available": True,
                    "position_query_available": True,
                    "replacement_blocked": True,
                },
            )

        if not _position_close_confirmed(pos):
            return RecoveryAction(
                "NOOP",
                "autonomous_recovery_broker_flat_close_unconfirmed",
                pid,
                local_id,
                "",
                {
                    "contract": contract,
                    "quote_health": qh,
                    "source": "negative_proof_position_check",
                    "broker_truth_unavailable": False,
                    "open_order_query_available": True,
                    "position_query_available": True,
                    "replacement_blocked": True,
                },
            )
        return RecoveryAction(
            "MARKED_CLOSED",
            "autonomous_recovery_contract_flat_at_broker",
            pid, local_id, "",
            {
                "contract": contract,
                "quote_health": qh,
                "source": "negative_proof_position_check",
                "terminal_fill_source": (
                    terminal_fill_source if terminal_exact_broker_id else ""
                ),
                "broker_truth_unavailable": False,
                "open_order_query_available": True,
                "position_query_available": True,
            },
        )

    return _mark_replacement_safe(
        exit_engine,
        pid,
        osm=osm,
        reason="autonomous_recovery_no_matching_live_exit_order",
        local_id=local_id,
        broker_id=terminal_exact_broker_id,
        details={
            "contract": contract,
            "quote_health": qh,
            "open_order_query_available": True,
            "position_query_available": True,
            "terminal_filled_qty": (
                terminal_filled_qty if terminal_exact_broker_id else None
            ),
            "terminal_fill_source": (
                terminal_fill_source if terminal_exact_broker_id else ""
            ),
            **terminal_late_fill_details,
        },
        replacement_qty=(
            terminal_replacement_qty
            if terminal_replacement_qty is not None
            else 0
        ),
    )


def recover_exit_engine(
    exit_engine: Any, *, broker: Any, osm: Any = None, max_positions: Optional[int] = None,
    order_monitor: Any = None,
) -> list[RecoveryAction]:
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
    if max_positions is None:
        selected_positions = positions
        deferred_positions = []
    else:
        try:
            recovery_limit = max(0, int(max_positions))
        except (TypeError, ValueError, OverflowError):
            recovery_limit = 0
        selected_positions = positions[:recovery_limit]
        deferred_positions = positions[recovery_limit:]

    for pos in selected_positions:
        lifecycle, lifecycle_reason = _durable_replacement_lifecycle(pos)
        has_durable_replacement = lifecycle is not None
        if lifecycle_reason not in {"absent", "inactive", "ok"} and getattr(
            pos, "exit_retry_liveness", None
        ):
            actions.append(
                RecoveryAction(
                    "NOOP",
                    "replacement_lifecycle_invalid_fail_closed",
                    _position_id(pos),
                    details={"lifecycle_error": lifecycle_reason, "replacement_blocked": True},
                )
            )
            continue
        if not (
            getattr(pos, "exit_identity_quarantine", False)
            or getattr(pos, "last_callback_identity_missing", False)
            or getattr(pos, "exit_in_flight", False)
            or has_durable_replacement
        ):
            continue
        try:
            if has_durable_replacement:
                durable_action = _recover_durable_replacement_pending(
                    pos,
                    broker=broker,
                    exit_engine=exit_engine,
                    osm=osm,
                    order_monitor=order_monitor,
                )
                if durable_action is not None:
                    actions.append(durable_action)
                    continue
            actions.append(
                recover_exit_position(
                    pos,
                    broker=broker,
                    exit_engine=exit_engine,
                    osm=osm,
                    order_monitor=order_monitor,
                )
            )
        except Exception as exc:
            log.exception("autonomous recovery failed for pos=%s: %s", _position_id(pos), exc)
            actions.append(RecoveryAction("ERROR", str(exc), _position_id(pos)))
    for pos in deferred_positions:
        actions.append(
            RecoveryAction(
                "DEFERRED",
                "autonomous_recovery_capacity_deferred",
                _position_id(pos),
                details={
                    "capacity_limit": recovery_limit,
                    "deferred": True,
                    "broker_mutation_blocked": True,
                    "replacement_blocked": True,
                },
            )
        )
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
