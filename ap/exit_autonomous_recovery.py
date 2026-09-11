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

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from ap.broker_submit_identity import canonical_broker_submit_key
from ap.exit_safety import (
    _normalize_contract,
    is_valid_exact_occ_contract,
    resolve_exit_broker_truth,
)
from ap.manual_close_reconciliation import (
    ORDERS_AVAILABLE_COMPLETE,
    ORDERS_AVAILABLE_EMPTY,
    ORDERS_MALFORMED,
    ORDERS_UNAVAILABLE,
    fetch_all_current_session_orders,
    order_contract as _shared_order_contract,
    broker_fill_timestamp_evidence,
    BROKER_FILL_TIMESTAMP_KEYS,
    BROKER_FILL_TIMESTAMP_SOURCE,
    BROKER_ORDER_UPDATED_AT_KEY,
    BROKER_ORDER_UPDATED_AT_SOURCE,
    FILL_TIMESTAMP_QUALITY_EXACT,
    FILL_TIMESTAMP_QUALITY_ORDER_UPDATE_ONLY,
    FILL_TIMESTAMP_QUALITY_UNAVAILABLE,
    order_has_instrument_identity as _shared_order_has_instrument_identity,
    order_has_structured_direction as _shared_order_has_structured_direction,
    order_is_exit_like as _shared_order_is_exit_like,
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
    return _shared_order_contract(raw)


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
    return _shared_order_is_exit_like(raw)


def _has_structured_order_direction(raw: dict) -> bool:
    return _shared_order_has_structured_direction(raw)


def _has_order_instrument_identity(raw: dict) -> bool:
    """Accept valid option or underlying identities in account-wide order pages."""
    return _shared_order_has_instrument_identity(raw)


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
    # Use the same account-scoped, includeTags=true, paginated seam as manual
    # reconciliation.  A one-page list_orders() result is not complete enough
    # to prove that an exact tagged EXIT is absent after a restart.
    try:
        fetch_state, result = fetch_all_current_session_orders(broker)
    except Exception as exc:
        log.warning(
            "broker order pagination failed during autonomous recovery: %s",
            exc,
        )
        return ORDERS_UNAVAILABLE, []
    if fetch_state not in {ORDERS_AVAILABLE_COMPLETE, ORDERS_AVAILABLE_EMPTY}:
        log.warning(
            "broker paginated orders returned %s during autonomous recovery",
            fetch_state,
        )
        return fetch_state, []
    if fetch_state == ORDERS_AVAILABLE_EMPTY:
        return fetch_state, []
    parse_state, rows = _parse_order_payload(result)
    if parse_state != "available":
        log.warning(
            "broker paginated orders returned %s during autonomous recovery",
            parse_state,
        )
        return ORDERS_MALFORMED, []
    return fetch_state, rows


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


def _validated_quantity_aliases(
    raw: dict, keys: tuple[str, ...]
) -> tuple[bool, Optional[int]]:
    """Return one quantity only when all present aliases agree."""
    values: list[int] = []
    for key in keys:
        if key not in raw or raw.get(key) in (None, ""):
            continue
        value = _strict_qty(raw.get(key))
        if value is None:
            return True, None
        values.append(value)
    if len(set(values)) > 1:
        return True, None
    return bool(values), (values[0] if values else None)


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


def _recovery_fill_timestamp_evidence(raw: dict) -> dict:
    """Classify broker fill chronology without promoting transaction_date."""
    evidence = broker_fill_timestamp_evidence(raw)
    return {
        **evidence,
        "filled_ts": (
            evidence.get("filled_ts").astimezone(timezone.utc).isoformat()
            if isinstance(evidence.get("filled_ts"), datetime)
            else evidence.get("filled_ts")
        ),
        "broker_order_updated_at": (
            evidence.get("broker_order_updated_at").astimezone(timezone.utc).isoformat()
            if isinstance(evidence.get("broker_order_updated_at"), datetime)
            else evidence.get("broker_order_updated_at")
        ),
    }


def _recovery_fill_timestamp(raw: dict) -> Optional[str]:
    """Return only an explicit, timezone-aware broker execution timestamp."""
    evidence = _recovery_fill_timestamp_evidence(raw)
    return (
        evidence.get("filled_ts")
        if evidence.get("fill_timestamp_quality") == FILL_TIMESTAMP_QUALITY_EXACT
        else None
    )


def _usable_recovery_fill_evidence(evidence: dict) -> bool:
    quality = str(evidence.get("fill_timestamp_quality") or "").strip()
    source = str(evidence.get("fill_timestamp_source") or "").strip()
    key = str(evidence.get("fill_timestamp_key") or "").strip()

    def _aware(value) -> bool:
        if value in (None, ""):
            return False
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return parsed.tzinfo is not None and parsed.utcoffset() is not None
        except (TypeError, ValueError, OverflowError):
            return False

    if quality == FILL_TIMESTAMP_QUALITY_EXACT:
        return (
            _aware(evidence.get("filled_ts"))
            and source == BROKER_FILL_TIMESTAMP_SOURCE
            and key in BROKER_FILL_TIMESTAMP_KEYS
        )
    if quality == FILL_TIMESTAMP_QUALITY_ORDER_UPDATE_ONLY:
        return (
            evidence.get("filled_ts") in (None, "")
            and _aware(evidence.get("broker_order_updated_at"))
            and source == BROKER_ORDER_UPDATED_AT_SOURCE
            and key == BROKER_ORDER_UPDATED_AT_KEY
        )
    return False


def _recovery_order_state_reason(state: str) -> str:
    """Keep incomplete fill chronology distinct from malformed containers."""
    if state == "fill_timestamp_unproven":
        return "broker_order_fill_timestamp_unproven"
    return f"broker_order_truth_{state}"


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
        order_alias_present, declared_order_qty = _validated_quantity_aliases(
            row, ("qty", "quantity", "order_qty")
        )
        filled_alias_present, declared_filled_qty = _validated_quantity_aliases(
            row,
            (
                "filled_qty",
                "filled_quantity",
                "exec_quantity",
                "exec_qty",
                "quantity_filled",
            ),
        )
        if (
            filled_qty is None
            or filled_qty <= 0
            or fill_price is None
            or (order_alias_present and declared_order_qty is None)
            or (filled_alias_present and declared_filled_qty is None)
            or filled_qty != order_qty
        ):
            return "malformed", None
        row["_recovery_fill_qty"] = filled_qty
        row["_recovery_fill_price"] = fill_price
        timestamp_evidence = _recovery_fill_timestamp_evidence(row)
        row["_recovery_fill_timestamp_quality"] = timestamp_evidence.get(
            "fill_timestamp_quality", FILL_TIMESTAMP_QUALITY_UNAVAILABLE
        )
        row["_recovery_fill_timestamp_source"] = timestamp_evidence.get(
            "fill_timestamp_source", ""
        )
        row["_recovery_fill_timestamp_key"] = timestamp_evidence.get(
            "fill_timestamp_key", ""
        )
        row["_recovery_broker_order_updated_at"] = timestamp_evidence.get(
            "broker_order_updated_at"
        )
        row["_recovery_filled_ts"] = timestamp_evidence.get("filled_ts")
    return "available", row


def _matching_open_exit_orders_with_truth(
    broker: Any,
    contract: str,
    *,
    exclude_broker_id: str = "",
    include_filled: bool = False,
) -> tuple[str, list[tuple[str, dict]]]:
    if not is_valid_exact_occ_contract(contract):
        return "identity_unproven", []
    state, rows = _list_open_orders_truth(broker)
    if state not in {ORDERS_AVAILABLE_COMPLETE, ORDERS_AVAILABLE_EMPTY}:
        return state, []
    if state == ORDERS_AVAILABLE_EMPTY:
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
        if status not in OPEN_BROKER_STATUSES and not (include_filled and status == "filled"):
            continue
        broker_id = _broker_order_id(row)
        if not broker_id:
            return "identity_unproven", []
        if exclude_broker_id and broker_id == exclude_broker_id:
            continue
        if status == "filled":
            filled_qty = _filled_order_quantity(row)
            fill_price = _extract_fill_price(row)
            order_alias_present, declared_order_qty = _validated_quantity_aliases(
                row, ("qty", "quantity", "order_qty")
            )
            filled_alias_present, declared_filled_qty = _validated_quantity_aliases(
                row,
                (
                    "filled_qty",
                    "filled_quantity",
                    "exec_quantity",
                    "exec_qty",
                    "quantity_filled",
                ),
            )
            if (
                filled_qty is None
                or filled_qty <= 0
                or fill_price is None
                or (order_alias_present and declared_order_qty is None)
                or (filled_alias_present and declared_filled_qty is None)
                or filled_qty != _qty(row)
            ):
                return "malformed", []
            fill_timestamp_evidence = _recovery_fill_timestamp_evidence(row)
            if not _usable_recovery_fill_evidence(fill_timestamp_evidence):
                # A filled account-wide match without a usable chronology
                # marker is not negative proof.  Stop the recovery decision
                # rather than allowing replacement authorization to proceed
                # on an incomplete row.
                return "fill_timestamp_unproven", []
            row["_recovery_fill_qty"] = filled_qty
            row["_recovery_fill_price"] = fill_price
            row["_recovery_fill_timestamp_quality"] = fill_timestamp_evidence.get(
                "fill_timestamp_quality"
            )
            row["_recovery_fill_timestamp_source"] = fill_timestamp_evidence.get(
                "fill_timestamp_source"
            )
            row["_recovery_fill_timestamp_key"] = fill_timestamp_evidence.get(
                "fill_timestamp_key"
            )
            row["_recovery_broker_order_updated_at"] = fill_timestamp_evidence.get(
                "broker_order_updated_at"
            )
            row["_recovery_filled_ts"] = fill_timestamp_evidence.get("filled_ts")
        matches.append((broker_id, row))
    return state, matches


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


def _exit_fill_order_keys(pos: Any, *, local_id: str, broker_id: str) -> tuple[str, ...]:
    """Return the durable identities that can own this fill watermark."""
    values = (
        broker_id,
        local_id,
        _norm(getattr(pos, "pending_exit_broker_order_id", "")),
        _norm(getattr(pos, "pending_exit_local_order_id", "")),
    )
    return tuple(dict.fromkeys(value for value in values if value))


def _previous_exit_cumulative_fill(
    pos: Any,
    *,
    local_id: str,
    broker_id: str,
) -> Optional[int]:
    """Read the current order's applied cumulative-fill watermark."""
    values: list[int] = []
    pending_value = getattr(pos, "pending_exit_filled_qty", None)
    if pending_value not in (None, ""):
        parsed = _strict_qty(pending_value)
        if parsed is None:
            return None
        values.append(parsed)

    watermarks = getattr(pos, "last_applied_exit_cum_fill_by_order", None)
    if watermarks not in (None, "") and not isinstance(watermarks, dict):
        return None
    for key in _exit_fill_order_keys(pos, local_id=local_id, broker_id=broker_id):
        if not isinstance(watermarks, dict) or key not in watermarks:
            continue
        raw_value = watermarks.get(key)
        if raw_value in (None, ""):
            continue
        parsed = _strict_qty(raw_value)
        if parsed is None:
            return None
        values.append(parsed)
    return max(values) if values else 0


def _record_exit_fill_watermark(
    pos: Any,
    *,
    local_id: str,
    broker_id: str,
    cumulative_filled: int,
) -> None:
    """Persist an in-memory watermark after a successful fill handoff."""
    keys = _exit_fill_order_keys(pos, local_id=local_id, broker_id=broker_id)
    watermarks = getattr(pos, "last_applied_exit_cum_fill_by_order", None)
    if not isinstance(watermarks, dict):
        watermarks = {}
        setattr(pos, "last_applied_exit_cum_fill_by_order", watermarks)
    for key in keys:
        prior = _strict_qty(watermarks.get(key))
        watermarks[key] = max(prior or 0, cumulative_filled)
    prior_scalar = _strict_qty(getattr(pos, "last_applied_exit_cum_fill", 0)) or 0
    setattr(pos, "last_applied_exit_cum_fill", max(prior_scalar, cumulative_filled))
    if local_id:
        setattr(pos, "last_applied_exit_local_order_id", local_id)
    if broker_id:
        setattr(pos, "last_applied_exit_broker_order_id", broker_id)


def _seed_exit_fill_watermark(
    pos: Any,
    *,
    local_id: str,
    broker_id: str,
    cumulative_filled: int,
) -> None:
    """Align the legacy hook with the cumulative value already applied."""
    watermarks = getattr(pos, "last_applied_exit_cum_fill_by_order", None)
    if not isinstance(watermarks, dict):
        watermarks = {}
        setattr(pos, "last_applied_exit_cum_fill_by_order", watermarks)
    for key in _exit_fill_order_keys(pos, local_id=local_id, broker_id=broker_id):
        prior = _strict_qty(watermarks.get(key))
        if prior is None or prior < cumulative_filled:
            watermarks[key] = cumulative_filled


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


def _order_records(raw: dict) -> list[dict]:
    records = [raw]
    nested = raw.get("raw") if isinstance(raw, dict) else None
    if isinstance(nested, dict):
        records.append(nested)
    return records


def _broker_account_id(broker: Any) -> str:
    """Resolve the account whose account-scoped order list was queried."""
    return _norm(
        getattr(broker, "account_id", None)
        or getattr(getattr(broker, "cfg", None), "account_id", None)
        or getattr(broker, "_account_id", None)
    ).lower()


def _order_account_identity_mismatch(raw: dict, broker: Any) -> str:
    """Reject an order row that explicitly names a different account."""
    expected = _broker_account_id(broker)
    values = [
        _norm(record.get(key)).lower()
        for record in _order_records(raw)
        for key in ("account_id", "account", "account_number")
        if _norm(record.get(key))
    ]
    if values and not expected:
        return "account_unproven"
    if expected and any(value != expected for value in values):
        return "account"
    return ""


def _order_matches_local_exit_tag(raw: dict, local_order_id: str) -> bool:
    """Match only the exact durable tag sent by the canonical submit path."""
    expected = canonical_broker_submit_key(local_order_id)
    if not expected:
        return False
    tags = [
        _norm(record.get(key))
        for record in _order_records(raw)
        for key in ("tag", "order_tag", "client_tag")
        if _norm(record.get(key))
    ]
    return bool(tags) and all(tag == expected for tag in tags)


def _recovery_order_state_machine(osm: Any, exit_engine: Any) -> Any:
    """Resolve the canonical OSM used by recovery, including the engine wiring."""
    return (
        osm
        or getattr(exit_engine, "order_state_machine", None)
        or getattr(exit_engine, "osm", None)
    )


def _durable_meta(row: dict) -> dict:
    raw_meta = row.get("meta")
    if isinstance(raw_meta, dict):
        return dict(raw_meta)
    if isinstance(raw_meta, str) and raw_meta.strip():
        try:
            decoded = json.loads(raw_meta)
        except Exception:
            return {}
        return dict(decoded) if isinstance(decoded, dict) else {}
    return {}


def _meta_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _read_durable_recovery_order(osm: Any, local_order_id: str) -> tuple[str, Optional[dict]]:
    """Read one durable order row without treating a missing read as proof."""
    getter = getattr(osm, "get_order", None) if osm is not None else None
    if not callable(getter):
        getter = getattr(osm, "_get_order", None) if osm is not None else None
    if not callable(getter):
        return "unavailable", None
    try:
        row = getter(local_order_id)
    except Exception as exc:
        log.warning(
            "durable EXIT recovery authority read failed local=%s: %s",
            local_order_id,
            exc,
        )
        return "unavailable", None
    if row is None:
        return "missing", None
    if isinstance(row, dict):
        return "available", dict(row)
    try:
        return "available", dict(row)
    except Exception:
        return "malformed", None


def _durable_exit_row_proof(
    osm: Any,
    pos: Any,
    *,
    local_order_id: str,
    contract: str,
    expected_statuses: set[str],
    expected_broker_order_id: str = "",
) -> tuple[bool, Optional[dict], str, dict]:
    """Prove the exact durable OSM identity before/after broker adoption.

    The missing-broker-id path may query Tradier only after this proof.  Keep
    every predicate explicit so a partial row cannot become broker ownership
    authority through a convenient in-memory position object.
    """
    state, row = _read_durable_recovery_order(osm, local_order_id)
    details: dict[str, Any] = {
        "authority_source": "durable_osm",
        "proof_state": state,
        "local_order_id": local_order_id,
    }
    if state != "available" or row is None:
        return (
            False,
            row,
            "broker_submit_authority_unavailable"
            if state == "unavailable"
            else "broker_submit_authority_missing"
            if state == "missing"
            else "broker_submit_authority_unproven",
            details,
        )

    expected_position_id = _position_id(pos)
    expected_client_id = _norm(getattr(pos, "client_id", ""))
    expected_execution_mode = _norm(getattr(pos, "execution_mode", "")).lower()
    expected_qty = _strict_qty(getattr(pos, "pending_exit_qty", None))
    expected_tag = canonical_broker_submit_key(local_order_id)
    meta = _durable_meta(row)
    mismatches: list[str] = []

    if row.get("local_order_id") != local_order_id:
        mismatches.append("local_order_id")
    if row.get("client_id") != expected_client_id:
        mismatches.append("client_id")
    if row.get("kind") != "EXIT":
        mismatches.append("kind")
    if row.get("status") not in expected_statuses:
        mismatches.append("status")
    if row.get("execution_mode") != expected_execution_mode:
        mismatches.append("execution_mode")
    if str(row.get("position_id") or "") != expected_position_id:
        mismatches.append("position_id")
    durable_contract = row.get("contract")
    if (
        not is_valid_exact_occ_contract(durable_contract)
        or _norm_contract(durable_contract) != contract
    ):
        mismatches.append("contract")
    durable_qty = _strict_qty(row.get("qty"))
    if expected_qty is None or expected_qty <= 0 or durable_qty != expected_qty:
        mismatches.append("qty")

    durable_broker_id = str(row.get("broker_order_id") or "").strip()
    if expected_broker_order_id:
        if durable_broker_id != expected_broker_order_id:
            mismatches.append("broker_order_id")
    elif durable_broker_id:
        mismatches.append("broker_order_id")

    if not expected_broker_order_id and row.get("submitted_ts"):
        mismatches.append("submitted_ts")

    submit_intent = _norm(meta.get("submit_intent_at"))
    payload_hash = _norm(meta.get("broker_submit_payload_hash"))
    if not submit_intent:
        mismatches.append("submit_intent_at")
    if not payload_hash:
        mismatches.append("broker_submit_payload_hash")
    if not expected_tag or meta.get("broker_submit_key") != expected_tag:
        mismatches.append("broker_submit_key")
    if meta.get("current_owner") != f"broker_submit:{expected_tag}":
        mismatches.append("current_owner")
    for flag in ("split_brain_quarantine", "reconciliation_required"):
        if _meta_flag(meta.get(flag)) or _meta_flag(row.get(flag)):
            mismatches.append(flag)

    details.update(
        {
            "expected_statuses": sorted(expected_statuses),
            "expected_qty": expected_qty,
            "expected_broker_order_id": expected_broker_order_id,
            "broker_submit_key": expected_tag,
            "failed_fields": mismatches,
        }
    )
    if mismatches:
        return False, row, "broker_submit_authority_unproven", details
    return True, row, "", details


def _adopt_tagged_exit_order(
    osm: Any,
    pos: Any,
    *,
    local_order_id: str,
    broker_order_id: str,
    contract: str,
    source: str,
    fill_timestamp_evidence: Optional[dict] = None,
) -> tuple[bool, Optional[dict], dict, str]:
    """Adopt a tagged broker order through OSM, then prove the reread."""
    adopt = getattr(osm, "adopt_broker_owned_exit_request", None)
    if not callable(adopt):
        return False, None, {}, "broker_submit_adoption_unavailable"
    expected_qty = _strict_qty(getattr(pos, "pending_exit_qty", None))
    if expected_qty is None or expected_qty <= 0:
        return False, None, {}, "broker_order_quantity_unproven"
    try:
        adoption_kwargs = {
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "execution_mode": _norm(getattr(pos, "execution_mode", "")).lower(),
            "client_id": _norm(getattr(pos, "client_id", "")),
            "position_id": _position_id(pos),
            "expected_qty": expected_qty,
            "source": source,
        }
        if fill_timestamp_evidence:
            adoption_kwargs.update(
                {
                    "fill_timestamp_quality": fill_timestamp_evidence.get(
                        "fill_timestamp_quality"
                    ),
                    "fill_timestamp_source": fill_timestamp_evidence.get(
                        "fill_timestamp_source"
                    ),
                    "fill_timestamp_key": fill_timestamp_evidence.get(
                        "fill_timestamp_key"
                    ),
                    "filled_ts": fill_timestamp_evidence.get("filled_ts"),
                    "broker_order_updated_at": fill_timestamp_evidence.get(
                        "broker_order_updated_at"
                    ),
                }
            )
        result = adopt(
            **adoption_kwargs,
        )
    except Exception as exc:
        log.warning(
            "durable EXIT adoption failed local=%s broker=%s: %s",
            local_order_id,
            broker_order_id,
            exc,
        )
        return False, None, {}, "broker_submit_adoption_failed"

    if not isinstance(result, dict):
        return False, None, {"raw_result_type": type(result).__name__}, "broker_submit_adoption_unproven"
    result = dict(result)
    disposition = _norm(result.get("disposition")).upper()
    accepted = disposition in {"ADOPTED", "ALREADY_BROKER_OWNED_ACTIVE"}
    result["adoption_accepted"] = accepted
    if not accepted:
        return False, None, result, "broker_submit_adoption_not_accepted"

    proven, refreshed, _proof_reason, proof_details = _durable_exit_row_proof(
        osm,
        pos,
        local_order_id=local_order_id,
        contract=contract,
        expected_statuses={"EXIT_SUBMITTED", "EXIT_ACKNOWLEDGED", "EXIT_PARTIAL_FILL"},
        expected_broker_order_id=broker_order_id,
    )
    result["durable_reread_proof"] = proof_details
    if not proven:
        return False, refreshed, result, "broker_submit_adoption_reread_unproven"
    return True, refreshed, result, ""


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

    recovery_osm = _recovery_order_state_machine(osm, exit_engine)

    # Exact broker identity path.
    if pending_broker_id:
        order_state, raw = _get_order_truth(broker, pending_broker_id)
        if order_state != "available" or raw is None:
            return _hold(
                _recovery_order_state_reason(order_state),
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
            fill_timestamp_evidence = _recovery_fill_timestamp_evidence(raw)
            filled_ts = fill_timestamp_evidence.get("filled_ts")
            remaining = _position_remaining(pos)
            previous_filled = _previous_exit_cumulative_fill(
                pos,
                local_id=local_id,
                broker_id=pending_broker_id,
            )
            details = {
                "status": status,
                "filled_qty": filled_qty,
                "filled_ts": filled_ts,
                "fill_timestamp_quality": fill_timestamp_evidence.get(
                    "fill_timestamp_quality"
                ),
                "fill_timestamp_source": fill_timestamp_evidence.get(
                    "fill_timestamp_source"
                ),
                "fill_timestamp_key": fill_timestamp_evidence.get(
                    "fill_timestamp_key"
                ),
                "broker_order_updated_at": fill_timestamp_evidence.get(
                    "broker_order_updated_at"
                ),
                "previous_cumulative_filled": previous_filled,
                "local_remaining": remaining,
                "contract": contract,
                "quote_health": qh,
            }
            if not _usable_recovery_fill_evidence(fill_timestamp_evidence):
                return _hold(
                    "broker_order_fill_timestamp_unproven",
                    pid,
                    local_id,
                    pending_broker_id,
                    {**details, "fill_price": fill_price},
                )
            if (
                not isinstance(filled_qty, int)
                or isinstance(filled_qty, bool)
                or filled_qty <= 0
                or fill_price is None
                or remaining is None
                or remaining <= 0
                or previous_filled is None
            ):
                return _hold(
                    "broker_order_fill_quantity_unproven",
                    pid,
                    local_id,
                    pending_broker_id,
                    details,
                )
            if filled_qty < previous_filled:
                details["reason"] = "cumulative_regression"
                return _hold(
                    "broker_order_fill_cumulative_regression",
                    pid,
                    local_id,
                    pending_broker_id,
                    details,
                )

            # Broker fill quantities are cumulative.  A restart can leave the
            # local position with only the unfilled remainder, so never compare
            # the cumulative broker value directly with that remainder.
            delta_qty = filled_qty - previous_filled
            details["delta_qty"] = delta_qty
            if delta_qty <= 0:
                return _hold("duplicate_exit_fill_ignored", pid, local_id, pending_broker_id, details)

            recovery_osm = osm or getattr(exit_engine, "order_state_machine", None)
            transition = getattr(recovery_osm, "transition", None)
            if callable(transition):
                if not local_id:
                    return _hold(
                        "canonical_exit_fill_identity_missing",
                        pid,
                        local_id,
                        pending_broker_id,
                        details,
                    )
                try:
                    transitioned = transition(
                        local_id,
                        "EXIT_FILLED",
                        broker_order_id=pending_broker_id,
                        filled_qty=filled_qty,
                        fill_price=fill_price,
                        filled_ts=filled_ts,
                        fill_timestamp_quality=fill_timestamp_evidence.get(
                            "fill_timestamp_quality"
                        ),
                        fill_timestamp_source=fill_timestamp_evidence.get(
                            "fill_timestamp_source"
                        ),
                        fill_timestamp_key=fill_timestamp_evidence.get(
                            "fill_timestamp_key"
                        ),
                        broker_order_updated_at=fill_timestamp_evidence.get(
                            "broker_order_updated_at"
                        ),
                    )
                except Exception as exc:
                    log.warning(
                        "canonical exit fill transition failed during recovery: %s",
                        exc,
                    )
                    return _hold(
                        "canonical_exit_fill_transition_failed",
                        pid,
                        local_id,
                        pending_broker_id,
                        details,
                    )
                if transitioned is not True:
                    return _hold("canonical_exit_fill_transition_failed", pid, local_id, pending_broker_id, details)
                _record_exit_fill_watermark(
                    pos,
                    local_id=local_id,
                    broker_id=pending_broker_id,
                    cumulative_filled=filled_qty,
                )
                action = "MARKED_CLOSED" if delta_qty >= remaining else "PARTIAL_FILL_APPLIED"
                return RecoveryAction(
                    action,
                    "broker_order_filled_via_canonical_osm",
                    pid,
                    local_id,
                    pending_broker_id,
                    {**details, "fill_price": fill_price, "filled_ts": filled_ts},
                )

            if delta_qty > remaining:
                return _hold("broker_order_fill_quantity_unproven", pid, local_id, pending_broker_id, details)
            if delta_qty == remaining:
                if not (exit_engine and hasattr(exit_engine, "mark_position_closed")):
                    return _hold("broker_order_fill_close_hook_missing", pid, local_id, pending_broker_id, details)
                try:
                    exit_engine.mark_position_closed(
                        pid,
                        reason="AUTONOMOUS_RECOVERY_BROKER_FILLED",
                        qty_filled=delta_qty,
                        fill_price=fill_price,
                        local_order_id=local_id,
                        broker_order_id=pending_broker_id,
                        cumulative_filled=filled_qty,
                        filled_ts=filled_ts,
                        fill_timestamp_quality=fill_timestamp_evidence.get(
                            "fill_timestamp_quality"
                        ),
                        fill_timestamp_source=fill_timestamp_evidence.get(
                            "fill_timestamp_source"
                        ),
                        fill_timestamp_key=fill_timestamp_evidence.get(
                            "fill_timestamp_key"
                        ),
                        broker_order_updated_at=fill_timestamp_evidence.get(
                            "broker_order_updated_at"
                        ),
                        reconciled=True,
                    )
                except Exception as exc:
                    log.warning("autonomous recovery close hook failed: %s", exc)
                    return _hold("broker_order_fill_close_hook_failed", pid, local_id, pending_broker_id, details)
                _record_exit_fill_watermark(
                    pos,
                    local_id=local_id,
                    broker_id=pending_broker_id,
                    cumulative_filled=filled_qty,
                )
                return RecoveryAction(
                    "MARKED_CLOSED", "broker_order_filled", pid, local_id, pending_broker_id,
                    {**details, "fill_price": fill_price, "filled_ts": filled_ts},
                )

            if not (exit_engine and hasattr(exit_engine, "note_partial_exit_fill")):
                return _hold("broker_order_partial_fill_hook_missing", pid, local_id, pending_broker_id, details)
            _seed_exit_fill_watermark(
                pos,
                local_id=local_id,
                broker_id=pending_broker_id,
                cumulative_filled=previous_filled,
            )
            try:
                exit_engine.note_partial_exit_fill(
                    pid,
                    qty_filled=delta_qty,
                    fill_price=fill_price,
                    local_order_id=local_id,
                    broker_order_id=pending_broker_id,
                    cumulative_filled=filled_qty,
                    filled_ts=filled_ts,
                    fill_timestamp_quality=fill_timestamp_evidence.get(
                        "fill_timestamp_quality"
                    ),
                    fill_timestamp_source=fill_timestamp_evidence.get(
                        "fill_timestamp_source"
                    ),
                    fill_timestamp_key=fill_timestamp_evidence.get(
                        "fill_timestamp_key"
                    ),
                    broker_order_updated_at=fill_timestamp_evidence.get(
                        "broker_order_updated_at"
                    ),
                )
            except Exception as exc:
                log.warning("autonomous recovery partial-fill hook failed: %s", exc)
                return _hold("broker_order_partial_fill_hook_failed", pid, local_id, pending_broker_id, details)
            _record_exit_fill_watermark(
                pos,
                local_id=local_id,
                broker_id=pending_broker_id,
                cumulative_filled=filled_qty,
            )
            return RecoveryAction(
                "PARTIAL_FILL_APPLIED", "broker_order_partial_fill", pid, local_id, pending_broker_id,
                {**details, "fill_price": fill_price, "filled_ts": filled_ts},
            )

        if status in TERMINAL_BROKER_STATUSES:
            # A terminal old id is not negative proof.  We only adopt a
            # different exact open order; replacement authorization remains a
            # separate follow-up with complete pagination/ownership proof.
            order_state, other_matches = _matching_open_exit_orders_with_truth(
                broker, contract, exclude_broker_id=pending_broker_id
            )
            if order_state not in {ORDERS_AVAILABLE_COMPLETE, ORDERS_AVAILABLE_EMPTY}:
                return _hold(
                    _recovery_order_state_reason(order_state),
                    pid,
                    local_id,
                    pending_broker_id,
                    {"old_status": status, "contract": contract, "quote_health": qh},
                )
            tagged_other_matches: list[tuple[str, dict]] = []
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
                account_mismatch = _order_account_identity_mismatch(other_raw, broker)
                if account_mismatch:
                    return _hold(
                        f"broker_order_{account_mismatch}",
                        pid,
                        local_id,
                        pending_broker_id,
                        {"broker_order_id": other_id, "identity_field": "account", "quote_health": qh},
                    )
                if _order_matches_local_exit_tag(other_raw, local_id):
                    if not _broker_account_id(broker):
                        return _hold(
                            "broker_order_account_identity_unproven",
                            pid,
                            local_id,
                            pending_broker_id,
                            {"broker_order_id": other_id, "quote_health": qh},
                        )
                    tagged_other_matches.append((other_id, other_raw))
                else:
                    return _hold(
                        "broker_order_owner_unproven",
                        pid,
                        local_id,
                        pending_broker_id,
                        {"broker_order_id": other_id, "quote_health": qh},
                    )
            if len(tagged_other_matches) == 1:
                other_id, other_raw = tagged_other_matches[0]
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
            if len(tagged_other_matches) > 1:
                return _hold(
                    "multiple_different_open_exits_block_replacement",
                    pid,
                    local_id,
                    pending_broker_id,
                    {"old_status": status, "matches": [item[0] for item in tagged_other_matches], "quote_health": qh},
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

    # Missing broker id: only an account-scoped exact-OCC row carrying the
    # durable canonical submit tag can be adopted.  Tradier rows do not carry
    # Angel Precision's client/mode fields; those synthetic fields are not
    # ownership proof.  This path still never cancels or authorizes a
    # replacement.
    authority_proven, _durable_order, authority_reason, authority_details = (
        _durable_exit_row_proof(
            recovery_osm,
            pos,
            local_order_id=local_id,
            contract=contract,
            expected_statuses={"EXIT_REQUESTED"},
        )
    )
    if not authority_proven:
        return _hold(
            authority_reason,
            pid,
            local_id,
            "",
            {"contract": contract, "quote_health": qh, **authority_details},
        )
    order_state, matches = _matching_open_exit_orders_with_truth(
        broker,
        contract,
        include_filled=True,
    )
    if order_state not in {ORDERS_AVAILABLE_COMPLETE, ORDERS_AVAILABLE_EMPTY}:
        return _hold(
            _recovery_order_state_reason(order_state),
            pid,
            local_id,
            "",
            {"contract": contract, "quote_health": qh},
        )
    tagged_matches: list[tuple[str, dict]] = []
    untagged_matches: list[tuple[str, dict]] = []
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
        account_mismatch = _order_account_identity_mismatch(raw, broker)
        if account_mismatch:
            return _hold(
                f"broker_order_{account_mismatch}",
                pid,
                local_id,
                broker_id,
                {"contract": contract, "identity_field": "account", "quote_health": qh},
            )
        if _order_matches_local_exit_tag(raw, local_id):
            if not _broker_account_id(broker):
                return _hold(
                    "broker_order_account_identity_unproven",
                    pid,
                    local_id,
                    broker_id,
                    {"contract": contract, "quote_health": qh},
                )
            tagged_matches.append((broker_id, raw))
        else:
            untagged_matches.append((broker_id, raw))
    if untagged_matches:
        return _hold(
            "broker_order_owner_unproven",
            pid,
            local_id,
            "",
            {"match_count": len(matches), "contract": contract, "quote_health": qh},
        )
    if len(tagged_matches) == 1:
        recovered_id, raw = tagged_matches[0]
        pending_qty = _pending_order_quantity(pos, raw)
        if pending_qty is None:
            return _hold("broker_order_quantity_unproven", pid, local_id, recovered_id, {"quote_health": qh})
        if pending_qty != authority_details.get("expected_qty"):
            return _hold(
                "broker_order_quantity_identity_mismatch",
                pid,
                local_id,
                recovered_id,
                {
                    "broker_order_qty": pending_qty,
                    "durable_requested_qty": authority_details.get("expected_qty"),
                    "quote_health": qh,
                },
            )
        fill_timestamp_evidence = None
        if _status(raw) == "filled":
            filled_qty = raw.get("_recovery_fill_qty")
            fill_price = raw.get("_recovery_fill_price")
            fill_timestamp_evidence = _recovery_fill_timestamp_evidence(raw)
            filled_ts = fill_timestamp_evidence.get("filled_ts")
            fill_details = {
                "status": "filled",
                "filled_qty": filled_qty,
                "fill_price": fill_price,
                "filled_ts": filled_ts,
                "fill_timestamp_quality": fill_timestamp_evidence.get(
                    "fill_timestamp_quality"
                ),
                "fill_timestamp_source": fill_timestamp_evidence.get(
                    "fill_timestamp_source"
                ),
                "fill_timestamp_key": fill_timestamp_evidence.get(
                    "fill_timestamp_key"
                ),
                "broker_order_updated_at": fill_timestamp_evidence.get(
                    "broker_order_updated_at"
                ),
                "contract": contract,
                "quote_health": qh,
            }
            if not _usable_recovery_fill_evidence(fill_timestamp_evidence):
                return _hold(
                    "broker_order_fill_timestamp_unproven",
                    pid,
                    local_id,
                    recovered_id,
                    fill_details,
                )
            transition = getattr(recovery_osm, "transition", None)
            if not callable(transition):
                return _hold(
                    "canonical_exit_fill_transition_unavailable",
                    pid,
                    local_id,
                    recovered_id,
                    fill_details,
                )
        adopted, _refreshed, adoption_result, adoption_reason = _adopt_tagged_exit_order(
            recovery_osm,
            pos,
            local_order_id=local_id,
            broker_order_id=recovered_id,
            contract=contract,
            source="autonomous_recovery",
            fill_timestamp_evidence=fill_timestamp_evidence,
        )
        if not adopted:
            return _hold(
                adoption_reason,
                pid,
                local_id,
                recovered_id,
                {
                    "contract": contract,
                    "quote_health": qh,
                    "adoption": adoption_result,
                },
            )
        if _status(raw) == "filled":
            try:
                transitioned = transition(
                    local_id,
                    "EXIT_FILLED",
                    broker_order_id=recovered_id,
                    filled_qty=filled_qty,
                    fill_price=fill_price,
                    filled_ts=filled_ts,
                    fill_timestamp_quality=fill_timestamp_evidence.get(
                        "fill_timestamp_quality"
                    ),
                    fill_timestamp_source=fill_timestamp_evidence.get(
                        "fill_timestamp_source"
                    ),
                    fill_timestamp_key=fill_timestamp_evidence.get(
                        "fill_timestamp_key"
                    ),
                    broker_order_updated_at=fill_timestamp_evidence.get(
                        "broker_order_updated_at"
                    ),
                )
            except Exception as exc:
                log.warning(
                    "canonical adopted exit fill transition failed during recovery: %s",
                    exc,
                )
                return _hold(
                    "canonical_exit_fill_transition_failed",
                    pid,
                    local_id,
                    recovered_id,
                    fill_details,
                )
            if transitioned is not True:
                return _hold(
                    "canonical_exit_fill_transition_failed",
                    pid,
                    local_id,
                    recovered_id,
                    fill_details,
                )
            _record_exit_fill_watermark(
                pos,
                local_id=local_id,
                broker_id=recovered_id,
                cumulative_filled=filled_qty,
            )
            return RecoveryAction(
                "MARKED_CLOSED",
                "broker_order_filled_via_canonical_osm",
                pid,
                local_id,
                recovered_id,
                {
                    **fill_details,
                    "ownership_evidence": "account_scoped_order_list+exact_occ+canonical_tag",
                    "adoption_disposition": adoption_result.get("disposition"),
                    "durable_reread_proof": True,
                },
            )
        # OSM is the durable authority.  This call only hydrates the runtime
        # mirror after accepted/idempotent adoption and a fresh durable reread.
        if exit_engine and hasattr(exit_engine, "set_pending_exit_order"):
            exit_engine.set_pending_exit_order(
                pid,
                local_order_id=local_id,
                broker_order_id=recovered_id,
                qty=pending_qty,
                reason="autonomous_recovery_matched_live_exit_order",
            )
        return RecoveryAction(
            "RECOVERED_BROKER_ID",
            "matched_single_live_exit_order",
            pid,
            local_id,
            recovered_id,
            {
                "contract": contract,
                "ownership_evidence": "account_scoped_order_list+exact_occ+canonical_tag",
                "adoption_disposition": adoption_result.get("disposition"),
                "durable_reread_proof": True,
                "quote_health": qh,
            },
        )
    if len(tagged_matches) > 1:
        return _hold(
            "multiple_tagged_live_exit_orders",
            pid,
            local_id,
            "",
            {"match_count": len(tagged_matches), "quote_health": qh},
        )
    if matches:
        # Same-contract rows without the exact durable tag remain ambiguous;
        # do not let a unique-looking Tradier row gain new authority.
        return _hold(
            "broker_order_owner_unproven",
            pid,
            local_id,
            "",
            {"match_count": len(matches), "contract": contract, "quote_health": qh},
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
