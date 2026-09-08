"""Broker-truth reconciliation for externally closed client positions.

Delegated to from ``ClientRunner._detect_manual_closes``. Never submits or
cancels broker orders. Adopts already-filled external Tradier EXIT orders
into the durable order ledger as EXIT_FILLED rows, then finalizes the
position via ``APPositionManager.close_position_from_exit_fill`` with
weighted-aggregate broker truth.

Post-review amendments (PR #386 hardening):

  1. Multi-fill adoption is now atomic per position — every fill for a
     given position is INSERTed inside ONE connection + advisory lock. If
     any fill fails, the transaction rolls back and NO evidence is left
     partially adopted. That eliminates the previous stranding path where
     fill A committed, fill B failed, and the next scan short-circuited on
     A appearing in known_exit_order_ids.

  2. The scanner distinguishes bot-submitted EXIT ids (a legitimate
     ownership fence) from externally-adopted EXIT ids (our own past work
     that must not fence us out of resuming/finalizing the SAME position).

  3. When previously-adopted external EXIT rows exist for a still-active
     position, the scanner reconstitutes the weighted aggregate directly
     from those durable rows and re-invokes the finalizer — closes the
     "adopted but finalizer failed" recovery gap without relying on the
     row-at-a-time reconciler (which overwrites position-level P&L).

  4. Position scan expanded from status='OPEN' only to the canonical
     active family {OPEN, CLOSING}. A CLOSING position with retained
     external evidence is no longer stranded.

  5. Broker orders fetched via paginated /v1/accounts/{id}/orders with an
     explicit high limit. Falls back to broker.list_orders() with a
     WARNING when pagination is unavailable, since that path is capped at
     ~25 by Tradier defaults.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("client_runner.manual_close")

MANUAL_CLOSE_INTERVAL_SEC = float(
    os.getenv("MANUAL_CLOSE_DETECT_INTERVAL_SEC", "120")
)
MANUAL_CLOSE_FUTURE_SKEW_SEC = float(
    os.getenv("MANUAL_CLOSE_ORDER_FUTURE_SKEW_SEC", "300")
)
MANUAL_CLOSE_ORDERS_PAGE_LIMIT = int(
    os.getenv("MANUAL_CLOSE_ORDERS_PAGE_LIMIT", "500")
)
# Order-list truth is safety-sensitive: an empty page is only negative proof
# when the account-scoped pagination completed.  Keep the transport/envelope
# state separate from the rows so callers cannot mistake a failed or truncated
# read for "no orders".
ORDERS_AVAILABLE_COMPLETE = "available_complete"
ORDERS_AVAILABLE_EMPTY = "available_empty"
ORDERS_MALFORMED = "malformed"
ORDERS_UNAVAILABLE = "unavailable"
ORDERS_INCOMPLETE = "incomplete"
_ORDER_PAGE_DATA = "data"
_ORDER_PAGE_EMPTY = "empty"
POSITIONS_AVAILABLE_COMPLETE_NONEMPTY = "available_complete_nonempty"
POSITIONS_AVAILABLE_COMPLETE_EMPTY = "available_complete_empty"
POSITIONS_UNAVAILABLE = "unavailable"
POSITIONS_MALFORMED = "malformed"
POSITIONS_INCOMPLETE = "incomplete"
POSITIONS_AMBIGUOUS = "ambiguous"
POSITION_COMPLETE_STATES = frozenset(
    {
        POSITIONS_AVAILABLE_COMPLETE_NONEMPTY,
        POSITIONS_AVAILABLE_COMPLETE_EMPTY,
    }
)
_POSITION_ERROR_STATUSES = frozenset(
    {"error", "failed", "failure", "unavailable"}
)
VALID_EXECUTION_MODES = frozenset({"paper", "live"})
MANUAL_CLOSE_SIDES = frozenset(
    {"sell_to_close", "sell-to-close", "selltoclose", "stc"}
)
FILLED_ORDER_STATUSES = frozenset(
    {"filled", "partially_filled", "partial_fill", "partially-filled"}
)
DURABLE_EXIT_FILLED_STATUSES = frozenset({"EXIT_FILLED", "EXIT_PARTIAL_FILL"})
ACTIVE_POSITION_STATUSES = ("OPEN", "CLOSING", "PARTIAL", "ACTIVE")
EXTERNAL_LOCAL_ID_PREFIX = "external-exit:"
BROKER_FILL_TIMESTAMP_SOURCE = "broker_response"
BROKER_FILL_TIMESTAMP_KEYS = (
    "last_fill_date",
    "filled_at",
    "filled_ts",
    "fill_ts",
    "transaction_date",
)


def _terminal_position_statuses() -> list[str]:
    """Canonical TERMINAL position statuses for PASS 0 candidate selection.

    Sourced from ap.position_manager.PositionStatus.TERMINAL so PASS 0
    matches whatever position_manager considers terminal — CLOSED,
    CLOSED_REPAIR, EXPIRED, STOPPED, TAKEN_PROFIT, ERROR, CANCELED,
    CANCELLED. PR #386 blocker 1 fix: previous revision referenced an
    undefined TERMINAL_POSITION_STATUSES name, which silently ran PASS 0
    with zero candidates forever.
    """
    from ap.position_manager import PositionStatus
    return sorted(PositionStatus.TERMINAL)
OCC_RE = re.compile(r"^[A-Z0-9.]{1,6}\d{6}[CP]\d{8}$")


def normalize_contract(value: Any) -> str:
    return str(value or "").upper().replace(" ", "").strip()


def is_valid_occ_contract(value: Any) -> bool:
    """Return True only for a normalized, complete OCC option symbol."""
    return bool(OCC_RE.fullmatch(normalize_contract(value)))


def position_direction(position: dict) -> str:
    return str(
        position.get("side") or position.get("direction") or ""
    ).upper().strip()


def positive_float(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        parsed = float(value)
    except Exception:
        return 0.0
    if not math.isfinite(parsed) or parsed <= 0:
        return 0.0
    return parsed


def positive_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        numeric = float(value)
    except Exception:
        return 0
    if not math.isfinite(numeric) or numeric <= 0 or not numeric.is_integer():
        return 0
    return int(numeric)


def nonnegative_int(value: Any) -> int | None:
    """Parse a durable non-negative integer without coercing bad truth."""
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric) or numeric < 0 or not numeric.is_integer():
        return None
    return int(numeric)


def parse_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw /= 1000.0
        try:
            parsed = datetime.fromtimestamp(raw, tz=timezone.utc)
        except Exception:
            return None
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except Exception:
            parsed = None
            for fmt in (
                "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
            ):
                try:
                    parsed = datetime.strptime(text, fmt)
                    break
                except Exception:
                    continue
            if parsed is None:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_broker_fill_timestamp(value: Any) -> datetime | None:
    """Parse an explicit broker execution timestamp, fail closed.

    Generic local lifecycle timestamps are intentionally not accepted here. A
    broker timestamp must be timezone-aware (or an unambiguous numeric epoch),
    because naive values and last-updated timestamps cannot establish exit
    chronology or execution provenance.
    """
    if value is None or value == "" or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        parsed = value
        try:
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                return None
        except Exception:
            return None
    elif isinstance(value, (int, float)):
        try:
            raw = float(value)
        except Exception:
            return None
        if not math.isfinite(raw):
            return None
        if raw > 10_000_000_000:
            raw /= 1000.0
        try:
            parsed = datetime.fromtimestamp(raw, tz=timezone.utc)
        except Exception:
            return None
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except Exception:
            parsed = None
            for fmt in (
                "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S%z",
            ):
                try:
                    parsed = datetime.strptime(text, fmt)
                    break
                except Exception:
                    continue
            if parsed is None:
                return None
        try:
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                return None
        except Exception:
            return None
    return parsed.astimezone(timezone.utc)


def _metadata_object(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


@dataclass(frozen=True)
class BrokerPositionSnapshot:
    """Validated broker-position truth used by manual-close PASS 2."""

    state: str
    rows: list[dict]
    reason: str = ""


_POSITION_OPTION_IDENTITY_KEYS = (
    "option_symbol",
    "optionSymbol",
    "option_contract",
    "optionContract",
    "contract",
)
_POSITION_GENERAL_IDENTITY_KEYS = ("symbol", "instrument")
_POSITION_UNDERLYING_KEYS = ("underlying",)
_POSITION_QUANTITY_KEYS = ("quantity", "qty")
_POSITION_OPTION_HINT_KEYS = (
    "option_type",
    "put_call",
    "right",
    "strike",
    "expiration",
    "expiration_date",
    "expiry",
)
_POSITION_NON_OPTION_SYMBOL_RE = re.compile(r"^[A-Z0-9.]{1,6}$")


def _position_field_is_present(value: Any) -> bool:
    return value is not None and not (
        isinstance(value, str) and not value.strip()
    )


def _position_envelope_has_error(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    status = str(value.get("status") or "").strip().lower()
    return status in _POSITION_ERROR_STATUSES or any(
        key in value and _position_field_is_present(value.get(key))
        for key in ("error", "errors", "message", "reason")
    )


def _position_quantity(row: dict) -> int | None:
    """Return one validated, signed, non-zero broker position quantity."""
    values: list[int] = []
    for key in _POSITION_QUANTITY_KEYS:
        if key not in row:
            continue
        raw_value = row.get(key)
        if not _position_field_is_present(raw_value):
            return None
        if isinstance(raw_value, bool):
            return None
        try:
            numeric = float(raw_value)
        except Exception:
            return None
        if not math.isfinite(numeric) or not numeric.is_integer():
            return None
        quantity = int(numeric)
        if quantity == 0:
            return None
        values.append(quantity)
    if not values or len(set(values)) != 1:
        return None
    return values[0]


def _position_identity(row: dict) -> tuple[str, str, str]:
    """Return ``(kind, identity, reason)`` for one broker position row."""
    exact_contracts: list[str] = []
    non_option_symbols: list[str] = []
    underlying_symbols: list[str] = []
    invalid_identity = False

    for key in _POSITION_OPTION_IDENTITY_KEYS:
        if key not in row or not _position_field_is_present(row.get(key)):
            continue
        normalized = normalize_contract(row.get(key))
        if is_valid_occ_contract(normalized):
            exact_contracts.append(normalized)
        else:
            invalid_identity = True

    for key in _POSITION_GENERAL_IDENTITY_KEYS:
        if key not in row or not _position_field_is_present(row.get(key)):
            continue
        normalized = normalize_contract(row.get(key))
        if is_valid_occ_contract(normalized):
            exact_contracts.append(normalized)
        elif _POSITION_NON_OPTION_SYMBOL_RE.fullmatch(normalized):
            non_option_symbols.append(normalized)
        else:
            invalid_identity = True

    for key in _POSITION_UNDERLYING_KEYS:
        if key not in row or not _position_field_is_present(row.get(key)):
            continue
        normalized = normalize_contract(row.get(key))
        if _POSITION_NON_OPTION_SYMBOL_RE.fullmatch(normalized):
            underlying_symbols.append(normalized)
        else:
            invalid_identity = True

    if len(set(exact_contracts)) > 1:
        return "ambiguous", "", "conflicting_exact_occ_identity"
    if invalid_identity:
        return "invalid", "", "invalid_or_weak_position_identity"

    if exact_contracts:
        contract = exact_contracts[0]
        underlying = contract[:-15]
        if any(
            symbol != underlying
            for symbol in (*non_option_symbols, *underlying_symbols)
        ):
            return "ambiguous", "", "position_identity_underlying_conflict"
        return "option", contract, ""

    option_hint = any(
        key in row and _position_field_is_present(row.get(key))
        for key in _POSITION_OPTION_HINT_KEYS
    )
    side = str(row.get("side") or row.get("direction") or "").upper().strip()
    if side in {"CALL", "PUT"}:
        option_hint = True
    if option_hint:
        return "invalid", "", "option_row_missing_exact_occ_identity"
    all_non_option_symbols = [*non_option_symbols, *underlying_symbols]
    if len(set(all_non_option_symbols)) > 1:
        return "ambiguous", "", "conflicting_non_option_identity"
    if not non_option_symbols:
        return "invalid", "", "non_option_identity_missing"
    return "non_option", non_option_symbols[0], ""


def _normalize_position_rows(rows: Any) -> BrokerPositionSnapshot:
    if not isinstance(rows, list):
        return BrokerPositionSnapshot(
            POSITIONS_MALFORMED,
            [],
            "broker_position_rows_malformed",
        )

    normalized: list[dict] = []
    option_contracts: set[str] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            return BrokerPositionSnapshot(
                POSITIONS_INCOMPLETE,
                [],
                f"position_row_{index}_not_mapping",
            )
        kind, identity, reason = _position_identity(row)
        if kind == "ambiguous":
            return BrokerPositionSnapshot(POSITIONS_AMBIGUOUS, [], reason)
        if kind != "option" and kind != "non_option":
            return BrokerPositionSnapshot(POSITIONS_INCOMPLETE, [], reason)
        quantity = _position_quantity(row)
        if quantity is None:
            return BrokerPositionSnapshot(
                POSITIONS_INCOMPLETE,
                [],
                f"position_row_{index}_quantity_invalid",
            )
        if kind == "option":
            if identity in option_contracts:
                return BrokerPositionSnapshot(
                    POSITIONS_AMBIGUOUS,
                    [],
                    f"duplicate_exact_occ:{identity}",
                )
            option_contracts.add(identity)
        normalized.append(
            {"symbol": identity, "quantity": quantity, "raw": dict(row)}
        )

    state = (
        POSITIONS_AVAILABLE_COMPLETE_NONEMPTY
        if normalized
        else POSITIONS_AVAILABLE_COMPLETE_EMPTY
    )
    return BrokerPositionSnapshot(state, normalized)


def normalize_positions_payload(payload: Any) -> BrokerPositionSnapshot:
    if not isinstance(payload, dict) or "positions" not in payload:
        return BrokerPositionSnapshot(
            POSITIONS_MALFORMED,
            [],
            "broker_positions_payload_malformed",
        )
    if _position_envelope_has_error(payload):
        return BrokerPositionSnapshot(
            POSITIONS_MALFORMED,
            [],
            "broker_positions_payload_error",
        )

    positions_node = payload.get("positions")
    if positions_node is None or positions_node == "null":
        return BrokerPositionSnapshot(POSITIONS_AVAILABLE_COMPLETE_EMPTY, [])
    if not isinstance(positions_node, dict):
        return BrokerPositionSnapshot(
            POSITIONS_MALFORMED,
            [],
            "broker_positions_node_malformed",
        )
    if _position_envelope_has_error(positions_node):
        return BrokerPositionSnapshot(
            POSITIONS_MALFORMED,
            [],
            "broker_positions_node_error",
        )
    if not positions_node:
        return BrokerPositionSnapshot(
            POSITIONS_MALFORMED,
            [],
            "broker_positions_node_malformed",
        )

    if "position" not in positions_node:
        return BrokerPositionSnapshot(
            POSITIONS_MALFORMED,
            [],
            "broker_positions_node_malformed",
        )
    rows = positions_node["position"]
    # Adapter contract: Tradier's empty-account response is represented by a
    # null positions node or a null position member.  Either is authoritative
    # empty only after the surrounding envelope has passed the error checks.
    if rows is None or rows == "null":
        return BrokerPositionSnapshot(POSITIONS_AVAILABLE_COMPLETE_EMPTY, [])
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list):
        return BrokerPositionSnapshot(
            POSITIONS_MALFORMED,
            [],
            "broker_position_rows_malformed",
        )
    return _normalize_position_rows(rows)


def fetch_authoritative_broker_positions(broker: Any) -> BrokerPositionSnapshot:
    authoritative = getattr(broker, "list_positions_authoritative", None)
    if callable(authoritative):
        try:
            rows = authoritative()
        except Exception as exc:
            return BrokerPositionSnapshot(
                POSITIONS_UNAVAILABLE,
                [],
                f"authoritative_positions_read_failed:{type(exc).__name__}",
            )
        return _normalize_position_rows(rows)

    raw_get = getattr(broker, "_get", None)
    cfg = getattr(broker, "cfg", None)
    account_id = str(getattr(cfg, "account_id", "") or "").strip()
    if not callable(raw_get) or not account_id:
        return BrokerPositionSnapshot(
            POSITIONS_UNAVAILABLE,
            [],
            "authoritative_broker_positions_unavailable",
        )

    try:
        payload = raw_get(f"/v1/accounts/{account_id}/positions")
    except Exception as exc:
        return BrokerPositionSnapshot(
            POSITIONS_UNAVAILABLE,
            [],
            f"broker_positions_read_failed:{type(exc).__name__}",
        )
    return normalize_positions_payload(payload)


def _broker_position_contract_quantities(
    snapshot: BrokerPositionSnapshot,
) -> tuple[dict[str, int] | None, str]:
    """Build the PASS 2 map only from an explicitly complete snapshot."""
    if not isinstance(snapshot, BrokerPositionSnapshot):
        return None, "snapshot_result_malformed"
    if snapshot.state not in POSITION_COMPLETE_STATES:
        return None, f"snapshot_state:{snapshot.state}"

    contract_quantities: dict[str, int] = {}
    for row in snapshot.rows:
        if not isinstance(row, dict):
            return None, "normalized_position_row_not_mapping"
        contract = normalize_contract(row.get("symbol"))
        # Non-option rows were validated at the snapshot boundary and do not
        # establish option-contract presence/absence. Exact option rows use
        # the same signed, non-zero quantity contract as normalization so a
        # valid short position remains presence evidence without becoming a
        # false absence.
        if not is_valid_occ_contract(contract):
            continue
        quantity = _position_quantity(row)
        if quantity is None:
            return None, "normalized_option_row_invalid"
        if contract in contract_quantities:
            return None, f"duplicate_exact_occ:{contract}"
        contract_quantities[contract] = quantity
    return contract_quantities, ""


def _order_page_row_is_structured(row: Any) -> bool:
    """Require enough envelope shape to distinguish a row from malformed data."""
    return isinstance(row, dict) and bool(order_id(row)) and bool(order_status(row))


def _normalize_orders_page(payload: Any) -> tuple[str, list[dict]]:
    """Normalize one order page without laundering malformed truth into empty."""
    if payload is None or payload == "null":
        return ORDERS_MALFORMED, []
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        if any(
            key in payload and payload.get(key) not in (None, "")
            for key in ("error", "errors", "message", "reason")
        ):
            return ORDERS_MALFORMED, []
        if "orders" not in payload:
            return ORDERS_MALFORMED, []
        node = payload.get("orders")
        if node is None or node == "null":
            return ORDERS_MALFORMED, []
        if isinstance(node, list):
            rows = node
        elif isinstance(node, dict):
            if "order" not in node:
                return ORDERS_MALFORMED, []
            rows = node.get("order")
        else:
            return ORDERS_MALFORMED, []
        if rows is None or rows == "null":
            return ORDERS_MALFORMED, []
        if isinstance(rows, dict):
            rows = [rows]
    else:
        return ORDERS_MALFORMED, []

    if not isinstance(rows, list):
        return ORDERS_MALFORMED, []
    if any(not _order_page_row_is_structured(row) for row in rows):
        return ORDERS_MALFORMED, []
    normalized = [dict(row) for row in rows]
    return (_ORDER_PAGE_EMPTY if not normalized else _ORDER_PAGE_DATA), normalized


def fetch_all_current_session_orders(broker: Any) -> tuple[str, list[dict]]:
    """Fetch every current-session order and preserve completeness state.

    Tradier's /v1/accounts/{id}/orders defaults to ~25 rows without an
    explicit limit and returns only the current market session. This helper
    requests an explicit high limit and paginates until a valid short/empty
    page. A fallback ``list_orders()`` result is deliberately INCOMPLETE
    because that API does not prove that all current-session rows were seen.
    """
    raw_get = getattr(broker, "_get", None)
    cfg = getattr(broker, "cfg", None)
    account_id = str(getattr(cfg, "account_id", "") or "").strip()

    if not callable(raw_get) or not account_id:
        list_orders = getattr(broker, "list_orders", None)
        if not callable(list_orders):
            return ORDERS_UNAVAILABLE, []
        log.warning(
            "MANUAL_CLOSE_ORDERS_PAGINATION_UNAVAILABLE using list_orders "
            "(subject to ~25-row default cap on Tradier)"
        )
        try:
            page_state, rows = _normalize_orders_page(list_orders())
        except Exception as exc:
            log.error("MANUAL_CLOSE_ORDERS_FALLBACK_FAILED err=%s", exc)
            return ORDERS_UNAVAILABLE, []
        if page_state == ORDERS_MALFORMED:
            return ORDERS_MALFORMED, []
        return ORDERS_INCOMPLETE, rows

    all_rows: list[dict] = []
    seen_ids: set[str] = set()
    page = 1
    limit = MANUAL_CLOSE_ORDERS_PAGE_LIMIT
    while True:
        try:
            payload = raw_get(
                f"/v1/accounts/{account_id}/orders?includeTags=true"
                f"&page={page}&limit={limit}"
            )
        except Exception as exc:
            log.warning(
                "MANUAL_CLOSE_ORDERS_PAGE_UNAVAILABLE page=%s err=%s",
                page, exc,
            )
            return ORDERS_UNAVAILABLE, []
        page_state, rows = _normalize_orders_page(payload)
        if page_state == ORDERS_MALFORMED:
            log.warning("MANUAL_CLOSE_ORDERS_PAGE_MALFORMED page=%s", page)
            return ORDERS_MALFORMED, []
        if page_state == _ORDER_PAGE_EMPTY:
            return (
                ORDERS_AVAILABLE_EMPTY if not all_rows
                else ORDERS_AVAILABLE_COMPLETE,
                all_rows,
            )
        added = 0
        for row in rows:
            oid = str(row.get("id") or row.get("order_id") or "").strip()
            if oid and oid in seen_ids:
                continue
            if oid:
                seen_ids.add(oid)
            all_rows.append(row)
            added += 1
        # Terminate when page is short (last page) or when nothing new was
        # added (defensive; broker occasionally returns overlapping pages).
        if added == 0:
            log.warning(
                "MANUAL_CLOSE_ORDERS_PAGE_INCOMPLETE_NO_PROGRESS page=%s total=%s",
                page, len(all_rows),
            )
            return ORDERS_INCOMPLETE, all_rows
        if len(rows) < limit:
            return ORDERS_AVAILABLE_COMPLETE, all_rows
        page += 1
        if page > 50:  # hard ceiling — 50 * 500 = 25k orders
            log.warning(
                "MANUAL_CLOSE_ORDERS_PAGE_HARD_STOP page=%s total=%s",
                page, len(all_rows),
            )
            return ORDERS_INCOMPLETE, all_rows


def order_legs(order: dict) -> list[dict]:
    legs = order.get("leg") or order.get("legs")
    if isinstance(legs, dict):
        nested = legs.get("leg") if "leg" in legs else legs
        if isinstance(nested, dict):
            return [nested]
        if isinstance(nested, list):
            return [item for item in nested if isinstance(item, dict)]
        return []
    if isinstance(legs, list):
        return [item for item in legs if isinstance(item, dict)]
    return []


_ORDER_IDENTITY_KEYS = (
    "option_symbol",
    "optionSymbol",
    "contract",
    "instrument",
    "option_contract",
    "optionContract",
    "symbol",
)
_ORDER_DIRECTION_KEYS = (
    "side",
    "action",
    "instruction",
    "order_action",
    "transaction_type",
    "trade_action",
    "position_effect",
)


def _order_records(order: dict) -> list[dict]:
    records = [order] if isinstance(order, dict) else []
    nested = order.get("raw") if isinstance(order, dict) else None
    if isinstance(nested, dict):
        records.append(nested)
    return records


def _order_identity_records(order: dict):
    for record in _order_records(order):
        yield record
        yield from order_legs(record)


def order_contract(order: dict) -> str:
    exact_values: set[str] = set()
    underlying_values: set[str] = set()
    invalid = False
    for record in _order_identity_records(order):
        for key in _ORDER_IDENTITY_KEYS:
            if key not in record or record.get(key) in (None, ""):
                continue
            value = record.get(key)
            contract = normalize_contract(value)
            if is_valid_occ_contract(contract):
                exact_values.add(contract)
            elif key in {"symbol", "instrument"} and re.fullmatch(
                r"[A-Z0-9.]{1,6}", contract
            ):
                underlying_values.add(contract)
            else:
                invalid = True
    if len(exact_values) != 1 or invalid:
        return ""
    exact = next(iter(exact_values))
    underlying = exact[:-15]
    if any(value != underlying for value in underlying_values):
        return ""
    return exact


def order_has_instrument_identity(order: dict) -> bool:
    """Return whether root/raw/leg identity fields are structurally valid."""
    saw_identity = False
    for record in _order_identity_records(order):
        for key in _ORDER_IDENTITY_KEYS:
            if key not in record or record.get(key) in (None, ""):
                continue
            value = record.get(key)
            normalized = normalize_contract(value)
            if is_valid_occ_contract(normalized):
                saw_identity = True
                continue
            if key in {"symbol", "instrument"} and re.fullmatch(
                r"[A-Z0-9.]{1,6}", normalized
            ):
                saw_identity = True
                continue
            return False
    return saw_identity


def order_direction_values(order: dict) -> list[str]:
    values: list[str] = []
    for record in _order_records(order):
        for candidate in (record, *order_legs(record)):
            for key in _ORDER_DIRECTION_KEYS:
                value = str(candidate.get(key) or "").lower().replace(" ", "_").strip()
                if value:
                    values.append(value)
    return values


def order_has_structured_direction(order: dict) -> bool:
    return bool(order_direction_values(order))


def order_is_exit_like(order: dict) -> bool:
    values = order_direction_values(order)
    compact = {re.sub(r"[\s_-]+", "", value) for value in values}
    if compact.intersection({"selltoclose", "stc"}):
        return True
    return "sell" in compact and bool(compact.intersection({"close", "closing"}))


def order_side(order: dict) -> str:
    values = order_direction_values(order)
    if not values:
        return ""
    compact = {re.sub(r"[\s_-]+", "", value) for value in values}
    if compact.intersection({"selltoclose", "stc"}):
        return "sell_to_close"
    if "sell" in compact and compact.intersection({"close", "closing"}):
        return "sell_to_close"
    return values[0]


def order_status(order: dict) -> str:
    return str(order.get("status") or "").lower().strip()


def order_id(order: dict) -> str:
    for key in ("id", "order_id", "broker_order_id"):
        value = str(order.get(key) or "").strip()
        if value:
            return value
    return ""


def order_filled_qty(order: dict) -> int:
    for key in (
        "exec_quantity",
        "filled_quantity",
        "filled_qty",
        "executed_quantity",
    ):
        qty = positive_int(order.get(key))
        if qty > 0:
            return qty
    if order_status(order) == "filled":
        return positive_int(order.get("quantity") or order.get("qty"))
    return 0


def order_fill_price(order: dict) -> float:
    for key in (
        "avg_fill_price",
        "average_fill_price",
        "fill_price",
        "last_fill_price",
    ):
        price = positive_float(order.get(key))
        if price > 0:
            return price
    return 0.0


def order_created_at(order: dict) -> datetime | None:
    for key in ("create_date", "created_at", "submitted_at"):
        parsed = parse_timestamp(order.get(key))
        if parsed is not None:
            return parsed
    return None


def _broker_fill_timestamp(order: dict) -> tuple[datetime | None, str | None]:
    candidates: list[tuple[str, datetime]] = []
    for key in BROKER_FILL_TIMESTAMP_KEYS:
        raw = order.get(key)
        if raw is None or raw == "":
            continue
        # Tradier documents transaction_date as the order's last-updated time,
        # so it is fill authority only for a terminal FILLED order. Never use
        # it for working/partial/lifecycle-only rows, and never fall back to
        # update_date or updated_at.
        if key == "transaction_date" and order_status(order) != "filled":
            continue
        parsed = parse_broker_fill_timestamp(raw)
        if parsed is None:
            # A present but malformed execution field is unsafe. Do not
            # silently fall back to a lifecycle/update timestamp.
            return None, None
        candidates.append((key, parsed))
    if candidates:
        first_key, first_ts = candidates[0]
        if any(parsed != first_ts for _, parsed in candidates[1:]):
            return None, None
        return first_ts, first_key

    return None, None


def order_filled_at(order: dict) -> datetime | None:
    return _broker_fill_timestamp(order)[0]


def normalize_broker_orders(raw_orders: Any) -> list[dict]:
    if raw_orders is None:
        return []
    if isinstance(raw_orders, dict):
        raw_orders = [raw_orders]
    if not isinstance(raw_orders, list):
        raise ValueError("BROKER_ORDERS_RESULT_MALFORMED")
    return [dict(order) for order in raw_orders if isinstance(order, dict)]


def _normalize_fill(order: dict) -> dict | None:
    broker_order_id = order_id(order)
    filled_qty = order_filled_qty(order)
    fill_price = order_fill_price(order)
    filled_at, timestamp_key = _broker_fill_timestamp(order)
    if (
        not broker_order_id
        or filled_qty <= 0
        or fill_price <= 0
        or filled_at is None
        or timestamp_key is None
    ):
        return None
    return {
        "broker_order_id": broker_order_id,
        "filled_qty": filled_qty,
        "fill_price": fill_price,
        "filled_at": filled_at,
        "fill_timestamp_source": BROKER_FILL_TIMESTAMP_SOURCE,
        "fill_timestamp_key": timestamp_key,
        "created_at": order_created_at(order),
        "raw_status": order_status(order),
        "raw_side": order_side(order),
    }



def _validate_durable_fills(
    fills: list[dict],
    *,
    position: dict,
    detected_at: datetime,
    client_id: str = "",
) -> list[dict]:
    """Re-prove durable adopted fills against the current position.

    PR #386 hardening: identity is MANDATORY. A durable row that merely
    shares a position_id but lacks any of the identity/economics below is
    corrupt evidence and cannot silently inflate the weighted-close
    aggregate. Every field must be present and exact:

      * broker_order_id: nonempty
      * filled_qty > 0
      * fill_price > 0
      * filled_at is a datetime instance (not None, not string, not epoch)
      * fill_timestamp_source is exactly broker_response
      * fill_timestamp_key is an accepted broker fill/event field
      * filled_at >= position entry timestamp (when entry is known)
      * db_status: nonempty AND in {EXIT_FILLED, EXIT_PARTIAL_FILL}
      * db_contract: nonempty AND exactly equals position contract
      * db_direction: nonempty AND in {CALL, PUT} AND equals position direction
      * pos_direction: valid CALL/PUT — reject the whole row if the position's
        own direction is unknown, since the direction check cannot then be made

    Rejected rows are logged loudly and dropped; no partial-credit acceptance.
    """
    pos_contract = normalize_contract(position.get("contract"))
    pos_direction = str(
        position.get("side") or position.get("direction") or ""
    ).upper().strip()
    opened_at = parse_timestamp(
        position.get("entry_ts") or position.get("opened_at")
    )
    position_id = str(position.get("id") or "").strip()

    if not is_valid_occ_contract(pos_contract):
        log.warning(
            "[%s] MANUAL_CLOSE_DURABLE_FILL_POSITION_CONTRACT_INVALID "
            "pos=%s contract=%r — rejected",
            client_id, position_id, pos_contract,
        )
        return []

    # Position direction validity is a precondition — if the position row
    # itself lacks a valid CALL/PUT direction we cannot prove alignment for
    # any fill.  Reject the entire durable set for this position.
    if pos_direction not in {"CALL", "PUT"}:
        for f in fills:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_POSITION_DIRECTION_INVALID "
                "pos=%s broker_id=%s pos_direction=%r — rejected",
                client_id, position_id,
                str(f.get("broker_order_id") or "").strip(), pos_direction,
            )
        return []

    valid: list[dict] = []
    for f in fills:
        bid = str(f.get("broker_order_id") or "").strip()
        db_contract = str(f.get("db_contract") or "").upper().strip()
        db_direction = str(f.get("db_direction") or "").upper().strip()
        db_status = str(f.get("raw_status") or "").upper().strip()
        filled_qty = positive_int(f.get("filled_qty"))
        fill_price = positive_float(f.get("fill_price"))
        filled_at = f.get("filled_at")
        timestamp_source = str(f.get("fill_timestamp_source") or "").strip()
        timestamp_key = str(f.get("fill_timestamp_key") or "").strip()

        if not bid:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_NO_ID pos=%s — rejected",
                client_id, position_id,
            )
            continue
        durable_local_order_id = str(f.get("local_order_id") or "").strip()
        if durable_local_order_id and (
            not client_id
            or durable_local_order_id != _external_local_order_id(client_id, bid)
        ):
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_LOCAL_ID_MISMATCH pos=%s "
                "broker_id=%s local_order_id=%r — rejected",
                client_id, position_id, bid, durable_local_order_id,
            )
            continue
        if filled_qty <= 0 or fill_price <= 0:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_ECONOMICS_INVALID pos=%s "
                "broker_id=%s qty=%s price=%s — rejected",
                client_id, position_id, bid, filled_qty, fill_price,
            )
            continue
        if not isinstance(filled_at, datetime):
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_TIMESTAMP_INVALID pos=%s "
                "broker_id=%s filled_at_type=%s — rejected",
                client_id, position_id, bid, type(filled_at).__name__,
            )
            continue
        try:
            timestamp_is_aware = (
                filled_at.tzinfo is not None and filled_at.utcoffset() is not None
            )
        except Exception:
            timestamp_is_aware = False
        if not timestamp_is_aware:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_TIMESTAMP_NAIVE pos=%s "
                "broker_id=%s — rejected",
                client_id, position_id, bid,
            )
            continue
        if (
            timestamp_source != BROKER_FILL_TIMESTAMP_SOURCE
            or timestamp_key not in BROKER_FILL_TIMESTAMP_KEYS
        ):
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_TIMESTAMP_PROVENANCE_INVALID "
                "pos=%s broker_id=%s source=%r key=%r — rejected",
                client_id, position_id, bid, timestamp_source, timestamp_key,
            )
            continue
        if not db_status or db_status not in DURABLE_EXIT_FILLED_STATUSES:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_STATUS_REJECTED pos=%s "
                "broker_id=%s status=%r — rejected",
                client_id, position_id, bid, db_status,
            )
            continue
        if not db_contract:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_CONTRACT_MISSING pos=%s "
                "broker_id=%s — rejected",
                client_id, position_id, bid,
            )
            continue
        if not is_valid_occ_contract(db_contract):
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_CONTRACT_INVALID pos=%s "
                "broker_id=%s contract=%r — rejected",
                client_id, position_id, bid, db_contract,
            )
            continue
        if db_contract != pos_contract:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_CONTRACT_MISMATCH pos=%s "
                "broker_id=%s expected=%s got=%s — rejected",
                client_id, position_id, bid, pos_contract, db_contract,
            )
            continue
        if not db_direction or db_direction not in {"CALL", "PUT"}:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_DIRECTION_INVALID pos=%s "
                "broker_id=%s direction=%r — rejected",
                client_id, position_id, bid, db_direction,
            )
            continue
        if db_direction != pos_direction:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_DIRECTION_MISMATCH pos=%s "
                "broker_id=%s expected=%s got=%s — rejected",
                client_id, position_id, bid, pos_direction, db_direction,
            )
            continue
        if opened_at is not None and filled_at < opened_at:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_TIMESTAMP_STALE pos=%s "
                "broker_id=%s fill_ts=%s entry_ts=%s — rejected",
                client_id, position_id, bid, filled_at, opened_at,
            )
            continue
        if (filled_at - detected_at).total_seconds() > MANUAL_CLOSE_FUTURE_SKEW_SEC:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_TIMESTAMP_FUTURE pos=%s "
                "broker_id=%s fill_ts=%s detected_at=%s — rejected",
                client_id, position_id, bid, filled_at, detected_at,
            )
            continue
        valid.append(f)

    return valid


def select_external_close_fills(
    *,
    orders: list[dict],
    position: dict,
    bot_exit_order_ids: set[str],
    adopted_fills: list[dict] | None = None,
    detected_at: datetime,
) -> tuple[dict | None, str]:
    """Pick the exact broker fill(s) that closed a missing position.

    Distinguishes three categories of matching orders:

      * bot_exit_order_ids  → legitimate bot EXIT ownership. If ANY match,
        the whole evidence set is rejected — the bot's own path owns this.
      * adopted_fills → pre-loaded from DB: our own previously-adopted
        external EXIT rows for THIS position with full fill evidence. Used
        directly as adopted_hits without re-scanning broker orders, which
        enables cross-session recovery when broker no longer returns
        previous-session orders. Never re-adopted.
      * everything else → candidate external fills to adopt now.

    Aggregate quantity constraint remains exact: adopted-so-far plus new
    candidates must equal the position's remaining quantity. Anything
    else is ambiguous and rejected.
    """
    contract = normalize_contract(position.get("contract"))
    opened_at = parse_timestamp(position.get("entry_ts") or position.get("opened_at"))
    required_qty = positive_int(
        position.get("quantity_remaining")
        if position.get("quantity_remaining") is not None
        else position.get("qty")
    )

    if not contract:
        return None, "position_contract_missing"
    if not is_valid_occ_contract(contract):
        return None, "position_contract_invalid"
    if opened_at is None:
        return None, "position_opened_at_missing"
    if required_qty <= 0:
        return None, "position_quantity_missing"

    bot_ids = {str(v or "").strip() for v in bot_exit_order_ids if v}
    # adopted_fills: pre-loaded from DB. Re-validate each fill against this
    # position's contract, direction, status, and entry timestamp before
    # allowing it to contribute to the weighted-close aggregate. A durable row
    # that merely shares a position_id must not silently corrupt the evidence.
    client_id_ctx = str(position.get("client_id") or "")
    pre_adopted: list[dict] = _validate_durable_fills(
        list(adopted_fills or []),
        position=position,
        detected_at=detected_at,
        client_id=client_id_ctx,
    )
    adopted_ids: set[str] = {
        str(f.get("broker_order_id") or "").strip()
        for f in pre_adopted
        if f.get("broker_order_id")
    }

    bot_hit = False
    external_fills: list[dict] = []

    for order in orders:
        if order_contract(order) != contract:
            continue
        if order_side(order) not in MANUAL_CLOSE_SIDES:
            continue
        if order_status(order) not in FILLED_ORDER_STATUSES:
            continue

        normalized = _normalize_fill(order)
        if normalized is None:
            continue
        if normalized["filled_at"] < opened_at:
            continue
        if (normalized["filled_at"] - detected_at).total_seconds() > MANUAL_CLOSE_FUTURE_SKEW_SEC:
            continue

        broker_order_id = normalized["broker_order_id"]
        if broker_order_id in bot_ids:
            bot_hit = True
            break
        if broker_order_id in adopted_ids:
            # Already durably adopted; do not re-adopt.
            continue
        external_fills.append(normalized)

    # Use DB-loaded adopted fills directly — do NOT re-scan broker orders for them.
    adopted_hits: list[dict] = pre_adopted

    if bot_hit:
        return None, "bot_owned_exit_order_present"

    # Dedupe both sets by broker_order_id, keeping the latest filled_at row.
    def _dedupe(rows: list[dict]) -> list[dict]:
        best: dict[str, dict] = {}
        for r in rows:
            cur = best.get(r["broker_order_id"])
            if cur is None or r["filled_at"] > cur["filled_at"]:
                best[r["broker_order_id"]] = r
        return sorted(best.values(), key=lambda r: r["filled_at"])

    # Dedupe: adopted_hits come from DB rows; external_fills from broker orders.
    adopted_hits = _dedupe(adopted_hits)
    external_fills = _dedupe(external_fills)

    if not adopted_hits and not external_fills:
        return None, "no_exact_external_filled_exit_order"

    adopted_qty = sum(int(f["filled_qty"]) for f in adopted_hits)
    new_qty = sum(int(f["filled_qty"]) for f in external_fills)
    total_qty = adopted_qty + new_qty
    if total_qty != required_qty:
        return None, f"external_fill_qty_ambiguous:{total_qty}/{required_qty}"

    # Weighted aggregate covers BOTH adopted + new — that's the real
    # weighted broker truth for this position's close.
    all_fills = sorted(adopted_hits + external_fills, key=lambda r: r["filled_at"])
    weighted_notional = sum(
        float(f["fill_price"]) * int(f["filled_qty"]) for f in all_fills
    )
    average_fill = weighted_notional / total_qty if total_qty > 0 else 0.0
    if average_fill <= 0:
        return None, "external_fill_price_invalid"

    return {
        "fills": external_fills,          # only NEW fills need adoption
        "adopted_fills": adopted_hits,    # already durable; do not re-insert
        "all_fills": all_fills,           # aggregate view for the finalizer
        "filled_qty": required_qty,
        "fill_price": round(average_fill, 6),
        "filled_ts": all_fills[-1]["filled_at"].isoformat(),
        "broker_order_id": all_fills[-1]["broker_order_id"],
        "broker_order_ids": [r["broker_order_id"] for r in all_fills],
    }, "exact_external_broker_fill"


def load_manual_close_state(
    client_id: str,
    execution_mode: str,
) -> tuple[list[dict], set[str], dict[str, list[dict]]]:
    """Return (active_positions, bot_exit_ids, adopted_fills_by_position).

    * active_positions covers the canonical active family (OPEN, CLOSING,
      PARTIAL, ACTIVE) plus any row with quantity_remaining > 0, scoped to
      the exact execution_mode of this runner. A PARTIAL or ACTIVE position
      with retained external evidence must remain scannable.
    * bot_exit_ids is the client's EXIT rows whose local_order_id does NOT
      start with the external-adoption prefix, filtered to the exact
      execution_mode so PAPER IDs never pollute the LIVE fence and vice versa.
    * adopted_fills_by_position maps position_id → list of full fill dicts
      (broker_order_id, filled_qty, fill_price, filled_at, created_at,
      raw_status, raw_side) for previously-adopted external EXIT rows. Loads
      the complete fill evidence from durable orders rows so that next-session
      finalization can reconstruct the weighted aggregate without depending on
      the broker returning previous-session orders.
    """
    from ap.db import conn, run_with_retry
    norm_mode = str(execution_mode or "").strip().lower()

    def _read():
        with conn() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    client_id,
                    contract,
                    underlying,
                    avg_fill,
                    qty,
                    quantity_remaining,
                    side,
                    direction,
                    local_order_id,
                    entry_ts,
                    opened_at,
                    execution_mode,
                    status,
                    exit_in_flight,
                    pending_exit_broker_order_id,
                    pending_exit_local_order_id
                FROM positions
                WHERE client_id=%s
                  AND LOWER(COALESCE(execution_mode,'')) = %s
                  AND (
                      UPPER(COALESCE(status,'')) = ANY(%s)
                      OR COALESCE(quantity_remaining, 0) > 0
                  )
                """,
                (client_id, norm_mode, list(ACTIVE_POSITION_STATUSES)),
            )
            positions = [dict(row) for row in (cursor.fetchall() or [])]

            cursor.execute(
                """
                SELECT
                    broker_order_id,
                    local_order_id,
                    position_id,
                    filled_qty,
                    fill_price,
                    filled_ts,
                    execution_mode,
                    status,
                    contract,
                    direction,
                    meta
                FROM orders
                WHERE client_id=%s
                  AND LOWER(COALESCE(execution_mode,'')) = %s
                  AND UPPER(COALESCE(kind,''))='EXIT'
                  AND COALESCE(broker_order_id,'') <> ''
                """,
                (client_id, norm_mode),
            )
            bot_ids: set[str] = set()
            adopted_fills_by_pos: dict[str, list[dict]] = {}
            for row in (cursor.fetchall() or []):
                broker_order_id = str(row.get("broker_order_id") or "").strip()
                if not broker_order_id:
                    continue
                local_order_id = str(row.get("local_order_id") or "").strip()
                position_id = str(row.get("position_id") or "").strip()
                if local_order_id.startswith(EXTERNAL_LOCAL_ID_PREFIX):
                    if (
                        not position_id
                        or local_order_id
                        != _external_local_order_id(client_id, broker_order_id)
                    ):
                        # An external-looking prefix is never a bot-owned
                        # fallback. It must be the exact client/order identity.
                        continue
                    row_status = str(row.get("status") or "").upper().strip()
                    if row_status not in DURABLE_EXIT_FILLED_STATUSES:
                        # Row exists with an external local_id prefix but has a
                        # non-durable status (e.g. still pending). Not valid as
                        # recovery evidence; skip without adding to bot_ids.
                        continue
                    metadata = _metadata_object(row.get("meta"))
                    if (
                        metadata.get("source")
                        != "manual_client_close_broker_fill"
                        or metadata.get("external_broker_order") is not True
                        or metadata.get("adopted_without_submit") is not True
                    ):
                        # Legacy/adulterated external rows are not restart
                        # truth and must not reach the finalizer.
                        continue
                    timestamp_source = str(
                        metadata.get("exit_fill_timestamp_source") or ""
                    ).strip()
                    timestamp_key = str(
                        metadata.get("exit_fill_timestamp_key") or ""
                    ).strip()
                    if (
                        timestamp_source != BROKER_FILL_TIMESTAMP_SOURCE
                        or timestamp_key not in BROKER_FILL_TIMESTAMP_KEYS
                    ):
                        # Legacy/adulterated external rows without an exact
                        # broker timestamp binding are not recovery truth.
                        continue
                    filled_qty = positive_int(row.get("filled_qty"))
                    fill_price = positive_float(row.get("fill_price"))
                    filled_at = parse_broker_fill_timestamp(row.get("filled_ts"))
                    db_contract = str(row.get("contract") or "").upper().strip()
                    db_direction = str(row.get("direction") or "").upper().strip()
                    if filled_qty > 0 and fill_price > 0 and filled_at is not None:
                        fill_dict: dict = {
                            "broker_order_id": broker_order_id,
                            "local_order_id": local_order_id,
                            "filled_qty": filled_qty,
                            "fill_price": fill_price,
                            "filled_at": filled_at,
                            "created_at": None,
                            "raw_status": row_status,
                            "raw_side": "sell_to_close",
                            "fill_timestamp_source": timestamp_source,
                            "fill_timestamp_key": timestamp_key,
                            # DB-sourced identity fields for cross-position validation.
                            "db_contract": db_contract,
                            "db_direction": db_direction,
                        }
                        adopted_fills_by_pos.setdefault(position_id, []).append(fill_dict)
                else:
                    bot_ids.add(broker_order_id)
            return positions, bot_ids, adopted_fills_by_pos

    return run_with_retry(_read)


def load_terminal_recovery_candidates(
    client_id: str,
    execution_mode: str,
) -> list[dict]:
    """PR #386 fix 2: candidates for proof/queue-only restart recovery.

    Returns terminal positions with zero remaining quantity that also carry
    at least one externally-adopted EXIT row in the durable ledger. Existing
    proof rows are intentionally not excluded: a crash after proof binding but
    before downstream queue cleanup must remain discoverable on restart.
    Scoped to the exact execution mode of the current runner.

    Handed to APPositionManager.repair_terminal_proof_from_persisted, which
    performs the actual FOR UPDATE re-read, invariant checks, and canonical
    proof repair. This function only surfaces the candidate set.
    """
    from ap.db import conn, run_with_retry
    norm_mode = str(execution_mode or "").strip().lower()

    def _read():
        with conn() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    client_id,
                    contract,
                    execution_mode,
                    status,
                    quantity_remaining,
                    qty,
                    contracts_exited,
                    exit_price,
                    realized_pnl,
                    realized_pnl_pct,
                    entry_ts,
                    exit_ts,
                    local_order_id,
                    broker_order_id,
                    side,
                    direction,
                    exit_reason,
                    close_source
                FROM positions p
                WHERE p.client_id = %s
                  AND LOWER(COALESCE(p.execution_mode, '')) = %s
                  AND UPPER(COALESCE(p.status, '')) = ANY(%s)
                  AND COALESCE(p.quantity_remaining, 0) <= 0
                  AND EXISTS (
                      SELECT 1 FROM orders o
                      WHERE o.client_id = p.client_id
                        AND o.position_id = p.id
                        AND LOWER(COALESCE(o.execution_mode, '')) = %s
                        AND UPPER(COALESCE(o.kind, '')) = 'EXIT'
                        AND UPPER(COALESCE(o.status, '')) = ANY(%s)
                        AND COALESCE(o.broker_order_id, '') <> ''
                        AND o.local_order_id = CONCAT(
                            'external-exit:', p.client_id, ':', o.broker_order_id
                        )
                        AND COALESCE(o.meta->>'source', '') =
                            'manual_client_close_broker_fill'
                        AND COALESCE(o.meta->>'external_broker_order', '') = 'true'
                        AND COALESCE(o.meta->>'adopted_without_submit', '') = 'true'
                        AND COALESCE(o.meta->>'exit_fill_timestamp_source', '') = %s
                        AND COALESCE(o.meta->>'exit_fill_timestamp_key', '') = ANY(%s)
                  )
                """,
                (
                    client_id,
                    norm_mode,
                    list(_terminal_position_statuses()),
                    norm_mode,
                    list(DURABLE_EXIT_FILLED_STATUSES),
                    BROKER_FILL_TIMESTAMP_SOURCE,
                    list(BROKER_FILL_TIMESTAMP_KEYS),
                ),
            )
            return [dict(row) for row in (cursor.fetchall() or [])]

    try:
        return run_with_retry(_read) or []
    except Exception as exc:
        log.error(
            "[%s] MANUAL_CLOSE_TERMINAL_RECOVERY_LOAD_FAILED mode=%s err=%s",
            client_id, norm_mode, exc,
        )
        return []


def _external_local_order_id(client_id: str, broker_order_id: str) -> str:
    return f"{EXTERNAL_LOCAL_ID_PREFIX}{client_id}:{broker_order_id}"


def _row_matches_expected(
    row: dict,
    *,
    client_id: str,
    position: dict,
    fill: dict,
    execution_mode: str,
) -> bool:
    if not isinstance(row, dict):
        return False
    expected_contract = normalize_contract(position.get("contract"))
    expected_side = position_direction(position)
    actual_mode = str(row.get("execution_mode") or "").lower().strip()
    try:
        actual_fill_price = float(row.get("fill_price") or 0)
    except Exception:
        actual_fill_price = 0.0
    metadata = _metadata_object(row.get("meta"))
    expected_timestamp_source = str(
        fill.get("fill_timestamp_source") or ""
    ).strip()
    expected_timestamp_key = str(fill.get("fill_timestamp_key") or "").strip()
    return bool(
        str(row.get("client_id") or "").strip().lower() == client_id.lower()
        and str(row.get("position_id") or "").strip() == str(position.get("id") or "").strip()
        and str(row.get("kind") or "").upper().strip() == "EXIT"
        and str(row.get("status") or "").upper().strip() in DURABLE_EXIT_FILLED_STATUSES
        and is_valid_occ_contract(expected_contract)
        and is_valid_occ_contract(row.get("contract"))
        and normalize_contract(row.get("contract")) == expected_contract
        and str(row.get("direction") or "").upper().strip() == expected_side
        and str(row.get("broker_order_id") or "").strip() == fill["broker_order_id"]
        and positive_int(row.get("filled_qty")) == int(fill["filled_qty"])
        and abs(actual_fill_price - float(fill["fill_price"])) < 0.000001
        and actual_mode == execution_mode
        and str(metadata.get("exit_fill_timestamp_source") or "").strip()
        == expected_timestamp_source
        and str(metadata.get("exit_fill_timestamp_key") or "").strip()
        == expected_timestamp_key
        and expected_timestamp_source == BROKER_FILL_TIMESTAMP_SOURCE
        and expected_timestamp_key in BROKER_FILL_TIMESTAMP_KEYS
    )


def adopt_external_exit_fills(
    *,
    client_id: str,
    execution_mode: str,
    position: dict,
    evidence: dict,
) -> tuple[bool, str]:
    """Atomically adopt every NEW external fill for one position.

    All fills for the same position run inside a single connection with
    ONE position-scoped advisory lock. If any fill fails validation or
    INSERT, the transaction is rolled back and nothing is persisted —
    the next scan re-observes the missing broker position and retries
    the whole evidence set. Prevents partial-adoption stranding.
    """
    from ap.db import conn, run_with_retry

    position_id = str(position.get("id") or "").strip()
    contract = normalize_contract(position.get("contract"))
    symbol = str(position.get("underlying") or "").upper().strip()
    direction = position_direction(position)
    if (
        not client_id
        or not position_id
        or not is_valid_occ_contract(contract)
        or direction not in {"CALL", "PUT"}
        or execution_mode not in VALID_EXECUTION_MODES
    ):
        return False, "external_exit_adoption_identity_invalid"

    fills = list(evidence.get("fills") or [])
    if not fills:
        # Nothing NEW to adopt (all fills were already durable). Caller
        # can still proceed to finalize with the aggregate.
        return True, "external_exit_adoption_nothing_to_do"

    lock_key = f"manual-close-adopt:{client_id}:{position_id}"

    def _adopt_all() -> tuple[bool, str]:
        with conn() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(('x' || md5(%s))::bit(64)::bigint)",
                (lock_key,),
            )
            for fill in fills:
                broker_order_id = str(fill.get("broker_order_id") or "").strip()
                filled_qty = positive_int(fill.get("filled_qty"))
                fill_price = positive_float(fill.get("fill_price"))
                filled_at = parse_broker_fill_timestamp(fill.get("filled_at"))
                created_at = parse_timestamp(fill.get("created_at"))
                timestamp_source = str(
                    fill.get("fill_timestamp_source") or ""
                ).strip()
                timestamp_key = str(fill.get("fill_timestamp_key") or "").strip()
                if (
                    not broker_order_id
                    or filled_qty <= 0
                    or fill_price <= 0
                    or filled_at is None
                    or timestamp_source != BROKER_FILL_TIMESTAMP_SOURCE
                    or timestamp_key not in BROKER_FILL_TIMESTAMP_KEYS
                ):
                    raise RuntimeError("external_exit_adoption_fill_invalid")

                local_order_id = _external_local_order_id(client_id, broker_order_id)
                meta = json.dumps(
                    {
                        "source": "manual_client_close_broker_fill",
                        "external_broker_order": True,
                        "broker_order_side": str(fill.get("raw_side") or "sell_to_close"),
                        "broker_order_status": str(fill.get("raw_status") or "filled"),
                        "broker_create_date_present": created_at is not None,
                        "adopted_without_submit": True,
                        "position_id": position_id,
                        "client_id": client_id,
                        "execution_mode": execution_mode,
                        "exit_fill_timestamp_source": timestamp_source,
                        "exit_fill_timestamp_key": timestamp_key,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )

                cursor.execute(
                    """
                    SELECT *
                    FROM orders
                    WHERE client_id=%s
                      AND broker_order_id=%s
                    LIMIT 2
                    """,
                    (client_id, broker_order_id),
                )
                existing = [dict(row) for row in (cursor.fetchall() or [])]
                if len(existing) > 1:
                    raise RuntimeError("external_exit_adoption_broker_id_ambiguous")
                if len(existing) == 1:
                    if not _row_matches_expected(
                        existing[0],
                        client_id=client_id,
                        position=position,
                        fill=fill,
                        execution_mode=execution_mode,
                    ):
                        raise RuntimeError("external_exit_adoption_existing_row_mismatch")
                    continue

                cursor.execute(
                    """
                    INSERT INTO orders (
                        client_id,
                        local_order_id,
                        broker_order_id,
                        position_id,
                        kind,
                        status,
                        symbol,
                        contract,
                        direction,
                        qty,
                        filled_qty,
                        fill_price,
                        created_ts,
                        updated_ts,
                        submitted_ts,
                        filled_ts,
                        meta,
                        execution_mode
                    )
                    VALUES (
                        %s,%s,%s,%s,
                        'EXIT','EXIT_FILLED',
                        %s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,
                        %s::jsonb,%s
                    )
                    ON CONFLICT (local_order_id) DO NOTHING
                    RETURNING *
                    """,
                    (
                        client_id,
                        local_order_id,
                        broker_order_id,
                        position_id,
                        symbol or contract,
                        contract,
                        direction,
                        filled_qty,
                        filled_qty,
                        fill_price,
                        created_at or filled_at,
                        datetime.now(timezone.utc),
                        created_at,
                        filled_at,
                        meta,
                        execution_mode,
                    ),
                )
                inserted = cursor.fetchone()
                if inserted:
                    if not _row_matches_expected(
                        dict(inserted),
                        client_id=client_id,
                        position=position,
                        fill=fill,
                        execution_mode=execution_mode,
                    ):
                        raise RuntimeError("external_exit_adoption_inserted_row_mismatch")
                    continue

                # ON CONFLICT DO NOTHING: another actor beat us. Reload
                # and validate identity.
                cursor.execute(
                    """
                    SELECT *
                    FROM orders
                    WHERE local_order_id=%s
                    LIMIT 2
                    """,
                    (local_order_id,),
                )
                collided = [dict(row) for row in (cursor.fetchall() or [])]
                if len(collided) != 1:
                    raise RuntimeError("external_exit_adoption_local_id_unresolved")
                if not _row_matches_expected(
                    collided[0],
                    client_id=client_id,
                    position=position,
                    fill=fill,
                    execution_mode=execution_mode,
                ):
                    raise RuntimeError("external_exit_adoption_existing_local_id_mismatch")
            return True, "external_exit_adoption_complete"

    try:
        return run_with_retry(_adopt_all)
    except Exception as exc:
        log.error(
            "[%s] MANUAL_CLOSE_EXIT_ADOPTION_FAILED pos=%s contract=%s "
            "fill_count=%s err=%s",
            client_id,
            position_id,
            contract,
            len(fills),
            exc,
        )
        return False, f"external_exit_adoption_error:{exc}"




def _finalize_position(
    *,
    finalizer,
    client_id: str,
    position_id: str,
    contract: str,
    evidence: dict,
) -> bool:
    # Canonical idempotency is handled inside APPositionManager
    # .close_position_from_exit_fill() under its SELECT ... FOR UPDATE row
    # lock. A detached pre-read (with no lock) cannot prevent a race and is
    # therefore removed. The finalizer returns True for already-terminal
    # positions and False for missing positions or DB errors, which is the
    # correct tri-state contract: terminal→evict, unknown/error→retain.
    broker_ids = ",".join(evidence["broker_order_ids"])
    exit_reason = (
        "MANUAL_CLIENT_CLOSE_BROKER_CONFIRMED "
        f"broker_order_ids={broker_ids}"
    )
    try:
        return bool(
            finalizer(
                position_id=position_id,
                exit_price=float(evidence["fill_price"]),
                filled_qty=int(evidence["filled_qty"]),
                filled_ts=str(evidence["filled_ts"]),
                broker_order_id=str(evidence["broker_order_id"]),
                external_close=True,
                close_source="manual_client_close_broker_fill",
                close_confidence="HIGH",
                exit_reason=exit_reason,
            )
        )
    except Exception as exc:
        log.critical(
            "[%s] MANUAL_CLOSE_FINALIZE_EXCEPTION pos=%s contract=%s "
            "broker_order_ids=%s err=%s",
            client_id,
            position_id,
            contract,
            broker_ids,
            exc,
        )
        return False



def _evict_exit_engine(
    runner: Any,
    *,
    client_id: str,
    position_id: str,
    contract: str,
) -> None:
    """Evict a finalized position from the in-memory exit engine."""
    core = getattr(runner, "core", None)
    exit_engine = getattr(core, "exit_eng", None) if core is not None else None
    mark_closed = getattr(exit_engine, "mark_position_closed", None)
    if callable(mark_closed):
        try:
            mark_closed(position_id)
        except Exception as exc:
            log.warning(
                "[%s] MANUAL_CLOSE_EXIT_ENGINE_EVICT_FAILED pos=%s contract=%s err=%s",
                client_id, position_id, contract, exc,
            )


def _recover_manual_close_downstream_truth(
    *,
    client_id: str,
    position_id: str,
    broker_exit_order_id: str,
    execution_mode: str,
) -> bool:
    """No-op until the downstream-truth guard is installed.

    The mandatory lifecycle guard replaces this hook at startup. Keeping the
    default inert preserves the reconciler's fail-closed behavior on branches
    where that optional guard is not present.
    """
    return False


def detect_manual_closes(self) -> None:
    """Finalize externally closed positions from exact broker fill truth.

    Throttled to MANUAL_CLOSE_INTERVAL_SEC. Two passes per tick:

      PASS 1 — Durable recovery (no broker call required):
        For each active position whose adopted EXIT rows already cover
        the full required quantity, re-validate the durable fills and call
        the canonical finalizer directly. This handles cross-session
        restarts (broker no longer returns previous-session orders) and
        the case where PASS 2 previously adopted fills but finalization
        failed. Runs even if the current-session broker order endpoint
        is unavailable.

      PASS 2 — New external fill discovery:
        Fetch authoritative broker positions to find contracts no longer
        held by the broker. For each missing position not yet resolved by
        PASS 1, fetch current-session broker orders, select exact external
        close evidence, adopt atomically, and finalize. Broker orders are
        fetched only AFTER durable-covered positions are handled so a
        transient order-endpoint failure cannot block already-evidenced
        recovery.
    """
    now_epoch = time.time()
    last_check = float(getattr(self, "_last_manual_close_check_ts", 0.0) or 0.0)
    if now_epoch - last_check < MANUAL_CLOSE_INTERVAL_SEC:
        return
    self._last_manual_close_check_ts = now_epoch

    client_id = str(getattr(self, "email", "") or "").strip().lower()
    runner_mode = str(getattr(self, "mode", "") or "").strip().lower()
    broker = getattr(self, "broker", None)

    if not client_id or runner_mode not in VALID_EXECUTION_MODES:
        log.error(
            "MANUAL_CLOSE_SCAN_SKIPPED invalid runner identity client=%s mode=%s",
            client_id,
            runner_mode,
        )
        return
    # PR #386 blocker fix: broker unavailability MUST NOT skip PASS 0
    # (proof-only recovery for terminal-without-proof) or the durable-only
    # arm of PASS 1. Only PASS 2 (broker discovery) requires broker access.
    # The broker=None gate is enforced below, immediately before PASS 2.

    # Always load durable DB state first — safe regardless of broker availability.
    try:
        active_positions, bot_exit_ids, adopted_fills_by_pos = load_manual_close_state(
            client_id, runner_mode
        )
    except Exception as exc:
        log.error(
            "[%s] MANUAL_CLOSE_SCAN_DB_READ_FAILED mode=%s err=%s",
            client_id,
            runner_mode,
            exc,
        )
        return

    pm_for_recovery = getattr(self, "position_manager", None)
    recovery_method = getattr(pm_for_recovery, "repair_terminal_proof_from_persisted", None)

    # detected_at is shared by PASS 0 durable-fill validation and the
    # PASS 1/2 fence machinery below.
    detected_at = datetime.fromtimestamp(now_epoch, tz=timezone.utc)

    # ── PASS 0: proof/queue recovery for terminal external-close rows ─────
    # A crash after position commit but before proof persist, or after proof
    # binding but before downstream queue cleanup, leaves a terminal row
    # (qty_remaining <= 0) with durable EXIT evidence. This pass repairs the
    # proof through the canonical function and replays the downstream truth
    # hook using ONLY persisted position/fill economics; it never calls the
    # broker, never reopens the position, and evicts the exit engine only
    # after proof binding is proven.
    if callable(recovery_method):
        try:
            recovery_candidates = load_terminal_recovery_candidates(
                client_id, runner_mode,
            )
        except Exception as exc:
            recovery_candidates = []
            log.error(
                "[%s] MANUAL_CLOSE_TERMINAL_RECOVERY_LOAD_ERROR mode=%s err=%s",
                client_id, runner_mode, exc,
            )
        for candidate in recovery_candidates:
            candidate_id = str(candidate.get("id") or "").strip()
            candidate_mode = str(candidate.get("execution_mode") or "").strip().lower()
            if not candidate_id or candidate_mode != runner_mode:
                continue
            # PR #386 blocker 2: PASS 0 must not authorize proof creation
            # on EXISTS alone. Load the adopted EXIT rows and validate
            # them through the same _validate_durable_fills policy used
            # by PASS 1. Only a fully-validated aggregate that agrees
            # with persisted position economics may pass.
            durable_for_candidate = adopted_fills_by_pos.get(candidate_id, [])
            if not durable_for_candidate:
                log.warning(
                    "[%s] MANUAL_CLOSE_TERMINAL_RECOVERY_NO_DURABLE_FILLS "
                    "pos=%s — deferring",
                    client_id, candidate_id,
                )
                continue
            valid_durable = _validate_durable_fills(
                durable_for_candidate,
                position=candidate,
                detected_at=detected_at,
                client_id=client_id,
            )
            if not valid_durable:
                log.warning(
                    "[%s] MANUAL_CLOSE_TERMINAL_RECOVERY_DURABLE_REJECTED "
                    "pos=%s — no valid EXIT evidence; deferring",
                    client_id, candidate_id,
                )
                continue
            adopted_qty = sum(int(f["filled_qty"]) for f in valid_durable)
            required_qty = positive_int(candidate.get("qty"))
            raw_contracts_exited = candidate.get("contracts_exited")
            if raw_contracts_exited in (None, ""):
                contracts_exited = 0
            else:
                contracts_exited = nonnegative_int(raw_contracts_exited)
            expected_external_qty = (
                required_qty - contracts_exited
                if contracts_exited is not None
                else 0
            )
            if (
                required_qty <= 0
                or contracts_exited is None
                or contracts_exited > required_qty
                or expected_external_qty <= 0
                or adopted_qty != expected_external_qty
            ):
                log.warning(
                    "[%s] MANUAL_CLOSE_TERMINAL_RECOVERY_QTY_MISMATCH "
                    "pos=%s total=%s prior_exited=%s expected_external=%s "
                    "adopted=%s — deferring",
                    client_id, candidate_id, required_qty, contracts_exited,
                    expected_external_qty, adopted_qty,
                )
                continue
            # A full external close can prove the persisted aggregate from
            # the external rows themselves. For a mixed bot-partial/manual
            # close, the external rows prove only the residual; the recovery
            # method must revalidate the already-terminal persisted truth and
            # must not be handed an external-only aggregate as if it covered
            # the original position quantity.
            durable_evidence = None
            if adopted_qty == required_qty:
                weighted_notional = sum(
                    float(f["fill_price"]) * int(f["filled_qty"])
                    for f in valid_durable
                )
                avg_price = weighted_notional / adopted_qty
                durable_evidence = {
                    "filled_qty": adopted_qty,
                    "fill_price": round(avg_price, 6),
                }
            try:
                ok, reason = recovery_method(
                    candidate_id,
                    expected_execution_mode=runner_mode,
                    durable_exit_evidence=durable_evidence,
                )
            except TypeError:
                # Older signature — fall back but retain fail-closed logging.
                try:
                    ok, reason = recovery_method(candidate_id)
                except Exception as exc:
                    log.error(
                        "[%s] MANUAL_CLOSE_TERMINAL_RECOVERY_ERROR pos=%s err=%s",
                        client_id, candidate_id, exc,
                    )
                    continue
            except Exception as exc:
                log.error(
                    "[%s] MANUAL_CLOSE_TERMINAL_RECOVERY_ERROR pos=%s err=%s",
                    client_id, candidate_id, exc,
                )
                continue
            if not ok:
                log.warning(
                    "[%s] MANUAL_CLOSE_TERMINAL_RECOVERY_DEFERRED pos=%s reason=%s "
                    "— proof not yet bound; next scan will retry",
                    client_id, candidate_id, reason,
                )
                continue
            log.info(
                "[%s] MANUAL_CLOSE_TERMINAL_RECOVERY_BOUND pos=%s reason=%s",
                client_id, candidate_id, reason,
            )
            latest_durable = max(
                valid_durable,
                key=lambda fill: fill["filled_at"],
            )
            broker_exit_order_id = str(
                latest_durable.get("broker_order_id") or ""
            ).strip()
            downstream_bound = False
            if broker_exit_order_id:
                try:
                    downstream_bound = (
                        _recover_manual_close_downstream_truth(
                            client_id=client_id,
                            position_id=candidate_id,
                            broker_exit_order_id=broker_exit_order_id,
                            execution_mode=runner_mode,
                        )
                        is True
                    )
                except Exception as exc:
                    log.error(
                        "[%s] MANUAL_CLOSE_TERMINAL_RECOVERY_DOWNSTREAM_ERROR "
                        "pos=%s err=%s",
                        client_id,
                        candidate_id,
                        exc,
                    )
            if not downstream_bound:
                log.warning(
                    "[%s] MANUAL_CLOSE_TERMINAL_RECOVERY_DOWNSTREAM_DEFERRED "
                    "pos=%s — proof not bound; retaining exit-engine ownership",
                    client_id,
                    candidate_id,
                )
                continue
            _core = getattr(self, "core", None)
            _exit_eng = getattr(_core, "exit_eng", None) if _core is not None else None
            _mark = getattr(_exit_eng, "mark_position_closed", None)
            if callable(_mark):
                try:
                    _mark(candidate_id)
                except Exception as exc:
                    log.warning(
                        "[%s] MANUAL_CLOSE_TERMINAL_RECOVERY_EVICT_FAILED "
                        "pos=%s err=%s",
                        client_id, candidate_id, exc,
                    )

    if not active_positions:
        return

    pm = getattr(self, "position_manager", None)
    finalizer = getattr(pm, "close_position_from_exit_fill", None)
    if not callable(finalizer):
        log.critical(
            "[%s] MANUAL_CLOSE_CANONICAL_FINALIZER_UNAVAILABLE active=%s",
            client_id,
            len(active_positions),
        )
        return

    def _check_fences(position: dict, position_id: str, contract: str) -> bool:
        """Return True if the position passes all ownership fences."""
        position_mode = str(position.get("execution_mode") or "").strip().lower()
        if position_mode not in VALID_EXECUTION_MODES or position_mode != runner_mode:
            log.critical(
                "[%s] MANUAL_CLOSE_MODE_FENCE pos=%s contract=%s "
                "runner_mode=%s position_mode=%s — no mutation",
                client_id, position_id, contract,
                runner_mode, position_mode or "missing",
            )
            return False
        if (
            bool(position.get("exit_in_flight"))
            or str(position.get("pending_exit_broker_order_id") or "").strip()
            or str(position.get("pending_exit_local_order_id") or "").strip()
        ):
            log.info(
                "[%s] MANUAL_CLOSE_SKIPPED_EXISTING_EXIT_OWNER pos=%s contract=%s",
                client_id, position_id, contract,
            )
            return False
        return True

    # ─── PASS 1: Durable recovery — finalize from DB evidence alone ───────────
    # Positions whose adopted EXIT rows already cover the full required quantity
    # are finalized here without any broker call. This handles:
    #   a) Cross-session restarts (broker no longer returns previous-session orders)
    #   b) Positions where adoption succeeded but finalization failed last scan
    #   c) Cases where the current-session broker order endpoint is unavailable
    # ──────────────────────────────────────────────────────────────────────────
    needs_broker_scan: list[dict] = []

    for position in active_positions:
        position_id = str(position.get("id") or "").strip()
        contract = normalize_contract(position.get("contract"))
        if not position_id or not is_valid_occ_contract(contract):
            continue
        if not _check_fences(position, position_id, contract):
            continue

        required_qty = positive_int(
            position.get("quantity_remaining")
            if position.get("quantity_remaining") is not None
            else position.get("qty")
        )
        if required_qty <= 0:
            continue

        durable_fills = adopted_fills_by_pos.get(position_id, [])
        if not durable_fills:
            needs_broker_scan.append(position)
            continue

        valid_durable = _validate_durable_fills(
            durable_fills,
            position=position,
            detected_at=detected_at,
            client_id=client_id,
        )
        if not valid_durable:
            needs_broker_scan.append(position)
            continue

        adopted_qty = sum(int(f["filled_qty"]) for f in valid_durable)
        if adopted_qty != required_qty:
            # Partial durable coverage — broker scan may supply remaining fills.
            needs_broker_scan.append(position)
            continue

        # Full durable coverage. Build aggregate and finalize without broker.
        all_fills = sorted(valid_durable, key=lambda r: r["filled_at"])
        weighted_notional = sum(
            float(f["fill_price"]) * int(f["filled_qty"]) for f in all_fills
        )
        avg_price = weighted_notional / adopted_qty

        durable_evidence: dict = {
            "fills": [],
            "adopted_fills": all_fills,
            "all_fills": all_fills,
            "filled_qty": adopted_qty,
            "fill_price": round(avg_price, 6),
            "filled_ts": all_fills[-1]["filled_at"].isoformat(),
            "broker_order_id": all_fills[-1]["broker_order_id"],
            "broker_order_ids": [f["broker_order_id"] for f in all_fills],
        }

        finalized = _finalize_position(
            finalizer=finalizer,
            client_id=client_id,
            position_id=position_id,
            contract=contract,
            evidence=durable_evidence,
        )
        if finalized:
            _evict_exit_engine(
                self, client_id=client_id, position_id=position_id, contract=contract
            )
            log.info(
                "[%s] MANUAL_CLOSE_PASS1_FINALIZED pos=%s contract=%s "
                "exit=%.4f qty=%s broker_order_ids=%s "
                "proof_path=canonical taxonomy_source=durable_exit_orders",
                client_id, position_id, contract,
                avg_price, adopted_qty,
                ",".join(durable_evidence["broker_order_ids"]),
            )
        else:
            log.critical(
                "[%s] MANUAL_CLOSE_PASS1_FINALIZE_FAILED pos=%s contract=%s "
                "— durable EXIT rows retained; next scan will retry",
                client_id, position_id, contract,
            )

    if not needs_broker_scan:
        return

    # ─── PASS 2: New external fill discovery via broker ────────────────────────
    # Broker calls happen here — AFTER durable-covered positions are handled.
    # A transient broker-positions or broker-orders failure does not block
    # positions that already have complete durable evidence.
    # ──────────────────────────────────────────────────────────────────────────
    if broker is None:
        # Only PASS 2 requires broker access; upstream passes have already run.
        return
    try:
        broker_position_snapshot = fetch_authoritative_broker_positions(broker)
    except Exception as exc:
        log.error(
            "[%s] MANUAL_CLOSE_SCAN_UNCERTAIN broker_positions_unavailable "
            "mode=%s err=%s — no state mutation",
            client_id, runner_mode, exc,
        )
        return

    broker_contract_qty, snapshot_reason = _broker_position_contract_quantities(
        broker_position_snapshot
    )
    if broker_contract_qty is None:
        log.error(
            "[%s] MANUAL_CLOSE_SCAN_UNCERTAIN broker_positions_state=%s "
            "reason=%s mode=%s — no missing-position inference or state mutation",
            client_id,
            getattr(broker_position_snapshot, "state", POSITIONS_MALFORMED),
            getattr(broker_position_snapshot, "reason", "") or snapshot_reason,
            runner_mode,
        )
        return

    missing_positions = [
        position
        for position in needs_broker_scan
        if normalize_contract(position.get("contract")) not in broker_contract_qty
    ]
    if not missing_positions:
        return

    # Fetch current-session orders only now — only for positions that still
    # need new external fill discovery (not for durable-covered ones).
    try:
        orders_state, broker_orders = fetch_all_current_session_orders(broker)
    except Exception as exc:  # defensive boundary for custom broker adapters
        orders_state, broker_orders = ORDERS_UNAVAILABLE, []
        log.error(
            "[%s] MANUAL_CLOSE_SCAN_UNCERTAIN broker_orders_failed "
            "missing_positions=%s err=%s — no state mutation",
            client_id, len(missing_positions), exc,
        )
    if orders_state not in {ORDERS_AVAILABLE_COMPLETE, ORDERS_AVAILABLE_EMPTY}:
        log.error(
            "[%s] MANUAL_CLOSE_SCAN_UNCERTAIN broker_orders_state=%s "
            "missing_positions=%s — no state mutation",
            client_id, orders_state, len(missing_positions),
        )
        return

    for position in missing_positions:
        position_id = str(position.get("id") or "").strip()
        contract = normalize_contract(position.get("contract"))
        if not position_id or not is_valid_occ_contract(contract):
            continue
        if not _check_fences(position, position_id, contract):
            continue

        adopted_fills_for_pos = adopted_fills_by_pos.get(position_id, [])
        evidence, reason_code = select_external_close_fills(
            orders=broker_orders,
            position=position,
            bot_exit_order_ids=bot_exit_ids,
            adopted_fills=adopted_fills_for_pos,
            detected_at=detected_at,
        )
        if evidence is None:
            log.warning(
                "[%s] MANUAL_CLOSE_EVIDENCE_MISSING pos=%s contract=%s "
                "reason=%s — position remains active",
                client_id, position_id, contract, reason_code,
            )
            continue

        adopted, adoption_reason = adopt_external_exit_fills(
            client_id=client_id,
            execution_mode=runner_mode,
            position=position,
            evidence=evidence,
        )
        if not adopted:
            log.critical(
                "[%s] MANUAL_CLOSE_ADOPTION_FAILED pos=%s contract=%s reason=%s "
                "— position remains active; next scan will retry",
                client_id, position_id, contract, adoption_reason,
            )
            continue

        finalized = _finalize_position(
            finalizer=finalizer,
            client_id=client_id,
            position_id=position_id,
            contract=contract,
            evidence=evidence,
        )
        if not finalized:
            log.critical(
                "[%s] MANUAL_CLOSE_FINALIZE_FAILED pos=%s contract=%s "
                "broker_order_ids=%s — durable EXIT rows retained; next "
                "scan will re-invoke finalizer with weighted aggregate",
                client_id, position_id, contract,
                ",".join(evidence["broker_order_ids"]),
            )
            continue

        _evict_exit_engine(
            self, client_id=client_id, position_id=position_id, contract=contract
        )
        log.info(
            "[%s] MANUAL_CLOSE_FINALIZED pos=%s contract=%s exit=%.4f "
            "qty=%s broker_order_ids=%s "
            "proof_path=canonical taxonomy_source=durable_exit_orders",
            client_id, position_id, contract,
            float(evidence["fill_price"]),
            int(evidence["filled_qty"]),
            ",".join(evidence["broker_order_ids"]),
        )
