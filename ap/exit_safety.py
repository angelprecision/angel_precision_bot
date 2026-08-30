from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Optional

from ap.broker_submit_identity import canonical_broker_submit_key
from ap.db import conn, run_with_retry
from ap.notify import post_discord
from ap.utils import now_utc_iso

log = logging.getLogger("ap.exit_safety")

_SCHEMA_CACHE_LOCK = threading.Lock()
_SCHEMA_CACHE: dict[str, set[str]] = {}
_ALERT_CACHE_LOCK = threading.Lock()
_ALERT_CACHE: dict[tuple[str, str, str, str, str], float] = {}

_DEFAULT_LOOKBACK_MINUTES = 60
_DEFAULT_THRESHOLD = 5
_ALERT_COOLDOWN_SECONDS = 900

_TERMINAL_CLOSE_SOURCES = {
    "reconciler_auto_close",
    "operator_manual_close",
    "expired",
    "expiry",
    "manual_close",
    "broker_position_missing",
}

_BROKER_REJECT_LAST_ERROR_PATTERNS = (
    "%broker_rejected_exit%",
    "%broker_http_4%",
    "%broker_status:rejected%",
    "%permanent_broker_reject%",
)


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_mode(value: Any) -> Optional[str]:
    normalized = _normalize_text(value)
    return normalized or None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except Exception:
        return None


def _normalize_contract(value: Any) -> str:
    return str(value or "").strip().upper().replace(" ", "")


def _extract_broker_account_id(broker: Any) -> str:
    return _normalize_text(
        getattr(broker, "account_id", None)
        or getattr(getattr(broker, "cfg", None), "account_id", None)
        or getattr(broker, "_account_id", None)
        or ""
    )


def _declared_broker_capability(broker: Any, name: str):
    """Return a broker method explicitly supplied by the adapter.

    ``getattr`` alone is unsafe with permissive test doubles and proxy
    objects that manufacture arbitrary attributes. Strict money-path
    capabilities must be present on the adapter type or explicitly attached
    to the instance; otherwise callers must classify the snapshot as
    unavailable rather than treating a fabricated callable as authority.
    """
    method = getattr(type(broker), name, None)
    if callable(method):
        return getattr(broker, name, None)
    instance_attrs = getattr(broker, "__dict__", {})
    if isinstance(instance_attrs, dict):
        method = instance_attrs.get(name)
        if callable(method):
            return method
    return None


def _extract_position_account_id(raw: dict[str, Any]) -> str:
    nested = raw.get("raw") if isinstance(raw.get("raw"), dict) else {}
    for key in ("account_id", "account", "account_number"):
        value = raw.get(key)
        if value not in (None, ""):
            return _normalize_text(value)
        value = nested.get(key)
        if value not in (None, ""):
            return _normalize_text(value)
    return ""


def _extract_position_contract(raw: dict[str, Any]) -> str:
    nested = raw.get("raw") if isinstance(raw.get("raw"), dict) else {}
    for key in ("contract", "option_symbol", "symbol", "instrument"):
        value = raw.get(key)
        if value not in (None, ""):
            return _normalize_contract(value)
        value = nested.get(key)
        if value not in (None, ""):
            return _normalize_contract(value)
    return ""


def _extract_long_position_qty(raw: dict[str, Any]) -> int:
    nested = raw.get("raw") if isinstance(raw.get("raw"), dict) else {}
    qty = None
    for key in ("quantity", "qty", "quantity_remaining", "remaining_quantity"):
        qty = _safe_int(raw.get(key))
        if qty is None:
            qty = _safe_int(nested.get(key))
        if qty is not None:
            break
    side_text = " ".join(
        str(v or "")
        for v in (
            raw.get("side"),
            raw.get("position_type"),
            raw.get("direction"),
            nested.get("side"),
            nested.get("position_type"),
            nested.get("direction"),
        )
    ).strip().lower()
    if qty is None:
        return 0
    if qty < 0:
        return 0
    if "short" in side_text:
        return 0
    return int(qty)


def _strict_position_quantity(raw: dict[str, Any]) -> Optional[int]:
    """Return one proven nonnegative integral position quantity, or ``None``."""
    nested = raw.get("raw") if isinstance(raw.get("raw"), dict) else {}
    observed: list[int] = []
    for container in (raw, nested):
        for key in ("quantity", "qty", "quantity_remaining", "remaining_quantity"):
            if key not in container:
                continue
            value = container.get(key)
            if isinstance(value, bool) or value in (None, ""):
                return None
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                return None
            if not math.isfinite(parsed) or not parsed.is_integer() or parsed < 0:
                return None
            observed.append(int(parsed))
    if not observed or len(set(observed)) != 1:
        return None
    side_text = " ".join(
        str(v or "")
        for v in (
            raw.get("side"),
            raw.get("position_type"),
            raw.get("direction"),
            nested.get("side"),
            nested.get("position_type"),
            nested.get("direction"),
        )
    ).strip().lower()
    if "short" in side_text:
        return None
    return observed[0]


def resolve_exit_broker_truth(
    *,
    broker: Any,
    client_id: str,
    contract: str,
) -> dict[str, Any]:
    checked_at = now_utc_iso()
    normalized_contract = _normalize_contract(contract)
    account_id = _extract_broker_account_id(broker)
    audit = {
        "source": "broker.list_positions",
        "checked_at": checked_at,
        "client_id": str(client_id or "").strip().lower(),
        "account": account_id,
        "contract": str(contract or ""),
        "normalized_contract": normalized_contract,
        "exact_contract_match": False,
    }

    # The production Tradier adapter exposes a strict capability so a request
    # failure cannot be collapsed into its legacy ``list_positions() == []``
    # compatibility result. Test doubles and older adapters retain fallback.
    # Resolve only a capability declared by the concrete adapter (or explicitly
    # attached to its instance); ``MagicMock`` creates arbitrary callable
    # attributes on demand, which must not masquerade as strict broker truth.
    strict_method = _declared_broker_capability(broker, "list_positions_strict")
    is_strict = callable(strict_method)
    list_positions = strict_method if is_strict else getattr(broker, "list_positions", None)
    audit["source"] = "broker.list_positions_strict" if is_strict else "broker.list_positions"
    if not callable(list_positions):
        audit["snapshot_status"] = "broker_positions_unavailable"
        audit["error"] = "broker_list_positions_missing"
        return {
            "broker_truth_open_qty": None,
            "is_fresh_exact": False,
            "audit": audit,
        }

    try:
        rows = list_positions()
    except Exception as exc:
        audit["snapshot_status"] = "broker_positions_error"
        audit["error"] = str(exc)
        return {
            "broker_truth_open_qty": None,
            "is_fresh_exact": False,
            "audit": audit,
        }

    if rows is None:
        # The strict adapter contract never uses ``None`` for a successful
        # empty account snapshot.  Treat it as an evidence gap so recovery
        # cannot mistake a malformed response for broker-flat truth.
        if is_strict:
            audit["snapshot_status"] = "broker_positions_malformed"
            audit["error"] = "strict_snapshot_none"
            return {
                "broker_truth_open_qty": None,
                "is_fresh_exact": False,
                "audit": audit,
            }
        rows = []
    if isinstance(rows, dict) and not is_strict:
        rows = [rows]
    if not isinstance(rows, list) or (is_strict and any(not isinstance(row, dict) for row in rows)):
        audit["snapshot_status"] = "broker_positions_malformed"
        audit["error"] = f"unexpected_payload:{type(rows).__name__}"
        return {
            "broker_truth_open_qty": None,
            "is_fresh_exact": False,
            "audit": audit,
        }

    matched_rows: list[dict[str, Any]] = []
    broker_truth_open_qty = 0
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        row_contract = _extract_position_contract(raw)
        if not row_contract or row_contract != normalized_contract:
            continue
        row_account = _extract_position_account_id(raw)
        if row_account and account_id and row_account != account_id:
            continue
        proven_qty = _strict_position_quantity(raw)
        if proven_qty is None:
            audit.update(
                {
                    "snapshot_status": "broker_positions_malformed",
                    "exact_contract_match": True,
                    "error": "exact_contract_quantity_unproven",
                }
            )
            return {
                "broker_truth_open_qty": None,
                "is_fresh_exact": False,
                "audit": audit,
            }
        long_qty = _extract_long_position_qty(raw)
        if long_qty != proven_qty:
            audit.update(
                {
                    "snapshot_status": "broker_positions_malformed",
                    "exact_contract_match": True,
                    "error": "exact_contract_direction_unproven",
                }
            )
            return {
                "broker_truth_open_qty": None,
                "is_fresh_exact": False,
                "audit": audit,
            }
        broker_truth_open_qty += max(int(long_qty), 0)
        matched_rows.append(
            {
                "contract": row_contract,
                "account": row_account or account_id,
                "long_qty": int(long_qty),
            }
        )

    if not matched_rows:
        # Production-shape fix: list_positions() succeeded and returned a valid list
        # but this OCC contract is absent. In real broker shape, a flat/closed option
        # simply disappears from the positions snapshot — it does NOT appear as a row
        # with qty=0. "Contract absent from fresh snapshot" = "broker confirms qty=0".
        #
        # This is the VZ/META production failure class: a stale synthetic local
        # position that the broker had already closed was flowing through as
        # "no broker truth" because the missing row was treated as unknown.
        # Treat absent-from-snapshot as exact flat only when list_positions() itself
        # succeeded and returned a parseable list (including empty list).
        # list_positions() error / malformed payload / missing method → None (no change).
        audit["snapshot_status"]      = "contract_absent_open_qty_zero"
        audit["exact_contract_match"] = False
        audit["broker_position_count"] = len(rows)
        return {
            "broker_truth_open_qty": 0,
            "is_fresh_exact": True,
            "audit": audit,
        }

    audit.update(
        {
            "snapshot_status": "exact_match",
            "exact_contract_match": True,
            "matched_row_count": len(matched_rows),
            "matched_rows": matched_rows,
            "broker_truth_open_qty": broker_truth_open_qty,
        }
    )
    return {
        "broker_truth_open_qty": int(broker_truth_open_qty),
        "is_fresh_exact": True,
        "audit": audit,
    }


_ACTIVE_BROKER_SELL_STATUSES = {
    "accepted", "ack", "new", "open", "pending", "pending_cancel",
    "partially_filled", "partial_fill", "submitted", "working",
}
_TERMINAL_BROKER_ORDER_STATUSES = {
    "canceled", "cancelled", "expired", "filled", "rejected",
}
_PROTECTIVE_ORDER_TYPES = {"stop", "stop_limit", "stop-limit", "stoplimit"}
_BROKER_EXECUTION_TIMESTAMP_KEYS = (
    "last_fill_date",
    "filled_at",
    "filled_ts",
    "fill_ts",
    "transaction_date",
)


def _exact_order_contract(raw: dict[str, Any]) -> str:
    for key in ("option_symbol", "contract", "symbol"):
        value = raw.get(key)
        if value not in (None, ""):
            normalized = _normalize_contract(value)
            if normalized:
                return normalized
    return ""


def _canonical_order_status(value: Any) -> str:
    return _normalize_text(value).replace("-", "_")


def _order_status(raw: dict[str, Any]) -> tuple[str | None, str | None]:
    status = _canonical_order_status(raw.get("status"))
    state = _canonical_order_status(raw.get("state"))
    if status and state and status != state:
        return None, "conflicting_status"
    normalized = status or state
    if not normalized:
        return None, "unknown_status"
    return normalized, None


def _order_id_evidence(raw: dict[str, Any]) -> tuple[str | None, str | None]:
    """Return one concrete broker ID and reject conflicting aliases."""
    observed_ids: list[str] = []
    for key in ("id", "order_id"):
        value = raw.get(key)
        if value in (None, ""):
            continue
        if isinstance(value, bool):
            return None, "order_id_unproven"
        normalized = str(value).strip()
        if not normalized:
            continue
        observed_ids.append(normalized)
    if not observed_ids:
        return None, "order_id_unproven"
    if len(set(observed_ids)) != 1:
        return None, "order_id_conflict"
    if observed_ids[0].upper() in {"?", "N/A", "UNKNOWN", "NULL", "NONE", "0"}:
        return None, "order_id_unproven"
    return observed_ids[0], None


def _order_type_evidence(
    raw: dict[str, Any], *, required: bool = True,
) -> tuple[str | None, str | None]:
    """Return one canonical order type and reject conflicting aliases."""
    type_values = [
        _normalize_text(raw[key]).replace(" ", "_")
        for key in ("type", "order_type")
        if raw.get(key) not in (None, "")
    ]
    if not type_values:
        return (None, "order_type_unproven") if required else (None, None)
    if len(set(type_values)) != 1:
        return None, "order_type_conflict"
    return type_values[0], None


def _order_class_issue(raw: dict[str, Any], *, required: bool = True) -> str | None:
    """Require one unambiguous option-order class for an exact OCC row."""
    class_values = [
        _normalize_text(raw[key]).replace(" ", "_")
        for key in ("class", "order_class")
        if raw.get(key) not in (None, "")
    ]
    if not class_values:
        return "order_class_unproven" if required else None
    if len(set(class_values)) != 1:
        return "order_class_conflict"
    if class_values[0] != "option":
        return "order_class_mismatch"
    return None


def _order_account_issue(raw: dict[str, Any], account: str) -> str | None:
    """Reject contradictory or mismatched account aliases when supplied."""
    account_values = [
        _normalize_text(raw[key])
        for key in ("account_id", "account", "account_number")
        if raw.get(key) not in (None, "")
    ]
    if len(set(account_values)) > 1:
        return "order_account_conflict"
    if account and account_values and account_values[0] != account:
        return "order_account_mismatch"
    return None


def _strict_nonnegative_order_int(value: Any) -> Optional[int]:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or not parsed.is_integer() or parsed < 0:
        return None
    return int(parsed)


def _strict_positive_order_int(value: Any) -> Optional[int]:
    parsed = _strict_nonnegative_order_int(value)
    return parsed if parsed is not None and parsed > 0 else None


def _order_quantity_evidence(
    raw: dict[str, Any], *, fallback_qty: Optional[int] = None,
) -> tuple[dict[str, Any] | None, str | None]:
    quantity_values: list[int] = []
    for key in ("quantity", "qty"):
        if key not in raw:
            continue
        parsed = _strict_positive_order_int(raw.get(key))
        if parsed is None:
            return None, "quantity_unproven"
        quantity_values.append(parsed)
    if not quantity_values:
        qty = fallback_qty
    else:
        if len(set(quantity_values)) != 1:
            return None, "quantity_conflict"
        qty = quantity_values[0]
    if qty is None:
        return None, "quantity_unproven"

    has_exec = "exec_quantity" in raw
    if has_exec:
        executed = _strict_nonnegative_order_int(raw.get("exec_quantity"))
        if executed is None:
            return None, "quantity_unproven"
    else:
        executed = 0
    if executed > qty:
        return None, "quantity_unproven"

    has_remaining = "remaining_quantity" in raw
    if has_remaining:
        remaining = _strict_nonnegative_order_int(raw.get("remaining_quantity"))
        if remaining is None or remaining > qty:
            return None, "quantity_unproven"
        if has_exec and remaining + executed != qty:
            return None, "quantity_unproven"
    else:
        remaining = qty - executed

    return {
        "qty": int(qty),
        "executed": int(executed),
        "remaining": int(remaining),
        "has_exec": has_exec,
        "has_remaining": has_remaining,
    }, None


def _parse_trusted_broker_execution_timestamp(value: Any) -> Optional[datetime]:
    """Parse an explicit broker execution timestamp, or return ``None``.

    Order creation/update timestamps are deliberately excluded.  A timestamp
    can fence a historical terminal fill only when it is timezone-aware (or an
    unambiguous numeric epoch); a naive broker value is not sufficient proof
    of which position lifecycle consumed the contracts.
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
            return None
        try:
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                return None
        except Exception:
            return None
    return parsed.astimezone(timezone.utc)


def _order_execution_timestamp(
    raw: dict[str, Any], *, status: str,
) -> tuple[Optional[datetime], str | None, str | None]:
    """Return one unambiguous broker execution timestamp for an order row.

    ``transaction_date`` is a last-update field in Tradier's response and is
    accepted only for a terminal FILLED order.  A present malformed field or
    contradictory execution aliases is an evidence gap, not a reason to fall
    back to an order creation timestamp.
    """
    candidates: list[tuple[str, datetime]] = []
    for key in _BROKER_EXECUTION_TIMESTAMP_KEYS:
        value = raw.get(key)
        if value in (None, ""):
            continue
        if key == "transaction_date" and status != "filled":
            continue
        parsed = _parse_trusted_broker_execution_timestamp(value)
        if parsed is None:
            return None, None, "execution_timestamp_unproven"
        candidates.append((key, parsed))
    if not candidates:
        return None, None, None
    first_key, first_timestamp = candidates[0]
    if any(timestamp != first_timestamp for _, timestamp in candidates[1:]):
        return None, None, "execution_timestamp_conflict"
    return first_timestamp, first_key, None


def _exact_order_snapshot_row_issue(
    raw: Any, *, target_contract: str, account: str = "",
    require_class: bool = False, require_type: bool = True,
) -> str | None:
    """Validate non-contradictory proof for an exact OCC order row."""
    if not isinstance(raw, dict):
        return "row_malformed"
    if _exact_order_contract(raw) != target_contract:
        return None

    _, id_issue = _order_id_evidence(raw)
    if id_issue:
        return id_issue
    status, status_issue = _order_status(raw)
    if status_issue:
        return status_issue
    if status not in (_ACTIVE_BROKER_SELL_STATUSES | _TERMINAL_BROKER_ORDER_STATUSES):
        return "unknown_status"
    account_issue = _order_account_issue(raw, account)
    # A row explicitly belonging to another account is unrelated inventory,
    # not ambiguous truth for this account.  The takeover inventory will skip
    # it after this account-boundary check; conflicting aliases still hold.
    if account_issue == "order_account_mismatch":
        return None
    if account_issue:
        return account_issue
    class_issue = _order_class_issue(raw, required=require_class)
    if class_issue:
        return class_issue
    if not _normalize_text(raw.get("side")):
        return "side_unproven"
    _, quantity_issue = _order_quantity_evidence(raw)
    if quantity_issue:
        return quantity_issue
    _, type_issue = _order_type_evidence(raw, required=require_type)
    if type_issue:
        return type_issue
    if "duration" in raw and not _normalize_text(raw.get("duration")):
        return "duration_unproven"
    return None


def _terminal_order_identity_issue(
    raw: Any,
    *,
    expected_broker_order_id: str,
    target_contract: str,
    account: str,
) -> str | None:
    """Require the cancel re-query to prove the exact protective order."""
    if not isinstance(raw, dict):
        return "get_order_malformed"

    observed_id, id_issue = _order_id_evidence(raw)
    if id_issue == "order_id_unproven":
        return "terminal_order_id_unproven"
    if id_issue == "order_id_conflict":
        return "terminal_order_id_conflict"
    if observed_id != str(expected_broker_order_id or "").strip():
        return "terminal_order_id_mismatch"

    explicit_contracts = []
    symbol_contract = ""
    for key in ("option_symbol", "contract", "symbol"):
        value = raw.get(key)
        if value not in (None, ""):
            normalized = _normalize_contract(value)
            if normalized:
                if key in {"option_symbol", "contract"}:
                    explicit_contracts.append(normalized)
                else:
                    symbol_contract = normalized
    if len(set(explicit_contracts)) > 1:
        return "terminal_order_contract_unproven"
    if explicit_contracts:
        if explicit_contracts[0] != target_contract:
            return "terminal_order_contract_mismatch"
        # Tradier includes the underlying ticker in ``symbol`` alongside the
        # exact OCC ``option_symbol``.  The explicit OCC alias is authoritative.
    elif symbol_contract != target_contract:
        return "terminal_order_contract_unproven"

    if _normalize_text(raw.get("side")) != "sell_to_close":
        return "terminal_order_side_mismatch"

    account_issue = _order_account_issue(raw, account)
    if account_issue == "order_account_conflict":
        return "terminal_order_account_conflict"
    if account_issue == "order_account_mismatch":
        return "terminal_order_account_mismatch"

    class_issue = _order_class_issue(raw, required=False)
    if class_issue:
        return f"terminal_{class_issue}"
    order_type, type_issue = _order_type_evidence(raw, required=False)
    if type_issue:
        return f"terminal_{type_issue}"
    if order_type and order_type not in _PROTECTIVE_ORDER_TYPES:
        return "terminal_order_type_mismatch"
    return None


def _order_snapshot_row_issue(
    raw: Any, *, target_contract: str, account: str = "",
) -> str | None:
    """Reject rows that cannot safely participate in an account order snapshot."""
    if not isinstance(raw, dict):
        return "row_malformed"
    aliases: list[tuple[str, str]] = []
    for key in ("option_symbol", "contract", "symbol"):
        value = raw.get(key)
        normalized = _normalize_contract(value) if value not in (None, "") else ""
        if normalized:
            aliases.append((key, normalized))
    if not aliases:
        return "contract_unproven"
    explicit_contracts = [
        value for key, value in aliases if key in {"option_symbol", "contract"}
    ]
    if len(set(explicit_contracts)) > 1:
        return "conflicting_contract"
    symbol_contract = next(
        (value for key, value in aliases if key == "symbol"), ""
    )
    if (
        target_contract
        and symbol_contract == target_contract
        and explicit_contracts
        and target_contract not in explicit_contracts
    ):
        return "conflicting_contract"

    # Exact-contract rows are validated by the takeover inventory so existing
    # ambiguity classifications remain specific. Rows that cannot identify a
    # contract at all are malformed at the snapshot boundary and must not
    # disappear as unrelated inventory.
    if _exact_order_contract(raw) == target_contract:
        return None

    _, id_issue = _order_id_evidence(raw)
    if id_issue:
        return id_issue
    status, status_issue = _order_status(raw)
    if status_issue:
        return status_issue
    if status not in (_ACTIVE_BROKER_SELL_STATUSES | _TERMINAL_BROKER_ORDER_STATUSES):
        return "unknown_status"
    if not _normalize_text(raw.get("side")):
        return "side_unproven"
    _, quantity_issue = _order_quantity_evidence(raw)
    if quantity_issue:
        return quantity_issue
    return None


def _order_snapshot_issue(
    rows: Any, *, target_contract: str, account: str = "",
) -> str | None:
    if not isinstance(rows, list):
        return "snapshot_malformed"
    for row in rows:
        issue = _order_snapshot_row_issue(
            row, target_contract=target_contract, account=account,
        )
        if issue:
            return issue
    return None


def _terminal_order_outcome(
    raw: dict[str, Any], candidate: dict[str, Any],
) -> tuple[str | None, int, str | None]:
    status, status_issue = _order_status(raw)
    if status_issue:
        return None, 0, status_issue
    evidence, quantity_issue = _order_quantity_evidence(
        raw, fallback_qty=int(candidate["qty"]),
    )
    if quantity_issue or evidence is None:
        return None, 0, quantity_issue or "quantity_unproven"
    if int(evidence["qty"]) != int(candidate["qty"]):
        return None, 0, "quantity_transition"

    # Tradier may report a partial fill with zero remaining as
    # ``partially_filled``.  A partial status with positive remaining is still
    # live and cannot authorize a replacement after a cancel request.
    if status in {"partially_filled", "partial_fill"}:
        if evidence["remaining"] > 0:
            return None, 0, "order_not_terminal"
        status = "filled"
    if status not in _TERMINAL_BROKER_ORDER_STATUSES:
        return None, 0, "order_not_terminal"

    # A non-filled terminal status does not prove that zero contracts were
    # consumed when the broker omits both execution and remaining quantity.
    # Treat that shape as unknown rather than authorizing a duplicate sell.
    if status != "filled" and not (
        evidence["has_exec"] or evidence["has_remaining"]
    ):
        return None, 0, "quantity_unproven"

    candidate_executed = int(candidate.get("executed") or 0)
    candidate_remaining = int(candidate["remaining"])
    if status == "filled":
        if evidence["has_exec"]:
            if evidence["executed"] <= candidate_executed:
                return None, 0, "quantity_unproven"
            consumed = max(evidence["executed"] - candidate_executed, 0)
        elif evidence["has_remaining"]:
            if evidence["remaining"] >= candidate_remaining:
                return None, 0, "quantity_unproven"
            consumed = candidate_remaining - evidence["remaining"]
        else:
            consumed = candidate_remaining
    elif evidence["has_exec"]:
        if evidence["executed"] < candidate_executed:
            return None, 0, "quantity_unproven"
        consumed = max(evidence["executed"] - candidate_executed, 0)
    elif evidence["has_remaining"]:
        if evidence["remaining"] > candidate_remaining:
            return None, 0, "quantity_unproven"
        consumed = candidate_remaining - evidence["remaining"]
    else:
        consumed = 0
    if consumed > candidate_remaining:
        return None, 0, "quantity_unproven"
    return status, int(consumed), None


def _merge_terminal_order_history(
    *snapshots: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, int], str | None]:
    """Merge terminal rows by broker ID and reject inconsistent transitions.

    A broker order can legitimately be present in both inventory snapshots.
    Its execution is cumulative, so the second observation is not a second
    fill.  The returned records and nonnegative execution deltas are used only
    for race/ambiguity detection; current broker position truth remains the
    sole replacement-size authority.
    """
    by_id: dict[str, dict[str, Any]] = {}
    execution_deltas: dict[str, int] = {}
    for snapshot in snapshots:
        for order in snapshot:
            raw = order.get("raw") if isinstance(order, dict) else None
            raw = raw if isinstance(raw, dict) else {}
            broker_order_id = str(
                order.get("broker_order_id") if isinstance(order, dict) else ""
            ).strip()
            if not broker_order_id:
                return {}, {}, "order_id_unproven"
            current = dict(order)
            previous = by_id.get(broker_order_id)
            if previous is not None:
                if current.get("status") != previous.get("status"):
                    return {}, {}, "status_transition"
                if current.get("qty") != previous.get("qty"):
                    return {}, {}, "quantity_transition"
                try:
                    exec_delta = int(current.get("executed", 0)) - int(previous.get("executed", 0))
                except (TypeError, ValueError):
                    return {}, {}, "quantity_unproven"
                if exec_delta < 0:
                    return {}, {}, "quantity_transition"
                try:
                    if int(current.get("remaining", 0)) > int(previous.get("remaining", 0)):
                        return {}, {}, "quantity_transition"
                except (TypeError, ValueError):
                    return {}, {}, "quantity_unproven"
                execution_deltas[broker_order_id] = (
                    execution_deltas.get(broker_order_id, 0) + exec_delta
                )
            else:
                execution_deltas[broker_order_id] = 0
            by_id[broker_order_id] = current
    return by_id, execution_deltas, None


def _scope_terminal_order_history(
    terminal_by_id: dict[str, dict[str, Any]],
    execution_deltas: dict[str, int],
    *,
    protective_broker_order_id: str,
    current_position_entry_ts: Optional[datetime],
) -> tuple[dict[str, dict[str, Any]], dict[str, int], dict[str, Any], str | None]:
    """Fence cumulative terminal fills to the current position lifecycle.

    A terminal sell with no execution delta is current only when its exact
    durable protective ID proves ownership or its trusted broker execution
    timestamp is at/after the canonical position entry.  A positive execution
    delta observed across this takeover attempt is current race evidence.  A
    pre-entry unchanged fill is historical noise and is excluded from the
    coherence subtraction.  Anything else remains unproven and holds closed.
    """
    current: dict[str, dict[str, Any]] = {}
    current_deltas: dict[str, int] = {}
    historical_ids: list[str] = []
    historical_consumed_qty = 0
    unproven_ids: list[str] = []

    for broker_order_id, order in terminal_by_id.items():
        try:
            consumed = max(int(order.get("consumed") or 0), 0)
            execution_delta = max(int(execution_deltas.get(broker_order_id, 0) or 0), 0)
        except (TypeError, ValueError):
            return {}, {}, {}, "quantity_unproven"
        if consumed <= 0:
            continue

        execution_timestamp = order.get("execution_timestamp")
        exact_durable_id = bool(
            protective_broker_order_id
            and broker_order_id == protective_broker_order_id
        )
        observed_during_takeover = execution_delta > 0

        # A dynamic execution increase is strong race evidence.  If the same
        # row also carries a trusted pre-entry timestamp, the evidence is
        # contradictory rather than something to silently prefer.
        if (
            current_position_entry_ts is not None
            and execution_timestamp is not None
            and execution_timestamp < current_position_entry_ts
            and (exact_durable_id or observed_during_takeover)
        ):
            return {}, {}, {}, "terminal_execution_lifecycle_conflict"

        if exact_durable_id or observed_during_takeover:
            current[broker_order_id] = order
            current_deltas[broker_order_id] = execution_delta
        elif (
            current_position_entry_ts is not None
            and execution_timestamp is not None
            and execution_timestamp >= current_position_entry_ts
        ):
            current[broker_order_id] = order
            current_deltas[broker_order_id] = execution_delta
        elif (
            current_position_entry_ts is not None
            and execution_timestamp is not None
            and execution_timestamp < current_position_entry_ts
        ):
            historical_ids.append(broker_order_id)
            historical_consumed_qty += consumed
        else:
            unproven_ids.append(broker_order_id)

    audit = {
        "terminal_current_fill_ids": sorted(current),
        "terminal_historical_fill_ids": sorted(historical_ids),
        "terminal_historical_consumed_qty": int(historical_consumed_qty),
        "terminal_unproven_fill_ids": sorted(unproven_ids),
    }
    if unproven_ids:
        return current, current_deltas, audit, "terminal_fill_lifecycle_unproven"
    return current, current_deltas, audit, None


def resolve_protective_exit_takeover(
    *,
    broker: Any,
    client_id: str,
    execution_mode: str,
    position_id: str,
    local_order_id: str,
    contract: str,
    requested_qty: int,
    protective_broker_order_id: str | None = None,
    current_position_entry_ts: Any = None,
) -> dict[str, Any]:
    """Prove broker sell ownership immediately before a LIVE EXIT POST.

    The account is supplied by the broker adapter itself; this routine never
    accepts a caller-provided account and never matches by underlying ticker.
    """
    mode = str(execution_mode or "").strip().lower()
    exact_contract = _normalize_contract(contract)
    account = _extract_broker_account_id(broker)
    parsed_position_entry_ts = _parse_entry_ts(current_position_entry_ts)
    audit: dict[str, Any] = {
        "event": "EXIT_PROTECTIVE_PREFLIGHT_START",
        "checked_at": now_utc_iso(),
        "client_id": str(client_id or ""),
        "execution_mode": mode,
        "position_id": str(position_id or ""),
        "local_order_id": str(local_order_id or ""),
        "contract": str(contract or ""),
        "requested_qty": requested_qty,
        "account": account,
        "current_position_entry_ts": (
            parsed_position_entry_ts.isoformat()
            if parsed_position_entry_ts is not None
            else None
        ),
        "current_position_entry_ts_proven": parsed_position_entry_ts is not None,
    }
    if mode != "live" or not exact_contract or not isinstance(requested_qty, int) \
            or isinstance(requested_qty, bool) or requested_qty <= 0:
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="identity_unproven")
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_IDENTITY_UNPROVEN", "audit": audit}
    if not account:
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="broker_account_unproven")
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_IDENTITY_UNPROVEN", "audit": audit}

    initial_position = resolve_exit_broker_truth(
        broker=broker, client_id=client_id, contract=contract,
    )
    initial_qty = initial_position.get("broker_truth_open_qty")
    audit["initial_position"] = initial_position.get("audit") or {}
    if initial_position.get("is_fresh_exact") is not True or not isinstance(initial_qty, int):
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="broker_position_unproven")
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_POSITION_UNPROVEN", "audit": audit}
    audit["broker_long_qty"] = initial_qty
    if initial_qty <= 0:
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="broker_already_flat")
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_BROKER_FLAT", "audit": audit}

    list_orders_strict = _declared_broker_capability(
        broker, "list_orders_strict"
    )
    if not callable(list_orders_strict):
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="list_orders_strict_unavailable")
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_ORDERS_UNAVAILABLE", "audit": audit}
    try:
        orders = list_orders_strict()
    except Exception as exc:
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="list_orders_strict_error", error=str(exc))
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_ORDERS_UNAVAILABLE", "audit": audit}
    if not isinstance(orders, list) or any(not isinstance(row, dict) for row in orders):
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="list_orders_malformed")
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_ORDERS_MALFORMED", "audit": audit}
    snapshot_issue = _order_snapshot_issue(
        orders, target_contract=exact_contract, account=account,
    )
    if snapshot_issue:
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason=snapshot_issue)
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_ORDERS_MALFORMED", "audit": audit}

    # A standing stop is GTC, while Tradier's account-order inventory is
    # session/day scoped.  When the originating ENTRY persisted an exact
    # protective broker ID, prove that order directly before treating its
    # absence from today's inventory as evidence that no protective sell is
    # live.  Missing, malformed, or identity-inconsistent proof holds closed.
    durable_order: dict[str, Any] | None = None
    durable_id = str(protective_broker_order_id or "").strip()
    if durable_id.upper() in {"", "?", "N/A", "UNKNOWN", "NULL", "NONE", "0"}:
        durable_id = ""
    if durable_id:
        audit["durable_protective_broker_order_id"] = durable_id
        get_order = getattr(broker, "get_order", None)
        if not callable(get_order):
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="durable_protective_get_order_unavailable")
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_IDENTITY_UNPROVEN", "audit": audit}
        try:
            durable_raw = get_order(durable_id)
        except Exception as exc:
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="durable_protective_get_order_error", error=str(exc))
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_IDENTITY_UNPROVEN", "audit": audit}
        if not isinstance(durable_raw, dict):
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="durable_protective_get_order_malformed")
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_IDENTITY_UNPROVEN", "audit": audit}
        durable_shape_issue = _exact_order_snapshot_row_issue(
            durable_raw, target_contract=exact_contract, account=account,
        )
        if durable_shape_issue:
            audit.update(
                event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED",
                reason=f"durable_protective_{durable_shape_issue}",
            )
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_IDENTITY_UNPROVEN", "audit": audit}
        observed_id, observed_id_issue = _order_id_evidence(durable_raw)
        if observed_id_issue:
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason=f"durable_protective_{observed_id_issue}")
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_IDENTITY_UNPROVEN", "audit": audit}
        if observed_id != durable_id:
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="durable_protective_id_mismatch")
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_IDENTITY_UNPROVEN", "audit": audit}
        if _exact_order_contract(durable_raw) != exact_contract:
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="durable_protective_contract_mismatch")
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_IDENTITY_UNPROVEN", "audit": audit}
        durable_account_issue = _order_account_issue(durable_raw, account)
        if durable_account_issue:
            audit.update(
                event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED",
                reason=f"durable_protective_{durable_account_issue}",
            )
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_IDENTITY_UNPROVEN", "audit": audit}
        durable_status, durable_status_issue = _order_status(durable_raw)
        durable_evidence, durable_quantity_issue = _order_quantity_evidence(durable_raw)
        if durable_status_issue or durable_evidence is None:
            audit.update(
                event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED",
                reason=durable_status_issue or durable_quantity_issue or "durable_protective_quantity_unproven",
            )
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_IDENTITY_UNPROVEN", "audit": audit}
        if durable_status in {"partially_filled", "partial_fill"} and durable_evidence["remaining"] == 0:
            durable_status = "filled"
        if durable_status not in (_ACTIVE_BROKER_SELL_STATUSES | _TERMINAL_BROKER_ORDER_STATUSES):
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="durable_protective_status_unproven")
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_IDENTITY_UNPROVEN", "audit": audit}
        if _normalize_text(durable_raw.get("side")) != "sell_to_close":
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="durable_protective_side_mismatch")
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_IDENTITY_UNPROVEN", "audit": audit}
        durable_type = _normalize_text(durable_raw.get("type") or durable_raw.get("order_type")).replace(" ", "_")
        if durable_type and durable_type not in _PROTECTIVE_ORDER_TYPES:
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="durable_protective_type_mismatch")
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_IDENTITY_UNPROVEN", "audit": audit}
        if durable_status in _ACTIVE_BROKER_SELL_STATUSES:
            if durable_evidence["remaining"] <= 0 or durable_type not in _PROTECTIVE_ORDER_TYPES:
                audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="durable_protective_active_proof_unproven")
                return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_IDENTITY_UNPROVEN", "audit": audit}
        durable_tag = str(durable_raw.get("tag") or "").strip()
        if durable_tag and durable_tag in {str(local_order_id or "").strip(), canonical_broker_submit_key(local_order_id)}:
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="durable_protective_is_canonical_exit")
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_IDENTITY_UNPROVEN", "audit": audit}
        durable_order = dict(durable_raw)

    inventory_orders = list(orders)
    if durable_order is not None:
        durable_matches = [
            index
            for index, row in enumerate(inventory_orders)
            if str(row.get("id") or row.get("order_id") or "").strip() == durable_id
        ]
        if len(durable_matches) > 1:
            audit.update(event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", reason="durable_protective_id_duplicated")
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", "audit": audit}
        if durable_matches:
            listed = inventory_orders[durable_matches[0]]
            listed_status, listed_status_issue = _order_status(listed)
            listed_evidence, listed_quantity_issue = _order_quantity_evidence(listed)
            if listed_status in {"partially_filled", "partial_fill"} and listed_evidence is not None and listed_evidence["remaining"] == 0:
                listed_status = "filled"
            if (
                listed_status_issue
                or listed_evidence is None
                or listed_status != durable_status
                or listed_evidence["qty"] != durable_evidence["qty"]
                or _exact_order_contract(listed) != exact_contract
                or _normalize_text(listed.get("side")) != "sell_to_close"
            ):
                audit.update(event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", reason="durable_protective_snapshot_transition")
                return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", "audit": audit}
            inventory_orders[durable_matches[0]] = durable_order
        else:
            # The exact GET proof is authoritative for a prior-session GTC
            # order that cannot appear in the current-day list snapshot.
            inventory_orders.append(durable_order)

    def _inventory(
        rows: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None]:
        active: list[dict[str, Any]] = []
        terminal_orders: list[dict[str, Any]] = []
        for row in rows:
            if _exact_order_contract(row) != exact_contract:
                continue
            row_shape_issue = _exact_order_snapshot_row_issue(
                row, target_contract=exact_contract, account=account,
            )
            if row_shape_issue:
                return [], [], row_shape_issue
            row_accounts = [
                _normalize_text(row[key])
                for key in ("account_id", "account", "account_number")
                if row.get(key) not in (None, "")
            ]
            row_account = row_accounts[0] if row_accounts else ""
            if row_account and account and row_account != account:
                continue
            broker_order_id, broker_order_id_issue = _order_id_evidence(row)
            if broker_order_id_issue or not broker_order_id:
                return [], [], broker_order_id_issue or "order_id_unproven"
            status, status_issue = _order_status(row)
            if status_issue:
                return [], [], status_issue
            evidence, quantity_issue = _order_quantity_evidence(row)
            if quantity_issue or evidence is None:
                return [], [], quantity_issue or "quantity_unproven"
            side = _normalize_text(row.get("side"))
            if not side:
                return [], [], "side_unproven"
            if side != "sell_to_close":
                continue
            order_type, order_type_issue = _order_type_evidence(row)
            if order_type_issue or not order_type:
                return [], [], order_type_issue or "order_type_unproven"
            if status in {"partially_filled", "partial_fill"} and evidence["remaining"] == 0:
                status = "filled"
            if status in _TERMINAL_BROKER_ORDER_STATUSES:
                execution_timestamp, execution_timestamp_key, execution_timestamp_issue = (
                    _order_execution_timestamp(row, status=status)
                )
                if execution_timestamp_issue:
                    return [], [], execution_timestamp_issue
                if status != "filled" and not (
                    evidence["has_exec"] or evidence["has_remaining"]
                ):
                    return [], [], "quantity_unproven"
                consumed = evidence["executed"]
                if status == "filled":
                    if evidence["has_exec"]:
                        if evidence["executed"] <= 0:
                            return [], [], "quantity_unproven"
                        consumed = evidence["executed"]
                    elif evidence["has_remaining"]:
                        if evidence["remaining"] >= evidence["qty"]:
                            return [], [], "quantity_unproven"
                        consumed = evidence["qty"] - evidence["remaining"]
                    else:
                        consumed = evidence["qty"]
                terminal_orders.append(
                    {
                        "raw": row,
                        "broker_order_id": broker_order_id,
                        "status": status,
                        "qty": evidence["qty"],
                        "executed": evidence["executed"],
                        "remaining": evidence["remaining"],
                        "consumed": int(consumed),
                        "order_type": order_type,
                        "execution_timestamp": execution_timestamp,
                        "execution_timestamp_key": execution_timestamp_key,
                    }
                )
                continue
            if status not in _ACTIVE_BROKER_SELL_STATUSES:
                return [], [], "unknown_status"
            if evidence["remaining"] <= 0:
                return [], [], "quantity_unproven"
            active.append(
                {
                    "raw": row,
                    "broker_order_id": broker_order_id,
                    "status": status,
                    "qty": evidence["qty"],
                    "executed": evidence["executed"],
                    "remaining": evidence["remaining"],
                    "order_type": order_type,
                }
            )
        return active, terminal_orders, None

    active_sells, terminal_orders, inventory_issue = _inventory(inventory_orders)
    if inventory_issue:
        audit.update(event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", reason=inventory_issue)
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", "audit": audit}

    if not active_sells:
        # Obtain the second order inventory before the final position read.
        # The position snapshot must be the last broker quantity observation so
        # a fill that moves between order snapshots cannot leave sizing based
        # on a stale quantity.
        try:
            post_orders = list_orders_strict()
        except Exception as exc:
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="post_order_list_orders_strict_error", error=str(exc))
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_ORDERS_UNAVAILABLE", "audit": audit}
        if not isinstance(post_orders, list) or any(not isinstance(row, dict) for row in post_orders):
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="post_order_list_orders_malformed")
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_ORDERS_MALFORMED", "audit": audit}
        post_snapshot_issue = _order_snapshot_issue(
            post_orders, target_contract=exact_contract, account=account,
        )
        if post_snapshot_issue:
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason=post_snapshot_issue)
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_ORDERS_MALFORMED", "audit": audit}
        post_active_sells, post_terminal_orders, post_inventory_issue = _inventory(post_orders)
        if post_inventory_issue or post_active_sells:
            audit.update(
                event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
                reason=post_inventory_issue or "active_sell_appeared_after_takeover",
                post_takeover_active_sell_count=len(post_active_sells),
            )
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", "audit": audit}
        terminal_by_id, execution_deltas, terminal_history_issue = _merge_terminal_order_history(
            terminal_orders, post_terminal_orders,
        )
        if terminal_history_issue:
            audit.update(
                event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
                reason=terminal_history_issue,
            )
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", "audit": audit}
        (
            scoped_terminal_by_id,
            scoped_execution_deltas,
            lifecycle_audit,
            lifecycle_issue,
        ) = _scope_terminal_order_history(
            terminal_by_id,
            execution_deltas,
            protective_broker_order_id=durable_id,
            current_position_entry_ts=parsed_position_entry_ts,
        )
        audit.update(lifecycle_audit)
        if lifecycle_issue:
            audit.update(
                event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED",
                reason=lifecycle_issue,
            )
            return {
                "allowed": False,
                "replacement_qty": 0,
                "reason": "EXIT_PROTECTIVE_POSITION_UNPROVEN",
                "audit": audit,
            }
        terminal_fill_ids = {
            broker_order_id
            for broker_order_id, order in scoped_terminal_by_id.items()
            if int(order.get("consumed") or 0) > 0
        }
        if len(terminal_fill_ids) > 1:
            audit.update(
                event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
                reason="multiple_terminal_fills",
                terminal_sell_count=len(terminal_fill_ids),
            )
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", "audit": audit}
        observed_execution_delta = sum(
            max(int(delta), 0) for delta in scoped_execution_deltas.values()
        )
        audit["observed_terminal_execution_delta"] = observed_execution_delta
        terminal_consumed_qty = sum(
            max(int(order.get("consumed") or 0), 0)
            for order in scoped_terminal_by_id.values()
        )
        audit["observed_terminal_execution_total"] = terminal_consumed_qty

        # This is deliberately the last broker quantity observation in the
        # no-active path.  Cumulative terminal execution is a coherence fence;
        # the final broker position remains the sole replacement-size authority.
        final_position = resolve_exit_broker_truth(
            broker=broker, client_id=client_id, contract=contract,
        )
        final_qty = final_position.get("broker_truth_open_qty")
        audit["final_position"] = final_position.get("audit") or {}
        if final_position.get("is_fresh_exact") is not True or not isinstance(final_qty, int):
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="post_order_position_unproven")
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_POSITION_UNPROVEN", "audit": audit}
        max_coherent_qty = max(
            initial_qty - max(observed_execution_delta, terminal_consumed_qty),
            0,
        )
        if final_qty > max_coherent_qty:
            audit.update(
                event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED",
                reason="position_snapshot_stale_after_order_fill",
                final_qty=final_qty,
                max_coherent_qty=max_coherent_qty,
            )
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_POSITION_UNPROVEN", "audit": audit}
        replacement_qty = min(
            requested_qty,
            max(final_qty, 0),
        )
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_ALLOWED", replacement_qty=replacement_qty)
        return {
            "allowed": replacement_qty > 0,
            "replacement_qty": replacement_qty,
            "reason": "EXIT_PROTECTIVE_NO_CONFLICT" if replacement_qty > 0 else "EXIT_PROTECTIVE_BROKER_FLAT",
            "audit": audit,
        }
    if len(active_sells) != 1:
        audit.update(event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", reason="multiple_active_sells", active_sell_count=len(active_sells))
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", "audit": audit}

    candidate = active_sells[0]
    raw = candidate["raw"]
    order_type = str(candidate.get("order_type") or "").strip()
    broker_order_id = str(candidate.get("broker_order_id") or "").strip()
    tag = str(raw.get("tag") or "").strip()
    canonical_tag = canonical_broker_submit_key(local_order_id)
    if tag and tag in {str(local_order_id or "").strip(), canonical_tag}:
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="canonical_exit_already_active", existing_broker_order_id=broker_order_id)
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_CANONICAL_BROKER_SELL_ACTIVE", "audit": audit}
    if order_type not in _PROTECTIVE_ORDER_TYPES or not broker_order_id:
        audit.update(event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", reason="non_protective_or_missing_id", protective_status=candidate["status"])
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", "audit": audit}

    audit.update(
        event="EXIT_PROTECTIVE_ORDER_FOUND",
        protective_broker_order_id=broker_order_id,
        protective_qty=candidate["qty"],
        protective_status=candidate["status"],
    )
    cancel = getattr(broker, "cancel_order", None)
    get_order = getattr(broker, "get_order", None)
    if not callable(cancel) or not callable(get_order):
        audit.update(event="EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN", reason="cancel_interface_unavailable")
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN", "audit": audit}
    audit["event"] = "EXIT_PROTECTIVE_CANCEL_REQUESTED"
    try:
        cancel_result = cancel(broker_order_id)
        audit["cancel_result"] = cancel_result if isinstance(cancel_result, dict) else {"malformed": True}
    except Exception as exc:
        audit["cancel_error"] = str(exc)
    try:
        terminal = get_order(broker_order_id)
    except Exception as exc:
        audit.update(event="EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN", get_order_error=str(exc))
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN", "audit": audit}
    if not isinstance(terminal, dict):
        audit.update(event="EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN", reason="get_order_malformed")
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN", "audit": audit}
    terminal_identity_issue = _terminal_order_identity_issue(
        terminal,
        expected_broker_order_id=broker_order_id,
        target_contract=exact_contract,
        account=account,
    )
    if terminal_identity_issue:
        audit.update(
            event="EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN",
            reason=terminal_identity_issue,
        )
        return {
            "allowed": False,
            "replacement_qty": 0,
            "reason": "EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN",
            "audit": audit,
        }
    terminal_status, consumed_qty, terminal_issue = _terminal_order_outcome(terminal, candidate)
    audit["protective_terminal_status"] = terminal_status or "missing"
    if terminal_issue:
        audit.update(event="EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN", reason=terminal_issue)
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN", "audit": audit}
    audit["protective_consumed_qty"] = int(consumed_qty)

    try:
        post_orders = list_orders_strict()
    except Exception as exc:
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="post_cancel_list_orders_strict_error", error=str(exc))
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_ORDERS_UNAVAILABLE", "audit": audit}
    if not isinstance(post_orders, list) or any(not isinstance(row, dict) for row in post_orders):
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="post_cancel_list_orders_malformed")
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_ORDERS_MALFORMED", "audit": audit}
    post_snapshot_issue = _order_snapshot_issue(
        post_orders, target_contract=exact_contract, account=account,
    )
    if post_snapshot_issue:
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason=post_snapshot_issue)
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_ORDERS_MALFORMED", "audit": audit}
    post_active_sells, post_terminal_orders, post_inventory_issue = _inventory(post_orders)
    if post_inventory_issue or post_active_sells:
        audit.update(
            event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
            reason=post_inventory_issue or "active_sell_appeared_after_takeover",
            post_takeover_active_sell_count=len(post_active_sells),
        )
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", "audit": audit}
    protective_broker_order_id = broker_order_id
    protective_post_terminals: list[dict[str, Any]] = []
    for post_terminal in post_terminal_orders:
        post_id = str(post_terminal.get("broker_order_id") or "").strip()
        if post_id == protective_broker_order_id:
            protective_post_terminals.append(post_terminal)
        elif post_terminal["consumed"] > 0:
            audit.update(
                event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
                reason="terminal_sell_appeared_after_takeover",
            )
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", "audit": audit}

    if len(protective_post_terminals) > 1:
        audit.update(
            event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
            reason="protective_order_duplicated_after_takeover",
        )
        return {
            "allowed": False,
            "replacement_qty": 0,
            "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
            "audit": audit,
        }

    candidate_executed = int(candidate.get("executed") or 0)
    observed_protective_execution_delta = int(consumed_qty)
    observed_protective_execution_total = max(
        candidate_executed + int(consumed_qty), candidate_executed,
    )
    if protective_post_terminals:
        post_consumed_qty = int(protective_post_terminals[0]["consumed"])
        if int(protective_post_terminals[0]["qty"]) != int(candidate["qty"]):
            audit.update(
                event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
                reason="protective_quantity_transition",
            )
            return {
                "allowed": False,
                "replacement_qty": 0,
                "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
                "audit": audit,
            }
        if post_consumed_qty < candidate_executed:
            audit.update(
                event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
                reason="protective_execution_regressed_after_takeover",
                post_consumed_qty=post_consumed_qty,
                candidate_executed=candidate_executed,
            )
            return {
                "allowed": False,
                "replacement_qty": 0,
                "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
                "audit": audit,
            }
        post_execution_delta = post_consumed_qty - candidate_executed
        if post_execution_delta < int(consumed_qty):
            audit.update(
                event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
                reason="protective_terminal_transition_inconsistent",
                post_execution_delta=post_execution_delta,
                terminal_consumed_qty=int(consumed_qty),
            )
            return {
                "allowed": False,
                "replacement_qty": 0,
                "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
                "audit": audit,
            }
        observed_protective_execution_delta = post_execution_delta
        observed_protective_execution_total = max(
            post_consumed_qty, observed_protective_execution_total,
        )
        audit["protective_post_terminal_consumed_qty"] = post_consumed_qty
    audit["observed_protective_execution_delta"] = observed_protective_execution_delta
    audit["observed_protective_execution_total"] = observed_protective_execution_total

    # Keep the final position read after the post-cancel order inventory.  A
    # protective fill can move between the cancel re-query and that inventory;
    # the final quantity must both reflect the observed execution and remain
    # the sole replacement-size authority.
    final_position = resolve_exit_broker_truth(
        broker=broker, client_id=client_id, contract=contract,
    )
    final_qty = final_position.get("broker_truth_open_qty")
    audit["final_position"] = final_position.get("audit") or {}
    if final_position.get("is_fresh_exact") is not True or not isinstance(final_qty, int):
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="post_cancel_position_unproven")
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_POSITION_UNPROVEN", "audit": audit}
    max_coherent_qty = max(initial_qty - observed_protective_execution_total, 0)
    if final_qty > max_coherent_qty:
        audit.update(
            event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED",
            reason="position_snapshot_stale_after_order_fill",
            final_qty=final_qty,
            max_coherent_qty=max_coherent_qty,
        )
        return {
            "allowed": False,
            "replacement_qty": 0,
            "reason": "EXIT_PROTECTIVE_POSITION_UNPROVEN",
            "audit": audit,
        }
    replacement_qty = min(
        requested_qty,
        max(final_qty, 0),
    )
    event = "EXIT_PROTECTIVE_FILLED_DURING_TAKEOVER" if terminal_status == "filled" or observed_protective_execution_delta > 0 or final_qty < initial_qty else "EXIT_PROTECTIVE_CANCEL_CONFIRMED"
    audit.update(event=event, broker_long_qty=final_qty, replacement_qty=replacement_qty)
    if replacement_qty <= 0:
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_BROKER_FLAT", "audit": audit}
    audit["event"] = "EXIT_PROTECTIVE_REPLACEMENT_ALLOWED"
    return {"allowed": True, "replacement_qty": replacement_qty, "reason": "EXIT_PROTECTIVE_REPLACEMENT_ALLOWED", "audit": audit}


def _table_columns(table_name: str) -> set[str]:
    with _SCHEMA_CACHE_LOCK:
        cached = _SCHEMA_CACHE.get(table_name)
        if cached is not None:
            return cached

    def _fn():
        with conn() as c:
            c.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                """,
                (table_name,),
            )
            return {str(r["column_name"]) for r in c.fetchall()}

    try:
        columns = run_with_retry(_fn) or set()
    except Exception as exc:
        log.debug("schema inspection failed for %s: %s", table_name, exc)
        columns = set()

    with _SCHEMA_CACHE_LOCK:
        _SCHEMA_CACHE[table_name] = columns
    return columns


def refresh_schema_cache() -> None:
    with _SCHEMA_CACHE_LOCK:
        _SCHEMA_CACHE.clear()


def has_orders_column(name: str) -> bool:
    return name in _table_columns("orders")


def has_positions_column(name: str) -> bool:
    return name in _table_columns("positions")


def _parse_entry_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _parse_max_exit_rejections_threshold() -> int:
    raw = os.getenv("MAX_EXIT_REJECTIONS_BEFORE_HALT")
    if raw is None or str(raw).strip() == "":
        return _DEFAULT_THRESHOLD
    try:
        threshold = int(str(raw).strip())
    except Exception:
        log.warning(
            "MAX_EXIT_REJECTIONS_BEFORE_HALT=%r invalid; using default=%d",
            raw,
            _DEFAULT_THRESHOLD,
        )
        return _DEFAULT_THRESHOLD
    if threshold <= 0:
        log.warning(
            "MAX_EXIT_REJECTIONS_BEFORE_HALT=%d disables exit rejection circuit breaker",
            threshold,
        )
    return threshold


def _persist_circuit_breaker_marker(
    db_conn,
    *,
    position_id: str,
    client_id: str,
    rejection_count: int,
) -> bool:
    position_columns = _table_columns("positions")
    metadata_col = "metadata" if "metadata" in position_columns else ("meta" if "meta" in position_columns else "")
    if not metadata_col:
        return False

    updates = [f"{metadata_col} = COALESCE({metadata_col}, '{{}}'::jsonb) || %s::jsonb"]
    params: list[Any] = [
        json.dumps(
            {
                "exit_circuit_breaker_tripped": True,
                "exit_circuit_breaker_tripped_at": now_utc_iso(),
                "exit_circuit_breaker_rejection_count": int(rejection_count),
            }
        )
    ]

    if "updated_ts" in position_columns:
        updates.append("updated_ts = NOW()")
    elif "updated_at" in position_columns:
        updates.append("updated_at = NOW()")

    params.extend([position_id, client_id])
    db_conn.execute(
        f"""
        UPDATE positions
        SET {", ".join(updates)}
        WHERE id = %s
          AND client_id = %s
        """,
        tuple(params),
    )
    return True


def _rate_limit_key(*, client_id: str, execution_mode: Optional[str], position_id: str, contract: str, reason: str) -> tuple[str, str, str, str, str]:
    return (
        str(client_id or "").strip().lower(),
        str(execution_mode or "").strip().lower(),
        str(position_id or "").strip(),
        str(contract or "").strip().upper(),
        str(reason or "").strip().lower(),
    )


def alert_exit_submission_halted(
    *,
    client_id: str,
    execution_mode: Optional[str],
    position_id: str,
    contract: str,
    reason: str,
    rejection_count: Optional[int] = None,
    threshold: Optional[int] = None,
) -> bool:
    cache_key = _rate_limit_key(
        client_id=client_id,
        execution_mode=execution_mode,
        position_id=position_id,
        contract=contract,
        reason=reason,
    )
    now = time.time()
    with _ALERT_CACHE_LOCK:
        last_ts = _ALERT_CACHE.get(cache_key, 0.0)
        if now - last_ts < _ALERT_COOLDOWN_SECONDS:
            return False
        _ALERT_CACHE[cache_key] = now

    details = [
        "EXIT HALT",
        f"client={client_id}",
        f"mode={execution_mode or 'unknown'}",
        f"position_id={position_id}",
        f"contract={contract}",
        f"reason={reason}",
    ]
    if rejection_count is not None:
        details.append(f"rejections={rejection_count}")
    if threshold is not None:
        details.append(f"threshold={threshold}")
    try:
        post_discord(" | ".join(details))
    except Exception as exc:
        log.warning("exit halt alert failed: %s", exc)
    return True


def _exit_position_terminal_state(
    db_conn,
    *,
    position_id,
    client_id,
    execution_mode=None,
    contract=None,
    broker_truth_open_qty=None,
    allow_missing_position_with_broker_truth: bool = False,
) -> dict:
    position_columns = _table_columns("positions")
    selected_columns = ["status"]
    for optional_col in ("quantity_remaining", "close_source", "entry_ts"):
        if optional_col in position_columns:
            selected_columns.append(optional_col)

    params: list[Any] = [position_id, client_id]
    sql = (
        f"SELECT {', '.join(selected_columns)} "
        "FROM positions "
        "WHERE id = %s AND client_id = %s"
    )
    normalized_mode = _normalize_mode(execution_mode)
    if normalized_mode and "execution_mode" in position_columns:
        sql += " AND LOWER(COALESCE(execution_mode, '')) = %s"
        params.append(normalized_mode)
    sql += " LIMIT 1"

    db_conn.execute(sql, tuple(params))
    row = db_conn.fetchone()
    if not row:
        broker_truth_qty = _safe_int(broker_truth_open_qty)
        synthetic_repair_id = str(position_id or "").startswith("broker-repair-")
        if broker_truth_qty is not None and broker_truth_qty > 0 and (
            allow_missing_position_with_broker_truth or synthetic_repair_id
        ):
            log.info(
                "exit guard allowing missing position from broker truth | client_id=%s execution_mode=%s position_id=%s contract=%s broker_truth_open_qty=%s synthetic_repair=%s",
                client_id,
                normalized_mode or "",
                position_id,
                contract or "",
                broker_truth_qty,
                synthetic_repair_id,
            )
            return {
                "blocked": False,
                "reason": None,
                "status": None,
                "quantity_remaining": broker_truth_qty,
                "close_source": None,
                "entry_ts": None,
            }
        log.warning(
            "exit guard blocked missing position | client_id=%s execution_mode=%s position_id=%s contract=%s",
            client_id,
            normalized_mode or "",
            position_id,
            contract or "",
        )
        return {
            "blocked": True,
            "reason": "position_missing",
            "status": None,
            "quantity_remaining": None,
            "close_source": None,
            "entry_ts": None,
        }

    status = row.get("status")
    quantity_remaining = row.get("quantity_remaining") if "quantity_remaining" in row else None
    close_source = row.get("close_source") if "close_source" in row else None
    entry_ts = row.get("entry_ts") if "entry_ts" in row else None

    status_norm = _normalize_text(status)
    close_source_norm = _normalize_text(close_source)
    qty_remaining_int = _safe_int(quantity_remaining)

    if status_norm == "closed":
        return {
            "blocked": True,
            "reason": "position_already_closed",
            "status": status,
            "quantity_remaining": qty_remaining_int,
            "close_source": close_source,
            "entry_ts": entry_ts,
        }

    if qty_remaining_int is not None and qty_remaining_int <= 0:
        return {
            "blocked": True,
            "reason": "position_quantity_depleted",
            "status": status,
            "quantity_remaining": qty_remaining_int,
            "close_source": close_source,
            "entry_ts": entry_ts,
        }

    if close_source_norm in _TERMINAL_CLOSE_SOURCES:
        return {
            "blocked": True,
            "reason": f"terminal_close_source:{close_source_norm}",
            "status": status,
            "quantity_remaining": qty_remaining_int,
            "close_source": close_source,
            "entry_ts": entry_ts,
        }

    return {
        "blocked": False,
        "reason": None,
        "status": status,
        "quantity_remaining": qty_remaining_int,
        "close_source": close_source,
        "entry_ts": entry_ts,
    }


def _should_halt_exit_after_rejections(
    db_conn,
    *,
    position_id,
    client_id,
    execution_mode=None,
    contract,
    entry_ts=None,
    broker_truth_open_qty: Optional[int] = None,
) -> dict:
    threshold = _parse_max_exit_rejections_threshold()
    if threshold <= 0:
        return {
            "blocked": False,
            "reason": None,
            "rejection_count": 0,
            "threshold": threshold,
        }

    order_columns = _table_columns("orders")
    normalized_mode = _normalize_mode(execution_mode)
    params: list[Any] = [client_id, contract]
    sql = [
        "SELECT COUNT(*) AS rejection_count",
        "FROM orders",
        "WHERE client_id = %s",
        "  AND kind = 'EXIT'",
        "  AND contract = %s",
        "  AND (",
        "        status = 'REJECTED'",
    ]

    if "last_error" in order_columns:
        sql.extend(
            [
                "        OR (",
                "            status = 'ERROR'",
                "            AND (",
            ]
        )
        for idx, pattern in enumerate(_BROKER_REJECT_LAST_ERROR_PATTERNS):
            sql.append("                COALESCE(last_error, '') ILIKE %s")
            if idx < len(_BROKER_REJECT_LAST_ERROR_PATTERNS) - 1:
                sql.append("                OR")
            params.append(pattern)
        sql.extend(
            [
                "            )",
                "        )",
            ]
        )
    sql.append("      )")

    if normalized_mode and "execution_mode" in order_columns:
        sql.append("  AND LOWER(COALESCE(execution_mode, '')) = %s")
        params.append(normalized_mode)

    timestamp_expr = None
    if "updated_ts" in order_columns and "created_ts" in order_columns:
        timestamp_expr = "COALESCE(updated_ts, created_ts)"
    elif "updated_ts" in order_columns:
        timestamp_expr = "updated_ts"
    elif "created_ts" in order_columns:
        timestamp_expr = "created_ts"

    parsed_entry_ts = _parse_entry_ts(entry_ts)
    if timestamp_expr and parsed_entry_ts is not None:
        sql.append(f"  AND {timestamp_expr} >= %s")
        params.append(parsed_entry_ts)
    elif timestamp_expr:
        sql.append(f"  AND {timestamp_expr} >= NOW() - (%s * INTERVAL '1 minute')")
        params.append(_DEFAULT_LOOKBACK_MINUTES)

    db_conn.execute("\n".join(sql), tuple(params))
    row = db_conn.fetchone() or {}
    rejection_count = int(row.get("rejection_count") or 0)
    blocked = rejection_count >= threshold

    if not blocked:
        return {
            "blocked": False,
            "reason": None,
            "rejection_count": rejection_count,
            "threshold": threshold,
        }

    # ── P0 (PR #307): broker-truth circuit breaker override ─────────────────
    # The circuit breaker fires on repeated exit rejections to prevent
    # dangerous flapping. But it must not trap a LIVE open broker position
    # with no way to close it.
    #
    # Case A — broker truth confirms open qty > 0:
    #   Allow one controlled protective close regardless of rejection count.
    #   The circuit breaker is designed to stop repeated blind exits, not to
    #   prevent closing a confirmed real position. Stamp
    #   PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH so the caller knows this was
    #   an override. The duplicate-in-flight guard in the exit engine still
    #   prevents concurrent double-submits.
    #
    # Case B — broker truth confirms flat (qty == 0) while circuit breaker
    #   keeps firing:
    #   The local/synthetic position believes it is open but the broker says
    #   it is flat. Stop repeated firing by returning a specific reason code
    #   SYNTHETIC_POSITION_STALE_BROKER_FLAT. The OSM caller must then mark
    #   the synthetic position stale so the exit engine stops evaluating it.
    #   Do NOT submit an exit order — there is nothing to close.
    _broker_qty = None
    try:
        _broker_qty = int(broker_truth_open_qty) if broker_truth_open_qty is not None else None
    except (TypeError, ValueError):
        _broker_qty = None

    if _broker_qty is not None:
        if _broker_qty > 0:
            # Case A: real broker exposure — allow the protective close
            log.warning(
                "exit_circuit_breaker OVERRIDE: broker_truth_open_qty=%d > 0 "
                "for position_id=%s client_id=%s contract=%s — "
                "allowing protective close despite %d rejections (threshold=%d). "
                "Duplicate-in-flight guard still active.",
                _broker_qty, position_id, client_id, contract,
                rejection_count, threshold,
            )
            return {
                "blocked": False,
                "reason": "PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH",
                "rejection_count": rejection_count,
                "threshold": threshold,
                "broker_truth_open_qty": _broker_qty,
                "circuit_breaker_overridden": True,
            }
        else:
            # Case B: broker is flat — stop repeated exit firing
            log.warning(
                "exit_circuit_breaker STALE_POSITION: broker_truth_open_qty=0 "
                "for position_id=%s client_id=%s contract=%s — "
                "synthetic position is stale; broker is flat. "
                "Blocking with SYNTHETIC_POSITION_STALE_BROKER_FLAT "
                "to stop repeated exit firing.",
                position_id, client_id, contract,
            )
            # Persist the circuit breaker marker so the entry brake (#306)
            # and the health dashboard can see this position is stuck.
            try:
                _persist_circuit_breaker_marker(
                    db_conn,
                    position_id=str(position_id),
                    client_id=str(client_id),
                    rejection_count=rejection_count,
                )
            except Exception as _cb_exc:
                log.debug("circuit breaker marker persistence failed: %s", _cb_exc)
            return {
                "blocked": True,
                "reason": "SYNTHETIC_POSITION_STALE_BROKER_FLAT",
                "rejection_count": rejection_count,
                "threshold": threshold,
                "broker_truth_open_qty": 0,
                "synthetic_stale": True,
            }

    # No broker truth supplied — original behavior: persist marker and block.
    if position_id:
        try:
            _persist_circuit_breaker_marker(
                db_conn,
                position_id=str(position_id),
                client_id=str(client_id),
                rejection_count=rejection_count,
            )
        except Exception as exc:
            log.debug("exit circuit breaker marker persistence failed: %s", exc)

    return {
        "blocked": blocked,
        "reason": "exit_circuit_breaker_tripped" if blocked else None,
        "rejection_count": rejection_count,
        "threshold": threshold,
    }


def evaluate_exit_submission_safety(
    *,
    position_id: str,
    client_id: str,
    execution_mode: Optional[str],
    contract: str,
    broker_truth_open_qty: Optional[int] = None,
    allow_missing_position_with_broker_truth: bool = False,
) -> dict:
    def _fn():
        with conn() as c:
            position_state = _exit_position_terminal_state(
                c,
                position_id=position_id,
                client_id=client_id,
                execution_mode=execution_mode,
                contract=contract,
                broker_truth_open_qty=broker_truth_open_qty,
                allow_missing_position_with_broker_truth=allow_missing_position_with_broker_truth,
            )
            if position_state.get("blocked"):
                return {
                    "blocked": True,
                    "reason": position_state.get("reason"),
                    "position_state": position_state,
                    "circuit_breaker": None,
                }

            circuit_breaker = _should_halt_exit_after_rejections(
                c,
                position_id=position_id,
                client_id=client_id,
                execution_mode=execution_mode,
                contract=contract,
                entry_ts=position_state.get("entry_ts"),
                broker_truth_open_qty=broker_truth_open_qty,
            )
            return {
                "blocked": bool(circuit_breaker.get("blocked")),
                "reason": circuit_breaker.get("reason"),
                "position_state": position_state,
                "circuit_breaker": circuit_breaker,
            }

    try:
        return run_with_retry(_fn)
    except Exception as exc:
        log.warning(
            "exit safety preflight unavailable; allowing submit to preserve existing flow | "
            "client_id=%s execution_mode=%s position_id=%s contract=%s err=%s",
            client_id,
            execution_mode or "",
            position_id,
            contract,
            exc,
        )
        return {
            "blocked": False,
            "reason": None,
            "position_state": None,
            "circuit_breaker": None,
        }
