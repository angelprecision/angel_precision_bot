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
    strict_method = getattr(type(broker), "list_positions_strict", None)
    list_positions = (
        getattr(broker, "list_positions_strict", None)
        if callable(strict_method)
        else getattr(broker, "list_positions", None)
    )
    audit["source"] = "broker.list_positions_strict" if callable(strict_method) else "broker.list_positions"
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
        rows = []
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list):
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
        long_qty = _extract_long_position_qty(raw)
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
    quantity_key = "quantity" if "quantity" in raw else "qty" if "qty" in raw else None
    if quantity_key is None:
        qty = fallback_qty
    else:
        qty = _strict_positive_order_int(raw.get(quantity_key))
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

    # Tradier may report a partial fill with zero remaining as
    # ``partially_filled``.  A partial status with positive remaining is still
    # live and cannot authorize a replacement after a cancel request.
    if status in {"partially_filled", "partial_fill"}:
        if evidence["remaining"] > 0:
            return None, 0, "order_not_terminal"
        status = "filled"
    if status not in _TERMINAL_BROKER_ORDER_STATUSES:
        return None, 0, "order_not_terminal"

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


def resolve_protective_exit_takeover(
    *,
    broker: Any,
    client_id: str,
    execution_mode: str,
    position_id: str,
    local_order_id: str,
    contract: str,
    requested_qty: int,
) -> dict[str, Any]:
    """Prove broker sell ownership immediately before a LIVE EXIT POST.

    The account is supplied by the broker adapter itself; this routine never
    accepts a caller-provided account and never matches by underlying ticker.
    """
    mode = str(execution_mode or "").strip().lower()
    exact_contract = _normalize_contract(contract)
    account = _extract_broker_account_id(broker)
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

    list_orders = getattr(broker, "list_orders", None)
    if not callable(list_orders):
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="list_orders_unavailable")
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_ORDERS_UNAVAILABLE", "audit": audit}
    try:
        orders = list_orders()
    except Exception as exc:
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="list_orders_error", error=str(exc))
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_ORDERS_UNAVAILABLE", "audit": audit}
    if not isinstance(orders, list) or any(not isinstance(row, dict) for row in orders):
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="list_orders_malformed")
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_ORDERS_MALFORMED", "audit": audit}

    def _inventory(
        rows: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None]:
        active: list[dict[str, Any]] = []
        terminal_orders: list[dict[str, Any]] = []
        for row in rows:
            if _exact_order_contract(row) != exact_contract:
                continue
            row_account = _normalize_text(row.get("account_id") or row.get("account"))
            if row_account and account and row_account != account:
                continue
            if _normalize_text(row.get("side")) != "sell_to_close":
                continue
            status, status_issue = _order_status(row)
            if status_issue:
                return [], [], status_issue
            evidence, quantity_issue = _order_quantity_evidence(row)
            if quantity_issue or evidence is None:
                return [], [], quantity_issue or "quantity_unproven"
            if status in {"partially_filled", "partial_fill"} and evidence["remaining"] == 0:
                status = "filled"
            if status in _TERMINAL_BROKER_ORDER_STATUSES:
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
                if consumed > 0:
                    terminal_orders.append(
                        {
                            "raw": row,
                            "status": status,
                            "qty": evidence["qty"],
                            "executed": evidence["executed"],
                            "remaining": evidence["remaining"],
                            "consumed": int(consumed),
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
                    "status": status,
                    "qty": evidence["qty"],
                    "executed": evidence["executed"],
                    "remaining": evidence["remaining"],
                }
            )
        return active, terminal_orders, None

    active_sells, terminal_orders, inventory_issue = _inventory(orders)
    if inventory_issue:
        audit.update(event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", reason=inventory_issue)
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", "audit": audit}

    if not active_sells:
        # Re-read after the order snapshot so a protective fill that became
        # terminal immediately before/during classification wins the race.
        final_position = resolve_exit_broker_truth(
            broker=broker, client_id=client_id, contract=contract,
        )
        final_qty = final_position.get("broker_truth_open_qty")
        audit["final_position"] = final_position.get("audit") or {}
        if final_position.get("is_fresh_exact") is not True or not isinstance(final_qty, int):
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="post_order_position_unproven")
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_POSITION_UNPROVEN", "audit": audit}
        try:
            post_orders = list_orders()
        except Exception as exc:
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="post_order_list_orders_error", error=str(exc))
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_ORDERS_UNAVAILABLE", "audit": audit}
        if not isinstance(post_orders, list) or any(not isinstance(row, dict) for row in post_orders):
            audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="post_order_list_orders_malformed")
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_ORDERS_MALFORMED", "audit": audit}
        post_active_sells, post_terminal_orders, post_inventory_issue = _inventory(post_orders)
        if post_inventory_issue or post_active_sells:
            audit.update(
                event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
                reason=post_inventory_issue or "active_sell_appeared_after_takeover",
                post_takeover_active_sell_count=len(post_active_sells),
            )
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", "audit": audit}
        all_terminal_orders = terminal_orders + post_terminal_orders
        if len(all_terminal_orders) > 1:
            audit.update(
                event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
                reason="multiple_terminal_fills",
                terminal_sell_count=len(all_terminal_orders),
            )
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", "audit": audit}
        consumed_qty = all_terminal_orders[0]["consumed"] if all_terminal_orders else 0
        replacement_qty = min(
            requested_qty,
            max(final_qty, 0),
            max(initial_qty - consumed_qty, 0),
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
    order_type = _normalize_text(raw.get("type") or raw.get("order_type")).replace(" ", "_")
    broker_order_id = str(raw.get("id") or raw.get("order_id") or "").strip()
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
    terminal_status, consumed_qty, terminal_issue = _terminal_order_outcome(terminal, candidate)
    audit["protective_terminal_status"] = terminal_status or "missing"
    if terminal_issue:
        audit.update(event="EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN", reason=terminal_issue)
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN", "audit": audit}
    audit["protective_consumed_qty"] = int(consumed_qty)

    final_position = resolve_exit_broker_truth(
        broker=broker, client_id=client_id, contract=contract,
    )
    final_qty = final_position.get("broker_truth_open_qty")
    audit["final_position"] = final_position.get("audit") or {}
    if final_position.get("is_fresh_exact") is not True or not isinstance(final_qty, int):
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="post_cancel_position_unproven")
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_POSITION_UNPROVEN", "audit": audit}
    try:
        post_orders = list_orders()
    except Exception as exc:
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="post_cancel_list_orders_error", error=str(exc))
        return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_PROTECTIVE_ORDERS_UNAVAILABLE", "audit": audit}
    if not isinstance(post_orders, list) or any(not isinstance(row, dict) for row in post_orders):
        audit.update(event="EXIT_PROTECTIVE_REPLACEMENT_BLOCKED", reason="post_cancel_list_orders_malformed")
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
    for post_terminal in post_terminal_orders:
        post_id = str(
            post_terminal["raw"].get("id")
            or post_terminal["raw"].get("order_id")
            or ""
        ).strip()
        if post_terminal["consumed"] > 0 and post_id != protective_broker_order_id:
            audit.update(
                event="EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
                reason="terminal_sell_appeared_after_takeover",
            )
            return {"allowed": False, "replacement_qty": 0, "reason": "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS", "audit": audit}
    replacement_qty = min(
        requested_qty,
        max(final_qty, 0),
        max(initial_qty - consumed_qty, 0),
    )
    event = "EXIT_PROTECTIVE_FILLED_DURING_TAKEOVER" if terminal_status == "filled" or consumed_qty > 0 or final_qty < initial_qty else "EXIT_PROTECTIVE_CANCEL_CONFIRMED"
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
