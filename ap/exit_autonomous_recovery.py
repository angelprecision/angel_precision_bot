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
    osm: Any, local_order_id: str, broker_order_id: str, attempt: int,
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
                return abs(int(float(val)))
        except Exception:
            pass
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


def _get_order(broker: Any, broker_order_id: str) -> Optional[dict]:
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
    if not isinstance(raw, dict) or not raw or not _status(raw):
        log.warning(
            "broker.get_order(%s) returned unavailable payload: %r",
            broker_order_id,
            raw,
        )
        raise _BrokerSnapshotUnavailable(
            f"broker.get_order returned malformed payload for {broker_order_id}"
        )
    return dict(raw)


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
            if not _contract(row):
                log.warning("broker.list_positions returned position without a contract during autonomous recovery")
                return False, []
            quantity_present = any(
                key in row for key in ("quantity", "qty", "position_qty", "long_quantity")
            )
            if not quantity_present:
                log.warning("broker.list_positions returned position without quantity during autonomous recovery")
                return False, []
            try:
                quantity = float(
                    row.get("quantity", row.get("qty", row.get("position_qty", row.get("long_quantity"))))
                )
                if not math.isfinite(quantity):
                    raise ValueError("non-finite quantity")
            except (TypeError, ValueError, OverflowError):
                log.warning("broker.list_positions returned non-numeric quantity during autonomous recovery")
                return False, []
        return True, rows
    except Exception as exc:
        log.warning("broker.list_positions failed during autonomous recovery: %s", exc)
        return False, []


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
        try:
            confirmed = _get_order(broker, broker_order_id)
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
        try:
            revoke(
                pid,
                reason="autonomous_replacement_generation_commit_failed",
                local_order_id=local_id,
                broker_order_id=broker_id,
                force=True,
            )
        except Exception as exc:
            log.error("autonomous replacement revoke after finalize failure failed for pos=%s: %s", pid, exc)
        return RecoveryAction(
            "NOOP", "replacement_generation_commit_failed", pid, local_id, broker_id, details,
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
            fresh_raw = _get_order(broker, broker_id)
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
    if not _persist_stale_exit_cancel_attempt(osm, local_id, broker_id, next_attempt):
        return RecoveryAction(
            "NOOP",
            "autonomous_cancel_attempt_durability_unconfirmed",
            _position_id(pos), local_id, broker_id,
            {"status": status, "cancel_attempt": next_attempt, "quote_health": quote_health_payload},
        )
    ok, proof = _cancel_order_with_proof(broker, broker_id)
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
    return _mark_replacement_safe(
        exit_engine,
        _position_id(pos),
        osm=osm,
        reason="autonomous_recovery_stale_known_exit_canceled",
        local_id=local_id,
        broker_id=broker_id,
        details=details,
        replacement_qty=max(0, int(getattr(pos, "pending_exit_qty", 0) or 0)),
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

    # Exact broker identity path.
    if pending_broker_id:
        # APExitEngine.set_pending_exit_order() refreshes its in-memory signal
        # timestamp while it adopts the exact broker identity.  Preserve the
        # stale-age proof from before that reconciliation so a dead monitor can
        # still take the bounded autonomous handoff.
        pre_reconciliation_age_seconds = _exit_age_seconds(pos, osm, local_id)
        try:
            raw = _get_order(broker, pending_broker_id)
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
                    try:
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
    if not _contract_held and contract:
        # Position is flat at broker — exit filled but callback was dropped.
        # Use the actual mark_position_closed contract and verify the engine
        # performed the close before claiming economic completion.
        close_qty = (
            getattr(pos, "pending_exit_qty", 0)
            or getattr(pos, "contracts", 0)
            or getattr(pos, "quantity_remaining", 0)
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
        broker_id="",
        details={
            "contract": contract,
            "quote_health": qh,
            "open_order_query_available": True,
            "position_query_available": True,
        },
    )


def recover_exit_engine(
    exit_engine: Any, *, broker: Any, osm: Any = None, max_positions: int = 10,
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
    for pos in positions[:max_positions]:
        if not (getattr(pos, "exit_identity_quarantine", False) or getattr(pos, "last_callback_identity_missing", False) or getattr(pos, "exit_in_flight", False)):
            continue
        try:
            actions.append(recover_exit_position(pos, broker=broker, exit_engine=exit_engine, osm=osm, order_monitor=order_monitor))
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
