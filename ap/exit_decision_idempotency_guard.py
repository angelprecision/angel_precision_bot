"""Exit-decision idempotency and broker-flat closed-position fencing.

The exit engine already checks active EXIT orders at the final submit seam, but
it records the decision before that seam and marks the in-memory position
in-flight only after the external callback returns. Production evidence showed a
closed QCOM position producing 2,905 identical SCALE_OUT decisions and a SPY
position producing 602.

This guard adds three narrow protections without changing exit policy:

* an in-process claim around the callback seam so concurrent engine paths cannot
  both call the broker-submit callback for one position;
* an active-EXIT-order and terminal broker-flat fence in the existing broker
  precheck seam, which runs before the engine decision lock;
* rate-limited decision-ledger writes so a persistent failure remains visible
  without inserting the same row every eight seconds.

DB or broker-truth failures fail open for exits: risk-reducing behavior is never
blocked merely because diagnostics are unavailable.
"""
from __future__ import annotations

import os
import math
import threading
import time
from typing import Any, Callable

from ap.db import conn, run_with_retry
from ap.logger import get_logger

log = get_logger("ap.exit_decision_idempotency_guard")

_PATCHED_ATTR = "_AP_EXIT_DECISION_IDEMPOTENCY_PATCHED"
_ORIGINAL_LEDGER_ATTR = "_AP_EXIT_LEDGER_ORIGINAL"
_ORIGINAL_PRECHECK_ATTR = "_AP_EXIT_PRECHECK_ORIGINAL"
_ORIGINAL_SUBMIT_ATTR = "_AP_EXIT_SUBMIT_ORIGINAL"

_ACTIVE_EXIT_STATUSES = {
    "EXIT_REQUESTED",
    "EXIT_SUBMITTED",
    "EXIT_ACKNOWLEDGED",
    "EXIT_PARTIAL_FILL",
}
_TERMINAL_POSITION_STATUSES = {
    "CLOSED",
    "CLOSED_REPAIR",
    "EXPIRED",
    "STOPPED",
    "TAKEN_PROFIT",
    "ERROR",
    "CANCELED",
    "CANCELLED",
}

_CLAIM_STATE_CLAIMED = "CLAIMED"
_CLAIM_STATE_BROKER_OWNED = "BROKER_OWNED"
_CLAIM_STATE_RELEASED_NO_SUBMIT = "RELEASED_NO_SUBMIT"
_CLAIM_STATE_AMBIGUOUS = "AMBIGUOUS"
_CONCLUSIVE_NO_SUBMIT_STATUSES = {"ERROR", "REJECTED", "CANCELED", "CANCELLED", "EXPIRED"}


def _bounded_float_env(name: str, default: float, minimum: float, maximum: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        log.error("invalid %s=%r; using conservative default=%s", name, raw, default)
        return default
    if not math.isfinite(value) or value < 0:
        log.error("unsafe %s=%r; using conservative default=%s", name, raw, default)
        return default
    return min(max(value, minimum), maximum)


def _bounded_int_env(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError, OverflowError):
        log.error("invalid %s=%r; using conservative default=%s", name, raw, default)
        return default
    if value < 0:
        log.error("unsafe %s=%r; using conservative default=%s", name, raw, default)
        return default
    return min(max(value, minimum), maximum)


# Documented deployment bounds: TTLs are at least one second and at most one
# day; caches retain at least 128 entries and never exceed 100k entries.
_ACTION_LEDGER_TTL = _bounded_float_env(
    "EXIT_DECISION_LEDGER_ACTION_DEDUPE_SECONDS", 60.0, 1.0, 86400.0
)
_HOLD_LEDGER_TTL = _bounded_float_env(
    "EXIT_DECISION_LEDGER_HOLD_DEDUPE_SECONDS", 300.0, 1.0, 86400.0
)
_LEDGER_CACHE_MAX = _bounded_int_env(
    "EXIT_DECISION_LEDGER_DEDUPE_CACHE_MAX", 4096, 128, 100000
)
_EXTERNAL_PRECHECK_TTL = _bounded_float_env(
    "EXIT_DECISION_EXTERNAL_PRECHECK_SECONDS", 30.0, 1.0, 86400.0
)
_PRECHECK_CACHE_MAX = _bounded_int_env(
    "EXIT_DECISION_PRECHECK_CACHE_MAX", 4096, 128, 100000
)
_TERMINAL_EXIT_STATUSES = (
    "EXIT_FILLED", "REJECTED", "CANCELED", "CANCELLED", "EXPIRED", "ERROR",
)

_LEDGER_LOCK = threading.Lock()
_LEDGER_LAST_WRITTEN: dict[tuple[str, ...], float] = {}
_PRECHECK_LOCK = threading.Lock()
_PRECHECK_LAST_RUN: dict[str, float] = {}


def _int(value: Any, default: int | None = 0) -> int | None:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _position_key(pos: Any) -> str:
    return str(
        getattr(pos, "position_id", "")
        or getattr(pos, "option_symbol", "")
        or getattr(pos, "ticker", "")
        or id(pos)
    )


def active_exit_order_blocks(order: Any) -> bool:
    if not isinstance(order, dict):
        return False
    return str(order.get("status") or "").strip().upper() in _ACTIVE_EXIT_STATUSES


def decision_fingerprint(pos: Any, decision: Any, client_id: str = "") -> tuple[str, ...]:
    """Stable fingerprint for one economically identical decision state."""
    return (
        str(client_id or getattr(pos, "client_id", "") or ""),
        _position_key(pos),
        str(getattr(pos, "option_symbol", "") or ""),
        str(getattr(decision, "action", "") or ""),
        str(_int(getattr(decision, "quantity", 0), 0) or 0),
        str(getattr(decision, "reason_code", "") or ""),
        str(getattr(decision, "reason", "") or ""),
        str(_int(getattr(pos, "quantity_remaining", 0), 0) or 0),
        str(getattr(pos, "pending_exit_local_order_id", "") or ""),
        str(getattr(pos, "pending_exit_broker_order_id", "") or ""),
    )


def _decision_should_act(decision: Any) -> bool:
    value = getattr(decision, "should_act", None)
    if value is not None:
        return bool(value)
    return str(getattr(decision, "action", "") or "").upper() in {"CLOSE_ALL", "SCALE_OUT"}


def _durable_exit_generation(pos: Any, client_id: str) -> tuple[str, int] | None:
    """Resolve the durable generation from terminal EXIT order history.

    Generation advances only after the prior broker-owned EXIT order reaches a
    terminal state.  Combined with remaining quantity, this produces the audit
    contract: client + real position + remaining qty + exit generation.
    """
    client_id = str(client_id or getattr(pos, "client_id", "") or "").strip()
    position_id = str(getattr(pos, "position_id", "") or "").strip()
    remaining_qty = _int(getattr(pos, "quantity_remaining", 0), 0) or 0
    if (
        not client_id
        or not position_id
        or position_id.lower().startswith("broker-repair-")
        or remaining_qty <= 0
    ):
        return None

    def _read_generation() -> int:
        with conn() as c:
            row = c.execute(
                "SELECT COUNT(DISTINCT local_order_id) AS terminal_exit_count "
                "FROM orders WHERE client_id=%s AND position_id::text=%s "
                "AND kind='EXIT' AND status IN %s",
                (client_id, position_id, _TERMINAL_EXIT_STATUSES),
            ).fetchone()
            row = dict(row) if row else {}
            return max(0, _int(row.get("terminal_exit_count"), 0) or 0) + 1

    generation = int(run_with_retry(_read_generation) or 1)
    key = f"{client_id}|{position_id}|{remaining_qty}|{generation}"
    return key, generation


def _claim_durable_decision_generation(
    *,
    generation_key: str,
    client_id: str,
    position_id: str,
    remaining_qty: int,
    exit_generation: int,
    decision: Any,
) -> dict:
    """Atomically claim one actionable decision for this durable generation."""
    def _claim() -> dict:
        with conn() as c:
            row = c.execute(
                "INSERT INTO exit_decision_generation_claims ("
                "generation_key, client_id, position_id, remaining_qty, "
                "exit_generation, decision_action, decision_reason_code, "
                "claim_state, claimed_at, released_at, local_order_id, broker_order_id, last_error"
                ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NULL,NULL,NULL,NULL) "
                "ON CONFLICT (generation_key) DO UPDATE SET "
                "client_id=EXCLUDED.client_id, "
                "position_id=EXCLUDED.position_id, "
                "remaining_qty=EXCLUDED.remaining_qty, "
                "exit_generation=EXCLUDED.exit_generation, "
                "decision_action=EXCLUDED.decision_action, "
                "decision_reason_code=EXCLUDED.decision_reason_code, "
                "claim_state=%s, "
                "claimed_at=NOW(), "
                "released_at=NULL, "
                "local_order_id=NULL, "
                "broker_order_id=NULL, "
                "last_error=NULL "
                "WHERE exit_decision_generation_claims.claim_state=%s "
                "RETURNING generation_key, client_id, position_id, remaining_qty, "
                "exit_generation, decision_action, decision_reason_code, claim_state, "
                "local_order_id, broker_order_id, last_error, claimed_at, released_at",
                (
                    generation_key,
                    client_id,
                    position_id,
                    remaining_qty,
                    exit_generation,
                    str(getattr(decision, "action", "") or ""),
                    str(getattr(decision, "reason_code", "") or ""),
                    _CLAIM_STATE_CLAIMED,
                    _CLAIM_STATE_CLAIMED,
                    _CLAIM_STATE_RELEASED_NO_SUBMIT,
                ),
            ).fetchone()
            if row:
                claimed = dict(row)
                claimed["claimed"] = True
                return claimed

            row = c.execute(
                "SELECT generation_key, client_id, position_id, remaining_qty, "
                "exit_generation, decision_action, decision_reason_code, claim_state, "
                "local_order_id, broker_order_id, last_error, claimed_at, released_at "
                "FROM exit_decision_generation_claims WHERE generation_key=%s LIMIT 1",
                (generation_key,),
            ).fetchone()
            existing = dict(row) if row else {"generation_key": generation_key}
            existing["claimed"] = False
            return existing

    return dict(run_with_retry(_claim) or {"generation_key": generation_key, "claimed": False})


def _update_durable_decision_generation(
    generation_key: str,
    *,
    claim_state: str,
    local_order_id: str = "",
    broker_order_id: str = "",
    error_text: str = "",
) -> None:
    def _update() -> None:
        with conn() as c:
            c.execute(
                "UPDATE exit_decision_generation_claims "
                "SET claim_state=%s, "
                "released_at=CASE WHEN %s=%s THEN NOW() ELSE released_at END, "
                "local_order_id=CASE WHEN %s<>'' THEN %s ELSE local_order_id END, "
                "broker_order_id=CASE WHEN %s<>'' THEN %s ELSE broker_order_id END, "
                "last_error=CASE WHEN %s<>'' THEN %s ELSE NULL END "
                "WHERE generation_key=%s",
                (
                    claim_state,
                    claim_state,
                    _CLAIM_STATE_RELEASED_NO_SUBMIT,
                    local_order_id,
                    local_order_id,
                    broker_order_id,
                    broker_order_id,
                    error_text,
                    error_text,
                    generation_key,
                ),
            )

    run_with_retry(_update)


def _extract_callback_trace_identity(callback_trace: dict) -> dict:
    identity = dict(callback_trace.get("identity") or {})
    result = callback_trace.get("result")
    if not isinstance(result, dict):
        return identity

    for source_key, target_key in (
        ("local_order_id", "local_order_id"),
        ("exit_local_order_id", "local_order_id"),
        ("broker_order_id", "broker_order_id"),
        ("order_id", "broker_order_id"),
        ("id", "broker_order_id"),
    ):
        value = result.get(source_key)
        if value is not None and not identity.get(target_key):
            identity[target_key] = str(value)
    if "accepted" not in identity and "accepted" in result:
        identity["accepted"] = bool(result.get("accepted"))
    if not identity.get("raw_status"):
        raw_status = result.get("status") or result.get("raw_status") or result.get("state")
        if raw_status is not None:
            identity["raw_status"] = str(raw_status)
    return identity


def _classify_submit_claim_outcome(
    engine: Any,
    pos: Any,
    callback_trace: dict,
    callback_returned: bool,
) -> tuple[str, str, str, str]:
    position_id = str(getattr(pos, "position_id", "") or "")
    try:
        active_order = _active_exit_order(engine, position_id)
    except Exception as exc:
        log.warning(
            "[%s] EXIT_DECISION_GENERATION_POST_SUBMIT_LOOKUP_FAILED position=%s error=%s",
            getattr(pos, "ticker", ""),
            position_id,
            exc,
        )
        active_order = None

    identity = _extract_callback_trace_identity(callback_trace)
    local_order_id = str(
        identity.get("local_order_id")
        or getattr(pos, "pending_exit_local_order_id", "")
        or (active_order or {}).get("local_order_id")
        or ""
    ).strip()
    broker_order_id = str(
        identity.get("broker_order_id")
        or getattr(pos, "pending_exit_broker_order_id", "")
        or (active_order or {}).get("broker_order_id")
        or ""
    ).strip()
    raw_status = str(identity.get("raw_status") or callback_trace.get("status") or "").strip().upper()
    error_text = str(callback_trace.get("error") or "")
    if not error_text and callback_trace.get("exception") is not None:
        error_text = str(callback_trace["exception"])

    if active_exit_order_blocks(active_order):
        return _CLAIM_STATE_BROKER_OWNED, local_order_id, broker_order_id, error_text
    if broker_order_id or (raw_status == "EXIT_SUBMITTED" and local_order_id):
        return _CLAIM_STATE_BROKER_OWNED, local_order_id, broker_order_id, error_text
    if bool(getattr(pos, "exit_in_flight", False)) and (local_order_id or broker_order_id):
        return _CLAIM_STATE_BROKER_OWNED, local_order_id, broker_order_id, error_text

    if not callback_trace.get("entered"):
        return _CLAIM_STATE_RELEASED_NO_SUBMIT, local_order_id, broker_order_id, error_text
    if callback_trace.get("exception") is not None:
        return _CLAIM_STATE_AMBIGUOUS, local_order_id, broker_order_id, error_text
    if error_text.startswith("BROKER_AMBIGUOUS_"):
        return _CLAIM_STATE_AMBIGUOUS, local_order_id, broker_order_id, error_text

    result = callback_trace.get("result")
    if isinstance(result, dict):
        if result.get("reconciliation_required") or result.get("split_brain"):
            return _CLAIM_STATE_AMBIGUOUS, local_order_id, broker_order_id, error_text
        if bool(result.get("identity_quarantine")) and raw_status not in {"", "EXIT_SUBMITTED"}:
            return _CLAIM_STATE_AMBIGUOUS, local_order_id, broker_order_id, error_text

    accepted = identity.get("accepted")
    if raw_status in _CONCLUSIVE_NO_SUBMIT_STATUSES:
        return _CLAIM_STATE_RELEASED_NO_SUBMIT, local_order_id, broker_order_id, error_text
    if accepted is False:
        return _CLAIM_STATE_RELEASED_NO_SUBMIT, local_order_id, broker_order_id, error_text
    if error_text.startswith("broker_conn_error:"):
        return _CLAIM_STATE_RELEASED_NO_SUBMIT, local_order_id, broker_order_id, error_text
    if callback_returned:
        return _CLAIM_STATE_AMBIGUOUS, local_order_id, broker_order_id, error_text
    return _CLAIM_STATE_RELEASED_NO_SUBMIT, local_order_id, broker_order_id, error_text


def should_write_ledger(
    pos: Any,
    decision: Any,
    *,
    client_id: str = "",
    now_monotonic: float | None = None,
) -> bool:
    """Return True once per fingerprint/TTL; terminal and in-flight repeats skip."""
    if bool(getattr(pos, "closed", False)):
        return False
    if (_int(getattr(pos, "quantity_remaining", 0), 0) or 0) <= 0:
        return False
    if bool(getattr(pos, "exit_in_flight", False)):
        return False

    now_value = time.monotonic() if now_monotonic is None else float(now_monotonic)
    fingerprint = decision_fingerprint(pos, decision, client_id)
    ttl = _ACTION_LEDGER_TTL if _decision_should_act(decision) else _HOLD_LEDGER_TTL

    with _LEDGER_LOCK:
        previous = _LEDGER_LAST_WRITTEN.get(fingerprint)
        if previous is not None and now_value - previous < max(0.0, ttl):
            return False
        _LEDGER_LAST_WRITTEN[fingerprint] = now_value

        if len(_LEDGER_LAST_WRITTEN) > max(128, _LEDGER_CACHE_MAX):
            cutoff = now_value - max(_ACTION_LEDGER_TTL, _HOLD_LEDGER_TTL, 60.0) * 2
            stale = [key for key, seen_at in _LEDGER_LAST_WRITTEN.items() if seen_at < cutoff]
            for key in stale:
                _LEDGER_LAST_WRITTEN.pop(key, None)
            while len(_LEDGER_LAST_WRITTEN) > max(128, _LEDGER_CACHE_MAX):
                _LEDGER_LAST_WRITTEN.pop(next(iter(_LEDGER_LAST_WRITTEN)), None)
    return True


def should_run_external_precheck(pos: Any, *, now_monotonic: float | None = None) -> bool:
    """Bound DB/OSM reconciliation reads; the final submit fence stays immediate."""
    if bool(getattr(pos, "closed", False)) or bool(getattr(pos, "exit_in_flight", False)):
        return False
    now_value = time.monotonic() if now_monotonic is None else float(now_monotonic)
    key = _position_key(pos)
    with _PRECHECK_LOCK:
        previous = _PRECHECK_LAST_RUN.get(key)
        if previous is not None and now_value - previous < max(0.0, _EXTERNAL_PRECHECK_TTL):
            return False
        _PRECHECK_LAST_RUN[key] = now_value
        if len(_PRECHECK_LAST_RUN) > max(128, _PRECHECK_CACHE_MAX):
            cutoff = now_value - max(_EXTERNAL_PRECHECK_TTL, 30.0) * 4
            stale = [cache_key for cache_key, seen_at in _PRECHECK_LAST_RUN.items() if seen_at < cutoff]
            for cache_key in stale:
                _PRECHECK_LAST_RUN.pop(cache_key, None)
            while len(_PRECHECK_LAST_RUN) > max(128, _PRECHECK_CACHE_MAX):
                _PRECHECK_LAST_RUN.pop(next(iter(_PRECHECK_LAST_RUN)), None)
    return True


def _active_exit_order(engine: Any, position_id: str) -> dict | None:
    osm = getattr(engine, "order_state_machine", None) or getattr(engine, "osm", None)
    if osm is None or not position_id:
        return None
    getter = getattr(osm, "_get_active_exit_order", None) or getattr(osm, "get_active_exit_order", None)
    if not callable(getter):
        return None
    row = getter(position_id)
    return row if isinstance(row, dict) else None


def _terminal_position_snapshot(pos: Any, engine: Any) -> dict | None:
    client_id = str(
        getattr(pos, "client_id", "")
        or getattr(engine, "client_id", "")
        or getattr(engine, "_email", "")
        or ""
    )
    position_id = str(getattr(pos, "position_id", "") or "")
    contract = str(getattr(pos, "option_symbol", "") or "").strip().upper()
    if not client_id:
        return None

    def _read() -> dict | None:
        with conn() as c:
            row = None
            if position_id and not position_id.lower().startswith("broker-repair-"):
                row = c.execute(
                    "SELECT id, status, qty, quantity_remaining, contract, client_id "
                    "FROM positions WHERE client_id=%s AND id::text=%s LIMIT 1",
                    (client_id, position_id),
                ).fetchone()
            if not row and contract and (
                not position_id or position_id.lower().startswith("broker-repair-")
            ):
                rows = c.execute(
                    "SELECT id, status, qty, quantity_remaining, contract, client_id "
                    "FROM positions WHERE client_id=%s AND UPPER(contract)=UPPER(%s) "
                    "ORDER BY COALESCE(entry_ts, created_at) DESC LIMIT 2",
                    (client_id, contract),
                ).fetchall()
                row = rows[0] if len(rows) == 1 else None
            return dict(row) if row else None

    try:
        snapshot = run_with_retry(_read)
    except Exception as exc:
        log.debug(
            "terminal position snapshot unavailable position=%s contract=%s error=%s",
            position_id,
            contract,
            exc,
        )
        return None
    if not snapshot:
        return None

    status = str(snapshot.get("status") or "").strip().upper()
    remaining = _int(snapshot.get("quantity_remaining"), None)
    if status in _TERMINAL_POSITION_STATUSES or remaining == 0:
        return snapshot
    return None


def _fresh_exact_broker_flat(pos: Any, engine: Any) -> bool:
    try:
        from ap.exit_safety import resolve_exit_broker_truth

        truth = resolve_exit_broker_truth(
            broker=getattr(engine, "broker", None),
            client_id=str(getattr(pos, "client_id", "") or getattr(engine, "client_id", "") or ""),
            contract=str(getattr(pos, "option_symbol", "") or ""),
        )
    except Exception as exc:
        log.debug("broker-flat confirmation unavailable position=%s error=%s", _position_key(pos), exc)
        return False

    if not isinstance(truth, dict) or truth.get("is_fresh_exact") is not True:
        return False
    open_qty = _int(truth.get("broker_truth_open_qty"), None)
    return open_qty == 0


def _mark_active_exit_owned(engine: Any, pos: Any, active_order: dict) -> None:
    with engine._lock:
        if bool(getattr(pos, "closed", False)):
            return
        pos.exit_in_flight = True
        pos.pending_exit_local_order_id = str(active_order.get("local_order_id") or "")
        pos.pending_exit_broker_order_id = str(active_order.get("broker_order_id") or "")


def _remove_terminal_broker_flat(engine: Any, pos: Any, terminal: dict) -> None:
    key = _position_key(pos)
    with engine._lock:
        pos.closed = True
        pos.quantity_remaining = 0
        pos.exit_in_flight = False
        pos.pending_exit_action = ""
        pos.pending_exit_reason = ""
        pos.pending_exit_qty = 0
        pos.pending_exit_local_order_id = ""
        pos.pending_exit_broker_order_id = ""
        engine._positions = [tracked for tracked in engine._positions if tracked is not pos]
        positions_by_id = getattr(engine, "_positions_by_id", None)
        if isinstance(positions_by_id, dict):
            positions_by_id.pop(str(getattr(pos, "position_id", "") or ""), None)
    with _PRECHECK_LOCK:
        _PRECHECK_LAST_RUN.pop(key, None)
    log.warning(
        "[%s] EXIT_EVALUATION_SUPPRESSED_TERMINAL_BROKER_FLAT position=%s contract=%s db_status=%s",
        getattr(pos, "ticker", ""),
        terminal.get("id"),
        terminal.get("contract"),
        terminal.get("status"),
    )


def wrap_ledger(original: Callable[..., Any]) -> Callable[..., Any]:
    def guarded(pos, decision, *, client_id: str = ""):
        if not should_write_ledger(pos, decision, client_id=client_id):
            return None
        return original(pos, decision, client_id=client_id)

    return guarded


def wrap_precheck(original: Callable[..., bool]) -> Callable[..., bool]:
    """Run bounded external lookups outside the engine decision lock."""
    def guarded(self, *args, **kwargs) -> bool:
        result = bool(original(self, *args, **kwargs))
        try:
            active_positions = list(self.active_positions())
        except Exception as exc:
            log.debug("exit precheck active snapshot unavailable: %s", exc)
            return result

        for pos in active_positions:
            if not should_run_external_precheck(pos):
                continue
            position_id = str(getattr(pos, "position_id", "") or "")
            try:
                active_order = _active_exit_order(self, position_id)
            except Exception as exc:
                log.debug("early active-exit lookup unavailable position=%s error=%s", position_id, exc)
                active_order = None
            if active_exit_order_blocks(active_order):
                try:
                    _mark_active_exit_owned(self, pos, active_order)
                except Exception as exc:
                    log.debug("active exit ownership hydration failed position=%s error=%s", position_id, exc)
                continue

            terminal = _terminal_position_snapshot(pos, self)
            if terminal and _fresh_exact_broker_flat(pos, self):
                try:
                    _remove_terminal_broker_flat(self, pos, terminal)
                except Exception as exc:
                    log.warning("terminal broker-flat suppression failed position=%s error=%s", position_id, exc)

        return result

    return guarded


def wrap_submit(original: Callable[..., bool]) -> Callable[..., bool]:
    def guarded(self, pos, decision, *args, **kwargs) -> bool:
        key = _position_key(pos)
        resolved_client = str(
            getattr(pos, "client_id", "")
            or getattr(self, "client_id", "")
            or getattr(self, "_email", "")
            or ""
        ).strip()
        position_id = str(getattr(pos, "position_id", "") or "")
        remaining_qty = _int(getattr(pos, "quantity_remaining", 0), 0) or 0
        with self._lock:
            claims = getattr(self, "_ap_exit_submit_claims", None)
            if claims is None:
                claims = set()
                self._ap_exit_submit_claims = claims
            if key in claims:
                log.warning(
                    "[%s] EXIT_PROCESS_CLAIM_BLOCKED_DUPLICATE position=%s action=%s qty=%s",
                    getattr(pos, "ticker", ""),
                    key,
                    getattr(decision, "action", ""),
                    getattr(decision, "quantity", 0),
                )
                return False
            claims.add(key)

        try:
            try:
                active_order = _active_exit_order(self, position_id)
            except Exception as exc:
                log.warning(
                    "[%s] EXIT_DECISION_ACTIVE_EXIT_FENCE_LOOKUP_FAILED position=%s error=%s",
                    getattr(pos, "ticker", ""),
                    position_id,
                    exc,
                )
                active_order = None
            if active_exit_order_blocks(active_order):
                try:
                    _mark_active_exit_owned(self, pos, active_order)
                except Exception as exc:
                    log.debug("active exit ownership hydration failed position=%s error=%s", position_id, exc)
                log.warning(
                    "[%s] EXIT_DECISION_ACTIVE_EXIT_FENCE_BLOCKED position=%s local_order_id=%s broker_order_id=%s status=%s",
                    getattr(pos, "ticker", ""),
                    position_id,
                    active_order.get("local_order_id"),
                    active_order.get("broker_order_id"),
                    active_order.get("status"),
                )
                return False

            generation_key = ""
            exit_generation = 0
            if _decision_should_act(decision):
                durable = _durable_exit_generation(pos, resolved_client)
                if durable is not None:
                    generation_key, exit_generation = durable
                    try:
                        claim = _claim_durable_decision_generation(
                            generation_key=generation_key,
                            client_id=resolved_client,
                            position_id=position_id,
                            remaining_qty=remaining_qty,
                            exit_generation=exit_generation,
                            decision=decision,
                        )
                    except Exception as exc:
                        log.critical(
                            "[%s] EXIT_DECISION_GENERATION_CLAIM_FAILED client_id=%s position_id=%s key=%s qty_remaining=%s exit_generation=%s action=%s reason_code=%s error=%s",
                            getattr(pos, "ticker", ""),
                            resolved_client,
                            position_id,
                            generation_key,
                            remaining_qty,
                            exit_generation,
                            getattr(decision, "action", ""),
                            getattr(decision, "reason_code", ""),
                            exc,
                        )
                        return False
                    if not claim.get("claimed"):
                        log.critical(
                            "[%s] EXIT_DECISION_GENERATION_DUPLICATE_SUPPRESSED client_id=%s position_id=%s key=%s qty_remaining=%s exit_generation=%s action=%s reason_code=%s existing_state=%s",
                            getattr(pos, "ticker", ""),
                            resolved_client,
                            position_id,
                            generation_key,
                            remaining_qty,
                            exit_generation,
                            getattr(decision, "action", ""),
                            getattr(decision, "reason_code", ""),
                            claim.get("claim_state", ""),
                        )
                        return False

            callback_attr = (
                "on_scale"
                if str(getattr(decision, "action", "") or "").upper() == "SCALE_OUT"
                else "on_exit"
            )
            original_callback = getattr(self, callback_attr, None)
            callback_trace = {
                "entered": False,
                "result": None,
                "exception": None,
                "identity": {},
                "status": "",
                "error": "",
            }
            if callable(original_callback):
                def traced_callback(*cb_args, **cb_kwargs):
                    callback_trace["entered"] = True
                    try:
                        result = original_callback(*cb_args, **cb_kwargs)
                    except Exception as exc:
                        callback_trace["exception"] = exc
                        raise
                    callback_trace["result"] = result
                    if isinstance(result, dict):
                        callback_trace["status"] = str(
                            result.get("status") or result.get("raw_status") or result.get("state") or ""
                        )
                        callback_trace["error"] = str(result.get("error") or "")
                    extractor = getattr(self, "_extract_exit_order_identity", None)
                    if callable(extractor):
                        try:
                            callback_trace["identity"] = extractor(result) or {}
                        except Exception:
                            callback_trace["identity"] = {}
                    return result

                setattr(self, callback_attr, traced_callback)

            try:
                callback_returned = bool(original(self, pos, decision, *args, **kwargs))
            finally:
                if callable(original_callback):
                    setattr(self, callback_attr, original_callback)

            if generation_key:
                claim_state, local_order_id, broker_order_id, error_text = _classify_submit_claim_outcome(
                    self,
                    pos,
                    callback_trace,
                    callback_returned,
                )
                _update_durable_decision_generation(
                    generation_key,
                    claim_state=claim_state,
                    local_order_id=local_order_id,
                    broker_order_id=broker_order_id,
                    error_text=error_text,
                )
            return callback_returned
        finally:
            with self._lock:
                claims = getattr(self, "_ap_exit_submit_claims", None)
                if isinstance(claims, set):
                    claims.discard(key)

    return guarded


def install_exit_decision_idempotency_guard() -> None:
    """Install idempotent wrappers on the actual production exit engine."""
    import ap_exit_engine as engine_module

    if getattr(engine_module, _PATCHED_ATTR, False):
        return

    engine_cls = engine_module.APExitEngine
    original_ledger = engine_module._ledger_exit_decision
    original_precheck = engine_cls._broker_position_precheck
    original_submit = engine_cls._submit_exit_decision

    setattr(engine_module, _ORIGINAL_LEDGER_ATTR, original_ledger)
    setattr(engine_cls, _ORIGINAL_PRECHECK_ATTR, original_precheck)
    setattr(engine_cls, _ORIGINAL_SUBMIT_ATTR, original_submit)

    engine_module._ledger_exit_decision = wrap_ledger(original_ledger)
    engine_cls._broker_position_precheck = wrap_precheck(original_precheck)
    engine_cls._submit_exit_decision = wrap_submit(original_submit)
    setattr(engine_module, _PATCHED_ATTR, True)
