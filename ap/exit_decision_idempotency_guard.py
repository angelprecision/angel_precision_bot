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
_ACTION_LEDGER_TTL = float(os.getenv("EXIT_DECISION_LEDGER_ACTION_DEDUPE_SECONDS", "60"))
_HOLD_LEDGER_TTL = float(os.getenv("EXIT_DECISION_LEDGER_HOLD_DEDUPE_SECONDS", "300"))
_LEDGER_CACHE_MAX = int(os.getenv("EXIT_DECISION_LEDGER_DEDUPE_CACHE_MAX", "4096"))
_EXTERNAL_PRECHECK_TTL = float(os.getenv("EXIT_DECISION_EXTERNAL_PRECHECK_SECONDS", "30"))
_PRECHECK_CACHE_MAX = int(os.getenv("EXIT_DECISION_PRECHECK_CACHE_MAX", "4096"))

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
            if not row and contract:
                row = c.execute(
                    "SELECT id, status, qty, quantity_remaining, contract, client_id "
                    "FROM positions WHERE client_id=%s AND UPPER(contract)=UPPER(%s) "
                    "ORDER BY COALESCE(entry_ts, created_at) DESC LIMIT 1",
                    (client_id, contract),
                ).fetchone()
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
            return bool(original(self, pos, decision, *args, **kwargs))
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
