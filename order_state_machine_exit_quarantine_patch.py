"""
order_state_machine_exit_quarantine_patch.py
===========================================
Runtime safety patch installer for APOrderStateMachine, APBrokerReconciler, and APExitEngine.

This file is intentionally used as the single installer because client_runner.py already calls:

    install_exit_quarantine_patch(APOrderStateMachine)

Installed protections
---------------------
1. OSM EXIT_SUBMITTED with missing broker_order_id -> APExitEngine quarantine.
2. OSM EXIT_FILLED scale-out/full-close classification is conservative and cumulative-fill aware.
3. APBrokerReconciler.run_once() is idempotent/throttled and locked to prevent double transitions.
4. APExitEngine.mark_position_closed() records trade outcomes into ap.performance_tracker.
"""

from __future__ import annotations

import logging
import threading
import time
from functools import wraps
from typing import Any

log = logging.getLogger("ap.order_state_machine.exit_quarantine_patch")

_PATCH_FLAG = "__ap_exit_quarantine_patch_installed__"
_ORIGINAL_ATTR = "__ap_exit_quarantine_original_handle_hooks__"
_RECONCILER_PATCH_FLAG = "__ap_reconciler_idempotency_patch_installed__"
_RECONCILER_ORIGINAL_ATTR = "__ap_reconciler_original_run_once__"
_EXIT_PERF_PATCH_FLAG = "__ap_exit_performance_patch_installed__"
_EXIT_PERF_ORIGINAL_ATTR = "__ap_exit_performance_original_mark_position_closed__"


def _safe_int(value: Any, default: int | None = 0) -> int | None:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _get_exit_engine(self):
    try:
        from ap.order_state_machine import _get_exit_engine_for_client
        return _get_exit_engine_for_client(getattr(self, "client_id", ""))
    except Exception:
        return None


def _call_exit_engine(ee, name: str, *args, **kwargs) -> bool:
    fn = getattr(ee, name, None)
    if not callable(fn):
        return False
    try:
        fn(*args, **kwargs)
        return True
    except TypeError:
        filtered = dict(kwargs)
        for k in ("identity_quarantine", "reconciled", "status"):
            filtered.pop(k, None)
        try:
            fn(*args, **filtered)
            return True
        except Exception:
            log.exception("exit engine hook %s failed after filtered retry", name)
            return True
    except Exception:
        log.exception("exit engine hook %s failed", name)
        return True


def _find_exit_engine_position(ee, position_id: str):
    pid = str(position_id or "")
    if not pid:
        return None
    for attr in ("_positions", "positions", "managed_positions", "open_positions", "active_positions"):
        container = getattr(ee, attr, None)
        if callable(container):
            try:
                container = container()
            except Exception:
                continue
        if container is None:
            continue
        if isinstance(container, dict):
            if pid in container:
                return container[pid]
            values = container.values()
        else:
            values = container
        try:
            for pos in values:
                if str(getattr(pos, "position_id", getattr(pos, "id", "")) or "") == pid:
                    return pos
        except Exception:
            continue
    return None


def _position_qty_context(ee, position_id: str) -> tuple[int | None, int | None]:
    pos = _find_exit_engine_position(ee, position_id)
    if pos is None:
        return None, None
    original = _safe_int(getattr(pos, "quantity", None), None)
    remaining = _safe_int(getattr(pos, "quantity_remaining", None), None)
    return original, remaining


def _install_reconciler_idempotency_patch() -> bool:
    try:
        from ap_reconciler import APBrokerReconciler
    except Exception as exc:
        log.warning("Reconciler idempotency patch skipped: %s", exc)
        return False

    if getattr(APBrokerReconciler, _RECONCILER_PATCH_FLAG, False):
        return True

    original = getattr(APBrokerReconciler, "run_once", None)
    if not callable(original):
        log.warning("Reconciler idempotency patch skipped: run_once missing")
        return False

    setattr(APBrokerReconciler, _RECONCILER_ORIGINAL_ATTR, original)

    @wraps(original)
    def run_once_guarded(self, *args, **kwargs):
        now = time.time()
        min_gap = float(getattr(self, "_run_once_min_gap_sec", 3.0))
        lock = getattr(self, "_run_once_lock", None)
        if lock is None:
            lock = threading.Lock()
            try:
                setattr(self, "_run_once_lock", lock)
            except Exception:
                pass

        if not lock.acquire(blocking=False):
            log.debug("[%s] Reconciler run_once skipped: already running", getattr(self, "client_id", "?"))
            return getattr(self, "_last_summary", {}) or {}

        try:
            last_ts = float(getattr(self, "_last_run_ts", 0.0) or 0.0)
            if now - last_ts < min_gap:
                log.debug("[%s] Reconciler run_once skipped: called too soon", getattr(self, "client_id", "?"))
                return getattr(self, "_last_summary", {}) or {}
            setattr(self, "_last_run_ts", now)
            summary = original(self, *args, **kwargs)
            try:
                setattr(self, "_last_summary", summary or {})
            except Exception:
                pass
            return summary
        finally:
            try:
                lock.release()
            except Exception:
                pass

    setattr(APBrokerReconciler, "run_once", run_once_guarded)
    setattr(APBrokerReconciler, _RECONCILER_PATCH_FLAG, True)
    log.info("APBrokerReconciler run_once idempotency patch installed")
    return True


def _install_exit_performance_patch() -> bool:
    try:
        from ap_exit_engine import APExitEngine
    except Exception as exc:
        log.warning("Exit performance patch skipped: %s", exc)
        return False

    if getattr(APExitEngine, _EXIT_PERF_PATCH_FLAG, False):
        return True

    original = getattr(APExitEngine, "mark_position_closed", None)
    if not callable(original):
        log.warning("Exit performance patch skipped: mark_position_closed missing")
        return False

    setattr(APExitEngine, _EXIT_PERF_ORIGINAL_ATTR, original)

    @wraps(original)
    def mark_position_closed_with_perf(self, position_id, *args, **kwargs):
        result = original(self, position_id, *args, **kwargs)
        try:
            pos = _find_exit_engine_position(self, str(position_id))
            if pos is not None:
                fill_price = kwargs.get("fill_price")
                qty_filled = kwargs.get("qty_filled") or kwargs.get("cumulative_filled")
                reason = kwargs.get("reason") or (args[0] if args else "") or "position_closed"
                from ap.performance_tracker import record_trade_outcome_from_position
                record_trade_outcome_from_position(
                    pos,
                    qty_filled=qty_filled,
                    fill_price=fill_price,
                    reason=str(reason),
                    supabase_client=getattr(self, "sb", None) or getattr(self, "supabase", None),
                )
        except Exception as exc:
            log.debug("performance outcome hook failed: %s", exc)
        return result

    setattr(APExitEngine, "mark_position_closed", mark_position_closed_with_perf)
    setattr(APExitEngine, _EXIT_PERF_PATCH_FLAG, True)
    log.info("APExitEngine performance outcome patch installed")
    return True


def install_exit_quarantine_patch(osm_cls):
    """Install OSM exit-hook safety plus companion runtime patches."""
    _install_reconciler_idempotency_patch()
    _install_exit_performance_patch()

    if getattr(osm_cls, _PATCH_FLAG, False):
        return osm_cls

    original = getattr(osm_cls, "_handle_exit_engine_hooks", None)
    if not callable(original):
        raise AttributeError("APOrderStateMachine._handle_exit_engine_hooks is missing")

    setattr(osm_cls, _ORIGINAL_ATTR, original)

    @wraps(original)
    def patched(self, *args, **kwargs):
        current = dict(kwargs.get("current") or {})
        new_status = str(kwargs.get("new_status") or "").upper()
        position_id = kwargs.get("position_id") or current.get("position_id")
        broker_order_id = kwargs.get("broker_order_id") or current.get("broker_order_id")
        local_order_id = kwargs.get("local_order_id") or current.get("local_order_id")
        filled_qty = kwargs.get("filled_qty")
        fill_price = kwargs.get("fill_price")
        kind = str(current.get("kind") or "").upper()

        if kind != "EXIT" or not position_id:
            return original(self, *args, **kwargs)

        ee = _get_exit_engine(self)
        if ee is None:
            return original(self, *args, **kwargs)

        order_qty = _safe_int(current.get("qty"), 0) or 0
        prev_filled = _safe_int(current.get("filled_qty"), 0) or 0
        cum_filled = _safe_int(filled_qty, None)

        if new_status == "EXIT_SUBMITTED" and not broker_order_id:
            _call_exit_engine(
                ee,
                "set_pending_exit_order",
                str(position_id),
                local_order_id=str(local_order_id or ""),
                broker_order_id="",
                qty=order_qty,
                reason=str(current.get("last_error") or "broker_accepted_missing_order_id_quarantine"),
                identity_quarantine=True,
            )
            log.critical(
                "[%s] EXIT_SUBMITTED missing broker_order_id bridged to exit engine quarantine | order=%s pos=%s",
                getattr(self, "client_id", "?"),
                local_order_id or "?",
                position_id,
            )
            return None

        if new_status == "EXIT_FILLED":
            if cum_filled is None or cum_filled <= 0:
                cum_filled = order_qty
            delta = max(0, int(cum_filled) - int(prev_filled))
            original_qty, remaining_qty = _position_qty_context(ee, str(position_id))

            if delta <= 0:
                _call_exit_engine(
                    ee,
                    "clear_exit_in_flight",
                    str(position_id),
                    local_order_id=str(local_order_id or ""),
                    broker_order_id=str(broker_order_id or ""),
                    reason="osm_exit_filled_no_new_delta",
                )
                return None

            if original_qty is not None and int(cum_filled) < int(original_qty):
                _call_exit_engine(
                    ee,
                    "note_partial_exit_fill",
                    str(position_id),
                    delta,
                    fill_price=fill_price,
                    local_order_id=str(local_order_id or ""),
                    broker_order_id=str(broker_order_id or ""),
                    cumulative_filled=int(cum_filled),
                )
                log.info(
                    "[%s] EXIT_FILLED treated as scale-out by cumulative/original proof | order=%s pos=%s cum=%s original=%s delta=%s",
                    getattr(self, "client_id", "?"),
                    local_order_id or "?",
                    position_id,
                    cum_filled,
                    original_qty,
                    delta,
                )
                return None

            if remaining_qty is None:
                _call_exit_engine(
                    ee,
                    "note_partial_exit_fill",
                    str(position_id),
                    delta,
                    fill_price=fill_price,
                    local_order_id=str(local_order_id or ""),
                    broker_order_id=str(broker_order_id or ""),
                    cumulative_filled=int(cum_filled),
                )
                log.critical(
                    "[%s] EXIT_FILLED remaining/original unknown — conservative partial handling | order=%s pos=%s delta=%s cum=%s",
                    getattr(self, "client_id", "?"),
                    local_order_id or "?",
                    position_id,
                    delta,
                    cum_filled,
                )
                return None

            if delta < int(remaining_qty):
                _call_exit_engine(
                    ee,
                    "note_partial_exit_fill",
                    str(position_id),
                    delta,
                    fill_price=fill_price,
                    local_order_id=str(local_order_id or ""),
                    broker_order_id=str(broker_order_id or ""),
                    cumulative_filled=int(cum_filled),
                )
                return None

            _call_exit_engine(
                ee,
                "mark_position_closed",
                str(position_id),
                reason="EXIT_FILLED",
                qty_filled=delta,
                fill_price=fill_price,
                local_order_id=str(local_order_id or ""),
                broker_order_id=str(broker_order_id or ""),
                cumulative_filled=int(cum_filled),
            )
            return None

        return original(self, *args, **kwargs)

    setattr(osm_cls, "_handle_exit_engine_hooks", patched)
    setattr(osm_cls, _PATCH_FLAG, True)
    log.info("APOrderStateMachine exit quarantine/fill-classification patch installed")
    return osm_cls


__all__ = ["install_exit_quarantine_patch"]
