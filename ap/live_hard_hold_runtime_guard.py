"""Install P0 live hard-hold guards for watcher and execution-core imports."""
from __future__ import annotations

import importlib.abc
import importlib.machinery
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from ap.live_submit_safety import require_fresh_trigger, require_live_identity

log = logging.getLogger("ap.live_hard_hold_runtime_guard")
ET = ZoneInfo("America/New_York")
_INSTALLED = False
_PATCHED: set[str] = set()


def _regular_session() -> bool:
    now = datetime.now(ET)
    return (now.hour > 9 or (now.hour == 9 and now.minute >= 30)) and (now.hour < 15 or (now.hour == 15 and now.minute < 30))


def _field(obj: Any, sig: dict, *names: str) -> Any:
    for name in names:
        if hasattr(obj, name) and getattr(obj, name) not in (None, ""):
            return getattr(obj, name)
        if sig.get(name) not in (None, ""):
            return sig.get(name)
    return None


def _float(v: Any) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def _mode_from(core: Any, signal: dict, plan: Any = None) -> str:
    vals = [signal.get("execution_mode"), signal.get("mode")]
    if plan is not None:
        vals += [getattr(plan, "execution_mode", None), getattr(plan, "mode", None)]
    vals += [getattr(core, "execution_mode", None), getattr(core, "mode", None)]
    for raw in vals:
        s = str(raw or "").strip().lower()
        if s in {"live", "paper"}:
            return s
    if isinstance(getattr(core, "paper", None), bool):
        return "paper" if core.paper else "live"
    return ""


def _stamp_first_breach(watched: Any, *, price: float, bid: float, ask: float) -> None:
    if getattr(watched, "trigger_crossed_at", None):
        return
    ts = datetime.now(timezone.utc)
    try:
        setattr(watched, "trigger_crossed_at", ts)
        setattr(watched, "first_breach_price", float(price or 0))
        setattr(watched, "first_breach_bid", float(bid or 0))
        setattr(watched, "first_breach_ask", float(ask or 0))
    except Exception:
        pass
    try:
        sig = getattr(watched, "signal", None)
        if isinstance(sig, dict):
            sig["trigger_crossed_at"] = ts.isoformat()
            sig["first_breach_price"] = float(price or 0)
            sig["first_breach_bid"] = float(bid or 0)
            sig["first_breach_ask"] = float(ask or 0)
    except Exception:
        pass


def _terminalize_live_submit_block(core: Any, watched: Any, *, reason: str, detail: str = "") -> None:
    signal = getattr(watched, "signal", {}) or {}
    if not isinstance(signal, dict):
        signal = {}
    local_order_id = str(signal.get("local_order_id") or getattr(watched, "local_order_id", "") or "").strip()
    if not local_order_id:
        return
    meta = {
        "live_submit_safety": {
            "ok": False,
            "reason": reason,
            "detail": detail,
            "blocked_at": datetime.now(timezone.utc).isoformat(),
            "symbol": str(getattr(watched, "ticker", "") or signal.get("symbol") or signal.get("ticker") or ""),
            "trigger_crossed_at": str(getattr(watched, "trigger_crossed_at", "") or signal.get("trigger_crossed_at") or ""),
            "first_breach_price": getattr(watched, "first_breach_price", signal.get("first_breach_price")),
        }
    }
    osm = (
        getattr(core, "order_state_machine", None)
        or getattr(core, "osm", None)
        or getattr(core, "order_state", None)
    )
    if osm is not None:
        try:
            fn = getattr(osm, "update_order_meta", None)
            if callable(fn):
                fn(local_order_id, meta)
        except Exception:
            pass
        for method_name in ("expire_pending_entry", "cancel_pending_entry"):
            try:
                fn = getattr(osm, method_name, None)
                if callable(fn) and fn(local_order_id, reason=reason):
                    return
            except Exception:
                pass
        try:
            status_cls = getattr(sys.modules.get("ap.order_state_machine"), "OrderStatus", None)
            error_status = getattr(status_cls, "ERROR", "ERROR")
            transition = getattr(osm, "transition", None)
            if callable(transition):
                transition(local_order_id, error_status, last_error=reason)
        except Exception:
            pass


def _patch_watcher(mod: Any) -> None:
    cls = getattr(mod, "APEntryWatcher", None)
    watched_cls = getattr(mod, "WatchedSignal", None)
    if cls is None or getattr(cls, "_AP_LIVE_HARD_HOLD_GUARD", False):
        return
    original_watch = cls.watch
    original_poll = getattr(cls, "_poll_active_signals", None)

    if watched_cls is not None and not getattr(watched_cls, "_AP_FIRST_BREACH_STAMP_GUARD", False):
        original_check = watched_cls.check

        def guarded_check(self: Any, bid: float, ask: float):
            side = str(getattr(self, "side", "") or "").upper()
            trigger = _float(getattr(self, "entry_trigger", 0))
            breach_count = int(getattr(self, "breach_count", 0) or 0)
            if trigger > 0 and breach_count == 0:
                if side == "CALL" and _float(ask) >= trigger:
                    _stamp_first_breach(self, price=_float(ask), bid=_float(bid), ask=_float(ask))
                elif side == "PUT" and _float(bid) <= trigger:
                    _stamp_first_breach(self, price=_float(bid), bid=_float(bid), ask=_float(ask))
            return original_check(self, bid, ask)

        watched_cls.check = guarded_check
        watched_cls._AP_FIRST_BREACH_STAMP_GUARD = True

    def guarded_watch(self: Any, plan: Any, local_order_id: str | None = None, *args: Any, **kwargs: Any):
        signal = getattr(plan, "signal", None) or getattr(plan, "signal_dict", None) or {}
        if not isinstance(signal, dict):
            signal = {}
        live = str(getattr(self, "mode", "") or "").upper() == "LIVE"
        ticker = str(_field(plan, signal, "ticker", "symbol") or "").upper()
        side = str(_field(plan, signal, "side", "direction") or "").upper()
        trigger = _float(_field(plan, signal, "entry_price", "trigger_price", "entry_trigger"))
        if live and _regular_session() and ticker and side in {"CALL", "PUT"} and trigger > 0:
            try:
                quotes = self._fetch_quotes([ticker]) if hasattr(self, "_fetch_quotes") else {}
                q = quotes.get(ticker) or quotes.get(ticker.upper()) or {}
                bid = _float(q.get("bid"))
                ask = _float(q.get("ask"))
                last = _float(q.get("last") or q.get("last_price"))
                fallback = (bid + ask) / 2.0 if bid > 0 and ask > 0 else max(bid, ask, last)
                if side == "CALL":
                    lane_price = ask if ask > 0 else fallback
                    quote_missing = lane_price <= 0
                    already = lane_price >= trigger
                else:
                    lane_price = bid if bid > 0 else fallback
                    quote_missing = lane_price <= 0
                    already = lane_price <= trigger if lane_price > 0 else False
                if quote_missing or already:
                    reason = "arm_quote_unavailable_live_blocked" if quote_missing else "arm_already_through_trigger"
                    try:
                        cancel = getattr(getattr(self, "order_state_machine", None), "cancel_pending_entry", None)
                        if callable(cancel) and local_order_id:
                            cancel(local_order_id, reason=f"watcher_block:{reason}")
                    except Exception:
                        pass
                    log.critical(
                        "P0_LIVE_HARD_HOLD_WATCH_ARM_BLOCK symbol=%s side=%s reason=%s lane_price=%.4f trigger=%.4f bid=%.4f ask=%.4f",
                        ticker, side, reason, lane_price, trigger, bid, ask,
                    )
                    return False
            except Exception as exc:
                log.critical("P0_LIVE_HARD_HOLD_WATCH_ARM_BLOCK symbol=%s reason=quote_exception error=%s", ticker, exc)
                return False
        return original_watch(self, plan, local_order_id, *args, **kwargs)

    def guarded_poll(self: Any, *args: Any, **kwargs: Any):
        """Preserve watcher ownership for trigger retries without swallowing callback errors.

        The underlying watcher removes triggered rows from _pending before calling
        on_trigger. This wrapper does not swallow callback exceptions; it re-adds
        the watcher before re-raising so the original retry logic can keep the row
        owned until callback success or retry exhaustion.
        """
        if not callable(original_poll):
            return None
        cb = getattr(self, "on_trigger", None)
        if not callable(cb) or getattr(cb, "_AP_RETRY_OWNERSHIP_GUARD", False):
            return original_poll(self, *args, **kwargs)

        def guarded_cb(watched: Any, *cb_args: Any, **cb_kwargs: Any):
            try:
                result = cb(watched, *cb_args, **cb_kwargs)
                return result
            except Exception:
                try:
                    attempts_before = int(getattr(watched, "_trigger_attempts", 0) or 0)
                    max_attempts = int(os.getenv("WATCHER_TRIGGER_CALLBACK_MAX_ATTEMPTS", "3"))
                    if attempts_before + 1 < max_attempts:
                        with self._lock:
                            if watched not in self._pending:
                                self._pending.append(watched)
                except Exception:
                    pass
                raise

        guarded_cb._AP_RETRY_OWNERSHIP_GUARD = True
        self.on_trigger = guarded_cb
        try:
            return original_poll(self, *args, **kwargs)
        finally:
            self.on_trigger = cb

    cls.watch = guarded_watch
    if callable(original_poll):
        cls._poll_active_signals = guarded_poll
    cls._AP_LIVE_HARD_HOLD_GUARD = True
    log.critical("P0_LIVE_HARD_HOLD_PATCHED target=ap_entry_watcher")


def _patch_core(mod: Any) -> None:
    cls = getattr(mod, "APExecutionCore", None)
    if cls is None or getattr(cls, "_AP_LIVE_HARD_HOLD_GUARD", False):
        return
    original = cls._on_entry_trigger

    def guarded_trigger(self: Any, watched: Any, *args: Any, **kwargs: Any):
        signal = getattr(watched, "signal", {}) or {}
        if not isinstance(signal, dict):
            signal = {}
        try:
            plan = self._recover_plan_for_revalidation(watched)
        except Exception:
            plan = None
        if _mode_from(self, signal, plan) == "live":
            client_id = str(signal.get("client_id") or signal.get("client_email") or getattr(self, "client_id", "") or getattr(self, "email", "") or "").strip()
            explicit_mode = signal.get("execution_mode") or getattr(plan, "execution_mode", None)
            decision = require_live_identity(client_id=client_id, execution_mode=explicit_mode)
            if not decision.ok:
                log.critical("P0_LIVE_HARD_HOLD_SUBMIT_BLOCK symbol=%s reason=%s", getattr(watched, "ticker", "?"), decision.reason)
                _terminalize_live_submit_block(self, watched, reason=decision.reason, detail=decision.detail)
                return None
            max_age = float(os.getenv("ENTRY_TRIGGER_MAX_AGE_SEC", "120"))
            crossed = getattr(watched, "trigger_crossed_at", None) or signal.get("trigger_crossed_at")
            decision = require_fresh_trigger(trigger_crossed_at=crossed, max_age_seconds=max_age)
            if not decision.ok:
                log.critical("P0_LIVE_HARD_HOLD_SUBMIT_BLOCK symbol=%s reason=%s detail=%s", getattr(watched, "ticker", "?"), decision.reason, decision.detail)
                _terminalize_live_submit_block(self, watched, reason=decision.reason, detail=decision.detail)
                return None
        return original(self, watched, *args, **kwargs)

    cls._on_entry_trigger = guarded_trigger
    cls._AP_LIVE_HARD_HOLD_GUARD = True
    log.critical("P0_LIVE_HARD_HOLD_PATCHED target=ap_execution_core")


def _patch(name: str, module: Any) -> None:
    if name in _PATCHED:
        return
    if name == "ap_entry_watcher":
        _patch_watcher(module)
    elif name == "ap_execution_core":
        _patch_core(module)
    _PATCHED.add(name)


class _Loader(importlib.abc.Loader):
    def __init__(self, loader: Any):
        self.loader = loader
    def create_module(self, spec: Any):
        create = getattr(self.loader, "create_module", None)
        return create(spec) if callable(create) else None
    def exec_module(self, module: Any) -> None:
        self.loader.exec_module(module)
        _patch(module.__name__, module)


class _Finder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname: str, path: Any = None, target: Any = None):
        if fullname not in {"ap_entry_watcher", "ap_execution_core"}:
            return None
        spec = importlib.machinery.PathFinder.find_spec(fullname, path)
        if spec and spec.loader:
            spec.loader = _Loader(spec.loader)
            return spec
        return None


def install_live_hard_hold_runtime_guard() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    for name in ("ap_entry_watcher", "ap_execution_core"):
        module = sys.modules.get(name)
        if module is not None:
            _patch(name, module)
    if not any(isinstance(x, _Finder) for x in sys.meta_path):
        sys.meta_path.insert(0, _Finder())
    log.critical("P0_LIVE_HARD_HOLD_RUNTIME_GUARD_INSTALLED")
