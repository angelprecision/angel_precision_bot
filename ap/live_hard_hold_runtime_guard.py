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


def _patch_watcher(mod: Any) -> None:
    cls = getattr(mod, "APEntryWatcher", None)
    if cls is None or getattr(cls, "_AP_LIVE_HARD_HOLD_GUARD", False):
        return
    original_watch = cls.watch
    original_setattr = cls.__setattr__
    WatchState = getattr(mod, "WatchState", None)

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
                mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else max(bid, ask, last)
                already = (side == "CALL" and mid >= trigger) or (side == "PUT" and mid <= trigger)
                if mid <= 0 or already:
                    reason = "arm_quote_unavailable_live_blocked" if mid <= 0 else "arm_already_through_trigger"
                    try:
                        cancel = getattr(getattr(self, "order_state_machine", None), "cancel_pending_entry", None)
                        if callable(cancel) and local_order_id:
                            cancel(local_order_id, reason=f"watcher_block:{reason}")
                    except Exception:
                        pass
                    log.critical("P0_LIVE_HARD_HOLD_WATCH_ARM_BLOCK symbol=%s side=%s reason=%s", ticker, side, reason)
                    return False
            except Exception as exc:
                log.critical("P0_LIVE_HARD_HOLD_WATCH_ARM_BLOCK symbol=%s reason=quote_exception error=%s", ticker, exc)
                return False
        return original_watch(self, plan, local_order_id, *args, **kwargs)

    def wrap_callback(self: Any, cb: Any):
        if not callable(cb) or getattr(cb, "_AP_LIVE_HARD_HOLD_GUARD", False):
            return cb
        def wrapped(watched: Any, *args: Any, **kwargs: Any):
            if not getattr(watched, "trigger_crossed_at", None):
                ts = datetime.now(timezone.utc)
                setattr(watched, "trigger_crossed_at", ts)
                try:
                    watched.signal["trigger_crossed_at"] = ts.isoformat()
                except Exception:
                    pass
            try:
                return cb(watched, *args, **kwargs)
            except Exception as exc:
                attempts = int(getattr(watched, "trigger_callback_attempts", 0) or 0) + 1
                setattr(watched, "trigger_callback_attempts", attempts)
                max_attempts = int(os.getenv("WATCHER_TRIGGER_CALLBACK_MAX_ATTEMPTS", "3"))
                if attempts < max_attempts:
                    if WatchState is not None:
                        watched.state = WatchState.PENDING
                    with self._lock:
                        if watched not in self._pending:
                            self._pending.append(watched)
                    log.error("P0_LIVE_HARD_HOLD_TRIGGER_CALLBACK_RETRY symbol=%s attempt=%d", getattr(watched, "ticker", "?"), attempts)
                    return None
                if WatchState is not None:
                    watched.state = WatchState.EXPIRED
                if hasattr(watched, "_release_dedup_key"):
                    watched._release_dedup_key()
                log.critical("P0_LIVE_HARD_HOLD_TRIGGER_CALLBACK_EXPIRED symbol=%s attempts=%d error=%s", getattr(watched, "ticker", "?"), attempts, exc)
                return None
        wrapped._AP_LIVE_HARD_HOLD_GUARD = True
        return wrapped

    def guarded_setattr(self: Any, name: str, value: Any) -> None:
        if name == "on_trigger":
            value = wrap_callback(self, value)
        original_setattr(self, name, value)

    cls.watch = guarded_watch
    cls.__setattr__ = guarded_setattr
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
                return None
            max_age = float(os.getenv("ENTRY_TRIGGER_MAX_AGE_SEC", "120"))
            crossed = getattr(watched, "trigger_crossed_at", None) or signal.get("trigger_crossed_at")
            decision = require_fresh_trigger(trigger_crossed_at=crossed, max_age_seconds=max_age)
            if not decision.ok:
                log.critical("P0_LIVE_HARD_HOLD_SUBMIT_BLOCK symbol=%s reason=%s detail=%s", getattr(watched, "ticker", "?"), decision.reason, decision.detail)
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
