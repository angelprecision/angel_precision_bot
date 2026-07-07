"""P0 integration wiring for live submit safety.

This module patches the real OrderStateMachine integration seams:

* APOrderStateMachine.submit_existing_entry() receives a final LIVE market-validity
  check immediately before the original submit path can broker POST.
* ap.order_state_machine.evaluate_exit_submission_safety is patched alongside
  ap.exit_safety.evaluate_exit_submission_safety so OSM's direct import binding
  cannot miss the broker-truth repair guard.

The implementation is deliberately fail-closed for LIVE and no-op for PAPER.
"""
from __future__ import annotations

import importlib.abc
import importlib.machinery
import sys
from datetime import datetime, timezone
from typing import Any

from ap.exit_circuit_breaker_broker_truth_guard import apply_broker_truth_exit_breaker_bypass
from ap.live_submit_safety import require_current_quote, require_remaining_opportunity

_TARGET = "ap.order_state_machine"
_PATCHED = "_AP_LIVE_SUBMIT_INTEGRATION_GUARD"
_BINDING_PATCHED = "_AP_EXIT_SAFETY_BINDING_PATCHED"
_INSTALLED = False


def _f(value: Any) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _s(value: Any) -> str:
    return str(value or "").strip()


def _mode_from_order_plan(current: dict, plan: Any) -> str:
    vals = [
        current.get("execution_mode"),
        (current.get("meta") or {}).get("execution_mode") if isinstance(current.get("meta"), dict) else None,
        getattr(plan, "execution_mode", None),
        getattr(plan, "mode", None),
    ]
    for raw in vals:
        mode = str(raw or "").strip().lower()
        if mode in {"live", "paper"}:
            return mode
    return ""


def _quote_underlying(broker: Any, ticker: str) -> dict:
    ticker = _s(ticker).upper()
    if not ticker:
        return {}
    base_url = (
        getattr(broker, "base_url", None)
        or getattr(getattr(broker, "cfg", None), "base_url", None)
        or "https://api.tradier.com"
    )
    session = getattr(broker, "session", None)
    get = getattr(session, "get", None)
    if not callable(get):
        return {}
    try:
        resp = get(
            f"{str(base_url).rstrip('/')}/v1/markets/quotes",
            params={"symbols": ticker, "greeks": "false"},
            headers={"Accept": "application/json"},
            timeout=5,
        )
        if hasattr(resp, "raise_for_status"):
            resp.raise_for_status()
        data = resp.json() if hasattr(resp, "json") else {}
        raw = (data or {}).get("quotes", {}).get("quote", {})
        if isinstance(raw, list):
            raw = raw[0] if raw else {}
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _final_market_validity_for_submit(*, current: dict, plan: Any, broker: Any) -> dict:
    ticker = _s(current.get("symbol") or getattr(plan, "ticker", "")).upper()
    side = _s(current.get("direction") or getattr(plan, "side", "") or getattr(plan, "direction", "")).upper()
    trigger = _f(current.get("trigger_price") or getattr(plan, "entry_trigger", 0) or getattr(plan, "trigger_price", 0))
    target = _f(current.get("target_underlying") or getattr(plan, "target_underlying", 0) or getattr(plan, "target_price", 0))
    quote = _quote_underlying(broker, ticker)
    bid = _f(quote.get("bid"))
    ask = _f(quote.get("ask"))
    last = _f(quote.get("last") or quote.get("last_price"))
    quote_decision, current_price = require_current_quote(bid=bid, ask=ask, last=last)
    payload = {
        "ok": False,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "symbol": ticker,
        "side": side,
        "bid": bid,
        "ask": ask,
        "last": last,
        "current_price": current_price,
        "trigger_price": trigger,
        "target_price": target,
    }
    if not quote_decision.ok:
        payload.update({"reason": quote_decision.reason, "detail": quote_decision.detail})
        return payload
    opp_decision = require_remaining_opportunity(
        side=side,
        current_price=current_price,
        trigger_price=trigger,
        target_price=target,
    )
    if not opp_decision.ok:
        payload.update({"reason": opp_decision.reason, "detail": opp_decision.detail})
        return payload
    payload.update({"ok": True, "reason": "FINAL_MARKET_VALIDITY_OK", "detail": ""})
    return payload


def _patch_exit_safety_binding(module: Any) -> None:
    if getattr(module, _BINDING_PATCHED, False):
        return
    original = getattr(module, "evaluate_exit_submission_safety", None)
    if not callable(original):
        return

    def guarded_evaluate_exit_submission_safety(*args, **kwargs):
        result = original(*args, **kwargs)
        return apply_broker_truth_exit_breaker_bypass(result, kwargs)

    module.evaluate_exit_submission_safety = guarded_evaluate_exit_submission_safety
    setattr(module, _BINDING_PATCHED, True)


def _patch_order_state_machine(module: Any) -> None:
    _patch_exit_safety_binding(module)
    cls = getattr(module, "APOrderStateMachine", None)
    if cls is None or getattr(cls, _PATCHED, False):
        return
    original_submit_existing_entry = cls.submit_existing_entry
    OrderStatus = getattr(module, "OrderStatus", None)
    error_status = getattr(OrderStatus, "ERROR", "ERROR")

    def guarded_submit_existing_entry(self, *, local_order_id: str, broker, plan=None, limit_price=None):
        current = self._get_order(local_order_id)
        if current:
            current = dict(current)
            if _mode_from_order_plan(current, plan) == "live":
                final = _final_market_validity_for_submit(current=current, plan=plan, broker=broker)
                try:
                    self.update_order_meta(local_order_id, {"final_market_validity": final})
                except Exception:
                    pass
                if not final.get("ok"):
                    reason = str(final.get("reason") or "LIVE_FINAL_MARKET_VALIDITY_BLOCK")
                    try:
                        self.transition(local_order_id, error_status, last_error=reason)
                    except Exception:
                        pass
                    return {
                        "ok": False,
                        "local_order_id": local_order_id,
                        "broker_order_id": current.get("broker_order_id"),
                        "status": error_status,
                        "error": reason,
                    }
        return original_submit_existing_entry(
            self,
            local_order_id=local_order_id,
            broker=broker,
            plan=plan,
            limit_price=limit_price,
        )

    cls.submit_existing_entry = guarded_submit_existing_entry
    setattr(cls, _PATCHED, True)


class _Loader(importlib.abc.Loader):
    def __init__(self, loader: Any):
        self.loader = loader

    def create_module(self, spec: Any):
        create = getattr(self.loader, "create_module", None)
        return create(spec) if callable(create) else None

    def exec_module(self, module: Any) -> None:
        self.loader.exec_module(module)
        _patch_order_state_machine(module)


class _Finder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname: str, path: Any = None, target: Any = None):
        if fullname != _TARGET:
            return None
        spec = importlib.machinery.PathFinder.find_spec(fullname, path)
        if spec and spec.loader:
            spec.loader = _Loader(spec.loader)
            return spec
        return None


def install_live_submit_integration_guard() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    module = sys.modules.get(_TARGET)
    if module is not None:
        _patch_order_state_machine(module)
    if not any(isinstance(finder, _Finder) for finder in sys.meta_path):
        sys.meta_path.insert(0, _Finder())
