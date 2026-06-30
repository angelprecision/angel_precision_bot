from __future__ import annotations

import importlib.util as _iu
import sys as _sys
import types as _types
from pathlib import Path as _Path
from typing import Any as _Any

_BASE_PATH = _Path(__file__).resolve().parent.parent / "order_state_machine.py"
_spec = _iu.spec_from_file_location("_ap_order_state_machine_base", _BASE_PATH)
if _spec is None or _spec.loader is None:
    raise ImportError(str(_BASE_PATH))
_base = _iu.module_from_spec(_spec)
_sys.modules["_ap_order_state_machine_base"] = _base
_spec.loader.exec_module(_base)

for _name in dir(_base):
    if _name.startswith("__") and _name not in {"__doc__", "__all__"}:
        continue
    globals()[_name] = getattr(_base, _name)

_ORIG_CLASS = _base.APOrderStateMachine
_ORIG_CREATE_ENTRY_ORDER = _base.APOrderStateMachine.create_entry_order
_SYNC_NAMES = {"conn", "run_with_retry", "log", "pg_errors", "emit_decision_event", "build_canonical_signal_id", "now_utc_iso"}


def _entry_direction(plan: _Any) -> tuple[str | None, str]:
    raw = getattr(plan, "side", None)
    if raw is None or str(raw or "").strip() == "":
        raw = getattr(plan, "direction", None)
    raw = str(raw or "").upper().strip()
    aliases = {
        "BUY": "CALL", "LONG": "CALL", "CALLS": "CALL", "BULLISH": "CALL",
        "SELL": "PUT", "SHORT": "PUT", "PUTS": "PUT", "BEARISH": "PUT",
    }
    out = aliases.get(raw, raw)
    return (out, raw) if out in {"CALL", "PUT"} else (None, raw)


class _PlanProxy:
    def __init__(self, plan: _Any, direction: str):
        self._plan = plan
        self.side = direction
        self.direction = direction

    def __getattr__(self, name: str) -> _Any:
        return getattr(self._plan, name)


class APOrderStateMachine(_ORIG_CLASS):
    def create_entry_order(self, plan, *, limit_price=None, reserved_cost=None, initial_status=None, meta=None, execution_mode=None) -> str:
        direction, raw = _entry_direction(plan)
        if direction is None:
            log.critical(
                "[%s] create_entry_order BLOCKED invalid_or_missing_entry_direction=%r plan=%s",
                self.client_id,
                raw,
                getattr(plan, "plan_id", "?"),
            )
            raise ValueError(f"invalid_or_missing_entry_direction:{raw!r}")
        return _ORIG_CREATE_ENTRY_ORDER(
            self,
            _PlanProxy(plan, direction),
            limit_price=limit_price,
            reserved_cost=reserved_cost,
            initial_status=initial_status,
            meta=meta,
            execution_mode=execution_mode,
        )


_base.APOrderStateMachine = APOrderStateMachine
globals()["APOrderStateMachine"] = APOrderStateMachine


class _Module(_types.ModuleType):
    def __setattr__(self, name: str, value: _Any) -> None:
        super().__setattr__(name, value)
        if name in _SYNC_NAMES or hasattr(_base, name):
            setattr(_base, name, value)


_sys.modules[__name__].__class__ = _Module

if isinstance(globals().get("__all__"), list) and "APOrderStateMachine" not in __all__:
    __all__.append("APOrderStateMachine")
