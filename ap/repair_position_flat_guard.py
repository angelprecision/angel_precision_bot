from __future__ import annotations

import importlib.abc
import importlib.machinery
import sys
from typing import Any

from ap.exit_broker_truth_flat import clear_flat_repair_position_memory

_PATCHED = "_AP_FLAT_REPAIR_RUNTIME_GUARD"
_INSTALLED = False
_TARGET = "ap_exit_engine"


def _is_flat_repair(pos: Any) -> bool:
    pid = str(getattr(pos, "position_id", "") or "")
    if not pid.startswith("broker-repair-"):
        return False
    try:
        qty = int(float(getattr(pos, "quantity_remaining", 0) or 0))
    except Exception:
        qty = 0
    return bool(getattr(pos, "closed", False)) or qty <= 0


def _patch_exit_engine(module: Any) -> None:
    cls = getattr(module, "APExitEngine", None)
    if cls is None or getattr(cls, _PATCHED, False):
        return

    original = cls._submit_exit_decision

    def guarded(self, pos, decision, *args, **kwargs):
        if _is_flat_repair(pos):
            pid = str(getattr(pos, "position_id", "") or "")
            with self._lock:
                clear_flat_repair_position_memory(pos)
                by_id = getattr(self, "_positions_by_id", None)
                if isinstance(by_id, dict):
                    by_id.pop(pid, None)
            emit = getattr(self, "_emit_exit_event", None)
            if callable(emit):
                emit(
                    pos,
                    decision="CLOSED",
                    reason_code="BROKER_TRUTH_POSITION_FLAT",
                    explanation="Flat broker-repair position cleared before callback.",
                    stage="exit_reconciliation",
                    extra_inputs={"position_id": pid},
                )
            return False
        return original(self, pos, decision, *args, **kwargs)

    cls._submit_exit_decision = guarded
    setattr(cls, _PATCHED, True)


class _Loader(importlib.abc.Loader):
    def __init__(self, loader: Any):
        self.loader = loader

    def create_module(self, spec: Any):
        create = getattr(self.loader, "create_module", None)
        return create(spec) if callable(create) else None

    def exec_module(self, module: Any) -> None:
        self.loader.exec_module(module)
        _patch_exit_engine(module)


class _Finder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname: str, path: Any = None, target: Any = None):
        if fullname != _TARGET:
            return None
        spec = importlib.machinery.PathFinder.find_spec(fullname, path)
        if spec and spec.loader:
            spec.loader = _Loader(spec.loader)
            return spec
        return None


def install_repair_position_flat_guard() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True

    module = sys.modules.get(_TARGET)
    if module is not None:
        _patch_exit_engine(module)

    if not any(isinstance(finder, _Finder) for finder in sys.meta_path):
        sys.meta_path.insert(0, _Finder())
