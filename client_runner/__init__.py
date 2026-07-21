"""P0 manual-close proof reconciliation shim.

The package shadows the legacy top-level ``client_runner.py`` module, re-exports
its surface, and overrides only ``ClientRunner._detect_manual_closes``.

The override is active for existing PAPER/LIVE runner health loops. It performs
broker and database reads, adopts already-filled external Tradier EXIT orders
into the durable order ledger, and delegates terminal position/proof mutation to
``APPositionManager.close_position_from_exit_fill``. It never submits or cancels
broker orders and never touches queues or signal admission.
"""

from __future__ import annotations

import importlib.util as _importlib_util
import sys as _sys
from pathlib import Path as _Path

from .manual_close_reconciliation import detect_manual_closes as _detect_manual_closes


_BASE_PATH = _Path(__file__).resolve().parent.parent / "client_runner.py"
_BASE_MODULE_NAME = "_client_runner_base"

_spec = _importlib_util.spec_from_file_location(_BASE_MODULE_NAME, _BASE_PATH)
if _spec is None or _spec.loader is None:  # pragma: no cover
    raise ImportError(f"Unable to load legacy client runner from {_BASE_PATH}")

_base = _importlib_util.module_from_spec(_spec)
_sys.modules[_BASE_MODULE_NAME] = _base
_spec.loader.exec_module(_base)

for _name in dir(_base):
    if not _name.startswith("__") or _name == "__doc__":
        globals()[_name] = getattr(_base, _name)

_BaseClientRunner = _base.ClientRunner


class ClientRunner(_BaseClientRunner):
    _detect_manual_closes = _detect_manual_closes


# Legacy supervisor functions resolve ClientRunner from their module globals at
# call time. Patch that binding so spawned runners use the hardened subclass.
_base.ClientRunner = ClientRunner
globals()["ClientRunner"] = ClientRunner
