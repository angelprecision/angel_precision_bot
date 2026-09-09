"""Production-shaped runtime caller proof for PR 603.

The test starts after startup recovery has already completed, creates the
incident-shaped due retry in durable test state, and lets the real
ClientRunner-owned scheduler call the existing recovery boundary.  It must
not call the execution-core retry method directly.
"""
from __future__ import annotations

import os
import sys
import types
from unittest.mock import MagicMock

import pytest


os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:1/test")


def _install_import_stubs() -> None:
    """Keep this runner-boundary test independent of external services."""
    def _stub(name: str):
        sys.modules.setdefault(name, types.ModuleType(name))

    _stub("supabase")
    _stub("cryptography")
    _stub("cryptography.fernet")
    _stub("ap.db")
    _stub("ap.queue")
    _stub("ap.order_monitor")
    _stub("ap.position_sizer")
    _stub("ap.market_intelligence")
    _stub("ap.worker_health")
    _stub("ap_reconciler")
    _stub("ap_recovery")
    _stub("ap.self_healing")

    supabase = sys.modules["supabase"]
    supabase.create_client = getattr(supabase, "create_client", lambda *a, **k: None)
    supabase.Client = getattr(supabase, "Client", type("Client", (), {}))
    fernet = sys.modules["cryptography.fernet"]
    fernet.Fernet = getattr(fernet, "Fernet", type("Fernet", (), {}))
    db = sys.modules["ap.db"]
    db.run_with_retry = getattr(db, "run_with_retry", lambda fn, *a, **k: fn(*a, **k))
    queue = sys.modules["ap.queue"]
    queue.enqueue_signal = getattr(queue, "enqueue_signal", lambda *a, **k: None)
    queue.worker_loop = getattr(queue, "worker_loop", lambda *a, **k: None)
    sys.modules["ap.order_monitor"].APOrderMonitor = getattr(
        sys.modules["ap.order_monitor"], "APOrderMonitor", type("APOrderMonitor", (), {})
    )
    sys.modules["ap.position_sizer"].APPositionSizer = getattr(
        sys.modules["ap.position_sizer"], "APPositionSizer", type("APPositionSizer", (), {})
    )
    sys.modules["ap.market_intelligence"].APEarningsGuard = getattr(
        sys.modules["ap.market_intelligence"], "APEarningsGuard", type("APEarningsGuard", (), {})
    )
    sys.modules["ap.market_intelligence"].APIVRankFilter = getattr(
        sys.modules["ap.market_intelligence"], "APIVRankFilter", type("APIVRankFilter", (), {})
    )
    sys.modules["ap.worker_health"].get_monitor = getattr(
        sys.modules["ap.worker_health"], "get_monitor", lambda: None
    )
    sys.modules["ap.worker_health"].init_monitor = getattr(
        sys.modules["ap.worker_health"], "init_monitor", lambda *a, **k: None
    )
    sys.modules["ap_reconciler"].APBrokerReconciler = getattr(
        sys.modules["ap_reconciler"], "APBrokerReconciler", type("APBrokerReconciler", (), {})
    )
    sys.modules["ap_recovery"].APStartupRecovery = getattr(
        sys.modules["ap_recovery"], "APStartupRecovery", type("APStartupRecovery", (), {})
    )
    sys.modules["ap.self_healing"].get_healer = getattr(
        sys.modules["ap.self_healing"], "get_healer", lambda: None
    )
    sys.modules["ap.self_healing"].init_self_healing = getattr(
        sys.modules["ap.self_healing"], "init_self_healing", lambda *a, **k: None
    )


_STUB_NAMES = {
    "ap",
    "supabase",
    "cryptography",
    "cryptography.fernet",
    "ap.db",
    "ap.queue",
    "ap.order_monitor",
    "ap.position_sizer",
    "ap.market_intelligence",
    "ap.worker_health",
    "ap_reconciler",
    "ap_recovery",
    "ap.self_healing",
}
_PREEXISTING_MODULES = {name: sys.modules.get(name) for name in _STUB_NAMES}
_install_import_stubs()
import client_runner as cr  # noqa: E402

for _name, _module in _PREEXISTING_MODULES.items():
    if _module is None:
        sys.modules.pop(_name, None)
    else:
        sys.modules[_name] = _module


class _PulseEvent:
    """One scheduler tick, then shutdown; avoids sleeping in the test."""

    def __init__(self, results: list[bool] | None = None) -> None:
        self.wait_calls: list[float] = []
        self._wait_count = 0
        self._results = results or [False, True]

    def wait(self, timeout: float) -> bool:
        self.wait_calls.append(timeout)
        result = self._results[min(self._wait_count, len(self._results) - 1)]
        self._wait_count += 1
        return result

    def is_set(self) -> bool:
        return False


def _runner() -> object:
    runner = object.__new__(cr.ClientRunner)
    runner.email = "jasoncosby1@gmail.com"
    runner.mode = "LIVE"
    runner.stopped = _PulseEvent()
    runner.stopping = _PulseEvent()
    runner.failed = _PulseEvent()
    runner.initialized = types.SimpleNamespace(is_set=lambda: True)
    runner.broker = object()
    runner.position_manager = object()
    runner.order_state_machine = object()
    runner.master_control = types.SimpleNamespace(mode="LIVE")
    runner.core = types.SimpleNamespace(entry_watcher=object(), exit_eng=object())
    runner.deferred_retry_calls = 0
    return runner


def test_live_ccep_retry_created_after_startup_is_consumed_by_runtime_scheduler(monkeypatch):
    """The exact incident shape advances without a runner restart."""
    runner = _runner()
    durable_row = {
        "local_order_id": "30a7ec8e-7c7a-4bf3-b7be-d73134c534d1",
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "symbol": "CCEP",
        "contract": "DEFERRED:CCEP",
        "status": "PENDING_TRIGGER",
        "lifecycle_state": "RETRY_WAIT",
        "materialization_status": "RETRY_PENDING",
        "materialization_generation": 7,
        "retry_attempt": 2,
        "broker_order_id": None,
        "submitted_ts": None,
    }
    startup_recovery_completed = True
    recovery_calls: list[dict] = []

    class _Recovery:
        def __init__(self, **kwargs):
            recovery_calls.append(kwargs)
            assert kwargs["client_id"] == durable_row["client_id"]
            assert kwargs["master_control"].mode == durable_row["execution_mode"].upper()

        def recover_deferred_lifecycles(self):
            assert startup_recovery_completed
            assert durable_row["materialization_status"] == "RETRY_PENDING"
            durable_row["materialization_status"] = "SELECTED"
            durable_row["lifecycle_state"] = "BROKER_READY"
            runner.deferred_retry_calls += 1
            return {"deferred_lifecycles_recovered": 1, "errors": []}

    monkeypatch.setattr(cr, "APStartupRecovery", _Recovery)
    monkeypatch.setenv("DEFERRED_RETRY_SCHEDULER_INTERVAL_SEC", "15")

    runner._start_deferred_breach_lifecycle_scheduler()
    runner.deferred_recovery_thread.join(timeout=2)

    assert not runner.deferred_recovery_thread.is_alive()
    assert runner.stopped.wait_calls == [15.0, 15.0]
    assert len(recovery_calls) == 1
    assert runner.deferred_retry_calls == 1
    assert durable_row["materialization_status"] == "SELECTED"
    assert durable_row["lifecycle_state"] == "BROKER_READY"


def test_runtime_scheduler_stops_before_shutdown_authority(monkeypatch):
    runner = _runner()
    runner.stopping = types.SimpleNamespace(is_set=lambda: True)
    tick = MagicMock()
    monkeypatch.setattr(runner, "_run_deferred_breach_lifecycle_recovery", tick)
    monkeypatch.setenv("DEFERRED_RETRY_SCHEDULER_INTERVAL_SEC", "15")

    runner._start_deferred_breach_lifecycle_scheduler()

    assert tick.call_count == 0
    assert getattr(runner, "deferred_recovery_thread", None) is None


def test_runtime_scheduler_survives_one_tick_failure(monkeypatch):
    runner = _runner()
    runner.stopped = _PulseEvent([False, False, True])
    tick = MagicMock(side_effect=[RuntimeError("transient recovery failure"), None])
    monkeypatch.setattr(runner, "_run_deferred_breach_lifecycle_recovery", tick)
    monkeypatch.setenv("DEFERRED_RETRY_SCHEDULER_INTERVAL_SEC", "15")

    runner._start_deferred_breach_lifecycle_scheduler()
    runner.deferred_recovery_thread.join(timeout=2)

    assert not runner.deferred_recovery_thread.is_alive()
    assert tick.call_count == 2
    assert runner.deferred_recovery_errors == 1


@pytest.mark.parametrize(
    ("email", "mode", "symbol"),
    [
        ("mo@example.com", "LIVE", "MO"),
        ("mmm@example.com", "PAPER", "MMM"),
        ("wfc@example.com", "LIVE", "WFC"),
    ],
)
def test_runtime_scheduler_preserves_client_and_mode_isolation(
    monkeypatch, email, mode, symbol
):
    """The same runtime caller handles LIVE/PAPER positive controls."""
    runner = _runner()
    runner.email = email
    runner.mode = mode
    runner.master_control.mode = mode
    recovery_calls: list[dict] = []

    class _Recovery:
        def __init__(self, **kwargs):
            recovery_calls.append(kwargs)

        def recover_deferred_lifecycles(self):
            return {
                "deferred_lifecycles_found": 1,
                "deferred_lifecycles_due": 1,
                "deferred_lifecycles_recovered": 1,
                "symbol": symbol,
                "errors": [],
            }

    monkeypatch.setattr(cr, "APStartupRecovery", _Recovery)
    monkeypatch.setenv("DEFERRED_RETRY_SCHEDULER_INTERVAL_SEC", "15")

    runner._start_deferred_breach_lifecycle_scheduler()
    runner.deferred_recovery_thread.join(timeout=2)

    assert len(recovery_calls) == 1
    assert recovery_calls[0]["client_id"] == email
    assert recovery_calls[0]["master_control"].mode == mode
