from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock


REPO_ROOT = Path(__file__).resolve().parents[1]


def queue_mod():
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    if "ap.queue" in sys.modules:
        importlib.reload(sys.modules["ap.queue"])
    import ap.queue as q  # noqa: WPS433
    return q


def test_source_uses_immediate_osm_cleanup_for_pending_trigger_transition_failure():
    src = (REPO_ROOT / "ap" / "queue.py").read_text()
    assert "WATCH_ARM_ABORTED_PENDING_TRIGGER_TRANSITION_FAILED" in src
    assert "expire_pending_entry" in src
    assert "cancel_pending_entry" in src
    assert 'local_order_id, "ERROR", last_error=_handoff_reason' in src


def test_dispatch_cleans_up_local_order_when_pending_trigger_transition_fails(monkeypatch):
    q = queue_mod()

    plan = SimpleNamespace(
        plan_id="plan-1",
        ticker="AAPL",
        side="CALL",
        trigger_type="breach",
        contracts=1,
        max_position_usd=100.0,
        metadata={},
        contract_symbol="AAPL260619C00200000",
    )
    master_control = SimpleNamespace(
        mode="PAPER",
        _equity_cache_ts=0.0,
        evaluate=lambda payload, client_id: SimpleNamespace(ok=True, plan=plan),
        revalidate_exposure=lambda plan, client_id: SimpleNamespace(ok=True, reason=None),
    )

    osm = MagicMock()
    osm.create_entry_order.return_value = "local-123"
    osm.mark_entry_pending_trigger.return_value = False
    osm.expire_pending_entry.return_value = True

    mark_job = MagicMock()
    monkeypatch.setattr(q, "_mark_job", mark_job)
    monkeypatch.setitem(
        sys.modules,
        "ap.authorization",
        SimpleNamespace(execution_mode_for_broker=lambda _broker: "paper"),
    )
    monkeypatch.setattr(q, "_now_et", lambda: SimpleNamespace(
        date=lambda: SimpleNamespace(isoformat=lambda: "2026-06-15"),
        hour=10,
        minute=0,
    ))
    monkeypatch.setattr(q, "_is_regular_session_et", lambda _dt: True)

    q._dispatch(
        job_id=7,
        client_id="tradefluencehq",
        signal_id="sig-1",
        payload={
            "signal_id": "sig-1",
            "ticker": "AAPL",
            "side": "CALL",
            "score": 80,
            "created_at": "2026-06-15T13:00:00Z",
        },
        master_control=master_control,
        contract_selector=None,
        order_state_machine=osm,
        entry_watcher=MagicMock(),
        broker=None,
    )

    osm.expire_pending_entry.assert_called_once_with(
        "local-123", reason="pending_trigger_transition_failed"
    )
    osm.cancel_pending_entry.assert_not_called()
    osm.transition.assert_not_called()
    mark_job.assert_called_once_with(7, "ERROR", error="pending_trigger_transition_failed")


def test_dispatch_falls_back_to_cancel_then_error_transition(monkeypatch):
    q = queue_mod()

    plan = SimpleNamespace(
        plan_id="plan-2",
        ticker="MSFT",
        side="CALL",
        trigger_type="breach",
        contracts=1,
        max_position_usd=100.0,
        metadata={},
        contract_symbol="MSFT260619C00400000",
    )
    master_control = SimpleNamespace(
        mode="PAPER",
        _equity_cache_ts=0.0,
        evaluate=lambda payload, client_id: SimpleNamespace(ok=True, plan=plan),
        revalidate_exposure=lambda plan, client_id: SimpleNamespace(ok=True, reason=None),
    )

    osm = MagicMock()
    osm.create_entry_order.return_value = "local-456"
    osm.mark_entry_pending_trigger.return_value = False
    osm.expire_pending_entry.return_value = False
    osm.cancel_pending_entry.return_value = False
    osm.transition.return_value = True

    mark_job = MagicMock()
    monkeypatch.setattr(q, "_mark_job", mark_job)
    monkeypatch.setitem(
        sys.modules,
        "ap.authorization",
        SimpleNamespace(execution_mode_for_broker=lambda _broker: "paper"),
    )
    monkeypatch.setattr(q, "_now_et", lambda: SimpleNamespace(
        date=lambda: SimpleNamespace(isoformat=lambda: "2026-06-15"),
        hour=10,
        minute=0,
    ))
    monkeypatch.setattr(q, "_is_regular_session_et", lambda _dt: True)

    q._dispatch(
        job_id=8,
        client_id="tradefluencehq",
        signal_id="sig-2",
        payload={
            "signal_id": "sig-2",
            "ticker": "MSFT",
            "side": "CALL",
            "score": 82,
            "created_at": "2026-06-15T13:00:00Z",
        },
        master_control=master_control,
        contract_selector=None,
        order_state_machine=osm,
        entry_watcher=MagicMock(),
        broker=None,
    )

    osm.expire_pending_entry.assert_called_once_with(
        "local-456", reason="pending_trigger_transition_failed"
    )
    osm.cancel_pending_entry.assert_called_once_with(
        "local-456", reason="pending_trigger_transition_failed"
    )
    osm.transition.assert_called_once_with(
        "local-456", "ERROR", last_error="pending_trigger_transition_failed"
    )
    mark_job.assert_called_once_with(8, "ERROR", error="pending_trigger_transition_failed")
