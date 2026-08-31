"""Source guards for truthful entry-routing and broker-outcome logs.

These assertions are intentionally observability-only. They verify that a
queued signal, broker ownership, and a durable fill remain distinct events.
"""
from __future__ import annotations

from pathlib import Path


_REPO = Path(__file__).resolve().parents[1]
_CLIENT_RUNNER = (_REPO / "client_runner.py").read_text()
_EXECUTION_CORE = (_REPO / "ap_execution_core.py").read_text()
_OSM = (_REPO / "ap" / "order_state_machine.py").read_text()
_QUEUE = (_REPO / "ap" / "queue.py").read_text()


class TestEntryLifecycleLogTruth:
    def test_blocked_route_warning_is_not_broker_outcome_evidence(self):
        idx = _CLIENT_RUNNER.find('"ROUTE_WARN')
        assert idx != -1
        block = _CLIENT_RUNNER[idx: idx + 650]
        assert "route_decision=QUEUED" in block
        assert "broker_submission=NOT_ATTEMPTED" in block
        assert "routing log is not broker-outcome evidence" in block

    def test_execution_core_logs_durable_broker_acceptance_identity(self):
        idx = _EXECUTION_CORE.find('"ENTRY_BROKER_ACCEPTED')
        assert idx != -1
        block = _EXECUTION_CORE[idx: idx + 900]
        for field in (
            "client_id=%s",
            "execution_mode=%s",
            "signal_id=%s",
            "canonical_signal_id=%s",
            "local_order_id=%s",
            "broker_order_id=%s",
            "status=%s",
            "source=execution_core",
        ):
            assert field in block

    def test_immediate_queue_path_uses_the_same_acceptance_event(self):
        idx = _QUEUE.find('"ENTRY_BROKER_ACCEPTED')
        assert idx != -1
        block = _QUEUE[idx: idx + 850]
        assert "canonical_signal_id=%s" in block
        assert "status=%s" in block
        assert "source=queue_immediate" in block

    def test_only_entry_filled_transition_emits_filled_event(self):
        guard = 'if kind.upper() == "ENTRY" and new_status == OrderStatus.FILLED:'
        guard_idx = _OSM.find(guard)
        event_idx = _OSM.find('"ENTRY_BROKER_FILLED')
        assert guard_idx != -1
        assert event_idx > guard_idx
        block = _OSM[event_idx: event_idx + 900]
        for field in (
            "client_id=%s",
            "execution_mode=%s",
            "signal_id=%s",
            "canonical_signal_id=%s",
            "local_order_id=%s",
            "broker_order_id=%s",
            "status=FILLED",
            "filled_qty=%s",
            "fill_price=%s",
            "source=osm_transition",
        ):
            assert field in block
