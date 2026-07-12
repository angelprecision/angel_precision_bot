import os

os.environ["INTELLIGENCE_CONTEXT_STORE_BACKEND"] = "memory"

from ap.intelligence_context_materializer import (  # noqa: E402
    build_intelligence_context_payload,
    enqueue_preopen_context,
    enqueue_pretrigger_context,
)
from ap.intelligence_context_worker import process_due_intelligence_jobs_once  # noqa: E402
from ap.intelligence_snapshot_store import (  # noqa: E402
    _reset_memory_store_for_tests,
    complete_job_with_snapshot,
    get_latest_snapshot,
    identity_key,
    write_snapshot,
)


def _signal():
    return {
        "signal_id": "sig-123",
        "canonical_signal_id": "canon-123",
        "client_id": "client@example.com",
        "execution_mode": "PAPER",
        "ticker": "SPY",
        "side": "CALL",
        "timeframe": "1d",
        "pattern": "2U",
        "trigger_price": 501.25,
        "stop_price": 497.0,
        "target_price": 508.0,
        "underlying_price": 500.0,
        "volume_context": {"current_volume": 1000, "avg_volume": 500},
        "sector": "SPY",
        "candles_1h": [{"t": "2026-07-10T15:00:00Z", "h": 501, "l": 499, "c": 500}],
        "candles_4h": [{"t": "2026-07-10T12:00:00Z", "h": 502, "l": 498, "c": 500}],
    }


def setup_function():
    _reset_memory_store_for_tests()


def test_pretrigger_enqueue_is_idempotent_and_observe_only():
    first = enqueue_pretrigger_context(
        _signal(),
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-123",
    )
    second = enqueue_pretrigger_context(
        _signal(),
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-123",
    )
    assert first["ok"] and first["inserted"]
    assert second["ok"] and second["duplicate"]
    assert first["job_id"] == second["job_id"]


def test_phase_mode_client_identity_keys_are_distinct():
    base = {
        "client_id": "client@example.com",
        "execution_mode": "PAPER",
        "canonical_signal_id": "canon-123",
        "local_order_id": "",
        "phase": "PRETRIGGER",
    }
    assert identity_key(**base) != identity_key(**{**base, "phase": "PREOPEN"})
    assert identity_key(**base) != identity_key(**{**base, "execution_mode": "LIVE"})
    assert identity_key(**base) != identity_key(**{**base, "client_id": "other@example.com"})


def test_worker_completion_writes_snapshot_before_marking_completed():
    enqueue_pretrigger_context(
        _signal(),
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-123",
    )
    result = process_due_intelligence_jobs_once(claim_owner="test-owner", limit=1)
    latest = get_latest_snapshot(
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-123",
        phase="PRETRIGGER",
    )
    assert result["completed"] == 1
    assert latest["snapshot"]["payload"]["observe_only"] is True
    assert latest["snapshot"]["payload"]["affected_eligibility"] is False
    assert latest["snapshot"]["payload"]["compatibility_key"] == "intelligence_evaluation"


def test_preopen_references_latest_pretrigger_parent_and_does_not_mutate_it():
    enqueue_pretrigger_context(
        _signal(),
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-123",
    )
    process_due_intelligence_jobs_once(claim_owner="test-owner", limit=1)
    parent = get_latest_snapshot(
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-123",
        phase="PRETRIGGER",
    )["snapshot"]
    preopen = enqueue_preopen_context(
        _signal(),
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-123",
        local_order_id="loid-1",
    )
    assert preopen["ok"] and preopen["inserted"]
    process_due_intelligence_jobs_once(claim_owner="test-owner-2", limit=1)
    child = get_latest_snapshot(
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-123",
        phase="PREOPEN",
    )["snapshot"]
    assert child["parent_snapshot_id"] == parent["id"]
    assert get_latest_snapshot(
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-123",
        phase="PRETRIGGER",
    )["snapshot"]["id"] == parent["id"]


def test_snapshot_persist_failure_prevents_job_completion(monkeypatch):
    job = {"id": "job-1", "claim_owner": "owner"}

    def fail_write_snapshot(**_kwargs):
        return {"ok": False, "error": "db_down"}

    monkeypatch.setattr("ap.intelligence_snapshot_store.write_snapshot", fail_write_snapshot)
    result = complete_job_with_snapshot(
        job,
        claim_owner="owner",
        snapshot_kwargs={
            "client_id": "client@example.com",
            "execution_mode": "PAPER",
            "canonical_signal_id": "canon-123",
            "phase": "PRETRIGGER",
            "input_hash": "abc",
            "status": "COMPLETE",
            "payload": {},
        },
    )
    assert result["ok"] is False
    assert result["completed"] is False


def test_materializer_does_not_call_broker_or_fabricate_missing_inputs():
    class BrokerShouldNotBeTouched:
        def __getattr__(self, name):
            raise AssertionError(f"broker touched: {name}")

    payload = build_intelligence_context_payload(
        {"signal_id": "s", "canonical_signal_id": "c", "ticker": "SPY", "side": "CALL"},
        phase="PRETRIGGER",
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="c",
    )
    _ = BrokerShouldNotBeTouched()
    assert payload["observe_only"] is True
    assert payload["affected_eligibility"] is False
    assert payload["status"] in {"PARTIAL", "UNAVAILABLE"}
    assert "underlying_price_missing" in payload["data_quality_warnings"]
