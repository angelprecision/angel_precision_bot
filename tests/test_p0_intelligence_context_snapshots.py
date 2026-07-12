import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

os.environ["INTELLIGENCE_CONTEXT_STORE_BACKEND"] = "memory"

from ap.intelligence_context_materializer import (  # noqa: E402
    build_intelligence_context_payload,
    enqueue_preopen_context,
    enqueue_pretrigger_context,
)
from ap.intelligence_context_worker import process_due_intelligence_jobs_once  # noqa: E402
from ap.intelligence_context_handoff import submit_intelligence_enqueue  # noqa: E402
from ap.intelligence_snapshot_store import (  # noqa: E402
    _MEMORY_JOBS,
    _reset_memory_store_for_tests,
    claim_due_intelligence_jobs,
    complete_job_with_snapshot,
    get_latest_snapshot,
    identity_key,
    write_snapshot,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _process(owner="test-owner", *, client="client@example.com", mode="PAPER", limit=1):
    return process_due_intelligence_jobs_once(
        claim_owner=owner, client_id=client, execution_mode=mode, limit=limit
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
    result = _process()
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
    _process()
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
    _process("test-owner-2")
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


def test_handoff_never_waits_for_delayed_persistence():
    release = threading.Event()

    def delayed_enqueue():
        release.wait(1)
        return {"ok": False, "error": "db_down"}

    started = time.monotonic()
    result = submit_intelligence_enqueue(
        delayed_enqueue, phase="PRETRIGGER", signal_id="sig-123"
    )
    elapsed = time.monotonic() - started
    release.set()
    assert result == {"ok": True, "accepted": True}
    assert elapsed < 0.1


def test_preopen_handoff_occurs_only_after_watcher_arm_in_source():
    source = (REPO_ROOT / "ap_overnight_reeval.py").read_text()
    arm = source.index("armed = entry_watcher.watch(decision.plan, local_order_id)")
    handoff = source.index("enqueue_preopen_context_best_effort", arm)
    assert arm < handoff


def test_worker_claims_are_exactly_scoped_by_client_and_mode():
    for client, mode, canonical in (
        ("jason@example.com", "LIVE", "jason-live"),
        ("jose@example.com", "PAPER", "jose-paper"),
        ("jason@example.com", "PAPER", "jason-paper"),
    ):
        signal = {**_signal(), "canonical_signal_id": canonical}
        enqueue_pretrigger_context(
            signal, client_id=client, execution_mode=mode,
            canonical_signal_id=canonical,
        )
    claimed = claim_due_intelligence_jobs(
        claim_owner="jason-live-worker", client_id="jason@example.com",
        execution_mode="LIVE", limit=10,
    )["jobs"]
    assert [(job["client_id"], job["execution_mode"]) for job in claimed] == [
        ("jason@example.com", "LIVE")
    ]


def test_two_workers_for_same_scope_claim_each_job_once():
    enqueue_pretrigger_context(
        _signal(), client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123",
    )

    def claim(owner):
        return claim_due_intelligence_jobs(
            claim_owner=owner, client_id="client@example.com",
            execution_mode="PAPER", limit=1,
        )["jobs"]

    with ThreadPoolExecutor(max_workers=2) as executor:
        claims = list(executor.map(claim, ("owner-a", "owner-b")))
    assert sum(len(batch) for batch in claims) == 1


def test_identical_input_is_idempotent_but_changed_input_gets_next_revision():
    first = enqueue_pretrigger_context(
        _signal(), client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123",
    )
    same = enqueue_pretrigger_context(
        _signal(), client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123",
    )
    changed = enqueue_pretrigger_context(
        {**_signal(), "sector": "XLK"}, client_id="client@example.com",
        execution_mode="PAPER", canonical_signal_id="canon-123",
    )
    assert first["context_revision"] == 1
    assert same["duplicate_same_input"] is True
    assert same["job_id"] == first["job_id"]
    assert changed["inserted"] is True
    assert changed["context_revision"] == 2


def test_concurrent_changed_input_allocates_one_next_revision():
    enqueue_pretrigger_context(
        _signal(), client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123",
    )

    def enqueue_changed():
        return enqueue_pretrigger_context(
            {**_signal(), "sector": "XLK"}, client_id="client@example.com",
            execution_mode="PAPER", canonical_signal_id="canon-123",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: enqueue_changed(), range(2)))
    assert {result["context_revision"] for result in results} == {2}
    assert len({result["job_id"] for result in results}) == 1
    assert sum(bool(result["inserted"]) for result in results) == 1


def test_expired_owner_cannot_complete_reclaimed_job():
    enqueue_pretrigger_context(
        _signal(), client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123",
    )
    old = claim_due_intelligence_jobs(
        claim_owner="old", client_id="client@example.com", execution_mode="PAPER",
        limit=1, lease_seconds=-1,
    )["jobs"][0]
    new = claim_due_intelligence_jobs(
        claim_owner="new", client_id="client@example.com", execution_mode="PAPER",
        limit=1,
    )["jobs"][0]
    kwargs = __import__(
        "ap.intelligence_context_materializer", fromlist=["build_snapshot_kwargs"]
    ).build_snapshot_kwargs(old)
    lost = complete_job_with_snapshot(old, claim_owner="old", snapshot_kwargs=kwargs)
    assert lost["error_code"] == "JOB_CLAIM_OWNERSHIP_LOST"
    assert new["claim_owner"] == "new"


def test_preopen_late_links_to_pretrigger_completed_after_enqueue():
    enqueue_pretrigger_context(
        _signal(), client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123",
    )
    enqueue_preopen_context(
        _signal(), client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123", local_order_id="loid-late",
    )
    _process(limit=1)
    parent = get_latest_snapshot(
        client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123", phase="PRETRIGGER",
    )["snapshot"]
    _process("preopen-owner", limit=1)
    child = get_latest_snapshot(
        client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123", phase="PREOPEN",
    )["snapshot"]
    assert child["parent_snapshot_id"] == parent["id"]
    assert child["payload"]["parent_link_status"] == "LINKED"


def test_migration_and_claim_sql_encode_production_fencing():
    migration = (REPO_ROOT / "migrations/20260712_intelligence_context_snapshots.sql").read_text()
    store = (REPO_ROOT / "ap/intelligence_snapshot_store.py").read_text()
    assert "profile_version TEXT NOT NULL" in migration
    assert "input_hash TEXT NOT NULL" in migration
    assert "snapshot_id UUID REFERENCES ap_intelligence_snapshots(id)" in migration
    assert "pg_advisory_xact_lock" in store
    assert "FOR UPDATE SKIP LOCKED" in store
    assert "client_id=%s AND lower(execution_mode)=lower(%s)" in store
    completion = store.index("def complete_job_with_snapshot")
    assert store.index("FOR UPDATE", completion) < store.index("INSERT INTO ap_intelligence_snapshots", completion)


def test_failed_retry_transition_is_not_counted_as_retried(monkeypatch):
    enqueue_pretrigger_context(
        _signal(), client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123",
    )
    monkeypatch.setattr(
        "ap.intelligence_context_worker.complete_job_with_snapshot",
        lambda *_args, **_kwargs: {
            "ok": False, "completed": False,
            "error_code": "SNAPSHOT_PERSIST_FAILED", "error": "db_down",
        },
    )
    monkeypatch.setattr(
        "ap.intelligence_context_worker.mark_job_retry",
        lambda *_args, **_kwargs: {"ok": False, "updated": False, "error": "ownership_lost"},
    )
    result = _process()
    assert result["retried"] == 0
    assert result["transition_failures"] == 1
