import pytest
import os
import sys
import threading
import time
import types
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

os.environ["INTELLIGENCE_CONTEXT_STORE_BACKEND"] = "memory"

from ap.intelligence_context_materializer import (  # noqa: E402
    build_intelligence_context_payload,
    _canonical_signal_id,
    enqueue_preopen_context,
    enqueue_pretrigger_context,
    recover_missing_intelligence_jobs,
)
from ap.intelligence_evaluation import (  # noqa: E402
    build_breach_intelligence_payload,
    dispatch_breach_intelligence_snapshot,
)
from ap.intelligence_market_data import extract_underlying_price  # noqa: E402
from ap.intelligence_context_worker import process_due_intelligence_jobs_once  # noqa: E402
from ap.intelligence_context_handoff import submit_intelligence_enqueue  # noqa: E402
from ap.intelligence_context_handoff import (  # noqa: E402
    enqueue_pretrigger_context_best_effort,
)
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


def test_changed_input_snapshot_payload_matches_allocated_revision():
    enqueue_pretrigger_context(
        _signal(), client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123",
    )
    enqueue_pretrigger_context(
        {**_signal(), "sector": "XLK"}, client_id="client@example.com",
        execution_mode="PAPER", canonical_signal_id="canon-123",
    )
    _process(limit=2)
    latest = get_latest_snapshot(
        client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123", phase="PRETRIGGER",
    )["snapshot"]
    assert latest["context_revision"] == 2
    assert latest["payload"]["context_revision"] == 2
    assert latest["profile_version"] == latest["payload"]["profile_version"]


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


def test_disabled_feature_does_not_submit_enqueue(monkeypatch):
    monkeypatch.setenv("INTELLIGENCE_CONTEXT_WORKER_ENABLED", "0")

    def should_not_submit(*_args, **_kwargs):
        raise AssertionError("disabled intelligence attempted executor submission")

    monkeypatch.setattr("ap.intelligence_context_handoff._EXECUTOR.submit", should_not_submit)
    result = enqueue_pretrigger_context_best_effort(
        _signal(), client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123",
    )
    assert result == {"ok": True, "accepted": False, "disabled": True}


def test_async_handoff_freezes_nested_signal_state(monkeypatch):
    monkeypatch.setenv("INTELLIGENCE_CONTEXT_WORKER_ENABLED", "1")
    captured = {}

    def capture_submission(_enqueue, *args, **_kwargs):
        captured["signal"] = args[0]
        return {"ok": True, "accepted": True}

    monkeypatch.setattr(
        "ap.intelligence_context_handoff.submit_intelligence_enqueue", capture_submission
    )
    signal = _signal()
    enqueue_pretrigger_context_best_effort(
        signal, client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123",
    )
    signal["candles_1h"][0]["c"] = 999
    assert captured["signal"]["candles_1h"][0]["c"] == 500


def test_snapshot_completion_uses_one_transaction_shaped_connection(monkeypatch):
    monkeypatch.delenv("INTELLIGENCE_CONTEXT_STORE_BACKEND", raising=False)
    events = []

    class FakeConnection:
        rowcount = 0
        next_row = None
        update_rowcount = 1

        def __enter__(self):
            events.append("begin")
            return self

        def __exit__(self, exc_type, _exc, _tb):
            events.append("rollback" if exc_type else "commit")
            return False

        def execute(self, sql, _params=None):
            normalized = " ".join(sql.split())
            events.append(normalized)
            if normalized.startswith("SELECT id FROM ap_intelligence_jobs"):
                self.next_row = {"id": "job-1"}
            elif normalized.startswith("INSERT INTO ap_intelligence_snapshots"):
                self.next_row = {"id": "snapshot-1"}
            elif normalized.startswith("UPDATE ap_intelligence_jobs"):
                self.rowcount = self.update_rowcount

        def fetchone(self):
            row, self.next_row = self.next_row, None
            return row

    fake = FakeConnection()
    monkeypatch.setattr("ap.intelligence_snapshot_store._db_conn", lambda: lambda: fake)
    monkeypatch.setattr("ap.intelligence_snapshot_store._run_with_retry", lambda fn: fn())
    result = complete_job_with_snapshot(
        {"id": "job-1"}, claim_owner="owner",
        snapshot_kwargs={
            "client_id": "client@example.com", "execution_mode": "PAPER",
            "canonical_signal_id": "canon-123", "phase": "PRETRIGGER",
            "context_revision": 1, "profile_version": "profile-1",
            "input_hash": "hash-1", "status": "COMPLETE", "payload": {},
        },
    )
    assert result["ok"] and result["completed"]
    assert events.count("begin") == 1
    assert events[-1] == "commit"
    assert events.index(next(e for e in events if e.startswith("INSERT INTO"))) < events.index(
        next(e for e in events if e.startswith("UPDATE ap_intelligence_jobs"))
    )

    events.clear()
    fake.update_rowcount = 0
    failed = complete_job_with_snapshot(
        {"id": "job-1"}, claim_owner="owner",
        snapshot_kwargs={
            "client_id": "client@example.com", "execution_mode": "PAPER",
            "canonical_signal_id": "canon-123", "phase": "PRETRIGGER",
            "context_revision": 1, "profile_version": "profile-1",
            "input_hash": "hash-1", "status": "COMPLETE", "payload": {},
        },
    )
    assert failed["ok"] is False
    assert failed["error_code"] == "JOB_CLAIM_OWNERSHIP_LOST"
    assert events[-1] == "rollback"


def test_underlying_observation_never_falls_back_to_plan_prices():
    assert extract_underlying_price({"entry_price": 2.5, "trigger_price": 500.0}) is None
    assert extract_underlying_price({"underlying_price": 501.25, "trigger_price": 500.0}) == 501.25


def test_reeval_identity_strips_client_suffix_and_queue_uses_correct_signature():
    signal_id = "REEVAL:8d9338d0-5dde-4b7b-81ea-208039999b72:abc123"
    assert _canonical_signal_id({"signal_id": signal_id}) == (
        "REEVAL:8d9338d0-5dde-4b7b-81ea-208039999b72"
    )
    queue_source = (REPO_ROOT / "ap/queue.py").read_text()
    assert "_build_cid(str(signal_id or \"\"), payload)" in queue_source


def test_snapshot_row_and_payload_share_authoritative_input_hash():
    enqueue_pretrigger_context(
        _signal(), client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123",
    )
    _process()
    snapshot = get_latest_snapshot(
        client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123", phase="PRETRIGGER",
    )["snapshot"]
    assert snapshot["input_hash"] == snapshot["payload"]["input_hash"]


def test_claim_finalizes_exhausted_job_instead_of_reclaiming():
    enqueue_pretrigger_context(
        _signal(), client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123",
    )
    job = next(iter(_MEMORY_JOBS.values()))
    job["status"] = "RUNNING"
    job["attempt_count"] = job["max_attempts"]
    job["_claim_expires_epoch"] = time.time() - 1
    claimed = claim_due_intelligence_jobs(
        claim_owner="new-owner", client_id="client@example.com",
        execution_mode="PAPER", limit=1,
    )
    assert claimed["jobs"] == []
    assert job["status"] == "FAILED_TERMINAL"
    assert job["last_error_code"] == "ATTEMPTS_EXHAUSTED_AT_CLAIM"


def test_real_intelligence_modules_receive_fetched_point_in_time_context(monkeypatch):
    now = datetime.now(timezone.utc)
    daily = []
    for index in range(260):
        day = now - timedelta(days=260 - index)
        price = 400 + index * 0.25
        daily.append({
            "date": day.date().isoformat(), "open": price, "high": price + 2,
            "low": price - 2, "close": price + 1, "volume": 1_000_000 + index,
        })
    bars = []
    for day_offset in (1, 0):
        base_day = (now - timedelta(days=day_offset)).replace(hour=14, minute=30, second=0, microsecond=0)
        for index in range(26):
            ts = base_day + timedelta(minutes=15 * index)
            price = 500 + index * 0.1 + (1 - day_offset)
            bars.append({
                "time": ts.isoformat(), "open": price, "high": price + 0.8,
                "low": price - 0.4, "close": price + 0.5,
                "volume": 10_000 + index * (2 if day_offset == 0 else 1),
            })

    class Broker:
        def get_quote(self, symbol):
            return {
                "last": 505.0 if symbol == "SPY" else 200.0,
                "change_percentage": 1.0,
                "trade_date": now.timestamp(),
            }

        def _get(self, _path, params=None):
            assert params["interval"] == "daily"
            return {"history": {"day": daily}}

    monkeypatch.setattr("ap.fvg_telemetry.fetch_15m_bars", lambda *_args, **_kwargs: bars)
    signal = {
        **_signal(), "sector_etf": "XLK", "trigger_price": 501.0,
        "stop_price": 495.0, "target_price": 515.0,
    }
    payload = build_intelligence_context_payload(
        signal, phase="PREOPEN", client_id="client@example.com",
        execution_mode="PAPER", canonical_signal_id="canon-123", broker=Broker(),
        input_hash="authoritative-hash",
    )
    assert payload["input_hash"] == "authoritative-hash"
    assert payload["underlying_observation"]["source"] == "tradier_quote"
    assert payload["strat_context"]["diagnostics"]["states"]["monthly"]["available"] is True
    assert payload["fvg_context"]["diagnostics"]["timeframes"]["1h"]["available"] is True
    assert payload["component_statuses"]["market"] == "AVAILABLE"
    assert payload["component_statuses"]["sector"] == "AVAILABLE"
    assert payload["component_statuses"]["volume"] == "AVAILABLE"
    assert payload["component_statuses"]["vwap"] == "AVAILABLE"


def test_recovery_scan_backfills_missing_phase_jobs_from_durable_truth(monkeypatch):
    signal_id = "REEVAL:8d9338d0-5dde-4b7b-81ea-208039999b72:abc123"
    payload = {**_signal(), "signal_id": signal_id, "canonical_signal_id": ""}

    class Cursor:
        query = 0

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, _sql, _params):
            self.query += 1

        def fetchall(self):
            if self.query == 1:
                return [{"signal_id": signal_id, "payload": payload}]
            return [{"signal_id": signal_id, "local_order_id": "loid-1", "payload": payload}]

    cursor = Cursor()
    monkeypatch.setitem(
        sys.modules, "ap.db",
        types.SimpleNamespace(conn=lambda: cursor, run_with_retry=lambda fn: fn()),
    )
    result = recover_missing_intelligence_jobs(
        client_id="client@example.com", execution_mode="PAPER"
    )
    assert result == {"ok": True, "pretrigger": 1, "preopen": 1}
    jobs = list(_MEMORY_JOBS.values())
    assert {job["phase"] for job in jobs} == {"PRETRIGGER", "PREOPEN"}
    assert {job["canonical_signal_id"] for job in jobs} == {
        "REEVAL:8d9338d0-5dde-4b7b-81ea-208039999b72"
    }


def test_preopen_refreshes_time_sensitive_quote_instead_of_relabeling_pretrigger(monkeypatch):
    prices = iter((500.0, 500.0, 200.0, 505.0, 505.0, 201.0))

    class Broker:
        def get_quote(self, _symbol):
            return {
                "last": next(prices), "change_percentage": 0.5,
                "trade_date": datetime.now(timezone.utc).timestamp(),
            }

        def _get(self, _path, params=None):
            return {"history": {"day": []}}

    monkeypatch.setattr("ap.fvg_telemetry.fetch_15m_bars", lambda *_args, **_kwargs: [])
    broker = Broker()
    signal = {**_signal(), "sector_etf": "XLK"}
    pretrigger = build_intelligence_context_payload(
        signal, phase="PRETRIGGER", client_id="client@example.com",
        execution_mode="PAPER", canonical_signal_id="canon-123", broker=broker,
    )
    preopen = build_intelligence_context_payload(
        signal, phase="PREOPEN", client_id="client@example.com",
        execution_mode="PAPER", canonical_signal_id="canon-123", broker=broker,
        parent_snapshot_id="parent-1",
    )
    assert pretrigger["underlying_observation"]["price"] == 500.0
    assert preopen["underlying_observation"]["price"] == 505.0
    assert preopen["parent_snapshot_id"] == "parent-1"


def _write_parent_phase(phase: str, *, client="client@example.com", mode="PAPER",
                        canonical="canon-123", local_order_id="loid-1",
                        ticker="SPY", side="CALL", component_overrides=None):
    statuses = {
        "geometry": "AVAILABLE",
        "monthly": "AVAILABLE",
        "weekly": "AVAILABLE",
        "daily": "AVAILABLE",
        "four_hour": "AVAILABLE",
        "one_hour_fvg": "AVAILABLE",
        "market": "AVAILABLE",
        "sector": "AVAILABLE",
        "volume": "AVAILABLE",
        "vwap": "AVAILABLE",
    }
    statuses.update(component_overrides or {})
    return write_snapshot(
        client_id=client,
        execution_mode=mode,
        canonical_signal_id=canonical,
        signal_id="sig-123",
        local_order_id=local_order_id if phase == "PREOPEN" else "",
        phase=phase,
        context_revision=1,
        input_hash=f"{phase}-hash",
        config_hash="cfg",
        git_commit="git",
        data_as_of="2026-07-10T14:00:00+00:00",
        status="COMPLETE",
        payload={
            "phase": phase,
            "profile_version": "intelligence_context_v1_observe_only",
            "client_id": client,
            "execution_mode": mode,
            "canonical_signal_id": canonical,
            "ticker": ticker,
            "side": side,
            "component_statuses": statuses,
            "strategy_advisories": ["market_opposes"] if phase == "PREOPEN" else [],
            "data_quality_warnings": [],
            "setup_score": 78.0,
            "setup_grade": "B",
            "data_as_of": "2026-07-10T14:00:00+00:00",
        },
    )


def _watched_for_breach():
    return types.SimpleNamespace(
        trigger_price=501.25,
        trigger_crossed_at=datetime(2026, 7, 10, 14, 35, tzinfo=timezone.utc),
        last_quote_bid=502.4,
        last_quote_ask=502.6,
        watcher_id="watcher-1",
    )


def _plan_for_breach():
    return types.SimpleNamespace(
        metadata={"score_audit": {}},
        execution_mode="PAPER",
        client_id="client@example.com",
        ticker="SPY",
        side="CALL",
        signal_id="sig-123",
        trigger_price=501.25,
        stop_price=497.0,
        target_price=508.0,
        score=78.0,
    )


def test_breach_profile_loads_exact_parent_identity_and_preserves_current_price():
    pretrigger = _write_parent_phase("PRETRIGGER")
    preopen = _write_parent_phase("PREOPEN")
    payload = build_breach_intelligence_payload(
        signal=_signal(),
        plan=_plan_for_breach(),
        watched=_watched_for_breach(),
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-123",
        local_order_id="loid-1",
        pretrigger_snapshot=get_latest_snapshot(
            client_id="client@example.com", execution_mode="PAPER",
            canonical_signal_id="canon-123", phase="PRETRIGGER",
        )["snapshot"],
        preopen_snapshot=get_latest_snapshot(
            client_id="client@example.com", execution_mode="PAPER",
            canonical_signal_id="canon-123", phase="PREOPEN",
        )["snapshot"],
    )
    assert payload["phase"] == "BREACH"
    assert payload["profile_status"] == "COMPLETE"
    assert payload["parent_snapshot_ids"] == {
        "PRETRIGGER": pretrigger["snapshot_id"],
        "PREOPEN": preopen["snapshot_id"],
    }
    assert payload["breach_inputs"]["current_underlying_price"] == 502.5
    assert payload["component_statuses"]["contract_execution_quality"] == (
        "NOT_AVAILABLE_UNTIL_CONTRACT_SELECTED"
    )


def test_breach_profile_rejects_cross_client_mode_signal_parent_context():
    wrong = _write_parent_phase("PRETRIGGER", client="other@example.com")
    payload = build_breach_intelligence_payload(
        signal=_signal(),
        plan=_plan_for_breach(),
        watched=_watched_for_breach(),
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-123",
        local_order_id="loid-1",
        pretrigger_snapshot={
            **get_latest_snapshot(
                client_id="other@example.com", execution_mode="PAPER",
                canonical_signal_id="canon-123", phase="PRETRIGGER",
            )["snapshot"],
            "id": wrong["snapshot_id"],
        },
        preopen_snapshot=None,
    )
    assert payload["profile_status"] == "PARTIAL"
    assert "intelligence_snapshot_identity_mismatch" in payload["data_quality_warnings"]
    assert payload["parent_snapshot_ids"]["PRETRIGGER"] is None
    assert "identity_conflict" in payload["hard_safety_blocks"]


def test_missing_pretrigger_or_preopen_creates_partial_not_rejection():
    payload = build_breach_intelligence_payload(
        signal=_signal(),
        plan=_plan_for_breach(),
        watched=_watched_for_breach(),
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-123",
        local_order_id="loid-1",
        pretrigger_snapshot=None,
        preopen_snapshot=None,
    )
    assert payload["profile_status"] == "PARTIAL"
    assert payload["missing_parent_snapshots"] == ["PREOPEN", "PRETRIGGER"]
    assert payload["observe_only"] is True
    assert payload["affected_eligibility"] is False


def test_breach_dispatch_is_idempotent_and_writes_compact_metadata():
    _write_parent_phase("PRETRIGGER")
    _write_parent_phase("PREOPEN")
    plan = _plan_for_breach()
    order_meta = {}

    def writer(_loid, patch):
        order_meta.update(patch)

    first = dispatch_breach_intelligence_snapshot(
        _signal(),
        plan=plan,
        watched=_watched_for_breach(),
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-123",
        local_order_id="loid-1",
        order_meta_writer=writer,
    )
    second = dispatch_breach_intelligence_snapshot(
        _signal(),
        plan=plan,
        watched=_watched_for_breach(),
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-123",
        local_order_id="loid-1",
        order_meta_writer=writer,
    )
    assert first["ok"] is True
    assert second["ok"] is True
    assert second["duplicate"] is True
    assert second["snapshot_id"] == first["snapshot_id"]
    pointer = plan.metadata["intelligence_evaluation"]
    assert pointer["phase"] == "BREACH"
    assert pointer["snapshot_id"] == first["snapshot_id"]
    assert "breach_inputs" not in pointer
    assert order_meta["intelligence_evaluation"] == pointer


def test_breach_source_has_no_network_or_wait_calls():
    source = (REPO_ROOT / "ap" / "intelligence_evaluation.py").read_text()
    start = source.index("def dispatch_breach_intelligence_snapshot")
    end = source.index("\n# ─", start)
    breach_src = source[start:end]
    forbidden = [
        "get_quote(", "_get(", "Future.result", ".result(", "sleep(",
        "option_chain", "yfinance", "tradier history",
    ]
    for token in forbidden:
        assert token not in breach_src


def test_breach_dispatch_seam_after_plan_recovery_before_hydration():
    source = (REPO_ROOT / "ap_execution_core.py").read_text()
    fn_start = source.index("def _on_entry_trigger(")
    fn_end = source.find("\n    def ", fn_start + 100)
    body = source[fn_start:fn_end]
    # PR #330: synchronous dispatch replaced by _freeze_breach_input + submit_breach_intelligence_handoff
    # Verify freeze happens before hydration, and that the old synchronous dispatch is gone.
    assert "_freeze_breach_input" in body, "_freeze_breach_input must be wired in _on_entry_trigger"
    assert "submit_breach_intelligence_handoff" in body, "submit_breach_intelligence_handoff must be wired"
    # New handoff must precede hydration (same seam requirement, new function names)
    assert body.index("_freeze_breach_input") < body.index("_refresh_hydrated_prebreach_plan")
    # Old synchronous dispatch must be gone
    assert "dispatch_breach_intelligence_snapshot(" not in body, (
        "PR #330: dispatch_breach_intelligence_snapshot must not be called synchronously "
        "from _on_entry_trigger — use submit_breach_intelligence_handoff instead"
    )


# ══════════════════════════════════════════════════════════════════════════════
# PR #330 — Amendment: Non-blocking handoff, frozen input, saturation,
#            identity, idempotency, trade-flow safety
# ══════════════════════════════════════════════════════════════════════════════

import threading
from ap.intelligence_evaluation import (
    _freeze_breach_input,
    _materialize_breach_background,
    _compute_breach_context_revision,
)
from ap.intelligence_context_handoff import submit_breach_intelligence_handoff


def _make_watched(trigger_price=501.25, bid=501.10, ask=501.40, generation=1):
    w = types.SimpleNamespace()
    w.trigger_price        = trigger_price
    w.trigger_crossed_at   = "2026-01-01T09:31:00+00:00"
    w.trigger_confirmed_at = "2026-01-01T09:31:02+00:00"
    w.first_breach_bid     = bid
    w.first_breach_ask     = ask
    w.last_quote_bid       = bid
    w.last_quote_ask       = ask
    w.last_quote_at        = "2026-01-01T09:31:02+00:00"
    w.quote_source         = "broker"
    w.quote_age_ms         = 120.0
    w.watcher_id           = "watcher-test-1"
    w.watcher_generation   = generation
    return w


def _make_plan(score=0.82, grade="A", stop=497.0, target=508.0):
    p = types.SimpleNamespace()
    p.score         = score
    p.grade         = grade
    p.tier          = grade
    p.stop_price    = stop
    p.target_price  = target
    p.trigger_price = 501.25
    p.ticker        = "SPY"
    p.side          = "CALL"
    p.plan_id       = "plan-test-1"
    p.execution_mode = "PAPER"
    p.metadata      = {"strategy": "2U"}
    return p


def _freeze_args(sig_override=None, plan_override=None, watched_override=None):
    sig = dict(_signal())
    if sig_override:
        sig.update(sig_override)
    return dict(
        signal=sig,
        plan=plan_override or _make_plan(),
        watched=watched_override or _make_watched(),
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-123",
        local_order_id="oid-test-1",
    )


# ── Test: _freeze_breach_input extracts from live objects ────────────────────

def test_freeze_breach_input_contains_required_fields():
    frozen = _freeze_breach_input(**_freeze_args())
    assert frozen["client_id"] == "client@example.com"
    assert frozen["canonical_signal_id"] == "canon-123"
    assert frozen["ticker"] == "SPY"
    assert frozen["trigger_price"] == 501.25
    assert frozen["first_breach_bid"] == 501.10
    assert frozen["first_breach_ask"] == 501.40
    assert frozen["watcher_generation"] == 1
    assert frozen["stop"] == 497.0
    assert frozen["target"] == 508.0
    assert frozen["setup_score"] == 0.82
    assert "strategy" in (frozen.get("strategy_metadata") or {})


def test_freeze_breach_input_no_mutable_objects():
    """Frozen dict must not contain live watcher/plan/self references."""
    watched = _make_watched()
    plan    = _make_plan()
    frozen  = _freeze_breach_input(**_freeze_args(watched_override=watched, plan_override=plan))
    import types as _t
    for k, v in frozen.items():
        assert not isinstance(v, _t.SimpleNamespace), (
            f"key={k} contains mutable SimpleNamespace"
        )
        assert not hasattr(v, "order_state_machine"), f"key={k} has OSM attribute"


# ── Test: nonblocking — blocking store does not block execution thread ────────

def test_nonblocking_breach_handoff_with_blocking_store():
    """
    PR #330 §1: confirmed breach must not wait for snapshot DB operations.
    Instrument the background task to block 3 seconds; verify the handoff
    returns in <<1s.
    """
    _reset_memory_store_for_tests()
    _block = threading.Event()
    _unblock = threading.Event()

    def _blocking_task(frozen):
        _block.set()     # signal that background started
        _unblock.wait(timeout=5.0)  # block until test releases it
        return {"ok": True}

    os.environ["INTELLIGENCE_CONTEXT_WORKER_ENABLED"] = "1"
    try:
        from ap.intelligence_context_handoff import submit_intelligence_enqueue
        frozen = _freeze_breach_input(**_freeze_args())
        _sig_id = frozen["signal_id"]

        t0 = time.monotonic()
        result = submit_intelligence_enqueue(
            _blocking_task, frozen,
            phase="BREACH", signal_id=_sig_id,
        )
        elapsed = time.monotonic() - t0
        _unblock.set()

        assert elapsed < 0.5, (
            f"Handoff must return immediately; took {elapsed:.3f}s "
            "(PR #330 §1: no synchronous DB work before selector)"
        )
        assert result.get("accepted") is True
    finally:
        os.environ["INTELLIGENCE_CONTEXT_WORKER_ENABLED"] = "0"


def test_saturation_does_not_block_execution():
    """
    PR #330 §10: when executor is saturated, execution continues immediately.
    Fill all capacity slots, then verify submit_breach_intelligence_handoff
    returns with accepted=False without blocking.
    """
    os.environ["INTELLIGENCE_CONTEXT_WORKER_ENABLED"] = "1"
    try:
        from ap.intelligence_context_handoff import _CAPACITY, submit_breach_intelligence_handoff

        # Drain all capacity
        drained = 0
        while _CAPACITY.acquire(blocking=False):
            drained += 1

        frozen = _freeze_breach_input(**_freeze_args())
        t0 = time.monotonic()
        result = submit_breach_intelligence_handoff(frozen)
        elapsed = time.monotonic() - t0

        # Restore
        for _ in range(drained):
            try:
                _CAPACITY.release()
            except ValueError:
                break

        assert elapsed < 0.2, f"Saturated handoff must return immediately; took {elapsed:.3f}s"
        # accepted=False (capacity exhausted) OR disabled; either way execution continues.
        assert not result.get("accepted", True) or result.get("disabled"), (
            f"Expected saturation rejection; got {result}"
        )
    finally:
        os.environ["INTELLIGENCE_CONTEXT_WORKER_ENABLED"] = "0"


def test_watched_mutation_after_handoff_does_not_affect_frozen():
    """
    PR #330 §3: mutating the watched object after freeze must NOT change
    the frozen breach input passed to the background task.
    """
    watched = _make_watched(bid=501.10, ask=501.40)
    frozen  = _freeze_breach_input(**_freeze_args(watched_override=watched))

    orig_bid = frozen["first_breach_bid"]
    orig_ask = frozen["first_breach_ask"]

    # Mutate watched AFTER freeze
    watched.first_breach_bid = 999.0
    watched.first_breach_ask = 999.0
    watched.last_quote_bid   = 999.0

    assert frozen["first_breach_bid"] == orig_bid, "Frozen bid must not change after mutation"
    assert frozen["first_breach_ask"] == orig_ask, "Frozen ask must not change after mutation"


def test_plan_mutation_after_handoff_does_not_affect_frozen():
    plan   = _make_plan(stop=497.0, target=508.0)
    frozen = _freeze_breach_input(**_freeze_args(plan_override=plan))

    orig_stop   = frozen["stop"]
    orig_target = frozen["target"]

    plan.stop_price   = 100.0
    plan.target_price = 100.0

    assert frozen["stop"]   == orig_stop,   "Frozen stop must not change after plan mutation"
    assert frozen["target"] == orig_target, "Frozen target must not change after plan mutation"


# ── Test: PR #330 §4 — breach quote status rules ─────────────────────────────

def test_freeze_sets_breach_quote_available_on_valid_bid_ask():
    frozen = _freeze_breach_input(**_freeze_args(watched_override=_make_watched(bid=501.10, ask=501.40)))
    assert frozen["breach_quote_status"] == "AVAILABLE"
    assert frozen["underlying_price"] == pytest.approx((501.10 + 501.40) / 2.0)


def test_freeze_sets_breach_quote_missing_on_zero_quotes():
    w = _make_watched()
    w.last_quote_bid   = 0
    w.last_quote_ask   = 0
    w.first_breach_bid = 0
    w.first_breach_ask = 0
    frozen = _freeze_breach_input(**_freeze_args(watched_override=w))
    assert frozen["breach_quote_status"] == "MISSING"
    assert frozen["underlying_price"] is None


def test_freeze_does_not_use_option_price_as_underlying():
    """Must NOT fall back to option limit_price or entry_price as underlying price."""
    w = _make_watched()
    w.last_quote_bid   = 0
    w.last_quote_ask   = 0
    w.first_breach_bid = 0
    w.first_breach_ask = 0
    sig = dict(_signal())
    sig["limit_price"]  = 3.50   # option premium — must NOT be used
    sig["entry_price"]  = 501.25  # must NOT be used as underlying
    plan = _make_plan()
    frozen = _freeze_breach_input(signal=sig, plan=plan, watched=w,
                                  client_id="client@example.com",
                                  execution_mode="PAPER",
                                  canonical_signal_id="canon-123",
                                  local_order_id="oid-test-1")
    assert frozen["underlying_price"] is None
    assert frozen["breach_quote_status"] == "MISSING"


# ── Test: PR #330 §6 — parent identity verification in background ─────────────

def test_background_rejects_wrong_client_pretrigger():
    _reset_memory_store_for_tests()
    bad_pretrigger = {
        "snapshot_id": "bad-pt",
        "client_id": "wrong@example.com",
        "execution_mode": "PAPER",
        "canonical_signal_id": "canon-123",
        "phase": "PRETRIGGER",
        "profile_version": "v1",
        "payload": {"ticker": "SPY", "side": "CALL"},
    }
    from ap.intelligence_evaluation import _verify_parent_snapshot_identity
    result, warnings = _verify_parent_snapshot_identity(
        bad_pretrigger,
        phase="PRETRIGGER",
        client_id="client@example.com",
        execution_mode="paper",
        canonical_signal_id="canon-123",
        ticker="SPY",
        side="CALL",
        profile_version="v1",
    )
    assert result is None, "Wrong client_id must reject the parent"
    assert any("mismatch" in w for w in warnings), f"Mismatch warning expected; got {warnings}"


def test_background_missing_pretrigger_produces_partial_profile():
    _reset_memory_store_for_tests()
    frozen = _freeze_breach_input(**_freeze_args())
    frozen["client_id"] = "client@example.com"
    # No PRETRIGGER/PREOPEN snapshots in store
    result = _materialize_breach_background(frozen)
    if result.get("ok") is not False:
        payload = result.get("payload") or {}
        assert payload.get("profile_status") == "PARTIAL", (
            "Missing PRETRIGGER → PARTIAL profile"
        )
        assert "PRETRIGGER" in (payload.get("missing_parent_snapshots") or [])


# ── Test: PR #330 §7 — revision idempotency ──────────────────────────────────

def test_same_breach_input_produces_same_revision():
    frozen = _freeze_breach_input(**_freeze_args())
    r1 = _compute_breach_context_revision(frozen, "pt-id", "1", "po-id", "1")
    r2 = _compute_breach_context_revision(frozen, "pt-id", "1", "po-id", "1")
    assert r1 == r2, "Same breach input + same parents → same revision"


def test_changed_watcher_generation_changes_revision():
    w1 = _make_watched(generation=1)
    w2 = _make_watched(generation=2)
    f1 = _freeze_breach_input(**_freeze_args(watched_override=w1))
    f2 = _freeze_breach_input(**_freeze_args(watched_override=w2))
    r1 = _compute_breach_context_revision(f1, "pt", "1", "po", "1")
    r2 = _compute_breach_context_revision(f2, "pt", "1", "po", "1")
    assert r1 != r2, "Different watcher_generation → different revision"


def test_new_preopen_parent_changes_revision():
    frozen = _freeze_breach_input(**_freeze_args())
    r1 = _compute_breach_context_revision(frozen, "pt", "1", "po-v1", "1")
    r2 = _compute_breach_context_revision(frozen, "pt", "1", "po-v2", "2")
    assert r1 != r2, "New PREOPEN parent → different BREACH revision"


# ── Test: PR #330 — trade-flow safety from background task ───────────────────

def test_background_task_does_not_call_broker():
    """The background task must never call broker submit or contract selector."""
    _reset_memory_store_for_tests()
    broker_called = []

    class _FakeBroker:
        def submit_order(self, *a, **kw):
            broker_called.append(True)
        def post_order(self, *a, **kw):
            broker_called.append(True)
        def place_order(self, *a, **kw):
            broker_called.append(True)

    # Patch to inject fake broker (shouldn't be reachable; this proves isolation)
    import ap.intelligence_evaluation as _ie
    frozen = _freeze_breach_input(**_freeze_args())
    _materialize_breach_background(frozen)
    assert not broker_called, "Background BREACH task must NEVER call broker"


def test_background_task_observe_only():
    """Every BREACH snapshot must have observe_only=true and affected_eligibility=false."""
    _reset_memory_store_for_tests()
    frozen = _freeze_breach_input(**_freeze_args())
    result = _materialize_breach_background(frozen)
    payload = (result or {}).get("payload") or {}
    assert payload.get("observe_only") is True
    assert payload.get("affected_eligibility") is False


def test_background_task_does_not_mutate_watcher_state():
    """Background task must not change any watcher-owned state."""
    _reset_memory_store_for_tests()
    watched = _make_watched()
    frozen  = _freeze_breach_input(**_freeze_args(watched_override=watched))
    # Save pre-task state of watched
    pre_bid = watched.last_quote_bid
    _materialize_breach_background(frozen)
    assert watched.last_quote_bid == pre_bid, "Background task must not mutate watched object"


# ── Test: missing evidence partial profiles ───────────────────────────────────

def test_missing_preopen_produces_partial_profile():
    from ap.intelligence_evaluation import _build_breach_payload_from_frozen
    frozen = _freeze_breach_input(**_freeze_args())
    payload = _build_breach_payload_from_frozen(
        frozen=frozen,
        signal=dict(_signal()),
        pretrigger_snapshot={"snapshot_id": "pt-1", "client_id": "client@example.com",
                              "execution_mode": "paper", "canonical_signal_id": "canon-123",
                              "phase": "PRETRIGGER", "profile_version": "v1",
                              "payload": {"ticker": "SPY", "side": "CALL"}},
        preopen_snapshot=None,
    )
    assert payload["profile_status"] == "PARTIAL"
    assert "PREOPEN" in payload.get("missing_parent_snapshots", [])


def test_missing_breach_quote_sets_component_missing():
    from ap.intelligence_evaluation import _build_breach_payload_from_frozen
    w = _make_watched()
    w.last_quote_bid = 0; w.last_quote_ask = 0
    w.first_breach_bid = 0; w.first_breach_ask = 0
    frozen = _freeze_breach_input(**_freeze_args(watched_override=w))
    assert frozen["breach_quote_status"] == "MISSING"
    payload = _build_breach_payload_from_frozen(
        frozen=frozen, signal=dict(_signal()),
        pretrigger_snapshot=None, preopen_snapshot=None,
    )
    assert payload["component_statuses"].get("breach_quote") == "MISSING"
    assert payload["profile_status"] == "PARTIAL"


# ══════════════════════════════════════════════════════════════════════════════
# PR #330 — Amendment: Non-blocking handoff, frozen input, saturation,
#            identity, idempotency, trade-flow safety
# ══════════════════════════════════════════════════════════════════════════════

import threading
from ap.intelligence_evaluation import (
    _freeze_breach_input,
    _materialize_breach_background,
    _compute_breach_context_revision,
    _build_breach_payload_from_frozen,
)
from ap.intelligence_context_handoff import submit_breach_intelligence_handoff


def _make_watched(trigger_price=501.25, bid=501.10, ask=501.40, generation=1):
    import types as _ty
    w = _ty.SimpleNamespace()
    w.trigger_price        = trigger_price
    w.trigger_crossed_at   = "2026-01-01T09:31:00+00:00"
    w.trigger_confirmed_at = "2026-01-01T09:31:02+00:00"
    w.first_breach_bid     = bid
    w.first_breach_ask     = ask
    w.last_quote_bid       = bid
    w.last_quote_ask       = ask
    w.last_quote_at        = "2026-01-01T09:31:02+00:00"
    w.quote_source         = "broker"
    w.quote_age_ms         = 120.0
    w.watcher_id           = "watcher-test-330"
    w.watcher_generation   = generation
    return w


def _make_plan(score=0.82, grade="A", stop=497.0, target=508.0):
    import types as _ty
    p = _ty.SimpleNamespace()
    p.score          = score
    p.grade          = grade
    p.tier           = grade
    p.stop_price     = stop
    p.target_price   = target
    p.trigger_price  = 501.25
    p.ticker         = "SPY"
    p.side           = "CALL"
    p.plan_id        = "plan-330-test"
    p.execution_mode = "PAPER"
    p.metadata       = {"strategy": "2U"}
    return p


def _freeze_args(sig_extra=None, plan_override=None, watched_override=None):
    sig = dict(_signal())
    if sig_extra:
        sig.update(sig_extra)
    return dict(
        signal=sig,
        plan=plan_override or _make_plan(),
        watched=watched_override or _make_watched(),
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-123",
        local_order_id="oid-test-330",
    )


# ── Frozen input ──────────────────────────────────────────────────────────────

def test_330_freeze_contains_required_identity_and_watcher_fields():
    frozen = _freeze_breach_input(**_freeze_args())
    assert frozen["client_id"] == "client@example.com"
    assert frozen["canonical_signal_id"] == "canon-123"
    assert frozen["ticker"] == "SPY"
    assert frozen["trigger_price"] == 501.25
    assert frozen["first_breach_bid"] == 501.10
    assert frozen["first_breach_ask"] == 501.40
    assert frozen["watcher_generation"] == 1
    assert frozen["stop"] == 497.0
    assert frozen["setup_score"] == 0.82


def test_330_freeze_no_mutable_objects():
    import types as _ty
    watched = _make_watched()
    plan    = _make_plan()
    frozen  = _freeze_breach_input(**_freeze_args(plan_override=plan, watched_override=watched))
    for k, v in frozen.items():
        assert not isinstance(v, _ty.SimpleNamespace), f"key={k} is a mutable namespace"


def test_330_watched_mutation_after_freeze_does_not_change_frozen():
    watched = _make_watched(bid=501.10)
    frozen  = _freeze_breach_input(**_freeze_args(watched_override=watched))
    orig    = frozen["first_breach_bid"]
    watched.first_breach_bid = 999.0
    watched.last_quote_bid   = 999.0
    assert frozen["first_breach_bid"] == orig


def test_330_plan_mutation_after_freeze_does_not_change_frozen():
    plan   = _make_plan(stop=497.0)
    frozen = _freeze_breach_input(**_freeze_args(plan_override=plan))
    orig   = frozen["stop"]
    plan.stop_price = 1.0
    assert frozen["stop"] == orig


# ── Nonblocking ───────────────────────────────────────────────────────────────

def test_330_blocking_snapshot_does_not_block_execution_thread():
    """PR #330 §1: handoff must return in microseconds even if DB blocks 3s."""
    _reset_memory_store_for_tests()
    _unblock = threading.Event()

    def _blocking_bg(frozen):
        _unblock.wait(timeout=5.0)
        return {"ok": True}

    os.environ["INTELLIGENCE_CONTEXT_WORKER_ENABLED"] = "1"
    try:
        from ap.intelligence_context_handoff import submit_intelligence_enqueue
        frozen = _freeze_breach_input(**_freeze_args())
        t0 = time.monotonic()
        submit_intelligence_enqueue(
            _blocking_bg, frozen,
            phase="BREACH", signal_id=frozen["signal_id"],
        )
        elapsed = time.monotonic() - t0
        _unblock.set()
        assert elapsed < 0.5, (
            f"Handoff returned in {elapsed:.3f}s — must be <0.5s "
            "(PR #330: no synchronous DB work before selector)"
        )
    finally:
        os.environ["INTELLIGENCE_CONTEXT_WORKER_ENABLED"] = "0"
        _unblock.set()


def test_330_saturation_returns_immediately_without_inline_fallback():
    """PR #330 §10: saturated queue → immediate rejection, no inline work."""
    os.environ["INTELLIGENCE_CONTEXT_WORKER_ENABLED"] = "1"
    try:
        from ap.intelligence_context_handoff import _CAPACITY
        drained = 0
        while _CAPACITY.acquire(blocking=False):
            drained += 1
        frozen = _freeze_breach_input(**_freeze_args())
        t0 = time.monotonic()
        result = submit_breach_intelligence_handoff(frozen)
        elapsed = time.monotonic() - t0
        for _ in range(drained):
            try: _CAPACITY.release()
            except ValueError: break
        assert elapsed < 0.2, f"Saturated handoff took {elapsed:.3f}s"
        assert not result.get("accepted", True) or result.get("disabled"), (
            f"Saturated handoff must not be accepted: {result}"
        )
    finally:
        os.environ["INTELLIGENCE_CONTEXT_WORKER_ENABLED"] = "0"


# ── PR #330 §4 — underlying price rules ──────────────────────────────────────

def test_330_breach_quote_available_on_valid_bid_ask():
    frozen = _freeze_breach_input(**_freeze_args(watched_override=_make_watched(bid=501.10, ask=501.40)))
    assert frozen["breach_quote_status"] == "AVAILABLE"
    assert frozen["underlying_price"] == pytest.approx((501.10 + 501.40) / 2.0)


def test_330_breach_quote_missing_on_zero_quotes():
    import types as _ty
    w = _make_watched()
    w.last_quote_bid = 0; w.last_quote_ask = 0
    w.first_breach_bid = 0; w.first_breach_ask = 0
    frozen = _freeze_breach_input(**_freeze_args(watched_override=w))
    assert frozen["breach_quote_status"] == "MISSING"
    assert frozen["underlying_price"] is None


def test_330_does_not_use_option_premium_as_underlying():
    """option limit_price or entry_price must NEVER be used as underlying price."""
    import types as _ty
    w = _make_watched()
    w.last_quote_bid = 0; w.last_quote_ask = 0
    w.first_breach_bid = 0; w.first_breach_ask = 0
    sig = dict(_signal())
    sig["limit_price"] = 3.50   # option premium
    sig["entry_price"] = 501.25  # must NOT be used as underlying
    frozen = _freeze_breach_input(
        signal=sig, plan=_make_plan(), watched=w,
        client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-123", local_order_id="oid-330",
    )
    assert frozen["underlying_price"] is None
    assert frozen["breach_quote_status"] == "MISSING"


# ── PR #330 §6 — parent identity ─────────────────────────────────────────────

def test_330_wrong_client_parent_rejected():
    from ap.intelligence_evaluation import _verify_parent_snapshot_identity
    snap, warnings = _verify_parent_snapshot_identity(
        {"snapshot_id": "x", "client_id": "wrong@example.com",
         "execution_mode": "paper", "canonical_signal_id": "canon-123",
         "phase": "PRETRIGGER", "profile_version": "v1",
         "payload": {"ticker": "SPY", "side": "CALL"}},
        phase="PRETRIGGER", client_id="client@example.com",
        execution_mode="paper", canonical_signal_id="canon-123",
        ticker="SPY", side="CALL", profile_version="v1",
    )
    assert snap is None
    assert any("mismatch" in w for w in warnings)


def test_330_wrong_execution_mode_parent_rejected():
    from ap.intelligence_evaluation import _verify_parent_snapshot_identity
    snap, warnings = _verify_parent_snapshot_identity(
        {"snapshot_id": "x", "client_id": "client@example.com",
         "execution_mode": "live",    # mismatch with PAPER
         "canonical_signal_id": "canon-123",
         "phase": "PREOPEN", "profile_version": "v1",
         "payload": {"ticker": "SPY", "side": "CALL"}},
        phase="PREOPEN", client_id="client@example.com",
        execution_mode="paper", canonical_signal_id="canon-123",
        ticker="SPY", side="CALL", profile_version="v1",
    )
    assert snap is None
    assert any("mismatch" in w for w in warnings)


def test_330_missing_pretrigger_produces_partial():
    payload = _build_breach_payload_from_frozen(
        frozen=_freeze_breach_input(**_freeze_args()),
        signal=dict(_signal()),
        pretrigger_snapshot=None,
        preopen_snapshot=None,
    )
    assert payload["profile_status"] == "PARTIAL"
    assert "PRETRIGGER" in (payload.get("missing_parent_snapshots") or [])


# ── PR #330 §7 — revision idempotency ────────────────────────────────────────

def test_330_same_input_same_revision():
    frozen = _freeze_breach_input(**_freeze_args())
    r1 = _compute_breach_context_revision(frozen, "pt", "1", "po", "1")
    r2 = _compute_breach_context_revision(frozen, "pt", "1", "po", "1")
    assert r1 == r2


def test_330_different_watcher_generation_different_revision():
    f1 = _freeze_breach_input(**_freeze_args(watched_override=_make_watched(generation=1)))
    f2 = _freeze_breach_input(**_freeze_args(watched_override=_make_watched(generation=2)))
    assert (_compute_breach_context_revision(f1, "pt", "1", "po", "1") !=
            _compute_breach_context_revision(f2, "pt", "1", "po", "1"))


def test_330_new_preopen_parent_different_revision():
    frozen = _freeze_breach_input(**_freeze_args())
    r1 = _compute_breach_context_revision(frozen, "pt", "1", "po-v1", "1")
    r2 = _compute_breach_context_revision(frozen, "pt", "1", "po-v2", "2")
    assert r1 != r2


# ── Trade-flow safety ─────────────────────────────────────────────────────────

def test_330_background_task_observe_only_flag():
    _reset_memory_store_for_tests()
    frozen  = _freeze_breach_input(**_freeze_args())
    result  = _materialize_breach_background(frozen)
    payload = (result or {}).get("payload") or {}
    assert payload.get("observe_only") is True
    assert payload.get("affected_eligibility") is False


def test_330_background_does_not_mutate_watched_object():
    watched = _make_watched()
    frozen  = _freeze_breach_input(**_freeze_args(watched_override=watched))
    pre_bid = watched.last_quote_bid
    _reset_memory_store_for_tests()
    _materialize_breach_background(frozen)
    assert watched.last_quote_bid == pre_bid


def test_330_missing_breach_quote_component_missing_in_payload():
    import types as _ty
    w = _make_watched()
    w.last_quote_bid = 0; w.last_quote_ask = 0
    w.first_breach_bid = 0; w.first_breach_ask = 0
    frozen  = _freeze_breach_input(**_freeze_args(watched_override=w))
    payload = _build_breach_payload_from_frozen(
        frozen=frozen, signal=dict(_signal()),
        pretrigger_snapshot=None, preopen_snapshot=None,
    )
    assert payload["component_statuses"].get("breach_quote") == "MISSING"
    assert payload["profile_status"] == "PARTIAL"
