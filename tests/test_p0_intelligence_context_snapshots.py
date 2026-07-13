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
    assert body.index("_recover_plan_for_revalidation") < body.index(
        "dispatch_breach_intelligence_snapshot"
    )
    assert body.index("dispatch_breach_intelligence_snapshot") < body.index(
        "_refresh_hydrated_prebreach_plan"
    )
