import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import psycopg2
import psycopg2.extras
import pytest


DATABASE_URL = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")
pytestmark = pytest.mark.skipif(not DATABASE_URL, reason="disposable PostgreSQL URL not configured")
REPO_ROOT = Path(__file__).resolve().parents[1]


class _Wrapper:
    def __init__(self, connection, cursor):
        self.connection = connection
        self.cursor = cursor

    @property
    def rowcount(self):
        return self.cursor.rowcount

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params)
        return self

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()


@contextmanager
def _conn():
    connection = psycopg2.connect(DATABASE_URL)
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield _Wrapper(connection, cursor)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


@pytest.fixture(autouse=True)
def _database(monkeypatch):
    migration = (REPO_ROOT / "migrations/20260712_intelligence_context_snapshots.sql").read_text()
    connection = psycopg2.connect(DATABASE_URL)
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute(migration)
        cursor.execute("TRUNCATE ap_intelligence_jobs, ap_intelligence_snapshots CASCADE")
    connection.close()
    monkeypatch.delenv("INTELLIGENCE_CONTEXT_STORE_BACKEND", raising=False)
    monkeypatch.setattr("ap.intelligence_snapshot_store._db_conn", lambda: _conn)
    monkeypatch.setattr("ap.intelligence_snapshot_store._run_with_retry", lambda fn: fn())


def _enqueue(input_hash, *, phase="PRETRIGGER", local_order_id=""):
    from ap.intelligence_snapshot_store import enqueue_intelligence_job
    return enqueue_intelligence_job(
        client_id="client@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-1", signal_id="signal-1", phase=phase,
        local_order_id=local_order_id,
        profile_version="profile-1", input_hash=input_hash,
        payload={"signal": {"ticker": "SPY"}},
    )


def test_real_postgres_revision_concurrency_claim_scope_and_atomic_completion():
    first = _enqueue("hash-a")
    with ThreadPoolExecutor(max_workers=2) as executor:
        changed = list(executor.map(lambda _: _enqueue("hash-b"), range(2)))
    assert first["context_revision"] == 1
    assert {item["context_revision"] for item in changed} == {2}
    assert len({item["job_id"] for item in changed}) == 1

    from ap.intelligence_snapshot_store import (
        claim_due_intelligence_jobs,
        complete_job_with_snapshot,
    )
    wrong = claim_due_intelligence_jobs(
        claim_owner="wrong", client_id="client@example.com",
        execution_mode="LIVE", limit=10,
    )
    assert wrong["jobs"] == []
    claimed = claim_due_intelligence_jobs(
        claim_owner="owner", client_id="client@example.com",
        execution_mode="PAPER", limit=1,
    )["jobs"][0]
    result = complete_job_with_snapshot(
        claimed, claim_owner="owner",
        snapshot_kwargs={
            "client_id": "client@example.com", "execution_mode": "PAPER",
            "canonical_signal_id": "canon-1", "signal_id": "signal-1",
            "phase": "PRETRIGGER", "context_revision": claimed["context_revision"],
            "profile_version": "profile-1", "input_hash": claimed["input_hash"],
            "status": "COMPLETE", "payload": {"input_hash": claimed["input_hash"]},
        },
    )
    assert result["ok"] and result["completed"]
    with _conn() as c:
        c.execute(
            "SELECT j.status, j.snapshot_id, s.payload FROM ap_intelligence_jobs j "
            "JOIN ap_intelligence_snapshots s ON s.id=j.snapshot_id WHERE j.id=%s",
            (claimed["id"],),
        )
        row = c.fetchone()
    assert row["status"] == "COMPLETED"
    assert row["snapshot_id"] is not None
    assert row["payload"]["input_hash"] == claimed["input_hash"]


def test_real_postgres_expired_owner_cannot_complete():
    _enqueue("hash-a")
    from ap.intelligence_snapshot_store import claim_due_intelligence_jobs, complete_job_with_snapshot
    claimed = claim_due_intelligence_jobs(
        claim_owner="expired", client_id="client@example.com",
        execution_mode="PAPER", limit=1, lease_seconds=-1,
    )["jobs"][0]
    result = complete_job_with_snapshot(
        claimed, claim_owner="expired",
        snapshot_kwargs={
            "client_id": "client@example.com", "execution_mode": "PAPER",
            "canonical_signal_id": "canon-1", "phase": "PRETRIGGER",
            "context_revision": 1, "profile_version": "profile-1",
            "input_hash": "hash-a", "status": "COMPLETE", "payload": {},
        },
    )
    assert result["error_code"] == "JOB_CLAIM_OWNERSHIP_LOST"
    with _conn() as c:
        c.execute("SELECT COUNT(*)::int AS count FROM ap_intelligence_snapshots")
        assert c.fetchone()["count"] == 0


def test_real_postgres_breach_phase_uses_existing_identity_and_claim_fence():
    result = _enqueue("breach-hash", phase="BREACH", local_order_id="order-1")
    assert result["ok"] and result["inserted"]
    from ap.intelligence_snapshot_store import claim_due_intelligence_jobs
    claimed = claim_due_intelligence_jobs(
        claim_owner="breach-owner", client_id="client@example.com",
        execution_mode="PAPER", limit=1,
    )
    assert claimed["ok"] is True
    assert len(claimed["jobs"]) == 1
    assert claimed["jobs"][0]["phase"] == "BREACH"
    assert claimed["jobs"][0]["local_order_id"] == "order-1"


def test_real_postgres_breach_duplicate_enqueue_is_idempotent():
    first = _enqueue("breach-dup-hash", phase="BREACH", local_order_id="order-dup")
    second = _enqueue("breach-dup-hash", phase="BREACH", local_order_id="order-dup")
    assert first["ok"] and first.get("inserted")
    assert second["ok"] and (second.get("duplicate") or not second.get("inserted"))
    with _conn() as c:
        c.execute(
            "SELECT COUNT(*)::int AS count FROM ap_intelligence_jobs "
            "WHERE phase='BREACH' AND local_order_id=%s AND input_hash=%s",
            ("order-dup", "breach-dup-hash"),
        )
        assert c.fetchone()["count"] == 1


def test_real_postgres_breach_identity_keys_do_not_cross_clients_or_modes():
    a = _enqueue("breach-a", phase="BREACH", local_order_id="loid-a")
    assert a["ok"] and a["inserted"]
    from ap.intelligence_snapshot_store import enqueue_intelligence_job
    other_client = enqueue_intelligence_job(
        client_id="other@example.com", execution_mode="PAPER",
        canonical_signal_id="canon-1", signal_id="signal-1", phase="BREACH",
        local_order_id="loid-a", profile_version="profile-1", input_hash="breach-a",
        payload={"signal": {"ticker": "SPY"}},
    )
    other_mode = enqueue_intelligence_job(
        client_id="client@example.com", execution_mode="LIVE",
        canonical_signal_id="canon-1", signal_id="signal-1", phase="BREACH",
        local_order_id="loid-a", profile_version="profile-1", input_hash="breach-a",
        payload={"signal": {"ticker": "SPY"}},
    )
    assert other_client["ok"] and other_client.get("inserted")
    assert other_mode["ok"] and other_mode.get("inserted")
    with _conn() as c:
        c.execute("SELECT COUNT(*)::int AS count FROM ap_intelligence_jobs WHERE phase='BREACH'")
        assert c.fetchone()["count"] == 3


def test_real_postgres_breach_claim_is_scoped_and_single_owner():
    _enqueue("breach-claim", phase="BREACH", local_order_id="order-claim")
    from ap.intelligence_snapshot_store import claim_due_intelligence_jobs
    first = claim_due_intelligence_jobs(
        claim_owner="owner-a", client_id="client@example.com",
        execution_mode="PAPER", limit=10,
    )
    second = claim_due_intelligence_jobs(
        claim_owner="owner-b", client_id="client@example.com",
        execution_mode="PAPER", limit=10,
    )
    assert first["ok"] and len(first["jobs"]) == 1
    assert second["ok"] and second["jobs"] == []
    assert first["jobs"][0]["phase"] == "BREACH"
