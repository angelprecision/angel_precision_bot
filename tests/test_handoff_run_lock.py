"""
tests/test_handoff_run_lock.py

Tests for ap_handoff_run_lock — server-side DB idempotency for morning jobs.

Proves real enforcement (not log-only job_window_key):
  - First caller of a run_key acquires; subsequent callers skip.
  - A completed lock blocks re-execution (backup correctly skips).
  - A failed lock is reclaimable (backup correctly retries).
  - A stale running lock is reclaimable (crashed run does not block forever).
  - DB errors fail open (degrade to endpoint-level idempotency, not corruption).
  - run_key is deterministic for the same inputs on the same trade_date.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO = Path(__file__).resolve().parents[1]


def _load_run_lock(fake_conn=None):
    """Load ap_handoff_run_lock with ap.db stubbed."""
    import importlib.util

    db_stub = MagicMock()
    if fake_conn is not None:
        db_stub.conn = fake_conn
    db_stub.run_with_retry = lambda fn, *a, **k: fn()

    with patch.dict(sys.modules, {"ap.db": db_stub}):
        spec = importlib.util.spec_from_file_location(
            "ap_handoff_run_lock_shim", _REPO / "ap_handoff_run_lock.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod


_mod = _load_run_lock()


# ---------------------------------------------------------------------------
# run_key determinism
# ---------------------------------------------------------------------------

class TestRunKey:
    def test_deterministic_same_inputs(self):
        td = date(2026, 6, 20)
        k1 = _mod.build_run_key(job_name="morning_handoff_audit",
                                execution_mode="live", client_scope="jason@x",
                                trade_date=td)
        k2 = _mod.build_run_key(job_name="morning_handoff_audit",
                                execution_mode="live", client_scope="jason@x",
                                trade_date=td)
        assert k1 == k2

    def test_differs_by_execution_mode(self):
        td = date(2026, 6, 20)
        live = _mod.build_run_key(job_name="x", execution_mode="live",
                                  client_scope="a", trade_date=td)
        paper = _mod.build_run_key(job_name="x", execution_mode="paper",
                                   client_scope="a", trade_date=td)
        assert live != paper

    def test_differs_by_trade_date(self):
        a = _mod.build_run_key(job_name="x", execution_mode="live",
                               client_scope="a", trade_date=date(2026, 6, 20))
        b = _mod.build_run_key(job_name="x", execution_mode="live",
                               client_scope="a", trade_date=date(2026, 6, 21))
        assert a != b

    def test_key_format(self):
        k = _mod.build_run_key(job_name="morning_handoff_audit",
                               execution_mode="live", client_scope="jason@x",
                               trade_date=date(2026, 6, 20))
        assert k.startswith("morning_job:2026-06-20:morning_handoff_audit:live:")


# ---------------------------------------------------------------------------
# Acquire semantics
# ---------------------------------------------------------------------------

class _FakeConnCtx:
    """Mimics _ConnWrapper used via `with conn() as c: c.execute(...)`.
    c.execute() sets c.rowcount based on the statement type."""
    def __init__(self, insert_rowcount, update_rowcount=0):
        self._insert_rowcount = insert_rowcount
        self._update_rowcount = update_rowcount
        self.rowcount = 0

    def execute(self, sql, params=None):
        s = sql.strip().upper()
        if s.startswith("INSERT"):
            self.rowcount = self._insert_rowcount
        elif s.startswith("UPDATE"):
            self.rowcount = self._update_rowcount

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _conn_factory(insert_rowcount, update_rowcount=0):
    ctx = _FakeConnCtx(insert_rowcount, update_rowcount)
    def factory():
        return ctx
    return factory


class TestAcquire:
    def _call_with_stub(self, factory, **kwargs):
        """Run try_acquire_run_lock with ap.db patched in sys.modules so the
        lazy `from ap.db import conn, run_with_retry` inside the function
        resolves to our stub (not the real module with heavy deps)."""
        db_stub = MagicMock()
        db_stub.conn = factory
        db_stub.run_with_retry = lambda fn, *a, **k: fn()
        with patch.dict(sys.modules, {"ap.db": db_stub}):
            return _mod.try_acquire_run_lock(**kwargs)

    def test_first_caller_wins(self):
        result = self._call_with_stub(
            _conn_factory(insert_rowcount=1),
            run_key="morning_job:2026-06-20:x:live:abc",
            job_name="x", execution_mode="live", client_scope="a",
            triggered_by="render_cron",
        )
        assert result["acquired"] is True
        assert result["reclaimed"] is False
        assert result["run_key"] == "morning_job:2026-06-20:x:live:abc"
        assert result["owner_token"]

    def test_second_caller_skips_when_lock_held(self):
        # INSERT conflicts (rowcount=0), UPDATE reclaim also fails (not stale) → 0
        result = self._call_with_stub(
            _conn_factory(insert_rowcount=0, update_rowcount=0),
            run_key="morning_job:2026-06-20:x:live:abc",
            job_name="x", execution_mode="live", client_scope="a",
            triggered_by="github_backup",
        )
        assert result == {
            "acquired": False,
            "run_key": "morning_job:2026-06-20:x:live:abc",
            "owner_token": None,
            "reason": "lock_held",
        }

    def test_failed_lock_is_reclaimable(self):
        # INSERT conflicts (0), but UPDATE reclaim succeeds (1) because status=failed
        result = self._call_with_stub(
            _conn_factory(insert_rowcount=0, update_rowcount=1),
            run_key="morning_job:2026-06-20:x:live:abc",
            job_name="x", execution_mode="live", client_scope="a",
            triggered_by="github_backup",
        )
        assert result["acquired"] is True
        assert result["reclaimed"] is True
        assert result["owner_token"]

    def test_db_error_fails_open(self):
        """If the lock table is unavailable, acquire returns True (fail open)
        so the morning job still runs — endpoints are themselves idempotent."""
        def _boom_factory():
            raise RuntimeError("db down")
        db_stub = MagicMock()
        db_stub.conn = _boom_factory
        db_stub.run_with_retry = lambda fn, *a, **k: fn()
        with patch.dict(sys.modules, {"ap.db": db_stub}):
            result = _mod.try_acquire_run_lock(
                run_key="k", job_name="x", execution_mode="live", client_scope="a",
            )
        assert result["acquired"] is True  # fail open
        assert result["reason"] == "lock_error_fail_open"
        assert result["owner_token"]


# ---------------------------------------------------------------------------
# Source guards
# ---------------------------------------------------------------------------

class TestSourceGuards:
    def test_insert_uses_on_conflict_do_nothing(self):
        src = (_REPO / "ap_handoff_run_lock.py").read_text()
        assert "ON CONFLICT (run_key) DO NOTHING" in src

    def test_reclaim_allows_failed_and_stale_running(self):
        src = (_REPO / "ap_handoff_run_lock.py").read_text()
        # Reclaim WHERE clause must allow failed OR stale running
        assert "status='failed'" in src
        assert "status='running'" in src
        assert "acquired_at <" in src

    def test_completed_lock_not_reclaimed(self):
        """Source guard: the reclaim UPDATE must not match status='completed'."""
        src = (_REPO / "ap_handoff_run_lock.py").read_text()
        # The reclaim clause references failed and running, never completed
        reclaim_idx = src.find("Reclaim if it is")
        reclaim_region = src[reclaim_idx: reclaim_idx + 800]
        assert "status='completed'" not in reclaim_region

    def test_migration_file_exists(self):
        mig = _REPO / "migrations" / "2026_06_20_handoff_run_locks.sql"
        assert mig.exists()
        sql = mig.read_text()
        assert "CREATE TABLE IF NOT EXISTS public.handoff_run_locks" in sql
        assert "run_key" in sql and "PRIMARY KEY" in sql

    def test_endpoint_uses_run_lock(self):
        """The morning_handoff_audit POST endpoint must use the run-lock."""
        app_src = (_REPO / "app.py").read_text()
        idx = app_src.find("def admin_morning_handoff_audit_post")
        region = app_src[idx: idx + 4000]
        assert "try_acquire_run_lock" in region
        assert "run_lock_held" in region

    def test_endpoint_dry_run_skips_lock(self):
        """dry_run must never take a lock — it must always be runnable."""
        app_src = (_REPO / "app.py").read_text()
        idx = app_src.find("def admin_morning_handoff_audit_post")
        region = app_src[idx: idx + 4000]
        assert "if not dry_run" in region


# ---------------------------------------------------------------------------
# Deploy-requirement documentation guards
# ---------------------------------------------------------------------------

class TestDeployDocs:
    def test_deploy_doc_exists(self):
        doc = _REPO / "DEPLOY_morning_jobs_automation.md"
        assert doc.exists(), "DEPLOY_morning_jobs_automation.md must document the migration requirement"

    def test_deploy_doc_names_migration(self):
        doc = (_REPO / "DEPLOY_morning_jobs_automation.md").read_text()
        assert "2026_06_20_handoff_run_locks.sql" in doc
        assert "fails open" in doc.lower() or "fail open" in doc.lower()

    def test_deploy_doc_covers_env_and_live_flag(self):
        doc = (_REPO / "DEPLOY_morning_jobs_automation.md").read_text()
        assert "BOT_URL" in doc and "SIGNING_SECRET" in doc
        assert "ENABLE_LIVE_AUTO_RELEASE_AFTER_OPEN" in doc

    def test_migration_header_warns_deploy_requirement(self):
        sql = (_REPO / "migrations" / "2026_06_20_handoff_run_locks.sql").read_text()
        assert "DEPLOY REQUIREMENT" in sql
        assert "FAILS OPEN" in sql or "fails open" in sql.lower()
