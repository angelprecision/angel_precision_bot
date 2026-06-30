from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch


_REPO = Path(__file__).resolve().parents[1]


def _load_run_lock():
    spec = importlib.util.spec_from_file_location(
        "ap_handoff_run_lock_owner_token_test",
        _REPO / "ap_handoff_run_lock.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _StatefulConn:
    def __init__(self):
        self.row = None
        self.rowcount = 0
        self.force_stale = False

    def execute(self, sql, params=None):
        sql_u = " ".join(sql.split()).upper()
        self.rowcount = 0

        if sql_u.startswith("INSERT INTO PUBLIC.HANDOFF_JOB_LOCKS"):
            run_key, trade_date, job_name, execution_mode, client_scope, triggered_by, owner_token = params
            if self.row is None:
                self.row = {
                    "run_key": run_key,
                    "trade_date": trade_date,
                    "job_name": job_name,
                    "execution_mode": execution_mode,
                    "client_scope": client_scope,
                    "triggered_by": triggered_by,
                    "status": "running",
                    "owner_token": owner_token,
                    "completed_at": None,
                    "result_summary": None,
                    "failed_reason": None,
                }
                self.rowcount = 1
            return

        if "SET STATUS='RUNNING'" in sql_u and "OWNER_TOKEN=%S" in sql_u:
            triggered_by, owner_token, run_key, _stale_seconds = params
            if (
                self.row is not None
                and self.row["run_key"] == run_key
                and (
                    self.row["status"] == "failed"
                    or (self.row["status"] == "running" and self.force_stale)
                )
            ):
                self.row.update({
                    "status": "running",
                    "triggered_by": triggered_by,
                    "completed_at": None,
                    "result_summary": None,
                    "failed_reason": None,
                    "owner_token": owner_token,
                })
                self.force_stale = False
                self.rowcount = 1
            return

        if "SET STATUS='COMPLETED'" in sql_u:
            result_summary, run_key, owner_token = params
            if (
                self.row is not None
                and self.row["run_key"] == run_key
                and self.row["owner_token"] == owner_token
                and self.row["status"] == "running"
            ):
                self.row.update({
                    "status": "completed",
                    "result_summary": result_summary,
                })
                self.rowcount = 1
            return

        if "SET STATUS='FAILED'" in sql_u:
            failed_reason, result_summary, run_key, owner_token = params
            if (
                self.row is not None
                and self.row["run_key"] == run_key
                and self.row["owner_token"] == owner_token
                and self.row["status"] == "running"
            ):
                self.row.update({
                    "status": "failed",
                    "failed_reason": failed_reason,
                    "result_summary": result_summary,
                })
                self.rowcount = 1
            return

        raise AssertionError(f"Unexpected SQL: {sql}")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_stale_owner_cannot_complete_new_owner_run():
    db_state = _StatefulConn()
    db_stub = MagicMock()
    db_stub.conn = lambda: db_state
    db_stub.run_with_retry = lambda fn, *a, **k: fn()

    with patch.dict(sys.modules, {"ap.db": db_stub}):
        mod = _load_run_lock()

        proc_a = mod.try_acquire_run_lock(
            "rk-1",
            job_name="morning_handoff_audit",
            execution_mode="live",
            client_scope="all_runners",
            triggered_by="render_cron",
        )
        assert proc_a["acquired"] is True
        assert proc_a["reclaimed"] is False

        db_state.force_stale = True
        proc_b = mod.try_acquire_run_lock(
            "rk-1",
            job_name="morning_handoff_audit",
            execution_mode="live",
            client_scope="all_runners",
            triggered_by="github_backup",
        )
        assert proc_b["acquired"] is True
        assert proc_b["reclaimed"] is True
        assert proc_b["owner_token"] != proc_a["owner_token"]
        assert db_state.row["status"] == "running"
        assert db_state.row["owner_token"] == proc_b["owner_token"]

        mod.mark_run_lock_completed("rk-1", proc_a["owner_token"], {"owner": "A"})
        assert db_state.row["status"] == "running"
        assert db_state.row["owner_token"] == proc_b["owner_token"]
        assert db_state.row["result_summary"] is None

        mod.mark_run_lock_completed("rk-1", proc_b["owner_token"], {"owner": "B"})
        assert db_state.row["status"] == "completed"
        assert db_state.row["result_summary"] == '{"owner": "B"}'


def test_owner_token_migration_and_sql_guards_exist():
    src = (_REPO / "ap_handoff_run_lock.py").read_text()
    sql = (_REPO / "migrations" / "20260629_handoff_job_locks_owner_token.sql").read_text()

    assert "owner_token" in src
    assert "failed_reason" in src
    assert "AND  owner_token=%s" in src
    assert "HANDOFF_RUN_LOCK_OWNER_MISMATCH" in src
    assert "ADD COLUMN IF NOT EXISTS owner_token TEXT" in sql
    assert "ADD COLUMN IF NOT EXISTS failed_reason TEXT" in sql
    assert "idx_handoff_job_locks_owner" in sql
