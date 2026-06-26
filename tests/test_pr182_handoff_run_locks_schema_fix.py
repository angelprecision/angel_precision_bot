"""
tests/test_pr182_handoff_run_locks_schema_fix.py
PR #182 — Fix handoff_run_locks schema collision.

Root cause:
  Two systems used the same table name with incompatible schemas.
  CREATE TABLE IF NOT EXISTS was a silent no-op → column "client_id" does not exist crash.

Fix:
  Conditional migration: rename only when safe, assert final state, raise on unexpected shape.
"""

from __future__ import annotations

from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]


# ─────────────────────────────────────────────────────────────────────────────
# Migration — conditional logic, idempotency, final assertions
# ─────────────────────────────────────────────────────────────────────────────

class TestFixMigration:

    def _sql(self) -> str:
        path = _REPO / "migrations" / "20260625_handoff_run_locks_schema_fix.sql"
        assert path.exists(), (
            "migrations/20260625_handoff_run_locks_schema_fix.sql must exist"
        )
        return path.read_text()

    # ── Rename is conditional, not blind ─────────────────────────────────────

    def test_rename_guarded_by_run_key_check(self):
        """Migration must only rename when run_key is present."""
        sql = self._sql()
        assert "v_run_has_run_key" in sql or "run_key" in sql, (
            "migration must check for run_key before renaming"
        )
        # Must check run_key AND absence of client_id before renaming
        assert "v_run_has_run_key AND NOT v_run_has_client_id" in sql or (
            "run_key" in sql and "NOT" in sql and "client_id" in sql
        ), "rename must be conditional on run_key present AND client_id absent"

    def test_rename_blocked_when_job_locks_exists(self):
        """Must raise an exception if handoff_job_locks already exists."""
        sql = self._sql()
        assert "v_job_locks_exists" in sql or "handoff_job_locks" in sql
        assert "RAISE EXCEPTION" in sql
        # The exception must fire when handoff_job_locks already exists
        assert "already exists" in sql, (
            "migration must raise if handoff_job_locks already exists before rename"
        )

    def test_rename_skipped_when_client_id_present(self):
        """If client_id already in handoff_run_locks, rename is skipped."""
        sql = self._sql()
        assert "v_run_has_client_id" in sql
        # Must have an ELSIF/ELSE branch that skips the rename
        assert "ELSIF v_run_has_client_id" in sql or (
            "ELSIF" in sql and "client_id" in sql
        ), "must skip rename when handoff_run_locks already has client_id"

    def test_unknown_schema_raises_exception(self):
        """Unknown table state (neither run_key nor client_id) must raise, not silently proceed."""
        sql = self._sql()
        # Must have a catch-all ELSE that raises
        assert "RAISE EXCEPTION" in sql
        assert "Unknown schema" in sql or "manual inspection" in sql.lower() or \
               "unknown" in sql.lower(), (
            "migration must raise on unknown schema state, not silently pass"
        )

    # ── Create is idempotent ──────────────────────────────────────────────────

    def test_create_uses_if_not_exists(self):
        """CREATE TABLE must be IF NOT EXISTS — safe to re-run."""
        sql = self._sql()
        assert "CREATE TABLE IF NOT EXISTS public.handoff_run_locks" in sql

    def test_creates_correct_columns(self):
        sql = self._sql()
        for col in ("client_id", "execution_mode", "trading_date", "stage",
                    "status", "last_run_at", "last_success_at", "last_error",
                    "details", "created_at", "updated_at"):
            assert col in sql, f"migration must create column: {col}"

    def test_creates_correct_primary_key(self):
        sql = self._sql()
        assert "PRIMARY KEY (client_id, execution_mode, trading_date, stage)" in sql

    # ── Final assertions ──────────────────────────────────────────────────────

    def test_final_assertion_on_handoff_job_locks_run_key(self):
        """Must assert handoff_job_locks has run_key after migration."""
        sql = self._sql()
        assert "ASSERTION FAILED" in sql
        assert "handoff_job_locks" in sql and "run_key" in sql, (
            "final assertion must check handoff_job_locks has run_key"
        )

    def test_final_assertion_raises_on_missing_client_id(self):
        """Must raise EXCEPTION if handoff_run_locks ends up without client_id."""
        sql = self._sql()
        assert "ASSERTION FAILED: public.handoff_run_locks missing client_id" in sql

    def test_final_assertion_raises_on_missing_stage(self):
        sql = self._sql()
        assert "ASSERTION FAILED: public.handoff_run_locks missing stage" in sql

    def test_final_assertion_raises_on_missing_trading_date(self):
        sql = self._sql()
        assert "ASSERTION FAILED: public.handoff_run_locks missing trading_date" in sql

    def test_final_assertion_raises_on_missing_execution_mode(self):
        sql = self._sql()
        assert "ASSERTION FAILED: public.handoff_run_locks missing execution_mode" in sql

    # ── Transaction wrapping ──────────────────────────────────────────────────

    def test_wrapped_in_transaction(self):
        """The whole migration must be atomic."""
        sql = self._sql()
        lines = [l.strip() for l in sql.splitlines()]
        assert "BEGIN;" in lines, "migration must open BEGIN;"
        assert "COMMIT;" in lines, "migration must COMMIT;"
        begin_idx  = next(i for i, l in enumerate(lines) if l == "BEGIN;")
        commit_idx = next(i for i, l in enumerate(lines) if l == "COMMIT;")
        assert begin_idx < commit_idx, "BEGIN must appear before COMMIT"


# ─────────────────────────────────────────────────────────────────────────────
# ap_handoff_run_lock.py — must target handoff_job_locks
# ─────────────────────────────────────────────────────────────────────────────

class TestHandoffRunLockModule:

    def _src(self) -> str:
        return (_REPO / "ap_handoff_run_lock.py").read_text()

    def test_sql_targets_handoff_job_locks(self):
        assert "handoff_job_locks" in self._src()

    def test_no_sql_insert_into_handoff_run_locks(self):
        src = self._src()
        for pattern in ("INTO public.handoff_run_locks", "INTO handoff_run_locks"):
            assert pattern not in src, f"must not contain: {pattern!r}"

    def test_no_sql_update_handoff_run_locks(self):
        src = self._src()
        for pattern in ("UPDATE public.handoff_run_locks", "UPDATE handoff_run_locks"):
            assert pattern not in src, f"must not contain: {pattern!r}"


# ─────────────────────────────────────────────────────────────────────────────
# ap/morning_handoff.py — unchanged, always correct
# ─────────────────────────────────────────────────────────────────────────────

class TestMorningHandoffModule:

    def _src(self) -> str:
        return (_REPO / "ap" / "morning_handoff.py").read_text()

    def test_still_targets_handoff_run_locks(self):
        assert "handoff_run_locks" in self._src()

    def test_uses_client_id_and_stage(self):
        src = self._src()
        assert "client_id" in src
        assert "stage" in src

    def test_does_not_use_run_key_column(self):
        assert "run_key" not in self._src(), (
            "morning_handoff.py must not reference run_key — that's the job-lock schema"
        )

    def test_does_not_target_handoff_job_locks(self):
        assert "handoff_job_locks" not in self._src()


# ─────────────────────────────────────────────────────────────────────────────
# Invariants
# ─────────────────────────────────────────────────────────────────────────────

def test_systems_target_different_tables():
    lock_src    = (_REPO / "ap_handoff_run_lock.py").read_text()
    handoff_src = (_REPO / "ap" / "morning_handoff.py").read_text()
    assert "handoff_job_locks" in lock_src
    assert "handoff_run_locks" in handoff_src
    assert "INTO public.handoff_run_locks" not in lock_src
    assert "UPDATE public.handoff_run_locks" not in lock_src
    assert "handoff_job_locks" not in handoff_src


def test_fix_migration_is_newer_than_broken_june22_migration():
    june22 = _REPO / "migrations" / "20260622_morning_handoff_run_locks.sql"
    fix    = _REPO / "migrations" / "20260625_handoff_run_locks_schema_fix.sql"
    assert june22.exists(), "June 22 migration must remain (audit trail)"
    assert fix.exists(), "fix migration must exist"
    assert fix.name > june22.name
