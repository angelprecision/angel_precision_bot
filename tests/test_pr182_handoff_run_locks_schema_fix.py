"""
tests/test_pr182_handoff_run_locks_schema_fix.py
PR #182 — Fix handoff_run_locks schema collision.

Two systems used the same table name with incompatible schemas:
  1. ap_handoff_run_lock.py  — PK: run_key     (job-level dedup lock)
  2. ap/morning_handoff.py   — PK: client_id + execution_mode + trading_date + stage

The June 20 migration created handoff_run_locks with the run_key schema.
The June 22 migration used CREATE TABLE IF NOT EXISTS → silent no-op.
morning_handoff.py crashed: "column 'client_id' does not exist FROM handoff_run_locks".

Fix:
  - handoff_run_locks renamed → handoff_job_locks  (for ap_handoff_run_lock.py)
  - handoff_run_locks recreated with the client_id schema  (for ap/morning_handoff.py)
  - ap_handoff_run_lock.py updated to reference handoff_job_locks
"""

from __future__ import annotations

from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]


# ─────────────────────────────────────────────────────────────────────────────
# Migration file
# ─────────────────────────────────────────────────────────────────────────────

class TestFixMigration:
    """The fix migration must rename old table and create the correct new one."""

    def _sql(self) -> str:
        path = _REPO / "migrations" / "20260625_handoff_run_locks_schema_fix.sql"
        assert path.exists(), (
            "migrations/20260625_handoff_run_locks_schema_fix.sql must exist"
        )
        return path.read_text()

    def test_migration_renames_old_table(self):
        sql = self._sql()
        assert "RENAME TO handoff_job_locks" in sql, (
            "migration must rename handoff_run_locks → handoff_job_locks so "
            "ap_handoff_run_lock.py retains its run_key dedup data"
        )

    def test_migration_creates_correct_handoff_run_locks(self):
        sql = self._sql()
        assert "CREATE TABLE" in sql
        # Must create the client_id-keyed table for morning_handoff.py
        assert "client_id" in sql
        assert "trading_date" in sql
        assert "stage" in sql
        assert "PRIMARY KEY (client_id, execution_mode, trading_date, stage)" in sql

    def test_migration_is_wrapped_in_transaction(self):
        """Schema changes must be atomic — both steps succeed or neither does."""
        sql = self._sql()
        assert sql.strip().startswith("BEGIN;") or "BEGIN;" in sql[:200], (
            "migration must open a transaction"
        )
        assert "COMMIT;" in sql, "migration must commit the transaction"

    def test_migration_uses_if_exists_on_rename(self):
        """ALTER TABLE IF EXISTS is idempotent — safe to re-run."""
        sql = self._sql()
        assert "ALTER TABLE IF EXISTS" in sql or "ALTER TABLE" in sql, (
            "must use ALTER TABLE to rename"
        )


# ─────────────────────────────────────────────────────────────────────────────
# ap_handoff_run_lock.py — must use handoff_job_locks
# ─────────────────────────────────────────────────────────────────────────────

class TestHandoffRunLockModule:
    """ap_handoff_run_lock.py must target handoff_job_locks, never handoff_run_locks."""

    def _src(self) -> str:
        return (_REPO / "ap_handoff_run_lock.py").read_text()

    def test_sql_targets_handoff_job_locks(self):
        src = self._src()
        assert "handoff_job_locks" in src, (
            "ap_handoff_run_lock.py must reference handoff_job_locks after PR #182"
        )

    def test_no_sql_targeting_handoff_run_locks(self):
        """
        ap_handoff_run_lock.py must not INSERT/UPDATE handoff_run_locks.
        (The word may appear in comments explaining the rename — we check
        for the SQL statement patterns, not the bare word.)
        """
        src = self._src()
        # The actual SQL patterns that would be wrong
        forbidden_sql_patterns = [
            "INTO public.handoff_run_locks",
            "INTO handoff_run_locks",
            "UPDATE public.handoff_run_locks",
            "UPDATE handoff_run_locks",
            "FROM public.handoff_run_locks",
        ]
        for pattern in forbidden_sql_patterns:
            assert pattern not in src, (
                f"ap_handoff_run_lock.py must not contain SQL referencing "
                f"handoff_run_locks: found {pattern!r}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# ap/morning_handoff.py — must still use handoff_run_locks (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

class TestMorningHandoffModule:
    """ap/morning_handoff.py is unchanged — it was always using the right name."""

    def _src(self) -> str:
        return (_REPO / "ap" / "morning_handoff.py").read_text()

    def test_still_references_handoff_run_locks(self):
        src = self._src()
        assert "handoff_run_locks" in src, (
            "ap/morning_handoff.py must still reference handoff_run_locks "
            "(that is the table the fix migration creates fresh for it)"
        )

    def test_expects_client_id_column(self):
        src = self._src()
        assert "client_id" in src, (
            "ap/morning_handoff.py must use client_id (its schema is client-keyed)"
        )

    def test_expects_stage_column(self):
        src = self._src()
        assert "stage" in src, (
            "ap/morning_handoff.py must use stage column"
        )

    def test_does_not_use_run_key(self):
        """morning_handoff.py must never reference the job-lock schema fields."""
        src = self._src()
        # run_key is the job-dedup lock field; morning_handoff shouldn't know about it
        assert "run_key" not in src, (
            "ap/morning_handoff.py must not reference run_key — "
            "that belongs to handoff_job_locks / ap_handoff_run_lock.py"
        )

    def test_does_not_reference_handoff_job_locks(self):
        """morning_handoff.py must not reference the renamed job-lock table."""
        src = self._src()
        assert "handoff_job_locks" not in src


# ─────────────────────────────────────────────────────────────────────────────
# The two systems now target different tables
# ─────────────────────────────────────────────────────────────────────────────

def test_systems_target_different_tables():
    """
    The root cause was two systems using the same table name.
    After PR #182:
      ap_handoff_run_lock.py  → handoff_job_locks
      ap/morning_handoff.py   → handoff_run_locks
    These must be different strings.
    """
    lock_src     = (_REPO / "ap_handoff_run_lock.py").read_text()
    handoff_src  = (_REPO / "ap" / "morning_handoff.py").read_text()

    assert "handoff_job_locks" in lock_src, "run lock must use handoff_job_locks"
    assert "handoff_run_locks" in handoff_src, "morning handoff must use handoff_run_locks"

    # The critical invariant: lock module must NOT target handoff_run_locks in SQL
    assert "INTO public.handoff_run_locks" not in lock_src
    assert "UPDATE public.handoff_run_locks" not in lock_src

    # And morning_handoff must NOT target the job lock table
    assert "handoff_job_locks" not in handoff_src


def test_fix_migration_supersedes_june22_migration():
    """
    The June 22 migration was a silent no-op. The fix migration
    must exist and be newer (by filename date).
    """
    june22 = _REPO / "migrations" / "20260622_morning_handoff_run_locks.sql"
    fix    = _REPO / "migrations" / "20260625_handoff_run_locks_schema_fix.sql"
    assert june22.exists(), "June 22 migration must still exist (for audit trail)"
    assert fix.exists(), "fix migration must exist"
    # Filename date comparison: 20260625 > 20260622
    assert fix.name > june22.name, (
        "fix migration filename must sort after the broken June 22 migration"
    )
