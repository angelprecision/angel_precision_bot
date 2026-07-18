"""P0 — Schema attestation and migration runner.

Incident pinned (2026-07-17/18 audit): PRs #361/#362 merged and deployed
while their migrations were never applied to production. The missing
``exit_decision_generation_claims`` table caused silent LIVE exit
suppression and disabled PAPER duplicate-exit fencing. These tests pin:

  1. attestation detects missing tables and missing columns;
  2. strict mode raises; non-strict reports and continues;
  3. LIVE defaults strict, PAPER defaults non-strict, env overrides work;
  4. an unreachable database under strict raises (LIVE must not trade
     against an unverifiable schema);
  5. the migration runner records, orders mixed legacy filenames
     chronologically, baselines without executing, dry-runs by default,
     applies pending files in order, stops at first failure, and refuses
     to run while recorded-file checksum drift exists.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

sys.modules.setdefault("psycopg2", MagicMock())
sys.modules.setdefault("psycopg2.extras", MagicMock())
sys.modules.setdefault("psycopg2.pool", MagicMock())

import ap.schema_attestation as sa
import ap.migration_runner as mr


# ---------------------------------------------------------------------------
# Fake DB layer
# ---------------------------------------------------------------------------

class FakeCursorConn:
    """Minimal stand-in for ap.db._ConnWrapper."""

    def __init__(self, harness):
        self.h = harness

    def execute(self, sql, params=()):
        self.h.executed.append((sql.strip(), tuple(params) if params else ()))
        self.h.last_sql = sql
        if self.h.raise_on and self.h.raise_on in sql:
            raise RuntimeError(f"forced failure on: {self.h.raise_on}")
        return self

    def fetchall(self):
        return self.h.next_rows(self.h.last_sql)

    def fetchone(self):
        rows = self.h.next_rows(self.h.last_sql)
        return rows[0] if rows else None


class FakeDB:
    def __init__(self):
        self.executed = []
        self.last_sql = ""
        self.raise_on = None
        self.information_schema_rows = []
        self.schema_migrations_rows = []

    def next_rows(self, sql):
        if "information_schema.columns" in sql:
            return list(self.information_schema_rows)
        if "FROM schema_migrations" in sql:
            return list(self.schema_migrations_rows)
        return []

    def conn(self):
        harness = self

        class _Ctx:
            def __enter__(self_inner):
                return FakeCursorConn(harness)

            def __exit__(self_inner, *exc):
                return False

        return _Ctx()


@pytest.fixture()
def fake_db(monkeypatch):
    db = FakeDB()
    fake_module = SimpleNamespace(conn=db.conn, run_with_retry=lambda fn, **kw: fn())
    monkeypatch.setitem(sys.modules, "ap.db", fake_module)
    return db


def _cols(table, names):
    return [{"table_name": table, "column_name": n} for n in names]


REQ = {
    "alpha": frozenset({"a1", "a2"}),
    "beta": frozenset({"b1"}),
}


# ---------------------------------------------------------------------------
# Attestation
# ---------------------------------------------------------------------------

def test_attestation_ok_when_all_present(fake_db, monkeypatch):
    monkeypatch.setenv("BOT_MODE", "PAPER")
    fake_db.information_schema_rows = _cols("alpha", ["a1", "a2", "extra"]) + _cols("beta", ["b1"])
    report = sa.attest_schema(required=REQ)
    assert report["ok"] is True
    assert report["missing_tables"] == []
    assert report["missing_columns"] == {}


def test_attestation_detects_missing_table_and_column_nonstrict(fake_db, monkeypatch):
    monkeypatch.setenv("BOT_MODE", "PAPER")
    fake_db.information_schema_rows = _cols("alpha", ["a1"])  # a2 missing, beta absent
    report = sa.attest_schema(required=REQ)
    assert report["ok"] is False
    assert report["missing_tables"] == ["beta"]
    assert report["missing_columns"] == {"alpha": ["a2"]}


def test_attestation_strict_raises_on_missing(fake_db, monkeypatch):
    monkeypatch.setenv("BOT_MODE", "PAPER")
    fake_db.information_schema_rows = _cols("alpha", ["a1", "a2"])  # beta absent
    with pytest.raises(sa.SchemaAttestationError, match="beta"):
        sa.attest_schema(strict=True, required=REQ)


def test_live_mode_defaults_strict(fake_db, monkeypatch):
    monkeypatch.setenv("BOT_MODE", "LIVE")
    monkeypatch.delenv("SCHEMA_ATTESTATION_STRICT", raising=False)
    fake_db.information_schema_rows = []
    with pytest.raises(sa.SchemaAttestationError):
        sa.attest_schema(required=REQ)


def test_paper_mode_defaults_nonstrict(fake_db, monkeypatch):
    monkeypatch.setenv("BOT_MODE", "PAPER")
    monkeypatch.delenv("SCHEMA_ATTESTATION_STRICT", raising=False)
    fake_db.information_schema_rows = []
    report = sa.attest_schema(required=REQ)
    assert report["ok"] is False  # reported, not raised


def test_env_override_forces_strict_in_paper(fake_db, monkeypatch):
    monkeypatch.setenv("BOT_MODE", "PAPER")
    monkeypatch.setenv("SCHEMA_ATTESTATION_STRICT", "1")
    fake_db.information_schema_rows = []
    with pytest.raises(sa.SchemaAttestationError):
        sa.attest_schema(required=REQ)


def test_unreachable_db_raises_under_strict(fake_db, monkeypatch):
    monkeypatch.setenv("BOT_MODE", "LIVE")
    fake_db.raise_on = "information_schema.columns"
    with pytest.raises(sa.SchemaAttestationError, match="could not run"):
        sa.attest_schema(strict=True, required=REQ)


def test_disabled_flag_skips_but_reports(fake_db, monkeypatch):
    monkeypatch.setenv("BOT_MODE", "LIVE")
    monkeypatch.setenv("SCHEMA_ATTESTATION_ENABLED", "0")
    report = sa.attest_schema(required=REQ)
    assert report["skipped"] is True
    assert report["ok"] is True  # skip is explicit, not a silent pass/fail
    monkeypatch.delenv("SCHEMA_ATTESTATION_ENABLED")


def test_production_declaration_covers_claims_table():
    """The exact table whose absence caused the incident must be declared."""
    assert "exit_decision_generation_claims" in sa.REQUIRED_SCHEMA
    assert "generation_key" in sa.REQUIRED_SCHEMA["exit_decision_generation_claims"]
    assert "proof_trades" in sa.REQUIRED_SCHEMA
    assert "performance_taxonomy" in sa.REQUIRED_SCHEMA["proof_trades"]


# ---------------------------------------------------------------------------
# Migration runner
# ---------------------------------------------------------------------------

@pytest.fixture()
def mig_dir(tmp_path):
    d = tmp_path / "migrations"
    d.mkdir()
    (d / "20260717_new_style.sql").write_text("SELECT 2;")
    (d / "2026_05_17_old_style.sql").write_text("SELECT 1;")
    (d / "no_date_last.sql").write_text("SELECT 3;")
    return d


def test_sort_key_orders_mixed_naming_chronologically(mig_dir):
    names = [p.name for p in mr._migration_files(mig_dir)]
    assert names == ["2026_05_17_old_style.sql", "20260717_new_style.sql", "no_date_last.sql"]


def test_status_reports_all_pending_when_ledger_empty(fake_db, mig_dir):
    report = mr.status(mig_dir)
    assert len(report["pending"]) == 3
    assert report["applied"] == [] and report["drifted"] == []


def test_baseline_records_without_executing_sql(fake_db, mig_dir):
    result = mr.baseline(mig_dir)
    assert len(result["baselined"]) == 3
    # No migration body was executed — only ledger DDL/inserts/selects.
    for sql, _ in fake_db.executed:
        assert not sql.startswith("SELECT 1;") and not sql.startswith("SELECT 2;")
    inserts = [s for s, _ in fake_db.executed if s.startswith("INSERT INTO schema_migrations")]
    assert len(inserts) == 3


def test_run_pending_default_is_dry_run(fake_db, mig_dir):
    result = mr.run_pending(directory=mig_dir)
    assert len(result["would_apply"]) == 3
    assert result["applied"] == []
    body_execs = [s for s, _ in fake_db.executed if s in ("SELECT 1;", "SELECT 2;", "SELECT 3;")]
    assert body_execs == []


def test_run_pending_applies_in_order_and_records(fake_db, mig_dir):
    result = mr.run_pending(apply=True, directory=mig_dir)
    assert result["applied"] == [
        "2026_05_17_old_style.sql", "20260717_new_style.sql", "no_date_last.sql",
    ]
    assert result["failed"] is None
    bodies = [s for s, _ in fake_db.executed if s in ("SELECT 1;", "SELECT 2;", "SELECT 3;")]
    assert bodies == ["SELECT 1;", "SELECT 2;", "SELECT 3;"]


def test_run_pending_stops_at_first_failure(fake_db, mig_dir):
    fake_db.raise_on = "SELECT 2;"
    result = mr.run_pending(apply=True, directory=mig_dir)
    assert result["applied"] == ["2026_05_17_old_style.sql"]
    assert result["failed"]["filename"] == "20260717_new_style.sql"
    bodies = [s for s, _ in fake_db.executed if s.startswith("SELECT 3;")]
    assert bodies == []  # third file never ran


def test_checksum_drift_refuses_to_apply(fake_db, mig_dir):
    fake_db.schema_migrations_rows = [{
        "filename": "2026_05_17_old_style.sql",
        "checksum": "not-the-real-checksum",
        "applied_at": None,
        "baselined": False,
    }]
    with pytest.raises(mr.MigrationChecksumDrift, match="old_style"):
        mr.run_pending(apply=True, directory=mig_dir)


def test_startup_hook_disabled_by_default(fake_db, mig_dir, monkeypatch):
    monkeypatch.delenv("ENABLE_MIGRATION_RUNNER", raising=False)
    assert mr.run_pending_on_startup() is None
