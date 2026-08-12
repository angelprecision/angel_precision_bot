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
sys.modules.setdefault("ap.db", MagicMock())

import ap.schema_attestation as sa
import ap.migration_runner as mr
import ap.trade_lifecycle_guards as lifecycle_guards


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
        if "CREATE TABLE IF NOT EXISTS schema_migrations" in sql:
            self.h.ledger_exists = True
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
        self.transactions = []
        self.committed_sql = []
        self.rolled_back_sql = []
        self.last_sql = ""
        self.raise_on = None
        self.information_schema_rows = []
        self.schema_migrations_rows = []
        # Existing runner tests model a database whose migration ledger has
        # already been initialized.  No-ledger safety tests opt out explicitly.
        self.ledger_exists = True

    def next_rows(self, sql):
        if "information_schema.columns" in sql:
            return list(self.information_schema_rows)
        if "to_regclass('schema_migrations')" in sql:
            return [{
                "ledger_name": "schema_migrations"
            }] if self.ledger_exists else []
        if "FROM schema_migrations" in sql:
            return list(self.schema_migrations_rows)
        return []

    def conn(self):
        harness = self

        class _Ctx:
            def __enter__(self_inner):
                self_inner.start_index = len(harness.executed)
                return FakeCursorConn(harness)

            def __exit__(self_inner, exc_type, exc, tb):
                tx_sql = [
                    sql for sql, _ in harness.executed[
                        getattr(self_inner, "start_index", len(harness.executed)):
                    ]
                ]
                harness.transactions.append({"committed": exc_type is None, "sql": tx_sql})
                if exc_type is None:
                    harness.committed_sql.extend(tx_sql)
                else:
                    harness.rolled_back_sql.extend(tx_sql)
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
    assert {
        "exit_local_order_id",
        "broker_exit_order_id",
        "broker_exit_fill_ts",
        "broker_exit_filled_qty",
    } <= sa.REQUIRED_SCHEMA["proof_trades"]


def test_production_declaration_covers_trigger_provenance_source_column():
    """The rollout SQL reads canonical_signal_id from the durable orders row."""
    assert "canonical_signal_id" in sa.REQUIRED_SCHEMA["orders"]


def test_lifecycle_manifest_records_healthy_schema_as_installed(monkeypatch):
    schema_module = SimpleNamespace(
        attest_schema=lambda strict=False: {
            "ok": True,
            "missing_tables": [],
            "missing_columns": {},
            "skipped": False,
            "error": None,
        }
    )
    guard_module = SimpleNamespace(install=lambda: None)
    monkeypatch.setitem(sys.modules, "ap.schema_attestation", schema_module)
    monkeypatch.setattr(
        lifecycle_guards,
        "_GUARDS",
        (("canonical_exit_fill_truth", "ap.fake_guard", "install", True),),
    )
    monkeypatch.setattr(lifecycle_guards.importlib, "import_module", lambda _name: guard_module)
    monkeypatch.setattr(lifecycle_guards, "_generation_claims_table_exists", lambda: True)

    manifest = lifecycle_guards.install_trade_lifecycle_guards()
    ok, diagnostics = lifecycle_guards.lifecycle_guard_preflight("live")

    assert manifest["_schema_attestation"]["status"] == "installed"
    assert ok is True
    assert "_schema_attestation" not in diagnostics["missing_required_guards"]


def test_lifecycle_preflight_blocks_failed_schema_attestation(monkeypatch):
    schema_module = SimpleNamespace(
        attest_schema=lambda strict=False: {
            "ok": False,
            "missing_tables": ["positions"],
            "missing_columns": {"positions": ["execution_mode"]},
            "skipped": False,
            "error": None,
        }
    )
    guard_module = SimpleNamespace(install=lambda: None)
    monkeypatch.setitem(sys.modules, "ap.schema_attestation", schema_module)
    monkeypatch.setattr(
        lifecycle_guards,
        "_GUARDS",
        (("canonical_exit_fill_truth", "ap.fake_guard", "install", True),),
    )
    monkeypatch.setattr(lifecycle_guards.importlib, "import_module", lambda _name: guard_module)
    monkeypatch.setattr(lifecycle_guards, "_generation_claims_table_exists", lambda: True)

    manifest = lifecycle_guards.install_trade_lifecycle_guards()
    ok, diagnostics = lifecycle_guards.lifecycle_guard_preflight("live")

    assert manifest["_schema_attestation"]["status"] == "failed"
    assert ok is False
    assert "_schema_attestation" in diagnostics["missing_required_guards"]


def test_lifecycle_preflight_blocks_schema_attestation_exception(monkeypatch):
    def _boom(strict=False):
        raise RuntimeError("db unavailable")

    guard_module = SimpleNamespace(install=lambda: None)
    monkeypatch.setitem(sys.modules, "ap.schema_attestation", SimpleNamespace(attest_schema=_boom))
    monkeypatch.setattr(
        lifecycle_guards,
        "_GUARDS",
        (("canonical_exit_fill_truth", "ap.fake_guard", "install", True),),
    )
    monkeypatch.setattr(lifecycle_guards.importlib, "import_module", lambda _name: guard_module)
    monkeypatch.setattr(lifecycle_guards, "_generation_claims_table_exists", lambda: True)

    manifest = lifecycle_guards.install_trade_lifecycle_guards()
    ok, diagnostics = lifecycle_guards.lifecycle_guard_preflight("live")

    assert manifest["_schema_attestation"]["status"] == "attestation_error"
    assert ok is False
    assert "_schema_attestation" in diagnostics["missing_required_guards"]


def test_lifecycle_preflight_keeps_other_required_install_failures_blocking(monkeypatch):
    schema_module = SimpleNamespace(
        attest_schema=lambda strict=False: {
            "ok": True,
            "missing_tables": [],
            "missing_columns": {},
            "skipped": False,
            "error": None,
        }
    )
    def _boom():
        raise RuntimeError("install failed")

    monkeypatch.setitem(sys.modules, "ap.schema_attestation", schema_module)
    monkeypatch.setattr(
        lifecycle_guards,
        "_GUARDS",
        (("canonical_exit_fill_truth", "ap.fake_guard", "install", True),),
    )
    monkeypatch.setattr(lifecycle_guards.importlib, "import_module", lambda _name: SimpleNamespace(install=_boom))
    monkeypatch.setattr(lifecycle_guards, "_generation_claims_table_exists", lambda: True)

    ok, diagnostics = lifecycle_guards.lifecycle_guard_preflight("live")

    assert ok is False
    assert diagnostics["missing_required_guards"] == ["canonical_exit_fill_truth"]


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


def test_baseline_records_without_executing_sql(fake_db, mig_dir, monkeypatch):
    # Attestation must pass for baseline to proceed; mock it to return ok.
    monkeypatch.setattr(sa, "attest_schema", lambda **kw: {"ok": True, "skipped": False})
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


def test_unqualified_apply_refuses_without_existing_ledger(fake_db, mig_dir):
    """The legacy apply command cannot replay history into an unknown DB."""
    fake_db.ledger_exists = False
    with pytest.raises(mr.MigrationLedgerRequired, match="does not exist"):
        mr.run_pending(apply=True, directory=mig_dir)

    assert fake_db.ledger_exists is False
    assert [sql for sql, _ in fake_db.executed if sql in {
        "SELECT 1;", "SELECT 2;", "SELECT 3;"
    }] == []


def test_targeted_migration_refuses_without_existing_ledger(fake_db, mig_dir):
    """A targeted rollout must not create a ledger or replay history."""
    fake_db.ledger_exists = False
    with pytest.raises(mr.MigrationLedgerRequired, match="does not exist"):
        mr.run_pending(
            apply=True,
            directory=mig_dir,
            only="20260717_new_style.sql",
        )

    assert fake_db.ledger_exists is False
    assert [sql for sql, _ in fake_db.executed if sql in {
        "SELECT 1;", "SELECT 2;", "SELECT 3;"
    }] == []


def test_targeted_migration_refuses_when_another_file_is_pending(fake_db, mig_dir):
    """The operator must prove the base set was recorded before targeting one file."""
    fake_db.ledger_exists = True
    with pytest.raises(mr.MigrationTargetError, match="other files are pending"):
        mr.run_pending(
            apply=True,
            directory=mig_dir,
            only="20260717_new_style.sql",
        )

    assert [sql for sql, _ in fake_db.executed if sql in {
        "SELECT 1;", "SELECT 2;", "SELECT 3;"
    }] == []
    assert _ledger_inserts(fake_db) == []


def test_targeted_migration_applies_only_named_pending_file(fake_db, mig_dir):
    """A proven single pending target executes and records only that file."""
    fake_db.ledger_exists = True
    old_path = mig_dir / "2026_05_17_old_style.sql"
    fake_db.schema_migrations_rows = [{
        "filename": old_path.name,
        "checksum": mr._sha256(old_path.read_text()),
        "applied_at": None,
        "baselined": True,
    }, {
        "filename": "no_date_last.sql",
        "checksum": mr._sha256((mig_dir / "no_date_last.sql").read_text()),
        "applied_at": None,
        "baselined": True,
    }]

    result = mr.run_pending(
        apply=True,
        directory=mig_dir,
        only="20260717_new_style.sql",
    )

    assert result["applied"] == ["20260717_new_style.sql"]
    bodies = [sql for sql, _ in fake_db.executed if sql in {
        "SELECT 1;", "SELECT 2;", "SELECT 3;"
    }]
    assert bodies == ["SELECT 2;"]
    assert len(_ledger_inserts(fake_db)) == 1


def test_startup_hook_disabled_by_default(fake_db, mig_dir, monkeypatch):
    monkeypatch.delenv("ENABLE_MIGRATION_RUNNER", raising=False)
    assert mr.run_pending_on_startup() is None


# ---------------------------------------------------------------------------
# Amendment tests — Blocker 1: baseline must not certify unapplied migrations
# ---------------------------------------------------------------------------

def _sa_raise(**kw):
    raise sa.SchemaAttestationError("attestation failed: missing_tables=['missing_table']")


def _sa_ok(**kw):
    return {"ok": True, "skipped": False, "missing_tables": [], "missing_columns": {}}


def test_baseline_raises_on_missing_table_writes_zero_rows(fake_db, mig_dir, monkeypatch):
    """A missing table must cause baseline to raise and write zero ledger rows."""
    monkeypatch.setattr(sa, "attest_schema", _sa_raise)
    with pytest.raises(mr.BaselineAttestationError, match="attestation failed"):
        mr.baseline(mig_dir)
    inserts = [s for s, _ in fake_db.executed if "INSERT INTO schema_migrations" in s]
    assert inserts == [], "No ledger rows must be written when attestation fails"


def test_baseline_raises_on_missing_column_writes_zero_rows(fake_db, mig_dir, monkeypatch):
    """A missing column (non-table-level gap) must also raise and write zero rows."""
    def _col_missing(**kw):
        raise sa.SchemaAttestationError(
            "attestation failed: missing_columns={'proof_trades': ['performance_taxonomy']}"
        )
    monkeypatch.setattr(sa, "attest_schema", _col_missing)
    with pytest.raises(mr.BaselineAttestationError):
        mr.baseline(mig_dir)
    inserts = [s for s, _ in fake_db.executed if "INSERT INTO schema_migrations" in s]
    assert inserts == []


def test_baseline_raises_when_db_unreachable_writes_zero_rows(fake_db, mig_dir, monkeypatch):
    """An unreachable database under strict attestation must raise and write zero rows."""
    def _db_unreachable(**kw):
        raise sa.SchemaAttestationError("Schema attestation could not run against the database")
    monkeypatch.setattr(sa, "attest_schema", _db_unreachable)
    with pytest.raises(mr.BaselineAttestationError, match="could not run|attestation failed"):
        mr.baseline(mig_dir)
    inserts = [s for s, _ in fake_db.executed if "INSERT INTO schema_migrations" in s]
    assert inserts == []


def test_baseline_passes_when_attestation_ok(fake_db, mig_dir, monkeypatch):
    """When attestation passes, baseline records all files normally."""
    monkeypatch.setattr(sa, "attest_schema", _sa_ok)
    result = mr.baseline(mig_dir)
    assert len(result["baselined"]) == 3
    inserts = [s for s, _ in fake_db.executed if "INSERT INTO schema_migrations" in s]
    assert len(inserts) == 3


def test_baseline_idempotent_when_attestation_passes(fake_db, mig_dir, monkeypatch):
    """Calling baseline twice records 3 files the first time and 0 the second."""
    monkeypatch.setattr(sa, "attest_schema", _sa_ok)
    # Simulate already-recorded files on second call by pre-populating the ledger.
    first = mr.baseline(mig_dir)
    assert len(first["baselined"]) == 3
    # Simulate subsequent status: all files now appear in schema_migrations_rows.
    fake_db.schema_migrations_rows = [
        {"filename": p.name, "checksum": mr._sha256(p.read_text()), "applied_at": None, "baselined": True}
        for p in mr._migration_files(mig_dir)
    ]
    second = mr.baseline(mig_dir)
    assert second["baselined"] == [], "Second baseline must be a no-op"


def test_baseline_force_bypasses_attestation_logs_critical(fake_db, mig_dir, monkeypatch, caplog):
    """force=True bypasses attestation and emits a CRITICAL warning."""
    # attest_schema would fail if called — but with force=True it must NOT be called.
    monkeypatch.setattr(sa, "attest_schema", _sa_raise)
    import logging
    with caplog.at_level(logging.CRITICAL, logger="ap.migration_runner"):
        result = mr.baseline(mig_dir, force=True)
    assert len(result["baselined"]) == 3, "force=True must still record files"
    assert any("FORCE" in rec.message for rec in caplog.records), (
        "CRITICAL warning must be emitted when force=True"
    )


def test_baseline_force_is_not_the_default(mig_dir):
    """baseline() without force must not accept attestation bypass implicitly."""
    import inspect
    sig = inspect.signature(mr.baseline)
    force_param = sig.parameters.get("force")
    assert force_param is not None, "baseline must have a force parameter"
    assert force_param.default is False, "force must default to False"


# ---------------------------------------------------------------------------
# Amendment tests — Blocker 2: migration SQL and ledger must be atomic
# ---------------------------------------------------------------------------

def test_migration_sql_failure_writes_no_ledger_row(fake_db, mig_dir):
    """If migration SQL fails, no schema_migrations row must be written."""
    fake_db.raise_on = "SELECT 1;"  # first migration body fails
    result = mr.run_pending(apply=True, directory=mig_dir)
    assert result["failed"]["filename"] == "2026_05_17_old_style.sql"
    inserts = [s for s, _ in fake_db.executed if "INSERT INTO schema_migrations" in s]
    assert inserts == [], "Ledger INSERT must not run when migration SQL fails"


def test_ledger_insert_failure_is_captured_as_failed_migration(fake_db, mig_dir):
    """If the ledger INSERT fails, the migration must appear in result['failed']
    and no further migrations must run — simulating transaction rollback."""
    fake_db.raise_on = "INSERT INTO schema_migrations"
    result = mr.run_pending(apply=True, directory=mig_dir)
    # The first migration body ran but the INSERT failed.
    assert result["failed"] is not None, "Failed INSERT must be captured in result['failed']"
    assert result["applied"] == [], "No migration must appear in applied when INSERT fails"
    # No third-file execution since we stopped at first failure.
    third = [s for s, _ in fake_db.executed if "SELECT 3;" in s]
    assert third == []
    assert "SELECT 1;" in fake_db.rolled_back_sql
    assert "SELECT 1;" not in fake_db.committed_sql


def test_successful_migration_writes_both_sql_and_ledger_in_same_pass(fake_db, mig_dir):
    """Both the body SQL and the ledger INSERT must appear in executed for each file."""
    result = mr.run_pending(apply=True, directory=mig_dir)
    assert result["applied"] == [
        "2026_05_17_old_style.sql", "20260717_new_style.sql", "no_date_last.sql",
    ]
    bodies = [s for s, _ in fake_db.executed if s in ("SELECT 1;", "SELECT 2;", "SELECT 3;")]
    assert bodies == ["SELECT 1;", "SELECT 2;", "SELECT 3;"]
    inserts = [s for s, _ in fake_db.executed if "INSERT INTO schema_migrations" in s]
    # One ledger INSERT per applied file — same transaction, not a separate call.
    assert len(inserts) == 3
    migration_txs = [
        tx for tx in fake_db.transactions
        if any(sql in ("SELECT 1;", "SELECT 2;", "SELECT 3;") for sql in tx["sql"])
    ]
    assert len(migration_txs) == 3
    for tx in migration_txs:
        assert tx["committed"] is True
        assert any("INSERT INTO schema_migrations" in sql for sql in tx["sql"])


def test_recorded_migration_not_re_executed_on_restart(fake_db, mig_dir):
    """A migration already in schema_migrations must not appear in would_apply."""
    fake_db.schema_migrations_rows = [{
        "filename": "2026_05_17_old_style.sql",
        "checksum": mr._sha256((mig_dir / "2026_05_17_old_style.sql").read_text()),
        "applied_at": None,
        "baselined": False,
    }]
    report = mr.status(mig_dir)
    assert "2026_05_17_old_style.sql" not in report["pending"]
    assert "2026_05_17_old_style.sql" in report["applied"]
    result = mr.run_pending(apply=True, directory=mig_dir)
    executed_bodies = [s for s, _ in fake_db.executed if "SELECT 1;" in s]
    assert executed_bodies == [], "Already-recorded migration must not re-execute"


def test_failure_on_N_prevents_N_plus_1_and_writes_no_ledger_for_either(fake_db, mig_dir):
    """Failure on migration N stops the run; N+1 must not execute or be recorded."""
    fake_db.raise_on = "SELECT 2;"  # second migration (N) fails
    result = mr.run_pending(apply=True, directory=mig_dir)
    assert result["applied"] == ["2026_05_17_old_style.sql"]
    assert result["failed"]["filename"] == "20260717_new_style.sql"
    third_bodies = [s for s, _ in fake_db.executed if "SELECT 3;" in s]
    assert third_bodies == [], "Migration N+1 must not execute after failure on N"
    # Ledger: only first migration's INSERT should appear.
    inserts = [s for s, _ in fake_db.executed if "INSERT INTO schema_migrations" in s]
    assert len(inserts) == 1, "Only the successful migration before failure may have a ledger row"


# ---------------------------------------------------------------------------
# Amendment tests — transaction-control guard (audit blocker)
#
# Migration files must not contain BEGIN/COMMIT/ROLLBACK/START TRANSACTION.
# run_pending() wraps each file in its own transaction so that migration SQL
# and the schema_migrations INSERT commit atomically.  A file that contains
# its own COMMIT breaks that guarantee: the migration commits before the
# ledger INSERT runs, recreating the exact unsafe state this runner was built
# to prevent.
# ---------------------------------------------------------------------------

@pytest.fixture()
def txn_mig_dir(tmp_path):
    d = tmp_path / "migrations_txn"
    d.mkdir()
    (d / "20260801_clean.sql").write_text("SELECT 'clean';")
    (d / "20260802_has_begin_commit.sql").write_text(
        "BEGIN;\nCREATE TABLE bad_table (id INT);\nCOMMIT;\n"
    )
    (d / "20260803_after_bad.sql").write_text("SELECT 'after';")
    return d


def _ledger_inserts(fake_db):
    return [s for s, _ in fake_db.executed if "INSERT INTO schema_migrations" in s]


def test_begin_commit_migration_refused_before_execution(fake_db, txn_mig_dir):
    """A BEGIN;...COMMIT; migration must be refused before any SQL runs."""
    with pytest.raises(mr.MigrationTransactionControlError) as excinfo:
        mr.run_pending(apply=True, directory=txn_mig_dir)
    err = str(excinfo.value)
    assert "20260802_has_begin_commit.sql" in err
    assert "BEGIN" in err
    assert "COMMIT" in err
    refused_body = [s for s, _ in fake_db.executed if "CREATE TABLE bad_table" in s]
    assert refused_body == [], "Refused migration body must never execute"


def test_begin_commit_migration_writes_no_ledger_row(fake_db, txn_mig_dir):
    """A refused migration must not produce a schema_migrations row."""
    with pytest.raises(mr.MigrationTransactionControlError):
        mr.run_pending(apply=True, directory=txn_mig_dir)
    assert _ledger_inserts(fake_db) == [], "No ledger rows for refused pending batch"


def test_migration_after_refused_txn_file_does_not_run(fake_db, txn_mig_dir):
    """Prevalidation refuses the full batch before clean A or later C can run."""
    with pytest.raises(mr.MigrationTransactionControlError):
        mr.run_pending(apply=True, directory=txn_mig_dir)
    before = [s for s, _ in fake_db.executed if "SELECT 'clean'" in s]
    after = [s for s, _ in fake_db.executed if "SELECT 'after'" in s]
    assert before == [], "Valid migration before invalid file must not run"
    assert after == [], "Migration after refused file must not run"
    assert _ledger_inserts(fake_db) == []


def test_commit_only_migration_refused(fake_db, tmp_path):
    """A file containing only COMMIT (no BEGIN) is still refused."""
    d = tmp_path / "m"
    d.mkdir()
    (d / "20260801_commit_only.sql").write_text("SELECT 1;\nCOMMIT;\n")
    with pytest.raises(mr.MigrationTransactionControlError) as excinfo:
        mr.run_pending(apply=True, directory=d)
    assert "20260801_commit_only.sql" in str(excinfo.value)
    assert "COMMIT" in str(excinfo.value)
    assert _ledger_inserts(fake_db) == []


def test_rollback_migration_refused(fake_db, tmp_path):
    """A file containing ROLLBACK is refused."""
    d = tmp_path / "m"
    d.mkdir()
    (d / "20260801_rollback.sql").write_text("BEGIN;\nSELECT 1;\nROLLBACK;\n")
    with pytest.raises(mr.MigrationTransactionControlError) as excinfo:
        mr.run_pending(apply=True, directory=d)
    assert "20260801_rollback.sql" in str(excinfo.value)
    assert "ROLLBACK" in str(excinfo.value)
    assert [s for s, _ in fake_db.executed if "SELECT 1;" in s] == []


@pytest.mark.parametrize(
    ("sql", "statement"),
    [
        ("begin;", "BEGIN"),
        ("COMMIT ;", "COMMIT"),
        ("Start Transaction;", "START TRANSACTION"),
        ("rollback;", "ROLLBACK"),
    ],
)
def test_transaction_control_detection_is_case_insensitive(sql, statement):
    assert mr._top_level_transaction_control(sql) == [statement]


def test_reports_every_offending_file_and_detected_statement(fake_db, tmp_path):
    d = tmp_path / "m"
    d.mkdir()
    (d / "20260801_begin_commit.sql").write_text("BEGIN;\nSELECT 1;\nCOMMIT;\n")
    (d / "20260802_start.sql").write_text("Start Transaction;\nSELECT 2;\n")
    (d / "20260803_rollback.sql").write_text("rollback;\n")
    with pytest.raises(mr.MigrationTransactionControlError) as excinfo:
        mr.run_pending(apply=True, directory=d)
    err = str(excinfo.value)
    assert "20260801_begin_commit.sql" in err
    assert "20260802_start.sql" in err
    assert "20260803_rollback.sql" in err
    assert "BEGIN" in err
    assert "COMMIT" in err
    assert "START TRANSACTION" in err
    assert "ROLLBACK" in err
    assert _ledger_inserts(fake_db) == []


def test_sql_comment_containing_begin_is_not_refused(fake_db, tmp_path):
    """A SQL comment mentioning BEGIN must not trigger the guard."""
    d = tmp_path / "m"
    d.mkdir()
    (d / "20260801_clean_with_comment.sql").write_text(
        "-- BEGIN is not used here\n/* COMMIT; ROLLBACK; */\nSELECT 'ok';\n"
    )
    result = mr.run_pending(apply=True, directory=d)
    assert result["failed"] is None
    assert result["applied"] == ["20260801_clean_with_comment.sql"]


def test_sql_string_containing_transaction_words_is_not_refused(fake_db, tmp_path):
    """Transaction words inside quoted strings are not top-level statements."""
    d = tmp_path / "m"
    d.mkdir()
    (d / "20260801_clean_with_string.sql").write_text(
        "SELECT 'BEGIN; COMMIT; ROLLBACK; START TRANSACTION;';\n"
    )
    result = mr.run_pending(apply=True, directory=d)
    assert result["failed"] is None
    assert result["applied"] == ["20260801_clean_with_string.sql"]


def test_postgres_escape_string_transaction_words_are_not_refused(fake_db, tmp_path):
    """Backslash-escaped quotes inside E strings must not corrupt masking."""
    d = tmp_path / "m"
    d.mkdir()
    (d / "20260801_clean_with_escape_string.sql").write_text(
        "SELECT E'BEGIN; it\\'s only text; COMMIT;';\n"
        "CREATE TABLE valid_table (id INT);\n"
    )
    result = mr.run_pending(apply=True, directory=d)
    assert result["failed"] is None
    assert result["applied"] == ["20260801_clean_with_escape_string.sql"]
    assert len(_ledger_inserts(fake_db)) == 1


def test_real_transaction_statement_after_escape_string_is_detected():
    sql = (
        "SELECT E'it\\'s only text';\n"
        "BEGIN;\n"
        "CREATE TABLE invalid_table (id INT);\n"
    )
    assert mr._top_level_transaction_control(sql) == ["BEGIN"]


def test_lowercase_postgres_escape_string_behaves_identically():
    accepted = "SELECT e'BEGIN; it\\'s only text; COMMIT;';\n"
    rejected = "SELECT e'it\\'s only text';\nBEGIN;\n"
    assert mr._top_level_transaction_control(accepted) == []
    assert mr._top_level_transaction_control(rejected) == ["BEGIN"]


def test_doubled_quotes_inside_postgres_escape_string_are_accepted(fake_db, tmp_path):
    d = tmp_path / "m"
    d.mkdir()
    (d / "20260801_escape_string_doubled_quote.sql").write_text(
        "SELECT E'it''s valid';\n"
    )
    result = mr.run_pending(apply=True, directory=d)
    assert result["failed"] is None
    assert result["applied"] == ["20260801_escape_string_doubled_quote.sql"]


def test_e_prefix_embedded_in_identifier_is_not_treated_as_escape_string():
    assert mr._top_level_transaction_control("someE'not an escape string';\nBEGIN;\n") == [
        "BEGIN"
    ]


def test_valid_before_invalid_escape_string_batch_executes_zero_sql(fake_db, tmp_path):
    d = tmp_path / "m"
    d.mkdir()
    (d / "20260801_valid_escape_string.sql").write_text(
        "SELECT E'BEGIN; it\\'s only text; COMMIT;';\n"
    )
    (d / "20260802_invalid_after_escape_string.sql").write_text(
        "SELECT E'it\\'s only text';\n"
        "BEGIN;\n"
        "CREATE TABLE invalid_table (id INT);\n"
    )
    with pytest.raises(mr.MigrationTransactionControlError) as excinfo:
        mr.run_pending(apply=True, directory=d)
    assert "20260802_invalid_after_escape_string.sql" in str(excinfo.value)
    assert "BEGIN" in str(excinfo.value)
    assert [s for s, _ in fake_db.executed if "SELECT E'BEGIN" in s] == []
    assert [s for s, _ in fake_db.executed if "CREATE TABLE invalid_table" in s] == []
    assert _ledger_inserts(fake_db) == []


def test_postgres_do_block_begin_end_is_not_refused(fake_db, tmp_path):
    """PL/pgSQL BEGIN inside a DO $$ block is not transaction control."""
    d = tmp_path / "m"
    d.mkdir()
    (d / "20260801_do_block.sql").write_text(
        "DO $$\n"
        "BEGIN\n"
        "    PERFORM 1;\n"
        "END\n"
        "$$;\n"
    )
    result = mr.run_pending(apply=True, directory=d)
    assert result["failed"] is None
    assert result["applied"] == ["20260801_do_block.sql"]
    assert len(_ledger_inserts(fake_db)) == 1


def test_production_shaped_begin_commit_pattern_is_detected():
    """The scanner catches the production-shaped BEGIN;...COMMIT; pattern."""
    production_shaped = (
        "-- Durable one-decision-per-exit-generation fence.\n"
        "-- Apply before deploying PR #361.\n\n"
        "BEGIN;\n\n"
        "CREATE TABLE IF NOT EXISTS exit_decision_generation_claims (\n"
        "    generation_key TEXT PRIMARY KEY\n"
        ");\n\n"
        "COMMIT;\n"
    )
    assert mr._top_level_transaction_control(production_shaped) == ["BEGIN", "COMMIT"], (
        "Production-shaped BEGIN;...COMMIT; pattern must be caught by the guard"
    )


def test_clean_migration_passes_txn_guard():
    """A migration without transaction control passes the guard."""
    clean = (
        "CREATE TABLE IF NOT EXISTS foo (id SERIAL PRIMARY KEY);\n"
        "ALTER TABLE bar ADD COLUMN IF NOT EXISTS x TEXT;\n"
    )
    assert mr._top_level_transaction_control(clean) == []


def test_dry_run_default_does_not_execute_or_prevalidate_invalid_pending(fake_db, txn_mig_dir):
    """Dry-run remains report-only even when a pending file would be refused on apply."""
    result = mr.run_pending(directory=txn_mig_dir)
    assert result["would_apply"] == [
        "20260801_clean.sql",
        "20260802_has_begin_commit.sql",
        "20260803_after_bad.sql",
    ]
    assert result["applied"] == []
    assert result["failed"] is None
    assert [s for s, _ in fake_db.executed if "SELECT 'clean'" in s] == []
    assert _ledger_inserts(fake_db) == []


def test_ledger_insert_failure_rolls_back_valid_migration_side_change(fake_db, tmp_path):
    """A valid migration body is rolled back if its ledger insert fails."""
    d = tmp_path / "m"
    d.mkdir()
    (d / "20260801_valid.sql").write_text(
        "CREATE TABLE valid_side_effect (id INT);\n"
    )
    fake_db.raise_on = "INSERT INTO schema_migrations"
    result = mr.run_pending(apply=True, directory=d)
    assert result["failed"]["filename"] == "20260801_valid.sql"
    assert result["applied"] == []
    assert "CREATE TABLE valid_side_effect (id INT);" in fake_db.rolled_back_sql
    assert "CREATE TABLE valid_side_effect (id INT);" not in fake_db.committed_sql
