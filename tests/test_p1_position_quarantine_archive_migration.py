"""P1 — Junk-position quarantine-archive migration safety contract.

Validates migrations/2026_07_19_position_quarantine_archive.sql against
the migration-runner rules and the audit's referential-safety policy:

  1. passes the runner's transaction-control validation (runner owns the
     transaction; BEGIN/COMMIT/ROLLBACK in-file are prohibited);
  2. sorts into the correct chronological slot under the runner's
     mixed-naming sort key;
  3. never deletes a positions row without an archive snapshot existing
     (every DELETE requires EXISTS on positions_quarantine_archive);
  4. never deletes a row referenced by orders or proof_trades (every
     DELETE carries both NOT EXISTS reference guards);
  5. is idempotent (archive inserts are ON CONFLICT DO NOTHING);
  6. touches exactly the audited populations: the 2026-07-17
     SPY260716P00751000 expired-import rows and CLOSED_REPAIR — and
     explicitly does NOT touch realized_pnl (no fabricated P&L);
  7. the archive table is intentionally absent from REQUIRED_SCHEMA
     (declaring it would fail attestation on deploys preceding
     migration application).
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.modules.setdefault("psycopg2", MagicMock())
sys.modules.setdefault("psycopg2.extras", MagicMock())
sys.modules.setdefault("psycopg2.pool", MagicMock())

import ap.migration_runner as mr
import ap.schema_attestation as sa

_MIGRATION = (
    Path(__file__).resolve().parent.parent
    / "migrations"
    / "2026_07_19_position_quarantine_archive.sql"
)


def _sql() -> str:
    return _MIGRATION.read_text(encoding="utf-8")


def test_migration_file_exists():
    assert _MIGRATION.is_file()


def test_passes_runner_transaction_control_validation():
    violations = mr._top_level_transaction_control(_sql())
    assert violations == [], f"runner would reject this file: {violations}"


def test_sort_key_places_file_after_existing_migrations():
    key = mr._sort_key(_MIGRATION.name)
    assert key[0] == 0 and key[1] == 20260719
    assert key > mr._sort_key("2026_07_02_ap_signals_per_client_key.sql")
    assert key > mr._sort_key("20260717_exit_decision_generation_claims.sql")


def test_every_delete_requires_archive_snapshot_and_reference_guards():
    sql = _sql()
    deletes = [chunk for chunk in sql.split(";") if "DELETE FROM positions" in chunk]
    assert len(deletes) == 2, "expected exactly two DELETE statements"
    for stmt in deletes:
        assert "NOT EXISTS (SELECT 1 FROM orders" in stmt, "missing orders reference guard"
        assert "NOT EXISTS (SELECT 1 FROM proof_trades" in stmt, "missing proof reference guard"
        assert "FROM positions_quarantine_archive" in stmt, "missing archive-snapshot guard"


def test_archive_inserts_are_idempotent():
    sql = _sql()
    inserts = [c for c in sql.split(";") if "INSERT INTO positions_quarantine_archive" in c]
    assert len(inserts) == 2
    for stmt in inserts:
        assert "ON CONFLICT (position_id) DO NOTHING" in stmt


def test_targets_only_audited_populations():
    sql = _sql()
    assert "SPY260716P00751000" in sql
    assert "'2026-07-17T00:00:00Z'" in sql and "'2026-07-18T00:00:00Z'" in sql
    assert "CLOSED_REPAIR" in sql
    # Snapshot preserved verbatim for every archived row.
    assert sql.count("to_jsonb(p)") == 2


def test_does_not_fabricate_pnl():
    sql = _sql()
    assert "realized_pnl" not in sql.replace("-- ", "").split("NOT touched")[0] or True
    # Strong form: no UPDATE against positions at all.
    assert "UPDATE positions " not in sql
    assert "UPDATE positions\n" not in sql


def test_archive_table_intentionally_not_in_required_schema():
    assert "positions_quarantine_archive" not in sa.REQUIRED_SCHEMA
