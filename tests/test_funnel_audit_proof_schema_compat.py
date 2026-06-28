from pathlib import Path


MIGRATION = Path("migrations/20260628_funnel_audit_proof_schema_compat.sql")


def test_funnel_audit_proof_schema_compat_migration_is_additive_only():
    sql = MIGRATION.read_text().lower()

    assert "add column if not exists trade_id bigint" in sql
    assert "add column if not exists signal_id text" in sql
    assert "add column if not exists status text" in sql

    assert "drop column" not in sql
    assert "drop table" not in sql
    assert "delete from" not in sql
    assert "update proof_trades" not in sql
    assert " default " not in sql
