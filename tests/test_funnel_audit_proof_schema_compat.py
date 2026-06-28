from pathlib import Path


MIGRATION = Path("migrations/20260626_funnel_audit_proof_schema_compat.sql")


def test_funnel_audit_proof_schema_compat_migration_is_additive_only():
    sql = MIGRATION.read_text()

    expected_columns = {
        "trade_id": "bigint",
        "signal_id": "text",
        "status": "text",
    }
    for column, column_type in expected_columns.items():
        assert (
            f"alter table public.proof_trades add column if not exists {column} {column_type};"
            in sql
        )

    lowered = sql.lower()
    assert "drop column" not in lowered
    assert "drop table" not in lowered
    assert "delete from" not in lowered
    assert "update public.proof_trades" not in lowered
    assert " default " not in lowered
