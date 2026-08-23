"""Static safety checks for the staged Supabase RLS lockdown migration."""

from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "migrations" / "20260823_rls_public_surface_lockdown.sql"

EXPECTED_TABLES = {
    "account_snapshots",
    "ap_audit_log",
    "ap_edge_buckets",
    "ap_signal_context_tags",
    "ap_signal_hub_merged",
    "ap_signal_hub_raw",
    "ap_signal_ledger",
    "ap_signal_option_outcomes",
    "ap_signals",
    "ap_signals_jason_manual_push_backup_20260616",
    "ap_trade_log",
    "ap_whitelist",
    "audit_log",
    "client_authorizations",
    "client_state",
    "kv",
    "orders",
    "orders_backup_jason_null_mode_2026_06_14",
    "orders_backup_jason_null_mode_orphans_2026_06_14",
    "processed_signals",
    "schema_migrations",
    "trade_fills",
}


def _sql() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def test_migration_targets_exact_live_scope() -> None:
    sql = _sql()
    enabled = re.findall(
        r"^ALTER TABLE public\.([a-z0-9_]+) ENABLE ROW LEVEL SECURITY;$",
        sql,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    revoked = re.findall(
        r"^REVOKE ALL ON TABLE public\.([a-z0-9_]+) FROM PUBLIC, anon, authenticated;$",
        sql,
        flags=re.IGNORECASE | re.MULTILINE,
    )

    assert set(enabled) == EXPECTED_TABLES
    assert set(revoked) == EXPECTED_TABLES
    assert len(enabled) == len(EXPECTED_TABLES)
    assert len(revoked) == len(EXPECTED_TABLES)


def test_migration_is_fail_closed_without_guessing_tenant_policies() -> None:
    sql = _sql()
    assert not re.search(r"^\s*CREATE\s+POLICY\b", sql, re.IGNORECASE | re.MULTILINE)
    assert not re.search(r"^\s*DROP\s+POLICY\b", sql, re.IGNORECASE | re.MULTILINE)
    assert not re.search(
        r"^\s*ALTER\s+TABLE\b.*\bFORCE\s+ROW\s+LEVEL\s+SECURITY\b",
        sql,
        re.IGNORECASE | re.MULTILINE,
    )


def test_migration_leaves_service_role_and_runner_transaction_control_alone() -> None:
    sql = _sql()
    revoke_statements = re.findall(
        r"^REVOKE\s+ALL\s+ON\s+TABLE\s+[^;]+;$",
        sql,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    assert revoke_statements
    assert all("service_role" not in statement.lower() for statement in revoke_statements)
    assert not re.search(r"^\s*(BEGIN|COMMIT|ROLLBACK)\s*;", sql, re.IGNORECASE | re.MULTILINE)
