"""Static safety checks for the staged Supabase RLS lockdown migration."""

from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "migrations" / "20260823_rls_public_surface_lockdown.sql"
VALIDATION = ROOT / "sql" / "validation" / "07_rls_hardening.sql"
ROLE_ACCESS_FIXTURE = ROOT / "tests" / "fixtures" / "rls_hardening_role_access.sql"
UNKNOWN_POLICY_FIXTURE = ROOT / "tests" / "fixtures" / "rls_hardening_unknown_policy_fixture.sql"
PRESERVED_POLICY_DRIFT_FIXTURE = ROOT / "tests" / "fixtures" / "rls_hardening_preserved_policy_drift_fixture.sql"
P0_WORKFLOW = ROOT / ".github" / "workflows" / "p0_regression.yml"

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

EXPECTED_SEQUENCES = {
    "ap_admin_audit_id_seq",
    "ap_audit_log_id_seq",
    "ap_client_account_snapshots_id_seq",
    "ap_intelligence_outcome_bindings_id_seq",
    "ap_system_events_id_seq",
    "applications_id_seq",
    "audit_log_id_seq",
    "blocked_signal_counterfactuals_id_seq",
    "bot_status_id_seq",
    "broker_order_audit_id_seq",
    "client_signal_opportunities_id_seq",
    "daily_performance_id_seq",
    "decision_events_id_seq",
    "exit_decision_ledger_id_seq",
    "market_data_id_seq",
    "operator_audit_log_id_seq",
    "option_outcomes_id_seq",
    "orders_id_seq",
    "proof_daily_summary_id_seq",
    "proof_trades_id_seq",
    "signal_outcomes_id_seq",
    "signals_id_seq",
    "trade_queue_id_seq",
    "trades_id_seq",
}

EXPECTED_POLICY_TABLES = {
    "alert_routes",
    "ap_admin_audit",
    "ap_signal_underlying_outcomes",
    "ap_system_control",
    "bot_status",
    "client_health",
    "content_queue",
    "daily_cadence_logs",
    "incidents",
    "market_data",
    "option_outcomes",
    "proof_daily_summary",
    "proof_trades",
    "proof_vault",
    "signal_outcomes",
    "signals",
    "system_health_events",
}

EXPECTED_VIEWS = {
    "ap_signal_funnel_daily",
    "ap_skipped_signal_outcomes",
    "ledger_funnel",
    "ledger_performance",
    "v2_equity_curve",
    "v2_last_20_trades",
    "v2_performance",
}

EXPECTED_DROPPED_POLICIES = {
    ("alert_routes", "anon_read"),
    ("ap_admin_audit", "anon_read"),
    ("ap_signal_underlying_outcomes", "anon_all_underlying"),
    ("ap_system_control", "anon_read"),
    ("bot_status", "anon_all_bot_status"),
    ("client_health", "anon_read_client_health"),
    ("content_queue", "anon_read"),
    ("daily_cadence_logs", "anon_read"),
    ("incidents", "anon_read"),
    ("market_data", "Anyone can read market data"),
    ("option_outcomes", "anon_all_option_outcomes"),
    ("proof_daily_summary", "anon_all_proof_daily"),
    ("proof_trades", "anon_all_proof_trades"),
    ("proof_trades", "service_role_all"),
    ("proof_vault", "anon_read"),
    ("signal_outcomes", "anon_all_signal_outcomes"),
    ("signals", "Anyone can read signals"),
    ("system_health_events", "anon_read"),
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
    sequence_revoked = re.findall(
        r"^REVOKE ALL ON SEQUENCE public\.([a-z0-9_]+) FROM PUBLIC, anon, authenticated;$",
        sql,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    sequence_grant = re.search(
        r"GRANT USAGE, SELECT, UPDATE ON SEQUENCE(?P<body>.*?)TO service_role;",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )

    assert set(enabled) == EXPECTED_TABLES
    assert set(revoked) == EXPECTED_TABLES | EXPECTED_POLICY_TABLES | EXPECTED_VIEWS
    assert set(sequence_revoked) == EXPECTED_SEQUENCES
    assert sequence_grant is not None
    assert set(re.findall(r"public\.([a-z0-9_]+)", sequence_grant.group("body"))) == EXPECTED_SEQUENCES
    assert len(enabled) == len(EXPECTED_TABLES)
    assert len(revoked) == len(EXPECTED_TABLES | EXPECTED_POLICY_TABLES | EXPECTED_VIEWS)
    assert len(sequence_revoked) == len(EXPECTED_SEQUENCES)


def test_migration_drops_only_live_verified_public_policy_rows() -> None:
    sql = _sql()
    dropped = re.findall(
        r'^DROP POLICY (?:"([^"]+)"|([a-z0-9_]+)) ON public\.([a-z0-9_]+);$',
        sql,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    normalized = {(policy_a or policy_b, table) for policy_a, policy_b, table in dropped}

    assert normalized == {(policy, table) for table, policy in EXPECTED_DROPPED_POLICIES}
    assert "service_write_client_health" in sql
    assert "DROP POLICY service_write_client_health" not in sql


def test_migration_is_fail_closed_without_guessing_tenant_policies() -> None:
    sql = _sql()
    assert not re.search(r"^\s*CREATE\s+POLICY\b", sql, re.IGNORECASE | re.MULTILINE)
    assert not re.search(
        r"^\s*ALTER\s+TABLE\b.*\bFORCE\s+ROW\s+LEVEL\s+SECURITY\b",
        sql,
        re.IGNORECASE | re.MULTILINE,
    )
    assert "roles::text IS DISTINCT FROM" in sql
    assert "rolbypassrls" in sql
    assert "service_role must retain BYPASSRLS" in sql
    assert "expected_permissive" in sql
    assert "p.permissive IS DISTINCT FROM" in sql
    assert "p.permissive = e.expected_permissive" in sql
    assert "unreviewed public tables with RLS disabled" in sql
    assert "unreviewed public sequences with anon/authenticated access" in sql
    assert "unreviewed permissive public/anon policy drift" in sql
    assert "preserved owner policy drift" in sql
    assert "unreviewed public/anon/authenticated policy identity drift" in sql
    assert "members_read_own" in sql
    assert "client_sees_own_trades" in sql
    assert "has_table_privilege('anon', c.oid, 'SELECT')" in sql


def test_migration_leaves_privileged_paths_and_runner_transaction_control_alone() -> None:
    sql = _sql()
    revoke_statements = re.findall(
        r"^REVOKE\s+ALL\s+ON\s+TABLE\s+[^;]+;$",
        sql,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    assert revoke_statements
    assert all("service_role" not in statement.lower() for statement in revoke_statements)
    assert sql.count("TO service_role;") >= 3
    assert not re.search(r"^\s*(BEGIN|COMMIT|ROLLBACK)\s*;", sql, re.IGNORECASE | re.MULTILINE)


def test_validation_is_read_only_and_has_hard_gates() -> None:
    sql = VALIDATION.read_text(encoding="utf-8")
    assert "\\set ON_ERROR_STOP on" in sql
    assert sql.count("RAISE EXCEPTION") >= 5
    assert "anon', format('public.%I', e.table_name), 'INSERT'" in sql
    assert "authenticated', format('public.%I', e.table_name), 'DELETE'" in sql
    assert "roles && ARRAY['public', 'anon', 'authenticated']::name[]" in sql
    assert "has_sequence_privilege('anon'" in sql
    assert "Public sequences remain reachable by anon/authenticated" in sql
    assert "rolbypassrls" in sql
    assert "service_role RLS bypass gate failed" in sql
    assert "has_table_privilege('anon', c.oid, 'SELECT')" in sql
    assert "Preserved owner policy drift" in sql
    assert "Unreviewed post-deploy public/anon/authenticated policy identity drift" in sql
    assert "('alert_routes', 'anon_read')" not in sql
    assert "('proof_trades', 'anon_all_proof_trades')" not in sql
    assert not re.search(r"^\s*(INSERT|UPDATE|DELETE|ALTER|DROP|CREATE)\b", sql, re.IGNORECASE | re.MULTILINE)


def test_role_access_fixture_exercises_real_session_roles() -> None:
    sql = ROLE_ACCESS_FIXTURE.read_text(encoding="utf-8")
    assert "SET LOCAL ROLE anon" in sql
    assert "SET LOCAL ROLE authenticated" in sql
    assert "SET LOCAL ROLE service_role" in sql
    assert "WHEN insufficient_privilege" in sql
    assert "INSERT INTO public.orders" in sql
    assert "CREATE" not in sql


def test_negative_policy_drift_fixtures_exercise_fail_closed_guards() -> None:
    unknown = UNKNOWN_POLICY_FIXTURE.read_text(encoding="utf-8")
    preserved = PRESERVED_POLICY_DRIFT_FIXTURE.read_text(encoding="utf-8")
    assert "CREATE POLICY unreviewed_members_policy" in unknown
    assert "user_id IS NOT NULL" in unknown
    assert "ALTER POLICY members_read_own" in preserved
    assert "USING (true)" in preserved


def test_p0_workflow_runs_the_migration_static_test() -> None:
    workflow = P0_WORKFLOW.read_text(encoding="utf-8")
    assert workflow.count("tests/test_rls_hardening_migration.py") == 1
    assert "Execute and validate RLS hardening migration" in workflow
    assert "tests/fixtures/rls_hardening_migration_fixture.sql" in workflow
    assert workflow.count("tests/fixtures/rls_hardening_role_access.sql") == 1
    assert workflow.count("tests/fixtures/rls_hardening_unknown_policy_fixture.sql") == 1
    assert workflow.count("tests/fixtures/rls_hardening_preserved_policy_drift_fixture.sql") == 1
    assert workflow.count("migrations/20260823_rls_public_surface_lockdown.sql") == 3
    assert workflow.count("sql/validation/07_rls_hardening.sql") == 1
