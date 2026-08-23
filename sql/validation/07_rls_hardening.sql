-- Read-only post-deploy validation for the RLS hardening review migration.
-- The catalog assertions intentionally raise on any mismatch so a staging
-- run cannot be mistaken for a successful security gate.

\set ON_ERROR_STOP on

\echo '== RLS hardening target table matrix =='
WITH expected(table_name) AS (
    VALUES
        ('account_snapshots'),
        ('ap_audit_log'),
        ('ap_edge_buckets'),
        ('ap_signal_context_tags'),
        ('ap_signal_hub_merged'),
        ('ap_signal_hub_raw'),
        ('ap_signal_ledger'),
        ('ap_signal_option_outcomes'),
        ('ap_signals'),
        ('ap_signals_jason_manual_push_backup_20260616'),
        ('ap_trade_log'),
        ('ap_whitelist'),
        ('audit_log'),
        ('client_authorizations'),
        ('client_state'),
        ('kv'),
        ('orders'),
        ('orders_backup_jason_null_mode_2026_06_14'),
        ('orders_backup_jason_null_mode_orphans_2026_06_14'),
        ('processed_signals'),
        ('schema_migrations'),
        ('trade_fills')
)
SELECT
    e.table_name,
    c.oid IS NOT NULL AS table_present,
    c.relrowsecurity AS rls_enabled,
    c.relforcerowsecurity AS rls_forced,
    pg_get_userbyid(c.relowner) AS owner,
    COALESCE(p.policy_count, 0) AS policy_count,
    has_table_privilege('anon', format('public.%I', e.table_name), 'SELECT') AS anon_select,
    has_table_privilege('anon', format('public.%I', e.table_name), 'INSERT') AS anon_insert,
    has_table_privilege('anon', format('public.%I', e.table_name), 'UPDATE') AS anon_update,
    has_table_privilege('anon', format('public.%I', e.table_name), 'DELETE') AS anon_delete,
    has_table_privilege('authenticated', format('public.%I', e.table_name), 'SELECT') AS authenticated_select,
    has_table_privilege('authenticated', format('public.%I', e.table_name), 'INSERT') AS authenticated_insert,
    has_table_privilege('authenticated', format('public.%I', e.table_name), 'UPDATE') AS authenticated_update,
    has_table_privilege('authenticated', format('public.%I', e.table_name), 'DELETE') AS authenticated_delete,
    has_table_privilege('service_role', format('public.%I', e.table_name), 'SELECT') AS service_select,
    has_table_privilege('service_role', format('public.%I', e.table_name), 'INSERT') AS service_insert,
    has_table_privilege('service_role', format('public.%I', e.table_name), 'UPDATE') AS service_update,
    has_table_privilege('service_role', format('public.%I', e.table_name), 'DELETE') AS service_delete
FROM expected e
LEFT JOIN pg_class c
  ON c.relname = e.table_name
 AND c.relnamespace = 'public'::regnamespace
 AND c.relkind IN ('r', 'p')
LEFT JOIN (
    SELECT tablename, count(*) AS policy_count
    FROM pg_policies
    WHERE schemaname = 'public'
    GROUP BY tablename
) p ON p.tablename = e.table_name
ORDER BY e.table_name;

\echo '== Privileged RLS bypass prerequisite =='
SELECT rolname, rolbypassrls
FROM pg_roles
WHERE rolname = 'service_role';

DO $$
DECLARE
    _bad TEXT;
BEGIN
    SELECT string_agg(
               CASE
                   WHEN r.oid IS NULL THEN e.role_name || ' (missing)'
                   ELSE e.role_name || ' (BYPASSRLS=false)'
               END,
               ', ' ORDER BY e.role_name
           )
      INTO _bad
      FROM (VALUES ('service_role')) AS e(role_name)
      LEFT JOIN pg_roles r ON r.rolname = e.role_name
     WHERE r.oid IS NULL OR NOT r.rolbypassrls;

    IF _bad IS NOT NULL THEN
        RAISE EXCEPTION 'service_role RLS bypass gate failed: %', _bad;
    END IF;
END $$;

\echo '== RLS hardening target summary =='
WITH expected(table_name) AS (
    VALUES
        ('account_snapshots'), ('ap_audit_log'), ('ap_edge_buckets'),
        ('ap_signal_context_tags'), ('ap_signal_hub_merged'),
        ('ap_signal_hub_raw'), ('ap_signal_ledger'),
        ('ap_signal_option_outcomes'), ('ap_signals'),
        ('ap_signals_jason_manual_push_backup_20260616'), ('ap_trade_log'),
        ('ap_whitelist'), ('audit_log'), ('client_authorizations'),
        ('client_state'), ('kv'), ('orders'),
        ('orders_backup_jason_null_mode_2026_06_14'),
        ('orders_backup_jason_null_mode_orphans_2026_06_14'),
        ('processed_signals'), ('schema_migrations'), ('trade_fills')
)
SELECT
    count(*) AS expected_tables,
    count(*) FILTER (WHERE c.oid IS NOT NULL) AS present_tables,
    count(*) FILTER (WHERE c.relrowsecurity) AS rls_enabled_tables,
    count(*) FILTER (WHERE c.oid IS NULL OR NOT c.relrowsecurity) AS missing_or_rls_disabled_tables,
    count(*) FILTER (WHERE
        has_table_privilege('anon', format('public.%I', e.table_name), 'SELECT')
        OR has_table_privilege('anon', format('public.%I', e.table_name), 'INSERT')
        OR has_table_privilege('anon', format('public.%I', e.table_name), 'UPDATE')
        OR has_table_privilege('anon', format('public.%I', e.table_name), 'DELETE')
    ) AS anon_privileged_tables,
    count(*) FILTER (WHERE
        has_table_privilege('authenticated', format('public.%I', e.table_name), 'SELECT')
        OR has_table_privilege('authenticated', format('public.%I', e.table_name), 'INSERT')
        OR has_table_privilege('authenticated', format('public.%I', e.table_name), 'UPDATE')
        OR has_table_privilege('authenticated', format('public.%I', e.table_name), 'DELETE')
    ) AS authenticated_privileged_tables,
    count(*) FILTER (WHERE NOT (
        has_table_privilege('service_role', format('public.%I', e.table_name), 'SELECT')
        AND has_table_privilege('service_role', format('public.%I', e.table_name), 'INSERT')
        AND has_table_privilege('service_role', format('public.%I', e.table_name), 'UPDATE')
        AND has_table_privilege('service_role', format('public.%I', e.table_name), 'DELETE')
    )) AS service_crud_gap_tables
FROM expected e
LEFT JOIN pg_class c
  ON c.relname = e.table_name
 AND c.relnamespace = 'public'::regnamespace
 AND c.relkind IN ('r', 'p');

DO $$
DECLARE
    _bad TEXT;
BEGIN
    WITH expected(table_name) AS (
        VALUES
            ('account_snapshots'), ('ap_audit_log'), ('ap_edge_buckets'),
            ('ap_signal_context_tags'), ('ap_signal_hub_merged'),
            ('ap_signal_hub_raw'), ('ap_signal_ledger'),
            ('ap_signal_option_outcomes'), ('ap_signals'),
            ('ap_signals_jason_manual_push_backup_20260616'), ('ap_trade_log'),
            ('ap_whitelist'), ('audit_log'), ('client_authorizations'),
            ('client_state'), ('kv'), ('orders'),
            ('orders_backup_jason_null_mode_2026_06_14'),
            ('orders_backup_jason_null_mode_orphans_2026_06_14'),
            ('processed_signals'), ('schema_migrations'), ('trade_fills')
    )
    SELECT string_agg(e.table_name, ', ' ORDER BY e.table_name)
      INTO _bad
      FROM expected e
      LEFT JOIN pg_class c
        ON c.relname = e.table_name
       AND c.relnamespace = 'public'::regnamespace
       AND c.relkind IN ('r', 'p')
     WHERE c.oid IS NULL
        OR NOT c.relrowsecurity
        OR pg_get_userbyid(c.relowner) <> 'postgres'
        OR has_table_privilege('anon', format('public.%I', e.table_name), 'SELECT')
        OR has_table_privilege('anon', format('public.%I', e.table_name), 'INSERT')
        OR has_table_privilege('anon', format('public.%I', e.table_name), 'UPDATE')
        OR has_table_privilege('anon', format('public.%I', e.table_name), 'DELETE')
        OR has_table_privilege('authenticated', format('public.%I', e.table_name), 'SELECT')
        OR has_table_privilege('authenticated', format('public.%I', e.table_name), 'INSERT')
        OR has_table_privilege('authenticated', format('public.%I', e.table_name), 'UPDATE')
        OR has_table_privilege('authenticated', format('public.%I', e.table_name), 'DELETE')
        OR NOT (
            has_table_privilege('service_role', format('public.%I', e.table_name), 'SELECT')
            AND has_table_privilege('service_role', format('public.%I', e.table_name), 'INSERT')
            AND has_table_privilege('service_role', format('public.%I', e.table_name), 'UPDATE')
            AND has_table_privilege('service_role', format('public.%I', e.table_name), 'DELETE')
        );

    IF _bad IS NOT NULL THEN
        RAISE EXCEPTION 'RLS hardening target table gate failed: %', _bad;
    END IF;
END $$;


\echo '== Public sequence privilege matrix =='
WITH expected(sequence_name) AS (
    VALUES
        ('ap_admin_audit_id_seq'), ('ap_audit_log_id_seq'),
        ('ap_client_account_snapshots_id_seq'),
        ('ap_intelligence_outcome_bindings_id_seq'), ('ap_system_events_id_seq'),
        ('applications_id_seq'), ('audit_log_id_seq'),
        ('blocked_signal_counterfactuals_id_seq'), ('bot_status_id_seq'),
        ('broker_order_audit_id_seq'), ('client_signal_opportunities_id_seq'),
        ('daily_performance_id_seq'), ('decision_events_id_seq'),
        ('exit_decision_ledger_id_seq'), ('market_data_id_seq'),
        ('operator_audit_log_id_seq'), ('option_outcomes_id_seq'),
        ('orders_id_seq'), ('proof_daily_summary_id_seq'),
        ('proof_trades_id_seq'), ('signal_outcomes_id_seq'),
        ('signals_id_seq'), ('trade_queue_id_seq'), ('trades_id_seq')
)
SELECT e.sequence_name,
       c.oid IS NOT NULL AS sequence_present,
       CASE WHEN c.oid IS NULL THEN NULL ELSE pg_get_userbyid(c.relowner) END AS owner,
       CASE WHEN c.oid IS NULL THEN NULL ELSE has_sequence_privilege('anon', c.oid, 'USAGE') END AS anon_usage,
       CASE WHEN c.oid IS NULL THEN NULL ELSE has_sequence_privilege('anon', c.oid, 'SELECT') END AS anon_select,
       CASE WHEN c.oid IS NULL THEN NULL ELSE has_sequence_privilege('anon', c.oid, 'UPDATE') END AS anon_update,
       CASE WHEN c.oid IS NULL THEN NULL ELSE has_sequence_privilege('authenticated', c.oid, 'USAGE') END AS authenticated_usage,
       CASE WHEN c.oid IS NULL THEN NULL ELSE has_sequence_privilege('authenticated', c.oid, 'SELECT') END AS authenticated_select,
       CASE WHEN c.oid IS NULL THEN NULL ELSE has_sequence_privilege('authenticated', c.oid, 'UPDATE') END AS authenticated_update,
       CASE WHEN c.oid IS NULL THEN NULL ELSE has_sequence_privilege('service_role', c.oid, 'USAGE') END AS service_usage,
       CASE WHEN c.oid IS NULL THEN NULL ELSE has_sequence_privilege('service_role', c.oid, 'SELECT') END AS service_select,
       CASE WHEN c.oid IS NULL THEN NULL ELSE has_sequence_privilege('service_role', c.oid, 'UPDATE') END AS service_update
FROM expected e
LEFT JOIN pg_class c
  ON c.relname = e.sequence_name
 AND c.relnamespace = 'public'::regnamespace
 AND c.relkind = 'S'
ORDER BY e.sequence_name;

DO $$
DECLARE
    _bad TEXT;
BEGIN
    WITH expected(sequence_name) AS (
        VALUES
            ('ap_admin_audit_id_seq'), ('ap_audit_log_id_seq'),
            ('ap_client_account_snapshots_id_seq'),
            ('ap_intelligence_outcome_bindings_id_seq'), ('ap_system_events_id_seq'),
            ('applications_id_seq'), ('audit_log_id_seq'),
            ('blocked_signal_counterfactuals_id_seq'), ('bot_status_id_seq'),
            ('broker_order_audit_id_seq'), ('client_signal_opportunities_id_seq'),
            ('daily_performance_id_seq'), ('decision_events_id_seq'),
            ('exit_decision_ledger_id_seq'), ('market_data_id_seq'),
            ('operator_audit_log_id_seq'), ('option_outcomes_id_seq'),
            ('orders_id_seq'), ('proof_daily_summary_id_seq'),
            ('proof_trades_id_seq'), ('signal_outcomes_id_seq'),
            ('signals_id_seq'), ('trade_queue_id_seq'), ('trades_id_seq')
    )
    SELECT string_agg(e.sequence_name, ', ' ORDER BY e.sequence_name)
      INTO _bad
      FROM expected e
      LEFT JOIN pg_class c
        ON c.relname = e.sequence_name
       AND c.relnamespace = 'public'::regnamespace
       AND c.relkind = 'S'
     WHERE c.oid IS NULL
        OR pg_get_userbyid(c.relowner) <> 'postgres'
        OR CASE WHEN c.oid IS NULL THEN false ELSE has_sequence_privilege('anon', c.oid, 'USAGE') END
        OR CASE WHEN c.oid IS NULL THEN false ELSE has_sequence_privilege('anon', c.oid, 'SELECT') END
        OR CASE WHEN c.oid IS NULL THEN false ELSE has_sequence_privilege('anon', c.oid, 'UPDATE') END
        OR CASE WHEN c.oid IS NULL THEN false ELSE has_sequence_privilege('authenticated', c.oid, 'USAGE') END
        OR CASE WHEN c.oid IS NULL THEN false ELSE has_sequence_privilege('authenticated', c.oid, 'SELECT') END
        OR CASE WHEN c.oid IS NULL THEN false ELSE has_sequence_privilege('authenticated', c.oid, 'UPDATE') END
        OR CASE WHEN c.oid IS NULL THEN false ELSE NOT has_sequence_privilege('service_role', c.oid, 'USAGE') END
        OR CASE WHEN c.oid IS NULL THEN false ELSE NOT has_sequence_privilege('service_role', c.oid, 'SELECT') END
        OR CASE WHEN c.oid IS NULL THEN false ELSE NOT has_sequence_privilege('service_role', c.oid, 'UPDATE') END;

    IF _bad IS NOT NULL THEN
        RAISE EXCEPTION 'Public sequence gate failed: %', _bad;
    END IF;

    SELECT string_agg(c.relname, ', ' ORDER BY c.relname)
      INTO _bad
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relkind = 'S'
       AND (
           has_sequence_privilege('anon', c.oid, 'USAGE')
           OR has_sequence_privilege('anon', c.oid, 'SELECT')
           OR has_sequence_privilege('anon', c.oid, 'UPDATE')
           OR has_sequence_privilege('authenticated', c.oid, 'USAGE')
           OR has_sequence_privilege('authenticated', c.oid, 'SELECT')
           OR has_sequence_privilege('authenticated', c.oid, 'UPDATE')
       );

    IF _bad IS NOT NULL THEN
        RAISE EXCEPTION 'Public sequences remain reachable by anon/authenticated: %', _bad;
    END IF;
END $$;

\echo '== Policy-lockdown table matrix =='
WITH expected(table_name) AS (
    VALUES
        ('alert_routes'), ('ap_admin_audit'),
        ('ap_signal_underlying_outcomes'), ('ap_system_control'),
        ('bot_status'), ('client_health'), ('content_queue'),
        ('daily_cadence_logs'), ('incidents'), ('market_data'),
        ('option_outcomes'), ('proof_daily_summary'), ('proof_trades'),
        ('proof_vault'), ('signal_outcomes'), ('signals'),
        ('system_health_events')
)
SELECT e.table_name,
       c.oid IS NOT NULL AS table_present,
       c.relrowsecurity AS rls_enabled,
       count(p.policyname) FILTER (
           WHERE p.roles && ARRAY['public', 'anon', 'authenticated']::name[]
             AND (p.qual = 'true' OR p.with_check = 'true')
       ) AS unsafe_policy_count,
       has_table_privilege('anon', format('public.%I', e.table_name), 'SELECT') AS anon_select,
       has_table_privilege('anon', format('public.%I', e.table_name), 'INSERT') AS anon_insert,
       has_table_privilege('anon', format('public.%I', e.table_name), 'UPDATE') AS anon_update,
       has_table_privilege('anon', format('public.%I', e.table_name), 'DELETE') AS anon_delete,
       has_table_privilege('authenticated', format('public.%I', e.table_name), 'SELECT') AS authenticated_select,
       has_table_privilege('authenticated', format('public.%I', e.table_name), 'INSERT') AS authenticated_insert,
       has_table_privilege('authenticated', format('public.%I', e.table_name), 'UPDATE') AS authenticated_update,
       has_table_privilege('authenticated', format('public.%I', e.table_name), 'DELETE') AS authenticated_delete,
       has_table_privilege('service_role', format('public.%I', e.table_name), 'SELECT') AS service_select,
       has_table_privilege('service_role', format('public.%I', e.table_name), 'INSERT') AS service_insert,
       has_table_privilege('service_role', format('public.%I', e.table_name), 'UPDATE') AS service_update,
       has_table_privilege('service_role', format('public.%I', e.table_name), 'DELETE') AS service_delete
FROM expected e
LEFT JOIN pg_class c
  ON c.relname = e.table_name
 AND c.relnamespace = 'public'::regnamespace
 AND c.relkind IN ('r', 'p')
LEFT JOIN pg_policies p
  ON p.schemaname = 'public' AND p.tablename = e.table_name
GROUP BY e.table_name, c.oid, c.relrowsecurity
ORDER BY e.table_name;

DO $$
DECLARE
    _bad TEXT;
BEGIN
    WITH expected(table_name) AS (
        VALUES
            ('alert_routes'), ('ap_admin_audit'),
            ('ap_signal_underlying_outcomes'), ('ap_system_control'),
            ('bot_status'), ('client_health'), ('content_queue'),
            ('daily_cadence_logs'), ('incidents'), ('market_data'),
            ('option_outcomes'), ('proof_daily_summary'), ('proof_trades'),
            ('proof_vault'), ('signal_outcomes'), ('signals'),
            ('system_health_events')
    )
    SELECT string_agg(e.table_name, ', ' ORDER BY e.table_name)
      INTO _bad
      FROM expected e
      LEFT JOIN pg_class c
        ON c.relname = e.table_name
       AND c.relnamespace = 'public'::regnamespace
       AND c.relkind IN ('r', 'p')
     WHERE c.oid IS NULL
        OR NOT c.relrowsecurity
        OR has_table_privilege('anon', format('public.%I', e.table_name), 'SELECT')
        OR has_table_privilege('anon', format('public.%I', e.table_name), 'INSERT')
        OR has_table_privilege('anon', format('public.%I', e.table_name), 'UPDATE')
        OR has_table_privilege('anon', format('public.%I', e.table_name), 'DELETE')
        OR has_table_privilege('authenticated', format('public.%I', e.table_name), 'SELECT')
        OR has_table_privilege('authenticated', format('public.%I', e.table_name), 'INSERT')
        OR has_table_privilege('authenticated', format('public.%I', e.table_name), 'UPDATE')
        OR has_table_privilege('authenticated', format('public.%I', e.table_name), 'DELETE')
        OR NOT (
            has_table_privilege('service_role', format('public.%I', e.table_name), 'SELECT')
            AND has_table_privilege('service_role', format('public.%I', e.table_name), 'INSERT')
            AND has_table_privilege('service_role', format('public.%I', e.table_name), 'UPDATE')
            AND has_table_privilege('service_role', format('public.%I', e.table_name), 'DELETE')
        );

    IF _bad IS NOT NULL THEN
        RAISE EXCEPTION 'Policy-lockdown table gate failed: %', _bad;
    END IF;
END $$;

\echo '== Public views requiring the privileged-only phase-1 gate =='
WITH expected(view_name) AS (
    VALUES
        ('ap_signal_funnel_daily'), ('ap_skipped_signal_outcomes'),
        ('ledger_funnel'), ('ledger_performance'), ('v2_equity_curve'),
        ('v2_last_20_trades'), ('v2_performance')
)
SELECT e.view_name,
       c.oid IS NOT NULL AS view_present,
       c.reloptions,
       has_table_privilege('anon', format('public.%I', e.view_name), 'SELECT') AS anon_select,
       has_table_privilege('authenticated', format('public.%I', e.view_name), 'SELECT') AS authenticated_select,
       has_table_privilege('service_role', format('public.%I', e.view_name), 'SELECT') AS service_select
FROM expected e
LEFT JOIN pg_class c
  ON c.relname = e.view_name
 AND c.relnamespace = 'public'::regnamespace
 AND c.relkind = 'v'
ORDER BY e.view_name;

DO $$
DECLARE
    _bad TEXT;
BEGIN
    WITH expected(view_name) AS (
        VALUES
            ('ap_signal_funnel_daily'), ('ap_skipped_signal_outcomes'),
            ('ledger_funnel'), ('ledger_performance'), ('v2_equity_curve'),
            ('v2_last_20_trades'), ('v2_performance')
    )
    SELECT string_agg(e.view_name, ', ' ORDER BY e.view_name)
      INTO _bad
      FROM expected e
      LEFT JOIN pg_class c
        ON c.relname = e.view_name
       AND c.relnamespace = 'public'::regnamespace
       AND c.relkind = 'v'
     WHERE c.oid IS NULL
        OR has_table_privilege('anon', format('public.%I', e.view_name), 'SELECT')
        OR has_table_privilege('authenticated', format('public.%I', e.view_name), 'SELECT')
        OR NOT has_table_privilege('service_role', format('public.%I', e.view_name), 'SELECT');

    IF _bad IS NOT NULL THEN
        RAISE EXCEPTION 'Public view gate failed: %', _bad;
    END IF;

    SELECT string_agg(c.relname, ', ' ORDER BY c.relname)
      INTO _bad
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relkind = 'v'
       AND (
           has_table_privilege('anon', c.oid, 'SELECT')
           OR has_table_privilege('authenticated', c.oid, 'SELECT')
       );

    IF _bad IS NOT NULL THEN
        RAISE EXCEPTION 'Unreviewed anon/authenticated public view access remains: %', _bad;
    END IF;
END $$;

\echo '== Remaining public/anon/authenticated permissive policies =='
SELECT schemaname, tablename, policyname, roles, cmd, qual, with_check
FROM pg_policies
WHERE schemaname = 'public'
  AND roles && ARRAY['public', 'anon', 'authenticated']::name[]
  AND (qual = 'true' OR with_check = 'true')
ORDER BY tablename, policyname;

DO $$
DECLARE
    _bad TEXT;
BEGIN
    SELECT string_agg(format('%s.%s', tablename, policyname), ', '
                      ORDER BY tablename, policyname)
      INTO _bad
      FROM pg_policies
     WHERE schemaname = 'public'
       AND roles && ARRAY['public', 'anon', 'authenticated']::name[]
       AND (qual = 'true' OR with_check = 'true');

    IF _bad IS NOT NULL THEN
        RAISE EXCEPTION 'Unsafe public/anon/authenticated permissive policies remain: %', _bad;
    END IF;
END $$;

\echo '== Reviewed public-role policy contracts and post-deploy identity allowlist =='
DO $$
DECLARE
    _bad TEXT;
BEGIN
    SELECT string_agg(format('%s.%s', e.table_name, e.policy_name), ', '
                      ORDER BY e.table_name, e.policy_name)
      INTO _bad
      FROM (VALUES
        ('members', 'members_read_own', '{public}', 'SELECT',
         '(auth.uid() = user_id)', '<null>', 'PERMISSIVE'),
        ('proof_trades', 'client_sees_own_trades', '{public}', 'SELECT',
         '(client_email = ((current_setting(''request.jwt.claims''::text, true))::json ->> ''email''::text))',
         '<null>', 'PERMISSIVE')
      ) AS e(table_name, policy_name, expected_roles, expected_cmd,
             expected_qual, expected_with_check, expected_permissive)
      LEFT JOIN pg_policies p
        ON p.schemaname = 'public'
       AND p.tablename = e.table_name
       AND p.policyname = e.policy_name
     WHERE p.policyname IS NULL
        OR p.roles::text IS DISTINCT FROM e.expected_roles
        OR p.cmd IS DISTINCT FROM e.expected_cmd
        OR COALESCE(p.qual, '<null>') IS DISTINCT FROM e.expected_qual
        OR COALESCE(p.with_check, '<null>') IS DISTINCT FROM e.expected_with_check
        OR p.permissive IS DISTINCT FROM e.expected_permissive;

    IF _bad IS NOT NULL THEN
        RAISE EXCEPTION 'Preserved owner policy drift: %', _bad;
    END IF;

    SELECT string_agg(format('%s.%s', p.tablename, p.policyname), ', '
                      ORDER BY p.tablename, p.policyname)
      INTO _bad
      FROM pg_policies p
     WHERE p.schemaname = 'public'
       AND p.roles && ARRAY['public', 'anon', 'authenticated']::name[]
       AND NOT EXISTS (
           SELECT 1
             FROM (VALUES
               ('members', 'members_read_own'),
               ('proof_trades', 'client_sees_own_trades')
             ) AS e(table_name, policy_name)
            WHERE p.tablename = e.table_name
              AND p.policyname = e.policy_name
       );

    IF _bad IS NOT NULL THEN
        RAISE EXCEPTION
            'Unreviewed post-deploy public/anon/authenticated policy identity drift: %', _bad;
    END IF;
END $$;

\echo '== Public table drift and default privileges =='
SELECT count(*) FILTER (WHERE NOT c.relrowsecurity) AS public_rls_disabled_tables,
       string_agg(c.relname, ', ' ORDER BY c.relname)
       FILTER (WHERE NOT c.relrowsecurity) AS public_rls_disabled_names
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p');

SELECT
    COALESCE(n.nspname, '(all schemas)') AS schema_name,
    r.rolname AS owner_role,
    d.defaclobjtype AS object_type,
    d.defaclacl::text AS default_acl
FROM pg_default_acl d
LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace
JOIN pg_roles r ON r.oid = d.defaclrole
WHERE d.defaclobjtype IN ('r', 'S', 'f')
  AND (n.nspname = 'public' OR n.nspname IS NULL)
ORDER BY schema_name, owner_role, object_type;

DO $$
DECLARE
    _bad TEXT;
BEGIN
    SELECT string_agg(c.relname, ', ' ORDER BY c.relname)
      INTO _bad
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relkind IN ('r', 'p')
       AND NOT c.relrowsecurity;

    IF _bad IS NOT NULL THEN
        RAISE EXCEPTION 'Public tables with RLS disabled remain: %', _bad;
    END IF;

    SELECT string_agg(
               format('%s:%s:%s', r.rolname, d.defaclobjtype,
                      COALESCE(g.rolname, 'PUBLIC')),
               ', ' ORDER BY r.rolname, d.defaclobjtype
           )
      INTO _bad
      FROM pg_default_acl d
      LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace
      JOIN pg_roles r ON r.oid = d.defaclrole
      CROSS JOIN LATERAL aclexplode(d.defaclacl) x
      LEFT JOIN pg_roles g ON g.oid = x.grantee
     WHERE d.defaclobjtype IN ('r', 'S', 'f')
       AND (n.nspname = 'public' OR n.nspname IS NULL)
       AND (x.grantee = 0 OR g.rolname IN ('anon', 'authenticated'));

    IF _bad IS NOT NULL THEN
        RAISE EXCEPTION 'PUBLIC/anon/authenticated default privileges remain: %', _bad;
    END IF;
END $$;
