-- SECURITY REVIEW DRAFT — DO NOT APPLY TO PRODUCTION WITHOUT STAGING PROOF.
--
-- Phase 1 of the Supabase RLS remediation.  The live project has 22 public
-- tables with RLS disabled and grants to anon/authenticated.  It also has
-- seven public security-definer views and 18 permissive public/anon policies
-- plus 24 public sequences with anon/authenticated privileges that would
-- remain reachable after a table-only lockdown.  This migration closes those
-- known public paths without inventing tenant policies: the
-- dashboard auth model and the client_email/client_id ownership bridge are
-- not finalized.
--
-- Safety contract:
--   * service_role and the direct PostgreSQL bot path retain their existing
--     privileges; service_role/postgres bypass RLS in this project.
--   * anon/authenticated receive no table or view access on the reviewed
--     public surface, so the Data API is deny-by-default until explicit,
--     owner-scoped policies are approved.
--   * anon/authenticated receive no access to the reviewed public sequences;
--     service_role retains the sequence privileges required by inserts.
--   * the preflight fails before DDL if the reviewed table/view/sequence/policy
--     scope, owner, or service_role access has drifted, including unexpected
--     exposed sequences or permissive public/anon policies.
--   * only the exact, live-verified permissive public/anon policies are
--     removed.  The service_role-only client_health policy is preserved.
--
-- This repository's migration runner owns the transaction.  Do not add
-- BEGIN/COMMIT/ROLLBACK here.

DO $$
DECLARE
    _sequence_names CONSTANT TEXT[] := ARRAY[
        'ap_admin_audit_id_seq',
        'ap_audit_log_id_seq',
        'ap_client_account_snapshots_id_seq',
        'ap_intelligence_outcome_bindings_id_seq',
        'ap_system_events_id_seq',
        'applications_id_seq',
        'audit_log_id_seq',
        'blocked_signal_counterfactuals_id_seq',
        'bot_status_id_seq',
        'broker_order_audit_id_seq',
        'client_signal_opportunities_id_seq',
        'daily_performance_id_seq',
        'decision_events_id_seq',
        'exit_decision_ledger_id_seq',
        'market_data_id_seq',
        'operator_audit_log_id_seq',
        'option_outcomes_id_seq',
        'orders_id_seq',
        'proof_daily_summary_id_seq',
        'proof_trades_id_seq',
        'signal_outcomes_id_seq',
        'signals_id_seq',
        'trade_queue_id_seq',
        'trades_id_seq'
    ];
    _missing_tables TEXT;
    _missing_sequences TEXT;
    _owner_drift TEXT;
    _sequence_owner_drift TEXT;
    _service_access_gap TEXT;
    _service_role_bypass_gap TEXT;
    _sequence_service_access_gap TEXT;
    _unexpected_disabled TEXT;
    _unexpected_sequence_access TEXT;
    _unexpected_view_access TEXT;
BEGIN
    SELECT string_agg(v.table_name, ', ' ORDER BY v.table_name)
      INTO _missing_tables
      FROM (VALUES
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
      ) AS v(table_name)
      LEFT JOIN pg_class c
        ON c.relname = v.table_name
       AND c.relnamespace = 'public'::regnamespace
       AND c.relkind IN ('r', 'p')
     WHERE c.oid IS NULL;

    IF _missing_tables IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: missing public tables: %',
            _missing_tables;
    END IF;

    SELECT string_agg(v.table_name, ', ' ORDER BY v.table_name)
      INTO _owner_drift
      FROM (VALUES
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
      ) AS v(table_name)
      JOIN pg_class c
        ON c.relname = v.table_name
       AND c.relnamespace = 'public'::regnamespace
     WHERE pg_get_userbyid(c.relowner) <> 'postgres';

    IF _owner_drift IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: unexpected table owner(s): %',
            _owner_drift;
    END IF;

    SELECT string_agg(v.table_name, ', ' ORDER BY v.table_name)
      INTO _service_access_gap
      FROM (VALUES
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
      ) AS v(table_name)
     WHERE NOT (
         has_table_privilege('service_role', format('public.%I', v.table_name), 'SELECT')
         AND has_table_privilege('service_role', format('public.%I', v.table_name), 'INSERT')
         AND has_table_privilege('service_role', format('public.%I', v.table_name), 'UPDATE')
         AND has_table_privilege('service_role', format('public.%I', v.table_name), 'DELETE')
     );

    IF _service_access_gap IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: service_role access gap(s): %',
            _service_access_gap;
    END IF;

    SELECT string_agg(
               CASE
                   WHEN r.oid IS NULL THEN e.role_name || ' (missing)'
                   ELSE e.role_name || ' (BYPASSRLS=false)'
               END,
               ', ' ORDER BY e.role_name
           )
      INTO _service_role_bypass_gap
      FROM (VALUES ('service_role')) AS e(role_name)
      LEFT JOIN pg_roles r ON r.rolname = e.role_name
     WHERE r.oid IS NULL OR NOT r.rolbypassrls;

    IF _service_role_bypass_gap IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: service_role must retain BYPASSRLS: %',
            _service_role_bypass_gap;
    END IF;

    SELECT string_agg(v.sequence_name, ', ' ORDER BY v.sequence_name)
      INTO _missing_sequences
      FROM unnest(_sequence_names) AS v(sequence_name)
      LEFT JOIN pg_class c
        ON c.relname = v.sequence_name
       AND c.relnamespace = 'public'::regnamespace
       AND c.relkind = 'S'
     WHERE c.oid IS NULL;

    IF _missing_sequences IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: missing public sequences: %',
            _missing_sequences;
    END IF;

    SELECT string_agg(v.sequence_name, ', ' ORDER BY v.sequence_name)
      INTO _sequence_owner_drift
      FROM unnest(_sequence_names) AS v(sequence_name)
      JOIN pg_class c
        ON c.relname = v.sequence_name
       AND c.relnamespace = 'public'::regnamespace
       AND c.relkind = 'S'
     WHERE pg_get_userbyid(c.relowner) <> 'postgres';

    IF _sequence_owner_drift IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: unexpected sequence owner(s): %',
            _sequence_owner_drift;
    END IF;

    SELECT string_agg(v.sequence_name, ', ' ORDER BY v.sequence_name)
      INTO _sequence_service_access_gap
      FROM unnest(_sequence_names) AS v(sequence_name)
     WHERE NOT (
         has_sequence_privilege('service_role', format('public.%I', v.sequence_name), 'USAGE')
         AND has_sequence_privilege('service_role', format('public.%I', v.sequence_name), 'SELECT')
         AND has_sequence_privilege('service_role', format('public.%I', v.sequence_name), 'UPDATE')
     );

    IF _sequence_service_access_gap IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: service_role sequence access gap(s): %',
            _sequence_service_access_gap;
    END IF;

    SELECT string_agg(c.relname, ', ' ORDER BY c.relname)
      INTO _unexpected_sequence_access
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
       )
       AND c.relname <> ALL (_sequence_names);

    IF _unexpected_sequence_access IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: unreviewed public sequences with anon/authenticated access: %',
            _unexpected_sequence_access;
    END IF;

    SELECT string_agg(c.relname, ', ' ORDER BY c.relname)
      INTO _unexpected_disabled
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relkind IN ('r', 'p')
       AND NOT c.relrowsecurity
       AND c.relname <> ALL (ARRAY[
           'account_snapshots',
           'ap_audit_log',
           'ap_edge_buckets',
           'ap_signal_context_tags',
           'ap_signal_hub_merged',
           'ap_signal_hub_raw',
           'ap_signal_ledger',
           'ap_signal_option_outcomes',
           'ap_signals',
           'ap_signals_jason_manual_push_backup_20260616',
           'ap_trade_log',
           'ap_whitelist',
           'audit_log',
           'client_authorizations',
           'client_state',
           'kv',
           'orders',
           'orders_backup_jason_null_mode_2026_06_14',
           'orders_backup_jason_null_mode_orphans_2026_06_14',
           'processed_signals',
           'schema_migrations',
           'trade_fills'
       ]);

    IF _unexpected_disabled IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: unreviewed public tables with RLS disabled: %',
            _unexpected_disabled;
    END IF;

    SELECT string_agg(c.relname, ', ' ORDER BY c.relname)
      INTO _unexpected_view_access
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relkind = 'v'
       AND (
           has_table_privilege('anon', c.oid, 'SELECT')
           OR has_table_privilege('authenticated', c.oid, 'SELECT')
       )
       AND c.relname <> ALL (ARRAY[
           'ap_signal_funnel_daily',
           'ap_skipped_signal_outcomes',
           'ledger_funnel',
           'ledger_performance',
           'v2_equity_curve',
           'v2_last_20_trades',
           'v2_performance'
       ]);

    IF _unexpected_view_access IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: unreviewed anon/authenticated public view access: %',
            _unexpected_view_access;
    END IF;
END $$;

-- The seven currently exposed public views are closed to PUBLIC/anon/
-- authenticated and explicitly retained for service_role.  The service-role
-- grants are explicit because REVOKE PUBLIC also removes inherited access.
DO $$
DECLARE
    _view_scope_gap TEXT;
    _view_service_gap TEXT;
BEGIN
    SELECT string_agg(v.view_name, ', ' ORDER BY v.view_name)
      INTO _view_scope_gap
      FROM (VALUES
        ('ap_signal_funnel_daily'),
        ('ap_skipped_signal_outcomes'),
        ('ledger_funnel'),
        ('ledger_performance'),
        ('v2_equity_curve'),
        ('v2_last_20_trades'),
        ('v2_performance')
      ) AS v(view_name)
      LEFT JOIN pg_class c
        ON c.relname = v.view_name
       AND c.relnamespace = 'public'::regnamespace
       AND c.relkind = 'v'
     WHERE c.oid IS NULL OR pg_get_userbyid(c.relowner) <> 'postgres';

    IF _view_scope_gap IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: public view scope/owner drift: %',
            _view_scope_gap;
    END IF;

    SELECT string_agg(v.view_name, ', ' ORDER BY v.view_name)
      INTO _view_service_gap
      FROM (VALUES
        ('ap_signal_funnel_daily'),
        ('ap_skipped_signal_outcomes'),
        ('ledger_funnel'),
        ('ledger_performance'),
        ('v2_equity_curve'),
        ('v2_last_20_trades'),
        ('v2_performance')
      ) AS v(view_name)
     WHERE NOT has_table_privilege(
         'service_role', format('public.%I', v.view_name), 'SELECT'
     );

    IF _view_service_gap IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: service_role view access gap(s): %',
            _view_service_gap;
    END IF;
END $$;

-- Verify the exact live permissive policies before dropping them.  A changed
-- policy definition is a hard stop rather than permission to drop a policy
-- whose ownership contract may have been repaired independently.
DO $$
DECLARE
    _policy_drift TEXT;
    _preserved_policy_drift TEXT;
    _unexpected_public_policy TEXT;
    _unexpected_permissive_policy TEXT;
    _policy_table_gap TEXT;
BEGIN
    SELECT string_agg(format('%s.%s', e.table_name, e.policy_name), ', '
                      ORDER BY e.table_name, e.policy_name)
      INTO _policy_drift
      FROM (VALUES
        ('alert_routes', 'anon_read', '{public}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
        ('ap_admin_audit', 'anon_read', '{public}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
        ('ap_signal_underlying_outcomes', 'anon_all_underlying', '{anon}', 'ALL', 'true', 'true', 'PERMISSIVE'),
        ('ap_system_control', 'anon_read', '{public}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
        ('bot_status', 'anon_all_bot_status', '{anon}', 'ALL', 'true', 'true', 'PERMISSIVE'),
        ('client_health', 'anon_read_client_health', '{anon}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
        ('content_queue', 'anon_read', '{public}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
        ('daily_cadence_logs', 'anon_read', '{public}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
        ('incidents', 'anon_read', '{public}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
        ('market_data', 'Anyone can read market data', '{anon}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
        ('option_outcomes', 'anon_all_option_outcomes', '{anon}', 'ALL', 'true', 'true', 'PERMISSIVE'),
        ('proof_daily_summary', 'anon_all_proof_daily', '{anon}', 'ALL', 'true', 'true', 'PERMISSIVE'),
        ('proof_trades', 'anon_all_proof_trades', '{anon}', 'ALL', 'true', 'true', 'PERMISSIVE'),
        ('proof_trades', 'service_role_all', '{public}', 'ALL', 'true', '<null>', 'PERMISSIVE'),
        ('proof_vault', 'anon_read', '{public}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
        ('signal_outcomes', 'anon_all_signal_outcomes', '{anon}', 'ALL', 'true', 'true', 'PERMISSIVE'),
        ('signals', 'Anyone can read signals', '{anon}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
        ('system_health_events', 'anon_read', '{public}', 'SELECT', 'true', '<null>', 'PERMISSIVE')
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

    IF _policy_drift IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: permissive policy drift: %',
            _policy_drift;
    END IF;

    -- These two owner-scoped public policies are retained intentionally.  The
    -- exact predicate, role, command, and permissiveness are part of the
    -- reviewed security boundary; a changed predicate is a hard stop.
    SELECT string_agg(format('%s.%s', e.table_name, e.policy_name), ', '
                      ORDER BY e.table_name, e.policy_name)
      INTO _preserved_policy_drift
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

    IF _preserved_policy_drift IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: preserved owner policy drift: %',
            _preserved_policy_drift;
    END IF;

    -- Do not classify only literal TRUE predicates as unsafe.  Any new or
    -- renamed policy visible to public/anon/authenticated is unreviewed until
    -- it is added to this explicit allowlist and contract-checked above.
    SELECT string_agg(format('%s.%s', p.tablename, p.policyname), ', '
                      ORDER BY p.tablename, p.policyname)
      INTO _unexpected_public_policy
      FROM pg_policies p
     WHERE p.schemaname = 'public'
       AND p.roles && ARRAY['public', 'anon', 'authenticated']::name[]
       AND NOT EXISTS (
           SELECT 1
             FROM (VALUES
               ('alert_routes', 'anon_read'),
               ('ap_admin_audit', 'anon_read'),
               ('ap_signal_underlying_outcomes', 'anon_all_underlying'),
               ('ap_system_control', 'anon_read'),
               ('bot_status', 'anon_all_bot_status'),
               ('client_health', 'anon_read_client_health'),
               ('content_queue', 'anon_read'),
               ('daily_cadence_logs', 'anon_read'),
               ('incidents', 'anon_read'),
               ('market_data', 'Anyone can read market data'),
               ('option_outcomes', 'anon_all_option_outcomes'),
               ('proof_daily_summary', 'anon_all_proof_daily'),
               ('proof_trades', 'anon_all_proof_trades'),
               ('proof_trades', 'service_role_all'),
               ('proof_vault', 'anon_read'),
               ('signal_outcomes', 'anon_all_signal_outcomes'),
               ('signals', 'Anyone can read signals'),
               ('system_health_events', 'anon_read'),
               ('members', 'members_read_own'),
               ('proof_trades', 'client_sees_own_trades')
             ) AS e(table_name, policy_name)
            WHERE p.tablename = e.table_name
              AND p.policyname = e.policy_name
       );

    IF _unexpected_public_policy IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: unreviewed public/anon/authenticated policy identity drift: %',
            _unexpected_public_policy;
    END IF;

    SELECT string_agg(format('%s.%s', p.tablename, p.policyname), ', '
                      ORDER BY p.tablename, p.policyname)
      INTO _unexpected_permissive_policy
      FROM pg_policies p
     WHERE p.schemaname = 'public'
       AND p.roles && ARRAY['public', 'anon', 'authenticated']::name[]
       AND (p.qual = 'true' OR p.with_check = 'true')
       AND NOT EXISTS (
           SELECT 1
             FROM (VALUES
               ('alert_routes', 'anon_read', '{public}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
               ('ap_admin_audit', 'anon_read', '{public}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
               ('ap_signal_underlying_outcomes', 'anon_all_underlying', '{anon}', 'ALL', 'true', 'true', 'PERMISSIVE'),
               ('ap_system_control', 'anon_read', '{public}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
               ('bot_status', 'anon_all_bot_status', '{anon}', 'ALL', 'true', 'true', 'PERMISSIVE'),
               ('client_health', 'anon_read_client_health', '{anon}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
               ('content_queue', 'anon_read', '{public}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
               ('daily_cadence_logs', 'anon_read', '{public}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
               ('incidents', 'anon_read', '{public}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
               ('market_data', 'Anyone can read market data', '{anon}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
               ('option_outcomes', 'anon_all_option_outcomes', '{anon}', 'ALL', 'true', 'true', 'PERMISSIVE'),
               ('proof_daily_summary', 'anon_all_proof_daily', '{anon}', 'ALL', 'true', 'true', 'PERMISSIVE'),
               ('proof_trades', 'anon_all_proof_trades', '{anon}', 'ALL', 'true', 'true', 'PERMISSIVE'),
               ('proof_trades', 'service_role_all', '{public}', 'ALL', 'true', '<null>', 'PERMISSIVE'),
               ('proof_vault', 'anon_read', '{public}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
               ('signal_outcomes', 'anon_all_signal_outcomes', '{anon}', 'ALL', 'true', 'true', 'PERMISSIVE'),
               ('signals', 'Anyone can read signals', '{anon}', 'SELECT', 'true', '<null>', 'PERMISSIVE'),
               ('system_health_events', 'anon_read', '{public}', 'SELECT', 'true', '<null>', 'PERMISSIVE')
             ) AS e(table_name, policy_name, expected_roles, expected_cmd,
                    expected_qual, expected_with_check, expected_permissive)
            WHERE p.tablename = e.table_name
              AND p.policyname = e.policy_name
              AND p.roles::text = e.expected_roles
              AND p.cmd = e.expected_cmd
              AND COALESCE(p.qual, '<null>') = e.expected_qual
              AND COALESCE(p.with_check, '<null>') = e.expected_with_check
              AND p.permissive = e.expected_permissive
       );

    IF _unexpected_permissive_policy IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: unreviewed permissive public/anon policy drift: %',
            _unexpected_permissive_policy;
    END IF;

    SELECT string_agg(v.table_name, ', ' ORDER BY v.table_name)
      INTO _policy_table_gap
      FROM (VALUES
        ('alert_routes'),
        ('ap_admin_audit'),
        ('ap_signal_underlying_outcomes'),
        ('ap_system_control'),
        ('bot_status'),
        ('client_health'),
        ('content_queue'),
        ('daily_cadence_logs'),
        ('incidents'),
        ('market_data'),
        ('option_outcomes'),
        ('proof_daily_summary'),
        ('proof_trades'),
        ('proof_vault'),
        ('signal_outcomes'),
        ('signals'),
        ('system_health_events')
      ) AS v(table_name)
      LEFT JOIN pg_class c
        ON c.relname = v.table_name
       AND c.relnamespace = 'public'::regnamespace
       AND c.relkind IN ('r', 'p')
     WHERE c.oid IS NULL
        OR NOT c.relrowsecurity
        OR pg_get_userbyid(c.relowner) <> 'postgres'
        OR NOT (
            has_table_privilege('service_role', format('public.%I', v.table_name), 'SELECT')
            AND has_table_privilege('service_role', format('public.%I', v.table_name), 'INSERT')
            AND has_table_privilege('service_role', format('public.%I', v.table_name), 'UPDATE')
            AND has_table_privilege('service_role', format('public.%I', v.table_name), 'DELETE')
        );

    IF _policy_table_gap IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: policy-table scope/owner/access drift: %',
            _policy_table_gap;
    END IF;
END $$;

-- The application role cannot alter Supabase-managed defaults owned by
-- supabase_admin.  Stop before any DDL until that owner-authorized follow-up
-- has removed PUBLIC/anon/authenticated defaults in public.
DO $$
DECLARE
    _default_privilege_gap TEXT;
BEGIN
    SELECT string_agg(
               format('%s:%s:%s', r.rolname, d.defaclobjtype,
                      COALESCE(g.rolname, 'PUBLIC')),
               ', ' ORDER BY r.rolname, d.defaclobjtype
           )
      INTO _default_privilege_gap
      FROM pg_default_acl d
      LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace
      JOIN pg_roles r ON r.oid = d.defaclrole
      CROSS JOIN LATERAL aclexplode(d.defaclacl) x
      LEFT JOIN pg_roles g ON g.oid = x.grantee
     WHERE r.rolname = 'supabase_admin'
       AND d.defaclobjtype IN ('r', 'S', 'f')
       AND (n.nspname = 'public' OR n.nspname IS NULL)
       AND (x.grantee = 0 OR g.rolname IN ('anon', 'authenticated'));

    IF _default_privilege_gap IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: supabase_admin default privileges require owner authorization: %',
            _default_privilege_gap;
    END IF;
END $$;

ALTER TABLE public.account_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ap_audit_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ap_edge_buckets ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ap_signal_context_tags ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ap_signal_hub_merged ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ap_signal_hub_raw ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ap_signal_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ap_signal_option_outcomes ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ap_signals ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ap_signals_jason_manual_push_backup_20260616 ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ap_trade_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ap_whitelist ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.audit_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.client_authorizations ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.client_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.kv ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.orders_backup_jason_null_mode_2026_06_14 ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.orders_backup_jason_null_mode_orphans_2026_06_14 ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.processed_signals ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.schema_migrations ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.trade_fills ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE public.account_snapshots FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ap_audit_log FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ap_edge_buckets FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ap_signal_context_tags FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ap_signal_hub_merged FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ap_signal_hub_raw FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ap_signal_ledger FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ap_signal_option_outcomes FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ap_signals FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ap_signals_jason_manual_push_backup_20260616 FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ap_trade_log FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ap_whitelist FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.audit_log FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.client_authorizations FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.client_state FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.kv FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.orders FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.orders_backup_jason_null_mode_2026_06_14 FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.orders_backup_jason_null_mode_orphans_2026_06_14 FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.processed_signals FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.schema_migrations FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.trade_fills FROM PUBLIC, anon, authenticated;

-- Make the privileged table path explicit after removing PUBLIC inheritance.
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE
    public.account_snapshots,
    public.ap_audit_log,
    public.ap_edge_buckets,
    public.ap_signal_context_tags,
    public.ap_signal_hub_merged,
    public.ap_signal_hub_raw,
    public.ap_signal_ledger,
    public.ap_signal_option_outcomes,
    public.ap_signals,
    public.ap_signals_jason_manual_push_backup_20260616,
    public.ap_trade_log,
    public.ap_whitelist,
    public.audit_log,
    public.client_authorizations,
    public.client_state,
    public.kv,
    public.orders,
    public.orders_backup_jason_null_mode_2026_06_14,
    public.orders_backup_jason_null_mode_orphans_2026_06_14,
    public.processed_signals,
    public.schema_migrations,
    public.trade_fills
TO service_role;

-- Sequences are a separate privilege surface from their owning tables.  The
-- live project grants anon/authenticated sequence access through defaults, so
-- close the existing 24 sequences while retaining the bot's nextval path.
REVOKE ALL ON SEQUENCE public.ap_admin_audit_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.ap_audit_log_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.ap_client_account_snapshots_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.ap_intelligence_outcome_bindings_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.ap_system_events_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.applications_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.audit_log_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.blocked_signal_counterfactuals_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.bot_status_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.broker_order_audit_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.client_signal_opportunities_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.daily_performance_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.decision_events_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.exit_decision_ledger_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.market_data_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.operator_audit_log_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.option_outcomes_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.orders_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.proof_daily_summary_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.proof_trades_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.signal_outcomes_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.signals_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.trade_queue_id_seq FROM PUBLIC, anon, authenticated;
REVOKE ALL ON SEQUENCE public.trades_id_seq FROM PUBLIC, anon, authenticated;

GRANT USAGE, SELECT, UPDATE ON SEQUENCE
    public.ap_admin_audit_id_seq,
    public.ap_audit_log_id_seq,
    public.ap_client_account_snapshots_id_seq,
    public.ap_intelligence_outcome_bindings_id_seq,
    public.ap_system_events_id_seq,
    public.applications_id_seq,
    public.audit_log_id_seq,
    public.blocked_signal_counterfactuals_id_seq,
    public.bot_status_id_seq,
    public.broker_order_audit_id_seq,
    public.client_signal_opportunities_id_seq,
    public.daily_performance_id_seq,
    public.decision_events_id_seq,
    public.exit_decision_ledger_id_seq,
    public.market_data_id_seq,
    public.operator_audit_log_id_seq,
    public.option_outcomes_id_seq,
    public.orders_id_seq,
    public.proof_daily_summary_id_seq,
    public.proof_trades_id_seq,
    public.signal_outcomes_id_seq,
    public.signals_id_seq,
    public.trade_queue_id_seq,
    public.trades_id_seq
TO service_role;

-- Close the separately audited public/anon policy tables.  RLS is already
-- enabled on this scope; with the permissive policies removed below, these
-- roles remain deny-by-default until explicit owner policies are approved.
REVOKE ALL ON TABLE public.alert_routes FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ap_admin_audit FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ap_signal_underlying_outcomes FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ap_system_control FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.bot_status FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.client_health FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.content_queue FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.daily_cadence_logs FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.incidents FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.market_data FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.option_outcomes FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.proof_daily_summary FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.proof_trades FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.proof_vault FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.signal_outcomes FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.signals FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.system_health_events FROM PUBLIC, anon, authenticated;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE
    public.alert_routes,
    public.ap_admin_audit,
    public.ap_signal_underlying_outcomes,
    public.ap_system_control,
    public.bot_status,
    public.client_health,
    public.content_queue,
    public.daily_cadence_logs,
    public.incidents,
    public.market_data,
    public.option_outcomes,
    public.proof_daily_summary,
    public.proof_trades,
    public.proof_vault,
    public.signal_outcomes,
    public.signals,
    public.system_health_events
TO service_role;

-- A security-definer view bypasses the caller's base-table RLS.  Revoke its
-- public Data API path and retain an explicit privileged read path.
REVOKE ALL ON TABLE public.ap_signal_funnel_daily FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ap_skipped_signal_outcomes FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ledger_funnel FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.ledger_performance FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.v2_equity_curve FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.v2_last_20_trades FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.v2_performance FROM PUBLIC, anon, authenticated;

GRANT SELECT ON TABLE
    public.ap_signal_funnel_daily,
    public.ap_skipped_signal_outcomes,
    public.ledger_funnel,
    public.ledger_performance,
    public.v2_equity_curve,
    public.v2_last_20_trades,
    public.v2_performance
TO service_role;

-- Drop only the exact live-verified public/anon policies preflighted above.
-- The service_role-only client_health.service_write_client_health policy is
-- intentionally preserved.
DROP POLICY anon_read ON public.alert_routes;
DROP POLICY anon_read ON public.ap_admin_audit;
DROP POLICY anon_all_underlying ON public.ap_signal_underlying_outcomes;
DROP POLICY anon_read ON public.ap_system_control;
DROP POLICY anon_all_bot_status ON public.bot_status;
DROP POLICY anon_read_client_health ON public.client_health;
DROP POLICY anon_read ON public.content_queue;
DROP POLICY anon_read ON public.daily_cadence_logs;
DROP POLICY anon_read ON public.incidents;
DROP POLICY "Anyone can read market data" ON public.market_data;
DROP POLICY anon_all_option_outcomes ON public.option_outcomes;
DROP POLICY anon_all_proof_daily ON public.proof_daily_summary;
DROP POLICY anon_all_proof_trades ON public.proof_trades;
DROP POLICY service_role_all ON public.proof_trades;
DROP POLICY anon_read ON public.proof_vault;
DROP POLICY anon_all_signal_outcomes ON public.signal_outcomes;
DROP POLICY "Anyone can read signals" ON public.signals;
DROP POLICY anon_read ON public.system_health_events;

-- Stop new objects created by the PostgreSQL migration owner from inheriting
-- public Data API access.  Supabase-managed supabase_admin defaults are
-- reported by the validation SQL and require owner-authorized follow-up.
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL ON TABLES FROM PUBLIC, anon, authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL ON SEQUENCES FROM PUBLIC, anon, authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL ON FUNCTIONS FROM PUBLIC, anon, authenticated;
