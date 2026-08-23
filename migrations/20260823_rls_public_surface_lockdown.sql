-- SECURITY REVIEW DRAFT — DO NOT APPLY TO PRODUCTION WITHOUT STAGING PROOF.
--
-- Phase 1 of the Supabase RLS remediation.  The live project has 22 public
-- tables with RLS disabled and grants to anon/authenticated.  It also has
-- seven public security-definer views and 18 permissive public/anon policies
-- that would remain reachable after a table-only lockdown.  This migration
-- closes those known public paths without inventing tenant policies: the
-- dashboard auth model and the client_email/client_id ownership bridge are
-- not finalized.
--
-- Safety contract:
--   * service_role and the direct PostgreSQL bot path retain their existing
--     privileges; service_role/postgres bypass RLS in this project.
--   * anon/authenticated receive no table or view access on the reviewed
--     public surface, so the Data API is deny-by-default until explicit,
--     owner-scoped policies are approved.
--   * the preflight fails before DDL if the reviewed table/view/policy scope,
--     owner, or service_role access has drifted.
--   * only the exact, live-verified permissive public/anon policies are
--     removed.  The service_role-only client_health policy is preserved.
--
-- This repository's migration runner owns the transaction.  Do not add
-- BEGIN/COMMIT/ROLLBACK here.

DO $$
DECLARE
    _missing_tables TEXT;
    _owner_drift TEXT;
    _service_access_gap TEXT;
    _unexpected_disabled TEXT;
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
           has_table_privilege('anon', format('public.%I', c.relname), 'SELECT')
           OR has_table_privilege('authenticated', format('public.%I', c.relname), 'SELECT')
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
    _policy_table_gap TEXT;
BEGIN
    SELECT string_agg(format('%s.%s', e.table_name, e.policy_name), ', '
                      ORDER BY e.table_name, e.policy_name)
      INTO _policy_drift
      FROM (VALUES
        ('alert_routes', 'anon_read', '{public}', 'SELECT', 'true', '<null>'),
        ('ap_admin_audit', 'anon_read', '{public}', 'SELECT', 'true', '<null>'),
        ('ap_signal_underlying_outcomes', 'anon_all_underlying', '{anon}', 'ALL', 'true', 'true'),
        ('ap_system_control', 'anon_read', '{public}', 'SELECT', 'true', '<null>'),
        ('bot_status', 'anon_all_bot_status', '{anon}', 'ALL', 'true', 'true'),
        ('client_health', 'anon_read_client_health', '{anon}', 'SELECT', 'true', '<null>'),
        ('content_queue', 'anon_read', '{public}', 'SELECT', 'true', '<null>'),
        ('daily_cadence_logs', 'anon_read', '{public}', 'SELECT', 'true', '<null>'),
        ('incidents', 'anon_read', '{public}', 'SELECT', 'true', '<null>'),
        ('market_data', 'Anyone can read market data', '{anon}', 'SELECT', 'true', '<null>'),
        ('option_outcomes', 'anon_all_option_outcomes', '{anon}', 'ALL', 'true', 'true'),
        ('proof_daily_summary', 'anon_all_proof_daily', '{anon}', 'ALL', 'true', 'true'),
        ('proof_trades', 'anon_all_proof_trades', '{anon}', 'ALL', 'true', 'true'),
        ('proof_trades', 'service_role_all', '{public}', 'ALL', 'true', '<null>'),
        ('proof_vault', 'anon_read', '{public}', 'SELECT', 'true', '<null>'),
        ('signal_outcomes', 'anon_all_signal_outcomes', '{anon}', 'ALL', 'true', 'true'),
        ('signals', 'Anyone can read signals', '{anon}', 'SELECT', 'true', '<null>'),
        ('system_health_events', 'anon_read', '{public}', 'SELECT', 'true', '<null>')
      ) AS e(table_name, policy_name, expected_roles, expected_cmd,
             expected_qual, expected_with_check)
      LEFT JOIN pg_policies p
        ON p.schemaname = 'public'
       AND p.tablename = e.table_name
       AND p.policyname = e.policy_name
     WHERE p.policyname IS NULL
        OR p.roles::text IS DISTINCT FROM e.expected_roles
        OR p.cmd IS DISTINCT FROM e.expected_cmd
        OR COALESCE(p.qual, '<null>') IS DISTINCT FROM e.expected_qual
        OR COALESCE(p.with_check, '<null>') IS DISTINCT FROM e.expected_with_check;

    IF _policy_drift IS NOT NULL THEN
        RAISE EXCEPTION
            'RLS hardening preflight failed: permissive policy drift: %',
            _policy_drift;
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
