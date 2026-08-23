-- SECURITY REVIEW DRAFT — DO NOT APPLY TO PRODUCTION WITHOUT STAGING PROOF.
--
-- Phase 1 of the Supabase RLS remediation.  The live project has 22 public
-- tables with RLS disabled and grants to anon/authenticated.  This migration
-- deliberately does not invent tenant policies: the dashboard auth model and
-- the client_email/client_id ownership bridge are not finalized.
--
-- Safety contract:
--   * service_role and the direct PostgreSQL bot path retain their existing
--     privileges; service_role/postgres bypass RLS in this project.
--   * anon/authenticated receive no table access and no RLS policies on this
--     phase-1 scope, so the Data API is deny-by-default for these tables.
--   * the preflight fails before DDL if the reviewed table scope, owner, or
--     service_role access has drifted.
--   * no view or existing-policy rewrite is included here.  Those changes
--     require staging consumer proof and are reported by the validation SQL.
--
-- This repository's migration runner owns the transaction.  Do not add
-- BEGIN/COMMIT/ROLLBACK here.

DO $$
DECLARE
    _missing_tables TEXT;
    _owner_drift TEXT;
    _service_access_gap TEXT;
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

-- Stop new objects created by the PostgreSQL migration owner from inheriting
-- public Data API access.  Supabase-managed supabase_admin defaults are
-- reported by the validation SQL and require owner-authorized follow-up.
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL ON TABLES FROM PUBLIC, anon, authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL ON SEQUENCES FROM PUBLIC, anon, authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL ON FUNCTIONS FROM PUBLIC, anon, authenticated;
