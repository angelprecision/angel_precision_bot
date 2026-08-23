-- Read-only post-deploy validation for the RLS hardening review migration.
-- Expected phase-1 result:
--   * 22/22 target tables have rls_enabled=true.
--   * anon/authenticated have no table privileges on the target scope.
--   * service_role retains SELECT/INSERT/UPDATE/DELETE.
--   * public views and permissive policies are reported as separate HOLD gates.

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
    c.relrowsecurity AS rls_enabled,
    c.relforcerowsecurity AS rls_forced,
    pg_get_userbyid(c.relowner) AS owner,
    COALESCE(p.policy_count, 0) AS policy_count,
    has_table_privilege('anon', format('public.%I', e.table_name), 'SELECT') AS anon_select,
    has_table_privilege('authenticated', format('public.%I', e.table_name), 'SELECT') AS authenticated_select,
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

\echo '== RLS hardening summary =='
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
    count(*) FILTER (WHERE c.relrowsecurity) AS rls_enabled_tables,
    count(*) FILTER (WHERE NOT c.relrowsecurity) AS rls_disabled_tables,
    count(*) FILTER (
        WHERE has_table_privilege('anon', format('public.%I', e.table_name), 'SELECT')
    ) AS anon_select_tables,
    count(*) FILTER (
        WHERE has_table_privilege('authenticated', format('public.%I', e.table_name), 'SELECT')
    ) AS authenticated_select_tables
FROM expected e
JOIN pg_class c
  ON c.relname = e.table_name
 AND c.relnamespace = 'public'::regnamespace
 AND c.relkind IN ('r', 'p');

\echo '== Public views requiring a separate consumer-approved gate =='
SELECT
    c.relname AS view_name,
    pg_get_userbyid(c.relowner) AS owner,
    c.reloptions,
    has_table_privilege('anon', format('public.%I', c.relname), 'SELECT') AS anon_select,
    has_table_privilege('authenticated', format('public.%I', c.relname), 'SELECT') AS authenticated_select
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('v', 'm')
ORDER BY c.relname;

\echo '== Permissive policies requiring policy-owner review =='
SELECT
    schemaname,
    tablename,
    policyname,
    roles,
    cmd,
    qual,
    with_check
FROM pg_policies
WHERE schemaname = 'public'
  AND (qual = 'true' OR with_check = 'true')
ORDER BY tablename, policyname;

\echo '== Default privileges remaining for Supabase-managed owners =='
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
