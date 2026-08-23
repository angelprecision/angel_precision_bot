-- Isolated PostgreSQL fixture for the reviewed Supabase RLS hardening
-- migration. The workflow creates a fresh database before running this file.

DO $$
DECLARE
    role_name TEXT;
BEGIN
    FOREACH role_name IN ARRAY ARRAY[
        'anon', 'authenticated', 'service_role', 'supabase_admin'
    ] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('CREATE ROLE %I NOLOGIN', role_name);
        END IF;
    END LOOP;
    EXECUTE 'ALTER ROLE service_role NOLOGIN BYPASSRLS';
END $$;

DO $$
DECLARE
    object_name TEXT;
BEGIN
    FOREACH object_name IN ARRAY ARRAY[
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
    ] LOOP
        EXECUTE format('CREATE TABLE public.%I (id bigint);', object_name);
        EXECUTE format(
            'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.%I TO PUBLIC, anon, authenticated, service_role;',
            object_name
        );
    END LOOP;

    FOREACH object_name IN ARRAY ARRAY[
        'alert_routes',
        'ap_admin_audit',
        'ap_signal_underlying_outcomes',
        'ap_system_control',
        'bot_status',
        'client_health',
        'content_queue',
        'daily_cadence_logs',
        'incidents',
        'market_data',
        'option_outcomes',
        'proof_daily_summary',
        'proof_trades',
        'proof_vault',
        'signal_outcomes',
        'signals',
        'system_health_events'
    ] LOOP
        EXECUTE format('CREATE TABLE public.%I (id bigint);', object_name);
        EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY;', object_name);
        EXECUTE format(
            'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.%I TO PUBLIC, anon, authenticated, service_role;',
            object_name
        );
    END LOOP;

    FOREACH object_name IN ARRAY ARRAY[
        'ap_signal_funnel_daily',
        'ap_skipped_signal_outcomes',
        'ledger_funnel',
        'ledger_performance',
        'v2_equity_curve',
        'v2_last_20_trades',
        'v2_performance'
    ] LOOP
        EXECUTE format(
            'CREATE VIEW public.%I AS SELECT 1::bigint AS id;',
            object_name
        );
        EXECUTE format(
            'GRANT SELECT ON TABLE public.%I TO PUBLIC, anon, authenticated, service_role;',
            object_name
        );
    END LOOP;

    FOREACH object_name IN ARRAY ARRAY[
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
    ] LOOP
        EXECUTE format('CREATE SEQUENCE public.%I;', object_name);
        EXECUTE format(
            'GRANT USAGE, SELECT, UPDATE ON SEQUENCE public.%I TO PUBLIC, anon, authenticated, service_role;',
            object_name
        );
    END LOOP;
END $$;

CREATE SCHEMA auth;
GRANT USAGE ON SCHEMA auth TO PUBLIC;
CREATE FUNCTION auth.uid()
RETURNS uuid
LANGUAGE sql
IMMUTABLE
AS 'SELECT NULL::uuid';

CREATE TABLE public.members (id bigint, user_id uuid);
ALTER TABLE public.members ENABLE ROW LEVEL SECURITY;
GRANT SELECT, INSERT, UPDATE, DELETE
    ON TABLE public.members TO PUBLIC, anon, authenticated, service_role;
ALTER TABLE public.proof_trades ADD COLUMN client_email text;

CREATE POLICY members_read_own ON public.members
    FOR SELECT TO PUBLIC USING (auth.uid() = user_id);
CREATE POLICY client_sees_own_trades ON public.proof_trades
    FOR SELECT TO PUBLIC USING (
        client_email = ((current_setting('request.jwt.claims', true))::json ->> 'email')
    );

CREATE POLICY anon_read ON public.alert_routes
    FOR SELECT TO PUBLIC USING (true);
CREATE POLICY anon_read ON public.ap_admin_audit
    FOR SELECT TO PUBLIC USING (true);
CREATE POLICY anon_all_underlying ON public.ap_signal_underlying_outcomes
    FOR ALL TO anon USING (true) WITH CHECK (true);
CREATE POLICY anon_read ON public.ap_system_control
    FOR SELECT TO PUBLIC USING (true);
CREATE POLICY anon_all_bot_status ON public.bot_status
    FOR ALL TO anon USING (true) WITH CHECK (true);
CREATE POLICY anon_read_client_health ON public.client_health
    FOR SELECT TO anon USING (true);
CREATE POLICY service_write_client_health ON public.client_health
    FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY anon_read ON public.content_queue
    FOR SELECT TO PUBLIC USING (true);
CREATE POLICY anon_read ON public.daily_cadence_logs
    FOR SELECT TO PUBLIC USING (true);
CREATE POLICY anon_read ON public.incidents
    FOR SELECT TO PUBLIC USING (true);
CREATE POLICY "Anyone can read market data" ON public.market_data
    FOR SELECT TO anon USING (true);
CREATE POLICY anon_all_option_outcomes ON public.option_outcomes
    FOR ALL TO anon USING (true) WITH CHECK (true);
CREATE POLICY anon_all_proof_daily ON public.proof_daily_summary
    FOR ALL TO anon USING (true) WITH CHECK (true);
CREATE POLICY anon_all_proof_trades ON public.proof_trades
    FOR ALL TO anon USING (true) WITH CHECK (true);
CREATE POLICY service_role_all ON public.proof_trades
    USING (true);
CREATE POLICY anon_read ON public.proof_vault
    FOR SELECT TO PUBLIC USING (true);
CREATE POLICY anon_all_signal_outcomes ON public.signal_outcomes
    FOR ALL TO anon USING (true) WITH CHECK (true);
CREATE POLICY "Anyone can read signals" ON public.signals
    FOR SELECT TO anon USING (true);
CREATE POLICY anon_read ON public.system_health_events
    FOR SELECT TO PUBLIC USING (true);
