-- Disposable role-based access proof for the isolated RLS hardening database.
-- This intentionally exercises SQL as anon, authenticated, and service_role;
-- it must never be run against a production database.

\set ON_ERROR_STOP on

\echo '== Anonymous access denial and reviewed owner-policy behavior =='
BEGIN;
SET LOCAL ROLE anon;
DO $$
DECLARE
    object_name TEXT;
    row_count INTEGER;
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
        'trade_fills',
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
        BEGIN
            EXECUTE format('SELECT 1 FROM public.%I LIMIT 1', object_name);
            RAISE EXCEPTION 'anon unexpectedly can SELECT public.%I', object_name;
        EXCEPTION
            WHEN insufficient_privilege THEN NULL;
        END;
        BEGIN
            EXECUTE format('INSERT INTO public.%I (id) VALUES (1)', object_name);
            RAISE EXCEPTION 'anon unexpectedly can INSERT public.%I', object_name;
        EXCEPTION
            WHEN insufficient_privilege THEN NULL;
        END;
        BEGIN
            EXECUTE format('UPDATE public.%I SET id = 1', object_name);
            RAISE EXCEPTION 'anon unexpectedly can UPDATE public.%I', object_name;
        EXCEPTION
            WHEN insufficient_privilege THEN NULL;
        END;
        BEGIN
            EXECUTE format('DELETE FROM public.%I WHERE false', object_name);
            RAISE EXCEPTION 'anon unexpectedly can DELETE public.%I', object_name;
        EXCEPTION
            WHEN insufficient_privilege THEN NULL;
        END;
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
        BEGIN
            EXECUTE format('SELECT 1 FROM public.%I LIMIT 1', object_name);
            RAISE EXCEPTION 'anon unexpectedly can SELECT public.%I', object_name;
        EXCEPTION
            WHEN insufficient_privilege THEN NULL;
        END;
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
        BEGIN
            PERFORM nextval(format('public.%I', object_name)::regclass);
            RAISE EXCEPTION 'anon unexpectedly can use sequence public.%', object_name;
        EXCEPTION
            WHEN insufficient_privilege THEN NULL;
        END;
    END LOOP;

    SELECT count(*) INTO row_count FROM public.members;
    IF row_count <> 0 THEN
        RAISE EXCEPTION 'anon owner-scoped members policy returned % rows', row_count;
    END IF;
END $$;
ROLLBACK;

\echo '== Authenticated access denial and reviewed owner-policy behavior =='
BEGIN;
SET LOCAL ROLE authenticated;
DO $$
DECLARE
    object_name TEXT;
    row_count INTEGER;
BEGIN
    FOREACH object_name IN ARRAY ARRAY[
        'account_snapshots', 'ap_audit_log', 'ap_edge_buckets',
        'ap_signal_context_tags', 'ap_signal_hub_merged', 'ap_signal_hub_raw',
        'ap_signal_ledger', 'ap_signal_option_outcomes', 'ap_signals',
        'ap_signals_jason_manual_push_backup_20260616', 'ap_trade_log',
        'ap_whitelist', 'audit_log', 'client_authorizations', 'client_state',
        'kv', 'orders', 'orders_backup_jason_null_mode_2026_06_14',
        'orders_backup_jason_null_mode_orphans_2026_06_14', 'processed_signals',
        'schema_migrations', 'trade_fills', 'alert_routes', 'ap_admin_audit',
        'ap_signal_underlying_outcomes', 'ap_system_control', 'bot_status',
        'client_health', 'content_queue', 'daily_cadence_logs', 'incidents',
        'market_data', 'option_outcomes', 'proof_daily_summary', 'proof_trades',
        'proof_vault', 'signal_outcomes', 'signals', 'system_health_events'
    ] LOOP
        BEGIN
            EXECUTE format('SELECT 1 FROM public.%I LIMIT 1', object_name);
            RAISE EXCEPTION 'authenticated unexpectedly can SELECT public.%I', object_name;
        EXCEPTION
            WHEN insufficient_privilege THEN NULL;
        END;
        BEGIN
            EXECUTE format('INSERT INTO public.%I (id) VALUES (1)', object_name);
            RAISE EXCEPTION 'authenticated unexpectedly can INSERT public.%I', object_name;
        EXCEPTION
            WHEN insufficient_privilege THEN NULL;
        END;
        BEGIN
            EXECUTE format('UPDATE public.%I SET id = 1', object_name);
            RAISE EXCEPTION 'authenticated unexpectedly can UPDATE public.%I', object_name;
        EXCEPTION
            WHEN insufficient_privilege THEN NULL;
        END;
        BEGIN
            EXECUTE format('DELETE FROM public.%I WHERE false', object_name);
            RAISE EXCEPTION 'authenticated unexpectedly can DELETE public.%I', object_name;
        EXCEPTION
            WHEN insufficient_privilege THEN NULL;
        END;
    END LOOP;

    FOREACH object_name IN ARRAY ARRAY[
        'ap_signal_funnel_daily', 'ap_skipped_signal_outcomes',
        'ledger_funnel', 'ledger_performance', 'v2_equity_curve',
        'v2_last_20_trades', 'v2_performance'
    ] LOOP
        BEGIN
            EXECUTE format('SELECT 1 FROM public.%I LIMIT 1', object_name);
            RAISE EXCEPTION 'authenticated unexpectedly can SELECT public.%I', object_name;
        EXCEPTION
            WHEN insufficient_privilege THEN NULL;
        END;
    END LOOP;

    FOREACH object_name IN ARRAY ARRAY[
        'ap_admin_audit_id_seq', 'ap_audit_log_id_seq',
        'ap_client_account_snapshots_id_seq',
        'ap_intelligence_outcome_bindings_id_seq', 'ap_system_events_id_seq',
        'applications_id_seq', 'audit_log_id_seq',
        'blocked_signal_counterfactuals_id_seq', 'bot_status_id_seq',
        'broker_order_audit_id_seq', 'client_signal_opportunities_id_seq',
        'daily_performance_id_seq', 'decision_events_id_seq',
        'exit_decision_ledger_id_seq', 'market_data_id_seq',
        'operator_audit_log_id_seq', 'option_outcomes_id_seq',
        'orders_id_seq', 'proof_daily_summary_id_seq', 'proof_trades_id_seq',
        'signal_outcomes_id_seq', 'signals_id_seq', 'trade_queue_id_seq',
        'trades_id_seq'
    ] LOOP
        BEGIN
            PERFORM nextval(format('public.%I', object_name)::regclass);
            RAISE EXCEPTION 'authenticated unexpectedly can use sequence public.%', object_name;
        EXCEPTION
            WHEN insufficient_privilege THEN NULL;
        END;
    END LOOP;

    SELECT count(*) INTO row_count FROM public.members;
    IF row_count <> 0 THEN
        RAISE EXCEPTION 'authenticated owner-scoped members policy returned % rows', row_count;
    END IF;
END $$;
ROLLBACK;

\echo '== Service-role access and CRUD preservation =='
BEGIN;
SET LOCAL ROLE service_role;
DO $$
DECLARE
    object_name TEXT;
    row_count INTEGER;
BEGIN
    FOREACH object_name IN ARRAY ARRAY[
        'account_snapshots', 'ap_audit_log', 'ap_edge_buckets',
        'ap_signal_context_tags', 'ap_signal_hub_merged', 'ap_signal_hub_raw',
        'ap_signal_ledger', 'ap_signal_option_outcomes', 'ap_signals',
        'ap_signals_jason_manual_push_backup_20260616', 'ap_trade_log',
        'ap_whitelist', 'audit_log', 'client_authorizations', 'client_state',
        'kv', 'orders', 'orders_backup_jason_null_mode_2026_06_14',
        'orders_backup_jason_null_mode_orphans_2026_06_14', 'processed_signals',
        'schema_migrations', 'trade_fills', 'alert_routes', 'ap_admin_audit',
        'ap_signal_underlying_outcomes', 'ap_system_control', 'bot_status',
        'client_health', 'content_queue', 'daily_cadence_logs', 'incidents',
        'market_data', 'option_outcomes', 'proof_daily_summary', 'proof_trades',
        'proof_vault', 'signal_outcomes', 'signals', 'system_health_events'
    ] LOOP
        EXECUTE format('SELECT count(*) FROM public.%I', object_name)
            INTO row_count;
    END LOOP;

    FOREACH object_name IN ARRAY ARRAY[
        'ap_signal_funnel_daily', 'ap_skipped_signal_outcomes',
        'ledger_funnel', 'ledger_performance', 'v2_equity_curve',
        'v2_last_20_trades', 'v2_performance'
    ] LOOP
        EXECUTE format('SELECT count(*) FROM public.%I', object_name)
            INTO row_count;
    END LOOP;

    FOREACH object_name IN ARRAY ARRAY[
        'ap_admin_audit_id_seq', 'ap_audit_log_id_seq',
        'ap_client_account_snapshots_id_seq',
        'ap_intelligence_outcome_bindings_id_seq', 'ap_system_events_id_seq',
        'applications_id_seq', 'audit_log_id_seq',
        'blocked_signal_counterfactuals_id_seq', 'bot_status_id_seq',
        'broker_order_audit_id_seq', 'client_signal_opportunities_id_seq',
        'daily_performance_id_seq', 'decision_events_id_seq',
        'exit_decision_ledger_id_seq', 'market_data_id_seq',
        'operator_audit_log_id_seq', 'option_outcomes_id_seq',
        'orders_id_seq', 'proof_daily_summary_id_seq', 'proof_trades_id_seq',
        'signal_outcomes_id_seq', 'signals_id_seq', 'trade_queue_id_seq',
        'trades_id_seq'
    ] LOOP
        PERFORM nextval(format('public.%I', object_name)::regclass);
    END LOOP;

    INSERT INTO public.orders (id) VALUES (900001);
    UPDATE public.orders SET id = 900002 WHERE id = 900001;
    DELETE FROM public.orders WHERE id = 900002;
END $$;
ROLLBACK;

\echo 'RLS hardening role-access fixture passed'
