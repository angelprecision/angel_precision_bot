-- =============================================================================
-- SCHEMA CHECK — STEP 0 before applying the signal-ledger view
-- =============================================================================
-- Run BOTH queries below and confirm the columns the view references actually
-- exist with these exact names. If anything is different, edit
-- sql/views/ap_multi_account_signal_ledger.sql before applying it.
-- =============================================================================

-- orders columns
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'orders'
ORDER BY ordinal_position;

-- ap_signals columns
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'ap_signals'
ORDER BY ordinal_position;
