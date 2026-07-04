-- P0 (PR #260): per-client ap_signals identity — (signal_id, client_email).
--
-- ⚠ DEPLOY REQUIREMENT: apply on Supabase BEFORE deploying the paired code
-- change in ap_signal_store.py (upsert on_conflict="signal_id,client_email").
-- Until applied, the store's upsert falls back to legacy single-key behavior
-- (fail-open to today's semantics, never worse).
--
-- Apply:  psql "$DATABASE_URL" -f migrations/2026_07_02_ap_signals_per_client_key.sql
-- Verify: SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
--         WHERE conrelid='public.ap_signals'::regclass AND contype='p';
--         -- expect: PRIMARY KEY (signal_id, client_email)
--
-- WHY
-- ap_signals was keyed by signal_id ALONE. Multi-client fanout meant:
--   1. OVERWRITE — SignalStore.upsert() from client B replaced client A's
--      client_email on the shared row (last-writer-wins row theft).
--   2. SUPPRESSION — one client's decision_status update (queued/executed)
--      pulled the shared row out of WATCHING, hiding the setup from every
--      other client's overnight reeval.
-- For client money, signal lifecycle must be per-client.
--
-- FK NOTE (deliberate, reviewed)
-- Three analytics tables referenced ap_signals(signal_id) with ON DELETE
-- CASCADE: ap_signal_option_outcomes, ap_signal_underlying_outcomes,
-- ap_signal_context_tags. A composite PK removes the single-column unique
-- target those FKs require. The FKs are DROPPED and replaced by plain
-- indexes: the link becomes an application-level join on signal_id (writes
-- are unchanged — outcome rows are keyed by raw scanner signal_id).
-- ap_signals is an append-only ledger; CASCADE delete was dead weight.
--
-- READ-PATH NOTE (verified before writing this migration)
-- ap_overnight_reeval deliberately reads ap_signals UNFILTERED by client
-- (fan-out design) and dedups by setup key (ticker, side, timeframe,
-- trigger) — per-client rows collapse safely at read time. No read change
-- required.
--
-- Idempotent: every statement is guarded; safe to re-run.

BEGIN;

-- 1. client_email must be NOT NULL for the composite key. Scanner-written
--    rows without a client are canonicalized to the '__shared__' sentinel
--    (they are shared setups by design — see read-path note).
UPDATE public.ap_signals
SET client_email = '__shared__'
WHERE client_email IS NULL OR client_email = '';

ALTER TABLE public.ap_signals
    ALTER COLUMN client_email SET DEFAULT '__shared__';

DO $$
BEGIN
    ALTER TABLE public.ap_signals ALTER COLUMN client_email SET NOT NULL;
EXCEPTION WHEN others THEN
    RAISE NOTICE 'client_email NOT NULL already set or blocked: %', SQLERRM;
END $$;

-- 2. Drop the three FKs that require signal_id to be uniquely constrained
--    on its own (see FK NOTE).
ALTER TABLE public.ap_signal_option_outcomes
    DROP CONSTRAINT IF EXISTS ap_signal_option_outcomes_signal_id_fkey;
ALTER TABLE public.ap_signal_underlying_outcomes
    DROP CONSTRAINT IF EXISTS ap_signal_underlying_outcomes_signal_id_fkey;
ALTER TABLE public.ap_signal_context_tags
    DROP CONSTRAINT IF EXISTS ap_signal_context_tags_signal_id_fkey;

-- Preserve join performance for the application-level link.
CREATE INDEX IF NOT EXISTS ap_signal_option_outcomes_signal_id_idx
    ON public.ap_signal_option_outcomes (signal_id);
CREATE INDEX IF NOT EXISTS ap_signal_underlying_outcomes_signal_id_idx
    ON public.ap_signal_underlying_outcomes (signal_id);
CREATE INDEX IF NOT EXISTS ap_signal_context_tags_signal_id_idx
    ON public.ap_signal_context_tags (signal_id);

-- 3. Swap the primary key: (signal_id) → (signal_id, client_email).
--    Existing rows cannot conflict (signal_id was unique).
DO $$
DECLARE
    _pk_cols text;
BEGIN
    SELECT string_agg(a.attname, ',' ORDER BY k.ord)
      INTO _pk_cols
      FROM pg_constraint c
      JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord) ON true
      JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
     WHERE c.conrelid = 'public.ap_signals'::regclass AND c.contype = 'p';

    IF _pk_cols = 'signal_id' THEN
        ALTER TABLE public.ap_signals DROP CONSTRAINT ap_signals_pkey;
        ALTER TABLE public.ap_signals
            ADD CONSTRAINT ap_signals_pkey PRIMARY KEY (signal_id, client_email);
        RAISE NOTICE 'ap_signals PK swapped to (signal_id, client_email)';
    ELSIF _pk_cols = 'signal_id,client_email' THEN
        RAISE NOTICE 'ap_signals composite PK already present — no-op';
    ELSE
        RAISE EXCEPTION 'ap_signals PK unexpected (%) — manual review required', _pk_cols;
    END IF;
END $$;

COMMIT;

-- ROLLBACK PLAN (manual, if ever needed):
--   Composite rows must first be collapsed to one per signal_id, then:
--     ALTER TABLE public.ap_signals DROP CONSTRAINT ap_signals_pkey;
--     ALTER TABLE public.ap_signals ADD CONSTRAINT ap_signals_pkey PRIMARY KEY (signal_id);
--   (FKs intentionally not restored — application-level join is the
--    permanent design.)
