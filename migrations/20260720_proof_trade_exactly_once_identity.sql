-- migrations/20260720_proof_trade_exactly_once_identity.sql
-- Exactly-once terminal proof binding: proof_event_key + proof_diagnostics
--
-- Idempotent (IF NOT EXISTS / WHERE-guarded). No top-level transaction control
-- (the migration runner owns the transaction).
--
-- proof_event_key: canonical economic identity for one terminal trade.
--   Format: entry:<normalized-client-id>:<originating-entry-local-order-id>
--   Derived exclusively from the originating ENTRY local order ID.
--   NOT derived from ticker, contract, timestamps, or EXIT order IDs.
--
-- proof_diagnostics: JSONB blob for repair/reconciler forensic payloads
--   that survive binding without replacing the decision reason.

ALTER TABLE public.proof_trades
ADD COLUMN IF NOT EXISTS proof_event_key text;

ALTER TABLE public.proof_trades
ADD COLUMN IF NOT EXISTS proof_diagnostics jsonb
NOT NULL DEFAULT '{}'::jsonb;

-- Backfill canonical proof_event_key only where the originating local_order_id
-- is nonempty AND unambiguous (exactly one proof row per client+order).
-- Historical ambiguous or broker-repair rows are left NULL intentionally.
UPDATE public.proof_trades pt
SET proof_event_key = 'entry:' || LOWER(TRIM(pt.client_email)) || ':' || TRIM(pt.local_order_id)
WHERE pt.proof_event_key IS NULL
  AND pt.local_order_id IS NOT NULL
  AND BTRIM(pt.local_order_id) <> ''
  AND (
    SELECT COUNT(*)
    FROM public.proof_trades pt2
    WHERE pt2.client_email = pt.client_email
      AND BTRIM(pt2.local_order_id) = BTRIM(pt.local_order_id)
  ) = 1;

-- Partial unique index: only rows with a nonempty proof_event_key are constrained.
-- Repair rows and historical rows with NULL proof_event_key are excluded.
CREATE UNIQUE INDEX IF NOT EXISTS uq_proof_trades_event_key
ON public.proof_trades (proof_event_key)
WHERE proof_event_key IS NOT NULL
  AND BTRIM(proof_event_key) <> '';
