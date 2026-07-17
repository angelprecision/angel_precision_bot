-- P0 — Durable identity for an already-submitted EXIT order.
--
-- The canonical EXIT-fill reconciler writes these fields only after the broker
-- has confirmed a fill. They preserve the active broker/local order identity
-- and remaining order quantity across process restart; they do not create any
-- submit or cancel authority.

BEGIN;

ALTER TABLE public.positions
    ADD COLUMN IF NOT EXISTS pending_exit_qty INTEGER,
    ADD COLUMN IF NOT EXISTS pending_exit_local_order_id TEXT,
    ADD COLUMN IF NOT EXISTS pending_exit_broker_order_id TEXT,
    ADD COLUMN IF NOT EXISTS pending_exit_action TEXT,
    ADD COLUMN IF NOT EXISTS pending_exit_reason TEXT;

COMMIT;
