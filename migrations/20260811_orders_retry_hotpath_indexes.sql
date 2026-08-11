-- =============================================================================
-- P0 #433 — production ENTRY/retry hot-path indexes
-- =============================================================================
-- These indexes are deliberately shaped from the current-main SQL in
-- ap/order_monitor.py and ap/morning_handoff.py.  The runtime keeps parsing
-- retry_ready_at/retry counters in Python; this migration never casts legacy
-- JSON text and therefore cannot turn malformed metadata into a statement
-- failure.
--
-- The partial predicates are fixed lifecycle predicates.  client_id and the
-- ordered timestamp are the only key columns because the callers keep their
-- exact client/mode ownership fences in the WHERE clause.  In particular, do
-- not replace those fences with a nullable execution_mode fallback or a LIVE
-- default merely to make an index expression shorter.
--
-- All statements are idempotent.  The migration runner owns the transaction;
-- do not add BEGIN/COMMIT or CREATE INDEX CONCURRENTLY here.

-- _check_armed_retries(): canceled ENTRY rows whose durable retry intent is
-- ARMED, returned in updated_ts order before the Python readiness parse.
CREATE INDEX IF NOT EXISTS idx_orders_entry_canceled_retry_armed_updated
    ON orders (client_id, updated_ts)
    WHERE kind = 'ENTRY'
      AND status = 'CANCELED'
      AND meta ->> 'retry_status' = 'ARMED';

-- _recover_stale_inflight_retries(): canceled ENTRY rows abandoned in the
-- broker-submit handoff, returned in updated_ts order before exact-owner CAS.
CREATE INDEX IF NOT EXISTS idx_orders_entry_canceled_retry_inflight_updated
    ON orders (client_id, updated_ts)
    WHERE kind = 'ENTRY'
      AND status = 'CANCELED'
      AND meta ->> 'retry_status' IN ('IN_FLIGHT', 'SUBMITTING');

-- morning_handoff._has_unowned_pending_trigger_orders(): watcher-held ENTRY
-- rows with no broker/submitted identity, returned in created_ts order.
-- No execution-mode predicate is introduced here; this index mirrors the
-- current caller exactly and does not change its result set.
CREATE INDEX IF NOT EXISTS idx_orders_entry_pending_trigger_recovery_created
    ON orders (client_id, created_ts)
    WHERE kind = 'ENTRY'
      AND status = 'PENDING_TRIGGER'
      AND broker_order_id IS NULL
      AND submitted_ts IS NULL;
