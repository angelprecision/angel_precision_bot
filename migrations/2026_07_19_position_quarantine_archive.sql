-- 2026_07_19: Archive quarantined junk position rows (2026-07-17/18 audit)
--
-- Incident: the reconciler expired-import loop (fixed forward by PR #369)
-- created 378 duplicate EXPIRED position rows for jose /
-- SPY260716P00751000 on 2026-07-17 (one per ~60s reconciler cycle,
-- 04:00-10:29 UTC). Production forensics confirmed these rows have ZERO
-- referencing rows in orders and ZERO in proof_trades — pure orphan noise
-- that pollutes dashboards, analytics, and any position-derived metric.
-- Separately, 49 stale CLOSED_REPAIR rows (all >7 days old) are permanent
-- candidates in scans and dashboard queries.
--
-- Policy: NEVER destroy money-adjacent history. Every removed row is
-- first copied verbatim (full-row JSONB snapshot) into
-- positions_quarantine_archive with an archive reason. Rows that ARE
-- referenced by orders or proof_trades are archived as a snapshot but
-- NOT deleted — deleting them would orphan order/proof references, which
-- is worse than dashboard noise. Idempotent throughout: re-running
-- archives/deletes nothing new.
--
-- NOTE (attestation): positions_quarantine_archive is deliberately NOT
-- added to ap/schema_attestation.REQUIRED_SCHEMA — no runtime execution
-- path references it. Declaring it would make attestation fail on any
-- deploy that precedes this migration's application, inverting the
-- deploy/apply order the runner supports.
--
-- Runner contract: no top-level transaction control in this file; the
-- migration runner owns the transaction (one per file).

CREATE TABLE IF NOT EXISTS positions_quarantine_archive (
    position_id     TEXT PRIMARY KEY,
    client_id       TEXT,
    contract        TEXT,
    status          TEXT,
    entry_ts        TIMESTAMPTZ,
    row_snapshot    JSONB NOT NULL,
    archive_reason  TEXT NOT NULL,
    referenced      BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_from_positions BOOLEAN NOT NULL DEFAULT FALSE,
    archived_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pos_quarantine_archive_reason
    ON positions_quarantine_archive (archive_reason, archived_at DESC);

-- ---------------------------------------------------------------------------
-- 1) The 378 expired-import-loop rows (jose / SPY260716P00751000, 2026-07-17)
--    Predicate is deliberately exact: client, contract, status, entry date.
-- ---------------------------------------------------------------------------
INSERT INTO positions_quarantine_archive
    (position_id, client_id, contract, status, entry_ts, row_snapshot,
     archive_reason, referenced)
SELECT p.id::text, p.client_id, p.contract, p.status, p.entry_ts,
       to_jsonb(p),
       'expired_import_loop_2026_07_17',
       (EXISTS (SELECT 1 FROM orders o WHERE o.position_id::text = p.id::text)
        OR EXISTS (SELECT 1 FROM proof_trades pt WHERE pt.position_id::text = p.id::text))
FROM positions p
WHERE p.status = 'EXPIRED'
  AND p.contract = 'SPY260716P00751000'
  AND p.entry_ts >= '2026-07-17T00:00:00Z'
  AND p.entry_ts <  '2026-07-18T00:00:00Z'
ON CONFLICT (position_id) DO NOTHING;

DELETE FROM positions p
WHERE p.status = 'EXPIRED'
  AND p.contract = 'SPY260716P00751000'
  AND p.entry_ts >= '2026-07-17T00:00:00Z'
  AND p.entry_ts <  '2026-07-18T00:00:00Z'
  AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.position_id::text = p.id::text)
  AND NOT EXISTS (SELECT 1 FROM proof_trades pt WHERE pt.position_id::text = p.id::text)
  AND EXISTS (
      SELECT 1 FROM positions_quarantine_archive a
      WHERE a.position_id = p.id::text
  );

UPDATE positions_quarantine_archive a
SET deleted_from_positions = TRUE
WHERE a.archive_reason = 'expired_import_loop_2026_07_17'
  AND a.referenced = FALSE
  AND NOT EXISTS (SELECT 1 FROM positions p WHERE p.id::text = a.position_id);

-- ---------------------------------------------------------------------------
-- 2) Stale CLOSED_REPAIR rows: snapshot ALL; delete only the unreferenced.
-- ---------------------------------------------------------------------------
INSERT INTO positions_quarantine_archive
    (position_id, client_id, contract, status, entry_ts, row_snapshot,
     archive_reason, referenced)
SELECT p.id::text, p.client_id, p.contract, p.status, p.entry_ts,
       to_jsonb(p),
       'stale_closed_repair_2026_07_19',
       (EXISTS (SELECT 1 FROM orders o WHERE o.position_id::text = p.id::text)
        OR EXISTS (SELECT 1 FROM proof_trades pt WHERE pt.position_id::text = p.id::text))
FROM positions p
WHERE p.status = 'CLOSED_REPAIR'
ON CONFLICT (position_id) DO NOTHING;

DELETE FROM positions p
WHERE p.status = 'CLOSED_REPAIR'
  AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.position_id::text = p.id::text)
  AND NOT EXISTS (SELECT 1 FROM proof_trades pt WHERE pt.position_id::text = p.id::text)
  AND EXISTS (
      SELECT 1 FROM positions_quarantine_archive a
      WHERE a.position_id = p.id::text
  );

UPDATE positions_quarantine_archive a
SET deleted_from_positions = TRUE
WHERE a.archive_reason = 'stale_closed_repair_2026_07_19'
  AND a.referenced = FALSE
  AND NOT EXISTS (SELECT 1 FROM positions p WHERE p.id::text = a.position_id);

-- The 17 CLOSED rows with NULL realized_pnl are intentionally NOT touched:
-- fabricating P&L in SQL is falsifying records. They require a
-- broker-truth repair job (orders/fills reconciliation), tracked as a
-- separate operator task.
