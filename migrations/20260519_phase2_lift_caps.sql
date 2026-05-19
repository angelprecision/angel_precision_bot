-- AUDIT PHASE-2: lift daily-trade cap and concurrent-position cap for active clients.
--
-- Before this migration: clients were initialized with max_trades_per_day=5 and
-- max_concurrent_positions=2. Combined with the slot-accounting bug (canceled
-- orders permanently burned slots), this caused the bot to refuse overnight
-- signals after just 5 morning cancels.
--
-- After this migration: every approved + subscription_active client gets the
-- new defaults (12 / 4). Apply manually after deploying the code change so
-- caps and gate logic land together.
--
-- SAFETY: only updates clients you want to push to the new caps. Review the
-- SELECT below first; uncomment the UPDATE when ready.

-- 1. Preview which clients will be affected.
SELECT email, max_trades_per_day, max_concurrent_positions
FROM   clients
WHERE  approved = TRUE
  AND  subscription_active = TRUE
ORDER  BY email;

-- 2. Apply (uncomment after review):
-- UPDATE clients
-- SET    max_trades_per_day       = 12,
--        max_concurrent_positions = 4
-- WHERE  approved            = TRUE
--   AND  subscription_active = TRUE
--   AND  (max_trades_per_day       < 12
--      OR max_concurrent_positions < 4);

-- 3. Per-client override (if a specific client wants more conservative caps):
-- UPDATE clients SET max_trades_per_day = 8 WHERE email = 'cautious@example.com';
