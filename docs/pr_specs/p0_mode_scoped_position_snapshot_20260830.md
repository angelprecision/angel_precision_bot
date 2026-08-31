# P0 Mode-Scoped Position Snapshot Repair

Base audit: `main@26d9d81c87c7c97fd1e0d33f83f9730e2e245c47`

## One surviving defect

`APExecutionCore._current_open_position_count()` and `_current_pending_entry_count()` call `position_manager.snapshot()` without `mode=`. `APPositionManager.snapshot()` requires exact `paper|live`; unscoped calls raise and degrade capacity truth to process-local fallback state.

The older #417 terminal-disposition concern has been repaired by later current-main lifecycle work and is out of scope.

## Required implementation

Expected production scope: `ap_execution_core.py` only.

1. Resolve exact canonical runtime execution mode from existing authority.
2. Pass `mode="live"` or `mode="paper"` to both snapshot reads.
3. Missing/malformed/conflicting mode must never invent a default.
4. Preserve current fallback only when an authoritative scoped snapshot itself fails.
5. No selector, score, sizing, threshold, scanner, broker submit/cancel, order, position, proof, queue, exit, or intelligence change.

## Required proof

- LIVE open/pending reads are scoped live.
- PAPER open/pending reads are scoped paper.
- invalid mode performs zero unscoped snapshot call.
- successful authoritative snapshot never uses local fallback.
- genuine snapshot exception preserves current fallback.
- focused capacity/lifecycle tests plus exact-head P0 CI.