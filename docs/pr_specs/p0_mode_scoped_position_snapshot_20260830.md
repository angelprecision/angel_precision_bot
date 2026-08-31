# P0 Mode-Scoped Position Snapshot Repair

Base audit: `main@2758a73c09eae09f7314b58268925af587e5d110`

## One surviving defect

`APExecutionCore._current_open_position_count()` and `_current_pending_entry_count()` call `position_manager.snapshot()` without `mode=`. `APPositionManager.snapshot()` requires exact `paper|live`; unscoped calls raise and degrade capacity truth to process-local fallback state. The snapshot implementation must also apply that mode to its active-position and position-derived fields; otherwise a valid scoped call still mixes PAPER and LIVE rows for the same client.

The older #417 terminal-disposition concern has been repaired by later current-main lifecycle work and is out of scope.

## Required implementation

Expected production scope: `ap_execution_core.py` and the mode-scoping SQL in `ap/position_manager.py`.

1. Resolve exact canonical runtime execution mode from existing authority.
2. Pass `mode="live"` or `mode="paper"` to both snapshot reads.
3. Scope active positions, terminal-position matching inputs, and position-derived capital/P&L by the exact mode inside `snapshot()`.
4. Missing/malformed/conflicting mode must never invent a default.
5. Preserve current fallback only when an authoritative scoped snapshot itself fails.
6. No selector, score, sizing, threshold, scanner, broker submit/cancel, order lifecycle, position mutation, proof, queue, exit, or intelligence change.

## Required proof

- LIVE open/pending reads are scoped live.
- PAPER open/pending reads are scoped paper.
- mixed-mode active positions and position capital never cross the snapshot boundary.
- invalid mode performs zero unscoped snapshot call.
- successful authoritative snapshot never uses local fallback.
- genuine snapshot exception preserves current fallback.
- focused capacity/lifecycle tests plus exact-head P0 CI.
