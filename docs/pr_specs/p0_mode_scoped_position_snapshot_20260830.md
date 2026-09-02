# P0 Mode-Scoped Position Snapshot Repair

Base audit (original): `main@2758a73c09eae09f7314b58268925af587e5d110`
**Rebased onto**: `main@16564d7e9df6fd1b4320c76173446c33891a994f` (post-rollback stabilized main)

## One surviving defect

`APExecutionCore._current_open_position_count()` and `_current_pending_entry_count()` call `position_manager.snapshot()` without `mode=`. `APPositionManager.snapshot()` requires exact `paper|live`; unscoped calls raise and degrade capacity truth to process-local fallback state. The snapshot implementation must also apply that mode to its active-position and position-derived fields; otherwise a valid scoped call still mixes PAPER and LIVE rows for the same client.

## Required implementation

**Production files changed (both intentional):**

- `ap_execution_core.py` — `_position_snapshot_mode()` canonical mode resolver; explicit `mode=` on the two `ExecutionCore` snapshot calls; fail-closed block when runtime mode is invalid.
- `ap/position_manager.py` — `snapshot(*, mode)` now mandatory; mode predicate `LOWER(TRIM(COALESCE(execution_mode, ''))) = %s` applied to all three position-derived SQL queries (active positions, terminal positions, capital/P&L summary).

Scope invariant: no selector, score, sizing, threshold, scanner, broker submit/cancel, order lifecycle, position mutation, proof, queue, exit, or intelligence change.

1. Resolve exact canonical runtime execution mode from existing authority.
2. Pass `mode="live"` or `mode="paper"` to both snapshot reads in `ExecutionCore`.
3. Scope active positions, terminal-position matching inputs, and position-derived capital/P&L by the exact mode inside `snapshot()`.
4. Missing/malformed/conflicting mode must never invent a default — raises immediately, fails closed.
5. Preserve current fallback only when an authoritative scoped snapshot itself fails.

## Proof — test coverage (14 structural/mock cases; exact-head P0 required)

**ExecutionCore layer (mocked `_RecordingPositionManager`):**
- LIVE/PAPER canonical mode resolution (including whitespace normalization)
- Successful scoped snapshot wins over local process fallback
- Real snapshot failure preserves existing fallback
- Invalid or conflicting identity (`""`, `"staging"`, `LIVE`+`staging`, `LIVE`+`paper`) never reaches `snapshot()`

**PositionManager SQL layer (structural/mock coverage; not PostgreSQL-backed):**
- Existing: `snapshot(mode="live")` returns only live-tagged position and scopes all three SQL queries with the mode predicate and `"live"` param
- NEW: LIVE runner sees only LIVE open positions; PAPER runner sees only PAPER open positions
- NEW: `capital_deployed` and `realized_pnl_today` sourced from mode-scoped summary query only
- NEW: `snapshot(mode=None)` and `snapshot(mode="staging")` raise before any data read reaches the DB

These tests do not prove psycopg2/PostgreSQL driver execution; exact-head P0 CI is the required integration evidence.

## Rebase audit (performed 2026-09-02)

- Rebased from `2758a73c` onto `16564d7e` (current stabilized main after #528 rollback)
- Merge base confirmed: `16564d7e9df6fd1b4320c76173446c33891a994f`
- No #528 files resurrected (`ap/pending_trigger_classifier.py`, `ap/pending_trigger_restart_recovery.py` — zero diff)
- Final production diff: `ap_execution_core.py` (+48 lines), `ap/position_manager.py` (+24 lines)
- 14/14 tests green on new exact head
