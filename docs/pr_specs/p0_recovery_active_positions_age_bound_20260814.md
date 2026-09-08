# P0: Fence startup recovery active-positions to the current runtime identity

**Date:** 2026-08-14 (amended after review)
**Base:** `main@b43ce9c5`
**Scope:** Surgical. One production file (`ap_recovery.py`), two functions:
`_load_active_positions` and the guard block in `_recover_positions`.

## Incident

At the 2026-08-14 market open, live/paper runtime fired
cancel/submit/position-create side effects against long-expired contracts. The
2026-08-13 slice showed the precursor: `FILL_EXECUTION_MODE_UNPROVEN` repeatedly
emitted for May 29 / June 5 2026 OCC contracts — months-old **NULL
execution_mode** rows being revisited as if fresh.

## Root cause

`APStartupRecovery._load_active_positions()` selected every `positions` row
where status was economically active (or `quantity_remaining > 0`) with **no
execution-mode fence**. `_recover_positions()` then registered every returned
row with `PositionManager` and bumped `master_control._position_count`. There
was no per-position proof that the row belonged to this runner's mode.

## Corrected invariant (this version)

An earlier draft of this PR bounded recovery by **age** (72h lookback). Review
correctly rejected that: age is not authority. A legitimate LIVE position held
across a long weekend / outage is still real exposure at the broker; a
stopwatch must never silently conclude it is dead. And age did not fix the
actual identity bug — a recent NULL-mode or opposite-mode row still passed.

This version enforces identity, not age:

1. **Execution-mode identity fence (SQL).** `_load_active_positions` resolves
   the canonical current runner mode via the existing `_execution_mode()` and
   filters positions to `LOWER(BTRIM(COALESCE(execution_mode,''))) = <mode>`.
   NULL, blank, malformed, or opposite-mode rows are excluded at the SQL
   boundary and never registered. This mirrors the fail-closed authority the
   deferred-recovery pass already uses.

2. **Fail closed on unknown mode.** If the runner mode is not PAPER/LIVE,
   `_load_active_positions` loads nothing and does not query the DB.

3. **No age authority.** The 72h cutoff is removed entirely.

4. **Expired-OCC exclusion (economic authority).** In `_recover_positions`, an
   option whose exact OCC expiry is before today is skipped. This reuses the
   reconciler's proven `ap.reconcile._is_expired` parser — no new OCC parsing.

5. **Parser failure never drops a position.** If the expiry check raises, the
   position falls through and is recovered rather than assumed dead.

## Behavioral tests (not SQL-text)

`tests/test_p0_recovery_active_positions_age_bound.py`:

- current runner mode is bound into the query (paper and live)
- unknown runner mode loads zero positions and does not query the DB
- an old but same-mode, unexpired position is still recovered (age is not authority)
- an expired OCC contract is excluded and counted, not registered
- an unparseable contract is recovered, not silently dropped

All 6 pass. `tests/test_recovery_freeze_hardening.py` — 22 passed, unchanged.
`tests/test_p0_canonical_exit_fill_truth.py` — 50 passed / 2 skipped, unchanged.

## Production state (verified read-only)

There are currently **zero** positions matching the active-family / residual-qty
candidate set, and zero older-than-72h rows retaining positive
`quantity_remaining` (historical rows are terminalized into CLOSED /
CLOSED_REPAIR / EXPIRED). This PR therefore **prevents recurrence**; it is not
rescuing an active DB zombie tonight. That is deliberate room to fix the
invariant correctly rather than merge under P0 urgency.

## Relationship / sequencing

- **#455** ("historical FILLED recovery") adds a 341-line new module targeting
  the FILLED *entry* path — distinct from position recovery, does not touch
  `_load_active_positions`. Independent.
- **#472** also edits `ap_recovery.py` but not these functions — no textual
  conflict expected. After #472 lands, this PR must be rebased/retested against
  the resulting main before merge; two recovery PRs green independently against
  `b43ce9c` is not proof of their combined runtime.

## Release gate

- Exact-head **P0 regression AND DB hot-path** CI must both pass before merge.
  (The earlier head had only the P0 regression run; DB hot-path evidence must be
  present on the amended head.)
- Review the mode-fence and expired-OCC exclusion.
- **Do not merge without explicit operator instruction. Only the operator merges.**
