# P0: Bound startup recovery active-positions query to a recent window

**Date:** 2026-08-14
**Base:** `main@b43ce9c5`
**Scope:** Surgical. One function, one file. `ap_recovery.py::_load_active_positions`.

## Incident

At the 2026-08-14 market open, live/paper runtime exhibited behavior not seen
before: cancel/submit/position-create side effects firing against long-expired
contracts. Runtime slices from 2026-08-13 already showed the precursor —
`FILL_EXECUTION_MODE_UNPROVEN` repeatedly emitted for May 29 / June 5 2026 OCC
contracts, i.e. months-old rows being revisited as if fresh.

## Root cause

`APStartupRecovery._load_active_positions()` selected every `positions` row for
the client where `status IN ('OPEN','CLOSING','PARTIAL','ACTIVE')` **or**
`quantity_remaining > 0`, with **no lower bound on age**.

Historical positions from prior months that were never cleanly closed (NULL
`execution_mode`, residual `quantity_remaining`) matched on every startup. They
were then:

1. re-registered with `PositionManager` (`_recover_positions`),
2. counted into `master_control._position_count`,
3. handed downstream where they get polled against the live broker,

which is the mechanism that produced the cancel/submit calls against dead
contracts.

## Fix (this PR — nothing else)

Add a single age lower bound to the recovery query:

```sql
AND COALESCE(entry_ts, created_at) >= %s
```

Cutoff = `now() - RECOVERY_ACTIVE_POSITION_LOOKBACK_HOURS` (default **72h**,
env-overridable, invalid/≤0 falls back to 72). Uses only imports already present
in the module (`os`, `datetime`, `timezone`, `timedelta`). No new files, no new
module, no change to any other recovery path.

The 72h default deliberately does **not** reuse
`STARTUP_WATCHER_RESEED_LOOKBACK_HOURS` (48h) so that tuning position recovery
can never silently alter watcher-reseed behavior.

## Invariant enforced

Startup recovery cannot re-register a position whose `entry_ts`/`created_at`
predate the lookback window. Stale historical positions can no longer re-enter
in-memory state or be polled against the broker.

Nothing else about recovery behavior is changed.

## Relationship to existing PRs

- **#455** claims "historical FILLED recovery" but adds a 341-line new module
  (`ap/filled_entry_recovery_authority.py`) targeting the FILLED **entry** path,
  which is distinct from position recovery (`_recover_positions`). It does not
  touch `_load_active_positions` and would not exclude these rows. This PR is
  narrower and independent.
- No open PR modifies `_load_active_positions` or `_recover_positions`
  (verified across #472, #436, #423, and all other recovery-touching PRs).

## Validation

- New test `tests/test_p0_recovery_active_positions_age_bound.py` reproduces the
  bug on current main (query had no age bound → **fails**), and passes after the
  fix.
- `tests/test_recovery_freeze_hardening.py` — **22 passed**, unchanged.
- Recovery-suite regression delta vs baseline: **exactly +1 pass, 0 new
  failures** (baseline 79 failed / 664 passed → 78 failed / 665 passed). The
  remaining pre-existing failures require a real local PostgreSQL fixture and are
  untouched by this change.
- `ap_recovery.py` compiles clean.

## Release gate

- Exact-head CI (P0 regression + DB hot-path) must pass before merge.
- Review the predicate and the 72h default; confirm it covers weekend +
  overnight-deferred windows for this deployment.
- **Do not merge without explicit `merge #N` instruction.**
