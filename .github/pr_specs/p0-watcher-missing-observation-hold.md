# P0 recovery contract: bounded watcher breach continuity

This forward-port applies only the historical #494 watcher invariant to the
current committed `origin/main`. It does not reopen or rebase historical PR
#494.

## Invariant

`WatchedSignal` requires multiple valid canonical breach observations:

- CALL evidence is the canonical ASK; PUT evidence is the canonical BID.
- A missing, zero, malformed, boolean, NaN, infinity, or otherwise unusable
  required side is unknown truth. It suppresses this poll's entry evidence and
  does not increment, reset, create `trigger_price`, or confirm.
- A crossed positive pair (`BID > ASK`) is internally inconsistent truth. Both
  sides are treated as unavailable for this poll; it cannot trigger entry or
  invalidate the active stop.
- If a pre-confirmation partial streak exists, unknown truth holds it only when
  `0 <= now - _last_valid_breach_observation_at <= 45` seconds.
- The anchor is the most recent valid canonical breach observation; missing
  polls do not move it. A missing anchor, elapsed time above 45 seconds, or
  negative elapsed time discards the partial streak and leaves the watcher
  `PENDING`.
- The next valid breach after expiry starts a new first-observation streak.
- A valid contradictory observation resets immediately; it receives no grace
  period.

The 45-second limit is a fixed safety invariant, not an environment setting.
The continuity anchor is process-local and never persisted. Confirmed durable
`trigger_crossed_at` truth remains governed by the existing lifecycle fields;
the pre-confirmation timer cannot reset it. CALL and PUT behavior is symmetric.

## Production seam

The exported `ap_entry_watcher.APEntryWatcher._poll_active_signals()` path must
pass raw canonical BID/ASK values through to `WatchedSignal.check()`. An absent
or shim-filtered LAST-only quote therefore reaches `check(None, None)`; LAST,
MID, MARK, and the opposite side are never promoted to trigger authority.

## Scope fence

Production behavior is limited to `ap_entry_watcher.py`:

- canonical quote normalization and missing-observation continuity;
- the narrow poll-loop compatibility needed to preserve raw missing-side truth;
- no broker submit/cancel, selector, request-scope reducer, quote cache,
  fill-monitor identity handoff, OSM, sizing, risk, queue, position, proof,
  exit, reconciler, or intelligence changes.

Tests and this contract may be added or adjusted. The P0 workflow may only gain
registrations for these watcher regression tests. This recovery remains Draft;
do not merge or deploy.
