# P0 Live Hard-Hold Patch Plan

Status: HARD HOLD LIVE until implemented and verified.

This branch was created for the META/VZ production incident shape:

- stale trigger entries after the move already happened
- blank/unknown execution_mode in live handoff/accounting
- deferred PENDING_TRIGGER rows surviving watcher terminal decisions
- trigger callback retry logged after watcher removal
- protective exit decisions blocked by exit_circuit_breaker_tripped

Required patch scope:

1. `ap_entry_watcher.py`
   - block regular-session arm/recovery rearm when price is already through trigger
   - add daily/overnight valid-branch already-through-trigger check
   - keep trigger-ready watcher owned until callback succeeds or bounded retry exhausts

2. `ap_execution_core.py`
   - remove DEFERRED invalidation bypass for real underlying invalidations
   - LIVE submit blocker for blank/null/unknown execution_mode
   - LIVE submit blocker for missing client_id
   - trigger-age guard before broker submit
   - final pre-submit fresh underlying quote gate

3. exit/accounting guard
   - block new LIVE entries when protective exit is firing but circuit breaker blocks submit
   - allow protective CLOSE_ALL for synthetic repair when broker truth says open qty > 0
   - close/stale synthetic row when broker truth says qty = 0

Acceptance: no LIVE submit unless client_id and execution_mode are preserved, trigger is fresh, current quote is fresh, remaining opportunity exists, and no broken exit/accounting state exists.
