# P1 Entry Execution Fix — Runbook

## Why this PR

Live data: **392 entries, 2 fills, 348 canceled, 44 expired**. Avg time-to-fill on the 2 wins was 33.5s. Old behavior was killing trades before they could land.

Four root causes, four fixes, one PR.

## What this PR DOES NOT change

- Exit engine, OSM, reconciler, kill-switch, risk gate, position sizer, P0-1..P0-5 audit work, score-based admission, force-close breaker — all preserved.
- EXIT order repegs still use the legacy 50% gap-close formula (it works fine for exits).

## What changed

### Fix A — Live Attempt-0 pricing uses ask (`ap/contract_selector.py`)
- `contract_selector._build_selected`: when `is_live and ENTRY_ATTEMPT0_PRICING=ASK`, execution_price_per_share = ask. Paper mode and the legacy blend path are preserved as fallbacks.
- `scoring_price_per_share` still uses mid (so cross-name comparisons stay comparable).
- `pricing_basis = "ASK_EXECUTION"` so the funnel telemetry reflects the live pricing.

### Fix B — Repeg ladder ask + 0.01, ask + 0.02 (`ap/retry_engine.py`)
- `REPEG_INTERVAL_SECS` default 30 → **6**
- `ENTRY_REPEG_LADDER_PENNIES` default `"0.01,0.02"` (env-tunable)
- `decide_repeg`: when `kind == "ENTRY"`, new_limit = `current_ask + ladder[attempt]` (instead of legacy 50%-gap-close)
- EXIT orders unchanged — they keep the legacy gap-close
- `order_monitor._try_repeg` now passes `kind` and `current_ask` into `order_row`

### Fix C — LOST_HANDOFF_30S + systemic guard (`ap/order_monitor.py`)
- `ORDER_TIMEOUT_CREATED` default 120 → **30**
- `ENTRY_LIMIT_MAX_AGE_SECONDS` default 150 → **25**
- `MISSED_MOVE_MIN_SECS` default 75 → **6**
- Reason code renamed `LOST_HANDOFF` → `LOST_HANDOFF_30S` to reflect the new threshold
- New systemic-rate guard: if 3+ `LOST_HANDOFF_30S` events fire within a 5-minute rolling window for the same client, emit `LOST_HANDOFF_SYSTEMIC` (CRITICAL log + decision_event + client_state flag), one-shot per window (no infinite self-heal loop)

### Fix D — `watcher_invalidated` forensic context + DEFERRED guard (`ap_execution_core.py`)
- `_on_signal_invalidate`: full forensic log now includes signal_id, plan_id, local_order_id, client_id, symbol, contract, side (CALL/PUT), trigger, current underlying, option bid/mid/ask, stop, target, setup age
- **DEFERRED guard**: if `contract.startswith("DEFERRED:")`, log `DEFERRED_CONTRACT_INVALIDATED` and return WITHOUT canceling. DEFERRED contracts mean contract selection at breach time hasn't been attempted yet — that's not a thesis invalidation. The watcher stays in PENDING_TRIGGER and breach-time contract selection runs as designed.

## Env vars to set on Render

These default to the right values now, but set them explicitly so they're audit-visible:

```
ORDER_TIMEOUT_CREATED=30
ORDER_TIMEOUT_CREATED_NO_BROKER_WARN=10
ENTRY_LIMIT_MAX_AGE_SECONDS=25
MISSED_MOVE_MIN_SECS=6
REPEG_INTERVAL_SECS=6
REPEG_MAX_ATTEMPTS=2
ENTRY_ATTEMPT0_PRICING=ASK
ENTRY_REPEG_LADDER_PENNIES=0.01,0.02
```

Rollback path if needed (no code revert needed):
```
ENTRY_ATTEMPT0_PRICING=BLEND          # reverts Attempt-0 to spread-adaptive blend
ORDER_TIMEOUT_CREATED=120             # reverts CREATED-cancel ceiling
ENTRY_LIMIT_MAX_AGE_SECONDS=150       # reverts hard ceiling
MISSED_MOVE_MIN_SECS=75               # reverts missed-move minimum
REPEG_INTERVAL_SECS=30                # reverts repeg cadence
```

## Acceptance criteria (verify after first full session)

- [ ] **No LOST_HANDOFF older than 30s** — every CREATED-without-broker order cancels at 30s, not 120s.
- [ ] **`watcher_invalidated` log lines include** signal_id, plan_id, local_order_id, client_id, symbol, contract, side, trigger, current underlying, opt bid/mid/ask, stop, target, age.
- [ ] **`DEFERRED:<symbol>` contracts** are NOT permanently canceled on invalidation — they wait for breach-time contract selection. Look for `DEFERRED_CONTRACT_INVALIDATED ignored` in logs.
- [ ] **Broker-submitted entries no longer sit 150s+** — `ENTRY_LIMIT_MAX_AGE_SECONDS=25` enforces 25s ceiling.
- [ ] **Valid entries reach broker at materially higher rate** — old: 13/348 canceled (3.7%); target: >25%.
- [ ] **Avg entry fill time drops under 10–15s** — old avg was 33.5s.
- [ ] **`entry_attempt`, `reason_bucket`, `seconds_to_fill`, repeg attempt** visible in dashboard.

## Tests

27 new tests in `tests/test_p1_entry_execution.py`:
- `TestAttemptZeroPricing` (4) — live ask path present, scoring still on mid, paper mode preserved, env var documented
- `TestRepegLadder` (8) — defaults, ladder constant, kind="ENTRY" branch, caller passes context, ladder math attempts 1 + 2, exit orders use legacy gap-close
- `TestLostHandoffSystemic` (7) — all timeout defaults, reason code, helper exists, threshold, one-shot guard
- `TestWatcherInvalidatedContext` (3) — DEFERRED guard present, full forensic fields, DEFERRED early-returns before cleanup
- `TestEnvVarTargets` (5 parameterized) — every spec env default matches the in-code default

All passing. No regressions on the existing audit suites (test_retry_engine, test_repeg_resubmit, test_funnel_fixes, test_osm_retry_idempotency, test_force_close_breaker, test_readiness_contract, test_queue_score_ordering — all green).
