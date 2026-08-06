# P0: Restore selector recovery capacity without bypassing chart invalidation

## Status

**Binding implementation contract. Production code and exact-head tests are required before merge. Do not merge this docs-only head.**

## Production evidence

On 2026-07-27, Jason LIVE produced seven deferred ENTRY lifecycles that ended as:

`BREACH_RETRY_EXHAUSTED:SELECTOR_REQUEST_BUDGET_EXHAUSTED`

Affected symbols:

- GE CALL
- CAT PUT
- ADSK CALL
- FDX PUT
- KLAC PUT
- CRWD CALL
- TMUS CALL

Production diagnostics prove the current recovery envelope is:

- direct option quote limit: 20 per selector request
- selector elapsed limit: 15 seconds
- breach selector attempts: 3
- retry delay: 20 seconds
- canonical budget conflict: `SELECTOR_MAX_DIRECT_QUOTE_CALLS=20`, `CONTRACT_REVALIDATE_TOP_N=20`, `DIRECT_QUOTE_RECOVERY_TOP_N=8`

The failure is not simply that a hard ceiling exists. The selector can spend the ceiling on contracts that are too far from the trigger, structurally unaffordable, outside useful delta/moneyness, or repeated on every retry. It can then label the entire lifecycle `SELECTOR_REQUEST_BUDGET_EXHAUSTED`, even when the chart setup should have invalidated before another selector pass.

User review of the underlying charts found the opportunity set materially stronger than the executed count. CRWD should have invalidated and must remain blocked by chart truth. This PR must recover valid opportunities without converting invalid setups into trades.

## Goal

Increase the number of valid deferred-breach setups that reach a real OCC contract and broker submission while preserving all existing quality, capital, spread, stop, target, direction, and invalidation authorities.

This is a trade-flow continuity repair, not a threshold-loosening PR.

## Non-goals

Do not change:

- scanner scores or minimum score
- intelligence admission or veto policy
- spread thresholds
- minimum BID
- OI or volume thresholds
- premium caps
- position sizing percentages
- max positions
- stop, target, or underlying geometry
- exit behavior
- broker duplicate-submit protections
- PAPER/LIVE identity fencing

Do not remove all selector limits. Do not fail open. Do not submit CRWD-like setups after invalidation.

## Required production changes

### 1. Increase bounded recovery capacity

Change hot-read defaults for deferred-breach selection:

- `SELECTOR_MAX_DIRECT_QUOTE_CALLS`: 20 -> 40
- `SELECTOR_MAX_TOTAL_ELAPSED_MS`: 15000 -> 25000
- `SELECTOR_MAX_EXPIRATION_CALLS`: 2 -> 3
- `SELECTOR_MAX_CHAIN_CALLS`: 6 -> 8
- `MAX_BREACH_SELECTOR_RETRIES`: 3 -> 5 total selector attempts
- `BREACH_SELECTOR_RETRY_DELAY_SECONDS`: 20 -> 8

The lifecycle remains bounded. Five attempts at an 8-second delay cannot become an open-ended retry loop. Existing EOD/cutoff and absolute entry-deadline fences remain authoritative.

Malformed or non-positive environment values must fall back to these defaults without raising.

### 2. Eliminate budget alias conflict

`SELECTOR_MAX_DIRECT_QUOTE_CALLS` remains the only authority.

Legacy aliases may be read for diagnostics, but they must not reduce or independently cap recovery. Add a startup/runtime diagnostic that clearly records:

- canonical configured value
- aliases
- effective value
- whether aliases conflict

Update deployment documentation so `DIRECT_QUOTE_RECOVERY_TOP_N` and `CONTRACT_REVALIDATE_TOP_N` are removed or aligned with the canonical value.

### 3. Make trigger-anchored contract intent active for every deferred breach

PR #396 currently orders quote spending through trigger-anchored strike intent only when the playbook flag is enabled. Production diagnostics on the failed rows show `playbook_enabled=false`.

For a deferred-breach plan with a valid scanner trigger:

1. Resolve the trigger-anchored primary strike.
2. Resolve the one-step OTM adjacent strike.
3. Rank those first for direct quote recovery.
4. Then rank a bounded nearby fallback band.

This ordering must be active for deferred contracts regardless of the optional playbook feature flag. The playbook flag may still govern broader expiration policy, but it must not disable the already-approved trigger anchor for quote spending.

Direct, non-deferred selector behavior remains unchanged unless it already uses the trigger-anchored policy.

### 4. Filter structurally hopeless candidates before provider calls

A candidate must not consume a direct quote call when facts already available from the chain prove it cannot be selected.

Before `broker.get_quote(occ_symbol)`, require:

- valid OCC symbol
- correct CALL/PUT side
- valid expiration/DTE for the active bucket
- strike inside the configured moneyness boundary
- delta inside the existing selector boundary when delta is present
- not already rejected by a terminal policy reason
- not already attempted in this selector lifecycle
- not clearly above the account budget/premium cap from a usable chain ASK, except for a small configurable recovery headroom

Use existing selector thresholds. Do not create weaker parallel thresholds.

Persist diagnostics for every skipped symbol:

- symbol
- skip reason
- strike
- delta
- chain BID/ASK
- estimated contract cost
- budget
- trigger-anchor tier

### 5. Carry attempted-symbol progress across retries

The five attempts must not each restart the same deterministic scan.

Persist a bounded selector recovery cursor in `orders.meta`, scoped by:

- local_order_id
- client_id
- execution_mode
- signal_id
- materialization generation
- expiration

Minimum persisted fields:

- attempted OCC symbols
- rejected OCC symbols with reason
- last ranked index
- expirations already probed
- selector attempt number
- provider timestamp / attempt timestamp

On retry:

- seed the new selector request context from the durable cursor
- skip already attempted symbols unless their prior result was explicitly transient and the retry timestamp is newer than the configured refresh interval
- continue with the next ranked candidates
- clear the cursor on selection, terminal invalidation, terminal quality failure, deadline expiry, or broker handoff

A restart must preserve progress. Process-local memory alone is insufficient.

### 6. Revalidate chart truth before every selector retry

Before attempt 2 through attempt 5:

- fetch fresh underlying truth through the approved market-data transport
- rerun the existing ticker-specific direction/stop/target/geometry authority
- do not call the option selector when the setup is no longer valid

Required outcomes:

- direction temporarily reverses but setup remains eligible -> REARM, no terminalization
- market truth unavailable/stale/unproven -> HOLD with bounded retry
- stop genuinely broken, target already complete, invalid geometry, or authoritative chart invalidation -> TERMINAL, zero selector calls, zero broker POST
- valid setup -> proceed to the next selector attempt

This requirement is the fence that keeps CRWD invalid while recovering the other valid opportunities. Do not implement a generic bypass around watcher or market-validity logic.

If the full PR #391 authority is not yet on main, extract only the already-reviewed pure classification and exact fresh-market-truth call needed by this retry seam. Do not import the oversized #392 branch wholesale.

### 7. Make final failure reasons truthful

`SELECTOR_REQUEST_BUDGET_EXHAUSTED` may be authoritative only when all are true:

- a provider-call or elapsed limit was actually reached
- at least one structurally eligible candidate remained unattempted
- the setup remained valid at the final chart-truth check

Otherwise preserve the real reason, such as:

- `DIRECT_QUOTE_ZERO_BID_ASK`
- `NO_AFFORDABLE_CONTRACT`
- `OI_TOO_LOW`
- `SPREAD_TOO_WIDE`
- `PREMIUM_CAP_EXCEEDED`
- `DELTA_OUT_OF_RANGE`
- `MARKET_SETUP_INVALIDATED`

Do not retry terminal quality or chart-invalid reasons merely because another DTE bucket later encountered a budget boundary.

### 8. Preserve broker and lifecycle safety

This PR may increase the number of selector attempts. It must not increase duplicate broker exposure.

Required invariants:

- one materialization owner per generation
- one active watcher/recovery owner
- one durable broker submit intent
- no second broker POST while an earlier POST is ambiguous
- no submit after cancellation/terminalization/deadline
- exact client_id and execution_mode fences on every order/meta mutation
- no position or proof_trades mutation before a confirmed fill

## Expected files

Keep production scope surgical. Expected maximum:

1. `ap/contract_selector.py`
2. `ap/contract_quote_revalidator.py`
3. `ap/selector_retry_policy.py`
4. `ap_execution_core.py`
5. `ap/deferred_materializer.py` or `ap/order_state_machine.py` only if needed for the durable recovery cursor
6. focused test file(s)
7. `.github/workflows/p0_regression.yml` only to add the focused suite

Do not rewrite the selector, watcher, queue, or order-state subsystem.

## Required tests

### Capacity and defaults

- default direct quote ceiling is 40
- default selector elapsed ceiling is 25000 ms
- default expiration/chain limits are 3/8
- default breach attempts are 5
- default retry delay is 8 seconds
- malformed env values fall back safely
- explicit lower/higher valid env values remain honored

### Trigger-anchored spending

- deferred CALL spends first quote on trigger-primary, then adjacent OTM
- deferred PUT spends first quote on trigger-primary, then adjacent OTM
- behavior is active when `playbook_enabled=false`
- direct non-deferred path is unchanged

### Structural prefilter

- deep OTM candidate consumes zero provider calls
- delta-terminal candidate consumes zero provider calls
- obviously unaffordable candidate consumes zero provider calls
- nearby zero-BID candidate consumes one provider call
- no quality threshold is relaxed

### Retry progress

- attempt 2 does not re-fetch attempt-1 symbols
- process restart reloads the durable cursor
- transient symbol may be retried only after refresh interval
- generation mismatch cannot reuse an old cursor
- client/mode mismatch fails closed

### Chart invalidation

- CRWD-style invalidation stops before selector and consumes zero direct quote calls
- temporary trigger reversal rearms instead of terminalizing
- stale/unproven market truth holds without POST
- stop broken/target complete remains terminal

### Reason truth

- budget reason requires actual exhausted detail and unattempted eligible candidates
- all candidates attempted with zero quotes -> `DIRECT_QUOTE_ZERO_BID_ASK`, not budget exhaustion
- all candidates unaffordable -> `NO_AFFORDABLE_CONTRACT`
- elapsed exhaustion with eligible candidates remaining -> budget exhaustion

### Money-path safety

- at most one broker POST across five attempts
- late fill/cancel ambiguity blocks replacement
- no order/position/proof mutation on chart invalidation
- exact client_id/execution_mode preserved

## Production replay gate

Build sanitized fixtures from the seven 2026-07-27 Jason LIVE rows:

- GE
- CAT
- ADSK
- FDX
- KLAC
- CRWD
- TMUS

The replay must not hard-code future outcomes or bypass quality rules. It must prove:

- CRWD terminates on chart invalidation before selector work
- valid rows are no longer lost solely because attempt 1 spent the entire quote budget on irrelevant/repeated contracts
- each row produces a truthful final reason
- any selected contract still passes existing spread, BID, OI/volume, delta, premium, affordability, and final quote gates

## Merge gate

HARD HOLD unless all are true:

1. Production implementation exists on the exact head.
2. Focused tests pass.
3. Exact-head P0 regression passes.
4. Seven-symbol replay evidence is attached.
5. No threshold was weakened.
6. CRWD-style invalidation produces zero selector calls and zero broker POST.
7. Five attempts cannot create duplicate or late broker exposure.
8. PR description lists exact deployment env changes and rollback values.

## Rollback

The PR must preserve hot-read rollback controls:

- set `MAX_BREACH_SELECTOR_RETRIES=3`
- set `BREACH_SELECTOR_RETRY_DELAY_SECONDS=20`
- set `SELECTOR_MAX_DIRECT_QUOTE_CALLS=20`
- set `SELECTOR_MAX_TOTAL_ELAPSED_MS=15000`
- disable durable retry-progress reuse with one explicit kill switch

Rollback must not require a database migration.