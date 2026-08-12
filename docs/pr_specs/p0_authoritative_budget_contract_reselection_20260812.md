# P0 CURRENT-MAIN WORK ORDER — Authoritative capital budget + contract reselection

## Status

**DRAFT / HARD HOLD — SPEC ONLY. DO NOT MERGE OR DEPLOY AS AN IMPLEMENTATION.**

This PR records a production-proven LIVE entry-conversion defect from 2026-08-12. The implementation must be rebuilt against then-current `main`, must reproduce the incident before changing production behavior, and must remain Draft/HARD HOLD until exact-head tests and independent money-path review are attached.

## Why this PR exists

Jason LIVE did not have a scanner drought on 2026-08-12. Master Control approved many setups, but several breach-time candidates repeatedly failed after a contract became actionable because the contract cost and the final per-position capital authority disagreed by only a few dollars.

Observed production examples repeatedly emitted:

```text
ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP
client_email=jasoncosby1@gmail.com
execution_mode=live
real_cost=$169
per_trade_budget=$166
computed_qty=0
original_qty=1
```

The repeated class appeared hundreds of times during the session and affected at least CL, CSX, DDOG, GS, MMM, and SBUX. Earlier selector/budget diagnostics for the same account were around `$169.147` from equity near `$1,691.47`, while later breach-time/final risk authority could resolve around `$166`.

The safety decision **not to buy a $169 contract when the authoritative cap is $166 is correct**. The defect is what happens next: the same logical setup can remain alive while the runtime repeatedly rediscovers/rechecks the same now-unaffordable contract or ultimately expires the whole entry instead of cleanly returning to an affordable-contract selection state.

This PR must not increase Jason's risk cap merely to make trades happen.

## Proven production truth vs hypothesis

### Proven

1. Master Control approved the underlying setups.
2. The account had no corresponding broker ENTRY for the affected candidates.
3. Final breach-time risk repeatedly rejected a real contract cost above the then-effective per-position cap.
4. The same symbols could be revisited on a short cadence with the same cost/cap mismatch.
5. Some affected deferred ENTRY rows ultimately expired with `breach_risk_check_false` or related deferred-materialization outcomes.

### Not yet proven

The exact reason the earlier selector budget and later final budget differ has not been reduced to one current-main line of code. Possible legitimate causes include updated equity, reservations, pending exposure, or a stricter final capital snapshot. The implementation task must trace that authority rather than assuming the earlier `$169.147` number should win.

**Final submit-time capital authority remains authoritative even when it is smaller than the selector's earlier budget.**

## Non-overlap with existing PRs

This PR owns one narrow seam: **a selected contract becomes unaffordable at final/breach-time capital revalidation while the underlying setup may still be valid.**

It must not duplicate or absorb:

- **#434** Gate G production-shape truth.
- **#435** breach-time setup intelligence.
- **#436** pre-submit entry-efficiency wait/rebreach lifecycle.
- **#437** selected-contract intelligence/telemetry. #437 evaluates a real selected contract; this PR defines what deterministic execution does when that real contract no longer fits authoritative capital.
- **#439** deferred-selector final-reason precedence. Do not rewrite candidate-scoped final-reason reduction.
- **#441** direct-quote provider budget/config parity. API quote-call budgets are not account capital budgets.
- **#430/#440** post-broker cancel/replace ownership. This PR is pre-broker when the contract is unaffordable.

If implementation discovers the defect is already completely closed by one of those PRs on current main, stop and attach the reproduction evidence instead of adding duplicate code.

## Canonical invariant

For one exact ENTRY opportunity:

```text
valid underlying setup
+ exact client/mode identity
+ authoritative current capital budget
+ fresh quality contract within that budget
+ all final submit gates
= at most one broker ENTRY submit
```

If a real selected contract fails authoritative capital revalidation:

```text
CONTRACT TOO EXPENSIVE
!= SIGNAL INVALID
!= RELAX RISK
!= RETRY SAME KNOWN-UNAFFORDABLE CONTRACT FOREVER
```

Instead, when the underlying opportunity remains structurally valid:

```text
selected contract rejected by final budget
-> contract-specific authority cleared/invalidated
-> exact logical ENTRY remains watcher/materialization owned
-> fresh selector pass receives authoritative maximum premium
-> cheaper quality contract selected OR bounded WAIT_FOR_AFFORDABLE_CONTRACT
-> all dynamic truth revalidated again before broker submit
```

If no acceptable contract can be found before the setup becomes invalid or retry authority is exhausted, terminalize honestly with a precise reason.

## Required durable decision vocabulary

Implementation may reuse existing fields if they can express the contract truthfully. Do not invent duplicate state if current OSM metadata can represent it.

At minimum diagnostics must distinguish:

- `CONTRACT_SELECTED_AFFORDABLE`
- `CONTRACT_RESELECT_REQUIRED_BUDGET_DRIFT`
- `WAITING_FOR_AFFORDABLE_QUALITY_CONTRACT`
- `NO_AFFORDABLE_QUALITY_CONTRACT_RETRYABLE`
- `NO_AFFORDABLE_QUALITY_CONTRACT_TERMINAL`
- `AUTHORITATIVE_BUDGET_UNAVAILABLE`
- `AUTHORITATIVE_BUDGET_IDENTITY_CONFLICT`
- existing truly terminal setup/risk outcomes

Names may follow current project taxonomy, but **do not collapse retryable affordability into `UNKNOWN_SELECTOR_RECOVERY_FAILURE` or generic `breach_risk_check_false` when the real reason is known.**

## Authoritative budget contract

Before implementation, inventory every current source participating in entry capital authority, including at minimum:

- account equity source and timestamp;
- configured max position percentage / absolute cap;
- total account deployment cap;
- current open-position capital;
- broker-proof pending ENTRY exposure;
- filled-but-unreconciled ENTRY exposure;
- reservations belonging to the current local order;
- exclusions used to prevent the current deferred row from double counting itself;
- exact `client_id`;
- exact `execution_mode`;
- snapshot timestamp/generation or another durable freshness identifier.

The selected budget passed into contract selection must be traceable. Final revalidation may legitimately return a stricter number, but the system must record why it changed.

A useful structured diagnostic shape is conceptually:

```text
budget_snapshot = {
  client_id,
  execution_mode,
  source,
  equity,
  max_position_pct,
  per_position_cap,
  total_capital_cap,
  deployed_capital,
  pending_submitted_entry_exposure,
  filled_unreconciled_entry_capital,
  current_order_exclusion,
  remaining_total_capacity,
  effective_contract_budget,
  observed_at,
  generation_or_hash,
}
```

Do not persist secrets. Do not create a second capital calculator if current Master Control already owns the calculation; expose/reuse its authoritative result.

## Required implementation order

### 1. Reproduce the 2026-08-12 mismatch on current main

Create a production-shaped test in which:

- exact client is Jason-shaped LIVE;
- equity/capital source first permits a contract around `$169`;
- final authoritative revalidation resolves `$166`;
- selected contract costs `$169`;
- quantity is 1;
- no broker order exists;
- underlying setup remains valid.

Prove current behavior before modifying it. Record the exact function sequence and durable state transitions.

### 2. Locate one capital authority

Do not make selector and Master Control independently choose competing budgets with no lineage. Either:

- selector consumes the current authoritative budget result directly; or
- selector consumes an explicitly versioned snapshot and final risk may supersede it, with the supersession causing deterministic reselection.

The final broker gate remains allowed to be stricter.

### 3. Invalidate the contract, not the setup

When final capital revalidation proves `real_cost > authoritative_budget` and the setup itself remains valid:

- broker ENTRY submit count must remain zero;
- the rejected OCC symbol/price must lose submit authority;
- preserve canonical signal/local-order/client/mode ownership;
- preserve breach/rebreach lineage;
- preserve retry/materialization generation;
- return to the existing selector continuation owner rather than inventing a new scheduler;
- pass the current authoritative max premium to reselection.

### 4. Never hammer a known-unaffordable contract

Within one unchanged budget/quote generation, the same OCC contract at the same-or-higher executable cost must not repeatedly consume retry cycles as though it were new information.

A retry may reconsider that contract only when material truth changed, for example:

- fresh executable ASK moved within budget;
- authoritative budget increased from fresh account truth;
- contract candidate universe changed;
- a new materialization/rebreach generation legitimately began.

Otherwise prefer another ranked quality candidate or wait for fresh data.

### 5. Preserve quality gates

Reselection must **not** solve the problem by buying junk.

Preserve current reviewed rules for spread, OI/volume, quote freshness, zero bid/ask, delta/DTE/moneyness, executable BUY price, and provider truth unless another separately reviewed PR changes them.

A cheaper contract that fails quality remains ineligible.

### 6. Bound waiting and retries

No infinite loops.

Reuse current deferred selector/materialization attempt authority. Do not create a fourth retry counter.

Every new attempt must have one durable owner and must stop when:

- setup/side is invalid;
- trigger/rebreach no longer satisfies current policy;
- target already completed;
- stop/thesis invalidated under current rules;
- account risk no longer permits entry;
- session window ends;
- current retry/materialization budget is exhausted;
- identity/cursor ownership becomes ambiguous.

### 7. Restart must preserve the same logical opportunity

A restart while waiting for an affordable contract must not:

- reset attempt count to generation zero;
- create a second `PENDING_TRIGGER` ENTRY;
- forget the known rejected contract/budget reason if needed for idempotency;
- cross client or PAPER/LIVE identity;
- submit a contract selected under stale budget authority without fresh revalidation.

## Likely current-main production scope

Start with the smallest real seam. Expected candidates are:

1. `ap_master_control.py` — authoritative final capital revalidation / diagnostics.
2. `ap_execution_core.py` — breach-time selected-contract continuation and disposition handling.
3. `ap/contract_selector.py` — only if it must consume an authoritative max premium/reselection exclusion.

`ap/order_state_machine.py` is conditional only if current durable metadata cannot safely represent the reselection lifecycle. If more than three production modules are required, stop and document why before expanding.

## Required regression matrix

At minimum prove:

1. `$169` selected / `$166` final cap / cheaper quality `$150` exists -> reject first contract, select `$150`, exactly one broker submit total.
2. `$169` / `$166` / no cheaper quality contract -> zero broker submit, bounded wait/retry.
3. Same unaffordable contract + unchanged budget/quote -> no repeated selector/risk churn counted as new authority.
4. Same contract later ASK falls to `$165` with fresh quote -> it may become eligible after full revalidation.
5. Budget later rises from fresh authoritative account truth -> fresh revalidation may authorize, never stale cached authority.
6. Budget falls further -> still zero submit.
7. Contract quality fails while affordable -> zero submit; affordability does not bypass quality.
8. Setup invalidates while waiting -> clean terminal, zero submit.
9. Target completes while waiting -> clean terminal, zero submit.
10. Direction reverses/rearms -> obey canonical #421/#436 ownership; old contract authority cannot leak into new breach generation.
11. Duplicate watcher callbacks -> at most one reselection claim and one broker submit.
12. Process restart before reselection -> same durable opportunity, no duplicate ENTRY row.
13. Process restart after affordable selection but before POST -> existing final submit fences decide; no duplicate POST.
14. `client_id` mismatch -> fail closed, zero mutation outside exact row.
15. `execution_mode` mismatch/missing/malformed -> fail closed, zero broker submit.
16. PAPER candidate cannot authorize LIVE submit.
17. Current deferred row is excluded exactly once from pending-capital math; no self-double-count.
18. Other broker-proof pending entries remain counted.
19. Filled-unreconciled exposure remains counted.
20. malformed numeric budget/cost (`NaN`, negative, bool, whitespace, infinity) -> fail closed.
21. zero/unknown budget in LIVE -> fail closed.
22. selector exception -> preserve truthful retry/terminal disposition; do not fabricate affordability failure.
23. provider quote unavailable -> normal selector retry taxonomy, no broker submit.
24. MMM/CSX/SBUX/DDOG production-shaped replay reproduces the former `$169/$166` churn and demonstrates bounded convergence.
25. Zero changes to scanner admission, score floor, intelligence authority, exit policy, proof economics, or queue fanout.

## Observability requirements

For every budget-driven reselection attempt emit enough structured evidence to answer:

- which exact client/mode/signal/local order;
- prior selected contract and executable cost;
- authoritative budget and its source/generation;
- why prior budget differed, if known;
- whether the contract, setup, or both were invalidated;
- reselection attempt number;
- new selected contract or wait/terminal reason;
- broker submission attempted: yes/no;
- final broker order id only after real acceptance.

Avoid log spam: repeated unchanged unaffordable state should be rate-limited/aggregated while durable decision truth remains queryable.

## Money-path audit requirements

Final review must explicitly prove:

- **broker submit:** zero on every unaffordable/reselection-wait path; exactly one maximum on successful affordable continuation;
- **broker cancel:** no new cancel authority;
- **orders:** only exact current ENTRY metadata/status mutations required by the established lifecycle;
- **positions:** zero pre-fill mutation;
- **proof_trades:** zero mutation;
- **trade_queue:** zero new writer;
- **client_id/execution_mode:** exact and nonblank at every authority boundary;
- **PAPER/LIVE:** no taxonomy or broker-domain crossing;
- **Jason junk-trade risk:** no threshold relaxation and no risk-cap increase.

## Success criterion

This PR is successful when a valid setup is no longer thrown away merely because one selected contract became a few dollars too expensive, **without increasing the account's risk limit and without buying lower-quality contracts**.

The desired behavior is better conversion of already-valid opportunities, not more reckless trades.

## Merge / deploy gate

**HARD HOLD** until all of the following exist on the exact implementation head:

1. current-main reproduction of the 2026-08-12 defect;
2. focused production-shaped tests above;
3. adjacent selector/recovery/risk P0 suite green;
4. PostgreSQL-backed identity/restart cases where durable state is touched;
5. exact-head GitHub CI green;
6. complete broker/mutation path audit;
7. independent whole-PR review;
8. explicit owner authorization for merge and separately for deployment.

No merge, deploy, migration application, environment change, or LIVE enablement is authorized by this spec.