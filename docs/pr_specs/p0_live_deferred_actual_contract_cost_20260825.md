# P0 — LIVE deferred breach must select the real affordable contract before final exposure revalidation

**Date:** 2026-08-25  
**Priority:** P0 / LIVE tradeflow blocker  
**Base:** `c2c90334103863f6a45e8ba555683a3e7fae916c` (`main` at PR creation)  
**Production runtime observed:** `174c3701ef08744752285427b4359d6824c62c83`  
**Primary owner seam:** `ap_execution_core.py` → deferred watcher breach → `_recover_plan_for_revalidation()` → `_breach_risk_check()` / `APMasterControl.revalidate_exposure()` → deferred contract selector → OSM submit  
**Risk posture:** preserve fail-closed LIVE controls; repair sequencing/authority only

---

## Executive summary

Jason LIVE missed an affordable `C` CALL on 2026-08-25 while both PAPER accounts selected and filled the same real OCC contract seconds later.

The missed LIVE trade was not caused by insufficient account equity and was not caused by the selector failing to find an affordable contract. The LIVE path rejected the trade **before real contract materialization** because a recovered deferred order's reserved/pre-selection sizing value was treated as if it were the actual selected contract cost.

Production proof:

- Signal: `C` CALL, signal id `e19cb42c-244e-43aa-aee9-e6c8df4be684`
- Jason LIVE local order: `8190e196-602e-4c00-8f55-40b553c6ded3`
- Jason trigger confirmed around underlying `$132.65`
- Jason row remained `contract=DEFERRED:C`
- Jason recovered revalidation plan logged `cost=$173`
- Jason refreshed per-trade budget was approximately `$170.92`
- Master Control rejected with `ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP`
- Jason order terminalized `PENDING_TRIGGER -> EXPIRED`
- Jason never received a broker order id
- Both PAPER accounts selected `C260828C00133000`
- PAPER limit was `$1.28`; fill was approximately `$1.26`
- One real contract therefore cost approximately `$126`
- Jason's max affordable premium was approximately `$1.71`, so the real `$1.26` contract was affordable with roughly `$45` of headroom

This is a sequencing/financial-authority defect. A placeholder/reserved dollar amount is being labeled and consumed as `real_cost` before the deferred selector has returned an OCC contract and executable price.

The required fix is **not** to loosen Jason's risk cap. It is to ensure that deferred LIVE materialization uses account capacity as an input to selector affordability, obtains a real OCC contract and price, then performs final exposure revalidation on the actual selected contract cost.

---

## Production incident proof

### Jason LIVE

Observed watcher path:

```text
[C] CALL CONFIRMED — ask=$132.65 held above $132.63 for 2 polls
[C] Breach confirmed @ $132.65 -- submitting approved queued plan
```

The watcher therefore did its job. The trade reached the breach callback.

Immediately afterward:

```text
[C] Recovered approved plan for breach revalidation from OSM order ... | cost=$173
```

Then Master Control:

```text
SMALL_ACCOUNT_CONTRACT_UNAFFORDABLE
selected_trade_cost=$173.06
per_trade_budget=$170.92
computed_qty=0
block_reason=ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP
```

Then ExecutionCore:

```text
[C] Breach exposure revalidation blocked: ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP
ENTRY_TRIGGER_BLOCKED_RETURN ... contract=DEFERRED:C ... reason=breach_risk_check_false
```

Then OSM:

```text
ORDER PENDING_TRIGGER -> EXPIRED
last_error=breach_risk_check_false
```

No real OCC contract had been persisted for Jason and no broker order was submitted.

### PAPER positive control

For the exact same canonical signal:

```text
tradefluencehq@gmail.com
contract=C260828C00133000
limit_price=1.28
filled
avg_fill=1.26

jose.vasquez4011@gmail.com
contract=C260828C00133000
limit_price=1.28
filled
avg_fill=1.26
```

The real selected contract cost for one contract was therefore approximately `$126`, materially below Jason's approximately `$170.92` refreshed per-position budget.

This is the positive control that proves the LIVE rejection was false.

---

## Exact code-level root cause

### 1. Deferred recovery converts reserved/pre-selection dollars into `max_position_usd`

In `ap_execution_core.py`, `_recover_plan_for_revalidation()` recovers the OSM row and computes:

```python
qty = int(order.get("qty") or 0)
reserved = float(order.get("reserved_cost") or 0)
limit_price = float(order.get("limit_price") or 0)
real_cost = reserved if reserved > 0 else (
    limit_price * qty * 100
    if limit_price > 0 and qty > 0
    else 0.0
)
```

It then constructs the recovered plan with:

```python
max_position_usd=real_cost
```

For a deferred order, `reserved_cost` is a reservation/sizing ceiling established before a real OCC contract exists. It is not necessarily the selected contract's executable cost.

The name `real_cost` is therefore financially false in the deferred state.

### 2. Master Control treats `plan.max_position_usd` as actual contract cost

In `APMasterControl.revalidate_exposure()`:

```python
real_cost = float(plan.max_position_usd)
```

The per-position gate then asks:

```text
real_cost > per_trade_budget ?
```

and emits:

```text
ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP
```

This is correct for a materialized plan whose `max_position_usd` was recomputed from a real selected contract.

It is incorrect for a `DEFERRED:<ticker>` plan whose `max_position_usd` still represents reservation authority or a sizing ceiling.

### 3. The false pre-selection rejection prevents selector materialization

The current breach path calls exposure revalidation while the contract is still `DEFERRED:C`. Because the recovered plan says approximately `$173.06` and the refreshed budget says approximately `$170.92`, revalidation returns false.

The deferred selector therefore never gets the opportunity to prove that `C260828C00133000` at approximately `$1.26` is affordable.

The bug is an authority inversion:

```text
CURRENT BROKEN ORDER

breach
  -> recover DEFERRED plan
  -> interpret reserved/sizing ceiling as actual contract cost
  -> final per-position revalidation
  -> false reject
  -> terminalize
  -> selector never materializes actual contract
```

The required order is:

```text
CORRECT DEFERRED LIVE ORDER

breach
  -> refresh LIVE equity / exposure / per-position capacity
  -> derive selector affordability ceiling
  -> run deferred breach contract selector
  -> require real OCC contract + valid executable price + qty
  -> recompute materialized plan cost from selected contract
  -> final exposure revalidation using actual selected contract cost
  -> fresh submit quote / existing submit invariants
  -> OSM broker submit
```

---

## Scope

### In scope

1. `ap_execution_core.py`
   - deferred breach sequencing
   - `_recover_plan_for_revalidation()` authority semantics if needed
   - explicit distinction between reservation/capacity and materialized actual cost
   - ensure `execution_mode=live` is preserved through any risk/selector call touched by this change

2. `ap_master_control.py`
   - only if a narrowly scoped API/helper is needed to expose per-position affordability/capacity without pretending a deferred reservation is actual cost
   - do not weaken `revalidate_exposure()` for materialized plans

3. `ap/contract_selector.py`
   - only if necessary to pass the refreshed affordability ceiling explicitly into deferred breach materialization
   - selector remains sole contract-quality authority

4. Existing OSM materialization/update seam
   - once selector succeeds, persist real `contract_symbol`, `limit_price`, `qty`, and actual materialized exposure before final submit revalidation

5. Tests
   - add production-shape regression coverage described below

### Explicitly out of scope

Do **not** fix these in this PR unless the implementation literally cannot compile without a tiny compatibility change:

- `position_manager.snapshot failed — snapshot_execution_mode_required:None`
- lifecycle `NONE -> TRIGGER_READY` illegal-transition diagnostics
- watcher ownership retained after terminal OSM state
- reconciler `tuple index out of range`
- exit engine `UNDERLYING_STOP_DEFERRED_DATA_UNAVAILABLE`
- exit decision/broker-submit disconnect
- Supabase latency/schema issues
- generalized sizing redesign
- PAPER sizing percentages
- score/tier policy
- selector quality thresholds
- max positions
- total capital cap policy

Those are separate defects. Do not let this P0 turn into an archaeological expedition through the entire trading platform.

---

## Required behavioral contract

### A. Placeholder state must never masquerade as actual contract cost

If `plan.contract_symbol` is missing, equals the underlying ticker, or starts with `DEFERRED:`, then pre-materialization dollars must not be emitted or consumed as `ACTUAL_CONTRACT_COST`.

Use explicit semantics. Examples of acceptable concepts:

- `reserved_cost`
- `selector_budget`
- `max_position_usd`
- `max_affordable_premium`
- `per_position_capacity`

But do not name or log those values as `real_cost` until a real OCC contract and executable price exist.

### B. LIVE remains fail-closed

The repair must not allow a trade through when the system cannot prove:

- valid `client_id`
- `execution_mode=live`
- current per-position capacity
- current total exposure capacity
- a real OCC contract
- valid positive qty
- valid executable option price
- final actual materialized cost within both per-position and total exposure caps

Any missing required data must block or follow the existing bounded retry taxonomy.

### C. Risk caps do not change

Do not change:

- Jason's 10% per-position policy
- total capital exposure cap
- max positions
- daily loss policy
- contract quality filters
- LIVE quote semantics

The only financial behavior change should be that an affordable real contract can be selected before the final actual-cost gate is evaluated.

### D. Selector remains contract-quality authority

Do not select a contract in ExecutionCore.

`APContractSelectionEngine.select()` remains the single contract-quality authority. ExecutionCore may provide the correct affordability budget/context, but it must not duplicate strike/DTE/delta/spread/OI/volume selection rules.

### E. Final revalidation must use materialized actual cost

After selector returns a valid selection, recompute the plan from the actual selection:

```text
actual_cost = executable_price_per_share * qty * 100
```

or the selector's canonical `premium_per_contract * qty` where that field is already authoritative and consistent.

Before broker submission, verify that the plan's persisted/materialized cost equals the selected contract cost within an appropriate money/rounding tolerance.

The final per-position gate must run on this actual materialized cost.

### F. Total exposure gate remains authoritative

A contract may be below the per-position budget but still exceed remaining portfolio capacity. That must continue to block.

Do not collapse the split-cap logic introduced in Master Control.

### G. Preserve identity and mode

Every touched path must preserve:

```text
client_id=jasoncosby1@gmail.com
execution_mode=live
canonical signal_id
local_order_id
```

No lookup may silently default LIVE to PAPER or unknown mode.

### H. No broker submit/cancel broadening

This PR must not add a new broker submission surface.

The final successful path must continue through the existing OSM `submit_existing_entry()` seam and existing submit-time quote refresh/invariants.

No direct Tradier submit call from selector or Master Control.

---

## Preferred implementation shape

Implementation may vary if the current code structure requires it, but the safest shape is:

### Step 1 — recover deferred order without claiming it has an actual cost

`_recover_plan_for_revalidation()` should preserve the existing reservation fields for audit/recovery, but the code path must distinguish:

```text
reservation authority / sizing ceiling
```

from:

```text
materialized selected contract cost
```

If changing the dataclass is too invasive, keep the existing field for compatibility but add an explicit deferred-state guard in ExecutionCore so it is not passed to the final actual-cost gate before materialization.

### Step 2 — obtain current affordability authority

Before deferred selector materialization, obtain the current LIVE account per-position capacity and total remaining capacity through a narrow Master Control helper or existing safe snapshot.

The helper must return capacity, not an approval based on fake actual cost.

Example conceptual return:

```python
{
    "per_trade_budget": 170.92,
    "remaining_total_capacity": 553.66,
    "max_affordable_premium": 1.7092,
    "execution_mode": "live",
}
```

Do not create a second independent formula that can drift from Master Control.

### Step 3 — run deferred selector with the current affordability ceiling

The selector receives the refreshed budget/capacity and may only return contracts that fit that account.

For the production C case, a `$1.26` contract should survive a `$1.7092` max affordable premium.

If no contract below the cap passes all selector quality rules, return an honest selector terminal reason. Do not synthesize an affordable contract.

### Step 4 — materialize OSM/plan from selector output

Require `_validate_deferred_selector_result()` to pass.

Copy canonical selector output into the approved plan/OSM row:

- real OCC contract
- executable price
- affordable qty
- actual cost
- selector diagnostics/materialization proof

No `DEFERRED:*` contract may reach submit.

### Step 5 — final actual-cost revalidation

Now call the existing final exposure revalidation using the materialized plan.

For C production shape:

```text
actual_cost ~ $126
per_trade_budget ~ $170.92
per-position gate = PASS
```

Then evaluate total exposure using the existing split-cap logic.

### Step 6 — existing submit path only

After final revalidation succeeds:

- preserve existing submit-time fresh quote behavior
- preserve acceptance cap behavior if enabled
- preserve OSM order-row handoff proof
- call `submit_existing_entry()`

---

## Required diagnostics changes

Diagnostics must stop lying about financial authority.

### Before materialization

Do not emit:

```text
selected_trade_cost=$173.06
ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP
```

when the contract is still `DEFERRED:<ticker>`.

If the capacity itself cannot support even the minimum possible selector price under existing policy, use an explicit capacity/selector-budget reason.

### After materialization

Emit both:

```text
selected_contract=<OCC>
selected_execution_price=<price>
selected_qty=<qty>
actual_selected_cost=<dollars>
per_trade_budget=<dollars>
remaining_total_capacity=<dollars>
```

Then any `ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP` reason is truthful.

### Preserve existing decision evidence

Do not remove:

- decision events
- materialization outcome/detail
- selector request kind
- dte ladder audit
- direct quote recovery audit
- local order id
- canonical signal id
- client id
- execution mode

---

## Required tests

### Test 1 — production C regression: affordable real contract must not be falsely blocked by recovered reservation

Create a regression test using the actual production shape:

```text
execution_mode = live
client = Jason-like small account
contract = DEFERRED:C
recovered reserved/max_position_usd = 173.06
refreshed per_trade_budget = 170.92
selector returns C260828C00133000
selector executable price = 1.26 (or 1.28)
selector qty = 1
actual selected cost = 126 (or 128)
remaining total capacity > actual selected cost
```

Assert:

- pre-materialization reservation `$173.06` is **not** treated as actual selected cost
- selector is called
- selector receives affordability consistent with approximately `$170.92`
- real OCC contract is copied onto plan
- plan qty is `1`
- plan materialized cost is approximately `$126`/`$128`, not `$173.06`
- final `revalidate_exposure()` sees materialized actual cost
- per-position gate passes
- OSM submit seam is reached exactly once
- no direct broker submission occurs outside OSM

### Test 2 — real selected contract genuinely over budget still blocks

Shape:

```text
per_trade_budget = 170.92
selector real contract price = 1.85
qty = 1
actual cost = 185
```

Assert final revalidation blocks with truthful:

```text
ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP
```

No broker submit.

### Test 3 — total exposure cap still blocks even if per-position cap passes

Shape:

```text
actual selected cost = 126
per_trade_budget = 170.92
remaining_total_capacity = 100
```

Assert total exposure gate blocks. No broker submit.

### Test 4 — selector finds no affordable quality contract

Selector returns no valid contract / explicit affordability terminal reason.

Assert:

- no fake OCC contract
- no submit
- terminal/retry taxonomy remains truthful
- no `ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP` unless a real contract was actually selected and evaluated

### Test 5 — missing/unknown execution mode fails closed

Any new helper/API must reject unknown execution mode.

Assert no default to LIVE or PAPER.

### Test 6 — identity preservation

Assert `client_id`, `execution_mode`, `signal_id`, and `local_order_id` survive:

```text
watcher -> recovery -> selector -> materialized plan -> final revalidation -> OSM submit
```

### Test 7 — non-deferred materialized path unchanged

A plan that already has a real OCC contract and actual `max_position_usd` must continue to enter `revalidate_exposure()` exactly as before.

This is essential. The fix must not reorder ordinary/non-deferred entries.

### Test 8 — existing deferred breach suite remains green

At minimum run:

```text
tests/test_p0_deferred_breach_submit_repair.py
tests/test_breach_block_diagnostics.py
tests/test_p0_watcher_recovery_execution_ownership.py
tests/test_p0_seam4_e2e_deferred_lifecycle.py
tests/test_p0_unknown_execution_mode_fail_closed.py
```

Plus any Master Control capital sizing/revalidation tests affected by the implementation.

---

## Existing test gap this PR must close

`tests/test_p0_deferred_breach_submit_repair.py` already proves that once a selector result exists, the real selected contract is copied onto the deferred plan and submit can proceed.

That suite does **not** prove the production sequence that failed on 2026-08-25:

```text
recovered deferred reservation > freshly recomputed per-position budget
BUT
real selectable OCC contract < freshly recomputed per-position budget
```

The new regression must exercise this exact inequality:

```text
reservation / placeholder cost > refreshed budget > actual selected contract cost
```

Specifically:

```text
173.06 > 170.92 > 126.00
```

That is the missing positive control.

---

## Forbidden implementations

Reject the PR if it does any of the following:

1. Raises Jason's per-position percentage to make the test pass.
2. Adds a `$2`, `$5`, percentage, or arbitrary epsilon to the risk cap.
3. Treats the PAPER fill as authoritative pricing for LIVE.
4. Copies PAPER sizing behavior into LIVE.
5. Skips final exposure revalidation after selector materialization.
6. Bypasses total-capital exposure checks.
7. Directly submits to Tradier from selector or Master Control.
8. Uses `DEFERRED:*` as a broker contract.
9. Creates a second copy of capital formulas in ExecutionCore.
10. Hardcodes `C`, Jason's email, `$170.92`, `$173.06`, `$1.26`, or any production-specific values into runtime logic.
11. Broadly changes selector DTE/delta/spread/OI/volume/moneyness policy.
12. Marks the false pre-selection rejection as retryable without fixing the authority ordering.
13. Silences diagnostics instead of fixing the decision.
14. Uses stale static premium estimates as final LIVE actual-cost truth.
15. Changes broker cancel/replace behavior unrelated to this seam.

---

## Acceptance criteria

This PR is mergeable only if all are true:

- [ ] Production C regression passes: `173.06 > 170.92 > 126` no longer false-blocks before selector.
- [ ] Deferred LIVE selector runs before final actual-cost revalidation.
- [ ] Real OCC contract is required before broker submit.
- [ ] Actual cost is derived from selector/submit-authoritative option pricing, not recovered reservation.
- [ ] Final per-position cap remains unchanged.
- [ ] Final total exposure cap remains unchanged.
- [ ] Genuine over-budget selected contract still blocks.
- [ ] Unknown execution mode fails closed.
- [ ] `client_id` and `execution_mode` are preserved end-to-end.
- [ ] No new direct broker submit/cancel surface is introduced.
- [ ] PAPER behavior is unchanged except tests/diagnostics that share safe helpers.
- [ ] Non-deferred materialized entry path is unchanged.
- [ ] Existing deferred selector retry taxonomy remains intact.
- [ ] Existing submit-time quote refresh remains intact.
- [ ] Existing deferred handoff proof remains intact.
- [ ] Diagnostics distinguish reservation/capacity from actual selected cost.
- [ ] Relevant targeted tests pass.

---

## Production validation after merge/deploy

Do not validate success by merely seeing fewer blocks.

For the first triggered deferred LIVE signal after deployment, capture one coherent trace and prove:

```text
WATCHER_TRIGGER_CALLBACK_ATTEMPT
-> breach confirmed
-> current LIVE capacity resolved
-> deferred selector request begins
-> real OCC contract selected
-> actual selected cost recorded
-> final CAPITAL_GATE evaluates that real selected cost
-> OSM submit_existing_entry called
-> broker_order_id persisted
```

For an affordable trade, expected diagnostic shape:

```text
contract=<real OCC>
actual_selected_cost <= per_trade_budget
projected_total_exposure <= total_capital_cap
CAPITAL_GATE decision=ALLOWED
```

For an unaffordable trade, expected shape:

```text
contract=<real OCC if selector reached materialization>
actual_selected_cost > per_trade_budget
ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP
```

or an honest selector affordability reason if no real contract under the cap exists.

Never again accept this impossible combination as a valid final rejection:

```text
contract=DEFERRED:<ticker>
reason=ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP
```

because no actual contract cost exists yet.

---

## Rollback plan

This should be a small sequencing/authority change. If post-deploy telemetry shows unexpected entry behavior:

1. revert this implementation commit/PR;
2. do not change account percentages as an emergency workaround;
3. restore prior fail-closed behavior;
4. use PAPER to reproduce before another LIVE attempt.

No schema migration is expected or desired for this repair.

---

## Review checklist

Reviewer must explicitly answer:

### Live behavior

- Does this change LIVE behavior? **Yes. Intentionally. It restores selection of contracts that are truly affordable but currently false-blocked pre-selection.**
- Is it flag-off? **No. This fixes an active incorrect LIVE decision seam.**

### Broker authority

- Does it add direct broker submit/cancel? **Must be NO.**
- Does successful execution still go through OSM? **Must be YES.**

### State mutation

- Orders: only existing materialization/submit lifecycle mutations.
- Positions: no new mutation path.
- `proof_trades`: no new mutation path.
- Queue: no new mutation path beyond existing signal/order lifecycle behavior.

### Identity

- `client_id` preserved? **Must be YES.**
- `execution_mode` preserved? **Must be YES.**
- No LIVE/PAPER taxonomy pollution? **Must be YES.**

### Financial truth

- Does any pre-selection reservation value still get called `actual contract cost`? **Must be NO.**
- Does final gate evaluate actual selected contract dollars? **Must be YES.**
- Can a genuinely over-budget selected contract submit? **Must be NO.**

### Regression risk

- Non-deferred entries unchanged? **Must be YES.**
- Selector quality policy unchanged? **Must be YES.**
- Total exposure cap unchanged? **Must be YES.**

---

## Merge recommendation

**HARD HOLD until implementation and the production-shape regression are present.**

Once the code satisfies this spec and all targeted tests pass, re-audit the actual diff under the production review rubric before merge.

The purpose of this PR is narrow: **stop the deferred LIVE breach path from rejecting a real affordable trade based on a pre-selection reservation that is not the selected contract's cost.**
