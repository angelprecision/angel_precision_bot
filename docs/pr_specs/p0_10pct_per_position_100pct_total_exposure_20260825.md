# P0 Capital Policy Contract — 10% Per Position, Up To 100% Aggregate Exposure

Date: 2026-08-25
Status: SPEC FIRST / DRAFT / HARD HOLD
Owner: Angel Precision

## 1. Purpose

This PR corrects the portfolio-level capital policy so the runtime matches the intended Angel Precision risk model:

- each individual position may use at most 10% of current account equity;
- multiple positions may coexist at that same per-position limit;
- aggregate deployed + canonical pending ENTRY exposure may grow up to 100% of current account equity;
- existing positions must not reduce the next position's 10% per-position budget unless the account is actually near the aggregate 100% exposure ceiling.

This is intentionally separate from PR #514.

PR #514 repairs a different defect: a DEFERRED reservation being interpreted as actual selected contract cost before materialization.

This PR changes only the intended aggregate capital policy from the current 40% default to 100%, while preserving the existing 10% per-position cap and the two-gate architecture introduced by PR #155.

## 2. Current policy in code

`APMasterControl.__init__()` currently resolves two separate capital authorities:

```python
_DEFAULT_MAX_POSITION_PCT = float(os.getenv("DEFAULT_MAX_POSITION_PCT", "0.10"))
_DEFAULT_MAX_TOTAL_CAPITAL_PCT = float(os.getenv("DEFAULT_MAX_TOTAL_CAPITAL_PCT", "0.40"))
```

The architecture then computes:

```python
per_trade_budget = account_equity * self.max_position_pct
total_capital_cap = account_equity * self.max_total_capital_pct
current_total_exposure = capital_deployed + pending_capital
remaining_total_cap = total_capital_cap - current_total_exposure
selector_budget = min(per_trade_budget, remaining_total_cap)
```

That architecture is correct.

The policy value `0.40` is not.

The current default means:

```text
max one position       = 10% of equity
max aggregate exposure = 40% of equity
```

The required policy is:

```text
max one position       = 10% of equity
max aggregate exposure = 100% of equity
```

## 3. Canonical Angel Precision capital policy

### 3.1 Per-position authority

For every new ENTRY candidate:

```text
per_position_cap = current_account_equity * 0.10
```

This cap is independent of already-deployed capital.

Example:

```text
equity             = $10,000
existing exposure  = $3,000
per-position cap   = $1,000
```

The next position still has a $1,000 per-position ceiling.

The next-position budget MUST NOT be computed as:

```text
$1,000 - $3,000
```

Existing exposure does not subtract from the per-position cap.

### 3.2 Aggregate authority

The portfolio may deploy up to current account equity:

```text
total_portfolio_cap = current_account_equity * 1.00
```

Aggregate exposure includes only the canonical exposure sources already recognized by Master Control. Do not invent a new exposure source in this PR.

Do not count terminal/canceled/expired/rejected phantom ENTRY rows as real exposure.

### 3.3 Remaining total capacity

```text
remaining_total_capacity =
    total_portfolio_cap - current_total_exposure
```

### 3.4 Selector/new-position budget

```text
selector_budget = min(
    per_position_cap,
    remaining_total_capacity,
)
```

This preserves both invariants:

1. no single trade may exceed 10% of equity;
2. all positions together may not exceed 100% of equity.

## 4. Required behavior examples

### Example A — no positions

```text
equity = $10,000
current exposure = $0
per-position cap = $1,000
total cap = $10,000
remaining total = $10,000
selector budget = $1,000
```

Expected: one qualifying trade may consume up to $1,000.

### Example B — three existing 10% positions

```text
equity = $10,000
current exposure = $3,000
per-position cap = $1,000
total cap = $10,000
remaining total = $7,000
selector budget = $1,000
```

Expected: fourth qualifying trade may still consume up to $1,000.

Existing exposure MUST NOT shrink the fourth trade below the normal per-position cap unless remaining total capacity itself is below $1,000.

### Example C — nine existing 10% positions

```text
equity = $10,000
current exposure = $9,000
per-position cap = $1,000
total cap = $10,000
remaining total = $1,000
selector budget = $1,000
```

Expected: tenth qualifying full-size position may still be admitted.

### Example D — account nearly fully deployed

```text
equity = $10,000
current exposure = $9,650
per-position cap = $1,000
total cap = $10,000
remaining total = $350
selector budget = $350
```

Expected: selector may only consider a valid position fitting within the remaining $350.

### Example E — fully deployed

```text
equity = $10,000
current exposure = $10,000
remaining total = $0
```

Expected:

```text
CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED
```

No new ENTRY broker submit.

### Example F — per-position violation while total capacity remains

```text
equity = $10,000
current exposure = $2,000
per-position cap = $1,000
remaining total = $8,000
candidate actual cost = $1,200
```

Expected:

```text
ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP
```

The fact that total capacity has $8,000 remaining must NOT permit a $1,200 individual position.

## 5. Relationship to PR #155

PR #155 introduced the correct two-cap architecture because the prior single-cap model incorrectly compared projected total exposure against a per-trade limit.

Historical wrong shape:

```text
per-trade cap = $198
already deployed = $183
new trade cost = $183
projected total = $366

WRONG:
$366 > $198 -> reject
```

Correct architecture:

### Gate 1 — per-position

```text
new_trade_actual_cost <= equity * max_position_pct
```

Existing positions are irrelevant to Gate 1.

### Gate 2 — portfolio total

```text
existing_exposure + new_trade_actual_cost
<= equity * max_total_capital_pct
```

This PR must preserve that architecture exactly. Do not recombine the two caps.

## 6. Required implementation

### Preferred minimal production change

In `ap_master_control.py`, change the default aggregate policy:

```python
_DEFAULT_MAX_TOTAL_CAPITAL_PCT = float(
    os.getenv("DEFAULT_MAX_TOTAL_CAPITAL_PCT", "1.00")
)
```

instead of:

```python
_DEFAULT_MAX_TOTAL_CAPITAL_PCT = float(
    os.getenv("DEFAULT_MAX_TOTAL_CAPITAL_PCT", "0.40")
)
```

Do not alter the existing default per-position value:

```text
DEFAULT_MAX_POSITION_PCT = 0.10
```

### Environment/config audit required

Before calling the implementation complete, search every runtime configuration path for any explicit override of:

```text
DEFAULT_MAX_TOTAL_CAPITAL_PCT
max_total_capital_pct
max_capital_pct
max_position_pct
```

including client runner construction, Master Control construction, deploy/config files, tests, and client-specific profiles.

If production explicitly passes `max_total_capital_pct=0.40`, changing only the module default will not change production behavior.

The implementation must prove the effective LIVE runtime value resolves to 1.00 when no intentional client override exists.

Do not silently delete a client-specific explicit override. If one exists, document it and change it only if it represents the same obsolete 40% policy.

## 7. Scope

Expected production scope should be tiny.

Likely production file:

```text
ap_master_control.py
```

Potential config caller files may be included only if they explicitly pin the obsolete 0.40 total cap.

Tests and documentation may be added as needed.

## 8. Hard exclusions

This PR MUST NOT change:

- the 10% per-position policy;
- position sizing formulas other than aggregate ceiling authority;
- contract selector quality thresholds;
- score thresholds;
- DTE, delta, moneyness, spread, OI, volume or premium quality rules;
- entry trigger logic;
- watcher logic;
- CALL/PUT arbitration;
- deferred materialization sequencing from PR #514;
- broker submit API shape;
- broker cancel behavior;
- order-state transitions;
- exits, stops, take profit or trailing logic;
- positions directly;
- proof_trades;
- reconciliation;
- daily-loss limits;
- max-position count;
- sector/ticker caps except test-fixture configuration needed to isolate this capital policy;
- LIVE/PAPER execution-mode semantics;
- client identity rules.

## 9. Broker safety

This PR must not introduce any new broker call.

It only changes how much aggregate exposure Master Control permits before existing broker-facing submission paths are eligible.

All broker submission must continue through the existing order state machine.

No direct Tradier call may be added.

## 10. Required regression tests

### Test 1 — default policy constants

Instantiate Master Control with no explicit aggregate override.

Assert:

```text
max_position_pct == 0.10
max_total_capital_pct == 1.00
```

### Test 2 — second position retains full 10% budget

```text
equity = 10,000
existing exposure = 1,000
```

Assert:

```text
per_trade_budget = 1,000
remaining_total = 9,000
selector_budget = 1,000
```

### Test 3 — fourth position retains full 10% budget

```text
equity = 10,000
existing exposure = 3,000
```

Assert selector/new-position budget remains exactly $1,000.

### Test 4 — fifth position now allowed beyond historical 40% boundary

Critical regression:

```text
equity = 10,000
existing exposure = 4,000
new valid trade cost = 1,000
projected exposure = 5,000
```

Under the obsolete 40% total cap this cannot proceed.

Under required policy:

```text
5,000 <= 10,000
```

Assert aggregate gate passes.

### Test 5 — tenth full-size position allowed

```text
equity = 10,000
existing exposure = 9,000
new trade = 1,000
projected total = 10,000
```

Assert Gate 2 passes exactly at the ceiling.

Do not change equality semantics merely for this PR.

### Test 6 — over-cap position blocks

```text
equity = 10,000
existing exposure = 10,000
new trade > 0
```

Assert total-exposure block and zero broker submit.

### Test 7 — partial final capacity

```text
equity = 10,000
existing exposure = 9,650
per-position = 1,000
remaining total = 350
```

Assert selector budget becomes $350.

### Test 8 — per-position cap remains hard

```text
equity = 10,000
existing exposure = 2,000
candidate actual cost = 1,200
```

Assert:

```text
ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP
```

No broker submit.

### Test 9 — total cap remains hard

```text
equity = 10,000
existing exposure = 9,500
candidate cost = 600
```

Per-position passes because 600 <= 1,000.

Projected exposure = 10,100.

Assert Gate 2 blocks with canonical total-cap/remaining-capacity reason.

### Test 10 — existing exposure does not shrink Gate 1

Parameterize existing exposure at:

```text
0%, 10%, 20%, 30%, 40%, 50%, 60%, 70%, 80%, 90%
```

For each case while remaining total capacity >= 10%:

```text
per_trade_budget == equity * 0.10
selector_budget == equity * 0.10
```

This proves the pre-PR-155 apples-vs-oranges bug stays dead.

### Test 11 — aggregate progression matrix

On $10,000 equity, prove projected exposure may progress through:

```text
10%, 20%, 30%, 40%, 50%, 60%, 70%, 80%, 90%, 100%
```

without aggregate-cap rejection.

Attempt 101% and assert rejection.

### Test 12 — LIVE/PAPER same percentage policy

For identical equity/exposure fixtures, assert both modes resolve the same 10%/100% percentages unless an explicit documented client/mode override exists.

Do not weaken LIVE fail-closed snapshot requirements to make this pass.

### Test 13 — env override still works

If:

```text
DEFAULT_MAX_TOTAL_CAPITAL_PCT=0.60
```

assert effective aggregate cap is 60%.

### Test 14 — malformed aggregate config behavior unchanged

Preserve current validation/error behavior. Do not invent a fail-open parser.

### Test 15 — explicit constructor override wins

If Master Control receives:

```python
max_total_capital_pct=0.50
```

assert effective cap remains 0.50.

Changing the default must not defeat explicit caller authority.

## 11. Production-shaped small-account regression

Use a Jason-sized fixture similar to Aug 25:

```text
equity ~= 1,709
per-position pct = 0.10
per-position budget ~= 170.90
```

Model:

```text
existing exposure ~= 130
```

Assert next position still receives approximately $170.90 of per-position capacity because remaining total capacity is far above that amount.

Then model existing exposure near $1,538. Remaining total should be about $171 and one final approximately 10% position should remain eligible if it fits.

At approximately $1,709 existing exposure, no new position should be eligible.

## 12. Diagnostics requirements

Capital diagnostics must continue to distinguish:

```text
max_position_pct
max_total_capital_pct
per_trade_budget
total_capital_cap
current_total_exposure
remaining_total_cap
selector_budget
```

Do not collapse these into one ambiguous value.

## 13. No false accounting

The 100% aggregate cap does NOT mean:

- allow margin beyond equity;
- invent leverage;
- ignore broker buying power;
- ignore pending submitted entries;
- ignore current exposure;
- submit if Tradier rejects for buying power;
- bypass broker account restrictions.

It means only:

> Angel Precision's internal aggregate exposure policy must stop imposing a 40% ceiling when the intended internal ceiling is 100%.

Broker/account constraints remain authoritative downstream.

## 14. Equality and rounding

Do not add arbitrary tolerance such as 1%, $5 or $20 to force trades through.

Preserve:

```text
individual position <= 10% equity
aggregate exposure <= 100% equity
```

Use existing currency/contract rounding conventions.

## 15. Interaction with PR #514

PR #514 and this PR solve independent layers.

PR #514:

```text
DEFERRED reservation != actual selected contract cost
```

This PR:

```text
per-position limit = 10%
aggregate limit = 100%
```

Do not make this PR depend on #514 implementation internals.

After both land, intended combined flow is:

```text
refresh equity/exposure
-> compute 10% per-position budget
-> compute remaining capacity under 100% aggregate cap
-> selector budget = min(the two)
-> select/materialize real contract
-> actual cost <= 10% per-position cap
-> projected total <= 100% aggregate cap
-> existing submit path
```

## 16. Required code audit before review-ready

Report every location referencing:

```text
DEFAULT_MAX_TOTAL_CAPITAL_PCT
max_total_capital_pct
max_position_pct
max_capital_pct
```

Classify each as:

```text
runtime authority
constructor/caller
configuration override
diagnostic only
test only
dead/legacy
```

This prevents changing one default while another caller quietly pins the old value elsewhere.

## 17. Required test suites

Run at minimum:

- targeted Master Control capital tests;
- `tests/test_p1_capital_diagnostics_clarity.py` if current;
- small-account LIVE affordability/resize tests;
- Master Control hardening tests;
- pending-capital and snapshot-freshness tests;
- exact blocking P0 suite on final head.

If an existing test asserts a 40% default, determine whether it intentionally tests an explicit 40% override or merely encodes the obsolete default. Only the latter should change.

## 18. Acceptance criteria

Implementation is complete only when all are true:

- default per-position remains 10%;
- default aggregate becomes 100%;
- env override remains functional;
- explicit constructor override remains functional;
- existing positions do not reduce each new position's 10% allowance while remaining total capacity >= 10%;
- positions may progress through 50%, 60%, 70%, 80%, 90% and exactly 100% aggregate exposure without aggregate-cap rejection;
- projected exposure above 100% blocks;
- one position above 10% blocks even with unused portfolio capacity;
- selector budget near full deployment becomes the lesser remaining-total value;
- current pending/open exposure remains counted through existing canonical sources;
- no broker submit/cancel code changed;
- no selector threshold changed;
- no watcher behavior changed;
- no exit logic changed;
- LIVE/PAPER identity semantics unchanged;
- diagnostics still expose both caps distinctly;
- targeted tests pass;
- blocking P0 CI passes.

## 19. Rollback

Rollback must remain trivial.

If production behavior indicates unacceptable aggregate deployment pressure, restore:

```text
DEFAULT_MAX_TOTAL_CAPITAL_PCT=0.40
```

through runtime configuration or revert the default change.

No schema migration, order-row rewrite, or position mutation is required.

## 20. Review rubric

HARD HOLD if any of the following appear:

- per-position cap raised above 10%;
- total cap removed rather than set to 100%;
- existing exposure subtracted from per-position cap;
- margin/leverage invented;
- broker buying-power checks bypassed;
- direct Tradier submit added;
- LIVE safety weakened;
- selector quality filters weakened;
- daily loss or max-position controls removed;
- client-specific hardcoding introduced;
- #514 sequencing work mixed into this PR;
- broad unrelated refactor;
- no 40%->50% crossing regression test;
- no exact 100% acceptance / >100% rejection test;
- no explicit override test;
- no audit proving runtime is not still pinned to 0.40 elsewhere.

## 21. Final invariant

```text
10% EACH POSITION
x
UP TO 10 FULL-SIZE POSITIONS
=
100% MAX INTERNAL ACCOUNT DEPLOYMENT
```

subject to every other existing eligibility, quality, risk, account, broker, lifecycle and execution control remaining intact.
