# P0 — Canonical deferred-plan ownership for blank contract shapes

## Status

**SPEC-FIRST DRAFT. IMPLEMENTATION REQUIRED. DO NOT MERGE OR DEPLOY YET.**

Created from current `main` after merged PR #524.

Reference main SHA at discovery: `ba1e86a01ed9c81423cc6b5baaa766e43310bec7`.

This PR is a surgical follow-up to #524. It must close one remaining deferred-materialization ownership gap without changing trade eligibility, selector quality, sizing, risk limits, broker behavior, or ordinary entry behavior.

---

## Production problem

PR #524 correctly moved deferred/materialization ownership proof ahead of attempt work and now fences materialization with durable `client_id`, `execution_mode`, order identity, owner, and generation.

However current `ap_execution_core.py` contains two different definitions of whether a plan is still deferred:

1. `APExecutionCore._plan_is_deferred(plan, ticker)` returns deferred when any of the following is true:
   - `metadata.contract_deferred` is true;
   - `contract_symbol` starts with `DEFERRED:`;
   - `contract_symbol` is blank.

2. `_on_entry_trigger()` builds `_deferred_history` before ownership preflight. That classifier recognizes many recovery/deferred history markers and explicit `DEFERRED:` shapes, but does **not** treat a present approved plan with `contract_symbol=""` as deferred solely because the contract is blank.

That means this valid deferred shape can split control flow:

```text
approved plan exists
contract_symbol = ""
contract_deferred marker absent
no recovery/materialization marker yet
```

Current behavior can become:

```text
_deferred_history == false
    -> skip _claim_deferred_materialization_for_trigger()

later:
_plan_is_deferred(plan) == true
    -> enter breach-time selector/materialization path

materialization owner/generation were never established
    -> copyback/terminal ownership cannot be proven
    -> otherwise eligible entry can be held or terminalized
```

This is internally inconsistent. One plan shape cannot be "ordinary" for ownership preflight and "deferred" for selector/materialization.

A post-merge review on #524 identified this exact mismatch.

### Current production evidence

Read-only production review after #524 found that recent LIVE ENTRY rows are currently represented as `DEFERRED:<ticker>` rather than blank-contract rows. No recent blank-contract ENTRY rows were found in the reviewed Monday-through-Thursday window.

Therefore:

- this is **not** the cause of the August 27 zero-trade morning;
- it is a latent P0 correctness hole in an active LIVE path;
- fixing it must not be sold as a trade-count increase or used to justify any eligibility loosening.

---

## Required invariant

> A plan that still requires contract materialization must be classified as deferred consistently before any selector, copyback, terminalization, or broker-facing work.

For an approved plan with an unresolved contract:

```text
blank contract
OR explicit DEFERRED:<ticker>
OR authoritative contract_deferred metadata
OR valid recovery/materialization history
```

must cause the callback to enter the deferred ownership preflight before attempt work.

A real canonical OCC contract must not be reclassified as unresolved merely because stale deferred metadata remains in memory.

The existing real-contract guard in `_plan_is_deferred()` must remain authoritative:

```text
real OCC contract -> not deferred by current contract shape
```

Historical recovery markers may still require the recovery proof path where current #524 expects it. Do not erase those semantics while canonicalizing current plan-shape classification.

---

## Exact implementation seam

Primary production file:

- `ap_execution_core.py`

Primary function:

- `APExecutionCore._on_entry_trigger()`

Relevant helpers/state:

- `APExecutionCore._plan_is_deferred()`
- `_deferred_history`
- `_preflight_plan`
- `_preflight_contract`
- `_claim_deferred_materialization_for_trigger()`
- `_materialization_owned`
- `_mat_owner`
- `_mat_generation`
- `_recovery_pre_claimed`

### Preferred implementation direction

Do **not** create a third independent blank/DEFERRED classifier.

Use the existing canonical plan-shape helper for the approved plan portion of `_deferred_history`, for example conceptually:

```python
_preflight_plan_is_deferred = (
    _preflight_plan is not None
    and self._plan_is_deferred(_preflight_plan, ticker)
)
```

and include that fact in the preflight decision while preserving the existing history/recovery markers.

The final code may be structured differently if tests prove the same invariant, but these two facts must never diverge again:

```text
needs materialization later
==
requires deferred ownership proof first
```

### Important subtlety

Do not simply replace all of `_deferred_history` with `_plan_is_deferred()`.

`_deferred_history` also carries restart/recovery/materialization history that may remain relevant after a real OCC contract has been hydrated. #524 deliberately added durable-state proof around those paths. Preserve those semantics.

The surgical target is the **current approved-plan contract-shape gap**, not a rewrite of deferred recovery.

---

## Live behavior impact

This change **does affect active LIVE behavior** when the exact blank-contract deferred shape occurs.

Expected before/after:

### Before

```text
approved deferred plan with contract_symbol=""
-> ownership preflight may be skipped
-> selector later treats plan as deferred
-> owner/generation missing
-> materialization cannot complete safely
```

### After

```text
approved deferred plan with contract_symbol=""
-> classified deferred before attempt work
-> durable ownership proof/claim required
-> selector allowed only after ownership is proven
-> copyback uses exact owner/generation fence
-> all existing final risk/submit gates remain unchanged
```

This is correctness/throughput preservation, not eligibility expansion.

---

## Hard scope exclusions

This PR must **not** change:

- scanner generation;
- overnight signal count;
- intraday generation;
- score floors;
- trigger levels;
- CALL/PUT breach confirmation rules;
- watcher continuity rules;
- direction arbitration from #512;
- selector moneyness;
- DTE constraints;
- delta constraints;
- spread limits;
- OI/volume limits;
- direct quote validity rules from #509;
- selector request/retry taxonomy from #504;
- affordability thresholds;
- per-position budget percentages;
- total exposure limits;
- max positions;
- daily loss controls;
- intelligence authority;
- broker price/ladder behavior;
- Tradier submit/cancel methods;
- fill monitor behavior;
- position mutation;
- `proof_trades` mutation;
- exit engine behavior;
- reconciler behavior;
- LIVE/PAPER taxonomy.

Do not add a direct broker submit or cancel call.

Do not bypass Master Control.

Do not bypass OSM.

Do not introduce a fallback that permits unowned materialization "for compatibility."

If ownership cannot be proven, the path must continue to fail closed / KEEP_WATCHER exactly as #524 intends.

---

## Identity invariants

Every test and implementation path must preserve exact:

- `client_id`;
- `execution_mode`;
- `local_order_id`;
- `signal_id` where required by current #524 proof;
- materialization owner;
- materialization generation;
- expected direction;
- exact OCC contract on broker-ready terminal/copyback fences.

`execution_mode` remains only canonical `live` / `paper` under existing durable authority rules.

Malformed, contradictory, or unprovable durable mode remains fail-closed.

No PAPER row may satisfy a LIVE ownership claim and vice versa.

---

## Required regression tests

Add a dedicated regression file, suggested name:

`tests/test_p0_deferred_blank_contract_ownership.py`

Register it in `.github/workflows/p0_regression.yml`.

At minimum prove all of the following.

### A. Blank approved plan is preflight-deferred

Production shape:

```python
approved_plan.contract_symbol = ""
approved_plan.metadata = {}
execution_mode = "live"
```

Assert:

- deferred ownership preflight is entered;
- `_claim_deferred_materialization_for_trigger()` is called before selector work;
- selector is not called if claim fails;
- broker submit is not called if claim fails.

### B. Explicit `DEFERRED:<ticker>` remains unchanged

Example:

```text
DEFERRED:SPY
```

Assert the existing #524 ownership preflight still occurs and behavior is unchanged.

### C. Metadata deferred marker remains unchanged

Example:

```python
contract_symbol = ""
metadata = {"contract_deferred": True}
```

Assert ownership preflight occurs exactly once.

### D. Real OCC ordinary plan does not become deferred

Example canonical OCC:

```text
SPY260828C00650000
```

With no recovery markers, assert:

- `_plan_is_deferred()` is false;
- deferred ownership claim is not entered;
- ordinary current-main behavior remains unchanged.

### E. Real OCC plus stale `contract_deferred` marker

Prove the real-contract guard still prevents stale memory metadata from reopening ordinary materialization solely from current plan shape.

If separate valid recovery history requires a recovery proof path under current #524 semantics, test that separately and preserve it.

### F. Durable row unavailable

For blank-contract deferred plan where the durable row cannot be read:

- KEEP_WATCHER / existing ownership-unproven reason;
- zero selector attempt;
- zero broker submit;
- zero terminal mutation without authority.

### G. Client mismatch

Memory client and durable order client differ:

- ownership not granted;
- zero selector;
- zero broker work.

### H. LIVE/PAPER mismatch

Memory mode `live`, durable mode `paper`, and mirror case:

- ownership not granted;
- zero selector;
- zero broker work.

### I. Malformed durable mode

Malformed or contradictory `orders.execution_mode` / metadata:

- fail closed;
- no materialization work.

### J. Successful blank-contract claim -> materialization

Exact happy-path test:

```text
blank approved contract
-> durable deferred row proven
-> owner/generation claimed
-> selector returns canonical OCC
-> copyback succeeds with same owner/generation
-> existing actual-cost revalidation executes
```

Assert no extra broker submit surface is introduced.

### K. Claim race / duplicate owner

When another valid materialization owner already holds the row:

- callback parks/keeps watcher under existing #524 semantics;
- no duplicate selector/materialization;
- no broker submit.

### L. Restart/recovery parity

A recovered blank-contract plan must follow the same ownership invariant and may not bypass durable proof because it came from restart state.

---

## Required source-level guards

Add assertions proving this PR does not introduce:

- direct `.submit_order(` calls in the modified execution-core seam;
- direct `.cancel_order(` calls in the modified execution-core seam;
- new writes to `positions`;
- new writes to `proof_trades`;
- new selector thresholds/constants;
- new risk thresholds/constants.

Where possible, compare counts or source slices against base to make accidental scope creep obvious.

---

## Required verification before review-ready

Codex must run and report:

1. the new regression file;
2. existing #524 deferred lifecycle tests;
3. `tests/test_p0_seam4_e2e_deferred_lifecycle.py`;
4. `tests/test_p0_deferred_due_retry_ownership.py`;
5. `tests/test_p0_osm_broker_submit_boundary.py`;
6. relevant #514/#524 named-shape tests;
7. the full P0 Regression Suite on the **exact final head SHA**.

No "tests should pass" language. Include exact commands, counts, and final SHA.

---

## Required Codex completion report

Before marking review-ready, post all of the following to the PR:

1. exact final head SHA;
2. exact changed production files;
3. exact changed test/workflow files;
4. before/after control flow for blank contract;
5. proof that explicit `DEFERRED:` behavior is unchanged;
6. proof that real OCC ordinary behavior is unchanged;
7. proof `client_id` is preserved;
8. proof `execution_mode` is preserved;
9. proof no broker submit/cancel surface was added;
10. proof no positions / `proof_trades` / queue eligibility mutation was added;
11. focused test output;
12. exact-head P0 CI result;
13. `git diff main...HEAD --stat`;
14. concise adversarial diff audit listing every production behavior change.

---

## Merge gate

**HARD HOLD** until implementation and exact-head verification prove:

- blank contract and explicit deferred contract use the same pre-attempt ownership invariant;
- no selector/materialization begins without ownership proof;
- failed proof never reaches broker-facing work;
- real OCC ordinary entries are unchanged;
- recovery/restart remains fail-closed;
- LIVE/PAPER identity remains exact;
- no eligibility or risk threshold is loosened.

Expected final verdict after successful implementation and review: **MERGE**.
