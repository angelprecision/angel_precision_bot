# P0 SPEC — Revalidate deferred entries only after real contract materialization

## Status

**SPEC ONLY / HARD HOLD. DO NOT MERGE OR DEPLOY AS AN IMPLEMENTATION.**

Base SHA: `b43ce9c53433bd0479baa87e9757b50740adaa01`

This PR is a surgical Codex work order. The branch must remain spec-only until Codex applies the production patch, the exact diff is reviewed, focused tests pass, and exact-head CI is green.

## One job

Stop a `DEFERRED:<ticker>` entry from treating its reserved account budget as if it were the actual selected option-contract cost before a real OCC contract exists.

The correct sequence is:

`breach -> kill switch/slot checks -> deferred selector -> real OCC + real qty + real executable price -> actual order cost -> final Master Control exposure revalidation -> existing submit gates -> broker`

Never:

`breach -> DEFERRED placeholder budget interpreted as real contract cost -> false capital reject -> expire valid setup`

## Production evidence / incident shape

Observed Jason LIVE production rows carry this legitimate deferred shape before breach materialization:

```text
contract = DEFERRED:<ticker>
limit_price = 0.01
qty = 1
reserved_cost = <account budget>
meta.max_position_usd = <account budget>
meta.contract_deferred = true
```

Examples observed on August 10/14 included account-budget values around `$170.435`, `$165.886`, and `$164.91`.

Those values are **reservation/selector budget authority**, not proof that a `$1.70435`, `$1.65886`, or `$1.64910` option contract has been selected.

The current breach path calls `master_control.revalidate_exposure(approved_plan)` before `_on_entry_trigger()` reaches deferred contract materialization. `revalidate_exposure()` consumes `plan.max_position_usd` as real selected-contract cost. A small equity/budget drift can therefore reject the placeholder itself as `ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP` before contract selection ever runs.

## Exact current-main code path

Line numbers below are pinned to base SHA `b43ce9c53433bd0479baa87e9757b50740adaa01`. Function anchors are binding if line numbers move after Codex edits.

### Production file 1 — `ap_execution_core.py`

1. **`_breach_risk_check()` — starts around line 2031.**
   - Keep kill-switch checks.
   - Keep open-position / pending-entry slot checks.
   - Current problematic exposure revalidation block is approximately lines **2149–2203**:

```python
approved_plan = self._recover_plan_for_revalidation(watched)
...
reval = self.master_control.revalidate_exposure(
    approved_plan,
    client_id=self.email or "default",
)
```

2. **`_on_entry_trigger()` — starts around line 3700.**
   - Current call to `_breach_risk_check(watched)` is approximately lines **3920–3948**.
   - Do not remove this call. It must continue to own kill-switch and slot protection.

3. **Deferred classification/materialization — approximately lines 4370+ and 5220–6460.**
   - `_deferred` is already derived from `contract_deferred`, missing contract, or `DEFERRED:` prefix.
   - Selector call and validated result copy-back are around **5220–5390**.
   - Current copy-back sets `contract_symbol`, `limit_price`, `contracts`, and conditionally `max_position_usd`.
   - A correct actual-cost formula already exists later in this same path as:

```python
qty * limit_price * 100
```

4. **Final pre-submit work begins after deferred materialization, before Step 4 / quote refresh around line 6460+.**
   - Final Master Control revalidation for a deferred entry must happen after actual materialization and before any broker ENTRY POST.

## Required implementation

### A. Separate pre-materialization budget from real contract cost

Inside `_breach_risk_check()`, after recovering `approved_plan`, determine whether the plan is still deferred using only existing production truth:

```python
meta = getattr(approved_plan, "metadata", None) or {}
contract = str(getattr(approved_plan, "contract_symbol", "") or "").strip()
is_deferred = bool(
    (isinstance(meta, dict) and meta.get("contract_deferred"))
    or not contract
    or contract.upper().startswith("DEFERRED:")
)
```

If `is_deferred`:

- **DO NOT** call `master_control.revalidate_exposure()` yet.
- **DO** preserve all existing kill-switch and position-slot checks.
- Return success from the capital-cost portion so `_on_entry_trigger()` can reach the already-existing deferred selector.
- Do not modify the budget, cap, qty, selector thresholds, or order row merely to make this pass.

This is not a risk bypass. There is no actual contract cost to validate yet.

### B. Make actual selected cost deterministic

After `_validate_deferred_selector_result()` proves:

- real OCC contract,
- positive executable per-share price,
- positive affordable quantity,

copy back the real cost unconditionally from those validated values:

```python
actual_contract_cost = round(
    float(_sel_price_candidate) * int(_sel_qty_candidate) * 100.0,
    2,
)
approved_plan.max_position_usd = actual_contract_cost
```

Do **not** depend on optional `selection.premium_per_contract` being present/nonzero to replace the placeholder budget.

If the acceptance-cap block intentionally clamps quantity to 1, recompute actual cost after that clamp from the final qty and executable selected price before final revalidation.

### C. Final capital authority still wins

For deferred entries only, after the real OCC contract / qty / price are materialized and after any allowed qty clamp, but **before quote-refresh submit gates or broker POST**, run the existing authority:

```python
final_reval = self.master_control.revalidate_exposure(
    approved_plan,
    client_id=_breach_client_id or self.email or "default",
)
```

Required behavior:

- `final_reval.ok == True` -> continue into the unchanged canonical submit stack.
- `final_reval.ok == False` -> terminalize through the existing deferred breach failure helper with the exact Master Control reason; zero broker POST.
- exception in LIVE -> fail closed through existing terminalization; zero broker POST.
- do not alter `max_position_pct`, total-cap math, or #155 per-position-cap semantics.

### D. Non-deferred behavior is frozen

For a plan that already carries a real OCC contract before breach:

- existing `_breach_risk_check()` Master Control exposure revalidation remains in its current place;
- do not add a second revalidation;
- do not change quote refresh, spread, drift, intelligence, entry confirmation, broker intent, or submit behavior.

Hydrated-prebreach rows retain their current dedicated hydration revalidation behavior. Do not duplicate it.

## HARD FILE BUDGET

### Production

**Maximum one production file:**

- `ap_execution_core.py`

If Codex believes another production file is required, **STOP and report the exact reason. Do not edit it.**

### Tests

Create only:

- `tests/test_p0_deferred_real_cost_revalidation.py`

Existing adjacent tests may be run unchanged. Do not rewrite unrelated tests to make the patch pass.

## Required tests

At minimum prove all of the following with production-shaped objects/data:

1. **Jason placeholder replay**
   - `contract="DEFERRED:UNP"`
   - `limit_price=0.01`
   - `reserved_cost/max_position_usd=165.886`
   - `contract_deferred=True`
   - fresh authoritative budget may be slightly below the old reservation
   - pre-materialization breach check must not classify `$165.886` as actual option cost and must not emit `ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP` from the placeholder.

2. **Affordable materialization succeeds**
   - selector returns a real OCC contract, price `1.42`, qty `1`
   - `approved_plan.max_position_usd == 142.00` before final MC call
   - final MC sees the real `$142` cost
   - at most one broker ENTRY POST can occur through the existing submit path.

3. **Actual contract too expensive still blocks**
   - selector returns a real OCC contract whose actual `price * qty * 100` exceeds final authoritative per-position budget
   - final MC rejects with the existing cap reason
   - zero broker POST
   - no risk threshold is relaxed.

4. **Optional `premium_per_contract` absent/zero**
   - validated selector price is still sufficient to compute actual cost
   - placeholder budget cannot survive into final MC revalidation.

5. **Qty clamp recomputes cost**
   - if the existing acceptance mode clamps qty, actual cost matches final qty exactly.

6. **Real preselected contract unchanged**
   - non-deferred real OCC plan still performs the original breach exposure revalidation exactly once
   - no extra selector call
   - no duplicate MC call.

7. **Identity preservation**
   - exact `client_id` preserved
   - exact `execution_mode` (`live|paper`) preserved
   - same `signal_id`, `canonical_signal_id`, and `local_order_id`
   - no PAPER/LIVE cross-contamination.

8. **Mutation safety**
   - zero position creation before broker fill
   - zero `proof_trades` mutation
   - zero new queue writer
   - no new broker cancel path
   - no broker POST on any block/error path.

9. **Diagnostics**
   - final MC block preserves exact reason downstream
   - deferred materialization diagnostics remain present; do not replace specific selector/MC reasons with generic `breach_risk_check_false` when the real failure is known.

## Mandatory adjacent regression commands

Codex should run the focused test plus the existing nearby safety suites that cover deferred materialization, cap behavior, and submit identity. At minimum:

```bash
python -m pytest -q \
  tests/test_p0_deferred_real_cost_revalidation.py \
  tests/test_breach_block_diagnostics.py \
  tests/test_p0_deferred_breach_submit_repair.py \
  tests/test_p0_acceptance_cap_and_materialization_outcome.py \
  tests/test_risk_cap_split.py \
  tests/test_master_control_hardening.py
python -m py_compile ap_execution_core.py tests/test_p0_deferred_real_cost_revalidation.py
git diff --check
```

If an adjacent test name has moved, use the current equivalent without editing unrelated behavior.

## Frozen non-goals

Do not change:

- scanner logic or score thresholds;
- trigger logic or watcher confirmation count;
- delta, DTE, moneyness, spread, OI, volume, premium or earnings gates;
- risk percentages, per-position cap, total-cap formula or max positions;
- selector ranking;
- retry counts or retry taxonomy;
- order monitor;
- broker adapter;
- broker submit/cancel implementation;
- exits;
- positions;
- `proof_trades`;
- queue lifecycle;
- intelligence behavior;
- database schema or migrations;
- environment variables;
- PAPER/LIVE routing.

## Safety invariants

- `contract too expensive` remains a valid block **after** a real contract exists.
- `deferred budget placeholder` is never equivalent to `actual contract cost`.
- final authoritative capital state always wins.
- no risk relaxation is authorized.
- no new broker path is authorized.
- no new order is created at breach; materialize the existing lifecycle only.
- exact `client_id` and `execution_mode` must survive every seam.
- a blocked entry cannot create a position or proof trade.

## Codex implementation instruction

Implement only the job above against this PR branch.

Before editing, inspect the current exact functions named in this spec. Do not copy a historical PR wholesale. Do not revive Aug 11–13 runtime complexity. If current code already closes any subcase, leave that subcase alone and add only the missing invariant.

After implementation, report:

1. exact production lines changed;
2. exact behavioral before/after;
3. broker POST/cancel exposure;
4. orders/positions/proof_trades/queue mutation exposure;
5. identity preservation proof;
6. focused test results;
7. full changed-file list.

**No merge, deploy, migration, environment mutation, or LIVE authority change is authorized by this spec.**