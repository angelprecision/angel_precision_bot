# P0 SPEC: Confirmed-trigger hysteresis + first-attempt direction-reversal rearm

## STATUS

**DRAFT / HARD HOLD. DO NOT MERGE OR DEPLOY AS A FIX UNTIL THE IMPLEMENTATION AND EXACT-HEAD REGRESSIONS ARE PRESENT.**

This branch starts from exact current `main` commit `3ac102dab320007409107ad36cc9494897d2799e` (merged #445). This PR is intentionally narrow. It owns only the final pre-broker LIVE market-truth seam after a trigger has already been confirmed.

## PRODUCTION INCIDENT: JASON / CVS CALL / 2026-08-13

Jason was eligible for the CVS CALL and the watcher confirmed the trigger before contract selection.

Production shape:

- client: `jasoncosby1@gmail.com`
- execution mode: `LIVE`
- signal: `f2341775-853a-42b7-bf69-2c408c346737`
- symbol / side: `CVS CALL`
- trigger: `95.15`
- watcher decision: `trigger_ready`
- trigger confirmation: two-poll breach confirmation
- selected contract: `CVS260821C00095000`
- selected contract quality: spread ~8.5%, OI 1883, volume 838, DTE 8
- final synchronous LIVE underlying quote: bid `95.11`, ask `95.14`, mid `95.125`
- retrace versus trigger: **$0.01 / ~1.05 bps**
- order result: `EXPIRED`
- last error: `live_submit_gate:CALL_NO_LONGER_ABOVE_TRIGGER`
- broker order id: `NULL`
- submitted timestamp: `NULL`

The immediate zero-broker-POST behavior was safe. The lifecycle decision was not. A one-cent retrace after a proven confirmed breach is ordinary quote noise, not evidence that the setup is permanently dead.

There are two defects at the same seam:

1. **No post-confirmation hysteresis.** `check_market_validity_gate()` treats any CALL ask below trigger or PUT bid above trigger, even by one cent, as a direction reversal.
2. **First-attempt reversal is terminalized.** The first final-submit path calls `_terminalize_breach_failure(...)` for any failed final market gate even though `classify_market_truth()` already maps `CALL_NO_LONGER_ABOVE_TRIGGER` / `PUT_NO_LONGER_BELOW_TRIGGER` to `REARM_DIRECTION_REVERSAL`.

## REQUIRED INVARIANT

> A confirmed trigger may tolerate bounded quote noise. A genuine temporary trigger reversal may suppress the immediate broker POST, but must not terminalize an otherwise-valid setup. Only terminal market truth may permanently kill the opportunity.

Required classification:

```text
confirmed trigger -> final fresh LIVE market truth

MICRO_RETRACE_WITHIN_HYSTERESIS -> continue normal submit path
GENUINE_DIRECTION_REVERSAL       -> zero broker POST + clean REARM
STOP_ALREADY_BROKEN              -> terminal
TARGET_ALREADY_COMPLETE          -> terminal
REMAINING_OPPORTUNITY_TOO_SMALL  -> terminal
IDENTITY / QUOTE AUTHORITY FAIL  -> existing fail-closed HOLD/terminal policy; never speculate
```

## CHANGE 1 — BOUNDED POST-CONFIRMATION TRIGGER HYSTERESIS

### Scope

Implement inside the final market-validity classifier, not the watcher trigger itself.

Do **not** lower or move `entry_trigger`. Do **not** change initial breach confirmation. The watcher must still prove the existing trigger using its existing consecutive-poll logic before hysteresis can matter.

### Default tolerance

Use a bounded absolute-plus-relative tolerance:

```python
trigger_hysteresis = max(
    0.02,
    min(abs(trigger_price) * 0.0005, 0.10),
)
```

Meaning:

- 5 bps relative allowance
- minimum 2 cents
- maximum 10 cents

For CVS 95.15 this resolves to ~4.76 cents, so 95.14 is inside the post-confirmation noise band.

The constants may be named module defaults and env-overridable only if the existing config style requires it. Do not create broad runtime knobs unless needed. Safe default behavior must be covered by tests.

### CALL

After a **proven confirmed breach**:

```text
ask >= trigger                              -> normal valid trigger lane
trigger - hysteresis <= ask < trigger       -> MICRO_RETRACE; trigger lane remains valid
ask < trigger - hysteresis                  -> CALL_NO_LONGER_ABOVE_TRIGGER
```

### PUT

Symmetric:

```text
bid <= trigger                              -> normal valid trigger lane
trigger < bid <= trigger + hysteresis       -> MICRO_RETRACE; trigger lane remains valid
bid > trigger + hysteresis                  -> PUT_NO_LONGER_BELOW_TRIGGER
```

### Audit

When hysteresis is used, append explicit audit fields without changing the canonical PASS reason:

```text
confirmed_trigger_hysteresis_applied = true
trigger_hysteresis_amount
trigger_retrace_amount
trigger_retrace_bps
trigger_check_price
trigger_price
```

Do not hide that a retrace occurred.

### Safety ordering

Stop and target checks remain authoritative. Hysteresis must **never** override:

- stop already broken
- target already complete
- remaining opportunity too small
- stale/missing/invalid LIVE quote
- client or execution-mode mismatch
- absolute entry deadline / cutoff

## CHANGE 2 — FIRST-ATTEMPT FINAL-SUBMIT REVERSAL MUST REARM

Current first-attempt behavior effectively does:

```python
if not final_market_result.passed:
    stamp_audit(...)
    _terminalize_breach_failure(...)
    return
```

That is wrong for canonical `REARM_DIRECTION_REVERSAL` authority.

Route the first final-submit market result through the existing `classify_market_truth()` contract before deciding lifecycle outcome.

Required routing:

```text
SUBMIT_VALID                 -> continue existing submit path
REARM_DIRECTION_REVERSAL     -> zero broker POST; clean same-opportunity rearm
TERMINAL_SETUP_COMPLETE      -> existing terminalization path
HOLD_MARKET_TRUTH_UNAVAILABLE-> existing fail-closed no-submit handling; do not reinterpret as valid
```

### Reuse #421

Do **not** invent a second rearm implementation.

Reuse the existing #421 order-state-machine transition `rearm_deferred_materialization_direction_reversal(...)` if and only if current-main identity/ownership preconditions are proven for the uninterrupted first-attempt watcher path.

Expected first-attempt rearm inputs must preserve exact:

- `client_id`
- `execution_mode`
- `signal_id` / canonical signal identity
- `local_order_id`
- current watcher token / owner
- materialization generation
- original absolute validity deadline / entry cutoff

If the existing #421 API cannot safely express the first-attempt ownership shape, add the smallest explicit OSM transition needed. Do not weaken its CAS conditions and do not synthesize watcher authority.

### Rearm state

On genuine direction reversal outside hysteresis:

- zero broker submit
- zero broker cancel
- same canonical opportunity survives
- same local order identity survives unless current-main #421 invariant explicitly requires otherwise
- return to clean pre-breach / pending-trigger lifecycle
- preserve immutable first-breach diagnostics
- clear active trigger confirmation evidence that would allow stale immediate resubmit
- clear stale selector cursor / active retry authority per #421
- require a **new confirmed re-breach** before materialization/submission
- preserve absolute deadline / cutoff; rearm must not extend the opportunity forever

## REQUIRED PRODUCTION FILE BUDGET

Start with exactly:

1. `ap/live_submit_gates.py`
2. `ap_execution_core.py`
3. one focused regression test file
4. `.github/workflows/p0_regression.yml` only to register the focused test

`ap/order_state_machine.py` is permitted **only** if the existing #421 transition cannot safely accept the ordinary first-attempt watcher ownership shape. If touched, justify the exact missing invariant in the PR description.

Do not touch anything else without proving why this seam cannot be fixed inside that budget.

## STRICT NON-GOALS

Do not change:

- scanner admission
- signal score / tier floors
- trigger generation
- watcher confirmation poll count
- contract moneyness / DTE policy
- delta / spread / OI / volume thresholds
- selector ranking
- direct-quote budget
- account sizing / max positions
- entry-efficiency policy (#436)
- cancel-replace continuity (#440)
- post-fill ownership
- exit engine
- positions / proof-trades semantics
- queue fanout
- intelligence

Do not make LIVE market-validity failures behave like PAPER. PAPER's log-only behavior is not the model for this fix.

## REQUIRED REGRESSION: EXACT CVS-SHAPED LIVE REPLAY

Add a focused regression using production-shaped values:

```text
client_id = jasoncosby1@gmail.com
execution_mode = live
symbol = CVS
side = CALL
trigger = 95.15
stop = 93.61
target = 96.69
contract = CVS260821C00095000
qty = 1
final bid = 95.11
final ask = 95.14
fresh synchronous LIVE quote authority
confirmed breach already proven
```

Expected:

- 95.14 is classified inside confirmed-trigger hysteresis
- `CALL_NO_LONGER_ABOVE_TRIGGER` is **not** emitted for this one-cent retrace
- normal final gate chain remains intact
- exactly one broker POST if every other submit gate passes
- exact LIVE client/mode/canonical/local identity preserved

## REQUIRED TEST MATRIX

1. CVS CALL 95.15 trigger / 95.14 ask -> inside hysteresis -> submit permitted if all other gates pass.
2. CALL at exact lower hysteresis boundary -> permitted.
3. CALL one tick outside lower hysteresis boundary -> `REARM_DIRECTION_REVERSAL`, zero broker POST.
4. PUT symmetric micro-retrace -> permitted.
5. PUT outside hysteresis -> rearm, zero broker POST.
6. Hysteresis cannot override CALL/PUT stop-broken terminal truth.
7. Hysteresis cannot override target-complete terminal truth.
8. Hysteresis cannot override remaining-opportunity terminal truth.
9. Stale/unknown LIVE quote -> zero broker POST.
10. Client-id mismatch -> zero broker POST.
11. Execution-mode mismatch -> zero broker POST.
12. First-attempt genuine reversal -> not `EXPIRED`; same canonical setup rearmed.
13. Re-armed setup later reclaims trigger -> requires fresh consecutive watcher confirmation.
14. Re-armed setup then passes selector/final market truth -> at most one broker POST.
15. Restart after rearm -> exact ownership recovered; no orphaned row.
16. Duplicate callback/recovery tick -> no duplicate broker POST.
17. Rearm before fill -> zero positions mutation and zero proof-trades mutation.
18. PAPER identity cannot authorize a LIVE Jason submit.
19. Absolute deadline / entry cutoff survives rearm unchanged.
20. Diagnostics reflect rearm; do not leave contradictory `orders=EXPIRED` while queue/opportunity remain active.

## MONEY-PATH PROOF REQUIRED BEFORE MERGE

Final audit must prove:

- exact broker POST call site unchanged except for the intended classification routing
- no broker POST on genuine reversal
- no broker cancel introduced
- exactly one POST on valid CVS micro-retrace replay
- LIVE/PAPER isolation
- exact client/mode preservation
- restart idempotency
- no position/proof creation before broker fill
- no stale trigger evidence reused after rearm
- no absolute-deadline extension

## MERGE GATE

**HARD HOLD** until:

- implementation is on this branch
- actual diff remains within the justified file budget
- exact CVS replay passes
- focused P0 regressions pass at exact head
- review confirms #436 and #440 are untouched
- no merge or deploy occurs without explicit approval
