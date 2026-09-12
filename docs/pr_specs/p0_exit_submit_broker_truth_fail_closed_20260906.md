# P0 SPEC: fail closed exit submission on unavailable broker truth

## STATUS

**SPEC ONLY / HARD HOLD / IMPLEMENTATION REQUIRED. DO NOT MERGE OR DEPLOY.**

Base authority for this spec:

- repository: `angelprecision/angel_precision_bot`
- current committed main: `eb1fdefd8fb35effd1752a8a4de50147c06b066b`
- audit date: 2026-09-06
- defect is active on committed main

This PR must remain surgical. It fixes one money-path fail-open seam in the exit engine. It must not redesign exit policy, add new broker authority, or broaden emergency behavior.

---

## PRODUCTION DEFECT

Current main explicitly allows normal autonomous EXIT execution to continue when authoritative broker-position truth is unavailable or malformed.

The defect exists at two caller boundaries.

### A. `_broker_position_precheck()` can fail but `_check_all_positions()` does not make that failure authoritative

Current main behavior in `ap_exit_engine.py`:

```text
_broker_position_precheck()
-> resolve_authoritative_broker_positions(...)
-> broker truth unavailable / malformed
-> self._broker_truth_snapshot_unavailable = True
-> return False
```

The caller then does:

```text
_check_all_positions()
-> self._broker_position_precheck()
-> ignores False result
-> only catches raised exceptions
-> continues evaluating local engine positions
```

The current source comment explicitly says:

```text
EXIT_UNSAFE_BROKER_TRUTH_UNAVAILABLE is logged if check fails — execution continues.
```

That means the safety precheck is diagnostic rather than authoritative.

### B. The final submit seam also continues when exact broker truth or the safety guard is unavailable

Current main in `_submit_exit_decision()` does all of the following:

```text
resolve_exit_broker_truth(...)
-> broker truth unavailable / malformed / UNKNOWN
-> emit degraded critical diagnostic
-> log "continuing with canonical exit callback"
-> continue
```

and:

```text
evaluate_exit_submission_safety(...)
-> raises
-> log "exit pre-submit guard unavailable; continuing with callback submit"
-> continue
-> invoke on_exit/on_scale callback
```

This is a direct fail-open money-path seam.

A broker outage, malformed response, identity-normalization failure, or guard exception can therefore leave the normal callback reachable using local position state.

---

## REQUIRED INVARIANT

For normal autonomous exit execution:

```text
broker truth unavailable
OR broker truth malformed
OR broker truth identity-ambiguous
OR broker truth quantity UNKNOWN
OR broker safety guard raises/unavailable

-> HOLD this position for this evaluation cycle
-> callback not invoked
-> broker EXIT POST count = 0
-> new broker cancel count = 0
-> no position close/quantity/economic mutation caused by this path
-> no proof_trades mutation
-> no queue/result mutation
-> diagnostics preserved
```

Unknown broker truth is not permission to submit.

The safe distinction must remain:

```text
AVAILABLE + exact OPEN long quantity -> normal exit path may continue
AVAILABLE + exact FLAT -> existing broker-flat handling, zero duplicate sell
UNAVAILABLE / MALFORMED / AMBIGUOUS / UNKNOWN -> HOLD, zero callback
```

---

## AUTHORITY AUDIT

Implementation must trace and preserve these authorities independently.

### Broker-position truth

Authority must include:

- exact account identity;
- exact OCC contract identity;
- exact long/flat classification;
- finite integral quantity where quantity is required;
- successful authoritative retrieval;
- malformed/unavailable state preserved distinctly from empty/flat state.

### Execution identity

The fail-closed gate must preserve:

- `client_id`;
- normalized `execution_mode`;
- `position_id`;
- exact OCC contract;
- existing callback identity / local order identity rules;
- existing duplicate-submit fences.

### Local state

Local engine state may remain useful for diagnostics and strategy evaluation, but it may not substitute for unavailable broker authority at the final normal submit boundary.

---

## REQUIRED CALLER TRACE

Do not patch a helper in isolation. The implementation review must show this exact path:

```text
_check_all_positions()
-> _broker_position_precheck()
-> authoritative broker truth classification
-> position evaluation
-> _submit_exit_decision()
-> resolve_exit_broker_truth()
-> quantity/identity classification
-> evaluate_exit_submission_safety()
-> action gate
-> on_exit/on_scale callback
-> downstream OSM/broker submit
```

For every changed function report:

```text
caller
-> validation
-> state read
-> authority resolution
-> mutation
-> return classification
-> downstream consumer
```

The fix is incomplete if one caller still treats unavailable truth as non-blocking.

---

## REQUIRED IMPLEMENTATION BEHAVIOR

### 1. `_broker_position_precheck()` failure must be consumed

If the precheck returns false or classifies the snapshot unavailable/malformed, `_check_all_positions()` must not allow normal autonomous exit callbacks for affected positions in that cycle.

Do not merely set `_broker_truth_snapshot_unavailable` and continue.

If the flag remains, it must be an enforced authority input rather than dead diagnostics.

### 2. Final submit seam must fail closed

In `_submit_exit_decision()`:

```text
not fresh exact
OR quantity None
OR BrokerPositionTruth.UNKNOWN
```

must return HOLD/False before callback invocation.

The current behavior text "continuing with canonical exit callback" must disappear from normal autonomous execution.

### 3. Safety-guard exceptions must block callback

`evaluate_exit_submission_safety(...)` raising or becoming unavailable must not authorize the callback.

Required:

```text
guard exception
-> emit diagnostic
-> clear only safe transient in-flight state as appropriate
-> return False
-> callback count = 0
```

Do not let exception handling silently broaden money-path authority.

### 4. Preserve exact broker-flat behavior

A successful complete authoritative snapshot proving exact broker flat must remain distinct from unavailable truth.

Do not convert flat to unavailable.

Do not convert unavailable to flat.

The existing duplicate-sell protection must remain intact.

### 5. Preserve exact broker-open behavior

Positive control must remain valid:

```text
exact broker OPEN
+ exact positive integral long qty
+ requested exit qty <= broker qty
+ all existing exit guards pass
-> one existing canonical callback may execute
```

The PR must not globally suppress exits or reduce legitimate risk-reducing trade flow.

### 6. Emergency / operator authority must stay separate

If any explicit emergency or operator-authorized flatten path intentionally has a different authority contract, keep it separate and prove it independently.

Do not make a normal autonomous callback an "emergency" path to bypass this guard.

No new emergency authority may be added in this PR.

---

## FAILURE-TIMING MATRIX

Execute behavior around these boundaries:

```text
before broker truth fetch
broker fetch raises
broker fetch returns malformed envelope
broker fetch returns malformed row
broker fetch returns exact OPEN
broker fetch returns exact FLAT
after truth classification but before safety guard
safety guard raises
after safety guard pass but before callback
callback invoked once on valid path
```

A process failure or exception at any pre-callback safety boundary must never convert UNKNOWN into permission to submit on retry.

---

## REQUIRED BEHAVIORAL TESTS

Structural source inspection is secondary only.

### Fail-first reproduction

On unpatched main, prove at least one current failing case where:

```text
valid local ManagedPosition
+ valid exit decision
+ broker truth unavailable
-> callback is reached
```

The test must fail before the implementation and pass after it.

### Mandatory negative cases

1. broker position transport exception -> callback 0.
2. malformed broker envelope -> callback 0.
3. malformed broker position row -> callback 0.
4. missing/ambiguous exact OCC identity -> callback 0.
5. non-finite/fractional/invalid broker quantity -> callback 0.
6. exact broker truth state UNKNOWN -> callback 0.
7. `evaluate_exit_submission_safety()` exception -> callback 0.
8. unavailable safety module/import -> callback 0 if that can occur in runtime.
9. `_broker_position_precheck()` returns False without raising -> later callback 0.
10. `_broker_truth_snapshot_unavailable=True` -> later callback 0.

### Positive controls

11. exact broker OPEN qty=1, requested qty=1, valid guard -> callback exactly 1.
12. exact broker OPEN qty=2, requested qty=1 -> callback exactly 1.
13. exact broker FLAT -> callback 0 and existing flat-handling path remains authoritative.
14. exact broker OPEN qty=1, requested qty=2 -> callback 0.
15. existing duplicate/in-flight EXIT fence still prevents duplicate callback.

### LIVE/PAPER and identity controls

16. LIVE position cannot consume PAPER broker identity.
17. wrong account cannot authorize callback.
18. wrong OCC cannot authorize callback.
19. wrong/missing client identity remains fail closed.
20. execution-mode contradiction remains fail closed.

---

## MONEY-PATH ASSERTIONS

For every fail-closed case assert all of the following, not merely a returned False:

- `on_exit` count = 0;
- `on_scale` count = 0;
- broker submit/POST count = 0;
- broker cancel count = 0 unless an already-existing separately authorized cleanup path is the explicit subject of the test;
- no new order mutation generated by this path;
- no position quantity/economics mutation;
- no `proof_trades` write;
- no queue/result write;
- diagnostics retain reason and identity.

For valid OPEN positive controls:

- callback count = exactly 1;
- no duplicate submit authority introduced.

---

## TEST QUALITY REQUIREMENTS

Do not accept tests that pass because the callback was never wired.

Every negative callback test must have a nearby positive control using the same fixture proving the callback can be reached under valid broker truth.

Mocks must preserve the actual production result shapes of:

- `resolve_authoritative_broker_positions()`;
- `resolve_exit_broker_truth()`;
- `BrokerPositionTruth` classification;
- `evaluate_exit_submission_safety()`.

Do not replace the entire exit engine with a stub that bypasses the real action gate.

At least one behavioral test must execute:

```text
_check_all_positions()
-> evaluate_exit()
-> _submit_exit_decision()
-> callback gate
```

or the closest production-equivalent full caller path available in the existing suite.

---

## SCOPE

Preferred production scope:

- `ap_exit_engine.py`
- `ap/exit_safety.py` only if an authority return contract must be clarified

Tests:

- focused exit-engine broker-truth/action-gate regressions
- P0 workflow registration if new test file is added

Do not modify:

- scanner/selector/score/sizing/risk policy;
- entry execution;
- broker submit implementation;
- broker cancel policy;
- reconciler quantity parsing (# separate P0);
- manual-close broker snapshot parsing (# separate P1);
- proof taxonomy;
- queue semantics;
- protective exit strategy thresholds.

---

## RELEASE GATE

**HARD HOLD** until all are true:

1. fail-first proves current callback reachability under unavailable broker truth;
2. unavailable/malformed/UNKNOWN broker truth blocks normal callbacks;
3. safety-guard exceptions block normal callbacks;
4. exact OPEN positive control still submits through the existing canonical path exactly once;
5. exact FLAT still blocks duplicate sell without being confused with unavailable truth;
6. no new submit/cancel authority appears in the diff;
7. client/mode/account/OCC identity remains exact;
8. exact-head PostgreSQL-backed P0 CI is green;
9. full final diff and review threads receive independent MERGE audit.

Do not merge or deploy this spec PR. Implementation must be committed to this branch or a replacement implementation branch and re-audited from the final exact head.