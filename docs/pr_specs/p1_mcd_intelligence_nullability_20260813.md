# P1 SPEC: Eliminate MCD intelligence nullable-comparison crash

## STATUS

**P1 / DRAFT / HOLD / CURRENT-MAIN IMPLEMENTATION WORK ORDER**

Base: `main@3ac102dab320007409107ad36cc9494897d2799e`.

This PR owns one defect only: the 2026-08-13 MCD intelligence exception:

`[MCD] Intel error ('<=' not supported between instances of 'int' and 'NoneType') — data-collection override`

The incident reproduced twice in PAPER runtime. Do not bundle historical order cleanup, max-position provenance, WATCHING readiness, selector changes, entry policy, or exit policy into this PR.

## IMPORTANT CURRENT-MAIN TRACE

`intelligence_bridge.py` itself has typed scalar helpers (`_safe_float`, `_safe_int`) and its visible `<=` comparisons are already guarded or against local numeric constants. The production exception therefore must be reproduced with a traceback before deciding whether the failing expression lives in the bridge, an `ap_intelligence` pipeline component, a risk/sizing helper, or a producer object reached by the bridge.

Do **not** grep for one suspicious comparison and patch it blindly.

## PRODUCTION BEHAVIOR

Observed MCD flow:

- Master Control approved the setup path at scanner/MC thresholds.
- Intelligence execution raised `TypeError: '<=' not supported between instances of 'int' and 'NoneType'`.
- PAPER/runtime used the existing data-collection unavailable/error policy and continued non-authoritatively.
- Current policy comments state LIVE defaults fail-closed for intelligence unavailable/timeout/error unless an explicit data-collection override is deliberately enabled.

The fix must preserve that policy split. A malformed intelligence field must never become favorable execution authority.

## REQUIRED ROOT-CAUSE PROCEDURE

### Phase 0: reproduce exact current-main exception

Build a production-shaped MCD fixture from the runtime payload and execute the exact `run_intelligence_check()` / Gate-G route used by Master Control.

Capture a full traceback in test/dev. Do not change runtime exception policy merely to obtain it.

The reproduction must identify:

1. exact file and line;
2. exact left operand and source field;
3. exact right operand and source field/config;
4. Python types of both operands;
5. which producer supplied `None`;
6. whether `None` means missing, unavailable, not-yet-computed, invalid, or a legitimate optional value;
7. whether the value is pre-decision point-in-time truth or derived/advisory data;
8. PAPER versus LIVE behavior at the same malformed boundary.

If the exact failure cannot be reproduced, first add bounded diagnostic context around the exception owner and HOLD. Do not invent a default.

### Phase 1: establish the typed contract at the producer/consumer seam

The repair belongs at the narrowest place where the semantic contract is known.

Preferred pattern:

- external/raw value -> explicit parser/validator -> typed optional value + status/reason -> consumer decision.

Do not use:

- `value or 0` when zero is semantically meaningful;
- arbitrary numeric defaults for missing evidence;
- `try/except TypeError: pass`;
- broad exception swallowing;
- a default that makes an unavailable field look favorable;
- a LIVE-only special case that hides malformed input.

For every nullable numeric field involved, classify at least:

- missing (`None`);
- blank string;
- boolean;
- unparsable string;
- NaN;
- positive/negative infinity;
- valid zero;
- valid positive/negative finite numeric;
- valid integral versus fractional value if an integer is required.

### Phase 2: define missing-data semantics explicitly

For the exact field that caused the crash, choose one explicit outcome based on its actual role:

- `UNAVAILABLE`: cannot make an authoritative decision;
- `ADVISORY_DATA_UNAVAILABLE`: canonical downstream authority can decide without this local estimate;
- `SKIP`: feature/profile not applicable;
- deterministic neutral value **only if** the domain contract truly defines one;
- hard validation error for corrupted impossible values.

Document why the chosen semantics are correct. Missing evidence is not automatically zero, false, or favorable.

### Phase 3: preserve Gate-G mode policy

Prove the same malformed payload has mode-correct behavior:

#### PAPER / research

May continue only under the already-reviewed data-collection/unavailable policy. The returned intelligence status/reason must clearly show that the result was unavailable/error-derived and not a successful approval.

#### LIVE

Default behavior remains fail-closed on this intelligence error/unavailability unless the existing explicit operator override is intentionally enabled.

Do not enable `INTEL_DATA_COLLECTION_OVERRIDE` in code, environment examples, tests, or defaults.

### Phase 4: improve diagnostics without leaking data

The current log gives only the exception message. Add enough bounded context at the exact owner to make a recurrence actionable:

- ticker;
- client id (or existing safe identifier);
- execution mode;
- intelligence profile/stage;
- stable field/reason code;
- operand validation statuses, not arbitrary full objects;
- commit SHA / run id if already available.

Do not log credentials, full account data, full option-chain payloads, or large signal blobs.

If a traceback is emitted in production, use normal logger exception handling and ensure it contains no sensitive request objects.

## EXPECTED FILE BUDGET

Start with the exact failing owner only after reproduction.

Likely candidates:

- `intelligence_bridge.py` if the consumer boundary is actually there;
- one file under `ap_intelligence/` if the traceback proves the failing comparison lives there;
- one narrow regression test file;
- optional shared numeric validator only if the same exact semantic field is consumed in more than one location.

Do not change `ap_master_control.py` unless reproduction proves it passes an invalid contract that cannot be corrected at the intelligence seam.

Do not touch:

- `ap/preopen_readiness.py` (#449);
- startup phantom cleanup (#451);
- `ap/fill_monitor.py` historical identity cleanup;
- selector/quote ranking;
- position sizing policy;
- risk thresholds;
- broker adapters;
- order/position/proof lifecycle;
- exit logic.

## REQUIRED REGRESSIONS

Minimum matrix for the exact failing field/path:

1. exact production-shaped MCD payload reproduces current-main TypeError before fix.
2. same payload after fix returns deterministic typed unavailable/valid outcome, no TypeError.
3. `None`.
4. blank string.
5. boolean `False`.
6. boolean `True`.
7. unparsable string.
8. `NaN`.
9. `+inf`.
10. `-inf`.
11. integer zero.
12. float zero.
13. valid positive integer.
14. valid positive float if allowed.
15. negative numeric where invalid.
16. fractional numeric where integer required.
17. missing producer container/object.
18. producer exception.
19. timeout/unavailable behavior unchanged.
20. PAPER malformed field -> existing data-collection/unavailable status, not authoritative success.
21. LIVE malformed field -> fail-closed by default.
22. LIVE explicit collection override -> only the already-reviewed override behavior; diagnostic still says unavailable/error.
23. malformed one field cannot be masked by another candidate's good evidence if the field is candidate-scoped.
24. one candidate's bad optional evidence cannot veto another candidate if current architecture defines candidate-scoped reduction.
25. no synthesized selected-contract evidence.
26. zero remains distinguishable from missing.
27. scanner score 70 path remains semantically unchanged when all intelligence fields are valid.
28. existing Gate-G production-shape tests remain green.
29. current #434/#438-related intelligence tests remain green where present.
30. exact-head P0 workflow remains green.

## MONEY-PATH / DATA-PROVENANCE INVARIANTS

- broker submit/cancel: untouched;
- orders/positions/proof/queue: untouched;
- risk/score thresholds: untouched;
- client/mode taxonomy: exact;
- no future/outcome data introduced;
- missing values stay missing/unavailable unless the semantic contract explicitly defines a neutral value;
- LIVE malformed intelligence cannot become favorable authority;
- PAPER collection result cannot be mislabeled as LIVE-valid approval.

## NON-OVERLAP / PR COORDINATION

- #448 is the umbrella Aug-13 runtime-integrity tracker. This PR should become the surgical child owner for its MCD nullable-comparison workstream.
- #450 owns the forward legacy `orders.execution_mode` write-path correction.
- #449 owns Jason LIVE WATCHING readiness.
- #451 owns startup phantom cleanup result shape.
- #438 owns broader intelligence promotion/profitability policy. Do not rebuild that here.

After this PR is opened, update #448 to point here and remove duplicate implementation from the umbrella branch.

## CODEX FINAL HANDOFF

Codex must update the PR with:

- exact pre-fix traceback;
- exact nullable field and producer;
- semantic reason why missing should map to the chosen outcome;
- final files changed;
- focused test count;
- adjacent Gate-G/intelligence test count;
- exact-head CI links;
- broker/DB mutation audit (`none` expected);
- PAPER/LIVE behavior table;
- fresh `MERGE / HOLD / HARD HOLD` verdict.

Do not merge or deploy from the implementation task.

## CURRENT VERDICT

**HOLD pending exact traceback and implementation.** The current system fails closed/non-authoritatively depending on mode, which is preferable to silently approving bad data, but a production intelligence path must not crash on a legitimate nullable field.