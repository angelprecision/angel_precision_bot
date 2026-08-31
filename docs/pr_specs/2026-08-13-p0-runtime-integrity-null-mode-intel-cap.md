# P0 SPEC — Repair Aug 13 runtime identity debt, intelligence None crash, and position-cap provenance

## Status

**DRAFT / HARD HOLD / IMPLEMENTATION CONTRACT ONLY.**

This branch is intentionally docs-only at creation so Codex can implement against the exact current-main state rather than inheriting stale production hunks.

- Repository: `angelprecision/angel_precision_bot`
- Base commit: `3ac102dab320007409107ad36cc9494897d2799e`
- Base commit message: merge PR #445, `P0 SPEC: Recover STUCK_TRIGGER_READY before broker ownership`
- Incident log timestamp: `2026-08-13 08:57:33Z–08:57:35Z`
- Branch: `spec/p0-aug13-runtime-integrity-20260813`
- Do not merge, deploy, mark ready, or change production configuration from this PR until implementation is complete, exact-head tests are green, and a fresh independent audit returns MERGE.

The goal is not to make the logs quieter. The goal is to remove three classes of uncertainty without weakening any existing money-path fence:

1. legacy FILLED orders with `execution_mode IS NULL` repeatedly re-entering fill-monitor recovery;
2. a real intelligence exception caused by a nullable value participating in an ordered comparison;
3. runtime `max_positions=50` being accepted from client config without clear provenance against other repository defaults/policies.

---

# 1. Production evidence from 2026-08-13

## 1.1 Repeated fill admission holds

Tradefluence PAPER produced repeated CRITICAL events with the same shape:

```text
FILL_EXECUTION_MODE_UNPROVEN
kind=ENTRY
mapped_status=FILLED
row_execution_mode=None
runtime_execution_mode=paper
mode_hold=True
price_hold=False
```

The paired decision event explicitly states:

```text
Broker fill admission failed before OSM, position, exit-engine, or broker side effects.
```

Observed examples include:

- SMCI `SMCI260529C00036500`, broker order `30789855`, avg fill `1.09`
- BA `BA260529C00225000`, broker order `30790638`, avg fill `2.02`
- ADBE `ADBE260605C00280000`, broker order `31342949`, avg fill `4.00`
- IWM `IWM260605P00287000`, broker order `31343127`, avg fill `2.60`
- NKE `NKE260605P00046000`, broker order `31343176`, avg fill `0.86`
- GOOGL `GOOGL260605P00370000`, broker order `31372263`, avg fill `2.26`
- META `META260605P00580000`, broker order `31418142`, avg fill `3.20`
- CSCO `CSCO260605C00128000`, broker order `31426233`, avg fill `2.55`
- NVDA `NVDA260605C00230000`, broker order `31426339`, avg fill `4.20`
- IWM `IWM260605C00290000`, broker order `31428933`, avg fill `2.91`
- UNH `UNH260605C00385000`, broker order `31534393`, avg fill `2.00`

The OCC expirations are May 29 or June 5, 2026 while the incident is August 13, 2026. These are therefore historical contracts being revisited by recovery, not plausible new August 13 option fills.

## 1.2 Snapshot correctly excludes legacy null-mode debt

The same startup slice reports:

```text
[tradefluencehq@gmail.com] SNAPSHOT_ORPHAN_FILLED_IGNORED ... null_mode_count=53 ... expected_mode=paper
[jose.vasquez4011@gmail.com] SNAPSHOT_ORPHAN_FILLED_IGNORED ... null_mode_count=32 ... expected_mode=paper
```

and:

```text
SNAPSHOT_RECONCILED_TERMINAL_FILLED_IGNORED ... ignored_capital=52479.00
SNAPSHOT_RECONCILED_TERMINAL_FILLED_IGNORED ... ignored_capital=57327.00
```

This behavior is intentional and must remain. Current regression coverage in `tests/test_pr112_snapshot_execution_mode_filter.py` explicitly requires historical null-mode orphan fills to consume zero current position slots and zero current capital.

Do **not** fix the incident by making position-manager count null-mode fills.

## 1.3 Intelligence exception

MCD produced twice:

```text
[intelligence_bridge] WARNING: [MCD] Intel error ('<=' not supported between instances of 'int' and 'NoneType') — data-collection override
```

The affected runtime is PAPER in the supplied slice.

Current `intelligence_bridge.py` policy says:

- LIVE defaults fail-closed on unavailable/timeout/error/skip/low-score/risk-veto;
- PAPER/research may use the existing data-collection override;
- LIVE data collection requires explicit `INTEL_DATA_COLLECTION_OVERRIDE=1`.

Preserve that distinction. The fix is to eliminate the TypeError and preserve honest unavailable/error semantics, not to convert missing data into favorable intelligence authority.

## 1.4 MCD queue path otherwise appears healthy

MCD master-control decisions are approved for both paper clients, contract selection is intentionally deferred outside market hours, and existing WATCHING rows are recognized by exact identity:

```text
Post-market signal — skipping contract selection (stale quotes). Will select at breach time with live quotes.
overnight_candidate_loaded=true ... contract_selection=deferred execution_pipeline=watching_for_morning_reeval
already WATCHING (exact identity match) ... mode=paper ticker=MCD side=CALL
```

Do not disturb this deferred-selection path while repairing the incident.

## 1.5 Runtime max position cap is 50

Both MCD decision events record:

```text
thresholds.max_positions = 50
```

Current runner code resolves:

```python
max_pos = int(client_cfg.get("max_concurrent_positions", os.getenv("MAX_POSITIONS", "10")) or 10)
```

so the code fallback is 10, while the actual runtime client configuration supplied 50.

Repository history also contains `migrations/20260519_phase2_lift_caps.sql`, whose documented planned active-client default is:

```text
max_trades_per_day = 12
max_concurrent_positions = 4
```

Do **not** assume 4, 10, 15, or 50 is correct merely because one source contains it. Codex must establish the current canonical policy/provenance before changing a money-limit value.

---

# 2. Existing PR overlap / do-not-duplicate map

## PR #130 — historical execution_mode backfill

Open PR #130 proposes backfilling `orders.execution_mode` from the member's **current** `tradier_account_mode`.

Do not copy that implementation.

A client being LIVE now is not proof that a historical May/June order was LIVE when submitted. Retroactively deriving economic taxonomy from current membership state can turn historical PAPER orders into LIVE rows and contaminate slot accounting, proof taxonomy, diagnostics, or later recovery.

This new PR should be treated as a safer replacement for the problem statement in #130, but do not close #130 automatically from implementation work. Final review should explicitly recommend whether #130 is superseded/closed.

## PR #199 — broad LIVE entry-quality/intelligence fail-closed work

PR #199 already owns the broader policy question of LIVE intelligence fail-closed behavior, stale entry quality, and final pre-submit admission.

Do not rebuild #199 here.

This PR owns only:

- reproducing and removing the specific nullable comparison crash seen on MCD;
- preserving exact existing PAPER-vs-LIVE error behavior;
- proving a LIVE intelligence exception still yields zero execution authority by default.

If the exact current-main root cause lies in code already modified by #199, port only the minimum current-main-safe correction and tests. Do not stack this branch on stale #199 implementation.

## PR #445 — current main

Base `3ac102d` includes #445. Do not revert, bypass, or duplicate its STUCK_TRIGGER_READY/prebroker ownership logic.

---

# 3. Exact current production paths to trace before editing

Codex must trace these paths on the branch head before changing code.

## 3.1 Fill / recovery path

At minimum inspect:

- `ap/fill_monitor.py::get_pending_orders()`
- `ap/fill_monitor.py::_is_canonical_owner_handoff_recovery()`
- `ap/fill_monitor.py::_durable_filled_entry_recovery_result()`
- the exact mode validation branch that emits `FILL_EXECUTION_MODE_UNPROVEN`
- `ap/position_manager.py` snapshot queries that emit `SNAPSHOT_ORPHAN_FILLED_IGNORED`
- any startup/reconciler path that repeatedly causes historical FILLED rows to qualify for fill-monitor work

Current `get_pending_orders()` includes broker-backed FILLED ENTRY rows when position/canonical-handoff recovery is unresolved. That explains why dead historical fills can be revisited.

The fix must distinguish:

1. **current/recent unresolved broker fill with missing mode**, which is a high-severity identity break and must remain fail-closed + visible;
2. **expired historical legacy row with missing mode**, which must remain non-authoritative but should not be treated as a fresh money-at-risk incident on every process start forever.

## 3.2 Intelligence path

Trace:

```text
APMasterControl.evaluate
  -> _run_intelligence
  -> intelligence_bridge.run_intelligence_check
  -> actual ap_intelligence pipeline/agent/tool that receives MCD production shape
  -> exception boundary
  -> _unavailable_gate / paper data-collection behavior
```

Do not stop at `intelligence_bridge` unless the failing comparison is there. Find the exact expression whose right-hand operand becomes `None` and reproduce it with the production-shaped MCD payload.

The log exception text is specifically:

```text
'<=' not supported between instances of 'int' and 'NoneType'
```

That generally means an integer/number is on the left and an unresolved nullable threshold/value is on the right. Do not merely add a global `try/except`; the exception is already caught. Fix the source of invalid ordered comparison.

## 3.3 Runtime cap provenance path

Trace from durable client/member configuration to the MCD decision event:

```text
Supabase/client row or config source
  -> client_runner client_cfg
  -> max_concurrent_positions resolution
  -> APPositionManager / APMasterControl construction
  -> master_control threshold trace
  -> decision_events.thresholds.max_positions
```

Identify:

- exact table and column supplying `50`;
- whether `clients`, `members`, env, cached config, or another source wins;
- whether there are duplicate authorities;
- whether runtime updates are cached/stale;
- what current live policy is intended for Jason;
- what current paper policy is intended for Jose/Tradefluence;
- whether any startup guard currently validates a nonsensical/high cap.

Do not hardcode a replacement number until that provenance is proven.

---

# 4. Required implementation: Workstream A — historical null-mode FILLED recovery

## 4.1 Invariant

**Unknown execution mode never becomes execution authority by inference.**

For every order/fill path:

```text
exact row mode == exact runtime/account mode -> eligible for normal reconciliation
missing/unknown row mode -> no OSM/position/exit/proof authority
explicit row/runtime mismatch -> no OSM/position/exit/proof authority
```

Never infer LIVE/PAPER from:

- current member account mode alone;
- current runner mode alone;
- email/client name;
- broker hostname alone;
- current environment alone;
- current subscription state;
- a default value;
- `COALESCE(..., 'paper')` or `COALESCE(..., 'live')`.

## 4.2 Historical-vs-current classification

Introduce a small deterministic classifier or equivalent narrow logic that can distinguish an unrecoverable historical legacy row from a current/recent identity failure.

Minimum evidence available to consider:

- `created_ts` / submitted/fill timestamps;
- OCC contract expiration parsed from exact contract identity;
- order status and broker ID;
- position ID presence;
- canonical handoff metadata;
- exact stored execution mode when present;
- any durable producer-bound mode evidence already present in order metadata;
- source signal/canonical identity only if it can be joined exactly and that source itself carries trustworthy mode evidence.

Preferred behavior:

### Historical legacy null-mode FILLED

If the order is provably historical/expired and execution mode remains unprovable:

- do not admit the fill;
- do not create/update a position;
- do not call exit engine;
- do not write proof truth;
- do not count slot/capital;
- do not call broker submit/cancel;
- do not spam one CRITICAL event per row on every monitor iteration;
- retain visible diagnostics through a bounded/summarized event such as a startup count, health state, or classified legacy quarantine metric.

The exact durable strategy may be one of:

1. exclude only **provably historical + null-mode + unrecoverable** rows from repeated active fill polling while preserving a summary diagnostic; or
2. persist a narrow non-economic recovery/quarantine marker that keeps them out of the active recovery query.

If using a durable marker, preserve the original mode as NULL. Do not backfill a guessed execution_mode just to make the query disappear.

### Current/recent null-mode FILLED

A current-session/recent broker fill with `execution_mode IS NULL` remains a CRITICAL incident:

- fail closed;
- zero downstream side effects;
- visible order/client/broker identity;
- operator diagnostic/alert if the existing critical-alert infrastructure supports it without creating a new dependency;
- do not silently classify it as historical merely because `position_id` is missing.

## 4.3 Safe historical mode repair, if implemented

A backfill is optional in this PR. If Codex chooses to backfill provable historical rows, it must use **producer-bound historical evidence**, not current client state.

Acceptable pattern conceptually:

```text
historical order row
+ exact originating signal/order producer identity
+ exact historical mode evidence recorded at creation/submission
+ no conflicting secondary mode evidence
=> backfill that exact mode
```

Any ambiguity remains NULL/quarantined.

Required audit counts before and after:

```text
rows_examined
rows_proven_paper
rows_proven_live
rows_conflicted
rows_unproven
rows_updated
```

A migration/script must default to dry-run or be idempotent and separately applied. No automatic production backfill on process boot.

## 4.4 Position-manager behavior is not the bug

Preserve the existing null-mode exclusion semantics in `ap/position_manager.py`.

Explicit regression:

```text
53 Tradefluence null-mode historical fills + 32 Jose null-mode historical fills
=> 0 active slots attributable to those rows
=> $0 current deployed/pending capital attributable to those rows
```

Do not remove `execution_mode IS NOT NULL` style safety merely to silence logs.

---

# 5. Required implementation: Workstream B — MCD intelligence nullable comparison

## 5.1 Reproduce before fixing

Add a production-shaped unit/integration fixture that reaches the real failing function and reproduces the exact TypeError on unmodified current-main behavior.

The fixture must preserve the missing field as `None`; do not manufacture a numeric substitute in the test.

Record in the test/spec comment:

- exact function;
- exact field/threshold name;
- value on left side of comparison;
- value on right side (`None`);
- where the nullable value originated;
- whether missing means unavailable, not-applicable, or producer bug.

## 5.2 Fix at the data boundary

Use existing typed helpers (`_safe_float`, `_safe_int`, or the equivalent local authority) where possible.

Required semantics:

- missing/blank/bool/non-finite/malformed value does not participate in ordered comparison;
- do not turn missing into 0 if 0 would be favorable;
- do not turn missing into an arbitrarily permissive maximum;
- if the value is required for a hard decision, return explicit unavailable/error status;
- if the value is advisory/not-applicable, mark that explicitly and skip only that advisory comparison;
- preserve the raw/problem field in diagnostics without leaking secrets.

Do not hide the defect with:

```python
try:
    ... <= ...
except TypeError:
    pass
```

or broad exception swallowing. The exception boundary already exists; this PR must make the internal contract typed and deterministic.

## 5.3 PAPER behavior

For PAPER/research, preserve the existing data-collection contract unless the current code proves otherwise:

- intelligence error/unavailable may produce the existing limited data-collection override;
- the result must retain `intel_status`, error/reason, and non-authoritative provenance;
- no malformed input may masquerade as a genuine high intelligence score;
- if the override is one-contract by contract, preserve that cap.

## 5.4 LIVE behavior

For LIVE default configuration:

```text
same missing/null input
=> no TypeError
=> explicit intel unavailable/error/block result
=> zero new execution authority
=> zero broker submit caused by the intelligence result
```

This PR must include a regression proving the exact MCD-like missing field cannot make Jason LIVE pass intelligence merely because PAPER has a data-collection override.

Do not expand into the whole #199 entry-quality stack.

## 5.5 Module-import fail-open audit

Current `ap_master_control.py` contains an old defensive import path whose comments/state may allow `_run_intelligence` to fail open if `intelligence_bridge` cannot be imported at module load.

Do **not** automatically redesign this behavior in this PR unless the exact current-main runtime path proves it remains active and overlaps the incident.

However, the implementation report must explicitly state:

- whether module-import failure can still approve LIVE;
- whether #199 or another open PR owns it;
- whether this PR changed it;
- why the final merge verdict is safe despite it.

No hidden “not part of the incident” omission on a money gate.

---

# 6. Required implementation: Workstream C — max_positions provenance and startup attestation

## 6.1 Problem statement

Today’s MCD decision records `max_positions=50`.

Repository evidence contains at least three values:

- runtime client config: 50;
- `client_runner.py` env fallback: 10;
- historical phase-2 migration documented intended default: 4.

This is an authority/provenance problem before it is a numeric tuning problem.

## 6.2 Trace and document the winner

Codex must identify the exact current source that resolves to 50 and add a regression around the real production shape.

Required diagnostic record at runner initialization or equivalent canonical seam:

```text
client_id
execution_mode
resolved_max_positions
source_kind          # e.g. durable_client_config | env_fallback | default
source_field
source_version/hash if available
raw_value
validation_result
```

Do not print broker tokens or unrelated member secrets.

## 6.3 Invalid/unproven cap handling

The system must not accept arbitrary malformed values by accident.

At minimum classify:

- missing
- blank
- bool
- non-integral
- zero
- negative
- unreasonably high according to an explicitly configured/declared policy ceiling
- valid positive integer

Do not silently clamp 50 to some guessed value and continue. Silent clamping hides config corruption.

Preferred contract:

- one canonical configured hard ceiling exists;
- client-specific cap may be lower than/equal to that ceiling;
- a client cap above the ceiling is a configuration error;
- LIVE startup fails closed/degrades entries before broker submit if the cap is invalid or above a required safety ceiling;
- PAPER may fail startup or enter visible degraded/hold mode according to existing runner conventions, but must not quietly accept an invalid value;
- exact cap provenance is included in startup diagnostics.

If no current canonical hard ceiling exists, do not invent one invisibly. Add an explicit required configuration authority or reuse an existing documented authority and make the absence visible. The PR description must state the chosen policy and why.

## 6.4 Do not conflate daily trade cap and concurrent position cap

`max_trades_per_day` and `max_concurrent_positions` are separate controls.

Do not change one to compensate for the other.

Do not fix old slot-accounting behavior here. Current null-mode slot exclusion must remain intact.

## 6.5 No broad sizing changes

Do not change:

- position percentage sizing;
- Kelly/tier logic;
- contract count max;
- daily loss thresholds;
- capital percentage gates;
- scanner score floors;
- contract selector quality thresholds.

This workstream is configuration provenance + validation only unless an exact current config bug is proven.

---

# 7. Required production-shaped acceptance tests

Codex must implement at least the following cases. More are welcome if current code reveals additional branches.

## A. Null-mode / fill-monitor recovery

1. Tradefluence-like expired May 29 FILLED ENTRY, broker id present, `execution_mode=NULL`, missing position -> no OSM/position/exit/proof authority.
2. Expired June 5 FILLED ENTRY same -> same.
3. 53 historical Tradefluence null-mode rows -> zero current slot/capital consumption.
4. 32 historical Jose null-mode rows -> zero current slot/capital consumption.
5. Historical null-mode rows do not emit one CRITICAL per row on every monitor cycle after classification/quarantine behavior is applied.
6. Historical rows remain queryable/auditable after suppression; no silent deletion.
7. Current-session FILLED row with `execution_mode=NULL` -> still CRITICAL/HOLD.
8. Current-session FILLED row with exact `execution_mode='paper'` in PAPER runtime -> normal reconciliation path remains available.
9. Current-session FILLED row with exact `execution_mode='live'` in LIVE runtime -> normal reconciliation path remains available.
10. PAPER row in LIVE runtime -> mismatch/HOLD, zero downstream side effects.
11. LIVE row in PAPER runtime -> mismatch/HOLD, zero downstream side effects.
12. Current client is LIVE today but historical row lacks historical mode evidence -> must not be backfilled LIVE from current membership state.
13. Current client is PAPER today but historical row lacks evidence -> must not be backfilled PAPER solely from current membership state.
14. Conflicting durable historical mode evidence -> remain unproven/quarantined.
15. Historical exact producer-bound PAPER evidence with no conflict -> if backfill feature exists, may become PAPER only through reviewed migration/tool path.
16. Historical exact producer-bound LIVE evidence with no conflict -> if backfill feature exists, may become LIVE only through reviewed migration/tool path.
17. No broker submit/cancel calls occur in historical cleanup classification.
18. No `proof_trades` mutation occurs in historical cleanup classification.
19. No `trade_queue` mutation occurs unless current architecture requires an exact diagnostic marker; if so prove it cannot rearm/submit.
20. Existing canonical-owner handoff recovery for a valid modern FILLED row remains functional.

## B. Intelligence nullable input

21. Exact production-shaped MCD missing field reproduces old TypeError before fix / returns deterministic status after fix.
22. Required numeric field `None` -> explicit unavailable/error, no ordered comparison.
23. Required numeric field blank string -> explicit unavailable/error.
24. Required numeric field bool -> explicit unavailable/error.
25. Required numeric field NaN/Inf -> explicit unavailable/error.
26. Advisory nullable field -> explicit advisory unavailable/not-applicable path, no fake score.
27. Valid zero where zero is semantically valid remains zero and is not treated as missing.
28. Valid positive numeric field preserves existing behavior.
29. PAPER MCD error path preserves data-collection override semantics and diagnostics.
30. LIVE MCD same missing field blocks by default.
31. LIVE block returns zero contracts / no executable authority according to current intelligence contract.
32. No broker submit/cancel is invoked by the intelligence-error test path.
33. `client_id` and `execution_mode` remain exact through the intelligence result.
34. No PAPER result is labeled LIVE or LIVE_OFFICIAL by fallback.

## C. max_positions provenance

35. Production-shaped `client_cfg.max_concurrent_positions=50` resolves source as durable client config, not env/default.
36. Missing client value + valid env resolves env source exactly.
37. Missing client + missing env resolves documented fallback only if fallback is allowed by chosen policy.
38. Blank client value is not silently interpreted as an arbitrary valid cap.
39. Bool cap rejected.
40. Fractional/non-integral cap rejected.
41. Zero rejected.
42. Negative rejected.
43. Above canonical safety ceiling -> visible configuration failure; LIVE cannot open new entries.
44. Valid client cap at/below ceiling passes.
45. Startup/decision diagnostics report exact resolved value + source.
46. Existing open positions/exits remain manageable when new entries are blocked for bad cap config; do not disable exit safety.
47. Cap validation causes zero broker submit/cancel itself.
48. No `proof_trades` mutation.
49. No PAPER/LIVE taxonomy mutation.

## D. End-to-end incident replay

50. Replay the supplied startup shape with 53 Tradefluence null-mode + 32 Jose null-mode historical FILLED rows, MCD nullable intel input, and client cap 50. Expected end state must clearly classify each issue without:

- creating a position from a null-mode historical fill;
- consuming slots/capital from null-mode debt;
- throwing the MCD TypeError;
- silently converting missing intel into approval authority;
- silently accepting an invalid/unproven cap policy;
- submitting/canceling a broker order as part of cleanup/diagnosis.

---

# 8. Expected production file budget

Start with the smallest exact current-main seam. Likely production candidates are:

1. `ap/fill_monitor.py`
2. `intelligence_bridge.py` **or the exact `ap_intelligence/...` producer that owns the bad nullable comparison**
3. `client_runner.py`
4. `ap/startup_guard.py` only if it is the existing canonical startup-policy seam
5. `ap/position_manager.py` only if tests reveal a required diagnostic change; its null-mode exclusion behavior must not be weakened
6. one read-only audit/helper module if needed for historical classification/provenance
7. one migration only if a durable quarantine marker or provable historical evidence field truly requires schema change

Expected tests:

- one focused null-mode recovery test module;
- one focused intelligence nullable-input test module;
- one focused cap-provenance/startup test module;
- one end-to-end incident replay test if practical;
- add focused files to the existing authoritative P0 workflow exactly once if repository policy requires it.

Do **not** touch without proof:

- `ap_execution_core.py`
- broker submit adapter
- order cancel adapter
- contract selector
- scanner admission
- exit engine
- proof logger/taxonomy
- trade queue fanout
- direction reversal/rearm
- order monitor cancel/replace stack
- PR #439 deferred selector work
- PR #440 cancel-replace work
- #423/#424/#428 EXIT stack

If the required implementation exceeds this budget, stop and explain the additional exact owner before expanding.

---

# 9. Money-path invariants

Before this PR can leave HARD HOLD, prove all of the following against the cumulative diff.

## Live behavior

- Historical cleanup may change reconciliation workload/logging, but must not create new trade authority.
- Intelligence fix may change an exception into a deterministic unavailable/advisory result; it must not make malformed inputs favorable.
- Cap validation may block new entries when configuration is invalid/unproven; it must not block existing position protection/exits.

## Broker submit/cancel

- Historical cleanup path: **zero submit, zero cancel**.
- Intelligence-error path: **zero submit caused by error/override tests unless the existing PAPER end-to-end harness deliberately reaches a later broker stub; LIVE malformed intel must never authorize submit.**
- Cap-attestation failure: **zero new ENTRY submit**; existing EXIT management must remain alive.

## Orders / positions / proof / queue

- No new position from unproven-mode historical fill.
- No slot/capital mutation from historical null-mode debt.
- No proof-trade creation/reclassification from historical cleanup.
- No queue rearm caused by cleanup.
- Any durable quarantine marker must be non-economic and must not change canonical `client_id`, `execution_mode`, broker identity, qty, price, or trade outcome.

## Identity

Preserve exact:

- `client_id`
- `execution_mode`
- local order id
- broker order id
- canonical signal id
- position id where present
- OCC contract
- kind/direction

No default mode fallback.

## Diagnostics

Do not trade observability for silence.

The final implementation must make it possible to answer:

- how many legacy null-mode rows exist per client;
- how many are historical vs current/recent;
- why each current null-mode fill is held;
- which MCD intel field was missing/malformed;
- whether the intel result was authoritative, advisory, or data-collection-only;
- where `max_positions` came from for each runner and whether it passed policy.

---

# 10. Codex execution order

Codex should execute this PR in the following order.

1. Pull/fetch current branch and confirm base ancestry contains `3ac102d`.
2. Read this entire spec.
3. Read current cumulative diff. It should initially contain only this spec file.
4. Read PR comments/review threads before implementation and again before final response.
5. Reproduce all three incident classes on current branch **before** editing production code.
6. Trace exact current production paths listed above.
7. Search open PRs again for overlap before touching each production file.
8. Implement Workstream A with the smallest reconciliation seam.
9. Run focused A tests.
10. Implement Workstream B at the exact nullable data boundary.
11. Run focused B tests and LIVE/PAPER parity tests.
12. Implement Workstream C after proving cap provenance; do not guess the numeric policy.
13. Run focused C tests.
14. Run the end-to-end incident replay.
15. Run adjacent current-main tests for fill monitor, position snapshot, intelligence admission, runner startup, and entry/exit safety.
16. Run authoritative P0 CI on the exact head.
17. Inspect exact changed files and cumulative diff for scope creep.
18. Update the PR description with exact implementation, test counts, CI run links/IDs, and any migrations/config prerequisites.
19. Do not merge/deploy.
20. Return a fresh `MERGE / HOLD / HARD HOLD` recommendation.

---

# 11. Required final implementation report

Codex must post/update the PR with:

```text
Base SHA:
Head SHA:
Files changed:
Production files changed:
Tests changed/added:
Focused test command + exact result:
Adjacent regression command + exact result:
P0 workflow run + exact result:
DB/schema workflow run if applicable:

Workstream A root cause:
Workstream A exact behavior change:
Historical rows classified:
Current null-mode behavior:
Mode backfill performed? yes/no
If yes, evidence rule + row counts:

Workstream B exact failing expression:
Nullable field name:
Producer/source of None:
PAPER result after fix:
LIVE result after fix:

Workstream C source of runtime 50:
Canonical cap policy chosen:
Why that authority is canonical:
Startup behavior on invalid cap:
Exit behavior while entries blocked:

Broker submit exposure changed? yes/no
Broker cancel exposure changed? yes/no
Orders mutated? exact fields/paths
Positions mutated? exact fields/paths
proof_trades mutated? yes/no
trade_queue mutated? yes/no
client_id preserved? proof
execution_mode preserved? proof
PAPER/LIVE taxonomy preserved? proof
Could this make Jason trade junk? explicit reasoning

Open PR overlap checked:
#130 disposition recommendation:
#199 overlap statement:
Other overlap:

Final verdict: MERGE / HOLD / HARD HOLD
```

No “tests pass” without exact counts. No “safe” without tracing broker/DB side effects. No merge command.

---

# 12. Initial verdict

**HARD HOLD.**

The current fail-closed mode gate is protecting the system and must not be weakened. The work is required because:

- 85 known PAPER historical null-mode rows are still recurring through runtime diagnostics/recovery;
- a production-shaped intelligence input can still throw a TypeError;
- runtime concurrent-position authority resolves to 50 without a clearly attested policy source in the decision trace.

This PR is complete only when those uncertainties are converted into deterministic, production-shaped contracts while preserving broker, identity, and PAPER/LIVE safety.