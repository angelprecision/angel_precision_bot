# P0: Pre-trigger stop activation and spread-confirmed watcher invalidation

> **DRAFT IMPLEMENTATION CONTRACT ONLY. DO NOT MERGE THIS SPEC AS A RUNTIME FIX.**
>
> This branch intentionally changes documentation only. It preserves the production defect, evidence, exact runtime path, required implementation boundaries, and acceptance tests so an implementation agent can work from a controlled contract later.

## Status

- Repository: `angelprecision/angel_precision_bot`
- Base branch: `main`
- Base SHA at branch creation: `715d26ede22c6078c22cf5796836789332b5dfe5`
- Spec branch: `spec/p0-pretrigger-stop-activation-spread-confirmation`
- Production behavior changed by this spec: **NO**
- Broker submit/cancel changed by this spec: **NO**
- Orders, positions, `proof_trades`, queue, or opportunity rows mutated by this spec: **NO**
- Required implementation verdict before coding: **HARD HOLD until this contract is reviewed**

---

## 1. Executive defect statement

A production ODFL PUT opportunity for Jason LIVE was permanently canceled at watcher attachment even though the PUT trigger had not breached and both BID and midpoint remained below the stored stop.

The current late-attachment classifier:

1. evaluates the scanner stop before the direction has ever produced a valid trigger breach;
2. defines a pending PUT stop break as `ask >= stop`;
3. terminalizes from one quote observation immediately after the five-minute open-protection window;
4. records diagnostics using the PUT trigger quote (`bid`) even though the stop decision was made from `ask`;
5. cancels the exact `PENDING_TRIGGER` order and rejects the queue opportunity before any selector or broker submit path can run.

This is a confirmed production trade-flow defect relative to the operator strategy invariant:

> The scanner stop is not active until that direction first produces a valid trigger breach. Before either direction breaches, both directional candidates remain eligible. A pre-trigger spread excursion must not permanently invalidate a direction.

The fix is **not** “use midpoint instead of ASK.” The repair must introduce explicit trigger-activation state, preserve it across retry/restart seams where necessary, and distinguish a confirmed adverse stop crossing from a spread-only ambiguous observation.

---

## 2. Exact production incident: ODFL PUT

### Canonical signal geometry

| Field | Value |
|---|---:|
| Symbol | `ODFL` |
| Direction | `PUT` |
| Signal ID | `cda0019a-0b30-467a-85e3-7e568e63d67e` |
| Pattern | `3-2-2` |
| Timeframe | `1d` |
| Score | `72` |
| Trigger | `228.26` |
| Stop | `231.74` |
| Target | `224.78` |
| Scanner underlying at signal | `232.99` |

### Jason LIVE order identity

| Field | Value |
|---|---|
| `client_id` | `jasoncosby1@gmail.com` |
| `execution_mode` | `live` |
| `local_order_id` | `8eb340b2-40a9-47dc-8cb5-d0e7a90b3ee6` |
| Contract | `DEFERRED:ODFL` |
| Initial status | `PENDING_TRIGGER` |
| Final status | `CANCELED` |
| `broker_order_id` | `null` |
| `position_id` | `null` |
| Submitted | `false` |
| Filled | `false` |
| Final `last_error` | `stop_already_broken_terminal` |

### Quote used at watcher arm

| Field | Value |
|---|---:|
| BID | `230.56` |
| ASK | `232.34` |
| Midpoint | `231.45` |
| Spread | `1.78` |
| Stop margin from BID | `-1.18` below stop |
| Stop margin from midpoint | `-0.29` below stop |
| Stop margin from ASK | `+0.60` above stop |

### Time boundary

- Order created: `2026-07-27T13:35:06.104636Z`
- Eastern time: `2026-07-27 09:35:06.104636 ET`
- Existing open protection: 09:30:00 through 09:34:59 ET
- First terminal evaluation: approximately six seconds after the protection window ended

### Cross-client reproduction

The same economic signal terminalized across all three clients:

| Client | Mode | BID | ASK | MID | Result |
|---|---|---:|---:|---:|---|
| Jason | LIVE | 230.56 | 232.34 | 231.45 | `stop_already_broken_terminal` |
| Tradefluence | PAPER | 230.92 | 232.43 | 231.675 | `stop_already_broken_terminal` |
| Jose | PAPER | 230.92 | 232.43 | 231.675 | `stop_already_broken_terminal` |

This proves the behavior is active, shared production logic rather than a Jason-only account or metadata defect.

---

## 3. Exact current runtime path

The implementation agent must re-read the current exact-head code before editing. Line numbers below may move; function and branch names are authoritative.

### Entry path

```text
trade_queue / overnight reevaluation
  -> master-control approval
  -> create exact PENDING_TRIGGER order
  -> APEntryWatcher.watch(plan, local_order_id)
  -> regular-session arm-time quote fetch
  -> late_attachment_policy_eligible branch
  -> ap.pending_trigger_classifier.classify_late_attachment(...)
  -> STOP_ALREADY_BROKEN_TERMINAL
  -> watcher audit persisted
  -> on_invalidate cleanup
  -> APOrderStateMachine cancel of the existing pending order
  -> queue/opportunity terminal result
```

### Production import path

`ap_execution_core.py` imports `APEntryWatcher` from the `ap_entry_watcher` package. The package shim loads the legacy top-level `ap_entry_watcher.py`, overrides narrow ownership/quote seams, and delegates `watch()` to the base implementation. The implementation must patch the code path that production actually imports, not a dead duplicate.

Required verification before coding:

1. Confirm `ap_execution_core.py` still imports `from ap_entry_watcher import APEntryWatcher`.
2. Confirm package resolution chooses `ap_entry_watcher/__init__.py`.
3. Confirm the shim still delegates arm-time late-attachment logic to top-level `ap_entry_watcher.py`.
4. Confirm no open PR has moved the classifier or watcher branch since this spec base.
5. Confirm `client_id` and `execution_mode` are present in the exact production plan/signal metadata used by the watcher.

### Current classifier semantics

Trigger quote:

```text
CALL trigger truth = ASK
PUT trigger truth  = BID
```

Stop quote:

```text
CALL stop truth = BID; broken when BID <= stop
PUT stop truth  = ASK; broken when ASK >= stop
```

The stop-side choice is not itself the entire defect. The primary defect is evaluating this stop before directional activation. The secondary defect is permanently terminalizing from one spread-only observation without sufficient quote-quality or confirmation evidence.

### Current diagnostic defect

After `_evaluate_stop()` evaluates the stop side, `classify_late_attachment()` builds the terminal detail from `quote_result.source` and `canonical_quote`. Those values represent the trigger lane, not necessarily the stop lane.

For the ODFL PUT:

- control flow used `ask=232.34` to prove `ask >= 231.74`;
- diagnostic stored `stop_broken_at_bid=230.56`;
- raw watcher reason embedded `228.2600`, the trigger, instead of the `231.74` stop.

The implementation must make control flow and diagnostics refer to the same evidence.

---

## 4. Required strategy invariant

### 4.1 Direction activation

A directional scanner stop becomes eligible to invalidate a pending entry only after the same direction has produced a valid trigger activation.

A valid activation must be tied to the existing canonical trigger lane and the watcher’s existing confirmation policy. It must not be inferred merely because:

- the current quote is on one side of the trigger at process startup;
- a late watcher attaches after open;
- the scanner signal contained a stop;
- the opposite direction breached;
- the underlying opened outside the prior bar;
- a last-only, mark, stale, midpoint, or opposite-side quote suggests a crossing.

Canonical direction activation:

```text
CALL: canonical ASK reaches/crosses the CALL trigger under the existing
      fresh-quote and watcher confirmation contract.

PUT:  canonical BID reaches/crosses the PUT trigger under the existing
      fresh-quote and watcher confirmation contract.
```

The agent must trace whether the normal watcher uses one poll or multiple polls to declare an ordinary breach. `direction_activated` must become true at the same authoritative seam that declares the breach valid, not at an earlier observation-only seam.

### 4.2 Pre-activation behavior

When `direction_activated == false`:

- do not terminalize from the scanner stop;
- preserve the exact `PENDING_TRIGGER` order;
- preserve watcher ownership or continue the appropriate late-attachment/reset state;
- do not invoke selector;
- do not submit to broker;
- do not create position or `proof_trades` rows;
- do not reject the queue merely because a stop-side quote temporarily crosses the stored scanner stop;
- emit truthful nonterminal diagnostics explaining that the stop was observed but inactive.

For the exact ODFL replay, expected classification is nonterminal because BID `230.56` has not breached PUT trigger `228.26`.

### 4.3 Post-activation behavior

When `direction_activated == true`, stop invalidation may be evaluated. The implementation must distinguish:

1. **Confirmed stop crossing**: terminal behavior may continue.
2. **Spread-only ambiguous crossing**: retain ownership and retry.
3. **Missing stop-side truth**: retain ownership and retry.
4. **Stale or untrusted quote**: retain ownership and retry under existing bounded truth rules.

Do not globally replace executable stop-side truth with midpoint. Midpoint is useful as an ambiguity diagnostic, not a universal execution substitute.

---

## 5. Required implementation design

### 5.1 Canonical activation state

Introduce one canonical state name. Preferred naming:

```text
watcher_direction_activated
watcher_direction_activated_at
watcher_direction_activation_quote
watcher_direction_activation_quote_source
watcher_direction_activation_generation
```

The exact names may change only if an existing canonical field already serves this purpose. Do not introduce parallel synonyms in multiple metadata layers.

Required properties:

- default false for a newly armed pre-trigger watcher;
- set true only at the valid trigger-confirmation seam;
- never set from stop logic;
- never set from the opposite direction;
- preserved across bounded selector retries/rearms when the same order remains pending after a valid breach;
- hydrated on restart/reattach only from exact durable proof, never from current price alone;
- scoped by exact `local_order_id`, `signal_id`, `client_id`, and `execution_mode`;
- idempotent if the activation write is retried;
- monotonic for the life of the exact order generation: false may become true, true must not silently become false.

### 5.2 Durable proof requirement

The implementation agent must trace the real retry/restart shapes before deciding whether in-memory state is enough.

At minimum inspect:

- ordinary watcher breach path;
- deferred selector retry path;
- `recovery_rearm` path;
- `materialization_resume` path;
- overnight reattach path;
- order metadata hydration into a reconstructed plan;
- any path that leaves the same exact order in `PENDING_TRIGGER` after a valid breach.

If any such path can cross a process restart, activation state must be durably persisted in the existing order metadata. Do not create a new table or migration for this PR.

A durable activation write must not:

- change contract, quantity, limit price, or reserved cost;
- change `client_id` or `execution_mode`;
- create a replacement order;
- reset selector retry counts;
- manufacture a broker submit proof;
- mutate positions or `proof_trades`.

### 5.3 Classifier input and result shape

Refactor `classify_late_attachment()` or introduce a narrowly named helper so it receives explicit activation state rather than inferring it.

Required input concept:

```python
classify_late_attachment(
    side=...,
    trigger_price=...,
    bid=...,
    ask=...,
    stop=...,
    direction_activated=...,
    target_complete=...,
    decisive_drift_exceeded=...,
    # quote-quality/confirmation context only if available at this seam
)
```

Required behavior:

```text
IF valid stop exists AND direction_activated is false:
    stop cannot produce STOP_ALREADY_BROKEN_TERMINAL

ELSE IF direction_activated is true:
    evaluate the canonical stop-side quote
    distinguish confirmed crossing from ambiguous/missing truth
```

Do not silently default a missing activation field to true. For legacy rows without durable activation proof, fail toward preserving the pending watcher, not terminalizing it as though activation were proven.

### 5.4 Spread-only ambiguity

A single executable-side quote crossing while midpoint remains on the safe side is an ambiguous spread shape, especially near the open.

Required examples:

```text
PUT:  ask >= stop but midpoint < stop
CALL: bid <= stop but midpoint > stop
```

Such a shape must not permanently cancel from one observation.

Preferred result taxonomy:

```text
STOP_CROSS_UNCONFIRMED_WIDE_SPREAD_RETRY
```

The exact name may change, but it must be:

- nonterminal;
- explicitly distinct from missing quote truth;
- explicitly distinct from a confirmed stop break;
- preserved in watcher/order diagnostics;
- excluded from broker submit and selector calls;
- bounded by the existing watcher lifetime and session cutoff.

Confirmation must reuse or narrowly extend existing poll-confirmation infrastructure. Do not add an independent background loop.

Minimum confirmation contract:

- at least two fresh observations;
- same exact watcher owner and generation;
- stop-side quote remains across the stop;
- quote timestamps advance or are proven fresh by existing quote metadata;
- no last-only fallback;
- no opposite-side fallback;
- no counting the same snapshot twice;
- reset confirmation count when the stop-side quote returns safe;
- preserve the watcher when evidence is missing or ambiguous;
- terminalize only after confirmation and only when `direction_activated == true`.

If the current quote provider does not expose timestamp/age at this seam, the agent must document that fact and use the strongest existing freshness proof. Do not fabricate freshness fields.

### 5.5 Truthful stop evidence object

The stop evaluator must return structured evidence, not only a string or boolean whose diagnostics are rebuilt from unrelated trigger variables.

Preferred conceptual shape:

```python
StopEvaluation(
    state="INACTIVE" | "SAFE" | "UNKNOWN" | "AMBIGUOUS" | "BROKEN",
    stop_level=...,
    stop_quote=...,
    stop_quote_source="bid" | "ask" | None,
    trigger_quote=...,
    trigger_quote_source="ask" | "bid" | None,
    bid=...,
    ask=...,
    midpoint=...,
    spread=...,
    spread_pct=...,
    cross_margin=...,
    direction_activated=...,
    confirmation_count=...,
    detail=...,
)
```

A dataclass is acceptable if it remains pure and side-effect free. Do not add a generic framework.

### 5.6 Correct diagnostics

For every stop decision, persist the actual evidence used:

- `stop_level`;
- `stop_quote_source`;
- `stop_quote`;
- `trigger_quote_source`;
- `trigger_quote`;
- raw BID and ASK;
- midpoint;
- spread and spread percentage when computable;
- cross margin;
- `direction_activated`;
- confirmation count;
- quote timestamp/age only when real;
- exact terminal or retry classification.

The ODFL incident should produce a truthful diagnostic resembling:

```text
PUT stop-side ASK 232.34 exceeded stop 231.74 by 0.60,
but BID 230.56 and midpoint 231.45 remained below stop;
direction_activated=false; terminalization withheld.
```

Never write `stop_broken_at_bid=230.56` when ASK caused the stop comparison.

Never embed only the trigger value in a stop-terminal reason. Include both trigger and stop as separately named fields.

### 5.7 Lifecycle and downstream behavior

Pre-activation or ambiguous stop observations must not invoke the existing terminal invalidation callback.

Expected nonterminal behavior:

- order remains `PENDING_TRIGGER`;
- watcher remains owned;
- queue is not changed to `REJECTED` by this observation;
- opportunity is not changed to `CANCELED` by this observation;
- selector is not called;
- broker submit/cancel/replace is not called;
- no position mutation;
- no `proof_trades` mutation.

Confirmed terminal behavior may continue through the existing cleanup path, but the reason code and persisted audit must be truthful.

Do not modify `ap/queue.py` merely to mask `watch_returned_false`. First make the watcher return the correct nonterminal result. If reason propagation from a real terminal result is impossible without one narrow caller change, the implementation agent must identify that exact seam and request scope approval before adding another production file.

---

## 6. File scope

### Expected runtime files

1. `ap/pending_trigger_classifier.py`
2. `ap_entry_watcher.py`

### Expected tests

Prefer one or two focused files:

1. `tests/test_p0_pretrigger_stop_activation.py`
2. `tests/test_p0_stop_spread_confirmation.py`

The workflow file may be edited only if the P0 workflow uses an explicit test-file allowlist and these tests would otherwise not run.

### Conditional file

`ap_entry_watcher/__init__.py` may be changed only if the production shim blocks the exact required state or API from reaching the base watcher. The agent must prove this before editing it. No speculative shim rewrite.

### Hard scope boundary

If the implementation appears to require more than three production files, stop and return an exact dependency explanation before coding further.

### Prohibited unrelated changes

Do not change:

- scanner generation or ranking;
- score, tier, EV, regime, intelligence, earnings, or quality gates;
- contract selection or DTE ladder policy;
- affordability, sizing, risk percentage, or max contracts;
- entry limit ladder, broker transport, submit, cancel, or replace methods;
- exit engine, runner trail, breakeven, hard stop percentage, or EOD close;
- position manager or reconciler behavior;
- `proof_trades` taxonomy;
- client onboarding, credentials, or broker account routing;
- paper/live execution policy;
- unrelated watcher conflict ownership;
- queue CAS behavior from PR #398/#405;
- broad package/module rewrites.

No full rollback of PR #388. Repair only the defective geometry/activation seam.

---

## 7. Canonical decision matrix

### Before activation

| Side | Trigger quote | Trigger state | Stop-side quote | Stop relation | Expected result |
|---|---|---|---|---|---|
| PUT | BID > trigger | not activated | ASK >= stop | apparent adverse cross | nonterminal; stop inactive |
| PUT | BID > trigger | not activated | ASK < stop | safe | ordinary watching |
| CALL | ASK < trigger | not activated | BID <= stop | apparent adverse cross | nonterminal; stop inactive |
| CALL | ASK < trigger | not activated | BID > stop | safe | ordinary watching |

### After activation

| Side | Activation | Stop-side shape | Expected result |
|---|---|---|---|
| PUT | true | ASK >= stop and midpoint >= stop for confirmed fresh polls | terminal confirmed stop |
| PUT | true | ASK >= stop but midpoint < stop | nonterminal ambiguity retry |
| PUT | true | ASK missing | truth unavailable retry |
| PUT | true | ASK returns below stop | safe; reset confirmation |
| CALL | true | BID <= stop and midpoint <= stop for confirmed fresh polls | terminal confirmed stop |
| CALL | true | BID <= stop but midpoint > stop | nonterminal ambiguity retry |
| CALL | true | BID missing | truth unavailable retry |
| CALL | true | BID returns above stop | safe; reset confirmation |

Midpoint support is a minimum ambiguity signal, not a standalone universal terminal condition. The agent must preserve executable-side authority while requiring enough evidence to avoid one-tick spread terminalization.

---

## 8. Mandatory production-shaped replay

### Replay A: exact ODFL PUT

```text
side=PUT
trigger_price=228.26
stop=231.74
target=224.78
bid=230.56
ask=232.34
midpoint=231.45
direction_activated=false
regular_session=true
late_attachment_policy_eligible=true
recovery_rearm=false
client_id=jasoncosby1@gmail.com
execution_mode=live
local_order_id=8eb340b2-40a9-47dc-8cb5-d0e7a90b3ee6
signal_id=cda0019a-0b30-467a-85e3-7e568e63d67e
```

Expected:

```text
classification is nonterminal
watch() accepts/retains ownership
same local_order_id
same signal_id
same client_id
same execution_mode
order remains PENDING_TRIGGER
queue not REJECTED
opportunity not CANCELED
selector call count = 0
broker submit call count = 0
broker cancel call count = 0
position mutations = 0
proof_trades mutations = 0
audit names ASK as stop-side quote
audit includes stop=231.74 and trigger=228.26 separately
audit states direction_activated=false
```

### Replay B: later valid PUT breach

After Replay A, feed fresh canonical BID observations satisfying the existing ordinary breach confirmation contract at or below `228.26`.

Expected:

- activation becomes true exactly once;
- activation proof is tied to the exact order generation;
- normal selector/materialization path remains unchanged;
- at most one broker submit;
- no duplicate watcher/order;
- no change to contract-selection policy.

### Replay C: activated spread-only stop observation

```text
side=PUT
direction_activated=true
stop=231.74
bid=230.56
ask=232.34
midpoint=231.45
```

Expected first observation:

- no terminalization;
- ambiguity retry classification;
- confirmation count 1;
- exact order and owner retained;
- no selector/broker call.

Expected safe next observation:

```text
ask < 231.74
```

- confirmation resets;
- watcher remains active.

Expected two distinct fresh confirming observations with stop-side and midpoint across stop:

- terminal confirmed stop;
- one cleanup transition;
- truthful audit using ASK.

### Replay D: CALL symmetry

Mirror every pre-activation, ambiguity, missing-truth, reset, and confirmed-terminal case for CALL using ASK as trigger truth and BID as stop truth.

---

## 9. Required tests

At minimum, tests must cover all of the following.

### Pure classifier tests

1. PUT pre-trigger, ASK above stop, direction inactive: not terminal.
2. CALL pre-trigger, BID below stop, direction inactive: not terminal.
3. PUT activated, ASK above stop, midpoint safe: ambiguous retry.
4. CALL activated, BID below stop, midpoint safe: ambiguous retry.
5. PUT activated, confirmed stop shape: terminal.
6. CALL activated, confirmed stop shape: terminal.
7. Missing PUT stop-side ASK: truth retry.
8. Missing CALL stop-side BID: truth retry.
9. Invalid side: fail closed/retry, never default CALL.
10. Invalid or missing activation metadata: preserve unless exact durable proof exists.
11. Diagnostic stop source/value equals the quote actually evaluated.
12. Diagnostic includes both trigger and stop.

### Watcher integration tests

13. Exact ODFL replay remains owned and `PENDING_TRIGGER`.
14. Exact ODFL replay does not call invalidation callback.
15. Later valid breach sets activation once.
16. Activation survives selector retry/rearm when same exact order is retained.
17. Restart hydration restores activation only from exact durable metadata.
18. Restart with no activation proof does not infer activation from current price.
19. Ambiguous stop quote does not remove watcher from pending registry.
20. Same quote snapshot cannot increment confirmation twice.
21. Safe quote resets ambiguity confirmation.
22. Two fresh confirmed stop observations invoke cleanup once.
23. No duplicate order, watcher, selector, or broker submit on retry.
24. Exact `client_id` preserved.
25. Exact `execution_mode` preserved for LIVE and PAPER.
26. PAPER and LIVE use the same geometry classification.
27. No cross-client activation leakage.
28. No cross-direction activation leakage.
29. Existing ordinary pre-open watcher behavior remains unchanged.
30. Existing genuine terminal stop case remains terminal after activation.

### Mutation assertions

Every nonterminal test must assert no mutation to:

- broker submit/cancel/replace call counts;
- positions;
- `proof_trades`;
- queue terminal status;
- opportunity terminal status;
- order identity fields;
- contract, quantity, limit, or reserved cost.

---

## 10. Hidden production-shape checks

The implementation review must explicitly inspect these risks.

### 10.1 Legacy rows

Older `PENDING_TRIGGER` rows will not have the new activation field. They must not be treated as activated merely because the field is absent. Define an explicit legacy fallback that preserves safety and trade flow without manufacturing proof.

### 10.2 JSONB string/dict shape

Production order metadata may arrive as a dict or JSON string depending on caller/test seams. Use the repository’s existing metadata normalization. Do not assume one shape.

### 10.3 Mode casing

Production contains `LIVE`, `live`, `PAPER`, and `paper` at different seams. Preserve the existing canonical normalization and never create a new taxonomy value.

### 10.4 Quote shape

Verify the real watcher quote payload:

- whether BID/ASK are floats, strings, `None`, zero, or NaN;
- whether timestamps/ages are present;
- whether the quote can be last-only;
- whether bid can exceed ask in malformed data;
- whether opening quotes can have large spread but valid positive sides.

Malformed or non-finite values must not become a stop break.

### 10.5 Restart generation

Activation from a prior order generation must not leak into a replacement order with a reused signal ID. Key durable proof to exact order identity and generation.

### 10.6 Opposite-direction candidates

A CALL breach must not activate the PUT stop, and a PUT breach must not activate the CALL stop. The user strategy deliberately allows the opposite direction to remain eligible until its own trigger/invalidity rules resolve it.

### 10.7 Selector retry ownership

If the deferred selector temporarily fails after a valid breach and the same order is rearmed, activation must not be lost. Otherwise the system could treat a genuinely activated direction as pre-trigger after retry.

### 10.8 Audit taxonomy pollution

Do not label an inactive pre-trigger stop observation as a strategy loss, stop-out, canceled trade, or proof trade. It is a watcher observation only.

### 10.9 Open-protection boundary

Add a test at `09:35:00 ET`. The fix must not depend solely on extending the existing five-minute window. The invariant must remain correct at 09:36, 10:00, and after restart.

---

## 11. Required implementation-agent workflow

Before changing code, the agent must:

1. Read this PR description and this entire contract.
2. Read the exact current-head diff against `main`.
3. Read all PR comments and review threads.
4. Re-fetch the exact changed files from the current head.
5. Trace the production import and runtime control flow.
6. Search for all callers of `classify_late_attachment`, `_evaluate_stop`, `watch`, `recovery_rearm`, and `materialization_resume`.
7. Inspect the real production metadata shape from existing tests and code.
8. Identify whether the feature is active or flag-off. This defect is currently active.
9. State whether the patch changes live behavior. It will.
10. State every mutation surface touched.

The implementation must be surgical. Do not begin by rewriting the watcher subsystem.

---

## 12. Required final report from Claude/Codex

The implementation agent must post all of the following in the PR before requesting review:

```text
Previous head SHA:
New exact head SHA:
Base SHA:
Branch:
Files changed, exactly:
Lines added/deleted per file:

Production import path verified:
Active or flag-off:
LIVE behavior changed:
PAPER behavior changed:
Broker submit touched:
Broker cancel/replace touched:
Orders mutated:
Positions mutated:
proof_trades mutated:
Queue mutated:
Opportunity rows mutated:
client_id preserved:
execution_mode preserved:

Exact activation persistence location:
Exact activation hydration location:
Exact terminal stop quote source for CALL:
Exact terminal stop quote source for PUT:
Exact ambiguity rule:
Exact confirmation rule:
Exact stale/missing truth behavior:

ODFL replay result:
CALL symmetric replay result:
Restart replay result:
Selector retry replay result:

Focused tests:
Adjacent P0 tests:
Full exact-head CI run URL/status:
git diff --check:
py_compile:

Known limitations:
Out-of-scope follow-ups:
Final recommendation: MERGE / HOLD / HARD HOLD
```

No “tests pass” summary without the exact command and exact count.

---

## 13. Merge gate

This spec PR remains **HARD HOLD / DO NOT MERGE** until runtime implementation is added and audited.

The eventual implementation is eligible for `MERGE` only when all are true:

- exact ODFL replay is nonterminal before activation;
- a later valid breach still reaches the existing selector/submit path exactly once;
- confirmed activated stop breaks remain terminal;
- spread-only one-tick crosses remain owned and retryable;
- diagnostics identify the actual stop quote source and value;
- restart/retry preserves exact activation truth;
- no broker submit/cancel changes were introduced;
- no positions or `proof_trades` are fabricated;
- `client_id` and `execution_mode` remain exact;
- PAPER/LIVE taxonomy remains coherent;
- exact-head CI is green;
- review confirms the patch did not expand into unrelated watcher, selector, queue, risk, or exit work.

Until then: **HARD HOLD**.
