# P1 SPEC — FVG / VI Exit-Target Intelligence on Current Exit Authority

## Status

**DRAFT / HARD HOLD. SPEC ONLY. DO NOT MERGE OR DEPLOY AS AN IMPLEMENTATION.**

This branch intentionally contains only this work order. Codex must implement on this branch after the dependencies below are stable, run the required tests, update the PR body with exact evidence, and leave the PR Draft/HOLD for independent whole-PR review.

No production enablement, broker deployment, migration application, merge, or live trading authorization is part of this task.

## Why this PR exists

Angel Precision already detects Fair Value Gaps and already exposes non-mutating target guidance in `ap/fair_value_gap.py`. The current module can identify an opposing FVG in the path, cap before an unconfirmed opposing 4H wall, and expose an `extension_candidate_to_opposing_fvg_front` when the original underlying target stops before the next opposing FVG.

What is still missing is the safe integration seam inside the **current** exit engine.

Today `ap_exit_engine.evaluate_exit()` returns `CLOSE_ALL` immediately when fresh underlying truth says `pos.is_at_target`. That means a position can reach its original scanner target while price is still moving strongly toward a nearby FVG/VI attraction area and the bot has no current-main mechanism to distinguish:

- target reached + continuation weak/stalling -> close at the original target;
- target reached + strong continuation + nearby proven attraction level -> hold/extend toward the attraction area;
- target reached into an opposing high-timeframe wall without reclaim/break confirmation -> protect/close, not extend;
- attraction level reached + rejection/stall -> close;
- attraction level broken/reclaimed with separately proven continuation -> runner policy may continue, but this PR must not silently invent unlimited target extension.

Concrete intended behavior:

```
CALL entry underlying: 100.00
original target:        102.00
next bearish FVG front: 103.50

price reaches 102.00
+ current market evidence shows strong continuation
+ FVG identity/data is fresh and unambiguous
+ no hard/EOD/technical-stop authority requires exit
=> do not blindly close at 102.00
=> expose a bounded hold/extension state toward approximately 103.50
=> close when the FVG front is reached/rejected or continuation fails
```

The same logic must work directionally for PUTs.

This is **not** permission to weaken risk controls, create a new exit submitter, or let an intelligence score own money.

---

# 1. Mandatory dependencies and integration order

Before production implementation is considered mergeable:

1. **PR #428** exact broker EXIT-fill/proof truth must be finalized so this PR cannot regress exit/P&L evidence.
2. **PR #429** canonical position ownership and standing-protection identity must be finalized so this PR cannot introduce a second exit owner.
3. Rebase or rebuild this branch onto the then-current `main`. Do not cherry-pick stale exit-engine implementations from historical branches.
4. Inventory all current FVG and VI producers/consumers before coding. Reuse existing canonical data where it is production-valid; do not create a duplicate zone engine because an old PR used a different shape.
5. Preserve current hard-stop, EOD-close, technical-stop, touched-profit, executable-BID, stale-quote, retry, canonical ownership, and broker-truth contracts.

Historical FVG PRs/branches are design evidence only. Current `main` is authority.

---

# 2. Existing current-main evidence Codex must preserve

At the current-main baseline used to write this spec:

- `ap/fair_value_gap.py` already has FVG lifecycle truth (`unfilled`, partial fill, midpoint touch, filled, broken/reclaimed), side-aware opposing/aligned classification, path intersection, 4H/1H confirmation rules, and `target_guidance`.
- `target_guidance.action == "extension_candidate_to_opposing_fvg_front"` already describes the exact magnet behavior needed here, but it is intentionally non-mutating.
- `ap_exit_engine.evaluate_exit()` currently checks fresh underlying target truth and immediately returns `CLOSE_ALL` for `TARGET HIT` before the lower soft-profit/runner branches.
- Current exit logic already contains exact executable-option truth, technical-stop authority, hard-stop precedence, EOD precedence, profit floors, runner trails, and retry/degraded-monitoring behavior.

Codex must wire intelligence **around those authorities**, not replace them.

---

# 3. Core architectural invariant

The target-intelligence layer is a **decision input**, not a broker authority.

Required ownership chain:

```
canonical position identity
    -> current exit-engine evaluation snapshot
    -> FVG / VI target-intelligence evaluation
    -> deterministic ExitDecision
    -> existing exit submission / ownership path
    -> existing order state machine / broker truth
```

Forbidden:

- direct broker submit from the FVG/VI helper;
- direct cancel/replace from the helper;
- direct `orders`, `positions`, `proof_trades`, or queue mutation from the helper;
- a second scheduler or exit worker;
- a second position owner;
- target modification stored as truth without position/client/mode/generation identity;
- using PAPER or unknown-mode evidence to authorize LIVE behavior;
- falling back to stale/missing FVG or continuation data as if it were positive confirmation.

Unknown or unavailable intelligence must degrade to the existing deterministic target behavior, not manufacture a hold.

---

# 4. Required decision model

Implement a pure or effectively pure policy object/helper with an explicit result contract. Names may follow repository conventions, but the result must be semantically equivalent to:

```
TargetIntelligenceDecision(
    disposition,
    original_target,
    effective_target,
    attraction_type,
    attraction_low,
    attraction_high,
    attraction_front,
    continuation_state,
    continuation_evidence,
    zone_state,
    data_quality,
    reason_code,
    diagnostics,
)
```

Allowed dispositions should be bounded and explicit, for example:

- `KEEP_ORIGINAL_TARGET`
- `HOLD_TOWARD_ATTRACTION`
- `CLOSE_AT_ORIGINAL_TARGET`
- `CLOSE_AT_ATTRACTION_FRONT`
- `CLOSE_ON_ATTRACTION_REJECTION`
- `HOLD_AFTER_CONFIRMED_BREAK` only if runner logic already owns the continuation; this PR must not create infinite extension
- `INTELLIGENCE_UNAVAILABLE`
- `IDENTITY_UNPROVEN`
- `DATA_STALE`
- `AMBIGUOUS_ZONE`

Do not encode behavior as arbitrary score thresholds with hidden meaning. The reason for every hold/close must be inspectable.

---

# 5. Identity requirements

Every authoritative evaluation must be scoped to the exact position:

- normalized `client_id`;
- exact `execution_mode` (`live` or `paper`, never inferred from transport);
- canonical `position_id`;
- exact OCC option contract for the managed position;
- underlying ticker;
- side (`CALL`/`PUT`);
- current position quantity/generation where current exit ownership uses it;
- evaluation timestamp.

If durable guidance is persisted, it must also carry a deterministic policy/version identifier and source timestamps/hashes so a restart cannot confuse old guidance with a new position generation.

A same-ticker FVG calculation for another client/mode/position may be analytically reusable only if it is market-data-only and immutable; it may not carry position authority across identities.

---

# 6. FVG / VI source-of-truth requirements

Codex must first inventory the repository and document which current modules/tables produce:

- 4H FVGs;
- 1H FVGs;
- any 15m/5m FVG confirmation currently used;
- Volume Imbalance / VI zones, if a current canonical implementation exists;
- candle source and timestamps;
- current underlying quote source and timestamp;
- any existing continuation/strength measurements.

Do not assume an old branch shape is still valid.

For each zone used in a money-adjacent decision, require:

- finite numeric bounds;
- correct low/high ordering;
- direction/type;
- timeframe;
- lifecycle state;
- source timestamp or source-candle identity;
- freshness relative to the evaluation clock;
- no ambiguous duplicate candidate at the same authority level without deterministic ranking.

If VI does not currently have a canonical production implementation, this PR may add a **pure analytical VI detector/evaluator**, but it must remain observe-only until separately proven. FVG alone must not be blocked by an absent VI module.

Do not let malformed VI/FVG data cause an exception that suppresses the existing target exit.

---

# 7. Directional attraction logic

For a CALL:

- price advances upward;
- the next opposing bearish FVG/VI above the original target may act as an attraction/exit area;
- use the **front boundary approached from below** as the first bounded extension target unless existing canonical policy proves another boundary;
- an unconfirmed opposing 4H wall in the path is not a reason to extend through it;
- a confirmed reclaim/break can remove the wall, but further continuation must remain under existing runner policy.

For a PUT:

- price advances downward;
- the next opposing bullish FVG/VI below the original target may act as an attraction/exit area;
- use the **front boundary approached from above**;
- same 4H confirmation rule in the opposite direction.

Do not use absolute distance without side direction. A zone behind price is not an attraction target for this decision.

---

# 8. Continuation-strength contract

The user intent is specifically: **only hold past the original target when the move is strong.**

Codex must implement continuation strength from deterministic current evidence, not an LLM judgment.

Preferred evidence order:

1. existing canonical strength fields already captured by current intelligence/snapshot code, if available and fresh;
2. otherwise a narrowly scoped pure calculation from fresh market data already available to the exit engine.

At minimum the calculation should be able to distinguish:

- `STRONG_CONTINUATION`
- `NEUTRAL_OR_UNPROVEN`
- `WEAKENING`
- `REVERSAL_OR_REJECTION`
- `DATA_UNAVAILABLE`

The implementation should consider existing project intent around 15m/5m continuation and zone penetration, but must inventory actual current code before choosing inputs. Candidate evidence can include:

- side-aware 15m/5m closes or body progression;
- fresh underlying movement through the original target;
- whether price is accepting beyond vs wicking/rejecting;
- volume/relative-volume only if current source is proven and timestamped;
- distance/extension from trigger so the bot does not chase a parabolic move;
- FVG/VI penetration/rejection;
- fresh quote timestamps.

Do not require every optional signal. Missing optional data must yield unproven/neutral, not a fabricated negative or positive.

Critically: a strong option P&L alone is **not** enough to certify underlying continuation through an FVG wall.

---

# 9. Precedence inside `evaluate_exit()`

This PR must explicitly preserve safety precedence.

Codex must trace current `evaluate_exit()` order and write tests proving the final precedence. The intended high-level contract is:

1. identity/evaluation snapshot validity;
2. catastrophic hard-stop authority;
3. EOD force-close authority;
4. confirmed technical stop and any other current non-negotiable safety exits;
5. touched-profit/profit-protection rules where current architecture gives them precedence;
6. target-intelligence decision at the target seam;
7. existing scale/runner/soft-exit logic.

Because current main checks `TARGET HIT` before hard/EOD pre-evaluation, Codex must **not casually reorder the entire exit engine** in this PR. Instead, find the narrowest integration that lets target guidance defer the ordinary target close without weakening hard/EOD/technical authorities. If a small precedence correction is required, prove it with existing P0 tests plus new adversarial tests.

The FVG/VI layer may suppress only the **ordinary target close** when all required extension evidence is proven. It may never suppress:

- hard catastrophic stop;
- EOD forced exit;
- confirmed technical stop;
- emergency/kill/manual-close authority;
- broker reconciliation truth;
- a current profit floor or runner protection branch that has higher established precedence after the architecture trace.

Document the exact final ordering in the PR body.

---

# 10. Effective-target lifecycle

Do not permanently mutate `underlying_target` just because one evaluation saw an FVG.

Preferred model:

- preserve `original_target` immutable for the position;
- derive an `effective_target` / `target_extension_candidate` for the current evaluation or a versioned bounded guidance state;
- if guidance is persisted for restart continuity, store it separately with identity, policy version, source zone identity, source timestamps, and expiry/invalidation rules;
- on stale/missing zone data after restart, fail back to protected existing behavior rather than blindly continuing an old extension.

Required invalidation conditions include at least:

- position identity/generation changes;
- zone becomes broken/reclaimed/invalid under the policy;
- zone is no longer ahead of price in the trade direction;
- continuation strength degrades or rejects;
- market data becomes stale;
- EOD/hard/technical safety authority fires;
- position closes or quantity reaches zero.

---

# 11. Interaction with scale-outs and runners

This PR is **not** a wholesale rewrite of runner management.

Required rules:

- one-contract positions must never attempt an impossible scale-out;
- if current runner/scale policy already left a runner, FVG/VI guidance may provide a bounded destination/exit context but not create a duplicate runner state machine;
- original-target extension must not reset `peak_pnl_pct`, touched-profit state, profit floors, or trailing state;
- reaching the FVG/VI front should normally return to existing close/runner authority, not silently create another recursive attraction target;
- a break through the first zone can be recorded for future intelligence/outcome attribution, but this PR does not authorize unlimited target hopping.

Keep #291/#292 historical volatility-ladder work separate. Reuse current concepts only if they still fit after tracing current main; do not import stale state machinery wholesale.

---

# 12. LIVE / PAPER rollout

Implementation must be staged:

### Stage 0 — observe only

Default. Compute and emit what target intelligence **would** have done while the existing target behavior remains authoritative.

Required telemetry per evaluated target event:

- exact client/mode/position/contract identity;
- original target;
- current underlying;
- selected FVG/VI zone and timeframe;
- zone front/bounds/lifecycle;
- continuation state and evidence timestamps;
- proposed disposition/effective target;
- actual existing exit decision;
- policy version.

### Stage 1 — PAPER authoritative

Only after observe-only replay proves no safety/preference regressions. PAPER may allow `HOLD_TOWARD_ATTRACTION` to affect ordinary target behavior.

### Stage 2 — LIVE shadow

LIVE still follows existing behavior while recommendation lift and false-hold risk are measured through canonical #432 outcome bindings / #438 promotion evidence when those are available.

### Stage 3 — limited LIVE

Requires separate explicit approval and evidence gate. Do not enable in this PR by default.

No environment default may silently activate LIVE target extension.

---

# 13. Required failure matrix

Codex must add tests for at least all of these:

## Identity

1. missing client -> no authoritative extension;
2. missing/invalid mode -> no authoritative extension;
3. wrong position ID -> no authoritative extension;
4. same ticker/contract foreign client evidence -> cannot control target;
5. PAPER recommendation cannot authorize LIVE.

## Data quality

6. missing current underlying -> existing safe behavior / HOLD according to current target-truth contract, never fabricate target hit;
7. stale underlying -> no authoritative extension;
8. malformed FVG bounds -> no authoritative extension;
9. stale FVG candle set -> no authoritative extension;
10. ambiguous same-rank attraction zones -> explicit ambiguity disposition;
11. missing VI -> FVG can still evaluate if VI is optional;
12. missing continuation data -> keep original target, not extend.

## CALL geometry

13. target 102, bearish FVG front 103.50, strong continuation -> extension candidate/HOLD toward 103.50;
14. same geometry, weak continuation -> close at original target;
15. same geometry, FVG behind current price -> no extension;
16. unconfirmed opposing 4H FVG at/before path -> do not extend through it;
17. confirmed reclaim + strong continuation -> bounded continuation permitted according to policy;
18. reach FVG front then reject -> close;
19. wick through front but no acceptance -> rejection/stall path, not automatic endless extension.

## PUT geometry

Mirror 13–19 downward.

## Safety precedence

20. hard stop and target extension simultaneously true -> hard stop wins;
21. EOD and extension true -> EOD wins;
22. confirmed technical stop and extension true -> stop wins;
23. manual/emergency/kill ownership -> extension cannot submit/cancel anything;
24. stale option BID must not be bypassed by FVG intelligence where existing soft-profit exit requires executable truth;
25. profit-floor/runner protection precedence remains exactly as audited.

## Restart/state

26. observe-only guidance restart -> no behavioral mutation;
27. persisted guidance with wrong position generation -> ignored/HOLD;
28. expired/stale guidance -> invalidated;
29. closed position -> guidance cannot resurrect monitoring or order submission;
30. process dies after guidance computation but before any exit decision -> restart remains idempotent.

## Broker money path

31. pure guidance evaluation makes zero broker calls;
32. HOLD/KEEP recommendation makes zero broker calls by itself;
33. a resulting close travels through the existing single exit submission path exactly once;
34. no new cancel path;
35. no direct proof/position/order/queue mutation from the intelligence helper.

---

# 14. Required tests and CI

At minimum add focused tests around:

- pure target-intelligence policy;
- real `evaluate_exit()` target seam;
- CALL/PUT FVG geometry;
- VI integration if VI is implemented/reused;
- stale/ambiguous data;
- one-contract and multi-contract runner behavior;
- current touched-profit/runner precedence;
- hard/EOD/technical-stop precedence;
- LIVE/PAPER isolation;
- exact identity preservation;
- restart persistence only if this PR adds durable guidance;
- zero direct broker mutations from the helper.

Run all existing exit safety suites touching:

- executable BID truth;
- touched-profit confirmation;
- one-contract runner precedence;
- technical stops;
- hard stop;
- EOD;
- exit retry/degraded monitoring;
- canonical position ownership;
- reconciler exact EXIT-fill truth after #428 lands.

GitHub exact-head P0 workflows must all pass on the final implementation SHA.

Do not claim local skipped PostgreSQL tests as proof. If a migration is introduced, add it to the sanctioned migration runner/schema attestation path and prove it on PostgreSQL CI.

---

# 15. Diagnostics / observability

Every nontrivial target-intelligence result must carry a stable reason code, not only prose.

Suggested event family:

- `EXIT_TARGET_INTELLIGENCE_KEEP_ORIGINAL`
- `EXIT_TARGET_INTELLIGENCE_HOLD_TO_FVG`
- `EXIT_TARGET_INTELLIGENCE_HOLD_TO_VI`
- `EXIT_TARGET_INTELLIGENCE_CLOSE_ORIGINAL`
- `EXIT_TARGET_INTELLIGENCE_CLOSE_ATTRACTION`
- `EXIT_TARGET_INTELLIGENCE_REJECTION`
- `EXIT_TARGET_INTELLIGENCE_UNAVAILABLE`
- `EXIT_TARGET_INTELLIGENCE_AMBIGUOUS`

Diagnostics must never contain a claim that a recommendation executed unless the existing order/broker lifecycle later proves it.

Preserve exact `client_id`, `execution_mode`, `position_id`, contract, source timestamps, and policy version in emitted metadata.

---

# 16. Non-goals

This PR must NOT:

- alter scanner thresholds;
- alter selector quality gates;
- alter entry sizing;
- create entry/re-entry authority;
- create an LLM trading decision;
- loosen spreads/OI/volume/delta requirements;
- change hard-stop thresholds;
- change EOD policy;
- change broker submit/cancel plumbing;
- replace canonical position ownership;
- change reconciler proof truth;
- create a generic all-stage recommendation engine;
- recursively chase unlimited FVGs;
- make #291's stale volatility ladder LIVE by implication.

---

# 17. Required Codex implementation report

Before requesting review, Codex must update the PR body with:

1. exact base SHA and final head SHA;
2. complete changed-file list;
3. current-main FVG/VI producer/consumer inventory;
4. exact target-seam call graph from `evaluate_exit()` through submission;
5. final precedence table for hard/EOD/technical/profit/target/runner paths;
6. every new configuration flag and default;
7. every DB/schema change, or explicit `none`;
8. every broker submit/cancel reachable from changed code, proving none were added;
9. every `orders`, `positions`, `proof_trades`, queue mutation reachable from changed code;
10. LIVE/PAPER behavior difference and rollout stage;
11. focused test commands and exact pass counts;
12. exact-head GitHub workflow run IDs/conclusions;
13. known limitations/held cases;
14. explicit answers: could this make Jason enter junk? could it suppress a mandatory exit? could it create duplicate exit submission? could stale FVG/VI data hold a position past target?

Leave the PR **Draft / HARD HOLD** after implementation. Independent whole-PR review and explicit release authorization are required.

---

# 18. Acceptance criteria

This work is complete only when all are true:

- the current exit engine can distinguish ordinary target hit from a proven bounded continuation opportunity toward the next valid FVG/VI attraction area;
- extension requires fresh, side-correct, identity-safe zone and continuation evidence;
- missing/stale/ambiguous intelligence cannot create a hold past target;
- mandatory risk exits retain precedence;
- no second exit owner, submitter, scheduler, queue, or proof authority exists;
- one-contract and runner behavior remains coherent;
- observe-only is the default and LIVE authority remains disabled absent explicit later promotion;
- exact-head tests and CI are green;
- the final audit finds zero money-path regressions.
