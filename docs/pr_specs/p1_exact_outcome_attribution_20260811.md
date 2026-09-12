# P1 SPEC — Exact Outcome Attribution from Frozen Intelligence Evidence

## Status

**DRAFT / HARD HOLD. SPEC ONLY. DO NOT MERGE OR DEPLOY AS AN IMPLEMENTATION.**

This branch intentionally contains only this work order. Codex must implement on this branch after the truth-binding prerequisites below are stable. The implementation must remain read-only / evidence-plane by default and must not acquire entry, exit, broker, queue, sizing, or ranking authority.

No merge, deployment, worker enablement, database migration application, or trading-authority promotion is authorized by this spec.

## Purpose

Angel Precision needs to move from merely knowing whether a trade won or lost to understanding **which pre-trade and in-trade conditions were present when that exact trade produced that exact outcome**.

This is not a causal-claim engine. The first version must build deterministic, auditable attribution facts such as:

- entry occurred immediately after trigger vs after retest;
- entry was extended from the trigger;
- spread was near the allowed ceiling;
- contract delta/DTE/liquidity profile;
- 4H/1H FVG path state;
- FVG/VI attraction or resistance in the path;
- breach-time 15m/5m strength state;
- underlying/target geometry;
- time of day;
- maximum favorable excursion / maximum adverse excursion when a canonical source exists;
- target was reached before reversal;
- exit occurred materially before later MFE;
- stop/target/trail/runner exit taxonomy;
- winner/loss and exact realized option return from canonical proof truth.

The output must support later questions like:

```
Among LIVE_OFFICIAL trades where a PRETRIGGER/PREOPEN snapshot proved:
- CALL
- fresh 15m strength
- first retest after trigger
- no opposing 4H FVG in path

what were:
- sample count
- win rate
- expectancy
- median return
- MFE / MAE distribution
- exit efficiency
```

The system must never answer that by fuzzy ticker/time joins, by current/recomputed market context, or by allowing outcome data to leak backward into the frozen pre-entry features.

---

# 1. Mandatory dependencies

Implementation must wait until these are stable on current `main`:

1. **PR #432** canonical intelligence outcome binding is amended and independently approved. This PR must consume its exact snapshot -> proof binding; it must not create a competing fuzzy join.
2. **PR #428** exact reconciler EXIT-fill/proof provenance is finalized so terminal economics and EXIT identity used by attribution are trustworthy.
3. **PR #435** canonical breach-time setup intelligence, if/when implemented, should become a preferred frozen feature source rather than recomputing setup context after the trade.
4. **PR #437** selected-contract pre-submit intelligence, if/when implemented, should become the preferred frozen contract feature source.
5. **PR #438** remains the separate profitability/promotion gate. This PR supplies attribution evidence; it does not itself promote intelligence into LIVE authority.

If #435/#437 are not yet implemented when Codex begins, design the attribution model so new frozen snapshot fields can be consumed later without changing identity semantics. Do not block the base attribution of fields already present in #432-bound snapshots/proofs.

---

# 2. Canonical truth chain

The only acceptable executed-trade attribution chain is:

```
ap_intelligence_snapshots
    exact immutable snapshot identity/hash
        -> ap_intelligence_outcome_bindings  (#432)
            exact binding identity
                -> proof_trades
                    exact terminal proof / economics / provenance
```

For LIVE executed outcomes:

- binding must resolve to exact `LIVE_OFFICIAL` proof truth;
- client identity must match exactly;
- execution mode must be `live` exactly;
- originating ENTRY local order identity must be proven by #432;
- proof eligibility/taxonomy must still be valid at attribution time;
- any existing binding must be revalidated according to the final #432 contract, not blindly trusted because a row exists.

For PAPER:

- keep a separate `PAPER_UNVERIFIED` / research-only taxonomy;
- never combine PAPER with LIVE in a metric that is later represented as LIVE performance evidence;
- every aggregation must expose the mode/taxonomy filter used.

Unknown/malformed mode is quarantined and cannot be silently defaulted to PAPER or LIVE.

Forbidden joins:

- ticker + date;
- ticker + approximate timestamp;
- signal text;
- most recent proof;
- same OCC contract without exact bound identity;
- current `positions` row by ticker;
- legacy `signal_outcomes` fuzzy identity;
- legacy `intelligence_bridge.record_trade_outcome(ticker, signal_id, pnl_pct)`.

---

# 3. Fundamental anti-leakage invariant

**Pre-decision features must come only from evidence frozen before the relevant action/outcome.**

Outcome facts may be joined afterward, but they may never modify or replace the frozen input values used to describe what the system knew at decision time.

Example:

Bad:

```
trade lost
-> recompute today's chart
-> label the old setup "weak momentum"
```

Required:

```
frozen PRETRIGGER/PREOPEN/BREACH snapshot had strength_state=WEAK
+ exact bound proof later lost -12.4%
-> attribution fact: pre_trade_strength_state=WEAK, outcome=-12.4%
```

If a feature was unavailable at decision time, preserve `UNKNOWN` / `MISSING` / `NOT_CAPTURED`. Do not backfill it from future candles and present it as pre-trade knowledge.

Post-trade path metrics such as MFE/MAE are explicitly post-outcome features and must live in a separate namespace from pre-trade evidence.

---

# 4. Terminology: attribution, not causation

This PR must not claim “the trade lost because X” unless a later experimental/statistical layer proves causality.

Use language like:

- `observed_pretrade_conditions`
- `observed_execution_conditions`
- `observed_exit_conditions`
- `outcome_metrics`
- `association_tags`

Avoid fields named `cause`, `root_cause`, or `reason_trade_lost` for analytical inference.

The system can produce deterministic labels such as `ENTRY_EXTENDED_FROM_TRIGGER` if the frozen numbers prove it. That is an observed classification, not a causal assertion.

---

# 5. Required attribution record model

Codex must implement one canonical versioned attribution result per **binding + attribution policy version**.

A conceptual shape:

```
OutcomeAttribution(
    binding_id,
    snapshot_id,
    proof_id,
    client_id,
    execution_mode,
    proof_taxonomy,
    canonical_signal_id,
    position_id,
    entry_local_order_id,
    snapshot_phase,
    snapshot_revision,
    snapshot_input_hash,
    snapshot_config_hash,
    snapshot_profile_hash,
    attribution_policy_version,

    pretrade_features={...},
    execution_features={...},
    path_features={...},
    exit_features={...},
    outcome={...},

    data_quality={...},
    missing_features=[...],
    evidence_refs={...},
    generated_at,
)
```

If persisted, the durable key must make recomputation/versioning explicit. Do not overwrite an older attribution version in place and pretend the historical analytical policy never changed.

Preferred uniqueness:

```
(binding_id, attribution_policy_version)
```

or an equivalent immutable versioned key.

If no persistence is necessary for the first implementation, a deterministic read-only builder is acceptable, but #438 and reporting consumers must be able to reproduce the exact same result from the same frozen inputs/version.

---

# 6. Feature namespaces

Keep feature families separate so future training/evaluation code cannot accidentally use post-outcome knowledge as an entry input.

## A. `pretrade_features`

Only frozen before entry/decision. Candidate fields, when actually present/proven in current snapshots:

- signal side;
- scanner/source setup identity;
- trigger price;
- scanner target / underlying target;
- stored technical stop geometry;
- distance trigger -> target;
- remaining R at snapshot time;
- current underlying vs trigger distance;
- extension beyond trigger in dollars, percent, ATR/vol units if captured;
- same-session gap/re-breach lineage if captured;
- 4H / 1H FVG state;
- opposing FVG in path;
- FVG attraction candidate;
- VI state if canonical and captured;
- 15m strength state;
- 5m confirmation state;
- volume/relative-volume state if captured;
- VWAP relationship if captured;
- regime/advisory fields if they were truly available at the time;
- time of day/session bucket.

Do not invent missing fields from current market data.

## B. `execution_features`

Use frozen selected-contract / entry evidence, never current chain values:

- contract symbol;
- option side;
- strike;
- expiry / DTE;
- delta;
- spread pct;
- bid/ask/mid/entry executable price as captured;
- OI;
- volume;
- chain/quote provenance;
- requested qty and actual filled qty when canonical;
- entry local order ID / broker order ID references where allowed;
- entry fill price and timestamp from canonical order/proof lineage;
- trigger-to-entry latency;
- underlying trigger-to-fill movement if both timestamps/prices are proven.

## C. `path_features`

Explicitly post-entry / post-decision analytics. These cannot be fed back as pre-entry features for the same trade.

Candidate metrics only if sourced canonically:

- MFE option pct;
- MAE option pct;
- MFE underlying pct;
- MAE underlying pct;
- time to MFE;
- time to MAE;
- time to first +10/+15/+25/etc threshold;
- time to first adverse threshold;
- original target touched timestamp;
- FVG/VI attraction front touched timestamp;
- rejection/break acceptance state after touch;
- maximum return after actual exit within a bounded diagnostic window, clearly marked counterfactual/research only.

Do not derive these from unreliable LAST-only or stale mark truth if executable BID truth is required by the existing proof/exit architecture. Codex must inventory the current QPM/price-history sources and document which metrics are executable vs underlying-only vs research-only.

## D. `exit_features`

- exact exit local order ID;
- exact broker exit order ID;
- exact filled timestamp/qty/price where #428 provenance exists;
- exit reason code/taxonomy;
- scale-out count;
- runner state;
- touched-profit state;
- peak P&L captured before exit if canonical;
- hard stop / technical stop / target / profit lock / runner trail / EOD / manual / reconciler classification;
- target-intelligence recommendation/version if future B work exists and was recorded before the exit decision.

## E. `outcome`

Canonical terminal economics only:

- realized option P&L pct;
- realized dollar P&L if canonically available;
- win bool;
- contracts;
- entry option price;
- exit option price;
- proof taxonomy;
- proof eligibility flags;
- proof ID.

No recomputation from current marks.

---

# 7. Deterministic derived labels

Implement small pure classifiers only where inputs prove the label.

Examples:

### Entry timing

- `ENTRY_AT_OR_NEAR_TRIGGER`
- `ENTRY_EXTENDED_FROM_TRIGGER`
- `ENTRY_AFTER_RETEST`
- `ENTRY_AFTER_REBREACH`
- `ENTRY_TIMING_UNKNOWN`

Thresholds must come from explicit versioned policy/config, not magic undocumented constants.

### FVG / VI path

- `NO_OPPOSING_ZONE_IN_PATH`
- `OPPOSING_FVG_IN_PATH`
- `TARGET_BEFORE_FVG_MAGNET`
- `ENTRY_INSIDE_OPPOSING_FVG`
- `FVG_STATE_UNKNOWN`

### Momentum / confirmation

Only map exact frozen source fields to stable normalized categories. Do not reconstruct from future candles.

### Exit efficiency

If canonical MFE and exact exit return exist:

```
exit_capture_ratio = realized_return / max_favorable_return
```

Use careful sign/zero semantics. Do not label a losing trade “bad exit” merely because later MFE became positive unless the counterfactual data source is explicitly research-only and bounded.

Potential facts:

- `EXIT_CAPTURED_HIGH_SHARE_OF_MFE`
- `EXIT_LEFT_MATERIAL_MFE`
- `EXIT_AFTER_LARGE_GIVEBACK`
- `EXIT_EFFICIENCY_UNPROVEN`

Again, association labels, not causal verdicts.

---

# 8. Identity and corruption handling

Attribution must fail closed for identity ambiguity.

Required checks:

- binding row exists exactly once;
- binding revalidation succeeds under final #432 rules;
- snapshot ID/hash fields match binding;
- proof ID matches binding;
- exact client equality;
- exact execution-mode equality;
- exact ENTRY local-order identity where required;
- proof taxonomy still eligible for the requested analytical cohort;
- snapshot immutable hash/config/profile fields are not malformed;
- duplicate candidate proofs -> HOLD;
- duplicate conflicting bindings -> HOLD;
- proof demoted after binding -> no LIVE-eligible attribution;
- mode changed/corrupted -> quarantine;
- missing snapshot -> HOLD;
- unknown snapshot phase -> retain but mark unsupported, never guess phase.

No “newest wins” on ambiguous evidence.

---

# 9. Data-quality contract

Every attribution result must carry machine-readable quality state.

At minimum distinguish:

- `PROVEN`
- `PARTIAL`
- `MISSING_OPTIONAL_FEATURES`
- `IDENTITY_UNPROVEN`
- `PROOF_INELIGIBLE`
- `BINDING_CONFLICT`
- `SNAPSHOT_HASH_MISMATCH`
- `SOURCE_STALE_OR_UNPROVEN`
- `UNSUPPORTED_LEGACY_SHAPE`

A partial attribution may be valid for reporting only if the core snapshot->binding->proof identity is proven. Optional missing features do not justify fuzzy backfill.

---

# 10. LIVE / PAPER / legacy cohort isolation

Every reporting/aggregation API must require an explicit cohort scope.

Minimum scope:

```
client_id or explicit aggregate authorization
execution_mode
proof_taxonomy
attribution_policy_version
snapshot phase/profile/version filters as applicable
```

Forbidden default:

```
all rows regardless of live/paper/legacy
```

LIVE performance evidence must never include:

- PAPER proofs;
- `PAPER_UNVERIFIED` bindings;
- unknown mode;
- synthetic/ineligible LIVE proofs;
- legacy fuzzy outcomes;
- unbound snapshots.

If the system offers combined research views, label them explicitly as research and never feed them to #438 LIVE promotion metrics without a separate approved rule.

---

# 11. Aggregation/reporting layer

Provide deterministic read-only aggregation helpers for future #438 and operator analysis.

At minimum support grouped metrics by one or more attribution labels/features with:

- sample count;
- wins/losses;
- win rate;
- mean return;
- median return;
- expectancy;
- return dispersion/quantiles where sensible;
- MFE/MAE summaries when available;
- exit capture ratio when available;
- missing-data counts;
- cohort identity/version.

Never hide the denominator. A 100% win rate on 1 trade must remain visibly `n=1`.

Do not implement automated strategy mutation or threshold tuning in this PR.

---

# 12. Interaction with #438 profitability promotion

#438 is the authority that decides whether an intelligence policy has enough evidence to progress through rollout stages.

This PR should make #438 more honest by giving it clean cohorts and attribution facts, but it must not:

- change #438 thresholds;
- mark a policy profitable by itself;
- enable LIVE;
- write ranking weights;
- mutate Gate G;
- change selector admission;
- change exit policy.

Expose read-only APIs/results that #438 can consume later.

---

# 13. Legacy paths that must stay non-authoritative

Codex must inventory and explicitly classify at least:

- `ap/performance_tracker.py`;
- `intelligence_bridge.record_trade_outcome`;
- `ap_feedback_loop.py` / `signal_outcomes`;
- decision/audit event tables;
- any historical trade-review table;
- any old ranking/performance reader;
- PR #337-era outcome/profitability implementation remnants.

None may become the canonical join merely because they contain convenient fields.

Where useful, legacy data can be reported separately as `LEGACY_DIAGNOSTIC_UNBOUND`, never silently upgraded.

---

# 14. Required failure/adversarial matrix

Codex must implement tests for at least:

## Binding/proof identity

1. exact valid LIVE binding + eligible LIVE_OFFICIAL proof -> attribution succeeds;
2. wrong proof ID in binding -> HOLD;
3. duplicate proof candidates -> HOLD;
4. proof taxonomy demoted after binding -> LIVE attribution ineligible;
5. client mismatch -> HOLD;
6. mode mismatch -> HOLD;
7. ENTRY local-order mismatch -> HOLD;
8. snapshot hash mismatch -> HOLD;
9. conflicting duplicate binding -> HOLD;
10. PAPER binding requested as LIVE cohort -> excluded/HOLD.

## Anti-leakage

11. frozen snapshot lacks momentum field but future candles have it -> attribution keeps missing; no backfill;
12. pre-entry feature differs from recomputed post-trade value -> frozen value wins;
13. post-trade MFE field never appears inside `pretrade_features`;
14. outcome/win cannot affect any derived pretrade label;
15. rerunning after market changes produces identical pretrade attribution for the same snapshot/version.

## Numeric corruption

16. NaN/Inf prices -> affected metric unproven, not propagated;
17. bool masquerading as quantity/price -> reject;
18. zero/negative contract qty where positive required -> reject metric/proof;
19. naive/malformed timestamps -> do not create ordered latency metric;
20. exit before entry timestamp -> HOLD terminal attribution.

## Cohort isolation

21. LIVE aggregation excludes PAPER;
22. PAPER aggregation excludes LIVE;
23. unknown mode excluded from both;
24. ineligible LIVE proof excluded;
25. legacy fuzzy outcome excluded.

## Aggregation

26. sample count exact;
27. no divide-by-zero on empty cohort;
28. single-sample cohort visibly n=1;
29. missing MFE/MAE does not drop the row from return metrics unless explicitly filtered;
30. policy version separation prevents old/new attribution semantics from mixing silently.

## Idempotency/versioning

31. same binding + same version recomputation deterministic;
32. same binding + new policy version creates separate result, not overwrite;
33. conflict on existing persisted attribution with mismatched immutable fields -> HOLD/no overwrite;
34. concurrent same-result persistence is idempotent if persistence is implemented.

## Money path

35. attribution builder makes zero broker calls;
36. zero order submit/cancel calls;
37. zero `orders` mutation;
38. zero `positions` mutation;
39. zero queue mutation;
40. zero `proof_trades` economics mutation;
41. zero Gate G/selector/sizing mutation.

---

# 15. Persistence, if needed

Prefer a pure deterministic attribution builder first. If durable persistence is required for scale/reporting, use an append-only/versioned evidence table, for example conceptually:

```
ap_intelligence_outcome_attributions
```

Required immutable references:

- binding ID;
- snapshot ID;
- proof ID;
- client ID;
- execution mode;
- policy version;
- snapshot hashes;
- attribution payload hash;
- generated timestamp.

Never copy realized proof economics into a mutable table and later treat the copy as more authoritative than `proof_trades`. If cached, the attribution row is a derived artifact whose provenance points back to proof truth.

Any migration must:

- be idempotent;
- use sanctioned migration runner conventions;
- be added to schema attestation as appropriate without creating a new broker startup gate unless necessary;
- include index/constraint definitions and PostgreSQL CI;
- never be applied to production from this PR task.

---

# 16. Required module boundaries

Exact names may follow current repository conventions, but prefer separation like:

- `ap/intelligence_outcome_attribution.py` — exact builder/classifiers;
- `ap/intelligence_outcome_metrics.py` — read-only aggregation if separation helps;
- tests dedicated to exact binding/anti-leakage/cohort behavior.

Do not stuff attribution into `ap_exit_engine.py`, selector, master control, or broker modules.

The binding module from #432 remains the identity authority. Attribution consumes it.

---

# 17. Observability / operator output

Expose enough diagnostic detail to answer why a row was excluded.

Suggested dispositions/events:

- `OUTCOME_ATTRIBUTION_PROVEN`
- `OUTCOME_ATTRIBUTION_PARTIAL`
- `OUTCOME_ATTRIBUTION_BINDING_UNPROVEN`
- `OUTCOME_ATTRIBUTION_PROOF_INELIGIBLE`
- `OUTCOME_ATTRIBUTION_HASH_CONFLICT`
- `OUTCOME_ATTRIBUTION_MODE_CONFLICT`
- `OUTCOME_ATTRIBUTION_LEGACY_EXCLUDED`

Do not log full sensitive account data unnecessarily. Preserve exact internal identity in structured evidence where required.

---

# 18. Required test/CI evidence

Run focused suites for:

- #432 outcome binding;
- proof taxonomy/eligibility;
- proof provenance after #428;
- snapshot immutability/materialization;
- LIVE/PAPER isolation;
- new attribution anti-leakage tests;
- aggregation math;
- PostgreSQL persistence/concurrency if a table is introduced.

Also run current P0 regression workflows on the exact final SHA.

If tests are skipped due to missing local Postgres, say so. Exact-head GitHub PostgreSQL workflow evidence must cover the DB-specific behavior before review.

`git diff --check` and `py_compile` for changed Python modules must pass.

---

# 19. Required Codex implementation report

Before requesting review, update the PR body with:

1. exact base/main SHA and final head SHA;
2. changed-file list;
3. snapshot/binding/proof call graph;
4. exact binding revalidation path used from final #432;
5. feature inventory grouped into pretrade/execution/path/exit/outcome;
6. explicit source and capture time for every authoritative feature family;
7. list of fields intentionally unavailable rather than backfilled;
8. all taxonomy/cohort rules;
9. attribution policy/versioning contract;
10. persistence schema/migration details or explicit `none`;
11. complete legacy producer/consumer inventory and why each remains non-authoritative;
12. all DB writes reachable from changed code;
13. proof that there are zero broker/order/position/queue/trading-authority mutations;
14. focused test commands/pass counts;
15. exact-head workflow run IDs/conclusions;
16. examples of one LIVE proven attribution, one PAPER research attribution, and one rejected ambiguous row using synthetic/test fixtures only;
17. explicit answers: can outcome leak into entry features? can PAPER influence LIVE evidence? can fuzzy ticker/time identity enter? can a demoted proof remain training-eligible? can this PR change a trade decision?

Leave PR **Draft / HARD HOLD** after implementation for independent whole-PR review.

---

# 20. Non-goals

This PR must NOT:

- decide ENTER/WAIT/SKIP;
- change Gate G;
- change selector thresholds;
- change contract choice;
- change position sizing;
- submit/cancel broker orders;
- change target/stop/runner logic;
- train or deploy a model;
- automatically adjust scoring weights;
- enable LIVE intelligence;
- claim causal reasons for wins/losses;
- promote legacy fuzzy outcome data;
- recompute old pre-trade features from future market data.

---

# 21. Acceptance criteria

Complete means:

- every executed outcome attribution is rooted in an exact revalidated #432 binding and canonical proof;
- frozen pre-decision evidence is never rewritten or backfilled from the future;
- pretrade, execution, path, exit, and outcome features are structurally separated;
- LIVE/PAPER/legacy cohorts cannot silently mix;
- deterministic labels are versioned and auditable;
- missing/ambiguous/corrupt identity fails closed;
- read-only aggregation exposes denominator and cohort/version truth;
- no money-path authority is added;
- exact-head tests/CI pass;
- final independent audit finds zero identity, leakage, or trading-authority regressions.
