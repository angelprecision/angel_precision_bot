# P1 CURRENT-MAIN WORK ORDER — Recalibrate catastrophic option airbag from exact replay evidence

## Status

**DRAFT / HARD HOLD — SPEC / EVIDENCE WORK FIRST. DO NOT MERGE OR DEPLOY A THRESHOLD CHANGE FROM THIS DOCUMENT ALONE.**

This PR exists because the current independent option-percentage airbag appears capable of converting a still-valid underlying thesis into a realized loss during ordinary short-DTE option convexity. That is potentially a profitability defect, but simply deleting or widening the stop would create a different and potentially worse account-risk defect.

The first implementation phase is therefore exact replay/data attribution. Production threshold authority must remain unchanged until the evidence gate in this document is satisfied.

## 2026-08-12 AAPL LIVE incident

Jason LIVE entered:

```text
signal_id: 83503891-b746-4c79-a39c-19e8cd8d4cd8
setup: daily 2-3 (operator nomenclature: daily 2-3-2 family)
side: PUT
contract: AAPL260814P00300000
DTE: 2
entry fill: 1.32
exit fill: 0.98
realized: -34 / -25.76%
```

Canonical position geometry included:

```text
stop_underlying: 307.55
target_underlying: 298.03
```

During the drawdown, decision telemetry observed the AAPL underlying around `303.96`. For a PUT, `303.96 < 307.55`, so the stored technical underlying stop was not breached at that observation.

The option itself reached roughly `-26.5%`, and the existing DTE threshold for `dte <= 2` is `-26%`. Current `ap/exit_thresholds.py::effective_thresholds()` returns:

```text
0DTE index: -18%
0DTE equity: -22%
DTE <= 2:   -26%
otherwise:  -33%
```

A real EXIT was then submitted and filled. Shortly afterward, the underlying moved back in the original PUT direction according to the live incident observation.

This case does **not** prove the option would definitely have recovered to profit; exact subsequent executable BID truth is required for that claim. It does prove that the current independent percentage airbag can fire while the stored underlying thesis geometry is still unbreached.

## Relationship to #403

Merged #403 intentionally separated two authorities:

1. stored underlying technical stop for ordinary thesis invalidation; and
2. independent `OPTION_CATASTROPHIC_STOP` as an account-protection airbag.

That separation is correct and must remain explicit.

This PR **does not undo #403** and must not relabel option-P&L drawdown as a technical stop. Its question is narrower:

> Is the current short-DTE catastrophic option airbag calibrated to distinguish genuine catastrophic option risk from normal temporary option-premium drawdown while the underlying thesis remains intact?

## Non-overlap with existing PRs

This PR owns only the **policy/evidence calibration of the independent catastrophic option airbag**.

It must not duplicate:

- **#403** underlying-authoritative technical-stop geometry.
- **#428** exact broker EXIT fill/proof finalization. #428 supplies trusted terminal exit evidence.
- **#429** canonical filled-entry/exit-owner handoff. #429 must fix ownership/hydration independently.
- **#435/#436** entry intelligence/timing. Better entries may reduce airbag events, but they do not define exit catastrophe authority.
- **#438** intelligence promotion profitability gate.
- **#442** FVG/VI target extension. Target intelligence is winner management, not catastrophic loss authority.
- **#443** exact outcome attribution. #443 may become the preferred durable cohort source, but this PR's strategy question remains separate.

If AAPL's exact exit is shown to be solely an ownership/hydration defect and current-main airbag did not independently authorize the close, record that result and stop. Do not tune thresholds without exact decision proof.

## Core safety invariant

The system needs two different loss concepts:

```text
THESIS INVALIDATION
= fresh underlying truth crosses stored side-aware technical stop
```

and

```text
CATASTROPHIC OPTION RISK
= option-level condition severe enough to protect account capital even if ordinary thesis geometry has not yet invalidated
```

They must stay distinct.

The catastrophic airbag must remain capable of exiting true disasters. The purpose of this PR is to determine whether a single flat DTE percentage is sufficient evidence of catastrophe.

## Phase 1 — exact replay/evidence only

Before changing a single threshold, build an exact replay cohort for recent executed options trades.

### Required identity

Every observation used to justify a LIVE policy change must be bound to:

- exact `client_id`;
- exact `execution_mode`;
- exact canonical position id;
- exact ENTRY local/broker order ids when available;
- exact EXIT local/broker order ids;
- exact OCC contract;
- side/direction;
- exact quote timestamps;
- canonical proof outcome when eligible.

Fuzzy ticker/time joins are forbidden for policy evidence.

LIVE and PAPER cohorts must be reported separately. PAPER may supplement mechanics/replay coverage but cannot be presented as LIVE profitability proof.

### Required features per airbag event

For every trade that hit or approached the catastrophic threshold, capture if canonically available:

- ticker and exact contract;
- CALL/PUT;
- entry/exit timestamps and executable fills;
- DTE at evaluation time;
- strike and underlying price;
- option BID/ASK and spread at entry, pre-stop, stop, and post-stop observations;
- option P&L from executable BID for a long option;
- delta, gamma, theta, IV where point-in-time evidence exists;
- stored trigger, stop, target;
- underlying price relative to stop and target;
- whether technical stop was clear, confirming, confirmed, unavailable, or identity-unproven;
- time since entry;
- MFE and MAE from trusted telemetry;
- max profit seen before drawdown;
- whether touched-profit/winner-protection had activated;
- subsequent underlying excursion after the airbag exit;
- subsequent executable option BID excursion where historical quote truth exists;
- realized outcome;
- reason taxonomy that actually authorized EXIT.

Missing evidence stays missing. Do not backfill future data into the decision-time feature snapshot.

## Required comparison cohorts

At minimum report:

1. airbag exits where technical stop was already confirmed;
2. airbag exits where technical stop was clearly unbreached;
3. airbag exits where underlying truth was unavailable;
4. airbag exits where owner/stop identity was unproven;
5. trades that approached -18/-22/-26/-33% but recovered without an airbag exit;
6. trades that continued deteriorating after those levels;
7. 0DTE index, 0DTE equity, 1–2DTE equity, and >2DTE separately;
8. CALL vs PUT;
9. spread/liquidity buckets;
10. entry-timing quality buckets when #435/#436 frozen evidence becomes available.

## Metrics required before policy change

For each candidate policy report:

- number of eligible exact trades;
- number of airbag events;
- average and median loss at airbag;
- final realized loss;
- post-airbag maximum favorable recovery from executable truth;
- post-airbag further adverse excursion;
- percentage of airbag exits where technical thesis was still clear;
- percentage where waiting would have materially worsened loss;
- percentage where waiting would have recovered meaningful value;
- average incremental dollars preserved/lost by candidate policy;
- worst-case incremental loss;
- expected loss conditional on each DTE bucket;
- concentration by ticker/day/client;
- uncertainty/sample-size warning.

No candidate is promoted based on AAPL alone.

## Candidate policy families to evaluate

These are **research candidates, not pre-approved implementation choices**.

### Candidate A — current flat DTE threshold

Baseline exactly current behavior.

### Candidate B — confirmation around the catastrophic boundary

Example concept:

```text
first executable-BID breach of catastrophic threshold
-> CATASTROPHIC_STOP_CONFIRMING
-> require a newer fresh observation still beyond threshold
-> close unless another mandatory authority already closes earlier
```

This must be tested for unacceptable extra loss during genuinely fast collapses.

### Candidate C — thesis-aware two-level airbag

Conceptually:

```text
option loss beyond ordinary catastrophic threshold
+ underlying technical stop clearly unbreached
= tighter monitoring / confirmation band

option loss beyond deeper absolute emergency threshold
= close regardless of technical geometry
```

A deeper unconditional floor must remain bounded if this family is selected.

### Candidate D — contract-risk-aware airbag

Explore whether DTE alone is insufficient and whether point-in-time factors such as delta, spread, IV expansion/collapse, or moneyness materially separate recoverable convexity from true catastrophe.

Do not introduce an opaque ML model into the exit money path. Any production rule must remain deterministic, inspectable, and bounded.

### Candidate E — no policy change

This must remain a legitimate result. If evidence shows the current -18/-22/-26/-33 airbag reduces expected loss and AAPL was primarily an entry/identity anomaly, preserve it.

## Mandatory precedence

Any later implementation must retain a clear precedence table.

Existing mandatory authorities such as these may not be weakened accidentally:

- manual/operator emergency close;
- kill/circuit-breaker authority;
- EOD mandatory flatten;
- exact confirmed underlying technical stop;
- broker-truth/reconciliation safety;
- any separately reviewed hard account-loss authority.

Profit-taking/target/FVG intelligence cannot veto a genuine catastrophic safety exit.

## Current-main files

### Phase 1 evidence/replay

Prefer no production behavior change. Add a replay/report helper or tests under the smallest non-money-path surface possible.

### If and only if evidence authorizes Phase 2

Expected production scope should remain approximately:

1. `ap/exit_thresholds.py` — deterministic threshold/profile authority.
2. `ap_exit_engine.py` — catastrophic confirmation/precedence only if required.

Do not broaden into selector, watcher, sizing, scanner, queue, proof writer, or broker adapter.

## Required AAPL replay

The regression must encode the exact known decision geometry:

```text
AAPL PUT
entry 1.32
2DTE
stored stop_underlying 307.55
underlying observation ~303.96
option P&L ~-26.5%
max prior MFE ~+2.27%
```

It must prove separately:

1. technical PUT stop is **not** confirmed at 303.96 vs 307.55;
2. current baseline catastrophic threshold for 2DTE is -26%;
3. current-policy replay reaches the independent airbag when all exact prerequisites are satisfied;
4. candidate policies produce their stated deterministic result;
5. no candidate may claim the underlying stop was breached when it was not.

## Adversarial regression matrix for any Phase 2 implementation

At minimum:

1. 2DTE equity at -25.9% -> baseline no airbag yet.
2. 2DTE equity at -26.0/-26.1% -> exact boundary behavior pinned.
3. 0DTE equity -22% boundary pinned.
4. 0DTE index -18% boundary pinned.
5. >2DTE -33% boundary pinned.
6. technical stop confirmed before airbag -> technical taxonomy wins honestly.
7. technical stop clear + option threshold breach -> candidate airbag behavior exactly as specified.
8. underlying truth unavailable -> never fabricate technical stop clarity/breach.
9. stop level invalid -> never claim technical breach.
10. owner identity unproven -> behavior follows #403/#429 fail-closed contract.
11. stale option quote -> cannot authorize fresh catastrophic exit.
12. last/mid/mark without executable BID -> cannot masquerade as long-option liquidation truth.
13. wide spread -> candidate behavior explicitly tested.
14. one transient bad BID then recovery -> confirmation candidate tested.
15. true rapid collapse across repeated fresh BIDs -> airbag still closes within bounded loss.
16. duplicate evaluation -> at most one EXIT intent/POST through existing idempotency owner.
17. restart during confirmation -> durable/derived state cannot mature from stale same observation.
18. client mismatch -> zero cross-account mutation.
19. execution-mode mismatch -> zero PAPER/LIVE pollution.
20. exact AAPL incident replay.
21. at least several recent losing LIVE trades and winning controls.
22. no target/profit/runner regression.
23. no manual/EOD/kill regression.
24. no new broker submit/cancel call site.
25. proof/economic mutation occurs only through existing canonical exit lifecycle.

## Promotion gate for a policy change

A Phase 2 threshold/logic change remains **HARD HOLD** unless the exact replay report demonstrates all of the following:

1. sufficient exact-identity sample to justify the DTE bucket being changed;
2. no dependence on one AAPL incident;
3. candidate reduces expected realized loss or improves recovery-adjusted outcome versus baseline;
4. worst-case extra loss remains within an explicitly approved account-risk bound;
5. true catastrophic cases are still exited reliably;
6. skipped/avoided-loss tradeoffs are reported, not hidden;
7. exact-head adversarial tests are green;
8. #428 exit proof and #429 owner identity prerequisites are stable enough that evidence is trustworthy;
9. independent risk review approves the deterministic rule;
10. explicit owner authorization is given for LIVE rollout.

## Rollout

If a new deterministic airbag policy is eventually approved:

```text
replay only
-> PAPER / historical shadow comparison
-> LIVE shadow decision telemetry without changing exits
-> explicitly approved limited LIVE authority
```

Do not make a new threshold live by changing a generic environment variable during this spec task.

## Money-path statement

This docs/evidence PR itself must add **zero broker submit/cancel authority** and must not mutate orders, positions, proof economics, or queues.

Any later Phase 2 implementation is an active EXIT money-path change and therefore requires a fresh exact broker/mutation audit.

## Success criterion

We should end with an option-loss airbag that protects Jason from genuine catastrophic contract deterioration **without routinely forcing exits from technically valid positions merely because short-DTE premium temporarily became convex and ugly**.

That conclusion must come from exact replay evidence, not from widening stops because one trade hurt.

No merge, deploy, environment mutation, or LIVE threshold change is authorized by this spec.