# P0 SPEC: Broker-confirmed EXIT fill must terminalize the exact canonical position

## Status

**SPEC ONLY / HARD HOLD. DO NOT MERGE OR DEPLOY THIS SPEC-ONLY HEAD.**

Base this work on exact current main:

`945e9da869a88bffebfd26d9fd3abbd982621c6c`

This PR owns one narrow downstream money-truth invariant:

> After an EXIT is broker-confirmed filled, the durable canonical position that economically owned that exact LIVE/PAPER option lifecycle must converge to the corresponding reduced/terminal state. A synthetic `broker-repair-*` runtime owner may never be allowed to become the only object that says `CLOSED` while the exact canonical `positions` row remains `OPEN`.

This is defense in depth. It does **not** replace #473. #473 owns FILLED ENTRY handoff/adoption so a pre-existing synthetic broker-repair owner is replaced by canonical ownership as soon as ENTRY truth is durable. This PR owns the separate downstream postcondition if a synthetic owner nevertheless reaches broker EXIT fill.

---

# 1. Production incident this PR must replay exactly

Date: **2026-08-17**

Client:

```text
jasoncosby1@gmail.com
```

Execution mode:

```text
live
```

Contract:

```text
SMCI260821P00038000
```

Canonical ENTRY:

```text
ENTRY local_order_id = ffc15a61-5df9-49ef-9935-0700ddc809d2
ENTRY broker_order_id = 142072638
ENTRY fill_price = 1.26
ENTRY filled_qty = 1
canonical positions.id = 4d371677-9627-43e6-ad9c-67402b2d4517
canonical positions.status = OPEN
canonical positions.qty = 1
canonical positions.quantity_remaining = NULL   <-- real production shape
canonical positions.execution_mode = live
canonical positions.contract = SMCI260821P00038000
canonical positions.local_order_id = ffc15a61-5df9-49ef-9935-0700ddc809d2
canonical positions.broker_order_id = 142072638
```

Before the ENTRY handoff fully converged, the exit engine had a protective synthetic owner:

```text
broker-repair-jasoncosby1@gmail.com-SMCI260821P00038000
```

That synthetic owner continued to receive quote/exit evaluations after the canonical ENTRY had filled.

Synthetic-owner EXIT:

```text
EXIT local_order_id = f257451e-ef5b-4fcb-9c65-a123c182abda
EXIT position_id = broker-repair-jasoncosby1@gmail.com-SMCI260821P00038000
EXIT broker_order_id = 142077333
EXIT filled_qty = 1
EXIT fill_price = 1.42
EXIT status = EXIT_FILLED
```

Broker truth at EXIT submit proved the exact OCC position was open with qty=1. The broker then filled the EXIT.

Decision trail:

```text
13:42:52 UTC  exit_reconciliation  SUBMITTED
               position_id=broker-repair-jasoncosby1@gmail.com-SMCI260821P00038000
               reason_code=OSM_PENDING_EXIT_ORDER_SET

13:43:06 UTC  exit_reconciliation  CLOSED
               position_id=broker-repair-jasoncosby1@gmail.com-SMCI260821P00038000
               reason_code=BROKER_CONFIRMED_CLOSED
               explanation=EXIT_FILLED
```

But durable canonical truth after the broker fill remained:

```text
positions.id = 4d371677-9627-43e6-ad9c-67402b2d4517
status = OPEN
qty = 1
quantity_remaining = NULL
exit_ts = NULL
close_source = NULL
execution_mode = live
```

That is the defect.

The broker was flat while local risk truth still counted an OPEN position. On Jason's ~$1.64k account this preserved roughly $126 of phantom exposure. Combined with the separate fake `sector="other"` defect owned by #483, later candidates were blocked as if the closed SMCI capital were still deployed.

---

# 2. Why this PR exists even though #473 exists

#473 is the primary root-cause repair. It strengthens the FILLED ENTRY handoff so completion is not truthful unless exactly one canonical exit owner exists and no exact-domain `broker-repair-*` owner remains.

That is necessary, but money systems also need a downstream invariant.

If any of the following happens:

- a process restarts between broker-repair creation and ENTRY adoption;
- an older deployment created a repair owner before #473 was deployed;
- an adoption path temporarily HOLDS to preserve protective monitoring;
- a future regression recreates a synthetic owner;
- an EXIT is submitted while the runtime owner is still synthetic;

then a broker-confirmed EXIT fill must still converge durable position truth or fail visibly.

A filled broker EXIT with a stale canonical OPEN row is not acceptable simply because an earlier layer was also wrong.

---

# 3. Explicit non-overlap with pending/open work

## #473 — FILLED ENTRY handoff restart safety

#473 owns:

- FILLED ENTRY durable identity binding;
- interrupted ENTRY handoff recovery;
- canonical exit-owner adoption/seeding;
- eliminating exact-domain broker-repair owners after canonical ENTRY truth is established.

This PR must **not** redesign that logic.

The new regression suite must run with #473-compatible production shapes, but this PR's authority begins only after an EXIT has broker-confirmed fill truth.

## #481 — broker position quantity / flat truth

#481 is merged into this base and owns:

- strict broker position quantity normalization;
- UNKNOWN vs flat semantics;
- absent exact contract in a successful authoritative snapshot as flat;
- autonomous broker-truth recovery safety.

Do not duplicate broker snapshot parsing in this PR.

## #483 — fake `other` sector

#483 owns sector identity. Do not touch `ap_master_control.py`, sector maps, concentration thresholds, or risk caps here.

## #474 — deferred real-cost revalidation

#474 owns pre-submit risk revalidation ordering after deferred contract materialization. Do not touch that path.

## #444 — affordable contract reselection

#444 owns bounded cheaper-contract reselection when final authoritative budget is below selected contract cost. Do not touch selector behavior here.

## #487 — reconciler broker-order UNKNOWN preservation

#487 owns reconciler handling of UNKNOWN broker-order truth. This PR starts only from broker-confirmed positive EXIT fill truth. Do not convert UNKNOWN to FILLED/CLOSED.

## #428 — old broad cumulative EXIT fill branch

#428 contains historical concepts around exact EXIT fill truth but is a broad stale branch touching many production files. Do not cherry-pick or merge #428 to solve this seam.

Use #428 only as historical evidence. Rebuild the smallest current-main repair required by the exact 2026-08-17 SMCI shape.

---

# 4. Confirmed current-main code that must be read before editing

Claude/Codex must read these exact files on the rebased head before changing anything:

1. `ap/exit_fill_truth_guard.py`
2. `ap/exit_position_ambiguity_guard.py`
3. `ap/trade_lifecycle_guards.py`
4. `ap/fill_monitor.py`
5. `ap/order_state_machine.py`
6. `ap/position_manager.py`
7. `ap_exit_engine.py`
8. focused tests listed later in this spec

Do not infer behavior from PR descriptions alone.

Current main already contains a canonical EXIT-fill guard. That matters. The implementation task is **not** "build an EXIT reconciler from scratch." It is:

> reproduce why the current guard did not leave the canonical SMCI row terminal, then repair the smallest exact seam and add a mandatory postcondition so this production shape cannot silently recur.

---

# 5. Current-main observations that constrain the implementation

## 5.1 `ap/exit_fill_truth_guard.py` already says it owns canonical EXIT fill projection

Its module contract says it runs only after OSM accepted an EXIT fill and never submits/cancels orders.

Preserve that boundary.

## 5.2 Synthetic/null position identity has a fallback resolver

Current `_resolve_position(...)` treats a real non-synthetic `position_id` as authoritative.

For synthetic/null identity it tries to find exactly one contemporaneous same-contract lifecycle.

That is directionally correct, but this PR must prove the resolver is using the full production identity domain and that the resolved row is the one actually updated.

## 5.3 Execution mode must be an identity fence

Current resolution must be audited for an exact `execution_mode` fence.

A LIVE synthetic EXIT must never close a PAPER position and vice versa, even under the same client and OCC contract.

Do not normalize missing/invalid mode into a guess.

Required modes are exactly:

```text
live
paper
```

Anything else is HOLD / identity unproven.

## 5.4 `quantity_remaining=NULL` is a real production shape

Jason SMCI had:

```text
qty=1
quantity_remaining=NULL
```

NULL `quantity_remaining` must not make a valid canonical OPEN position invisible when `qty` and the exact ENTRY identity prove ownership.

At the same time, do not globally reinterpret NULL as an arbitrary positive quantity. Quantity truth must be derived through the existing canonical position/fill projection rules.

## 5.5 Same OCC can legitimately appear in more than one lifecycle

Never resolve by:

- newest row;
- ticker only;
- first OPEN row;
- nearest timestamp alone;
- synthetic position-id string parsing alone;
- same client without mode;
- same contract across multiple candidate lifecycles.

If more than one canonical candidate survives the exact identity filters, quarantine/HOLD and emit diagnostics. Do not guess where money belongs.

---

# 6. Required invariant

For every broker-confirmed EXIT fill accepted by OSM:

```text
EXIT.client_id            == canonical_position.client_id
EXIT.execution_mode       == canonical_position.execution_mode == exact live|paper
EXIT.contract             == canonical_position.contract       == exact OCC
```

Then one of two outcomes must be durable and observable.

## Outcome A — exact canonical lifecycle resolved

The broker fill is cumulatively projected into that canonical position.

For full exit:

```text
positions.status              = terminal canonical status
positions.quantity_remaining  = 0
positions.contracts_exited    = total exited qty
positions.exit_price          = broker-fill-derived weighted exit
positions.realized_pnl        = broker-fill-derived realized P&L
positions.realized_pnl_pct    = broker-fill-derived realized P&L percent
positions.exit_ts             = broker-confirmed final fill timestamp
positions.close_source        = canonical broker-confirmed source
```

Use current repository terminal taxonomy and proof contracts. Do not invent a new fake status if an existing canonical status/source applies.

For partial exit:

```text
positions remains active
quantity_remaining > 0
contracts_exited reflects cumulative broker-confirmed exit qty
no terminal proof is emitted
```

## Outcome B — exact canonical lifecycle cannot be proven

Do not guess.

Persist/emit a durable diagnostic such as:

```text
EXIT_FILLED_CANONICAL_POSITION_UNPROVEN
```

or use an existing precise equivalent if the repository already defines one.

The diagnostic must include enough structured identity to investigate:

- client_id
- execution_mode
- exact OCC contract
- EXIT local_order_id
- EXIT broker_order_id
- requested/synthetic position_id
- candidate canonical position ids
- filled_qty
- fill_price
- filled_ts
- reason code

Do not create a second position and do not rewrite a foreign position.

The broker fill itself remains true. This PR must not pretend an already-filled EXIT did not happen merely because local ownership is ambiguous.

---

# 7. Required postcondition after a full broker EXIT fill

After the transaction completes, reread durable state.

If a canonical position was resolved, require all of the following before reporting reconciliation success:

```text
canonical position exists
client_id exact
execution_mode exact
contract exact OCC
status terminal
quantity_remaining == 0
exit_ts present
exit_price finite and > 0
contracts_exited >= filled exited qty
```

If current schema legitimately permits one terminal row to preserve `quantity_remaining=NULL`, STOP and document that schema contract before coding. Do not weaken the postcondition by assumption.

For the Jason SMCI replay specifically, the acceptance test must prove the original canonical row `4d371...` is terminal after the synthetic-owner EXIT fill.

It is not enough for:

- the runtime synthetic object to say closed;
- the EXIT order to say `EXIT_FILLED`;
- a decision event to say `BROKER_CONFIRMED_CLOSED`;
- proof_trades to say closed;
- broker positions to be flat.

The canonical `positions` row must converge too.

---

# 8. Implementation authority and hard scope

## Preferred production scope

Start with **one production file only**:

```text
ap/exit_fill_truth_guard.py
```

Expected focused test file:

```text
tests/test_p0_canonical_exit_fill_truth.py
```

A new focused test file may be used instead if keeping the exact SMCI incident isolated materially improves clarity:

```text
tests/test_p0_exit_fill_canonical_position_convergence.py
```

## Conditional second production file

`ap/exit_position_ambiguity_guard.py` may change **only if fail-first reproduction proves that wrapper blocks or misroutes the canonical resolver**.

If that file is changed, the PR description must show:

1. the failing current-main test;
2. the exact wrapper branch causing the failure;
3. why fixing only `ap/exit_fill_truth_guard.py` is insufficient.

## Forbidden production files without STOP-and-report

Do not modify:

- `ap_master_control.py`
- `ap_execution_core.py`
- `ap_entry_watcher.py`
- `ap_overnight_reeval.py`
- selectors
- scanners
- queue code
- contract sizing
- `ap_exit_engine.py`
- `ap/position_manager.py`
- broker adapter code
- migrations
- proof taxonomy
- risk thresholds

If Claude believes one of these must change, STOP and report the exact blocker rather than expanding scope.

The reason `ap_exit_engine.py` is forbidden here is important: #473 already owns runtime canonical adoption. This PR is a durable broker-fill convergence guard, not another owner-registry rewrite.

---

# 9. Step-by-step implementation order for Claude/Codex

Follow this order. Do not jump directly to code.

## Step 1 — Rebase before analysis

Fetch origin and rebase the implementation branch onto latest `main`.

Record:

```text
base SHA
head SHA before implementation
```

If current main moved beyond `945e9da...`, use the newer exact main and re-run every source-code observation in this spec against that head.

Do not implement against a stale tree.

## Step 2 — Read this spec completely

Do not start editing until the production incident, non-overlap rules, and postconditions are understood.

## Step 3 — Read PR #473's actual diff and current adoption tests

Confirm that #473 owns ENTRY-time synthetic-to-canonical adoption and that this PR is downstream only.

Specifically inspect:

- `_seed_exit_engine(...)`
- `APExitEngine.adopt_canonical_position_identity(...)`
- the real-engine synthetic-to-canonical tests in `tests/test_p0_live_executable_bid_pnl.py`

Do not fork or duplicate that code.

## Step 4 — Read the actual current EXIT-fill call path

Trace, with function names, the exact path:

```text
broker reports EXIT fill
-> OSM accepts EXIT_FILLED / EXIT_PARTIAL_FILL
-> fill monitor EXIT sync hook
-> installed lifecycle guard(s)
-> canonical EXIT-fill projection
-> positions update
-> proof/update side effects
-> exit-engine fill callback / runtime owner update
```

Write this path into the implementation PR description before claiming a fix.

## Step 5 — Prove guard installation

Read `ap/trade_lifecycle_guards.py` and `client_runner.py`.

Prove the canonical EXIT-fill guard and ambiguity wrapper are actually installed on LIVE Jason runtime.

If the guard is not installed in the relevant path, that is the root cause and must be documented before implementation.

Do not assume monkeypatch installation from unit tests equals production installation.

## Step 6 — Build a fail-first exact SMCI replay

Before changing production code, create a focused regression using these exact identifiers and shapes:

```text
client_id=jasoncosby1@gmail.com
execution_mode=live
contract=SMCI260821P00038000
entry_local=ffc15a61-5df9-49ef-9935-0700ddc809d2
entry_broker=142072638
canonical_position_id=4d371677-9627-43e6-ad9c-67402b2d4517
canonical_status=OPEN
qty=1
quantity_remaining=NULL
entry_fill=1.26
exit_local=f257451e-ef5b-4fcb-9c65-a123c182abda
exit_position_id=broker-repair-jasoncosby1@gmail.com-SMCI260821P00038000
exit_broker=142077333
exit_fill=1.42
exit_filled_qty=1
```

The pre-fix assertion must show the current defect, not merely test a helper in isolation.

Minimum fail-first assertion:

```text
broker EXIT fill is accepted
but canonical position remains OPEN / not terminal
```

If the exact current-main replay unexpectedly passes, STOP. Do not invent a patch. Determine what differed between test and production installation/data shape.

Potential differences to investigate if it passes:

- wrapper installation order;
- `_sync_exit_price` already patched by another guard;
- local `position_id` shape at the exact call site;
- `execution_mode` absent in one row;
- `quantity_remaining=NULL` behavior;
- transaction rollback after projection;
- proof update exception rolling back position update;
- different connection/commit behavior;
- EXIT order not included by `_load_exit_fills` because of synthetic `position_id`;
- current EXIT row status not visible at transaction time;
- multiple historical same-contract candidates;
- call path that closes runtime owner but bypasses canonical fill guard.

Do not modify production code until the fail-first test identifies the actual path.

## Step 7 — Add execution-mode fencing to canonical resolution if missing

Any synthetic/null fallback resolution must filter exact:

```text
client_id
execution_mode
contract
```

Execution mode must be canonical lower-case `live|paper` and proven on both sides.

Missing/invalid/conflicting mode = identity unproven.

Do not use `LOWER(COALESCE(execution_mode,''))` to turn NULL into a candidate.

## Step 8 — Preserve real position-id authority

If an EXIT order carries a non-synthetic canonical `position_id`, that identity is authoritative.

If that exact row does not exist or conflicts on client/mode/OCC:

```text
HOLD / quarantine
```

Do not fall back to another same-contract row.

## Step 9 — Harden synthetic/null fallback without guessing

Synthetic/null EXIT `position_id` can be repaired only if exact durable evidence resolves one canonical lifecycle.

Resolution priority should prefer stronger durable identity when available:

1. exact canonical position already linked through originating ENTRY identity;
2. exact canonical position id recoverable from durable order linkage;
3. only then an exactly-one active/contemporaneous candidate in the same client/mode/OCC risk domain.

If current schema/call path cannot supply stronger identity for the synthetic EXIT, the exactly-one fallback is acceptable only with explicit ambiguity diagnostics.

Never cross execution mode.

## Step 10 — Treat `quantity_remaining=NULL` as production reality, not absence of a position

Do not filter the canonical candidate out merely because `quantity_remaining` is NULL when:

```text
status is active
qty is positive
exact ENTRY identity is durable
exact client/mode/OCC match
```

But do not manufacture a remaining quantity from NULL before projection. Use existing cumulative broker EXIT fill math against canonical `qty`.

## Step 11 — Ensure the current EXIT fill is included in cumulative fill projection

For a synthetic EXIT order, `_load_exit_fills(...)` must include the current local EXIT order even though its persisted `position_id` may be `broker-repair-*` rather than canonical.

Preserve duplicate-fill/idempotency protections.

A replayed identical broker callback must not double-count quantity or P&L.

## Step 12 — Update exactly one canonical row

All canonical position updates must be exact-cardinality.

Expected:

```text
rows_updated == 1
```

Zero or >1 is a hard reconciliation failure, not success.

Do not update by ticker or contract alone.

## Step 13 — Add mandatory durable postcondition reread

After update, reread the exact canonical `positions.id` under the same client and exact execution mode.

For full exit assert terminal truth.

For partial assert remaining quantity and active truth.

If postcondition fails:

- do not emit a successful canonical reconciliation marker;
- emit durable failure diagnostics;
- preserve the already-true broker EXIT fill on the order;
- do not resubmit or cancel anything.

## Step 14 — Make diagnostics name both identities

Every synthetic-owner recovery diagnostic must contain:

```text
synthetic_position_id
canonical_position_id (if resolved)
client_id
execution_mode
contract
exit_local_order_id
exit_broker_order_id
```

Operators must be able to see that the broker EXIT occurred under synthetic ownership but durable canonical state was repaired or quarantined.

## Step 15 — Do not let proof writing roll back canonical money truth unless transaction contract explicitly requires atomicity

Audit current transaction ordering carefully.

If proof mutation fails after canonical position projection, determine whether the current transaction intentionally rolls everything back. Do not casually split atomicity.

However, a proof-table shape problem must never result in the system publicly claiming canonical reconciliation success while the position is still OPEN.

If proof and position atomicity is current policy, preserve it and surface the proof failure as the reason canonical convergence could not commit.

## Step 16 — Run exact regressions before any broader suite

Run the fail-first SMCI test first after patch.

Then test all negative cases below.

## Step 17 — Audit broker mutation surface

Static and runtime traps must prove this PR adds:

```text
0 broker ENTRY submits
0 broker EXIT submits
0 broker cancels
0 replace/amend calls
```

This PR consumes broker-confirmed fill truth only.

## Step 18 — Audit persistent mutation surface

Expected mutation may include only the already-owned canonical EXIT reconciliation writes:

- exact `positions` row projection/terminalization;
- exact related proof/order reconciliation already owned by current guard.

No scanner/signal/queue/risk/sizing mutation.

## Step 19 — Run adjacent #473-compatible tests

The final branch must be compatible with #473's identity semantics. If #473 is not yet merged when this is implemented, test both:

1. current main plus this PR;
2. this PR rebased after #473 or #473 head plus this patch, as appropriate to merge order.

Do not merge two individually-green P0s that conflict semantically at the handoff seam.

## Step 20 — exact-head CI

Run P0 Regression Suite on the exact implementation head.

Return the exact run ID and head SHA.

Local tests alone are not merge authority.

---

# 10. Required production-shaped regression matrix

At minimum implement all cases below.

## A. Exact Jason SMCI full-close replay

Synthetic EXIT owner, canonical DB row `OPEN`, `qty=1`, `quantity_remaining=NULL`.

Expected:

```text
EXIT remains broker-confirmed filled
canonical row becomes terminal
quantity_remaining=0
exit_price=1.42
client_id preserved
execution_mode=live preserved
contract exact
synthetic runtime id is diagnostic only, not durable canonical identity
```

## B. Same replay with canonical `quantity_remaining=1`

Must also converge.

## C. Real canonical EXIT position_id

EXIT already points to canonical position id.

Expected ordinary canonical path remains unchanged.

## D. PAPER/LIVE collision trap

Create two same-client same-OCC positions:

```text
one live
one paper
```

Synthetic LIVE EXIT may resolve only LIVE.

Synthetic PAPER EXIT may resolve only PAPER.

Missing mode resolves neither.

## E. Wrong client trap

Same OCC/mode under another client must never qualify.

## F. Multiple canonical LIVE candidates, same OCC

If two active/contemporaneous canonical positions survive exact client/mode/OCC fencing and no stronger durable identity disambiguates them:

```text
quarantine / HOLD
0 position updates
```

No newest-row guessing.

## G. Current EXIT local row inclusion

Synthetic EXIT order's `position_id` does not equal canonical position id.

Current local EXIT order must still be included exactly once in projection.

## H. Duplicate broker callback

Same `EXIT_FILLED` callback processed twice.

Expected:

```text
no double decrement
no doubled realized P&L
no duplicate proof close
canonical state remains idempotently terminal
```

## I. Partial fill

Canonical qty=3, broker EXIT filled_qty=1.

Expected:

```text
quantity_remaining=2
status remains active
no terminal proof
```

Then second fill completes remaining quantity and terminalizes exactly once.

## J. Exit overfill

Cumulative EXIT fills exceed canonical qty.

Expected HOLD/error and zero corrupt projection.

## K. Missing/invalid fill economics

Reject/quarantine:

- filled_qty <= 0
- fractional quantity
- bool quantity
- fill_price <= 0
- NaN/Inf price
- missing broker order identity if current canonical contract requires it

Do not write fake zero economics.

## L. Synthetic owner only, no canonical DB position

Broker EXIT fill is true but no exact canonical position can be resolved.

Expected:

```text
EXIT fill retained as broker truth
canonical reconciliation diagnostic emitted
0 position fabrication
0 broker mutation
```

## M. Canonical row already terminal

Replayed EXIT callback against exact already-terminal canonical lifecycle.

Expected idempotent success only if cumulative fill truth is consistent. Conflicting economics or identity must quarantine.

## N. Same ticker, wrong OCC

Never bind.

## O. Missing execution mode

Never bind by defaulting to runtime/client mode.

## P. Foreign synthetic repair owner

A `broker-repair-*` for same OCC but foreign client or wrong mode cannot donate identity/quote/proof authority.

## Q. Position update cardinality zero

Must not report successful canonical reconciliation.

## R. Position update cardinality >1

Must not report successful canonical reconciliation.

## S. Proof failure after canonical candidate resolution

Verify transaction semantics and visible failure. Never emit a clean `BROKER_CONFIRMED_CLOSED` canonical success if durable position row did not commit terminal.

## T. No broker mutation trap

Patch/trap every broker mutation method reachable from the test harness. All remain zero.

---

# 11. Required adjacent regression slice

Run at minimum:

```bash
python -m pytest -q \
  tests/test_p0_canonical_exit_fill_truth.py \
  tests/test_p0_exit_position_ambiguity_guard.py \
  tests/test_p0_terminal_close_proof_binding.py \
  tests/test_p0_live_executable_bid_pnl.py \
  tests/test_fill_monitor_mvp_hardening.py \
  tests/test_p0_exit_closed_guard_and_circuit_breaker.py
```

If #473 is merged or is the intended parent at implementation time, also run:

```bash
python -m pytest -q \
  tests/test_p0_filled_entry_restart_handoff_recovery.py \
  tests/test_p0_filled_entry_durable_identity_handoff.py
```

Then:

```bash
python -m py_compile ap/exit_fill_truth_guard.py
python -m py_compile ap/exit_position_ambiguity_guard.py  # only if changed
git diff --check
```

Then exact-head P0 CI.

---

# 12. Live-behavior audit

## Does this change LIVE behavior?

Yes, narrowly.

Today a broker-confirmed EXIT fill can leave an exact canonical local position OPEN under the reproduced synthetic-owner race.

After this repair, the already-filled broker EXIT must converge exact durable canonical position state when identity is proven.

This does **not** create a new broker action.

## Flag-off or active?

This is a money-truth invariant and should not be hidden behind a strategy flag.

If implementation requires a rollout guard for observability, it may be introduced only if default behavior remains fail-safe and the PR explicitly states what happens in LIVE when disabled. Do not use a flag to preserve known false OPEN state indefinitely.

## Broker submit/cancel?

No new submit/cancel authority.

Expected delta:

```text
ENTRY POST delta = 0
EXIT POST delta = 0
cancel delta = 0
replace delta = 0
```

## Orders mutation?

Only existing reconciliation metadata/status ownership if already part of canonical EXIT-fill guard. Do not rewrite ENTRY orders.

## Positions mutation?

Yes. That is the point: project already-confirmed broker EXIT fills into the exact canonical position.

## proof_trades mutation?

Only preserve the existing canonical proof reconciliation contract. Do not widen proof taxonomy or manufacture proof from unresolved identity.

## Queue mutation?

None.

## client_id / execution_mode preservation?

Mandatory exact fence.

## Production metadata shape?

Must replay the exact SMCI shape including:

```text
quantity_remaining=NULL
synthetic EXIT position_id
real canonical position id
exact live mode
exact OCC
exact entry local/broker identity
```

## Downstream diagnostics?

Must improve, not erase, diagnostics. Successful synthetic-to-canonical convergence must name both ids. Unresolved convergence must be visible and structured.

## PAPER/LIVE taxonomy pollution?

Must be impossible by construction through exact execution-mode fencing.

## Could this make Jason trade junk?

No. This PR does not change scanner admission, score, selector, DTE, delta, spread, liquidity, trigger, risk thresholds, sizing, or submit logic.

The only downstream trade-flow effect is legitimate: once a broker-closed position is durably terminal instead of falsely OPEN, later risk snapshots stop counting phantom exposure. Those later entries still must clear every ordinary admission and execution gate.

---

# 13. Forbidden shortcuts

Do not solve this by:

- deleting all `broker-repair-*` objects globally;
- setting all same-contract positions CLOSED;
- matching by ticker;
- matching by latest row;
- ignoring execution mode;
- treating every NULL `quantity_remaining` as zero;
- treating every NULL `quantity_remaining` as qty;
- increasing risk caps;
- clearing positions table manually in production as the software fix;
- bypassing proof validation;
- submitting another EXIT because local state stayed OPEN after the broker fill;
- adding a background loop that repeatedly closes arbitrary rows;
- swallowing reconciliation exceptions and emitting success anyway.

---

# 14. Required implementation PR description from Claude/Codex

Before asking for MERGE, return all of the following in the PR body or a single audit comment:

1. exact base SHA;
2. exact head SHA;
3. full changed-file list;
4. current-main fail-first SMCI replay result;
5. exact root cause identified from that replay;
6. exact function path changed;
7. why #473 does not duplicate this change;
8. why #428 was not reused;
9. broker mutation audit;
10. orders/positions/proof/queue mutation audit;
11. client/mode/OCC identity proof;
12. `quantity_remaining=NULL` production-shape proof;
13. duplicate-callback idempotency proof;
14. partial-fill proof;
15. ambiguity fail-closed proof;
16. focused test command and result;
17. adjacent regression command and result;
18. exact-head P0 CI run ID/result;
19. `git diff --check` result;
20. final **MERGE / HOLD / HARD HOLD** verdict.

If any required proof is unavailable, verdict is HOLD or HARD HOLD, never MERGE by optimism.

---

# 15. Merge ordering

Preferred ordering:

1. independently audit and land #473 first if it passes, because it closes the upstream synthetic-owner handoff race;
2. rebase this PR onto the post-#473 main;
3. rerun the exact SMCI downstream EXIT-fill replay;
4. implement only the still-reproducing downstream convergence gap;
5. run exact-head P0 CI;
6. merge only if both ENTRY adoption and EXIT terminal convergence invariants coexist without duplicate ownership or duplicate broker authority.

If #473 fully eliminates the fail-first reproduction **and** current canonical EXIT-fill guard correctly terminalizes an intentionally injected synthetic-owner EXIT under the exact production shape, this PR may legitimately close as no-code/obsolete after evidence. Do not force a production patch merely because this spec exists.

---

# 16. Current verdict

**HARD HOLD.**

Spec only. No production behavior is authorized by this head.

The implementation branch must first reproduce the exact Jason SMCI failure against the then-current main and prove the smallest code seam before editing production logic.
