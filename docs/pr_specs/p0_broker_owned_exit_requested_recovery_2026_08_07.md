# P0 — Recover broker-owned EXIT_REQUESTED after broker submit handoff failure

**Status: IMPLEMENTATION CONTRACT ONLY / HARD HOLD.**

This specification preserves a production lifecycle defect observed on 2026-08-07 and an older matching row from 2026-07-29. It does not itself fix production. Do not merge, deploy, approve, or mark Ready until the runtime correction is implemented, focused replay tests pass, and exact-head P0 CI is green.

## Incident requiring this repair

### 2026-08-07 Tradefluence ORCL PAPER

A real EXIT order was created for ORCL and received a real broker order id:

- local order: `90da1f28-ce4b-44a8-b258-7f13d5992251`
- kind: `EXIT`
- quantity: `4`
- broker order id: `36661364`
- durable status: `EXIT_REQUESTED`
- `submitted_ts`: null
- `filled_ts`: null
- durable `filled_qty`: 0
- later broker truth: filled
- repeated local error: `illegal_transition:EXIT_REQUESTED->EXIT_FILLED`

The order therefore crossed the external broker boundary, but durable local lifecycle truth remained in the pre-broker state.

The order monitor repeatedly observed broker fill truth and deferred/advanced toward fill handling, but the OSM correctly rejected the generic transition because `EXIT_REQUESTED -> EXIT_FILLED` is not a legal transition. Reconciliation later closed the economic position, leaving the broker-backed order row permanently stale.

### Historical recurrence — 2026-07-29 Tradefluence IBM PAPER

An older IBM EXIT row has the same durable shape:

- `status=EXIT_REQUESTED`
- nonblank broker order id
- missing `submitted_ts`
- repeated illegal transition errors

This proves the ORCL incident is not a single bad tick. It is a recurrent broker-submit/durable-state handoff hole.

## Current-main code seam

Current baseline for this specification:

`188f2338de3ca4b3e687aa07fd6b2c5ea4b2ab0b`

The current OSM transition graph intentionally permits:

`EXIT_REQUESTED -> EXIT_SUBMITTED`

but does **not** generically permit:

`EXIT_REQUESTED -> EXIT_ACKNOWLEDGED`

or:

`EXIT_REQUESTED -> EXIT_FILLED`

That strictness is correct and must remain. A local `EXIT_REQUESTED` row with no broker ownership is a pre-submit intent and must not be allowed to fabricate a fill.

The current fill monitor also intentionally polls only broker-backed orders in:

- `SUBMITTED`
- `ACKNOWLEDGED`
- `PARTIAL_FILL`
- `EXIT_SUBMITTED`
- `EXIT_ACKNOWLEDGED`
- `EXIT_PARTIAL_FILL`

It excludes `EXIT_REQUESTED` because ordinary `EXIT_REQUESTED` rows may be local intents that have never reached a broker.

The current exit-decision idempotency guard, however, treats `EXIT_REQUESTED` as an active EXIT state. That prevents duplicate exits, which is good. When a row is `EXIT_REQUESTED` **and also has a real broker order id**, the combination can therefore become a dead zone:

`duplicate-submit guard blocks new exit -> fill monitor does not own row -> broker reports fill -> OSM refuses fabricated direct transition -> stale row persists`

## Canonical invariant

**Once the broker accepts an EXIT and Angel Precision obtains a nonblank exact broker order id, that exit is broker-owned. Durable local state may never remain semantically pre-broker.**

The recovery contract is:

`EXIT_REQUESTED + exact broker ownership evidence -> evidence-fenced adoption to EXIT_SUBMITTED -> normal canonical ACK/PARTIAL/FILL/terminal lifecycle`

Do not weaken the generic transition graph to accomplish this.

## Required production correction

### 1. `ap/order_state_machine.py` — add an evidence-fenced broker-ownership adoption method

Add one narrowly scoped method conceptually equivalent to:

`adopt_broker_owned_exit_request(...)`

It may operate only when all of the following are proven:

- exact `client_id` matches the OSM instance;
- exact `local_order_id` exists;
- `kind='EXIT'`;
- current durable `status='EXIT_REQUESTED'`;
- exact nonblank `broker_order_id` is supplied and matches any broker id already stored on the row;
- `execution_mode` is exactly `live` or `paper` and matches the expected caller mode;
- position identity is nonblank and, when supplied, matches the expected position;
- quantity is positive;
- the row is not terminal.

The adoption must atomically move only that exact row to `EXIT_SUBMITTED`, preserve the broker id, preserve any existing authoritative `submitted_ts` without synthesizing one, preserve existing metadata, persist `broker_ownership_adopted_at` as recovery provenance/age authority, and append a diagnostic marker such as:

`broker_ownership_adopted_from_exit_requested=true`

Recovery time is not authoritative broker-acceptance time. When the original broker submission time cannot be proven, `submitted_ts` remains null and monitor age uses `broker_ownership_adopted_at`. Broker ownership is proven by the exact nonblank `broker_order_id` and exact recovery identity, not by manufacturing a submission timestamp.

Use an exact CAS predicate. Zero rows means ownership/identity mismatch and returns a non-success result. Database errors remain errors.

Do **not** add `EXIT_REQUESTED -> EXIT_FILLED` or `EXIT_REQUESTED -> EXIT_ACKNOWLEDGED` to the generic transition table.

Do **not** infer broker ownership from elapsed time, position state, contract match, or `exit_in_flight` alone.

### 2. `ap/exit_decision_idempotency_guard.py` — make broker acceptance and durable ownership one handoff

Audit the external exit callback seam where a broker order id is returned and the durable generation claim becomes broker-owned.

Required behavior:

1. create/reserve the local EXIT intent as today;
2. call the external broker submit callback exactly once;
3. if the callback proves **no broker submission**, retire/release the local intent using the existing no-submit path;
4. if the callback returns a nonblank broker order id, immediately route that exact identity through the OSM broker-ownership adoption/submit transition;
5. only after durable broker ownership is confirmed may the generation claim be treated as normally `BROKER_OWNED`;
6. if broker acceptance is proven but the durable adoption fails, preserve the exact broker id in the durable claim and classify the condition explicitly, for example:
   - `BROKER_OWNED_DURABILITY_GAP`
   - `EXIT_BROKER_OWNERSHIP_ADOPTION_FAILED`
7. that failure state must continue blocking duplicate exit submissions while making the order discoverable by recovery.

A broker-accepted order must never be downgraded to a pre-submit/no-submit outcome merely because the local transition failed afterward.

### 3. `ap/fill_monitor.py` — recover only broker-backed EXIT_REQUESTED rows

Do **not** simply add every `EXIT_REQUESTED` row to `get_pending_orders()`.

Add a dedicated recovery query/path, or a tightly fenced UNION branch, for rows satisfying all of:

- exact client;
- `kind='EXIT'`;
- `status='EXIT_REQUESTED'`;
- nonblank/non-`N/A` broker order id;
- exact execution mode proven;
- positive quantity.

Before normal broker polling/fill processing, route the row through the OSM adoption method so local state becomes `EXIT_SUBMITTED`.

After successful adoption, use the existing canonical broker-status and fill path. Do not duplicate position accounting or proof writes in this recovery branch.

If adoption fails because identity cannot be proven, emit a critical classified diagnostic and do not submit, cancel, or fabricate a fill.

Rows with `EXIT_REQUESTED` and no broker id remain excluded from broker polling.

### 4. `ap/order_monitor.py` — normalize broker ownership before advancing ACK/FILL truth

The order monitor currently sees stale `EXIT_REQUESTED` rows and may discover broker ACK/FILL truth.

When the local order is `EXIT_REQUESTED` **and** an exact broker id is proven, it must first call the same OSM broker-ownership adoption method before advancing to `EXIT_ACKNOWLEDGED`, `EXIT_PARTIAL_FILL`, `EXIT_FILLED`, or a broker terminal failure.

Do not special-case a direct illegal transition. Normalize ownership first, then use the normal transition graph.

If adoption cannot be proven, hold and emit a diagnostic. Do not clear exit ownership and do not create a replacement.

### 5. Restart/autonomous recovery — change only if a focused replay proves necessary

Expected first-pass production scope is exactly:

1. `ap/order_state_machine.py`
2. `ap/exit_decision_idempotency_guard.py`
3. `ap/fill_monitor.py`
4. `ap/order_monitor.py`
5. `ap/self_healing.py`

`ap/self_healing.py` is allowed only for narrow runtime execution-mode provenance wiring. It adds no lifecycle owner, no broker behavior, and no generalized self-healing redesign. Missing or unproven execution mode must HOLD rather than default to LIVE.

Do not change `ap_exit_engine.py`, `ap/exit_autonomous_recovery.py`, `ap_recovery.py`, `ap_reconciler.py`, or `ap/position_manager.py` unless a real-method restart test proves the five-file correction cannot recover the durable production shape.

If scope must expand, stop and document why before changing another production file.

## Broker and database safety invariants

1. This PR adds **zero new broker submit behavior**.
2. This PR adds **zero new broker cancel behavior**.
3. It only repairs durable ownership after a submit that already happened.
4. No position quantity changes on broker submission, ACK, reservation, or adoption.
5. Position quantity changes only through existing confirmed cumulative fill/reconciliation authority.
6. `client_id` must remain exact and never default.
7. `execution_mode` must remain exact `live`/`paper`; unknown mode fails closed.
8. No PAPER row may be promoted to LIVE or vice versa.
9. No contract-only fuzzy broker ownership in LIVE.
10. A broker lookup failure is UNKNOWN/HOLD, never evidence of cancel, fill, or no-submit.
11. No replacement exit may be submitted while this broker-owned generation is unresolved.
12. Existing diagnostics must be preserved and the new recovery reason appended, not overwrite original failure provenance.

## Required diagnostics

Use stable reason codes equivalent to:

- `EXIT_BROKER_OWNERSHIP_ADOPTED_FROM_REQUESTED`
- `EXIT_BROKER_OWNERSHIP_ADOPTION_FAILED`
- `BROKER_OWNED_EXIT_REQUEST_RECOVERY_HOLD`
- `EXIT_REQUESTED_BROKER_FILLED_RECOVERED`
- `EXIT_REQUESTED_BROKER_ACTIVE_RECOVERED`
- `EXIT_REQUESTED_BROKER_TERMINAL_RECOVERED`

Every diagnostic must include when available:

- client id
- execution mode
- local order id
- broker order id
- position id
- contract
- requested qty
- cumulative broker filled qty
- previous local status
- adopted local status
- broker status
- source component

## Mandatory regression replay

Create one focused P0 test module using real OSM/order-monitor/fill-monitor/idempotency-guard methods with broker/DB boundaries mocked or a real PostgreSQL test schema.

Minimum cases:

1. **ORCL production shape**: EXIT_REQUESTED, qty 4, broker id present, submitted_ts null, broker says FILLED -> adopt to EXIT_SUBMITTED -> canonical EXIT_FILLED -> one fill application.
2. **IBM historical shape**: same durable shape survives restart -> recovered without duplicate broker submit.
3. EXIT_REQUESTED with no broker id -> remains a local intent; zero broker status calls.
4. blank/whitespace/`N/A` broker id -> not ownership proof.
5. broker active/working -> adopt to EXIT_SUBMITTED/ACK path; no replacement submit.
6. broker partially filled -> adoption first, then canonical cumulative partial fill.
7. broker FILLED -> cumulative fill applied exactly once even across repeated polls.
8. broker CANCELED/REJECTED/EXPIRED -> adoption first, then normal terminal transition.
9. broker lookup exception -> HOLD; broker ownership remains protected; zero replacement submit.
10. client mismatch -> adoption fails closed before broker mutation.
11. execution mode mismatch/unknown -> fails closed; no taxonomy rewrite.
12. broker id mismatch against durable row -> fails closed.
13. position id mismatch -> fails closed.
14. duplicate concurrent recovery calls -> exactly one durable adoption; no duplicate fill side effects.
15. order monitor encountering broker fill from EXIT_REQUESTED -> zero `ILLEGAL_TRANSITION` events after repair.
16. fill monitor normal EXIT_SUBMITTED behavior remains unchanged.
17. normal pre-broker EXIT_REQUESTED flow remains unchanged.
18. no direct `proof_trades` or `trade_queue` writes introduced.
19. zero new broker POSTs and zero new broker DELETE/cancel calls in the recovery path.
20. diagnostics preserve original failure provenance.

## Interaction with #423 and #424

This PR is a prerequisite for the exit-reliability stack.

- This PR establishes truthful broker ownership/lifecycle state.
- #423 owns stale working-exit cancel/reprice/retry liveness.
- #424 owns reservation-aware quantity and EXIT ALL behavior.

Do not duplicate #423 cancel/retry logic here.
Do not duplicate #424 available-to-close quantity logic here.

Required merge/review order:

`#422 -> this PR -> #423 -> #424`

Each PR must be independently reviewable and independently replayable.

## Final audit gate

Before MERGE consideration:

- read PR description;
- read actual cumulative diff;
- read all review comments/threads;
- list changed files and confirm scope;
- verify the exact current production path;
- verify real broker metadata shape;
- prove no additional broker submit/cancel exposure;
- prove order/position/proof/queue mutation boundaries;
- prove `client_id` and `execution_mode` preservation;
- prove diagnostics survive downstream;
- prove PAPER/LIVE taxonomy isolation;
- prove no new path can cause Jason LIVE to duplicate an exit or remain unprotected;
- run focused replay;
- run exact-head P0 CI;
- issue MERGE / HOLD / HARD HOLD.

Until all of the above is complete, verdict is **HARD HOLD**.
