# P0 SPEC: Prove filled ENTRY durable identity before exit-engine handoff

## Status

**DRAFT / SPEC ONLY. DO NOT MERGE OR DEPLOY THIS SPEC-ONLY HEAD.**

This branch starts from exact current `main`:

`6f7a37488f398951bdf1635f51b5192b9f7bdab2`

That base already includes the August 14 rollback recovery and merged PR #460 broker EXIT fill truth. Do not rebase this work onto an older pre-rollback or pre-#460 tree.

This PR has one job only:

> After a broker-confirmed ENTRY fill resolves a canonical DB position, prove and persist the exact ENTRY <-> position identity before the exit engine is allowed to adopt/seed canonical ownership.

No strategy, selector, retry, sizing, stop, target, profit-taking, reconciler, proof-trade, queue, or scanner behavior belongs in this PR.

---

## Production incident this closes

August 14 Jason LIVE PEP exposed the same durable identity seam previously seen around the August 10 INTC incident.

Known production shape:

- client: `jasoncosby1@gmail.com`
- execution mode: `live`
- contract: `PEP260821C00141000`
- ENTRY local order: `1b8ec150-38cc-424c-93e5-b1a1820ea50b`
- ENTRY broker order: `141999576`
- broker ENTRY fill: `$1.58`
- canonical position: `86ed1496-75ef-472c-8a36-94eee1698509`
- filled ENTRY `orders.position_id`: missing at fill handoff before repair
- canonical position `local_order_id`: missing
- canonical position `broker_order_id`: missing

The later broker EXIT could be real and correctly priced while downstream proof remains unable to bind the trade cleanly because the ENTRY/position durable identity was never closed.

This is not a pricing problem and not an intelligence problem. It is an identity handoff problem.

---

## Confirmed current-main code path

Current `ap/fill_monitor.py` does this after OSM confirms a terminal ENTRY fill:

```text
OSM -> FILLED
-> _open_position_safe(...)
-> _place_standing_stop_best_effort(...)
-> _seed_exit_engine(...)
-> best-effort UPDATE orders.position_id
```

That ordering is wrong for canonical ownership. `_seed_exit_engine()` can run before durable `orders.position_id` is proven.

`_open_position_safe()` already passes all of the correct execution identity into `APPositionManager.open_position()`:

- `local_order_id`
- `broker_order_id`
- `execution_mode`
- exact contract
- signal/plan identity

Do not redesign that API.

The remaining seam is inside `APPositionManager.open_position()` idempotency behavior: if an already-existing OPEN position is found through plan/signal fallback, `open_position()` correctly returns that existing position id, but it does not backfill missing `local_order_id` / `broker_order_id` on that pre-existing row.

Do not rewrite Position Manager in this PR. The fill handoff has the strongest broker-confirmed execution identity and should close the missing durable link there.

---

# Required invariant

For every terminal broker-confirmed ENTRY fill, before canonical exit-engine adoption/seed is considered successful:

```text
orders.client_id            == positions.client_id
orders.execution_mode       == positions.execution_mode == exact live|paper
orders.contract             == positions.contract       == exact OCC contract
orders.local_order_id       == positions.local_order_id
orders.broker_order_id      == positions.broker_order_id
orders.position_id          == positions.id
```

Signal and plan provenance are fenced separately:

```text
signal_id:
  - populated ENTRY/position values must agree exactly;
  - the reconciler-plan exception requires an exact, nonblank ENTRY and
    position signal_id.

plan_id:
  - normally the ENTRY and position values must agree;
  - the one permitted exception is a position plan exactly shaped as
    reconciled:{exact OCC}:{nonblank fingerprint}, provided client_id,
    execution_mode, OCC, signal_id, local_order_id and broker_order_id
    are all exact as described above.
```

The reconciled plan is provenance and **must not be overwritten**. Every other
plan mismatch fails closed.

The identity must be proven by rereading durable DB state after the write.

No ticker-only matching. No same-underlying matching. No first-open-position matching. No timestamp-nearest matching. No fallback to another client. No PAPER/LIVE normalization guessing. No overwrite of conflicting durable identity.

If any exact identity conflicts, **HOLD / fail closed**. Do not seed a second owner and do not overwrite the conflicting identity.

---

# Hard scope budget

## Production files

**Exactly one production file is permitted:**

1. `ap/fill_monitor.py`

## Test files

1. Add one focused regression file:
   - `tests/test_p0_filled_entry_durable_identity_handoff.py`
2. `.github/workflows/p0_regression.yml` may change only to add that exact test file to the existing P0 invocation if needed.

## Documentation

- This spec file.

## Forbidden production files

Do **not** modify:

- `ap/position_manager.py`
- `ap_exit_engine.py`
- `ap/order_state_machine.py`
- `ap/order_monitor.py`
- `ap/execution.py`
- `ap_execution_core.py`
- `ap_reconciler.py`
- `ap/manual_close_reconciliation.py`
- scanners
- selectors
- master control
- queue code
- proof writers
- migrations
- broker adapter code

If Codex believes any second production file is required, **STOP** and report the exact blocker instead of expanding scope.

Do not cherry-pick old PR #429. Do not restore its cumulative branch. Use historical #429 only as conceptual evidence for the invariant above.

---

# Exact implementation work

## 1. Add one fill-monitor helper for durable identity closure

Add a private helper in `ap/fill_monitor.py` near `_open_position_safe()`:

```python
def _bind_filled_entry_durable_identity(
    *,
    position_id: str,
    order: dict,
    result: dict,
) -> tuple[bool, str]:
    ...
```

The exact return type may be a tiny immutable result object instead of a tuple if that genuinely reduces ambiguity, but do not build a subsystem.

### Input preconditions

Before opening a transaction, require:

- nonblank `position_id`
- nonblank `client_id`
- nonblank `local_order_id`
- proven non-placeholder `broker_order_id`
- `execution_mode` exactly `live` or `paper`
- nonblank exact contract
- `kind == ENTRY`
- positive integral filled quantity from `result.filled_qty` / durable order state

Use existing helpers where available. Do not add a parallel identity-normalization framework.

Missing or malformed required identity returns failure with **zero DB mutation**.

### Transaction shape

Use the existing DB helpers (`conn`, `run_with_retry`). Inside one transaction:

1. Lock the exact ENTRY row with `FOR UPDATE` using:
   - `client_id`
   - `local_order_id`

2. Lock the exact canonical position row with `FOR UPDATE` using:
   - `id = position_id`
   - `client_id`

3. Validate **all conflicts before performing any update**.

### Required ENTRY-row validation

The locked order must prove:

- `kind = 'ENTRY'`
- terminal filled state appropriate to this already-confirmed fill path (`FILLED`; do not broaden lifecycle taxonomy)
- exact same `client_id`
- exact same `execution_mode`
- exact same OCC contract
- exact same non-placeholder broker order id
- positive durable `filled_qty`
- `position_id` is either NULL/blank or already exactly `position_id`

If the row already contains a different nonblank `position_id`, return conflict. **Never overwrite it.**

### Required position-row validation

The locked position must prove:

- exact same `client_id`
- exact same `execution_mode`
- exact same OCC contract
- active/open canonical position state used by the fill path
- if both sides have `signal_id`, they must be exactly equal
- if both sides have `plan_id`, they must be exactly equal
- `local_order_id` is NULL/blank or already exactly the ENTRY local id
- `broker_order_id` is NULL/blank or already exactly the ENTRY broker id

If either durable execution id contains a different nonblank value, return conflict. **Never overwrite it.**

Do not accept same ticker as proof of position identity.

### Allowed mutations

After all validation passes:

- backfill `positions.local_order_id` only when NULL/blank
- backfill `positions.broker_order_id` only when NULL/blank
- bind `orders.position_id` only when NULL/blank
- update normal timestamp columns only as required by existing repository conventions

Do not rewrite existing identical values merely for activity timestamps if avoidable.

### Mandatory postcondition reread

Still inside the transaction, reread both locked rows and require:

```text
positions.id              == position_id
positions.client_id       == client_id
positions.execution_mode  == order.execution_mode
positions.contract        == order.contract
positions.local_order_id  == order.local_order_id
positions.broker_order_id == order.broker_order_id
orders.position_id        == position_id
orders.local_order_id     == order.local_order_id
orders.broker_order_id    == order.broker_order_id
```

Return success only after this reread passes.

---

## 2. Change the ENTRY-fill handoff ordering, and nothing else

Current order is effectively:

```text
open/find canonical position
-> standing stop
-> seed exit engine
-> best-effort order.position_id write
```

Change only the canonical identity portion to:

```text
open/find canonical position
-> standing stop (leave current best-effort protection semantics unchanged)
-> _bind_filled_entry_durable_identity(...)
-> if bind fails: HOLD, diagnostic, DO NOT seed new canonical owner
-> if bind succeeds: _seed_exit_engine(...)
```

Remove the old standalone best-effort `UPDATE orders SET position_id=...` block after `_seed_exit_engine()`. Its responsibility moves into the atomic identity helper.

Do not move, redesign, retry, deduplicate, or otherwise alter broker standing-stop behavior in this PR.

Do not alter OSM terminal transition behavior.

Do not alter `_open_position_safe()` except the minimum call/diagnostic plumbing required by this invariant.

---

## 3. Make `_seed_exit_engine()` report success/failure without redesigning it

The current helper returns `None` on success and on several failure/hold paths, so the caller cannot truthfully know whether canonical ownership was established.

In this same file only, make `_seed_exit_engine()` return a tiny result, for example:

```python
(True, "ADOPTED")
(True, "ALREADY_CANONICAL")
(True, "SEEDED")
(False, "MODE_UNPROVEN")
(False, "ADOPTION_RETRY")
(False, "SEED_FAILED")
```

Do not change the underlying exit-engine algorithms.

Map the helper's **existing** successful branches to success:

- structured adoption disposition `ADOPTED`
- structured adoption disposition `ALREADY_CANONICAL_REPAIR_REMOVED`
- existing canonical position already present by exact `position_id`
- `seed_position(...)` returns without exception
- fallback `add_position(...)` returns without exception

Map the helper's **existing** fail/hold branches to failure:

- unresolved side
- execution mode unresolved/conflicting
- `RETRY_*` adoption disposition
- adoption exception
- seed/add-position failure

Do not add another exit-engine owner registry, another repair scanner, or another broker-repair subsystem.

### Caller behavior

After a successful durable identity bind:

- call `_seed_exit_engine()` once
- if it reports success, continue existing behavior
- if it reports failure, write/emit a durable visible marker:
  - `FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN`
- do not attempt a second seed in the same pass
- do not create another position
- do not submit or cancel a broker order because of this failure

If durable identity binding fails, use:

- `FILLED_ENTRY_POSITION_IDENTITY_BIND_FAILED`

Do not invent a large taxonomy. Those two markers are enough.

Use an exact client + local order update when persisting the marker. Never mutate a foreign row.

---

# No restart-recovery expansion in this PR

This PR fixes the hot path that creates new bad identity.

Do **not** add a broad restart scanner, startup reconciliation subsystem, historical repair loop, or DB cleanup job here.

Historical rows can be repaired separately under an explicit exact-identity operation after this hot path is proven. If Codex sees a restart-recovery concern, document it as follow-up evidence rather than expanding this PR.

This boundary is intentional. The goal is to get the normal broker fill path boring again before adding recovery machinery.

---

# Required regression cases

Create `tests/test_p0_filled_entry_durable_identity_handoff.py`.

Use production-shaped tests. At minimum prove all of the following.

## A. Jason PEP exact incident replay

Initial durable state:

```text
client = jasoncosby1@gmail.com
mode = live
contract = PEP260821C00141000
ENTRY local = 1b8ec150-38cc-424c-93e5-b1a1820ea50b
ENTRY broker = 141999576
fill = 1.58
position = 86ed1496-75ef-472c-8a36-94eee1698509
orders.position_id = NULL
positions.local_order_id = NULL
positions.broker_order_id = NULL
```

The canonical position is otherwise exact by client/mode/contract/signal/plan.

After the handoff:

```text
orders.position_id          = canonical position id
positions.local_order_id    = exact ENTRY local id
positions.broker_order_id   = exact ENTRY broker id
```

Assert the bind completes **before** `_seed_exit_engine()` is called.

## B. Existing already-correct identity is idempotent

All three durable links already match. Re-running the helper succeeds with no duplicate position and no conflict.

## C. Conflicting order.position_id

Filled ENTRY already points to a different nonblank position id.

Expected:

- failure
- zero overwrite
- zero exit-engine seed
- zero broker mutation

## D. Conflicting position.local_order_id

Canonical position contains another nonblank local ENTRY id.

Expected zero overwrite and zero seed.

## E. Conflicting position.broker_order_id

Canonical position contains another nonblank broker ENTRY id.

Expected zero overwrite and zero seed.

## F. Wrong client

Same contract and even same broker/local-looking values under another client must never qualify.

## G. Wrong execution mode

LIVE order cannot bind PAPER position and vice versa.

## H. Wrong OCC, same ticker

A different PEP OCC contract can never qualify. Explicitly cover this so the reconciler-style same-underlying mistake cannot reappear in ENTRY identity.

## I. Wrong signal/plan identity

Exact contract but conflicting durable signal/plan identity returns HOLD and zero mutation.

## J. Missing/placeholder broker id

Blank, `N/A`, boolean-like placeholder, or otherwise unproven broker identity returns failure before DB mutation.

## K. `_seed_exit_engine()` result truth

Prove that existing success paths report success and existing RETRY/error paths report failure without adding broker calls.

## L. Handoff failure marker

Binding failure records `FILLED_ENTRY_POSITION_IDENTITY_BIND_FAILED` on only the exact ENTRY row and never seeds.

## M. Owner-unproven marker

Binding succeeds but seed/adoption reports failure. Record `FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN`; do not perform a second seed or broker mutation.

## N. Broker mutation trap

Trap broker mutation methods and prove this PR introduces:

- zero new ENTRY POST
- zero new ordinary EXIT POST
- zero new broker cancel

The existing standing-stop call is pre-existing behavior and must not be increased, retried, or duplicated by the patch.

---

# Required adjacent regression slice

Run at minimum:

```bash
python -m pytest -q \
  tests/test_p0_filled_entry_durable_identity_handoff.py \
  tests/test_fill_monitor_mvp_hardening.py \
  tests/test_p0_live_executable_bid_pnl.py \
  tests/test_p0_broker_owned_exit_requested_recovery.py \
  tests/test_p0_reconciler_external_close_broker_truth.py
```

Then:

```bash
python -m py_compile ap/fill_monitor.py
git diff --check
```

Then run exact-head P0 Regression Suite on the final implementation head.

Do not claim merge readiness from local tests alone.

---

# Explicit non-goals

This PR must not change:

- trade admission
- scanner scores
- Gate G
- trigger levels
- watcher expiry
- direction-reversal policy
- selector ranking
- DTE ladder
- quote quality thresholds
- moneyness thresholds
- affordability
- quantity / sizing
- retry count
- stale-entry chase behavior
- stop thresholds
- target thresholds
- runner behavior
- scale-out behavior
- P&L math
- broker fill truth
- reconciler economics
- manual-close reconciliation
- proof-trade schema or proof writer behavior
- queue/fanout
- LIVE/PAPER account routing
- migrations
- historical data cleanup

No threshold tuning belongs here.

---

# Codex stop conditions

Codex must stop and report instead of expanding the branch if any of these occur:

1. A second production file appears necessary.
2. The fix seems to require changing broker submit/cancel behavior.
3. The fix seems to require changing Position Manager idempotency semantics globally.
4. The fix seems to require changing exit-engine owner-domain logic.
5. The fix seems to require a migration.
6. The fix starts touching selector/watcher/retry/intelligence code.
7. Production-shaped tests reveal that the canonical position cannot be proven by exact client + mode + OCC + plan/signal + broker/local ENTRY identity.

Bring the blocker back for review. Do not solve it by broadening scope.

---

# Acceptance trace

The final implementation must be explainable in this exact short trace:

```text
broker ENTRY FILLED
-> OSM terminal fill truth
-> open/find canonical DB position
-> preserve existing standing-stop behavior
-> atomically prove/backfill exact ENTRY <-> position durable identity
-> reread and prove postcondition
-> seed/adopt canonical exit owner once
-> truthful success/failure result
```

The change is complete when future normal fills cannot leave the system in the August 14 PEP shape:

```text
FILLED ENTRY with broker id
+ canonical position exists
+ orders.position_id missing
+ positions.local_order_id missing
+ positions.broker_order_id missing
```

That state must become impossible on the normal fill path without adding any new trading authority.

**Current verdict: IMPLEMENTED / REVIEWED.** The scoped implementation and
exact-head P0 verification passed. The PR remains open/Draft pending formal
review; no merge or deployment is authorized by this PR.
