# P0 SPEC — Recover PENDING_TRIGGER broker submit intent without repost

## Status

IMPLEMENTED IN PR #472 / REVIEW REQUIRED. Do not merge or deploy until release review is complete.

## One production problem

A broker-submit crash window can leave an ENTRY row in this durable shape:

```text
status = PENDING_TRIGGER
exact OCC contract selected
qty > 0
meta.submit_intent_at present
meta.broker_submit_key present
meta.lifecycle_state = SUBMITTING
broker_order_id = NULL
submitted_ts = NULL
```

Once `submit_intent_at` exists, AP no longer knows whether the broker accepted the POST before the process lost the response. The row is broker-ambiguous.

It must NOT be phantom-canceled by age, rearmed to the watcher, reselected, sent through another submit path, or treated as proof that no broker order exists.

The only safe next action is exact broker reconciliation using the durable submit identity.

## Production regression fixture

Use the sanitized production shape from Tradefluence PAPER JNJ:

```text
client_id       = tradefluencehq@gmail.com
execution_mode  = paper
local_order_id  = 9f1907fe-7ab4-4e5a-b305-7fc9129f684d
symbol          = JNJ
contract        = JNJ260821C00260000
status          = PENDING_TRIGGER
qty             = 4
limit_price     = 4.57
broker_order_id = NULL
submitted_ts    = NULL

meta.watcher_audit.reason_code = trigger_ready
meta.selected_contract         = JNJ260821C00260000
meta.selected_qty              = 4
meta.live_submit_gate.all_passed = true
meta.current_owner             = broker_submit:9f1907fe-7ab4-4e5a-b305-7fc9129f
meta.lifecycle_state           = SUBMITTING
meta.submit_intent_at          = present
meta.submit_started_at         = present
meta.broker_submit_key         = 9f1907fe-7ab4-4e5a-b305-7fc9129f
meta.broker_submit_payload_hash = present
```

This PR does NOT repair that historical production row. It uses the shape as a regression fixture only.

## Required invariant

For an exact-mode ENTRY row:

```text
PENDING_TRIGGER + durable submit intent + no durable broker_order_id
```

means BROKER OWNERSHIP AMBIGUOUS.

Therefore there is NO new broker POST, broker cancel, selector retry, watcher rearm, age-based phantom cancel, or terminal cleanup until exact broker truth resolves the submit intent.

If exactly one remote order matches all required identity:

```text
canonical broker submit tag
+ exact OCC contract
+ buy_to_open
+ exact integral quantity
```

then AP may adopt only:

```text
PENDING_TRIGGER -> SUBMITTED
broker_order_id = exact remote broker id
```

After that, the normal fill monitor owns every later broker state.

Even when the broker listing already reports `filled`, `partially_filled`, `rejected`, `canceled`, or `expired`, this recovery path must stop locally at `SUBMITTED`. It must NOT write fill quantity, fill price, FILLED, PARTIAL_FILL, or terminal state. Fill monitor must poll the exact adopted broker id and process normal truth.

This preserves PR #470's canonical filled ENTRY identity handoff.

## HARD FILE BUDGET

### Production: exactly seven files

1. `ap/db.py`
2. `ap_reconciler.py`
3. `ap_execution_core.py`
4. `client_runner.py`
5. `ap/order_monitor.py`
6. `ap_recovery.py`
7. `ap/brokers/tradier.py`

`ap/order_monitor.py` and `ap_recovery.py` are required lifecycle fences, not a broader recovery redesign:

- `ap/order_monitor.py` must not hydrate, rearm, or ghost-sweep an evidence-bearing broker-ambiguous row before reconciler ownership resolves.
- `ap_recovery.py` must not stale-expire, terminalize, or watcher-reseed that row before reconciler ownership resolves.

Without these two fences, existing hydration, stale cleanup, ghost sweep, and startup reseed paths can mutate a `PENDING_TRIGGER` row that already has durable submit evidence before broker reconciliation runs.

`ap/brokers/tradier.py` is required only at the broker-listing seam: it requests `includeTags=true` and the bounded maximum `limit=1500`, so the reconciler receives durable submit tags and can search the current account-order window instead of the Tradier default first 25 orders. It does not add POST, cancel, fill, or terminal-state behavior.

No other production files are in scope.

### Tests and spec

1. Add `tests/test_p0_pending_trigger_broker_intent_recovery.py`.
2. `.github/workflows/p0_regression.yml` may change only to add that exact focused test if needed.
3. This spec document.

If an eighth production file appears necessary, STOP and report the exact blocker. Do not expand scope.

## Forbidden production edits

Do NOT modify OSM, entry watcher, contract selector/revalidator, sizing/risk policy, scanners, queue, fill monitor, position manager, exit engine, proof writers, manual-close reconciliation, reconciler position economics, intelligence, migrations, Render, scheduled jobs, or package/import-root files.

The `ap/order_monitor.py` and `ap_recovery.py` edits remain limited to the lifecycle fences stated above; they do not authorize a general pending-trigger restart-recovery or classifier redesign.

Historical #445/#456 are evidence only. Do not cherry-pick either branch.

# Exact implementation

## 1. `ap/db.py` — narrow read aperture

Update `get_open_orders_for_reconcile(...)` so the existing open-status set remains unchanged, plus only this ENTRY exception:

```sql
status = 'PENDING_TRIGGER'
AND kind = 'ENTRY'
AND (
  submitted_ts IS NOT NULL
  OR NULLIF(BTRIM(COALESCE(meta->>'submit_intent_at','')), '') IS NOT NULL
)
```

Do NOT broadly include all `PENDING_TRIGGER` rows. Ordinary watcher-owned rows with no submit evidence remain invisible to broker reconciliation. Preserve exact client and exact normalized execution-mode filtering. No DB writes.

## 2. `client_runner.py` — wire the existing core

When constructing `APBrokerReconciler`, pass the already-existing client execution core as `execution_core=self.core` (or the current exact attribute if named equivalently).

Do not construct a second core. Do not change runner startup order.

## 3. `ap_reconciler.py` — route submit-intent rows before phantom cleanup

Add optional backward-compatible constructor input `execution_core=None` and store it.

In `_reconcile_orders`, after exact row-mode validation but BEFORE generic missing-broker-id handling, detect:

```text
kind = ENTRY
status = PENDING_TRIGGER
AND durable submitted_ts or meta.submit_intent_at is present
```

Route only that row to one small helper such as `_reconcile_pending_trigger_submit_intent(order, summary)`.

The helper must:

1. require exact local_order_id, client_id, execution_mode;
2. require `self.execution_core`;
3. call only `execution_core.reconcile_deferred_broker_intent(local_order_id=...)`;
4. never invoke selector/watcher/materialization;
5. never call broker submit/cancel itself;
6. never fall through to `_handle_order_without_broker_id()` in the same pass.

Outcome mapping:

```text
ALREADY_RECONCILED / SUBMITTED -> broker ownership accepted; stop this row this pass
RECONCILE_PENDING / KEEP_WATCHER -> HOLD; diagnostic only
NOT_IN_CRASH_WINDOW -> HOLD on this route; never reinterpret as safe to POST
unknown/malformed -> HOLD
```

Do not add a retry scheduler. Do not age-cancel these rows.

## 4. `ap_execution_core.py` — adoption stops at SUBMITTED

Use the existing `reconcile_deferred_broker_intent(local_order_id=...)`. Do not create a second implementation.

Preserve its no-POST design.

A remote broker order may be adopted only when exactly one order matches:

- canonicalized durable `broker_submit_key` tag;
- exact full OCC contract equal to the durable selected/order contract;
- `side == buy_to_open`;
- exact positive integral quantity equal to durable `qty`.

HOLD on broker listing unavailable/malformed, zero matches after durable submit intent, multiple tag matches, same ticker/wrong OCC, wrong side, missing/placeholder broker id, malformed/fractional/nonpositive qty, qty mismatch, client mismatch, or execution-mode mismatch.

No ticker-only, nearest-time, nearest-strike, same-underlying, or price-similarity fallback.

### Critical lifecycle change

Current code can adopt a remote terminal/fill status immediately after setting `SUBMITTED`. Remove that behavior from this method.

After an exact broker match, persist only:

```text
status = SUBMITTED
broker_order_id = exact remote broker id
```

Remote status may be retained only as diagnostic metadata.

Do NOT transition here directly to FILLED, PARTIAL_FILL, REJECTED, CANCELED, or EXPIRED. Do NOT write filled_qty or fill_price here. Normal fill monitor must own the next transition using the exact broker id.

If an existing trustworthy broker-submission timestamp helper is already available in this file, it may be used. Do not invent chronology from reconciliation time merely to populate `submitted_ts`; otherwise leave it unchanged/null.

# Broker authority

Permitted: read/list broker orders.

Forbidden: submit/place/replace/cancel broker calls. The focused test must trap mutation methods and prove zero calls.

# Required tests

Create only `tests/test_p0_pending_trigger_broker_intent_recovery.py`.

Minimum cases:

1. Exact Tradefluence JNJ-shaped row + one matching working remote -> local `SUBMITTED`, exact broker id adopted, zero POST/cancel.
2. Remote status `filled` -> local still only `SUBMITTED`; no fill price/qty written here.
3. Remote `partially_filled` -> local still only `SUBMITTED`.
4. Remote `rejected/canceled/expired` -> local still only `SUBMITTED`; fill monitor owns terminal truth.
5. Durable submit intent + zero remote match -> remains `PENDING_TRIGGER`, zero POST/cancel, no phantom cancel.
6. Multiple tag matches -> HOLD.
7. Same ticker/wrong OCC -> HOLD.
8. Wrong side -> HOLD.
9. Fractional/malformed/wrong qty -> HOLD.
10. Wrong client -> untouched.
11. LIVE/PAPER mismatch -> untouched.
12. Ordinary `PENDING_TRIGGER` with no submit evidence is NOT returned by `get_open_orders_for_reconcile`.
13. Evidence-bearing `PENDING_TRIGGER` is returned only for exact client/mode.
14. Reconciler missing `execution_core` -> HOLD, no age cancel.
15. `client_runner` passes its exact existing core instance into reconciler.
16. Broker mutation trap -> zero submit/replace/cancel across all recovery cases.

Run adjacent reconciler exact-mode tests, existing deferred broker-intent tests, fill monitor normal ENTRY tests, and PR #470 handoff tests if present on the integration base.

# Acceptance trace

```text
PENDING_TRIGGER ENTRY + durable submit intent + no broker id
  -> reconciler narrow read aperture
  -> exact client/mode fence
  -> execution_core.reconcile_deferred_broker_intent
  -> read broker list by durable submit tag

0 match / ambiguity / mismatch
  -> HOLD, zero POST

exactly one exact match
  -> adopt exact broker_order_id
  -> local SUBMITTED ONLY
  -> normal fill monitor
  -> normal ACK/PARTIAL/FILLED/terminal truth
  -> PR #470 exact filled ENTRY identity handoff
```

# Explicit non-goals

Do not fix generic STUCK_TRIGGER_READY without submit intent, watcher rearm, deferred selector retries, selector quote budget (#471), historical #456 quote-cache/scheduled-job issues, sizing metadata, intelligence/Gate G (#434), post-cancel retry (#430), or FILLED-entry restart recovery after #470 failure markers.

# Codex instruction

Implement this spec exactly on the PR branch. Do not restore historical #445/#456 wholesale. Keep production changes to the seven named files and one focused test. If an eighth production file is required, STOP and explain the blocker instead of expanding scope.

Do not merge, deploy, change env vars, mutate production data, or clean historical rows.

Run focused tests, adjacent reconciler/fill-monitor tests, `python -m py_compile` on changed production files, `git diff --check`, then exact-head P0 Regression Suite.
