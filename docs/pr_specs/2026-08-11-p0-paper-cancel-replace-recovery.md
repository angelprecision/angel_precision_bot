# P0 SPEC — Entry cancel/replace requires broker-confirmed terminal truth

## Status

**SPEC ONLY / HARD HOLD. DO NOT MERGE OR DEPLOY AS AN IMPLEMENTATION.**

This branch predates current `main@f26d31cef3d3bf5d3c5d5ff16fb260f245602f89`. Rebase onto current main before implementation. Because PR #478 also changes the Tradier adapter, implement/rebase this PR after #478 so one reviewed adapter contract exists rather than two competing versions.

## Audit amendment — proven current-main defect

The 2026-08-15 lifecycle audit proved an exact cancel/fill race in the existing entry repeg path.

Current path:

```text
ap/order_monitor.py::_check_stale_entry_cancel()
  -> _try_repeg()
  -> ap/retry_engine.py::apply_repeg()
  -> broker.cancel_order(old_broker_order_id)
  -> local order reprice bookkeeping
  -> broker.place_order(... buy_to_open ...)
```

Current `TradierBroker.cancel_order()`:

1. sends DELETE;
2. computes `ok` from DELETE HTTP status / immediate response;
3. re-queries the order into `confirmed_status`;
4. returns the original `ok` without making `confirmed_status` authoritative for replacement safety.

Therefore this is possible:

```python
{
  "ok": True,
  "status": "filled",
  "error": None,
}
```

`apply_repeg()` currently rejects only a missing cancel result or a result carrying `error`. It does not reject confirmed `filled` / `partially_filled`, so it can continue into another `buy_to_open` after the old order filled.

### Important audit correction

Do **not** repeat the claim that this line clears the durable broker id before replacement:

```python
update_order(... broker_order_id=None)
```

Current `ap.db.update_order()` only includes `broker_order_id` in the SQL UPDATE when the argument is not `None`. So passing `None` leaves the old broker id in place. The dangerous sequence is instead:

```text
cancel HTTP success
-> broker re-query may say FILLED
-> apply_repeg ignores that filled confirmation
-> local status may move to CREATED while old broker id remains
-> replacement buy_to_open is submitted
-> on replacement success, broker_order_id is overwritten with the new id
```

That eventually loses durable primary ownership of the old filled broker order while the account can hold both fills.

## Binding invariant

**A replacement ENTRY POST is forbidden until the exact prior broker order is proven terminal and incapable of filling.**

Authoritative replacement outcomes:

```text
old broker status = canceled/cancelled  -> may continue to fresh revalidation
old broker status = rejected            -> may continue only if existing policy treats it as replaceable
old broker status = expired             -> may continue only if existing policy treats it as replaceable
old broker status = filled              -> STOP replacement; route to fill ownership
old broker status = partially_filled    -> STOP full replacement; route cumulative fill truth first
old broker status = pending/open/working/accepted/etc -> old order still owns broker risk; zero replacement
broker status unavailable/unknown/error -> HOLD; zero replacement
conflicting/malformed broker truth       -> HOLD; zero replacement
```

HTTP 200/204 from the cancel request is not terminal proof by itself.

## Required production changes

### 1. `ap/brokers/tradier.py::cancel_order`

Return a result whose replacement-safety meaning reflects the confirmed broker state, not merely DELETE transport success.

Required fields may keep existing shape, but callers must be able to distinguish at minimum:

- cancel request accepted and broker confirms canceled;
- broker confirms filled;
- broker confirms partially filled;
- broker confirms still active;
- confirmation unavailable/ambiguous;
- cancel request itself failed.

Preferred behavior for the current dict contract:

```text
confirmed canceled          -> ok=True, terminal=True, replacement_safe=True
confirmed filled            -> ok=False for cancel/replace authorization, status='filled', terminal=True, replacement_safe=False
confirmed partially_filled  -> ok=False, status='partially_filled', replacement_safe=False
confirmed active            -> ok=False, replacement_safe=False
confirmation unavailable    -> ok=False, status='unknown', replacement_safe=False
```

If changing generic `ok` semantics would break unrelated consumers, add explicit authoritative fields such as `cancel_confirmed` / `replacement_safe` and migrate **every current consumer** that relies on `ok`. Do not leave one caller on transport truth and another on broker truth.

The post-cancel status lookup must not convert query failure into confirmed cancellation.

Coordinate with #478: position-query failure semantics and cancel-confirmation failure semantics should follow the same principle that unavailable broker truth stays unavailable.

### 2. `ap/retry_engine.py::apply_repeg`

Before any local CREATED/reprice mutation or replacement POST:

- require exact old `broker_order_id`;
- call the adapter cancel path;
- inspect authoritative confirmed result;
- replacement may continue only on a broker-confirmed safe terminal cancellation outcome;
- `filled` -> preserve old broker identity, zero replacement POST, return a distinct disposition requiring fill reconciliation;
- `partially_filled` -> preserve old broker identity, zero full replacement POST, return a distinct disposition requiring cumulative partial-fill reconciliation;
- active/unknown/unavailable -> HOLD, preserve old broker identity, zero replacement POST;
- never transition the local lifecycle back toward pre-submit state on filled/partial/unknown truth.

Do not call fill-monitor internals directly from retry_engine if that would create a second fill owner. Instead surface a deterministic result that the existing order monitor/fill monitor can consume or simply leave the canonical broker-backed order visible for fill monitor to process.

### 3. Preserve order identity until replacement acceptance is proven

For a confirmed canceled old order, preserve durable lineage:

- prior broker order id;
- current broker order id;
- local order id;
- replacement generation / repeg attempt;
- client id;
- execution mode;
- signal/canonical signal id;
- contract and qty;
- original/replacement limit;
- cancel request timestamp;
- broker confirmation status/timestamp.

Do not depend on `broker_order_id=None` to clear a column. If a deliberate clear is ever required, use a dedicated explicit DB primitive with exact CAS semantics. This PR should prefer retaining old identity until the new broker id is accepted and then store prior identity in durable lineage metadata.

## Replacement submission ambiguity

The replacement `broker.place_order()` already passes a canonical tag derived from the local order id. That is useful but not sufficient if caller behavior after an exception is ambiguous.

For any replacement POST outcome where bytes may have reached the broker but no trustworthy response id returned:

- do not issue another blind POST;
- reconcile/lookup by canonical tag using the hardened OSM/broker identity pattern;
- accepted-by-tag -> adopt the exact broker id;
- no authoritative answer -> HOLD/reconcile, not retry;
- only an explicitly safe pre-POST failure may be retried according to an existing bounded policy.

If implementing proper ambiguous-submit handling requires moving replacement submission behind the canonical OSM submit authority, do that rather than reproducing its logic inside retry_engine. Do not create a third ambiguity classifier.

## Separation from PR #475

This PR owns **cancel/replace broker truth and old/new order non-overlap**.

PR #475 owns **post-cancel retry orchestration and removal of `ap.execution.process_signal()`**.

Do not let both PRs become replacement submitters.

Required sequencing:

1. #478 broker-truth adapter semantics;
2. this #440 cancel/replace truth fence;
3. #475 rebase on the result and route any later post-cancel retry through one canonical ENTRY authority.

## Existing PAPER incident continuity

Retain the useful original reproduction cases from 2026-08-11:

- José PAPER AAPL / TSLA replacement HTTP 400;
- Tradefluence PAPER AAPL / AVGO replacement HTTP 400;
- TSLA MISSED_MOVE terminal behavior.

Preserve sanitized Tradier HTTP status and provider payload for failed replacement requests. Never persist credentials/tokens/auth headers.

MISSED_MOVE remains terminal. No aggressive chase or market-entry fallback is authorized.

## Expected production scope

Starting set:

- `ap/brokers/tradier.py`
- `ap/retry_engine.py`

`ap/order_monitor.py` may be touched only if the exact `apply_repeg` result needs to be routed into existing fill/reconcile ownership or diagnostics. Do not add another broker submitter there.

`ap/order_state_machine.py` should be reused for ambiguity/idempotency authority if needed, not copied.

Do not change scanners, signal admission, selector quality thresholds, sizing, exit strategy, proof economics, or queue fanout.

## Required tests

Create or extend a focused current-main suite, preferably `tests/test_p0_entry_cancel_replace_terminal_truth.py`.

Minimum cases:

1. DELETE 200 + re-query canceled -> replacement eligibility may proceed.
2. DELETE 204 + re-query canceled -> may proceed.
3. DELETE 200 + re-query filled -> zero replacement POST.
4. DELETE 200 + re-query partially_filled -> zero full replacement POST.
5. DELETE 200 + re-query pending/open/working -> zero replacement.
6. DELETE 200 + re-query unknown -> zero replacement.
7. DELETE 200 + get_order timeout -> zero replacement.
8. DELETE 200 + get_order connection failure -> zero replacement.
9. DELETE 200 + malformed confirmation -> zero replacement.
10. DELETE failure -> zero replacement.
11. filled race preserves old broker_order_id durably until fill monitor consumes it.
12. partial-fill race preserves old broker_order_id and cumulative fill visibility.
13. filled/partial race does not transition local status to CREATED.
14. unknown confirmation does not transition local status to CREATED.
15. `update_order(... broker_order_id=None)` behavior is explicitly covered so no test assumes it clears the column.
16. confirmed canceled + still-valid fresh setup -> at most one replacement POST.
17. replacement accepted -> exact new broker id stored, prior broker id retained in lineage diagnostics.
18. replacement rejected 4xx -> no second POST; exact sanitized provider response durable.
19. replacement read-timeout/5xx ambiguous -> tag lookup/adoption or HOLD, zero blind second POST.
20. duplicate monitor ticks -> one cancel/replace generation maximum.
21. restart after cancel request before confirmation -> no replacement without broker proof.
22. restart after confirmed cancel before replacement -> at most one replacement generation.
23. restart after replacement accepted before DB persistence -> broker tag lookup prevents duplicate.
24. client mismatch -> zero broker mutation.
25. execution_mode mismatch -> zero broker mutation.
26. PAPER order cannot authorize LIVE replacement.
27. LIVE order cannot use sandbox identity.
28. qty/contract mismatch -> HOLD.
29. late fill observed while cancellation in flight -> fill truth wins.
30. partial fill observed while cancellation in flight -> cumulative fill truth wins.
31. MISSED_MOVE -> zero replacement.
32. no pre-fill position/proof mutation is fabricated by cancel/replace logic.

Run adjacent order-monitor, retry-engine, fill-monitor, OSM idempotency, broker adapter, and #475-focused suites after rebasing.

## Money-path audit

- Live behavior: **YES.** This closes duplicate-entry risk.
- Broker cancel: **YES, existing path only; semantics hardened.**
- Broker submit: **YES, existing replacement path; no new submitter.**
- Orders: status/identity lineage may change only after authoritative broker truth.
- Positions/proof: no fabricated mutation; fills are delegated to canonical fill ownership.
- Queue: no change in this PR.
- client_id/execution_mode: exact.
- Diagnostics: preserve transport result separately from confirmed broker status.
- PAPER/LIVE taxonomy: strict.
- Could this make Jason trade junk? The current race can duplicate a LIVE entry. The repair must make filled/partial/unknown old-order truth categorically ineligible for a replacement buy.

## Claude implementation instruction

Before coding:

1. rebase onto current main after #478;
2. read every caller of `TradierBroker.cancel_order()` and document expected semantics;
3. read `_try_repeg`, `apply_repeg`, order-monitor broker-status handling, fill-monitor cumulative fill handling, and OSM tagged submit ambiguity logic;
4. reproduce DELETE-200 + confirmed-FILLED with a failing production-path test.

Then implement the smallest single-authority repair.

Before requesting review, update the PR with:

- exact base/head SHA;
- changed-file list;
- cancel_order caller inventory;
- before/after cancel result contract;
- exact old/new broker-order identity lineage;
- broker call counts for all race cases;
- restart/idempotency proof;
- client/mode proof;
- focused + adjacent test counts;
- exact-head CI;
- fresh MERGE / HOLD / HARD HOLD recommendation.

No merge, deploy, migration, environment mutation, or production-data mutation is authorized.