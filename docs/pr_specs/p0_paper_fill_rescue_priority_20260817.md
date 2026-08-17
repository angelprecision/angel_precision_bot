# P0 — PAPER fill rescue must own marketable-unfilled ENTRY before generic MISSED_MOVE

**Status:** SPEC ONLY / HARD HOLD / DO NOT MERGE OR DEPLOY AS CREATED

**Base:** `main@d0d37e79ae698e604eb8080065d2314b161de351`

## Production incident and exact defect

On 2026-08-17, Tradefluence PAPER sent four valid BUY_TO_OPEN orders to Tradier sandbox and received zero fills even though every selected contract subsequently moved materially green. Jose reproduced the same failure class. The initial broker-submit pricing was not stale:

- NOW: current ask 1.51, submitted limit 1.53
- SMCI: current ask 1.27, submitted limit 1.29
- QCOM: current ask 2.10, submitted limit 2.12
- UNH: current ask 3.50, submitted limit 3.52

The defect is control ordering inside `ap/order_monitor.py::_check_stale_entry_cancel()`.

Current main executes generic missed-move logic before PAPER-specific sandbox rescue:

```text
SUBMITTED / ACKNOWLEDGED PAPER ENTRY
-> age >= MISSED_MOVE_MIN_SECS (default 6)
-> _get_option_price(symbol)
-> _try_repeg(...)
-> if current > submitted_limit * MISSED_MOVE_PRICE_MULT (default 1.07)
-> _handle_stale_entry(... cancel ...)
-> return True

ONLY LATER, inside the hard-age branch:
-> resolve client mode
-> _paper_entry_retry_and_fallback(...)
-> 10s / 25s / 40s fresh-quote reprices
-> 45s bounded market fallback
```

That means the generic branch can cancel the exact PAPER order at ~6-11 seconds before the PAPER-only rescue added by merged PR #69 gets authority. The code designed specifically for Tradier sandbox non-fill is therefore downstream of an earlier terminal branch.

This PR owns **ordering and ownership only**. It does not invent a new repricer, submitter, retry engine, broker adapter, or market-order path.

## Hard dependencies and overlap

### Dependency: PR #440 first

The current `_paper_replace(...)` helper performs:

```text
broker.cancel_order(old_broker_id)
-> broker.place_order(replacement)
-> overwrite local broker_order_id
```

PR #440 owns the required broker-truth fence: HTTP cancel success is not proof the old order cannot fill. A replacement may be created only after the exact old broker order is proven terminal/canceled/rejected/expired and unable to fill; broker outcome UNKNOWN must HOLD.

**Do not implement this PR by weakening #440.** Rebase this work onto the exact current main after #440 is implemented/merged, or port #440's terminal-truth primitive into the then-current canonical helper if #440 is superseded.

### Dependency/sequence: PR #475 after this

PR #475 owns true post-cancel ENTRY retry and retirement of `ap.execution.process_signal()` as a second submit authority. This PR must not call `ap.execution.process_signal()`, `ap.post_cancel_retry` submit logic, or create a second post-cancel scheduler.

This PR's job is to rescue a still-owned PAPER broker order **before unnecessary terminal cancellation**. #475 owns what happens after an entry generation is legitimately terminal and eligible for a fresh canonical retry.

### Existing PR #69 is retained

Reuse the current merged methods:

- `APOrderMonitor._paper_entry_retry_and_fallback`
- `APOrderMonitor._paper_replace_at_new_limit`
- `APOrderMonitor._paper_replace_to_market`
- `APOrderMonitor._paper_replace`
- `_stamp_paper_retry_meta`

Do not create parallel helpers with different metadata or broker ownership.

## Required architecture

For an ENTRY whose durable execution mode is PAPER and whose exact broker order is still open/unfilled, the order of authority must be:

```text
broker/order identity proof
-> exact current OCC quote
-> PAPER sandbox rescue state machine
   -> WAIT until next rung
   -> or cancel/terminal-proof/reprice exact order generation
   -> or bounded market fallback after terminal proof
   -> or HOLD if broker truth is unknown
   -> or RESCUE_EXHAUSTED when bounded policy is honestly exhausted
-> only after PAPER rescue is terminal/exhausted may generic stale-entry terminal policy act
```

For LIVE, current generic missed-move behavior must remain unchanged unless a separate reviewed LIVE PR changes it.

## Implementation contract

### 1. Resolve execution mode before generic MISSED_MOVE authority

In `_check_stale_entry_cancel(...)`, derive the authoritative order/client mode before any branch that can cancel or replace an ENTRY.

Required priority:

1. canonical top-level `orders.execution_mode` when present and valid;
2. the monitor's runner-bound `self.client_mode` only as a consistency check / legacy compatibility source if current main still requires it;
3. if top-level and runner mode conflict, HOLD and emit an identity conflict. Never select one silently.

Do not infer PAPER/LIVE from broker URL, client email, environment, or nested arbitrary metadata when canonical top-level identity is available.

### 2. PAPER mode gets a single rescue owner

When all are true:

- kind is ENTRY;
- status is `SUBMITTED` or `ACKNOWLEDGED` (plus only other broker-open ENTRY statuses already intentionally supported by the current helper);
- exact `broker_order_id` is nonblank;
- execution mode is proven PAPER;
- quantity and exact OCC contract are proven;

then generic `_try_repeg()` / `MISSED_MOVE_ENTRY_CANCEL` must not independently cancel/replace the order while PAPER rescue is eligible/active.

There must not be two concurrent PAPER cancel/replace owners.

Recommended implementation shape: make `_paper_entry_retry_and_fallback()` return an explicit typed/disposition result instead of `None`, for example:

```text
NOT_APPLICABLE
WAITING_FOR_RUNG
REPLACED_LIMIT
MARKET_FALLBACK_SUBMITTED
FILLED_OR_TERMINAL_OBSERVED
HOLD_BROKER_TRUTH_UNKNOWN
RESCUE_EXHAUSTED
ERROR_HOLD
```

Names may differ, semantics may not.

The caller must consume the disposition. It may not infer success from a stale in-memory `order` dict after the helper mutates durable broker/local identity.

### 3. Fresh exact-contract quote at every economic action

Keep #69's good behavior: immediately before each PAPER price action, fetch a fresh quote for the **exact durable OCC contract**, using the monitor's intended market-data quote broker (`_quote_broker()` / existing canonical helper), not the old planned premium and not the underlying ticker.

Validation before a reprice/market fallback:

- contract exact/non-placeholder OCC identity;
- bid and ask numeric, finite, positive;
- ask >= bid;
- quote freshness proven under the existing quote timestamp contract where available;
- spread bounded for market fallback;
- current order quantity positive integer;
- exact client/mode/local-order/broker-order identity still agrees with the durable row.

If the quote is missing/stale/malformed, HOLD or wait according to the bounded existing policy. Never manufacture a price from last-only or old selector price.

### 4. Reprice from current ASK, never from planned premium

Each limit rung must be calculated from the current exact-contract ask at that rung:

```text
new_limit = fresh_ask + ENTRY_PAPER_ASK_CROSS_CENTS
```

Preserve existing tick/rounding behavior. Do not use the original selector ask, original order limit, midpoint, underlying movement, or current last as BUY authority.

Persist the same production diagnostics #69 already established: old/new limit, current bid/ask/mid/last, quote timestamp/source if available, spread, attempt number, rung age, broker id generation, outcome.

### 5. Generic MISSED_MOVE becomes observation-only for an active PAPER rescue

A PAPER option moving above `limit * 1.07` while the sandbox order is unfilled is not evidence that Angel Precision should abandon the setup. It is exactly the failure #69 was intended to rescue.

For active/eligible PAPER rescue:

- generic missed-move may emit telemetry such as `PAPER_ENTRY_MISSED_MOVE_OBSERVED`;
- it may not cancel the broker order;
- it may not submit a replacement;
- it may not arm post-cancel retry;
- it may not terminalize the local order.

LIVE generic missed-move behavior remains untouched.

### 6. Broker truth controls every replacement

Consume the #440 canonical terminal-proof primitive. Required transition:

```text
old broker order open/unknown -> zero replacement POST
old broker order partially/fully filled -> consume fill truth; zero duplicate BUY
old broker order terminal-canceled/rejected/expired, exact identity proven -> at most one replacement generation
```

Cancel transport success alone is insufficient.

Late fill between cancel request and confirmation must win. The system must adopt/process that fill, not fire another BUY.

### 7. Market fallback remains PAPER-only and bounded

Keep the existing 45s-style bounded fallback behavior unless production replay proves a different timing is required. This PR is not authorization to tune retry frequency aggressively.

Market fallback requirements:

- execution mode proven PAPER;
- exact old broker order proven terminal first;
- fresh sane spread under `PAPER_ENTRY_MARKET_FALLBACK_MAX_SPREAD_PCT`;
- one fallback maximum per logical order/recovery generation;
- canonical broker submit tag remains bound to local order/generation;
- no LIVE market fallback introduced.

### 8. Do not invoke legacy post-cancel execution

Forbidden imports/calls from the PAPER rescue path:

- `from ap.execution import process_signal`
- `ap.execution.process_signal`
- any second direct broker submit helper outside the existing canonical PAPER replacement owner
- a new retry daemon/thread/timer

#475 will remove legacy post-cancel submit authority separately.

### 9. Durable idempotency and restart

After any process restart, the exact local row/meta/broker truth must be sufficient to decide:

- which PAPER rescue rung already ran;
- whether market fallback was already attempted;
- which exact broker order generation is current;
- whether the previous broker order is terminal, open, filled, partial, or unknown;
- whether the next action is WAIT/HOLD/reprice/fallback/terminal.

A restart may never reset attempts and replay 10/25/40/45 again against the same economic entry.

### 10. Preserve diagnostics downstream

Do not replace rich reasons with a generic `CANCELED` or `retry_failed` bucket.

At minimum preserve/extend:

- `PAPER_ENTRY_MARKETABLE_UNFILLED`
- current quote and quote source/timestamp
- `reprice_attempt_count`
- rung age
- old/new broker id if replacement occurs
- old/new limit
- terminal broker proof used before replacement
- fallback used/outcome
- missed-move observed but suppressed because PAPER rescue owned the order
- final terminal reason if rescue exhausts

## Production file budget

Expected production file:

1. `ap/order_monitor.py`

Tests:

- new `tests/test_p0_paper_fill_rescue_priority.py`
- reuse/extend #440 tests after its implementation
- register focused test in `.github/workflows/p0_regression.yml`

If implementation requires a second production file because current-main canonical broker-terminal proof lives elsewhere, STOP and document why before editing that file. Do not casually widen into `ap_execution_core.py`, `ap/execution.py`, selector, watcher, master control, position manager, exit engine, proof logger, queue, or Tradier adapter.

## Required fail-first replays

Before production code changes, reproduce on exact current main:

1. PAPER order age 6-11s, current option > 1.07x limit, helper not yet granted rescue authority -> generic `MISSED_MOVE_ENTRY_CANCEL` fires.
2. NOW 1.51 ask / 1.53 submitted limit, later option > 2.20 -> cancel before rescue.
3. SMCI 1.27 ask / 1.29 submitted limit, later > 1.73 -> cancel/retry topology.
4. QCOM 2.10 ask / 2.12 limit, later > 2.83.
5. UNH 3.50 ask / 3.52 limit, later > 6.40.
6. Same shapes in LIVE -> existing generic LIVE behavior remains the baseline and must not change.

## Minimum acceptance suite

1. PAPER 6s + +8% option move: zero generic cancel, zero replacement before first eligible rung.
2. PAPER 10s: fresh exact OCC quote fetched; one bounded reprice attempt.
3. PAPER 25s and 40s: each rung runs at most once across repeated monitor cycles.
4. PAPER 45s: sane spread + exact terminal old-order proof -> one market fallback maximum.
5. Old order still OPEN after cancel request -> zero replacement.
6. Broker terminal state UNKNOWN -> HOLD, zero replacement.
7. Late fill after cancel request -> fill consumed, zero replacement BUY.
8. Partial fill -> no full-quantity duplicate replacement; exact remainder semantics delegated to canonical owner.
9. Restart after rung 1 -> rung 1 does not replay.
10. Restart after fallback -> fallback does not replay.
11. quote bid/ask zero, crossed, NaN, bool, missing, stale -> no economic action.
12. current last price alone cannot authorize a BUY reprice.
13. top-level execution_mode PAPER + runner PAPER -> PAPER rescue.
14. top-level LIVE + runner PAPER conflict -> HOLD, zero broker mutation.
15. top-level PAPER + runner LIVE conflict -> HOLD, zero broker mutation.
16. LIVE order at same ages/prices follows byte-for-byte existing generic path; no PAPER helper call.
17. exact `client_id`, `execution_mode`, `local_order_id`, OCC contract and broker tag preserved across replacement.
18. no position row before fill.
19. no `proof_trades` write before fill.
20. no queue mutation introduced.
21. zero import/call of `ap.execution.process_signal()` from the rescue path.
22. all #440 cancel-terminal-truth regressions remain green.
23. #475 post-cancel tests remain structurally compatible after rebase.

## Money-path declaration

- **Changes live behavior:** PAPER execution behavior YES; LIVE behavior MUST be NO.
- **Broker submit/cancel:** YES, only through the existing PAPER cancel/replace owner and only after #440 terminal proof.
- **Orders mutation:** YES, existing PAPER order lifecycle/meta/broker id generation only.
- **Positions mutation:** NO new mutation; fills continue through canonical fill ownership.
- **proof_trades mutation:** NO.
- **queue mutation:** NO.
- **client_id / execution_mode:** exact preservation mandatory.
- **PAPER/LIVE taxonomy:** strict PAPER-only behavior; no fallback from unknown to PAPER.
- **Risk of making Jason trade junk:** must be zero because Jason LIVE cannot enter this branch.

## Delivery contract for Claude/Codex

Return all of the following before asking for review:

1. exact rebased base SHA;
2. exact head SHA;
3. complete changed-file list;
4. base-to-head diff stat;
5. fail-first test names/results on unpatched current main;
6. post-fix focused test names/counts;
7. #440 adjacent suite results;
8. #475 adjacent compatibility results;
9. exact proof LIVE path is unchanged;
10. exact broker call-count matrix for open/unknown/canceled/filled/partial/late-fill cases;
11. proof every reprice reads fresh exact-contract quote;
12. proof no legacy `process_signal()` path is reachable;
13. P0 exact-head GitHub CI run id/conclusion;
14. fresh money-path audit;
15. final MERGE / HOLD / HARD HOLD recommendation.

Do not merge, deploy, enable flags, or mutate production data from this PR task.

**Current verdict: HARD HOLD until #440 lands/rebases, implementation exists, actual diff is independently audited, and exact-head CI passes.**
