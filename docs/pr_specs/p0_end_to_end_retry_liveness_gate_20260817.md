# P0 — End-to-end retry liveness release gate for ENTRY, position ownership, and EXIT

**Status:** SPEC ONLY / HARD HOLD / TEST-AND-PREFLIGHT OWNER ONLY

**Base:** `main@d0d37e79ae698e604eb8080065d2314b161de351`

## Why this PR exists

The current repository has multiple individually sophisticated recovery/retry implementations and multiple open P0 PRs, yet production behavior still shows the system failing to retry at the places that matter:

- PAPER entry orders can reach the broker and die before the PAPER rescue owns them;
- post-cancel ENTRY retry arms and then fails in the legacy `ap.execution.process_signal()` path;
- filled/partial ENTRY ownership has separate restart work;
- position recovery has separate mode/broker-truth work;
- unfilled EXIT retry can be structurally suppressed by deployed defaults;
- one subsystem's green unit tests do not prove the entire lifecycle converges.

This PR does **not** create a fourth retry engine. Its only job is to make end-to-end liveness a release invariant that all owner PRs must satisfy together.

## Hard non-overlap

Behavior owners remain:

- #440 — ENTRY cancel/replace broker-terminal truth;
- #482 — PAPER sandbox rescue gets authority before generic MISSED_MOVE;
- #475 — post-cancel ENTRY retry returns to canonical ENTRY/OSM authority;
- #473 — FILLED ENTRY restart-safe handoff;
- #480 — ENTRY partial fill immediate position ownership;
- #476 — startup active-position recovery identity/mode/expired OCC;
- #481 — exact Tradier position/order truth and autonomous exit recovery/fill routing;
- #469 — current-main unfilled EXIT retry/reprice liveness and exit price improvement;
- #423 — older EXIT retry invariant/test source, not a diff to merge wholesale;
- #424 — reservation-aware exit quantity / EXIT ALL;
- #428 — exact broker EXIT fill/proof truth.

This PR may consume those APIs/tests after they land. It may not duplicate their production logic.

## Core liveness invariant

For every money-bearing lifecycle state, Angel Precision must converge to a finite explicit disposition under supported production defaults.

Forbidden generic shape:

```text
money/opportunity is still economically live
AND no current canonical owner can progress it
AND no explicit HOLD/quarantine says why
AND no bounded retry/recovery remains scheduled
```

A retry system is not healthy because a function named `retry` exists. It is healthy only when the exact production state can move to the next legitimate state without an operator flipping undocumented environment variables.

## Required convergence tables

### A. Pre-broker ENTRY / watcher / deferred selector

A valid signal/watcher must converge to one of:

1. still legitimately WATCHING with durable owner;
2. RETRYABLE_DATA with bounded due retry and durable generation;
3. selected/materialized exact OCC contract and broker-ready state;
4. explicit terminal structural rejection with exact reason;
5. explicit HOLD because identity/market truth is unproven.

Forbidden:

```text
trigger confirmed
AND retryable data failure occurred
AND order silently terminalizes or loses owner
```

Current merged #439/#401/#404 invariants should be reused.

### B. Broker-open ENTRY

A broker-reaching BUY must converge to:

1. FILLED -> fill/position ownership;
2. PARTIAL -> immediate ownership of filled qty + safe exact remainder handling;
3. still-open owner with bounded PAPER rescue / existing LIVE policy;
4. broker-confirmed terminal no-fill -> at most one canonical post-cancel retry if policy allows;
5. explicit HOLD if broker outcome is unknown.

Forbidden:

- generic PAPER missed-move cancel before #69 rescue authority (#482);
- replacement while old broker order can still fill (#440);
- `ap.execution.process_signal()` as retry broker submit authority (#475);
- duplicate BUY after late/partial fill.

### C. FILLED/PARTIAL ENTRY -> position ownership

Any positive broker-confirmed ENTRY fill must converge to a durable position owner before the original ENTRY generation can be forgotten.

Required states:

1. full fill -> one position with exact fill qty/cost/identity;
2. partial fill -> position exists for filled qty immediately; remainder ownership remains explicit;
3. crash between broker fill and DB handoff -> restart recovery proves broker/ENTRY identity then reconstructs exactly once;
4. ambiguous broker/contract/client/mode truth -> HOLD, never fabricate position or retry BUY.

Forbidden:

```text
broker owns option contracts
AND AP has no durable position owner/protective path
```

Consume #473/#480/#481 rather than inventing a new owner.

### D. Open position monitoring/recovery

A durable/broker-proven open position must converge after restart/outage to:

1. canonical exit-engine/quote-monitor ownership;
2. explicit HOLD because broker position truth is unknown;
3. broker-confirmed flat -> canonical close/reconciliation;
4. expired exact OCC -> current approved expiration handling.

Unknown/malformed broker quantity or missing exact contract identity can never become flat. Consume #476/#481.

### E. Broker-open EXIT

A submitted SELL_TO_CLOSE must converge to:

1. FILLED -> exact position/proof close truth;
2. PARTIAL -> consume cumulative fill and own exact remaining qty;
3. still-open exact owner -> monitor/reprice policy progresses;
4. broker-confirmed terminal cancel/reject/expire -> one canonical replacement/re-evaluation;
5. broker truth unknown -> HOLD;
6. emergency escalation when existing policy explicitly requires it.

Forbidden:

```text
broker position still open
AND old EXIT no longer progressing
AND no live broker EXIT owner
AND position stuck CLOSING/in_flight
```

Supported defaults may not suppress this convergence. In particular, liveness cannot depend on an operator remembering to set:

```text
ORDER_MONITOR_MODE=active
ALLOW_ORDER_MONITOR_POSITION_REOPEN=1
```

when repository defaults are `watchdog` and `0`. #469 owns the behavior repair.

## This PR's allowed implementation

After owner PRs are implemented/rebased, this PR may add only:

1. cross-lifecycle production-shaped tests;
2. a read-only `ap/retry_liveness_preflight.py` or equivalent if useful;
3. CI workflow registration;
4. documentation/runbook.

The preflight, if created, must:

- perform zero broker submit/cancel;
- perform zero DB writes;
- not mutate orders/positions/proof/queue;
- inspect supported config/defaults/schema/identity requirements;
- return `PASS` or `HOLD` with exact failed invariant;
- never "repair" production state.

No production retry, submit, cancel, position, or exit decision logic belongs in this PR.

## Required dependency order before implementation

Do not build a fake green integration suite against known-broken owners. Minimum sequence:

1. rebase/review #440;
2. implement/review #482;
3. implement/review #475;
4. stabilize #473/#480 position ownership;
5. stabilize #481/#476 broker/position recovery truth;
6. implement/review #469 against current main;
7. then implement this integration gate on the resulting exact main.

If ownership PR numbering changes/supersedes, update the dependency table first.

## Required end-to-end scenarios

Use real production-shaped rows/metadata and real canonical classes with brokers stubbed at transport boundary. Do not mock away the lifecycle under test.

### ENTRY scenarios

1. PAPER NOW-style marketable order, sandbox no fill, option runs +40%: #482 rescue gets authority, no premature generic cancel.
2. PAPER rescue reprice, old broker order still open: zero replacement.
3. PAPER late fill after cancel request: fill wins, no second BUY.
4. broker-confirmed terminal no-fill + policy-valid retry: #475 creates one canonical retry generation.
5. same retry at 09:31 single stock: no legacy `ap.execution` time-gate failure because legacy submitter is unreachable.
6. duplicate monitor ticks on ARMED retry: one claim/handoff.
7. crash after claim before handoff: restart still at most one retry generation.
8. MISSED_MOVE terminal post-cancel case: no chase retry.
9. original partial fill: positive qty becomes position-owned; no full duplicate retry.
10. broker order truth UNKNOWN: HOLD, zero duplicate BUY.

### Position ownership/restart scenarios

11. full ENTRY fill then crash before handoff -> restart creates/adopts one exact position.
12. partial ENTRY fill 1/4 -> position owns 1 immediately; remaining 3 explicit.
13. duplicate recovery pass -> no second position/no duplicate quantity.
14. missing exact OCC identity -> HOLD.
15. broker positions malformed -> HOLD.
16. unrelated broker short/fractional equity does not poison exact target truth after #481.
17. execution-mode conflict -> HOLD, no cross-mode adoption.
18. exact successful broker absence with proven OCC -> authoritative flat only where canonical rules allow.

### EXIT scenarios

19. non-emergency EXIT unfilled past stale threshold under default configuration -> system progresses, not permanent watchdog inertness.
20. old EXIT broker status UNKNOWN -> HOLD, no replacement SELL.
21. old EXIT confirmed canceled -> exit owner is re-armed/re-evaluated and at most one replacement occurs.
22. partial EXIT fill -> remaining qty only.
23. late full fill during cancel -> no replacement.
24. restart with old EXIT still open -> adopt existing broker order.
25. restart after confirmed cancel before replacement -> one replacement max.
26. position cannot remain permanently `CLOSING` solely because `ALLOW_ORDER_MONITOR_POSITION_REOPEN=0` default exists.
27. emergency hard-stop/EOD path remains capable of escalation under its existing rules.
28. actual broker fill, not submitted limit, closes P&L/proof.
29. SCALE_OUT fill cannot full-close residual position (#481 regression retained).
30. missing contract identity cannot wildcard-match unrelated broker exit orders (#481 regression retained).

### Cross-mode/account scenarios

31. Jose PAPER lifecycle cannot create/adopt Jason LIVE order/position.
32. Tradefluence PAPER retry stays PAPER through every generation.
33. Jason LIVE retry stays LIVE and never touches sandbox.
34. same OCC held by two clients remains isolated.
35. unknown mode never defaults to LIVE or PAPER authority.

### Mutation/broker count scenarios

36. pre-fill ENTRY retry path: zero position/proof mutation.
37. HOLD states: zero broker submit/cancel unless the canonical owner has exact authority for the action being tested.
38. retry claim loser: zero broker call.
39. exit replacement only after exact old-order terminal proof.
40. each logical generation has at most one broker submit authority.

## Static architecture assertions

Add source/AST tests after owner PRs land:

- post-cancel ENTRY retry path contains zero call/import of `ap.execution.process_signal`;
- no new direct broker submitter exists in recovery/preflight modules;
- PAPER rescue and generic entry repeg cannot both own the same branch/generation;
- EXIT retry has one submit/cancel owner;
- supported default configuration cannot statically short-circuit all EXIT liveness;
- recovery/preflight modules do not write `proof_trades` or `trade_queue` unless an existing canonical owner explicitly requires it and the owner PR documents the mutation.

## Optional read-only production preflight

If implemented, report at minimum:

```text
entry_post_cancel_retry_authority: PASS/HOLD
paper_fill_rescue_ordering: PASS/HOLD
entry_cancel_terminal_truth: PASS/HOLD
filled_entry_handoff: PASS/HOLD
partial_entry_ownership: PASS/HOLD
position_broker_truth: PASS/HOLD
exit_retry_supported_defaults: PASS/HOLD
exit_broker_order_truth: PASS/HOLD
client_mode_identity: PASS/HOLD
overall: PASS/HOLD
```

For each HOLD include exact code/config/schema reason. No remediation writes.

## Release rule

Angel Precision cannot call retry/recovery production-healthy unless this exact-head integration gate passes after the owner PRs are merged/rebased.

A green focused owner suite is insufficient if the integration gate demonstrates a stranded state.

## Money-path declaration

- Live behavior change from this PR: **NO** if kept test/read-only as required.
- Broker submit/cancel: zero from this PR.
- Orders/positions/proof/queue writes: zero from this PR.
- client_id/execution_mode: only asserted/read.
- PAPER/LIVE taxonomy: cross-mode contamination tests mandatory.
- Could this make Jason trade junk: no, because it cannot approve or submit a trade.

## Claude/Codex delivery contract

Before review return:

1. exact merged/current-main SHA containing all dependency owners;
2. exact owner PR/head mapping used by each scenario;
3. changed files;
4. proof zero production money-path logic changed;
5. 40-scenario results with exact test names;
6. static architecture assertion results;
7. optional preflight sample output;
8. exact-head P0 + DB hot-path CI run ids;
9. all remaining HOLD reasons;
10. final MERGE/HOLD/HARD HOLD.

Do not merge/deploy any dependency from this task. Do not change environment variables merely to make the gate pass.

**Current verdict: HARD HOLD until owner repairs land; this PR then becomes the release proof that retries actually work end to end.**
