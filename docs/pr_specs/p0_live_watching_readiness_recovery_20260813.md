# P0 SPEC: Restore Jason LIVE WATCHING readiness without replaying stale signals

## STATUS

**P0 / HARD HOLD / CURRENT-MAIN IMPLEMENTATION WORK ORDER**

Base this implementation on `main@3ac102dab320007409107ad36cc9494897d2799e` (merge commit for #445). Do not merge, deploy, alter Render env, or bulk-mutate production rows from this task. Codex must reproduce on current main first, implement surgically, and return exact-head evidence for independent review.

## INCIDENT: 2026-08-13 JASON LIVE

Production facts:

- client `jasoncosby1@gmail.com`
- execution mode `LIVE`
- production Tradier broker
- equity `$1656.59`
- `max_trades=20`, `max_positions=15`
- risk: capital/sector/ticker 10%, calls=15, puts=15, score floor 70, context floor 60
- LIVE preflight OK; control stack PASSED
- broker positions=0, engine positions=0
- morning handoff before/after: `watching_rows=133`, `new_rows=0`, `pending_trigger_rows=0`
- readiness error: `watching_rows_missing_orders_recommend_new_rescue`
- runner: `ENTERING DEGRADED MODE: preopen_readiness_enforcement_failed:startup:status_degraded`
- runtime: `entries_allowed=False`
- repeated `entries_allowed BLOCKED` warnings

This is not a broker rejection, risk-cap block, scanner block, or max-position block. It is a local readiness/recovery ownership contradiction.

## CONFIRMED ROOT CAUSE

### A. `ap/preopen_readiness.py` over-classifies historical WATCHING debt

Current `_query_client_state(client_id)` selects every `trade_queue.status='WATCHING'` row older than the five-minute grace period with no matching ENTRY order. The query has no lower time bound, no trading-session relevance bound, no explicit execution-mode predicate, no source-signal terminal-state classification, and no recovery relevance classifier.

Any old WATCHING row can therefore remain a `watching_orphan` forever. Any nonempty set adds `watching_rows_missing_orders_recommend_new_rescue`, which makes readiness non-OK and the LIVE runner fail closed.

### B. `ap_recovery.py` correctly refuses broad LIVE replay

Current `_reseed_watchers()` emits `LIVE_RECOVERY_REPLAY_SKIPPED ... reason=live_no_replay_policy` and does not blindly reset LIVE WATCHING rows. Preserve this safety intent.

Current deadlock:

`historical WATCHING orphan -> readiness says current ownership failure -> recovery refuses broad LIVE replay -> degraded mode -> entries_allowed=False`

### C. #445 is not the owner

#445 owns `PENDING_TRIGGER` / `STUCK_TRIGGER_READY` pre-broker recovery. This incident has `pending_trigger_rows=0`. Do not weaken or reopen #445 to fix WATCHING readiness.

### D. #380 is stale design evidence only

#380 attempted a LIVE WATCHING classifier on a July lifecycle. Do not cherry-pick, rebase, or merge it. Preserve only its invariant: LIVE WATCHING recovery must be narrow, per-row, identity-proven, and never broad replay.

## REQUIRED OUTCOME

Restore LIVE entry liveness when the only readiness debt is historical WATCHING archaeology, while preserving fail-closed behavior for genuinely relevant current/prior-session ownership ambiguity.

Canonical requirements:

- historical orphan WATCHING rows remain visible diagnostically but cannot freeze today's LIVE runner forever;
- relevant current/prior-session rows with ambiguous ownership still block;
- a recoverable row may re-enter the existing evaluation path at most once;
- recovery code never submits/cancels broker orders, creates positions, or mutates proof trades;
- no broad LIVE `WATCHING -> NEW` update;
- no mode inference from email, current member mode, pod mode, or broker URL;
- readiness and recovery share one classification contract so they cannot disagree again;
- all recovered opportunities still pass current market validity, Master Control, selector, final-submit confirmation, affordability, risk/capital, trigger/breach, and no-chase policy.

## IMPLEMENTATION ORDER

### Phase 0: reproduce first

Create a production-shaped PostgreSQL regression matching Jason:

- LIVE client;
- 133 WATCHING rows spanning multiple sessions;
- no PENDING_TRIGGER rows;
- no active positions/broker positions;
- no matching ENTRY orders for historical rows.

Prove current main:

1. `_query_client_state()` returns the rows as orphans.
2. readiness adds `watching_rows_missing_orders_recommend_new_rescue`.
3. readiness becomes non-OK.
4. startup enters degraded mode.
5. `entries_allowed=False`.
6. `_reseed_watchers()` repairs zero broad LIVE rows.

### Phase 1: census actual WATCHING shape before choosing policy

Build a read-only classifier/report over the actual Jason rows. Capture at minimum:

- queue id, client, signal id, status, created/started timestamps, last_error;
- explicit payload execution_mode and signal/generated date fields;
- ticker, side, timeframe, pattern;
- source `ap_signals` timestamps/decision state/client/mode identity;
- all matching ENTRY orders, statuses, local IDs, broker IDs, submitted/fill evidence;
- matching position evidence;
- terminal/invalidation evidence.

Report counts under explicit classes:

- `HISTORICAL_DIAGNOSTIC_ONLY`
- `CURRENT_SESSION_ACTIONABLE_ORPHAN`
- `PRIOR_SESSION_RECOVERY_CANDIDATE`
- `RECENT_IDENTITY_AMBIGUOUS`
- `ORDER_OWNED_NOT_ORPHAN`
- `POSITION_OWNED_NOT_ORPHAN`
- `SOURCE_TERMINAL_NOT_RECOVERABLE`
- `UNKNOWN_UNCLASSIFIED`

Do not bulk-update production.

### Phase 2: one canonical relevance/ownership classifier

Prefer one pure/shared helper consumed by both `ap/preopen_readiness.py` and `ap_recovery.py`.

Minimum inputs:

- exact client id;
- queue status and created timestamp;
- signal id;
- explicit durable execution-mode evidence;
- source signal ET trading date;
- source lifecycle/decision state;
- order existence/economic evidence;
- position evidence;
- current trading date and canonical prior NYSE session;
- lifecycle stage: startup / pre-overnight / post-reeval / market-open.

Mode rules:

- recover LIVE only from explicit, non-conflicting LIVE evidence;
- never `COALESCE(...,'live')`;
- never use current membership mode as historical truth;
- missing/conflicting recent mode evidence is HOLD;
- ancient unprovable rows remain diagnostic/quarantined, never reactivated, but do not freeze unrelated current-session flow forever.

Session rules:

- use America/New_York and canonical NYSE trading calendar;
- distinguish current session, immediately prior valid session, and older history;
- test Friday->Monday, holidays, and UTC-date vs ET-date boundaries.

Ownership rules:

- any exact ENTRY order means classify under canonical order lifecycle, not queue replay;
- broker/submission/fill evidence forbids speculative replay;
- matching position forbids replay;
- ambiguity is HOLD.

### Phase 3: narrow LIVE recovery

Keep broad LIVE replay forbidden. Only exact classified candidates may receive a guarded lifecycle transition.

For any recoverable candidate:

- no direct broker submit/cancel;
- no direct order creation;
- no position/proof mutation;
- no bypass of MC/overnight reevaluation/selector/final gates;
- CAS/expected-state transition required so duplicate workers cannot restore twice;
- preserve exact logical opportunity identity;
- stamp durable recovery reason/source without destroying prior diagnostics;
- missed/consumed move must follow existing no-chase/terminal policy.

Preferred conceptual continuation:

`proven unowned relevant WATCHING -> guarded NEW/re-evaluation authority -> existing MC/reeval -> watcher/PENDING_TRIGGER -> existing breach -> selector/final guards -> broker`

### Phase 4: fix readiness semantics

Readiness must expose separate counts:

- actionable orphan count;
- recent ambiguous orphan count;
- historical diagnostic count;
- bounded IDs for actionable/ambiguous rows;
- total historical diagnostic count.

Only current/relevant rows may block readiness. Do not solve this with a crude `NOW()-48h` cutoff alone.

### Phase 5: verify startup timing and sticky degraded behavior

Incident startup occurred while readiness itself reported `overnight_reeval_pending_startup`. Trace:

`run_preopen_autonomous_readiness(stage='startup') -> _enforce_post_overnight_readiness(... context='startup') -> degraded reason -> _set_entry_permission()`.

Required behavior:

- before the actual readiness/overnight deadline, expected pending state must not create an unrecoverable sticky degraded condition solely from historical rows;
- at/after the deadline, unresolved relevant LIVE ownership must fail closed before submit;
- later healthy readiness must clear both `preopen_readiness_enforcement_failed` and `preopen_readiness_blocked` families through the existing shared clear path;
- historical-only diagnostics must not keep entries frozen.

## FILE BUDGET

Likely production files:

1. `ap/preopen_readiness.py`
2. `ap_recovery.py`
3. one small shared classifier helper if needed
4. `client_runner.py` only if timing/sticky-degraded reproduction proves a separate current-main bug

Avoid `ap/order_state_machine.py`, `ap_execution_core.py`, `ap/pending_trigger_restart_recovery.py`, and `ap/pending_trigger_classifier.py` unless a focused reproduction proves the existing #445 boundary cannot accept the handoff safely.

No scanner, selector, sizing, intelligence, exit, proof, reconciler, broker-adapter, or queue-fanout rewrites.

## STRICT NON-GOALS

Do not:

- broad-reset LIVE WATCHING rows;
- default missing mode;
- change Jason risk/max positions/max trades/floors;
- alter contract thresholds or entry chase policy;
- add broker authority;
- change exits/proof taxonomy;
- repair PAPER NULL-mode history or the MCD intel None bug here;
- repair the separate startup phantom-clear tuple error here;
- silently change LIVE authorization enforcement;
- delete historical rows merely to make readiness green.

## MONEY-PATH INVARIANTS

- live behavior changes only to restore legitimate entry availability / narrow exact recovery;
- zero new broker submit/cancel authority;
- zero direct position/proof mutation;
- queue mutation only via exact guarded recovery transition;
- `client_id` exact;
- `execution_mode` exact and explicit;
- production metadata shapes only;
- PAPER/LIVE isolation mandatory;
- historical diagnostics preserved;
- every recovered row still passes every current downstream admission and execution gate.

## REQUIRED REGRESSIONS

At minimum cover:

1. 133 historical LIVE WATCHING rows -> readiness not frozen; diagnostic count 133; zero mutation/broker calls.
2. 132 historical + 1 current actionable orphan -> that one remains fail-closed.
3. active ENTRY order -> not orphan.
4. terminal ENTRY order -> explicit policy, never blind restore.
5. broker_order_id -> zero replay.
6. submitted_ts/submit-intent -> zero replay.
7. fill evidence -> zero replay.
8. matching position -> zero replay.
9. terminal source signal -> zero replay.
10. missing recent mode -> HOLD.
11. explicit PAPER evidence -> never LIVE.
12. conflicting mode -> HOLD.
13. prior-session ordinary boundary.
14. Friday->Monday.
15. holiday boundary.
16. UTC/ET date boundary.
17. exact no-owner candidate -> at most one guarded transition.
18. duplicate workers -> one winner, no duplicate opportunity.
19. restart after restore -> no second restore.
20. historical row unchanged across repeated readiness.
21. already-through/consumed setup -> missed/no-chase, zero broker submit from recovery.
22. missing market revalidation data -> HOLD/retry, not approval.
23. startup pre-deadline historical-only debt -> no sticky LIVE freeze.
24. deadline unresolved relevant orphan -> fail closed.
25. later healthy readiness -> clears degraded reason and restores entry eligibility subject to all other gates.
26. Jason runtime limits remain 15 positions / 20 trades / 70 score / 60 context / 10% capital.
27. unrelated broker call-count tests unchanged.
28. no position/proof mutation in classifier/recovery tests.
29. #445 PENDING_TRIGGER/STUCK_TRIGGER_READY focused suite green.
30. exact-head P0 workflow green.

## PRODUCTION-SHAPE DRY RUN REQUIRED BEFORE MERGE CONSIDERATION

After implementation, run read-only classification against actual Jason rows and report the counts by class plus the readiness result that **would** be produced. Do not apply bulk repair from this implementation task.

## FINAL REVIEW CONTRACT

Before any merge decision:

- confirm branch ancestry from then-current main;
- read full cumulative diff and all comments;
- trace `WATCHING -> readiness -> recovery -> queue/MC -> watcher -> order` exactly;
- prove no alternate broker path;
- prove client/mode fences;
- run PostgreSQL incident replay + #445 adjacent suites + exact-head CI;
- separately report authorization-gate env and quarantined historical EXIT proof rows without changing them here;
- issue fresh `MERGE / HOLD / HARD HOLD`.

## CURRENT VERDICT

**HARD HOLD until implementation and exact-head evidence.** The current safety system is preventing bad LIVE behavior, but it is also disabling all Jason entry flow because stale historical WATCHING debt is being treated as current ownership failure. Restore liveness without converting old rows into fresh trading authority.