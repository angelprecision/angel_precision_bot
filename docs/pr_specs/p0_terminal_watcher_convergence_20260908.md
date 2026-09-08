# P0 — Terminal durable row must terminate the exact behavior-active watcher

**Status:** SPEC ONLY / HARD HOLD  
**Do not merge or deploy from this spec branch.**  
**Audited base:** `main@98eeaadae05f9e4e1db624703ec1e3cd758b732c`  
**Incident date:** 2026-09-08  
**Primary production example:** Jason LIVE TMO

## 1. Executive summary

On September 8, 2026, Jason LIVE proved an ownership-convergence defect between durable pending-trigger recovery and the in-memory entry watcher.

Restart recovery correctly terminalized the exact TMO entry order as `CANCELED` with durable reason `restart_stuck_trigger_ready_no_broker_proof`. The recovery terminalizer reread the canonical order and accepted the terminal state/reason as durable.

The in-memory watcher then reread the same terminal order through `APEntryWatcher._resolve_trigger_callback_disposition()` but failed to recognize the terminal reason because its terminal-reason verifier checks a narrower/different set of fields than restart recovery writes and verifies.

The watcher therefore returned/behaved as `KEEP_WATCHER` even though the canonical order was already terminal. It continued polling TMO and writing fresh `trigger_ready` watcher audit events long after the durable order was canceled.

This is a P0 ownership split:

```text
canonical durable order authority = TERMINAL
in-memory watcher authority       = STILL ACTIVE
```

A terminal canonical entry row must never retain a behavior-active entry watcher for the same exact identity.

## 2. Production incident: TMO

Jason LIVE TMO local order:

`b9e29854-1c97-43d6-986b-eef0506bdf5a`

Observed canonical durable terminal state:

- `status=CANCELED`
- `last_error=restart_stuck_trigger_ready_no_broker_proof`
- `meta.restart_recovery_cls=STUCK_TRIGGER_READY`
- `meta.restart_recovery_terminal_reason=restart_stuck_trigger_ready_no_broker_proof` written by recovery terminalization path
- no broker order
- no submitted timestamp
- no position

Yet the same in-memory watcher remained live and continued recording:

- `watcher_audit.reason_code=trigger_ready`
- fresh `watcher_audit.evaluated_at` values after cancellation
- increasing confirmed-breach poll counts

Later durable snapshots still showed `status=CANCELED` while watcher audit continued to advance.

This is not a Tradier rejection, option-spread rejection, or selector failure. The durable order was already terminal. The bug is failure to converge in-memory watcher ownership to that terminal truth.

## 3. Exact code-contract mismatch

### 3.1 Restart recovery terminalizer

`ap/pending_trigger_restart_recovery.py::_terminalize_with_reason()`:

1. writes `meta.restart_recovery_terminal_reason`;
2. calls the canonical pending-entry cancel helper;
3. rereads the order;
4. verifies exact local order/client/mode identity;
5. verifies terminal status;
6. accepts the requested terminal reason from a durable reason family that includes at least:
   - `orders.last_error`;
   - `meta.restart_recovery_terminal_reason`;
   - `meta.terminal_reason`;
   - `meta.reason_code`;
   - `meta.final_reason`;
   - `meta.watcher_invalidation_reason`;
   - watcher-audit reason where applicable.

The recovery path therefore considers TMO durably terminal.

### 3.2 Entry watcher terminal verifier

`ap_entry_watcher.py::APEntryWatcher._resolve_trigger_callback_disposition()` rereads the order and classifies terminal status through an internal `_terminal_family()` / `_terminal_reason_present()` check.

On the audited main SHA, `_terminal_reason_present()` recognizes a narrower family centered on:

- `meta.reason_code`;
- `meta.final_reason`;
- `meta.materialization_reason`.

It does not consistently recognize the exact durable terminal authorities written by restart recovery, especially:

- top-level `orders.last_error`;
- `meta.restart_recovery_terminal_reason`.

Therefore:

```text
status=CANCELED
+ last_error=restart_stuck_trigger_ready_no_broker_proof
+ restart_recovery_terminal_reason=...
```

can fail the watcher verifier's terminal-reason test.

The watcher keeps ownership even though canonical recovery has already terminalized the order.

## 4. Scope ownership

### This PR owns

- convergence from exact canonical terminal entry truth to exact watcher removal;
- one shared/consistent durable terminal-reason vocabulary at the callback verification seam;
- preventing `KEEP_WATCHER` when the same exact canonical row is proven terminal;
- exact removal of `_pending` ownership and exact dedup/direction ownership associated with that watcher;
- idempotent handling when terminalization races the watcher callback;
- ensuring no post-terminal `_on_entry_trigger()` callback occurs for that exact watcher/order identity.

### This PR does NOT own

- PR #580 lifecycle restoration from `NONE -> ADOPTED -> WATCHING`;
- PR #596 due deferred materialization retry liveness;
- PR #569 pre-open readiness/fresh market truth;
- retry taxonomy changes;
- selector quality changes;
- broker submit behavior;
- broker cancel authority changes;
- position/proof creation;
- exit logic;
- scanner behavior;
- sector identity;
- overnight readiness attempt durability.

## 5. Expected production files

Primary expected production file:

- `ap_entry_watcher.py`

Tests should exercise the actual durable callback-verification behavior.

A shared terminal-reason helper may be introduced only if it genuinely reduces drift between existing canonical authorities and does not broaden behavioral scope. If a shared helper requires touching `ap/pending_trigger_restart_recovery.py`, its behavior must remain unchanged and regression-proven.

Do not redesign pending-trigger recovery in this PR.

## 6. Binding invariant

For the exact same local-order identity:

```text
canonical order status in terminal family
+ exact client identity
+ exact execution mode
+ exact local order id
+ durable terminal reason from canonical terminal authority
-> callback disposition TERMINAL_DURABLE
-> exact watcher removed
-> exact dedup ownership released
-> exact direction ownership released/cleared only when owned by that watcher
-> no future trigger callback for that watcher
```

The allowed terminal status family must remain whatever the current canonical watcher contract already supports, currently including:

- `REJECTED`
- `EXPIRED`
- `CANCELED`
- `ERROR`

Do not add unrelated status semantics.

## 7. Durable terminal reason authority

The watcher verifier must recognize terminal reasons from the same canonical durable sources the production system actually writes.

At minimum evaluate the applicable reason family consistently across:

### Top-level order authority

- `orders.last_error`

### Meta authority

- `meta.restart_recovery_terminal_reason`
- `meta.terminal_reason`
- `meta.reason_code`
- `meta.final_reason`
- `meta.materialization_reason`
- `meta.watcher_invalidation_reason`

### Watcher audit

A watcher-audit terminal invalidation reason may support terminal proof only if it is already an accepted canonical terminal authority.

`watcher_audit.reason_code=trigger_ready` by itself is NOT terminal proof.

Do not treat arbitrary non-empty watcher audit text as permission to remove a watcher.

## 8. Conflict handling

### 8.1 Terminal status with no durable reason

If status is terminal but no recognized durable terminal reason exists:

- do not infer a reason;
- do not manufacture terminal proof;
- return/retain fail-closed ownership (`KEEP_WATCHER` or current equivalent) until an authority resolves it;
- zero broker action.

### 8.2 Contradictory terminal reasons

If multiple explicit durable reason authorities conflict in a way that implies different lifecycle ownership or broker safety:

- HOLD / retain ownership fail-closed;
- emit structured diagnostic identifying the conflicting fields;
- do not silently choose first/non-empty/last writer.

If multiple fields contain the same exact reason, accept them as duplicate consistent authority.

### 8.3 Broker handoff contradiction

If the row is terminal but also contains unresolved broker-submit/handoff markers that make local terminalization ambiguous:

- ordinary watcher cleanup must not invent broker truth;
- route to existing reconciliation authority or HOLD;
- zero new broker POST/cancel.

This PR must not become a broker-intent reconciler.

## 9. Exact identity before watcher removal

Before removing a watcher due to a terminal durable row, prove the callback is reading the same order identity the watcher owns.

At minimum:

- non-empty local order id;
- reread local order id equals watcher local order id;
- exact client id matches runtime/watcher client;
- exact execution mode matches runtime/watcher mode;
- signal identity/canonical signal identity checks remain at least as strict as current behavior where those values are available and authoritative;
- wrong or missing required identity must never evict a different watcher.

No fuzzy ticker-only or same-contract-only cleanup is allowed.

## 10. Watcher registry cleanup contract

When `TERMINAL_DURABLE` is proven:

1. the exact watcher leaves `_pending`;
2. its exact dedup key is released;
3. direction ownership is released only if this watcher actually holds that ownership;
4. no broad ticker-level eviction removes unrelated watchers;
5. no new callback occurs after cleanup;
6. repeated cleanup/replay is idempotent.

If registry cleanup itself cannot be proven successful, emit a P0 diagnostic. Do not lie that terminal convergence completed.

## 11. Race model

The critical race to reproduce is:

```text
watcher is behavior-active
-> trigger is confirmed
-> watcher enters callback
-> concurrent restart-recovery/health path terminalizes canonical row
-> callback disposition verifier rereads row
-> canonical row is now CANCELED/EXPIRED/ERROR/REJECTED with durable reason
```

Required result:

`TERMINAL_DURABLE`

not `KEEP_WATCHER`.

The terminal durable row is later authority than the stale in-memory callback context.

This must be proven without allowing a second broker action.

## 12. Required fail-first TMO replay

Create a production-shaped test using the TMO identity:

- local order id `b9e29854-1c97-43d6-986b-eef0506bdf5a`;
- LIVE mode;
- deferred entry watcher behavior-active;
- callback starts while row is still pending;
- terminalizer changes exact row to `CANCELED`;
- durable reason exists only in the production authorities that exposed the bug:
  - `orders.last_error=restart_stuck_trigger_ready_no_broker_proof` and/or
  - `meta.restart_recovery_terminal_reason=restart_stuck_trigger_ready_no_broker_proof`;
- no broker order id;
- no submitted timestamp.

On current main, prove the watcher verifier fails to treat this shape as terminal or otherwise remains active.

After fix, prove:

```text
callback verifier -> TERMINAL_DURABLE
-> exact watcher removed
-> zero subsequent callback attempts
```

## 13. Required behavioral/adversarial tests

### Terminal reason vocabulary

1. `CANCELED + orders.last_error` -> `TERMINAL_DURABLE`.
2. `CANCELED + meta.restart_recovery_terminal_reason` -> `TERMINAL_DURABLE`.
3. `EXPIRED + meta.terminal_reason` -> terminal.
4. `REJECTED + meta.reason_code` -> terminal.
5. `ERROR + meta.final_reason` -> terminal.
6. existing `meta.materialization_reason` terminal behavior remains unchanged.
7. consistent duplicate reason across multiple fields remains terminal.

### Negative authority

8. terminal status + no recognized reason -> retain/HOLD, no eviction.
9. terminal status + only `watcher_audit.reason_code=trigger_ready` -> retain/HOLD.
10. non-terminal PENDING_TRIGGER + terminal-looking stale text -> not terminal.
11. wrong local order id -> no eviction.
12. wrong client -> no eviction.
13. wrong execution mode -> no eviction.
14. missing required identity -> no eviction.
15. conflicting terminal reason authorities -> HOLD/diagnostic.

### Race behavior

16. callback begins before terminalization, reread occurs after terminalization -> terminal wins.
17. terminalization before callback begins -> callback never performs normal entry work after durable terminal reread.
18. terminalization races watcher poll -> at most one callback attempt and no post-terminal repeat.
19. two cleanup observers -> idempotent exact watcher removal.
20. process restart after terminalization -> terminal row does not rehydrate behavior-active watcher.

### Registry/dedup

21. exact watcher removed from `_pending`.
22. exact dedup key released.
23. unrelated same-ticker watcher remains.
24. opposite-side watcher remains unless existing direction-conflict authority independently removes it.
25. direction claim is released only if owned by exact terminal watcher.
26. repeated terminal replay does not corrupt dedup state.

### Money path

27. zero broker submit calls from terminal convergence.
28. zero broker cancel calls added by this PR.
29. zero position mutation.
30. zero proof_trades mutation.
31. zero synthetic queue terminalization outside existing canonical behavior.

### Submitted/broker-intent negative controls

32. `SUBMITTED` family remains governed by submitted verification, not terminal cleanup.
33. broker intent ambiguity remains reconciliation/HOLD.
34. terminal-looking local state cannot erase proven broker ownership.

## 14. Production observability required

Add or preserve a structured terminal-convergence event containing enough identity to audit the cleanup without leaking secrets, for example:

```text
WATCHER_TERMINAL_DURABLE_CONVERGED
local_order_id=...
client_id=...
execution_mode=...
status=CANCELED
reason=restart_stuck_trigger_ready_no_broker_proof
watcher_removed=true
broker_submit=NOT_ATTEMPTED
broker_cancel=NOT_ATTEMPTED
```

If cleanup cannot be proven:

```text
WATCHER_TERMINAL_CONVERGENCE_UNPROVEN
```

with the exact reason.

Do not log success before exact watcher removal is known.

## 15. No-regression constraints

Do not:

- modify `LEGAL_TRANSITIONS` to make illegal recovery behavior legal;
- change #580's lifecycle-restoration ownership;
- change retry policy;
- loosen selector quality;
- rearm a terminal row;
- create a new broker submit path;
- create a new broker cancel path;
- cancel broker orders as part of watcher cleanup;
- create positions/proof rows;
- remove watchers by ticker alone;
- release dedup before terminal identity is proven;
- interpret UNKNOWN broker truth as absent.

## 16. Implementation evidence required

The implementation PR must show the exact changed control path:

```text
watcher poll
-> trigger callback
-> callback result / durable reread
-> submitted verification
-> terminal verification
-> terminal reason authority resolution
-> TERMINAL_DURABLE
-> pending watcher cleanup
-> dedup/direction cleanup
```

For every changed function, document:

- caller;
- durable state read;
- identity checks;
- terminal reason sources;
- mutation/cleanup;
- return classification;
- downstream consumer.

## 17. CI / release gate

The implementation remains HARD HOLD until:

1. Rebased onto current implementation `main`.
2. Diff is surgical and limited to terminal watcher convergence.
3. Exact TMO fail-first replay fails on base and passes on head.
4. Terminal reason alias matrix passes behaviorally.
5. Terminal-vs-callback race test passes.
6. Exact watcher/dedup cleanup is proven.
7. Submitted/broker-intent negative controls remain green.
8. Zero new broker submit/cancel authority is demonstrated.
9. Exact-head P0 is green at the attested HEAD SHA.
10. `pull_request` merge-ref runs the same valid test list and is green.
11. Independent backwards audit confirms no silent trade-flow regression.
12. PR stays draft until explicitly cleared.

## 18. Merge verdict

**HARD HOLD.**

This spec authorizes implementation and testing only. It does not authorize merge or deployment.
