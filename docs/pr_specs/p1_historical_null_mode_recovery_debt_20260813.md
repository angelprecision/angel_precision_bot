# P1 SPEC: Quarantine historical NULL-mode FILLED recovery debt without weakening current fill safety

## STATUS

**P1 / DRAFT / HOLD / CURRENT-MAIN IMPLEMENTATION WORK ORDER**

Base: `main@3ac102dab320007409107ad36cc9494897d2799e`.

This PR owns the existing historical `orders.execution_mode IS NULL` recovery debt observed on 2026-08-13. It does not own the forward write-path defect now addressed by #450, the MCD nullability bug (#452), Jason WATCHING readiness (#449), or startup phantom result shape (#451).

## INCIDENT SHAPE

Runtime repeatedly emitted `FILL_EXECUTION_MODE_UNPROVEN` while reconciling PAPER broker FILLED ENTRY rows with top-level `orders.execution_mode=NULL`.

Observed examples included expired OCC contracts with May 29 and June 5, 2026 expirations being reconsidered on August 13, 2026.

Position-manager diagnostics simultaneously reported:

- Tradefluence: `null_mode_count=53`; historical capital excluded `$52,479`
- Jose: `null_mode_count=32`; historical capital excluded `$57,327`

The current fail-closed behavior is correct: these rows do not become current positions, do not consume current slot/capital accounting, and do not proceed through OSM/position/exit side effects because execution mode is unproven.

The defect is **liveness/forensic debt**, not permission to infer mode: dead expired legacy rows can keep re-entering fill-monitor recovery and generating CRITICAL noise forever.

## CURRENT-MAIN TRACE

`ap/fill_monitor.py::get_pending_orders(client_id)` includes ENTRY rows in FILLED state when position ownership/recovery conditions indicate reconciliation may still be needed. The query includes top-level `execution_mode`, but there is no simple age/expiry retirement boundary for historical unprovable FILLED debt.

The later fill-admission guard correctly produces:

- `FILL_EXECUTION_MODE_UNPROVEN` when row/runtime mode is absent/invalid;
- no OSM, position, exit-engine, or broker side effects.

`ap.position_manager` intentionally excludes historical NULL-mode orphan FILLED orders from active slot/capital accounting. Existing tests document that `execution_mode IS NOT NULL` is a deliberate safety requirement.

**Do not weaken either protection.**

## REQUIRED OUTCOME

Distinguish:

1. **current/recent money-at-risk fill with unproven mode** -> remains CRITICAL/HOLD and repeatedly visible until resolved by authoritative evidence;
2. **historical expired legacy debt with no possible current economic exposure** -> retained for audit but removed from active recovery churn;
3. **historical row whose current exposure cannot be disproven** -> HOLD/quarantine, never silently retired;
4. **valid modern row with explicit mode** -> existing canonical recovery unchanged.

No historical row may be relabeled LIVE or PAPER merely to silence logs.

## NON-NEGOTIABLE IDENTITY RULE

Never infer historical execution mode from:

- member's current account mode;
- current pod mode;
- current broker URL;
- client email/name;
- current runtime mode;
- today's membership configuration;
- PAPER/LIVE majority for that client.

Historical mode may be backfilled only if immutable per-order/per-event evidence proves it exactly and the migration/repair is separately reviewed. This implementation does not require such backfill.

## RELATIONSHIP TO #450

#450 proposes to persist top-level `orders.execution_mode` on the deprecated compatibility insert path going forward. Treat it as prevention of **new** debt only.

This PR owns the already-existing historical debt and must work correctly even if #450 is not merged.

If #450 lands first, refresh onto current main and preserve its explicit-mode write contract.

## REQUIRED IMPLEMENTATION ORDER

### Phase 0: reproduce current-main churn

Create production-shaped PostgreSQL fixtures containing:

- explicit PAPER client runtime;
- dozens of historical ENTRY/FILLED rows with `execution_mode=NULL`;
- OCC expirations months in the past;
- no current matching positions;
- no active broker ownership evidence;
- position_id absent/unusable in the same shape that causes fill-monitor recovery selection.

Prove current main:

1. `get_pending_orders()` selects the historical rows.
2. fill admission maps broker truth to FILLED.
3. `FILL_EXECUTION_MODE_UNPROVEN` fires.
4. no position/OSM/exit side effects occur.
5. next poll/restart selects the same debt again.
6. position-manager current slot/capital accounting excludes them.

Also create a **recent** NULL-mode FILLED control that must remain active HOLD.

### Phase 1: read-only census of real historical debt

Before deciding retirement criteria, classify actual NULL-mode rows by:

- client id;
- local/broker order id;
- contract/OCC expiration;
- kind/status;
- created/submitted/filled/updated timestamps;
- position_id;
- signal/canonical signal id;
- broker evidence fields;
- recovery owner/retry/handoff metadata;
- matching current/historical position rows;
- matching proof rows only for diagnostics, never as fuzzy identity authority;
- current broker order/position evidence only if the existing safe adapter can query exact identity without mutation.

Report counts under:

- `RECENT_MODE_UNPROVEN_HOLD`
- `HISTORICAL_EXPIRED_NO_CURRENT_EXPOSURE`
- `HISTORICAL_EXPOSURE_AMBIGUOUS_HOLD`
- `MODE_PROVABLE_FROM_IMMUTABLE_ORDER_EVIDENCE`
- `MODERN_EXPLICIT_MODE_NORMAL_RECOVERY`
- `UNKNOWN`

Do not mutate production during census.

### Phase 2: define an explicit historical-debt classifier

Prefer a pure helper with a structured result:

- `ACTIVE_HOLD`
- `HISTORICAL_DIAGNOSTIC_ONLY`
- `NORMAL_RECOVERY`
- `AMBIGUOUS_HOLD`

Inputs should include:

- row execution mode state;
- runtime mode state;
- OCC expiration parsed exactly;
- row age/fill timestamps;
- order status/kind;
- position ownership evidence;
- broker ownership/economic evidence already available;
- recovery owner metadata;
- current date/time in a testable parameter.

#### Expiration rules

An OCC contract whose expiration date is definitively in the past is strong evidence that the option contract itself cannot be a current open tradable exposure, but do not make expiration alone sufficient for retirement if there is conflicting current order/position/reconciliation evidence.

Malformed/unparseable contract expiration -> never assume expired.

#### Age rules

Do not use an arbitrary `NOW()-N days` cutoff as the only condition. Age may support classification, but current-exposure proof and contract lifecycle must win.

### Phase 3: remove historical diagnostic-only rows from active recovery churn

Preferred behavior is **query/recovery eligibility exclusion with explicit diagnostics**, not rewriting economic history.

Possible safe shapes, in order of preference:

1. `get_pending_orders()` or a narrow post-query filter excludes only rows proven `HISTORICAL_DIAGNOSTIC_ONLY` from active reconciliation while emitting bounded aggregate diagnostics;
2. durable non-economic `orders.meta` quarantine marker only if needed for efficiency/idempotency and only through a separately tested guarded write;
3. terminal status mutation only if existing lifecycle taxonomy explicitly supports a non-economic historical retirement state and downstream readers are proven safe.

Do not change FILLED economic truth merely to make the poller quiet.

### Phase 4: observability

Replace per-poll CRITICAL storms for proven historical debt with bounded aggregate diagnostics, for example:

- client;
- historical diagnostic count;
- oldest/newest timestamp;
- expired-contract count;
- ambiguous HOLD count;
- recent active HOLD count;
- stable reason code;
- commit/pod context.

A **recent/current** NULL-mode fill must still log at CRITICAL/HOLD severity because it can represent current money-path corruption.

Do not suppress real current incidents behind an aggregate historical counter.

## EXPECTED FILE BUDGET

Likely:

- `ap/fill_monitor.py`
- one dedicated regression test file
- optional tiny pure helper module if classification would otherwise clutter fill monitor
- `ap/position_manager.py` only for read-only diagnostics/tests; do not weaken its mode exclusion

Avoid:

- `ap/order_state_machine.py`
- `ap_execution_core.py`
- exit engine
- broker submit/cancel adapters
- proof logger/taxonomy
- queue
- selector/scanner/sizing
- current WATCHING/PENDING_TRIGGER recovery stacks.

## REQUIRED REGRESSIONS

At minimum:

1. expired May-29-2026 NULL-mode FILLED row on Aug-13 -> historical diagnostic, no active recovery side effects.
2. expired Jun-05-2026 NULL-mode FILLED row -> same.
3. 53 Tradefluence + 32 Jose historical shape -> bounded diagnostics, no repeated per-row active recovery churn.
4. recent NULL-mode FILLED row -> still `FILL_EXECUTION_MODE_UNPROVEN` HOLD.
5. recent LIVE runtime with NULL row -> HOLD, never inferred LIVE.
6. recent PAPER runtime with NULL row -> HOLD, never inferred PAPER.
7. expired contract but matching active/current position evidence -> ambiguous HOLD, not retired.
8. expired contract with active broker-order evidence -> ambiguous HOLD.
9. malformed OCC symbol -> HOLD, not treated expired.
10. future expiration NULL-mode -> active HOLD.
11. expiration today -> active/current classifier, not historical retirement solely from date.
12. explicit `execution_mode=paper` modern FILLED -> normal existing PAPER recovery unchanged.
13. explicit `execution_mode=live` modern FILLED -> normal existing LIVE recovery unchanged.
14. row mode/runtime mode mismatch -> existing mismatch HOLD unchanged.
15. position-manager NULL-mode orphan -> zero slot usage.
16. position-manager NULL-mode orphan -> zero capital usage.
17. one real explicit LIVE unreconciled FILLED order -> still consumes appropriate current slot/capital under existing rules.
18. historical retirement cannot create/update position.
19. historical retirement cannot call exit engine.
20. historical retirement cannot mutate proof trades.
21. historical retirement cannot submit/cancel broker orders.
22. historical retirement cannot change execution_mode.
23. historical retirement cannot change filled qty/price/status economics unless separately proven existing taxonomy requires it.
24. repeated polls produce stable/no-duplicate diagnostic behavior.
25. restart produces same classification without resurrecting active recovery.
26. #450 forward-write tests remain green if branch is rebased after #450.
27. `tests/test_pr112_snapshot_execution_mode_filter.py` remains green / base failures are explicitly compared.
28. fill-monitor mode-admission tests remain green.
29. position-manager slot/capital tests remain green.
30. exact-head P0 + DB hot-path workflows pass.

## MONEY-PATH INVARIANTS

- no new broker submit/cancel;
- no historical mode inference;
- no new position/proof authority;
- current/recent unproven fills remain fail-closed;
- explicit modern rows follow existing recovery unchanged;
- historical diagnostics do not contaminate PAPER/LIVE proof datasets;
- client id exact;
- execution mode remains unknown when historically unknown;
- economic fill truth is preserved.

## NON-OVERLAP

- #450: forward compatibility write path.
- #448: umbrella tracker and cap provenance.
- #452: MCD runtime nullability.
- #449: LIVE WATCHING readiness.
- #451: startup phantom result shape.
- #428: EXIT/proof truth.

## CODEX FINAL HANDOFF

Update this PR with:

- exact classifier root cause/current query path;
- real historical debt census counts;
- final retirement criteria and why each is safe;
- final file list;
- broker/DB/order/position/proof mutation audit;
- focused tests;
- adjacent fill/position tests;
- exact-head CI links;
- before/after runtime log volume expectation;
- fresh `MERGE / HOLD / HARD HOLD` verdict.

Do not bulk-edit production history, merge, or deploy from this task.

## CURRENT VERDICT

**HOLD until implementation and evidence.** The current fail-closed gate is correct. The repair is to stop ancient, provably non-economic legacy debt from being treated as an active recovery candidate forever, without manufacturing historical PAPER/LIVE truth.