# P0 SPEC: Recover interrupted FILLED ENTRY handoff without replaying broker side effects

## STATUS

**IMPLEMENTED / DRAFT / REVIEW REQUIRED / DO NOT MERGE OR DEPLOY.**

Amended after independent audit to close the Window-A pair-cancel durability
gap, narrow the EXIT-scope validation change back to ENTRY-only, add a
behavioral Window-E owner-rehydration test, expand the production file
budget for exact symbol-lock ownership proof, and close the restart
pair-truth hole. See the "P0 AMENDMENT" and "WINDOW E" sections, the
"FINAL AMENDMENT: symbol-lock ownership" section, and the "FINAL AMENDMENT:
restart pair-truth" section below for the complete, current contract. No
merge, deploy, or production-data mutation is authorized by this amendment.

Base at spec creation:

```text
main@b43ce9c53433bd0479baa87e9757b50740adaa01
```

This PR is a binding implementation contract for a **separate companion to #470**.

#470 owns the normal broker-confirmed ENTRY fill hot path and must provide the exact durable identity bind + truthful canonical exit-owner proof primitives before this PR is implemented.

If #470 is not merged into the implementation base, or if the exact-head current implementation does not expose equivalent verified primitives, **STOP. Do not recreate, fork, or partially duplicate #470 inside this PR.** Rebase after #470 or report the blocker.

This PR is also separate from #472. #472 is `PENDING_TRIGGER/SUBMITTING` broker-intent recovery before broker ownership is proven. This PR starts only after a broker-confirmed ENTRY is durably terminal `FILLED`.

---

# ONE JOB

Make the downstream handoff of a broker-confirmed ENTRY fill restart-safe after the order has become durably `FILLED`, while preventing a recovered/historical FILLED row from replaying fresh-fill broker mutations.

Required end state:

```text
broker ENTRY FILLED
-> durable local FILLED truth
-> crash at any point
-> restart
-> prove current broker position risk
-> recover/find canonical DB position
-> prove exact ENTRY <-> position durable identity using #470
-> seed/adopt exactly one canonical exit-engine owner using #470
-> release entry guards
-> mark handoff COMPLETE
```

The recovery path must have **zero authority** to submit a new ENTRY, ordinary EXIT, standing stop, pair cancel, or broker cancel.

---

# WHY THIS PR EXISTS

Current `ap/fill_monitor.py` polls only nonterminal broker-backed rows such as:

```text
SUBMITTED
ACKNOWLEDGED
PARTIAL_FILL
EXIT_SUBMITTED
EXIT_ACKNOWLEDGED
EXIT_PARTIAL_FILL
```

A normal ENTRY fill can therefore do this:

```text
broker says FILLED
-> OSM persists orders.status = FILLED
-> process dies
```

On restart the row is terminal and no longer selected by the ordinary pending-order query. The downstream position / identity / exit-owner handoff can remain incomplete indefinitely.

This is not theoretical. Read-only production evidence observed before this spec included at least:

```text
Jason LIVE PEP
local_order_id = 1b8ec150-38cc-424c-93e5-b1a1820ea50b
broker_order_id = 141999576
contract = PEP260821C00141000
status = FILLED
filled_qty = 1
fill_price = 1.58
position_id = NULL

Jose PAPER PEP
local_order_id = 82ece028-fd81-4edf-9581-182c00d8f41a
broker_order_id = 37024154
contract = PEP260821C00141000
status = FILLED
filled_qty = 11
fill_price = 1.47
position_id = NULL
```

Production also contains older FILLED rows with `FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN` and many historical FILLED rows. Therefore the implementation must recover **current open risk only**, not blindly replay every historical fill.

---

# CRASH WINDOWS THAT MUST ALL BE CLOSED

The implementation is incomplete unless it proves all of these.

## Window A: after terminal local fill, before position

```text
broker FILLED
-> durable local FILLED
-> CRASH
-> no canonical DB position yet
```

Restart must recover only if current broker position truth proves the exact OCC risk still exists.

## P0 AMENDMENT: Window A pair-cancel outcome durability

Independent audit found Window A as originally implemented did not account
for the opposite 1-1 pair order. Fresh-fill ordering is:

```text
IN_PROGRESS -> OSM FILLED -> _cancel_pair_opposite() -> position -> ...
```

A crash between `OSM FILLED` and `_cancel_pair_opposite()` resolving left
recovery with zero cancel authority (correct) but also zero knowledge of
whether the opposite pair order was ever addressed — meaning recovery
could reach `COMPLETE` while a live opposite-side order still existed,
producing unintended double exposure.

Closed via a durable `filled_entry_pair_resolution_state` field in the
same `orders.meta` handoff namespace:

```text
NOT_APPLICABLE   — no opposite pair existed for this fill
CONFIRMED        — an opposite pair existed; broker cancel AND local OSM
                    CANCELED transition were both durably confirmed
OUTCOME_UNPROVEN — pair existed or applicability cannot be safely
                    reconstructed; recovery gets zero cancel authority
                    and must HOLD, never COMPLETE
```

`_cancel_pair_opposite()` now returns `(state, detail)` instead of `None`;
every existing exit branch (no pair, broker cancel failure, missing
broker id, unresolved side, local CAS miss after broker cancel succeeded,
`ImportError`, generic exception) maps to `NOT_APPLICABLE` or
`OUTCOME_UNPROVEN` — only a fully-confirmed broker+local cancel yields
`CONFIRMED`. The fresh-fill path persists this immediately after calling
`_cancel_pair_opposite()`. The `COMPLETE` write itself is additionally
gated at the SQL layer:
`COALESCE(meta->>'filled_entry_pair_resolution_state','') IN
('NOT_APPLICABLE','CONFIRMED')` — a crash before this is durably proven
cannot reach `COMPLETE` regardless of any application-level bug, since the
CAS predicate itself refuses the write. `recover_interrupted_filled_entry_handoff`
first proves the current broker risk, then finds or recreates the exact
canonical local position, binds the exact identity, and seeds one exit owner.
Only after that ownership proof does it check this state for both
`ACTIVE_EXISTING` and `ACTIVE_RECREATE`. An unproven pair state therefore
holds with the exposure managed, without guard release or `COMPLETE`;
`NOT_APPLICABLE` and `CONFIRMED` permit the guard-release/`COMPLETE` phase.
Recovery never calls `_cancel_pair_opposite` itself (verified by the existing
AST-based static guard test). Any value other than an exact-match `NOT_APPLICABLE` or
`CONFIRMED` string — missing key, wrong case, wrong type, trailing
whitespace — is treated as unproven and fails closed; no normalization is
applied on read.

Production change: `ap/fill_monitor.py` plus the new read-only
`ap/filled_entry_recovery_authority.py`. No manual-close/state API change, no
new broker cancel authority, and no second pair-cancel implementation.

## Window B: after position exists, before durable identity bind

```text
broker FILLED
-> durable local FILLED
-> canonical position created/found
-> CRASH
-> orders.position_id and/or position execution IDs incomplete
```

Restart must reuse #470's exact identity bind. No second position may be created if the existing canonical position can be found idempotently.

## Window C: after fresh-fill standing-stop broker call, before bind

```text
broker FILLED
-> local position
-> existing fresh-fill standing-stop code may call broker
-> CRASH
```

Restart must **NOT** place another standing stop. Historical/failure recovery is not fresh-fill authority.

If current code cannot prove whether the previous process's standing-stop broker call succeeded, that ambiguity must not be resolved by submitting another stop from this PR.

## Window D: after identity bind succeeds, before canonical exit-owner proof completes

```text
broker FILLED
-> position exists
-> exact ENTRY <-> position bind succeeds
-> CRASH
-> exit owner not proven
```

A `position_id IS NULL` predicate alone cannot detect this window because the durable identity may already be correct. The implementation therefore needs an explicit durable handoff state.

## Window E: after handoff completed in one process, then the whole process restarts

In-memory exit-engine ownership disappears with the process unless another startup path deterministically rehydrates it.

Before coding, trace current exact-head startup behavior and prove one of these:

1. current startup already rehydrates every exact active canonical DB position into exactly one behavior-active exit owner; or
2. this PR must perform a once-per-process restart owner verification/rehydration using the existing #470 seed/adoption primitive.

Do **not** assume completed durable identity implies in-memory owner survival.

If existing startup rehydration is proven, add regression evidence and do not duplicate it. If it is absent or incomplete, implement the smallest restart sweep in `ap/fill_monitor.py`; do not edit the exit engine merely to create another hydration system.

## P0 AMENDMENT: Window E behavioral proof

Independent audit found the only existing Window E evidence was a
source-order assertion (`_run_startup_recovery` runs before
`_seed_exit_engine_from_db` before the monitors) — proof of call
ordering, not proof of actual rehydration behavior.

`client_runner._seed_exit_engine_from_db()` delegates to
`APExitEngine.seed_from_db()`, which is pre-existing, generic, and reads
`position_manager.get_active_positions()` — it has no awareness of
`filled_entry_handoff_state` and treats a recovery-completed position
identically to a fresh-fill-completed one, because both paths write the
position row through the same shared `_bind_filled_entry_durable_identity`
/ `_open_position_safe` / `_seed_exit_engine` primitives (verified: single
definition of each, called from both `process_pending_order` and
`recover_interrupted_filled_entry_handoff`, no parallel reimplementation).

Added `test_seed_from_db_rehydrates_exactly_one_owner_for_complete_position`:
builds one exact active DB position row (client/mode/OCC/position_id
matching what a COMPLETE handoff leaves behind), rehydrates a fresh
`APExitEngine` with a broker double that raises on any submit/cancel/stop
call, and asserts exactly one owner in `engine._positions` with exact
identity match. No production change was required — existing generic
mechanism, unmodified.

---

# FINAL AMENDMENT: symbol-lock ownership

Independent audit found a second, separate money-path defect after the
Window A–E amendments above: `_release_entry_guards_atomically()` decided
whether to delete the per-symbol advisory lock (`ap/state.py`'s
`acquire_symbol_lock()`) using timestamp ordering as a proxy for ownership:

```text
delete_symbol_lock = lock_ts <= filled_ts_epoch
```

This is not ownership proof. Concrete counterexample:

```text
t=0   ENTRY A acquires the symbol lock (lock_ts = 0)
t=90  A's lock TTL (90s) expires
t=91  a newer, unrelated same-symbol ENTRY B reacquires the lock
t=95  ENTRY A finally FILLS; process crashes before A completes handoff
      restart recovery processes A
      -> lock_ts (91) <= filled_ts (95) -> old code DELETEs B's live lock
```

Deleting another in-flight ENTRY's active symbol lock is unrecoverable
money-path corruption (it would allow a second concurrent same-symbol
ENTRY to bypass the lock's intended mutual exclusion).

## Fix: exact owner_id proof, not timestamp inference

`ap/state.py`'s `acquire_symbol_lock()` gained an optional `owner_id`
parameter. When supplied, the kv lock payload becomes
`{"ts": ..., "owner_id": <exact local_order_id>}` instead of the legacy
`{"ts": ...}`. `ap/execution.py` was changed to generate `local_order_id`
*before* calling `acquire_symbol_lock()` (moved earlier than its prior
call site; no other behavioral effect, since nothing read
`local_order_id` between the old and new assignment points) and pass that
same id as `owner_id`, and the identical id is later persisted on the
ENTRY order row as before.

`_release_entry_guards_atomically()` in `ap/fill_monitor.py` now requires
an **exact** match between the current lock's `owner_id` and this order's
own `local_order_id` before it may delete the lock:

```text
current lock row absent                    -> nothing to delete, proceed
current lock owner_id == this local_order_id -> exact ownership proven, delete
current lock owner_id belongs to another order -> PRESERVE
current lock has no owner_id (legacy payload)  -> PRESERVE
current lock payload is malformed/unparseable  -> PRESERVE
```

Timestamp ordering is never consulted for this decision. In every
PRESERVE case, this order's own equity reservation release and durable
`filled_entry_guards_release_claimed` / `filled_entry_guards_released`
marker persistence still proceed normally — only the lock DELETE itself
is withheld. A preserved lock is bounded by its own TTL and expires on
its own; the worst outcome is a bounded symbol-reuse delay, never another
order's corrupted risk state.

## File budget consequence

This fix required touching `ap/state.py` and `ap/execution.py` in
addition to `ap/fill_monitor.py`, expanding the original two-file budget
to four files (see "HARD FILE BUDGET" above). No other production file
was touched; `ap/filled_entry_recovery_authority.py` is unaffected by
this amendment.

## Test evidence

`tests/test_p0_filled_entry_restart_handoff_recovery.py` proves, among
others:

- exact `owner_id == local_order_id` match -> lock deleted
- current lock row absent -> handoff completes normally, nothing to delete
- mismatched `owner_id` (belongs to a different order) -> preserved
- missing `owner_id` (legacy `{"ts": ...}` payload) -> preserved
- malformed/unparseable lock payload -> preserved
- the exact TTL-reacquire-before-old-fill counterexample above,
  reproduced directly: ENTRY A's lock TTL expires, ENTRY B reacquires the
  same symbol, ENTRY A's restart-recovery later runs -> B's lock survives
- a dedicated real-Postgres integration test exercises two concurrent
  real DB sessions racing `recover_interrupted_filled_entry_handoff()`
  for the same order, seeding the symbol-lock kv row with the exact
  owner-tagged shape `acquire_symbol_lock()` would have written, and
  proves the race still converges to exactly one `COMPLETE` outcome and
  one `HOLD`
- in every preserve case, equity reservation release and the durable
  guard-release marker are proven to still complete

---

# FINAL AMENDMENT: restart pair-truth

Independent audit found a third money-path defect, in
`_cancel_pair_opposite()` (`ap/fill_monitor.py`), separate from the
Window A pair-cancel *durability* fix above (which governs what happens
once a pair *is* found) and the symbol-lock fix above. This defect is
about how the function decided a pair was never present at all:

```text
cancel_local_id = pair_manager.on_fill(...)

if not cancel_local_id:
    return "NOT_APPLICABLE", "no_opposite_pair"
```

`SignalPairManager` (`ap/signal_pair_manager.py`) is a process-memory-only
registry — its `_pairs` dict is empty on every fresh process start. After
a restart, `pair_manager.on_fill(...)` returns `None` for **every** fill
until pairs are freshly re-registered that session, regardless of whether
the fill's ENTRY actually was part of a live 1-1 CALL/PUT pair before the
restart. The code above manufactured durable negative pair truth
(`NOT_APPLICABLE`) from that in-memory absence — meaning a genuine 1-1
pair with two still-live broker orders could be wrongly concluded to have
"no opposite pair," allowing the handoff to reach `COMPLETE` while a live
opposite-side order remained unaddressed.

## Core invariant

**Process-memory absence is never durable negative proof.** Only durable
evidence already persisted on the order row may authorize
`NOT_APPLICABLE`.

## Fix: durable pattern-based applicability classification

A new local helper, `_durable_pair_applicability(order)`, classifies
durable pair-applicability from the order's own persisted `pattern`
field, using the exact same substring markers `SignalPairManager.register()`
already uses (`"1-1"`, `"1_1"`, `"inside"`), so the two can never diverge.
It returns exactly one of `PAIR`, `NON_PAIR`, `UNKNOWN` — a missing,
blank, or otherwise unusable pattern is always `UNKNOWN`, never defaulted
to `NON_PAIR`.

`_cancel_pair_opposite()`'s behavior when `pair_manager.on_fill(...)`
returns `None` is now:

```text
durable NON_PAIR -> NOT_APPLICABLE / DURABLE_NON_PAIR
durable PAIR     -> OUTCOME_UNPROVEN / PAIR_REGISTRY_MISSING_AFTER_RESTART
UNKNOWN          -> OUTCOME_UNPROVEN / PAIR_APPLICABILITY_UNPROVEN
```

No speculative broker cancel, no loose ticker search, and no inferred
opposite identity are performed in either `OUTCOME_UNPROVEN` branch —
pure HOLD. The pre-existing CASE-A path (pair manager DID return an exact
opposite `local_order_id`) — broker-cancel-first, explicit confirmation
required, local OSM transition only after confirmation, `CONFIRMED` only
on full success — is untouched.

## Lifecycle: uncertainty blocks completion, not position protection

`OUTCOME_UNPROVEN` pair state does not mean the filled position goes
unmanaged. The existing fresh-fill caller already gates only `COMPLETE`
and guard-release on `pair_state in {NOT_APPLICABLE, CONFIRMED}`; canonical
DB position creation/recreation, exact durable ENTRY-position identity
bind, and canonical exit-engine owner seed/adoption all proceed above
that gate regardless of pair state. Pair uncertainty blocks handoff
*completion authority* — it does not block position risk management. No
caller changes were required for this amendment; the existing gate
already implements this distinction correctly once
`_cancel_pair_opposite()` stopped manufacturing false `NOT_APPLICABLE`.

`recover_interrupted_filled_entry_handoff()` was independently confirmed
(via the existing AST-based static guard test,
`test_recovery_never_calls_cancel_pair_opposite_statically`, unmodified
and still passing) to never call `_cancel_pair_opposite` or reference
`pair_manager` at all — recovery only ever reads the already-persisted
`filled_entry_pair_resolution_state` marker. Zero pair-cancel authority
in recovery, unchanged by this amendment.

## Test evidence

`tests/test_p0_filled_entry_restart_handoff_recovery.py` proves, among
others:

- the primary merge-gate regression: a real (not mocked)
  `SignalPairManager` with an empty `_pairs` registry (simulating a
  process restart) combined with a durable `pattern="1-1"` ENTRY yields
  `OUTCOME_UNPROVEN` / `PAIR_REGISTRY_MISSING_AFTER_RESTART`, with zero
  broker-cancel calls and zero OSM transition calls
- the same proof repeated across the full pair-pattern taxonomy
  (`"1-1"`, `"1-1 continuation"`, `"1_1_break"`, `"inside"`,
  `"inside_bar"`)
- durable non-pair patterns (`"breakout"`, `"continuation"`, `"fvg"`,
  `"orb"`) still resolve `NOT_APPLICABLE` / `DURABLE_NON_PAIR` — ordinary
  trades are not stuck
- missing/blank/whitespace-only pattern resolves `OUTCOME_UNPROVEN` /
  `PAIR_APPLICABILITY_UNPROVEN`, never `NOT_APPLICABLE`
- all pre-existing CASE-A (registered pair found) tests, negative
  broker-cancel controls, crash-window tests, and the fresh-fill
  `OUTCOME_UNPROVEN` / `CONFIRMED` / `NOT_APPLICABLE` lifecycle tests
  (including the test proving canonical position/bind/exit-owner still
  proceed while guards remain unreleased under `OUTCOME_UNPROVEN`)
  remain unchanged and passing

Production change for this amendment: `ap/fill_monitor.py` only.

---

# HISTORICAL EVIDENCE: USE, DO NOT CHERRY-PICK

## Historical #429

#429 previously introduced the correct general liveness idea: reselect terminal `FILLED` ENTRY rows only when downstream canonical handoff remained incomplete.

Its recovery predicate recognized shapes such as:

```text
position_id missing
OR canonical_owner_handoff_retry_required = true
OR canonical handoff entry proof exists but is not true
OR last_error starts FILLED_ENTRY_POSITION_IDENTITY_BIND_FAILED
OR last_error starts FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN
```

It also correctly skipped a second terminal OSM transition for an already-durable `FILLED` row.

**Do not cherry-pick #429.** It accumulated broader recovery and standing-stop behavior that must not be restored wholesale.

## Historical #455

#455 identified the safety flaw created by historical FILLED replay:

```text
historical broker order still says FILLED
!=
current option position still exists now
```

Its read-only `filled_entry_recovery_authority` concept is the right present-tense boundary. It classified current risk into:

```text
ACTIVE_EXISTING
ACTIVE_RECREATE
NONACTIONABLE
HOLD
```

#455 was incomplete and never finished current-main integration.

**Do not cherry-pick #455.** Port only the narrow present-tense authority concept onto fresh current main and tighten it to this spec.

## P0 AMENDMENT: legacy FILLED row contract

`FILLED_ENTRY_LEGACY_GUARD_RELEASE_UNPROVEN` already correctly holds any
row whose `filled_entry_handoff_state` predates this durable protocol
(not `IN_PROGRESS`/`HOLD`/`COMPLETE`). This behavior is unchanged by this
amendment and must remain fail-closed.

This PR provides restart-safe handoff for fills processed **after** this
durable protocol exists. Legacy FILLED rows (Jason/Jose or any other
client) with unproven historical guard-release or pair-cancel outcomes
remain `HOLD` unless a separately authorized exact historical repair
proves those outcomes. No production-data repair is authorized in this
PR or this amendment.

---

# HARD FILE BUDGET

## SUPERSEDED — see "FINAL AMENDMENT: symbol-lock ownership" below

The original budget below (maximum TWO production files) governed this PR
through its Window A–E amendments. It was intentionally superseded by a
later, independently-audited amendment that expanded the budget to FOUR
production files in order to close a real money-path defect: restart
recovery could delete a live, active symbol lock belonging to a different
in-flight ENTRY (see the dedicated section below for the full defect,
fix, and evidence). That expansion is final and accepted. The two-file
figure below is preserved for history only and must not be treated as
the current constraint.

## Original (superseded): Production: maximum TWO files

```text
1. ap/fill_monitor.py
2. ap/filled_entry_recovery_authority.py
```

`ap/filled_entry_recovery_authority.py` may be newly created if it does not exist on current main.

If a third production file appears necessary, **STOP and explain exactly why. Do not expand scope.**

## FINAL (current): Production: FOUR files

```text
1. ap/fill_monitor.py
2. ap/filled_entry_recovery_authority.py
3. ap/state.py
4. ap/execution.py
```

`ap/state.py` and `ap/execution.py` were added solely to carry an exact
`owner_id` token from symbol-lock acquisition through to restart-recovery
deletion authority — see "FINAL AMENDMENT: symbol-lock ownership" below.
No other change was made to either file. Any further production file
beyond these four still requires the original STOP-and-explain discipline.

## P0 AMENDMENT: EXIT scope correction

Independent audit found `check_order_with_broker()` had been changed to
apply the new strict positive-integral/positive-finite admission gate
globally to `{"FILLED", "EXIT_FILLED"}`, collaterally changing EXIT
terminalization control flow. This PR's one job is FILLED ENTRY restart
handoff; EXIT semantics are out of scope.

Removed the global gate. Verified no protection was lost:

- EXIT still has its own pre-existing PR #235 hardening (`filled_qty<=0`
  block, predates this PR) unchanged.
- ENTRY's admission boundary (`_validate_filled_entry_admission`) already
  independently re-validates `result.get("filled_qty")` /
  `result.get("avg_fill")` with the same strict helpers, so ENTRY lost no
  protection either — the removed block was fully redundant for ENTRY.

The raw-value parsing hardening in `check_order_with_broker` (using
`_strict_positive_integral`/`_strict_positive_finite` instead of naive
`int()`/`float()` coercion for `filled_qty`/`avg_fill`) was kept, since
it is a strict superset improvement over the old casts and ENTRY's
admission boundary depends on receiving accurately-coerced values — only
the new hard error/short-circuit behavior was removed.

## Tests

Final focused files:

```text
tests/test_p0_filled_entry_restart_handoff_recovery.py
tests/test_p0_real_exit_engine_canonical_adoption.py
tests/test_p0_filled_entry_durable_identity_handoff.py
tests/test_fill_monitor_mvp_hardening.py
```

`tests/test_p0_historical_filled_recovery_authority.py`, listed in an
earlier draft of this spec, was superseded — its intended coverage is
provided by `tests/test_p0_filled_entry_restart_handoff_recovery.py`,
which already exercises `ap/filled_entry_recovery_authority.py` directly.
No separate file was created for it.

`.github/workflows/p0_regression.yml` may change only to register the exact new focused test files if not already present.

This binding spec file may change as implementation findings are recorded.

---

# FORBIDDEN PRODUCTION EDITS

Do not modify:

```text
ap/position_manager.py
ap_exit_engine.py
ap/order_state_machine.py
ap/order_monitor.py
ap_execution_core.py
ap_reconciler.py
ap/manual_close_reconciliation.py
scanners
entry watcher
contract selector
quote revalidator
queue
proof writers
broker adapters
sizing
risk thresholds
entry intelligence
exit intelligence
migrations
schema
```

Do not add a new submitter, cancel owner, retry engine, background scheduler, or startup service.

---

# FROZEN STRATEGY / MONEY-PATH BEHAVIOR

This PR must not change:

```text
entry eligibility
trigger logic
contract ranking
DTE
delta
moneyness
spread
OI / volume
premium gates
affordability
position sizing
max contracts
retry counts
entry price policy
profit targets
stops
runner thresholds
technical exits
scanner behavior
watcher behavior
PAPER/LIVE execution semantics
```

Live behavior changes only in **recovery of a broker-confirmed FILLED ENTRY's downstream local ownership handoff**.

---

# REQUIRED DURABLE HANDOFF STATE

Use existing `orders.meta` JSONB. **No migration.**

Use one explicit state key rather than unrelated booleans that can disagree.

Recommended contract:

```text
meta.filled_entry_handoff_state = IN_PROGRESS | COMPLETE | NONACTIONABLE | HOLD
```

Supporting evidence fields may include:

```text
filled_entry_handoff_started_at
filled_entry_handoff_completed_at
filled_entry_handoff_position_id
filled_entry_handoff_reason_code
filled_entry_handoff_checked_at
```

Names may be adjusted only if current #470 already has an equivalent canonical namespace. Reuse current exact-head naming instead of creating duplicate marker families.

## Marker ordering invariant

For an ordinary fresh ENTRY fill, after broker fill admission succeeds but **before** the order becomes durably terminal `FILLED`, persist/claim `IN_PROGRESS` using exact order identity.

Why before terminal transition:

```text
marker write succeeds -> process can safely transition FILLED
marker write fails -> do NOT make the row terminal and invisible
```

If the `IN_PROGRESS` marker cannot be durably persisted, HOLD that iteration before the terminal OSM transition. The ordinary pending row remains pollable on the next cycle.

The claim must be idempotent and exact-fenced by at least:

```text
client_id
local_order_id
proven broker_order_id
exact OCC contract
kind = ENTRY
exact lowercase execution_mode live|paper
```

Do not overwrite `COMPLETE`, `NONACTIONABLE`, or conflicting durable evidence.

## Completion ordering invariant

Do not mark `COMPLETE` until all required downstream work is proven:

```text
canonical DB position exists
AND
#470 exact ENTRY <-> position durable identity bind succeeds
AND
#470 exact canonical exit owner is proven
AND
entry equity/symbol guards have been released through the existing idempotent release path
```

`COMPLETE` should be the final durable lifecycle write for this handoff, except noncritical audit/observability.

If completion persistence fails, leave the row recoverable (`IN_PROGRESS`) so restart can safely replay only the idempotent downstream handoff.

---

# RECOVERY CANDIDATE QUERY

Do **not** add terminal `FILLED` rows to ordinary `get_pending_orders()`.

Create a separate query, e.g.:

```python
get_interrupted_filled_entry_handoffs(client_id)
```

The separate query is a money-path boundary. Recovery rows must never accidentally execute the fresh-fill branch.

Candidate must require:

```text
exact client_id
kind = ENTRY
status = FILLED
execution_mode exactly live|paper
proven non-placeholder broker_order_id
positive filled_qty
positive finite fill_price
nonblank OCC contract
```

And one incomplete-handoff condition:

```text
handoff_state = IN_PROGRESS
OR orders.position_id missing/blank
OR last_error starts FILLED_ENTRY_POSITION_IDENTITY_BIND_FAILED
OR last_error starts FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN
OR an exact current #470 retry marker proves canonical ownership incomplete
```

Exclude:

```text
handoff_state = COMPLETE
handoff_state = NONACTIONABLE
malformed/blank execution mode
placeholder broker IDs
EXIT rows
non-FILLED rows
```

Do not use ticker-only, same-underlying, nearest-time, plan-only, signal-only, cross-client, or cross-mode recovery selection.

Bound the query and order deterministically. If pagination is needed, use deterministic/keyset progression. Do not repeatedly starve later rows behind a fixed oldest-N page.

---

# PRESENT-TENSE CURRENT-RISK AUTHORITY

Create/port a read-only helper in:

```text
ap/filled_entry_recovery_authority.py
```

Recommended API shape:

```python
evaluate_filled_entry_recovery_authority(
    *,
    order,
    pm,
    broker_positions,
    runtime_execution_mode,
    today=None,
) -> dict
```

Do not make this helper submit/cancel broker orders or mutate DB state.

## Fetch broker positions ONCE per client recovery batch

If any recovery candidates exist in a loop iteration, obtain one authoritative current broker-position snapshot for that client's execution broker using the strict recovery-local broker-position parser in `ap/filled_entry_recovery_authority.py`.

Do not call the broker positions endpoint independently once per historical FILLED row.

If the authoritative snapshot is unavailable, malformed, auth-failed, or contradictory, the batch/candidates HOLD. Do not reinterpret an unavailable response as an empty account.

## Runtime-mode fence

Require:

```text
order.execution_mode == runtime_execution_mode == live|paper
```

A client-scoped loop is not sufficient mode proof.

Malformed, absent, or conflicting runtime mode -> HOLD.

## OCC validity / expiration

Require exact full OCC validation consistent with current #470's strict OCC parser.

Do not use a regex search that accepts a valid-looking suffix after malformed prefix text.

If contract expiration is strictly before current market date -> `NONACTIONABLE`.

Expiration today is not automatically historical/nonactionable.

## Current broker position matching

Use exact normalized OCC contract.

Required outcomes:

```text
0 exact matches after an authoritative successful snapshot
-> NONACTIONABLE / NO_CURRENT_BROKER_POSITION

>1 exact matches or otherwise ambiguous position shape
-> HOLD

1 exact match with malformed/nonpositive/negative quantity
-> HOLD
```

Do not use absolute value to turn a negative/short quantity into a valid long position.

Do not infer same-ticker positions are equivalent.

## Local canonical position matching

First use exact execution identities when available through current Position Manager helpers:

```text
local_order_id
broker_order_id
orders.position_id
```

If no exact local position is found but current broker risk is proven, return `ACTIVE_RECREATE`; the caller may invoke current #470-compatible `_open_position_safe()` and rely on current Position Manager idempotency to return an already-existing plan/signal position rather than inventing a duplicate.

For any located local position require exact:

```text
client_id
execution_mode
OCC contract
nonterminal/open risk
```

If local position is terminal but broker position is currently open -> HOLD.

If local remaining quantity is available, require exact equality with current broker quantity.

If no local position exists, require durable ENTRY `filled_qty` exactly equals current broker quantity before `ACTIVE_RECREATE`. A mismatch may represent a later partial exit or aggregation and must HOLD.

Never infer ownership from aggregate same-OCC quantity across multiple unrelated local positions.

## Required authority dispositions

### `ACTIVE_EXISTING`

Current exact OCC broker risk exists and an exact active local canonical position is proven.

Allowed next step: #470 bind/owner repair only.

### `ACTIVE_RECREATE`

Current exact OCC broker risk exists, no exact local canonical position is found, and durable filled quantity exactly equals current broker quantity.

Allowed next step: call `_open_position_safe()` once, then #470 bind/owner repair.

### `NONACTIONABLE`

Only when present-tense truth is authoritative, e.g.:

```text
expired OCC
or
successful current broker-position snapshot proves exact OCC absent
```

Do not create/reseed position or exit ownership.

Persist a terminal recovery disposition so the row is not selected forever.

Do not create proof trades or fabricate a close.

### `HOLD`

Any unavailable/ambiguous/contradictory state:

```text
broker positions unavailable
runtime mode unproven/mismatch
malformed OCC
quantity conflict
multiple matches
local terminal vs broker open
identity conflict
local/current quantity conflict
```

Zero money-path mutation beyond exact diagnostic/retry state on the ENTRY row.

---

# DEDICATED RECOVERY PROCESSOR

Create a separate function, e.g.:

```python
recover_interrupted_filled_entry_handoff(...)
```

Do not route recovered terminal FILLED rows through the ordinary fresh-fill side-effect branch.

## Explicitly forbidden inside the recovery processor

```python
osm.transition(..., 'FILLED')
_cancel_pair_opposite(...)
_place_standing_stop_best_effort(...)
_establish_*standing_stop*(...)
broker.submit_order(...)
broker.place_order(...)
broker.place_stop_order(...)
broker.cancel_order(...)
HTTP POST /orders
HTTP DELETE /orders/<id>
```

There must be an explicit broker-mutation trap test proving these stay at zero.

The recovery processor may read current broker positions and current DB state.

---

# RECOVERY EXECUTION BY DISPOSITION

## ACTIVE_EXISTING

Build a durable fill result only from already-persisted exact ENTRY truth:

```text
status = FILLED
filled_qty = orders.filled_qty
avg_fill = orders.fill_price
source = durable_filled_entry_restart_recovery
```

Do not ask historical broker order status to re-prove the fill.

Then:

```text
existing canonical position
-> #470 _bind_filled_entry_durable_identity(...)
-> #470 _seed_exit_engine(...)
-> #470 exact canonical owner verification
-> existing idempotent entry guard release
-> persist COMPLETE last
```

If bind fails:

```text
remain recoverable
persist/retain FILLED_ENTRY_POSITION_IDENTITY_BIND_FAILED
HOLD
```

If seed/adoption/owner proof fails:

```text
remain recoverable
persist/retain FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN
HOLD
```

No second seed in the same pass after a failed result.

## ACTIVE_RECREATE

Use the same durable fill result and call existing `_open_position_safe()` exactly once.

Required exact inputs:

```text
client
runtime-proven execution mode
exact OCC
local ENTRY id
proven ENTRY broker id
signal/plan provenance
filled_qty
fill_price
```

Then perform the same:

```text
#470 bind
-> #470 seed/adopt
-> exact owner proof
-> guard release
-> COMPLETE last
```

No new position-creation implementation may be introduced outside current Position Manager authority.

## NONACTIONABLE

Persist exact handoff state/reason such as:

```text
filled_entry_handoff_state = NONACTIONABLE
filled_entry_handoff_reason_code = HISTORICAL_FILLED_RECOVERY_NO_CURRENT_BROKER_POSITION
or HISTORICAL_FILLED_RECOVERY_EXPIRED_CONTRACT
filled_entry_handoff_checked_at = now
```

Then exclude from future recovery scans.

Forbidden:

```text
position creation
position close
exit owner creation
proof write
broker mutation
queue mutation
```

## HOLD

Persist a visible reason without destroying the retry predicate.

HOLD must not be misclassified as NONACTIONABLE.

Examples:

```text
FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_UNAVAILABLE
FILLED_ENTRY_RECOVERY_EXECUTION_MODE_MISMATCH
FILLED_ENTRY_RECOVERY_BROKER_POSITION_AMBIGUOUS
FILLED_ENTRY_RECOVERY_POSITION_QUANTITY_CONFLICT
FILLED_ENTRY_RECOVERY_LOCAL_TERMINAL_BROKER_PRESENT_CONFLICT
```

Do not hammer unavailable broker state in a tight inner loop. Recovery runs at the existing fill-monitor cadence, with one broker-position snapshot per client iteration.

---

# STANDING-STOP CONTRACT

This PR does **not** automatically recreate broker-native standing-stop protection on restart.

Fresh-fill behavior stays unchanged unless the implementation must add only the `IN_PROGRESS` handoff marker before OSM terminalization.

Recovered FILLED behavior:

```text
never submit a standing stop
```

Reason:

```text
previous process may have died after broker accepted the stop but before local code recorded the outcome
```

Submitting another stop from historical recovery can create duplicate protective sell orders.

Historical #429 had a durable `SUBMITTING -> SUBMITTED/FAILED/OUTCOME_UNPROVEN` standing-stop claim design with concrete broker stop ID proof. That design may be referenced as evidence only. **Do not port it into this PR unless exact current-main analysis proves restart recovery cannot be safe without it.** If that becomes necessary, STOP and amend this spec before coding because it materially expands broker-mutation scope.

---

# PROCESS-RESTART OWNER REHYDRATION (WINDOW E)

Before implementation, Codex must trace current exact-head startup wiring for the exit engine.

Provide evidence answering:

```text
When a client process restarts with an active DB position whose FILLED ENTRY handoff is already COMPLETE, what exact code reconstructs the behavior-active exit-engine owner?
```

## If current startup hydration is proven

Requirements:

```text
exact client
exact live|paper mode
exact OCC
exact canonical position_id
one behavior-active owner
no broker-repair duplicate
```

Add a focused restart regression and leave startup architecture unchanged.

## If current startup hydration is not proven

Implement the smallest **once-per-process** verification/reseed path in `ap/fill_monitor.py` using the existing #470 seed/adoption function.

It may inspect only active, nonterminal canonical positions linked to exact FILLED ENTRY identity.

It must:

```text
verify exact owner already exists -> no-op
or
seed/adopt via #470 -> verify exactly one canonical owner
```

It must not:

```text
create a new DB position
replay pair cancel
place standing stop
submit/cancel broker order
rewrite proof
```

Do not run an unbounded historical sweep every poll cycle. This COMPLETE-owner verification is process-restart work, not ordinary recurring historical repair.

---

# ORDER/POSITION MUTATION BOUNDARY

Allowed durable writes:

```text
orders.meta handoff state/evidence
orders.last_error handoff diagnostic, exact-fenced
#470-approved orders.position_id backfill
#470-approved missing position local/broker ENTRY identity backfill
current Position Manager's existing position creation path only when ACTIVE_RECREATE is proven
```

Forbidden:

```text
overwrite a conflicting nonblank durable identity
change execution_mode
change contract
change client_id
change filled_qty
change fill_price
change entry economics
change position quantity
fabricate exit economics
create proof trade
mutate queue
```

Historical CLOSED positions are not repair targets for this PR.

Do not overwrite a closed position's current terminal broker identity with an old ENTRY broker id.

---

# REQUIRED STEP-BY-STEP IMPLEMENTATION ORDER FOR CODEX

Codex must implement in this order and stop at any failed invariant. Do not make one giant patch and hope tests explain the wreckage afterward.

## Step 0: exact-head preflight / dependency trace

1. Fetch current `main` SHA.
2. Confirm #470 is merged or rebase this branch onto the exact implementation head after #470.
3. List exact #470 helper names/signatures for:
   - durable ENTRY <-> position bind
   - exit-engine seed/adoption
   - exact owner verification
   - handoff failure persistence if present
4. Trace current startup exit-engine hydration for Window E.
5. Trace current fresh-fill standing-stop and pair-cancel call sites.
6. Confirm `get_pending_orders()` still excludes terminal FILLED rows.
7. Report findings before changing production code.

If #470 primitives are absent: STOP.

## Step 1: add focused tests for the failure before implementation

Create the new focused recovery tests first.

At minimum prove current exact head fails the crash-window scenarios without recovery.

Do not weaken existing tests.

## Step 2: add durable IN_PROGRESS / COMPLETE handoff state

In `ap/fill_monitor.py` only.

Add exact-fenced helper(s) for handoff state persistence.

Claim/persist `IN_PROGRESS` after valid broker ENTRY fill admission but before terminal OSM FILLED transition.

Marker persistence failure -> no terminal transition in that iteration.

Mark `COMPLETE` only after exact owner proof and guard release.

## Step 3: add read-only current-risk authority

Create/port `ap/filled_entry_recovery_authority.py`.

No writes, submits, or cancels.

Use pre-fetched authoritative broker positions snapshot + exact runtime mode + exact OCC + quantity/local-position truth.

Implement and test ACTIVE_EXISTING / ACTIVE_RECREATE / NONACTIONABLE / HOLD.

## Step 4: add separate recovery query

Add `get_interrupted_filled_entry_handoffs(client_id)`.

Do not modify ordinary pending-query semantics to include terminal FILLED.

Exact identity and incomplete-handoff predicate only.

## Step 5: add dedicated recovery processor

Add `recover_interrupted_filled_entry_handoff(...)`.

No OSM terminal replay.
No pair cancel.
No standing stop.
No broker mutation.

Use only durable fill truth + current broker-position authority + existing #470/PM primitives.

## Step 6: wire recovery into fill monitor loop

When recovery candidates exist:

```text
fetch one authoritative current broker-position snapshot for this client
process recovery candidates
then ordinary pending orders
```

Recovery of exposed current risk should not be starved behind ordinary polling.

Deduplicate by exact local order id if any query overlap becomes possible.

Preserve stop_event behavior and existing poll cadence.

## Step 7: close Window E

Use Step 0 startup trace.

Either prove existing startup hydration with tests or add smallest once-per-process COMPLETE active-owner verification in fill monitor.

No exit-engine production edit.

## Step 8: run focused + adjacent regression suite

Do not proceed to broad CI until focused recovery tests pass.

## Step 9: exact-head money-path audit

List changed files and grep/trace all broker POST/cancel call sites in changed production code.

Prove this PR added none.

## Step 10: exact-head P0 CI

Only after focused/adjacent tests and diff audit pass.

PR remains Draft until independent audit.

---

# REQUIRED REGRESSION MATRIX

At minimum implement these tests.

## Crash/liveness

1. Crash immediately after durable `FILLED`, current broker position exists -> recover/find canonical position and complete owner handoff.
2. Crash after position creation but before bind -> bind the existing position, no duplicate.
3. Crash after fresh-fill standing-stop broker call -> recovery performs zero standing-stop calls.
4. Crash after bind success but before owner seed -> `IN_PROGRESS` detects recovery even with nonblank `orders.position_id`.
5. Crash after owner proof but before COMPLETE persistence -> recovery is idempotent and completes.
6. COMPLETE handoff is excluded from incomplete recovery polling.
7. Duplicate recovery ticks -> same position, same identity, exactly one owner.
8. Process restart after COMPLETE active position -> exact canonical owner exists after startup, whether by proven existing startup hydration or this PR's once-per-process verification.

## Production replay

9. Jason LIVE PEP shape with `position_id=NULL` and exact current broker position -> ACTIVE_RECREATE/resolve using exact client/live/OCC/broker/fill identity.
10. Jose PAPER PEP equivalent stays strictly paper and cannot adopt LIVE owner/state.

## Historical/nonactionable

11. Historical FILLED order whose exact OCC is absent from authoritative current broker positions -> NONACTIONABLE, zero position/owner creation.
12. Expired exact OCC -> NONACTIONABLE without broker mutation.
13. NONACTIONABLE row is not selected forever on subsequent scans.

## HOLD/fail-closed

14. broker positions endpoint unavailable -> HOLD.
15. malformed broker positions payload -> HOLD, not empty-account interpretation.
16. runtime mode absent -> HOLD.
17. runtime mode conflict -> HOLD.
18. order live / runtime paper -> HOLD.
19. same ticker but wrong OCC -> no match / HOLD or NONACTIONABLE only according to exact current snapshot; never cross-contract repair.
20. multiple exact broker-position matches/ambiguous shape -> HOLD.
21. broker quantity zero/negative/malformed -> HOLD.
22. current broker quantity != local remaining qty -> HOLD.
23. ACTIVE_RECREATE durable fill qty != current broker qty -> HOLD.
24. terminal local position while broker exact OCC remains open -> HOLD.
25. conflicting nonblank orders.position_id -> no overwrite.
26. conflicting nonblank position local order id -> no overwrite.
27. conflicting nonblank position broker ENTRY id -> no overwrite.
28. malformed/placeholder ENTRY broker id -> not a candidate / HOLD.
29. malformed OCC prefix with valid-looking suffix -> reject exact OCC validation.

## Side-effect traps

30. recovered FILLED row -> zero `_cancel_pair_opposite` calls.
31. recovered FILLED row -> zero standing-stop calls.
32. recovered FILLED row -> zero ENTRY broker POST.
33. recovered FILLED row -> zero ordinary EXIT broker POST.
34. recovered FILLED row -> zero broker cancel/DELETE.
35. NONACTIONABLE -> zero orders.position_id bind, zero PM create, zero exit seed, zero proof write.
36. HOLD -> zero PM create, zero exit seed, zero broker mutation.

## Durable state ordering

37. IN_PROGRESS persistence occurs before OSM terminal FILLED transition.
38. IN_PROGRESS persistence failure prevents terminal OSM FILLED transition that iteration.
39. COMPLETE is written only after bind success + exact owner proof + guard release.
40. COMPLETE persistence failure leaves row recoverable, not falsely done.
41. failed bind retains retry-visible state/error.
42. failed seed/owner proof retains retry-visible state/error.

## Idempotency / isolation

43. replay twice -> one canonical DB position.
44. replay twice -> one behavior-active exact owner.
45. wrong client same OCC is untouched.
46. wrong execution mode same OCC is untouched.
47. `broker-repair-*` owner cannot remain active beside proven canonical owner after successful #470 adoption.
48. no proof_trades mutation.
49. no queue mutation.
50. no strategy threshold/config mutation.

---

# ADJACENT TESTS THAT MUST STAY GREEN

After focused tests, run at least the current equivalents of:

```text
tests/test_p0_filled_entry_durable_identity_handoff.py
tests/test_fill_monitor_mvp_hardening.py
tests/test_p0_broker_owned_exit_requested_recovery.py
tests/test_p0_reconciler_external_close_broker_truth.py
tests/test_p0_live_executable_bid_pnl.py
```

Also include any current test that proves broker-repair -> canonical owner collapse and execution-mode isolation.

If #470 renamed/merged tests, use exact current-main paths and document the mapping.

---

# REQUIRED STATIC / DIFF VERIFICATION

At minimum:

```bash
python -m py_compile ap/fill_monitor.py ap/filled_entry_recovery_authority.py
git diff --check
```

Then inspect the exact diff and prove:

```text
production files <= 2
no new broker submit call site
no new broker cancel call site
no proof writer change
no queue change
no PM/exit-engine/OSM production edit
no schema/migration
```

Then exact-head P0 Regression Suite.

---

# OBSERVABILITY

Recovery must be visible without creating a competing taxonomy for ordinary broker fill truth.

Suggested recovery-specific audit/reason codes:

```text
FILLED_ENTRY_HANDOFF_RECOVERY_STARTED
FILLED_ENTRY_HANDOFF_RECOVERY_COMPLETED
FILLED_ENTRY_RECOVERY_CURRENT_POSITION_PROVEN
HISTORICAL_FILLED_RECOVERY_NO_CURRENT_BROKER_POSITION
HISTORICAL_FILLED_RECOVERY_EXPIRED_CONTRACT
FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_UNAVAILABLE
FILLED_ENTRY_RECOVERY_EXECUTION_MODE_UNPROVEN
FILLED_ENTRY_RECOVERY_EXECUTION_MODE_MISMATCH
FILLED_ENTRY_RECOVERY_BROKER_POSITION_AMBIGUOUS
FILLED_ENTRY_RECOVERY_POSITION_QUANTITY_CONFLICT
FILLED_ENTRY_RECOVERY_LOCAL_TERMINAL_BROKER_PRESENT_CONFLICT
```

Do not emit a second fake `ORDER_FILLED` lifecycle event merely because recovery ran. The broker fill is historical durable truth; this PR records handoff recovery provenance.

---

# ACCEPTANCE TRACE

A successful crash recovery must read like this:

```text
terminal FILLED ENTRY recovery candidate
-> exact client/mode/OCC/broker/fill admission
-> one authoritative current broker-position snapshot
-> current-risk disposition ACTIVE_EXISTING or ACTIVE_RECREATE
-> find/create canonical DB position through existing PM authority
-> #470 exact durable ENTRY <-> position bind
-> #470 canonical exit owner seed/adopt
-> prove exactly one behavior-active owner for exact client/mode/OCC/position
-> zero fresh-fill broker mutations
-> release existing entry guards
-> persist COMPLETE last
```

A historical/non-current row:

```text
terminal FILLED ENTRY candidate
-> authoritative current broker snapshot proves exact OCC absent or expired
-> persist NONACTIONABLE
-> zero position creation
-> zero exit owner creation
-> zero broker mutation
-> zero proof mutation
```

An ambiguous row:

```text
terminal FILLED ENTRY candidate
-> current truth unavailable/contradictory
-> HOLD
-> visible reason
-> zero money-path mutation
```

---

# MONEY-PATH STATEMENT

Live behavior changes: **YES, but only after a broker-confirmed ENTRY is already durably FILLED and its downstream handoff was interrupted/restarted.**

New broker submit authority: **NO**.

New broker cancel authority: **NO**.

Recovered standing-stop authority: **NO**.

Recovered pair-cancel authority: **NO**.

New ENTRY eligibility: **NO**.

New EXIT decision logic: **NO**.

New sizing/risk/threshold behavior: **NO**.

Proof-trade writes: **NO**.

Queue writes: **NO**.

Schema/migration: **NO**.

---

# FINAL CODEX INSTRUCTION

Implement only after Step 0 proves the exact dependency surface on current main.

Do not cherry-pick #429 or #455.
Do not broaden #470.
Do not combine with #472.
Do not redesign Position Manager or Exit Engine.
Do not use historical broker order `FILLED` as proof that current risk still exists.
Do not make terminal FILLED rows ordinary fresh-fill work again.
Do not replay broker mutations during recovery.

Build the smallest current-main implementation that makes the canonical FILLED ENTRY handoff restart-safe, proves current broker risk first, reuses #470 for durable identity/owner closure, and remains idempotent under repeated crashes/restarts.

**DRAFT / SPEC ONLY. No merge, deploy, environment change, migration, production-data repair, or LIVE activation is authorized by this PR.**
