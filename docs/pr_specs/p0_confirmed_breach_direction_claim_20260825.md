# P0 — Confirmed-Breach Direction Claim

**Date:** 2026-08-25  
**Base main SHA:** `26cb2b4c3f019ba592cbd00882835fc825f3193a`  
**Branch:** `fix/p0-confirmed-breach-direction-claim-20260825`  
**Status:** SPEC / IMPLEMENTATION REQUIRED — **DO NOT MERGE OR DEPLOY YET**

---

## 0. Executive summary

Angel Precision currently makes an opposite-direction ownership decision **before either side has breached**.

For an eligible CALL and eligible PUT on the same client/mode/ticker, the watcher can allow whichever side is registered first to monopolize the ticker and cancel the later side with:

`watcher_block:opposite_side_conflict`

When score and timeframe are tied, this is effectively **first-registration-wins**.

That is the wrong authority.

The intended production invariant is:

> **Pre-breach eligibility does not grant directional execution ownership. Confirmed breach grants directional execution ownership.**

If both directions independently survive admission and are healthy pre-breach watchers, both must be allowed to watch. The first direction to obtain a valid, confirmed trigger becomes the winner. Before the winner is handed to contract selection / ExecutionCore / OSM broker submit, every co-armed opposite watcher for the same client + execution mode + ticker must be terminalized with exact durable cancellation proof. If that proof cannot be obtained, fail closed and do not submit either direction.

This PR must remove the current arrival-order bias **without** loosening any scanner, score, intelligence, trigger, selector, moneyness, DTE, delta, spread, OI, premium, affordability, sizing, live-submit, broker, position, exit, or risk gate.

---

# 1. Production evidence

## 1.1 Aug 24 LIVE symptom

Supabase production audit for Jason LIVE showed **33** `watcher_block:opposite_side_conflict` cancellations on Aug 24.

Every one of those 33 blocked entries was a **CALL**.

Every one had an existing **PUT** watcher on the same symbol.

Examples included:

- CMCSA PUT existing -> CMCSA CALL blocked
- AMZN PUT existing -> AMZN CALL blocked
- ADBE PUT existing -> ADBE CALL blocked
- BIIB PUT existing -> BIIB CALL blocked
- DDOG PUT existing -> DDOG CALL blocked
- CPRT PUT existing -> CPRT CALL blocked
- REGN PUT existing -> REGN CALL blocked
- TSLA PUT existing -> TSLA CALL blocked
- META PUT existing -> META CALL blocked
- WDAY PUT existing -> WDAY CALL blocked

The persisted watcher audit shape included:

- `reason_code=opposite_side_conflict`
- `raw_reason=opposite_side_conflict:existing_watcher_wins`
- current direction `CALL`
- conflicting direction `PUT`
- conflicting state `PENDING`
- current score `70`
- conflicting score `70`
- same `1d` timeframe in the analyzed production examples
- LIVE Tradier watcher quote identity

A representative CMCSA production pair:

- PUT trigger: approximately `26.42`
- CALL trigger: approximately `26.97`
- both score `70`
- both timeframe `1d`
- both pattern `1-2_2U`
- PUT became the retained watcher
- CALL was canceled with `watcher_block:opposite_side_conflict`
- the retained PUT later expired without trading

## 1.2 Multi-day production pattern

Read-only Supabase audit across the affected dates:

| Date | Opposite-side LIVE blocks | Blocked CALL | Equal score | Same timeframe |
|---|---:|---:|---:|---:|
| 2026-08-18 | 19 | 19 | 19 | 19 |
| 2026-08-20 | 27 | 27 | 27 | 27 |
| 2026-08-24 | 33 | 33 | 33 | 33 |
| **Total** | **79** | **79** | **79** | **79** |

For all 79 analyzed conflicts:

- blocked side = CALL
- retained side = PUT
- score tied
- timeframe tied
- pattern tied within each pair
- retained watcher was still pre-breach / `PENDING`

This is not a random distribution.

## 1.3 The retained side often never breached

For the analyzed conflicts:

- Aug 18: 13/19 retained PUTs never breached; 4 more breached but later died in selector
- Aug 20: 21/27 retained PUTs never breached; 4 more breached but later died in selector
- Aug 24: 26/33 retained PUTs never breached; 7 more breached but later died downstream

The critical issue is not that every blocked CALL would have traded. It would be incorrect to claim that.

Counterfactual outcome data for the available production rows showed the blocked CALLs on Aug 18, Aug 20, and Aug 24 did **not** later cross their CALL trigger.

Therefore:

- **Do not claim 79 missed trades.**
- **Do claim 79 deterministic examples of pre-breach directional ownership chosen without market breach authority.**

Several Aug 24 CALLs came close to trigger, proving the architecture has real missed-trade potential if price moves only slightly differently:

- DXCM: trigger `92.56`, observed peak `92.34`, ~0.24% below
- DDOG: trigger `236.31`, observed peak `235.62`, ~0.29% below
- C: trigger `132.23`, observed peak `131.67`, ~0.42% below
- CMCSA: trigger `26.97`, observed peak `26.85`, ~0.45% below

The bug is architectural even when the counterfactual trade outcome on a given day is no-trade.

---

# 2. Root cause

## 2.1 Overnight re-evaluation changes processing order

`ap_overnight_reeval.py` sorts fetched WATCHING signals using `_overnight_signal_sort_key()`:

1. tier
2. score
3. recency
4. stable row identity

The recency component prefers newer signals.

Production pair audit found that the blocked CALL signal was actually created **before** the corresponding PUT in all 79 analyzed cases, typically by roughly 0.6–1.0 seconds.

Because the PUT was slightly newer, morning re-evaluation processed PUT first.

This means scanner creation order was not the cause of PUT ownership.

The downstream morning sort converted:

`CALL created first -> PUT created second`

into:

`PUT watcher armed first -> CALL watcher armed second`

Changing this sort is **not** the fix. Reversing sort order would merely convert a PUT-first bias into a CALL-first bias.

## 2.2 The package shim is the production watcher surface

The repository contains both:

- top-level `ap_entry_watcher.py`
- package `ap_entry_watcher/__init__.py`

The package intentionally shadows the top-level module, imports the base implementation, and overrides ownership / quote-confirmation seams.

Implementation MUST treat `ap_entry_watcher/__init__.py` as the active ownership layer and must not patch only the legacy top-level module while leaving the shim behavior unchanged.

## 2.3 Exact admission defect

The shim serializes registry admission through `_watch_admission_gate`.

Current high-level flow in `APEntryWatcher.add_signal()`:

1. collect opposite-side watchers
2. prune stale / replaceable opposites
3. choose best remaining opposite
4. run `_candidate_wins(new, existing)`
5. if new does not win -> block new with `opposite_side_conflict`
6. if new wins -> durably cancel old watcher and admit new

Current `_candidate_wins()` semantics:

- higher new score -> new wins
- score tie inside tolerance + higher new timeframe tier -> new wins
- otherwise -> existing watcher wins

Therefore equal-score + equal-timeframe healthy pre-breach opposites reduce to:

> **existing watcher wins because it was registered first**

That is deterministic registration-order ownership.

No trigger breach is involved in this decision.

## 2.4 Existing pair-manager contract contradicts the watcher behavior

`ap/signal_pair_manager.py` already documents a directional pair contract for 1-1 setups:

- scanner may generate CALL and PUT
- both enter watcher
- first side to breach/fill wins
- other side is canceled
- if neither breaches, both expire

`ap/queue.py` still registers pair-manager candidates and describes it as an opposite-side cancellation mechanism.

`ap/fill_monitor.py` still contains pair-opposite cancellation after confirmed fill.

However the current watcher can cancel the later side before the pair manager ever becomes useful.

Do **not** blindly revive or generalize the old pair manager as the only fix. Its lifecycle/cancellation identity must be re-audited before being trusted as the pre-submit authority, and cancellation after fill is later than the desired safety boundary.

---

# 3. Required production invariant

## 3.1 Core rule

For one exact execution ownership key:

`(client_id, execution_mode, ticker)`

healthy, independently eligible pre-breach CALL and PUT watchers may coexist.

Neither side owns execution merely because it was registered first.

Directional execution ownership is created only when a direction obtains a **confirmed trigger** under the existing watcher trigger-confirmation rules.

## 3.2 Co-arm rule

A new opposite watcher may co-arm only when the existing opposite is a healthy, ordinary, pre-breach active watcher.

At minimum the implementation must distinguish:

### Co-arm eligible

- exact same client identity
- exact same execution mode
- exact same ticker
- opposite normalized direction
- watcher state is ordinary pre-breach `PENDING`
- watcher is active
- watcher is not ownership-quarantined
- watcher is not rearm-only / disarmed waiting for reclaim
- watcher has no proven prior confirmed trigger
- durable order identity required by cancellation proof is present

### Do not automatically co-arm

Preserve fail-closed handling for unsafe or special states including, at minimum:

- ownership quarantine
- malformed/missing side
- missing durable identity
- rearm-only watcher where current safety behavior intentionally retains ownership
- already-triggered watcher
- unproven cancellation state
- same-side duplicate / same-side replacement behavior

The implementation may refine this matrix, but it may not turn `opposite_side_conflict` off globally.

## 3.3 Stronger-score behavior

Do not assume that a higher pre-breach scanner score should automatically kill the opposite direction.

Both sides have already passed upstream admission. A stronger score may remain diagnostic, but a pre-breach score difference is not automatically sufficient authority to decide which market direction is allowed to exist.

If implementation retains any score-based pre-breach exclusion, it MUST justify the exact policy from existing product semantics and provide production-shape tests proving it is intentional rather than inherited registration-order behavior.

Default target for this PR:

> If both directions are independently execution-eligible and ordinary pre-breach, both may watch regardless of arrival order. Market breach chooses direction.

---

# 4. Trigger-time winner arbitration

## 4.1 Required safety boundary

The losing opposite side must be durably canceled **before the winning side is allowed to proceed into the broker-submission handoff**.

The desired sequence is:

`confirmed breach -> direction claim -> cancel opposite PENDING_TRIGGER -> prove cancellation -> on_trigger / selector / ExecutionCore / OSM submit`

Do not use:

`both submit -> broker decides -> cancel loser later`

That is explicitly out of scope and unsafe.

## 4.2 Use existing durable trigger evidence

The watcher already distinguishes first breach evidence from final confirmation:

- `_pending_first_breach_at`
- `trigger_crossed_at`
- momentum-poll confirmation

After required consecutive breach polls, the confirmed watcher promotes the pending first-breach timestamp into `trigger_crossed_at`.

That timestamp, with its existing provenance contract, should be the primary directional ordering authority if two opposites confirm in the same polling cycle.

## 4.3 Same-cycle / simultaneous confirmation

The base watcher evaluates active watchers and can accumulate multiple `completed` trigger actions before dispatching callbacks.

A naive co-arm implementation can therefore produce this shape:

1. CALL check reaches TRIGGERED
2. PUT check reaches TRIGGERED in same poll batch
3. callback dispatch begins afterward

The PR MUST handle this explicitly.

Do not allow callback list order, `_pending` list order, registration order, dict order, or Python incidental iteration order to choose the direction.

Required deterministic logic:

1. compare proven `trigger_crossed_at` values
2. earliest proven confirmed first-breach wins
3. if one side lacks valid provenance while the other has valid provenance, valid side wins only if doing so is consistent with the existing trigger-evidence contract
4. if both are genuinely indistinguishable / ambiguous, **fail closed** unless a separately documented deterministic quality tie-break is explicitly approved

Ambiguity must never produce two broker submits.

## 4.4 Atomicity / serialization

Introduce a narrow per-watcher or per-ownership-key claim gate that serializes:

- winner selection
- opposite cancellation
- proof verification
- release to `on_trigger`

Do not hold the broad watcher `_lock` across broker/network/DB operations.

Reuse the existing shim philosophy:

- narrow lock for registry/claim state
- durable OSM proof outside broad registry lock
- exact identity recheck before removing watcher ownership

## 4.5 Cancellation proof

The shim already has strong cancellation-proof helpers:

- `_identity()`
- `_cancel_conflicting_watcher_with_proof()`
- `_verify_row()`
- `_remove_after_proof()`
- `_prove_remove_all()`

Prefer reusing / extracting these rather than introducing a second, weaker cancellation contract.

For the losing opposite direction:

- expected `local_order_id` required
- `client_id` required
- `execution_mode` required
- signal identity required
- cancellation must be proven by exact OSM return or durable terminal reread
- identity mismatch = HOLD / fail closed
- cancellation exception + unreadable durable row = HOLD / fail closed
- row still `PENDING_TRIGGER` after cancel failure = HOLD / fail closed

Only after all losing opposites are proven terminal may the winning `on_trigger` callback proceed.

## 4.6 Winner failure after loser cancellation

This edge case MUST be tested and documented:

- winner confirms breach
- opposite is successfully canceled
- winner then fails contract selection or live-submit gate

Expected behavior for this PR:

- do not resurrect the canceled opposite from stale trigger evidence
- preserve exact terminal reason for winner
- a future separately generated signal may create a new lifecycle if it independently qualifies

Do not create hidden automatic direction reversal after a confirmed winner has already claimed and canceled the opposite unless separately designed and audited.

---

# 5. Admission behavior requirements

## 5.1 Same-side behavior unchanged

This PR is not a same-side dedup rewrite.

Preserve current same-side handling unless a direct dependency forces a minimal change.

Examples that must remain protected:

- duplicate signal identity
- lower/equal-quality same-side duplicate where existing watcher owns lifecycle
- replacement requiring durable cancellation proof

## 5.2 Rearm behavior unchanged unless explicitly necessary

Existing tests intentionally protect rearm ownership, including cases where an opposite direction cannot sneak through a disarmed/rearm watcher.

Do not casually break those tests.

If this PR changes rearm/opposite semantics, the PR description MUST state exactly why and add dedicated tests for:

- higher-score rearm owner
- equal-score rearm owner
- reclaimed watcher
- rearm expiration
- opposite ordinary watcher admission
- restart identity

Default scope is healthy ordinary pre-breach active watchers only.

## 5.3 Stale watcher handling

Current stale-opposite pruning may remain.

Do not use stale pruning as a substitute for confirmed-breach arbitration.

A healthy fresh opposite must not be killed merely because it was second to register.

---

# 6. Identity and mode invariants

Every new co-arm and winner-claim path MUST preserve and verify:

- `client_id`
- `execution_mode`
- `signal_id`
- canonical signal identity where present
- `local_order_id`
- ticker
- normalized CALL/PUT direction

No LIVE/PAPER crossover is acceptable.

A PAPER watcher must never cancel or claim against a LIVE watcher even if ticker and signal shape match.

A LIVE watcher must never cancel or claim a PAPER order.

If identity is incomplete, fail closed rather than guessing.

---

# 7. Observability requirements

The present `opposite_side_conflict` audit is useful and must not disappear into silent behavior.

Add structured diagnostics for the new lifecycle.

Suggested stable reason/event codes:

### Admission

- `opposite_side_coarmed`
- `opposite_side_coarm_refused`

Suggested fields:

- `current_local_order_id`
- `current_signal_id`
- `current_direction`
- `current_score`
- `current_timeframe`
- `conflicting_local_order_id`
- `conflicting_signal_id`
- `conflicting_direction`
- `conflicting_score`
- `conflicting_timeframe`
- `coarm_eligible`
- `coarm_policy_version`
- `client_id`
- `execution_mode`

### Trigger claim

- `direction_claim_won`
- `direction_claim_lost`
- `direction_claim_ambiguous_hold`
- `direction_claim_cancel_unproven_hold`

Suggested fields:

- winner / loser local order IDs
- winner / loser signal IDs
- winner / loser directions
- winner / loser `trigger_crossed_at`
- winner / loser confirmation timestamps if available
- exact cancellation proof outcome
- durable loser status after cancellation
- ownership key `(client_id, execution_mode, ticker)`

Do not collapse these back into generic `opposite_side_conflict` in durable rows.

The post-deploy production question must be answerable directly from Supabase:

> For every ticker that had both eligible directions, which sides co-armed, which side confirmed first, was the opposite durably canceled before broker handoff, and did exactly one side reach submit?

---

# 8. Required tests

## 8.1 Production-shape replay fixture

Create a deterministic fixture representing the actual Aug 24 shape:

- same client
- `execution_mode=live`
- same ticker
- same pattern
- same score (`70` is acceptable for exact replay)
- same timeframe (`1d`)
- distinct signal IDs
- distinct local order IDs
- CALL scanner row created first
- PUT scanner row created slightly later
- overnight reevaluation / registration processes PUT first
- both are healthy and independently eligible

Expected result:

- PUT admission succeeds
- CALL admission also succeeds
- neither admission cancels the other
- both remain pre-breach PENDING watchers

## 8.2 Registration-order parity

Run both permutations:

- PUT registered first, CALL second
- CALL registered first, PUT second

Given identical market quote sequence, final winner MUST be identical.

Registration order may not affect broker-facing outcome.

## 8.3 CALL wins

Quote sequence:

- CALL crosses and confirms required momentum polls first
- PUT remains unbreached

Assert:

- CALL direction claim wins
- PUT durable pending entry is canceled and cancellation proven
- PUT watcher removed only after proof
- CALL `on_trigger` invoked exactly once
- PUT `on_trigger` never invoked
- no second broker submit path

## 8.4 PUT wins

Mirror of CALL test.

Assert exact directional symmetry.

## 8.5 Neither breaches

Both co-arm.

Neither trigger confirms before natural expiration.

Assert:

- no `on_trigger`
- no broker submit
- both expire through normal lifecycle
- no false direction claim

## 8.6 Same poll batch — CALL first-breach earlier

Force both sides to reach TRIGGERED in one poll batch but give CALL earlier valid `trigger_crossed_at`.

Assert:

- CALL wins regardless of iteration order
- PUT canceled before CALL callback reaches execution handoff

## 8.7 Same poll batch — PUT first-breach earlier

Mirror.

## 8.8 Same poll batch — truly ambiguous

Force equal/indistinguishable valid trigger timestamps or otherwise ambiguous ordering.

Expected:

- fail closed
- zero broker handoff callbacks
- explicit `direction_claim_ambiguous_hold`
- no incidental list-order winner

If implementation chooses an approved quality tie-break instead, encode it explicitly and test both permutations.

## 8.9 Opposite cancellation failure

OSM `cancel_pending_entry` returns false and durable row remains `PENDING_TRIGGER`.

Assert:

- winning callback does not proceed
- explicit hold reason
- no broker submit
- losing watcher remains owned/quarantined as required for retry/repair

## 8.10 Cancellation raises + durable terminal reread

Assert durable terminal reread may prove cancellation if exact identity matches.

## 8.11 Cancellation raises + row unreadable

Assert fail closed.

## 8.12 Identity mismatch

Durable loser row returns wrong:

- local order ID
- client ID
- execution mode
- signal identity

Assert fail closed and zero winning handoff.

## 8.13 LIVE/PAPER isolation

Create otherwise identical watcher pairs across modes.

Assert no cross-mode co-arm arbitration or cancellation.

## 8.14 Same-side regression

Preserve existing same-side conflict behavior.

## 8.15 Rearm regression

Preserve existing rearm conflict tests unless deliberately amended.

## 8.16 Last-only quote regression

The package shim currently hardens last-only quotes at the final polling boundary.

Co-arm / claim changes MUST preserve this behavior.

A last-only quote with no executable bid/ask must not create a confirmed trigger or direction claim when `WATCHER_ALLOW_LAST_ONLY_TRIGGER` is disabled.

## 8.17 Callback retry semantics

The base watcher intentionally keeps triggered watchers owned until `on_trigger` succeeds or exhausts retries.

Do not break this.

Prove:

- direction claim does not cause duplicate callback submissions on retry
- already-canceled losing side is not canceled repeatedly in a way that corrupts lifecycle
- retry of the winning callback cannot allow the loser to re-enter

## 8.18 Restart / recovery

At minimum prove that existing confirmed trigger provenance and lifecycle identity remain valid after restart for the winning side.

Do not reuse an old losing-side trigger after the loser was terminalized.

---

# 9. Broker and mutation scope

## Intended behavior change

This is an **active LIVE behavior change** after deployment.

It changes which pre-breach watcher plans are allowed to remain armed and when opposite local pending entries are canceled.

## Allowed mutations

Only the minimum required local order/watcher mutation necessary to:

- preserve both healthy pre-breach opposite watchers
- cancel losing `PENDING_TRIGGER` lifecycle with durable proof at confirmed winner claim

## Explicitly not allowed

This PR must not:

- loosen trigger levels
- loosen score floors
- change intelligence admission authority
- change selector thresholds
- change moneyness thresholds
- change DTE thresholds
- change delta thresholds
- change spread/OI/volume gates
- change affordability
- change position sizing
- increase max positions
- change daily loss controls
- change broker limit-price logic
- add immediate execution
- submit both opposite sides to broker
- create positions directly
- mutate `proof_trades`
- change exits
- change reconciler ownership
- change queue taxonomy unrelated to this seam

---

# 10. Interaction with PR #504 / current main

Current main at spec creation:

`26cb2b4c3f019ba592cbd00882835fc825f3193a`

This includes:

- merged PR #504 request-scope structural selector truth
- merged PR #509 strict direct-quote cache truth

Do not revert, duplicate, or weaken either fix.

This P0 is independent of #504's selector taxonomy:

- #504 addresses what happens **after a trigger reaches deferred contract selection**
- this PR addresses whether valid opposite directions are prematurely removed **before trigger**

Together they remove two different funnel bottlenecks:

1. false pre-breach directional starvation
2. false request-level moneyness terminalization after breach

---

# 11. Trade-flow expectation

This fix should increase **opportunity throughput**, not force trading volume.

Expected funnel change:

Before:

`eligible CALL + eligible PUT -> one canceled at arm time -> only one direction can ever breach`

After:

`eligible CALL + eligible PUT -> both watch -> market decides which valid trigger occurs first`

This can open more legitimate trade flow because the bot no longer discards one valid directional opportunity solely due to processing order.

However:

- if neither side breaches, still no trade
- if winner breaches but selector rejects every contract, still no trade
- if live-submit gates fail, still no trade
- if risk gates block, still no trade

Success is **not** measured by “more trades at any cost.”

Success is measured by:

> no valid direction is killed solely because it registered second, while exactly one direction can proceed to execution after confirmed market evidence.

---

# 12. Post-deploy acceptance metrics

For the first LIVE sessions after deployment, query by client/mode/ticker and report:

1. number of opposite-direction eligible pairs
2. number co-armed
3. number where neither breached
4. number where CALL confirmed first
5. number where PUT confirmed first
6. number of direction-claim ambiguity holds
7. number of opposite cancellation proof failures
8. number of winning claims reaching selector
9. number receiving contract selected
10. number reaching broker submit
11. number broker acknowledged
12. number filled
13. any ticker with >1 broker-facing ENTRY submit across opposite directions

Hard production invariant:

**zero client/mode/ticker lifecycles may produce opposing broker submits from the same co-armed opportunity set.**

Any violation is P0 and requires immediate hold/rollback.

---

# 13. Implementation guidance

Preferred implementation location:

`ap_entry_watcher/__init__.py`

Reason:

- this is the active package shim
- it already owns opposite-side durable cancellation semantics
- it already serializes watcher admission
- it already provides proof-based cancellation helpers
- it already hardens quote confirmation without rewriting the legacy base

Minimize modifications to top-level `ap_entry_watcher.py` unless a small explicit hook is truly required for pre-dispatch winner arbitration.

A clean design may:

1. add `_is_coarmable_opposite(...)`
2. change `add_signal()` so healthy ordinary pre-breach opposites are retained instead of candidate-score arbitration cancel/block
3. add a narrow `_direction_claim_gate`
4. add `_direction_claim_key(watched)` using exact client/mode/ticker identity
5. add `_resolve_confirmed_direction_claim(watched)`
6. before invoking the real `on_trigger`, inspect triggered opposites and choose using proven `trigger_crossed_at`
7. cancel and prove all losing opposite PENDING_TRIGGER lifecycles
8. invoke original `on_trigger` only after proof
9. preserve callback retry ownership semantics
10. emit structured audit events

Be careful if wrapping `self.on_trigger` inside `_poll_active_signals()`:

- base polling may classify multiple watchers before callbacks are dispatched
- callback wrapper must be idempotent across retry attempts
- temporarily replacing callback must be restored in `finally`
- do not hold `_lock` during OSM DB cancellation
- do not let later callback dispatch for an already-lost watcher call execution

Alternative designs are acceptable if they prove the same invariants and remain surgical.

---

# 14. Review gate

Do not mark this PR mergeable until the reviewer can answer **YES** to all of the following:

- Does an equal-score/equal-timeframe CALL still get canceled merely because PUT registered first? **Must be NO.**
- Can both healthy pre-breach directions be watched? **YES.**
- Is direction decided by confirmed market trigger evidence rather than queue order? **YES.**
- Can both sides ever reach broker submit from one pair? **NO.**
- Is loser cancellation durably proven before winner broker handoff? **YES.**
- Does ambiguous same-cycle confirmation fail closed? **YES.**
- Are client_id and execution_mode exact? **YES.**
- Is LIVE/PAPER crossover impossible? **YES.**
- Are same-side dedup and rearm safety preserved? **YES.**
- Are #504 selector semantics preserved? **YES.**
- Are #509 direct-quote cache semantics preserved? **YES.**
- Are trade eligibility thresholds unchanged? **YES.**
- Are there exact production-shape replay tests? **YES.**
- Does registration-order parity test pass? **YES.**
- Does full P0 regression suite pass on exact head? **YES.**

Until then:

**HARD HOLD / DO NOT MERGE / DO NOT DEPLOY.**
