# P0 SPEC — Zero/absent market evidence must never confirm an ENTRY trigger

## Status

**IMPLEMENTED / INDEPENDENTLY AUDITED / PENDING FINAL MERGE AUTHORIZATION.**

This document originated as a spec-only, pre-implementation HARD HOLD. It has
since been implemented across four amendments, each independently audited
and each with fail-first regression proof. It is retained here as the
binding record of the final invariant and control-flow requirements, updated
to match the implementation as merged into the PR branch. It is no longer a
pre-implementation document — treat every "must"/"required" statement below
as a description of what the shipped code in `ap_entry_watcher.py` and
`ap/pending_trigger_classifier.py` actually does, verified by the regression
suites listed in "Amendment history" below.

Base: `main@37f61d004b44bfebd6708680b3261f90e528e0d5`.

Implementing PR: #479 (`spec/p0-watcher-zero-quote-breach-guard-20260815`).
Head implementing this spec at time of writing:
`2ee0a5ecb6f1c6f8f95a931ae692b3cade66461a`.

Independent review and Angel's explicit "merge #479" authorization are still
required before merge. This status update does not itself authorize merge
or deployment.

## Proven production defect

Current watcher flow:

```text
ap_entry_watcher.py::_fetch_quotes()
  -> APEntryWatcher._poll_active_signals()
  -> WatchedSignal.check()
  -> _dispatch_completion()
  -> on_trigger
  -> execution core / OSM broker submit path
```

In the active poll path, when bid and ask are both zero the watcher substitutes `last` into both sides before calling `WatchedSignal.check()`:

```python
if bid == 0 and ask == 0:
    last = float(quote.get("last", 0) or 0)
    bid = ask = last
```

The PUT trigger branch then evaluates:

```python
if bid <= self.entry_trigger:
```

with no positive-bid requirement. Therefore all-zero market evidence can count as a valid PUT breach because `0 <= positive_trigger` is true. Two consecutive invalid polls can confirm the trigger and persist `trigger_price=0.0` / `breach_price=0.0`.

The same last-only substitution also allows a positive LAST value to masquerade as executable bid/ask evidence for trigger authority.

## Binding invariant

**Absence of executable side evidence is never trigger evidence.**

For trigger confirmation:

- CALL requires a finite, strictly positive ASK from the quote's ask field.
- PUT requires a finite, strictly positive BID from the quote's bid field.
- LAST is never allowed to become BID/ASK authority for breach confirmation.
- bool, NaN, +/-Infinity, malformed, missing, zero, and negative side values are unavailable, not prices.
- the momentum requirement means consecutive **valid authoritative** breach polls; an invalid/unavailable poll cannot advance or preserve a half-confirmed breach as though it were valid continuity.

Four amendments during implementation refined this invariant further. The
rules below are the final, audited, shipped behavior — they supersede the
original single-sentence versions of the same rules that appeared earlier
in this document's history (preserved for context in "Amendment history").

### Final invalid-quote rule (Amendments 1–3)

Missing/invalid trigger-side evidence (ASK for CALL, BID for PUT) must:

- **suppress trigger evidence** for that poll (no breach-count advance, no
  new/re-confirmed `trigger_crossed_at`, no `trigger_price`/`breach_price`
  write);
- **reset pre-confirmation momentum** — pre-confirmation only (an invalid
  poll before `trigger_crossed_at` exists breaks two-poll continuity so a
  later valid poll cannot silently combine with the missing one);
- **never suppress independently provable lifecycle safety.** The original
  spec's "return the existing non-triggered state" instruction (§ Required
  production changes, historical text below) was proven too broad: an
  unconditional early return also blinded three unrelated, independently
  provable safety checks that must always be allowed to run regardless of
  whether trigger-side evidence itself is available:
  1. the active per-side scanner stop (Amendment 1) — opposite-side quote
     validity is independent of entry-side quote validity and must still be
     able to prove a broken stop;
  2. the intraday stale-move expiration check (Amendment 3, Blocker 2) —
     the opposite side's quote can independently prove a stale move even
     when the required trigger-side quote is missing;
  3. the late-attachment classifier's stop-truth evaluation (Amendment 2) —
     see "Confirmed active-stop UNKNOWN rule" below.

### Confirmed active-stop UNKNOWN rule (Amendments 1, 2, 3 Blocker 1)

Once `trigger_crossed_at` is durably set and a scanner stop exists, the
opposite (stop-side) quote is the sole authority for stop truth — proving
it broken or proving it intact is entirely independent of whether the
entry-side quote happens to be present on any given poll. Three states:

- **stop-side quote proves the stop broken** → `INVALIDATED`, no callback,
  regardless of entry-side quote availability (Amendment 1; Amendment 3
  Blocker 1's "stop truth returns broken" case; Amendment 2's late-attachment
  equivalent).
- **stop-side quote proves the stop intact** → existing retry/re-trigger
  lifecycle proceeds exactly as it did before these amendments — this is
  not new authority, it is preservation of pre-existing behavior.
- **stop-side quote is itself unavailable ("UNKNOWN")** → **HOLD**: remain
  PENDING, no callback, `trigger_crossed_at` and any deferred retry deadline
  are left untouched, and — critically — the code must never fabricate
  either a stop-safe or a stop-broken result merely because the quote is
  temporarily missing. Reason codes: `ACTIVE_STOP_TRUTH_UNAVAILABLE_BID`
  (CALL) / `ACTIVE_STOP_TRUTH_UNAVAILABLE_ASK` (PUT), set on
  `last_trigger_evidence_reason` in `ap_entry_watcher.py`.

This same three-way rule is enforced in two structurally distinct control
paths that do not share code — see "Why a second production file was
necessary" below.

### Retry-backoff rule (Amendment 4)

`deferred_retry_not_before` throttles **execution retry authority only** —
whether a fresh `TRIGGERED` result may dispatch to the `on_trigger`
callback. It must never throttle **safety observation authority** — whether
`WatchedSignal.check()` runs at all. Concretely, in
`APEntryWatcher._poll_active_signals()`:

- `w.check()` is called on every poll for every active watcher, unconditionally
  of whether that watcher is inside a deferred-retry backoff window;
- `EXPIRED` / `INVALIDATED` results dispatch to terminal cleanup normally
  regardless of defer state — safety may always terminalize a lifecycle,
  even mid-backoff, and a transient stop break during the backoff window
  permanently kills that pending-entry lifecycle even if the underlying
  later recovers before the retry deadline;
- only a `TRIGGERED` result is gated on the deadline: if execution retry is
  still deferred, the watcher is restored to `PENDING` (reason code
  `WATCHER_RETRY_EXECUTION_DEFERRED_SAFETY_ACTIVE`) and the callback is not
  dispatched; `deferred_retry_not_before` is left untouched so the existing
  retry lifecycle resumes exactly as before once the deadline passes.

## Required production changes

### 1. `WatchedSignal.check()` must enforce the invariant itself

Do not rely only on the caller to sanitize values. The method that owns trigger state must be safe when called directly.

Before any CALL/PUT breach comparison:

- reject bool;
- parse numeric safely;
- require `math.isfinite`;
- require side price `> 0`;
- require `entry_trigger` itself to be finite and positive before comparison.

If the authoritative side price is unavailable while the signal is still watching:

- do not increment `breach_count`;
- do not set `breach_price`;
- do not set `trigger_price`;
- do not persist first-breach evidence;
- clear/reset any pending one-poll momentum confirmation so the eventual trigger still requires two consecutive valid authoritative breach polls;
- ~~return the existing non-triggered state~~ — **superseded by Amendment 3
  (Blocker 2), see "Final invalid-quote rule" above.** The original
  instruction to unconditionally return was proven to itself be a defect:
  it blocked the independently provable stale-move expiration check (and,
  in the already-confirmed case, the active-stop check) from ever running
  on a poll where trigger-side evidence happened to be missing. The shipped
  behavior instead suppresses trigger evidence only and falls through to
  those unrelated safety checks; it returns the non-triggered state only
  if nothing else terminalizes.

Do not invalidate the entire signal merely because one quote poll is unavailable. This is a data-unavailable HOLD, not a thesis failure.

### 2. `_poll_active_signals()` must stop converting LAST into trigger-side truth

At the exact path that calls `w.check(bid, ask, ...)`, pass the raw normalized bid and ask values. Remove the `bid = ask = last` substitution for breach authority.

If another non-authoritative display/drift/reclaim path needs LAST as a display fallback, keep that behavior scoped to that consumer. Do not globally delete every LAST fallback without tracing its owner.

### 3. Preserve current trigger identity and persistence behavior

Once two valid side-authoritative polls confirm a breach, preserve the existing trigger timestamp/provenance persistence and LIVE fail-closed write behavior. This PR changes the evidence required to reach that point, not the downstream ownership contract.

## Explicit non-goals

Do not change in this PR:

- trigger thresholds or stop/target geometry;
- `MOMENTUM_POLLS_REQUIRED` count;
- watch duration;
- overnight eligibility;
- contract selector;
- direct-option quote cache (#477 domain);
- broker submit/cancel;
- position sizing;
- intelligence gates;
- quote-age semantics (F11 is a separate P1 unless required solely to preserve an existing invariant);
- PAPER/LIVE behavior except that neither mode may use invalid side evidence as a trigger.

## Production file budget

Original expected production file:

- `ap_entry_watcher.py`

**Actual shipped production files:**

- `ap_entry_watcher.py`
- `ap/pending_trigger_classifier.py`

### Why a second production file was necessary

`ap/pending_trigger_classifier.py::classify_late_attachment()` (PR #388
Block-2) is a structurally independent control path from the ordinary
CALL/PUT breach logic in `ap_entry_watcher.py`. `WatchedSignal.check()`
routes through it whenever a watcher's `late_attachment_state` is active
(`AWAITING_FIRST_TRUTH` / `WITHIN_CONTINUATION` / `WAITING_RESET` — the
states a recovery reattachment via `watch(recovery_rearm=True)` seeds when
durable `trigger_crossed_at` is carried into a reconstructed watcher).

This classifier owned its own, separate implementation of "is the
canonical trigger-side quote available," and — before Amendment 2 — it
checked that availability *before* ever evaluating the opposite-side stop,
unconditionally returning a retry classification when the entry-side quote
was missing even when `trigger_previously_breached=True` and the opposite
side had already proven the stop broken. Fixing the "Confirmed active-stop
UNKNOWN rule" for the late-attachment path required moving the
`trigger_previously_breached` stop-evaluation block ahead of the
canonical-quote-availability short-circuit inside `classify_late_attachment`
itself — that fix cannot be made from `ap_entry_watcher.py` alone, because
the classifier is the sole owner of that decision for late-attachment
states.

Per the original file-budget instruction above ("If another production
file is needed, STOP and explain the missing contract before expanding
scope"): this is that explanation, recorded after the fact. No other file
outside these two was touched for any of the four amendments.

## Required tests

Create `tests/test_p0_watcher_zero_quote_breach_guard.py`.

Drive `WatchedSignal.check()` directly and the real `_poll_active_signals()` path.

Minimum cases:

1. PUT trigger=100, bid=0 ask=0 last=0 for two polls -> remains untriggered.
2. PUT trigger=100, bid=0 ask=0 last=95 for two polls -> remains untriggered; LAST cannot impersonate BID.
3. CALL trigger=100, bid=0 ask=0 last=105 for two polls -> remains untriggered; LAST cannot impersonate ASK.
4. PUT bid missing / ask positive -> no breach authority.
5. CALL ask missing / bid positive -> no breach authority.
6. PUT bid negative -> no breach.
7. CALL ask negative -> no breach.
8. bool side value -> no breach.
9. NaN -> no breach.
10. +Infinity/-Infinity -> no breach.
11. malformed scalar -> no breach and no exception escapes the watcher loop.
12. valid PUT bid below trigger on first poll -> pending count=1, not triggered.
13. invalid poll after first valid PUT breach -> pending momentum resets/clears.
14. next valid PUT breach after invalid poll -> count=1, not triggered.
15. second consecutive valid PUT breach -> triggers exactly once with positive trigger/breach price.
16. equivalent CALL sequence using ASK.
17. valid non-breach poll resets pending confirmation according to existing momentum semantics.
18. invalid quote cannot persist `trigger_crossed_at` or trigger provenance.
19. invalid quote path produces zero broker submit calls through a wired callback stub.
20. valid two-poll breach still invokes existing callback exactly once.
21. LIVE/PAPER identity is preserved exactly through valid trigger callback; no taxonomy changes.
22. open-protect / restart paths that call `check()` cannot bypass the same side-price invariant.

## Diagnostics

Retain enough diagnostics to distinguish:

```text
TRIGGER_EVIDENCE_UNAVAILABLE_BID
TRIGGER_EVIDENCE_UNAVAILABLE_ASK
```

or equivalent stable internal reason codes if the watcher already has a canonical data-unavailable taxonomy. Do not create a second lifecycle state merely for diagnostics.

Log/audit should preserve ticker, side, raw availability class, quote-age field as currently supplied, local order/signal identity, and whether a pending breach confirmation was reset.

**Shipped reason-code taxonomy** (all in `ap_entry_watcher.py`, no new
lifecycle state added for any of them, consistent with the instruction
above):

```text
TRIGGER_EVIDENCE_UNAVAILABLE_ASK         # CALL, entry-side quote unavailable
TRIGGER_EVIDENCE_UNAVAILABLE_BID         # PUT,  entry-side quote unavailable
ACTIVE_STOP_TRUTH_UNAVAILABLE_BID        # CALL, stop-side quote unavailable, trigger already confirmed
ACTIVE_STOP_TRUTH_UNAVAILABLE_ASK        # PUT,  stop-side quote unavailable, trigger already confirmed
WATCHER_RETRY_EXECUTION_DEFERRED_SAFETY_ACTIVE  # entry re-proven during retry backoff, callback withheld until due
```

## Money-path audit

- Live behavior: **YES, protective entry gate correction.**
- Flag-off or active: current watcher active path; correction must not rely on a new flag.
- Broker submit/cancel: this file does not submit directly; invalid evidence must produce zero downstream submit callback.
- Orders/positions/proof/queue: no direct new mutation.
- client_id/execution_mode: unchanged and exact.
- production metadata shape: preserve current `trigger_crossed_at` provenance contract.
- diagnostics: must remain visible downstream.
- PAPER/LIVE pollution: none.
- Could this make Jason trade junk? The defect can; this PR must make that impossible for zero/last-only evidence.

**Final audit result: PASS.** Verified across all four amendments: zero
direct broker submit/cancel calls in every fail-first and regression
scenario (asserted explicitly via `broker.submit_order.call_count == 0` /
`broker.cancel_order.call_count == 0`); no orders/positions/proof_trades/
queue mutation; `client_id` and `execution_mode` preserved exactly through
every callback path exercised; no PAPER/LIVE taxonomy pollution found in
any amendment.

## Amendment history

Each amendment below was independently audited after the prior one shipped,
with fail-first reproduction captured against the pre-fix head before any
production change was made, and full regression coverage run before push.

| # | Commit | What it fixed | Files | Regression coverage |
|---|--------|----------------|-------|----------------------|
| — | `ab7a803` | Original fix: CALL requires valid finite positive ASK, PUT requires BID; LAST has zero trigger authority; invalid quote pre-confirmation resets two-poll continuity. | `ap_entry_watcher.py` | `tests/test_p0_watcher_zero_quote_breach_guard.py` (initial 41 cases) |
| 1 | `5ca460a` | Invalid-quote early return ran before the per-side scanner-stop check — a missing entry-side quote on an already-confirmed, callback-retry-pending watcher could blind it to a valid opposite-side quote that had broken the active stop. | `ap_entry_watcher.py` | +6 tests: confirmed-stop-break-invalidates (CALL/PUT), no-callback-refire (CALL/PUT), stop-intact-preserved (CALL/PUT) |
| 2 | `2e6c113` | `classify_late_attachment()` checked entry-side quote availability before evaluating the opposite-side stop — the same class of defect as Amendment 1, but in the structurally independent late-attachment control path (see "Why a second production file was necessary"). | `ap/pending_trigger_classifier.py` | +18 tests across `tests/test_p0_late_attachment_continuation.py` (8 pure-function) and `tests/test_p0_late_attachment_watcher_integration.py` (10 integration, all 3 late-attachment states × CALL/PUT) |
| 3 | `9e35b0a` | Two blockers: (1) confirmed retry could re-fire `TRIGGERED` on valid entry-side evidence alone while stop-side truth was unknown — fail-open; (2) pre-confirmation invalid trigger-side evidence returned early, before the stale-move expiration check could run. | `ap_entry_watcher.py` | +9 required regression tests (both blockers, CALL/PUT, hold/recover-intact/recover-broken/stale-safety-reachable/no-stale-condition-preserved) |
| 4 | `2ee0a5e` | `_poll_active_signals()` skipped `WatchedSignal.check()` entirely during a deferred-retry backoff window, so a transient stop break during that window was invisible to the state machine. | `ap_entry_watcher.py` | +8 tests driving the real scheduler path with no manual `deferred_retry_not_before` clearing (stop-breaks-during-defer, stop-intact-holds-and-resumes, stop-truth-unavailable-holds, entry-missing+stop-broken-combo — all CALL/PUT) |

Cumulative dedicated-suite size: `tests/test_p0_watcher_zero_quote_breach_guard.py`
grew from 41 to 59 tests across amendments 1, 3, and 4 (amendment 2's tests
live in the late-attachment files above, since that is the file the fix
touched). All 59 pass at head `2ee0a5e`. Full CI-exact P0 Regression Suite
(116 files) passed at every amendment's exact head, most recently
3953 passed / 28 skipped / 0 failed against a fresh local Postgres 16
instance, and green on GitHub Actions at `2ee0a5e`
(https://github.com/angelprecision/angel_precision_bot/actions/runs/31978575659).

## Claude implementation instruction

Start from exact current main. Read `WatchedSignal.check`, `_poll_active_signals`, every nearby `bid == 0 and ask == 0` branch, and the trigger timestamp persistence path before editing.

Reproduce the exact PUT zero-quote bug first with a failing production-path test. Then harden the state owner (`check`) and remove LAST substitution only from breach authority.

After implementation report:

1. exact changed functions;
2. how invalid side values are classified;
3. exact momentum-reset behavior;
4. proof that LAST cannot authorize CALL/PUT breach;
5. zero broker callback count on all invalid cases;
6. valid two-poll regression proof;
7. focused + adjacent tests;
8. exact-head CI SHA;
9. fresh MERGE / HOLD / HARD HOLD recommendation.

This instruction governed the original implementation pass (`ab7a803`) and
each subsequent amendment audit. It remains accurate as a description of
process, not as a statement that implementation has not yet occurred — see
"Status" and "Amendment history" above for the current state.

Do not merge, deploy, change environment configuration, or mutate production data without Angel's explicit authorization.