# P0 Implementation Brief: Preserve Watcher Breach Continuity Across Missing Observations

> Status: implementation brief only. This branch is intentionally opened as a draft PR before production code changes so implementation can be done and audited against a fixed contract. Remove this brief before merge if repository policy prefers no retained PR-spec files.

> **AMENDMENT (final merge-gate audit): this brief originally specified an UNBOUNDED HOLD — "missing canonical quote never resets existing breach count," full stop. That was superseded during audit by a bounded-continuity model before merge, because an unbounded HOLD would let a valid first breach remain eligible to combine with a later valid breach across an unlimited period of unknown market truth, which is unacceptable for LIVE entry confirmation. The sections below are updated in place to reflect the actual, final, bounded model. Do not revert to unbounded HOLD based on the superseded "HOLD is neither increment nor reset, full stop" language that appeared in earlier drafts of this document — the current text below is authoritative.**

## Objective

Repair watcher liveness so an unusable/missing canonical quote **holds** existing breach continuity instead of resetting it — but only within a bounded temporal window measured from the most recent valid canonical breach observation. Beyond that window, stale partial evidence must be discarded and a later valid breach begins a brand-new confirmation streak rather than combining with expired evidence. All current safety hardening is preserved unchanged.

This is a tightly scoped semantic correction. Do not revert the watcher hardening wholesale. Do not restore LAST-price trigger authority. Do not weaken stop handling. Do not change selector, broker submission, OSM, queue, positions, or proof trades.

## Proven regression

Known-good July 29 behavior effectively skipped a ticker when no usable quote was present:

```python
quote = quotes.get(w.ticker)
if not quote:
    continue
```

That meant missing market-data truth did not mutate breach continuity.

Example known-good semantics:

```text
valid breach observation
breach_count = 1

next poll has no usable canonical quote
HOLD
breach_count remains 1

next valid breach observation
breach_count = 2
trigger confirms
```

Current watcher hardening correctly refuses to use unavailable required-side evidence, but current base/shim semantics allow that absence to reset pre-confirmation continuity.

Current failure shape:

```text
valid breach -> count 1
missing required side -> count 0
valid breach -> count 1
```

Intermittent quote gaps can therefore prevent a legitimate multi-poll confirmation indefinitely.

**Second-order regression found and fixed during audit before merge:** an unbounded HOLD (missing quote always preserves continuity, with no time limit) closes the above gap but opens a worse one — a valid first breach could remain eligible to combine with a valid second breach separated by an arbitrarily long period of unknown market truth, including the case where the delayed second poll is *itself* immediately valid with zero intermediate missing polls in between. That is not acceptable for LIVE entry confirmation, since `MOMENTUM_POLLS_REQUIRED = 2` is meant to represent two reasonably continuous observations, not "two valid observations whenever they eventually both happen to occur." See "Required invariant" below for the bounded fix.

## Primary production file

`ap_entry_watcher.py`

Potentially adjust tests and, only if necessary for the cross-file seam, `ap_entry_watcher/__init__.py`.

Do not redesign the shim/package architecture in this PR.

## Required invariant

Implement exactly:

```text
VALID BREACH OBSERVATION
=> if prior partial breach continuity exists and is FRESH
   (0 <= elapsed since last valid breach observation <= WATCHER_BREACH_CONTINUITY_MAX_GAP_SEC):
     increment breach continuity
=> if prior partial breach continuity exists and is STALE
   (elapsed > WATCHER_BREACH_CONTINUITY_MAX_GAP_SEC, elapsed < 0, or no anchor recorded):
     discard the stale partial continuity FIRST, then treat this
     observation as a brand-new first breach
=> if no prior partial breach continuity exists:
     begin a new first breach as usual

VALID CONTRADICTORY/NON-BREACH OBSERVATION
=> reset breach continuity immediately, no grace period, no bounded-gap
   consideration whatsoever

NO USABLE REQUIRED-SIDE OBSERVATION
=> if partial breach continuity exists and is FRESH: HOLD unchanged
=> if partial breach continuity exists and is STALE: discard it,
   remain PENDING with no fabricated evidence
=> if no partial breach continuity exists: remain PENDING, nothing to do
```

**HOLD is neither increment nor reset — but HOLD is bounded, not unconditional.** A missing/unusable observation preserves prior valid evidence only while that evidence remains within `WATCHER_BREACH_CONTINUITY_MAX_GAP_SEC` (45 seconds by default) of the most recent valid canonical breach observation. Elapsed time is measured from the last VALID breach observation, not from the number of missing polls processed — a single missing/delayed poll arriving after the gap has elapsed is enough to expire continuity, even with zero other missing polls in between.

**Fail-closed on backward clock movement:** the freshness check requires `elapsed >= 0` in addition to `elapsed <= MAX_GAP`. A negative elapsed value (wall-clock moved backward between observations — clock correction, VM state restore, clock sync anomaly) must be treated as STALE, never as fresh. This applies identically to both the missing-observation HOLD path and the valid-breach staleness backstop. Do not implement `elapsed <= MAX_GAP` without also requiring `elapsed >= 0` — an unbounded-below freshness check is a real, exploitable fail-open bug for a LIVE confirmation timer, not a theoretical concern.

## Canonical trigger-side authority must remain unchanged

Preserve current trigger evidence sides:

- CALL trigger evidence -> ASK
- PUT trigger evidence -> BID

Do not use LAST, MARK, MID, or opposite side as a substitute for missing canonical trigger evidence.

If the required canonical side is missing or unusable, that is still **not valid evidence**. The only change is that absence must not erase already accumulated valid evidence, and only within the bounded continuity window described above.

## Required pre-confirmation behavior

Where current logic effectively does something like:

```python
if required_quote_unavailable:
    suppress_entry_breach_evidence = True
    if not already_confirmed:
        breach_count = 0
        pending_first_breach_at = None
        breach_price = 0
```

change the pre-confirmation semantics so unusable required-side truth, **while prior partial breach continuity remains within the bounded gap**:

- does not increment breach count
- does not reset breach count
- does not clear first-breach evidence solely because truth is unavailable
- does not manufacture a trigger price
- does not call `on_trigger`
- does not advance a valid-observation timestamp as though a real canonical observation occurred

Preserve the existing state until a usable canonical observation arrives, **provided that state has not gone stale**. Once prior partial continuity exceeds the bounded gap (or the recorded anchor is missing, or the elapsed calculation is negative), discard it — see "Required invariant" above — and remain PENDING awaiting a completely fresh first breach.

## Valid contradiction must still reset

Example CALL with trigger 100:

```text
poll 1 ASK 100.10 -> breach_count 1
poll 2 no ASK -> HOLD at 1 (within bounded gap)
poll 3 ASK 99.80 -> valid contradiction -> reset to 0
poll 4 ASK 100.12 -> breach_count 1
```

Do not make breach state sticky across actual contradictory market truth. Only absence of usable truth gets HOLD semantics, and even that HOLD is bounded — see "Required invariant" above. A valid contradiction resets immediately regardless of how much or how little time has elapsed; the bounded-gap timer plays no role in the contradiction path.

## Preserve LAST-only hardening

If bid and ask are unavailable but LAST is present and crosses the trigger, LAST must not become entry authority by default.

Example:

```text
bid = None
ask = None
last = 101.00
```

For a CALL requiring ASK, this means:

```text
no usable canonical observation
=> HOLD existing continuity (bounded — see "Required invariant")
=> no increment
=> no callback
```

Do not recreate legacy `bid = ask = last` trigger authority.

**Shim/poll-loop behavior for LAST-only quotes (verified during audit — document this exact, empirically-confirmed mechanism):** the real production poll path, `APEntryWatcher._poll_active_signals()` (the shim override in `ap_entry_watcher/__init__.py`, not the base class), wraps `_fetch_quotes` so that any LAST-only quote (`bid<=0 and ask<=0 and last>0`, gated off only by the `WATCHER_ALLOW_LAST_ONLY_TRIGGER` env var, default unset/off) is replaced with an empty dict `{}` for that ticker before the base poll loop runs. The shim's own inline comment claims this "makes the legacy poll loop skip this ticker" — **that comment is stale relative to the current base `_poll_active_signals` implementation and must not be trusted at face value.** Verified directly (spying on `WatchedSignal.check()` calls through the real `_poll_active_signals()` → hardened-`_fetch_quotes` → base-loop path): the base loop does **not** skip the ticker. It defensively coerces the empty/falsy quote to `{}`, extracts `bid = quote.get("bid")` / `ask = quote.get("ask")` (both `None`), and calls `WatchedSignal.check(None, None, ...)` exactly as it would for any other missing-observation poll. Expiry is therefore **immediate/active, not lazy**: the bounded-continuity model's ordinary missing-observation branch (8A/8B/8C — see "Required invariant" above) runs on the LAST-only poll itself, HOLDing if the prior partial breach is still fresh or discarding it immediately if it has gone stale — there is no deferral to "the next poll that actually reaches `check()`," because this poll *does* reach `check()`. This is safe either way (an inferred lazy-expiry model would also have been safe per the audit's own analysis), but the test required below must assert the actually-observed mechanism, not the shim comment's inaccurate description of it.

## Preserve post-confirmation safety

This is mandatory.

Once a trigger lifecycle has already been durably confirmed, missing entry-side truth must not disable independently provable stop/lifecycle safety.

Preserve behavior where:

```text
trigger already confirmed
entry-side quote unavailable
stop-side canonical quote proves stop violated
=> stop safety may terminalize
```

Likewise:

```text
confirmed lifecycle
stop-side truth unavailable
=> HOLD/retry rather than fabricate safety truth
```

Do not blindly re-fire the entry callback because trigger state was already confirmed.

The bounded-continuity timer described above has zero authority once `trigger_crossed_at` is durable. It applies only pre-confirmation.

## Preserve retry-backoff safety

If `on_trigger` failed and the watcher is in callback retry backoff:

- do not call `on_trigger` before retry is due
- continue observing independently provable stop safety
- preserve existing ownership/retry semantics

The current safety-active retry behavior is valuable and must remain. The bounded-continuity timer must not interfere with retry-backoff timing in any way — they are independent mechanisms.

## Trigger timestamp persistence

Do not broaden this PR into a redesign of trigger timestamp persistence. The separate observed behavior where LIVE callback can be delayed if trigger timestamp persistence fails should remain visible and testable, but this PR's core fix is missing-observation continuity.

If implementation touches adjacent code, preserve fail-closed durability behavior and existing logs such as `WATCHER_TRIGGER_TIMESTAMP_PERSIST_FAILED` / retry diagnostics. Do not make timestamp write failures silently succeed.

## No durable-state inventions

Do not add a database column, table, or queue field.

Do not mutate orders, positions, proof_trades, trade_queue, or broker orders except through existing watcher callback paths after a genuine confirmed trigger.

This PR is about watcher observation semantics, not lifecycle architecture. The bounded-continuity anchor (`_last_valid_breach_observation_at`) is process-local, non-durable, in-memory state only — it must never be persisted, and a process restart naturally and intentionally loses it.

## Required tests

### 1. CALL breach -> outage -> breach (within the bounded gap)

CALL trigger 100:

```text
poll 1 ask 100.05 -> count 1
poll 2 ask unavailable, within gap -> count remains 1
poll 3 ask 100.06, within gap of poll 1 -> count 2 -> trigger confirms exactly once
```

### 2. PUT breach -> outage -> breach (within the bounded gap)

Equivalent test using canonical BID.

Expected: same bounded HOLD semantics.

### 2b. CALL/PUT breach -> outage beyond the bounded gap -> new first breach

```text
poll 1 ask 100.05 -> count 1
poll 2 (arriving after WATCHER_BREACH_CONTINUITY_MAX_GAP_SEC has elapsed since poll 1)
  ask unavailable -> stale continuity discarded -> count 0
poll 3 ask 100.06 -> NEW count 1, PENDING, no callback
poll 4 ask 100.07 within gap of poll 3 -> count 2 -> confirms
```

### 2c. Delayed valid poll cannot revive stale continuity (mandatory)

```text
poll 1 ask 100.05 -> count 1
poll 2 (arriving after the bounded gap has elapsed, with ZERO intermediate
  missing polls processed) ask 100.06, itself a valid breach
=> old partial continuity discarded FIRST
=> current observation becomes a NEW first breach: count 1, PENDING, no callback
```

This proves the bounded gap is measured by elapsed time, not by counting missing polls — the timeout cannot be bypassed simply because no missing poll happened to be processed during the gap.

### 2d. Exact boundary

`elapsed == WATCHER_BREACH_CONTINUITY_MAX_GAP_SEC` must be treated as fresh (still eligible). `elapsed` any amount greater than the max gap must be treated as stale.

### 2e. Negative elapsed (backward clock movement) fails closed

```text
poll 1 (recorded as the anchor) at simulated time T
poll 2 at simulated time T - 10 seconds (clock moved backward)
=> elapsed = -10
=> must be treated as STALE, not fresh, despite -10 <= MAX_GAP
=> stale continuity discarded; poll 2 becomes a new first breach if valid,
   or remains PENDING with continuity cleared if unusable
```

Required for both the missing-observation HOLD path and the valid-breach staleness backstop. Required for both CALL and PUT.

### 3. Valid contradiction resets

CALL:

```text
breach -> missing (within gap) -> valid ask below trigger
```

Expected: reset to 0 on the valid contradictory observation, immediately, regardless of elapsed time.

### 4. Multiple missing polls (within the bounded gap)

```text
breach_count 1
missing
missing
missing
(all within WATCHER_BREACH_CONTINUITY_MAX_GAP_SEC of the original breach)
```

Expected:

- count remains 1
- callback count remains 0
- no fabricated observation state
- the anchor stays pinned to the original first-breach observation, not advanced by the missing polls

### 5. LAST-only quote

```text
bid = None
ask = None
last crosses trigger
```

Expected via direct `WatchedSignal.check(None, None)`:

- no increment
- no reset solely due to absence (within the bounded gap)
- reset if beyond the bounded gap (stale)
- no callback
- existing continuity held (while fresh)

**Additionally required** (see shim/poll-loop test below): a second test driving the actual real production path, `APEntryWatcher._poll_active_signals()`, with a LAST-only quote, proving the shim's ticker-skip behavior for LAST-only quotes and that this lazy-expiry behavior is safe (no false confirmation results from it).

### 6. Unusable canonical values

Test required-side values that are unavailable/non-authoritative under current validity policy, including appropriate production-shaped cases for zero, NaN, infinity, malformed/non-numeric values.

Expected:

- no increment
- no reset (within the bounded gap)
- reset (beyond the bounded gap)
- no trigger
- no exception

### 7. Confirmed trigger + broken stop

Construct already-confirmed watcher. Remove entry-side quote but provide canonical stop-side evidence proving stop violation.

Expected:

- stop safety still wins
- no entry callback replay
- the bounded-continuity timer plays no role (post-confirmation)

### 8. Confirmed trigger + stop-side unavailable

Expected:

- HOLD/retry semantics
- no blind callback
- no false terminal state
- the bounded-continuity timer plays no role (post-confirmation)

### 9. Retry backoff

Make trigger callback fail and enter retry state. During backoff:

- callback does not fire early
- stop observation remains active
- ownership remains consistent with current hardening
- this must remain true even if the backoff window spans longer than the bounded-continuity gap — the two mechanisms are independent

### 10. Missing observation with no prior breach

If `breach_count = 0` and canonical quote is missing:

- count remains 0
- no fake first-breach timestamp/state is created
- no anchor is fabricated

### 11. Shim/base integration regression

Add at least one test through the **real exported watcher package path**, not only an isolated helper.

This regression arose at the semantic seam between:

- `ap_entry_watcher/__init__.py`
- `ap_entry_watcher.py`

Integration test should prove, via direct `WatchedSignal.check()` calls through the exported package path:

```text
shim receives last-only/unusable observation
=> passes non-authoritative/no-usable truth downstream
=> base watcher preserves existing breach continuity (within the bounded gap)
=> no LAST-based trigger
```

**This is necessary but not sufficient.** A separate test is additionally required that drives the actual real production poll entrypoint, `APEntryWatcher._poll_active_signals()` (the shim class, imported via the package path — not the base class directly), end to end for the LAST-only scenario, because the shim wraps `_fetch_quotes` in a way that is a materially different code path than calling `check(None, None)` directly, and a test that only exercises the latter does not actually prove what happens on the real production route. Verified during audit (see "Preserve LAST-only hardening" above): the shim's own inline comment inaccurately describes this as "the legacy poll loop skips this ticker" — empirically, `check(None, None)` **is** called on a LAST-only poll, and the bounded-continuity model's ordinary missing-observation handling applies immediately, not lazily. The required test must assert the actually-observed behavior:

```text
Short-gap case:
valid ASK breach at t=0
LAST-only quote at t=+20s (within the bounded gap)
=> HOLD: breach_count stays at 1, no callback
next valid ASK breach at t=+30s (still within gap of t=0)
=> confirms normally: breach_count=2, exactly one callback

Long-gap case:
valid ASK breach at t=0
LAST-only quote at t=+60s (beyond the bounded gap)
=> stale continuity is discarded IMMEDIATELY on this LAST-only poll
   itself (not lazily deferred to a later poll) — breach_count resets
   to 0 right here, no callback
next valid ASK breach at t=+61s
=> treated as a fresh first breach: breach_count=1, PENDING, no callback,
   trigger_crossed_at is None
```

Both cases must drive the real `_poll_active_signals()` entrypoint with a `_fetch_quotes` override returning the actual quote shapes (e.g. `{"bid": 0, "ask": 0, "last": 105.0}` for the LAST-only poll — matching the shim's `_is_last_only_quote` gate exactly: `bid<=0 and ask<=0 and last>0`), not a direct `check()` call.

This is required to prevent the same cross-file contract drift from returning later, and to prevent a future change from silently trusting the shim's stale inline comment about skip-vs-call semantics instead of the actual, verified behavior.

## Explicit non-goals

Do not modify:

- `ap_execution_core.py`
- `ap/order_state_machine.py`
- `ap/order_monitor.py`
- `ap/contract_selector.py`
- `ap/contract_quote_revalidator.py`
- `ap/brokers/tradier.py`
- `ap/queue.py`
- `client_runner.py`
- fill monitor
- exit engine
- position lifecycle
- proof trades

No threshold changes. No trade-count tuning. No broker behavior changes. No selector changes. No risk loosening. No stop relaxation. No change to `MOMENTUM_POLLS_REQUIRED` or the watcher poll interval.

## Acceptance criteria

This PR is acceptable only if all are true:

1. Missing canonical quote means HOLD **only while prior partial breach continuity remains within `WATCHER_BREACH_CONTINUITY_MAX_GAP_SEC` of the last valid breach observation** (default 45 seconds). Beyond that window, missing canonical quote means RESET.
2. Missing quote never increments breach count, at any elapsed time.
3. Missing quote never resets existing breach count **while that continuity remains fresh (elapsed, using a fail-closed `0 <= elapsed <= MAX_GAP` check, is within the bounded gap)**. Once continuity goes stale (elapsed exceeds the gap, or elapsed is negative due to backward clock movement, or no anchor was ever recorded), missing quote — and, symmetrically, the next valid breach observation encountered after continuity has gone stale — resets/discards it.
4. A valid non-breach/contradictory quote still resets immediately, unconditionally, with no bounded-gap consideration.
5. LAST-only still cannot trigger entry, whether observed via a direct `check()` call or via the real production poll loop's LAST-only ticker-skip path.
6. CALL remains ASK-authoritative.
7. PUT remains BID-authoritative.
8. Post-confirmation stop safety remains active; the bounded-continuity timer has no authority post-confirmation.
9. Retry-backoff safety remains active, independent of the bounded-continuity timer.
10. `on_trigger` fires at most once per legitimate confirmation lifecycle.
11. No LIVE/PAPER quality thresholds change. `MOMENTUM_POLLS_REQUIRED` and the watcher poll interval are unchanged.
12. No broker/position/queue/proof mutation is added.
13. The freshness/staleness elapsed-time comparison is fail-closed against backward clock movement: negative elapsed values are always treated as stale, on both the missing-observation HOLD path and the valid-breach staleness backstop.
14. The real production poll entrypoint (`APEntryWatcher._poll_active_signals()`), not only a direct `WatchedSignal.check()` call, is exercised by at least one LAST-only regression test proving the shim's ticker-skip behavior and its safe lazy-expiry consequence.

## Review checklist before merge

Review must explicitly answer:

- Does this change LIVE behavior? Yes only in watcher liveness under missing/unusable canonical observations, bounded to a 45-second continuity window; no quality/risk threshold is loosened.
- Is it flag-off or active? State the exact runtime path/default.
- Does it touch broker submit/cancel? Required answer: no.
- Does it mutate orders/positions/proof_trades/queue? Required answer: no new mutation.
- Does it preserve `client_id` / `execution_mode` through existing callback paths? Required answer: yes.
- Does it use the real production quote shape?
- Are diagnostics preserved downstream?
- Does it pollute PAPER/LIVE taxonomy? Required answer: no.
- Could it make Jason trade junk? Required answer: no; only valid canonical observations, within the bounded continuity window, can increment/confirm.
- Is the elapsed-time comparison fail-closed against backward clock movement? Required answer: yes — negative elapsed is always stale.
- Does at least one test exercise the real `APEntryWatcher._poll_active_signals()` poll loop for the LAST-only case, not only a direct `WatchedSignal.check()` call? Required answer: yes.

## Size constraint

Keep production diff small. The essential behavior change is conceptually:

```text
required canonical quote unavailable:
RESET
```

becoming:

```text
required canonical quote unavailable:
HOLD, but only while prior partial breach continuity remains within
WATCHER_BREACH_CONTINUITY_MAX_GAP_SEC of the last valid breach
observation (fail-closed against negative elapsed time); RESET once
that continuity goes stale.
```

while preserving all safety branches around it. Do not rewrite the watcher.