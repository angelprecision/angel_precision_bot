# P0 SPEC — Zero/absent market evidence must never confirm an ENTRY trigger

## Status

**SPEC ONLY / HARD HOLD. DO NOT MERGE OR DEPLOY AS AN IMPLEMENTATION.**

Base: `main@f26d31cef3d3bf5d3c5d5ff16fb260f245602f89`.

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
- return the existing non-triggered state.

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

Expected production file:

- `ap_entry_watcher.py`

If another production file is needed, STOP and explain the missing contract before expanding scope.

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

Do not merge, deploy, change environment configuration, or mutate production data.