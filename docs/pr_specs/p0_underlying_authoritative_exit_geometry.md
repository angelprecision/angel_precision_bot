# P0 Underlying-Authoritative Exit Geometry

> **DRAFT IMPLEMENTATION CONTRACT ONLY. DO NOT MERGE OR DEPLOY.**
>
> Post-#425 amendment base: current `main` at `187fb30e43a98de027bb6a05a70e08fdff6ff5d6`
>
> This branch preserves the July 28, 2026 NOW LIVE incident and the smallest safe implementation contract. It is rebased onto the final merged #425 lifecycle and remains a draft/HARD HOLD.

## Incident

Jason's LIVE account entered:

- ticker: `NOW`
- side: `CALL`
- contract: `NOW260731C00113000`
- entry premium: `$1.82`
- trigger underlying: `108.15`
- stored underlying stop: `104.94`
- stored target: `111.36`
- realized database result: `-$59`, `-32.42%`

The exit decision stream showed:

1. repeated normal exit evaluations;
2. five-minute post-entry grace suppressing loss exits;
3. `STOP_BREACH_STARTED` near `-29.7%` executable-BID loss;
4. hard-stop confirmation reporting `no_underlying_data`;
5. the underlying subsequently moving materially in the CALL direction.

The failure is not simply that a stopped trade later recovered. The safety defect is that option-premium loss acquired thesis-invalidating authority while fresh underlying truth was unavailable.

## Required invariant

A stored technical underlying stop is authoritative for ordinary thesis invalidation.

- CALL: technical stop may fire only after fresh underlying truth confirms `underlying_price <= stop_underlying`.
- PUT: technical stop may fire only after fresh underlying truth confirms `underlying_price >= stop_underlying`.
- Option BID drawdown alone must not be labeled or treated as a technical underlying stop.
- The percentage hard stop remains a distinct catastrophic account-protection path.
- Missing/stale underlying data must never be represented as a confirmed underlying breach.

## Scope

Target implementation files are expected to be limited to:

1. `ap_exit_engine.py`
2. `ap/position_quote_monitor.py` only if required to provide fresh underlying truth already consumed by the engine
3. one focused regression test file

The final exact-head verification also makes the existing soft-exit truth
fixture clock non-future at CI start time and keeps its current-date 0DTE
fixtures intact. That is test-only; it adds no production path or lifecycle.

Do not touch:

- scanner scoring;
- intelligence or regime admission;
- queue or watcher ownership;
- selector thresholds;
- broker entry submission;
- position sizing;
- order cancellation policy;
- proof-trades semantics unrelated to the exit stamp.

## Required decision taxonomy

The implementation must keep these cases distinct:

### `UNDERLYING_TECHNICAL_STOP_CONFIRMED`

Requirements:

- fresh underlying price;
- valid `stop_underlying`;
- side-aware breach geometry;
- confirmation policy satisfied;
- diagnostics include price, stop, side, quote timestamp, age, source, and confirmation count.

### `UNDERLYING_STOP_DEFERRED_DATA_UNAVAILABLE`

Requirements:

- technical stop cannot be confirmed because underlying truth is missing, invalid, or stale;
- no false technical-stop classification;
- quote recovery remains eligible;
- the catastrophic option stop is evaluated independently.

### `OPTION_CATASTROPHIC_STOP`

Requirements:

- uses fresh executable BID or an already-approved provenance-aware hard-exit reference;
- remains reachable during entry grace;
- has its own explicit reason code;
- must never claim the underlying crossed its stored stop;
- logs the option loss threshold and the exact price authority used.

## Confirmation policy

The implementation should reuse existing state where possible and avoid a new subsystem.

Acceptable confirmation:

- two fresh consecutive underlying observations beyond the stop, separated by the normal polling interval; or
- an existing closed-bar confirmation source already available in the runtime.

The first breach time remains the start of the hysteresis window, while the
stored latest breached-observation timestamp advances whenever a newer quote is
seen. Once the wall-clock window has elapsed, the current quote must be newer
than that stored marker; repeated evaluations of the same quote cannot mature a
technical stop.

Do not add a provider, background worker, database table, or general-purpose geometry framework.

## Entry-grace relationship

The current five-minute entry grace may defer soft loss exits. It must not:

- suppress `OPTION_CATASTROPHIC_STOP`;
- manufacture an underlying stop from option BID loss;
- reset or erase a confirmed underlying breach;
- interfere with winner protection.

The first fresh technical-stop breach is remembered for confirmation, but it
does not preempt existing winner protection. Touched-profit, profit-floor /
giveback, scale-out, runner-trail, and small-win branches retain priority
**during the CONFIRMING window only**. If none of those branches acts, the
engine returns `UNDERLYING_STOP_CONFIRMING` as a HOLD.

**Once the technical stop matures to CONFIRMED** (two independent fresh
underlying observations beyond the stop), it is evaluated ahead of every
winner-protection branch and fires immediately, without deferring to
touched-profit, profit-floor, scale-out, runner-trail, or small-win state.
This is a deliberate design decision, not an oversight: this system's edge
is structural — entries, stops, and targets are set off underlying price
action (The Strat), not off unrealized option P&L. Profit-floor logic exists
to protect gains while the underlying thesis is still intact; it is not a
competing thesis authority. Once the underlying itself has confirmed that
the stop level was crossed, the thesis is proven dead, and that signal must
not be overridden by a downstream P&L heuristic — the same principle this
PR enforces in the other direction (option BID drawdown alone must not
manufacture a technical stop). Regression coverage for this exact ordering:
`test_confirmed_technical_stop_overrides_touched_profit_by_thesis_design`.

## Client and mode safety

Every exit stamp and broker submission must preserve:

- `client_id`;
- `execution_mode`;
- canonical position/order identity;
- option symbol and underlying ticker;
- exact quote provenance.

Technical-stop identity accepts only the exact durable `live` or `paper`
execution-mode values. Uppercase or whitespace-variant values are ambiguous and
must return `UNDERLYING_STOP_IDENTITY_UNPROVEN`.

No PAPER midpoint or analytics mark may gain LIVE exit authority.

## Required regression cases

1. NOW CALL, option BID approximately `-29.7%`, fresh underlying above `104.94`: HOLD; no technical stop.
2. Same position, first fresh underlying reading below `104.94`: start confirmation; no broker exit yet.
3. Same position, second fresh reading below `104.94`: one technical-stop exit submission through the real `_submit_exit_decision()` handoff; a repeated poll is idempotent.
4. PUT mirror geometry.
5. Missing underlying, option loss above catastrophic threshold: defer technical stop; catastrophic path evaluated separately.
6. Missing underlying, option loss below catastrophic threshold: HOLD with recovery diagnostics.
7. Entry age under five minutes, catastrophic threshold breached: catastrophic stop remains reachable.
8. Duplicate poll after exit submission: no second broker exit.
9. Stale underlying value numerically beyond stop: no technical-stop exit.
10. PAPER midpoint below threshold while executable BID is not: no LIVE-style stop authority.
11. New breached quote before the horizon advances the latest-observation marker; repeated evaluation of that same quote through the horizon remains HOLD; a newer post-horizon quote may confirm.
12. Process death before technical-stop confirmation clears the in-memory timer; restart requires a fresh post-restart confirmation sequence.
13. Exact mode authority: `live`/`paper` may proceed under their identity contracts; uppercase, whitespace, empty, NULL, and malformed modes return `UNDERLYING_STOP_IDENTITY_UNPROVEN`.
14. Post-#425 guarded submit/adoption: one broker POST, one broker-owned adoption, zero cancels, unchanged pre-fill position/proof state, and restart blocked from resubmitting.

## Acceptance evidence

Before this PR can become mergeable, the final PR description must contain:

- previous and new exact head SHA;
- exact changed-file list;
- focused test command and result;
- adjacent exit regression result;
- replay of the July 28 NOW geometry;
- explicit confirmation that broker submit/cancel, queue, selector, scoring, positions, and proof-trades were not broadened beyond the intended exit transition;
- exact-head CI status.

## Current status

The production-path amendment is limited to `ap_exit_engine.py` plus this contract
and its focused regression matrix. It reuses the existing QPM truth fields,
confirmation timer, hard-exit resolver, entry-grace ordering, and exit-in-flight
submission fence; no broker, queue, selector, scoring, sizing, or proof-trade
architecture was added.

The final #425 base is `187fb30e43a98de027bb6a05a70e08fdff6ff5d6`; the rebase
replayed only the existing #403 suffix, was conflict-free, and required no
production conflict-resolution edits. Cumulative changed files are:
`ap_exit_engine.py`,
`docs/pr_specs/p0_underlying_authoritative_exit_geometry.md`,
`tests/test_p0_underlying_authoritative_exit_geometry.py`, and the existing
`tests/test_soft_exit_executable_truth.py` fixture-only clock correction. No
`ap_exit_engine.py` production line changed in this amendment — it closes a
documentation/test coverage gap only.

The focused deterministic matrix is green locally (`32 passed`), including the
fresh-observation-at-horizon regression, exact execution-mode authority cases,
first-breach winner-protection regression, confirmed technical-stop
submit/idempotency handoff, process-death reconfirmation, post-#425
broker-owned adoption, and — added in this amendment —
`test_confirmed_technical_stop_overrides_touched_profit_by_thesis_design`,
which locks in that a matured (CONFIRMED) technical stop fires ahead of
touched-profit/profit-floor/scale-out/runner-trail/small-win protection by
design (see "Entry-grace relationship" above for the full reasoning). The
directly affected #425 lifecycle suites are green (`399 passed, 2 skipped` in
`317.36s`). The confirmed-stop integration regression records exactly one
broker POST, one broker-owned adoption, zero cancels, unchanged pre-fill
quantity/closed/proof state, and no restart resubmission. The deterministic
NOW replay remains HOLD with no technical stop.

This PR stays Draft/HARD HOLD and is not authorized to merge, deploy, approve,
or mark Ready. Exact head SHA and CI run for this amendment must be recorded
in the PR description at time of push, not hardcoded here, so this doc does
not go stale again.
