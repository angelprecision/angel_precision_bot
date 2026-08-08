# P0 Underlying-Authoritative Exit Geometry

> **DRAFT IMPLEMENTATION CONTRACT ONLY. DO NOT MERGE OR DEPLOY.**
>
> Amendment base: current `main` at `188f2338de3ca4b3e687aa07fd6b2c5ea4b2ab0b`
>
> This branch preserves the July 28, 2026 NOW LIVE incident and the smallest safe implementation contract. The amendment is rebased onto current `main`; it remains a draft and must be rebased again after PR #425 is complete.

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

Do not add a provider, background worker, database table, or general-purpose geometry framework.

## Entry-grace relationship

The current five-minute entry grace may defer soft loss exits. It must not:

- suppress `OPTION_CATASTROPHIC_STOP`;
- manufacture an underlying stop from option BID loss;
- reset or erase a confirmed underlying breach;
- interfere with winner protection.

## Client and mode safety

Every exit stamp and broker submission must preserve:

- `client_id`;
- `execution_mode`;
- canonical position/order identity;
- option symbol and underlying ticker;
- exact quote provenance.

No PAPER midpoint or analytics mark may gain LIVE exit authority.

## Required regression cases

1. NOW CALL, option BID approximately `-29.7%`, fresh underlying above `104.94`: HOLD; no technical stop.
2. Same position, first fresh underlying reading below `104.94`: start confirmation; no broker exit yet.
3. Same position, second fresh reading below `104.94`: one technical-stop exit submission.
4. PUT mirror geometry.
5. Missing underlying, option loss above catastrophic threshold: defer technical stop; catastrophic path evaluated separately.
6. Missing underlying, option loss below catastrophic threshold: HOLD with recovery diagnostics.
7. Entry age under five minutes, catastrophic threshold breached: catastrophic stop remains reachable.
8. Duplicate poll after exit submission: no second broker exit.
9. Stale underlying value numerically beyond stop: no technical-stop exit.
10. PAPER midpoint below threshold while executable BID is not: no LIVE-style stop authority.

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

The focused deterministic matrix is green locally (`17 passed`). The adjacent
current-main exit suites are also green (`438 passed`). These are local
pre-push results; exact-head GitHub CI remains the publication gate. This PR
stays Draft/HARD HOLD and is not authorized to merge or deploy until PR #425 is
complete and this branch has been rebased again.
