# Angel Precision — Intraday Discovery Stack Map (2026-09-12)

## Goal

Continuously monitor a bounded highly liquid ticker universe during RTH and surface developing intraday setups without contaminating the existing scanner, watcher, selector, broker, or intelligence-policy paths.

Initial bounded universe examples:
- SPY
- QQQ
- IWM
- AAPL
- GOOGL
- NVDA
- MSFT

The universe is intentionally small and liquid. Expansion is a separate reviewed decision.

Target opportunity volume such as 5–10 setups/day is a discovery objective, not a quota and not permission to lower setup quality.

## Architecture

`market updates -> #617 forming-bar state -> #618 pure setup classifier -> #619 candidate scanner -> future promotion seam`

No PR in this stack may silently jump directly into watcher or broker authority.

## #617 — deterministic forming-bar state

Owns only:
- incremental RTH OHLC state;
- 5m / 15m / 30m / 60m session-anchored buckets;
- deterministic bar roll/finalization;
- completed-vs-forming distinction;
- bounded ticker/timeframe state;
- restart reconstruction from exact accepted market-data inputs.

Does not own:
- pattern/setup classification;
- FVG/VI policy;
- scanner orchestration;
- candidate persistence into current trading pipeline;
- watcher/selector/broker/risk/sizing/orders/positions/proof/queue.

Performance rule:
Do not refetch/rebuild full history for every tick/minute/timeframe. Seed once, then update incrementally.

## #618 — pure setup classifier

Owns only deterministic functions over bar state.

Initial STRAT primitives:
- `1` inside;
- `2U` high break only;
- `2D` low break only;
- `3` both sides.

Initial setup target:
- developing 2-3-2;
- completed 2-3-2.

Classifier must be pure:
- no DB;
- no network;
- no broker;
- no clock reads beyond supplied inputs;
- no global state;
- no side effects.

A forming candle may move state during its life:
`1 -> 2U -> 3`, etc. Once both sides are broken the current bar is `3`, not still a `2`.

## #619 — bounded candidate scanner

Owns only orchestration and deterministic candidate identity/deduplication.

Inputs:
- canonical #617 bar state;
- canonical #618 classifier.

Output:
- observe-only intraday setup candidate.

Candidate identity must include enough information to avoid duplicates and cross-session confusion, e.g. ticker + timeframe + setup family + direction + bar/session identity.

Required behaviors:
- reevaluate on accepted bar-state updates or bounded minute cadence;
- emit once per canonical candidate generation;
- invalidate/update when forming state changes and no longer matches;
- preserve opposite-side/same-ticker independence;
- keep current session separate from another session/day;
- no full-history refetch per setup evaluation.

Does not own:
- bar math;
- setup math;
- FVG/VI intelligence;
- watcher install;
- selector;
- risk/sizing;
- broker;
- orders;
- positions;
- proof;
- trade queue.

## Future promotion seam

After #617/#618/#619 are independently proven, a separate future PR may map an observe-only intraday candidate into the canonical Angel Precision signal/watcher pipeline.

That future PR must preserve:
- exact client/execution-mode identity;
- candidate generation identity;
- duplicate-submit fences;
- existing selector/risk/sizing authority;
- pre-fill position/proof invariants;
- intelligence admission rules then current on main.

It must not be smuggled into #619.
