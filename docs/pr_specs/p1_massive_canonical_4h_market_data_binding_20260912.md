# P1 — Massive Canonical 4H Market-Data Authority

## Status

**DRAFT / HARD HOLD / PREFLIGHT FIRST. DO NOT MERGE OR DEPLOY PRODUCTION AUTHORITY UNTIL THE ACCESS + ALIGNMENT GATES PASS.**

Base at creation: `main@71f25dabf739bce3360593ac44a7c7bf71ef9513`.

## Architectural decision

Angel Precision will not use TradingView as a production market-data backend.

Canonical source split:

- **Massive (formerly Polygon): canonical higher-timeframe stock structure source, beginning with 4H bars.**
- **Tradier: execution plus the existing completed 5m/15m confirmation path.**
- **TradingView: human/reference geometry validation only.**

The purpose of this PR is to prove and normalize Massive 4H data. It has zero trading authority.

## Stage 0 — mandatory credential/access preflight before production implementation

Do not write the production adapter until this gate is satisfied.

1. Use an environment-only credential. Preferred canonical name: `MASSIVE_API_KEY`. A compatibility alias `POLYGON_API_KEY` may be accepted temporarily, but production code must never contain a literal key or fallback credential.
2. Any credential previously committed to source code is considered exposed and MUST NOT be reused for this authority. Rotate/revoke it outside this PR.
3. Execute a read-only authenticated smoke request against Massive stock custom aggregates:
   `/v2/aggs/ticker/{ticker}/range/4/hour/{from}/{to}`.
4. Record only sanitized proof in the PR: HTTP success/failure class, provider `status`, ticker, result count, first/last bar timestamps, schema keys, request duration, and plan/rate-limit diagnostics if available. Never log or commit the API key or auth header.
5. Prove at least SPY, QQQ, NVDA, MSFT and one less-liquid-but-approved AP underlying return parseable aggregate data.
6. A 401/403, unsupported endpoint, plan restriction, recurring 429, malformed response, or zero usable bars keeps this PR on HARD HOLD until explained.

Passing tests with mocked HTTP is not a substitute for this one-time authenticated provider preflight.

## Provider endpoint contract

Preferred native request:

`GET /v2/aggs/ticker/{stocksTicker}/range/4/hour/{from}/{to}`

Required query semantics:

- `adjusted=true` unless an independently reviewed split-adjustment policy changes this;
- `sort=asc`;
- sufficient limit/date span to cover the structure lookback;
- no client/account-specific parameterization.

Normalize provider output into an AP-owned immutable bar type with at least:

- `ticker`;
- `timeframe = 4h`;
- `open`;
- `high`;
- `low`;
- `close`;
- `volume`;
- `vwap` when provider supplies it;
- `transactions` when provider supplies it;
- `started_at` from provider timestamp;
- `completed_at` derived only from the accepted canonical alignment contract;
- `source = massive`;
- `adjusted`;
- provider/schema version;
- deterministic source-bar identity/hash.

Reject nonfinite OHLC, impossible OHLC geometry, missing/ambiguous timestamps, duplicate-conflicting bars, and unordered data rather than silently repairing them.

## Stage 1 — mandatory 4H boundary/alignment proof

Massive supports custom `4/hour` bars, but custom stock aggregates can cover premarket, regular hours and after-hours. Therefore native `4/hour` output is **not automatically Angel Precision's canonical chart semantics** merely because the endpoint name says four hours.

Before calling native bars canonical:

1. Collect at least 20 known 4H reference examples from the chart semantics Aamiyah actually uses. The references are validation fixtures only, never runtime dependencies.
2. Include multiple tickers and dates, ideally SPY/QQQ plus several single names used by AP.
3. Compare exact provider bar `started_at`, expected completion boundary, O/H/L/C and resulting FVG geometry.
4. Include ordinary sessions plus available DST/early-close/holiday-adjacent cases.
5. Record all differences. Do not hide them behind an undocumented tolerance.
6. Explicitly choose and version the accepted AP 4H alignment.

### Alignment decision

**Preferred path:** native Massive `4/hour` bars if their boundaries match the chosen AP/TradingView reference semantics sufficiently for exact structure identity.

**Fallback path if native boundaries do not match:** Massive remains the canonical provider, but build the accepted AP 4H bars deterministically from smaller Massive aggregates (for example 1h/30m/minute bars) using an explicit session calendar/alignment rule.

Do **not** fall back to the current Tradier-15m-derived pseudo-4H implementation simply because Massive native boundaries differ.

The chosen alignment must have a stable version such as `massive_4h_alignment_v1` and every downstream map/snapshot must retain it.

## Completion and session semantics

Only completed canonical 4H bars may enter the durable structure map.

The adapter must explicitly define:

- timezone normalization;
- DST behavior;
- regular-hours vs extended-hours inclusion;
- early-close behavior;
- holiday/no-session behavior;
- how the final partial regular-session bucket is treated if the accepted alignment uses RTH anchoring;
- split adjustment;
- missing intervals/no-trade intervals;
- latest completed bar at a supplied `as_of`.

Do not infer a completed bar from wall-clock time alone when provider/session evidence disagrees.

## Lookback

The current Tradier FVG telemetry sees roughly ~10 sessions due to a 15-calendar-day fetch. This new source must support a materially deeper configurable higher-timeframe lookback so older active 4H structures are not invisible merely because they predate the latest two weeks.

Initial implementation should expose a bounded configurable history window suitable for replay and active FVG/gap tracking. Do not hard-code a tiny lookback into classifier logic.

## Efficiency / cache contract

Fetch by ticker, not by client.

Five clients trading NVDA must share the same canonical NVDA 4H dataset.

Expected ownership:

`unique ticker -> provider fetch/refresh -> normalized completed 4H bars -> shared cache/persistence`

Requirements:

- one bounded refresh per ticker/generation;
- singleflight/deduplication for concurrent refresh requests;
- deterministic cache key including ticker, source/alignment version and data boundary;
- freshness metadata;
- last-good immutable snapshot retained separately from refresh failure state;
- provider outage does not delete or rewrite last-good history;
- no synchronous network call in watcher/selector/broker hot paths;
- no per-client duplicate provider fetch.

Premarket/scanner preparation is the primary v1 refresh point. Intraday refresh may be added separately when a newly completed canonical 4H bar requires it, but never as an entry-time blocking request.

## Failure semantics

Provider/auth/rate-limit/network/schema/calendar/alignment failure means **HTF data unavailable/stale**, not adverse trading truth.

This PR has no authority to:

- reject a signal;
- create WAIT;
- terminalize a watcher;
- suppress selector/broker;
- mutate orders/positions/proof;
- fabricate a current 4H bar;
- silently substitute Tradier as canonical 4H authority.

Expose explicit status/reason/freshness fields.

## Security requirements

- No API keys in source, fixtures, docs, logs, exception text, screenshots or PR bodies.
- No hard-coded fallback key.
- Environment-only secret injection.
- Sanitize URLs if query-string auth is ever used.
- Unit tests use fake credentials/transports.
- Live provider preflight logs only sanitized metadata.

## Required tests

At minimum:

1. normalized valid native 4/hour response;
2. ascending ordering and deterministic identity;
3. completed vs forming exclusion;
4. missing bar interval;
5. duplicate identical bar;
6. duplicate conflicting bar;
7. malformed/nonfinite OHLC;
8. provider empty response;
9. 401/403 auth failure;
10. 429 rate limit;
11. 5xx/network timeout;
12. environment credential missing;
13. secret never appears in returned diagnostics/loggable error;
14. split-adjusted semantics preserved;
15. DST transition;
16. early close;
17. holiday/no-session;
18. extended-hours contamination check;
19. native alignment comparison fixtures;
20. deterministic smaller-bar fallback aggregation if native alignment is rejected;
21. same ticker multi-client requests share one refresh;
22. different tickers remain isolated;
23. restart/cache reload preserves exact source identity;
24. stale last-good dataset remains readable with stale flag after refresh failure;
25. zero watcher/selector/broker/order/position/proof mutation.

## Explicit non-scope

No FVG/gap/imbalance calculation. No 4H structure map. No candidate relation. No #621 persistence changes. No #622 runtime bridge. No #627 classifier. No #628 promotion. No execution changes.

## Downstream contract

This PR should expose one narrow read/refresh interface that the next 4H Structure Map PR consumes. Downstream code must not know Massive HTTP response details.

Proposed conceptual boundary:

`get_canonical_4h_bars(ticker, as_of=...) -> CanonicalHTFBarSet`

Exact naming may differ, but transport normalization must stay here.

## Merge gate

HARD HOLD until all are true:

- clean rotated/environment-only credential exists;
- authenticated Massive smoke request passes;
- native 4/hour alignment decision is documented from 20 reference cases;
- normalized source contract is deterministic;
- no secret exposure;
- focused tests green;
- exact-head P0/cohesion green where enrolled;
- genuine merge-ref parity;
- `git diff --check`;
- independent audit clears one unchanged final SHA.
