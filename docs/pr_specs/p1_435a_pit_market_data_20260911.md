# PR #435-A — Point-in-Time Market-Data Foundation

## Status

**DRAFT / HARD HOLD / FAIL-FIRST RECUT. DO NOT MERGE OR DEPLOY YET.**

Base at creation: `main@db4e3584b3319040b9e7bec519c26ce1b20a6c24`.

This PR is the first surgical recut of historical PR #435. It deliberately does **not** carry the old #435 branch forward.

## Objective

Provide one deterministic, point-in-time market-data foundation for later BREACH intelligence.

At an exact durable breach timestamp, the intelligence layer must be able to obtain or reuse only evidence that was knowable at that timestamp:

- completed 5-minute bars;
- completed 15-minute bars;
- completed 1-hour bars derived from the 15-minute series;
- completed 4-hour RTH-session-anchored bars derived from the 15-minute series;
- daily history bounded to the same as-of date;
- a frozen breach-price observation from already-present breach evidence, never a later quote.

No market-structure interpretation belongs here. #435-B owns FVG/VI/regime/pullback classification.

## Current-main defects this recut must fail-first

### 1. BREACH context uses worker-time instead of breach-time

`ap/intelligence_market_data.py::collect_point_in_time_context()` currently sets `now_utc = datetime.now(timezone.utc)` and uses current quote/history/intraday data regardless of `phase="BREACH"`.

A delayed worker can therefore evaluate an earlier breach using information observed after the breach.

### 2. BREACH currently performs current quote / market / sector reads

A BREACH snapshot must not ask a later worker for a fresh underlying, market, or sector quote and treat that later observation as breach-time evidence.

For BREACH, current quote reads are forbidden. The underlying observation must come only from already-frozen signal evidence, with explicit provenance.

### 3. 15-minute cache is ticker-only

`ap/fvg_telemetry.py::fetch_15m_bars()` currently caches only by ticker for the full TTL.

Two as-of reads that cross a newly completed 15-minute boundary can therefore share an older cached series even though the later breach requires an additional completed candle.

### 4. BREACH does not expose exact 5m/15m point-in-time candles

Current `collect_point_in_time_context()` only returns derived 1h/4h intraday context. #435-B and later #577 need the exact 5m/15m evidence as well.

## Production scope

Keep production code to these files unless a fresh audit proves another file is strictly required:

1. `ap/fvg_telemetry.py`
2. `ap/intelligence_market_data.py`

No watcher, OSM, execution-core, selector, scanner, risk, sizing, reconciler, broker order, queue, position, proof, or schema file belongs in #435-A.

## Required implementation

### A. Generalize bounded Tradier intraday reads without breaking existing callers

Inside `ap/fvg_telemetry.py`:

- preserve the existing public `fetch_15m_bars(ticker, broker, *, now=None)` signature;
- add one shared internal/public helper for bounded intraday timesales reads supporting exactly `5min` and `15min`;
- add a compatibility wrapper `fetch_5m_bars(ticker, broker, *, now=None)`;
- continue using the existing broker/data-broker session and `_resolve_base_url()` transport;
- do not add a second provider or a second HTTP client;
- preserve fail-soft behavior: market-data failure returns `[]` and never affects trading.

Tradier supports `5min` and `15min` on `/v1/markets/timesales`; no unsupported interval may be introduced.

### B. Point-in-time cache identity

For `now is not None`, cache and singleflight identity must include at least:

`symbol + interval + completed as-of bucket`.

For `now is None`, preserve the existing live/TTL behavior, but interval identity must still prevent 5m and 15m collisions.

A cached series may satisfy an as-of request only if its latest completed-bar coverage reaches the completed boundary required by that request, **or** the exact immutable PIT key was populated by a successful provider response that authoritatively returned no completed bars for that bucket. A transport/provider exception must never be marked as authoritative empty and must remain retryable.

### C. Exclude incomplete/future candles

When an as-of timestamp is supplied:

- only timezone-aware timestamps are authoritative;
- a candle is usable only when `bar_open + interval <= as_of`;
- malformed/naive candle timestamps are ignored;
- bars after the as-of timestamp never influence derived data;
- the current in-progress 5m/15m candle is excluded;
- derived 1h/4h candles are emitted only when every expected 15m constituent slot for the completed RTH bucket exists exactly once; a missing, duplicate, or misaligned constituent makes that derived bucket unavailable rather than synthesizing partial OHLC.

### D. BREACH as-of authority in `collect_point_in_time_context`

For `phase == "BREACH"`:

1. Parse `signal.trigger_crossed_at` as the sole as-of timestamp.
2. It must be timezone-aware. Missing/malformed/naive authority fails closed for BREACH evidence.
3. Set `evidence_now = trigger_crossed_at`.
4. Fetch daily history bounded to `evidence_now`.
5. Fetch 15m and 5m timesales with `now=evidence_now`.
6. If contemporaneously frozen `candles_15m` / `candles_5m` already exist in the signal, they may be used as fail-soft fallback only after filtering to the same as-of boundary.
7. Return completed `5m`, `15m`, derived `1h`, and derived `4h` candles under `data_sources.candles`.
8. Return an explicit `as_of` field equal to the breach timestamp.
9. Never call the current quote, market quote, or sector quote readers for BREACH.
10. Underlying BREACH observation may use only already-frozen signal evidence such as `breach_price` / frozen underlying price and must identify that source; no fresh quote may substitute. Generic current or signal-time prices cannot satisfy BREACH observation authority and cannot be used as fallback evidence.

The temporal boundary is binding:

- `trigger_crossed_at` is the immutable event time and remains unchanged for the life of the signal.
- For the initial BREACH snapshot, `data_as_of` is exactly `trigger_crossed_at`; the current result's `as_of` is that exact trigger-time cutoff.
- That snapshot is a forensic PIT materialization primitive. It cannot establish post-trigger 5m/15m completion or acceptance evidence; a candle closing after the trigger is correctly absent.
- A later confirmation snapshot, owned by a future recut such as #615, must retain the original `trigger_crossed_at` separately and use an explicit later `data_as_of` / `confirmation_as_of`. It must never rewrite the event time or silently substitute worker time.
- For timestamped scalar breach evidence, `source_timestamp` identifies the source observation while `observed_at` identifies the snapshot observation time; they may differ by design.

This collector is not the future synchronous breach-entry gate. Future wiring must follow `breach -> cached structure -> latest completed 5m/15m evidence -> classification -> selector` and consume precomputed/cached structural context rather than synchronously invoking `_collect_breach_context()` to download history and rebuild all context. Precompute, watcher, and acceptance policy are outside #614. Missing or unknown market evidence is observe-only state and is not automatic trade rejection.

For `PRETRIGGER` and `PREOPEN`, preserve current behavior. Do not add the 5m read to those paths in this PR.

### E. Daily-history as-of bound

Allow `_history()` to accept an optional as-of datetime. BREACH history requests must use that date as their `end` rather than worker-time `datetime.now()`.

## Required fail-first / regression proof

Create `tests/test_p0_435a_pit_market_data.py` and register it in the canonical P0 inventory.

Minimum matrix:

1. BREACH with a durable timestamp does not call current underlying/market/sector quote readers.
2. BREACH passes the exact parsed timestamp into both 5m and 15m historical reads.
3. Missing/malformed/naive `trigger_crossed_at` causes a fail-closed BREACH context and zero broker/market-data reads.
4. 5m candle whose close boundary is after `as_of` is excluded.
5. 15m candle whose close boundary is after `as_of` is excluded.
6. Derived 1h contains only fully completed 15m input buckets and is omitted when any expected constituent is missing.
7. Derived 4h contains only fully completed RTH-session-anchored input buckets and is omitted when any expected constituent is missing.
8. Two as-of reads crossing a completed 15m boundary cannot share stale cached coverage.
9. 5m and 15m cache/singleflight keys cannot collide.
10. Same ticker + same live interval still coalesces under existing TTL behavior.
11. PRETRIGGER/PREOPEN current-quote behavior remains unchanged.
12. Any market-data exception remains fail-soft and cannot raise into trading.
13. No broker submit/cancel/order/position/proof/queue mutation is reachable from the new foundation.
14. Two reads of the same exact PIT bucket reuse a successful authoritative empty snapshot instead of refetching.
15. A provider/transport failure returning no data is not cached as authoritative empty and is retried on the next request.

## Required fail-first evidence

Before implementation, show at least these failures against current base `db4e3584...`:

- a delayed BREACH worker performs current quote access;
- a later as-of request within TTL can reuse an earlier ticker-only 15m cache;
- BREACH output lacks exact `5m` / `15m` candle sets and an explicit breach `as_of`.

Do not weaken the fail-first test after implementation.

## Safety invariants

- Observe-only only.
- Zero change to trade eligibility.
- Zero change to watcher timing.
- Zero selector/ranking/sizing/risk change.
- Zero broker POST/cancel/replace authority.
- Zero order/position/proof/queue mutation.
- No LIVE/PAPER identity changes.
- No schema/migration changes.
- Market-data failure degrades to missing evidence, never a trading failure.

## Explicit non-scope

Do **not** include:

- FVG detection/classification;
- VI classification;
- regime/pullback/setup archetype;
- touch lineage;
- entry-readiness policy;
- WAIT/REARM/READY decisions;
- intelligence snapshot persistence changes;
- runtime BREACH handoff/watcher integration;
- selected-contract evidence (#437);
- outcome/path tracking (#577).

Those belong to later recuts.

## Merge gate

Remain HARD HOLD until:

- fail-first is demonstrated on the exact current base;
- focused tests pass on an unchanged head;
- exact-head P0 passes;
- merge-ref P0 passes against then-current main;
- diff audit confirms production scope is still only the intended market-data foundation;
- no new money-path authority is present.