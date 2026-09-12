# P1 Intraday Forming-Bar State

## Status
SPEC ONLY / DRAFT / HARD HOLD. No implementation, merge, or deployment authority.

Base: `main@846525a2ee798071bb0079078f0aec871c415677`.

## Purpose
Maintain deterministic in-session OHLC state for a deliberately small, highly liquid universe (initial examples: SPY, QQQ, IWM, AAPL, GOOGL, NVDA, MSFT) so setups that develop after the open can be detected without rebuilding full history on every evaluation.

This PR owns bar state only. It does not know what a 2-3-2, FVG break, pullback, reclaim, breakout, or entry is.

## Required behavior
- Session anchor: US equity RTH, America/New_York, 09:30 ET.
- Maintain current forming bars for 5m, 15m, 30m, and 60m plus completed-bar history required by downstream pure classifiers.
- 60m buckets are session anchored from 09:30 ET, not wall-clock 10:00/11:00 buckets.
- Update forming OHLC incrementally from the smallest trustworthy live market-data unit available; do not refetch 15 days of history on every tick/minute.
- Finalize a bar exactly once when its bucket closes, then start the next forming bucket.
- Preserve `open`, `high`, `low`, `close/last`, bucket start/end, source timestamp, symbol, session date, and data-quality status.
- Out-of-order, duplicate, stale, future, malformed, or cross-symbol observations must not corrupt a completed bar.
- Restart must reconstruct the same completed/forming state from canonical history plus bounded current-session evidence.
- No synthetic future close. A forming bar is explicitly `FORMING`; a finalized bar is explicitly `COMPLETED`.
- Missing market data is `UNKNOWN/MISSING`, never an invented flat candle.

## Performance contract
For the bounded liquid universe, one market-data observation may update multiple timeframe states in-memory, but must not issue one historical network fetch per timeframe/setup. Reuse #614-style normalized point-in-time/historical evidence where appropriate.

## Explicit non-scope
No setup detection, FVG/VI interpretation, signal creation, watcher creation, selector, sizing, risk, broker submit/cancel/replace, order/position/proof/queue mutation, LIVE/PAPER routing, or admission policy.

## Mandatory tests
1. 09:30 ET starts deterministic 5m/15m/30m/60m forming bars.
2. High/low update monotonically and close follows latest accepted observation.
3. 60m 09:30-10:30 finalizes once and 10:30-11:30 starts cleanly.
4. Same for 5m/15m/30m boundaries.
5. Duplicate observation is idempotent.
6. Out-of-order/stale observation cannot rewrite finalized truth.
7. Future observation rejected.
8. Wrong ticker cannot leak state.
9. Restart mid-bucket reconstructs the same OHLC.
10. Missing transport degrades state without emitting setup/trade authority.
11. Multi-ticker state isolation.
12. Zero broker/order/position/proof/queue calls.

## Merge gate
Implementation must remain narrowly scoped to bar-state ownership and tests. Exact-head and merge-ref P0/cohesion must be green on one unchanged SHA. Any setup or money-path authority appearing in this PR is a HARD HOLD.
