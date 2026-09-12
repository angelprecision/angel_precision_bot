# PR #617 — Implementation Contract and Authority Trace

## Scope

`ap/intraday_bar_state.py` owns only deterministic market bar state for the
bounded universe `SPY`, `QQQ`, `IWM`, `AAPL`, `GOOGL`, `NVDA`, and `MSFT` across
`5m`, `15m`, `30m`, and `60m` regular-session timeframes.

It has no runtime wiring, network transport, database table, setup classifier,
candidate scanner, watcher, selector, risk/sizing, broker, order, position,
proof, or queue authority.

## Current-main trace

The current committed main was inspected before selecting the seam:

| Existing component | Evidence | #617 decision |
| --- | --- | --- |
| `ap/fvg_telemetry.py` | Tradier `/v1/markets/timesales`, `5min`/`15min`, `session_filter=open`, normalized source timestamps, TTL and single-flight caching | Reuse only as a supplied seed-shape authority; do not wire it into this state engine because its owner is observe-only FVG telemetry and it persists signal diagnostics |
| `ap/daily_continuation_validator.py` | Direct same-day Tradier timesales fetch for continuation validation | Do not add a second caller or route validation transport into #617 |
| `ap/overnight_daily_validator.py` | Quote plus timesales reads for overnight validation | Do not reuse its validator-specific runtime path |
| `ap_entry_watcher.py` | Batched Tradier quote polling through the watcher execution path | Do not reuse: quote polling is execution-owned and does not provide a trustworthy source event timestamp contract |
| `ap/position_quote_monitor.py` | Shared cached account/position quote polling | Do not reuse: account/exit-owned and not a bar-state observation authority |
| `ap.flatline_alarm.is_trading_day` | Existing canonical full-closure/weekend/extra-holiday helper | Default calendar delegates full-session truth here |
| Existing calendar helpers | No current-main early-close/session-close authority was found | Default calendar carries explicit NYSE early-close exceptions and accepts an injected richer session authority |
| `ap.scanner_utils.TICKERS` | Broad scanner universe | Not reused; #617 keeps its exact seven-ticker universe |

There is no safe existing live producer that supplies exact source event time
without crossing a watcher/validation or account-data boundary. Therefore the
implementation exposes source-agnostic ingestion and one-shot seed APIs only.
Actual runtime subscription/wiring remains a separate PR.

## Input and volume contract

`IntradayBarState.ingest()` accepts a mapping or `MarketObservation`:

- `ticker`/`symbol`;
- aware `timestamp`, `source_timestamp`, or `time`;
- either a positive finite `price`/`last` value or a complete valid OHLC;
- optional `timeframe`/`timeframes` target restriction;
- optional `source_observation_id`, `source_identity`, and `source_version`;
- optional non-negative `volume` only when `volume_kind="incremental"`.

Volume is therefore an incremental contribution associated with one unique
source observation. It is summed once after duplicate identity checks. Missing
volume remains `None`; cumulative volume is rejected rather than guessed.

For a supplied OHLC observation, the exact merge is: first `open`, maximum
`high`, minimum `low`, newest accepted `close`, and sum of unique incremental
volume contributions.

## Lifecycle and safety contract

- Bucket placement uses the source timestamp, never worker wall-clock time.
- Buckets are half-open: `[bucket_start, bucket_end)`.
- The RTH open is 09:30 ET; each timeframe is anchored from that open.
- The final bucket is clipped to the authoritative session close.
- A forming bar is created only by an accepted observation.
- A forming bar is frozen exactly once at its boundary or explicit `advance()`.
- Completed bars are immutable; late observations return
  `LATE_COMPLETED_BUCKET`.
- Exact source identity replays return `DUPLICATE`; a reused source ID with a
  different payload returns `CONFLICTING_DUPLICATE`.
- Without a source ID, the canonical identity includes ticker, source time,
  OHLC/price, volume, provenance, and target timeframes. Equal prices at
  different source times are distinct observations.
- A backward source timestamp in the same forming bucket returns
  `OUT_OF_ORDER_FORMING` and does not mutate state.
- An explicit `as_of` rejects a source timestamp later than that cutoff as a
  future observation. No implicit current-time decision is made.
- `seed_once()` invokes a supplied iterable/callback at most once. Incremental
  ingestion never asks the seed source for history.
- `snapshot()`/`from_snapshot()` provide bounded reconstruction without a new
  database table. The snapshot includes completed bars, the current forming
  bars, and duplicate identities needed for idempotent restart replay.

## Verification

`tests/test_p1_intraday_forming_bar_state.py` exercises the required basic,
boundary, early-close, malformed-input, duplicate, late/out-of-order,
restart, multi-ticker, seed-call-count, input-immutability, no-classifier, and
no-money-path cases. The test imports the production module directly so the
focused suite does not initialize the repository's unrelated database-backed
package guards.
