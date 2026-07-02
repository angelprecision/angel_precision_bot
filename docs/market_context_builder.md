# Market context builder

`build_market_context_for_signal(signal, data_sources=None)` creates a JSON-safe, observe-only market context payload for the position score profile stack.

## Scope

The builder copies only data already present in `signal`, `data_sources`, or nested provided context such as `context`, `market_context`, or `provided_context`. It does not retrieve, guess, synthesize, or fill missing candle data.

## Output buckets

The context contains:

- `context_version = market_context_v1`
- `ticker`
- `timeframes_available`
- `candles` for `monthly`, `weekly`, `daily`, `4h`, `2d`, `3d`, `4d`, and `5d`
- `levels` for scanner and higher-timeframe levels
- `trend` with `vwap`, `ema_stack`, and `price_above_vwap`
- `volume` with `relative_volume` and `volume_ratio`
- `sector` with sector direction fields
- `news_earnings` with `earnings_date`, `earnings_risk`, `news_risk`, and `catalyst`
- `screenshot_context`
- `missing_data`
- `warnings`
- `diagnostics.observe_only = True`
- `diagnostics.fake_data_used = False`

## Missing data and warnings

Core timeframes are `monthly`, `weekly`, `daily`, and `4h`. Missing core candles are represented as empty lists and recorded in `missing_data`.

Optional `2d`, `3d`, `4d`, and `5d` candles pass through when present but are not marked missing when absent.

Malformed candle containers, missing OHLC keys, and non-numeric OHLC values are recorded in `warnings` for diagnostics only.

## Data quality alignment

If #209A provides `ap.data_quality_context.evaluate_data_quality_context()` or `score_data_quality_context()`, the builder calls it defensively and merges returned `missing_data`, `warnings`, `bad_shape`, and `stale_data` diagnostics.

If that module is unavailable, the builder still emits compatible `missing_data`, `warnings`, and `diagnostics.fake_data_used = False` fields.

## Screenshot context

Screenshot annotations may pass through into `screenshot_context`, but screenshot context remains observe-only and is not active trading authority.
