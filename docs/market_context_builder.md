# Market context builder

`ap.market_context_builder.build_market_context_for_signal()` creates the observe-only market context payload consumed by the position score profile stack.

## Safety boundary

This module is diagnostic only.

It does **not**:

- call broker APIs
- submit or cancel orders
- read or mutate queue, orders, positions, or proof-trades tables
- change `client_id`, `execution_mode`, `signal["score"]`, `plan.score`, or any live/paper taxonomy
- fetch unavailable data
- guess, infer, synthesize, or fake missing candle data
- make screenshot annotations active trading authority

Missing candles are represented as empty lists and recorded in `missing_data` for core timeframes.

## Public API

```python
from ap.market_context_builder import build_market_context_for_signal

context = build_market_context_for_signal(signal, data_sources=data_sources)
```

`signal` is the scanner signal dictionary. `data_sources` is an optional in-memory dictionary of already-available context. The function does not fetch unavailable data.

The builder copies only from:

- `signal`
- `data_sources`
- provided context nested under keys such as `context`, `market_context`, or `provided_context`

## Output shape

The returned dictionary is JSON-safe and shaped as:

```python
{
    "context_version": "market_context_v1",
    "ticker": "WFC",
    "timeframes_available": [],
    "candles": {
        "monthly": [],
        "weekly": [],
        "daily": [],
        "4h": [],
        "2d": [],
        "3d": [],
        "4d": [],
        "5d": [],
    },
    "levels": {
        "scanner_entry": None,
        "scanner_stop": None,
        "scanner_target": None,
        "monthly_high": None,
        "monthly_low": None,
        "weekly_high": None,
        "weekly_low": None,
        "daily_high": None,
        "daily_low": None,
        "four_hour_high": None,
        "four_hour_low": None,
    },
    "trend": {
        "vwap": None,
        "ema_stack": None,
        "price_above_vwap": None,
    },
    "volume": {
        "relative_volume": None,
        "volume_ratio": None,
    },
    "sector": {
        "sector": None,
        "sector_direction": None,
        "sector_green": None,
        "sector_red": None,
    },
    "news_earnings": {
        "earnings_date": None,
        "earnings_risk": None,
        "news_risk": None,
        "catalyst": None,
    },
    "screenshot_context": None,
    "missing_data": [],
    "warnings": [],
    "diagnostics": {
        "observe_only": True,
        "data_sources_used": [],
        "fake_data_used": False,
    },
}
```

## Supported candle inputs

The builder accepts candle lists from:

```python
data_sources = {
    "candles": {
        "monthly": [...],
        "weekly": [...],
        "daily": [...],
        "4h": [...],
        "2d": [...],
        "3d": [...],
        "4d": [...],
        "5d": [...],
    }
}
```

It also tolerates equivalent direct keys such as `daily_candles`, `four_hour_candles`, `2d_candles`, and signal-level `candles` if that data is already present.

## Missing data policy

Core timeframes are `monthly`, `weekly`, `daily`, and `4h`. If any of those are unavailable, the corresponding `candles.<timeframe>` key is added to `missing_data`.

Optional multi-day timeframes `2d`, `3d`, `4d`, and `5d` are passed when available but are not marked missing when absent.

The builder also records missing scanner entry/stop/target levels and passive context gaps such as missing VWAP, volume, and sector context.

## Data quality alignment

If #209A provides `ap.data_quality_context.evaluate_data_quality_context()` or `score_data_quality_context()`, the builder will call it defensively and merge returned `missing_data`, `warnings`, `bad_shape`, and `stale_data` diagnostics.

If that module is not present, the builder still emits compatible fields:

- `missing_data`
- `warnings`
- `diagnostics.fake_data_used = False`

Local candle quality warnings include malformed candle containers, missing OHLC keys, and non-numeric OHLC values. These warnings are diagnostic only and do not approve or block trades.

## News/earnings placeholder

The passive `news_earnings` bucket is reserved for later scoring modules. It copies these keys when provided:

- `earnings_date`
- `earnings_risk`
- `news_risk`
- `catalyst`

Absent values stay `None` and are not guessed.

## Screenshot context passthrough

If `signal` or `data_sources` includes screenshot annotations under `screenshot_context`, `screenshot_annotations`, `chart_screenshot`, or `screenshot`, the value is copied to `screenshot_context` in JSON-safe form.

Screenshot context remains observe-only. It does not approve, block, resize, submit, cancel, or otherwise control trading behavior.
