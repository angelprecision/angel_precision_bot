# Market context builder

`ap.market_context_builder.build_market_context_for_signal()` creates the observe-only market context payload consumed by the position score profile stack.

## Safety boundary

This module is diagnostic only.

It does **not**:

- call broker APIs
- submit or cancel orders
- read or mutate queue, orders, positions, or proof-trades tables
- change `client_id`, `execution_mode`, `signal["score"]`, `plan.score`, or any live/paper taxonomy
- guess or synthesize missing candle data

Missing candles are represented as empty lists and recorded in `missing_data` for core timeframes.

## Public API

```python
from ap.market_context_builder import build_market_context_for_signal

context = build_market_context_for_signal(signal, data_sources=data_sources)
```

`signal` is the scanner signal dictionary. `data_sources` is an optional in-memory dictionary of already-available context. The function does not fetch unavailable data.

## Output shape

The returned dictionary is JSON-safe and shaped as:

```python
{
    "context_version": "market_context_v1",
    "ticker": "WFC",
    "timeframes_available": ["monthly", "weekly", "daily", "4h"],
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
    "missing_data": [],
    "diagnostics": {
        "observe_only": True,
        "data_sources_used": [],
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
