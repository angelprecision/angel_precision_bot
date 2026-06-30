# P1 Observe-Only Position Score Profile

## Purpose

This PR creates the first bot-level scoring subsystem for every scanner candidate.

The scanner remains the candidate generator. The bot now has a pure, observe-only way to build a richer case file around each signal:

- scanner setup quality
- trigger/stop/target geometry
- remaining opportunity to target
- higher-timeframe Strat confluence
- 4H + daily fair value gap context
- price stacking against scanner/setup levels
- volume confirmation
- VWAP / trend alignment when available
- contract execution quality
- historical ticker/pattern feedback

## Safety boundary

This PR is intentionally observe-only.

It does not replace scanner score, alter plan score, alter client or mode fields, submit/cancel anything, or mutate queue/accounting tables.

The output is a diagnostic object only:

```python
profile = build_position_score_profile(signal, market_context)
```

Future integration should attach it under:

```python
plan.metadata["position_score_profile"]
plan.metadata["score_audit"]["position_score_profile"]
```

## Data shape

The scoring engine accepts optional context. If context is missing, it records missing fields instead of guessing.

```python
market_context = {
    "candles": {
        "monthly": [...],
        "weekly": [...],
        "daily": [...],
        "4h": [...],
        "2d": [...],
        "3d": [...],
        "4d": [...],
        "5d": [...],
    },
    "levels": {
        "weekly_level": 123.45,
        "monthly_high": 130.00,
    },
    "trend": {
        "vwap": 121.25,
        "ema_stack": "bullish",
    },
    "volume": {
        "relative_volume": 1.5,
    },
}
```

## FVG rules

The FVG module uses a three-candle imbalance model:

- Bullish FVG: candle 1 high < candle 3 low
- Bearish FVG: candle 1 low > candle 3 high

The bot treats FVGs as support/resistance zones:

- CALLs prefer bullish FVG support below or near entry.
- PUTs prefer bearish FVG resistance above or near entry.
- Entry inside the middle of an FVG is friction.
- Entry inside an opposing FVG is a block recommendation.
- Target into an opposing FVG is a block recommendation.

## Strat rules

The higher-timeframe module classifies bars as:

- `1` inside bar
- `2U` directional up
- `2D` directional down
- `3` outside bar

Monthly and weekly alignment matter most. Daily and 4H refine the setup. Optional 2D/3D/4D/5D candles can be passed if data enrichment later supports them.

## Why bot level first

Bot-level scoring is safer because it can compare scanner setup levels against execution reality, consume richer context later, and roll out observe-only before it changes live behavior.

## Next PR

Wire `build_position_score_profile()` into `APMasterControl.evaluate()` after the plan is created and before final approval, metadata-only.

That follow-up PR should only attach:

```python
plan.metadata["position_score_profile"]
plan.metadata["score_audit"]["position_score_profile"]
```

It must not make the score active until we have proof from real overnight payloads.
