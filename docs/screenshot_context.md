# Screenshot context bridge

PR 214 adds a small normalization bridge for chart screenshot intelligence.

This is separate from PR #201. PR #201 is the chart screenshot memory confirmation foundation and Supabase/backfill path. This module is only the shape normalizer that downstream score diagnostics can consume later.

## Public function

```python
from ap.screenshot_context import normalize_screenshot_context

context = normalize_screenshot_context(raw)
```

## Input example

```python
{
    "ticker": "WFC",
    "timeframe": "4h",
    "image_url": "...",
    "visual_bias": "bullish",
    "detected_fvg_zones": [],
    "support_levels": [],
    "resistance_levels": [],
    "operator_notes": "",
    "confidence": 0.72,
}
```

Nested timeframe payloads are also accepted:

```python
{
    "ticker": "WFC",
    "image_url": "...",
    "visual_bias": "mixed",
    "timeframes": {
        "4h": {
            "support_levels": [],
            "resistance_levels": [],
            "fvg_zones": [],
            "trend_label": "uptrend",
            "confidence": 0.72,
        },
        "daily": {
            "support_levels": [],
            "resistance_levels": [],
            "confidence": 0.61,
        },
    },
}
```

## Output shape

```python
{
    "context_version": "screenshot_context_v1",
    "available": True,
    "visual_bias": "bullish",
    "timeframes": {
        "4h": {
            "support_levels": [],
            "resistance_levels": [],
            "fvg_zones": [],
            "trend_label": None,
            "confidence": 0.72,
        }
    },
    "missing_data": [],
    "warnings": [],
    "diagnostics": {
        "observe_only": True,
        "trading_authority": False,
        "active_gate": False,
        "can_approve_trade": False,
        "can_block_trade": False,
    },
}
```

## Safety boundary

Screenshots are evidence only.

This module must not:

- approve trades
- block trades
- submit broker orders
- cancel broker orders
- mutate orders
- mutate positions
- mutate `proof_trades`
- mutate `trade_queue`
- create paper/live taxonomy changes
- infer fake `client_id` or fake `execution_mode`

The returned diagnostics explicitly mark the context as observe-only and non-authoritative.

## Missing data behavior

Missing screenshot data returns a stable non-crashing context:

```python
{
    "context_version": "screenshot_context_v1",
    "available": False,
    "visual_bias": "unclear",
    "timeframes": {},
    "missing_data": ["screenshot_context"],
    "warnings": ["screenshot_context_missing"],
    "diagnostics": {
        "observe_only": True,
        "trading_authority": False,
        "active_gate": False,
        "can_approve_trade": False,
        "can_block_trade": False,
    },
}
```

Missing image URLs, unknown visual labels, unusable levels, and malformed confidence values are warnings only.

## Future integration

A later PR may attach this normalized object into a position score profile or score audit metadata.

That later PR must still keep screenshots observe-only unless a separate reviewed active-gate PR explicitly changes the trading boundary after proof.
