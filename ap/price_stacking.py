from __future__ import annotations

from typing import Any

MAX_SCORE = 10.0
TOLERANCE_PCT = 0.35


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def score_price_stacking(signal: dict[str, Any], market_context: dict[str, Any] | None = None) -> dict[str, Any]:
    sig = dict(signal or {})
    ctx = market_context or {}
    levels = ctx.get("levels") if isinstance(ctx.get("levels"), dict) else sig.get("levels") if isinstance(sig.get("levels"), dict) else {}
    entry = _to_float(sig.get("entry_price") or sig.get("trigger_price") or levels.get("scanner_entry"))
    missing = []
    warnings = []
    matched = []
    if entry is None:
        missing.append("entry_price")
    if not levels:
        missing.append("levels")
    if entry is not None:
        for name, raw in levels.items():
            values = raw if isinstance(raw, list) else [raw]
            for item in values:
                value = item.get("price") if isinstance(item, dict) else item
                price = _to_float(value)
                if price is None:
                    continue
                distance_pct = abs(price - entry) / abs(entry) * 100.0 if entry else 0.0
                if distance_pct <= TOLERANCE_PCT:
                    matched.append({"name": str(name), "price": price, "distance_pct": round(distance_pct, 4)})
    score = min(MAX_SCORE, len(matched) * 2.0)
    return {
        "score": round(score, 2),
        "max_score": MAX_SCORE,
        "status": "missing_data" if missing else "ok",
        "matched_levels": matched,
        "stack_count": len(matched),
        "support_stack_below_entry": None,
        "resistance_stack_above_entry": None,
        "entry_into_resistance": False,
        "entry_into_support": False,
        "stop_protected_by_stack": False,
        "target_has_clean_air": None,
        "missing_data": sorted(set(missing)),
        "warnings": sorted(set(warnings)),
        "block_recommendations": [],
        "boosts": [],
        "penalties": [],
        "diagnostics": {
            "observe_only": True,
            "block_recommendations_are_diagnostic_only": True,
            "entry": entry,
        },
    }
