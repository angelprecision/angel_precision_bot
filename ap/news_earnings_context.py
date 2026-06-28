from __future__ import annotations

from typing import Any

MAX_SCORE = 5.0


def score_news_earnings_context(signal: dict[str, Any], market_context: dict[str, Any] | None = None) -> dict[str, Any]:
    sig = dict(signal or {})
    ctx = market_context or {}
    payload = ctx.get("news_earnings") if isinstance(ctx.get("news_earnings"), dict) else {}
    earnings_risk = sig.get("earnings_risk", payload.get("earnings_risk"))
    news_risk = sig.get("news_risk", payload.get("news_risk"))
    catalyst = sig.get("catalyst", payload.get("catalyst"))
    missing = []
    warnings = []
    blocks = []
    score = MAX_SCORE
    if earnings_risk is None:
        missing.append("earnings_risk")
    elif bool(earnings_risk):
        warnings.append("earnings_risk_present")
        blocks.append("earnings_risk_present")
        score -= 3.0
    if news_risk is None:
        missing.append("news_risk")
    elif bool(news_risk):
        warnings.append("news_risk_present")
        score -= 1.0
    return {
        "score": round(max(0.0, min(MAX_SCORE, score)), 2),
        "max_score": MAX_SCORE,
        "status": "block_recommended" if blocks else "missing_data" if missing else "ok",
        "missing_data": sorted(set(missing)),
        "warnings": sorted(set(warnings)),
        "block_recommendations": sorted(set(blocks)),
        "boosts": [],
        "penalties": warnings,
        "diagnostics": {
            "observe_only": True,
            "block_recommendations_are_diagnostic_only": True,
            "earnings_risk": earnings_risk,
            "news_risk": news_risk,
            "catalyst": catalyst,
            "no_external_fetch": True,
        },
    }
