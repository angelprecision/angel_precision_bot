from __future__ import annotations

from dataclasses import dataclass
from typing import Any

MIN_WIN_RATE = 80.0
MIN_PROFIT_FACTOR = 2.0
MAX_DRAWDOWN_FLOOR = -5.0
MIN_TRADES = 30

@dataclass
class Scorecard:
    passed: bool
    score: float
    reasons: list[str]


def evaluate_bundle(bundle: dict[str, Any]) -> Scorecard:
    agg = bundle.get("aggregate", {})
    wr = float(agg.get("win_rate_pct", 0.0) or 0.0)
    pf = float(agg.get("profit_factor", 0.0) or 0.0)
    dd = float(agg.get("max_drawdown_pct", 0.0) or 0.0)
    trades = int(agg.get("trade_count", 0) or 0)
    profit = float(agg.get("total_profit_pct", 0.0) or 0.0)

    reasons = []
    passed = True

    if wr < MIN_WIN_RATE:
        passed = False
        reasons.append(f"win_rate_pct {wr:.2f} < {MIN_WIN_RATE}")
    if pf < MIN_PROFIT_FACTOR:
        passed = False
        reasons.append(f"profit_factor {pf:.2f} < {MIN_PROFIT_FACTOR}")
    if dd < MAX_DRAWDOWN_FLOOR:
        passed = False
        reasons.append(f"max_drawdown_pct {dd:.2f} < {MAX_DRAWDOWN_FLOOR}")
    if trades < MIN_TRADES:
        passed = False
        reasons.append(f"trade_count {trades} < {MIN_TRADES}")

    score = 0.0
    score += wr * 4.0
    score += pf * 30.0
    score += max(dd, -5.0) * 3.0
    score += profit * 1.5
    score += min(trades, 200) * 0.2

    per_pair = bundle.get("per_pair", {})
    weak_symbols = []
    for symbol, metrics in per_pair.items():
        s_wr = float(metrics.get("win_rate_pct", 0.0) or 0.0)
        s_pf = float(metrics.get("profit_factor", 0.0) or 0.0)
        if s_wr < 70.0 or s_pf < 1.5:
            weak_symbols.append(symbol)
    if weak_symbols:
        score -= 10.0 * len(weak_symbols)
        reasons.append("weak symbols: " + ", ".join(weak_symbols))

    if passed:
        reasons.append("passed hard gates")

    return Scorecard(passed=passed, score=score, reasons=reasons)
