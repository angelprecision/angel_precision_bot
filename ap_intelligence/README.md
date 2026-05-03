# Angel Precision Intelligence Layer

Extracted and adapted from the best open-source AI hedge fund repos:
- [virattt/ai-hedge-fund](https://github.com/virattt/ai-hedge-fund) (MIT) — Risk manager, technical agent, portfolio manager, backtesting metrics
- [ValueCell-ai/valuecell](https://github.com/ValueCell-ai/valuecell) (MIT) — Base agent lifecycle, paper trading gateway, decision composer pattern
- [ralliesai/rallies-cli](https://github.com/ralliesai/rallies-cli) (GPL-3.0) — Prompt engineering patterns, planning agent style

All code has been rewritten and adapted for Angel Precision's architecture:
- Tradier as primary data source (your existing API)
- Options-first position sizing
- Strat pattern scanner integration
- Discord webhook output

---

## File Structure

```
ap_intelligence/
├── ap_signal_pipeline.py          ← MAIN ENTRY POINT — runs everything
├── tools/
│   └── ap_data_tools.py           ← Tradier + yfinance data layer with caching
├── agents/
│   ├── ap_risk_manager.py         ← Volatility + correlation adjusted position sizing
│   ├── ap_technical_agent.py      ← 5-strategy technical ensemble
│   ├── ap_sentiment_agent.py      ← News + insider + short interest
│   ├── ap_fundamentals_agent.py   ← Profitability / growth / health / valuation filter
│   └── ap_portfolio_manager.py    ← Final weighted vote + LLM synthesis → EXECUTE/SKIP
└── backtesting/
    └── ap_backtest_metrics.py     ← Sharpe, Sortino, Drawdown, Win Rate, Profit Factor
```

---

## Quickstart — Drop Into Your Scanner

```python
from ap_intelligence.ap_signal_pipeline import APSignalPipeline

pipeline = APSignalPipeline(
    portfolio_value=10_000,
    openai_api_key="sk-...",   # Optional — falls back to rule-based if not set
)

result = pipeline.run(
    ticker="NVDA",
    scanner_signal="bullish",        # From your Strat scanner
    scanner_confidence=88,           # From your confidence scorer
    underlying_price=950.00,         # Current underlying price
    option_cost_per_contract=300.0,  # Debit per contract (premium × 100)
)

print(result["action"])       # "execute" or "skip"
print(result["contracts"])    # Number of contracts approved
print(result["confidence"])   # 0-100 weighted confidence
print(result["reasoning"])    # Human-readable rationale

# Post to Discord
pipeline.send_discord(result, webhook_url="https://discord.com/api/webhooks/...")
```

---

## How Each Agent Works

### Risk Manager (`ap_risk_manager.py`)
Adapted from virattt. Key upgrades for AP:
- **Daily loss kill switch** — no new trades if daily P&L drops below threshold
- **SPY trend gate** — CALL blocked on BEAR trend, PUT blocked on BULL trend
- **VIX gate** — blocks trades when VIX < 12 or > 35
- **Volatility-adjusted limits** — high vol stocks get smaller position caps
- **Correlation multiplier** — reduces size when new trade correlates with existing positions

### Technical Agent (`ap_technical_agent.py`)
5-strategy weighted ensemble:
| Strategy | Weight | What it measures |
|---|---|---|
| Strat Pattern | 15% | Your scanner signal (direct passthrough at 80% conf) |
| Momentum | 30% | RSI + MACD + Rate of Change |
| Trend Following | 25% | EMA stack (8/21/55) + ADX strength |
| Mean Reversion | 15% | Bollinger Band position + Z-score |
| Volatility | 15% | ATR expansion + volume surge |

### Sentiment Agent (`ap_sentiment_agent.py`)
| Source | Weight | Method |
|---|---|---|
| News Headlines | 55% | GPT-4 Mini classification per headline |
| Insider Trades | 25% | Net buy/sell direction from yfinance |
| Short Interest | 20% | Short ratio analysis |

### Portfolio Manager (`ap_portfolio_manager.py`)
Final weighted vote across all agents:
| Agent | Vote Weight |
|---|---|
| Scanner (Strat) | 35% |
| Technical | 30% |
| Fundamentals | 20% |
| Sentiment | 15% |

Threshold: 60% weighted confidence required to EXECUTE.
Risk manager has absolute veto power regardless of signal strength.

---

## Backtest Metrics (`ap_backtest_metrics.py`)
Computes all institutional metrics:
- Sharpe Ratio (vs 4.34% risk-free rate)
- Sortino Ratio (downside deviation only)
- Calmar Ratio (return / max drawdown)
- Max Drawdown + date
- Win Rate, Profit Factor, Avg Win/Loss
- Alpha vs SPY benchmark

---

## Environment Variables
```
TRADIER_ACCESS_TOKEN=your_tradier_token
OPENAI_API_KEY=sk-...              # Optional but enables LLM sentiment + portfolio synthesis
AP_CACHE_DIR=/tmp/ap_cache         # Where to store API response cache
```
