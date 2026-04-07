"""
ap_backtest_metrics.py — Angel Precision Backtest Performance Metrics
======================================================================
Adapted from virattt/ai-hedge-fund backtesting/metrics.py (MIT License)

Computes institutional-grade performance metrics:
- Sharpe Ratio
- Sortino Ratio
- Max Drawdown (with date)
- Win Rate
- Profit Factor
- Avg Win / Avg Loss
- Calmar Ratio

Usage:
    from ap_intelligence.backtesting.ap_backtest_metrics import APMetrics
    m = APMetrics()
    results = m.compute(trades_df, portfolio_values)
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional


ANNUAL_TRADING_DAYS = 252
RISK_FREE_RATE      = 0.0434   # ~4.34% (2026 T-bill)


@dataclass
class APPerformanceMetrics:
    # Returns
    total_return_pct:    Optional[float] = None
    annualized_return:   Optional[float] = None
    # Risk-Adjusted
    sharpe_ratio:        Optional[float] = None
    sortino_ratio:       Optional[float] = None
    calmar_ratio:        Optional[float] = None
    # Drawdown
    max_drawdown_pct:    Optional[float] = None
    max_drawdown_date:   Optional[str]   = None
    # Trade-Level
    total_trades:        int = 0
    win_rate:            Optional[float] = None
    profit_factor:       Optional[float] = None
    avg_win_pct:         Optional[float] = None
    avg_loss_pct:        Optional[float] = None
    best_trade_pct:      Optional[float] = None
    worst_trade_pct:     Optional[float] = None
    # Benchmark
    spy_return_pct:      Optional[float] = None
    alpha:               Optional[float] = None


class APMetrics:
    """
    Compute all Angel Precision performance metrics from a list of
    portfolio value points and a trades DataFrame.
    """

    def compute(
        self,
        portfolio_values: list[dict],    # [{Date, Portfolio Value}, ...]
        trades: list[dict] = None,       # [{ticker, entry, exit, pnl_pct}, ...]
        spy_return_pct: float = None,
    ) -> APPerformanceMetrics:

        m = APPerformanceMetrics()

        if not portfolio_values:
            return m

        df = pd.DataFrame(portfolio_values)
        if df.empty or "Portfolio Value" not in df.columns:
            return m

        df = df.set_index("Date").sort_index()
        df["Daily Return"] = df["Portfolio Value"].pct_change()
        returns = df["Daily Return"].dropna()

        if len(returns) < 2:
            return m

        # ── Total + Annualized Return ─────────────────────────
        start_val = df["Portfolio Value"].iloc[0]
        end_val   = df["Portfolio Value"].iloc[-1]
        n_days    = (df.index[-1] - df.index[0]).days or 1

        m.total_return_pct  = round((end_val / start_val - 1) * 100, 2)
        m.annualized_return = round(((end_val / start_val) ** (365 / n_days) - 1) * 100, 2)

        # ── Sharpe Ratio ──────────────────────────────────────
        daily_rf    = RISK_FREE_RATE / ANNUAL_TRADING_DAYS
        excess      = returns - daily_rf
        std_excess  = excess.std()
        if std_excess > 1e-12:
            m.sharpe_ratio = round(float(np.sqrt(ANNUAL_TRADING_DAYS) * excess.mean() / std_excess), 3)

        # ── Sortino Ratio ─────────────────────────────────────
        downside   = np.minimum(excess, 0)
        down_dev   = float(np.sqrt(np.mean(downside ** 2)))
        if down_dev > 1e-12:
            m.sortino_ratio = round(float(np.sqrt(ANNUAL_TRADING_DAYS) * excess.mean() / down_dev), 3)

        # ── Max Drawdown ──────────────────────────────────────
        roll_max = df["Portfolio Value"].cummax()
        drawdown = (df["Portfolio Value"] - roll_max) / roll_max
        if len(drawdown) > 0:
            min_dd = float(drawdown.min())
            m.max_drawdown_pct  = round(min_dd * 100, 2)
            if min_dd < 0:
                m.max_drawdown_date = drawdown.idxmin().strftime("%Y-%m-%d")

        # ── Calmar Ratio ──────────────────────────────────────
        if m.max_drawdown_pct and m.max_drawdown_pct < 0 and m.annualized_return is not None:
            m.calmar_ratio = round(m.annualized_return / abs(m.max_drawdown_pct), 3)

        # ── Trade-Level Stats ─────────────────────────────────
        if trades:
            trade_df = pd.DataFrame(trades)
            if "pnl_pct" in trade_df.columns:
                pnls = pd.to_numeric(trade_df["pnl_pct"], errors="coerce").dropna()
                m.total_trades = len(pnls)

                if m.total_trades > 0:
                    wins   = pnls[pnls > 0]
                    losses = pnls[pnls <= 0]

                    m.win_rate    = round(len(wins) / m.total_trades * 100, 1)
                    m.avg_win_pct = round(float(wins.mean()) * 100, 2) if len(wins) > 0 else None
                    m.avg_loss_pct= round(float(losses.mean()) * 100, 2) if len(losses) > 0 else None
                    m.best_trade_pct  = round(float(pnls.max()) * 100, 2)
                    m.worst_trade_pct = round(float(pnls.min()) * 100, 2)

                    gross_profit = float(wins.sum())
                    gross_loss   = abs(float(losses.sum()))
                    if gross_loss > 0:
                        m.profit_factor = round(gross_profit / gross_loss, 3)

        # ── Benchmark ─────────────────────────────────────────
        if spy_return_pct is not None:
            m.spy_return_pct = round(spy_return_pct, 2)
            if m.total_return_pct is not None:
                m.alpha = round(m.total_return_pct - spy_return_pct, 2)

        return m

    def format_report(self, m: APPerformanceMetrics) -> str:
        """Print-ready performance report."""
        lines = [
            "═" * 50,
            "  ANGEL PRECISION — BACKTEST PERFORMANCE REPORT",
            "═" * 50,
            f"  Total Return:       {m.total_return_pct:+.2f}%" if m.total_return_pct else "  Total Return:       N/A",
            f"  Annualized Return:  {m.annualized_return:+.2f}%" if m.annualized_return else "  Annualized Return:  N/A",
            "─" * 50,
            f"  Sharpe Ratio:       {m.sharpe_ratio:.3f}" if m.sharpe_ratio else "  Sharpe Ratio:       N/A",
            f"  Sortino Ratio:      {m.sortino_ratio:.3f}" if m.sortino_ratio else "  Sortino Ratio:      N/A",
            f"  Calmar Ratio:       {m.calmar_ratio:.3f}" if m.calmar_ratio else "  Calmar Ratio:       N/A",
            "─" * 50,
            f"  Max Drawdown:       {m.max_drawdown_pct:.2f}%" if m.max_drawdown_pct else "  Max Drawdown:       N/A",
            f"  Max DD Date:        {m.max_drawdown_date}" if m.max_drawdown_date else "",
            "─" * 50,
            f"  Total Trades:       {m.total_trades}",
            f"  Win Rate:           {m.win_rate:.1f}%" if m.win_rate else "  Win Rate:           N/A",
            f"  Profit Factor:      {m.profit_factor:.3f}" if m.profit_factor else "  Profit Factor:      N/A",
            f"  Avg Win:            {m.avg_win_pct:+.2f}%" if m.avg_win_pct else "  Avg Win:            N/A",
            f"  Avg Loss:           {m.avg_loss_pct:+.2f}%" if m.avg_loss_pct else "  Avg Loss:           N/A",
            f"  Best Trade:         {m.best_trade_pct:+.2f}%" if m.best_trade_pct else "",
            f"  Worst Trade:        {m.worst_trade_pct:+.2f}%" if m.worst_trade_pct else "",
            "─" * 50,
            f"  SPY Return:         {m.spy_return_pct:+.2f}%" if m.spy_return_pct else "  SPY Return:         N/A",
            f"  Alpha vs SPY:       {m.alpha:+.2f}%" if m.alpha else "  Alpha vs SPY:       N/A",
            "═" * 50,
        ]
        return "\n".join([l for l in lines if l is not None])
