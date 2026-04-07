"""
ap_audit_log.py — Angel Precision Trade Audit Log
===================================================
Logs EVERY decision with full reasoning for:
- Client trust ("why did it take/skip this?")
- Performance diagnostics
- Marketing evidence ("filtered 31% of junk setups")
- Tuning data

Writes to:
1. ap_audit.jsonl  (append-only log — one JSON per line)
2. Supabase (optional, if SUPABASE_URL + SUPABASE_KEY are set)

Usage:
    from ap_intelligence.ap_audit_log import APAuditLog
    log = APAuditLog()
    log.record(result)       # result from APSignalPipeline.run()
    log.summary()            # quick stats
    log.compare_filtered_vs_unfiltered()  # for marketing proof
"""

import os
import json
import datetime
import pandas as pd
from pathlib import Path
from typing import Optional


AUDIT_FILE = Path(os.environ.get("AP_AUDIT_FILE", "/tmp/ap_audit.jsonl"))


class APAuditLog:
    """
    Append-only audit log for every AP pipeline decision.
    Every entry has: what, why, score, outcome (if known).
    """

    def __init__(self, log_file: Path = AUDIT_FILE):
        self.log_file = log_file
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

    # ─────────────────────────────────────────────
    # RECORD A DECISION
    # ─────────────────────────────────────────────
    def record(self, pipeline_result: dict, outcome_pnl: float = None):
        """
        Log a pipeline decision.

        Parameters
        ----------
        pipeline_result : dict — output from APSignalPipeline.run()
        outcome_pnl : float — actual P&L % if known at time of logging (for closed trades)
        """
        entry = {
            "timestamp":   pipeline_result.get("timestamp", datetime.datetime.now().isoformat()),
            "ticker":      pipeline_result.get("ticker"),
            "action":      pipeline_result.get("action"),       # "execute" | "skip"
            "direction":   pipeline_result.get("direction"),
            "score":       pipeline_result.get("score", 0),
            "size_tier":   pipeline_result.get("size_tier", "skip"),
            "contracts":   pipeline_result.get("contracts", 0),
            "max_usd":     pipeline_result.get("max_usd", 0),
            "reasoning":   pipeline_result.get("reasoning", ""),
            "hard_block":  pipeline_result.get("hard_block", ""),
            # Score breakdown
            "score_setup":    pipeline_result.get("score_breakdown", {}).get("setup_quality", 0),
            "score_technical":pipeline_result.get("score_breakdown", {}).get("technical_align", 0),
            "score_regime":   pipeline_result.get("score_breakdown", {}).get("regime_fit", 0),
            "score_contract": pipeline_result.get("score_breakdown", {}).get("contract_quality", 0),
            "score_fund":     pipeline_result.get("score_breakdown", {}).get("fundamentals", 0),
            "score_sentiment":pipeline_result.get("score_breakdown", {}).get("sentiment", 0),
            # Signal inputs
            "scanner_signal":   pipeline_result.get("signal_breakdown", {}).get("scanner", {}).get("signal"),
            "scanner_conf":     pipeline_result.get("signal_breakdown", {}).get("scanner", {}).get("confidence"),
            "tech_signal":      pipeline_result.get("signal_breakdown", {}).get("technical", {}).get("signal"),
            "tech_conf":        pipeline_result.get("signal_breakdown", {}).get("technical", {}).get("confidence"),
            "fund_signal":      pipeline_result.get("signal_breakdown", {}).get("fundamentals", {}).get("signal"),
            "sent_signal":      pipeline_result.get("signal_breakdown", {}).get("sentiment", {}).get("signal"),
            # Risk context
            "spy_trend":   pipeline_result.get("signal_breakdown", {}).get("risk", {}).get("spy_trend"),
            "vix":         pipeline_result.get("signal_breakdown", {}).get("risk", {}).get("vix"),
            "vol_pct":     pipeline_result.get("signal_breakdown", {}).get("risk", {}).get("vol_pct"),
            # Outcome (filled in later when trade closes)
            "outcome_pnl":    outcome_pnl,
            "outcome_known":  outcome_pnl is not None,
        }

        with open(self.log_file, "a") as f:
            f.write(json.dumps(entry) + "\n")

        # Optional: write to Supabase
        self._write_supabase(entry)

        return entry

    def record_outcome(self, ticker: str, timestamp: str, pnl_pct: float):
        """
        Update an existing log entry with the outcome P&L.
        Rewrites the file — only use for small logs or use Supabase in production.
        """
        entries = self._load_all()
        for e in entries:
            if e["ticker"] == ticker and e["timestamp"] == timestamp:
                e["outcome_pnl"] = pnl_pct
                e["outcome_known"] = True
                break
        self._rewrite(entries)

    # ─────────────────────────────────────────────
    # ANALYTICS
    # ─────────────────────────────────────────────
    def load_df(self) -> pd.DataFrame:
        entries = self._load_all()
        if not entries:
            return pd.DataFrame()
        return pd.DataFrame(entries)

    def summary(self) -> dict:
        """Quick stats on all logged decisions."""
        df = self.load_df()
        if df.empty:
            return {"total": 0}

        executes = df[df["action"] == "execute"]
        skips    = df[df["action"] == "skip"]
        with_outcome = executes[executes["outcome_known"] == True]

        stats = {
            "total_decisions":    len(df),
            "executed":           len(executes),
            "skipped":            len(skips),
            "filter_rate_pct":    round(len(skips) / len(df) * 100, 1) if len(df) > 0 else 0,
            "tier_breakdown":     executes["size_tier"].value_counts().to_dict() if not executes.empty else {},
            "trades_with_outcome":len(with_outcome),
        }

        if not with_outcome.empty:
            wins   = with_outcome[with_outcome["outcome_pnl"] > 0]
            losses = with_outcome[with_outcome["outcome_pnl"] <= 0]
            stats["win_rate_pct"]    = round(len(wins) / len(with_outcome) * 100, 1)
            stats["avg_win_pct"]     = round(float(wins["outcome_pnl"].mean() * 100), 2) if len(wins) > 0 else None
            stats["avg_loss_pct"]    = round(float(losses["outcome_pnl"].mean() * 100), 2) if len(losses) > 0 else None
            stats["profit_factor"]   = round(float(wins["outcome_pnl"].sum()) / abs(float(losses["outcome_pnl"].sum())), 3) \
                                       if len(losses) > 0 and losses["outcome_pnl"].sum() != 0 else None
            stats["best_trade_pct"]  = round(float(with_outcome["outcome_pnl"].max() * 100), 2)
            stats["worst_trade_pct"] = round(float(with_outcome["outcome_pnl"].min() * 100), 2)

        return stats

    def compare_filtered_vs_unfiltered(self) -> dict:
        """
        THE MARKETING TABLE.
        Compares: what would have happened if ALL scanner signals were taken
        vs what actually happened with the intelligence layer.

        Requires outcome_pnl to be set on executed entries.
        Returns evidence for sales pitch.
        """
        df = self.load_df()
        if df.empty or "outcome_pnl" not in df.columns:
            return {"error": "No outcome data yet. Record trade outcomes first."}

        # "Unfiltered" = everything the scanner sent (all rows where scanner_signal != neutral)
        # "Filtered"   = only executed trades
        all_signals = df[df["scanner_signal"].isin(["bullish", "bearish"])].copy()
        executed    = df[(df["action"] == "execute") & (df["outcome_known"] == True)].copy()
        skipped_outcomes = df[(df["action"] == "skip") & (df["outcome_known"] == True)].copy()

        def trade_stats(subset: pd.DataFrame) -> dict:
            if subset.empty or "outcome_pnl" not in subset.columns:
                return {}
            pnls = subset["outcome_pnl"].dropna()
            if len(pnls) == 0:
                return {}
            wins   = pnls[pnls > 0]
            losses = pnls[pnls <= 0]
            return {
                "n":              len(pnls),
                "win_rate":       round(len(wins) / len(pnls) * 100, 1),
                "avg_win_pct":    round(float(wins.mean() * 100), 2) if len(wins) > 0 else 0,
                "avg_loss_pct":   round(float(losses.mean() * 100), 2) if len(losses) > 0 else 0,
                "profit_factor":  round(float(wins.sum()) / abs(float(losses.sum())), 3)
                                  if losses.sum() != 0 else 999,
                "total_return":   round(float(pnls.sum() * 100), 2),
                "max_loss":       round(float(pnls.min() * 100), 2),
            }

        unfiltered_stats = trade_stats(all_signals)
        filtered_stats   = trade_stats(executed)
        avoided_stats    = trade_stats(skipped_outcomes)

        # Calculate improvements
        improvements = {}
        if unfiltered_stats and filtered_stats:
            if unfiltered_stats.get("win_rate") and filtered_stats.get("win_rate"):
                improvements["win_rate_delta"] = round(
                    filtered_stats["win_rate"] - unfiltered_stats["win_rate"], 1)
            if unfiltered_stats.get("profit_factor") and filtered_stats.get("profit_factor"):
                improvements["profit_factor_delta"] = round(
                    filtered_stats["profit_factor"] - unfiltered_stats["profit_factor"], 3)
            if unfiltered_stats.get("max_loss") and filtered_stats.get("max_loss"):
                improvements["max_loss_reduction"] = round(
                    unfiltered_stats["max_loss"] - filtered_stats["max_loss"], 2)
            if len(df) > 0:
                improvements["pct_signals_filtered"] = round(
                    len(df[df["action"] == "skip"]) / len(df) * 100, 1)

        return {
            "unfiltered_all_scanner_signals": unfiltered_stats,
            "filtered_intelligence_layer":    filtered_stats,
            "avoided_trades":                 avoided_stats,
            "improvements":                   improvements,
            "marketing_bullets": _format_marketing_bullets(improvements, filtered_stats, unfiltered_stats),
        }

    def breakdown_by_slice(self) -> dict:
        """
        Performance breakdown by:
        - Ticker
        - Regime (SPY trend)
        - Score tier
        - Direction
        - Sentiment alignment
        """
        df = self.load_df()
        if df.empty:
            return {}

        executed = df[(df["action"] == "execute") & (df["outcome_known"] == True)].copy()
        if executed.empty:
            return {"error": "No executed trades with outcomes yet"}

        results = {}

        for slice_col in ["ticker", "spy_trend", "size_tier", "direction"]:
            if slice_col not in executed.columns:
                continue
            group_stats = {}
            for val, group in executed.groupby(slice_col):
                pnls = group["outcome_pnl"].dropna()
                if len(pnls) < 2:
                    continue
                wins = pnls[pnls > 0]
                losses = pnls[pnls <= 0]
                group_stats[str(val)] = {
                    "n":           len(pnls),
                    "win_rate":    round(len(wins) / len(pnls) * 100, 1),
                    "avg_return":  round(float(pnls.mean() * 100), 2),
                    "profit_factor": round(float(wins.sum()) / abs(float(losses.sum())), 3)
                                     if losses.sum() != 0 else 999,
                }
            results[slice_col] = group_stats

        return results

    # ─────────────────────────────────────────────
    # INTERNAL
    # ─────────────────────────────────────────────
    def _load_all(self) -> list[dict]:
        if not self.log_file.exists():
            return []
        entries = []
        with open(self.log_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return entries

    def _rewrite(self, entries: list[dict]):
        with open(self.log_file, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

    def _write_supabase(self, entry: dict):
        """Write to Supabase ap_audit_log table if configured."""
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        if not url or not key:
            return
        try:
            import requests
            requests.post(
                f"{url}/rest/v1/ap_audit_log",
                headers={"apikey": key, "Authorization": f"Bearer {key}",
                         "Content-Type": "application/json", "Prefer": "return=minimal"},
                json=entry, timeout=5,
            )
        except Exception:
            pass  # Never let Supabase failure block the pipeline


def _format_marketing_bullets(improvements: dict, filtered: dict, unfiltered: dict) -> list[str]:
    """Generate sales-ready bullet points from comparison data."""
    bullets = []
    if improvements.get("pct_signals_filtered"):
        bullets.append(
            f"Filtered out {improvements['pct_signals_filtered']:.0f}% of low-quality scanner setups"
        )
    if improvements.get("win_rate_delta") and improvements["win_rate_delta"] > 0:
        bullets.append(
            f"Improved win rate from {unfiltered.get('win_rate', '?')}% → "
            f"{filtered.get('win_rate', '?')}% "
            f"(+{improvements['win_rate_delta']:.1f}pp)"
        )
    if improvements.get("profit_factor_delta") and improvements["profit_factor_delta"] > 0:
        bullets.append(
            f"Improved profit factor from {unfiltered.get('profit_factor', '?'):.2f} → "
            f"{filtered.get('profit_factor', '?'):.2f} "
            f"(+{improvements['profit_factor_delta']:.3f})"
        )
    if improvements.get("max_loss_reduction") and improvements["max_loss_reduction"] > 0:
        bullets.append(
            f"Reduced worst single trade from {unfiltered.get('max_loss', '?')}% → "
            f"{filtered.get('max_loss', '?')}% "
            f"(reduced drawdown by {improvements['max_loss_reduction']:.1f}%)"
        )
    return bullets
