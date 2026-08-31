"""
ap_strategy_evolver.py
Angel Precision — Autonomous Strategy Parameter Optimizer

LEGACY LEAKY RESEARCH ONLY.

This script is retained for reproducibility, but its simplified simulator uses
historical ``win_rate`` and ``avg_return`` as scoring inputs.  Those fields are
outcomes, so results from this script are not valid promotion evidence and must
never be copied into live thresholds.  New policy evaluation belongs in
``ap.profitability_objective``.

This IS the Auto-Quant pattern, adapted for your system.
Instead of mutating FreqTrade strategies, this mutates the weights and
thresholds inside ap_strat_agent.py and backtests them against your
actual historical setups from ap_strat_setups.csv.

Goal: autonomously find the gate_threshold + STRATEGY_WEIGHTS combination
that maximizes win_rate while keeping trade_count reasonable (>15 trades
per symbol per quarter). Target: 80%+ win rate.

Usage:
    # Run overnight with Claude Code or as a standalone script
    python ap_strategy_evolver.py --source ap_strat_setups.csv --rounds 100 \
        --allow-outcome-leakage

    # Or import and drive from Claude Code:
    from ap_strategy_evolver import run_evolution_loop
    run_evolution_loop(source_csv="ap_strat_setups.csv", rounds=50)

Loop per Auto-Quant pattern:
    Mutate weights → backtest on historical setups → score result →
    keep or discard → log to results_evolver.tsv → repeat

Anti-overfitting rule (stolen from Auto-Quant's oracle-gaming detection):
    High win rate with <10 qualifying trades = discard.
    Win rate improvement >15% with no trade count change = flag as suspicious.
"""

import csv
import json
import logging
import random
import copy
import argparse
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Evolver] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default baseline weights (mirrors ap_strat_agent.py)
# ---------------------------------------------------------------------------

BASELINE_CONFIG = {
    "gate_threshold": 62.0,
    "weights": {
        "strat_sequence":       0.32,
        "timeframe_continuity": 0.28,
        "market_regime":        0.18,
        "volume_confirmation":  0.10,
        "time_gate":            0.07,
        "options_liquidity":    0.05,
    },
    # EV score scaling params
    "min_win_rate_for_ev": 0.65,    # sequences below this get penalized
    "high_ev_threshold":   0.85,    # above this gets max confidence boost
    "confidence_floor":    20.0,    # minimum confidence before gate
}

RESULTS_FILE = Path("results_evolver.tsv")
MIN_TRADES_FOR_VALID_RESULT = 10     # Anti-oracle-gaming threshold


# ---------------------------------------------------------------------------
# Experiment Configuration
# ---------------------------------------------------------------------------

@dataclass
class EvolverConfig:
    """One experiment's parameter set."""
    gate_threshold: float
    weights: dict
    min_win_rate_for_ev: float
    high_ev_threshold: float
    confidence_floor: float
    generation: int = 0
    parent_id: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "gate_threshold": self.gate_threshold,
            "weights": self.weights,
            "min_win_rate_for_ev": self.min_win_rate_for_ev,
            "high_ev_threshold": self.high_ev_threshold,
            "confidence_floor": self.confidence_floor,
            "generation": self.generation,
        }

    def weight_sum(self) -> float:
        return sum(self.weights.values())

    def normalize_weights(self):
        """Ensure weights sum to 1.0."""
        total = self.weight_sum()
        if total > 0:
            self.weights = {k: round(v / total, 4) for k, v in self.weights.items()}


@dataclass
class BacktestResult:
    """Result from backtesting a config against historical setups."""
    config: EvolverConfig
    win_rate: float          # 0.0 – 1.0
    trade_count: int         # number of setups that passed the gate
    avg_return: float        # average return on passing trades
    blocked_count: int       # setups blocked by this gate
    pass_through_rate: float # what % of signals this config passes
    sharpe_proxy: float      # win_rate * avg_return / std_dev (approximation)
    suspicious: bool = False # oracle-gaming flag
    status: str = "keep"     # "keep" | "discard" | "suspicious"
    note: str = ""

    def is_valid(self) -> bool:
        return (
            self.trade_count >= MIN_TRADES_FOR_VALID_RESULT
            and not self.suspicious
        )

    def score(self) -> float:
        """Single composite score for keep/discard decision."""
        if not self.is_valid():
            return 0.0
        # Heavily weight win rate, balance with trade count and pass rate
        # Don't want 100% win rate with 3 trades (overfitting)
        trade_count_bonus = min(self.trade_count / 30, 1.0) * 10
        return (self.win_rate * 70) + (min(self.avg_return, 0.30) / 0.30 * 20) + trade_count_bonus


# ---------------------------------------------------------------------------
# Historical Setup Loader
# ---------------------------------------------------------------------------

def load_historical_setups(csv_path: str) -> list[dict]:
    """
    Load ap_strat_setups.csv (your Polygon backtest data).
    Expected columns: ticker, timeframe, sequence, direction, win_rate, avg_return, trade_count
    """
    setups = []
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Historical setup CSV not found: {csv_path}. "
            "Synthetic fallback is forbidden for strategy evaluation."
        )

    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                setups.append({
                    "ticker":      row.get("ticker", ""),
                    "timeframe":   row.get("timeframe", "1D"),
                    "sequence":    row.get("sequence", ""),
                    "direction":   row.get("direction", "bullish"),
                    "win_rate":    float(row.get("win_rate", 0.5)),
                    "avg_return":  float(row.get("avg_return", 0.05)),
                    "trade_count": int(row.get("trade_count", 10)),
                    "ev_score":    float(row.get("ev_score", 50)),
                })
            except (ValueError, KeyError) as e:
                logger.debug(f"Skipped row: {e}")

    logger.info(f"Loaded {len(setups)} historical setups from {csv_path}")
    return setups


def _generate_synthetic_setups() -> list[dict]:
    """Synthetic test data mimicking ap_strat_setups.csv structure."""
    return [
        # High EV setups (should pass)
        {"ticker": "NVDA", "timeframe": "4D", "sequence": "2D→2U→1",   "direction": "bullish", "win_rate": 1.00, "avg_return": 0.218, "trade_count": 12, "ev_score": 95},
        {"ticker": "TSLA", "timeframe": "1W", "sequence": "2D→2U→2U",  "direction": "bullish", "win_rate": 0.90, "avg_return": 0.253, "trade_count": 20, "ev_score": 91},
        {"ticker": "AMD",  "timeframe": "3D", "sequence": "2U→2D→2U",  "direction": "bullish", "win_rate": 0.92, "avg_return": 0.110, "trade_count": 13, "ev_score": 88},
        {"ticker": "AAPL", "timeframe": "1D", "sequence": "2D→2U",     "direction": "bullish", "win_rate": 0.72, "avg_return": 0.062, "trade_count": 35, "ev_score": 74},
        {"ticker": "MSFT", "timeframe": "1D", "sequence": "2U→2D",     "direction": "bearish", "win_rate": 0.70, "avg_return": 0.058, "trade_count": 28, "ev_score": 72},
        {"ticker": "SPY",  "timeframe": "1W", "sequence": "2D→2U",     "direction": "bullish", "win_rate": 0.75, "avg_return": 0.081, "trade_count": 22, "ev_score": 77},
        {"ticker": "QQQ",  "timeframe": "1D", "sequence": "3→2U",      "direction": "bullish", "win_rate": 0.68, "avg_return": 0.054, "trade_count": 31, "ev_score": 70},
        # Weak setups (should be filtered)
        {"ticker": "GME",  "timeframe": "1D", "sequence": "1→2U",      "direction": "bullish", "win_rate": 0.45, "avg_return": 0.032, "trade_count": 18, "ev_score": 42},
        {"ticker": "AMC",  "timeframe": "1D", "sequence": "2U→1",      "direction": "bearish", "win_rate": 0.48, "avg_return": 0.028, "trade_count": 14, "ev_score": 38},
        {"ticker": "RIVN", "timeframe": "1D", "sequence": "3→2D",      "direction": "bearish", "win_rate": 0.55, "avg_return": 0.041, "trade_count": 11, "ev_score": 52},
        # Medium setups
        {"ticker": "META", "timeframe": "1D", "sequence": "2D→2U→1",   "direction": "bullish", "win_rate": 0.67, "avg_return": 0.071, "trade_count": 24, "ev_score": 68},
        {"ticker": "GOOG", "timeframe": "1W", "sequence": "2D→2U",     "direction": "bullish", "win_rate": 0.73, "avg_return": 0.065, "trade_count": 16, "ev_score": 73},
        {"ticker": "AMZN", "timeframe": "1D", "sequence": "2U→2D→2U",  "direction": "bullish", "win_rate": 0.61, "avg_return": 0.059, "trade_count": 19, "ev_score": 62},
    ]


# ---------------------------------------------------------------------------
# Gate Simulator
# Simulates ap_strat_agent.py scoring on historical setups
# ---------------------------------------------------------------------------

def simulate_gate(setup: dict, config: EvolverConfig) -> dict:
    """
    Simulate whether a historical setup would have passed the strat agent gate
    under a given config. Returns {"passes": bool, "confidence": float}.

    This is a simplified simulation of ap_strat_agent weighted scoring.
    For full fidelity, call the real ap_strat_agent() — but that requires
    live data. For evolution purposes, we use ev_score as the ground truth.
    """
    ev_score = setup.get("ev_score", 50)
    win_rate = setup.get("win_rate", 0.5)
    avg_return = setup.get("avg_return", 0.05)

    # Simulate strat_sequence score
    w_seq = config.weights.get("strat_sequence", 0.32)
    if win_rate >= config.high_ev_threshold:
        seq_conf = 95.0
    elif win_rate >= config.min_win_rate_for_ev:
        seq_conf = 65 + (win_rate - config.min_win_rate_for_ev) / (config.high_ev_threshold - config.min_win_rate_for_ev) * 30
    else:
        seq_conf = 30.0

    # Simulate timeframe_continuity (proxy: use ev_score)
    w_tf = config.weights.get("timeframe_continuity", 0.28)
    tf_conf = min(ev_score * 1.1, 95)

    # Simulate market_regime (proxy: neutral, as we don't have live regime here)
    w_reg = config.weights.get("market_regime", 0.18)
    reg_conf = 70.0    # assume neutral/mildly favorable regime in backtest

    # Simulate volume_confirmation (proxy: use avg_return as proxy for move quality)
    w_vol = config.weights.get("volume_confirmation", 0.10)
    vol_conf = min(50 + avg_return * 200, 90)

    # Time gate: prime hours assumed for historical
    w_time = config.weights.get("time_gate", 0.07)
    time_conf = 88.0

    # Options liquidity: assume tradeable tickers (synthetic data)
    w_opt = config.weights.get("options_liquidity", 0.05)
    opt_conf = 75.0

    # Weighted combination (simplified version of weighted_signal_combination)
    total_w = w_seq + w_tf + w_reg + w_vol + w_time + w_opt
    if total_w == 0:
        return {"passes": False, "confidence": 0}

    composite = (
        seq_conf * w_seq +
        tf_conf  * w_tf  +
        reg_conf * w_reg +
        vol_conf * w_vol +
        time_conf * w_time +
        opt_conf  * w_opt
    ) / total_w

    # Agreement modifier (how unanimous are sub-scores?)
    sub_scores = [seq_conf, tf_conf, reg_conf, vol_conf, time_conf, opt_conf]
    avg_sub = sum(sub_scores) / len(sub_scores)
    std_proxy = sum(abs(s - avg_sub) for s in sub_scores) / len(sub_scores)
    agreement = max(0, 1 - std_proxy / 50)
    final_conf = composite * (0.65 + 0.35 * agreement)
    final_conf = max(config.confidence_floor, min(final_conf, 100))

    passes = final_conf >= config.gate_threshold
    return {"passes": passes, "confidence": final_conf}


# ---------------------------------------------------------------------------
# Backtester
# ---------------------------------------------------------------------------

def backtest_config(config: EvolverConfig, setups: list[dict]) -> BacktestResult:
    """
    Run the simulated gate against all historical setups.
    Returns win rate, trade count, avg return of PASSING setups.
    """
    passing_setups = []
    blocked_count = 0

    for setup in setups:
        result = simulate_gate(setup, config)
        if result["passes"]:
            passing_setups.append(setup)
        else:
            blocked_count += 1

    trade_count = len(passing_setups)
    if trade_count == 0:
        return BacktestResult(
            config=config, win_rate=0, trade_count=0, avg_return=0,
            blocked_count=blocked_count, pass_through_rate=0,
            sharpe_proxy=0, status="discard",
            note="Gate blocked ALL signals — too restrictive"
        )

    win_rates = [s["win_rate"] for s in passing_setups]
    returns   = [s["avg_return"] for s in passing_setups]
    win_rate  = sum(win_rates) / len(win_rates)
    avg_ret   = sum(returns)   / len(returns)
    pass_rate = trade_count / len(setups)

    # Sharpe proxy: win_rate / variance of win_rates
    if len(win_rates) > 1:
        mean_wr = win_rate
        var     = sum((w - mean_wr) ** 2 for w in win_rates) / len(win_rates)
        std     = var ** 0.5
        sharpe  = (win_rate * avg_ret) / (std + 0.01)
    else:
        sharpe = win_rate * avg_ret

    # Oracle-gaming check (Anti-overfitting — from Auto-Quant)
    suspicious = (
        (win_rate > 0.95 and trade_count < MIN_TRADES_FOR_VALID_RESULT) or
        (pass_rate < 0.05 and win_rate > 0.90)   # passes almost nothing but claims high win rate
    )

    return BacktestResult(
        config=config,
        win_rate=win_rate,
        trade_count=trade_count,
        avg_return=avg_ret,
        blocked_count=blocked_count,
        pass_through_rate=pass_rate,
        sharpe_proxy=sharpe,
        suspicious=suspicious,
    )


# ---------------------------------------------------------------------------
# Mutation Engine
# ---------------------------------------------------------------------------

MUTATION_STRATEGIES = [
    "adjust_gate",              # ±2-8 on gate_threshold
    "shift_weight",             # move weight from one domain to another
    "amplify_top_weight",       # boost the highest-performing weight
    "penalize_bottom_weight",   # reduce the lowest-performing weight
    "adjust_ev_thresholds",     # tweak min_win_rate_for_ev / high_ev_threshold
    "reset_to_equal_weights",   # try flat weights — simplicity criterion
]


def mutate(config: EvolverConfig, mutation_type: Optional[str] = None) -> EvolverConfig:
    """
    Mutate a config by one of several strategies.
    Returns a NEW config (does not modify in place).
    """
    new = EvolverConfig(
        gate_threshold=config.gate_threshold,
        weights=dict(config.weights),
        min_win_rate_for_ev=config.min_win_rate_for_ev,
        high_ev_threshold=config.high_ev_threshold,
        confidence_floor=config.confidence_floor,
        generation=config.generation + 1,
    )

    strategy = mutation_type or random.choice(MUTATION_STRATEGIES)

    if strategy == "adjust_gate":
        delta = random.uniform(-8, 8)
        new.gate_threshold = round(max(40, min(85, new.gate_threshold + delta)), 1)

    elif strategy == "shift_weight":
        keys = list(new.weights.keys())
        src, dst = random.sample(keys, 2)
        shift = random.uniform(0.01, 0.08)
        if new.weights[src] - shift > 0.02:
            new.weights[src] = round(new.weights[src] - shift, 4)
            new.weights[dst] = round(new.weights[dst] + shift, 4)
        new.normalize_weights()

    elif strategy == "amplify_top_weight":
        top = max(new.weights, key=new.weights.get)
        others = [k for k in new.weights if k != top]
        boost = random.uniform(0.02, 0.06)
        new.weights[top] = round(new.weights[top] + boost, 4)
        drain_each = boost / len(others)
        for k in others:
            new.weights[k] = round(max(0.02, new.weights[k] - drain_each), 4)
        new.normalize_weights()

    elif strategy == "penalize_bottom_weight":
        bottom = min(new.weights, key=new.weights.get)
        others = [k for k in new.weights if k != bottom]
        reduction = random.uniform(0.01, 0.04)
        if new.weights[bottom] - reduction > 0.01:
            new.weights[bottom] = round(new.weights[bottom] - reduction, 4)
            boost_each = reduction / len(others)
            for k in others:
                new.weights[k] = round(new.weights[k] + boost_each, 4)
            new.normalize_weights()

    elif strategy == "adjust_ev_thresholds":
        new.min_win_rate_for_ev = round(random.uniform(0.55, 0.75), 2)
        new.high_ev_threshold   = round(random.uniform(0.80, 0.95), 2)
        if new.high_ev_threshold <= new.min_win_rate_for_ev:
            new.high_ev_threshold = new.min_win_rate_for_ev + 0.10

    elif strategy == "reset_to_equal_weights":
        # Simplicity criterion: try flat weights
        n = len(new.weights)
        flat = round(1.0 / n, 4)
        new.weights = {k: flat for k in new.weights}
        new.normalize_weights()

    return new


# ---------------------------------------------------------------------------
# Results Logger (mirrors Auto-Quant's results.tsv format)
# ---------------------------------------------------------------------------

def log_result(result: BacktestResult, experiment_id: int, description: str):
    """
    Append one row to results_evolver.tsv.
    Schema: id | win_rate | trade_count | avg_return | pass_rate | sharpe | gate | weights | status | note
    """
    file_exists = RESULTS_FILE.exists()
    with open(RESULTS_FILE, "a", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        if not file_exists:
            writer.writerow([
                "id", "win_rate", "trade_count", "avg_return", "pass_rate",
                "sharpe", "gate", "top_weights", "status", "note"
            ])
        top_weights = sorted(
            result.config.weights.items(), key=lambda x: x[1], reverse=True
        )[:3]
        top_weights_str = " | ".join(f"{k}={v:.2f}" for k, v in top_weights)
        writer.writerow([
            experiment_id,
            f"{result.win_rate:.4f}",
            result.trade_count,
            f"{result.avg_return:.4f}",
            f"{result.pass_through_rate:.3f}",
            f"{result.sharpe_proxy:.4f}",
            result.config.gate_threshold,
            top_weights_str,
            result.status,
            description,
        ])


# ---------------------------------------------------------------------------
# Main Evolution Loop (the Auto-Quant pattern)
# ---------------------------------------------------------------------------

def run_evolution_loop(
    source_csv: str = "ap_strat_setups.csv",
    rounds: int = 100,
    target_win_rate: float = 0.80,
    *,
    allow_outcome_leakage: bool = False,
):
    """
    Autonomous parameter evolution loop.

    LOOP FOREVER (up to `rounds` iterations):
        1. Mutate current best config
        2. Backtest against historical setups
        3. Decide keep or discard
        4. Log to results_evolver.tsv
        5. Repeat

    Per Auto-Quant: DO NOT stop to ask for confirmation.
    Keep iterating until interrupted or rounds exhausted.
    """
    if not allow_outcome_leakage:
        raise RuntimeError(
            "Legacy strategy evolver is blocked because simulate_gate() uses "
            "historical win_rate and avg_return as inputs. Pass "
            "allow_outcome_leakage=True only for reproducibility research; use "
            "ap.profitability_objective for promotion evidence."
        )

    logger.warning(
        "LEGACY LEAKY RESEARCH: results are not valid paper/live promotion evidence"
    )
    logger.info("="*60)
    logger.info("AP STRATEGY EVOLVER — Starting evolution loop")
    logger.info(f"Target: {target_win_rate:.0%} win rate | Rounds: {rounds}")
    logger.info("="*60)

    # Load historical data
    setups = load_historical_setups(source_csv)
    if not setups:
        logger.error("No setups loaded — cannot evolve")
        return

    # Initialize with baseline
    current_config = EvolverConfig(**{k: copy.deepcopy(v) for k, v in BASELINE_CONFIG.items()})
    baseline_result = backtest_config(current_config, setups)
    baseline_result.status = "keep"
    logger.info(
        f"BASELINE: win_rate={baseline_result.win_rate:.2%} | "
        f"trades={baseline_result.trade_count} | "
        f"avg_ret={baseline_result.avg_return:.2%} | "
        f"gate={current_config.gate_threshold}"
    )
    log_result(baseline_result, 0, f"baseline — gate={current_config.gate_threshold}")

    best_result = baseline_result
    best_config = current_config
    stagnation_count = 0
    STAGNATION_LIMIT = 5   # Force radical mutation after 5 stable rounds

    for i in range(1, rounds + 1):
        # Pick mutation strategy
        if stagnation_count >= STAGNATION_LIMIT:
            strategy = random.choice(["reset_to_equal_weights", "adjust_gate"])
            stagnation_count = 0
            logger.info(f"[{i:03d}] Stagnation — forcing radical mutation: {strategy}")
        else:
            strategy = None   # random

        candidate = mutate(best_config, mutation_type=strategy)
        result = backtest_config(candidate, setups)

        # Oracle-gaming detection (Auto-Quant's key insight)
        if result.suspicious:
            result.status = "discard"
            note = (
                f"SUSPICIOUS — win_rate={result.win_rate:.2%} but trades={result.trade_count} "
                f"(oracle-gaming, discarding)"
            )
            logger.warning(f"[{i:03d}] {note}")
            log_result(result, i, note)
            continue

        if not result.is_valid():
            result.status = "discard"
            note = f"Invalid — trade_count={result.trade_count} < {MIN_TRADES_FOR_VALID_RESULT} minimum"
            logger.info(f"[{i:03d}] DISCARD — {note}")
            log_result(result, i, note)
            continue

        # Keep/discard decision (LLM-style: read FULL result, not just one metric)
        score_delta = result.score() - best_result.score()
        win_rate_delta = result.win_rate - best_result.win_rate
        trade_delta = result.trade_count - best_result.trade_count

        is_improvement = (
            score_delta > 0 and
            result.trade_count >= MIN_TRADES_FOR_VALID_RESULT and
            not (win_rate_delta > 0.15 and abs(trade_delta) < 2)  # suspicious jump
        )

        if is_improvement:
            result.status = "keep"
            stagnation_count = 0
            note = (
                f"gate={candidate.gate_threshold} | "
                f"win={result.win_rate:.2%}(+{win_rate_delta:+.2%}) | "
                f"trades={result.trade_count} | "
                f"score_delta=+{score_delta:.1f}"
            )
            logger.info(f"[{i:03d}] ✅ KEEP — {note}")
            best_result = result
            best_config = candidate
        else:
            result.status = "discard"
            stagnation_count += 1
            note = (
                f"gate={candidate.gate_threshold} | "
                f"win={result.win_rate:.2%} | "
                f"score_delta={score_delta:+.1f} — no improvement"
            )
            logger.info(f"[{i:03d}] ❌ DISCARD — {note}")

        log_result(result, i, note)

        # Early exit if target hit
        if best_result.win_rate >= target_win_rate:
            logger.info(
                f"\n🎯 TARGET REACHED: {best_result.win_rate:.2%} win rate "
                f"at round {i}!"
            )
            break

    # Final report
    logger.info("\n" + "="*60)
    logger.info("EVOLUTION COMPLETE")
    logger.info("="*60)
    logger.info(f"Best win rate:    {best_result.win_rate:.2%}")
    logger.info(f"Best trade count: {best_result.trade_count}")
    logger.info(f"Best avg return:  {best_result.avg_return:.2%}")
    logger.info(f"Gate threshold:   {best_config.gate_threshold}")
    logger.info(f"Best weights:")
    for k, v in sorted(best_config.weights.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"  {k:<25}: {v:.3f}")
    logger.info(f"\nFull log: {RESULTS_FILE}")

    # Save best config as JSON for ap_strat_agent.py update
    best_config_path = Path("best_evolver_config.json")
    with open(best_config_path, "w") as f:
        json.dump({
            "achieved_win_rate": best_result.win_rate,
            "trade_count":       best_result.trade_count,
            "avg_return":        best_result.avg_return,
            "rounds_run":        rounds,
            "evolved_at":        datetime.now().isoformat(),
            "config":            best_config.to_dict(),
        }, f, indent=2)
    logger.info(f"Best config saved to: {best_config_path}")
    logger.warning(
        "Do not promote this config to paper or live thresholds; the simulator "
        "contains outcome leakage."
    )

    return best_config, best_result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AP Strategy Evolver — autonomous parameter optimizer")
    parser.add_argument("--source", default="ap_strat_setups.csv", help="Path to ap_strat_setups.csv")
    parser.add_argument("--rounds", type=int, default=100, help="Number of evolution rounds")
    parser.add_argument("--target", type=float, default=0.80, help="Target win rate (e.g. 0.80)")
    parser.add_argument(
        "--allow-outcome-leakage",
        action="store_true",
        help="Acknowledge this legacy simulator is invalid for policy promotion",
    )
    args = parser.parse_args()

    run_evolution_loop(
        source_csv=args.source,
        rounds=args.rounds,
        target_win_rate=args.target,
        allow_outcome_leakage=args.allow_outcome_leakage,
    )
