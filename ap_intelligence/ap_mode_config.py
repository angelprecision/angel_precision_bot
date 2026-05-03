"""
ap_mode_config.py — Angel Precision Execution Mode Configuration
================================================================
Two modes. One switch. No architecture changes.

PRODUCTION  — Elite filters. Protect capital. Client-ready.
RESEARCH    — Wider intake. Collect intraday data. Learn fast.

Set via:
  1. Environment variable:  AP_MODE=research   (or "production")
  2. Direct instantiation:  APModeConfig(mode="research")
  3. Pipeline param:        pipeline.run(..., mode="research")

WHAT LOOSENS IN RESEARCH MODE
──────────────────────────────
  Score threshold:    60 → 50  (more setups enter execution tier)
  Score tiers:        New 50-54 LOG_ONLY + 55-64 MICRO_PILOT added
  Fundamentals weight:halved for intraday (swing filter, less relevant)
  OI floor:          500 → 250
  Volume floor:      100 → 50
  Delta min:         0.25 → 0.20  (slightly wider)
  Max spread:        UNCHANGED (bad spreads poison data)

WHAT NEVER LOOSENS
──────────────────
  Daily loss kill switch     (-5% day = stop)
  Max risk per trade         (2% account)
  SPY/VIX regime awareness   (still tracked + scored, not hard-blocked in research)
  Gross exposure caps        (sector + direction limits)
  Spread protection          (10% max, always)
  Position sizing discipline (contracts still calculated from ATR stop + delta)

WHY THIS DESIGN
───────────────
  You don't need a worse bot.  You need a wider intake mode.
  Research mode feeds your audit log with more real decisions so you can
  tune the system with evidence instead of guessing.

  After 2-4 weeks of research data, run ap_backtest_comparison.py against
  your audit log — it will tell you exactly where to set the production
  thresholds for your live intraday flow.
"""

from __future__ import annotations
import os
from dataclasses import dataclass, field
from typing import Literal


ModeType = Literal["production", "research"]


# ─────────────────────────────────────────────────────────────────────────────
# PRODUCTION CONFIG  (current defaults — what you had before)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ScoreTier:
    """Single size tier entry."""
    min_score:         float
    size_multiplier:   float
    label:             str
    confidence_bucket: str
    live_trade:        bool  = True   # False = paper/log only


# Production tiers — strict, capital-protecting
PRODUCTION_TIERS = [
    ScoreTier(min_score=90, size_multiplier=1.00, label="full",    confidence_bucket="A+",   live_trade=True),
    ScoreTier(min_score=75, size_multiplier=0.75, label="reduced", confidence_bucket="A",    live_trade=True),
    ScoreTier(min_score=60, size_multiplier=0.50, label="pilot",   confidence_bucket="B",    live_trade=True),
    # Anything below 60 → skip (implicit — not in list)
]

# Research tiers — wider intake, log everything
RESEARCH_TIERS = [
    ScoreTier(min_score=90, size_multiplier=1.00, label="full",        confidence_bucket="A+",      live_trade=True),
    ScoreTier(min_score=75, size_multiplier=0.75, label="reduced",     confidence_bucket="A",       live_trade=True),
    ScoreTier(min_score=65, size_multiplier=0.50, label="pilot",       confidence_bucket="B",       live_trade=True),
    ScoreTier(min_score=55, size_multiplier=0.25, label="micro_pilot", confidence_bucket="C",       live_trade=True),
    ScoreTier(min_score=50, size_multiplier=0.00, label="log_only",    confidence_bucket="D",       live_trade=False),
    # Anything below 50 → skip
]


@dataclass
class ContractConfig:
    """Contract quality gate thresholds."""
    max_spread_pct:    float  # Bid/ask spread as fraction of mid
    min_open_interest: int
    min_daily_volume:  int
    min_delta:         float
    max_delta:         float
    min_dte:           int


PRODUCTION_CONTRACT_CONFIG = ContractConfig(
    max_spread_pct    = 0.10,
    min_open_interest = 500,
    min_daily_volume  = 100,
    min_delta         = 0.25,
    max_delta         = 0.70,
    min_dte           = 1,
)

RESEARCH_CONTRACT_CONFIG = ContractConfig(
    max_spread_pct    = 0.10,   # NEVER loosened — bad spreads = bad data
    min_open_interest = 250,    # 500 → 250
    min_daily_volume  = 50,     # 100 → 50
    min_delta         = 0.20,   # 0.25 → 0.20  (slightly wider)
    max_delta         = 0.75,   # 0.70 → 0.75  (allows slightly deeper calls)
    min_dte           = 1,
)


@dataclass
class ScorecardWeights:
    """
    Scorecard dimension weights (must sum to 100).
    In research/intraday mode, fundamentals weight is halved
    (it's a swing trade filter — less relevant for 60min setups).
    The freed weight goes to contract quality (execution reality).
    """
    setup_quality:    float  # Scanner signal
    technical_align:  float  # Technical agent
    regime_fit:       float  # SPY/VIX regime
    contract_quality: float  # Spread/OI/volume/delta/DTE
    fundamentals:     float  # Profitability/growth/health
    sentiment:        float  # Tie-breaker

    def validate(self):
        total = (self.setup_quality + self.technical_align + self.regime_fit +
                 self.contract_quality + self.fundamentals + self.sentiment)
        assert abs(total - 100) < 0.01, f"Weights must sum to 100, got {total}"


PRODUCTION_WEIGHTS = ScorecardWeights(
    setup_quality    = 30,
    technical_align  = 25,
    regime_fit       = 15,
    contract_quality = 15,
    fundamentals     = 10,
    sentiment        = 5,
)

RESEARCH_WEIGHTS = ScorecardWeights(
    setup_quality    = 30,   # Unchanged — scanner is the foundation
    technical_align  = 25,   # Unchanged — price action is still the edge
    regime_fit       = 15,   # Unchanged — still tracked, still penalizes bad regime
    contract_quality = 20,   # +5 — intraday lives or dies on contract quality
    fundamentals     = 5,    # -5 — swing filter, less relevant intraday
    sentiment        = 5,    # Unchanged — still tie-breaker only
)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN CONFIG CLASS
# ─────────────────────────────────────────────────────────────────────────────

class APModeConfig:
    """
    Angel Precision mode configuration.

    Usage
    ─────
        # From env variable (set AP_MODE=research in GitHub Secrets / Render)
        cfg = APModeConfig()

        # Explicit
        cfg = APModeConfig(mode="research")
        cfg = APModeConfig(mode="production")

        # Access
        cfg.tiers           → list[ScoreTier]
        cfg.contracts        → ContractConfig
        cfg.weights          → ScorecardWeights
        cfg.skip_threshold   → float  (minimum score to take any action)
        cfg.is_research      → bool
        cfg.is_production    → bool
    """

    def __init__(self, mode: ModeType = None):
        # Priority: explicit arg > env var > default (production)
        if mode is None:
            env_mode = os.environ.get("AP_MODE", "production").lower().strip()
            mode = "research" if env_mode == "research" else "production"

        if mode not in ("production", "research"):
            raise ValueError(f"mode must be 'production' or 'research', got: {mode!r}")

        self.mode: ModeType = mode

    # ── Properties ────────────────────────────────────────────────────────

    @property
    def is_research(self) -> bool:
        return self.mode == "research"

    @property
    def is_production(self) -> bool:
        return self.mode == "production"

    @property
    def tiers(self) -> list[ScoreTier]:
        return RESEARCH_TIERS if self.is_research else PRODUCTION_TIERS

    @property
    def contracts(self) -> ContractConfig:
        return RESEARCH_CONTRACT_CONFIG if self.is_research else PRODUCTION_CONTRACT_CONFIG

    @property
    def weights(self) -> ScorecardWeights:
        return RESEARCH_WEIGHTS if self.is_research else PRODUCTION_WEIGHTS

    @property
    def skip_threshold(self) -> float:
        """Minimum score to take any action (log or trade)."""
        return 50.0 if self.is_research else 60.0

    @property
    def live_threshold(self) -> float:
        """Minimum score to take a live trade (not just log)."""
        return 55.0 if self.is_research else 60.0

    # ── Tier lookup ────────────────────────────────────────────────────────

    def get_tier(self, score: float) -> ScoreTier | None:
        """
        Return the matching ScoreTier for a given score.
        Returns None if score is below skip_threshold.
        """
        if score < self.skip_threshold:
            return None
        for tier in sorted(self.tiers, key=lambda t: t.min_score, reverse=True):
            if score >= tier.min_score:
                return tier
        return None

    def should_execute(self, score: float) -> bool:
        """True if this score should result in a live trade."""
        tier = self.get_tier(score)
        return tier is not None and tier.live_trade and tier.size_multiplier > 0

    def should_log(self, score: float) -> bool:
        """True if this score should at least be logged (even if no trade)."""
        return score >= self.skip_threshold

    # ── Summary ────────────────────────────────────────────────────────────

    def summary(self) -> str:
        lines = [
            f"AP Mode: {self.mode.upper()}",
            f"  Skip below:     {self.skip_threshold}",
            f"  Live trade at:  {self.live_threshold}+",
            f"  Tiers:",
        ]
        for t in self.tiers:
            trade_label = "LIVE TRADE" if t.live_trade else "LOG ONLY"
            lines.append(f"    {t.min_score}+ → {t.label} ({t.size_multiplier*100:.0f}% size) [{trade_label}]")
        lines += [
            f"  Contract gates:",
            f"    Spread ≤ {self.contracts.max_spread_pct*100:.0f}%",
            f"    OI ≥ {self.contracts.min_open_interest}",
            f"    Volume ≥ {self.contracts.min_daily_volume}",
            f"    Delta {self.contracts.min_delta}–{self.contracts.max_delta}",
            f"  Weights (out of 100):",
            f"    Setup {self.weights.setup_quality} | Tech {self.weights.technical_align} | "
            f"Regime {self.weights.regime_fit} | Contract {self.weights.contract_quality} | "
            f"Fund {self.weights.fundamentals} | Sentiment {self.weights.sentiment}",
        ]
        return "\n".join(lines)

    def __repr__(self):
        return f"APModeConfig(mode={self.mode!r})"


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE SINGLETONS  (import these directly if you don't need to switch)
# ─────────────────────────────────────────────────────────────────────────────

PRODUCTION_MODE = APModeConfig(mode="production")
RESEARCH_MODE   = APModeConfig(mode="research")


# ─────────────────────────────────────────────────────────────────────────────
# CLI — print mode summary
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "─"*52)
    print(PRODUCTION_MODE.summary())
    print("─"*52)
    print(RESEARCH_MODE.summary())
    print("─"*52 + "\n")
