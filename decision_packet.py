"""
decision_packet.py
Angel Precision — Canonical Decision Object

The single source of truth that flows from trade_interrogation_engine
to master_control, queue, execution, Discord, Supabase logs, and copilot.

Nothing downstream executes a trade without reading this packet first.
Master control remains the FINAL authority — this packet is advisory.

Statuses:
    EXECUTE   — all gates clear, high confidence, send to master control for approval
    WATCH     — setup valid but not yet actionable (time, regime, overextension)
    BLOCK     — hard block, do not queue
    ESCALATE  — conflicting signals, needs human review
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import json
import pytz

ET = pytz.timezone("America/New_York")


# ---------------------------------------------------------------------------
# Sub-objects
# ---------------------------------------------------------------------------

@dataclass
class RiskProfile:
    stop_price: Optional[float] = None
    target_price: Optional[float] = None
    reward_to_risk: Optional[float] = None
    stop_distance_pct: Optional[float] = None
    max_loss_dollars: Optional[float] = None
    position_size_contracts: Optional[int] = None

    def is_valid(self) -> bool:
        if self.reward_to_risk is None:
            return False
        return self.reward_to_risk >= 1.5   # minimum R:R threshold

    def to_dict(self) -> dict:
        return {
            "stop":         self.stop_price,
            "target":       self.target_price,
            "rr":           round(self.reward_to_risk, 2) if self.reward_to_risk else None,
            "stop_pct":     round(self.stop_distance_pct, 3) if self.stop_distance_pct else None,
            "max_loss":     self.max_loss_dollars,
            "size":         self.position_size_contracts,
            "rr_valid":     self.is_valid(),
        }


@dataclass
class BestContract:
    symbol: Optional[str] = None          # e.g. "NVDA250620C00900000"
    expiration: Optional[str] = None      # "2025-06-20"
    strike: Optional[float] = None
    option_type: Optional[str] = None     # "call" | "put"
    delta: Optional[float] = None
    iv_rank: Optional[float] = None
    open_interest: Optional[int] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    spread_pct: Optional[float] = None
    liquidity_ok: bool = False

    def to_dict(self) -> dict:
        return {
            "symbol":     self.symbol,
            "exp":        self.expiration,
            "strike":     self.strike,
            "type":       self.option_type,
            "delta":      self.delta,
            "ivr":        self.iv_rank,
            "oi":         self.open_interest,
            "bid":        self.bid,
            "ask":        self.ask,
            "spread_pct": round(self.spread_pct, 3) if self.spread_pct else None,
            "liquid":     self.liquidity_ok,
        }


@dataclass
class ReentryCondition:
    """What exact condition makes a WATCH/BLOCK trade actionable."""
    trigger_type: str = ""      # "price", "time", "regime", "indicator"
    description: str = ""       # human-readable
    trigger_value: Optional[float] = None
    trigger_direction: str = ""  # "above" | "below" | "equals"

    def to_dict(self) -> dict:
        return {
            "type":      self.trigger_type,
            "condition": self.description,
            "value":     self.trigger_value,
            "direction": self.trigger_direction,
        }


# ---------------------------------------------------------------------------
# Status constants
# ---------------------------------------------------------------------------

class DecisionStatus:
    EXECUTE  = "EXECUTE"
    WATCH    = "WATCH"
    BLOCK    = "BLOCK"
    ESCALATE = "ESCALATE"


# ---------------------------------------------------------------------------
# Main Packet
# ---------------------------------------------------------------------------

@dataclass
class DecisionPacket:
    """
    The canonical output of trade_interrogation_engine.
    Everything downstream reads THIS — not raw scanner data.
    """

    # Identity
    ticker: str
    pattern: str                   # e.g. "2D→2U→1 on 4D"
    scanner_source: str            # which scanner fired this
    signal_direction: str          # "bullish" | "bearish"

    # Decision
    status: str = DecisionStatus.BLOCK
    actionable: bool = False
    quality_score: float = 0.0    # 0-100 composite
    confidence: float = 0.0       # from ap_strat_agent

    # Reasoning
    why: list = field(default_factory=list)           # list of reason strings
    blocked_by: list = field(default_factory=list)    # what specifically blocked it
    warnings: list = field(default_factory=list)      # soft flags, not hard blocks

    # Trade details (only populated if EXECUTE or WATCH)
    risk: Optional[RiskProfile] = None
    best_contract: Optional[BestContract] = None
    reentry_condition: Optional[ReentryCondition] = None

    # Context snapshot at time of decision
    spy_trend: str = "UNKNOWN"
    vix_level: Optional[float] = None
    relative_volume: Optional[float] = None
    regime_label: str = ""

    # Strategy fit flags
    strategy_fit: bool = True
    strategy_fit_notes: str = ""

    # Portfolio context
    sector: str = ""
    sector_exposure_ok: bool = True
    correlation_conflict: bool = False
    existing_position: bool = False

    # Event risk
    earnings_within_days: Optional[int] = None
    has_catalyst: bool = False
    catalyst_note: str = ""

    # Paper vs live
    paper_trade_ok: bool = False
    live_trade_ok: bool = False

    # Metadata
    timestamp: str = field(default_factory=lambda: datetime.now(ET).isoformat())
    interrogation_ms: Optional[int] = None   # how long evaluation took
    packet_version: str = "1.0"

    # Raw strat agent signal (for audit)
    strat_agent_output: dict = field(default_factory=dict)

    # ---------------------------------------------------------------------------
    # Derived helpers
    # ---------------------------------------------------------------------------

    def is_executable(self) -> bool:
        return (
            self.status == DecisionStatus.EXECUTE
            and self.actionable
            and self.quality_score >= 70
            and (self.risk is None or self.risk.is_valid())
        )

    def needs_human(self) -> bool:
        return self.status == DecisionStatus.ESCALATE

    def get_discord_summary(self) -> str:
        """One-line Discord alert string."""
        emoji = {
            DecisionStatus.EXECUTE:  "🟢",
            DecisionStatus.WATCH:    "👁️",
            DecisionStatus.BLOCK:    "🔴",
            DecisionStatus.ESCALATE: "⚠️",
        }.get(self.status, "❓")

        line = (
            f"{emoji} **{self.ticker}** | {self.status} | "
            f"{self.signal_direction.upper()} | {self.pattern} | "
            f"score={self.quality_score:.0f} conf={self.confidence:.0f}%"
        )
        if self.reentry_condition and self.status == DecisionStatus.WATCH:
            line += f"\n   ↳ Watch for: {self.reentry_condition.description}"
        if self.blocked_by and self.status == DecisionStatus.BLOCK:
            line += f"\n   ↳ Blocked: {' | '.join(self.blocked_by[:2])}"
        return line

    def get_copilot_explanation(self) -> str:
        """Human-readable explanation for copilot/client queries."""
        lines = [
            f"**{self.ticker}** — {self.status}",
            f"Direction: {self.signal_direction.upper()} | Pattern: {self.pattern}",
            f"Quality: {self.quality_score:.0f}/100 | Confidence: {self.confidence:.0f}%",
            f"Regime: {self.regime_label}",
            "",
        ]
        if self.why:
            lines.append("**Reasons:**")
            lines.extend([f"  • {r}" for r in self.why])
        if self.blocked_by:
            lines.append("\n**Blocked by:**")
            lines.extend([f"  ✗ {b}" for b in self.blocked_by])
        if self.warnings:
            lines.append("\n**Warnings:**")
            lines.extend([f"  ⚠ {w}" for w in self.warnings])
        if self.reentry_condition:
            lines.append(f"\n**Re-entry trigger:** {self.reentry_condition.description}")
        if self.risk and self.risk.stop_price:
            lines.append(f"\n**Risk:** Stop={self.risk.stop_price} | Target={self.risk.target_price} | R:R={self.risk.reward_to_risk:.1f}")
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "ticker":           self.ticker,
            "pattern":          self.pattern,
            "scanner_source":   self.scanner_source,
            "direction":        self.signal_direction,
            "status":           self.status,
            "actionable":       self.actionable,
            "quality_score":    round(self.quality_score, 1),
            "confidence":       round(self.confidence, 1),
            "live_trade_ok":    self.live_trade_ok,
            "paper_trade_ok":   self.paper_trade_ok,
            "why":              self.why,
            "blocked_by":       self.blocked_by,
            "warnings":         self.warnings,
            "risk":             self.risk.to_dict() if self.risk else None,
            "best_contract":    self.best_contract.to_dict() if self.best_contract else None,
            "reentry":          self.reentry_condition.to_dict() if self.reentry_condition else None,
            "context": {
                "spy_trend":    self.spy_trend,
                "vix":          self.vix_level,
                "rvol":         self.relative_volume,
                "regime":       self.regime_label,
            },
            "strategy_fit":     self.strategy_fit,
            "event_risk": {
                "earnings_days": self.earnings_within_days,
                "has_catalyst":  self.has_catalyst,
                "note":          self.catalyst_note,
            },
            "portfolio": {
                "sector":             self.sector,
                "sector_ok":          self.sector_exposure_ok,
                "correlation_issue":  self.correlation_conflict,
                "existing_position":  self.existing_position,
            },
            "strat_agent":      self.strat_agent_output,
            "timestamp":        self.timestamp,
            "packet_version":   self.packet_version,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_supabase_row(self) -> dict:
        """Flat dict for Supabase `decision_log` table insert."""
        return {
            "ticker":         self.ticker,
            "pattern":        self.pattern,
            "scanner_source": self.scanner_source,
            "direction":      self.signal_direction,
            "status":         self.status,
            "quality_score":  self.quality_score,
            "confidence":     self.confidence,
            "live_ok":        self.live_trade_ok,
            "paper_ok":       self.paper_trade_ok,
            "spy_trend":      self.spy_trend,
            "vix":            self.vix_level,
            "regime":         self.regime_label,
            "blocked_by":     json.dumps(self.blocked_by),
            "why":            json.dumps(self.why),
            "reentry":        self.reentry_condition.description if self.reentry_condition else None,
            "rr":             self.risk.reward_to_risk if self.risk else None,
            "created_at":     self.timestamp,
        }
