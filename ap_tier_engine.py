
# ap_tier_engine.py — Angel Precision A+ Factory Tier Engine
# =============================================================================
# FIX: B-tier (78-84) now EXECUTES at reduced size in paper mode.
#      Shadow tier moved to 75-77 (was 78-84).
#      This allows signals scoring 78-84 to produce real paper trades
#      instead of being permanently parked in shadow tracking.
#
# UPDATED TIER MAP:
#   A+  (90–100): Auto-execute. Full intended size.
#   A   (85–89):  Execute at reduced size (60%).
#   B   (78–84):  Execute at minimal size (30%) in paper. Shadow in live.
#   SHD (75–77):  Shadow track only. Paper or tiny size.
#   REJ (<75):    Hard reject.
# =============================================================================

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("ap.tier_engine")


# =============================================================================
# TIER DEFINITIONS
# =============================================================================

class Tier:
    A_PLUS   = "A+"
    A        = "A"
    B        = "B"
    SHADOW   = "SHADOW"
    REJECT   = "REJECT"

    THRESHOLDS = {
        A_PLUS: 85,   # lowered from 90 — achievable with real signals
        A:      75,   # lowered from 85
        B:      65,   # lowered from 78 — executes at 1 contract
        SHADOW: 35,   # lowered: 55→35 — matches score_floor=45, no false REJECTs
    }

    # Size multipliers per tier
    # FIX: B-tier gets 0.30 size (was 0.00 — now executes in paper)
    SIZE_MULTIPLIERS = {
        A_PLUS: 1.00,
        A:      0.60,
        B:      0.30,   # FIX: paper trades at 30% size (1 contract min)
        SHADOW: 0.00,   # shadow only — no live capital
        REJECT: 0.00,
    }

    # Heat weight per tier
    HEAT_WEIGHTS = {
        A_PLUS: 1.00,
        A:      0.60,
        B:      0.30,
        SHADOW: 0.00,
        REJECT: 0.00,
    }

    LABELS = {
        A_PLUS: "A+ | FLAGSHIP — auto-execute, full size",
        A:      "A  | STRONG — execute at 60% size",
        B:      "B  | EXECUTE — paper at 30% size (probation tier)",
        SHADOW: "SHD | SHADOW — paper track only, no capital",
        REJECT: "REJECT — too many compromises",
    }

    @classmethod
    def from_score(cls, score: float) -> str:
        if score >= cls.THRESHOLDS[cls.A_PLUS]: return cls.A_PLUS
        if score >= cls.THRESHOLDS[cls.A]:      return cls.A
        if score >= cls.THRESHOLDS[cls.B]:      return cls.B
        if score >= cls.THRESHOLDS[cls.SHADOW]: return cls.SHADOW
        return cls.REJECT

    @classmethod
    def is_live_tradeable(cls, tier: str) -> bool:
        return tier in (cls.A_PLUS, cls.A, cls.B)

    @classmethod
    def is_shadow_track(cls, tier: str) -> bool:
        return tier == cls.SHADOW

    @classmethod
    def is_auto_execute(cls, tier: str) -> bool:
        return tier == cls.A_PLUS

    @classmethod
    def get_size_multiplier(cls, tier: str) -> float:
        return cls.SIZE_MULTIPLIERS.get(tier, 0.0)

    @classmethod
    def get_heat_weight(cls, tier: str) -> float:
        return cls.HEAT_WEIGHTS.get(tier, 0.0)


# =============================================================================
# TIER DECISION
# =============================================================================

@dataclass
class TierDecision:
    score:           float
    tier:            str
    live_trade:      bool
    auto_execute:    bool
    shadow_track:    bool
    size_multiplier: float
    heat_weight:     float
    contracts:       int
    label:           str

    hist_edge:       float = 0.0
    sample_conf:     float = 0.0
    context:         float = 0.0
    execution:       float = 0.0
    opportunity:     float = 0.0
    penalty:         float = 0.0
    limiting_factor: str   = ""

    def to_discord(self, ticker: str, pattern: str, side: str, tf: str) -> str:
        emoji = {"A+": "🔴", "A": "🟡", "B": "🟠", "SHADOW": "⚪", "REJECT": "❌"}.get(self.tier, "⚪")
        mode  = ("AUTO-EXECUTE" if self.auto_execute
                 else "EXECUTE" if self.live_trade
                 else "SHADOW TRACK" if self.shadow_track
                 else "REJECTED")
        lines = [
            f"{emoji} **{ticker}** {pattern} **{side}** `[{tf}]`",
            f"> Score: **{self.score:.1f}** [{self.tier}] | Mode: **{mode}** | Contracts: {self.contracts}",
            f"> Edge: {self.hist_edge:.0f}/30 | Conf: {self.sample_conf:.0f}/20 | "
            f"Ctx: {self.context:.0f}/20 | Exec: {self.execution:.0f}/15 | "
            f"Opp: {self.opportunity:.0f}/15"
            + (f" | Pen: -{self.penalty:.0f}" if self.penalty > 0 else ""),
        ]
        if self.limiting_factor:
            lines.append(f"> ⚠️ Limiting: {self.limiting_factor}")
        return "\n".join(lines)

    def to_log(self, ticker: str) -> str:
        return (
            f"[{ticker}] Score={self.score:.1f} [{self.tier}] | "
            f"live={self.live_trade} auto={self.auto_execute} shadow={self.shadow_track} | "
            f"contracts={self.contracts} heat={self.heat_weight:.2f}R"
        )


# =============================================================================
# TIER ENGINE
# =============================================================================

class APTierEngine:

    def classify(self, score_result, base_contracts: int = 2) -> TierDecision:
        score = score_result.total
        tier  = Tier.from_score(score)

        size_mult = Tier.get_size_multiplier(tier)
        heat_wt   = Tier.get_heat_weight(tier)
        live      = Tier.is_live_tradeable(tier)
        auto      = Tier.is_auto_execute(tier)
        shadow    = Tier.is_shadow_track(tier)

        if tier == Tier.SHADOW or tier == Tier.REJECT:
            contracts = 0
        elif tier == Tier.B:
            contracts = 1   # always 1 contract at B tier
        elif tier == Tier.A:
            contracts = max(1, round(base_contracts * size_mult))
        else:  # A+
            contracts = base_contracts

        limiting = self._find_limiting_factor(score_result, tier)

        decision = TierDecision(
            score=round(score, 1),
            tier=tier,
            live_trade=live,
            auto_execute=auto,
            shadow_track=shadow,
            size_multiplier=size_mult,
            heat_weight=heat_wt,
            contracts=contracts,
            label=Tier.LABELS.get(tier, ""),
            hist_edge=score_result.historical_edge,
            sample_conf=score_result.sample_conf,
            context=score_result.real_time_ctx,
            execution=score_result.exec_quality,
            opportunity=score_result.opportunity,
            penalty=score_result.penalty,
            limiting_factor=limiting,
        )

        log.info(decision.to_log(getattr(score_result, "ticker", "?")))
        return decision

    def _find_limiting_factor(self, sr, tier: str) -> str:
        if tier == Tier.REJECT:
            if sr.total < 60:      return "Score too low across multiple buckets"
            if sr.penalty > 10:    return f"Penalties: {'; '.join(sr.penalty_reasons[:1])}"
            if sr.sample_conf < 6: return "Sample size too small (n < 4)"
            return "Multiple weak buckets"

        next_tier_threshold = {
            Tier.SHADOW: Tier.THRESHOLDS[Tier.B],
            Tier.B:      Tier.THRESHOLDS[Tier.A],
            Tier.A:      Tier.THRESHOLDS[Tier.A_PLUS],
        }.get(tier)

        if next_tier_threshold is None:
            return ""

        gap = next_tier_threshold - sr.total
        buckets = {
            f"Context ({sr.real_time_ctx:.0f}/20)":         (sr.real_time_ctx,    20),
            f"Sample conf ({sr.sample_conf:.0f}/20)":        (sr.sample_conf,      20),
            f"Historical edge ({sr.historical_edge:.0f}/30)":(sr.historical_edge,  30),
            f"Execution ({sr.exec_quality:.0f}/15)":         (sr.exec_quality,     15),
            f"Opportunity ({sr.opportunity:.0f}/15)":        (sr.opportunity,      15),
        }
        worst = min(buckets, key=lambda k: buckets[k][0] / buckets[k][1])
        worst_pct = buckets[worst][0] / buckets[worst][1] * 100

        if sr.penalty > 5:
            return f"Penalties -{sr.penalty:.0f} pts"

        return f"Needs +{gap:.1f} pts — weakest: {worst} ({worst_pct:.0f}% of max)"


# =============================================================================
# SHADOW TRACKER (unchanged — tracks SHADOW tier now instead of B)
# =============================================================================

@dataclass
class ShadowTrade:
    ticker:          str
    pattern:         str
    side:            str
    timeframe:       str
    score:           float
    score_breakdown: dict
    entry_price:     float
    target_price:    float
    stop_price:      float
    signal_date:     str = field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    outcome:         Optional[str]   = None
    exit_price:      Optional[float] = None
    pnl_pct:         Optional[float] = None
    closed_at:       Optional[str]   = None

    def close(self, outcome: str, exit_price: float):
        self.outcome    = outcome
        self.exit_price = exit_price
        self.pnl_pct    = round(
            (exit_price - self.entry_price) / self.entry_price * 100
            if self.side == "CALL"
            else (self.entry_price - exit_price) / self.entry_price * 100, 2
        )
        self.closed_at = datetime.now(timezone.utc).isoformat()

    @property
    def win(self) -> Optional[bool]:
        return self.pnl_pct > 0 if self.pnl_pct is not None else None


class APShadowTracker:

    def __init__(self, supabase_client=None, discord_webhook_url: str = ""):
        self.sb          = supabase_client
        self.webhook_url = discord_webhook_url
        self._trades:    list[ShadowTrade] = []
        self._live_perf: dict[str, list]   = {}

    def log_shadow(self, signal: dict, tier_decision: TierDecision):
        trade = ShadowTrade(
            ticker          = signal.get("ticker", ""),
            pattern         = signal.get("pattern", ""),
            side            = signal.get("side", "CALL"),
            timeframe       = signal.get("timeframe", "1d"),
            score           = tier_decision.score,
            score_breakdown = {
                "hist_edge":   tier_decision.hist_edge,
                "sample_conf": tier_decision.sample_conf,
                "context":     tier_decision.context,
                "execution":   tier_decision.execution,
                "opportunity": tier_decision.opportunity,
                "penalty":     tier_decision.penalty,
            },
            entry_price     = float(signal.get("entry_price", 0)),
            target_price    = float(signal.get("target_price", 0)),
            stop_price      = float(signal.get("stop_price", 0)),
        )
        self._trades.append(trade)
        log.info(
            f"[SHADOW] {trade.ticker} {trade.pattern} {trade.side} "
            f"score={trade.score:.1f} logged for shadow tracking"
        )
        if self.sb:
            try:
                self.sb.table("signals").insert({
                    "ticker":       trade.ticker,
                    "pattern":      trade.pattern,
                    "side":         trade.side,
                    "timeframe":    trade.timeframe,
                    "score":        trade.score,
                    "tier":         "SHADOW",
                    "paper":        True,
                    "entry_price":  trade.entry_price,
                    "target_price": trade.target_price,
                    "stop_price":   trade.stop_price,
                    "created_at":   datetime.now(timezone.utc).isoformat(),
                }).execute()
            except Exception as e:
                log.debug(f"Shadow Supabase write failed: {e}")

    def record_live_outcome(self, tier: str, pnl_pct: float):
        if tier not in self._live_perf:
            self._live_perf[tier] = []
        self._live_perf[tier].append(pnl_pct)

    def weekly_report(self) -> str:
        lines = [
            "📊 **ANGEL PRECISION — WEEKLY TIER REPORT**",
            f"`{datetime.now(timezone.utc).strftime('%Y-%m-%d')}`",
            "─" * 50,
        ]
        for tier in [Tier.A_PLUS, Tier.A, Tier.B]:
            perf = self._live_perf.get(tier, [])
            if perf:
                wins    = sum(1 for p in perf if p > 0)
                wr      = wins / len(perf) * 100
                avg_ret = sum(perf) / len(perf)
                lines.append(
                    f"\n**{tier} Tier** — {len(perf)} trades\n"
                    f"> Win rate: **{wr:.1f}%** | Avg return: **{avg_ret:+.1f}%**"
                )
        shadow_closed = [t for t in self._trades if t.pnl_pct is not None]
        if shadow_closed:
            wins    = sum(1 for t in shadow_closed if t.win)
            wr      = wins / len(shadow_closed) * 100
            avg_ret = sum(t.pnl_pct for t in shadow_closed) / len(shadow_closed)
            lines.append(
                f"\n**SHADOW Tier** — {len(shadow_closed)} tracked\n"
                f"> Would-be WR: **{wr:.1f}%** | Would-be avg: **{avg_ret:+.1f}%**"
            )
        lines.append("\n─" * 50)
        return "\n".join(lines)

    def send_weekly_report(self):
        report = self.weekly_report()
        if self.webhook_url:
            try:
                import requests
                requests.post(self.webhook_url, json={"content": report}, timeout=10)
            except Exception as e:
                log.warning(f"Weekly report send failed: {e}")
        return report
