# ap_tier_engine.py — Angel Precision A+ Factory Tier Engine
# =============================================================================
# The single source of truth for how every signal is classified and sized.
#
# TIER MAP:
#   A+  (90–100): Auto-execute. Full intended size. These are your flagship trades.
#   A   (85–89):  Execute at reduced size. Very good but one thing is slightly off.
#   B   (78–84):  Shadow track ONLY. Paper or tiny size. Research tier / probation.
#   REJ (<78):    Hard reject. Never enters the pipeline.
#
# SIZING RULES:
#   A+: base_contracts × spread_modifier × feedback_modifier      (full)
#   A:  above × 0.60                                              (reduced)
#   B:  1 contract max, paper mode only, no live capital          (shadow)
#
# HEAT CAPS (per tier):
#   A+: uses full heat allocation
#   A:  counted at 0.6× heat weight
#   B:  not counted toward live heat (paper only)
#
# This file is the ONLY place where tier logic lives.
# Everything else (execution_core, allocator, scanner) imports from here.
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
    A_PLUS   = "A+"    # 90+
    A        = "A"     # 85–89
    B        = "B"     # 78–84  (shadow tier)
    REJECT   = "REJECT"

    THRESHOLDS = {
        A_PLUS: 90,
        A:      85,
        B:      78,
    }

    # Size multipliers per tier (applied on top of spread/feedback modifiers)
    SIZE_MULTIPLIERS = {
        A_PLUS: 1.00,
        A:      0.60,
        B:      0.00,   # no live size — paper only
        REJECT: 0.00,
    }

    # Heat weight per tier (A+ uses full 1R, A uses 0.6R, B uses 0)
    HEAT_WEIGHTS = {
        A_PLUS: 1.00,
        A:      0.60,
        B:      0.00,
        REJECT: 0.00,
    }

    # Human labels
    LABELS = {
        A_PLUS: "A+ | FLAGSHIP — auto-execute, full size",
        A:      "A  | STRONG — execute at 60% size",
        B:      "B  | SHADOW — paper track only, no live capital",
        REJECT: "REJECT — too many compromises",
    }

    @classmethod
    def from_score(cls, score: float) -> str:
        if score >= cls.THRESHOLDS[cls.A_PLUS]: return cls.A_PLUS
        if score >= cls.THRESHOLDS[cls.A]:      return cls.A
        if score >= cls.THRESHOLDS[cls.B]:      return cls.B
        return cls.REJECT

    @classmethod
    def is_live_tradeable(cls, tier: str) -> bool:
        return tier in (cls.A_PLUS, cls.A)

    @classmethod
    def is_shadow_track(cls, tier: str) -> bool:
        return tier == cls.B

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
# TIER DECISION — full picture per signal
# =============================================================================

@dataclass
class TierDecision:
    score:          float
    tier:           str
    live_trade:     bool
    auto_execute:   bool
    shadow_track:   bool
    size_multiplier: float
    heat_weight:    float
    contracts:      int
    label:          str

    # Score breakdown for display
    hist_edge:      float = 0.0
    sample_conf:    float = 0.0
    context:        float = 0.0
    execution:      float = 0.0
    opportunity:    float = 0.0
    penalty:        float = 0.0

    # What's holding it back from A+ (if applicable)
    limiting_factor: str = ""

    def to_discord(self, ticker: str, pattern: str, side: str, tf: str) -> str:
        emoji = {"A+": "🔴", "A": "🟡", "B": "⚪", "REJECT": "❌"}.get(self.tier, "⚪")
        mode  = ("AUTO-EXECUTE" if self.auto_execute
                 else "CONFIRM" if self.live_trade
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
    """
    Classifies every scored signal into a tier and determines:
      - live trade vs shadow track vs reject
      - contract count
      - heat weight for allocator
      - what's limiting it from the next tier

    Usage:
        tier_engine = APTierEngine()
        decision = tier_engine.classify(score_result, base_contracts=3)
    """

    def classify(self, score_result, base_contracts: int = 2) -> TierDecision:
        """
        Classify a ScoreResult into a TierDecision.
        score_result: ScoreResult from APScorer
        base_contracts: from _get_base_contracts() in execution core
        """
        score = score_result.total
        tier  = Tier.from_score(score)

        size_mult  = Tier.get_size_multiplier(tier)
        heat_wt    = Tier.get_heat_weight(tier)
        live       = Tier.is_live_tradeable(tier)
        auto       = Tier.is_auto_execute(tier)
        shadow     = Tier.is_shadow_track(tier)

        # Contract count
        if tier == Tier.B:
            contracts = 1    # shadow: 1 paper contract only
        elif tier == Tier.A:
            contracts = max(1, round(base_contracts * size_mult))
        elif tier == Tier.A_PLUS:
            contracts = base_contracts
        else:
            contracts = 0

        # What's limiting it from the next tier
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
        """
        Identify the single biggest thing holding this signal back from the next tier.
        Useful for Discord output and self-improvement feedback.
        """
        if tier == Tier.REJECT:
            if sr.total < 60:      return "Score too low across multiple buckets"
            if sr.penalty > 10:    return f"Penalties: {'; '.join(sr.penalty_reasons[:1])}"
            if sr.sample_conf < 6: return "Sample size too small (n < 4)"
            return "Multiple weak buckets"

        next_tier_threshold = {
            Tier.B:  Tier.THRESHOLDS[Tier.A],
            Tier.A:  Tier.THRESHOLDS[Tier.A_PLUS],
        }.get(tier)

        if next_tier_threshold is None:
            return ""   # A+ — nothing above

        gap = next_tier_threshold - sr.total

        # Find weakest bucket relative to max possible
        buckets = {
            f"Context ({sr.real_time_ctx:.0f}/20)":      (sr.real_time_ctx,    20),
            f"Sample confidence ({sr.sample_conf:.0f}/20)": (sr.sample_conf,  20),
            f"Historical edge ({sr.historical_edge:.0f}/30)": (sr.historical_edge, 30),
            f"Execution ({sr.exec_quality:.0f}/15)":      (sr.exec_quality,    15),
            f"Opportunity ({sr.opportunity:.0f}/15)":     (sr.opportunity,     15),
        }

        worst_label = min(buckets, key=lambda k: buckets[k][0] / buckets[k][1])
        worst_pct   = buckets[worst_label][0] / buckets[worst_label][1] * 100

        if sr.penalty > 5:
            return f"Penalties -{sr.penalty:.0f} pts ({'; '.join(sr.penalty_reasons[:1])})"

        return f"Needs +{gap:.1f} pts — weakest: {worst_label} ({worst_pct:.0f}% of max)"


# =============================================================================
# SHADOW TRACKER
# =============================================================================

@dataclass
class ShadowTrade:
    """A B-tier trade being paper-tracked."""
    ticker:       str
    pattern:      str
    side:         str
    timeframe:    str
    score:        float
    score_breakdown: dict
    entry_price:  float
    target_price: float
    stop_price:   float
    signal_date:  str = field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d"))

    # Outcome (filled when closed)
    outcome:      Optional[str]   = None   # "TARGET", "STOP", "EXPIRE", "TIME"
    exit_price:   Optional[float] = None
    pnl_pct:      Optional[float] = None
    closed_at:    Optional[str]   = None

    def close(self, outcome: str, exit_price: float):
        self.outcome   = outcome
        self.exit_price = exit_price
        self.pnl_pct   = round((exit_price - self.entry_price) / self.entry_price * 100
                               if self.side == "CALL"
                               else (self.entry_price - exit_price) / self.entry_price * 100, 2)
        self.closed_at = datetime.now(timezone.utc).isoformat()

    @property
    def win(self) -> Optional[bool]:
        return self.pnl_pct > 0 if self.pnl_pct is not None else None


class APShadowTracker:
    """
    Tracks B-tier (78–84) trades in paper mode.
    After 5 trading days, generates a performance report comparing:
      - B tier win rate vs A tier win rate
      - Average option return by tier
      - Which B setups are graduating toward A
      - Which B setups are junk and should be removed
    """

    def __init__(self, supabase_client=None, discord_webhook_url: str = ""):
        self.sb          = supabase_client
        self.webhook_url = discord_webhook_url
        self._trades:    list[ShadowTrade] = []
        self._live_perf: dict[str, list]   = {}   # tier → [pnl_pcts]

    def log_shadow(self, signal: dict, tier_decision: TierDecision):
        """Log a B-tier signal for paper tracking."""
        trade = ShadowTrade(
            ticker        = signal.get("ticker",""),
            pattern       = signal.get("pattern",""),
            side          = signal.get("side","CALL"),
            timeframe     = signal.get("timeframe","1d"),
            score         = tier_decision.score,
            score_breakdown = {
                "hist_edge":   tier_decision.hist_edge,
                "sample_conf": tier_decision.sample_conf,
                "context":     tier_decision.context,
                "execution":   tier_decision.execution,
                "opportunity": tier_decision.opportunity,
                "penalty":     tier_decision.penalty,
            },
            entry_price   = float(signal.get("entry_price", 0)),
            target_price  = float(signal.get("target_price", 0)),
            stop_price    = float(signal.get("stop_price", 0)),
        )
        self._trades.append(trade)
        log.info(
            f"[SHADOW] {trade.ticker} {trade.pattern} {trade.side} "
            f"score={trade.score:.1f} logged for paper tracking"
        )

        # Write to Supabase signals table with paper flag
        if self.sb:
            try:
                self.sb.table("signals").insert({
                    "ticker":       trade.ticker,
                    "pattern":      trade.pattern,
                    "side":         trade.side,
                    "timeframe":    trade.timeframe,
                    "score":        trade.score,
                    "tier":         "B",
                    "paper":        True,
                    "entry_price":  trade.entry_price,
                    "target_price": trade.target_price,
                    "stop_price":   trade.stop_price,
                    "created_at":   datetime.now(timezone.utc).isoformat(),
                }).execute()
            except Exception as e:
                log.debug(f"Shadow Supabase write failed: {e}")

    def record_live_outcome(self, tier: str, pnl_pct: float):
        """Record a live trade outcome for tier comparison."""
        if tier not in self._live_perf:
            self._live_perf[tier] = []
        self._live_perf[tier].append(pnl_pct)

    def weekly_report(self) -> str:
        """
        Generate the weekly performance comparison report.
        Run this every Friday after close.
        """
        lines = [
            "📊 **ANGEL PRECISION — WEEKLY TIER REPORT**",
            f"`{datetime.now(timezone.utc).strftime('%Y-%m-%d')}`",
            "─" * 50,
        ]

        # Live tier comparison
        for tier in [Tier.A_PLUS, Tier.A]:
            perf = self._live_perf.get(tier, [])
            if perf:
                wins    = sum(1 for p in perf if p > 0)
                wr      = wins / len(perf) * 100
                avg_ret = sum(perf) / len(perf)
                lines.append(
                    f"\n**{tier} Tier** — {len(perf)} trades\n"
                    f"> Win rate: **{wr:.1f}%** | Avg return: **{avg_ret:+.1f}%**\n"
                    f"> Wins: {wins} | Losses: {len(perf)-wins}"
                )

        # Shadow tier (B) — what would have happened
        shadow_closed = [t for t in self._trades if t.pnl_pct is not None]
        if shadow_closed:
            wins    = sum(1 for t in shadow_closed if t.win)
            wr      = wins / len(shadow_closed) * 100
            avg_ret = sum(t.pnl_pct for t in shadow_closed) / len(shadow_closed)
            lines.append(
                f"\n**B Tier (Shadow)** — {len(shadow_closed)} tracked\n"
                f"> Would-be win rate: **{wr:.1f}%** | Would-be avg: **{avg_ret:+.1f}%**\n"
                f"> ⚠️ These were NOT traded with real capital"
            )

            # Graduating setups (B trades that scored well AND would have won)
            graduating = [
                t for t in shadow_closed
                if t.win and t.score >= 82
            ]
            if graduating:
                lines.append(f"\n🟢 **Potential graduates to A tier** ({len(graduating)} setups):")
                for t in sorted(graduating, key=lambda x: x.score, reverse=True)[:5]:
                    lines.append(f"  • {t.ticker} {t.pattern} {t.side} | score={t.score:.0f} | return={t.pnl_pct:+.1f}%")

            # Confirmed junk (losing AND low context)
            junk = [
                t for t in shadow_closed
                if not t.win and t.score_breakdown.get("context", 20) < 12
            ]
            if junk:
                lines.append(f"\n🔴 **Confirmed junk** ({len(junk)} setups — do not promote):")
                for t in sorted(junk, key=lambda x: x.pnl_pct or 0)[:5]:
                    lines.append(f"  • {t.ticker} {t.pattern} {t.side} | score={t.score:.0f} | return={t.pnl_pct:+.1f}%")

        # A+ vs B comparison insight
        a_plus_perf = self._live_perf.get(Tier.A_PLUS, [])
        b_perf      = [t.pnl_pct for t in shadow_closed if t.pnl_pct is not None]
        if a_plus_perf and b_perf:
            a_wr = sum(1 for p in a_plus_perf if p > 0) / len(a_plus_perf) * 100
            b_wr = sum(1 for p in b_perf if p > 0) / len(b_perf) * 100
            lines.append(
                f"\n**KEY INSIGHT**\n"
                f"> A+ live win rate: **{a_wr:.1f}%** ({len(a_plus_perf)} trades)\n"
                f"> B shadow win rate: **{b_wr:.1f}%** ({len(b_perf)} tracked)\n"
                + ("> ✅ Threshold is correctly set — A+ outperforms B" if a_wr > b_wr + 5
                   else "> ⚠️ B tier close to A+ — consider re-examining threshold")
            )

        lines.append("\n─" * 50)
        return "\n".join(lines)

    def send_weekly_report(self):
        """Send weekly report to Discord."""
        report = self.weekly_report()
        if self.webhook_url:
            try:
                import requests
                requests.post(self.webhook_url, json={"content": report}, timeout=10)
            except Exception as e:
                log.warning(f"Weekly report send failed: {e}")
        else:
            print(report)
        return report


# =============================================================================
# STANDALONE DEMO — shows how the tiers work in practice
# =============================================================================

if __name__ == "__main__":
    from ap_scorer import APScorer, MarketContext, ExecutionContext, OpportunityContext, get_default_liquidity

    scorer      = APScorer()
    tier_engine = APTierEngine()
    shadow      = APShadowTracker()

    demo_signals = [
        # What an A+ looks like: n=25, strong setup, perfect context, elite chain
        {"label":"A+ candidate",  "n":25, "wr":85.0, "ev":8000, "ret":77.0, "opt":1737.0, "entry":162,"target":178,"stop":155,"rr":2.28, "ticker":"BA"},
        # A: n=10, solid but one thing slightly off
        {"label":"A  candidate",  "n":13, "wr":80.0, "ev":5000, "ret":50.0, "opt":1000.0, "entry":162,"target":178,"stop":155,"rr":2.00, "ticker":"BA"},
        # B: good pattern, weak sample/context
        {"label":"B  candidate",  "n":7,  "wr":85.0, "ev":2000, "ret":25.0, "opt":300.0,  "entry":855,"target":892,"stop":830,"rr":1.48, "ticker":"NVDA"},
        # Reject: tiny sample, penalty fires
        {"label":"REJ candidate", "n":2,  "wr":100.0,"ev":400,  "ret":4.0,  "opt":150.0,  "entry":205,"target":212,"stop":199,"rr":1.00, "ticker":"AAPL"},
    ]

    print("\n" + "="*70)
    print("ANGEL PRECISION — TIER ENGINE DEMO")
    print("="*70)

    for d in demo_signals:
        v, oi = get_default_liquidity(d["ticker"])
        sig = {"ticker":d["ticker"],"pattern":"2U-2U-2D-2U","side":"CALL","timeframe":"1d",
               "n_occurrences":d["n"],"win_rate":d["wr"],"ev_score":d["ev"],
               "avg_return":d["ret"],"avg_opt_ret":d["opt"],
               "entry_price":d["entry"],"target_price":d["target"],"stop_price":d["stop"],"rr_ratio":d["rr"]}
        mkt = MarketContext(spy_trend="uptrend",is_trend_day=True,setup_direction="CALL",rel_volume=1.8,htf_aligned=True,htf_count=2)
        exc = ExecutionContext(spread_pct=0.04,volume=v,open_interest=oi,rr_ratio=d["rr"],entry_clean=True)
        dist = abs(d["target"]-d["entry"])/d["entry"]*100
        opp = OpportunityContext(target_distance_pct=dist,avg_opt_ret_on_wins=d["opt"],path_clean=True,resistance_count=0)

        score_result = scorer.score_signal(sig, market_ctx=mkt, exec_ctx=exc, opp_ctx=opp)
        decision     = tier_engine.classify(score_result, base_contracts=3)

        print(f"\n{d['label']:16s} | Score={decision.score:5.1f} [{decision.tier:6s}] | "
              f"contracts={decision.contracts} | heat={decision.heat_weight:.2f}R | "
              f"live={decision.live_trade} auto={decision.auto_execute}")
        print(f"  Label: {decision.label}")
        if decision.limiting_factor:
            print(f"  Gap:   {decision.limiting_factor}")

        if decision.shadow_track:
            shadow.log_shadow(sig, decision)

    print("\n" + "="*70)
    print(f"Shadow trades logged: {len(shadow._trades)}")
    print("="*70)
