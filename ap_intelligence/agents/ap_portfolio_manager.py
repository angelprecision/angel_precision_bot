"""
ap_portfolio_manager.py — Angel Precision Portfolio Manager v2 (ELITE)
=======================================================================
Adapted from virattt/ai-hedge-fund (MIT) + ValueCell composer pattern

KEY CHANGES from v1:
1. SCORECARD SYSTEM — not raw weighted vote
   Each dimension scored 0–25, total 0–100, hard size tiers
2. SINGLE CONFIDENCE SCALING HERE — not in risk manager
3. CORRECT WEIGHTS — Scanner 40%, Technical 35%, Fundamentals 15%, Sentiment 10%
4. HARD RULES — contract quality, regime, concentration = always block
5. SENTIMENT DEMOTED — tie-breaker + catalyst boost only, never vetoes clean setup
6. OPTIONAL LLM SYNTHESIS — for reasoning text, never for the go/no-go decision

Scorecard:
  Setup Quality    (Scanner)      0–30
  Technical Align  (Technical)    0–25
  Regime Fit       (SPY/VIX)      0–15
  Contract Quality (Risk Mgr)     0–15
  Fundamentals     (Fund Agent)   0–10
  Sentiment/Catalyst              0–5
  ─────────────────────────────────────
  Total                           0–100

Size tiers:
  90–100 → 100% size
  75–89  → 75% size
  60–74  → 50% size
  < 60   → SKIP

Hard blocks (override any score):
  - Risk manager rejected
  - Contract quality failed
  - Regime blocks signal direction
"""

import os
import json
from dataclasses import dataclass
from typing import Optional

from ap_intelligence.ap_mode_config import APModeConfig, ScoreTier


@dataclass
class ScoreBreakdown:
    setup_quality: float       # 0–30  (scanner signal + confidence)
    technical_align: float     # 0–25  (technical agent)
    regime_fit: float          # 0–15  (SPY trend + VIX context)
    contract_quality: float    # 0–15  (from risk manager output)
    fundamentals: float        # 0–10
    sentiment: float           # 0–5   (tie-breaker only)
    total: float               # 0–100


@dataclass
class PortfolioDecision:
    ticker: str
    action: str                    # "execute" | "skip"
    direction: str                 # "bullish" | "bearish" | "neutral"
    contracts: int                 # After confidence bucket sizing
    max_usd: float
    score: float                   # 0–100 scorecard total
    size_tier: str                 # "full" | "reduced" | "pilot" | "skip"
    confidence_bucket: str         # "A+" | "A" | "B" | "skip"
    reasoning: str
    score_breakdown: ScoreBreakdown
    signal_breakdown: dict
    confidence: float = 0.0         # normalised 0.0–1.0 (score / 100) for bridge
    hard_block: str = ""           # If blocked, why


# ─────────────────────────────────────────────
# SIZE TIERS — confidence scaling lives HERE ONLY
# Driven by APModeConfig — PRODUCTION or RESEARCH
# ─────────────────────────────────────────────
def _size_tier(score: float, mode_cfg: APModeConfig = None) -> tuple[str, str, float]:
    """Returns (tier_label, confidence_bucket, size_multiplier)"""
    if mode_cfg is None:
        mode_cfg = APModeConfig()  # reads AP_MODE env var
    tier = mode_cfg.get_tier(score)
    if tier is None:
        return "skip", "skip", 0.0
    return tier.label, tier.confidence_bucket, tier.size_multiplier


class APPortfolioManager:
    """
    Angel Precision Portfolio Manager v2 — SCORECARD SYSTEM.

    Decision is entirely deterministic (rule-based scorecard).
    LLM is optional — only generates the reasoning text, never the decision.
    """

    def __init__(
        self,
        openai_api_key: str = None,
        use_llm: bool = True,
        mode: str = None,           # "production" | "research" | None (reads AP_MODE env)
    ):
        self.openai_api_key = openai_api_key or os.environ.get("OPENAI_API_KEY", "")
        self.use_llm = use_llm and bool(self.openai_api_key)
        self.mode_cfg = APModeConfig(mode=mode)

    def decide(self, ticker: str, signals: dict) -> PortfolioDecision:
        """
        Run scorecard and return final decision.

        signals keys:
            scanner:      {signal, confidence}
            technical:    {signal, confidence, breakdown}
            sentiment:    {signal, confidence}   ← tie-breaker only
            fundamentals: {signal, confidence}
            risk:         {approved, reason, max_contracts, max_position_usd,
                           spy_trend, vix, contract_quality_passes, ...}
        """
        risk = signals.get("risk", {})
        scanner = signals.get("scanner", {})
        technical = signals.get("technical", {})
        sentiment = signals.get("sentiment", {})
        fundamentals = signals.get("fundamentals", {})

        direction = scanner.get("signal", "neutral")

        # ── HARD BLOCKS ───────────────────────────────────────
        # These override the scorecard entirely
        if direction == "neutral":
            return self._block(ticker, direction, signals,
                               "Scanner signal is neutral — no directional setup")

        if not risk.get("approved", False):
            return self._block(ticker, direction, signals,
                               f"Risk Manager: {risk.get('reason', 'rejected')}")

        if not risk.get("contract_quality_passes", True):
            return self._block(ticker, direction, signals,
                               f"Contract quality failed: {risk.get('contract_rejection', '')}")

        # ── SCORECARD ─────────────────────────────────────────
        sb = self._score(scanner, technical, sentiment, fundamentals, risk, direction, ticker)

        tier_label, bucket, size_mult = _size_tier(sb.total, self.mode_cfg)

        if tier_label == "skip":
            threshold = self.mode_cfg.skip_threshold
            return self._block(ticker, direction, signals,
                               f"Score {sb.total:.1f} < {threshold:.0f} — insufficient edge ({self.mode_cfg.mode} mode)",
                               score=sb.total, score_breakdown=sb)

        # Research mode: log_only tier — record but do not execute
        tier_obj = self.mode_cfg.get_tier(sb.total)
        if tier_obj and not tier_obj.live_trade:
            return self._block(ticker, direction, signals,
                               f"Score {sb.total:.1f} — LOG ONLY (research mode, below live threshold)",
                               score=sb.total, score_breakdown=sb)

        # ── POSITION SIZING — one place only ──────────────────
        raw_contracts = risk.get("max_contracts", 0)
        raw_usd       = risk.get("max_position_usd", 0.0)

        # Minimum 2 contracts when the risk manager approved 2 or more.
        # If risk manager only approved 1 (tight budget / low equity), respect that.
        # Rule: never force 2 contracts on an account that can only afford 1.
        _min_floor      = 2 if raw_contracts >= 2 else 1
        final_contracts = max(_min_floor, int(raw_contracts * size_mult))
        final_usd       = round(raw_usd * size_mult, 2)

        # ── REASONING ─────────────────────────────────────────
        if self.use_llm:
            reasoning = self._llm_reasoning(ticker, direction, sb, signals)
        else:
            reasoning = self._rule_reasoning(ticker, direction, sb, bucket)

        return PortfolioDecision(
            ticker=ticker,
            action="execute",
            direction=direction,
            contracts=final_contracts,
            max_usd=final_usd,
            score=sb.total,
            size_tier=tier_label,
            confidence_bucket=bucket,
            confidence=sb.total / 100.0,  # normalised for bridge
            reasoning=reasoning,
            score_breakdown=sb,
            signal_breakdown=signals,
        )

    # ─────────────────────────────────────────────
    # SCORECARD
    # ─────────────────────────────────────────────
    def _score(self, scanner, technical, sentiment, fundamentals, risk, direction, ticker: str = "") -> ScoreBreakdown:

        # Pull weights from mode config (production vs research)
        w = self.mode_cfg.weights

        # ── 1. Setup Quality — 0 to w.setup_quality ───────────
        scan_conf = scanner.get("confidence", 0) / 100.0  # 0.0–1.0
        setup_quality = scan_conf * w.setup_quality

        # ── 2. Technical Alignment — 0 to w.technical_align ───
        tech_sig  = technical.get("signal", "neutral")
        tech_conf = technical.get("confidence", 0) / 100.0
        if tech_sig == direction:
            tech_score = tech_conf * w.technical_align
        elif tech_sig == "neutral":
            tech_score = tech_conf * (w.technical_align * 0.4)  # Neutral = 40% credit
        else:
            tech_score = 0.0                # Opposing signal = zero

        # ── 3. Regime Fit — 0 to w.regime_fit ────────────────
        spy_trend = risk.get("spy_trend", "UNKNOWN")
        vix       = risk.get("vix", 20)

        # Index tickers get full regime credit — they ARE the regime,
        # judged on their own 232 technical merit (mirrors risk_manager exemption).
        is_index = (ticker or "").upper() in {"SPY", "QQQ", "IWM", "DIA"}

        if (direction == "bullish" and spy_trend == "BULL") or \
           (direction == "bearish" and spy_trend == "BEAR"):
            regime_score = w.regime_fit
        elif is_index:
            regime_score = w.regime_fit         # Full credit — index self-regime
        elif spy_trend == "CHOPPY":
            regime_score = w.regime_fit * 0.47   # ~7/15 in production
        else:
            regime_score = w.regime_fit * 0.20   # ~3/15 — risk mgr blocked hard misalignment

        # VIX adjustment within regime score
        if 15 <= vix <= 25:
            regime_score *= 1.0    # Normal — no adjustment
        elif vix < 15:
            regime_score *= 0.8    # Too quiet — less premium
        elif vix > 30:
            regime_score *= 0.7    # Elevated — more uncertainty

        # ── 4. Contract Quality — 0 to w.contract_quality ────
        # Risk manager already checked hard gates — score by quality margin
        cq_passes  = risk.get("contract_quality_passes", True)
        spread_pct = risk.get("spread_pct", 0.05)
        oi         = risk.get("open_interest", 500)

        if cq_passes:
            # Better quality = higher score (spread + OI each contribute 50%)
            spread_score = max(0, 1 - (spread_pct / 0.10)) * (w.contract_quality * 0.5)
            oi_score     = min(oi / 2000, 1.0) * (w.contract_quality * 0.5)
            contract_quality_score = spread_score + oi_score
        else:
            contract_quality_score = 0.0

        # ── 5. Fundamentals — 0 to w.fundamentals ────────────
        # In research/intraday mode this weight is halved (swing filter)
        fund_sig  = fundamentals.get("signal", "neutral")
        fund_conf = fundamentals.get("confidence", 50) / 100.0
        if fund_sig == direction:
            fund_score = fund_conf * w.fundamentals
        elif fund_sig == "neutral":
            fund_score = w.fundamentals * 0.5   # Neutral = half credit
        else:
            fund_score = w.fundamentals * 0.2   # Bad fundamentals = small drag, not full block

        # ── 6. Sentiment — 0 to w.sentiment (TIE-BREAKER ONLY)
        sent_sig  = sentiment.get("signal", "neutral")
        sent_conf = sentiment.get("confidence", 50) / 100.0
        if sent_sig == direction:
            sent_score = sent_conf * w.sentiment
        elif sent_sig == "neutral":
            sent_score = w.sentiment * 0.5
        else:
            sent_score = 0.0

        total = setup_quality + tech_score + regime_score + contract_quality_score + fund_score + sent_score

        return ScoreBreakdown(
            setup_quality=round(setup_quality, 2),
            technical_align=round(tech_score, 2),
            regime_fit=round(regime_score, 2),
            contract_quality=round(contract_quality_score, 2),
            fundamentals=round(fund_score, 2),
            sentiment=round(sent_score, 2),
            total=round(min(total, 100.0), 2),
        )

    # ─────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────
    def _block(self, ticker, direction, signals, reason,
               score=0.0, score_breakdown=None) -> PortfolioDecision:
        if score_breakdown is None:
            score_breakdown = ScoreBreakdown(0,0,0,0,0,0,0)
        return PortfolioDecision(
            ticker=ticker, action="skip", direction=direction,
            contracts=0, max_usd=0.0, score=score,
            size_tier="skip", confidence_bucket="skip",
            confidence=0.0,
            reasoning=reason, score_breakdown=score_breakdown,
            signal_breakdown=signals, hard_block=reason,
        )

    def _rule_reasoning(self, ticker, direction, sb, bucket) -> str:
        action = "CALL" if direction == "bullish" else "PUT"
        return (
            f"{ticker} → {action} | Score: {sb.total:.1f}/100 | Tier: {bucket} | "
            f"Setup:{sb.setup_quality:.0f} Tech:{sb.technical_align:.0f} "
            f"Regime:{sb.regime_fit:.0f} Contract:{sb.contract_quality:.0f} "
            f"Fund:{sb.fundamentals:.0f} Sent:{sb.sentiment:.0f}"
        )

    def _llm_reasoning(self, ticker, direction, sb, signals) -> str:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_api_key)

            payload = {
                "ticker":     ticker,
                "direction":  direction,
                "score":      sb.total,
                "breakdown":  {
                    "setup_quality":   sb.setup_quality,
                    "technical_align": sb.technical_align,
                    "regime_fit":      sb.regime_fit,
                    "contract_quality":sb.contract_quality,
                    "fundamentals":    sb.fundamentals,
                    "sentiment":       sb.sentiment,
                },
                "spy_trend":  signals.get("risk", {}).get("spy_trend"),
                "vix":        signals.get("risk", {}).get("vix"),
            }

            resp = client.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[{
                    "role": "user",
                    "content": (
                        "You are a portfolio manager at Angel Precision, an elite options trading firm. "
                        "Write a 2-sentence trade rationale. Be direct and specific. "
                        "Reference the score drivers. No fluff.\n\n"
                        f"Trade data:\n{json.dumps(payload, indent=2, default=str)}"
                    )
                }],
                max_tokens=120, temperature=0.2,
            )
            return resp.choices[0].message.content.strip()
        except Exception:
            return self._rule_reasoning(ticker, direction, sb, "")


# ─────────────────────────────────────────────
# DISCORD FORMATTER
# ─────────────────────────────────────────────
def format_decision_for_discord(decision: PortfolioDecision) -> str:
    if decision.action == "skip":
        emoji = "⚪"
        header = f"SKIP — {decision.ticker}"
        if decision.hard_block:
            header += f"\n**Reason:** {decision.hard_block}"
        return f"## {emoji} Angel Precision | {header}"

    action = "CALL" if decision.direction == "bullish" else "PUT"
    tier_emoji = {"full": "🟢🟢", "reduced": "🟢", "pilot": "🟡", "micro_pilot": "🟡", "log_only": "⚪"}.get(decision.size_tier, "⚪")
    sb = decision.score_breakdown

    lines = [
        f"## {tier_emoji} Angel Precision | {decision.ticker} — {action} | Score: {decision.score:.0f}/100",
        f"**Tier:** {decision.confidence_bucket} ({decision.size_tier.upper()}) | "
        f"**Contracts:** {decision.contracts} | **Max:** ${decision.max_usd:,.0f}",
        f"**Reasoning:** {decision.reasoning}",
        "",
        "**Scorecard:**",
        f"Setup Quality:   {sb.setup_quality:.0f}/30",
        f"Technical Align: {sb.technical_align:.0f}/25",
        f"Regime Fit:      {sb.regime_fit:.0f}/15",
        f"Contract Qual:   {sb.contract_quality:.0f}/15",
        f"Fundamentals:    {sb.fundamentals:.0f}/10",
        f"Sentiment:       {sb.sentiment:.0f}/5",
    ]
    return "\n".join(lines)
