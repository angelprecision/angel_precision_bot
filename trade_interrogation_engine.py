"""
trade_interrogation_engine.py
Angel Precision — Universal Signal Interrogation Layer

Sits BETWEEN scanner → master_control.
Runs every incoming signal through 8 mandatory gates.
Outputs a DecisionPacket.

Master control remains final authority — this is advisory/pre-filter.

Pipeline:
    scanner hit (ANY ticker)
        ↓
    trade_interrogation_engine.evaluate()
        ↓
    DecisionPacket (EXECUTE | WATCH | BLOCK | ESCALATE)
        ↓
    master_control (FINAL AUTHORITY)
        ↓
    queue / execution

Nothing here executes a trade. Ever.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional, TYPE_CHECKING

from ap.admission_thresholds import (
    build_threshold_trace,
    log_admission_thresholds,
    resolve_admission_thresholds,
)

from decision_packet import (
    DecisionPacket, DecisionStatus,
    RiskProfile, BestContract, ReentryCondition,
)

if TYPE_CHECKING:
    pass

log = logging.getLogger("ap.interrogation")

# ---------------------------------------------------------------------------
# Config — all tunable via env vars
# ---------------------------------------------------------------------------

INTERROGATION_ENABLED   = os.getenv("INTERROGATION_ENABLED", "1").lower() in {"1", "true", "yes"}
_ADMISSION_THRESHOLDS   = resolve_admission_thresholds()
MIN_QUALITY_TO_EXECUTE  = float(_ADMISSION_THRESHOLDS.thresholds.interrogation_floor)
MIN_RR_RATIO            = float(os.getenv("INTERROGATION_MIN_RR",    "1.5"))
MAX_SPREAD_PCT          = float(os.getenv("MAX_SPREAD_PCT",          "0.12"))
MIN_OPEN_INTEREST       = int(os.getenv("MIN_OPEN_INTEREST",         "100"))
EARNINGS_BLACKOUT_DAYS  = int(os.getenv("EARNINGS_BLACKOUT_DAYS",    "3"))
MAX_SECTOR_EXPOSURE_PCT = float(os.getenv("MAX_SECTOR_EXPOSURE_PCT", "0.35"))

# Signals the evolver has approved (loaded once at startup)
_EVOLVER_CONFIG: Optional[dict] = None
_EVOLVER_GATE: float = MIN_QUALITY_TO_EXECUTE


def _load_evolver_config() -> dict:
    """Load best_evolver_config.json if it exists; else use baseline."""
    global _EVOLVER_CONFIG, _EVOLVER_GATE
    if _EVOLVER_CONFIG is not None:
        return _EVOLVER_CONFIG
    import json
    from pathlib import Path
    cfg_path = Path("best_evolver_config.json")
    if cfg_path.exists():
        try:
            with open(cfg_path) as f:
                raw = json.load(f)
            _EVOLVER_CONFIG = raw.get("config", {})
            _EVOLVER_GATE   = float(_EVOLVER_CONFIG.get("gate_threshold", MIN_QUALITY_TO_EXECUTE))
            log.info(
                "Evolver config loaded | gate=%.1f | win_rate=%.1f%%",
                _EVOLVER_GATE,
                raw.get("achieved_win_rate", 0) * 100,
            )
        except Exception as e:
            log.warning("Could not load best_evolver_config.json: %s — using defaults", e)
            _EVOLVER_CONFIG = {}
    else:
        _EVOLVER_CONFIG = {}
    return _EVOLVER_CONFIG


# ---------------------------------------------------------------------------
# Sector map (used for concentration checks)
# ---------------------------------------------------------------------------

_SECTOR_MAP: dict[str, str] = {
    "NVDA": "tech",  "AMD": "tech",   "INTC": "tech",  "QCOM": "tech",
    "AAPL": "tech",  "MSFT": "tech",  "GOOG": "tech",  "GOOGL": "tech",
    "META": "tech",  "AMZN": "tech",  "TSLA": "tech",  "NFLX": "tech",
    "ORCL": "tech",  "CRM":  "tech",  "NOW":  "tech",  "PLTR": "tech",
    "MSTR": "crypto","COIN": "crypto",
    "SPY":  "index", "QQQ":  "index", "IWM":  "index", "DIA":  "index",
    "GS":   "finance","MS":  "finance","JPM": "finance","WFC":  "finance",
    "XOM":  "energy","CVX": "energy",
    "JNJ":  "health","PFE": "health", "MRNA": "health",
}

def _get_sector(ticker: str) -> str:
    return _SECTOR_MAP.get(ticker.upper().strip(), "other")


# ---------------------------------------------------------------------------
# Gate 1 — Entry permission
# ---------------------------------------------------------------------------

def _gate_entry_permission(signal: dict, state: Optional[dict]) -> tuple[bool, list[str], list[str]]:
    """Returns (allowed, blocked_by, warnings)."""
    blocked, warnings = [], []

    if state is None:
        warnings.append("state_unavailable: proceeding without state gate")
        return True, blocked, warnings

    mode = str(state.get("mode", "PAPER")).upper()
    if mode == "READ_ONLY":
        blocked.append("mode=READ_ONLY: all entries disabled")
        return False, blocked, warnings

    if bool(state.get("kill_switch", False)):
        blocked.append("kill_switch=ON")
        return False, blocked, warnings

    daily_loss = float(state.get("realized_pnl_today") or 0)
    start_eq   = float(state.get("starting_equity_today") or 0)
    if start_eq > 0 and daily_loss < 0:
        loss_pct = abs(daily_loss) / start_eq
        if loss_pct >= float(os.getenv("DAILY_MAX_LOSS_PCT", "0.06")):
            blocked.append(f"daily_loss_gate: lost {loss_pct:.1%} today (limit 6%)")
            return False, blocked, warnings

    trades_today = int(state.get("trades_taken_today") or 0)
    max_trades   = int(state.get("max_trades_per_day") or int(os.getenv("MAX_TRADES_PER_DAY", "5")))
    if trades_today >= max_trades:
        blocked.append(f"max_trades_reached: {trades_today}/{max_trades} today")
        return False, blocked, warnings

    return True, blocked, warnings


# ---------------------------------------------------------------------------
# Gate 2 — Regime fit
# ---------------------------------------------------------------------------

def _gate_regime(signal: dict, spy_trend: str, vix: Optional[float]) -> tuple[bool, list[str], list[str]]:
    blocked, warnings = [], []
    side = str(signal.get("side") or signal.get("direction") or "CALL").upper()
    is_call = side in {"CALL", "CALLS", "BULLISH", "BUY"}
    is_put  = side in {"PUT", "PUTS", "BEARISH", "SELL"}

    trend_up = spy_trend.upper() in {"BULL", "BULLISH", "UP", "UPTREND"}
    trend_dn = spy_trend.upper() in {"BEAR", "BEARISH", "DOWN", "DOWNTREND"}

    if is_call and trend_dn:
        blocked.append(f"regime_mismatch: CALL in BEAR market (SPY={spy_trend})")
        return False, blocked, warnings
    if is_put and trend_up:
        blocked.append(f"regime_mismatch: PUT in BULL market (SPY={spy_trend})")
        return False, blocked, warnings

    if vix is not None:
        if vix < 11:
            warnings.append(f"VIX={vix:.1f} very low — premium cheap but regime quiet")
        if vix > 40:
            blocked.append(f"VIX={vix:.1f} > 40 — extreme volatility gate active")
            return False, blocked, warnings
        if vix > 30:
            warnings.append(f"VIX={vix:.1f} elevated — reduce size / tighter stops")

    if spy_trend.upper() == "UNKNOWN":
        warnings.append("spy_trend=UNKNOWN — regime gate soft-pass")

    return True, blocked, warnings


# ---------------------------------------------------------------------------
# Gate 3 — Technical positioning (overextension check)
# ---------------------------------------------------------------------------

def _gate_technical(signal: dict) -> tuple[bool, list[str], list[str], float]:
    """Returns (ok, blocked_by, warnings, tech_score)."""
    blocked, warnings = [], []
    score = signal.get("score") or 0
    try:
        score = float(score)
    except Exception:
        score = 0.0

    ev_score = signal.get("ev_score")
    try:
        ev_score = float(ev_score) if ev_score is not None else score
    except Exception:
        ev_score = score

    # Pattern quality from scanner
    pattern = str(signal.get("pattern") or signal.get("pattern_id") or "")
    has_pattern = bool(pattern.strip())
    if not has_pattern:
        warnings.append("no_pattern_identified: signal missing pattern field")

    # Timeframe check — prefer higher timeframes (more reliable)
    timeframe = str(signal.get("timeframe") or "1d").lower()
    tf_bonus = 0.0
    if timeframe in {"1w", "weekly"}:
        tf_bonus = 8.0
    elif timeframe in {"4d", "3d", "2d"}:
        tf_bonus = 5.0
    elif timeframe in {"1d", "daily"}:
        tf_bonus = 3.0

    tech_score = min(ev_score + tf_bonus, 100.0)

    # Hard block if scanner score is critically low
    if score < 30:
        blocked.append(f"score={score:.0f} critically low (min 30)")
        return False, blocked, warnings, tech_score

    return True, blocked, warnings, tech_score


# ---------------------------------------------------------------------------
# Gate 4 — Liquidity / options quality
# ---------------------------------------------------------------------------

def _gate_liquidity(signal: dict) -> tuple[bool, list[str], list[str], Optional[BestContract]]:
    blocked, warnings = [], []
    contract_info: Optional[BestContract] = None

    spread_pct = signal.get("spread_pct")
    open_interest = signal.get("open_interest")
    bid = signal.get("bid") or signal.get("option_bid")
    ask = signal.get("ask") or signal.get("option_ask")
    contract_symbol = signal.get("contract_symbol") or signal.get("contract") or signal.get("option_symbol")

    # Build what we know from the signal
    if contract_symbol:
        spread = None
        if bid and ask:
            try:
                bid_f, ask_f = float(bid), float(ask)
                mid = (bid_f + ask_f) / 2
                spread = (ask_f - bid_f) / mid if mid > 0 else None
            except Exception:
                spread = None

        liq_ok = True
        if spread is not None and spread > MAX_SPREAD_PCT:
            warnings.append(f"spread_wide: {spread:.1%} > {MAX_SPREAD_PCT:.0%}")
            liq_ok = False
        if open_interest is not None:
            try:
                if int(open_interest) < MIN_OPEN_INTEREST:
                    warnings.append(f"low_OI: {open_interest} < {MIN_OPEN_INTEREST}")
                    liq_ok = False
            except Exception:
                pass

        contract_info = BestContract(
            symbol=str(contract_symbol),
            bid=float(bid) if bid else None,
            ask=float(ask) if ask else None,
            spread_pct=spread,
            open_interest=int(open_interest) if open_interest else None,
            liquidity_ok=liq_ok,
        )
    else:
        warnings.append("no_contract_in_signal: will rely on options intelligence for selection")

    # Hard block only if explicit bad spread data
    if spread_pct is not None:
        try:
            sp = float(spread_pct)
            if sp > 0.25:
                blocked.append(f"spread_too_wide: {sp:.1%} — likely illiquid")
                return False, blocked, warnings, contract_info
        except Exception:
            pass

    return True, blocked, warnings, contract_info


# ---------------------------------------------------------------------------
# Gate 5 — Risk / R:R validation
# ---------------------------------------------------------------------------

def _gate_risk(signal: dict) -> tuple[bool, list[str], list[str], Optional[RiskProfile]]:
    blocked, warnings = [], []

    trigger = signal.get("trigger") or {}
    entry   = float(signal.get("entry_price") or trigger.get("entry") or 0)
    stop    = float(signal.get("stop_price")  or trigger.get("stop") or 0)
    target  = float(signal.get("target_price") or trigger.get("pt1") or trigger.get("pt2") or 0)

    if not entry or not stop or not target:
        warnings.append("incomplete_risk_levels: stop or target missing — master control will size conservatively")
        return True, blocked, warnings, None

    side = str(signal.get("side") or "CALL").upper()
    is_call = side in {"CALL", "BULLISH", "BUY"}

    # Validate direction
    if is_call:
        if stop >= entry:
            blocked.append(f"invalid_stop: stop {stop} >= entry {entry} for CALL")
            return False, blocked, warnings, None
        if target <= entry:
            warnings.append(f"target {target} <= entry {entry}: no upside defined")
    else:
        if stop <= entry:
            blocked.append(f"invalid_stop: stop {stop} <= entry {entry} for PUT")
            return False, blocked, warnings, None
        if target >= entry:
            warnings.append(f"target {target} >= entry {entry}: no downside defined")

    # R:R calculation
    risk_dist   = abs(entry - stop)
    reward_dist = abs(target - entry)
    rr = reward_dist / risk_dist if risk_dist > 0 else 0

    stop_pct = risk_dist / entry if entry > 0 else 0

    profile = RiskProfile(
        stop_price=stop,
        target_price=target,
        reward_to_risk=round(rr, 2),
        stop_distance_pct=round(stop_pct, 4),
    )

    if rr < MIN_RR_RATIO:
        if rr < 1.0:
            blocked.append(f"rr={rr:.2f} < 1.0: risk exceeds reward (hard block)")
            return False, blocked, warnings, profile
        warnings.append(f"rr={rr:.2f} below preferred {MIN_RR_RATIO:.1f}: proceed with caution")

    if stop_pct > 0.08:
        warnings.append(f"wide_stop: {stop_pct:.1%} — high capital at risk per contract")

    return True, blocked, warnings, profile


# ---------------------------------------------------------------------------
# Gate 6 — Event risk (earnings blackout)
# ---------------------------------------------------------------------------

def _gate_event_risk(signal: dict, broker=None) -> tuple[bool, list[str], list[str], Optional[int]]:
    blocked, warnings = [], []
    ticker = str(signal.get("ticker") or signal.get("symbol") or "").upper()
    earnings_days: Optional[int] = None

    if not ticker or broker is None:
        warnings.append("event_risk: earnings check skipped (no broker or ticker)")
        return True, blocked, warnings, earnings_days

    try:
        from ap.market_intelligence import APEarningsGuard
        guard = APEarningsGuard(broker, blackout_days=EARNINGS_BLACKOUT_DAYS)
        result = guard.check(ticker)
        if result.get("blocked"):
            days = result.get("days_until")
            earnings_days = days
            blocked.append(f"earnings_blackout: {ticker} reports in {days}d ({result.get('earnings_date')})")
            return False, blocked, warnings, earnings_days
        earnings_days = result.get("days_until")
    except Exception as e:
        log.debug("Earnings check failed for %s: %s", ticker, e)
        warnings.append(f"earnings_check_failed: {e} — proceeding without blackout gate")

    return True, blocked, warnings, earnings_days


# ---------------------------------------------------------------------------
# Gate 7 — Portfolio conflict
# ---------------------------------------------------------------------------

def _gate_portfolio(signal: dict, position_manager=None) -> tuple[bool, list[str], list[str]]:
    blocked, warnings = [], []
    ticker  = str(signal.get("ticker") or signal.get("symbol") or "").upper()
    sector  = _get_sector(ticker)

    has_existing = False
    sector_ok    = True

    if position_manager is not None:
        try:
            # Check if already in this ticker
            has_existing = position_manager.has_open_position(ticker)
            if has_existing:
                warnings.append(f"existing_position: already have {ticker} open — size will be managed")

            # Sector concentration check via snapshot
            snap = position_manager.snapshot()
            deployed = float(snap.get("capital_deployed") or 0)
            equity   = float(snap.get("equity") or float(os.getenv("INITIAL_EQUITY", "25000")))

            if equity > 0:
                sector_pct = deployed / equity
                if sector_pct > MAX_SECTOR_EXPOSURE_PCT:
                    warnings.append(f"sector_exposure: {sector} at {sector_pct:.0%} of capital")

        except Exception as e:
            log.debug("Portfolio conflict check failed: %s", e)
            warnings.append(f"portfolio_check_failed: {e}")

    return True, blocked, warnings


# ---------------------------------------------------------------------------
# Gate 8 — AP Strategy Fit (The Strat + AP exit rules)
# ---------------------------------------------------------------------------

def _gate_strategy_fit(signal: dict) -> tuple[bool, list[str], list[str], float]:
    """
    Score how well this signal fits AP's specific edge framework.

    AP rules:
      1. Cut losers fast (hard stop in exit engine at -30%)
      2. Scale out 50-75% at profit protect windows
      3. Leave a runner
      4. Only trade setups with proven historical EV

    Returns (ok, blocked_by, warnings, fit_score 0-100)
    """
    blocked, warnings = [], []

    ev_score   = float(signal.get("ev_score") or signal.get("score") or 0)
    timeframe  = str(signal.get("timeframe") or "1d").lower()
    pattern    = str(signal.get("pattern") or signal.get("pattern_id") or "")
    grade      = str(signal.get("grade") or signal.get("tier") or "B").upper()

    fit_score = 50.0  # baseline

    # EV score from backtest library
    if ev_score >= 85:
        fit_score += 25
    elif ev_score >= 70:
        fit_score += 15
    elif ev_score >= 55:
        fit_score += 5
    elif ev_score < 40:
        fit_score -= 20
        warnings.append(f"low_ev_score: {ev_score:.0f} — setup has weak historical edge")

    # Grade bonus
    grade_bonus = {"A+": 20, "A": 15, "B+": 10, "B": 5, "C": -5, "D": -15}.get(grade, 0)
    fit_score += grade_bonus

    # Timeframe bonus (higher tf = more reliable on Strat)
    if timeframe in {"1w", "weekly"}:
        fit_score += 10
    elif timeframe in {"4d", "3d"}:
        fit_score += 7
    elif timeframe in {"2d"}:
        fit_score += 4

    # Pattern specificity bonus
    if "→" in pattern or "->" in pattern:
        fit_score += 5   # sequenced pattern — higher specificity

    # Hard block: extremely low EV + low score combination
    if ev_score < 30 and float(signal.get("score") or 0) < 40:
        blocked.append(f"strategy_fit_fail: ev={ev_score:.0f} + score={signal.get('score'):.0f} both too weak")
        return False, blocked, warnings, max(0, fit_score)

    fit_score = max(0.0, min(100.0, fit_score))

    if fit_score < 40:
        warnings.append(f"weak_strategy_fit: score {fit_score:.0f}/100")

    return True, blocked, warnings, fit_score


# ---------------------------------------------------------------------------
# Composite quality score
# ---------------------------------------------------------------------------

def _compute_quality(signal: dict, tech_score: float, fit_score: float, rr: Optional[float]) -> float:
    """Weighted composite 0-100."""
    ev_score  = float(signal.get("ev_score") or signal.get("score") or 50)
    sig_score = float(signal.get("score") or 50)

    rr_bonus = 0.0
    if rr is not None:
        if rr >= 3.0:
            rr_bonus = 10
        elif rr >= 2.0:
            rr_bonus = 6
        elif rr >= 1.5:
            rr_bonus = 3

    quality = (
        sig_score * 0.30 +     # raw scanner score
        ev_score  * 0.25 +     # historical EV
        tech_score * 0.25 +    # timeframe + pattern quality
        fit_score  * 0.20      # AP strategy fit
    ) + rr_bonus

    return max(0.0, min(100.0, quality))


# ---------------------------------------------------------------------------
# Re-entry condition builder
# ---------------------------------------------------------------------------

def _build_reentry(signal: dict, blocked_by: list[str]) -> Optional[ReentryCondition]:
    """Build a concrete re-entry trigger description from block reasons."""
    if not blocked_by:
        return None

    primary_block = blocked_by[0] if blocked_by else ""
    ticker = str(signal.get("ticker") or signal.get("symbol") or "").upper()

    if "regime_mismatch" in primary_block:
        side = "BULL" if "PUT" in primary_block else "BEAR"
        return ReentryCondition(
            trigger_type="regime",
            description=f"SPY trend shifts to {side}",
            trigger_direction="equals",
        )
    if "max_trades" in primary_block or "daily_loss" in primary_block:
        return ReentryCondition(
            trigger_type="time",
            description="New trading day opens",
            trigger_direction="above",
        )
    if "kill_switch" in primary_block:
        return ReentryCondition(
            trigger_type="admin",
            description="Kill switch disabled",
            trigger_direction="equals",
        )
    if "earnings_blackout" in primary_block:
        return ReentryCondition(
            trigger_type="time",
            description=f"{ticker} earnings event passes",
            trigger_direction="above",
        )
    if "rr=" in primary_block:
        target = signal.get("target_price")
        return ReentryCondition(
            trigger_type="price",
            description=f"Target price extends to R:R ≥ {MIN_RR_RATIO:.1f}",
            trigger_value=float(target) if target else None,
            trigger_direction="above",
        )

    return ReentryCondition(
        trigger_type="indicator",
        description=f"Block condition resolves: {primary_block[:60]}",
    )


# ---------------------------------------------------------------------------
# MAIN: evaluate()
# ---------------------------------------------------------------------------

class APTradeInterrogationEngine:
    """
    Ticker-agnostic signal interrogation layer.
    Call evaluate() on every scanner signal before passing to master_control.
    """

    def __init__(self, broker=None, position_manager=None, state_loader=None):
        """
        Args:
            broker:           Tradier broker instance (for earnings + chain checks)
            position_manager: APPositionManager for portfolio conflict checks
            state_loader:     callable(client_id) → state dict, or None to read from DB
        """
        self.broker           = broker
        self.position_manager = position_manager
        self.state_loader     = state_loader
        self._admission_thresholds = resolve_admission_thresholds()
        _load_evolver_config()   # load evolver config at init
        log_admission_thresholds(
            log,
            component="APTradeInterrogationEngine",
            resolved=self._admission_thresholds,
        )
        log.info(
            "APTradeInterrogationEngine ready | gate=%.1f | evolver_gate=%.1f | threshold_config_hash=%s",
            MIN_QUALITY_TO_EXECUTE,
            _EVOLVER_GATE,
            self._admission_thresholds.config_hash,
        )

    def _load_state(self, signal: dict) -> Optional[dict]:
        client_id = str(signal.get("client_id") or "default")
        if self.state_loader is not None:
            try:
                return self.state_loader(client_id)
            except Exception as e:
                log.warning("state_loader failed for %s: %s", client_id, e)
                return None
        try:
            from ap.db import get_client_state
            return get_client_state(client_id)
        except Exception as e:
            log.warning("DB state load failed: %s", e)
            return None

    def evaluate(self, signal: dict) -> DecisionPacket:
        """
        Run all 8 gates. Return DecisionPacket.
        Never raises — always returns a packet, worst case BLOCK.
        """
        start_ms = int(time.time() * 1000)

        ticker    = str(signal.get("ticker") or signal.get("symbol") or "UNKNOWN").upper()
        pattern   = str(signal.get("pattern") or signal.get("pattern_id") or "")
        source    = str(signal.get("source") or signal.get("scanner_source") or "scanner")
        raw_side  = str(signal.get("side") or signal.get("direction") or "CALL").upper()
        direction = "bullish" if raw_side in {"CALL", "CALLS", "BULLISH", "BUY"} else "bearish"

        # Safety: if engine is disabled, pass-through as EXECUTE
        if not INTERROGATION_ENABLED:
            return DecisionPacket(
                ticker=ticker, pattern=pattern, scanner_source=source,
                signal_direction=direction, status=DecisionStatus.EXECUTE,
                actionable=True, quality_score=75.0, confidence=75.0,
                why=["interrogation_disabled: pass-through"],
                live_trade_ok=True, paper_trade_ok=True,
            )

        all_blocked: list[str] = []
        all_warnings: list[str] = []
        all_why:     list[str] = []

        state = self._load_state(signal)

        # Detect SPY trend from signal context (scanner embeds this) or state
        spy_trend = str(
            signal.get("spy_trend") or
            signal.get("market_trend") or
            (state or {}).get("spy_trend") or
            "UNKNOWN"
        )
        vix_level: Optional[float] = None
        try:
            vix_level = float(signal.get("vix") or signal.get("vix_level") or 0) or None
        except Exception:
            pass

        sector = _get_sector(ticker)

        # ── Gate 1: Entry permission ─────────────────────────────────────────
        g1_ok, g1_blocked, g1_warn = _gate_entry_permission(signal, state)
        all_blocked.extend(g1_blocked)
        all_warnings.extend(g1_warn)
        if g1_ok:
            all_why.append("entries_permitted")

        # ── Gate 2: Regime ───────────────────────────────────────────────────
        g2_ok, g2_blocked, g2_warn = _gate_regime(signal, spy_trend, vix_level)
        all_blocked.extend(g2_blocked)
        all_warnings.extend(g2_warn)
        if g2_ok:
            all_why.append(f"regime_ok: SPY={spy_trend}")

        # ── Gate 3: Technical ────────────────────────────────────────────────
        g3_ok, g3_blocked, g3_warn, tech_score = _gate_technical(signal)
        all_blocked.extend(g3_blocked)
        all_warnings.extend(g3_warn)
        if g3_ok:
            all_why.append(f"technical_ok: score={tech_score:.0f}")

        # ── Gate 4: Liquidity ────────────────────────────────────────────────
        g4_ok, g4_blocked, g4_warn, best_contract = _gate_liquidity(signal)
        all_blocked.extend(g4_blocked)
        all_warnings.extend(g4_warn)
        if g4_ok:
            all_why.append("liquidity_ok")

        # ── Gate 5: Risk / R:R ───────────────────────────────────────────────
        g5_ok, g5_blocked, g5_warn, risk_profile = _gate_risk(signal)
        all_blocked.extend(g5_blocked)
        all_warnings.extend(g5_warn)
        rr = risk_profile.reward_to_risk if risk_profile else None
        if g5_ok and rr:
            all_why.append(f"rr_ok: {rr:.1f}:1")

        # ── Gate 6: Event risk ───────────────────────────────────────────────
        g6_ok, g6_blocked, g6_warn, earnings_days = _gate_event_risk(signal, self.broker)
        all_blocked.extend(g6_blocked)
        all_warnings.extend(g6_warn)
        if g6_ok:
            all_why.append("event_risk_clear")

        # ── Gate 7: Portfolio conflict ───────────────────────────────────────
        g7_ok, g7_blocked, g7_warn = _gate_portfolio(signal, self.position_manager)
        all_blocked.extend(g7_blocked)
        all_warnings.extend(g7_warn)

        # ── Gate 8: Strategy fit ─────────────────────────────────────────────
        g8_ok, g8_blocked, g8_warn, fit_score = _gate_strategy_fit(signal)
        all_blocked.extend(g8_blocked)
        all_warnings.extend(g8_warn)
        if g8_ok:
            all_why.append(f"strategy_fit: {fit_score:.0f}/100")

        # ── Composite quality score ──────────────────────────────────────────
        quality = _compute_quality(signal, tech_score, fit_score, rr)
        confidence = float(signal.get("score") or signal.get("confidence") or quality)

        # ── Decision ─────────────────────────────────────────────────────────
        effective_gate = max(_EVOLVER_GATE, self._admission_thresholds.thresholds.interrogation_floor)
        all_gates_ok = g1_ok and g2_ok and g3_ok and g4_ok and g5_ok and g6_ok and g7_ok and g8_ok

        if all_blocked and any(
            k in b for b in all_blocked
            for k in ("regime_mismatch", "kill_switch", "READ_ONLY", "daily_loss", "earnings_blackout")
        ) and not all_gates_ok:
            # Temporary / time-conditional block → WATCH (can become executable)
            status    = DecisionStatus.WATCH
            actionable = False
            reentry   = _build_reentry(signal, all_blocked)
        elif not all_gates_ok:
            # Hard non-time block → BLOCK
            status    = DecisionStatus.BLOCK
            actionable = False
            reentry   = _build_reentry(signal, all_blocked)
        elif quality < effective_gate:
            # Gates pass but quality too low → WATCH
            status    = DecisionStatus.WATCH
            actionable = False
            all_warnings.append(f"quality={quality:.0f} < gate={effective_gate:.0f}: watching")
            reentry = ReentryCondition(
                trigger_type="indicator",
                description=f"Signal quality improves above {effective_gate:.0f} (currently {quality:.0f})",
            )
        elif len(all_warnings) >= 4:
            # Many soft flags but no hard block → ESCALATE for human review
            status    = DecisionStatus.ESCALATE
            actionable = True
            reentry   = None
        else:
            # All clear
            status    = DecisionStatus.EXECUTE
            actionable = True
            reentry   = None

        # ── Determine live vs paper ok ────────────────────────────────────────
        live_ok  = (status == DecisionStatus.EXECUTE) and quality >= (effective_gate + 5)
        paper_ok = status in {DecisionStatus.EXECUTE, DecisionStatus.ESCALATE}

        elapsed = int(time.time() * 1000) - start_ms

        try:
            scanner_score_value = float(signal.get("score") or signal.get("confidence") or 0)
        except Exception:
            scanner_score_value = None
        threshold_trace = {
            "scanner_floor": build_threshold_trace(
                threshold_name="scanner_floor",
                score_value=scanner_score_value,
                floor_value=self._admission_thresholds.thresholds.scanner_floor,
                passed=(
                    scanner_score_value >= self._admission_thresholds.thresholds.scanner_floor
                    if scanner_score_value is not None else None
                ),
                source=self._admission_thresholds.sources["scanner_floor"],
            ),
            "interrogation_floor": build_threshold_trace(
                threshold_name="interrogation_floor",
                score_value=round(quality, 1),
                floor_value=effective_gate,
                passed=quality >= effective_gate,
                source=(
                    f"evolver_config:gate_threshold>{self._admission_thresholds.sources['interrogation_floor']}"
                    if _EVOLVER_GATE > self._admission_thresholds.thresholds.interrogation_floor
                    else self._admission_thresholds.sources["interrogation_floor"]
                ),
            ),
        }

        packet = DecisionPacket(
            ticker=ticker,
            pattern=pattern,
            scanner_source=source,
            signal_direction=direction,
            status=status,
            actionable=actionable,
            quality_score=round(quality, 1),
            confidence=round(confidence, 1),
            why=all_why,
            blocked_by=all_blocked,
            warnings=all_warnings,
            risk=risk_profile,
            best_contract=best_contract,
            reentry_condition=reentry,
            spy_trend=spy_trend,
            vix_level=vix_level,
            regime_label=f"SPY={spy_trend}" + (f" VIX={vix_level:.0f}" if vix_level else ""),
            strategy_fit=g8_ok,
            sector=sector,
            sector_exposure_ok=True,
            correlation_conflict=False,
            existing_position=False,
            earnings_within_days=earnings_days,
            paper_trade_ok=paper_ok,
            live_trade_ok=live_ok,
            interrogation_ms=elapsed,
            strat_agent_output={
                "score":         signal.get("score"),
                "ev_score":      signal.get("ev_score"),
                "grade":         signal.get("grade"),
                "timeframe":     signal.get("timeframe"),
                "gate_used":     effective_gate,
                "tech_score":    round(tech_score, 1),
                "fit_score":     round(fit_score, 1),
                "threshold_config_hash": self._admission_thresholds.config_hash,
                "threshold_trace": threshold_trace,
            },
        )

        log.info(
            "[%s] interrogation | %s | quality=%.0f | gate=%.0f | %dms | why=%s",
            ticker, status, quality, effective_gate, elapsed,
            (all_blocked[:1] or all_why[:1] or ["ok"])[0],
        )

        return packet


# ---------------------------------------------------------------------------
# Module-level singleton (created once, injected into app at startup)
# ---------------------------------------------------------------------------

_engine_instance: Optional[APTradeInterrogationEngine] = None


def get_engine(broker=None, position_manager=None, state_loader=None) -> APTradeInterrogationEngine:
    """Return or create the module-level engine singleton."""
    global _engine_instance
    if _engine_instance is None:
        _engine_instance = APTradeInterrogationEngine(
            broker=broker,
            position_manager=position_manager,
            state_loader=state_loader,
        )
    return _engine_instance
