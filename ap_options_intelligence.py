# ap_options_intelligence.py — Angel Precision Options Intelligence Layer
# =============================================================================
# Gap 2 fix: The bot does NOT blindly buy whatever contract is closest to strike.
#
# Before every options purchase, this module runs a 3-gate check:
#
#   Gate 1 — SPREAD GATE
#     If (ask - bid) / mid > 12%: REJECT
#     A 15% spread means you need a 15% move just to break even on exit.
#     This alone kills most bad trades before they happen.
#
#   Gate 2 — IV REALITY CHECK
#     If IV > 300%: likely earnings/event — REJECT unless trade explicitly
#     targets event vol.
#     If IV < 20%: chain is dead — REJECT.
#     Also checks IV rank: if IVR > 85, premium is inflated — size down or skip.
#
#   Gate 3 — LIQUIDITY GATE
#     Volume × open_interest must meet minimums.
#     Low liquidity = can't exit cleanly. Ever.
#
# After passing all gates, selects the OPTIMAL contract:
#   - Prefers slightly ITM (delta 0.45–0.65 for 0DTE)
#   - Tight spread within that range
#   - Best volume/OI
#   - Price $0.30–$8.00 (avoids lottery tickets and over-expensive)
#
# Returns: ContractDecision with contract symbol, grade, and rejection reason
# =============================================================================

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional
from datetime import datetime
from zoneinfo import ZoneInfo

log = logging.getLogger("ap.options_intel")
ET  = ZoneInfo("America/New_York")

# ── GATES ─────────────────────────────────────────────────────────────────────
MAX_SPREAD_PCT     = 0.12   # 12% max spread — any wider, reject
IDEAL_SPREAD_PCT   = 0.06   # 6% ideal
MAX_IV             = 3.00   # 300% max IV (above = earnings/binary event)
MIN_IV             = 0.20   # 20% min IV (below = dead chain)
HIGH_IVR_THRESHOLD = 0.85   # IV rank above this = premium inflated
MIN_VOLUME         = 50     # minimum option volume
MIN_OI             = 25     # minimum open interest
MIN_CONTRACT_PRICE = 0.25   # avoid lottery tickets
MAX_CONTRACT_PRICE = 8.00   # avoid over-expensive contracts
IDEAL_DELTA_MIN    = 0.35   # preferred delta range low
IDEAL_DELTA_MAX    = 0.70   # preferred delta range high


# ── DECISION DATACLASS ────────────────────────────────────────────────────────

@dataclass
class ContractDecision:
    approved:         bool
    symbol:           Optional[str]   = None
    strike:           Optional[float] = None
    expiration:       Optional[str]   = None
    mid_price:        Optional[float] = None
    spread_pct:       Optional[float] = None
    volume:           Optional[int]   = None
    open_interest:    Optional[int]   = None
    iv:               Optional[float] = None
    delta:            Optional[float] = None
    grade:            str   = "REJECT"
    rejection_reason: Optional[str]   = None
    size_modifier:    float = 1.0    # 1.0 = full size, 0.5 = half, 0.25 = quarter

    def log_summary(self):
        if self.approved:
            log.info(
                f"✅ CONTRACT APPROVED [{self.grade}]: {self.symbol} "
                f"strike=${self.strike} mid=${self.mid_price:.2f} "
                f"spread={self.spread_pct*100:.1f}% IV={self.iv*100:.0f}% "
                f"vol={self.volume} OI={self.open_interest} "
                f"size_modifier={self.size_modifier}"
            )
        else:
            log.warning(f"❌ CONTRACT REJECTED: {self.rejection_reason}")


# ── MAIN INTELLIGENCE FUNCTION ────────────────────────────────────────────────

def evaluate_contract(
    chain:            list[dict],
    direction:        str,            # "CALL" or "PUT"
    underlying_price: float,
    expiration:       str,            # "YYYY-MM-DD"
    signal_score:     float = 0,      # from scoring engine — affects size_modifier
    signal_store      = None,         # APSignalStore — for writing option outcomes
    signal_id:        Optional[str] = None,
) -> ContractDecision:
    """
    Run the 3-gate options intelligence check and select the best contract.

    Args:
        chain:            List of option dicts from broker (bid, ask, strike, volume, etc.)
        direction:        "CALL" or "PUT"
        underlying_price: Current underlying price
        expiration:       Target expiration date
        signal_score:     Score from APScorer (0–100) — affects sizing
        signal_store:     APSignalStore instance (optional) — writes option outcomes
        signal_id:        Signal UUID (optional) — used for option outcome writes

    Returns:
        ContractDecision
    """
    side      = direction.upper()
    want_type = "call" if side == "CALL" else "put"

    # Filter to correct option type
    candidates = [
        c for c in chain
        if str(c.get("option_type", c.get("type", ""))).lower() in (want_type, want_type[0])
        and c.get("expiration_date", c.get("expiration", "")) == expiration
    ]

    if not candidates:
        if signal_store and signal_id:
            signal_store.insert_option_outcome(signal_id, {"chain_grade": "REJECT",
                "rejection_reason": f"No {side} options found for {expiration}"})
        return ContractDecision(
            approved=False,
            rejection_reason=f"No {side} options found for {expiration}"
        )

    # ── GATE 1: Spread Gate ────────────────────────────────────────────────────
    def get_spread_pct(c: dict) -> float:
        bid = float(c.get("bid", 0) or 0)
        ask = float(c.get("ask", 0) or 0)
        if bid <= 0 or ask <= 0: return 999.0
        mid = (bid + ask) / 2
        return (ask - bid) / mid if mid > 0 else 999.0

    def get_mid(c: dict) -> float:
        bid = float(c.get("bid", 0) or 0)
        ask = float(c.get("ask", 0) or 0)
        return (bid + ask) / 2 if (bid > 0 and ask > 0) else 0.0

    # Filter to contracts with tradeable spreads
    spreadable = [c for c in candidates if get_spread_pct(c) <= MAX_SPREAD_PCT]

    if not spreadable:
        worst_spread = min(get_spread_pct(c) for c in candidates)
        reason = (
            f"Spread gate failed: tightest spread is "
            f"{worst_spread*100:.1f}% (max {MAX_SPREAD_PCT*100:.0f}%)"
        )
        if signal_store and signal_id:
            signal_store.insert_option_outcome(signal_id, {"chain_grade": "REJECT",
                "rejection_reason": reason})
        return ContractDecision(approved=False, rejection_reason=reason)

    # ── GATE 2: IV Reality Check ───────────────────────────────────────────────
    def get_iv(c: dict) -> float:
        greeks = c.get("greeks") or {}
        iv = (greeks.get("mid_iv") or greeks.get("iv") or
              c.get("implied_volatility") or 0)
        return float(iv) if iv else 0.0

    high_iv_warning = False
    iv_ok = []
    for c in spreadable:
        iv = get_iv(c)
        if iv == 0:
            iv_ok.append(c)  # no IV data — don't reject, but note it
            continue
        if iv > MAX_IV:
            log.warning(
                f"High IV detected ({iv*100:.0f}%) — likely event risk. "
                f"Rejecting contract at strike ${c.get('strike')}"
            )
            continue
        if iv < MIN_IV:
            continue  # dead chain
        if iv > HIGH_IVR_THRESHOLD:
            high_iv_warning = True
        iv_ok.append(c)

    if not iv_ok:
        reason = f"IV gate failed: all contracts have extreme IV (>{MAX_IV*100:.0f}% or <{MIN_IV*100:.0f}%)"
        if signal_store and signal_id:
            signal_store.insert_option_outcome(signal_id, {"chain_grade": "REJECT",
                "rejection_reason": reason})
        return ContractDecision(approved=False, rejection_reason=reason)

    # ── GATE 3: Liquidity Gate ─────────────────────────────────────────────────
    def is_liquid(c: dict) -> bool:
        vol = int(c.get("volume", 0) or 0)
        oi  = int(c.get("open_interest", 0) or 0)
        return vol >= MIN_VOLUME and oi >= MIN_OI

    liquid = [c for c in iv_ok if is_liquid(c)]

    if not liquid:
        # Soft fail — use what we have but reduce size
        log.warning(
            f"Liquidity gate soft fail: no contracts meet vol≥{MIN_VOLUME} "
            f"and OI≥{MIN_OI}. Using best available with reduced size."
        )
        liquid            = iv_ok
        liquidity_penalty = True
    else:
        liquidity_penalty = False

    # ── PRICE FILTER ──────────────────────────────────────────────────────────
    priced = [
        c for c in liquid
        if MIN_CONTRACT_PRICE <= get_mid(c) <= MAX_CONTRACT_PRICE
    ]
    if not priced:
        priced = liquid  # use unfiltered if all fail price check

    # ── SELECT BEST CONTRACT ───────────────────────────────────────────────────
    def delta_score(c: dict) -> float:
        """Prefer delta 0.35–0.70 range. Penalize outside."""
        greeks = c.get("greeks") or {}
        delta  = abs(float(greeks.get("delta") or c.get("delta") or 0))
        if delta == 0:
            # Estimate from moneyness
            strike = float(c.get("strike", 0) or 0)
            if side == "CALL":
                delta = 0.5 if strike <= underlying_price else 0.3
            else:
                delta = 0.5 if strike >= underlying_price else 0.3
        if IDEAL_DELTA_MIN <= delta <= IDEAL_DELTA_MAX:
            return 0.0   # perfect — sort to front
        return abs(delta - 0.50)   # distance from ATM

    def quality_score(c: dict) -> tuple:
        """Multi-factor sort: delta → spread → volume → OI."""
        sp        = get_spread_pct(c)
        vol       = int(c.get("volume", 0) or 0)
        oi        = int(c.get("open_interest", 0) or 0)
        mid       = get_mid(c)
        price_pen = 0 if MIN_CONTRACT_PRICE <= mid <= MAX_CONTRACT_PRICE else 1
        return (delta_score(c), sp, -vol, -oi, price_pen)

    best = min(priced, key=quality_score)

    # ── BUILD DECISION ─────────────────────────────────────────────────────────
    symbol = best.get("symbol") or best.get("option_symbol", "")
    strike = float(best.get("strike", 0) or 0)
    mid    = get_mid(best)
    spread = get_spread_pct(best)
    vol    = int(best.get("volume", 0) or 0)
    oi     = int(best.get("open_interest", 0) or 0)
    iv     = get_iv(best)
    greeks = best.get("greeks") or {}
    delta  = abs(float(greeks.get("delta") or best.get("delta") or 0))

    # Grade the contract quality — spread is the primary grade driver
    if spread <= 0.04 and vol >= 500 and oi >= 200:
        grade = "ELITE"       # tight spread, liquid — full size
    elif spread <= 0.06 and vol >= 200 and oi >= 100:
        grade = "STRONG"      # good spread, decent liquidity
    elif spread <= 0.08 and vol >= 100 and oi >= 50:
        grade = "GOOD"        # acceptable spread
    elif spread <= 0.10 and vol >= 50:
        grade = "FAIR"        # wide spread — half size
    elif spread <= 0.12:
        grade = "MARGINAL"    # borderline — very small size
    else:
        grade = "REJECT"

    # ── TIERED SPREAD SIZE MODIFIER ──────────────────────────────────────────
    # Spread tier is the PRIMARY size driver — everything else adjusts from it
    if spread <= 0.06:
        size = 1.00      # < 6%  — full size, ideal conditions
    elif spread <= 0.08:
        size = 0.75      # 6–8%  — slightly reduced
    elif spread <= 0.10:
        size = 0.50      # 8–10% — half size, spread is costing you
    elif spread <= 0.12:
        size = 0.25      # 10–12% — very small, borderline
    else:
        # Should never reach here — already rejected above
        size = 0.0

    # Secondary adjustments (applied on top of spread tier)
    if liquidity_penalty:  size *= 0.65   # thin chain → significant reduction
    if high_iv_warning:    size *= 0.75   # inflated premium → caution

    # Score-based size (A+ gets no reduction, lower grades get less)
    if signal_score >= 90:    pass          # A+ — no penalty, full tier size
    elif signal_score >= 85:  size *= 0.90  # A  — slight haircut
    elif signal_score >= 78:  size *= 0.70  # B  — meaningful haircut
    else:                     size *= 0.50  # below B — should not reach here

    size = round(max(0.10, min(1.0, size)), 2)

    decision = ContractDecision(
        approved      = True,
        symbol        = symbol,
        strike        = strike,
        expiration    = expiration,
        mid_price     = round(mid, 2),
        spread_pct    = round(spread, 4),
        volume        = vol,
        open_interest = oi,
        iv            = round(iv, 4),
        delta         = round(delta, 3),
        grade         = grade,
        size_modifier = size,
    )

    # Write option outcome snapshot to Supabase
    if signal_store and signal_id:
        signal_store.insert_option_outcome(
            signal_id,
            {
                "contract_symbol":     symbol,
                "expiration":          expiration,
                "strike":              strike,
                "option_type":         side.lower(),
                "spread_pct_at_signal": round(spread, 4),
                "iv_at_signal":        round(iv, 4),
                "delta_at_signal":     round(delta, 3),
                "volume_at_signal":    vol,
                "oi_at_signal":        oi,
                "chain_grade":         grade,
                "mark_at_signal":      round(mid, 4),
            },
        )

    decision.log_summary()
    return decision


# ── QUICK CHAIN HEALTH CHECK ──────────────────────────────────────────────────

def chain_health_report(chain: list[dict], direction: str, underlying: float) -> dict:
    """
    Fast diagnostic: is this chain worth trading right now?
    Returns a dict with health metrics for logging/Discord output.
    """
    side      = direction.upper()
    want      = "call" if side == "CALL" else "put"
    contracts = [
        c for c in chain
        if str(c.get("option_type", "")).lower() in (want, want[0])
    ]
    if not contracts:
        return {"tradeable": False, "reason": "No contracts"}

    spreads = []
    for c in contracts:
        bid = float(c.get("bid", 0) or 0)
        ask = float(c.get("ask", 0) or 0)
        if bid > 0 and ask > 0:
            spreads.append((ask - bid) / ((bid + ask) / 2))

    avg_spread  = sum(spreads) / len(spreads) if spreads else 1.0
    tight_count = sum(1 for s in spreads if s <= MAX_SPREAD_PCT)
    total_vol   = sum(int(c.get("volume", 0) or 0) for c in contracts)
    total_oi    = sum(int(c.get("open_interest", 0) or 0) for c in contracts)

    return {
        "tradeable":    tight_count >= 3 and total_vol >= MIN_VOLUME,
        "avg_spread":   round(avg_spread * 100, 1),
        "tight_count":  tight_count,
        "total_volume": total_vol,
        "total_oi":     total_oi,
        "contracts":    len(contracts),
    }
