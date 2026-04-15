# ap/contract_selection.py - DEPRECATED (MED-019)
# The canonical contract selection module is ap/contract_selector.py (APContractSelectionEngine).
# This module is only used by the legacy ap/execution.py path.
# Do not add new code here -- use ap/contract_selector.py instead.
#
# ORIGINAL HEADER:
# ap/contract_selection.py - PRODUCTION-GRADE CONTRACT SELECTION
# =====================================================================
# FIX: resolve_contract_symbol now uses underlying_price (current price)
#      to select ATM/near-ATM strikes instead of using entry_trigger as strike.
#      Entry trigger is the BREACH level, not the strike to buy.
#      Premium cap raised to $10/share to accommodate SPY/QQQ.
# =====================================================================

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

from ap.logger import get_logger

log = get_logger("ap.contract_selection")
ET = ZoneInfo("America/New_York")


class ContractFilters:
    """Production filters tuned for 0DTE options trading"""
    MIN_VOLUME = 100
    MIN_OPEN_INTEREST = 50
    PREFERRED_VOLUME = 500
    MAX_SPREAD_PCT = 0.10
    IDEAL_SPREAD_PCT = 0.05
    MIN_CONTRACT_PRICE = 0.10
    MAX_CONTRACT_PRICE = 10.00    # raised — SPY/QQQ ATM can be $5-10
    IDEAL_MIN_PRICE = 0.50
    IDEAL_MAX_PRICE = 10.00       # raised — accommodate liquid index options
    MIN_DELTA = 0.20              # lowered slightly — near ATM 0DTE
    MAX_DELTA = 0.85
    MAX_THETA = 0.50              # raised — 0DTE theta is always high
    MAX_IV = 5.00                 # raised — 0DTE IV spikes are normal
    MIN_IV = 0.05
    MAX_OTM_DISTANCE_PCT = 0.05
    PREFER_ITM_DISTANCE_PCT = 0.01  # tighter — want very close to money


def _parse_dates(expirations: list[str]):
    return sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in expirations)

def _is_mwf(d):
    return d.weekday() in (0, 2, 4)

def _is_friday(d):
    return d.weekday() == 4


def pick_expiration(expirations: list[str], hint: str | None) -> str:
    if not expirations:
        raise ValueError("No expirations available")

    today = datetime.now(ET).date()
    exp_dates = _parse_dates(expirations)

    if exp_dates[-1] < today:
        raise ValueError(f"All expirations are in the past (today={today})")

    hint_norm = hint.strip().upper() if hint else None
    candidates = [d for d in exp_dates if d >= today]

    if not candidates:
        raise ValueError(f"No future expirations available (today={today})")

    if hint_norm == "0DTE":
        if today in exp_dates:
            log.info(f"✅ 0DTE expiration: {today}")
            return today.strftime("%Y-%m-%d")
        raise ValueError(f"0DTE requested but today ({today}) not available")

    if hint_norm == "DAILY":
        chosen = today if today in exp_dates else candidates[0]
        log.info(f"✅ DAILY expiration: {chosen} ({(chosen-today).days}d out)")
        return chosen.strftime("%Y-%m-%d")

    if hint_norm == "MWF":
        mwf = [d for d in candidates if _is_mwf(d)]
        if mwf:
            log.info(f"✅ MWF expiration: {mwf[0]}")
            return mwf[0].strftime("%Y-%m-%d")
        return candidates[0].strftime("%Y-%m-%d")

    if hint_norm in ("WEEKLY", "WEEK", "EOW", "ENDOFWEEK"):
        fridays = [d for d in candidates if _is_friday(d)]
        if fridays:
            log.info(f"✅ WEEKLY expiration: {fridays[0]}")
            return fridays[0].strftime("%Y-%m-%d")
        return candidates[0].strftime("%Y-%m-%d")

    for d in candidates:
        if (d - today).days <= 30:
            log.info(f"✅ Expiration selected: {d} ({(d-today).days}d out)")
            return d.strftime("%Y-%m-%d")

    nearest = candidates[0]
    log.warning(f"⚠️ All expirations >30d, using nearest: {nearest}")
    return nearest.strftime("%Y-%m-%d")


def _get_strike(o: dict) -> float:
    try:
        return float(o.get("strike") or 0)
    except (ValueError, TypeError):
        return 0.0

def _get_mid_price(o: dict) -> float:
    try:
        bid = float(o.get("bid") or 0)
        ask = float(o.get("ask") or 0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        return 0.0
    except (ValueError, TypeError):
        return 0.0

def _is_liquid(o: dict) -> bool:
    try:
        return (int(o.get("volume") or 0) >= ContractFilters.MIN_VOLUME and
                int(o.get("open_interest") or 0) >= ContractFilters.MIN_OPEN_INTEREST)
    except (ValueError, TypeError):
        return False

def _has_tight_spread(o: dict) -> bool:
    try:
        bid = float(o.get("bid") or 0)
        ask = float(o.get("ask") or 0)
        if bid <= 0 or ask <= 0:
            return False
        mid = (bid + ask) / 2.0
        return ((ask - bid) / mid) <= ContractFilters.MAX_SPREAD_PCT if mid > 0 else False
    except (ValueError, TypeError):
        return False

def _is_price_valid(o: dict) -> bool:
    mid = _get_mid_price(o)
    return ContractFilters.MIN_CONTRACT_PRICE <= mid <= ContractFilters.MAX_CONTRACT_PRICE

def _has_valid_greeks(o: dict) -> bool:
    try:
        delta = abs(float(o.get("delta") or 0))
        theta = abs(float(o.get("theta") or 0))
        if delta == 0:
            return True
        return (ContractFilters.MIN_DELTA <= delta <= ContractFilters.MAX_DELTA and
                theta <= ContractFilters.MAX_THETA)
    except (ValueError, TypeError):
        return True

def _has_sane_iv(o: dict) -> bool:
    try:
        greeks = o.get("greeks") or {}
        iv = float(greeks.get("mid_iv") or greeks.get("iv") or
                   o.get("implied_volatility") or 0)
        if iv == 0:
            return True
        return ContractFilters.MIN_IV <= iv <= ContractFilters.MAX_IV
    except (ValueError, TypeError):
        return True

def _quality_score(o: dict, reference_price: float) -> tuple:
    """Score by closeness to reference price (ATM), then liquidity."""
    try:
        strike = _get_strike(o)
        volume = int(o.get("volume") or 0)
        oi = int(o.get("open_interest") or 0)
        bid = float(o.get("bid") or 0)
        ask = float(o.get("ask") or 0)
        mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else 0
        spread_pct = ((ask - bid) / mid) if mid > 0 else 999.0
        distance = abs(strike - reference_price)
        price_score = 0 if ContractFilters.IDEAL_MIN_PRICE <= mid <= ContractFilters.IDEAL_MAX_PRICE else 1
        return (distance, -volume, -oi, spread_pct, price_score)
    except Exception:
        return (float('inf'), 0, 0, 999.0, 0)


def resolve_contract_symbol(
    chain: list[dict],
    strike: float,             # entry_trigger / signal strike level — used as fallback only
    direction: str,
    *,
    underlying_price: Optional[float] = None,
    prefer_itm: bool = True,
    apply_filters: bool = True,
    log_filtering: bool = True,
) -> str:
    """
    Find the best ATM/near-ATM option contract.

    KEY FIX: `strike` param is the signal's entry trigger (breach level),
    NOT the option strike to buy. We use `underlying_price` (current price)
    as the reference for ATM selection. If underlying_price is not passed,
    we fall back to `strike` as the reference — but callers should always
    pass underlying_price for correct ATM selection.
    """
    want = (direction or "").upper()
    if want not in ("CALL", "PUT"):
        raise ValueError(f"Invalid direction: {direction}")

    if not chain:
        raise ValueError("Empty option chain")

    want_type = "call" if want == "CALL" else "put"

    def _option_type(o: dict) -> str:
        return str(o.get("option_type") or o.get("type") or o.get("right") or "").lower()

    def _is_wanted_type(o: dict) -> bool:
        t = _option_type(o)
        return t in ("call", "c") if want_type == "call" else t in ("put", "p")

    # Type filter
    filtered = [o for o in chain if _is_wanted_type(o)]
    if not filtered:
        raise ValueError(f"No {want} options in chain ({len(chain)} total)")
    if log_filtering:
        log.info(f"Type filter: {len(chain)} total → {len(filtered)} {want}s")

    # Quality filters
    if apply_filters:
        liquid       = [o for o in filtered      if _is_liquid(o)]
        tight_spread = [o for o in liquid        if _has_tight_spread(o)]
        price_ok     = [o for o in tight_spread  if _is_price_valid(o)]
        greeks_ok    = [o for o in price_ok      if _has_valid_greeks(o)]
        iv_ok        = [o for o in greeks_ok     if _has_sane_iv(o)]

        if log_filtering:
            log.info(f"Liquidity filter: {len(filtered)} → {len(liquid)} (volume≥{ContractFilters.MIN_VOLUME}, OI≥{ContractFilters.MIN_OPEN_INTEREST})")
            log.info(f"Spread filter: {len(liquid)} → {len(tight_spread)} (spread≤{ContractFilters.MAX_SPREAD_PCT*100:.0f}%)")
            log.info(f"Price filter: {len(tight_spread)} → {len(price_ok)} (${ContractFilters.MIN_CONTRACT_PRICE:.2f}-${ContractFilters.MAX_CONTRACT_PRICE:.2f})")

        # Use best available set (graceful degradation)
        if iv_ok:
            filtered = iv_ok
            if log_filtering:
                log.info(f"✅ {len(filtered)} high-quality contracts remaining after all filters")
        elif greeks_ok:
            filtered = greeks_ok
            log.warning(f"⚠️ IV filter too strict, using {len(filtered)} pre-IV contracts")
        elif price_ok:
            filtered = price_ok
            log.warning(f"⚠️ Greeks filter too strict, using {len(filtered)} pre-Greeks contracts")
        elif tight_spread:
            filtered = tight_spread
            log.warning(f"⚠️ Price filter too strict, using {len(filtered)} spread-filtered contracts")
        elif liquid:
            filtered = liquid
            log.warning(f"⚠️ Spread filter too strict, using {len(filtered)} liquid contracts")
        else:
            log.error(f"❌ All filters rejected — using type-only filtered list ({len(filtered)} contracts)")

    # FIX: use underlying_price as ATM reference, not the signal entry trigger
    # The entry trigger is the BREACH LEVEL (e.g. $520.50 for SPY)
    # The ATM strike is the current market price (also ~$520 for SPY)
    # These are often the same or very close — but we must use underlying_price
    # so we pick a real ATM strike, not try to match an exact trigger price.
    atm_reference = float(underlying_price) if underlying_price else float(strike)

    if prefer_itm and underlying_price is not None:
        up = float(underlying_price)

        if want == "CALL":
            # ITM CALL: strike <= underlying
            itm = [o for o in filtered if _get_strike(o) <= up]
            if itm:
                best = min(itm, key=lambda o: _quality_score(o, up))
                sym = best.get("symbol") or best.get("option_symbol")
                if not sym:
                    raise ValueError("No symbol field in contract")
                log.info(
                    f"✅ ITM CALL: strike=${_get_strike(best):.2f} underlying=${up:.2f} "
                    f"mid=${_get_mid_price(best):.2f} vol={best.get('volume',0)} OI={best.get('open_interest',0)} | {sym}"
                )
                return sym
        else:
            # ITM PUT: strike >= underlying
            itm = [o for o in filtered if _get_strike(o) >= up]
            if itm:
                best = min(itm, key=lambda o: _quality_score(o, up))
                sym = best.get("symbol") or best.get("option_symbol")
                if not sym:
                    raise ValueError("No symbol field in contract")
                log.info(
                    f"✅ ITM PUT: strike=${_get_strike(best):.2f} underlying=${up:.2f} "
                    f"mid=${_get_mid_price(best):.2f} vol={best.get('volume',0)} OI={best.get('open_interest',0)} | {sym}"
                )
                return sym

        log.warning(f"⚠️ No ITM {want} found — falling back to closest to ATM")

    # Fallback: closest to ATM reference
    best = min(filtered, key=lambda o: _quality_score(o, atm_reference))
    sym = best.get("symbol") or best.get("option_symbol")
    if not sym:
        raise ValueError("No symbol field in contract")

    log.info(
        f"✅ MATCHED: ATM=${atm_reference:.2f} → strike=${_get_strike(best):.2f} "
        f"mid=${_get_mid_price(best):.2f} vol={best.get('volume',0)} OI={best.get('open_interest',0)} | {sym}"
    )
    return sym


def analyze_chain_quality(chain: list[dict], direction: str = "CALL") -> dict:
    if not chain:
        return {"error": "Empty chain"}
    want_type = "call" if direction.upper() == "CALL" else "put"
    def _is_type(o):
        t = str(o.get("option_type") or o.get("type") or "").lower()
        return t in ("call","c") if want_type=="call" else t in ("put","p")
    filtered = [o for o in chain if _is_type(o)]
    if not filtered:
        return {"error": f"No {direction} options"}
    return {
        "total_contracts":        len(filtered),
        "liquid_contracts":       sum(1 for o in filtered if _is_liquid(o)),
        "tight_spread_contracts": sum(1 for o in filtered if _has_tight_spread(o)),
        "valid_price_contracts":  sum(1 for o in filtered if _is_price_valid(o)),
        "avg_volume":  sum(int(o.get("volume") or 0) for o in filtered) / len(filtered),
        "avg_oi":      sum(int(o.get("open_interest") or 0) for o in filtered) / len(filtered),
        "avg_mid_price": sum(_get_mid_price(o) for o in filtered) / len(filtered),
    }
