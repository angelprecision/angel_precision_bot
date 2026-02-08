# ap/contract_selection.py - STRICT CONTRACT SELECTION (MWF/DAILY/WEEKLY + ITM strike)
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from ap.logger import get_logger

log = get_logger("ap.contract_selection")
ET = ZoneInfo("America/New_York")

# ---------- Expiration helpers ----------

def _parse_dates(expirations: list[str]):
    return sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in expirations)

def _is_mwf(d):
    # Mon=0 Wed=2 Fri=4
    return d.weekday() in (0, 2, 4)

def _is_friday(d):
    return d.weekday() == 4

def pick_expiration(expirations: list[str], hint: str | None) -> str:
    """
    Strict expiration selection.

    Supported hint values (case-insensitive):
    - 0DTE  : TODAY ONLY (strict)
    - DAILY : pick today if available else next available
    - MWF   : pick nearest M/W/F (>= today)
    - WEEKLY: pick nearest Friday (>= today)
    - None/other: pick nearest future <=30d else nearest future
    """
    if not expirations:
        raise ValueError("No expirations available")

    today = datetime.now(ET).date()
    exp_dates = _parse_dates(expirations)

    if exp_dates[-1] < today:
        log.error(f"Stale option chain: latest={exp_dates[-1]} today={today}")
        raise ValueError(f"All expirations are in the past (today={today}). Raw={expirations}")

    hint_norm = hint.strip().upper() if hint else None
    candidates = [d for d in exp_dates if d >= today]
    if not candidates:
        raise ValueError(f"No future expirations available. Raw={expirations}")

    # 0DTE strict
    if hint_norm == "0DTE":
        if today in exp_dates:
            log.info(f"✅ 0DTE expiration selected: {today}")
            return today.strftime("%Y-%m-%d")
        log.warning(f"❌ 0DTE rejected: today ({today}) not in {expirations}")
        raise ValueError(f"0DTE requested but today ({today}) not in expirations. Raw={expirations}")

    # DAILY (practical)
    if hint_norm == "DAILY":
        chosen = today if today in exp_dates else candidates[0]
        log.info(f"✅ DAILY expiration selected: {chosen}")
        return chosen.strftime("%Y-%m-%d")

    # MWF
    if hint_norm == "MWF":
        mwf = [d for d in candidates if _is_mwf(d)]
        if mwf:
            log.info(f"✅ MWF expiration selected: {mwf[0]}")
            return mwf[0].strftime("%Y-%m-%d")
        # fallback
        log.warning("⚠️ MWF requested but none found; using nearest future")
        return candidates[0].strftime("%Y-%m-%d")

    # WEEKLY = "end of week" preference
    if hint_norm in ("WEEKLY", "WEEK", "EOW", "ENDOFWEEK"):
        fridays = [d for d in candidates if _is_friday(d)]
        if fridays:
            log.info(f"✅ WEEKLY expiration selected (Friday): {fridays[0]}")
            return fridays[0].strftime("%Y-%m-%d")
        # fallback
        log.warning("⚠️ WEEKLY requested but no Friday found; using nearest future")
        return candidates[0].strftime("%Y-%m-%d")

    # Default behavior: nearest <=30 days else nearest future
    for d in candidates:
        days_out = (d - today).days
        if days_out <= 30:
            log.info(f"✅ Expiration selected: {d} ({days_out}d)")
            return d.strftime("%Y-%m-%d")

    nearest = candidates[0]
    days_out = (nearest - today).days
    log.warning(f"⚠️ All expirations >30d; using nearest future {nearest} ({days_out}d)")
    return nearest.strftime("%Y-%m-%d")


# ---------- Strike selection (closest ITM) ----------

def resolve_contract_symbol(
    chain: list[dict],
    strike: float,
    direction: str,
    *,
    underlying_price: float | None = None,
    prefer_itm: bool = True,
) -> str:
    """
    Finds the best option symbol for CALL/PUT.

    If underlying_price is provided and prefer_itm=True:
      - CALL: choose closest ITM strike (<= underlying)
      - PUT : choose closest ITM strike (>= underlying)
    Fallback: choose closest strike to requested strike.
    """
    want = (direction or "").upper()
    if want not in ("CALL", "PUT"):
        raise ValueError(f"Invalid direction: {direction}")

    want_type = "call" if want == "CALL" else "put"

    def _otype(o: dict) -> str:
        return str(o.get("option_type") or o.get("type") or o.get("right") or "").lower()

    def _is_wanted(o: dict) -> bool:
        t = _otype(o)
        if want_type == "call":
            return t in ("call", "c")
        return t in ("put", "p")

    def _strike(o: dict) -> float:
        try:
            return float(o.get("strike"))
        except Exception:
            return float("nan")

    filtered = [o for o in chain if _is_wanted(o)]
    if not filtered:
        raise ValueError(f"No {want} options in chain")

    # Prefer closest ITM
    if prefer_itm and (underlying_price is not None):
        up = float(underlying_price)
        if want == "CALL":
            itms = [o for o in filtered if _strike(o) <= up]
            if itms:
                best = min(itms, key=lambda o: abs(_strike(o) - up))
                sym = best.get("symbol") or best.get("option_symbol")
                if not sym:
                    raise ValueError("No symbol in selected option contract")
                log.info(f"✅ ITM CALL selected: strike={_strike(best):.2f} underlying={up:.2f} ({sym})")
                return sym
        else:  # PUT
            itms = [o for o in filtered if _strike(o) >= up]
            if itms:
                best = min(itms, key=lambda o: abs(_strike(o) - up))
                sym = best.get("symbol") or best.get("option_symbol")
                if not sym:
                    raise ValueError("No symbol in selected option contract")
                log.info(f"✅ ITM PUT selected: strike={_strike(best):.2f} underlying={up:.2f} ({sym})")
                return sym

    # Fallback: closest to requested strike
    def dist(o: dict) -> float:
        try:
            return abs(_strike(o) - float(strike))
        except Exception:
            return float("inf")

    best = min(filtered, key=dist)
    sym = best.get("symbol") or best.get("option_symbol")
    if not sym:
        raise ValueError("No symbol in selected option contract")

    actual = _strike(best)
    log.info(f"✅ Matched strike {float(strike):.2f} → {actual:.2f} ({sym})")
    return sym

