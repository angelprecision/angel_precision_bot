# ap/contract_selection.py - STRICT CONTRACT SELECTION

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from ap.logger import get_logger

log = get_logger("ap.contract_selection")
ET = ZoneInfo("America/New_York")


def pick_expiration(expirations: list[str], hint: str | None) -> str:
    """
    Strict expiration selection.

    Safety rules:
    - 0DTE means TODAY ONLY
    - stale data (all past) => reject
    - otherwise pick nearest future <=30 days, else pick nearest future (warn)
    """
    if not expirations:
        raise ValueError("No expirations available")

    today = datetime.now(ET).date()
    exp_dates = sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in expirations)

    if exp_dates[-1] < today:
        log.error(f"Stale option chain: latest={exp_dates[-1]} today={today}")
        raise ValueError(f"All expirations are in the past (today={today}). Raw={expirations}")

    hint_norm = hint.strip().upper() if hint else None

    if hint_norm == "0DTE":
        if today in exp_dates:
            log.info(f"✅ 0DTE expiration selected: {today}")
            return today.strftime("%Y-%m-%d")
        log.warning(f"❌ 0DTE rejected: today ({today}) not in {expirations}")
        raise ValueError(f"0DTE requested but today ({today}) not in expirations. Raw={expirations}")

    candidates = [d for d in exp_dates if d >= today]
    if not candidates:
        raise ValueError(f"No future expirations available. Raw={expirations}")

    for d in candidates:
        days_out = (d - today).days
        if days_out <= 30:
            log.info(f"✅ Expiration selected: {d} ({days_out}d)")
            return d.strftime("%Y-%m-%d")

    nearest = candidates[0]
    days_out = (nearest - today).days
    log.warning(f"⚠️ All expirations >30d; using nearest future {nearest} ({days_out}d)")
    return nearest.strftime("%Y-%m-%d")


def resolve_contract_symbol(chain: list[dict], strike: float, direction: str) -> str:
    """
    Finds the closest strike option symbol for CALL/PUT.
    Hardened for varying broker keys.
    """
    want = (direction or "").upper()
    if want not in ("CALL", "PUT"):
        raise ValueError(f"Invalid direction: {direction}")

    want_type = "call" if want == "CALL" else "put"

    def _otype(o: dict) -> str:
        return str(
            o.get("option_type")
            or o.get("type")
            or o.get("right")  # "C"/"P"
            or ""
        ).lower()

    def _is_wanted(o: dict) -> bool:
        t = _otype(o)
        if want_type == "call":
            return t in ("call", "c")
        return t in ("put", "p")

    filtered = [o for o in chain if _is_wanted(o)]
    if not filtered:
        raise ValueError(f"No {want} options in chain")

    def dist(o: dict) -> float:
        try:
            return abs(float(o.get("strike", 0)) - float(strike))
        except Exception:
            return float("inf")

    best = min(filtered, key=dist)
    sym = best.get("symbol") or best.get("option_symbol")
    if not sym:
        raise ValueError("No symbol in selected option contract")

    actual = float(best.get("strike", 0) or 0)
    log.info(f"✅ Matched strike ${float(strike):.2f} → ${actual:.2f} ({sym})")
    return sym

