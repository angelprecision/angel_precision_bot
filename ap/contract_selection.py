# ap/contract_selection.py
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

def pick_expiration(expirations: list[str], hint: str | None) -> str:
    if not expirations:
        raise ValueError("No expirations available")

    today = datetime.now(ET).date()

    # Tradier dates come as "YYYY-MM-DD"
    exp_dates = [datetime.strptime(d, "%Y-%m-%d").date() for d in expirations]
    exp_dates.sort()

    if hint and hint.upper() == "0DTE":
        # choose today if available, else next
        for d in exp_dates:
            if d == today:
                return d.strftime("%Y-%m-%d")
        return exp_dates[0].strftime("%Y-%m-%d")

    # Weekly/default: choose next expiration >= today
    for d in exp_dates:
        if d >= today:
            return d.strftime("%Y-%m-%d")
    return exp_dates[-1].strftime("%Y-%m-%d")

def resolve_contract_symbol(chain: list[dict], strike: float, direction: str) -> str:
    """
    direction: CALL or PUT
    chain item fields typically include:
      - symbol (option symbol)
      - strike
      - option_type ('call'/'put')
    """
    want_type = "call" if direction.upper() == "CALL" else "put"
    filtered = [o for o in chain if str(o.get("option_type","")).lower() == want_type]
    if not filtered:
        raise ValueError("No options of desired type in chain")

    # choose closest strike
    def dist(o):
        return abs(float(o.get("strike")) - float(strike))

    best = min(filtered, key=dist)
    sym = best.get("symbol")
    if not sym:
        raise ValueError("No option symbol in selected contract")
    return sym
