from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

HARD_STOP_PCT = -0.33
IMMEDIATE_TP_PCT = 0.12
PROFIT_LOCK_PCT = 0.12
INDEX_ETFS = {"QQQ", "SPY", "IWM", "DIA", "SPX"}


def et_session_date():
    return datetime.now(ET).date()


def option_expiration_date(option_symbol: str):
    import re

    match = re.search(r"(\d{6})[CP]", option_symbol or "")
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%y%m%d").date()
    except Exception:
        return None


def option_dte(option_symbol: str, *, session_date=None) -> int:
    exp = option_expiration_date(option_symbol or "")
    if exp is None:
        return 999
    sd = session_date or et_session_date()
    return (exp - sd).days


def option_root(option_symbol: str) -> str:
    import re

    sym = (option_symbol or "").upper().strip()
    match = re.search(r"(\d{6})[CP]", sym)
    if not match:
        return sym[:8]
    return sym[:match.start()].strip()


def option_profile(pos, *, session_date=None) -> tuple[int, bool, str]:
    """Return (dte, is_index, profile_label) for a position.

    AMENDMENT (PR #385 review — index-prefix misclassification):
    the previous `any(root.startswith(t) for t in INDEX_ETFS)` matched
    SPXL / SPXS / SPXU (all begin with "SPX") and misclassified those
    3× leveraged equity ETFs as index products.  Under the amended
    engine index membership is exact-root only.

    AMENDMENT (PR #385 review — replay determinism):
    `session_date` is now propagated from the caller (e.g. the ET
    session date derived from evaluate_exit's `now_et`) so historical
    replays cannot silently swap 0DTE for negative-DTE just because the
    host wall-clock has rolled past the replayed date.
    """
    symbol = str(getattr(pos, "option_symbol", getattr(pos, "optionsymbol", "")) or "").upper()
    ticker = str(getattr(pos, "ticker", "") or "").upper()
    root = option_root(symbol)
    dte = option_dte(symbol, session_date=session_date)
    index_roots = INDEX_ETFS | {"SPXW", "NDX", "NDXP", "RUT", "RUTW"}
    is_index = root in index_roots or ticker in index_roots
    profile = "0DTE-idx" if (dte == 0 and is_index) else "0DTE-eq" if dte == 0 else f"{dte}DTE"
    return dte, is_index, profile


def effective_thresholds(pos, *, session_date=None) -> tuple[float, float, float]:
    """Return (hard_stop, immediate_tp, profit_lock) adjusted for DTE/instrument.

    `session_date` forwards to `option_profile` so evaluate_exit callers
    can pin DTE to their supplied evaluation clock rather than the host
    wall-clock date.
    """
    dte, is_index, _ = option_profile(pos, session_date=session_date)
    if dte == 0 and is_index:
        return -0.18, 0.20, 0.08
    if dte == 0:
        return -0.22, 0.22, 0.10
    if dte <= 2:
        return -0.26, 0.25, 0.12
    return HARD_STOP_PCT, IMMEDIATE_TP_PCT, PROFIT_LOCK_PCT
