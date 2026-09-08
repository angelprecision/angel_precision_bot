"""
P0 — #589 canonical executable-underlying sector coverage.

Proves every underlying that can actually reach entry execution resolves to a
canonical risk sector via ap.exposure_gate.get_sector, so Master Control (after
#548) never falls back to a synthetic shared bucket. The executable set is
derived independently from real production entry authorities, not by echoing the
production map's own keys.

Caret-prefixed benchmark aliases (e.g. ^GSPC) are intentionally excluded: they
are normalized to ETF proxies before execution (ap_master_control._INDEX_TO_ETF,
contract_selector._INDEX_MAP, queue._INDEX_ALIAS), so they never reach the
resolver as an order underlying.
"""
import pytest

from ap.exposure_gate import get_sector, check_exposure, SECTOR_MAP, MAX_OPEN_PER_SECTOR

# --- Independently derived executable universe (from production sources) ------
from ap.scanner_utils import TICKERS, _ZERO_DTE
import ap_master_control as _mc
import ap.contract_selector as _cs

_SYNTHETIC_UNKNOWNS = ("ZZUNKNOWN1", "ZZUNKNOWN2")
# Benchmark aliases normalized to ETF proxies before execution; not executable
# underlyings in their own right. Kept as an explicit, minimal allow-list.
_NON_EXECUTABLE_BENCHMARK_ALIASES = {"^GSPC", "^NDX", "^RUT", "^DJI"}


def _executable_universe():
    prio = set(getattr(_mc, "_PRIORITY_TICKERS", set()))
    prem = {str(k).upper() for k in getattr(_mc, "_PREMIUM_ESTIMATES", {})}
    csk = {str(k).upper() for k in getattr(_cs, "TICKER_MAX_PREMIUM_PER_CONTRACT", {})}
    for junk in ("_DEFAULT",):
        prem.discard(junk)
        csk.discard(junk)
    raw = set(TICKERS) | set(_ZERO_DTE) | prio | prem | csk
    raw = {s for s in raw if s and not s.startswith("^")}
    raw -= _NON_EXECUTABLE_BENCHMARK_ALIASES
    return sorted(raw)


EXECUTABLE = _executable_universe()
_FORBIDDEN_SECTOR_VALUES = {"OTHER", "UNKNOWN", "MISC", "UNMAPPED", "SECTOR_UNKNOWN", ""}


# --- §10 mandatory coverage invariant ----------------------------------------
@pytest.mark.parametrize("symbol", EXECUTABLE)
def test_every_executable_underlying_has_canonical_sector(symbol):
    sector = get_sector(symbol)
    assert sector is not None, (
        f"executable underlying {symbol!r} resolves to None; it would skip the "
        f"sector cap once Master Control consumes this map (#589)"
    )
    assert sector not in _FORBIDDEN_SECTOR_VALUES


def test_no_executable_underlying_unmapped_in_bulk():
    unmapped = [s for s in EXECUTABLE if get_sector(s) is None]
    assert unmapped == [], f"unmapped executable underlyings: {unmapped}"


def test_map_hygiene_no_blank_or_synthetic_values():
    for sym, sec in SECTOR_MAP.items():
        assert sym and sym.strip() == sym, f"blank/dirty symbol key: {sym!r}"
        assert sym.upper() == sym, f"non-uppercase symbol key: {sym!r}"
        assert sec and sec.strip() == sec, f"blank sector for {sym!r}"
        assert sec.upper() not in _FORBIDDEN_SECTOR_VALUES, f"synthetic bucket {sec!r} for {sym!r}"


# --- §11 positive identity ----------------------------------------------------
@pytest.mark.parametrize("symbol,expected", [
    ("BMY", "HEALTHCARE"), ("NEE", "UTILITIES"), ("PEP", "CONSUMER"),
    ("SPX", "INDEX"), ("NDX", "INDEX"), ("COIN", "CRYPTO"), ("MSTR", "CRYPTO"),
    ("T", "COMMUNICATION"), ("VZ", "COMMUNICATION"),
    ("AMT", "REAL_ESTATE"), ("LIN", "MATERIALS"),
    ("CSCO", "TECH"), ("ABT", "HEALTHCARE"), ("HON", "INDUSTRIAL"),
])
def test_positive_identity(symbol, expected):
    assert get_sector(symbol) == expected


@pytest.mark.parametrize("raw,expected", [
    (" csco ", "TECH"), ("nee", "UTILITIES"), (" spx ", "INDEX"),
])
def test_case_and_whitespace(raw, expected):
    assert get_sector(raw) == expected


@pytest.mark.parametrize("bad", list(_SYNTHETIC_UNKNOWNS) + [None, "", "   "])
def test_unknown_controls_stay_none(bad):
    assert get_sector(bad) is None


# --- §12 same-sector / cross-sector behavior through the real gate ------------
def _open(*syms):
    return [{"underlying": s, "symbol": s} for s in syms]


@pytest.mark.parametrize("existing,candidate,sector", [
    ("UNH", "BMY", "HEALTHCARE"),
    ("WMT", "PEP", "CONSUMER"),
    ("T", "VZ", "COMMUNICATION"),
    ("SPY", "SPX", "INDEX"),
    ("COIN", "MSTR", "CRYPTO"),
    ("AMT", "SPG", "REAL_ESTATE"),
    ("LIN", "DOW", "MATERIALS"),
])
def test_same_sector_aggregates(existing, candidate, sector):
    r = check_exposure("t", candidate, open_positions=_open(existing))
    assert r["sector"] == sector
    assert r["open_same_sector"] == 1, f"{existing} should aggregate with {candidate} in {sector}"


@pytest.mark.parametrize("existing,candidate", [
    ("QQQ", "BMY"), ("QQQ", "NEE"), ("CSCO", "KO"),
    ("T", "AAPL"), ("AMT", "JPM"), ("COIN", "NVDA"),
])
def test_cross_sector_does_not_aggregate(existing, candidate):
    r = check_exposure("t", candidate, open_positions=_open(existing))
    assert r["open_same_sector"] == 0, f"{existing} must NOT aggregate with {candidate}"
    assert r["ok"] is True


def test_sector_cap_still_blocks_when_saturated():
    # Two distinct healthcare names already open -> a third healthcare candidate
    # is blocked by the real sector cap (proves protection is intact).
    assert MAX_OPEN_PER_SECTOR == 2
    r = check_exposure("t", "BMY", open_positions=_open("UNH", "JNJ"))
    assert r["open_same_sector"] == 2
    assert r["ok"] is False
    assert r["reason_code"] == "SECTOR_EXPOSURE_LIMIT"


def test_unknown_candidate_skips_sector_cap_but_not_symbol():
    # Genuinely unknown candidate: no sector aggregation, sector cap N/A.
    r = check_exposure("t", "ZZUNKNOWN1", open_positions=_open("ZZUNKNOWN2", "AAPL"))
    assert r["sector"] is None
    assert r["open_same_sector"] is None
    assert r["ok"] is True


# --- §13 existing-behavior preservation ---------------------------------------
@pytest.mark.parametrize("symbol,expected", [
    ("AAPL", "TECH"), ("QCOM", "TECH"), ("TSLA", "AUTO"), ("JPM", "FINANCIAL"),
    ("UNH", "HEALTHCARE"), ("XOM", "ENERGY"), ("WMT", "CONSUMER"),
    ("BA", "INDUSTRIAL"), ("SPY", "INDEX"), ("QQQ", "INDEX"),
    ("IWM", "INDEX"), ("DIA", "INDEX"),
])
def test_existing_identity_preserved(symbol, expected):
    assert get_sector(symbol) == expected


# --- §15 paper/live parity (identity is symbol-only, mode-independent) --------
@pytest.mark.parametrize("symbol", ["QQQ", "BMY", "NEE", "COIN", "SPX", "ZZUNKNOWN1"])
def test_mode_independent_identity(symbol):
    # get_sector takes only the symbol; there is no mode/env parameter/branch.
    assert get_sector(symbol) == get_sector(symbol)
    import os
    prev = os.environ.get("EXECUTION_MODE")
    try:
        os.environ["EXECUTION_MODE"] = "LIVE"
        live = get_sector(symbol)
        os.environ["EXECUTION_MODE"] = "PAPER"
        paper = get_sector(symbol)
        assert live == paper
    finally:
        if prev is None:
            os.environ.pop("EXECUTION_MODE", None)
        else:
            os.environ["EXECUTION_MODE"] = prev
