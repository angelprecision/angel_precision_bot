# tests/test_p0_underlying_quote_hydration.py
# P0: underlying quote hydration at metadata validation.
#
# Evidence base (live Supabase, 2026-06-29 → 2026-07-03):
#   301 signals rejected metadata_invalid:zero_underlying; 0/301 raw payloads
#   carried an underlying key under any alias. Producers: daily Strat scanner
#   (245) and failed_dir_intraday_{1,2,3}tf (56).
#
# Invariants under test:
#   T1  No resolver registered → behavior identical to pre-PR (fail closed).
#   T2  Resolver returns positive price → signal hydrated, full revalidation
#       passes, marker + provenance stamped, canonical keys injected.
#   T3  Resolver returns None / 0 / negative / garbage → fail closed.
#   T4  Resolver raises → fail closed, no exception escapes.
#   T5  Never overwrite an existing positive underlying.
#   T6  Hydration succeeds but ANOTHER field invalid → new reason surfaced,
#       signal still rejected (hydration alone never passes a signal).
#   T7  Broker resolver price preference: last > mid(bid,ask) > close; zero
#       fields (holiday/closed-market Tradier quotes) treated as absent.
#   T8  Intraday timeframe (60m) hydrates — the population with no carveout.

from __future__ import annotations

import pytest

from ap.underlying_quote_hydration import (
    HYDRATION_MARKER,
    HYDRATION_SOURCE,
    build_broker_resolver,
    clear_quote_resolver,
    register_quote_resolver,
    resolver_registered,
    try_hydrate_underlying,
)
from ap.entry_metadata_guard import validate_entry_metadata, ZERO_UNDERLYING


@pytest.fixture(autouse=True)
def _clean_resolver():
    clear_quote_resolver()
    yield
    clear_quote_resolver()


def _intraday_signal(**overrides) -> dict:
    """Replica of a real failed_dir_intraday_2tf payload (GOOGL 2026-07-03,
    signal 0f3542c0) — carries entry/stop/target but NO underlying key."""
    sig = {
        "signal_id": "0f3542c0-244f-40c1-90af-bec8391e9bfa",
        "client_id": "tradefluencehq@gmail.com",
        "ticker": "GOOGL",
        "symbol": "GOOGL",
        "side": "PUT",
        "direction": "PUT",
        "pattern": "FAILED_DIR_2U_30min+60min",
        "timeframe": "60m",
        "score": 78,
        "entry_trigger": 360.61,
        "entry_price": 360.61,
        "stop_price": 358.83,
        "target_price": 362.50,
        "execution_mode": "paper",
    }
    sig.update(overrides)
    return sig


def _validate(sig: dict):
    return validate_entry_metadata(
        plan=sig, client_id=sig.get("client_id"), execution_mode="paper"
    )


# ── T1: no resolver → unchanged fail-closed ─────────────────────────────────

def test_t1_no_resolver_registered_fails_closed():
    sig = _intraday_signal()
    assert not resolver_registered()
    assert try_hydrate_underlying(sig, ticker="GOOGL") is None
    res = _validate(sig)
    assert not res.ok
    assert res.reason == ZERO_UNDERLYING
    assert HYDRATION_MARKER not in sig


# ── T2: happy path — hydrate, stamp, revalidate ─────────────────────────────

def test_t2_positive_quote_hydrates_and_validation_passes():
    register_quote_resolver(lambda s: 361.42, name="unit_test_resolver")
    sig = _intraday_signal()
    price = try_hydrate_underlying(sig, ticker="GOOGL")
    assert price == pytest.approx(361.42)
    assert sig["underlying_at_signal"] == pytest.approx(361.42)
    assert sig["underlying_price"] == pytest.approx(361.42)
    assert sig[HYDRATION_MARKER] is True
    assert sig[HYDRATION_SOURCE] == "unit_test_resolver"
    res = _validate(sig)
    assert res.ok, f"expected pass after hydration, got {res.reason}"


def test_t2b_nested_containers_stamped():
    register_quote_resolver(lambda s: 100.0, name="unit_test_resolver")
    sig = _intraday_signal(metadata={}, payload={"existing": 1})
    try_hydrate_underlying(sig, ticker="GOOGL")
    assert sig["metadata"]["underlying_at_signal"] == pytest.approx(100.0)
    assert sig["payload"]["underlying_price"] == pytest.approx(100.0)
    assert sig["payload"]["existing"] == 1  # untouched


# ── T3: bad resolver values → fail closed ───────────────────────────────────

@pytest.mark.parametrize("bad", [None, 0, 0.0, -5.25, "not_a_number", ""])
def test_t3_non_positive_or_garbage_quote_fails_closed(bad):
    register_quote_resolver(lambda s: bad, name="unit_test_resolver")
    sig = _intraday_signal()
    assert try_hydrate_underlying(sig, ticker="GOOGL") is None
    assert HYDRATION_MARKER not in sig
    res = _validate(sig)
    assert not res.ok
    assert res.reason == ZERO_UNDERLYING


# ── T4: resolver raises → swallowed, fail closed ────────────────────────────

def test_t4_resolver_exception_fails_closed():
    def _boom(_s):
        raise RuntimeError("tradier 502")
    register_quote_resolver(_boom, name="unit_test_resolver")
    sig = _intraday_signal()
    assert try_hydrate_underlying(sig, ticker="GOOGL") is None
    res = _validate(sig)
    assert not res.ok


# ── T5: never overwrite existing positive underlying ────────────────────────

def test_t5_existing_positive_underlying_never_overwritten():
    register_quote_resolver(lambda s: 999.99, name="unit_test_resolver")
    sig = _intraday_signal(underlying_price=360.00)
    assert try_hydrate_underlying(sig, ticker="GOOGL") is None
    assert sig["underlying_price"] == pytest.approx(360.00)
    assert HYDRATION_MARKER not in sig


# ── T6: hydration alone never passes a signal ───────────────────────────────

def test_t6_hydrated_but_other_field_invalid_still_rejected():
    # Stop/target are only REQUIRED for daily timeframes (guard line ~187),
    # so this invariant must be exercised on a 1d signal, not intraday.
    register_quote_resolver(lambda s: 361.42, name="unit_test_resolver")
    sig = _intraday_signal(timeframe="1d")
    sig.pop("stop_price")           # remove stop → missing_stop expected
    try_hydrate_underlying(sig, ticker="GOOGL")
    res = _validate(sig)
    assert not res.ok
    assert res.reason != ZERO_UNDERLYING  # new reason surfaced, not stale one
    assert res.reason == "metadata_invalid:missing_stop"


# ── T7: broker resolver price preference + holiday zero-quotes ──────────────

class _FakeBroker:
    def __init__(self, quote):
        self._q = quote
    def get_quote(self, symbol):
        return self._q


@pytest.mark.parametrize("quote,expected", [
    ({"last": 361.42, "bid": 361.40, "ask": 361.44}, 361.42),          # last wins
    ({"last": None, "bid": 361.40, "ask": 361.44}, 361.42),            # midpoint
    ({"last": 0, "bid": 0, "ask": 0, "close": 360.61}, 360.61),        # close fallback
    ({"last": 0, "bid": 0, "ask": 0, "close": 0}, None),               # holiday zeros → absent
    ({}, None),                                                         # QUOTE_EMPTY path
    ({"last": None, "bid": 361.50, "ask": 361.40}, None),              # inverted → reject
])
def test_t7_broker_resolver_price_preference(quote, expected):
    resolver = build_broker_resolver(_FakeBroker(quote))
    got = resolver("GOOGL")
    if expected is None:
        assert got is None
    else:
        assert got == pytest.approx(expected)


# ── T8: missing/blank ticker → no-op ────────────────────────────────────────

@pytest.mark.parametrize("ticker", ["", "   ", None])
def test_t8_blank_ticker_noop(ticker):
    register_quote_resolver(lambda s: 100.0, name="unit_test_resolver")
    sig = _intraday_signal()
    assert try_hydrate_underlying(sig, ticker=ticker) is None
    assert HYDRATION_MARKER not in sig
