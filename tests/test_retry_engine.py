"""
Tests for ap/retry_engine.py — the alignment-aware re-peg decision.

Critical user-stated invariant: every trade that canceled today (2026-05-19)
would have been a loss if filled later. The retry engine MUST refuse to re-peg
when the underlying alignment has broken, regardless of how attractive the
option price gap looks.

Run:
    pytest tests/test_retry_engine.py -xvs
"""
from __future__ import annotations

import time
import importlib
import os

import pytest


@pytest.fixture
def re_engine():
    """Reload the module so env-var overrides take effect per-test."""
    import ap.retry_engine as r
    importlib.reload(r)
    return r


def _order(**kwargs):
    """Build a minimal order_row dict with the defaults the engine reads."""
    base = {
        "id": "local-test-1",
        "broker_order_id": "broker-test-1",
        "limit_price": 2.17,
        "direction": "CALL",
        "signal_entry_price": 427.50,   # MSFT example
        "repeg_attempts": 0,
        "last_repeg_ts": 0,
        "meta": {},
    }
    base.update(kwargs)
    return base


# ─── Gate 1: TIME ────────────────────────────────────────────────────────────

def test_max_attempts_blocks_repeg(re_engine):
    d = re_engine.decide_repeg(
        order_row=_order(repeg_attempts=2),  # default REPEG_MAX_ATTEMPTS=2
        current_option_price=2.30,
        underlying_spot=428.00,
    )
    assert d.ok is False
    assert d.reason == "max_attempts_reached"


def test_too_soon_since_last_repeg(re_engine):
    d = re_engine.decide_repeg(
        order_row=_order(last_repeg_ts=time.time() - 5),  # only 5s ago
        current_option_price=2.30,
        underlying_spot=428.00,
    )
    assert d.ok is False
    assert d.reason == "too_soon_since_last_repeg"


def test_repeg_allowed_after_interval(re_engine):
    d = re_engine.decide_repeg(
        order_row=_order(last_repeg_ts=time.time() - 60),  # 60s ago
        current_option_price=2.30,
        underlying_spot=428.00,
    )
    assert d.ok is True


# ─── Gate 2: PROXIMITY ───────────────────────────────────────────────────────

def test_runaway_quote_blocks_repeg(re_engine):
    # Option is 15% above limit -- way past REPEG_PROXIMITY_PCT (default 8%)
    d = re_engine.decide_repeg(
        order_row=_order(limit_price=2.17),
        current_option_price=2.50,  # 15% above
        underlying_spot=428.00,
    )
    assert d.ok is False
    assert d.reason == "runaway_quote"


def test_no_gap_to_close(re_engine):
    # Option price <= limit -- no chase needed
    d = re_engine.decide_repeg(
        order_row=_order(limit_price=2.17),
        current_option_price=2.15,
        underlying_spot=428.00,
    )
    assert d.ok is False
    assert d.reason == "no_gap_to_close"


# ─── Gate 3: ALIGNMENT ───────────────────────────────────────────────────────

def test_call_stale_thesis_blocks_repeg(re_engine):
    """The MSFT case from 2026-05-19: option ran but underlying came back.
    Re-peg must be REJECTED because the thesis is dead."""
    # Signal entry was at 427.50. Underlying has now dropped to 426.00.
    # Default drift tolerance is 0.2% (= floor 426.6450), so 426.00 < floor.
    d = re_engine.decide_repeg(
        order_row=_order(direction="CALL", signal_entry_price=427.50),
        current_option_price=2.30,
        underlying_spot=426.00,
    )
    assert d.ok is False
    assert d.reason == "stale_thesis_call"


def test_call_aligned_allows_repeg(re_engine):
    """Underlying is still above the signal entry -- thesis holds, re-peg OK."""
    d = re_engine.decide_repeg(
        order_row=_order(direction="CALL", signal_entry_price=427.50),
        current_option_price=2.30,
        underlying_spot=428.50,
    )
    assert d.ok is True
    assert d.reason == "aligned_repeg"
    # New limit = 2.17 + 0.50 * (2.30 - 2.17) = 2.17 + 0.065 = 2.235
    # Python's round() uses banker's rounding: 2.235 -> 2.23 (rounds to even).
    # Either 2.23 or 2.24 is acceptable financially — both move the limit halfway.
    assert 2.22 <= d.new_limit_price <= 2.25, f"got {d.new_limit_price}"
    assert d.attempts_used == 1


def test_put_stale_thesis_blocks_repeg(re_engine):
    """For a PUT, underlying must STAY BELOW the signal level."""
    d = re_engine.decide_repeg(
        order_row=_order(direction="PUT", signal_entry_price=200.00),
        current_option_price=2.30,
        underlying_spot=201.00,  # underlying came back UP -> thesis dead
    )
    assert d.ok is False
    assert d.reason == "stale_thesis_put"


def test_put_aligned_allows_repeg(re_engine):
    d = re_engine.decide_repeg(
        order_row=_order(direction="PUT", signal_entry_price=200.00),
        current_option_price=2.30,
        underlying_spot=199.00,
    )
    assert d.ok is True


# ─── Edge cases ──────────────────────────────────────────────────────────────

def test_repeg_disabled_returns_false(re_engine, monkeypatch):
    monkeypatch.setattr(re_engine, "REPEG_ENABLED", False)
    d = re_engine.decide_repeg(
        order_row=_order(),
        current_option_price=2.30,
        underlying_spot=428.00,
    )
    assert d.ok is False
    assert d.reason == "repeg_disabled"


def test_missing_signal_entry_falls_back_to_allow(re_engine):
    """For legacy orders without signal_entry_price recorded, the engine
    should allow re-peg (with a log warning) rather than refusing every
    legacy order. New code should always populate signal_entry_price."""
    d = re_engine.decide_repeg(
        order_row=_order(signal_entry_price=None),
        current_option_price=2.30,
        underlying_spot=428.00,
    )
    assert d.ok is True


def test_zero_or_negative_limit_blocks(re_engine):
    d = re_engine.decide_repeg(
        order_row=_order(limit_price=0),
        current_option_price=2.30,
        underlying_spot=428.00,
    )
    assert d.ok is False
    assert d.reason == "no_limit_price"


def test_zero_or_negative_current_blocks(re_engine):
    d = re_engine.decide_repeg(
        order_row=_order(),
        current_option_price=0,
        underlying_spot=428.00,
    )
    assert d.ok is False
    assert d.reason == "no_current_price"


def test_unknown_direction_blocks(re_engine):
    d = re_engine.decide_repeg(
        order_row=_order(direction="SHORT"),
        current_option_price=2.30,
        underlying_spot=428.00,
    )
    assert d.ok is False
    assert d.reason == "unknown_direction"


def test_meta_signal_entry_price_is_read(re_engine):
    """If order_row has meta={'signal_entry_price': X} instead of top-level,
    the engine must still find it."""
    o = _order(signal_entry_price=None, meta={"signal_entry_price": 427.50})
    d = re_engine.decide_repeg(
        order_row=o,
        current_option_price=2.30,
        underlying_spot=426.00,  # stale
    )
    assert d.ok is False
    assert d.reason == "stale_thesis_call"


# ─── The full MSFT replay from 2026-05-19 ────────────────────────────────────

def test_msft_2026_05_19_replay(re_engine):
    """End-to-end: replay the MSFT scenario the user described.
    Signal said BUY $427.50 CALL @ MSFT=427.50. Bot posted limit @ $2.17.
    Option drifted to $2.32-$2.45 while underlying came back to ~426.
    The engine MUST refuse to re-peg (stale thesis) -> normal cancel runs."""
    msft_order = {
        "id": "msft-replay",
        "broker_order_id": "broker-msft",
        "limit_price": 2.17,
        "direction": "CALL",
        "signal_entry_price": 427.50,
        "repeg_attempts": 0,
        "last_repeg_ts": 0,
        "meta": {"score": 78, "ticker": "MSFT"},
    }
    # 75s in, option @ $2.32, underlying drifted down to 426.00
    d = re_engine.decide_repeg(
        order_row=msft_order,
        current_option_price=2.32,
        underlying_spot=426.00,
    )
    assert d.ok is False
    assert d.reason == "stale_thesis_call", (
        "MSFT 2026-05-19 replay: every trade today that would have filled on "
        "retry was a loss. The engine MUST refuse to re-peg when the underlying "
        f"has unwound from signal entry. Got: ok={d.ok} reason={d.reason}"
    )
