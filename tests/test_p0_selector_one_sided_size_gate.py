"""P0 (monday-trade-flow-readiness): one-sided-size trap fix in
_pro_contract_quality (ap/contract_selector.py).

Production forensics: Tradier chain rows frequently report book size on only
one side (e.g. bid_size=12, ask_size=0). The previous gate
`if bid_size or ask_size:` rejected such rows as size_too_thin even when the
quote was live and tight. The hard size gate must apply ONLY when BOTH sides
report a size.

Proves:
  1. One-sided reporting (bid_size=12, ask_size=0) with an otherwise-liquid
     tight quote is NOT rejected as size_too_thin (regression the fix targets).
  2. Two-sided thin book (bid_size=1, ask_size=1) STILL hard-rejects — the
     genuine protection is not weakened.
  3. Two-sided healthy book still passes.
  4. No-size rows (both zero) still fall through to vol/OI gates (unchanged).
  5. All other gates (zero bid/ask, bid floor, spread, vol/OI) are untouched.
"""
from __future__ import annotations
import os
os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")


from ap.contract_selector import _pro_contract_quality


def _opt(**overrides):
    """A liquid, tight SPY 0DTE-ish row that passes every gate by default."""
    base = {
        "bid": 1.80,
        "ask": 1.86,
        "volume": 500,
        "open_interest": 5000,
        "bid_size": 25,
        "ask_size": 25,
    }
    base.update(overrides)
    return base


def test_one_sided_size_is_not_rejected_as_thin():
    tier, reason = _pro_contract_quality(_opt(bid_size=12, ask_size=0), "SPY", 0)
    assert tier != "REJECT", f"one-sided size must not reject (got {reason})"
    assert not reason.startswith("size_too_thin")


def test_one_sided_size_other_side():
    tier, reason = _pro_contract_quality(_opt(bid_size=0, ask_size=9), "SPY", 0)
    assert tier != "REJECT", f"one-sided size must not reject (got {reason})"


def test_two_sided_thin_still_rejects():
    tier, reason = _pro_contract_quality(_opt(bid_size=1, ask_size=1), "SPY", 0)
    assert tier == "REJECT"
    assert reason.startswith("size_too_thin")


def test_two_sided_mixed_thin_still_rejects():
    # both sides REPORT, one side below the hard min → genuine thinness
    tier, reason = _pro_contract_quality(_opt(bid_size=25, ask_size=2), "SPY", 0)
    assert tier == "REJECT"
    assert reason.startswith("size_too_thin")


def test_two_sided_healthy_passes():
    tier, reason = _pro_contract_quality(_opt(), "SPY", 0)
    assert tier in ("A", "B")


def test_no_size_reported_falls_through_to_liquidity_gates():
    # zero/absent both sides: size gate skipped, vol/OI gate still governs
    tier, reason = _pro_contract_quality(
        _opt(bid_size=0, ask_size=0, volume=0, open_interest=0), "SPY", 0
    )
    assert tier == "REJECT"
    assert reason.startswith("illiquid_")


def test_other_gates_untouched_zero_bid():
    tier, reason = _pro_contract_quality(_opt(bid=0), "SPY", 0)
    assert (tier, reason[:4]) == ("REJECT", "zero")


def test_other_gates_untouched_spread():
    tier, reason = _pro_contract_quality(_opt(bid=1.00, ask=1.30), "SPY", 0)
    assert tier == "REJECT"
    assert reason.startswith("spread_too_wide")
