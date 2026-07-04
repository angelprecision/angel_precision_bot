from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


_MODULE_PATH = Path(__file__).resolve().parents[1] / "ap" / "expected_move.py"
_SPEC = importlib.util.spec_from_file_location("ap_expected_move_test", _MODULE_PATH)
assert _SPEC and _SPEC.loader
_MOD = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MOD
_SPEC.loader.exec_module(_MOD)

atm_iv_from_chain = _MOD.atm_iv_from_chain
expected_move_1d = _MOD.expected_move_1d
feasibility_ratio = _MOD.feasibility_ratio


def test_expected_move_and_ratio_compute_correctly():
    chain = [
        {"option_type": "call", "strike": 100, "bid": 2.0, "ask": 2.2, "iv": 0.30},
        {"option_type": "put", "strike": 100, "bid": 2.1, "ask": 2.3, "iv": 0.20},
    ]
    atm = atm_iv_from_chain(chain, 100.0)
    assert atm.quality == "ok"
    assert round(atm.value, 4) == 0.25

    move = expected_move_1d(100.0, atm.value)
    assert round(move.value, 6) == round(100.0 * 0.25 * (1 / 252) ** 0.5, 6)

    ratio = feasibility_ratio(100.0, 102.0, move.value)
    assert round(ratio.value, 6) == round(2.0 / move.value, 6)


def test_missing_iv_returns_non_blocking_quality_signal():
    chain = [
        {"option_type": "call", "strike": 100, "bid": 2.0, "ask": 2.2},
        {"option_type": "put", "strike": 100, "bid": 2.1, "ask": 2.3},
    ]
    atm = atm_iv_from_chain(chain, 100.0)
    assert atm.value is None
    assert atm.quality == "unavailable"
    assert atm.reason == "missing_iv"


def test_single_leg_iv_is_supported():
    chain = [
        {"option_type": "call", "strike": 100, "bid": 2.0, "ask": 2.2, "iv": 0.32},
        {"option_type": "put", "strike": 100, "bid": 2.1, "ask": 2.3},
    ]
    atm = atm_iv_from_chain(chain, 100.0)
    assert atm.quality == "single_leg"
    assert atm.value == 0.32


def test_stale_iv_is_not_used_when_live_leg_has_no_iv():
    chain = [
        {"option_type": "call", "strike": 100, "bid": 2.0, "ask": 2.2},
        {"option_type": "put", "strike": 100, "iv": 0.28},
    ]
    atm = atm_iv_from_chain(chain, 100.0)
    assert atm.value is None
    assert atm.quality == "stale"
    assert atm.reason == "stale_chain"


def test_live_single_leg_iv_wins_over_stale_other_leg_iv():
    chain = [
        {"option_type": "call", "strike": 100, "bid": 2.0, "ask": 2.2, "iv": 0.31},
        {"option_type": "put", "strike": 100, "iv": 0.28},
    ]
    atm = atm_iv_from_chain(chain, 100.0)
    assert atm.quality == "single_leg"
    assert atm.value == 0.31


def test_stale_chain_is_flagged():
    chain = [
        {"option_type": "call", "strike": 100, "iv": 0.32},
        {"option_type": "put", "strike": 100, "iv": 0.28},
    ]
    atm = atm_iv_from_chain(chain, 100.0)
    assert atm.value is None
    assert atm.quality == "stale"
    assert atm.reason == "stale_chain"
