from __future__ import annotations

import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")
sys.modules.setdefault("psycopg2", MagicMock())
sys.modules.setdefault("psycopg2.extras", MagicMock())
sys.modules.setdefault("psycopg2.pool", MagicMock())

import ap.contract_selector as cs


class _Broker:
    def __init__(self, base_url: str = "https://sandbox.tradier.com"):
        self.cfg = SimpleNamespace(base_url=base_url, access_token="tok")

    def get_quote(self, symbol: str):
        return {}


def _plan():
    return SimpleNamespace(
        ticker="HD",
        side="CALL",
        max_position_usd=1500.0,
        score=72.0,
        tier="B",
        signal_id="sig-hd-1",
        client_id="tradefluencehq@gmail.com",
        pattern="daily_breakout",
        timeframe="1d",
        metadata={},
    )


def _engine():
    eng = cs.APContractSelectionEngine(_Broker(), mode="paper")
    eng._emit_selector_event = lambda *args, **kwargs: None
    return eng


def test_non_empty_zero_bid_ask_chain_returns_quote_zero_not_no_chain(monkeypatch):
    eng = _engine()
    plan = _plan()
    monkeypatch.setattr(
        eng,
        "_fetch_chain_with_price",
        lambda ticker, direction: ([{
            "symbol": "HD260626C00340000",
            "bid": 0.0,
            "ask": 0.0,
            "open_interest": 40,
            "volume": 25,
            "option_type": "call",
            "expiration_date": "2026-06-26",
            "_ticker": "HD",
        }], 340.0),
    )
    monkeypatch.setattr(cs, "_should_revalidate", lambda reason: False)

    assert eng.select(plan) is None
    failure = plan.metadata["selector_failure"]
    assert failure["reason_code"] == "CHAIN_ROW_ZERO_BID_ASK"
    assert failure["queue_reason_code"] == "QUOTE_ZERO_BID_ASK"
    assert failure["chain_rows"] == 1
    assert failure["top_reject_buckets"]["CHAIN_ROW_ZERO_BID_ASK"] == 1


def test_empty_chain_maps_to_no_chain_data(monkeypatch):
    eng = _engine()
    plan = _plan()
    monkeypatch.setattr(eng, "_fetch_chain_with_price", lambda ticker, direction: ([], 340.0))

    assert eng.select(plan) is None
    failure = plan.metadata["selector_failure"]
    assert failure["reason_code"] == "CHAIN_EMPTY"
    assert failure["queue_reason_code"] == "NO_CHAIN_DATA"


def test_direct_quote_zero_bid_ask_is_honest(monkeypatch):
    eng = _engine()
    plan = _plan()
    monkeypatch.setattr(
        eng,
        "_fetch_chain_with_price",
        lambda ticker, direction: ([{
            "symbol": "MDT260626C00090000",
            "bid": 0.0,
            "ask": 0.0,
            "open_interest": 200,
            "volume": 200,
            "option_type": "call",
            "expiration_date": "2026-06-26",
            "_ticker": "MDT",
        }], 90.0),
    )
    monkeypatch.setattr(cs, "_should_revalidate", lambda reason: True)
    monkeypatch.setattr(
        cs,
        "_revalidate_direct",
        lambda broker, opt, result: {
            "action": "REJECT_DIRECT_ZERO",
            "reason_code": "DIRECT_QUOTE_ZERO_BID_ASK",
            "audit": {"direct_bid": 0.0, "direct_ask": 0.0},
        },
    )

    assert eng.select(plan) is None
    failure = plan.metadata["selector_failure"]
    assert failure["reason_code"] == "DIRECT_QUOTE_ZERO_BID_ASK"
    assert failure["queue_reason_code"] == "QUOTE_ZERO_BID_ASK"


def test_oi_rejection_stays_oi_too_low(monkeypatch):
    eng = _engine()
    plan = _plan()
    monkeypatch.setattr(
        eng,
        "_fetch_chain_with_price",
        lambda ticker, direction: ([{
            "symbol": "USB260626C00045000",
            "bid": 1.2,
            "ask": 1.3,
            "open_interest": 0,
            "volume": 10,
            "option_type": "call",
            "expiration_date": "2026-06-26",
            "_ticker": "USB",
            "_underlying_price": 45.0,
            "greeks": {"delta": 0.4},
            "strike": 45.0,
        }], 45.0),
    )
    monkeypatch.setattr(cs, "_should_revalidate", lambda reason: False)

    assert eng.select(plan) is None
    failure = plan.metadata["selector_failure"]
    assert failure["reason_code"] == "OI_TOO_LOW"
    assert failure["queue_reason_code"] == "OI_TOO_LOW"


def test_spread_rejection_stays_spread_too_wide(monkeypatch):
    eng = _engine()
    plan = _plan()
    monkeypatch.setattr(
        eng,
        "_fetch_chain_with_price",
        lambda ticker, direction: ([{
            "symbol": "LOW260626C00230000",
            "bid": 1.0,
            "ask": 3.0,
            "open_interest": 500,
            "volume": 500,
            "option_type": "call",
            "expiration_date": "2026-06-26",
            "_ticker": "LOW",
            "_underlying_price": 230.0,
            "greeks": {"delta": 0.4},
            "strike": 230.0,
        }], 230.0),
    )
    monkeypatch.setattr(cs, "_should_revalidate", lambda reason: False)

    assert eng.select(plan) is None
    failure = plan.metadata["selector_failure"]
    assert failure["reason_code"] == "SPREAD_TOO_WIDE"
    assert failure["queue_reason_code"] == "SPREAD_TOO_WIDE"


def test_failure_audit_includes_source_mode_and_base_url(monkeypatch):
    eng = _engine()
    plan = _plan()
    monkeypatch.setattr(eng, "_fetch_chain_with_price", lambda ticker, direction: ([], 340.0))

    eng.select(plan)
    failure = plan.metadata["selector_failure"]
    assert failure["quote_source"] == "tradier_sandbox"
    assert failure["chain_source"] == "tradier_options_chain"
    assert failure["tradier_base_url"] == "https://sandbox.tradier.com"
    assert failure["sandbox_mode"] is True
    assert failure["execution_mode"] == "paper"


def test_quality_thresholds_unchanged():
    assert cs._PRO_MIN_BID == 0.10
    assert cs._PRO_T1_SPREAD_HARD_MAX == 0.10
    assert cs._PRO_T2_SPREAD_HARD_MAX == 0.12
