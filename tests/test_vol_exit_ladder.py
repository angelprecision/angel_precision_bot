from __future__ import annotations

import os
import sys
import types
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_vol_exit_ladder",
)
os.environ.setdefault("ENCRYPTION_KEY", "vol-exit-ladder-test-key")

if "supabase" not in sys.modules:
    _supa_stub = types.ModuleType("supabase")
    _supa_stub.create_client = lambda *a, **kw: None
    _supa_stub.Client = type("Client", (), {})
    sys.modules["supabase"] = _supa_stub

import ap_exit_engine as mod  # noqa: E402
from ap.expected_move import build_vol_exit_snapshot  # noqa: E402


def _pos(**overrides) -> mod.ManagedPosition:
    defaults = dict(
        ticker="SPY",
        option_symbol="SPY260530C00500000",
        side="CALL",
        quantity=3,
        entry_price=1.00,
        underlying_entry=500.00,
        underlying_target=505.00,
        underlying_stop=499.00,
        position_id="pos-vol-1",
        client_id="paper@example.com",
        signal_id="sig-vol-1",
        execution_mode="paper",
        quantity_remaining=3,
        current_option_price=1.00,
        current_underlying=500.00,
        opened_at=datetime.now(timezone.utc),
        entry_atm_iv=0.40,
        expected_move_1d_pct_underlying=0.025,
        expected_option_daily_range_pct=0.40,
    )
    defaults.update(overrides)
    return mod.ManagedPosition(**defaults)


@pytest.fixture(autouse=True)
def _reset_env(monkeypatch):
    monkeypatch.delenv("EXIT_LADDER_MODE", raising=False)
    monkeypatch.delenv("VOL_EXIT_ENABLED", raising=False)
    monkeypatch.delenv("VOL_EXIT_PAPER_ONLY", raising=False)
    monkeypatch.delenv("VOL_EXIT_LIVE_ENABLED", raising=False)
    monkeypatch.delenv("VOL_EXIT_TP_K", raising=False)
    monkeypatch.delenv("VOL_EXIT_W1_K", raising=False)
    monkeypatch.delenv("VOL_EXIT_W2_K", raising=False)
    monkeypatch.delenv("VOL_EXIT_W3_K", raising=False)
    monkeypatch.delenv("VOL_EXIT_HARD_STOP_K", raising=False)
    monkeypatch.delenv("VOL_EXIT_THETA_STOP_K", raising=False)


def test_default_legacy_produces_identical_thresholds():
    pos = _pos()
    ladder = mod._resolve_exit_ladder(pos)
    hard_stop, immediate_tp, profit_lock = mod._effective_thresholds(pos)

    assert ladder["ladder_mode"] == "legacy"
    assert ladder["hard_stop"] == hard_stop
    assert ladder["immediate_tp"] == immediate_tp
    assert ladder["profit_lock"] == profit_lock
    assert ladder["scale_out_1"] == mod.SCALE_OUT_1_THRESHOLD
    assert ladder["scale_out_2"] == mod.SCALE_OUT_2_THRESHOLD
    assert ladder["protect_3"] == mod.PROTECT_3_THRESHOLD
    assert ladder["theta_stop"] == mod.THETA_STOP_LOSS_PCT


def test_paper_enabled_uses_vol_scaled(monkeypatch):
    monkeypatch.setenv("EXIT_LADDER_MODE", "vol_scaled")
    monkeypatch.setenv("VOL_EXIT_ENABLED", "true")
    monkeypatch.setenv("VOL_EXIT_PAPER_ONLY", "true")

    pos = _pos(expected_option_daily_range_pct=0.40, entry_atm_iv=0.40, execution_mode="paper")
    ladder = mod._resolve_exit_ladder(pos)

    assert ladder["ladder_mode"] == "vol_scaled"
    assert ladder["immediate_tp"] == pytest.approx(0.20)
    assert ladder["scale_out_1"] == pytest.approx(0.40)
    assert ladder["scale_out_2"] == pytest.approx(0.24)
    assert ladder["protect_3"] == pytest.approx(0.16)
    assert ladder["hard_stop"] == pytest.approx(-0.30)
    assert ladder["theta_stop"] == pytest.approx(-0.34)


def test_live_paper_only_stays_legacy(monkeypatch):
    monkeypatch.setenv("EXIT_LADDER_MODE", "vol_scaled")
    monkeypatch.setenv("VOL_EXIT_ENABLED", "true")
    monkeypatch.setenv("VOL_EXIT_PAPER_ONLY", "true")
    monkeypatch.setenv("VOL_EXIT_LIVE_ENABLED", "false")

    pos = _pos(execution_mode="live")
    ladder = mod._resolve_exit_ladder(pos)

    assert ladder["ladder_mode"] == "legacy"
    assert ladder["fallback_reason"] in {"live_disabled", "paper_only_live_disabled"}


def test_missing_iv_falls_back_to_legacy(monkeypatch):
    monkeypatch.setenv("EXIT_LADDER_MODE", "vol_scaled")
    monkeypatch.setenv("VOL_EXIT_ENABLED", "true")

    pos = _pos(entry_atm_iv=None, expected_option_daily_range_pct=0.50)
    ladder = mod._resolve_exit_ladder(pos)

    assert ladder["ladder_mode"] == "legacy"
    assert ladder["fallback_reason"] == "missing_iv"


def test_high_iv_creates_wider_bands(monkeypatch):
    monkeypatch.setenv("EXIT_LADDER_MODE", "vol_scaled")
    monkeypatch.setenv("VOL_EXIT_ENABLED", "true")

    high = build_vol_exit_snapshot(
        entry_atm_iv=1.00,
        abs_delta=0.50,
        underlying_price=500.0,
        option_premium_per_share=2.0,
    )
    low = build_vol_exit_snapshot(
        entry_atm_iv=0.20,
        abs_delta=0.50,
        underlying_price=500.0,
        option_premium_per_share=2.0,
    )

    high_ladder = mod._resolve_exit_ladder(_pos(
        entry_atm_iv=high["entry_atm_iv"],
        expected_move_1d_pct_underlying=high["expected_move_1d_pct_underlying"],
        expected_option_daily_range_pct=high["expected_option_daily_range_pct"],
    ))
    low_ladder = mod._resolve_exit_ladder(_pos(
        entry_atm_iv=low["entry_atm_iv"],
        expected_move_1d_pct_underlying=low["expected_move_1d_pct_underlying"],
        expected_option_daily_range_pct=low["expected_option_daily_range_pct"],
    ))

    assert high_ladder["scale_out_1"] > low_ladder["scale_out_1"]
    assert high_ladder["hard_stop"] < low_ladder["hard_stop"]


def test_low_iv_creates_tighter_bands(monkeypatch):
    monkeypatch.setenv("EXIT_LADDER_MODE", "vol_scaled")
    monkeypatch.setenv("VOL_EXIT_ENABLED", "true")

    lower = build_vol_exit_snapshot(
        entry_atm_iv=0.18,
        abs_delta=0.40,
        underlying_price=500.0,
        option_premium_per_share=3.0,
    )
    higher = build_vol_exit_snapshot(
        entry_atm_iv=0.60,
        abs_delta=0.40,
        underlying_price=500.0,
        option_premium_per_share=3.0,
    )

    lower_ladder = mod._resolve_exit_ladder(_pos(
        entry_atm_iv=lower["entry_atm_iv"],
        expected_move_1d_pct_underlying=lower["expected_move_1d_pct_underlying"],
        expected_option_daily_range_pct=lower["expected_option_daily_range_pct"],
    ))
    higher_ladder = mod._resolve_exit_ladder(_pos(
        entry_atm_iv=higher["entry_atm_iv"],
        expected_move_1d_pct_underlying=higher["expected_move_1d_pct_underlying"],
        expected_option_daily_range_pct=higher["expected_option_daily_range_pct"],
    ))

    assert lower_ladder["immediate_tp"] < higher_ladder["immediate_tp"]
    assert lower_ladder["protect_3"] < higher_ladder["protect_3"]


def test_exit_decision_stamp_includes_ladder_info(monkeypatch):
    monkeypatch.setenv("EXIT_LADDER_MODE", "vol_scaled")
    monkeypatch.setenv("VOL_EXIT_ENABLED", "true")

    events = []
    monkeypatch.setattr(mod, "emit_decision_event", lambda **kw: events.append(kw))

    engine = mod.APExitEngine(broker=MagicMock(), data_broker=MagicMock())
    pos = _pos(expected_option_daily_range_pct=0.40, entry_atm_iv=0.40)
    engine._emit_exit_event(
        pos,
        decision="HOLD",
        reason_code="TEST_EVENT",
        explanation="test ladder stamp",
    )

    assert events, "expected an exit decision event"
    ladder = events[0]["context"]["exit_ladder"]
    assert ladder["ladder_mode"] == "vol_scaled"
    assert ladder["k_values"]["tp_k"] == pytest.approx(0.5)
    assert ladder["expected_option_daily_range_pct"] == pytest.approx(0.40)
    assert ladder["fallback_reason"] == ""
