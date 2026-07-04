from __future__ import annotations

import importlib
import sys
from unittest.mock import MagicMock

import ap_master_control as mc_mod


def _build_mc():
    pm = MagicMock()
    pm.client_id = "client@example.com"
    pm.has_pending_entry.return_value = False
    pm.snapshot = MagicMock(return_value={
        "open_count": 0,
        "open_tickers": set(),
        "open_position_ids": [],
        "open_positions": [],
        "closing_positions": [],
        "calls_open": 0,
        "puts_open": 0,
        "capital_deployed": 0.0,
        "position_capital_deployed": 0.0,
        "pending_entry_capital": 0.0,
        "filled_unreconciled_entry_capital": 0.0,
        "pending_entries": 0,
        "watcher_count": 0,
        "pending_exits": 0,
        "trades_today": 0,
        "realized_pnl_today": 0.0,
        "total_trades": 0,
        "_snapshot_ok": True,
        "_snapshot_ts": "2026-07-04T14:00:00+00:00",
        "_snapshot_age_sec": 0.1,
        "_snapshot_error": "",
    })
    mc = mc_mod.APMasterControl(
        mode="paper",
        account_equity=25000.0,
        position_manager=pm,
        supabase_client=None,
    )
    mc._kill_switch_fn = lambda: False
    mc._entries_paused_fn = None
    mc._has_durable_duplicate_signal = lambda **kwargs: (False, "ok", "")
    mc._pending_capital_from_snapshot_or_db = lambda *args, **kwargs: 0.0
    mc._equity_snapshot = lambda: (25000.0, -500.0)
    mc._base_contracts = lambda *args, **kwargs: 1
    mc._run_intelligence = lambda signal: {"approved": True, "score": 42.0, "reasoning": "ok", "_available": False}
    mc._run_final_quality_gates = lambda **kwargs: None
    mc._persist_dedup = lambda *args, **kwargs: None
    mc._store_update = lambda *args, **kwargs: None
    mc._sector_capital_deployed = lambda *args, **kwargs: 0.0
    mc._ticker_capital_deployed = lambda *args, **kwargs: 0.0
    mc._daily_loss_check = lambda *args, **kwargs: False
    mc._seed_dedup_from_db = lambda *args, **kwargs: None
    return mc


def _signal(**overrides):
    signal = {
        "signal_id": "sig-feas-1",
        "ticker": "AAPL",
        "symbol": "AAPL",
        "side": "CALL",
        "direction": "CALL",
        "score": 78.0,
        "pattern": "2-3",
        "timeframe": "1d",
        "entry_price": 100.0,
        "target_price": 103.0,
        "current_price": 100.0,
        "metadata": {},
    }
    signal.update(overrides)
    return signal


def test_observation_ratio_above_limit_does_not_block(monkeypatch):
    monkeypatch.setenv("ENABLE_FEASIBILITY_OBSERVE", "true")
    monkeypatch.setenv("ENABLE_FEASIBILITY_ENFORCE", "false")
    monkeypatch.setenv("FEASIBILITY_MAX_RATIO", "1.2")
    mc = _build_mc()
    signal = _signal(option_chain=[
        {"option_type": "call", "strike": 100, "bid": 2.0, "ask": 2.2, "iv": 0.20},
        {"option_type": "put", "strike": 100, "bid": 2.1, "ask": 2.3, "iv": 0.20},
    ])

    obs = mc._build_feasibility_observation(signal)
    assert obs["feasibility_ratio"] is not None
    assert obs["would_block"] is True
    assert obs["source"] == "live_chain"
    assert obs["quality"] == "ok"


def test_missing_iv_proceeds_and_stamps_unavailable(monkeypatch):
    monkeypatch.setenv("ENABLE_FEASIBILITY_OBSERVE", "true")
    monkeypatch.setenv("ENABLE_FEASIBILITY_ENFORCE", "false")
    mc = _build_mc()
    signal = _signal(option_chain=[
        {"option_type": "call", "strike": 100, "bid": 2.0, "ask": 2.2},
        {"option_type": "put", "strike": 100, "bid": 2.1, "ask": 2.3},
    ])

    obs = mc._build_feasibility_observation(signal)
    mc._stamp_feasibility_observation(signal, obs)

    assert obs["status_code"] == "EXPECTED_MOVE_UNAVAILABLE"
    assert obs["would_block"] is False
    assert signal["metadata"]["expected_move_status"] == "EXPECTED_MOVE_UNAVAILABLE"


def test_archiver_snapshot_source_and_single_leg_quality(monkeypatch):
    monkeypatch.setenv("ENABLE_FEASIBILITY_OBSERVE", "true")
    monkeypatch.setenv("ENABLE_FEASIBILITY_ENFORCE", "false")
    mc = _build_mc()
    signal = _signal(metadata={
        "archiver_snapshot": [
            {"option_type": "call", "strike": 100, "bid": 2.0, "ask": 2.2, "iv": 0.25},
            {"option_type": "put", "strike": 100, "bid": 2.1, "ask": 2.3},
        ]
    })

    obs = mc._build_feasibility_observation(signal)

    assert obs["source"] == "archiver_snapshot"
    assert obs["quality"] == "single_leg"
    assert obs["expected_move"] is not None


def test_stale_only_iv_does_not_produce_ratio_and_never_blocks(monkeypatch):
    monkeypatch.setenv("ENABLE_FEASIBILITY_OBSERVE", "true")
    monkeypatch.setenv("ENABLE_FEASIBILITY_ENFORCE", "false")
    mc = _build_mc()
    signal = _signal(option_chain=[
        {"option_type": "call", "strike": 100, "bid": 2.0, "ask": 2.2},
        {"option_type": "put", "strike": 100, "iv": 0.25},
    ])

    obs = mc._build_feasibility_observation(signal)
    mc._stamp_feasibility_observation(signal, obs)

    assert obs["quality"] == "stale"
    assert obs["expected_move"] is None
    assert obs["feasibility_ratio"] is None
    assert obs["status_code"] == "EXPECTED_MOVE_UNAVAILABLE"
    assert obs["would_block"] is False
    assert signal["metadata"]["expected_move_status"] == "EXPECTED_MOVE_UNAVAILABLE"


def test_approval_event_and_plan_metadata_receive_fields(monkeypatch):
    monkeypatch.setenv("ENABLE_FEASIBILITY_OBSERVE", "true")
    monkeypatch.setenv("ENABLE_FEASIBILITY_ENFORCE", "false")
    events = []
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))

    mc = _build_mc()
    signal = _signal(option_chain=[
        {"option_type": "call", "strike": 100, "bid": 2.0, "ask": 2.2, "iv": 0.30},
        {"option_type": "put", "strike": 100, "bid": 2.1, "ask": 2.3, "iv": 0.30},
    ])

    decision = mc.evaluate(signal, client_id="client@example.com")

    assert decision.ok is True
    meta = decision.plan.metadata["expected_move_feasibility"]
    assert meta["source"] == "live_chain"
    assert decision.plan.metadata["score_audit"]["expected_move_feasibility"]["source"] == "live_chain"
    assert events[0]["inputs"]["feasibility_ratio"] == meta["feasibility_ratio"]
    assert events[0]["context"]["expected_move_feasibility"]["source"] == "live_chain"


def test_direct_import_ap_master_control_leaves_metadata_guard_installed(monkeypatch):
    monkeypatch.delenv("ENABLE_FEASIBILITY_OBSERVE", raising=False)
    for name in (
        "ap_master_control",
        "ap",
        "ap.master_control_metadata_guard",
        "ap.entry_metadata_guard",
        "ap.expected_move",
    ):
        sys.modules.pop(name, None)

    fresh = importlib.import_module("ap_master_control")

    assert getattr(fresh.APMasterControl, "_entry_metadata_guard_installed", False) is True
