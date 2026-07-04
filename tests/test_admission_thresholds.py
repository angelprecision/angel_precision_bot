from __future__ import annotations

import importlib
import logging
import os
import sys
import types
from pathlib import Path

import pytest

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_admission_thresholds",
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _reload_thresholds_module():
    import ap.admission_thresholds as mod

    return importlib.reload(mod)


def _reload_interrogation_module():
    import trade_interrogation_engine as mod

    return importlib.reload(mod)


def test_env_precedence_preserved(monkeypatch):
    monkeypatch.setenv("GATE_G_SCANNER_MIN_ELIGIBLE", "71")
    monkeypatch.setenv("INTERROGATION_MIN_SCORE", "63")
    monkeypatch.setenv("SCORE_FLOOR", "68")
    monkeypatch.setenv("CONTEXT_FLOOR", "4")
    mod = _reload_thresholds_module()
    resolved = mod.resolve_admission_thresholds()
    assert resolved.thresholds.scanner_floor == 71.0
    assert resolved.thresholds.interrogation_floor == 63.0
    assert resolved.thresholds.mc_score_floor == 68.0
    assert resolved.thresholds.context_floor == 4.0
    assert resolved.sources["scanner_floor"] == "env:GATE_G_SCANNER_MIN_ELIGIBLE"
    assert resolved.sources["interrogation_floor"] == "env:INTERROGATION_MIN_SCORE"
    assert resolved.sources["mc_score_floor"] == "env:SCORE_FLOOR"
    assert resolved.sources["context_floor"] == "env:CONTEXT_FLOOR"


def test_config_hash_changes_only_when_threshold_changes(monkeypatch):
    monkeypatch.delenv("SCORE_FLOOR", raising=False)
    monkeypatch.delenv("CONTEXT_FLOOR", raising=False)
    mod = _reload_thresholds_module()
    first = mod.resolve_admission_thresholds(mc_score_floor=65.0, context_floor=0.0)

    monkeypatch.setenv("SCORE_FLOOR", "65")
    monkeypatch.setenv("CONTEXT_FLOOR", "0")
    mod = _reload_thresholds_module()
    same_values = mod.resolve_admission_thresholds(mc_score_floor=65.0, context_floor=0.0)
    assert first.config_hash == same_values.config_hash

    monkeypatch.setenv("SCORE_FLOOR", "66")
    mod = _reload_thresholds_module()
    changed = mod.resolve_admission_thresholds(mc_score_floor=66.0, context_floor=0.0)
    assert changed.config_hash != first.config_hash


def test_missing_interrogation_packet_becomes_unavailable_not_zero(monkeypatch):
    monkeypatch.delenv("INTERROGATION_MIN_SCORE", raising=False)
    mod = _reload_thresholds_module()
    resolved = mod.resolve_admission_thresholds()
    trace = mod.build_interrogation_packet_threshold_trace(None, resolved=resolved)
    assert trace["score_value"] is None
    assert trace["floor_value"] == 62.0
    assert trace["passed"] is None
    assert trace["source"] == "unavailable:interrogation_packet_missing"


def test_interrogation_startup_log_contains_resolved_thresholds(monkeypatch, caplog):
    monkeypatch.setenv("INTERROGATION_MIN_SCORE", "62")
    mod = _reload_interrogation_module()
    with caplog.at_level(logging.INFO, logger="ap.interrogation"):
        mod.APTradeInterrogationEngine()
    combined = " ".join(caplog.messages)
    assert "Admission thresholds" in combined
    assert "threshold_config_hash=" in combined
    assert "interrogation_floor=62.0" in combined


def test_master_control_startup_log_contains_resolved_thresholds(caplog):
    if "supabase" not in sys.modules:
        supa = types.ModuleType("supabase")
        supa.create_client = lambda *a, **kw: None
        supa.Client = type("Client", (), {})
        sys.modules["supabase"] = supa
    class _FakeConn:
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False
        def execute(self, *args, **kwargs):
            return self
        def fetchall(self):
            return []

    apdb = types.ModuleType("ap.db")
    apdb.conn = lambda *a, **kw: _FakeConn()
    apdb.run_with_retry = lambda fn, *a, **kw: fn()
    sys.modules["ap.db"] = apdb
    import ap_master_control as mc_mod
    mc_mod = importlib.reload(mc_mod)

    with caplog.at_level(logging.INFO, logger="ap.master_control"):
        mc_mod.APMasterControl(
            mode="paper",
            position_manager=types.SimpleNamespace(
                snapshot=lambda: {
                    "capital_deployed": 0.0,
                    "open_positions": [],
                    "closing_positions": [],
                    "realized_pnl_today": 0.0,
                    "pending_entries": [],
                    "pending_entry_capital": 0.0,
                    "_snapshot_ok": True,
                },
                has_pending_entry=lambda *_a, **_kw: False,
            ),
        )
    combined = " ".join(caplog.messages)
    assert "Admission thresholds" in combined
    assert "mc_score_floor" in combined
    assert "context_floor" in combined


def test_interrogation_behavior_identical_fixture_watch_vs_execute(monkeypatch):
    mod = _reload_interrogation_module()

    monkeypatch.setattr(mod, "_gate_entry_permission", lambda signal, state: (True, [], []))
    monkeypatch.setattr(mod, "_gate_regime", lambda signal, spy_trend, vix: (True, [], []))
    monkeypatch.setattr(mod, "_gate_technical", lambda signal: (True, [], [], 70.0))
    monkeypatch.setattr(mod, "_gate_liquidity", lambda signal: (True, [], [], None))
    monkeypatch.setattr(mod, "_gate_risk", lambda signal: (True, [], [], None))
    monkeypatch.setattr(mod, "_gate_event_risk", lambda signal, broker: (True, [], [], None))
    monkeypatch.setattr(mod, "_gate_portfolio", lambda signal, pm: (True, [], []))
    monkeypatch.setattr(mod, "_gate_strategy_fit", lambda signal: (True, [], [], 10.0))

    engine = mod.APTradeInterrogationEngine()
    base_signal = {
        "ticker": "AAPL",
        "pattern": "test",
        "source": "scanner",
        "side": "CALL",
        "timeframe": "1d",
        "score": 70.0,
    }

    monkeypatch.setattr(mod, "_compute_quality", lambda signal, tech_score, fit_score, rr: 61.0)
    watch_packet = engine.evaluate(dict(base_signal))
    assert watch_packet.status == mod.DecisionStatus.WATCH

    monkeypatch.setattr(mod, "_compute_quality", lambda signal, tech_score, fit_score, rr: 62.0)
    execute_packet = engine.evaluate(dict(base_signal))
    assert execute_packet.status == mod.DecisionStatus.EXECUTE
    assert execute_packet.strat_agent_output["threshold_trace"]["interrogation_floor"]["floor_value"] == 62.0


def test_docs_file_exists_and_describes_full_funnel():
    doc = REPO_ROOT / "docs" / "ADMISSION_THRESHOLDS.md"
    assert doc.exists()
    text = doc.read_text()
    assert "scanner_floor" in text
    assert "interrogation_floor" in text
    assert "mc_score_floor" in text
    assert "mc_priority_floor" in text
    assert "context_floor" in text
