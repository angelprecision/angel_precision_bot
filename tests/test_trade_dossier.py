from __future__ import annotations

import json
import logging
import pathlib
import sys
import time
import types
from datetime import datetime, timezone

from ap.trade_dossier import (
    REQUIRED_COLUMNS,
    build_trade_dossier,
    persist_trade_dossier,
)


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class _FakeConn:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.calls = []
        self.rowcount = 1

    def execute(self, sql, params=None):
        if self.fail:
            raise RuntimeError("db down")
        self.calls.append((sql, params))

    def fetchall(self):
        return [{"column_name": c} for c in REQUIRED_COLUMNS]


def _signal(**overrides):
    base = {
        "signal_id": "sig-1",
        "canonical_signal_id": "canon-1",
        "ticker": "SPY",
        "direction": "CALL",
        "strategy": "daily_breakout",
        "timeframe": "1d",
        "entry": 500.0,
        "stop": 495.0,
        "target": 512.0,
        "daily_trend": "UP",
        "spy_trend": "UP",
        "sector_trend": "UP",
        "score": 82,
    }
    base.update(overrides)
    return base


def test_approved_signal_creates_dossier():
    dossier = build_trade_dossier(
        _signal(),
        client_id="client-a",
        execution_mode="PAPER",
        decision_context={"master_control_decision": "APPROVE", "approved": True},
    )

    assert dossier["decision"] == "APPROVE"
    assert dossier["client_id"] == "client-a"
    assert dossier["execution_mode"] == "PAPER"
    assert dossier["dossier"]["decision_snapshot"]["created_before_execution"] is True


def test_blocked_rejected_and_watching_signals_create_dossiers():
    for decision in ("REJECT", "WATCH", "BLOCK"):
        dossier = build_trade_dossier(
            _signal(signal_id=f"sig-{decision}", canonical_signal_id=f"canon-{decision}"),
            client_id="client-a",
            execution_mode="PAPER",
            decision_context={
                "master_control_decision": decision,
                "decision_reason": f"{decision.lower()}_reason",
                "approved": decision == "WATCH",
            },
        )
        assert dossier["decision"] == decision
        assert dossier["dossier"]["identity"]["canonical_signal_id"] == f"canon-{decision}"


def test_missing_levels_creates_incomplete_not_fake_levels():
    dossier = build_trade_dossier(
        _signal(entry=None, stop=None, target=None),
        client_id="client-a",
        execution_mode="PAPER",
    )

    levels = dossier["dossier"]["levels"]
    assert dossier["dossier_status"] == "INCOMPLETE"
    assert dossier["case_grade"] == "INCOMPLETE"
    assert levels["entry"] is None
    assert levels["stop"] is None
    assert levels["target"] is None
    assert levels["level_quality"] == "UNAVAILABLE"


def test_bad_rr_marks_weak_or_invalid_but_does_not_block():
    dossier = build_trade_dossier(
        _signal(entry=500, stop=499, target=500.5),
        client_id="client-a",
        execution_mode="PAPER",
    )

    assert dossier["dossier"]["levels"]["planned_rr"] == 0.5
    assert dossier["dossier"]["levels"]["level_quality"] == "WEAK"
    assert dossier["decision"] is None


def test_duplicate_canonical_client_mode_uses_same_dossier_id_and_upsert():
    a = build_trade_dossier(_signal(), client_id="client-a", execution_mode="PAPER")
    b = build_trade_dossier(_signal(score=90), client_id="client-a", execution_mode="PAPER")
    conn = _FakeConn()

    assert a["dossier_id"] == b["dossier_id"]
    assert persist_trade_dossier(conn, a) is True
    assert "ON CONFLICT (canonical_signal_id, client_id, execution_mode, schema_version)" in conn.calls[-1][0]


def test_paper_and_live_same_canonical_remain_separated():
    paper = build_trade_dossier(_signal(), client_id="client-a", execution_mode="PAPER")
    live = build_trade_dossier(_signal(), client_id="client-a", execution_mode="LIVE")

    assert paper["canonical_signal_id"] == live["canonical_signal_id"]
    assert paper["dossier_id"] != live["dossier_id"]


def test_writer_exception_returns_false():
    dossier = build_trade_dossier(_signal(), client_id="client-a", execution_mode="PAPER")

    assert persist_trade_dossier(_FakeConn(fail=True), dossier) is False


def test_malformed_signal_never_raises():
    dossier = build_trade_dossier(
        {"signal_id": object(), "trigger": "not-a-dict"},
        client_id="client-a",
        execution_mode="PAPER",
    )

    assert dossier["dossier_status"] in {"INCOMPLETE", "UNAVAILABLE", "READY", "INVALID"}
    assert dossier["client_id"] == "client-a"


def test_human_summary_deterministic():
    a = build_trade_dossier(_signal(), client_id="client-a", execution_mode="PAPER")
    b = build_trade_dossier(_signal(), client_id="client-a", execution_mode="PAPER")

    assert a["dossier"]["review_summary"]["operator_summary"] == b["dossier"]["review_summary"]["operator_summary"]


def test_jsonb_size_cap_enforced(monkeypatch):
    monkeypatch.setattr("ap.trade_dossier.MAX_DOSSIER_JSON_BYTES", 1000)
    dossier = build_trade_dossier(
        _signal(huge="x" * 100000),
        client_id="client-a",
        execution_mode="PAPER",
    )

    encoded = json.dumps(dossier["dossier"], default=str).encode()
    assert len(encoded) < 5000
    assert dossier["dossier"].get("size_cap_applied") is True


def test_client_id_and_execution_mode_preserved_exactly():
    dossier = build_trade_dossier(_signal(), client_id="Jason@Example.COM", execution_mode="paper-custom")

    assert dossier["client_id"] == "Jason@Example.COM"
    assert dossier["execution_mode"] == "paper-custom"


def test_trade_date_defaults_to_america_new_york(monkeypatch):
    class _FakeDateTime:
        @classmethod
        def now(cls, tz=None):
            base = datetime(2026, 7, 4, 1, 30, tzinfo=timezone.utc)
            return base.astimezone(tz) if tz is not None else base

    monkeypatch.setattr("ap.trade_dossier.datetime", _FakeDateTime)
    dossier = build_trade_dossier(
        _signal(trade_date=None, date=None),
        client_id="client-a",
        execution_mode="PAPER",
    )

    assert dossier["trade_date"] == "2026-07-03"
    assert dossier["dossier"]["identity"]["trade_date"] == "2026-07-03"


def test_no_order_queue_position_or_proof_mutation_in_dossier_module():
    src = (REPO_ROOT / "ap" / "trade_dossier.py").read_text()

    assert "UPDATE orders" not in src
    assert "INSERT INTO orders" not in src
    assert "trade_queue" not in src
    assert "positions" not in src
    assert "proof_trades" not in src
    assert "result_json" not in src


def test_master_control_dossier_failure_does_not_alter_decision(monkeypatch):
    db_mod = types.ModuleType("ap.db")
    db_mod.conn = lambda: (_ for _ in ()).throw(RuntimeError("db down"))
    db_mod.run_with_retry = lambda fn, *args, **kwargs: fn()
    monkeypatch.setitem(sys.modules, "ap.db", db_mod)

    from ap_master_control import APMasterControl

    signal = {"signal_id": "bad-1", "ticker": "SPY", "score": 80}
    monkeypatch.setenv("ENABLE_TRADE_DOSSIER", "false")
    before = APMasterControl(mode="paper", client_id="client-a").evaluate(dict(signal), client_id="client-a")

    monkeypatch.setenv("ENABLE_TRADE_DOSSIER", "true")
    after = APMasterControl(mode="paper", client_id="client-a").evaluate(dict(signal), client_id="client-a")

    assert (after.ok, after.stage, after.reason, after.reason_code) == (
        before.ok,
        before.stage,
        before.reason,
        before.reason_code,
    )


def test_block_db_connect_failure_returns_immediately_and_preserves_decision(monkeypatch):
    calls = {"count": 0}

    class _SlowDB:
        @staticmethod
        def conn():
            calls["count"] += 1
            time.sleep(1.0)
            raise RuntimeError("db down")

    db_mod = types.ModuleType("ap.db")
    db_mod.conn = _SlowDB.conn
    db_mod.run_with_retry = lambda fn, *args, **kwargs: fn()
    monkeypatch.setitem(sys.modules, "ap.db", db_mod)
    monkeypatch.setenv("ENABLE_TRADE_DOSSIER", "true")

    from ap_master_control import APMasterControl

    mc = APMasterControl(mode="paper", client_id="client-a")
    mc._cache_trade_dossier_signal("sig-block-fast", {"signal_id": "sig-block-fast", "ticker": "SPY"})
    baseline_calls = calls["count"]
    started = time.perf_counter()
    decision = mc._block(
        "sig-block-fast",
        "SPY",
        "client-a",
        "blocked_system",
        "exit_engine_down__protective_systems_unavailable",
    )
    elapsed = time.perf_counter() - started

    assert decision.ok is False
    assert decision.reason == "exit_engine_down__protective_systems_unavailable"
    assert elapsed < 0.5
    assert calls["count"] == baseline_calls


def test_block_logs_db_unavailable_and_does_not_alter_decision(monkeypatch, caplog):
    monkeypatch.setenv("ENABLE_TRADE_DOSSIER", "true")
    from ap_master_control import APMasterControl

    mc = APMasterControl(mode="paper", client_id="client-a")
    mc._cache_trade_dossier_signal("sig-block-log", {"signal_id": "sig-block-log", "ticker": "SPY"})
    with caplog.at_level(logging.WARNING):
        decision = mc._block(
            "sig-block-log",
            "SPY",
            "client-a",
            "blocked_system",
            "invalid_or_missing_side",
        )

    assert decision.ok is False
    assert decision.stage == "blocked_system"
    assert "trade_dossier_write_skipped_db_unavailable" in caplog.text


def test_blocked_signal_gets_dossier_when_async_writer_succeeds(monkeypatch):
    writes = []
    monkeypatch.setenv("ENABLE_TRADE_DOSSIER", "true")
    from ap_master_control import APMasterControl

    mc = APMasterControl(mode="paper", client_id="client-a")
    mc._trade_dossier_db_healthy = True
    mc._trade_dossier_db_last_ok_ts = time.time()
    monkeypatch.setattr(mc, "_start_trade_dossier_worker", lambda: None)
    monkeypatch.setattr(
        mc,
        "_write_trade_dossier_now",
        lambda **kwargs: writes.append(kwargs),
    )
    mc._cache_trade_dossier_signal(
        "sig-block-async",
        {
            "signal_id": "sig-block-async",
            "canonical_signal_id": "canon-block-async",
            "ticker": "SPY",
        },
    )

    decision = mc._block(
        "sig-block-async",
        "SPY",
        "client-a",
        "blocked_system",
        "kill_switch_active",
    )

    assert decision.ok is False
    payload = mc._trade_dossier_queue.get_nowait()
    mc._process_trade_dossier_payload(payload)

    assert len(writes) == 1
    assert writes[0]["signal"]["signal_id"] == "sig-block-async"
    assert writes[0]["client_id"] == "client-a"
    assert writes[0]["decision_context"]["master_control_decision"] == "REJECT"


def test_cache_entry_is_evicted_after_block(monkeypatch):
    monkeypatch.setenv("ENABLE_TRADE_DOSSIER", "false")
    from ap_master_control import APMasterControl

    mc = APMasterControl(mode="paper", client_id="client-a")
    mc._cache_trade_dossier_signal("sig-evict", {"signal_id": "sig-evict", "ticker": "SPY"})
    mc._block("sig-evict", "SPY", "client-a", "blocked_system", "invalid_side")

    assert "sig-evict" not in mc._trade_dossier_signal_cache
    assert "sig-evict" not in mc._trade_dossier_signal_cache_ts


def test_cache_does_not_grow_unbounded_after_many_approvals(monkeypatch):
    monkeypatch.setenv("ENABLE_TRADE_DOSSIER", "false")
    from ap_master_control import APMasterControl

    mc = APMasterControl(mode="paper", client_id="client-a")
    for idx in range(600):
        signal_id = f"sig-{idx}"
        signal = {"signal_id": signal_id, "ticker": f"T{idx}", "side": "CALL", "score": 80}
        mc._cache_trade_dossier_signal(signal_id, signal)
        mc._emit_trade_dossier(
            signal,
            client_id="client-a",
            decision_context={"master_control_decision": "APPROVE", "approved": True},
            background=True,
            cache_signal_id=signal_id,
        )

    assert mc._trade_dossier_signal_cache == {}
    assert mc._trade_dossier_signal_cache_ts == {}


def test_migration_creates_trade_dossiers_idempotently():
    sql = (REPO_ROOT / "migrations" / "20260704_trade_dossiers.sql").read_text()
    sql_up = sql.upper()

    assert "CREATE TABLE IF NOT EXISTS TRADE_DOSSIERS" in sql_up
    assert "UNIQUE (CANONICAL_SIGNAL_ID, CLIENT_ID, EXECUTION_MODE, SCHEMA_VERSION)" in sql_up
    assert "CREATE INDEX IF NOT EXISTS IDX_TRADE_DOSSIERS_CLIENT_DATE" in sql_up
    assert "DROP TABLE" not in sql_up
    assert "DELETE FROM" not in sql_up


def test_master_control_insertion_points_are_observe_only():
    src = (REPO_ROOT / "ap_master_control.py").read_text()

    assert "def _emit_trade_dossier" in src
    assert "self._emit_trade_dossier(" in src
    assert "def _enqueue_trade_dossier_write" in src
    assert "require_db_health=True" in src
    assert "trade_dossier_write_skipped_db_unavailable" in src
    helper = src[src.find("def _emit_trade_dossier"):src.find("def _compute_bootstrap_mode")]
    assert "submit_entry" not in helper
    assert "create_entry_order" not in helper
    assert "trade_queue" not in helper
    assert "positions" not in helper
    assert "proof_trades" not in helper
