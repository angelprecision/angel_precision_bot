from __future__ import annotations

import sys
import types
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from zoneinfo import ZoneInfo

import ap_overnight_reeval as ov


FIXED_ET = datetime(2026, 7, 22, 9, 31, tzinfo=ZoneInfo("America/New_York"))


def _signal(
    signal_id: str,
    ticker: str,
    *,
    side: str = "CALL",
    tier: str = "A",
    score: float = 80.0,
    created_at: str = "2026-07-21T20:00:00+00:00",
) -> dict:
    return {
        "signal_id": signal_id,
        "canonical_signal_id": signal_id,
        "ticker": ticker,
        "symbol": ticker,
        "side": side,
        "timeframe": "1d",
        "score": score,
        "tier": tier,
        "pattern": "2-1-2",
        "entry_trigger": 101.0 if side == "CALL" else 95.0,
        "created_at": created_at,
        "force_overnight_reeval_only": True,
        "do_not_queue_directly": True,
    }


def _job(row_id: object, signal: dict) -> dict:
    return {
        "id": row_id,
        "signal_id": signal["signal_id"],
        "payload": signal,
        "created_ts": signal["created_at"],
        "_source": "trade_queue",
    }


class _MasterControl:
    def evaluate(self, signal: dict, *, client_id: str):
        original_signal_id = str(signal["signal_id"]).split(":", 2)[1]
        plan = SimpleNamespace(
            plan_id=f"plan-{original_signal_id}",
            signal_id=original_signal_id,
            canonical_signal_id=original_signal_id,
            client_id=client_id,
            execution_mode="PAPER",
            ticker=signal["ticker"],
            side=signal["side"],
            direction=signal["side"],
            score=float(signal["score"]),
            tier=signal["tier"],
            timeframe="1d",
            pattern=signal["pattern"],
            entry_trigger=float(signal["entry_trigger"]),
            trigger_price=float(signal["entry_trigger"]),
            trigger_type="breach",
            prior_day_high=101.0,
            prior_day_low=95.0,
            contract_symbol="SHOULD_NOT_SURVIVE",
            contracts=1,
            limit_price=9.99,
            max_position_usd=999.0,
            metadata={
                "canonical_signal_id": original_signal_id,
                "client_id": client_id,
            },
        )
        return SimpleNamespace(ok=True, plan=plan, reason="approved", score=plan.score)


class _OSM:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.create_calls: list[dict] = []

    def create_entry_order(self, plan, **kwargs):
        if self.fail:
            raise RuntimeError("insert failed")
        local_order_id = f"local-{len(self.create_calls) + 1}"
        self.create_calls.append({"plan": plan, "local_order_id": local_order_id, **kwargs})
        return local_order_id

    def get_order(self, local_order_id: str) -> dict:
        return {"local_order_id": local_order_id, "status": "PENDING_TRIGGER"}


class _Watcher:
    def __init__(self):
        self.calls: list[tuple[object, str]] = []

    def watch(self, plan, local_order_id: str) -> bool:
        self.calls.append((plan, local_order_id))
        return True


def _run_harness(
    monkeypatch,
    jobs: list[dict],
    *,
    snapshot_available: bool = True,
    duplicate_signal_ids: set[str] | None = None,
    osm: _OSM | None = None,
    client_id: str = "jose@example.com",
    execution_mode: str = "PAPER",
):
    counters = {"prior": [], "snapshot": []}

    class _Broker:
        def __init__(self):
            self.submit_order = MagicMock()
            self.place_order = MagicMock()
            self.cancel_order = MagicMock()
            self.replace_order = MagicMock()

        def get_prior_day_levels(self, ticker: str) -> dict:
            counters["prior"].append(ticker)
            return {
                "prior_day_high": 101.0,
                "prior_day_low": 95.0,
                "prior_day_close": 98.0,
                "source": "test",
                "observed_at": FIXED_ET.isoformat(),
            }

    broker = _Broker()

    validator = types.ModuleType("ap.overnight_daily_validator")

    def _fetch_snapshot(ticker: str, _broker):
        counters["snapshot"].append(ticker)
        if not snapshot_available:
            return None
        return {
            "last": 100.0,
            "source": "test",
            "observed_at": FIXED_ET.isoformat(),
        }

    validator.fetch_market_snapshot = _fetch_snapshot
    validator.validate_overnight_daily_signal = lambda **kwargs: SimpleNamespace(
        valid=kwargs.get("snapshot") is not None,
        reason_code="" if kwargs.get("snapshot") is not None else "SNAPSHOT_UNAVAILABLE",
        reason_text="" if kwargs.get("snapshot") is not None else "snapshot unavailable",
    )
    validator.InvalidationReason = object
    monkeypatch.setitem(sys.modules, "ap.overnight_daily_validator", validator)

    auth = types.ModuleType("ap.authorization")
    auth.is_live_broker = lambda _broker: execution_mode == "LIVE"
    auth.broker_live_mode_known = lambda _broker: True
    auth.check_live_authorization = lambda _client_id: None
    auth.authorization_gate_enforced = lambda: False
    auth.LIVE_AUTHORIZATION_GATE_UNAVAILABLE = "LIVE_AUTHORIZATION_GATE_UNAVAILABLE"
    auth.execution_mode_for_broker = lambda _broker: execution_mode
    monkeypatch.setitem(sys.modules, "ap.authorization", auth)

    intel = types.ModuleType("ap.intelligence_context_handoff")
    intel.enqueue_preopen_context_best_effort = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "ap.intelligence_context_handoff", intel)

    rejected: list[tuple[object, str, str]] = []
    errors: list[tuple[object, str, str]] = []
    waiting: list[tuple[object, str, str]] = []
    armed_rows: list[tuple[object, str, str]] = []
    duplicate_signal_ids = duplicate_signal_ids or set()
    monkeypatch.setattr(ov, "_et_now", lambda: FIXED_ET)
    monkeypatch.setattr(ov, "_OVERNIGHT_SNAPSHOT_FAIL_CLOSED", False)
    monkeypatch.setattr(ov, "_fetch_watching_signals", lambda _client_id: list(jobs))
    monkeypatch.setattr(
        ov,
        "_shared_watch_arm_failure_already_recorded",
        lambda signal_id, *_args, **_kwargs: signal_id in duplicate_signal_ids,
    )
    monkeypatch.setattr(
        ov,
        "_mark_job_rejected",
        lambda row_id, owner, reason: rejected.append((row_id, owner, reason)),
    )
    monkeypatch.setattr(
        ov,
        "_mark_job_error",
        lambda row_id, owner, reason: errors.append((row_id, owner, reason)),
    )
    monkeypatch.setattr(
        ov,
        "_mark_job_watching_reason",
        lambda row_id, owner, reason: waiting.append((row_id, owner, reason)),
    )
    monkeypatch.setattr(
        ov,
        "_mark_job_watching_armed",
        lambda row_id, owner, contract: armed_rows.append((row_id, owner, contract)),
    )

    selector = MagicMock()
    osm = osm or _OSM()
    watcher = _Watcher()
    result = ov.run_overnight_reeval(
        client_id=client_id,
        broker=broker,
        data_broker=broker,
        master_control=_MasterControl(),
        contract_selector=selector,
        order_state_machine=osm,
        entry_watcher=watcher,
        force=True,
    )
    return SimpleNamespace(
        result=result,
        broker=broker,
        counters=counters,
        selector=selector,
        osm=osm,
        watcher=watcher,
        rejected=rejected,
        errors=errors,
        waiting=waiting,
        armed_rows=armed_rows,
    )


def test_mixed_terminal_and_56_deferred_rows_remain_retryable(monkeypatch):
    jobs = [_job("duplicate", _signal("duplicate", "DUP"))]
    jobs.extend(_job(index, _signal(f"deferred-{index}", f"T{index:02d}")) for index in range(56))

    state = _run_harness(
        monkeypatch,
        jobs,
        snapshot_available=False,
        duplicate_signal_ids={"duplicate"},
    )

    assert state.result["result_class"] == "RETRYABLE_PARTIAL_DEFERRED"
    assert state.result["completed"] is False
    assert state.result["retryable"] is True
    assert state.result["retry_reason"] == "retryable_rows_remain"
    assert state.result["terminal_rejected"] == 1
    assert state.result["retryable_deferred"] == 56
    assert state.result["unresolved"] == 0


def test_second_attempt_drains_only_remaining_inventory_once(monkeypatch):
    duplicate = _job("duplicate", _signal("duplicate", "DUP"))
    remaining = [_job(index, _signal(f"remaining-{index}", f"R{index:02d}")) for index in range(12)]

    first = _run_harness(
        monkeypatch,
        [duplicate, *remaining],
        snapshot_available=False,
        duplicate_signal_ids={"duplicate"},
    )
    assert first.result["retryable_deferred"] == 12

    second = _run_harness(monkeypatch, remaining, snapshot_available=True)

    assert second.result["result_class"] == "COMPLETED_WITH_DECISIONS"
    assert second.result["armed"] == 12
    assert second.result["retryable_deferred"] == 0
    assert len(second.osm.create_calls) == 12
    assert len(second.watcher.calls) == 12
    assert all(call["plan"].signal_id.startswith("remaining-") for call in second.osm.create_calls)
    assert not second.rejected


def test_armed_trade_queue_row_leaves_watching_inventory(monkeypatch):
    calls = []

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, params):
            calls.append((query, params))

    db = types.ModuleType("ap.db")
    db.conn = lambda: _Cursor()
    db.run_with_retry = lambda fn: fn()
    monkeypatch.setitem(sys.modules, "ap.db", db)

    ov._mark_job_watching_armed(42, "jose@example.com", "DEFERRED:AAPL")

    query, params = calls[0]
    assert "SET status = 'ARMED'" in query
    assert "AND status = 'WATCHING'" in query
    assert params == ("armed:contract=DEFERRED:AAPL", 42, "jose@example.com")


def test_all_deferred_and_fully_resolved_classifications(monkeypatch):
    deferred = [_job(index, _signal(f"deferred-{index}", f"D{index}")) for index in range(3)]
    all_deferred = _run_harness(monkeypatch, deferred, snapshot_available=False)
    assert all_deferred.result["result_class"] == "RETRYABLE_ALL_DEFERRED"
    assert all_deferred.result["completed"] is False
    assert all_deferred.result["retryable"] is True

    resolved_jobs = [
        _job("duplicate", _signal("duplicate", "DUP")),
        _job("valid", _signal("valid", "AAPL")),
    ]
    resolved = _run_harness(
        monkeypatch,
        resolved_jobs,
        duplicate_signal_ids={"duplicate"},
    )
    assert resolved.result["result_class"] == "COMPLETED_WITH_DECISIONS"
    assert resolved.result["completed"] is True
    assert resolved.result["retryable"] is False
    assert resolved.result["armed"] == 1
    assert resolved.result["terminal_rejected"] == 1


def test_200_rows_arm_before_selector_and_reuse_market_data_per_ticker(monkeypatch):
    jobs = []
    for index in range(100):
        ticker = f"S{index:03d}"
        jobs.append(_job(f"{index}-call", _signal(f"{ticker}-call", ticker, side="CALL")))
        jobs.append(_job(f"{index}-put", _signal(f"{ticker}-put", ticker, side="PUT")))

    state = _run_harness(monkeypatch, jobs)

    assert state.result["armed"] == 200
    assert len(state.osm.create_calls) == 200
    assert len(state.watcher.calls) == 200
    state.selector.select.assert_not_called()
    assert len(state.counters["prior"]) == 100
    assert len(state.counters["snapshot"]) == 100
    assert all(call["plan"].contract_symbol.startswith("DEFERRED:") for call in state.osm.create_calls)
    assert all(call["plan"].metadata["deferred_breach_selection"] is True for call in state.osm.create_calls)
    state.broker.submit_order.assert_not_called()
    state.broker.place_order.assert_not_called()
    state.broker.cancel_order.assert_not_called()
    state.broker.replace_order.assert_not_called()


def test_osm_materialization_failure_is_visible_and_never_arms(monkeypatch):
    state = _run_harness(
        monkeypatch,
        [_job(1, _signal("create-fail", "FAIL"))],
        osm=_OSM(fail=True),
    )

    assert state.result["result_class"] == "RETRYABLE_ROW_ERRORS"
    assert state.result["completed"] is False
    assert state.result["terminal_errors"] == 1
    assert state.result["unresolved"] == 0
    assert state.watcher.calls == []
    assert state.errors[0][2] == "order_materialization_failed:create_entry_order:RuntimeError"


def test_processing_order_and_identity_mode_are_deterministic(monkeypatch):
    jobs = [
        _job("4", _signal("b-high", "B_HIGH", tier="B", score=99, created_at="2026-07-21T21:00:00+00:00")),
        _job("3", _signal("a-low", "A_LOW", tier="A", score=80, created_at="2026-07-21T22:00:00+00:00")),
        _job("2", _signal("a-high-2", "A_HIGH_2", tier="A", score=90, created_at="2026-07-21T21:00:00+00:00")),
        _job("1", _signal("a-high-1", "A_HIGH_1", tier="A", score=90, created_at="2026-07-21T21:00:00+00:00")),
    ]

    state = _run_harness(monkeypatch, jobs, client_id="jose@example.com", execution_mode="PAPER")

    assert [call["plan"].ticker for call in state.osm.create_calls] == [
        "A_HIGH_1",
        "A_HIGH_2",
        "A_LOW",
        "B_HIGH",
    ]
    assert all(call["execution_mode"] == "PAPER" for call in state.osm.create_calls)
    assert all(call["plan"].client_id == "jose@example.com" for call in state.osm.create_calls)
    assert len({call["local_order_id"] for call in state.osm.create_calls}) == 4
