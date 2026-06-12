from __future__ import annotations

import types
import sys
from datetime import datetime
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import ap_overnight_reeval as ov


class _Broker:
    def get_prior_day_levels(self, _ticker):
        return {"prior_day_high": 101.0, "prior_day_low": 99.0}


class _MasterControl:
    def evaluate(self, signal, client_id=None):
        plan = types.SimpleNamespace(
            ticker=signal.get("ticker"),
            symbol=signal.get("ticker"),
            side=signal.get("side"),
            direction=signal.get("side"),
            score=float(signal.get("score") or 70),
            timeframe=signal.get("timeframe") or "1d",
            pattern=signal.get("pattern") or "2-1-2",
            entry_trigger=signal.get("entry_trigger"),
            trigger_price=signal.get("entry_trigger"),
            trigger_type="breach",
            prior_day_high=signal.get("prior_day_high"),
            prior_day_low=signal.get("prior_day_low"),
            contract_symbol=None,
            contracts=1,
            limit_price=0.01,
            metadata={},
            plan_id="plan-1",
            max_position_usd=100,
        )
        return types.SimpleNamespace(ok=True, plan=plan, reason="")


class _ContractSelector:
    def select(self, _plan):
        return None


class _OSM:
    def __init__(self, order=None):
        self.order = order
        self.created = []
        self.pending = []

    def create_entry_order(self, plan, **_kwargs):
        local_id = f"local-{len(self.created) + 1}"
        self.created.append((local_id, plan))
        self.order = {
            "local_order_id": local_id,
            "kind": "ENTRY",
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "submitted_ts": None,
        }
        return local_id

    def mark_entry_pending_trigger(self, local_order_id):
        self.pending.append(local_order_id)
        return True

    def get_order(self, local_order_id):
        if self.order and self.order.get("local_order_id") == local_order_id:
            return self.order
        return None

    def get_active_entry_orders(self):
        return [self.order] if self.order else []


class _Watcher:
    def __init__(self, owns=False):
        self.owns = owns
        self.watched = []

    def has_order(self, local_order_id):
        return bool(self.owns and local_order_id)

    def watch(self, plan, local_order_id):
        self.watched.append((plan, local_order_id))
        return True


class _Positions:
    def get_position_by_local_order(self, _local_order_id):
        return None

    def get_position_by_broker_order(self, _broker_order_id):
        return None

    def get_active_positions(self):
        return []


def _signal(signal_id="sig-1", created_at="2026-06-11T20:00:00Z"):
    return {
        "signal_id": signal_id,
        "ticker": "AAPL",
        "symbol": "AAPL",
        "side": "CALL",
        "score": 72.0,
        "timeframe": "1d",
        "pattern": "2-1-2",
        "created_at": created_at,
        "entry_trigger": 101.0,
    }


def _job(source="ap_signals", **overrides):
    sig = overrides.pop("payload", _signal())
    job = {
        "id": "sup:sig-1" if source == "ap_signals" else 10,
        "signal_id": sig["signal_id"],
        "payload": sig,
        "created_ts": sig["created_at"],
        "status": "WATCHING",
        "_source": source,
    }
    job.update(overrides)
    return job


@pytest.fixture(autouse=True)
def _patch_runtime(monkeypatch):
    monkeypatch.setattr(ov, "_et_now", lambda: datetime(2026, 6, 12, 9, 15))
    monkeypatch.setattr(ov, "_is_trading_day", lambda _dt: True)
    monkeypatch.setattr(
        "ap.overnight_daily_validator.fetch_market_snapshot",
        lambda _ticker, _broker: {"last": 100.0},
    )
    monkeypatch.setattr(
        "ap.overnight_daily_validator.validate_overnight_daily_signal",
        lambda **_kwargs: types.SimpleNamespace(valid=True, reason_code="", reason_text=""),
    )
    fake_auth = types.ModuleType("ap.authorization")
    fake_auth.execution_mode_for_broker = lambda _broker: "paper"
    fake_auth.broker_live_mode_known = lambda _broker: True
    fake_auth.is_live_broker = lambda _broker: False
    fake_auth.check_live_authorization = lambda _client_id: None
    fake_auth.authorization_gate_enforced = lambda: False
    fake_auth.LIVE_AUTHORIZATION_GATE_UNAVAILABLE = "LIVE_AUTHORIZATION_GATE_UNAVAILABLE"
    monkeypatch.setitem(sys.modules, "ap.authorization", fake_auth)
    monkeypatch.setattr(ov, "_record_overnight_arm_proof", lambda **_kwargs: True)


def _run_once(monkeypatch, jobs, *, proof=None, watcher=None, osm=None):
    monkeypatch.setattr(ov, "_fetch_watching_signals", lambda _client_id: jobs)
    monkeypatch.setattr(ov, "_get_overnight_arm_proof", lambda **_kwargs: proof)
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda *_args, **_kwargs: None)
    watcher = watcher or _Watcher()
    osm = osm or _OSM()
    result = ov.run_overnight_reeval(
        client_id="client-a",
        broker=_Broker(),
        master_control=_MasterControl(),
        contract_selector=_ContractSelector(),
        order_state_machine=osm,
        entry_watcher=watcher,
        position_manager=_Positions(),
        force=True,
    )
    return result, osm, watcher


def test_same_client_setup_session_already_armed_no_duplicate_local_order(monkeypatch):
    proof = {
        "local_order_id": "local-existing",
        "arm_state": "ARMED",
        "retryable": False,
        "metadata": {"overnight_session_date": "2026-06-12"},
    }

    result, osm, watcher = _run_once(monkeypatch, [_job()], proof=proof)

    assert result["skipped"] == 1
    assert osm.created == []
    assert watcher.watched == []


def test_same_session_watcher_owned_entry_without_ledger_skips_duplicate(monkeypatch):
    sig = _signal()
    setup_identity = ov._overnight_setup_identity(sig, entry_trigger=101.0)
    canonical = ov._normalize_overnight_canonical_signal_id(sig["signal_id"], sig)
    arm_key = ov._overnight_arm_key(
        client_id="client-a",
        canonical_signal_id=canonical,
        setup_identity=setup_identity,
        session_date="2026-06-12",
    )
    existing_order = {
        "local_order_id": "local-existing",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "meta": {
            "overnight_arm_key": arm_key,
            "overnight_session_date": "2026-06-12",
            "overnight_setup_identity": setup_identity,
            "overnight_canonical_signal_id": canonical,
        },
    }

    result, osm, watcher = _run_once(
        monkeypatch,
        [_job(payload=sig)],
        proof=None,
        watcher=_Watcher(owns=True),
        osm=_OSM(order=existing_order),
    )

    assert result["skipped"] == 1
    assert osm.created == []
    assert watcher.watched == []


def test_same_client_setup_prior_session_current_session_can_arm(monkeypatch):
    def prior_session_filtered(**_kwargs):
        return None

    monkeypatch.setattr(ov, "_get_overnight_arm_proof", prior_session_filtered)
    monkeypatch.setattr(ov, "_fetch_watching_signals", lambda _client_id: [_job()])

    osm = _OSM()
    watcher = _Watcher()
    result = ov.run_overnight_reeval(
        client_id="client-a",
        broker=_Broker(),
        master_control=_MasterControl(),
        contract_selector=_ContractSelector(),
        order_state_machine=osm,
        entry_watcher=watcher,
        position_manager=_Positions(),
        force=True,
    )

    assert result["armed"] == 1
    assert len(osm.created) == 1
    assert len(watcher.watched) == 1


def test_retryable_same_session_proof_can_retry(monkeypatch):
    proof = {
        "local_order_id": "local-old",
        "arm_state": "MISSED",
        "retryable": True,
        "metadata": {"overnight_session_date": "2026-06-12"},
    }

    result, osm, watcher = _run_once(monkeypatch, [_job()], proof=proof)

    assert result["armed"] == 1
    assert len(osm.created) == 1
    assert len(watcher.watched) == 1


def test_prior_day_after_hours_deferred_watching_proceeds_to_arm(monkeypatch):
    sig = _signal(created_at="2026-06-11T21:30:00Z")
    job = _job(
        "trade_queue",
        payload=sig,
        last_error="after_hours_deferred:awaiting_overnight_reeval",
        started_ts=None,
    )

    result, osm, watcher = _run_once(monkeypatch, [job], proof=None)

    assert result["armed"] == 1
    assert len(osm.created) == 1
    assert len(watcher.watched) == 1


def test_stale_watching_with_no_watcher_order_broker_position_repaired(monkeypatch):
    marked = []
    proofs = []
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda job_id, client_id, reason: marked.append((job_id, client_id, reason)))
    monkeypatch.setattr(ov, "_record_overnight_arm_proof", lambda **kwargs: proofs.append(kwargs) or True)

    sig = _signal()
    job = _job(
        "trade_queue",
        payload=sig,
        result_json={"local_order_id": "local-stale"},
        last_error="armed:contract=DEFERRED:AAPL",
        started_ts="2026-06-11T20:00:00Z",
    )

    repaired = ov._repair_stale_trade_queue_watching(
        job=job,
        signal=sig,
        client_id="client-a",
        signal_id=sig["signal_id"],
        canonical_signal_id=sig["signal_id"],
        session_date="2026-06-12",
        setup_identity="AAPL|CALL|1d|2-1-2|101.0000",
        arm_key="key",
        ticker="AAPL",
        side="CALL",
        entry_watcher=_Watcher(owns=False),
        order_state_machine=_OSM(order=None),
        position_manager=_Positions(),
    )

    assert repaired is True
    assert marked[0][2] == "STALE_WATCHING_REPAIRED:no_watcher_no_order_no_broker_no_position"
    assert proofs[0]["state"] == "TERMINAL"


def test_prior_day_local_order_without_viable_ownership_allows_stale_repair(monkeypatch):
    marked = []
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda job_id, client_id, reason: marked.append((job_id, client_id, reason)))
    monkeypatch.setattr(ov, "_record_overnight_arm_proof", lambda **_kwargs: True)
    sig = _signal(created_at="2026-06-11T21:30:00Z")
    job = _job("trade_queue", payload=sig, result_json={"local_order_id": "local-stale"})

    repaired = ov._repair_stale_trade_queue_watching(
        job=job,
        signal=sig,
        client_id="client-a",
        signal_id=sig["signal_id"],
        canonical_signal_id=sig["signal_id"],
        session_date="2026-06-12",
        setup_identity="setup",
        arm_key="key",
        ticker="AAPL",
        side="CALL",
        entry_watcher=_Watcher(owns=False),
        order_state_machine=_OSM(order=None),
        position_manager=_Positions(),
    )

    assert repaired is True
    assert marked[0][2] == "STALE_WATCHING_REPAIRED:no_watcher_no_order_no_broker_no_position"


def test_prior_day_started_watching_without_viable_ownership_allows_stale_repair(monkeypatch):
    marked = []
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda job_id, client_id, reason: marked.append((job_id, client_id, reason)))
    monkeypatch.setattr(ov, "_record_overnight_arm_proof", lambda **_kwargs: True)
    sig = _signal(created_at="2026-06-11T21:30:00Z")
    job = _job("trade_queue", payload=sig, started_ts="2026-06-11T21:31:00Z")

    repaired = ov._repair_stale_trade_queue_watching(
        job=job,
        signal=sig,
        client_id="client-a",
        signal_id=sig["signal_id"],
        canonical_signal_id=sig["signal_id"],
        session_date="2026-06-12",
        setup_identity="setup",
        arm_key="key",
        ticker="AAPL",
        side="CALL",
        entry_watcher=_Watcher(owns=False),
        order_state_machine=_OSM(order=None),
        position_manager=_Positions(),
    )

    assert repaired is True
    assert marked[0][2] == "STALE_WATCHING_REPAIRED:no_watcher_no_order_no_broker_no_position"


def test_after_hours_deferred_without_started_is_not_repaired(monkeypatch):
    marked = []
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda *args, **kwargs: marked.append(args))
    sig = _signal(created_at="2026-06-11T21:30:00Z")
    job = _job(
        "trade_queue",
        payload=sig,
        last_error="after_hours_deferred:awaiting_overnight_reeval",
        started_ts=None,
    )

    repaired = ov._repair_stale_trade_queue_watching(
        job=job,
        signal=sig,
        client_id="client-a",
        signal_id=sig["signal_id"],
        canonical_signal_id=sig["signal_id"],
        session_date="2026-06-12",
        setup_identity="setup",
        arm_key="key",
        ticker="AAPL",
        side="CALL",
        entry_watcher=_Watcher(owns=False),
        order_state_machine=_OSM(order=None),
        position_manager=_Positions(),
    )

    assert repaired is False
    assert marked == []


def test_watching_with_watcher_owned_local_order_preserved(monkeypatch):
    marked = []
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda *args, **kwargs: marked.append(args))
    sig = _signal()
    job = _job("trade_queue", payload=sig, result_json={"local_order_id": "local-live"})

    repaired = ov._repair_stale_trade_queue_watching(
        job=job,
        signal=sig,
        client_id="client-a",
        signal_id=sig["signal_id"],
        canonical_signal_id=sig["signal_id"],
        session_date="2026-06-12",
        setup_identity="setup",
        arm_key="key",
        ticker="AAPL",
        side="CALL",
        entry_watcher=_Watcher(owns=True),
        order_state_machine=_OSM(order={"local_order_id": "local-live", "kind": "ENTRY", "status": "PENDING_TRIGGER"}),
        position_manager=_Positions(),
    )

    assert repaired is False
    assert marked == []


def test_watching_with_broker_order_preserved(monkeypatch):
    marked = []
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda *args, **kwargs: marked.append(args))
    sig = _signal()
    job = _job("trade_queue", payload=sig, result_json={"local_order_id": "local-submitted"}, broker_order_id="BRK-1")

    repaired = ov._repair_stale_trade_queue_watching(
        job=job,
        signal=sig,
        client_id="client-a",
        signal_id=sig["signal_id"],
        canonical_signal_id=sig["signal_id"],
        session_date="2026-06-12",
        setup_identity="setup",
        arm_key="key",
        ticker="AAPL",
        side="CALL",
        entry_watcher=_Watcher(owns=False),
        order_state_machine=_OSM(order=None),
        position_manager=_Positions(),
    )

    assert repaired is False
    assert marked == []


def test_shared_ap_signals_remains_watching_for_other_clients(monkeypatch):
    marked = []
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda *args, **kwargs: marked.append(args))
    sig = _signal()

    repaired = ov._repair_stale_trade_queue_watching(
        job=_job("ap_signals", payload=sig),
        signal=sig,
        client_id="client-a",
        signal_id=sig["signal_id"],
        canonical_signal_id=sig["signal_id"],
        session_date="2026-06-12",
        setup_identity="setup",
        arm_key="key",
        ticker="AAPL",
        side="CALL",
        entry_watcher=_Watcher(owns=False),
        order_state_machine=_OSM(order=None),
        position_manager=_Positions(),
    )

    assert repaired is False
    assert marked == []


def test_reeval_id_normalizes_to_underlying_canonical_signal_id():
    cid = ov._normalize_overnight_canonical_signal_id(
        "REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9:abc123",
        {},
    )

    assert cid == "603bce5b-352e-44e0-b00b-6baa826ca2c9"
