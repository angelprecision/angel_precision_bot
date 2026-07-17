from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace

import ap.exit_decision_idempotency_guard as guard


def _reset_guard_caches() -> None:
    with guard._LEDGER_LOCK:
        guard._LEDGER_LAST_WRITTEN.clear()
    with guard._PRECHECK_LOCK:
        guard._PRECHECK_LAST_RUN.clear()


def _pos(**overrides):
    values = {
        "position_id": "position-1",
        "client_id": "client@example.com",
        "ticker": "SPY",
        "option_symbol": "SPY260716P00751000",
        "quantity_remaining": 3,
        "closed": False,
        "exit_in_flight": False,
        "pending_exit_action": "",
        "pending_exit_reason": "",
        "pending_exit_qty": 0,
        "pending_exit_local_order_id": "",
        "pending_exit_broker_order_id": "",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _decision(**overrides):
    values = {
        "action": "SCALE_OUT",
        "quantity": 1,
        "reason_code": "TP_SCALE_OUT",
        "reason": "SCALE_1 (+15%)",
        "should_act": True,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class _FakeEngine:
    def __init__(self, positions, *, osm=None):
        self._lock = threading.RLock()
        self._positions = list(positions)
        self._positions_by_id = {
            str(position.position_id): position
            for position in positions
            if getattr(position, "position_id", "")
        }
        self.order_state_machine = osm
        self.osm = None

    def active_positions(self):
        with self._lock:
            return [
                position
                for position in self._positions
                if not position.closed and int(position.quantity_remaining or 0) > 0
            ]


def test_active_exit_statuses_block_early_re_evaluation() -> None:
    for status in (
        "EXIT_REQUESTED",
        "EXIT_SUBMITTED",
        "EXIT_ACKNOWLEDGED",
        "EXIT_PARTIAL_FILL",
    ):
        assert guard.active_exit_order_blocks({"status": status}) is True

    assert guard.active_exit_order_blocks({"status": "EXIT_FILLED"}) is False
    assert guard.active_exit_order_blocks(None) is False


def test_ledger_records_one_decision_per_fingerprint_window() -> None:
    _reset_guard_caches()
    pos = _pos()
    decision = _decision()
    assert guard.should_write_ledger(pos, decision, now_monotonic=100.0) is True
    assert guard.should_write_ledger(pos, decision, now_monotonic=101.0) is False
    assert guard.should_write_ledger(
        pos,
        decision,
        now_monotonic=100.0 + guard._ACTION_LEDGER_TTL + 0.01,
    ) is True


def test_durable_generation_key_uses_terminal_orders_and_remaining_qty(monkeypatch) -> None:
    class Cursor:
        def execute(self, sql, params):
            assert "COUNT(DISTINCT local_order_id)" in sql
            assert params[0:2] == ("client@example.com", "position-1")
            return self

        def fetchone(self):
            return {"terminal_exit_count": 2}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(guard, "conn", lambda: Cursor())
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())

    assert guard._durable_exit_generation(_pos(quantity_remaining=3), "client@example.com") == (
        "client@example.com|position-1|3|3",
        3,
    )


def test_durable_generation_claim_is_atomic(monkeypatch) -> None:
    captured = {}

    class Cursor:
        rowcount = 1

        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return self

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(guard, "conn", lambda: Cursor())
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())

    assert guard._claim_durable_decision_generation(
        generation_key="client|position|3|1",
        client_id="client",
        position_id="position",
        remaining_qty=3,
        exit_generation=1,
        decision=_decision(),
    ) is True
    assert "ON CONFLICT (generation_key) DO NOTHING" in captured["sql"]


def test_ledger_wrapper_suppresses_duplicate_durable_generation(monkeypatch) -> None:
    _reset_guard_caches()
    calls = []
    monkeypatch.setattr(
        guard,
        "_durable_exit_generation",
        lambda pos, client_id: ("client|position|3|1", 1),
    )
    monkeypatch.setattr(
        guard,
        "_claim_durable_decision_generation",
        lambda **kwargs: False,
    )
    wrapped = guard.wrap_ledger(
        lambda pos, decision, client_id="": calls.append((pos, decision, client_id))
    )

    assert wrapped(_pos(), _decision(), client_id="client@example.com") is None
    assert calls == []


def test_durable_claim_migration_is_locked_to_internal_roles() -> None:
    migration = (
        Path(__file__).resolve().parents[1]
        / "migrations"
        / "20260717_exit_decision_generation_claims.sql"
    ).read_text()

    assert "generation_key       TEXT PRIMARY KEY" in migration
    assert "ENABLE ROW LEVEL SECURITY" in migration
    assert "REVOKE ALL ON TABLE exit_decision_generation_claims FROM anon, authenticated" in migration


def test_ledger_suppresses_closed_zero_and_inflight_positions() -> None:
    _reset_guard_caches()
    decision = _decision()
    assert guard.should_write_ledger(_pos(closed=True), decision, now_monotonic=1.0) is False
    assert guard.should_write_ledger(_pos(quantity_remaining=0), decision, now_monotonic=1.0) is False
    assert guard.should_write_ledger(_pos(exit_in_flight=True), decision, now_monotonic=1.0) is False


def test_external_precheck_cache_bounds_db_and_osm_reads() -> None:
    _reset_guard_caches()
    pos = _pos(position_id="position-cache")
    assert guard.should_run_external_precheck(pos, now_monotonic=100.0) is True
    assert guard.should_run_external_precheck(pos, now_monotonic=101.0) is False
    assert guard.should_run_external_precheck(
        pos,
        now_monotonic=100.0 + guard._EXTERNAL_PRECHECK_TTL + 0.01,
    ) is True
    assert guard.should_run_external_precheck(_pos(exit_in_flight=True), now_monotonic=500.0) is False


def test_submit_wrapper_allows_only_one_concurrent_callback() -> None:
    _reset_guard_caches()
    callback_entered = threading.Event()
    callback_release = threading.Event()
    callback_count = 0
    callback_count_lock = threading.Lock()

    def original(engine, pos, decision, *args, **kwargs):
        nonlocal callback_count
        with callback_count_lock:
            callback_count += 1
        callback_entered.set()
        assert callback_release.wait(timeout=2.0)
        return True

    wrapped = guard.wrap_submit(original)
    engine = SimpleNamespace(_lock=threading.RLock())
    pos = _pos()
    decision = _decision()
    first_result: list[bool] = []

    thread = threading.Thread(target=lambda: first_result.append(wrapped(engine, pos, decision)))
    thread.start()
    assert callback_entered.wait(timeout=2.0)

    second_result = wrapped(engine, pos, decision)
    callback_release.set()
    thread.join(timeout=2.0)

    assert thread.is_alive() is False
    assert first_result == [True]
    assert second_result is False
    assert callback_count == 1
    assert engine._ap_exit_submit_claims == set()


def test_precheck_hydrates_existing_durable_exit_order(monkeypatch) -> None:
    _reset_guard_caches()

    class FakeOSM:
        def get_active_exit_order(self, position_id):
            assert position_id == "position-active-exit"
            return {
                "status": "EXIT_ACKNOWLEDGED",
                "local_order_id": "exit-local-1",
                "broker_order_id": "exit-broker-1",
            }

    pos = _pos(position_id="position-active-exit")
    engine = _FakeEngine([pos], osm=FakeOSM())
    wrapped = guard.wrap_precheck(lambda self: True)
    monkeypatch.setattr(guard, "_terminal_position_snapshot", lambda pos, engine: None)

    assert wrapped(engine) is True
    assert pos.exit_in_flight is True
    assert pos.pending_exit_local_order_id == "exit-local-1"
    assert pos.pending_exit_broker_order_id == "exit-broker-1"
    assert engine.active_positions() == [pos]


def test_precheck_removes_terminal_position_only_with_fresh_exact_broker_flat(monkeypatch) -> None:
    _reset_guard_caches()
    pos = _pos(position_id="position-terminal")
    engine = _FakeEngine([pos])
    wrapped = guard.wrap_precheck(lambda self: True)

    monkeypatch.setattr(guard, "_active_exit_order", lambda engine, position_id: None)
    monkeypatch.setattr(
        guard,
        "_terminal_position_snapshot",
        lambda pos, engine: {
            "id": "position-terminal",
            "status": "CLOSED",
            "contract": "SPY260716P00751000",
        },
    )
    monkeypatch.setattr(guard, "_fresh_exact_broker_flat", lambda pos, engine: True)

    assert wrapped(engine) is True
    assert pos.closed is True
    assert pos.quantity_remaining == 0
    assert engine.active_positions() == []
    assert "position-terminal" not in engine._positions_by_id


def test_precheck_preserves_exit_when_broker_truth_is_unavailable(monkeypatch) -> None:
    _reset_guard_caches()
    pos = _pos(position_id="position-unknown-broker")
    engine = _FakeEngine([pos])
    wrapped = guard.wrap_precheck(lambda self: False)

    monkeypatch.setattr(guard, "_active_exit_order", lambda engine, position_id: None)
    monkeypatch.setattr(
        guard,
        "_terminal_position_snapshot",
        lambda pos, engine: {
            "id": "position-unknown-broker",
            "status": "CLOSED",
            "contract": "SPY260716P00751000",
        },
    )
    monkeypatch.setattr(guard, "_fresh_exact_broker_flat", lambda pos, engine: False)

    assert wrapped(engine) is False
    assert pos.closed is False
    assert pos.quantity_remaining == 3
    assert engine.active_positions() == [pos]
