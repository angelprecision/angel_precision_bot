from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))

from ap import exit_safety as exit_safety_mod  # noqa: E402
from ap import order_state_machine as osm_mod  # noqa: E402
from ap.broker_submit_identity import canonical_broker_submit_key  # noqa: E402
from ap.exit_autonomous_recovery import recover_exit_engine, recover_exit_position  # noqa: E402
from ap.order_state_machine import APOrderStateMachine  # noqa: E402


class _FakeConn:
    def __init__(self, resolver):
        self._resolver = resolver
        self._row = None
        self._rows = []
        self.queries: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params=()):
        self.queries.append((sql, tuple(params)))
        payload = self._resolver(sql, tuple(params))
        if isinstance(payload, tuple):
            self._row, self._rows = payload
        else:
            self._row, self._rows = payload, []
        return self

    def fetchone(self):
        return self._row

    def fetchall(self):
        return list(self._rows)


class _FakeConnContext:
    def __init__(self, fake_conn):
        self._fake_conn = fake_conn

    def __enter__(self):
        return self._fake_conn

    def __exit__(self, exc_type, exc, tb):
        return False


class _MockOSM:
    submit_exit = APOrderStateMachine.submit_exit
    update_order_meta = APOrderStateMachine.update_order_meta

    def __init__(self):
        self.client_id = "jason@example.com"
        self.transitions = []
        self.exit_row = None

    def _get_active_exit_order(self, position_id):
        if self.exit_row and self.exit_row["position_id"] == position_id:
            return dict(self.exit_row)
        return None

    def _get_order(self, local_order_id):
        if self.exit_row and self.exit_row["local_order_id"] == local_order_id:
            return dict(self.exit_row)
        return None

    def create_exit_order(self, **kwargs):
        self.create_exit_kwargs = kwargs
        self.exit_row = {
            "local_order_id": "L-EXIT-001",
            "client_id": self.client_id,
            "position_id": kwargs["position_id"],
            "kind": "EXIT",
            "status": "EXIT_REQUESTED",
            "execution_mode": kwargs["execution_mode"],
            "contract": kwargs["contract"],
            "qty": kwargs["qty"],
            "broker_order_id": "",
            "submitted_ts": None,
            "meta": {},
        }
        return "L-EXIT-001"

    def persist_exit_submit_intent(self, local_order_id, **kwargs):
        if not self.exit_row or self.exit_row["local_order_id"] != local_order_id:
            return False
        key = kwargs["broker_submit_key"]
        self.exit_row["meta"].update({
            "lifecycle_state": "SUBMITTING",
            "submit_intent_at": "2026-08-08T12:00:00+00:00",
            "broker_submit_key": key,
            "broker_submit_payload_hash": kwargs["payload_hash"],
            "current_owner": f"broker_submit:{key}",
        })
        return True

    def transition(self, local_order_id, new_status, **kwargs):
        self.transitions.append((local_order_id, new_status, kwargs))
        if self.exit_row and self.exit_row["local_order_id"] == local_order_id:
            self.exit_row["status"] = new_status
            if kwargs.get("broker_order_id"):
                self.exit_row["broker_order_id"] = kwargs["broker_order_id"]
        return True

    def _resolve_underlying_symbol(self, *, symbol, contract):
        return symbol

    @staticmethod
    def _is_broker_accept_status(status: str) -> bool:
        return status in ("open", "pending", "ok", "accepted")

    def _emit_transition_event(self, **kwargs):
        return None

    def _flag_split_brain_order(self, *args, **kwargs):
        return None

    def _lookup_order_by_tag(self, broker, base_url: str, account_id: str, tag: str):
        return None


def _resp(status_code, json_body=None, text=""):
    response = MagicMock()
    response.status_code = status_code
    response.text = text
    if json_body is not None:
        response.json.return_value = json_body
    return response


@pytest.fixture(autouse=True)
def _schema(monkeypatch):
    monkeypatch.setattr(
        exit_safety_mod,
        "_table_columns",
        lambda table: {
            "positions": {"status", "quantity_remaining", "close_source", "entry_ts"},
            "orders": {"client_id", "kind", "status", "contract", "execution_mode", "created_ts", "updated_ts", "last_error"},
        }[table],
    )
    with exit_safety_mod._ALERT_CACHE_LOCK:
        exit_safety_mod._ALERT_CACHE.clear()


@pytest.fixture
def mock_broker():
    broker = MagicMock()
    broker.base_url = "https://api.tradier.com"
    broker.account_id = "ACC123"
    broker.session = MagicMock()
    # Positive OSM controls require an authoritative held snapshot; individual
    # flat/unavailable tests override this explicitly.
    broker.list_positions.return_value = [
        {"symbol": "SMCI260626P00032500", "quantity": 1, "account_id": "ACC123"},
    ]
    return broker


def _patch_db(monkeypatch, resolver):
    fake_conn = _FakeConn(resolver)
    monkeypatch.setattr(exit_safety_mod, "conn", lambda: _FakeConnContext(fake_conn))
    monkeypatch.setattr(exit_safety_mod, "run_with_retry", lambda fn, *a, **k: fn())
    monkeypatch.setattr(osm_mod, "conn", lambda: _FakeConnContext(fake_conn))
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn, *a, **k: fn())
    return fake_conn


def _open_position_row(quantity_remaining: int = 1):
    return {
        "status": "OPEN",
        "quantity_remaining": quantity_remaining,
        "close_source": None,
        "entry_ts": "2026-06-25T14:30:00+00:00",
    }


def test_exit_guard_blocks_closed_position_before_tradier_call(monkeypatch, mock_broker):
    _patch_db(
        monkeypatch,
        lambda sql, params: {"status": "CLOSED", "quantity_remaining": 0, "close_source": None, "entry_ts": "2026-06-25T14:30:00+00:00"}
        if "FROM positions" in sql
        else {"rejection_count": 0},
    )
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-1",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is False
    assert result["reason"] == "position_already_closed"
    assert mock_broker.session.post.call_count == 0


def test_exit_guard_blocks_zero_quantity_before_tradier_call(monkeypatch, mock_broker):
    _patch_db(
        monkeypatch,
        lambda sql, params: {"status": "OPEN", "quantity_remaining": 0, "close_source": None, "entry_ts": "2026-06-25T14:30:00+00:00"}
        if "FROM positions" in sql
        else {"rejection_count": 0},
    )
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-2",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is False
    assert result["reason"] == "position_quantity_depleted"
    assert mock_broker.session.post.call_count == 0


def test_exit_guard_missing_position_blocks_before_tradier_call(monkeypatch, mock_broker):
    _patch_db(
        monkeypatch,
        lambda sql, params: None if "FROM positions" in sql else {"rejection_count": 0},
    )
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="missing-pos",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is False
    assert result["reason"] == "position_missing"
    assert mock_broker.session.post.call_count == 0


def test_exit_guard_allows_open_position(monkeypatch, mock_broker):
    _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )
    mock_broker.session.post.return_value = _resp(
        200,
        json_body={"order": {"id": "BO-1", "status": "open"}},
    )
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-open",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is True
    assert mock_broker.session.post.call_count == 1


def test_exit_retry_guard_blocks_closed_position(monkeypatch, mock_broker):
    _patch_db(
        monkeypatch,
        lambda sql, params: {"status": "CLOSED", "quantity_remaining": 0, "close_source": "RECONCILER_AUTO_CLOSE", "entry_ts": "2026-06-25T14:30:00+00:00"}
        if "FROM positions" in sql
        else {"rejection_count": 1},
    )
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-retry",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is False
    assert result["reason"] == "position_already_closed"
    assert mock_broker.session.post.call_count == 0


def test_exit_circuit_breaker_trips_at_threshold(monkeypatch, mock_broker):
    monkeypatch.setenv("MAX_EXIT_REJECTIONS_BEFORE_HALT", "5")
    _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 5},
    )
    mock_broker.list_positions.return_value = []
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-breaker",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is False
    assert result["reason"] == "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
    assert mock_broker.session.post.call_count == 0


def test_exit_circuit_breaker_trips_at_threshold_for_error_broker_rejects(monkeypatch, mock_broker):
    monkeypatch.setenv("MAX_EXIT_REJECTIONS_BEFORE_HALT", "5")
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 5},
    )
    mock_broker.list_positions.return_value = []
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-breaker-error",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is False
    assert result["reason"] == "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
    assert not any("FROM orders" in sql for sql, _params in fake_conn.queries)
    assert mock_broker.session.post.call_count == 0


def test_exit_circuit_breaker_does_not_trip_below_threshold(monkeypatch, mock_broker):
    monkeypatch.setenv("MAX_EXIT_REJECTIONS_BEFORE_HALT", "5")
    _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 4},
    )
    mock_broker.session.post.return_value = _resp(
        200,
        json_body={"order": {"id": "BO-2", "status": "open"}},
    )
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-breaker-allow",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is True
    assert mock_broker.session.post.call_count == 1


def test_exit_circuit_breaker_scopes_by_client_and_execution_mode(monkeypatch):
    monkeypatch.setenv("MAX_EXIT_REJECTIONS_BEFORE_HALT", "5")

    def resolver(sql, params):
        if "FROM orders" not in sql:
            return None
        client_id = params[0]
        contract = params[1]
        exec_mode = params[-2]
        if client_id == "jason@example.com" and exec_mode == "live" and contract == "SMCI260626P00032500":
            return {"rejection_count": 5}
        return {"rejection_count": 0}

    fake_conn = _FakeConn(resolver)
    result_live = None
    result_paper = None
    result_other_client = None

    monkeypatch.setattr(exit_safety_mod, "run_with_retry", lambda fn, *a, **k: fn())

    with _FakeConnContext(fake_conn) as c:
        result_live = exit_safety_mod._should_halt_exit_after_rejections(
            c,
            position_id="pos-live",
            client_id="jason@example.com",
            execution_mode="live",
            contract="SMCI260626P00032500",
            entry_ts="2026-06-25T14:30:00+00:00",
        )
        result_paper = exit_safety_mod._should_halt_exit_after_rejections(
            c,
            position_id="pos-paper",
            client_id="jason@example.com",
            execution_mode="paper",
            contract="SMCI260626P00032500",
            entry_ts="2026-06-25T14:30:00+00:00",
        )
        result_other_client = exit_safety_mod._should_halt_exit_after_rejections(
            c,
            position_id="pos-other",
            client_id="other@example.com",
            execution_mode="live",
            contract="SMCI260626P00032500",
            entry_ts="2026-06-25T14:30:00+00:00",
        )

    assert result_live["blocked"] is True
    assert result_paper["blocked"] is False
    assert result_other_client["blocked"] is False


def test_exit_circuit_breaker_invalid_env_uses_default(monkeypatch):
    monkeypatch.setenv("MAX_EXIT_REJECTIONS_BEFORE_HALT", "not-a-number")
    fake_conn = _FakeConn(lambda sql, params: {"rejection_count": 5})

    result = exit_safety_mod._should_halt_exit_after_rejections(
        fake_conn,
        position_id="pos-invalid-env",
        client_id="jason@example.com",
        execution_mode="live",
        contract="SMCI260626P00032500",
        entry_ts="2026-06-25T14:30:00+00:00",
    )

    assert result["blocked"] is True
    assert result["threshold"] == 5


def test_exit_circuit_breaker_counts_error_broker_reject_rows(monkeypatch):
    monkeypatch.setenv("MAX_EXIT_REJECTIONS_BEFORE_HALT", "5")
    fake_conn = _FakeConn(lambda sql, params: {"rejection_count": 5})

    result = exit_safety_mod._should_halt_exit_after_rejections(
        fake_conn,
        position_id="pos-error-rejects",
        client_id="jason@example.com",
        execution_mode="live",
        contract="SMCI260626P00032500",
        entry_ts="2026-06-25T14:30:00+00:00",
    )

    order_sql, order_params = next((sql, params) for sql, params in fake_conn.queries if "FROM orders" in sql)

    assert result["blocked"] is True
    assert result["reason"] == "exit_circuit_breaker_tripped"
    assert "status = 'ERROR'" in order_sql
    assert "COALESCE(last_error, '') ILIKE %s" in order_sql
    assert any("broker_rejected_exit" in str(param).lower() for param in order_params)
    assert any("broker_http_4" in str(param).lower() for param in order_params)


def test_exit_circuit_breaker_alert_failure_does_not_crash_exit_path(monkeypatch, mock_broker):
    monkeypatch.setenv("MAX_EXIT_REJECTIONS_BEFORE_HALT", "5")
    _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 5},
    )
    mock_broker.list_positions.return_value = []
    monkeypatch.setattr(exit_safety_mod, "post_discord", lambda content: (_ for _ in ()).throw(RuntimeError("discord down")))
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-alert-fail",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is False
    assert result["reason"] == "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
    assert mock_broker.session.post.call_count == 0


def test_broker_repair_position_with_broker_truth_is_not_blocked_as_missing(monkeypatch):
    _patch_db(
        monkeypatch,
        lambda sql, params: None if "FROM positions" in sql else {"rejection_count": 0},
    )

    result = exit_safety_mod.evaluate_exit_submission_safety(
        position_id="broker-repair-jason@example.com-SMCI260626P00032500",
        client_id="jason@example.com",
        execution_mode="live",
        contract="SMCI260626P00032500",
        broker_truth_open_qty=3,
        allow_missing_position_with_broker_truth=True,
    )

    assert result["blocked"] is False
    assert result["reason"] is None
    assert result["position_state"]["quantity_remaining"] == 3


@pytest.mark.parametrize(
    ("broker_truth_mode", "guard_error"),
    [
        ("held", False),
        ("unavailable", False),
        ("unavailable", True),
    ],
    ids=["held", "unavailable", "guard-error"],
)
def test_exit_engine_callback_path_preserves_canonical_exit_liveness(
    monkeypatch, broker_truth_mode, guard_error
):
    from ap_exit_engine import APExitEngine, ExitDecision, ManagedPosition
    import ap_exit_engine as exit_engine_mod

    engine = APExitEngine.__new__(APExitEngine)
    engine.client_id = "jason@example.com"
    engine._email = "jason@example.com"
    engine._lock = __import__("threading").Lock()
    engine._thread = None
    engine._running = False
    engine.run_id = "run-1"
    engine.strategy_version = "test"
    engine.git_commit = "test"
    engine.master_control = type("MC", (), {"mode": "live"})()
    engine.on_scale = None
    engine.order_state_machine = None
    engine.osm = None
    engine.broker = MagicMock()
    if broker_truth_mode == "held":
        engine.broker.list_positions.return_value = [
            {"symbol": "SMCI260626P00032500", "quantity": 3},
        ]
    else:
        engine.broker.list_positions.side_effect = RuntimeError("positions down")
    engine._positions = []
    engine._positions_by_id = {}
    engine._can_submit_exit = lambda *args, **kwargs: True
    engine.hydrate_pending_exit_identity_from_db = lambda pos: False
    events = []
    engine._emit_exit_event = lambda *args, **kwargs: events.append(kwargs)
    engine._extract_exit_order_identity = lambda result: {
        "accepted": True,
        "local_order_id": "L-EXIT-001",
        "broker_order_id": "BO-EXIT-001",
        "raw_status": "accepted",
    }
    engine._mark_exit_submitted = lambda current_pos, decision, local_order_id="", broker_order_id="": (
        setattr(current_pos, "exit_in_flight", True),
        setattr(current_pos, "pending_exit_local_order_id", local_order_id),
        setattr(current_pos, "pending_exit_broker_order_id", broker_order_id),
    )

    calls = {}

    def _fake_guard(**kwargs):
        if guard_error:
            raise RuntimeError("truth guard down")
        calls.update(kwargs)
        return {
            "blocked": False,
            "reason": None,
            "position_state": {
                "blocked": False,
                "status": None,
                "quantity_remaining": kwargs.get("broker_truth_open_qty"),
                "close_source": None,
                "entry_ts": None,
            },
            "circuit_breaker": {"blocked": False, "reason": None, "rejection_count": 0, "threshold": 5},
        }

    monkeypatch.setattr(exit_safety_mod, "evaluate_exit_submission_safety", _fake_guard)
    monkeypatch.setattr(exit_engine_mod, "_is_option_quote_stale", lambda pos, now_utc: (False, 0.0, "fresh"))
    monkeypatch.setattr(exit_engine_mod, "_classify_exit_decision", lambda decision: "RUNNER_TRAIL")

    callback_calls = []
    engine.on_exit = lambda pos, decision: callback_calls.append((pos.position_id, decision.quantity)) or {
        "accepted": True,
        "local_order_id": "L-EXIT-001",
        "broker_order_id": "BO-EXIT-001",
        "status": "accepted",
    }

    position_id = (
        "broker-repair-jason@example.com-SMCI260626P00032500"
        if broker_truth_mode == "held"
        else "pos-live-unavailable"
    )
    pos = ManagedPosition(
        ticker="SMCI",
        option_symbol="SMCI260626P00032500",
        side="PUT",
        quantity=3,
        entry_price=1.0,
        underlying_entry=100.0,
        underlying_target=90.0,
        underlying_stop=110.0,
        position_id=position_id,
        client_id="jason@example.com",
        execution_mode="live",
        current_bid=1.2,
        current_ask=1.3,
        current_option_price=1.25,
        current_underlying=99.0,
        quantity_remaining=3,
    )

    decision = ExitDecision(
        action="CLOSE_ALL",
        quantity=3,
        reason="manual test close",
        urgency="HIGH",
        pnl_pct=0.1,
        suggested_limit=1.2,
    )
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}

    if broker_truth_mode == "held":
        result = engine._submit_exit_decision(pos, decision)
    else:
        submit = getattr(APExitEngine, "_AP_EXIT_SUBMIT_ORIGINAL", APExitEngine._submit_exit_decision)
        result = submit(engine, pos, decision)

    assert result is True
    assert callback_calls == [(position_id, 3)]
    assert pos.exit_in_flight is True
    if broker_truth_mode == "unavailable":
        assert any(event.get("reason_code") == "BROKER_TRUTH_UNAVAILABLE" for event in events)
    if guard_error:
        assert calls == {}
    else:
        assert calls["position_id"] == position_id
        assert calls["broker_truth_open_qty"] == (3 if broker_truth_mode == "held" else None)
        assert calls["allow_missing_position_with_broker_truth"] is (broker_truth_mode == "held")


def test_exit_engine_callback_path_does_not_allow_missing_non_repair_position(monkeypatch):
    from ap_exit_engine import APExitEngine, ExitDecision, ManagedPosition
    import ap_exit_engine as exit_engine_mod

    engine = APExitEngine.__new__(APExitEngine)
    engine.client_id = "jason@example.com"
    engine._email = "jason@example.com"
    engine._lock = __import__("threading").Lock()
    engine._thread = None
    engine._running = False
    engine.run_id = "run-1"
    engine.strategy_version = "test"
    engine.git_commit = "test"
    engine.master_control = type("MC", (), {"mode": "live"})()
    engine.on_scale = None
    engine.order_state_machine = None
    engine.osm = None
    engine.broker = MagicMock()
    engine.broker.list_positions.return_value = [
        {"symbol": "SMCI260626P00032500", "quantity": 3},
    ]
    engine._positions = []
    engine._positions_by_id = {}
    engine.hydrate_pending_exit_identity_from_db = lambda pos: False
    engine._emit_exit_event = lambda *args, **kwargs: None
    engine._extract_exit_order_identity = lambda result: {
        "accepted": True,
        "local_order_id": "L-EXIT-001",
        "broker_order_id": "BO-EXIT-001",
        "raw_status": "accepted",
    }
    engine._mark_exit_submitted = lambda current_pos, decision, local_order_id="", broker_order_id="": (
        setattr(current_pos, "exit_in_flight", True),
        setattr(current_pos, "pending_exit_local_order_id", local_order_id),
        setattr(current_pos, "pending_exit_broker_order_id", broker_order_id),
    )

    calls = {}

    def _fake_guard(**kwargs):
        calls.update(kwargs)
        return {
            "blocked": True,
            "reason": "position_missing",
            "position_state": {"blocked": True, "reason": "position_missing"},
            "circuit_breaker": None,
        }

    monkeypatch.setattr(exit_safety_mod, "evaluate_exit_submission_safety", _fake_guard)
    monkeypatch.setattr(exit_engine_mod, "_is_option_quote_stale", lambda pos, now_utc: (False, 0.0, "fresh"))
    monkeypatch.setattr(exit_engine_mod, "_classify_exit_decision", lambda decision: "RUNNER_TRAIL")

    callback_calls = []
    engine.on_exit = lambda pos, decision: callback_calls.append((pos.position_id, decision.quantity)) or {
        "accepted": True,
        "local_order_id": "L-EXIT-001",
        "broker_order_id": "BO-EXIT-001",
        "status": "accepted",
    }

    pos = ManagedPosition(
        ticker="SMCI",
        option_symbol="SMCI260626P00032500",
        side="PUT",
        quantity=3,
        entry_price=1.0,
        underlying_entry=100.0,
        underlying_target=90.0,
        underlying_stop=110.0,
        position_id="pos-live-123",
        client_id="jason@example.com",
        execution_mode="live",
        current_bid=1.2,
        current_ask=1.3,
        current_option_price=1.25,
        current_underlying=99.0,
        quantity_remaining=3,
    )

    decision = ExitDecision(
        action="CLOSE_ALL",
        quantity=3,
        reason="manual test close",
        urgency="HIGH",
        pnl_pct=0.1,
        suggested_limit=1.2,
    )
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}

    result = engine._submit_exit_decision(pos, decision)

    assert result is False
    assert callback_calls == []
    assert calls["position_id"] == "pos-live-123"
    assert calls["allow_missing_position_with_broker_truth"] is False


def test_exit_manager_open_positions_query_includes_execution_mode():
    src = (REPO_ROOT / "ap" / "exit_manager.py").read_text()
    idx = src.find("def get_open_positions():")
    assert idx != -1
    region = src[idx: idx + 500]
    assert "execution_mode" in region


def test_broker_truth_exact_occ_allows_protective_close(monkeypatch, mock_broker):
    monkeypatch.setenv("MAX_EXIT_REJECTIONS_BEFORE_HALT", "5")
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 5},
    )
    mock_broker.list_positions.return_value = [
        {"symbol": "SMCI260626P00032500", "quantity": 1, "account_id": "ACC123"},
    ]
    mock_broker.session.post.return_value = _resp(
        200,
        json_body={"order": {"id": "BO-PROTECT", "status": "open"}},
    )
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-protect",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is True
    assert mock_broker.session.post.call_count == 1
    assert any("UPDATE orders " in sql and "SET meta = COALESCE(meta, '{}'::jsonb)" in sql for sql, _ in fake_conn.queries)


def test_broker_flat_exact_match_blocks_without_broker_post_and_preserves_reconciliation(monkeypatch, mock_broker):
    monkeypatch.setenv("MAX_EXIT_REJECTIONS_BEFORE_HALT", "5")
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 5},
    )
    mock_broker.list_positions.return_value = [
        {"symbol": "SMCI260626P00032500", "quantity": 0, "account_id": "ACC123"},
    ]
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-flat",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is False
    assert result["reason"] == "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
    assert mock_broker.session.post.call_count == 0
    position_updates = [(sql, params) for sql, params in fake_conn.queries if "UPDATE positions" in sql]
    assert position_updates
    assert all("status = 'CLOSED'" not in sql for sql, _ in position_updates)
    assert all("quantity_remaining = 0" not in sql for sql, _ in position_updates)
    assert any("reconciler_manual_close_needed" in str(params) for _, params in position_updates)


def test_broker_flat_exact_match_without_breaker_preserves_reconciliation(monkeypatch, mock_broker):
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )
    mock_broker.list_positions.return_value = [
        {"symbol": "SMCI260626P00032500", "quantity": 0, "account_id": "ACC123"},
    ]
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-flat-no-breaker",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is False
    assert result["reason"] == "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
    assert mock_broker.session.post.call_count == 0
    position_updates = [(sql, params) for sql, params in fake_conn.queries if "UPDATE positions" in sql]
    assert position_updates
    assert all("status = 'CLOSED'" not in sql for sql, _ in position_updates)
    assert all("quantity_remaining = 0" not in sql for sql, _ in position_updates)
    assert any("reconciler_manual_close_needed" in str(params) for _, params in position_updates)


def test_broker_wrong_occ_contract_does_not_override_breaker(monkeypatch, mock_broker):
    """
    Production-shape fix: broker returns a DIFFERENT OCC contract (260703 vs 260626).
    The requested contract (260626) is absent from the snapshot — that means the broker
    confirms qty=0 for 260626 (it closed/expired). Must fire SYNTHETIC_POSITION_STALE_BROKER_FLAT,
    not fall through as "no broker truth."
    OLD expected "exit_circuit_breaker_tripped" — that was the VZ/META bug.
    """
    monkeypatch.setenv("MAX_EXIT_REJECTIONS_BEFORE_HALT", "5")
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 5},
    )
    mock_broker.list_positions.return_value = [
        {"symbol": "SMCI260703P00032500", "quantity": 1, "account_id": "ACC123"},
    ]
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-wrong-occ",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is False
    assert result["reason"] == "SYNTHETIC_POSITION_STALE_BROKER_FLAT", (
        "Broker has a DIFFERENT option (260703) but NOT 260626. "
        "260626 absent from snapshot = broker confirms 260626 is flat. "
        "Must block as stale, not fall through to circuit_breaker_tripped."
    )
    assert mock_broker.session.post.call_count == 0


def test_broker_other_account_does_not_override_breaker(monkeypatch, mock_broker):
    """
    Production-shape fix: broker returns the contract but for a different account
    (OTHER-ACC vs ACC123). After account-filtering, no match for ACC123.
    Absent-for-this-account = broker confirms ACC123 is flat on this contract.
    Must fire SYNTHETIC_POSITION_STALE_BROKER_FLAT, not "no broker truth."
    OLD expected "exit_circuit_breaker_tripped" — that was the bug.
    """
    monkeypatch.setenv("MAX_EXIT_REJECTIONS_BEFORE_HALT", "5")
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 5},
    )
    mock_broker.list_positions.return_value = [
        {"symbol": "SMCI260626P00032500", "quantity": 1, "account_id": "OTHER-ACC"},
    ]
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-other-account",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is False
    assert result["reason"] == "SYNTHETIC_POSITION_STALE_BROKER_FLAT", (
        "Contract is in OTHER-ACC, not ACC123. For ACC123, contract is absent = flat. "
        "Must block as stale, not circuit_breaker_tripped."
    )
    assert mock_broker.session.post.call_count == 0


def test_broker_truth_unavailable_preserves_canonical_exit_liveness(monkeypatch, mock_broker):
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )
    mock_broker.list_positions.side_effect = RuntimeError("positions down")
    mock_broker.session.post.return_value = _resp(
        200,
        json_body={"order": {"id": "BO-LIVE", "status": "open"}},
    )
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-unavailable",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is True
    assert mock_broker.session.post.call_count == 1
    assert any(
        "canonical_exit_submit" in str(params)
        for sql, params in fake_conn.queries
        if "UPDATE orders" in sql
    )
    assert not any(
        "UPDATE positions" in sql and "status = 'CLOSED'" in sql
        for sql, _ in fake_conn.queries
    )


def test_osm_allows_open_position_when_broker_truth_method_is_unavailable(monkeypatch, mock_broker):
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )
    mock_broker.list_positions = None
    mock_broker.session.post.return_value = _resp(
        200,
        json_body={"order": {"id": "BO-LIVE-METHOD-MISSING", "status": "open"}},
    )
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-osm-truth-unavailable",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is True
    assert mock_broker.session.post.call_count == 1
    assert not any("UPDATE positions" in sql and "status = 'CLOSED'" in sql for sql, _ in fake_conn.queries)


def test_requested_qty_greater_than_broker_truth_blocks_no_oversell(monkeypatch, mock_broker):
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row(quantity_remaining=2) if "FROM positions" in sql else {"rejection_count": 0},
    )
    mock_broker.list_positions.return_value = [
        {"symbol": "SMCI260626P00032500", "quantity": 1, "account_id": "ACC123"},
    ]
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-insufficient",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=2,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is False
    assert result["reason"] == "EXIT_BLOCKED_BROKER_QTY_INSUFFICIENT"
    assert mock_broker.session.post.call_count == 0
    assert not any(
        "UPDATE positions" in sql and "status = 'CLOSED'" in sql
        for sql, _ in fake_conn.queries
    )


def test_empty_positions_preserves_position_for_manual_fill_reconciliation(monkeypatch, mock_broker):
    """
    Production-shape fix (formerly 'test_contract_not_matched_does_not_mark_position_closed').
    Empty positions list = broker confirms ALL positions flat = this contract is flat.
    OSM must fire SYNTHETIC_POSITION_STALE_BROKER_FLAT and persist only the
    reconciliation marker; exact external fill adoption remains separate.
    OLD expected circuit_breaker_tripped + NO position close. That was the VZ/META bug.
    """
    monkeypatch.setenv("MAX_EXIT_REJECTIONS_BEFORE_HALT", "5")
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 5},
    )
    mock_broker.list_positions.return_value = []
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-no-match",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is False
    assert result["reason"] == "SYNTHETIC_POSITION_STALE_BROKER_FLAT", (
        "Empty broker positions list MUST block with SYNTHETIC_POSITION_STALE_BROKER_FLAT. "
        "It must NOT fall through as 'no broker truth' and trigger circuit_breaker_tripped. "
        "That fallthrough was the VZ/META production shape bug."
    )
    assert mock_broker.session.post.call_count == 0
    position_updates = [(sql, params) for sql, params in fake_conn.queries if "UPDATE positions" in sql]
    assert position_updates
    assert all("status = 'CLOSED'" not in sql for sql, _ in position_updates)
    assert all("quantity_remaining = 0" not in sql for sql, _ in position_updates)
    assert any("reconciler_manual_close_needed" in str(params) for _, params in position_updates)


def test_duplicate_exit_guard_still_blocks_before_override(monkeypatch, mock_broker):
    monkeypatch.setenv("MAX_EXIT_REJECTIONS_BEFORE_HALT", "5")
    _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 5},
    )
    mock_broker.list_positions.return_value = [
        {"symbol": "SMCI260626P00032500", "quantity": 1, "account_id": "ACC123"},
    ]

    class _ExistingExitOSM(_MockOSM):
        def _get_active_exit_order(self, position_id):
            return {"local_order_id": "L-EXISTING", "status": "SUBMITTED", "broker_order_id": "BO-EXISTING"}

    osm = _ExistingExitOSM()
    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-duplicate",
        contract="SMCI260626P00032500",
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is False
    assert result["error"].startswith("active_exit_already_exists")
    assert mock_broker.session.post.call_count == 0


# ---------------------------------------------------------------------------
# PR #566 containment: autonomous recovery must preserve exact authority.
# ---------------------------------------------------------------------------

_RECOVERY_OCC = "IWM260901P00293000"
_RECOVERY_OTHER_OCC = "IWM260901P00294000"


class _RecoveryBroker:
    def __init__(self, *, orders=None, positions=None, get_order_payload=None,
                 order_exc=None, position_exc=None):
        self._orders = orders
        self._positions = positions
        self._get_order_payload = get_order_payload
        self._order_exc = order_exc
        self._position_exc = position_exc
        self.account_id = "acct-566"
        self.list_orders_calls = 0
        self.list_positions_calls = 0
        self.get_order_calls = []
        self.cancel_calls = []
        self.post_calls = 0

    def list_orders(self, status="open"):
        self.list_orders_calls += 1
        if self._order_exc is not None:
            raise self._order_exc
        return self._orders

    def list_positions(self):
        self.list_positions_calls += 1
        if self._position_exc is not None:
            raise self._position_exc
        return self._positions

    def get_order(self, broker_order_id):
        self.get_order_calls.append(broker_order_id)
        if isinstance(self._get_order_payload, BaseException):
            raise self._get_order_payload
        return self._get_order_payload

    def cancel_order(self, broker_order_id):
        self.cancel_calls.append(broker_order_id)
        return {"ok": True, "status": "canceled", "id": broker_order_id}


class _RawRecoveryBroker(_RecoveryBroker):
    def __init__(self, *, positions_payload, orders_payload=None):
        super().__init__(orders=[], positions=[], position_exc=RuntimeError("legacy path must not run"))
        self.cfg = SimpleNamespace(account_id="acct-566")
        self.positions_payload = positions_payload
        self.orders_payload = orders_payload or {"orders": {"order": []}}
        self.raw_get_calls = []

    def _get(self, path, params=None):
        self.raw_get_calls.append(path)
        if path.endswith("/positions"):
            return self.positions_payload
        if path.endswith("/orders"):
            return self.orders_payload
        raise AssertionError(path)


class _RecoveryHooks:
    def __init__(self):
        self.open_calls = []
        self.closed_calls = []
        self.partial_calls = []
        self.replacement_calls = []

    def set_pending_exit_order(self, position_id, **kwargs):
        self.open_calls.append((position_id, kwargs))

    def mark_position_closed(self, position_id, **kwargs):
        self.closed_calls.append((position_id, kwargs))

    def note_partial_exit_fill(self, position_id, **kwargs):
        self.partial_calls.append((position_id, kwargs))

    def mark_exit_replacement_safe(self, position_id, **kwargs):
        self.replacement_calls.append((position_id, kwargs))


def _recovery_position(
    *,
    contract=_RECOVERY_OCC,
    pending_broker_id="",
    remaining=2,
    pending_qty=None,
    pending_filled=0,
):
    if pending_qty is None:
        pending_qty = remaining
    return SimpleNamespace(
        position_id="pos-recovery-566",
        closed=False,
        option_symbol=contract,
        contract="",
        symbol="IWM",
        pending_exit_local_order_id="local-exit-566",
        pending_exit_broker_order_id=pending_broker_id,
        pending_exit_qty=pending_qty,
        pending_exit_filled_qty=pending_filled,
        quantity_remaining=remaining,
        contracts=remaining,
        client_id="jason@example.com",
        execution_mode="live",
    )


def _recovery_open_order(contract=_RECOVERY_OCC, broker_id="broker-exit-566", **extra):
    return {
        "id": broker_id,
        "symbol": "IWM",
        "option_symbol": contract,
        "side": "sell_to_close",
        "status": "open",
        "quantity": 2,
        **extra,
    }


def _recovery_held_position(contract=_RECOVERY_OCC, quantity=2):
    return {"symbol": contract, "quantity": quantity}


def _assert_no_recovery_mutation(broker, hooks):
    assert broker.cancel_calls == []
    assert broker.post_calls == 0
    assert hooks.closed_calls == []
    assert hooks.replacement_calls == []


def test_pr566_recovery_flat_snapshot_requires_exact_external_fill():
    broker = _RecoveryBroker(orders=[], positions=[])
    hooks = _RecoveryHooks()

    result = recover_exit_position(
        _recovery_position(), broker=broker, exit_engine=hooks
    )

    assert result.action == "NOOP"
    assert result.reason == "broker_flat_requires_exact_external_fill"
    _assert_no_recovery_mutation(broker, hooks)


def test_pr566_recovery_position_transport_failure_is_hold_not_flat():
    broker = _RecoveryBroker(
        orders=[], positions=[], position_exc=RuntimeError("positions unavailable")
    )
    hooks = _RecoveryHooks()

    result = recover_exit_position(
        _recovery_position(), broker=broker, exit_engine=hooks
    )

    assert result.action == "NOOP"
    assert result.reason == "broker_position_truth_unavailable"
    _assert_no_recovery_mutation(broker, hooks)


def test_pr566_raw_tradier_position_path_bypasses_laundered_legacy_adapter():
    broker = _RawRecoveryBroker(
        positions_payload={"positions": {"position": []}}
    )
    hooks = _RecoveryHooks()

    result = recover_exit_position(
        _recovery_position(), broker=broker, exit_engine=hooks
    )

    assert result.reason == "broker_flat_requires_exact_external_fill"
    assert broker.list_positions_calls == 0
    assert any(path.endswith("/positions") for path in broker.raw_get_calls)
    _assert_no_recovery_mutation(broker, hooks)


def test_pr566_incomplete_empty_positions_container_is_unknown_not_flat():
    broker = _RawRecoveryBroker(positions_payload={"positions": {}})

    truth = exit_safety_mod.resolve_exit_broker_truth(
        broker=broker,
        client_id="jason@example.com",
        contract=_RECOVERY_OCC,
    )

    assert truth["broker_truth_open_qty"] is None
    assert truth["is_fresh_exact"] is False
    assert truth["audit"]["snapshot_status"] == "broker_positions_malformed"
    assert broker.list_positions_calls == 0


def test_pr566_authoritative_list_orders_wins_over_raw_endpoint_fallback():
    broker = _RawRecoveryBroker(
        positions_payload={
            "positions": {
                "position": [{"symbol": _RECOVERY_OCC, "quantity": 2}]
            }
        },
        orders_payload={"orders": {"order": []}},
    )
    broker._orders = [
        _recovery_open_order(
            broker_id="authoritative-open",
            tag=canonical_broker_submit_key("local-exit-566"),
        )
    ]
    hooks = _RecoveryHooks()

    result = recover_exit_position(
        _recovery_position(), broker=broker, exit_engine=hooks
    )

    assert result.action == "RECOVERED_BROKER_ID"
    assert result.broker_order_id == "authoritative-open"
    assert broker.list_orders_calls == 1
    assert broker.raw_get_calls == []
    _assert_no_recovery_mutation(broker, hooks)


def test_pr566_unrelated_equity_order_does_not_poison_option_recovery():
    broker = _RecoveryBroker(
        orders=[
            {
                "id": "unrelated-equity-order",
                "symbol": "AAPL",
                "side": "buy",
                "status": "open",
                "quantity": 1,
            },
            _recovery_open_order(
                broker_id="option-exit-order",
                tag=canonical_broker_submit_key("local-exit-566"),
            ),
        ],
        positions=[_recovery_held_position()],
    )
    hooks = _RecoveryHooks()

    result = recover_exit_position(
        _recovery_position(), broker=broker, exit_engine=hooks
    )

    assert result.action == "RECOVERED_BROKER_ID"
    assert result.broker_order_id == "option-exit-order"
    _assert_no_recovery_mutation(broker, hooks)


@pytest.mark.parametrize("quantity", [True, False, 1.5, "nan", "unknown", -1])
def test_pr566_malformed_position_quantity_cannot_authorize_recovery(quantity):
    broker = _RecoveryBroker(
        orders=[], positions=[_recovery_held_position(quantity=quantity)]
    )
    hooks = _RecoveryHooks()

    result = recover_exit_position(
        _recovery_position(), broker=broker, exit_engine=hooks
    )

    assert result.action == "NOOP"
    assert result.reason == "broker_position_truth_malformed"
    _assert_no_recovery_mutation(broker, hooks)


def test_pr566_conflicting_position_identity_is_not_flat():
    broker = _RecoveryBroker(
        orders=[],
        positions=[{
            "symbol": _RECOVERY_OCC,
            "option_symbol": _RECOVERY_OTHER_OCC,
            "quantity": 1,
        }],
    )
    hooks = _RecoveryHooks()

    result = recover_exit_position(
        _recovery_position(), broker=broker, exit_engine=hooks
    )

    assert result.action == "NOOP"
    assert result.reason == "broker_position_truth_malformed"
    _assert_no_recovery_mutation(broker, hooks)


def test_pr566_pending_lookup_failure_does_not_fallback_to_order_scan():
    broker = _RecoveryBroker(
        orders=[_recovery_open_order(_RECOVERY_OCC, "different-order")],
        positions=[_recovery_held_position()],
        get_order_payload=RuntimeError("order lookup unavailable"),
    )
    hooks = _RecoveryHooks()

    result = recover_exit_position(
        _recovery_position(pending_broker_id="pending-order"),
        broker=broker,
        exit_engine=hooks,
    )

    assert result.action == "NOOP"
    assert result.reason == "broker_order_truth_unavailable"
    assert broker.get_order_calls == ["pending-order"]
    assert broker.list_orders_calls == 0
    _assert_no_recovery_mutation(broker, hooks)


def test_pr566_filled_partial_requires_exact_fill_fields_and_preserves_remaining():
    broker = _RecoveryBroker(
        positions=[_recovery_held_position()],
        get_order_payload={
            "id": "filled-order",
            "symbol": "IWM",
            "option_symbol": _RECOVERY_OCC,
            "side": "sell_to_close",
            "status": "filled",
            "filled_qty": 1,
            "avg_fill_price": 1.25,
            "quantity": 1,
        },
    )
    hooks = _RecoveryHooks()

    result = recover_exit_position(
        _recovery_position(pending_broker_id="filled-order"),
        broker=broker,
        exit_engine=hooks,
    )

    assert result.action == "PARTIAL_FILL_APPLIED"
    assert len(hooks.partial_calls) == 1
    assert hooks.partial_calls[0][1]["qty_filled"] == 1
    assert hooks.partial_calls[0][1]["fill_price"] == 1.25
    assert hooks.partial_calls[0][1]["cumulative_filled"] == 1
    assert hooks.closed_calls == []
    assert hooks.replacement_calls == []
    assert broker.cancel_calls == []


def test_pr566_cumulative_fill_restart_applies_only_delta_once():
    broker = _RecoveryBroker(
        positions=[_recovery_held_position()],
        get_order_payload={
            "id": "filled-order-restart",
            "symbol": "IWM",
            "option_symbol": _RECOVERY_OCC,
            "side": "sell_to_close",
            "status": "filled",
            "quantity": 2,
            "exec_quantity": 2,
            "avg_fill_price": 1.25,
        },
    )
    hooks = _RecoveryHooks()
    position = _recovery_position(
        pending_broker_id="filled-order-restart",
        remaining=1,
        pending_qty=2,
        pending_filled=1,
    )

    first = recover_exit_position(position, broker=broker, exit_engine=hooks)
    second = recover_exit_position(position, broker=broker, exit_engine=hooks)

    assert first.action == "MARKED_CLOSED"
    assert first.details["delta_qty"] == 1
    assert len(hooks.closed_calls) == 1
    assert hooks.closed_calls[0][1]["qty_filled"] == 1
    assert hooks.closed_calls[0][1]["cumulative_filled"] == 2
    assert hooks.partial_calls == []
    assert second.action == "NOOP"
    assert second.reason == "duplicate_exit_fill_ignored"
    assert len(hooks.closed_calls) == 1
    assert hooks.replacement_calls == []
    assert broker.cancel_calls == []


def test_pr566_filled_recovery_delegates_to_canonical_osm_and_dedupes():
    broker = _RecoveryBroker(
        positions=[_recovery_held_position()],
        get_order_payload={
            "id": "filled-order-osm",
            "symbol": "IWM",
            "option_symbol": _RECOVERY_OCC,
            "side": "sell_to_close",
            "status": "filled",
            "quantity": 2,
            "exec_quantity": 2,
            "avg_fill_price": 1.25,
        },
    )
    hooks = _RecoveryHooks()
    position = _recovery_position(
        pending_broker_id="filled-order-osm",
        remaining=1,
        pending_qty=2,
        pending_filled=1,
    )

    class _CanonicalOSM:
        def __init__(self):
            self.calls = []

        def transition(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return True

    osm = _CanonicalOSM()

    first = recover_exit_position(position, broker=broker, exit_engine=hooks, osm=osm)
    second = recover_exit_position(position, broker=broker, exit_engine=hooks, osm=osm)

    assert first.action == "MARKED_CLOSED"
    assert first.reason == "broker_order_filled_via_canonical_osm"
    assert len(osm.calls) == 1
    assert osm.calls[0][0] == ("local-exit-566", "EXIT_FILLED")
    assert osm.calls[0][1] == {
        "broker_order_id": "filled-order-osm",
        "filled_qty": 2,
        "fill_price": 1.25,
    }
    assert hooks.closed_calls == []
    assert hooks.partial_calls == []
    assert second.reason == "duplicate_exit_fill_ignored"
    assert len(osm.calls) == 1


def test_pr566_same_contract_wrong_tag_is_not_adopted():
    broker = _RecoveryBroker(
        orders=[_recovery_open_order(tag=canonical_broker_submit_key("other-exit-566"))],
        positions=[_recovery_held_position()],
    )
    hooks = _RecoveryHooks()

    result = recover_exit_position(_recovery_position(), broker=broker, exit_engine=hooks)

    assert result.action == "NOOP"
    assert result.reason == "broker_order_owner_unproven"
    assert hooks.open_calls == []
    _assert_no_recovery_mutation(broker, hooks)


def test_pr566_conflicting_filled_order_quantity_requires_hold():
    broker = _RecoveryBroker(
        positions=[_recovery_held_position()],
        get_order_payload={
            "id": "conflicting-filled-order",
            "symbol": "IWM",
            "option_symbol": _RECOVERY_OCC,
            "side": "sell_to_close",
            "status": "filled",
            "quantity": 1,
            "exec_quantity": 2,
            "avg_fill_price": 1.25,
        },
    )
    hooks = _RecoveryHooks()

    result = recover_exit_position(
        _recovery_position(pending_broker_id="conflicting-filled-order"),
        broker=broker,
        exit_engine=hooks,
    )

    assert result.action == "NOOP"
    assert result.reason == "broker_order_truth_malformed"
    assert hooks.open_calls == []
    assert hooks.partial_calls == []
    _assert_no_recovery_mutation(broker, hooks)


def test_pr566_filled_limit_price_without_execution_price_is_hold():
    broker = _RecoveryBroker(
        positions=[_recovery_held_position()],
        get_order_payload={
            "id": "filled-limit-only",
            "symbol": "IWM",
            "option_symbol": _RECOVERY_OCC,
            "side": "sell_to_close",
            "status": "filled",
            "quantity": 2,
            "price": 1.25,
        },
    )
    hooks = _RecoveryHooks()

    result = recover_exit_position(
        _recovery_position(pending_broker_id="filled-limit-only"),
        broker=broker,
        exit_engine=hooks,
    )

    assert result.action == "NOOP"
    assert result.reason == "broker_order_truth_malformed"
    _assert_no_recovery_mutation(broker, hooks)


def test_pr566_terminal_pending_order_plus_flat_snapshot_stays_nonterminal():
    broker = _RecoveryBroker(
        orders=[],
        positions=[],
        get_order_payload={
            "id": "canceled-order",
            "symbol": "IWM",
            "option_symbol": _RECOVERY_OCC,
            "side": "sell_to_close",
            "status": "canceled",
            "quantity": 2,
        },
    )
    hooks = _RecoveryHooks()

    result = recover_exit_position(
        _recovery_position(pending_broker_id="canceled-order"),
        broker=broker,
        exit_engine=hooks,
    )

    assert result.action == "NOOP"
    assert result.reason == "broker_flat_requires_exact_external_fill"
    _assert_no_recovery_mutation(broker, hooks)


def test_pr566_missing_broker_id_does_not_adopt_unowned_same_contract_order():
    broker = _RecoveryBroker(
        orders=[_recovery_open_order()],
        positions=[_recovery_held_position()],
    )
    hooks = _RecoveryHooks()

    result = recover_exit_position(
        _recovery_position(), broker=broker, exit_engine=hooks
    )

    assert result.action == "NOOP"
    assert result.reason == "broker_order_owner_unproven"
    assert hooks.open_calls == []
    _assert_no_recovery_mutation(broker, hooks)


def test_pr566_missing_broker_id_with_held_position_defers_replacement():
    broker = _RecoveryBroker(
        orders=[], positions=[_recovery_held_position()]
    )
    hooks = _RecoveryHooks()

    result = recover_exit_position(
        _recovery_position(), broker=broker, exit_engine=hooks
    )

    assert result.action == "NOOP"
    assert result.reason == "replacement_authorization_deferred"
    _assert_no_recovery_mutation(broker, hooks)


def test_pr566_malformed_order_container_cannot_be_negative_proof():
    broker = _RecoveryBroker(
        orders={"orders": {"unexpected": []}},
        positions=[_recovery_held_position()],
    )
    hooks = _RecoveryHooks()

    result = recover_exit_position(
        _recovery_position(), broker=broker, exit_engine=hooks
    )

    assert result.action == "NOOP"
    assert result.reason == "broker_order_truth_malformed"
    assert broker.list_positions_calls == 0
    _assert_no_recovery_mutation(broker, hooks)


def test_pr566_real_tradier_tag_allows_single_open_order_adoption():
    broker = _RecoveryBroker(
        orders=[_recovery_open_order(
            broker_id="owned-order",
            tag=canonical_broker_submit_key("local-exit-566"),
        )],
        positions=[_recovery_held_position()],
    )
    hooks = _RecoveryHooks()

    result = recover_exit_position(
        _recovery_position(), broker=broker, exit_engine=hooks
    )

    assert result.action == "RECOVERED_BROKER_ID"
    assert result.broker_order_id == "owned-order"
    assert len(hooks.open_calls) == 1
    _assert_no_recovery_mutation(broker, hooks)


def test_pr566_restart_marker_is_revisited_without_synthetic_close():
    position = _recovery_position()
    position.protective_monitoring_state = "BROKER_FLAT_CLOSE_PENDING"
    position.exit_in_flight = False

    class _Engine(_RecoveryHooks):
        _email = "jason@example.com"
        master_control = SimpleNamespace(mode="live")

        def active_positions(self):
            return [position]

    broker = _RecoveryBroker(orders=[], positions=[])
    engine = _Engine()

    actions = recover_exit_engine(engine, broker=broker)

    assert len(actions) == 1
    assert actions[0].reason == "broker_flat_requires_exact_external_fill"
    assert engine.closed_calls == []
    assert engine.replacement_calls == []
    assert broker.cancel_calls == []


def test_pr566_padded_occ_position_identity_is_canonicalized():
    broker = _RecoveryBroker(
        orders=[], positions=[_recovery_held_position()]
    )
    padded = _recovery_position(contract="IWM  260901P00293000")

    result = recover_exit_position(padded, broker=broker, exit_engine=_RecoveryHooks())

    assert result.reason == "replacement_authorization_deferred"
