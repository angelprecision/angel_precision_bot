from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))

from ap import exit_safety as exit_safety_mod  # noqa: E402
from ap import order_state_machine as osm_mod  # noqa: E402
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

    def rewrite_exit_submit_intent_payload(self, local_order_id, **kwargs):
        if not self.exit_row or self.exit_row["local_order_id"] != local_order_id:
            return False
        if self.exit_row["meta"].get("broker_submit_payload_hash") != kwargs["prior_payload_hash"]:
            return False
        self.exit_row["meta"]["broker_submit_payload_hash"] = kwargs["replacement_payload_hash"]
        self.exit_row["meta"]["protective_takeover_replacement_qty"] = kwargs["replacement_qty"]
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
    broker.list_orders.return_value = []
    # The protective takeover requires the authoritative order capability;
    # keep this test double's strict method tied to the legacy-configured
    # response so existing scenarios exercise the same inventory sequence.
    broker.list_orders_strict = broker.list_orders
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
    mock_broker.list_positions.return_value = [
        {"symbol": "SMCI260626P00032500", "quantity": 1, "side": "PUT", "account_id": "ACC123"}
    ]
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


def test_now_incident_cancels_exact_protective_stop_before_replacement(monkeypatch, mock_broker):
    """Fail-first replay of LIVE NOW stop 143387714 reserving the only contract."""
    contract = "NOW260828P00122000"
    _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )
    mock_broker.list_positions.return_value = [
        {"symbol": contract, "quantity": 1, "side": "PUT", "account_id": "ACC123"}
    ]
    mock_broker.list_orders.side_effect = [[
        {
            "id": "143387714",
            "status": "open",
            "class": "option",
            "type": "stop",
            "side": "sell_to_close",
            "option_symbol": contract,
            "quantity": 1,
            "exec_quantity": 0,
            "duration": "gtc",
            "account_id": "ACC123",
        }
    ], []]
    mock_broker.cancel_order.return_value = {
        "ok": True,
        "status": "canceled",
        "broker_order_id": "143387714",
    }
    mock_broker.get_order.return_value = {
        "id": "143387714",
        "status": "canceled",
        "option_symbol": contract,
        "side": "sell_to_close",
        "quantity": 1,
        "exec_quantity": 0,
    }
    mock_broker.session.post.return_value = _resp(
        200, json_body={"order": {"id": "CANONICAL-EXIT-1", "status": "open"}}
    )

    result = _MockOSM().submit_exit(
        broker=mock_broker,
        position_id="position-now-live-1",
        contract=contract,
        symbol="NOW",
        direction="PUT",
        qty=1,
        limit_price=1.08,
        execution_mode="live",
    )

    assert result["ok"] is True
    mock_broker.cancel_order.assert_called_once_with("143387714")
    mock_broker.get_order.assert_called_once_with("143387714")
    assert mock_broker.session.post.call_count == 1
    assert mock_broker.session.post.call_args.kwargs["data"]["quantity"] == 1


def test_partial_protective_fill_rewrites_durable_payload_and_posts_residual(monkeypatch, mock_broker):
    contract = "NOW260828P00122000"
    _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row(quantity_remaining=2) if "FROM positions" in sql else {"rejection_count": 0},
    )
    mock_broker.list_positions.side_effect = [
        [{"symbol": contract, "quantity": 2, "account_id": "ACC123"}],
        [{"symbol": contract, "quantity": 2, "account_id": "ACC123"}],
        [{"symbol": contract, "quantity": 1, "account_id": "ACC123"}],
    ]
    mock_broker.list_orders.side_effect = [[{
        "id": "143387714", "status": "open", "type": "stop",
        "side": "sell_to_close", "option_symbol": contract,
        "quantity": 2, "exec_quantity": 0, "account_id": "ACC123",
    }], []]
    mock_broker.cancel_order.return_value = {"ok": True, "status": "canceled"}
    mock_broker.get_order.return_value = {
        "id": "143387714", "status": "filled", "type": "stop",
        "side": "sell_to_close", "option_symbol": contract,
        "quantity": 2, "exec_quantity": 1,
    }
    mock_broker.session.post.return_value = _resp(
        200, json_body={"order": {"id": "CANONICAL-RESIDUAL-1", "status": "open"}}
    )
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker, position_id="position-now-live-2", contract=contract,
        symbol="NOW", direction="PUT", qty=2, limit_price=1.08,
        execution_mode="live",
    )

    assert result["ok"] is True
    assert mock_broker.cancel_order.call_count == 1
    assert mock_broker.session.post.call_count == 1
    assert mock_broker.session.post.call_args.kwargs["data"]["quantity"] == 1
    assert osm.exit_row["qty"] == 2
    assert osm.exit_row["meta"]["protective_takeover_replacement_qty"] == 1


def test_repeated_exit_tick_keeps_one_takeover_owner_and_one_post(monkeypatch, mock_broker):
    contract = "NOW260828P00122000"
    _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )
    mock_broker.list_positions.return_value = [
        {"symbol": contract, "quantity": 1, "account_id": "ACC123"}
    ]
    mock_broker.list_orders.side_effect = [[{
        "id": "143387714", "status": "open", "type": "stop",
        "side": "sell_to_close", "option_symbol": contract,
        "quantity": 1, "exec_quantity": 0, "account_id": "ACC123",
    }], []]
    mock_broker.cancel_order.return_value = {"ok": True, "status": "canceled"}
    mock_broker.get_order.return_value = {
        "id": "143387714", "status": "canceled", "type": "stop",
        "side": "sell_to_close", "option_symbol": contract,
        "quantity": 1, "exec_quantity": 0,
    }
    mock_broker.session.post.return_value = _resp(
        200, json_body={"order": {"id": "CANONICAL-EXIT-ONCE", "status": "open"}}
    )
    osm = _MockOSM()
    kwargs = dict(
        broker=mock_broker, position_id="position-now-live-repeat", contract=contract,
        symbol="NOW", direction="PUT", qty=1, limit_price=1.08,
        execution_mode="live",
    )

    first = osm.submit_exit(**kwargs)
    second = osm.submit_exit(**kwargs)

    assert first["ok"] is True
    assert second["ok"] is False
    assert mock_broker.cancel_order.call_count == 1
    assert mock_broker.session.post.call_count == 1


def test_restart_during_takeover_cannot_reclaim_submit_intent_or_mutate_broker(monkeypatch, mock_broker):
    contract = "NOW260828P00122000"
    _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )

    class _RestartedOSM(_MockOSM):
        def persist_exit_submit_intent(self, local_order_id, **kwargs):
            return False

    osm = _RestartedOSM()
    osm.exit_row = {
        "local_order_id": "L-EXIT-RESTART",
        "client_id": osm.client_id,
        "position_id": "position-now-live-restart",
        "kind": "EXIT",
        "status": "EXIT_REQUESTED",
        "execution_mode": "live",
        "contract": contract,
        "qty": 1,
        "broker_order_id": "",
        "submitted_ts": None,
        "meta": {
            "submit_intent_at": "2026-08-26T14:00:00+00:00",
            "current_owner": "broker_submit:L-EXIT-RESTART",
        },
    }
    result = osm.submit_exit(
        broker=mock_broker, position_id="position-now-live-restart", contract=contract,
        symbol="NOW", direction="PUT", qty=1, limit_price=1.08,
        execution_mode="live", local_order_id="L-EXIT-RESTART",
    )

    assert result["ok"] is False
    assert result["error"] == "EXIT_SUBMIT_INTENT_FENCE_LOST"
    assert mock_broker.cancel_order.call_count == 0
    assert mock_broker.session.post.call_count == 0


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
    assert result["reason"] == "exit_circuit_breaker_tripped"
    assert mock_broker.session.post.call_count == 0


def test_exit_circuit_breaker_trips_at_threshold_for_error_broker_rejects(monkeypatch, mock_broker):
    monkeypatch.setenv("MAX_EXIT_REJECTIONS_BEFORE_HALT", "5")
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 5},
    )
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

    order_sql = next(sql for sql, _params in fake_conn.queries if "FROM orders" in sql)

    assert "status = 'REJECTED'" in order_sql
    assert "status = 'ERROR'" in order_sql
    assert "COALESCE(last_error, '') ILIKE %s" in order_sql
    assert result["ok"] is False
    assert result["reason"] == "exit_circuit_breaker_tripped"
    assert mock_broker.session.post.call_count == 0


def test_circuit_breaker_override_does_not_bypass_unresolved_protective_stop(monkeypatch, mock_broker):
    """Broker-open truth may override the rejection gate, never takeover safety."""
    monkeypatch.setenv("MAX_EXIT_REJECTIONS_BEFORE_HALT", "5")
    _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 5},
    )
    contract = "NOW260828P00122000"
    stop = {
        "id": "143387714",
        "status": "open",
        "class": "option",
        "type": "stop",
        "side": "sell_to_close",
        "option_symbol": contract,
        "quantity": 1,
        "exec_quantity": 0,
        "duration": "gtc",
        "account_id": "ACC123",
    }
    mock_broker.list_positions.return_value = [
        {"symbol": contract, "quantity": 1, "side": "PUT", "account_id": "ACC123"}
    ]
    mock_broker.list_orders.return_value = [stop]
    mock_broker.cancel_order.return_value = {"ok": True, "status": "canceled"}
    mock_broker.get_order.return_value = dict(stop)
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=mock_broker,
        position_id="pos-breaker-protective",
        contract=contract,
        symbol="NOW",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result["ok"] is False
    assert result["error"] == "EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN"
    mock_broker.cancel_order.assert_called_once_with("143387714")
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
    mock_broker.list_positions.return_value = [
        {"symbol": "SMCI260626P00032500", "quantity": 1, "side": "PUT", "account_id": "ACC123"}
    ]
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
    assert result["reason"] == "exit_circuit_breaker_tripped"
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


def test_exit_engine_callback_path_allows_broker_repair_position_with_broker_truth(monkeypatch):
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

    pos = ManagedPosition(
        ticker="SMCI",
        option_symbol="SMCI260626P00032500",
        side="PUT",
        quantity=3,
        entry_price=1.0,
        underlying_entry=100.0,
        underlying_target=90.0,
        underlying_stop=110.0,
        position_id="broker-repair-jason@example.com-SMCI260626P00032500",
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

    assert result is True
    assert callback_calls == [("broker-repair-jason@example.com-SMCI260626P00032500", 3)]
    assert calls["position_id"] == "broker-repair-jason@example.com-SMCI260626P00032500"
    assert calls["broker_truth_open_qty"] == 3
    assert calls["allow_missing_position_with_broker_truth"] is True


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


def test_terminal_no_order_stop_marker_keeps_canonical_exit_eligible(monkeypatch, mock_broker):
    """A broker-proven rejected optional stop must not fence the real exit."""
    contract = "NOW260828P00122000"
    _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )
    mock_broker.list_positions.return_value = [
        {"symbol": contract, "quantity": 1, "side": "PUT", "account_id": "ACC123"}
    ]
    mock_broker.list_orders.return_value = []
    mock_broker.session.post.return_value = _resp(
        200,
        json_body={"order": {"id": "CANONICAL-EXIT-REJECTED-STOP", "status": "open"}},
    )

    class _RejectedStopOSM(_MockOSM):
        def get_orders_for_position(self, position_id):
            return [
                {
                    "position_id": position_id,
                    "kind": "ENTRY",
                    "client_id": self.client_id,
                    "contract": contract,
                    "execution_mode": "live",
                    "meta": {
                        "protective_order": {
                            "protective_order_state": "TERMINAL_NO_ORDER",
                            "protective_broker_order_id": None,
                            "protective_contract": contract,
                            "protective_source": "standing_stop",
                            "execution_mode": "live",
                            "client_id": self.client_id,
                        }
                    },
                }
            ]

    result = _RejectedStopOSM().submit_exit(
        broker=mock_broker,
        position_id="position-now-live-rejected-stop",
        contract=contract,
        symbol="NOW",
        direction="PUT",
        qty=1,
        limit_price=1.08,
        execution_mode="live",
    )

    assert result["ok"] is True
    assert mock_broker.cancel_order.call_count == 0
    assert mock_broker.list_positions.call_count >= 1
    assert mock_broker.list_orders_strict.call_count >= 1
    assert mock_broker.session.post.call_count == 1
    assert mock_broker.session.post.call_args.kwargs["data"]["side"] == "sell_to_close"
    assert mock_broker.session.post.call_args.kwargs["data"]["quantity"] == 1


def test_broker_flat_exact_match_blocks_without_broker_post_and_marks_stale(monkeypatch, mock_broker):
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
    assert any("UPDATE positions" in sql and "status = 'CLOSED'" in sql for sql, _ in fake_conn.queries)


def test_broker_flat_exact_match_blocks_even_without_circuit_breaker_trip(monkeypatch, mock_broker):
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
    assert any("UPDATE positions" in sql and "status = 'CLOSED'" in sql for sql, _ in fake_conn.queries)


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


def test_broker_truth_unavailable_preserves_original_breaker(monkeypatch, mock_broker):
    monkeypatch.setenv("MAX_EXIT_REJECTIONS_BEFORE_HALT", "5")
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 5},
    )
    mock_broker.list_positions.side_effect = RuntimeError("positions down")
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

    assert result["ok"] is False
    assert result["reason"] == "exit_circuit_breaker_tripped"
    assert mock_broker.session.post.call_count == 0
    assert not any(
        "UPDATE positions" in sql and "status = 'CLOSED'" in sql
        for sql, _ in fake_conn.queries
    )


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


def test_empty_positions_marks_stale_position_closed(monkeypatch, mock_broker):
    """
    Production-shape fix (formerly 'test_contract_not_matched_does_not_mark_position_closed').
    Empty positions list = broker confirms ALL positions flat = this contract is flat.
    OSM must fire SYNTHETIC_POSITION_STALE_BROKER_FLAT and mark position CLOSED.
    OLD expected circuit_breaker_tripped + NO position close. That was the VZ/META bug.
    NEW (correct) behavior: stale position gets marked CLOSED to stop repeat-fire loop.
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
    # Position row must be marked CLOSED to prevent repeat-fire loop
    assert any(
        "UPDATE positions" in sql and "'CLOSED'" in sql
        for sql, _ in fake_conn.queries
    ), "Empty broker snapshot must mark position CLOSED to stop the repeat-fire loop"


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
