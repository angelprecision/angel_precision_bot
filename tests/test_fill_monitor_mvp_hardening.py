from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
os.environ.setdefault("AP_ENV", "test")

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


class _Resp:
    status_code = 200
    text = ""

    def json(self):
        return {"quotes": {"quote": {"last": 99.25}}}


class _Session:
    def __init__(self):
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, params, headers, timeout))
        return _Resp()


class _Broker:
    def __init__(self, raw):
        self.raw = raw
        self.cfg = type("Cfg", (), {"base_url": "https://execution.example"})()
        self.session = _Session()

    def get_order(self, broker_order_id):
        payload = dict(self.raw)
        payload.setdefault("id", broker_order_id)
        return payload

    def get_quote(self, symbol):
        return {"last": 0}


class _DataBroker:
    def __init__(self):
        self.cfg = type("Cfg", (), {"base_url": "https://data.example"})()
        self.session = _Session()

    def get_quote(self, symbol):
        return {"last": 101.5}


class _PM:
    def __init__(self):
        self.kwargs = None

    def open_position(self, **kwargs):
        self.kwargs = kwargs
        return "pos-1"


class _ExitEngine:
    def __init__(self):
        self.added = []

    def add_position(self, mp):
        self.added.append(mp)


def _base_order(**overrides):
    order = {
        "client_id": "client@example.com",
        "local_order_id": "ord-1",
        "broker_order_id": "brk-1",
        "kind": "ENTRY",
        "symbol": "AAPL",
        "contract": "AAPL260626P00195000",
        "direction": None,
        "qty": 1,
        "limit_price": 1.20,
        "reserved_cost": 120.0,
        "status": "ACKNOWLEDGED",
        "execution_mode": "paper",
        "plan_id": "plan-1",
        "signal_id": "sig-1",
        "tier": "B",
        "score": 75,
        "pattern": "daily",
        "trigger_price": None,
        "filled_qty": 0,
        "fill_price": None,
    }
    order.update(overrides)
    return order


def test_resolves_put_from_occ_contract_when_direction_missing(monkeypatch):
    from ap import fill_monitor as fm

    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **k: None)

    order = _base_order(direction=None, contract="AAPL260626P00195000")
    broker = _Broker({"status": "filled", "exec_quantity": 1, "avg_fill_price": 1.05})

    result = fm.check_order_with_broker(broker, order)

    assert result["status"] == "FILLED"
    assert order["direction"] == "PUT"
    assert order["_fill_monitor_side_source"] == "occ_contract"


def test_broker_filled_zero_qty_is_error_not_filled(monkeypatch):
    from ap import fill_monitor as fm

    events = []
    monkeypatch.setattr(fm, "audit", lambda *a, **k: events.append(("audit", a, k)))
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **k: events.append(("event", a, k)))

    order = _base_order(direction="CALL", contract="AAPL260626C00195000")
    broker = _Broker({"status": "filled", "quantity": 0, "avg_fill_price": 1.05})

    result = fm.check_order_with_broker(broker, order)

    assert result["status"] == "ERROR"
    assert result["reason"] == "BROKER_FILLED_ZERO_QTY"
    assert result["filled_qty"] == 0
    assert events


def test_release_symbol_lock_even_when_reserved_cost_missing(monkeypatch):
    from ap import fill_monitor as fm

    released_equity = []
    released_locks = []
    monkeypatch.setattr(fm, "release_equity", lambda *a: released_equity.append(a))
    monkeypatch.setattr(fm, "release_symbol_lock", lambda *a: released_locks.append(a))

    fm._release_entry_guards(
        _base_order(symbol="TSLA", reserved_cost=None, limit_price=None, qty=None)
    )

    assert released_equity == []
    assert released_locks == [("client@example.com", "TSLA")]


def test_open_position_uses_data_broker_for_underlying_entry(monkeypatch):
    from ap import fill_monitor as fm

    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    monkeypatch.setattr(fm, "_record_position_create_failure", lambda *a, **k: None)

    pm = _PM()
    execution_broker = _Broker({"status": "filled", "exec_quantity": 1, "avg_fill_price": 1.10})
    data_broker = _DataBroker()
    order = _base_order(
        direction=None,
        contract="AAPL260626C00195000",
        trigger_price=None,
        underlying_entry=None,
        entry_underlying=None,
        last_underlying_price=None,
    )

    position_id = fm._open_position_safe(
        pm,
        order=order,
        result={"filled_qty": 1, "avg_fill": 1.10},
        plan_id="plan-1",
        signal_id="sig-1",
        local_id="ord-1",
        broker=execution_broker,
        quote_broker=data_broker,
    )

    assert position_id == "pos-1"
    assert pm.kwargs["side"] == "CALL"
    assert pm.kwargs["underlying_entry"] == 101.5


def test_exit_engine_seed_uses_occ_resolved_put(monkeypatch):
    from ap import fill_monitor as fm

    class _MP:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    monkeypatch.setattr(fm, "_load_managed_position_class", lambda: _MP)
    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)

    ee = _ExitEngine()
    fm._seed_exit_engine(
        ee,
        "pos-1",
        _base_order(direction=None, contract="AAPL260626P00195000"),
        {"filled_qty": 1, "avg_fill": 1.10},
        "sig-1",
    )

    assert len(ee.added) == 1
    assert ee.added[0].side == "PUT"
    assert ee.added[0].signal["side"] == "PUT"


def test_seed_exit_engine_retry_adoption_does_not_seed_duplicate_owner(monkeypatch):
    from ap import fill_monitor as fm
    from ap_exit_engine import CanonicalAdoptionResult

    class _RetryExitEngine(_ExitEngine):
        def __init__(self):
            super().__init__()
            self.adoption_result = None

        def adopt_canonical_position_identity(self, **kwargs):
            self.adoption_result = CanonicalAdoptionResult(
                disposition="RETRY_REPAIR_IDENTITY_UNPROVEN",
                adopted=False,
                safe_to_seed=False,
                retryable=True,
                reason="active_count=2",
            )
            return self.adoption_result

    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    ee = _RetryExitEngine()

    fm._seed_exit_engine(
        ee,
        "pos-1",
        _base_order(direction=None, contract="AAPL260626P00195000"),
        {"filled_qty": 1, "avg_fill": 1.10},
        "sig-1",
    )

    assert ee.adoption_result is not None
    assert ee.adoption_result.disposition == "RETRY_REPAIR_IDENTITY_UNPROVEN"
    assert ee.added == []


# ─────────────────────────────────────────────────────────────────────────────
# PR #235 additional parity coverage — helper unit tests + shim-removal lock.
# ─────────────────────────────────────────────────────────────────────────────

def test_resolve_order_option_side_prefers_canonical_direction():
    from ap import fill_monitor as fm
    side, src = fm._resolve_order_option_side({"direction": "CALL", "contract": "AAPL260626P00195000"})
    assert side == "CALL" and src == "order_direction"


def test_resolve_order_option_side_falls_back_to_occ():
    from ap import fill_monitor as fm
    side, src = fm._resolve_order_option_side({"direction": None, "contract": "AAPL260626P00195000"})
    assert side == "PUT" and src == "occ_contract"

    side, src = fm._resolve_order_option_side({"contract": "SPY260117C00500000"})
    assert side == "CALL" and src == "occ_contract"


def test_resolve_order_option_side_returns_none_for_unparseable():
    from ap import fill_monitor as fm
    side, src = fm._resolve_order_option_side({"direction": None, "contract": None, "symbol": "AAPL"})
    assert side is None and src == "missing_or_unparseable"

    # BUY/SELL are execution actions — never mapped to CALL/PUT.
    side, src = fm._resolve_order_option_side({"direction": "BUY", "contract": None})
    assert side is None


def test_select_quote_broker_prefers_explicit_data_broker():
    from ap import fill_monitor as fm

    class _Exec: pass
    exec_b = _Exec()
    data_b = object()
    assert fm._select_quote_broker(exec_b, data_b) is data_b


def test_select_quote_broker_falls_back_to_execution_broker_attribute():
    from ap import fill_monitor as fm

    class _Exec: pass
    exec_b = _Exec()
    attached = object()
    exec_b.data_broker = attached
    assert fm._select_quote_broker(exec_b, None) is attached


def test_fill_monitor_legacy_module_is_deleted():
    """PR #235: the shim/legacy split must be gone.  fill_monitor.py is the
    single source of truth again; no ap.fill_monitor_legacy import path."""
    import importlib.util
    origin = importlib.util.find_spec("ap.fill_monitor").origin
    assert origin is not None and origin.endswith("fill_monitor.py")
    assert importlib.util.find_spec("ap.fill_monitor_legacy") is None


def test_broker_partial_fill_zero_qty_returns_error_not_partial_fill(monkeypatch):
    from ap import fill_monitor as fm

    events = []
    monkeypatch.setattr(fm, "audit", lambda *a, **k: events.append(("audit", a, k)))
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **k: events.append(("event", a, k)))

    order = _base_order(direction="CALL", contract="AAPL260626C00195000")
    broker = _Broker({"status": "partially_filled", "exec_quantity": 0, "avg_fill_price": 1.25})

    result = fm.check_order_with_broker(broker, order)

    assert result["status"] == "ERROR"
    assert result["reason"] == "BROKER_FILLED_ZERO_QTY"
    assert result["filled_qty"] == 0
    assert any(item[0] == "audit" for item in events)
    assert any(item[0] == "event" for item in events)


def test_broker_exit_partial_fill_zero_qty_returns_error_not_exit_partial(monkeypatch):
    from ap import fill_monitor as fm

    events = []
    monkeypatch.setattr(fm, "audit", lambda *a, **k: events.append(("audit", a, k)))
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **k: events.append(("event", a, k)))

    order = _base_order(kind="EXIT", direction="CALL", contract="AAPL260626C00195000")
    broker = _Broker({"status": "partially_filled", "exec_quantity": 0, "avg_fill_price": 1.25})

    result = fm.check_order_with_broker(broker, order)

    assert result["status"] == "ERROR"
    assert result["reason"] == "BROKER_FILLED_ZERO_QTY"
    assert result["filled_qty"] == 0
    assert any(item[0] == "audit" for item in events)
    assert any(item[0] == "event" for item in events)


@pytest.mark.parametrize("response_id", ["different-broker-order"])
def test_broker_response_identity_mismatch_is_not_fill_truth(monkeypatch, response_id):
    from ap import fill_monitor as fm

    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **k: None)
    result = fm.check_order_with_broker(
        SimpleNamespace(
            get_order=lambda _broker_order_id: {
                **({} if response_id is None else {"id": response_id}),
                "status": "FILLED",
                "exec_quantity": 1,
                "avg_fill_price": 1.05,
            }
        ),
        _base_order(direction="CALL", contract="AAPL260626C00195000"),
    )

    assert result["status"] == "ERROR"
    assert result["reason"] == "BROKER_ORDER_ID_MISMATCH"
    assert result["filled_qty"] == 0


def test_broker_unavailable_without_response_id_preserves_reason_taxonomy(monkeypatch):
    from ap import fill_monitor as fm

    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    result = fm.check_order_with_broker(
        SimpleNamespace(
            get_order=lambda _broker_order_id: {
                "status": "ERROR",
                "reason": "timeout",
            }
        ),
        _base_order(direction="CALL", contract="AAPL260626C00195000"),
    )

    assert result["status"] == "ERROR"
    assert result["reason"] == "timeout"
    assert result["raw"]["_broker_response_unavailable"] is True
    assert "_broker_order_id_mismatch" not in result["raw"]
    assert fm._broker_poll_unavailable_for_durable_filled_recovery(result)


@pytest.mark.parametrize(
    ("error", "failure_class", "reason_prefix"),
    [
        (TimeoutError("read timed out"), "TIMEOUT", "BROKER_READ_TIMEOUT_AMBIGUOUS"),
        (ConnectionError("network reset"), "NETWORK", "BROKER_CONN_ERROR"),
        (PermissionError("unauthorized"), "AUTH", "BROKER_AUTH_ERROR"),
    ],
)
def test_broker_get_exception_is_unavailable_not_identity_mismatch(
    monkeypatch, error, failure_class, reason_prefix
):
    from ap import fill_monitor as fm

    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)

    class _BrokerGetFailure:
        def get_order(self, _broker_order_id):
            raise error

    result = fm.check_order_with_broker(
        _BrokerGetFailure(),
        _base_order(direction="CALL", contract="AAPL260626C00195000"),
    )

    assert result["status"] == "ERROR"
    assert result["reason"].startswith(reason_prefix)
    assert result["raw"]["_broker_get_failed"] is True
    assert result["raw"]["_broker_response_unavailable"] is True
    assert result["raw"]["_broker_read_failure_class"] == failure_class
    assert "_broker_order_id_mismatch" not in result["raw"]
    assert fm._broker_poll_unavailable_for_durable_filled_recovery(result)


def test_unknown_broker_get_exception_does_not_authorize_db_only_recovery(monkeypatch):
    from ap import fill_monitor as fm

    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)

    class _BrokerGetFailure:
        def get_order(self, _broker_order_id):
            raise ValueError("adapter payload parser failed")

    result = fm.check_order_with_broker(
        _BrokerGetFailure(),
        _base_order(direction="CALL", contract="AAPL260626C00195000"),
    )

    assert result["status"] == "ERROR"
    assert result["raw"]["_broker_read_failure_class"] == "UNAVAILABLE"
    assert not fm._broker_poll_unavailable_for_durable_filled_recovery(result)


def test_successful_broker_response_without_id_is_missing_identity_not_mismatch(monkeypatch):
    from ap import fill_monitor as fm

    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    result = fm.check_order_with_broker(
        SimpleNamespace(
            get_order=lambda _broker_order_id: {
                "status": "FILLED",
                "exec_quantity": 1,
                "avg_fill_price": 1.05,
            }
        ),
        _base_order(direction="CALL", contract="AAPL260626C00195000"),
    )

    assert result["status"] == "ERROR"
    assert result["reason"] == "BROKER_RESPONSE_MISSING_ID"
    assert result["filled_qty"] == 0
    assert "_broker_order_id_mismatch" not in result["raw"]
    assert not fm._broker_poll_unavailable_for_durable_filled_recovery(result)


def test_conflicting_broker_response_identity_aliases_are_not_fill_truth(monkeypatch):
    from ap import fill_monitor as fm

    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **k: None)
    result = fm.check_order_with_broker(
        SimpleNamespace(
            get_order=lambda _broker_order_id: {
                "id": "brk-1",
                "order_id": "brk-2",
                "status": "FILLED",
                "exec_quantity": 1,
                "avg_fill_price": 1.05,
            }
        ),
        _base_order(direction="CALL", contract="AAPL260626C00195000"),
    )

    assert result["status"] == "ERROR"
    assert result["reason"] == "BROKER_ORDER_ID_MISMATCH"
    assert result["filled_qty"] == 0


def test_broker_fill_anomaly_state_is_not_environment_configurable(monkeypatch):
    from ap import fill_monitor as fm

    monkeypatch.setenv("FILL_MONITOR_ANOMALY_STATUS", "FILLED")
    assert fm.FILL_ANOMALY_STATUS == "BROKER_FILL_ANOMALY"


@pytest.mark.parametrize("raw_status", ["CANCELED", "REJECTED", "EXPIRED"])
def test_terminal_broker_status_preserves_positive_cumulative_fill(monkeypatch, raw_status):
    from ap import fill_monitor as fm

    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **k: None)
    result = fm.check_order_with_broker(
        SimpleNamespace(
            get_order=lambda _broker_order_id: {
                "id": _broker_order_id,
                "status": raw_status,
                "exec_quantity": 1,
                "avg_fill_price": 1.05,
            }
        ),
        _base_order(direction="CALL", contract="AAPL260626C00195000"),
    )

    assert result["status"] == "FILLED"
    assert result["filled_qty"] == 1
    assert "terminal_remainder_status" not in result


def test_active_broker_status_with_positive_fill_remains_held(monkeypatch):
    from ap import fill_monitor as fm

    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **k: None)
    result = fm.check_order_with_broker(
        SimpleNamespace(
            get_order=lambda _broker_order_id: {
                "id": _broker_order_id,
                "status": "OPEN",
                "exec_quantity": 1,
                "avg_fill_price": 1.05,
            }
        ),
        _base_order(direction="CALL", contract="AAPL260626C00195000"),
    )

    assert result["status"] == "ERROR"
    assert result["reason"] == "BROKER_FILL_STATUS_QUANTITY_CONFLICT"
    assert result["filled_qty"] == 0


@pytest.mark.parametrize("kind", ["ENTRY", "EXIT"])
def test_terminal_broker_status_maps_partial_cumulative_fill_and_remainder(
    monkeypatch, kind
):
    from ap import fill_monitor as fm

    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **k: None)
    result = fm.check_order_with_broker(
        SimpleNamespace(
            get_order=lambda _broker_order_id: {
                "id": _broker_order_id,
                "status": "CANCELED",
                "exec_quantity": 1,
                "avg_fill_price": 1.05,
            }
        ),
        _base_order(
            kind=kind,
            direction="CALL",
            contract="AAPL260626C00195000",
            qty=2,
        ),
    )

    assert result["status"] == ("PARTIAL_FILL" if kind == "ENTRY" else "EXIT_PARTIAL_FILL")
    assert result["filled_qty"] == 1
    assert result["terminal_remainder_status"] == "CANCELED"
    assert result["terminal_remainder_qty"] == 1


@pytest.mark.parametrize("stop_pct", ["nan", "inf", "-0.1", "1", "2"])
def test_invalid_standing_stop_percentage_does_not_call_broker(monkeypatch, stop_pct):
    from ap import fill_monitor as fm

    monkeypatch.setenv("BROKER_STANDING_STOP_PCT", stop_pct)
    broker_calls = []
    broker = SimpleNamespace(
        place_stop_order=lambda **kwargs: broker_calls.append(kwargs),
    )
    result = fm._place_standing_stop_best_effort(
        broker=broker,
        order=_base_order(),
        qty=1,
        entry_price=1.05,
    )

    assert result["outcome"] == "FAILED"
    assert broker_calls == []
