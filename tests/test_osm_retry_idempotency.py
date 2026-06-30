"""
tests/test_osm_retry_idempotency.py — P0-1 + P0-2 codified.

Covers:
  - Exit submission retry (503, ReadTimeout dedupe, ConnError, 400 permanent)
  - Entry submission retry — submit_entry + submit_existing_entry
  - Tradier tag idempotency on all submit paths
  - apply_repeg passes tag through
  - SimBroker backward compat (tag kw-only, default None)

These were runtime tests in earlier sessions; this file codifies them into
the canonical pytest suite. Run with:
    DATABASE_URL=postgresql://x python3 -m pytest tests/test_osm_retry_idempotency.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# Ensure DATABASE_URL is set before importing OSM (defensive — the bot's db
# module reads env at import time)
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

sys.path.insert(0, str(REPO_ROOT))


# ────────────────────────────────────────────────────────────────────────────
# Shared fixtures / stubs
# ────────────────────────────────────────────────────────────────────────────

class _Quiet:
    """Quiet logger stub — avoids log noise in test output."""
    def info(self, *a, **kw): pass
    def warning(self, *a, **kw): pass
    def error(self, *a, **kw): pass
    def critical(self, *a, **kw): pass
    def debug(self, *a, **kw): pass


@pytest.fixture(autouse=True)
def _silence_osm_logger(monkeypatch):
    """Auto-applied to every test in this module."""
    from ap import order_state_machine as osm_mod
    monkeypatch.setattr(osm_mod, "log", _Quiet())


@pytest.fixture
def mock_broker():
    """Mock Tradier broker with .session.post / .session.get mocked."""
    b = MagicMock()
    b.cfg.base_url = "https://sandbox.tradier.com"
    b.cfg.account_id = "VATEST"
    b.base_url = "https://sandbox.tradier.com"
    b.account_id = "VATEST"
    b.session = MagicMock()
    return b


@pytest.fixture
def fast_sleep():
    """Patch _time_module.sleep so retry tests don't actually wait."""
    from ap import order_state_machine as osm_mod
    with patch.object(osm_mod, "_time_module", MagicMock(sleep=lambda s: None)):
        yield


@pytest.fixture
def mock_osm():
    """Minimal OSM instance with just the methods under test bound."""
    from ap.order_state_machine import APOrderStateMachine

    class _MockOSM:
        def __init__(self):
            self.client_id = "test@x.com"

        _lookup_order_by_tag      = APOrderStateMachine._lookup_order_by_tag
        _submit_order_with_retry  = APOrderStateMachine._submit_order_with_retry
        submit_exit               = APOrderStateMachine.submit_exit
        submit_entry              = APOrderStateMachine.submit_entry
        submit_existing_entry     = APOrderStateMachine.submit_existing_entry

        # Stubs for behavior outside the unit under test
        def _get_active_exit_order(self, pid): return None
        def create_exit_order(self, **kw): return "L-EXIT-001"
        def create_entry_order(self, plan, **kw): return "L-ENTRY-001"
        def transition(self, *a, **kw): return True
        def _resolve_underlying_symbol(self, *, symbol, contract): return symbol
        @staticmethod
        def _is_broker_accept_status(s): return s in ("open", "pending", "ok", "accepted")
        def _emit_transition_event(self, **kw): pass
        def _flag_split_brain_order(self, *a, **kw): pass

        def _get_order(self, oid):
            return {
                "local_order_id": "L-EX-001", "status": "CREATED", "kind": "ENTRY",
                "ticker": "BA", "contract": "BA260522C00220000", "qty": 2,
                "limit_price": 1.50, "plan_id": "p", "signal_id": "s",
                "client_id": "test@x.com", "direction": "CALL",
                "execution_mode": "paper", "timeframe": "1d", "pattern": "2-3",
                "score": 75.0, "trigger_price": 220.0, "underlying_entry": 220.25,
                "target_price": 223.0, "stop_price": 218.5,
            }

    return _MockOSM()


@pytest.fixture
def plan_stub():
    """Minimal plan object for submit_entry."""
    class _Plan:
        plan_id = "plan-1"
        signal_id = "sig-1"
        ticker = "BA"
        contract_symbol = "BA260522C00220000"
        contracts = 2
        limit_price = 1.50
        execution_mode = "paper"
        side = "CALL"
        direction = "CALL"
        timeframe = "1d"
        pattern = "2-3"
        score = 75.0
        trigger_price = 220.0
        underlying_entry = 220.25
        target_price = 223.0
        stop_price = 218.5
    return _Plan()


def _resp(status_code, json_body=None, text=""):
    """Build a mock requests.Response."""
    r = MagicMock()
    r.status_code = status_code
    r.text = text
    if json_body is not None:
        r.json.return_value = json_body
    return r


# ────────────────────────────────────────────────────────────────────────────
# P0-1: Exit retry + idempotency
# ────────────────────────────────────────────────────────────────────────────

class TestExitRetryAndIdempotency:
    """Exit submission retries on transient broker errors; recovers from
    ambiguous read-timeouts via Tradier tag lookup (no double-submit)."""

    def test_exit_retries_on_503_then_succeeds(self, mock_osm, mock_broker, fast_sleep):
        mock_broker.session.post.side_effect = [
            _resp(503, text="down"),
            _resp(200, json_body={"order": {"id": "BO-EXIT-1", "status": "ok"}}),
        ]
        result = mock_osm.submit_exit(
            broker=mock_broker, position_id="pos-1",
            contract="BA260522C00220000", symbol="BA",
            direction="CALL", qty=1, limit_price=1.50,
        )
        assert result["ok"] is True
        assert mock_broker.session.post.call_count == 2
        assert result["broker_order_id"] == "BO-EXIT-1"

    def test_exit_read_timeout_recovers_via_tag_lookup(
        self, mock_osm, mock_broker, fast_sleep,
    ):
        """ReadTimeout = ambiguous. Order may have landed. Look up by tag."""
        import requests
        mock_broker.session.post.side_effect = requests.exceptions.ReadTimeout("t")
        mock_broker.session.get.return_value = _resp(200, json_body={
            "orders": {"order": [{
                "id": "BO-RECOVERED", "tag": "L-EXIT-001", "status": "open"
            }]}
        })
        result = mock_osm.submit_exit(
            broker=mock_broker, position_id="pos-1",
            contract="BA260522C00220000", symbol="BA",
            direction="CALL", qty=1, limit_price=1.50,
        )
        assert result["ok"] is True
        assert result["broker_order_id"] == "BO-RECOVERED"
        # CRITICAL: post called only ONCE — no double-submit
        assert mock_broker.session.post.call_count == 1
        # GET called to look up by tag
        assert mock_broker.session.get.call_count == 1

    def test_exit_400_is_permanent_no_retry(self, mock_osm, mock_broker, fast_sleep):
        mock_broker.session.post.return_value = _resp(400, text="bad symbol")
        result = mock_osm.submit_exit(
            broker=mock_broker, position_id="pos-1",
            contract="BA260522C00220000", symbol="BA",
            direction="CALL", qty=1, limit_price=1.50,
        )
        assert result["ok"] is False
        # Permanent: no retry
        assert mock_broker.session.post.call_count == 1
        assert "broker_http_400" in result["error"]

    def test_exit_3x_503_exhausts(self, mock_osm, mock_broker, fast_sleep):
        mock_broker.session.post.return_value = _resp(503, text="down")
        result = mock_osm.submit_exit(
            broker=mock_broker, position_id="pos-1",
            contract="BA260522C00220000", symbol="BA",
            direction="CALL", qty=1, limit_price=1.50,
        )
        assert result["ok"] is False
        # Exhausted at 3 attempts
        assert mock_broker.session.post.call_count == 3


# ────────────────────────────────────────────────────────────────────────────
# P0-2: Entry retry + idempotency
# ────────────────────────────────────────────────────────────────────────────

class TestEntryRetryAndIdempotency:
    """Both entry submission paths (submit_entry / submit_existing_entry)
    use the same retry + tag-idempotency helper as submit_exit."""

    def test_submit_entry_sends_tag(self, mock_osm, mock_broker, plan_stub):
        mock_broker.session.post.return_value = _resp(
            200, json_body={"order": {"id": "BO-ENTRY-1", "status": "ok"}}
        )
        result = mock_osm.submit_entry(broker=mock_broker, plan=plan_stub, limit_price=1.50)
        assert result["ok"] is True
        data = mock_broker.session.post.call_args.kwargs["data"]
        # Tag is the local order id — required for idempotency on retry
        assert data["tag"] == "L-ENTRY-001"
        assert data["side"] == "buy_to_open"

    def test_submit_entry_retries_on_503(
        self, mock_osm, mock_broker, plan_stub, fast_sleep,
    ):
        mock_broker.session.post.side_effect = [
            _resp(503, text="down"),
            _resp(200, json_body={"order": {"id": "BO-ENTRY-2", "status": "ok"}}),
        ]
        result = mock_osm.submit_entry(broker=mock_broker, plan=plan_stub, limit_price=1.50)
        assert result["ok"] is True
        assert mock_broker.session.post.call_count == 2

    def test_submit_entry_read_timeout_recovers_via_tag(
        self, mock_osm, mock_broker, plan_stub, fast_sleep,
    ):
        import requests
        mock_broker.session.post.side_effect = requests.exceptions.ReadTimeout("t")
        mock_broker.session.get.return_value = _resp(200, json_body={
            "orders": {"order": [{
                "id": "BO-ENTRY-3", "tag": "L-ENTRY-001", "status": "open"
            }]}
        })
        result = mock_osm.submit_entry(broker=mock_broker, plan=plan_stub, limit_price=1.50)
        assert result["ok"] is True
        assert result["broker_order_id"] == "BO-ENTRY-3"
        # No double-submit
        assert mock_broker.session.post.call_count == 1

    def test_submit_entry_400_permanent_no_retry(
        self, mock_osm, mock_broker, plan_stub, fast_sleep,
    ):
        mock_broker.session.post.return_value = _resp(400, text="rejected")
        result = mock_osm.submit_entry(broker=mock_broker, plan=plan_stub, limit_price=1.50)
        assert result["ok"] is False
        assert mock_broker.session.post.call_count == 1

    def test_submit_existing_entry_sends_tag(self, mock_osm, mock_broker):
        mock_broker.session.post.return_value = _resp(
            200, json_body={"order": {"id": "BO-EX1", "status": "ok"}}
        )
        result = mock_osm.submit_existing_entry(
            broker=mock_broker, local_order_id="L-EX-001",
        )
        assert result["ok"] is True
        data = mock_broker.session.post.call_args.kwargs["data"]
        assert data["tag"] == "L-EX-001"

    def test_submit_existing_entry_retries_on_503(
        self, mock_osm, mock_broker, fast_sleep,
    ):
        mock_broker.session.post.side_effect = [
            _resp(503, text="down"),
            _resp(200, json_body={"order": {"id": "BO-EX2", "status": "ok"}}),
        ]
        result = mock_osm.submit_existing_entry(
            broker=mock_broker, local_order_id="L-EX-001",
        )
        assert result["ok"] is True
        assert mock_broker.session.post.call_count == 2


# ────────────────────────────────────────────────────────────────────────────
# Cross-cutting: tag signature on the broker layer
# ────────────────────────────────────────────────────────────────────────────

class TestBrokerTagSignature:
    """Tradier and Sim brokers must accept `tag` as keyword-only with default
    None so existing callers still work."""

    def test_tradier_place_order_has_tag_param(self):
        import inspect
        from ap.brokers.tradier import TradierBroker
        sig = inspect.signature(TradierBroker.place_order)
        assert "tag" in sig.parameters
        assert sig.parameters["tag"].kind == inspect.Parameter.KEYWORD_ONLY
        assert sig.parameters["tag"].default is None

    def test_sim_broker_accepts_tag(self):
        from ap.broker import SimBroker
        sb = SimBroker()
        # Without tag (back-compat)
        r1 = sb.place_order("BA", "BA260522C00220000", 1, 1.50)
        assert r1.broker_order_id != "N/A"
        # With tag
        r2 = sb.place_order("BA", "BA260522C00220000", 1, 1.50, tag="test-id")
        assert r2.broker_order_id != "N/A"

    def test_tradier_place_order_includes_tag_in_payload(self):
        from unittest.mock import patch
        from ap.brokers.tradier import TradierBroker, TradierConfig
        cfg = TradierConfig(
            base_url="https://sandbox.tradier.com",
            access_token="tok",
            account_id="VATEST",
        )
        broker = TradierBroker(cfg)
        captured = {}

        def fake_post(path, data=None):
            captured["data"] = dict(data)
            return {"order": {"id": "BO-X", "status": "ok"}}

        with patch.object(broker, "_post", fake_post):
            broker.place_order("BA", "BA260522C00220000", 1, 1.50, tag="L-EX-001")
        assert captured["data"]["tag"] == "L-EX-001"

    def test_tradier_place_order_omits_tag_when_not_passed(self):
        from unittest.mock import patch
        from ap.brokers.tradier import TradierBroker, TradierConfig
        cfg = TradierConfig(
            base_url="https://sandbox.tradier.com",
            access_token="tok",
            account_id="VATEST",
        )
        broker = TradierBroker(cfg)
        captured = {}

        def fake_post(path, data=None):
            captured["data"] = dict(data)
            return {"order": {"id": "BO-X", "status": "ok"}}

        with patch.object(broker, "_post", fake_post):
            broker.place_order("BA", "BA260522C00220000", 1, 1.50)
        # No tag key when not passed (preserves Tradier default behavior)
        assert "tag" not in captured["data"]


# ────────────────────────────────────────────────────────────────────────────
# Repeg path also uses tag (covered by integration not unit due to deps)
# ────────────────────────────────────────────────────────────────────────────

class TestRepegPassesTag:
    """apply_repeg in retry_engine.py must forward `tag=str(local_oid)` to
    broker.place_order so repegs also benefit from broker-side idempotency."""

    def test_apply_repeg_source_passes_tag(self):
        """Source-level assertion (function isn't easily mockable without
        full DB). If this regresses, an integration test would catch it
        but this gives early warning."""
        import ast
        src = (REPO_ROOT / "ap" / "retry_engine.py").read_text()
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "apply_repeg":
                fn_src = ast.get_source_segment(src, node)
                assert "tag=str(local_oid)" in fn_src, (
                    "apply_repeg must pass tag=str(local_oid) to place_order"
                )
                return
        pytest.fail("apply_repeg function not found in retry_engine.py")
