"""
tests/test_osm_retry_idempotency.py — P0-1 + P0-2 codified.

Covers:
  - Exit submission ambiguity holds (503/429, ReadTimeout, ConnError) and 400 rejection
  - Entry submission ambiguity holds — submit_entry + submit_existing_entry
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
    b.list_positions.return_value = [
        {"symbol": "BA260522C00220000", "quantity": 2, "account_id": "VATEST"},
        {"symbol": "SMCI260626P00032500", "quantity": 3, "account_id": "VATEST"},
    ]
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
            self.row = {
                "local_order_id": "L-EX-001", "status": "CREATED", "kind": "ENTRY",
                "ticker": "BA", "contract": "BA260522C00220000", "qty": 2,
                "limit_price": 1.50, "plan_id": "p", "signal_id": "s",
                "client_id": "test@x.com", "direction": "CALL",
                "execution_mode": "paper", "timeframe": "1d", "pattern": "2-3",
                "score": 75.0, "trigger_price": 220.0, "underlying_entry": 220.25,
                "target_price": 223.0, "stop_price": 218.5,
                "broker_order_id": None, "submitted_ts": None, "meta": {},
            }

        _lookup_order_by_tag      = APOrderStateMachine._lookup_order_by_tag
        _submit_order_with_retry  = APOrderStateMachine._submit_order_with_retry
        submit_exit               = APOrderStateMachine.submit_exit
        submit_entry              = APOrderStateMachine.submit_entry
        submit_existing_entry     = APOrderStateMachine.submit_existing_entry

        # Stubs for behavior outside the unit under test
        def _get_active_exit_order(self, pid):
            if self.row.get("kind") == "EXIT" and self.row.get("position_id") == pid:
                return dict(self.row)
            return None
        def create_exit_order(self, **kw):
            self.row = {
                "local_order_id": "L-EXIT-001",
                "client_id": self.client_id,
                "position_id": kw["position_id"],
                "kind": "EXIT",
                "status": "EXIT_REQUESTED",
                "execution_mode": kw["execution_mode"],
                "contract": kw["contract"],
                "qty": kw["qty"],
                "broker_order_id": "",
                "submitted_ts": None,
                "meta": {},
            }
            return "L-EXIT-001"
        def create_entry_order(self, plan, **kw):
            self.row.update({
                "local_order_id": "L-ENTRY-001",
                "plan_id": plan.plan_id,
                "signal_id": plan.signal_id,
                "contract": plan.contract_symbol,
                "qty": int(plan.contracts),
                "limit_price": float(kw.get("limit_price") or plan.limit_price),
                "execution_mode": str(
                    kw.get("execution_mode") or plan.execution_mode
                ).lower(),
                "status": "CREATED",
                "broker_order_id": None,
                "submitted_ts": None,
                "meta": {},
            })
            return "L-ENTRY-001"
        def transition(self, _local_order_id, status, **fields):
            self.row["status"] = str(status)
            self.row.update(fields)
            return True
        def update_order_meta(self, _local_order_id, patch):
            self.row["meta"].update(patch)
            return True
        def persist_entry_submit_intent(self, _local_order_id, **kwargs):
            key = kwargs["broker_submit_key"]
            self.row["meta"].update({
                "lifecycle_state": "SUBMITTING",
                "submit_intent_at": "2026-07-15T09:30:00Z",
                "broker_submit_key": key,
                "broker_submit_payload_hash": kwargs["payload_hash"],
                "current_owner": f"broker_submit:{key}",
            })
            return True
        def persist_exit_submit_intent(self, _local_order_id, **kwargs):
            key = kwargs["broker_submit_key"]
            self.row["meta"].update({
                "lifecycle_state": "SUBMITTING",
                "submit_intent_at": "2026-08-08T12:00:00Z",
                "broker_submit_key": key,
                "broker_submit_payload_hash": kwargs["payload_hash"],
                "current_owner": f"broker_submit:{key}",
            })
            return True
        def _resolve_underlying_symbol(self, *, symbol, contract): return symbol
        @staticmethod
        def _is_broker_accept_status(s): return s in ("open", "pending", "ok", "accepted")
        def _emit_transition_event(self, **kw): pass
        def _flag_split_brain_order(self, *a, **kw): pass

        def _get_order(self, oid):
            return dict(self.row)

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
    """Exit submission never blindly retries a response that may conceal an
    accepted broker order; it first reconciles the canonical Tradier tag."""

    @pytest.mark.parametrize("status_code", [503, 429])
    def test_exit_http_ambiguity_without_tag_proof_never_reposts(
        self, mock_osm, mock_broker, fast_sleep, status_code,
    ):
        mock_broker.session.post.return_value = _resp(status_code, text="uncertain")
        mock_broker.session.get.return_value = _resp(
            200, json_body={"orders": {"order": []}},
        )
        result = mock_osm.submit_exit(
            broker=mock_broker, position_id="pos-1",
            contract="BA260522C00220000", symbol="BA",
            direction="CALL", qty=1, limit_price=1.50,
        )
        assert result["ok"] is False
        assert result["status"] == "EXIT_REQUESTED"
        assert result["error"] == (
            f"BROKER_AMBIGUOUS_HTTP_{status_code}_RECONCILIATION_REQUIRED"
        )
        assert result["reconciliation_required"] is True
        assert result["identity_quarantine"] is True
        assert mock_broker.session.post.call_count == 1
        assert mock_broker.session.get.call_count == 1

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
        assert mock_osm.row["status"] == "EXIT_SUBMITTED"
        assert mock_osm.row["broker_order_id"] == "BO-RECOVERED"
        # CRITICAL: post called only ONCE — no double-submit
        assert mock_broker.session.post.call_count == 1
        # GET called to look up by tag
        assert mock_broker.session.get.call_count == 1

    @pytest.mark.parametrize(
        "failure_kind",
        ["json", "read_timeout", "connection_error", "request_exception", "unexpected"],
    )
    def test_exit_ambiguous_without_tag_proof_never_reposts(
        self, mock_osm, mock_broker, fast_sleep, failure_kind,
    ):
        import requests

        if failure_kind == "json":
            response = _resp(200)
            response.json.side_effect = ValueError("truncated json")
            mock_broker.session.post.return_value = response
        else:
            error_type = {
                "read_timeout": requests.exceptions.ReadTimeout,
                "connection_error": requests.exceptions.ConnectionError,
                "request_exception": requests.exceptions.RequestException,
                "unexpected": RuntimeError,
            }[failure_kind]
            mock_broker.session.post.side_effect = error_type("t")
        mock_broker.session.get.return_value = _resp(
            200, json_body={"orders": {"order": []}},
        )

        result = mock_osm.submit_exit(
            broker=mock_broker, position_id="pos-1",
            contract="BA260522C00220000", symbol="BA",
            direction="CALL", qty=1, limit_price=1.50,
        )

        assert result["ok"] is False
        assert result["reconciliation_required"] is True
        assert result["identity_quarantine"] is True
        assert result["error"].startswith("BROKER_AMBIGUOUS_")
        if failure_kind == "unexpected":
            assert result["error"].startswith("BROKER_AMBIGUOUS_UNEXPECTED_")
        assert mock_broker.session.post.call_count == 1
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

    def test_exit_503_recovers_by_tag_without_second_post(
        self, mock_osm, mock_broker, fast_sleep,
    ):
        mock_broker.session.post.return_value = _resp(503, text="down")
        mock_broker.session.get.return_value = _resp(200, json_body={
            "orders": {"order": [{
                "id": "BO-EXIT-503", "tag": "L-EXIT-001", "status": "open",
            }]},
        })
        result = mock_osm.submit_exit(
            broker=mock_broker, position_id="pos-1",
            contract="BA260522C00220000", symbol="BA",
            direction="CALL", qty=1, limit_price=1.50,
        )
        assert result["ok"] is True
        assert result["broker_order_id"] == "BO-EXIT-503"
        assert mock_broker.session.post.call_count == 1
        assert mock_broker.session.get.call_count == 1


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

    @pytest.mark.parametrize("status_code", [503, 429])
    def test_submit_entry_http_ambiguity_without_tag_proof_never_reposts(
        self, mock_osm, mock_broker, plan_stub, fast_sleep, status_code,
    ):
        mock_broker.session.post.return_value = _resp(status_code, text="uncertain")
        mock_broker.session.get.return_value = _resp(
            200, json_body={"orders": {"order": []}},
        )
        result = mock_osm.submit_entry(broker=mock_broker, plan=plan_stub, limit_price=1.50)
        assert result["ok"] is False
        assert result["error"] == (
            f"BROKER_AMBIGUOUS_HTTP_{status_code}_RECONCILIATION_REQUIRED"
        )
        assert result["reconciliation_required"] is True
        assert result["identity_quarantine"] is True
        assert mock_broker.session.post.call_count == 1
        assert mock_broker.session.get.call_count == 1

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

    @pytest.mark.parametrize("status_code", [503, 429])
    def test_submit_existing_entry_http_ambiguity_never_reposts(
        self, mock_osm, mock_broker, fast_sleep, status_code,
    ):
        mock_broker.session.post.return_value = _resp(status_code, text="uncertain")
        mock_broker.session.get.return_value = _resp(
            200, json_body={"orders": {"order": []}},
        )
        result = mock_osm.submit_existing_entry(
            broker=mock_broker, local_order_id="L-EX-001",
        )
        assert result["ok"] is False
        assert result["error"] == (
            f"BROKER_AMBIGUOUS_HTTP_{status_code}_RECONCILIATION_REQUIRED"
        )
        assert result["reconciliation_required"] is True
        assert result["identity_quarantine"] is True
        assert mock_broker.session.post.call_count == 1
        assert mock_broker.session.get.call_count == 1


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
    """apply_repeg must forward the canonical local id as the broker tag so
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
                assert "tag=canonical_broker_submit_key(local_oid)" in fn_src, (
                    "apply_repeg must pass the canonical submit key to place_order"
                )
                return
        pytest.fail("apply_repeg function not found in retry_engine.py")
