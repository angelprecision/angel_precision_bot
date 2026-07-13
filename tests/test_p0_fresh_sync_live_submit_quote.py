"""P0 proof for fresh synchronous quote provenance at final LIVE submit."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
import os
import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

from ap.live_submit_gates import GateOutcome, check_market_validity_gate


def _fresh_sync(**overrides):
    values = {
        "side": "CALL",
        "trigger_price": 100.0,
        "stop_price": 95.0,
        "target_price": 110.0,
        "current_bid": 100.8,
        "current_ask": 101.0,
        "quote_age_ms": None,
        "quote_source": "tradier",
        "quote_fetched_at": datetime.now(timezone.utc).isoformat(),
        "quote_provenance": "synchronous_submit_fetch",
        "execution_mode": "live",
    }
    values.update(overrides)
    return check_market_validity_gate(**values)


def test_live_synchronous_call_without_provider_age_passes():
    result = _fresh_sync()

    assert result.passed is True
    assert result.reason_code == GateOutcome.PASS
    assert (
        result.audit["quote_freshness_code"]
        == GateOutcome.CURRENT_PRICE_FRESH_SYNC_FETCH
    )
    assert result.audit["quote_age_ms"] >= 0


def test_live_synchronous_put_without_provider_age_passes():
    result = _fresh_sync(
        side="PUT",
        trigger_price=100.0,
        stop_price=105.0,
        target_price=90.0,
        current_bid=99.0,
        current_ask=99.2,
    )

    assert result.passed is True
    assert (
        result.audit["quote_freshness_code"]
        == GateOutcome.CURRENT_PRICE_FRESH_SYNC_FETCH
    )


def test_live_cached_quote_with_missing_age_blocks():
    result = _fresh_sync(quote_fetched_at=None, quote_provenance="cached")

    assert result.passed is False
    assert result.reason_code == GateOutcome.CURRENT_PRICE_AGE_UNKNOWN


def test_live_malformed_fetch_timestamp_blocks():
    result = _fresh_sync(quote_fetched_at="not-a-timestamp")

    assert result.passed is False
    assert result.reason_code == GateOutcome.CURRENT_PRICE_AGE_UNKNOWN


def test_live_genuinely_stale_quote_blocks():
    result = _fresh_sync(
        quote_age_ms=None,
        quote_fetched_at=(datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat(),
    )

    assert result.passed is False
    assert result.reason_code == GateOutcome.CURRENT_PRICE_STALE


def test_provider_age_present_and_fresh_succeeds_without_sync_provenance():
    result = _fresh_sync(
        quote_age_ms=100,
        quote_fetched_at=None,
        quote_provenance=None,
    )

    assert result.passed is True
    assert result.reason_code == GateOutcome.PASS
    assert result.audit["quote_freshness_code"] is None


@pytest.mark.parametrize(
    ("bid", "ask"),
    [(0.0, 101.0), (100.8, 0.0)],
)
def test_live_zero_bid_or_ask_blocks_with_zero_reason(bid, ask):
    result = _fresh_sync(current_bid=bid, current_ask=ask)

    assert result.passed is False
    assert result.reason_code == GateOutcome.CURRENT_PRICE_ZERO


@pytest.mark.parametrize(
    ("bid", "ask"),
    [(None, None), ("bad", "worse")],
)
def test_live_missing_bid_ask_blocks_with_missing_reason(bid, ask):
    result = _fresh_sync(current_bid=bid, current_ask=ask)

    assert result.passed is False
    assert result.reason_code == GateOutcome.CURRENT_PRICE_MISSING


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("current_bid", math.nan),
        ("current_ask", math.nan),
        ("quote_age_ms", math.nan),
        ("current_bid", math.inf),
        ("current_ask", -math.inf),
        ("quote_age_ms", math.inf),
    ],
)
def test_live_non_finite_quote_values_block_with_invalid_reason(field, value):
    result = _fresh_sync(**{field: value})

    assert result.passed is False
    assert result.reason_code == GateOutcome.CURRENT_PRICE_INVALID


def test_live_inverted_bid_ask_blocks_with_invalid_reason():
    result = _fresh_sync(current_bid=101.0, current_ask=100.9)

    assert result.passed is False
    assert result.reason_code == GateOutcome.CURRENT_PRICE_INVALID


def test_live_future_fetch_timestamp_beyond_tolerance_blocks(monkeypatch):
    monkeypatch.setenv("LIVE_SYNC_QUOTE_MAX_FUTURE_SKEW_MS", "1000")
    result = _fresh_sync(
        quote_fetched_at=(datetime.now(timezone.utc) + timedelta(seconds=5)).isoformat(),
    )

    assert result.passed is False
    assert result.reason_code == GateOutcome.CURRENT_PRICE_AGE_UNKNOWN


def test_live_future_fetch_timestamp_within_tolerance_clamps_to_fresh(monkeypatch):
    monkeypatch.setenv("LIVE_SYNC_QUOTE_MAX_FUTURE_SKEW_MS", "1000")
    result = _fresh_sync(
        quote_fetched_at=(datetime.now(timezone.utc) + timedelta(milliseconds=100)).isoformat(),
    )

    assert result.passed is True
    assert result.audit["quote_age_ms"] == 0.0


def test_live_timezone_naive_fetch_timestamp_blocks():
    result = _fresh_sync(quote_fetched_at=datetime.now().isoformat())

    assert result.passed is False
    assert result.reason_code == GateOutcome.CURRENT_PRICE_AGE_UNKNOWN


def test_live_quote_fetch_failure_is_distinct():
    result = _fresh_sync(
        current_bid=None,
        current_ask=None,
        quote_fetch_failed=True,
        quote_fetch_error="ReadTimeout",
    )

    assert result.passed is False
    assert result.reason_code == GateOutcome.CURRENT_PRICE_FETCH_FAILED
    assert result.audit["quote_fetch_error"] == "ReadTimeout"


def test_paper_missing_age_behavior_remains_unchanged():
    result = _fresh_sync(
        execution_mode="paper",
        quote_fetched_at=None,
        quote_provenance=None,
    )

    assert result.passed is True


@pytest.mark.parametrize(
    ("side", "bid", "ask", "reason"),
    [
        ("CALL", 99.0, 99.5, GateOutcome.CALL_NO_LONGER_ABOVE_TRIGGER),
        ("PUT", 100.5, 101.0, GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER),
    ],
)
def test_trigger_lane_remains_call_ask_put_bid(side, bid, ask, reason):
    result = _fresh_sync(
        side=side,
        current_bid=bid,
        current_ask=ask,
        stop_price=95.0 if side == "CALL" else 105.0,
        target_price=110.0 if side == "CALL" else 90.0,
    )

    assert result.passed is False
    assert result.reason_code == reason


def test_stop_broken_opportunity_still_blocks():
    result = _fresh_sync(current_bid=94.0, current_ask=94.5)

    assert result.passed is False
    assert result.reason_code in {
        GateOutcome.CALL_NO_LONGER_ABOVE_TRIGGER,
        GateOutcome.CALL_STOP_ALREADY_BROKEN,
    }


def test_target_already_hit_opportunity_still_blocks():
    result = _fresh_sync(
        target_price=105.0,
        current_bid=105.2,
        current_ask=105.4,
    )

    assert result.passed is False
    assert result.reason_code == GateOutcome.TARGET_ALREADY_INVALID


def test_remaining_opportunity_gate_still_blocks():
    result = _fresh_sync(
        target_price=105.0,
        current_bid=104.6,
        current_ask=104.8,
    )

    assert result.passed is False
    assert result.reason_code == GateOutcome.REMAINING_OPPORTUNITY_TOO_SMALL


class _Response:
    status_code = 200
    text = "ok"

    @staticmethod
    def json():
        return {"order": {"id": "TR-ENTRY-1", "status": "ok"}}


class _RuntimeOSM:
    """Real submit_existing_entry with only DB transport replaced by memory."""

    from ap.order_state_machine import APOrderStateMachine as _RealOSM

    submit_existing_entry = _RealOSM.submit_existing_entry
    _submit_order_with_retry = _RealOSM._submit_order_with_retry
    _lookup_order_by_tag = _RealOSM._lookup_order_by_tag

    def __init__(self, row, *, fail_all_pass_audit=False):
        self.row = row
        self.client_id = row["client_id"]
        self.execution_mode = row["execution_mode"]
        self.fail_all_pass_audit = fail_all_pass_audit

    def _get_order(self, _local_order_id):
        return dict(self.row)

    get_order = _get_order

    def update_order_meta(self, _local_order_id, patch):
        if (
            self.fail_all_pass_audit
            and isinstance(patch, dict)
            and (patch.get("live_submit_gate") or {}).get("all_passed") is True
        ):
            raise RuntimeError("audit write unavailable")
        self.row["meta"].update(patch)
        return True

    def transition(self, _local_order_id, status, **fields):
        self.row["status"] = str(status)
        self.row.update(fields)
        return True

    def persist_deferred_submit_intent(
        self,
        _local_order_id,
        *,
        owner,
        generation,
        execution_mode,
        payload_hash,
        broker_submit_key,
    ):
        self.row["meta"].update(
            {
                "submit_intent_at": datetime.now(timezone.utc).isoformat(),
                "recovery_submit_owner": owner,
                "recovery_submit_generation": generation,
                "recovery_submit_execution_mode": execution_mode,
                "broker_submit_payload_hash": payload_hash,
                "broker_submit_key": broker_submit_key,
            }
        )
        return True

    def expire_pending_entry(self, _local_order_id, reason):
        self.row["status"] = "EXPIRED"
        self.row["last_error"] = reason
        return True

    @staticmethod
    def _is_broker_accept_status(status):
        return str(status or "").lower() in {"ok", "open", "accepted", "pending"}

    @staticmethod
    def _resolve_underlying_symbol(*, symbol, contract):
        return symbol

    def _emit_transition_event(self, **_kwargs):
        return None

    def _flag_split_brain_order(self, *_args, **_kwargs):
        raise AssertionError("unexpected split brain")


def _run_real_live_submit(monkeypatch, *, side="CALL", fail_all_pass_audit=False):
    import ap_execution_core as core_module

    now = datetime.now(timezone.utc)
    trigger = 100.0
    quote = (
        {"bid": 100.8, "ask": 101.0, "source": "tradier"}
        if side == "CALL"
        else {"bid": 99.0, "ask": 99.2, "source": "tradier"}
    )
    stop = 95.0 if side == "CALL" else 105.0
    target = 110.0 if side == "CALL" else 90.0
    contract = (
        "AAPL260717C00200000" if side == "CALL" else "AAPL260717P00200000"
    )
    row = {
        "local_order_id": "L-LIVE-SYNC-1",
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "symbol": "AAPL",
        "ticker": "AAPL",
        "contract": contract,
        "qty": 1,
        "limit_price": 1.26,
        "reserved_cost": 126.0,
        "signal_id": "sig-live-sync-1",
        "direction": side,
        "timeframe": "1d",
        "pattern": "2-3",
        "score": 95.0,
        "trigger_price": trigger,
        "stop_price": stop,
        "target_price": target,
        "underlying_entry": quote["ask"] if side == "CALL" else quote["bid"],
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {"trigger_crossed_at": (now - timedelta(seconds=2)).isoformat()},
    }
    plan = SimpleNamespace(
        contract_symbol=contract,
        execution_price_per_share=1.26,
        ask=1.25,
        mid=1.20,
        affordable_contracts=1,
        premium_per_contract=126.0,
        contracts=1,
        limit_price=1.26,
        side=side,
        direction=side,
        execution_mode="live",
        client_id=row["client_id"],
        signal_id=row["signal_id"],
        ticker="AAPL",
        trigger_price=trigger,
        stop_underlying=stop,
        target_underlying=target,
        metadata={
            "queue_id": 77,
            "recovery_submit_fenced": True,
            "recovery_submit_owner": "broker-ready-owner-1",
            "recovery_submit_generation": 7,
            "ownership_kind": "broker_ready_recovery",
        },
    )
    watched = SimpleNamespace(
        signal={
            "ticker": "AAPL",
            "side": side,
            "entry_price": trigger,
            "stop_price": stop,
            "target_price": target,
            "signal_id": row["signal_id"],
            "local_order_id": row["local_order_id"],
            "client_id": row["client_id"],
            "execution_mode": "live",
            "recovery_submit_fenced": True,
            "recovery_submit_owner": "broker-ready-owner-1",
            "recovery_submit_generation": 7,
            "ownership_kind": "broker_ready_recovery",
        },
        trigger_price=trigger,
        ticker="AAPL",
        trigger_crossed_at=now - timedelta(seconds=2),
    )

    osm = _RuntimeOSM(row, fail_all_pass_audit=fail_all_pass_audit)
    broker = SimpleNamespace(
        cfg=SimpleNamespace(
            base_url="https://api.tradier.com",
            account_id="LIVE-ACCOUNT",
        ),
        base_url="https://api.tradier.com",
        account_id="LIVE-ACCOUNT",
        get_quote=MagicMock(return_value=quote),
        session=MagicMock(),
    )

    def _post(*_args, **_kwargs):
        assert row["meta"].get("submit_intent_at"), "submit intent must precede POST"
        assert row["meta"].get("broker_submit_payload_hash")
        return _Response()

    broker.session.post.side_effect = _post
    core = core_module.APExecutionCore.__new__(core_module.APExecutionCore)
    core.paper = False
    core.mode = core.execution_mode = "LIVE"
    core.client_id = core.client_email = row["client_id"]
    core.broker = broker
    core.store = MagicMock()
    core.contract_selector = MagicMock()
    core._breach_risk_check = MagicMock(return_value=True)
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)
    core._refresh_hydrated_prebreach_plan = MagicMock(return_value=False)
    core._cleanup_pending_entry_order = MagicMock(
        side_effect=AssertionError("successful recovered submit must not use generic cleanup")
    )
    core.order_state_machine = osm

    fake_execution = types.ModuleType("ap.execution")
    fake_execution._refresh_ask_at_submit = lambda *_args, **_kwargs: (
        1.25,
        5,
        True,
        "",
        {
            "submit_bid": 1.20,
            "submit_ask": 1.25,
            "submit_last": 1.23,
            "submit_mid": 1.225,
            "spread_pct": 0.04,
        },
    )
    monkeypatch.setitem(sys.modules, "ap.execution", fake_execution)
    monkeypatch.setenv("LIVE_CONFIRMATION_REQUIRED", "0")
    monkeypatch.setenv("ENTRY_CUTOFF_ET_HHMM", "2359")

    core_module.APExecutionCore._on_entry_trigger(core, watched)
    return row, broker, watched, core


@pytest.mark.parametrize("side", ["CALL", "PUT"])
def test_real_live_submit_path_posts_once_with_fresh_sync_quote(monkeypatch, side):
    row, broker, _watched, _core = _run_real_live_submit(monkeypatch, side=side)

    broker.session.post.assert_called_once()
    assert row["status"] == "SUBMITTED"
    assert row["broker_order_id"] == "TR-ENTRY-1"
    assert row["meta"]["submit_intent_at"]
    assert (
        row["meta"]["final_market_validity"]["quote_freshness_code"]
        == GateOutcome.CURRENT_PRICE_FRESH_SYNC_FETCH
    )


def test_332_broker_ready_recovery_owner_generation_reaches_fresh_sync_gate(monkeypatch):
    row, broker, watched, core = _run_real_live_submit(monkeypatch)

    broker.session.post.assert_called_once()
    core._cleanup_pending_entry_order.assert_not_called()
    assert row["status"] == "SUBMITTED"
    assert row["broker_order_id"] == "TR-ENTRY-1"
    ownership_context = watched.signal["_callback_ownership_context"]
    assert ownership_context["is_recovered"] is True
    assert ownership_context["ownership_kind"] == "broker_ready_recovery"
    assert ownership_context["owner"] == "broker-ready-owner-1"
    assert ownership_context["generation"] == 7
    assert (
        row["meta"]["final_market_validity"]["quote_freshness_code"]
        == GateOutcome.CURRENT_PRICE_FRESH_SYNC_FETCH
    )


def test_all_pass_audit_failure_does_not_cancel_valid_broker_submit(monkeypatch):
    row, broker, _watched, _core = _run_real_live_submit(
        monkeypatch,
        fail_all_pass_audit=True,
    )

    broker.session.post.assert_called_once()
    assert row["status"] == "SUBMITTED"
