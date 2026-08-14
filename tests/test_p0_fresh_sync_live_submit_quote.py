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
                "lifecycle_state": "SUBMITTING",
                "submit_intent_at": datetime.now(timezone.utc).isoformat(),
                "recovery_submit_owner": owner,
                "recovery_submit_generation": generation,
                "recovery_submit_execution_mode": execution_mode,
                "broker_submit_payload_hash": payload_hash,
                "broker_submit_key": broker_submit_key,
                "current_owner": f"broker_submit:{broker_submit_key}",
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
        "meta": {
            "trigger_crossed_at": (now - timedelta(seconds=2)).isoformat(),
            "contract_deferred": True,
            "broker_ready": True,
            "lifecycle_state": "BROKER_READY",
            "materialization_generation": 7,
            "recovery_submit_owner": "broker-ready-owner-1",
        },
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


class _FinalGateDeferredOSM:
    """Small durable-row model for the final-gate reversal replay."""

    def __init__(self, row):
        self.row = row
        self.client_id = row["client_id"]
        self.execution_mode = row["execution_mode"]
        self.copyback_contracts = []
        self.rearm_calls = []
        self.submit_calls = 0

    def _copy_row(self):
        return {**self.row, "meta": dict(self.row.get("meta") or {})}

    def get_order(self, _local_order_id):
        return self._copy_row()

    _get_order = get_order

    def update_order_meta(self, _local_order_id, patch):
        self.row["meta"].update(dict(patch or {}))
        return True

    def claim_deferred_materialization(self, local_order_id, **kwargs):
        self.row["meta"].update(
            {
                "lifecycle_state": "MATERIALIZING",
                "materialization_status": "RUNNING",
                "materialization_in_flight": True,
                "materialization_owner": kwargs["owner"],
                "watcher_token": kwargs["owner"],
                "current_owner": kwargs["owner"],
                "materialization_generation": kwargs["generation"],
                "materialization_lease_until": kwargs["lease_until"],
                "trigger_crossed_at": kwargs["trigger_crossed_at"],
                "trigger_price": kwargs["trigger_price"],
                "observed_underlying_price": kwargs["observed_underlying_price"],
                "signal_id": kwargs["signal_id"],
                "local_order_id": local_order_id,
                "client_id": self.client_id,
                "execution_mode": kwargs["execution_mode"],
                "broker_ready": False,
            }
        )
        return True

    def persist_selector_recovery_cursor(self, _local_order_id, *, cursor, **_kwargs):
        self.row["meta"]["selector_recovery_cursor_v1"] = dict(cursor or {})
        return True

    def persist_deferred_broker_ready(self, local_order_id, **kwargs):
        self.copyback_contracts.append(kwargs["contract"])
        self.row.update(
            {
                "contract": kwargs["contract"],
                "limit_price": kwargs["limit_price"],
                "qty": kwargs["qty"],
                "reserved_cost": kwargs["reserved_cost"],
                "contract_selection_status": "CONTRACT_SELECTED",
            }
        )
        self.row["meta"].update(
            {
                **dict(kwargs.get("selector_meta") or {}),
                "contract_deferred": False,
                "lifecycle_state": "BROKER_READY",
                "materialization_status": "SELECTED",
                "materialization_in_flight": False,
                "materialization_owner": kwargs["owner"],
                "current_owner": kwargs["owner"],
                "materialization_generation": kwargs["generation"],
                "broker_ready": True,
                "selected_contract": kwargs["contract"],
                "selected_limit": kwargs["limit_price"],
                "selected_qty": kwargs["qty"],
                "selected_reserved_cost": kwargs["reserved_cost"],
                "selector_recovery_cursor_v1": None,
                "signal_id": kwargs["signal_id"],
                "local_order_id": local_order_id,
                "client_id": self.client_id,
                "execution_mode": kwargs["execution_mode"],
            }
        )
        return True

    def rearm_deferred_materialization_direction_reversal(
        self,
        local_order_id,
        *,
        owner,
        watcher_token,
        generation,
        signal_id,
        execution_mode,
        market_truth_audit,
    ):
        current_meta = self.row["meta"]
        if (
            current_meta.get("materialization_owner") != owner
            or int(current_meta.get("materialization_generation") or 0) != generation
            or current_meta.get("signal_id") != signal_id
            or current_meta.get("execution_mode") != execution_mode
        ):
            return False
        self.rearm_calls.append(
            {
                "local_order_id": local_order_id,
                "owner": owner,
                "watcher_token": watcher_token,
                "generation": generation,
                "signal_id": signal_id,
                "execution_mode": execution_mode,
            }
        )
        self.row.update(
            {
                "contract": f"DEFERRED:{self.row['symbol']}",
                "limit_price": 0,
                "qty": 0,
                "reserved_cost": 0,
                "contract_selection_status": "DEFERRED_REARM",
            }
        )
        self.row["meta"].update(
            {
                "lifecycle_state": "",
                "materialization_status": "",
                "materialization_in_flight": False,
                "materialization_owner": "",
                "materialization_lease_until": "",
                "current_owner": watcher_token,
                "watcher_token": watcher_token,
                "watcher_generation": generation,
                "broker_ready": False,
                "contract_deferred": True,
                "contract_selection_status": "REARM_REQUIRED",
                "contract_symbol": "",
                "selected_contract": "",
                "selected_limit": 0,
                "selected_qty": 0,
                "selected_reserved_cost": 0,
                "limit_price": 0,
                "contracts": 0,
                "retry_attempt": 0,
                "breach_attempt_count": 0,
                "materialization_attempts": 0,
                "selector_recovery_cursor_v1": None,
                "final_market_truth_status": "REARM_DIRECTION_REVERSAL",
                "final_market_truth": dict(market_truth_audit or {}),
            }
        )
        for key in (
            "trigger_crossed_at",
            "trigger_crossed_at_provenance",
            "triggered_at",
            "trigger_confirmed_at",
            "last_confirmed_trigger_at",
            "original_trigger_crossed_at",
            "first_breach_bid",
            "first_breach_ask",
            "last_trigger_confirmation_quote",
        ):
            self.row["meta"].pop(key, None)
        return True

    def submit_existing_entry(self, *, local_order_id, broker, **_kwargs):
        self.submit_calls += 1
        if self.row["meta"].get("broker_ready") is not True:
            return {
                "ok": False,
                "local_order_id": local_order_id,
                "broker_order_id": None,
                "error": "MATERIALIZATION_DURABLE_STATE_MISMATCH:broker_ready",
            }
        broker.session.post(local_order_id, self.row["contract"])
        self.row["status"] = "SUBMITTED"
        self.row["broker_order_id"] = "TR-FINAL-REARM-1"
        self.row["meta"].update(
            {
                "lifecycle_state": "SUBMITTED",
                "broker_ready": False,
                "broker_order_id": "TR-FINAL-REARM-1",
            }
        )
        return {
            "ok": True,
            "local_order_id": local_order_id,
            "broker_order_id": "TR-FINAL-REARM-1",
            "status": "SUBMITTED",
        }


@pytest.mark.parametrize(
    ("side", "stop", "target", "final_quote", "contract"),
    [
        (
            "CALL",
            94.0,
            105.0,
            {"bid": 95.11, "ask": 95.14, "source": "tradier"},
            "CVS260821C00095000",
        ),
        (
            "PUT",
            96.0,
            85.0,
            {"bid": 95.16, "ask": 95.19, "source": "tradier"},
            "CVS260821P00095000",
        ),
    ],
)
def test_final_live_market_reversal_rearms_then_requires_fresh_rebreach(
    monkeypatch, side, stop, target, final_quote, contract
):
    """The post-copyback final gate must rearm, then a later breach may submit once."""
    from ap_entry_watcher import WatchState, WatchedSignal
    import ap_execution_core as core_module

    ticker = "CVS"
    client_id = "jasoncosby1@gmail.com"
    local_order_id = f"L-FINAL-REARM-{side}"
    signal_id = f"sig-final-rearm-{side.lower()}"
    canonical_signal_id = f"canonical-final-rearm-{side.lower()}"
    owner = f"watcher-token-{side.lower()}"
    trigger = 95.15
    row = {
        "local_order_id": local_order_id,
        "client_id": client_id,
        "execution_mode": "live",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "symbol": ticker,
        "ticker": ticker,
        "contract": f"DEFERRED:{ticker}",
        "qty": 0,
        "limit_price": 0,
        "reserved_cost": 0,
        "signal_id": signal_id,
        "direction": side,
        "trigger_price": trigger,
        "stop_price": stop,
        "target_price": target,
        "underlying_entry": trigger,
        "broker_order_id": None,
        "submitted_ts": None,
        "contract_selection_status": "DEFERRED",
        "meta": {
            "contract_deferred": True,
            "broker_ready": False,
            "lifecycle_state": "",
            "materialization_generation": 7,
            "retry_attempt": 0,
            "breach_attempt_count": 0,
            "materialization_attempts": 0,
        },
    }
    plan = SimpleNamespace(
        plan_id=f"plan-final-rearm-{side.lower()}",
        contract_symbol=f"DEFERRED:{ticker}",
        execution_price_per_share=1.25,
        ask=1.25,
        mid=1.225,
        affordable_contracts=1,
        premium_per_contract=125.0,
        contracts=1,
        max_position_usd=250.0,
        limit_price=1.25,
        side=side,
        direction=side,
        execution_mode="live",
        client_id=client_id,
        signal_id=signal_id,
        ticker=ticker,
        trigger_price=trigger,
        stop_underlying=stop,
        target_underlying=target,
        metadata={
            "contract_deferred": True,
            "canonical_signal_id": canonical_signal_id,
        },
    )
    watched = WatchedSignal(
        {
            "ticker": ticker,
            "side": side,
            "entry_price": trigger,
            "stop_price": stop,
            "target_price": target,
            "signal_id": signal_id,
            "local_order_id": local_order_id,
            "client_id": client_id,
            "execution_mode": "live",
            "watcher_token": owner,
            "canonical_signal_id": canonical_signal_id,
        }
    )
    first_breach_quote = (
        (95.16, 95.18) if side == "CALL" else (95.12, 95.14)
    )
    assert watched.check(*first_breach_quote) == WatchState.PENDING
    assert watched.check(*first_breach_quote) == WatchState.TRIGGERED

    osm = _FinalGateDeferredOSM(row)
    selector = MagicMock()
    selector.select.return_value = SimpleNamespace(
        contract_symbol=contract,
        execution_price_per_share=1.25,
        affordable_contracts=1,
        premium_per_contract=125.0,
        bid=1.20,
        ask=1.25,
        mid=1.225,
        pricing_basis="ask",
        expiration_date="2026-08-21",
        strike=95.0,
        option_type=side,
        dte=8,
        delta=0.5,
        open_interest=1000,
        volume=100,
        candidate_audit={"candidates_considered": 1},
    )
    selector.get_last_failure.return_value = None
    selector.get_last_dte_ladder_audit.return_value = {}
    selector.data_broker = MagicMock()
    selector.data_broker.get_quote.return_value = final_quote

    broker = SimpleNamespace(
        cfg=SimpleNamespace(
            base_url="https://api.tradier.com",
            account_id="LIVE-ACCOUNT",
        ),
        base_url="https://api.tradier.com",
        account_id="LIVE-ACCOUNT",
        get_quote=MagicMock(return_value=final_quote),
        session=MagicMock(),
    )
    core = core_module.APExecutionCore.__new__(core_module.APExecutionCore)
    core.paper = False
    core.mode = core.execution_mode = "LIVE"
    core.client_id = core.email = client_id
    core.broker = broker
    core.store = MagicMock()
    core.position_manager = MagicMock()
    core.position_manager.snapshot.return_value = {
        "open_count": 0,
        "pending_entries": 0,
    }
    core._max_positions = 5
    core.contract_selector = selector
    core.order_state_machine = osm
    core._breach_risk_check = MagicMock(return_value=True)
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)
    core._refresh_hydrated_prebreach_plan = MagicMock(return_value=False)
    core._cleanup_pending_entry_order = MagicMock(
        side_effect=AssertionError("market reversal must rearm, not cleanup")
    )

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
    monkeypatch.setenv("SELECTOR_DURABLE_RECOVERY_CURSOR_ENABLED", "1")
    monkeypatch.setenv("INTELLIGENCE_EVIDENCE_ENABLED", "0")
    monkeypatch.setenv("DEFERRED_SMALL_ACCOUNT_FALLBACK", "0")

    first_result = core_module.APExecutionCore._on_entry_trigger(core, watched)

    assert first_result["disposition"] == "KEEP_WATCHER"
    assert first_result["reason_code"] == "REARM_DIRECTION_REVERSAL"
    assert first_result["expected_client_id"] == client_id
    assert first_result["expected_execution_mode"] == "live"
    assert first_result["expected_signal_id"] == signal_id
    assert first_result["expected_canonical_signal_id"] == canonical_signal_id
    assert first_result["expected_generation"] == 8
    assert selector.select.call_count == 1
    assert osm.copyback_contracts == [contract]
    assert osm.rearm_calls == [
        {
            "local_order_id": local_order_id,
            "owner": owner,
            "watcher_token": owner,
            "generation": 8,
            "signal_id": signal_id,
            "execution_mode": "live",
        }
    ]
    assert broker.get_quote.call_count == 1
    assert broker.session.post.call_count == 0
    assert osm.submit_calls == 0
    assert row["status"] == "PENDING_TRIGGER"
    assert row["broker_order_id"] is None
    assert row["contract"] == f"DEFERRED:{ticker}"
    assert row["limit_price"] == 0
    assert row["qty"] == 0
    assert row["reserved_cost"] == 0
    assert row["contract_selection_status"] == "DEFERRED_REARM"
    assert row["meta"]["broker_ready"] is False
    assert row["meta"]["contract_deferred"] is True
    assert row["meta"]["lifecycle_state"] == ""
    assert row["meta"]["materialization_generation"] == 8
    assert row["meta"]["selector_recovery_cursor_v1"] is None
    assert row["meta"]["selected_contract"] == ""
    assert row["meta"]["selected_limit"] == 0
    assert row["meta"]["selected_qty"] == 0
    assert row["meta"]["final_market_truth_status"] == "REARM_DIRECTION_REVERSAL"
    assert row["meta"]["final_market_truth"]["reason_code"] in {
        "CALL_NO_LONGER_ABOVE_TRIGGER",
        "PUT_NO_LONGER_BELOW_TRIGGER",
    }
    assert core.position_manager.mock_calls == []
    assert not any("proof" in str(call).lower() for call in core.store.mock_calls)
    assert watched.trigger_price is None
    assert watched.trigger_crossed_at is None
    assert watched.breach_count == 0
    assert watched.signal["signal_id"] == signal_id
    assert watched.signal["local_order_id"] == local_order_id
    assert watched.signal["client_id"] == client_id
    assert watched.signal["execution_mode"] == "live"
    assert watched.signal["canonical_signal_id"] == canonical_signal_id
    assert watched.signal["contract_symbol"] == f"DEFERRED:{ticker}"
    assert watched.signal["contract_deferred"] is True
    assert plan.contract_symbol == f"DEFERRED:{ticker}"
    assert plan.limit_price == 0
    assert plan.contracts == 0

    # The old materialized OCC cannot be submitted from the reset row, even
    # if a caller still presents the pre-reversal plan object.
    stale_submit = osm.submit_existing_entry(
        local_order_id=local_order_id,
        broker=broker,
        plan=plan,
        limit_price=1.25,
    )
    assert stale_submit["ok"] is False
    assert "MATERIALIZATION_DURABLE_STATE_MISMATCH" in stale_submit["error"]
    assert broker.session.post.call_count == 0

    # The watcher loop changes the callback result back to PENDING.  A single
    # observation of the old reversal quote cannot fire again; two fresh
    # direction-confirming polls are required before the core is re-entered.
    watched.state = WatchState.PENDING
    pretrigger_bid_ask = (
        (95.11, 95.14) if side == "CALL" else (95.16, 95.19)
    )
    assert watched.check(*pretrigger_bid_ask) == WatchState.PENDING
    assert selector.select.call_count == 1
    assert watched.check(*first_breach_quote) == WatchState.PENDING
    assert watched.check(*first_breach_quote) == WatchState.TRIGGERED
    assert selector.select.call_count == 1

    # Feed the now-confirmed fresh breach through the full callback again.
    broker.get_quote.reset_mock()
    broker.get_quote.return_value = {
        "bid": first_breach_quote[0],
        "ask": first_breach_quote[1],
        "source": "tradier",
    }
    core_module.APExecutionCore._on_entry_trigger(core, watched)

    assert core._breach_risk_check.call_count == 2
    assert selector.select.call_count == 2
    assert broker.session.post.call_count == 1
    assert osm.submit_calls == 2  # one stale-row guard, one current submit
    assert row["status"] == "SUBMITTED"
    assert row["broker_order_id"] == "TR-FINAL-REARM-1"
    assert row["meta"]["materialization_generation"] == 9


@pytest.mark.parametrize(
    ("side", "bid", "ask", "stop", "target", "reason"),
    [
        ("CALL", 93.8, 94.2, 95.0, 110.0, GateOutcome.CALL_STOP_ALREADY_BROKEN),
        ("PUT", 96.2, 96.5, 96.0, 85.0, GateOutcome.PUT_STOP_ALREADY_BROKEN),
        ("CALL", 105.1, 105.3, 95.0, 105.0, GateOutcome.TARGET_ALREADY_INVALID),
        ("PUT", 84.7, 84.9, 96.0, 85.0, GateOutcome.TARGET_ALREADY_INVALID),
        (
            "CALL",
            104.6,
            104.8,
            95.0,
            105.0,
            GateOutcome.REMAINING_OPPORTUNITY_TOO_SMALL,
        ),
    ],
)
def test_final_market_terminal_geometry_does_not_classify_as_rearm(
    side, bid, ask, stop, target, reason
):
    from ap.live_submit_gates import MarketTruthAuthority, classify_market_truth

    result = _fresh_sync(
        side=side,
        trigger_price=95.15,
        stop_price=stop,
        target_price=target,
        current_bid=bid,
        current_ask=ask,
    )

    assert result.passed is False
    assert result.reason_code == reason
    assert classify_market_truth(result) == MarketTruthAuthority.TERMINAL_SETUP_COMPLETE


@pytest.mark.parametrize("generation", [True, 0, -1, 1.5, "1.5", "bad", None])
def test_final_rearm_rejects_malformed_generation_without_sql(monkeypatch, generation):
    import ap.order_state_machine as osm_mod
    from ap.order_state_machine import APOrderStateMachine

    class _NoSqlCursor:
        rowcount = 1
        executed = False

        def execute(self, *_args):
            self.executed = True
            return self

    class _NoSqlConn:
        def __init__(self, cursor):
            self.cursor = cursor

        def __enter__(self):
            return self.cursor

        def __exit__(self, *_args):
            return False

    cursor = _NoSqlCursor()
    monkeypatch.setattr(osm_mod, "conn", lambda: _NoSqlConn(cursor))
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())
    osm = object.__new__(APOrderStateMachine)
    osm.client_id = "jasoncosby1@gmail.com"

    assert osm.rearm_deferred_materialization_direction_reversal(
        "order-1",
        owner="watcher-token",
        watcher_token="watcher-token",
        generation=generation,
        signal_id="sig-1",
        execution_mode="live",
        market_truth_audit={"reason": "CALL_NO_LONGER_ABOVE_TRIGGER"},
    ) is False
    assert cursor.executed is False
