from __future__ import annotations

import copy
import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://dummy:dummy@127.0.0.1:1/dummy")

import ap_execution_core as core_mod
from ap.order_state_machine import APOrderStateMachine
from ap_entry_watcher import APEntryWatcher
from ap_recovery import APStartupRecovery


CLIENT_ID = "jason@example.com"
LOCAL_ORDER_ID = "oid-pr323-e2e-1"
SIGNAL_ID = "sig-pr323-e2e-1"
REAL_OCC = "SPY260717C00600000"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None = None) -> str:
    return (dt or _now()).isoformat()


def _row() -> dict:
    return {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "live",
        "signal_id": SIGNAL_ID,
        "plan_id": "plan-pr323-e2e-1",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "symbol": "SPY",
        "direction": "CALL",
        "score": 92.0,
        "tier": "A",
        "trigger_price": 600.0,
        "stop_underlying": 595.0,
        "target_underlying": 605.0,
        "pattern": "breakout",
        "timeframe": "1d",
        "contract": "DEFERRED:SPY",
        "qty": 1,
        "limit_price": 0.01,
        "reserved_cost": 1.0,
        "meta": {
            "contract_deferred": True,
            "materialization_status": "QUEUED",
            "materialization_generation": 0,
            "broker_ready": False,
            "execution_mode": "live",
            "queue_id": 323,
            "entry_cutoff_et": "1530",
        },
    }


class _FakeConfirmResult:
    passed = True
    fail_reason = None

    def __init__(self) -> None:
        self.metadata = {"live_entry_ts": _iso()}

    def to_meta(self, **kwargs):
        return {"passed": True, **kwargs}


class _Selector:
    def __init__(self) -> None:
        self.calls = 0
        self._last_failure = None
        self.dte_ladder_enabled = True

    def select(self, _approved_plan):
        self.calls += 1
        if self.calls == 1:
            self._last_failure = {
                "reason_code": "CHAIN_ROW_ZERO_BID_ASK",
                "stage": "quality_filter",
                "explanation": "transient zero quote",
                "chain_rows": 12,
                "survivor_count": 0,
                "quote_source": "tradier_live",
                "tradier_base_url": "https://api.tradier.com/v1",
            }
            return None
        self._last_failure = None
        return types.SimpleNamespace(
            contract_symbol=REAL_OCC,
            bid=2.09,
            ask=2.10,
            mid=2.095,
            affordable_contracts=1,
            execution_price_per_share=2.10,
            candidate_audit={"underlying_price": 600.25},
            expiration_date="2026-07-17",
            dte=3,
            delta=0.44,
            open_interest=1200,
            volume=500,
        )

    def get_last_failure(self):
        return self._last_failure

    def get_last_dte_ladder_audit(self):
        return {"buckets_attempted": 1}


class _Broker:
    def __init__(self) -> None:
        self.base_url = "https://api.tradier.com/v1"
        self.account_id = "VA123"
        self.cfg = types.SimpleNamespace(base_url=self.base_url, account_id=self.account_id)
        self.session = types.SimpleNamespace()

    def get_quote(self, _ticker: str) -> dict:
        quote_ts = _now() - timedelta(seconds=4)
        return {
            "bid": 600.30,
            "ask": 600.32,
            "quote_timestamp": quote_ts.isoformat(),
            "source": "tradier_live",
        }


class _StatefulOSM:
    def __init__(self) -> None:
        self.client_id = CLIENT_ID
        self.execution_mode = "live"
        self.row = _row()
        self.post_payloads: list[dict] = []
        self.claimed_generations: list[int] = []
        self.fail_next_row_read = False
        self.proof_read_failures_remaining = 1
        self.terminalizations: list[tuple[str, str]] = []
        self.submit_existing_entry = APOrderStateMachine.submit_existing_entry.__get__(self, type(self))
        self._is_broker_accept_status = APOrderStateMachine._is_broker_accept_status

    def _copy_row(self) -> dict:
        return copy.deepcopy(self.row)

    def _merge_meta(self, patch: dict) -> None:
        meta = self.row.setdefault("meta", {})
        for key, value in patch.items():
            if isinstance(value, dict) and isinstance(meta.get(key), dict):
                meta[key].update(value)
            else:
                meta[key] = copy.deepcopy(value)

    def has_order(self, local_order_id):
        return local_order_id == LOCAL_ORDER_ID

    def get_order_by_signal(self, signal_id):
        return self._copy_row() if signal_id == SIGNAL_ID else None

    def _get_order(self, local_order_id):
        return self.get_order(local_order_id)

    def get_order(self, local_order_id):
        if local_order_id != LOCAL_ORDER_ID:
            return None
        if self.fail_next_row_read:
            self.fail_next_row_read = False
            raise RuntimeError("db_hiccup")
        return self._copy_row()

    def update_order_meta(self, local_order_id, patch):
        assert local_order_id == LOCAL_ORDER_ID
        self._merge_meta(patch)
        return True

    def claim_deferred_materialization(self, local_order_id, **kwargs):
        assert local_order_id == LOCAL_ORDER_ID
        self.claimed_generations.append(int(kwargs["generation"]))
        self._merge_meta({
            "materialization_generation": int(kwargs["generation"]),
            "materialization_owner": kwargs["owner"],
            "materialization_lease_until": kwargs["lease_until"],
            "materialization_status": "RUNNING",
            "lifecycle_state": "MATERIALIZING",
            "trigger_crossed_at": kwargs["trigger_crossed_at"],
            "trigger_price": kwargs["trigger_price"],
            "observed_underlying_price": kwargs["observed_underlying_price"],
            "execution_mode": "live",
            "signal_id": kwargs["signal_id"],
        })
        return True

    def schedule_deferred_materialization_retry(self, local_order_id, **kwargs):
        assert local_order_id == LOCAL_ORDER_ID
        self._merge_meta({
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_owner": kwargs["owner"],
            "materialization_generation": int(kwargs["generation"]),
            "retry_reason": kwargs["reason_code"],
            "retry_attempt": int(kwargs["attempt"]),
            "retry_max_attempts": int(kwargs["max_attempts"]),
            "next_retry_at": kwargs["next_retry_at"],
            "materialization_next_retry_at": kwargs["next_retry_at"],
            "selector_failure": copy.deepcopy(kwargs["selector_failure"]),
            "broker_ready": False,
        })
        return True

    def persist_deferred_broker_ready(self, local_order_id, **kwargs):
        assert local_order_id == LOCAL_ORDER_ID
        self.row["contract"] = kwargs["contract"]
        self.row["limit_price"] = float(kwargs["limit_price"])
        self.row["qty"] = int(kwargs["qty"])
        self.row["reserved_cost"] = float(kwargs["reserved_cost"])
        self._merge_meta({
            "contract_deferred": False,
            "lifecycle_state": "BROKER_READY",
            "materialization_status": "SELECTED",
            "materialization_owner": kwargs["owner"],
            "materialization_generation": int(kwargs["generation"]),
            "broker_ready": True,
            "selected_contract": kwargs["contract"],
            "selected_limit": float(kwargs["limit_price"]),
            "selected_qty": int(kwargs["qty"]),
            "selected_at": _iso(),
            "selected_quote_at": _iso(),
            "selector_meta": copy.deepcopy(kwargs["selector_meta"]),
        })
        if self.proof_read_failures_remaining > 0:
            self.fail_next_row_read = True
            self.proof_read_failures_remaining -= 1
        return True

    def persist_pre_submit_proof_retry(self, local_order_id, **kwargs):
        assert local_order_id == LOCAL_ORDER_ID
        self._merge_meta({
            "lifecycle_state": "PRE_SUBMIT_PROOF_RETRY",
            "proof_retry_owner": kwargs["owner"],
            "proof_retry_attempt": int(kwargs["retry_attempt"]),
            "proof_retry_max_attempts": int(kwargs["max_attempts"]),
            "proof_retry_next_at": kwargs["next_retry_at"],
            "proof_retry_deadline": kwargs["retry_deadline"],
            "absolute_entry_deadline": kwargs["retry_deadline"],
            "proof_retry_last_read_error": kwargs["read_error"],
            "selected_at": kwargs["selected_at"],
            "selected_quote_at": kwargs["selected_quote_at"],
            "broker_ready": True,
        })
        return True

    def claim_pre_submit_proof_retry(self, local_order_id, **kwargs):
        assert local_order_id == LOCAL_ORDER_ID
        meta = self.row["meta"]
        if str(meta.get("lifecycle_state")) != "PRE_SUBMIT_PROOF_RETRY":
            return False
        if int(meta.get("materialization_generation") or 0) != int(kwargs["expected_generation"]):
            return False
        self._merge_meta({
            "lifecycle_state": "BROKER_READY",
            "materialization_owner": kwargs["owner"],
            "materialization_generation": int(kwargs["new_generation"]),
            "proof_retry_attempt": int(kwargs["attempt"]),
            "proof_retry_claimed_at": kwargs["claimed_at"],
        })
        return True

    def claim_deferred_broker_ready_submit(self, local_order_id, **kwargs):
        assert local_order_id == LOCAL_ORDER_ID
        meta = self.row["meta"]
        if str(meta.get("lifecycle_state")) != "BROKER_READY":
            return False
        if int(meta.get("materialization_generation") or 0) != int(kwargs["generation"]):
            return False
        self._merge_meta({
            "recovery_submit_owner": kwargs["owner"],
            "recovery_submit_generation": int(kwargs["generation"]),
            "recovery_submit_lease_until": _iso(_now() + timedelta(seconds=30)),
        })
        return True

    def persist_deferred_submit_intent(self, local_order_id, **kwargs):
        assert local_order_id == LOCAL_ORDER_ID
        meta = self.row["meta"]
        if meta.get("submit_intent_at"):
            return False
        self._merge_meta({
            "lifecycle_state": "SUBMITTING",
            "submit_started_at": _iso(),
            "submit_intent_at": _iso(),
            "broker_submit_key": kwargs["broker_submit_key"],
            "current_owner": kwargs["owner"],
            "broker_submit_payload_hash": kwargs["payload_hash"],
        })
        return True

    def transition(self, local_order_id, to_status, **kwargs):
        assert local_order_id == LOCAL_ORDER_ID
        self.row["status"] = str(to_status)
        if "broker_order_id" in kwargs:
            self.row["broker_order_id"] = kwargs["broker_order_id"]
        if "submitted_ts" in kwargs:
            self.row["submitted_ts"] = kwargs["submitted_ts"]
        if "last_error" in kwargs:
            self.row["last_error"] = kwargs["last_error"]
        return True

    def terminalize_deferred_breach(
        self,
        local_order_id,
        *,
        reason_code,
        terminal_status,
        diagnostics=None,
        **_kwargs,
    ):
        assert local_order_id == LOCAL_ORDER_ID
        self.row["status"] = terminal_status
        self.row["last_error"] = reason_code
        self.terminalizations.append((reason_code, terminal_status))
        self._merge_meta({
            "lifecycle_state": terminal_status,
            "reason_code": reason_code,
            "final_reason": reason_code,
            "terminal_diagnostics": diagnostics or {},
            "current_owner": "",
            "broker_ready": False,
        })
        return True

    def expire_pending_entry(self, local_order_id, reason):
        return self.terminalize_deferred_breach(
            local_order_id,
            reason_code=reason,
            terminal_status="EXPIRED",
            diagnostics={},
        )

    def cancel_pending_entry(self, local_order_id, reason):
        return self.terminalize_deferred_breach(
            local_order_id,
            reason_code=reason,
            terminal_status="CANCELED",
            diagnostics={},
        )

    def _lookup_order_by_tag(self, *_args, **_kwargs):
        return None

    def _flag_split_brain_order(self, *_args, **_kwargs):
        return None

    def _emit_transition_event(self, **_kwargs):
        return None

    def _submit_order_with_retry(self, **kwargs):
        self.post_payloads.append(copy.deepcopy(kwargs["order_data"]))
        return (
            {"id": "TR-323", "status": "open"},
            None,
            "TR-323",
            "ACK",
        )


class _RecoveryCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, *_args, **_kwargs):
        return self

    def fetchall(self):
        return self.rows


class _RecoveryConn:
    def __init__(self, rows):
        self.cursor = _RecoveryCursor(rows)

    def __enter__(self):
        return self.cursor

    def __exit__(self, *_args):
        return False


class _NoopCursor:
    rowcount = 1

    def execute(self, *_args, **_kwargs):
        return self

    def fetchall(self):
        return []


class _NoopConn:
    def __enter__(self):
        return _NoopCursor()

    def __exit__(self, *_args):
        return False


def _approved_plan() -> types.SimpleNamespace:
    return types.SimpleNamespace(
        contract_symbol="DEFERRED:SPY",
        limit_price=0.01,
        contracts=1,
        max_position_usd=500.0,
        side="CALL",
        direction="CALL",
        execution_mode="live",
        client_id=CLIENT_ID,
        signal_id=SIGNAL_ID,
        trigger_price=600.0,
        underlying_price=600.0,
        stop_underlying=595.0,
        target_underlying=605.0,
        metadata={
            "contract_deferred": True,
            "queue_id": 323,
        },
        ticker="SPY",
        plan_id="plan-pr323-e2e-1",
        score=92.0,
        tier="A",
        timeframe="1d",
        pattern="breakout",
    )


def _build_core(osm: _StatefulOSM, broker: _Broker, selector: _Selector):
    core = types.SimpleNamespace(
        client_id=CLIENT_ID,
        email=CLIENT_ID,
        execution_mode="live",
        mode="LIVE",
        paper=False,
        broker=broker,
        order_state_machine=osm,
        contract_selector=selector,
        master_control=types.SimpleNamespace(mode="LIVE", max_positions=5, _kill_switch_fn=lambda: False),
        _kill_switch=False,
        _max_positions=5,
    )
    core.store = types.SimpleNamespace(
        update_status=lambda *a, **k: None,
        update_signal_fields=lambda *a, **k: None,
    )
    core._breach_risk_check = lambda watched: True
    core._recover_plan_for_revalidation = lambda watched: (
        ((getattr(watched, "signal", {}) or {}).get("_approved_plan"))
        or _approved_plan()
    )
    core._emit_breach_diag = lambda *a, **k: None
    core._current_open_position_count = lambda: 0
    core._current_pending_entry_count = lambda: 0
    core._refresh_hydrated_prebreach_plan = lambda *a, **k: False
    core._cleanup_pending_entry_order = core_mod.APExecutionCore._cleanup_pending_entry_order.__get__(core, type(core))
    core._is_real_occ_contract = core_mod.APExecutionCore._is_real_occ_contract
    core.resume_deferred_broker_ready_order = core_mod.APExecutionCore.resume_deferred_broker_ready_order.__get__(core, type(core))
    core._on_entry_trigger = core_mod.APExecutionCore._on_entry_trigger.__get__(core, type(core))
    return core


def _build_watcher(osm: _StatefulOSM, core):
    watcher = APEntryWatcher(None, order_state_machine=osm, mode="LIVE")
    _orig_watch = watcher.watch
    watcher.watch = lambda plan, local_order_id, **kwargs: _orig_watch(
        plan,
        local_order_id,
        recovery_rearm=kwargs.get("recovery_rearm", False),
        no_cancel_on_reject=kwargs.get("no_cancel_on_reject", False),
    )
    watcher.on_trigger = core._on_entry_trigger
    watcher._insert_watcher_audit_row = lambda *a, **k: None
    watcher._persist_watcher_audit = lambda *a, **k: None
    watcher._fetch_quotes = lambda _tickers: {
        "SPY": {
            "bid": 600.20,
            "ask": 600.22,
            "quote_age_ms": 10,
        }
    }
    return watcher


def _run_recovery(osm: _StatefulOSM, broker: _Broker, watcher, core):
    rec = APStartupRecovery(
        client_id=CLIENT_ID,
        broker=broker,
        osm=osm,
        pm=None,
        master_control=types.SimpleNamespace(mode="LIVE"),
        entry_watcher=watcher,
        execution_core=core,
    )
    result = {"deferred_lifecycles_recovered": 0}
    rows = [osm.get_order(LOCAL_ORDER_ID)]
    with patch("ap.db.conn", lambda: _RecoveryConn(rows)), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        rec._recover_deferred_breach_lifecycles(result)
    return result


def test_real_watcher_to_recovery_to_single_post_call_graph(monkeypatch):
    osm = _StatefulOSM()
    broker = _Broker()
    selector = _Selector()

    monkeypatch.setenv("ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE", "0")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_DELAY_SECONDS", "0")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "2359")
    monkeypatch.setenv("LIVE_CONFIRMATION_REQUIRED", "1")
    monkeypatch.setenv("PRE_SUBMIT_PROOF_RETRY_DELAY_SECONDS", "0")
    monkeypatch.setenv("PRE_SUBMIT_PROOF_RETRY_DEADLINE_SECONDS", "120")

    def _refresh_ask_at_submit(_broker, _contract):
        return (
            2.10,
            15,
            True,
            "ok",
            {
                "spread_pct": 0.02,
                "submit_bid": 2.09,
                "submit_ask": 2.10,
                "submit_mid": 2.095,
                "submit_last": 2.10,
            },
        )

    fake_execution = types.ModuleType("ap.execution")
    fake_execution._refresh_ask_at_submit = _refresh_ask_at_submit

    with patch.dict(sys.modules, {"ap.execution": fake_execution}), patch(
        "ap_entry_confirmation.check_entry_confirmation",
        return_value=_FakeConfirmResult(),
    ), patch("ap.db.conn", lambda: _NoopConn()), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        core1 = _build_core(osm, broker, selector)
        watcher1 = _build_watcher(osm, core1)
        assert watcher1.watch(_approved_plan(), LOCAL_ORDER_ID) is True
        for watched in watcher1._pending:
            watched.overnight = False

        watcher1._poll_active_signals(open_protect_active=False)
        watcher1._poll_active_signals(open_protect_active=False)

        row_after_first_trigger = osm.get_order(LOCAL_ORDER_ID)
        meta_after_first_trigger = row_after_first_trigger["meta"]
        assert meta_after_first_trigger["lifecycle_state"] == "RETRY_WAIT"
        assert meta_after_first_trigger["selector_failure"]["reason_code"] == "CHAIN_ROW_ZERO_BID_ASK"
        assert row_after_first_trigger["status"] == "PENDING_TRIGGER"
        assert row_after_first_trigger["broker_order_id"] is None

        core2 = _build_core(osm, broker, selector)
        watcher2 = _build_watcher(osm, core2)
        _run_recovery(osm, broker, watcher2, core2)
        assert LOCAL_ORDER_ID in [w.signal["local_order_id"] for w in watcher2._pending]
        for watched in watcher2._pending:
            watched.overnight = False

        watcher2._poll_active_signals(open_protect_active=False)
        watcher2._poll_active_signals(open_protect_active=False)

        row_after_second_trigger = osm.get_order(LOCAL_ORDER_ID)
        meta_after_second_trigger = row_after_second_trigger["meta"]
        assert row_after_second_trigger["contract"] == REAL_OCC
        assert row_after_second_trigger["limit_price"] > 0.01
        assert meta_after_second_trigger["selected_contract"] == REAL_OCC
        assert meta_after_second_trigger["lifecycle_state"] == "PRE_SUBMIT_PROOF_RETRY"
        assert meta_after_second_trigger["proof_retry_last_read_error"].startswith("read_error:")
        assert row_after_second_trigger["broker_order_id"] is None

        osm.fail_next_row_read = False
        core3 = _build_core(osm, broker, selector)
        watcher3 = _build_watcher(osm, core3)
        result_a = _run_recovery(osm, broker, watcher3, core3)
        result_b = _run_recovery(osm, broker, watcher3, core3)

        final_row = osm.get_order(LOCAL_ORDER_ID)
        final_meta = final_row["meta"]

        assert result_a["deferred_lifecycles_recovered"] == 1
        assert result_b["deferred_lifecycles_recovered"] == 0
        assert len(osm.post_payloads) == 1
        assert osm.post_payloads[0]["option_symbol"] == REAL_OCC
        assert osm.post_payloads[0]["price"] > 0.01
        assert not osm.post_payloads[0]["option_symbol"].startswith("DEFERRED:")
        assert final_row["status"] == "SUBMITTED"
        assert final_row["broker_order_id"] == "TR-323"
        assert final_row["client_id"] == CLIENT_ID
        assert final_row["execution_mode"] == "live"
        assert final_row["local_order_id"] == LOCAL_ORDER_ID
        assert final_meta["original_trigger_crossed_at"] == final_meta["trigger_crossed_at"]
        assert final_meta["last_confirmed_trigger_at"] != final_meta["original_trigger_crossed_at"]
        assert final_meta["live_submit_gate"]["all_passed"] is True
        assert final_meta["live_submit_gate"]["trigger_age_gate"]["effective_anchor"] == "last_confirmed_trigger_at"
        assert final_meta["proof_retry_deadline"] == final_meta["absolute_entry_deadline"]
        assert str(final_meta.get("selector_failure", {}).get("reason_code") or "") == "CHAIN_ROW_ZERO_BID_ASK"
        assert selector.calls == 2
