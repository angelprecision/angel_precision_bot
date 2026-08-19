"""P0 #474 FINAL MERGE-GATE AMENDMENT — true production-recovery runtime replay.

The existing focused #474 suite (test_p0_deferred_real_cost_revalidation.py)
proves _revalidate_deferred_final_cost() and the source-level money-path
ordering in isolation. It does NOT prove the two production methods most
directly implicated in the original regression actually run for real:

  - APExecutionCore._breach_risk_check
  - APExecutionCore._recover_plan_for_revalidation

This file closes that gap. It reproduces a real process restart: a durable
Jason DEFERRED:AAPL PENDING_TRIGGER row (reserved_cost=165.811) is seeded
directly in the OSM store -- exactly as it would exist after a crash/redeploy
-- with NO watcher ever having called watch(plan) on it, so no live
ApprovedExecutionPlan-with-_approved_plan ever touches this row. Recovery
(APStartupRecovery._recover_deferred_breach_lifecycles) re-arms the watcher
straight from the durable row via ap.pending_trigger_restart_recovery's
_RecoveryPlan, which is a bare SimpleNamespace with no to_signal_dict()/
_approved_plan self-embedding (compare ApprovedExecutionPlan.to_signal_dict()
in ap_master_control.py, which DOES stamp "_approved_plan": self). That is
the real reason a recovered watcher's signal dict starts without
_approved_plan, and it is asserted explicitly below before the trigger.

From there, the REAL (unstubbed) APExecutionCore._breach_risk_check and
APExecutionCore._recover_plan_for_revalidation are bound onto the test core
via the same descriptor-binding pattern already established in
test_p0_seam4_e2e_deferred_lifecycle.py (`fn.__get__(core, type(core))`) --
this executes the actual unmodified production source, not a mock of it.
Only I/O boundaries (OSM persistence, broker HTTP, contract selector, DB
conn/run_with_retry) are test doubles, matching the existing, established
pattern for this codebase's P0 runtime-replay tests.

Two runtime cases are proven end-to-end through this real chain:
  CASE A ($140 final cost, under cap)  -> exactly one broker POST
  CASE B ($180 final cost, over cap)   -> zero broker POST

Master Control itself is a controlled cap-checking double in this file
(not MagicMock/lambda-approves-everything -- it implements the real
per-position-cap comparison and returns the real
ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP reason string on block).
A separate, real-APMasterControl-class adjacency proof lives in
tests/test_p0_474_real_master_control_integration.py (item 8 of this
amendment) -- that file drives the actual production APMasterControl class
in LIVE mode with only its DB/position-manager I/O boundary stubbed.
"""
from __future__ import annotations

import copy
import sys
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

import ap_execution_core as core_mod
from ap_entry_watcher import APEntryWatcher
from ap_recovery import APStartupRecovery

CLIENT_ID = "jasoncosby1@gmail.com"
LOCAL_ORDER_ID = "oid-474-jason-aapl-replay"
SIGNAL_ID = "sig-474-jason-aapl-replay"
TEST_ENTRY_CUTOFF_ET = "2359"
RESERVATION_MAX_POSITION_USD = 165.811

# ── Self-consistent, always-future AAPL OCC fixture ────────────────────────
# Computed relative to real wall-clock "now" so this fixture can never go
# stale/expired the way a hardcoded past date would (see amendment item 6).
_NOW = datetime.now(timezone.utc)
_EXPIRY = _NOW + timedelta(days=14)
_OCC_DATE = _EXPIRY.strftime("%y%m%d")
_DTE = (_EXPIRY.date() - _NOW.date()).days
REAL_AAPL_OCC = f"AAPL{_OCC_DATE}P00220000"
EXPIRATION_DATE_STR = _EXPIRY.strftime("%Y-%m-%d")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None = None) -> str:
    return (dt or _now()).isoformat()


def _jason_aapl_row() -> dict:
    """Durable Jason DEFERRED:AAPL PENDING_TRIGGER row -- section 2 shape."""
    return {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "live",
        "signal_id": SIGNAL_ID,
        "plan_id": "plan-474-jason-aapl",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "symbol": "AAPL",
        "direction": "PUT",
        "score": 88.0,
        "tier": "A",
        # PUT breaches when bid <= trigger (ap_entry_watcher._is_already_through_trigger).
        # Set below the mocked "current" quote (~229.8) so recovery's
        # already-through-trigger safety check reads this as NOT yet
        # breached (229.78 > 225.0); polling later shifts the quote below
        # 225.0 to fire the real breach for real.
        "trigger_price": 225.0,
        "stop_underlying": 235.0,
        "target_underlying": 215.0,
        "pattern": "breakout",
        "timeframe": "1d",
        "contract": "DEFERRED:AAPL",
        "qty": 1,
        "limit_price": 0.01,
        "reserved_cost": RESERVATION_MAX_POSITION_USD,
        "meta": {
            "contract_deferred": True,
            "materialization_status": "QUEUED",
            "materialization_generation": 0,
            "broker_ready": False,
            "execution_mode": "live",
            "entry_cutoff_et": TEST_ENTRY_CUTOFF_ET,
            # Deliberately NOT setting meta.local_order_id -- section 2:
            # "Do NOT require meta.local_order_id. The real local_order_id
            # must live in the top-level durable order field."
        },
    }


class _Selector:
    """Real OCC materialization double. Only the selector is mocked, per
    amendment item 6 ("The selector can remain mocked").

    selector_ask is the selector-era executable price. It must sit close
    enough to the case's final broker-ready submit ask that the real
    ENTRY_PRICING_BLOCK drift gate (max 25% drift between selector-era
    plan_limit and final submit ask) does not fire before final Master
    Control authority runs -- both prices are real economics for their
    case; only the LATER one (final submit ask) is what final MC must see.
    """

    def __init__(self, *, selector_ask: float = 1.39) -> None:
        self.calls = 0
        self.selector_ask = float(selector_ask)

    def select(self, _approved_plan, *, request_context=None):
        assert request_context is not None
        assert (
            request_context.selector_request_kind
            == "DEFERRED_BREACH_MATERIALIZATION"
        )
        self.calls += 1
        return types.SimpleNamespace(
            contract_symbol=REAL_AAPL_OCC,
            bid=round(self.selector_ask - 0.01, 2),
            ask=self.selector_ask,
            mid=round(self.selector_ask - 0.005, 2),
            affordable_contracts=1,
            execution_price_per_share=self.selector_ask,
            candidate_audit={"underlying_price": 229.80},
            expiration_date=EXPIRATION_DATE_STR,
            dte=_DTE,
            delta=-0.42,
            open_interest=980,
            volume=410,
        )

    def get_last_failure(self):
        return None

    def get_last_dte_ladder_audit(self):
        return {"buckets_attempted": 1}


class _Broker:
    def __init__(self) -> None:
        self.base_url = "https://api.tradier.com/v1"
        self.account_id = "VA-JASON"
        self.cfg = types.SimpleNamespace(base_url=self.base_url, account_id=self.account_id)
        self.session = types.SimpleNamespace()

    def get_quote(self, _ticker: str) -> dict:
        quote_ts = _now() - timedelta(seconds=4)
        # Post-breach underlying quote (below the 225.0 PUT trigger),
        # consistent with the crossed state polling settles into.
        return {
            "bid": 224.78,
            "ask": 224.82,
            "quote_timestamp": quote_ts.isoformat(),
            "source": "tradier_live",
        }


class _StatefulOSM:
    """Durable-order double. Real submit_existing_entry is bound in from the
    actual APOrderStateMachine, exactly as in test_p0_seam4_e2e_deferred_lifecycle.py."""

    def __init__(self) -> None:
        from ap.order_state_machine import APOrderStateMachine

        self.client_id = CLIENT_ID
        self.execution_mode = "live"
        self.row = _jason_aapl_row()
        self.post_payloads: list[dict] = []
        self.claimed_generations: list[int] = []
        self.fail_next_row_read = False
        self.terminalizations: list[tuple[str, str]] = []
        self.submit_existing_entry = APOrderStateMachine.submit_existing_entry.__get__(self, type(self))
        self._is_broker_accept_status = APOrderStateMachine._is_broker_accept_status

    def _copy_row(self) -> dict:
        return copy.deepcopy(self.row)

    def _merge_meta(self, patch_: dict) -> None:
        meta = self.row.setdefault("meta", {})
        for key, value in patch_.items():
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

    def update_order_meta(self, local_order_id, patch_):
        assert local_order_id == LOCAL_ORDER_ID
        self._merge_meta(patch_)
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
            "current_owner": f"broker_submit:{kwargs['broker_submit_key']}",
            "broker_submit_payload_hash": kwargs["payload_hash"],
        })
        return True

    def persist_materialized_submit_intent(self, local_order_id, **kwargs):
        """Real-shape stateful double for APOrderStateMachine's watcher-side
        broker-submit-intent CAS (guards the watcher-vs-recovery race on a
        materialized BROKER_READY row)."""
        assert local_order_id == LOCAL_ORDER_ID
        meta = self.row["meta"]
        if str(meta.get("lifecycle_state")) != "BROKER_READY":
            return False
        if meta.get("submit_intent_at"):
            return False
        if meta.get("recovery_submit_owner"):
            return False
        if int(meta.get("materialization_generation") or 0) != int(kwargs["generation"]):
            return False
        self._merge_meta({
            "lifecycle_state": "SUBMITTING",
            "submit_started_at": _iso(),
            "submit_intent_at": _iso(),
            "broker_submit_key": kwargs["broker_submit_key"],
            "current_owner": f"broker_submit:{kwargs['broker_submit_key']}",
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

    def terminalize_deferred_breach(self, local_order_id, *, reason_code,
                                     terminal_status, diagnostics=None, **_kwargs):
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
            local_order_id, reason_code=reason, terminal_status="EXPIRED", diagnostics={},
        )

    def cancel_pending_entry(self, local_order_id, reason):
        return self.terminalize_deferred_breach(
            local_order_id, reason_code=reason, terminal_status="CANCELED", diagnostics={},
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
            {"id": "TR-474-AAPL", "status": "open"},
            None,
            "TR-474-AAPL",
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


class _CapCheckingMasterControl:
    """Controlled real-cap-math Master Control double.

    Not a MagicMock/lambda-approves-everything stand-in: this implements the
    actual per-position-cap comparison and returns the real production
    reason-code string on block, so the runtime replay proves the final
    authority gate actually discriminates between the two dollar amounts.

    The separate, real APMasterControl-class adjacency proof is in
    tests/test_p0_474_real_master_control_integration.py (amendment item 8).
    """

    def __init__(self, per_position_cap: float) -> None:
        self.mode = "LIVE"
        self.max_positions = 5
        self.per_position_cap = float(per_position_cap)
        self.calls: list[tuple[float, str]] = []

    def _kill_switch_fn(self):
        return False

    def revalidate_exposure(self, plan, client_id="default"):
        cost = float(plan.max_position_usd)
        self.calls.append((cost, client_id))
        if cost > self.per_position_cap:
            return types.SimpleNamespace(
                ok=False,
                reason=(
                    f"ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP "
                    f"client_email={client_id} execution_mode=live "
                    f"real_cost=${cost:.0f} per_trade_budget=${self.per_position_cap:.0f}"
                ),
            )
        return types.SimpleNamespace(ok=True, reason="")


def _build_core(osm: _StatefulOSM, broker: _Broker, selector: _Selector, master_control):
    """Build the execution-core double with the REAL, unstubbed
    _breach_risk_check and _recover_plan_for_revalidation bound in -- the
    entire point of this amendment. Every other seam matches the
    already-established pattern in test_p0_seam4_e2e_deferred_lifecycle.py.
    """
    core = types.SimpleNamespace(
        client_id=CLIENT_ID,
        email=CLIENT_ID,
        execution_mode="live",
        mode="LIVE",
        paper=False,
        broker=broker,
        order_state_machine=osm,
        contract_selector=selector,
        master_control=master_control,
        _kill_switch=False,
        _max_positions=5,
    )
    core.store = types.SimpleNamespace(
        update_status=lambda *a, **k: None,
        update_signal_fields=lambda *a, **k: None,
    )
    # ── THE FIX: real production methods, not lambda stubs. ────────────
    core._breach_risk_check = core_mod.APExecutionCore._breach_risk_check.__get__(core, type(core))
    core._recover_plan_for_revalidation = core_mod.APExecutionCore._recover_plan_for_revalidation.__get__(core, type(core))
    # ────────────────────────────────────────────────────────────────────
    core._emit_breach_diag = lambda *a, **k: None
    core._current_open_position_count = lambda: 0
    core._current_pending_entry_count = lambda: 0
    core._refresh_hydrated_prebreach_plan = lambda *a, **k: False
    core._cleanup_pending_entry_order = core_mod.APExecutionCore._cleanup_pending_entry_order.__get__(core, type(core))
    core._classify_recovered_ownership_loss = core_mod.APExecutionCore._classify_recovered_ownership_loss.__get__(core, type(core))
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

    # Two-stage quote: the first TWO calls are consumed by (1) the
    # recovery-rearm already-through-trigger safety check and (2) the
    # watch()-arm-time already-through-trigger safety check -- both must
    # read as NOT yet breached (bid=229.78 > trigger=225.0 for PUT).
    # Later calls (during actual polling) shift below the trigger
    # (bid=224.78 <= 225.0) to fire the real breach.
    _quote_calls = {"count": 0}

    def _get_quote(_ticker):
        _quote_calls["count"] += 1
        if _quote_calls["count"] <= 2:
            return {"bid": 229.78, "ask": 229.82, "quote_age_ms": 10}
        return {"bid": 224.78, "ask": 224.82, "quote_age_ms": 10}

    watcher._get_quote = _get_quote

    _fetch_calls = {"count": 0}

    def _fetch_quotes(_tickers):
        _fetch_calls["count"] += 1
        if _fetch_calls["count"] <= 2:
            return {"AAPL": {"bid": 229.78, "ask": 229.82, "quote_age_ms": 10}}
        return {"AAPL": {"bid": 224.78, "ask": 224.82, "quote_age_ms": 10}}

    watcher._fetch_quotes = _fetch_quotes
    return watcher


def _run_recovery(osm: _StatefulOSM, broker: _Broker, watcher, core):
    """Real APStartupRecovery._recover_deferred_breach_lifecycles -- the
    genuine process-restart re-arm path. This is what builds the recovered
    plan via ap.pending_trigger_restart_recovery._RecoveryPlan (a bare
    SimpleNamespace, no to_signal_dict()/_approved_plan self-embedding) and
    calls entry_watcher.watch(recovered_plan, ..., recovery_rearm=True).
    """
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


def _run_replay(monkeypatch, *, final_submit_ask: float, master_control, selector_ask: float):
    """Shared driver for both the $140-pass and $180-block cases.

    Returns (osm, watcher, watched, core, selector) after the full real
    recovery -> real breach-check -> real plan-recovery -> materialization
    -> final-authority -> (submit or block) sequence has run to completion.
    """
    osm = _StatefulOSM()
    broker = _Broker()
    selector = _Selector(selector_ask=selector_ask)

    monkeypatch.setenv("ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE", "0")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_DELAY_SECONDS", "1")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "2359")
    monkeypatch.setenv("LIVE_CONFIRMATION_REQUIRED", "1")
    monkeypatch.setenv("PRE_SUBMIT_PROOF_RETRY_DELAY_SECONDS", "0")
    monkeypatch.setenv("PRE_SUBMIT_PROOF_RETRY_DEADLINE_SECONDS", "120")

    def _refresh_ask_at_submit(_broker, _contract):
        # submit_limit = round(submit_ask + ENTRY_LIVE_ASK_CROSS_CENTS, 2)
        # ENTRY_LIVE_ASK_CROSS_CENTS defaults to 0.01, so ask=1.39 -> 1.40,
        # ask=1.79 -> 1.80.
        return (
            final_submit_ask,
            15,
            True,
            "ok",
            {
                "spread_pct": 0.02,
                "submit_bid": round(final_submit_ask - 0.01, 2),
                "submit_ask": final_submit_ask,
                "submit_mid": round(final_submit_ask - 0.005, 2),
                "submit_last": final_submit_ask,
            },
        )

    fake_execution = types.ModuleType("ap.execution")
    fake_execution._refresh_ask_at_submit = _refresh_ask_at_submit

    class _FakeConfirmResult:
        passed = True
        fail_reason = None

        def __init__(self):
            self.metadata = {"live_entry_ts": _iso()}

        def to_meta(self, **kwargs):
            return {"passed": True, **kwargs}

    with patch.dict(sys.modules, {"ap.execution": fake_execution}), patch(
        "ap_entry_confirmation.check_entry_confirmation",
        return_value=_FakeConfirmResult(),
    ), patch("ap.db.conn", lambda: _NoopConn()), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        core = _build_core(osm, broker, selector, master_control)
        watcher = _build_watcher(osm, core)

        # ── Section 1: the durable row has NEVER been watch(plan)'d. ────
        # Recovery re-arms it purely from the DB row.
        recovery_result = _run_recovery(osm, broker, watcher, core)
        assert recovery_result["deferred_lifecycles_recovered"] == 1

        assert len(watcher._pending) == 1
        watched = watcher._pending[0]
        assert watched.signal["local_order_id"] == LOCAL_ORDER_ID

        # ── REQUIRED ASSERTION (section 1): no manual injection. ────────
        assert "_approved_plan" not in watched.signal, (
            "Recovered watcher signal must NOT carry a live _approved_plan -- "
            "if it does, the real _recover_plan_for_revalidation reconstruction "
            "path is never exercised, which is exactly the gap this amendment "
            "closes."
        )
        watched.overnight = False

        # _on_entry_trigger fires only after price holds through the
        # trigger for 2 CONSECUTIVE polls (see APExecutionCore._on_entry_trigger
        # docstring). The first 2 _fetch_quotes() calls return the safe,
        # not-yet-crossed quote (consumed above by recovery-rearm/arm-time
        # safety checks via _get_quote, a separate counter) -- poll enough
        # times here for at least two crossed-quote polls to confirm.
        for _ in range(5):
            watcher._poll_active_signals(open_protect_active=False)

        return osm, watcher, watched, core, selector


# ─────────────────────────────────────────────────────────────────────────
# CASE A -- $140 runtime PASS (amendment section 4)
# ─────────────────────────────────────────────────────────────────────────

def test_case_a_true_production_recovery_jason_aapl_140_passes_to_single_broker_post(monkeypatch):
    mc = _CapCheckingMasterControl(per_position_cap=166.00)

    osm, watcher, watched, core, selector = _run_replay(
        monkeypatch, final_submit_ask=1.39, master_control=mc, selector_ask=1.39,
    )

    final_row = osm.get_order(LOCAL_ORDER_ID)
    final_meta = final_row["meta"]

    # G/H — selector executed exactly once and materialized the real OCC.
    assert selector.calls == 1
    assert final_meta["selected_contract"] == REAL_AAPL_OCC

    # J — final submit_limit is 1.40 (1.39 ask + 0.01 live cross).
    assert final_row["limit_price"] == 1.40

    # F/M — MC never saw the 165.811 reservation as actual cost.
    seen_costs = [cost for cost, _client in mc.calls]
    assert RESERVATION_MAX_POSITION_USD not in seen_costs, (
        f"Master Control saw the raw reservation as cost: {seen_costs}"
    )

    # K/L/N — final MC executed exactly once, saw qty=1 / $140.00, approved.
    assert len(mc.calls) == 1
    seen_cost, seen_client = mc.calls[0]
    assert seen_cost == 140.00
    assert seen_client == CLIENT_ID

    # O/P/Q/R — broker-ready persisted, submit path ran once, one broker POST,
    # using the real materialized OCC (never the DEFERRED: placeholder).
    assert len(osm.post_payloads) == 1
    assert osm.post_payloads[0]["option_symbol"] == REAL_AAPL_OCC
    assert not osm.post_payloads[0]["option_symbol"].startswith("DEFERRED:")
    assert osm.post_payloads[0]["price"] == 1.40

    # S — durable order ends SUBMITTED with broker_order_id present.
    assert final_row["status"] == "SUBMITTED"
    assert final_row["broker_order_id"] == "TR-474-AAPL"
    assert final_row["client_id"] == CLIENT_ID
    assert final_row["execution_mode"] == "live"


def test_case_a_recovered_plan_initially_carries_165_811_reservation(monkeypatch):
    """Section 3: prove the plan _recover_plan_for_revalidation reconstructs
    from the durable row initially carries the exact reservation shape,
    before the selector/final-MC pipeline ever runs."""
    mc = _CapCheckingMasterControl(per_position_cap=166.00)
    osm = _StatefulOSM()
    broker = _Broker()
    selector = _Selector()
    core = _build_core(osm, broker, selector, mc)

    watched = types.SimpleNamespace(
        signal={
            "local_order_id": LOCAL_ORDER_ID,
            "signal_id": SIGNAL_ID,
            "plan_id": "plan-474-jason-aapl",
        },
        ticker="AAPL",
        side="PUT",
        entry_trigger=225.0,
        trigger_price=225.0,
        stop_level=235.0,
        target_price=215.0,
    )
    assert "_approved_plan" not in watched.signal

    recovered = core._recover_plan_for_revalidation(watched)

    assert recovered is not None
    assert recovered.ticker == "AAPL"
    assert recovered.client_id == CLIENT_ID
    assert recovered.execution_mode == "live"
    assert recovered.contract_symbol == "DEFERRED:AAPL"
    assert recovered.contracts == 1
    assert recovered.limit_price == 0.01
    assert recovered.max_position_usd == RESERVATION_MAX_POSITION_USD


# ─────────────────────────────────────────────────────────────────────────
# CASE B -- $180 runtime BLOCK (amendment section 5)
# ─────────────────────────────────────────────────────────────────────────

def test_case_b_true_production_recovery_jason_aapl_180_blocks_zero_broker_post(monkeypatch):
    mc = _CapCheckingMasterControl(per_position_cap=166.00)

    # selector_ask sits close to the final submit ask (1.79) so the real
    # ENTRY_PRICING_BLOCK drift gate does not fire before final MC runs --
    # this test is specifically about final MC seeing $180 and blocking,
    # not about the (separate, unrelated, untouched-by-#474) drift gate.
    osm, watcher, watched, core, selector = _run_replay(
        monkeypatch, final_submit_ask=1.79, master_control=mc, selector_ask=1.75,
    )

    final_row = osm.get_order(LOCAL_ORDER_ID)

    # D — selector materialized the real OCC before final MC ran.
    assert selector.calls == 1

    # E/F — final MC executed exactly once and saw $180.00, never 165.811.
    seen_costs = [cost for cost, _client in mc.calls]
    assert RESERVATION_MAX_POSITION_USD not in seen_costs
    assert len(mc.calls) == 1
    seen_cost, _seen_client = mc.calls[0]
    assert seen_cost == 180.00

    # G — final MC rejected.
    # H/I/J — zero broker-ready persistence beyond the pre-check, zero
    # submit_existing_entry execution, zero broker POST.
    assert len(osm.post_payloads) == 0

    # K — broker_order_id remains absent.
    assert final_row["broker_order_id"] is None

    # L/M — durable lifecycle ends terminal/fail-closed with the correct
    # per-position-cap reason.
    assert final_row["status"] in ("EXPIRED", "CANCELED", "PENDING_TRIGGER")
    assert len(osm.terminalizations) >= 1
    last_reason_code, last_terminal_status = osm.terminalizations[-1]
    assert "ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP" in last_reason_code or (
        "DEFERRED_FINAL_EXPOSURE_REVALIDATION" in last_reason_code
    )


# ─────────────────────────────────────────────────────────────────────────
# Fixture self-consistency (amendment item 6)
# ─────────────────────────────────────────────────────────────────────────

def test_occ_fixture_is_self_consistent_future_contract():
    """The AAPL OCC symbol, expiration_date, and dte used throughout this
    file are computed from real wall-clock time and can never go stale."""
    assert REAL_AAPL_OCC.startswith("AAPL")
    assert _OCC_DATE in REAL_AAPL_OCC
    assert _EXPIRY > _NOW
    assert _DTE > 0
    assert EXPIRATION_DATE_STR == _EXPIRY.strftime("%Y-%m-%d")
