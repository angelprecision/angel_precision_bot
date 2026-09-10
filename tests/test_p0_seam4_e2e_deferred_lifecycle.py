from __future__ import annotations

import copy
import json
import os
import sys
import threading
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://dummy:dummy@127.0.0.1:1/dummy")

import ap_execution_core as core_mod
import ap.db as db_mod
import ap.order_state_machine as osm_mod
from ap.order_state_machine import APOrderStateMachine
from ap_entry_watcher import APEntryWatcher
from ap_recovery import APStartupRecovery


CLIENT_ID = "jason@example.com"
LOCAL_ORDER_ID = "oid-pr323-e2e-1"
SIGNAL_ID = "sig-pr323-e2e-1"
REAL_OCC = "SPY260717C00600000"
TEST_ENTRY_CUTOFF_ET = "2359"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None = None) -> str:
    return (dt or _now()).isoformat()


def _row() -> dict:
    return {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "canonical_signal_id": SIGNAL_ID,
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
            "entry_cutoff_et": TEST_ENTRY_CUTOFF_ET,
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

    def select(self, _approved_plan, *, request_context=None):
        assert request_context is not None
        assert (
            request_context.selector_request_kind
            == "DEFERRED_BREACH_MATERIALIZATION"
        )
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
        self.broker_ready_terminalization_calls: list[dict] = []
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

    def read_trigger_confirmation_authority(
        self,
        local_order_id,
        *,
        client_id,
        execution_mode,
        signal_id,
        canonical_signal_id,
        expected_materialization_generation=None,
    ):
        """Strict in-memory equivalent of the production durable readback."""
        row = self.get_order(local_order_id)
        if not row:
            return None
        meta = row.get("meta") or {}
        if (
            str(row.get("client_id") or "").strip().lower()
            != str(client_id or "").strip().lower()
            or str(row.get("execution_mode") or "").strip().lower()
            != str(execution_mode or "").strip().lower()
            or str(row.get("signal_id") or "").strip()
            != str(signal_id or "").strip()
            or str(
                row.get("canonical_signal_id")
                or meta.get("canonical_signal_id")
                or ""
            ).strip()
            != str(canonical_signal_id or "").strip()
            or str(row.get("status") or "").upper() != "PENDING_TRIGGER"
            or row.get("broker_order_id") not in (None, "")
            or row.get("submitted_ts") is not None
            or meta.get("submit_intent_at") not in (None, "")
            or meta.get("broker_ready") not in (None, False, "")
        ):
            return None
        crossed = meta.get("trigger_crossed_at")
        provenance = meta.get("trigger_crossed_at_provenance")
        if not isinstance(crossed, str) or not crossed.strip() or not isinstance(provenance, dict):
            return None
        expected_provenance = {
            "canonical_signal_id": str(canonical_signal_id or "").strip(),
            "client_id": str(client_id or "").strip().lower(),
            "execution_mode": str(execution_mode or "").strip().lower(),
            "local_order_id": str(local_order_id or "").strip(),
        }
        actual_provenance = {
            "canonical_signal_id": str(provenance.get("canonical_signal_id") or "").strip(),
            "client_id": str(provenance.get("client_id") or "").strip().lower(),
            "execution_mode": str(provenance.get("execution_mode") or "").strip().lower(),
            "local_order_id": str(provenance.get("local_order_id") or "").strip(),
        }
        if actual_provenance != expected_provenance:
            return None
        generation = meta.get("materialization_generation")
        if generation is not None and (
            isinstance(generation, bool) or not isinstance(generation, int) or generation < 1
        ):
            return None
        if expected_materialization_generation is not None and generation != expected_materialization_generation:
            return None
        try:
            parsed = datetime.fromisoformat(
                crossed[:-1] + "+00:00" if crossed.endswith("Z") else crossed
            )
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                return None
        except (TypeError, ValueError, OverflowError):
            return None
        return {
            "proven": True,
            "trigger_crossed_at": crossed,
            "trigger_crossed_at_provenance": copy.deepcopy(provenance),
            "materialization_generation": generation,
        }

    def update_order_meta(self, local_order_id, patch, **_expected):
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
            "materialization_in_flight": False,
            "materialization_owner": kwargs["owner"],
            "current_owner": kwargs["owner"],
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

    def terminalize_owned_broker_ready_materialization(
        self,
        local_order_id,
        *,
        reason,
        terminal_status="EXPIRED",
        owner,
        generation,
        retry_attempt,
        client_id,
        execution_mode,
        expected_direction,
        expected_contract_symbol,
        diagnostics=None,
    ):
        assert local_order_id == LOCAL_ORDER_ID
        self.broker_ready_terminalization_calls.append({
            "reason": reason,
            "terminal_status": terminal_status,
            "owner": owner,
            "generation": generation,
            "retry_attempt": retry_attempt,
            "client_id": client_id,
            "execution_mode": execution_mode,
            "expected_direction": expected_direction,
            "expected_contract_symbol": expected_contract_symbol,
            "diagnostics": diagnostics or {},
        })
        meta = self.row["meta"]
        if not (
            self.row.get("status") == "PENDING_TRIGGER"
            and self.row.get("client_id") == client_id
            and str(self.row.get("execution_mode") or "").lower() == str(execution_mode).lower()
            and self.row.get("direction") == expected_direction
            and self.row.get("contract") == expected_contract_symbol
            and not self.row.get("broker_order_id")
            and self.row.get("submitted_ts") is None
            and meta.get("lifecycle_state") == "BROKER_READY"
            and meta.get("materialization_status") == "SELECTED"
            and meta.get("materialization_in_flight") is False
            and meta.get("broker_ready") is True
            and meta.get("materialization_owner") == owner
            and meta.get("current_owner") == owner
            and int(meta.get("materialization_generation") or 0) == int(generation)
            and int(meta.get("retry_attempt") or 0) == int(retry_attempt)
            and not meta.get("submit_intent_at")
            and not meta.get("broker_submit_key")
            and not meta.get("recovery_submit_owner")
        ):
            return False
        self.row["status"] = terminal_status
        self.row["last_error"] = reason
        self.terminalizations.append((reason, terminal_status))
        self._merge_meta({
            "lifecycle_state": terminal_status,
            "materialization_status": "FAILED_TERMINAL",
            "materialization_in_flight": False,
            "materialization_owner": "",
            "current_owner": "",
            "broker_ready": False,
            "reason_code": reason,
            "final_reason": reason,
            "terminal_diagnostics": diagnostics or {},
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
        assert local_order_id == LOCAL_ORDER_ID
        self._merge_meta({
            "lifecycle_state": "SUBMITTING",
            "submit_started_at": _iso(),
            "submit_intent_at": _iso(),
            "broker_submit_key": kwargs.get("broker_submit_key"),
            "current_owner": f"broker_submit:{kwargs.get('broker_submit_key', '')}",
            "broker_submit_payload_hash": kwargs.get("payload_hash"),
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


def _broker_ready_row() -> dict:
    row = _row()
    row["contract"] = REAL_OCC
    row["qty"] = 1
    row["limit_price"] = 2.10
    row["reserved_cost"] = 210.0
    row["meta"] = {
        "contract_deferred": False,
        "materialization_status": "SELECTED",
        "materialization_generation": 1,
        "broker_ready": True,
        "execution_mode": "live",
        "lifecycle_state": "BROKER_READY",
        "selected_contract": REAL_OCC,
        "selected_limit": 2.10,
        "selected_qty": 1,
        "selected_reserved_cost": 210.0,
        "trigger_crossed_at": _iso(_now() - timedelta(seconds=10)),
        "trigger_price": 600.0,
        "observed_underlying_price": 600.20,
        "entry_cutoff_et": TEST_ENTRY_CUTOFF_ET,
    }
    return row


def _live_broker_ready_plan() -> types.SimpleNamespace:
    return types.SimpleNamespace(
        contract_symbol=REAL_OCC,
        limit_price=2.10,
        contracts=1,
        max_position_usd=210.0,
        side="CALL",
        direction="CALL",
        execution_mode="live",
        client_id=CLIENT_ID,
        signal_id=SIGNAL_ID,
        trigger_price=600.0,
        underlying_price=600.20,
        stop_underlying=595.0,
        target_underlying=605.0,
        metadata={
            "contract_deferred": False,
            "materialization_generation": 1,
            "broker_ready": True,
            "lifecycle_state": "BROKER_READY",
            "selected_contract": REAL_OCC,
            "selected_limit": 2.10,
            "selected_qty": 1,
            "trigger_crossed_at": _iso(_now() - timedelta(seconds=10)),
            "trigger_price": 600.0,
        },
        ticker="SPY",
        plan_id="plan-pr323-concurrency",
        score=92.0,
        tier="A",
        timeframe="1d",
        pattern="breakout",
    )


def _live_broker_ready_watched(plan) -> types.SimpleNamespace:
    crossed = datetime.fromisoformat(str(plan.metadata["trigger_crossed_at"]))
    if crossed.tzinfo is None:
        crossed = crossed.replace(tzinfo=timezone.utc)
    signal = {
        "signal_id": SIGNAL_ID,
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "live",
        "ticker": "SPY",
        "side": "CALL",
        "entry_price": 600.0,
        "stop_price": 595.0,
        "target_price": 605.0,
        "contract_symbol": REAL_OCC,
        "contracts": 1,
        "limit_price": 2.10,
        "reserved_cost": 210.0,
        "contract_deferred": False,
        "_approved_plan": plan,
    }
    return types.SimpleNamespace(
        signal=signal,
        ticker="SPY",
        side="CALL",
        trigger_price=600.0,
        entry_trigger=600.0,
        stop_level=595.0,
        target_price=605.0,
        trigger_crossed_at=crossed,
        triggered_at=crossed,
        breach_price=600.20,
    )


class _ConcurrentTxnStore:
    def __init__(self, row: dict, *, interleaving: str) -> None:
        self.row = copy.deepcopy(row)
        self.interleaving = interleaving
        self.lock = threading.Lock()
        self.intent_barrier = threading.Barrier(2)
        self.watcher_intent_ready = threading.Event()
        self.recovery_intent_ready = threading.Event()
        self.recovery_persist_finished = threading.Event()
        self.recovery_claim_successes = 0
        self.recovery_claim_attempts = 0
        self.recovery_intent_successes = 0
        self.recovery_intent_attempts = 0
        self.watcher_intent_successes = 0
        self.watcher_intent_attempts = 0
        self.watcher_intent_errors: list[str] = []

    @staticmethod
    def _merge_meta(target: dict, patch: dict) -> None:
        for key, value in patch.items():
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                target[key].update(value)
            else:
                target[key] = copy.deepcopy(value)

    def get_row_copy(self) -> dict:
        with self.lock:
            return copy.deepcopy(self.row)

    def update_order_meta(self, patch: dict, **_expected) -> None:
        with self.lock:
            self._merge_meta(self.row.setdefault("meta", {}), patch)

    def transition(self, to_status: str, **kwargs) -> None:
        with self.lock:
            self.row["status"] = str(to_status)
            if "broker_order_id" in kwargs:
                self.row["broker_order_id"] = kwargs["broker_order_id"]
            if "submitted_ts" in kwargs:
                self.row["submitted_ts"] = kwargs["submitted_ts"]
            if "last_error" in kwargs:
                self.row["last_error"] = kwargs["last_error"]

    def execute(self, sql: str, params: tuple) -> int:
        if (
            "recovery_submit_lease_until" in sql
            and len(params) == 5
        ):
            return self._execute_recovery_claim(params)
        if "NULLIF(meta->>'recovery_submit_lease_until','')" in sql and len(params) == 6:
            return self._execute_recovery_submit_intent(params)
        if "COALESCE(signal_id,'') = %s" in sql and len(params) == 6:
            return self._execute_watcher_submit_intent(params)
        raise AssertionError(sql)

    def _execute_recovery_claim(self, params: tuple) -> int:
        patch_json, local_order_id, client_id, generation, now_iso = params
        patch = json.loads(patch_json)
        self.recovery_claim_attempts += 1
        with self.lock:
            meta = self.row["meta"]
            ok = (
                self.row["local_order_id"] == local_order_id
                and self.row["client_id"] == client_id
                and str(self.row["status"] or "").upper() == "PENDING_TRIGGER"
                and not self.row.get("broker_order_id")
                and self.row.get("submitted_ts") is None
                and meta.get("broker_ready") is True
                and str(meta.get("lifecycle_state") or "") == "BROKER_READY"
                and int(meta.get("materialization_generation") or 0) == int(generation)
                and str(meta.get("submit_intent_at") or "") == ""
                and (
                    str(meta.get("recovery_submit_owner") or "") == ""
                    or str(meta.get("recovery_submit_lease_until") or "") < str(now_iso)
                )
            )
            if ok:
                self._merge_meta(meta, patch)
                self.recovery_claim_successes += 1
                return 1
        return 0

    def _execute_recovery_submit_intent(self, params: tuple) -> int:
        patch_json, local_order_id, client_id, mode, generation, owner = params
        patch = json.loads(patch_json)
        self.recovery_intent_attempts += 1
        self.recovery_intent_ready.set()
        if self.interleaving == "watcher_reaches_pre_submit_first":
            self.intent_barrier.wait(timeout=5)
        with self.lock:
            meta = self.row["meta"]
            ok = (
                self.row["local_order_id"] == local_order_id
                and self.row["client_id"] == client_id
                and str(self.row["execution_mode"] or "").lower() == str(mode)
                and str(self.row["status"] or "").upper() == "PENDING_TRIGGER"
                and not self.row.get("broker_order_id")
                and self.row.get("submitted_ts") is None
                and str(meta.get("lifecycle_state") or "") == "BROKER_READY"
                and meta.get("broker_ready") is True
                and int(meta.get("materialization_generation") or 0) == int(generation)
                and str(meta.get("recovery_submit_owner") or "") == str(owner)
                and str(meta.get("submit_intent_at") or "") == ""
            )
            if ok:
                self._merge_meta(meta, patch)
                self.recovery_intent_successes += 1
                rc = 1
            else:
                rc = 0
        self.recovery_persist_finished.set()
        return rc

    def _execute_watcher_submit_intent(self, params: tuple) -> int:
        patch_json, local_order_id, client_id, mode, signal_id, generation = params
        patch = json.loads(patch_json)
        self.watcher_intent_attempts += 1
        self.watcher_intent_ready.set()
        self.intent_barrier.wait(timeout=5)
        self.recovery_persist_finished.wait(timeout=5)
        with self.lock:
            meta = self.row["meta"]
            ok = (
                self.row["local_order_id"] == local_order_id
                and self.row["client_id"] == client_id
                and str(self.row["execution_mode"] or "").lower() == str(mode)
                and str(self.row["signal_id"] or "") == str(signal_id)
                and str(self.row["status"] or "").upper() == "PENDING_TRIGGER"
                and not self.row.get("broker_order_id")
                and self.row.get("submitted_ts") is None
                and str(meta.get("lifecycle_state") or "") == "BROKER_READY"
                and meta.get("broker_ready") is True
                and int(meta.get("materialization_generation") or 0) == int(generation)
                and str(meta.get("submit_intent_at") or "") == ""
                and str(meta.get("recovery_submit_owner") or "") == ""
            )
            if ok:
                self._merge_meta(meta, patch)
                self.watcher_intent_successes += 1
                return 1
        return 0


class _ConcurrentCursor:
    def __init__(self, store: _ConcurrentTxnStore) -> None:
        self.store = store
        self.rowcount = 0

    def execute(self, sql, params):
        self.rowcount = self.store.execute(sql, params)
        return self


class _ConcurrentConn:
    def __init__(self, store: _ConcurrentTxnStore) -> None:
        self.store = store

    def __enter__(self):
        return _ConcurrentCursor(self.store)

    def __exit__(self, *_args):
        return False


class _ConcurrentOSM:
    def __init__(self, store: _ConcurrentTxnStore) -> None:
        self.client_id = CLIENT_ID
        self.execution_mode = "live"
        self.store = store
        self.post_payloads: list[dict] = []
        self.post_submit_intents: list[str] = []
        self.cancel_calls = 0
        self.expire_calls = 0
        self.terminalizations: list[tuple[str, str]] = []
        self.submit_existing_entry = APOrderStateMachine.submit_existing_entry.__get__(self, type(self))
        self.claim_deferred_broker_ready_submit = APOrderStateMachine.claim_deferred_broker_ready_submit.__get__(self, type(self))
        self.persist_deferred_submit_intent = APOrderStateMachine.persist_deferred_submit_intent.__get__(self, type(self))
        self.persist_materialized_submit_intent = APOrderStateMachine.persist_materialized_submit_intent.__get__(self, type(self))
        self._is_broker_accept_status = APOrderStateMachine._is_broker_accept_status

    def has_order(self, local_order_id):
        return local_order_id == LOCAL_ORDER_ID

    def get_order_by_signal(self, signal_id):
        return self.get_order(LOCAL_ORDER_ID) if signal_id == SIGNAL_ID else None

    def _get_order(self, local_order_id):
        return self.get_order(local_order_id)

    def get_order(self, local_order_id):
        if local_order_id != LOCAL_ORDER_ID:
            return None
        return self.store.get_row_copy()

    def update_order_meta(self, local_order_id, patch, **_expected):
        assert local_order_id == LOCAL_ORDER_ID
        self.store.update_order_meta(patch)
        return True

    def transition(self, local_order_id, to_status, **kwargs):
        assert local_order_id == LOCAL_ORDER_ID
        self.store.transition(to_status, **kwargs)
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
        self.terminalizations.append((reason_code, terminal_status))
        self.store.transition(terminal_status, last_error=reason_code)
        self.store.update_order_meta({
            "lifecycle_state": terminal_status,
            "reason_code": reason_code,
            "final_reason": reason_code,
            "terminal_diagnostics": diagnostics or {},
            "current_owner": "",
            "broker_ready": False,
        })
        return True

    def expire_pending_entry(self, local_order_id, reason):
        assert local_order_id == LOCAL_ORDER_ID
        self.expire_calls += 1
        return False

    def cancel_pending_entry(self, local_order_id, reason):
        assert local_order_id == LOCAL_ORDER_ID
        self.cancel_calls += 1
        return False

    def _lookup_order_by_tag(self, *_args, **_kwargs):
        current = self.store.get_row_copy()
        return current.get("broker_order_id")

    def _flag_split_brain_order(self, *_args, **_kwargs):
        return None

    def _emit_transition_event(self, **_kwargs):
        return None

    def _submit_order_with_retry(self, **kwargs):
        current = self.store.get_row_copy()
        current_meta = current.get("meta") or {}
        submit_intent_at = str(current_meta.get("submit_intent_at") or "").strip()
        assert submit_intent_at, "broker POST attempted before durable submit intent"
        self.post_submit_intents.append(submit_intent_at)
        self.post_payloads.append(copy.deepcopy(kwargs["order_data"]))
        return (
            {"id": "TR-323", "status": "open"},
            None,
            "TR-323",
            "ACK",
        )


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


def _build_core(
    osm: _StatefulOSM,
    broker: _Broker,
    selector: _Selector,
    master_control=None,
):
    if master_control is None:
        master_control = types.SimpleNamespace(
            mode="LIVE",
            max_positions=5,
            _kill_switch_fn=lambda: False,
            get_entry_capacity=lambda **_kwargs: {
                "ok": True,
                "reason_code": "CAPACITY_AVAILABLE",
                "account_equity": 1709.2,
                "per_trade_budget": 170.92,
                "total_capital_cap": 683.68,
                "current_total_exposure": 0.0,
                "remaining_total_capacity": 683.68,
                "selector_budget": 170.92,
                "max_affordable_premium": 1.7092,
            },
            revalidate_exposure=lambda _plan, **_kwargs: types.SimpleNamespace(
                ok=True,
                reason_code="EXPOSURE_ALLOWED",
                reason="allowed",
            ),
        )
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
    core._classify_recovered_ownership_loss = core_mod.APExecutionCore._classify_recovered_ownership_loss.__get__(core, type(core))
    core._is_real_occ_contract = core_mod.APExecutionCore._is_real_occ_contract
    core._strict_materialization_int = core_mod.APExecutionCore._strict_materialization_int
    core._live_materialization_lease = core_mod.APExecutionCore._live_materialization_lease
    core._claim_deferred_materialization_for_trigger = core_mod.APExecutionCore._claim_deferred_materialization_for_trigger.__get__(core, type(core))
    core._plan_is_deferred = core_mod.APExecutionCore._plan_is_deferred
    core.resume_deferred_broker_ready_order = core_mod.APExecutionCore.resume_deferred_broker_ready_order.__get__(core, type(core))
    core._on_entry_trigger = core_mod.APExecutionCore._on_entry_trigger.__get__(core, type(core))
    return core


def _build_watcher(osm: _StatefulOSM, core, *, ticker="SPY"):
    watcher = APEntryWatcher(None, order_state_machine=osm, mode="LIVE")
    _orig_watch = watcher.watch
    _quote_calls = {"count": 0}
    _single_quote_calls = {"count": 0}
    watcher.watch = lambda plan, local_order_id, **kwargs: _orig_watch(
        plan,
        local_order_id,
        recovery_rearm=kwargs.get("recovery_rearm", False),
        no_cancel_on_reject=kwargs.get("no_cancel_on_reject", False),
    )
    watcher.on_trigger = core._on_entry_trigger
    watcher._insert_watcher_audit_row = lambda *a, **k: None
    watcher._persist_watcher_audit = lambda *a, **k: None

    def _get_quote(_ticker):
        _single_quote_calls["count"] += 1
        if _single_quote_calls["count"] <= 2:
            return {
                "bid": 599.80,
                "ask": 599.82,
                "quote_age_ms": 10,
            }
        return {
            "bid": 600.20,
            "ask": 600.22,
            "quote_age_ms": 10,
        }

    watcher._get_quote = _get_quote

    def _fetch_quotes(_tickers):
        _quote_calls["count"] += 1
        if _quote_calls["count"] == 1:
            return {
                ticker: {
                    "bid": 599.80,
                    "ask": 599.82,
                    "quote_age_ms": 10,
                }
            }
        return {
            ticker: {
                "bid": 600.20,
                "ask": 600.22,
                "quote_age_ms": 10,
            }
        }

    watcher._fetch_quotes = _fetch_quotes
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
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_DELAY_SECONDS", "1")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "2359")
    monkeypatch.setenv("LIVE_CONFIRMATION_REQUIRED", "1")
    monkeypatch.setenv("PRE_SUBMIT_PROOF_RETRY_DELAY_SECONDS", "0")
    monkeypatch.setenv("PRE_SUBMIT_PROOF_RETRY_DEADLINE_SECONDS", "120")

    def _refresh_ask_at_submit(_broker, _contract):
        return (
            2.09,
            15,
            True,
            "ok",
            {
                "spread_pct": 0.02,
                "submit_bid": 2.08,
                "submit_ask": 2.09,
                "submit_mid": 2.085,
                "submit_last": 2.09,
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
        watcher1._poll_active_signals(open_protect_active=False)

        row_after_first_trigger = osm.get_order(LOCAL_ORDER_ID)
        meta_after_first_trigger = row_after_first_trigger["meta"]
        assert meta_after_first_trigger["lifecycle_state"] == "RETRY_WAIT"
        assert meta_after_first_trigger["selector_failure"]["reason_code"] == "CHAIN_ROW_ZERO_BID_ASK"
        assert row_after_first_trigger["status"] == "PENDING_TRIGGER"
        assert row_after_first_trigger["broker_order_id"] is None
        # Make the persisted retry due without weakening the production rule
        # that zero/non-positive delay values fall back to the safe default.
        due_at = _iso(_now() - timedelta(seconds=1))
        osm.row["meta"]["next_retry_at"] = due_at
        osm.row["meta"]["materialization_next_retry_at"] = due_at

        core2 = _build_core(osm, broker, selector)
        watcher2 = _build_watcher(osm, core2)
        _run_recovery(osm, broker, watcher2, core2)
        assert LOCAL_ORDER_ID in [w.signal["local_order_id"] for w in watcher2._pending]
        for watched in watcher2._pending:
            watched.overnight = False

        watcher2._poll_active_signals(open_protect_active=False)
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


@pytest.mark.parametrize(
    "interleaving",
    [
        "watcher_reaches_pre_submit_first",
        "recovery_reaches_pre_submit_first",
    ],
)
def test_live_watcher_and_recovery_worker_race_to_single_broker_post(monkeypatch, interleaving):
    broker = _Broker()
    store = _ConcurrentTxnStore(_broker_ready_row(), interleaving=interleaving)
    osm = _ConcurrentOSM(store)
    watcher_plan = _live_broker_ready_plan()
    recovery_plan = copy.deepcopy(watcher_plan)
    watched = _live_broker_ready_watched(watcher_plan)

    watcher_core = _build_core(osm, broker, _Selector())
    recovery_core = _build_core(osm, broker, _Selector())
    watcher = _build_watcher(osm, watcher_core)

    monkeypatch.setenv("ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE", "0")
    monkeypatch.setenv("LIVE_CONFIRMATION_REQUIRED", "1")
    monkeypatch.setenv("DEFERRED_RECOVERY_MAX_TRIGGER_AGE_SECONDS", "300")
    monkeypatch.setenv("DEFERRED_RECOVERY_MAX_ATTEMPTS", "20")
    monkeypatch.setenv("DEFERRED_RECOVERY_RETRY_DELAY_SECONDS", "30")

    def _refresh_ask_at_submit(_broker, _contract):
        return (
            2.09,
            15,
            True,
            "ok",
            {
                "spread_pct": 0.02,
                "submit_bid": 2.08,
                "submit_ask": 2.09,
                "submit_mid": 2.085,
                "submit_last": 2.09,
            },
        )

    fake_execution = types.ModuleType("ap.execution")
    fake_execution._refresh_ask_at_submit = _refresh_ask_at_submit

    watcher_result = {}
    recovery_result = {}
    thread_errors = {}

    monkeypatch.setattr(osm_mod, "conn", lambda: _ConcurrentConn(store))
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn, *a, **k: fn())
    monkeypatch.setattr(db_mod, "conn", lambda: _ConcurrentConn(store))
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn, *a, **k: fn())
    monkeypatch.setitem(
        APOrderStateMachine.claim_deferred_broker_ready_submit.__globals__,
        "conn",
        lambda: _ConcurrentConn(store),
    )
    monkeypatch.setitem(
        APOrderStateMachine.persist_deferred_submit_intent.__globals__,
        "conn",
        lambda: _ConcurrentConn(store),
    )
    monkeypatch.setitem(
        APOrderStateMachine.persist_materialized_submit_intent.__globals__,
        "conn",
        lambda: _ConcurrentConn(store),
    )
    monkeypatch.setitem(
        APOrderStateMachine.claim_deferred_broker_ready_submit.__globals__,
        "run_with_retry",
        lambda fn, *a, **k: fn(),
    )
    monkeypatch.setitem(
        APOrderStateMachine.persist_deferred_submit_intent.__globals__,
        "run_with_retry",
        lambda fn, *a, **k: fn(),
    )
    monkeypatch.setitem(
        APOrderStateMachine.persist_materialized_submit_intent.__globals__,
        "run_with_retry",
        lambda fn, *a, **k: fn(),
    )

    def _run_watcher():
        try:
            watcher_result["value"] = watcher.on_trigger(watched)
        except Exception as exc:
            thread_errors["watcher"] = exc

    def _run_recovery():
        try:
            recovery_result["value"] = recovery_core.resume_deferred_broker_ready_order(
                local_order_id=LOCAL_ORDER_ID,
                plan=recovery_plan,
            )
        except Exception as exc:
            thread_errors["recovery"] = exc

    with patch.dict(sys.modules, {"ap.execution": fake_execution}), patch(
        "ap_entry_confirmation.check_entry_confirmation",
        return_value=_FakeConfirmResult(),
    ):
        watcher_thread = threading.Thread(target=_run_watcher, name="watcher-race")
        recovery_thread = threading.Thread(target=_run_recovery, name="recovery-race")

        if interleaving == "watcher_reaches_pre_submit_first":
            watcher_thread.start()
            assert store.watcher_intent_ready.wait(timeout=5)
            recovery_thread.start()
        else:
            recovery_thread.start()
            assert store.recovery_intent_ready.wait(timeout=5)
            watcher_thread.start()

        watcher_thread.join(timeout=10)
        recovery_thread.join(timeout=10)

    assert not watcher_thread.is_alive()
    assert not recovery_thread.is_alive()
    assert thread_errors == {}

    final_row = osm.get_order(LOCAL_ORDER_ID)
    final_meta = final_row["meta"]
    watcher_outcome = watcher_result.get("value") or {}
    recovery_outcome = recovery_result.get("value") or {}

    assert store.recovery_claim_attempts == 1
    assert store.recovery_claim_successes == 1
    assert store.recovery_intent_attempts == 1
    assert store.recovery_intent_successes == 1
    assert store.watcher_intent_attempts == (
        1 if interleaving == "watcher_reaches_pre_submit_first" else 0
    )
    assert store.watcher_intent_successes == 0

    assert len(osm.post_payloads) == 1
    assert len(osm.post_submit_intents) == 1
    assert osm.post_payloads[0]["tag"] == LOCAL_ORDER_ID[:32]
    assert osm.post_payloads[0]["option_symbol"] == REAL_OCC
    assert osm.post_payloads[0]["price"] > 0.01
    assert not osm.post_payloads[0]["option_symbol"].startswith("DEFERRED:")

    assert recovery_outcome["configured_max_attempts"] == 20
    assert recovery_outcome["effective_max_attempts"] == 10
    assert recovery_outcome["max_attempts"] == 10
    assert recovery_outcome["remaining_recovery_window_seconds"] >= 289
    assert recovery_outcome["disposition"] == "SUBMITTED"
    assert recovery_outcome["reason_code"] == "RECOVERY_CANONICAL_SUBMIT_ACCEPTED"

    if interleaving == "watcher_reaches_pre_submit_first":
        assert watcher_outcome["disposition"] in {
            "RECONCILE_PENDING", "OWNERSHIP_TRANSFERRED", "SUBMITTED",
        }
        assert watcher_outcome["reason_code"] == "MATERIALIZATION_SUBMIT_OWNERSHIP_TRANSFERRED"
    else:
        assert watcher_result.get("value") is None

    assert osm.cancel_calls == 0
    assert osm.expire_calls == 0
    assert osm.terminalizations == []
    assert final_row["status"] == "SUBMITTED"
    assert final_row["broker_order_id"] == "TR-323"
    assert final_row["submitted_ts"] is not None
    assert final_row["client_id"] == CLIENT_ID
    assert final_row["execution_mode"] == "live"
    assert final_row["local_order_id"] == LOCAL_ORDER_ID
    assert final_row["contract"] == REAL_OCC
    assert final_meta["selected_contract"] == REAL_OCC
    assert final_meta["broker_ready"] is True
    assert str(final_meta.get("lifecycle_state") or "") in {"SUBMITTING", "SUBMITTED", "ACKNOWLEDGED"}
    assert str(final_meta.get("recovery_submit_owner") or "").startswith("recovery_submit:")
    assert str(final_meta.get("submit_intent_at") or "").strip()

    watcher_again = watcher.on_trigger(watched)
    recovery_again = recovery_core.resume_deferred_broker_ready_order(
        local_order_id=LOCAL_ORDER_ID,
        plan=recovery_plan,
    )

    assert len(osm.post_payloads) == 1
    assert watcher_again is None
    assert recovery_again["disposition"] == "KEEP_WATCHER"
    assert recovery_again["reason_code"] in {
        "RECOVERY_ALREADY_SUBMITTED",
        "RECOVERY_STATUS_NOT_ELIGIBLE:SUBMITTED",
    }


def test_penny_selector_result_terminalizes_without_broker_post(monkeypatch):
    class _PennySelector:
        dte_ladder_enabled = True

        def select(self, *_args, **_kwargs):
            return types.SimpleNamespace(
                contract_symbol=REAL_OCC,
                bid=0.01,
                ask=0.01,
                mid=0.01,
                execution_price_per_share=0.01,
                premium_per_contract=1.0,
                affordable_contracts=1,
                metadata={
                    "selector_failure": {
                        "reason_code": "CHEAP_CONTRACT_ONLY_CHOICE",
                        "stage": "cheap_contract_gate",
                        "explanation": "cheap only choice",
                    }
                },
            )

        def get_last_failure(self):
            return {
                "reason_code": "CHEAP_CONTRACT_ONLY_CHOICE",
                "stage": "cheap_contract_gate",
                "explanation": "cheap only choice",
                "selector_failure_class": "quality",
                "quality_failure": True,
            }

        def get_last_dte_ladder_audit(self):
            return {"buckets_attempted": 1}

    broker = _Broker()
    osm = _StatefulOSM()
    core = _build_core(osm, broker, _PennySelector())
    watcher = _build_watcher(osm, core)

    fake_execution = types.ModuleType("ap.execution")
    fake_execution._refresh_ask_at_submit = lambda *_args, **_kwargs: (
        2.09,
        15,
        True,
        "ok",
        {
            "spread_pct": 0.02,
            "submit_bid": 2.08,
            "submit_ask": 2.09,
            "submit_mid": 2.085,
            "submit_last": 2.09,
        },
    )

    with patch.dict(sys.modules, {"ap.execution": fake_execution}), patch(
        "ap_entry_confirmation.check_entry_confirmation",
        return_value=_FakeConfirmResult(),
    ), patch("ap.db.conn", lambda: _NoopConn()), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        assert watcher.watch(_approved_plan(), LOCAL_ORDER_ID) is True
        result = watcher.on_trigger(watcher._pending[0])

    row = osm.get_order(LOCAL_ORDER_ID)
    meta = row["meta"]

    assert result["disposition"] == "TERMINAL_DURABLE"
    assert result["reason_code"] == "CHEAP_CONTRACT_ONLY_CHOICE"
    assert len(osm.post_payloads) == 0
    assert row["status"] == "EXPIRED"
    assert row["broker_order_id"] is None
    assert row["contract"] == "DEFERRED:SPY"
    assert row["limit_price"] == pytest.approx(0.01)
    assert meta["broker_ready"] is False
    assert str(meta.get("submit_intent_at") or "") == ""
    assert meta["terminal_diagnostics"]["selector_failure"]["reason_code"] == "CHEAP_CONTRACT_ONLY_CHOICE"
    assert meta["terminal_diagnostics"]["deferred_selector_audit"]["reason_code"] == "CHEAP_CONTRACT_ONLY_CHOICE"
    assert meta["terminal_diagnostics"]["selected_limit_price"] == pytest.approx(0.01)
    assert str(meta.get("lifecycle_state") or "") == "EXPIRED"


def test_cheap_gate_none_terminalizes_with_exact_reason_and_no_generic_overwrite(monkeypatch):
    class _NoneCheapSelector:
        dte_ladder_enabled = True

        def select(self, *_args, **_kwargs):
            return None

        def get_last_failure(self):
            return {
                "reason_code": "CHEAP_CONTRACT_NO_UPGRADE",
                "stage": "cheap_contract_gate",
                "explanation": "all candidates below premium floor",
                "selector_failure_class": "quality",
                "quality_failure": True,
                "data_failure": False,
                "chain_rows": 4,
                "survivor_count": 0,
            }

        def get_last_dte_ladder_audit(self):
            return {"buckets_attempted": 1}

    broker = _Broker()
    osm = _StatefulOSM()
    core = _build_core(osm, broker, _NoneCheapSelector())
    watcher = _build_watcher(osm, core)

    fake_execution = types.ModuleType("ap.execution")
    fake_execution._refresh_ask_at_submit = lambda *_args, **_kwargs: (
        2.09,
        15,
        True,
        "ok",
        {
            "spread_pct": 0.02,
            "submit_bid": 2.08,
            "submit_ask": 2.09,
            "submit_mid": 2.085,
            "submit_last": 2.09,
        },
    )

    with patch.dict(sys.modules, {"ap.execution": fake_execution}), patch(
        "ap_entry_confirmation.check_entry_confirmation",
        return_value=_FakeConfirmResult(),
    ), patch("ap.db.conn", lambda: _NoopConn()), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        assert watcher.watch(_approved_plan(), LOCAL_ORDER_ID) is True
        result = watcher.on_trigger(watcher._pending[0])

    row = osm.get_order(LOCAL_ORDER_ID)
    meta = row["meta"]

    assert result["disposition"] == "TERMINAL_DURABLE"
    assert row["status"] == "EXPIRED"
    assert row["last_error"] == "breach_time_contract_selection:CHEAP_CONTRACT_NO_UPGRADE"
    assert len(osm.post_payloads) == 0
    assert meta["broker_ready"] is False
    assert meta["last_breach_failure_reason_code"] == "CHEAP_CONTRACT_NO_UPGRADE"
    assert meta["terminal_diagnostics"]["deferred_selector_audit"]["reason_code"] == "CHEAP_CONTRACT_NO_UPGRADE"
    assert "NO_CONTRACT_FOUND" not in row["last_error"]
    assert str(meta.get("lifecycle_state") or "") == "EXPIRED"


def test_cheap_gate_terminal_write_failure_keeps_watcher_owned(monkeypatch):
    class _FailTerminalOSM(_StatefulOSM):
        def terminalize_deferred_breach(self, *_args, **_kwargs):
            return False

        def expire_pending_entry(self, *_args, **_kwargs):
            return False

        def cancel_pending_entry(self, *_args, **_kwargs):
            return False

    class _NoneCheapSelector:
        dte_ladder_enabled = True

        def select(self, *_args, **_kwargs):
            return None

        def get_last_failure(self):
            return {
                "reason_code": "CHEAP_CONTRACT_NO_UPGRADE",
                "stage": "cheap_contract_gate",
                "explanation": "all candidates below premium floor",
                "selector_failure_class": "quality",
                "quality_failure": True,
            }

        def get_last_dte_ladder_audit(self):
            return {"buckets_attempted": 1}

    broker = _Broker()
    osm = _FailTerminalOSM()
    core = _build_core(osm, broker, _NoneCheapSelector())
    watcher = _build_watcher(osm, core)

    fake_execution = types.ModuleType("ap.execution")
    fake_execution._refresh_ask_at_submit = lambda *_args, **_kwargs: (
        2.09,
        15,
        True,
        "ok",
        {
            "spread_pct": 0.02,
            "submit_bid": 2.08,
            "submit_ask": 2.09,
            "submit_mid": 2.085,
            "submit_last": 2.09,
        },
    )

    with patch.dict(sys.modules, {"ap.execution": fake_execution}), patch(
        "ap_entry_confirmation.check_entry_confirmation",
        return_value=_FakeConfirmResult(),
    ), patch("ap.db.conn", lambda: _NoopConn()), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        assert watcher.watch(_approved_plan(), LOCAL_ORDER_ID) is True
        watched = watcher._pending[0]
        result = watcher.on_trigger(watched)
        disposition, _ = watcher._resolve_trigger_callback_disposition(watched, result)

    row = osm.get_order(LOCAL_ORDER_ID)
    meta = row["meta"]

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "BREACH_TERMINAL_WRITE_FAILED"
    assert disposition == "KEEP_WATCHER"
    assert len(osm.post_payloads) == 0
    assert row["status"] == "PENDING_TRIGGER"
    assert str(meta.get("lifecycle_state") or "") == "MATERIALIZING"
    assert str(meta.get("materialization_owner") or "").startswith("watcher:")
    assert meta["broker_ready"] is False


def test_stale_deferred_penny_broker_ready_recovery_cannot_submit(monkeypatch):
    broker = _Broker()
    osm = _StatefulOSM()
    osm.row["status"] = "PENDING_TRIGGER"
    osm.row["contract"] = "DEFERRED:SPY"
    osm.row["limit_price"] = 0.01
    osm.row["reserved_cost"] = 1.0
    osm.row["meta"].update({
        "contract_deferred": True,
        "broker_ready": True,
        "lifecycle_state": "BROKER_READY",
        "selected_contract": "DEFERRED:SPY",
        "selected_limit": 0.01,
        "selected_qty": 1,
        "trigger_crossed_at": _iso(_now() - timedelta(seconds=10)),
        "trigger_price": 600.0,
        "observed_underlying_price": 600.20,
    })

    core = _build_core(osm, broker, _Selector())

    monkeypatch.setenv("DEFERRED_RECOVERY_MAX_TRIGGER_AGE_SECONDS", "300")
    monkeypatch.setenv("DEFERRED_RECOVERY_MAX_ATTEMPTS", "20")
    monkeypatch.setenv("DEFERRED_RECOVERY_RETRY_DELAY_SECONDS", "30")

    outcome = core.resume_deferred_broker_ready_order(
        local_order_id=LOCAL_ORDER_ID,
        plan=_live_broker_ready_plan(),
    )

    row = osm.get_order(LOCAL_ORDER_ID)
    meta = row["meta"]

    assert outcome["disposition"] == "TERMINAL_DURABLE"
    assert outcome["reason_code"] == "RECOVERY_INVALID_OCC_CONTRACT"
    assert len(osm.post_payloads) == 0
    assert str(meta.get("submit_intent_at") or "") == ""
    assert row["broker_order_id"] is None
    assert row["contract"] == "DEFERRED:SPY"
    assert row["limit_price"] == pytest.approx(0.01)


PR514_CLIENT_ID = "jasoncosby1@gmail.com"


class _DeferredCapacityMC:
    def __init__(
        self,
        *,
        remaining_total_capacity: float = 500.0,
        final_ok: bool = True,
        final_reason: str = "EXPOSURE_ALLOWED",
        final_ok_sequence=None,
        final_reason_sequence=None,
        resize_qty_sequence=None,
    ) -> None:
        self.mode = "LIVE"
        self.max_positions = 5
        self._kill_switch_fn = lambda: False
        self.remaining_total_capacity = remaining_total_capacity
        self.final_ok = final_ok
        self.final_reason = final_reason
        self.final_ok_sequence = list(final_ok_sequence or [])
        self.final_reason_sequence = list(final_reason_sequence or [])
        self.resize_qty_sequence = list(resize_qty_sequence or [])
        self.capacity_calls: list[dict] = []
        self.final_calls: list[dict] = []

    def get_entry_capacity(self, **kwargs):
        self.capacity_calls.append(dict(kwargs))
        selector_budget = min(170.92, self.remaining_total_capacity)
        return {
            "ok": selector_budget > 0,
            "reason_code": "CAPACITY_AVAILABLE" if selector_budget > 0 else "CAPITAL_LIMIT_NO_REMAINING",
            "account_equity": 1709.2,
            "per_trade_budget": 170.92,
            "total_capital_cap": 683.68,
            "current_total_exposure": 683.68 - self.remaining_total_capacity,
            "remaining_total_capacity": self.remaining_total_capacity,
            "selector_budget": selector_budget,
            "max_affordable_premium": selector_budget / 100.0,
        }

    def revalidate_exposure(self, plan, *, client_id):
        _call_index = len(self.final_calls)
        self.final_calls.append({
            "client_id": client_id,
            "signal_id": getattr(plan, "signal_id", ""),
            "execution_mode": getattr(plan, "execution_mode", ""),
            "contract": getattr(plan, "contract_symbol", ""),
            "price": getattr(plan, "limit_price", 0),
            "qty": getattr(plan, "contracts", 0),
            "actual_selected_cost": getattr(plan, "max_position_usd", 0),
        })
        if _call_index < len(self.resize_qty_sequence):
            _resize_qty = self.resize_qty_sequence[_call_index]
            if _resize_qty is not None:
                plan.contracts = _resize_qty
        _final_ok = (
            self.final_ok_sequence[_call_index]
            if _call_index < len(self.final_ok_sequence)
            else self.final_ok
        )
        _final_reason = (
            self.final_reason_sequence[_call_index]
            if _call_index < len(self.final_reason_sequence)
            else self.final_reason
        )
        return types.SimpleNamespace(
            ok=_final_ok,
            reason_code=_final_reason,
            reason=_final_reason,
        )


class _CSelector:
    dte_ladder_enabled = True

    def __init__(
        self,
        *,
        execution_price_per_share: float = 1.26,
        affordable_contracts: int = 1,
    ):
        self.execution_price_per_share = execution_price_per_share
        self.affordable_contracts = affordable_contracts
        self.calls = 0

    def select(self, _plan, *, request_context=None):
        assert request_context is not None
        self.calls += 1
        return types.SimpleNamespace(
            contract_symbol="C260828C00133000",
            bid=1.25,
            ask=1.28,
            mid=1.265,
            affordable_contracts=self.affordable_contracts,
            execution_price_per_share=self.execution_price_per_share,
            candidate_audit={"underlying_price": 130.0},
            expiration_date="2026-08-28",
            dte=3,
            strike=133.0,
            option_type="CALL",
        )

    def get_last_failure(self):
        return None

    def get_last_dte_ladder_audit(self):
        return {"buckets_attempted": 1}


def _run_c_deferred_materialization(
    monkeypatch,
    *,
    selector,
    master_control,
    refresh_ask: float = 1.28,
    refresh_bid: float | None = None,
    refresh_mid: float | None = None,
):
    osm = _StatefulOSM()
    osm.proof_read_failures_remaining = 0
    osm.client_id = PR514_CLIENT_ID
    osm.row.update({
        "symbol": "C",
        "contract": "DEFERRED:C",
        "reserved_cost": 173.06,
        "client_id": PR514_CLIENT_ID,
    })
    osm.row["meta"].update({
        "contract_deferred": True,
        "execution_mode": "live",
    })
    plan = _approved_plan()
    plan.ticker = "C"
    plan.client_id = PR514_CLIENT_ID
    plan.contract_symbol = "DEFERRED:C"
    plan.max_position_usd = 173.06
    plan.metadata = {
        "contract_deferred": True,
        "execution_mode": "live",
        "queue_id": 514,
    }
    broker = _Broker()
    core = _build_core(osm, broker, selector, master_control=master_control)
    core.client_id = PR514_CLIENT_ID
    core.email = PR514_CLIENT_ID
    core._recover_plan_for_revalidation = lambda _watched: plan
    watcher = _build_watcher(osm, core)

    fake_execution = types.ModuleType("ap.execution")
    refresh_bid = (
        round(refresh_ask - 0.03, 2)
        if refresh_bid is None
        else refresh_bid
    )
    refresh_mid = (
        round((refresh_bid + refresh_ask) / 2.0, 4)
        if refresh_mid is None
        else refresh_mid
    )
    fake_execution._refresh_ask_at_submit = lambda *_args, **_kwargs: (
        refresh_ask,
        15,
        True,
        "ok",
        {
            "spread_pct": (refresh_ask - refresh_bid) / refresh_mid,
            "submit_bid": refresh_bid,
            "submit_ask": refresh_ask,
            "submit_mid": refresh_mid,
            "submit_last": refresh_ask,
        },
    )

    with patch.dict(sys.modules, {"ap.execution": fake_execution}), patch(
        "ap_entry_confirmation.check_entry_confirmation",
        return_value=_FakeConfirmResult(),
    ), patch("ap.db.conn", lambda: _NoopConn()), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        assert watcher.watch(plan, LOCAL_ORDER_ID) is True
        watched = watcher._pending[0]
        result = watcher.on_trigger(watched)
    return result, osm, broker, master_control


def test_pr514_live_deferred_c_materializes_actual_cost_before_final_gate(monkeypatch):
    mc = _DeferredCapacityMC()
    result, osm, broker, mc = _run_c_deferred_materialization(
        monkeypatch,
        selector=_CSelector(execution_price_per_share=1.26),
        master_control=mc,
    )

    assert len(osm.post_payloads) == 1
    assert mc.capacity_calls == [{
        "client_id": PR514_CLIENT_ID,
        "execution_mode": "live",
        "ticker": "C",
        "signal_id": SIGNAL_ID,
        "exclude_local_order_id": LOCAL_ORDER_ID,
    }]
    assert len(mc.final_calls) == 2
    selector_final, broker_boundary_final = mc.final_calls
    assert selector_final["client_id"] == PR514_CLIENT_ID
    assert selector_final["signal_id"] == SIGNAL_ID
    assert selector_final["execution_mode"] == "live"
    assert selector_final["contract"] == "C260828C00133000"
    assert selector_final["price"] == pytest.approx(1.26)
    assert selector_final["qty"] == 1
    assert selector_final["actual_selected_cost"] == pytest.approx(126.0)
    assert broker_boundary_final["price"] == pytest.approx(1.29)
    assert broker_boundary_final["qty"] == 1
    assert broker_boundary_final["actual_selected_cost"] == pytest.approx(129.0)
    assert osm.row["contract"] == "C260828C00133000"
    assert osm.row["reserved_cost"] == pytest.approx(129.0)
    assert osm.row["meta"]["selector_meta"]["actual_selected_cost"] == pytest.approx(126.0)
    assert osm.row["meta"]["selector_meta"]["selector_effective_budget"] == pytest.approx(170.92)


def test_pr514_live_deferred_final_gate_blocks_real_contract_over_per_position_cap(monkeypatch):
    mc = _DeferredCapacityMC(
        final_ok=False,
        final_reason="ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP",
    )
    result, osm, broker, mc = _run_c_deferred_materialization(
        monkeypatch,
        selector=_CSelector(execution_price_per_share=2.00),
        master_control=mc,
    )

    assert result["disposition"] == "TERMINAL_DURABLE"
    assert "ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP" in result["reason_code"]
    assert len(mc.final_calls) == 1
    assert mc.final_calls[0]["actual_selected_cost"] == pytest.approx(200.0)
    assert osm.post_payloads == []


def test_pr514_live_deferred_final_gate_blocks_total_cap_independently(monkeypatch):
    mc = _DeferredCapacityMC(
        remaining_total_capacity=100.0,
        final_ok=False,
        final_reason="ACTUAL_CONTRACT_COST_EXCEEDS_REMAINING_TOTAL_CAPACITY",
    )
    result, osm, broker, mc = _run_c_deferred_materialization(
        monkeypatch,
        selector=_CSelector(execution_price_per_share=1.26),
        master_control=mc,
    )

    assert result["disposition"] == "TERMINAL_DURABLE"
    assert "ACTUAL_CONTRACT_COST_EXCEEDS_REMAINING_TOTAL_CAPACITY" in result["reason_code"]
    assert mc.capacity_calls[0]["execution_mode"] == "live"
    assert mc.final_calls[0]["actual_selected_cost"] == pytest.approx(126.0)
    assert osm.post_payloads == []


def test_pr514_live_deferred_broker_boundary_blocks_refreshed_per_position_cost(
    monkeypatch,
):
    mc = _DeferredCapacityMC(
        final_ok_sequence=[True, False],
        final_reason_sequence=[
            "EXPOSURE_ALLOWED",
            "ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP",
        ],
    )
    result, osm, broker, mc = _run_c_deferred_materialization(
        monkeypatch,
        selector=_CSelector(execution_price_per_share=1.70),
        master_control=mc,
        refresh_ask=1.72,
    )

    assert result["disposition"] == "TERMINAL_DURABLE"
    assert "ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP" in result["reason_code"]
    assert len(mc.final_calls) == 2
    assert mc.final_calls[0]["actual_selected_cost"] == pytest.approx(170.0)
    assert mc.final_calls[1]["price"] == pytest.approx(1.73)
    assert mc.final_calls[1]["actual_selected_cost"] == pytest.approx(173.0)
    assert osm.post_payloads == []


def test_pr514_live_deferred_broker_boundary_blocks_refreshed_total_cost(
    monkeypatch,
):
    mc = _DeferredCapacityMC(
        remaining_total_capacity=172.0,
        final_ok_sequence=[True, False],
        final_reason_sequence=[
            "EXPOSURE_ALLOWED",
            "ACTUAL_CONTRACT_COST_EXCEEDS_REMAINING_TOTAL_CAPACITY",
        ],
    )
    result, osm, broker, mc = _run_c_deferred_materialization(
        monkeypatch,
        selector=_CSelector(execution_price_per_share=1.70),
        master_control=mc,
        refresh_ask=1.72,
    )

    assert result["disposition"] == "TERMINAL_DURABLE"
    assert "ACTUAL_CONTRACT_COST_EXCEEDS_REMAINING_TOTAL_CAPACITY" in result[
        "reason_code"
    ]
    assert mc.final_calls[0]["actual_selected_cost"] == pytest.approx(170.0)
    assert mc.final_calls[1]["actual_selected_cost"] == pytest.approx(173.0)
    assert osm.post_payloads == []


def test_pr514_live_deferred_broker_boundary_submits_when_refreshed_cost_fits(
    monkeypatch,
):
    mc = _DeferredCapacityMC(
        remaining_total_capacity=180.0,
        final_ok_sequence=[True, True],
        final_reason_sequence=["EXPOSURE_ALLOWED", "EXPOSURE_ALLOWED"],
    )
    result, osm, broker, mc = _run_c_deferred_materialization(
        monkeypatch,
        selector=_CSelector(execution_price_per_share=1.70),
        master_control=mc,
        refresh_ask=1.72,
    )

    assert result is None or result.get("disposition") in {None, "SUBMITTED"}
    assert len(osm.post_payloads) == 1
    assert len(mc.final_calls) == 2
    assert mc.final_calls[1]["price"] == pytest.approx(1.73)
    assert mc.final_calls[1]["actual_selected_cost"] == pytest.approx(173.0)
    assert osm.row["reserved_cost"] == pytest.approx(173.0)


def test_pr514_live_deferred_broker_boundary_recomputes_cost_after_qty_resize(
    monkeypatch,
):
    mc = _DeferredCapacityMC(
        final_ok_sequence=[True, True],
        final_reason_sequence=["EXPOSURE_ALLOWED", "EXPOSURE_ALLOWED"],
        resize_qty_sequence=[None, 1],
    )
    result, osm, broker, mc = _run_c_deferred_materialization(
        monkeypatch,
        selector=_CSelector(
            execution_price_per_share=0.85,
            affordable_contracts=2,
        ),
        master_control=mc,
        refresh_ask=0.87,
    )

    assert result is None or result.get("disposition") in {None, "SUBMITTED"}
    assert len(osm.post_payloads) == 1
    assert mc.final_calls[0]["qty"] == 2
    assert mc.final_calls[0]["actual_selected_cost"] == pytest.approx(170.0)
    assert mc.final_calls[1]["qty"] == 2
    assert mc.final_calls[1]["actual_selected_cost"] == pytest.approx(176.0)
    assert osm.row["qty"] == 1
    assert osm.row["reserved_cost"] == pytest.approx(88.0)


def test_pr514_live_deferred_no_affordable_selector_result_stays_terminal(monkeypatch):
    class _NoAffordableSelector(_CSelector):
        def select(self, _plan, *, request_context=None):
            self.calls += 1
            return None

        def get_last_failure(self):
            return {
                "reason_code": "UNTRADEABLE_FOR_ACCOUNT_SIZE",
                "stage": "affordability",
            }

    mc = _DeferredCapacityMC()
    result, osm, broker, mc = _run_c_deferred_materialization(
        monkeypatch,
        selector=_NoAffordableSelector(),
        master_control=mc,
    )

    assert result["disposition"] == "TERMINAL_DURABLE"
    assert "UNTRADEABLE_FOR_ACCOUNT_SIZE" in result["reason_code"]
    assert mc.final_calls == []
    assert osm.post_payloads == []


def test_pr514_deferred_unknown_mode_fails_closed_before_capacity_or_selector(monkeypatch):
    osm = _StatefulOSM()
    osm.row["meta"]["contract_deferred"] = True
    plan = _approved_plan()
    plan.execution_mode = "staging"
    plan.metadata = {"contract_deferred": True, "execution_mode": "staging"}
    mc = _DeferredCapacityMC()
    selector = _CSelector()
    core = _build_core(osm, _Broker(), selector, master_control=mc)
    watched = types.SimpleNamespace(
        signal={
            "_approved_plan": plan,
            "signal_id": SIGNAL_ID,
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "staging",
        },
        ticker="SPY",
        side="CALL",
        trigger_price=600.0,
        entry_trigger=600.0,
        stop_level=595.0,
        target_price=605.0,
    )

    result = core._on_entry_trigger(watched)

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "MATERIALIZATION_OWNERSHIP_UNPROVEN"
    assert mc.capacity_calls == []
    assert selector.calls == 0
    assert osm.post_payloads == []


# ─────────────────────────────────────────────────────────────────────────────
# PR #514 addendum — named production-shape callback liveness
# ─────────────────────────────────────────────────────────────────────────────

_PR514_LIVENESS_SHAPES = (
    ("C", "C260828C00133000"),
    ("PANW", "PANW260918C00400000"),
    ("IDXX", "IDXX260918C00700000"),
    ("NXPI", "NXPI260918C00250000"),
    ("ADI", "ADI260918C00200000"),
)


class _PR514SuccessfulSelector(_Selector):
    """Selector double that succeeds on its first materialization attempt."""

    def select(self, _approved_plan, *, request_context=None):
        assert request_context is not None
        assert (
            request_context.selector_request_kind
            == "DEFERRED_BREACH_MATERIALIZATION"
        )
        self.calls += 1
        self._last_failure = None
        return types.SimpleNamespace(
            contract_symbol=REAL_OCC,
            bid=2.09,
            ask=2.10,
            mid=2.095,
            affordable_contracts=1,
            execution_price_per_share=2.10,
            candidate_audit={"underlying_price": 600.25},
            expiration_date="2026-09-18",
            dte=3,
            delta=0.44,
            open_interest=1200,
            volume=500,
        )


class _PR514StrictMaterializationOSM(_StatefulOSM):
    """In-memory OSM that enforces the production owner/generation fences."""

    def __init__(self) -> None:
        super().__init__()
        self.claim_attempts = 0
        self.claim_successes = 0

    def claim_deferred_materialization(self, local_order_id, **kwargs):
        self.claim_attempts += 1
        meta = self.row.get("meta") or {}
        lifecycle = str(meta.get("lifecycle_state") or "").upper()
        materialization_status = str(
            meta.get("materialization_status") or ""
        ).upper()
        if str(self.row.get("status") or "").upper() in {
            "SUBMITTED",
            "ACKNOWLEDGED",
            "PARTIAL_FILL",
            "FILLED",
            "REJECTED",
            "EXPIRED",
            "CANCELED",
            "ERROR",
        }:
            return False
        if (
            bool(meta.get("materialization_in_flight"))
            or lifecycle in {"MATERIALIZING", "BROKER_READY", "SUBMITTING"}
            or materialization_status in {"RUNNING", "SELECTED"}
        ):
            return False

        _generation = kwargs.get("generation", kwargs.get("new_generation"))
        try:
            _generation = int(_generation)
            _prior_generation = int(meta.get("materialization_generation") or 0)
        except (TypeError, ValueError):
            return False
        if _generation != _prior_generation + 1:
            return False

        # The existing seam-4 double predates the OSM new_generation alias.
        # Normalize only at the test-double boundary; production code remains
        # exercised through its actual generation-bearing callback path.
        _claim_kwargs = dict(kwargs)
        _claim_kwargs.setdefault("generation", _generation)
        if "execution_mode" in _claim_kwargs:
            _execution_mode = _claim_kwargs["execution_mode"]
        else:
            _execution_mode = self.execution_mode
        claimed = bool(
            super().claim_deferred_materialization(
                local_order_id,
                **_claim_kwargs,
            )
        )
        if claimed:
            self.claim_successes += 1
            self._merge_meta({
                "materialization_in_flight": True,
                "execution_mode": str(_execution_mode or "").lower(),
            })
        return claimed

    def schedule_deferred_materialization_retry(self, local_order_id, **kwargs):
        meta = self.row.get("meta") or {}
        if (
            str(meta.get("materialization_owner") or "")
            != str(kwargs.get("owner") or "")
            or int(meta.get("materialization_generation") or 0)
            != int(kwargs.get("generation") or 0)
        ):
            return False
        scheduled = bool(
            super().schedule_deferred_materialization_retry(
                local_order_id,
                **kwargs,
            )
        )
        if scheduled:
            self._merge_meta({"materialization_in_flight": False})
        return scheduled

    def persist_deferred_broker_ready(self, local_order_id, **kwargs):
        meta = self.row.get("meta") or {}
        if (
            str(meta.get("materialization_owner") or "")
            != str(kwargs.get("owner") or "")
            or int(meta.get("materialization_generation") or 0)
            != int(kwargs.get("generation") or 0)
            or not bool(meta.get("materialization_in_flight"))
        ):
            return False
        persisted = bool(
            super().persist_deferred_broker_ready(
                local_order_id,
                **kwargs,
            )
        )
        if persisted:
            self._merge_meta({"materialization_in_flight": False})
        return persisted


def _pr514_liveness_fixture(monkeypatch, ticker, contract_symbol, selector):
    """Build the existing seam-4 production-shaped harness for one symbol."""
    _module = sys.modules[__name__]
    monkeypatch.setattr(_module, "CLIENT_ID", f"pr514-{ticker.lower()}@example.com")
    monkeypatch.setattr(_module, "LOCAL_ORDER_ID", f"oid-pr514-{ticker.lower()}")
    monkeypatch.setattr(_module, "SIGNAL_ID", f"sig-pr514-{ticker.lower()}")
    monkeypatch.setattr(_module, "REAL_OCC", contract_symbol)

    osm = _PR514StrictMaterializationOSM()
    osm.proof_read_failures_remaining = 0
    osm.row["symbol"] = ticker
    osm.row["contract"] = f"DEFERRED:{ticker}"
    osm.row["meta"].update({
        "contract_deferred": True,
        "execution_mode": "live",
    })
    plan = _approved_plan()
    plan.ticker = ticker
    plan.contract_symbol = f"DEFERRED:{ticker}"
    plan.metadata.update({
        "contract_deferred": True,
        "execution_mode": "live",
        "queue_id": 514,
    })
    broker = _Broker()
    core = _build_core(osm, broker, selector)
    # The generic seam-4 builder intentionally keeps its historical SPY
    # fallback. Bind this production-shaped plan explicitly so every matrix
    # case reaches the named ticker/mode and OCC identity.
    core._recover_plan_for_revalidation = lambda _watched: plan
    watcher = _build_watcher(osm, core, ticker=ticker)
    return osm, broker, selector, core, watcher, plan


def _pr514_liveness_execution_module():
    fake_execution = types.ModuleType("ap.execution")
    fake_execution._refresh_ask_at_submit = lambda *_args, **_kwargs: (
        2.09,
        15,
        True,
        "ok",
        {
            "spread_pct": 0.02,
            "submit_bid": 2.08,
            "submit_ask": 2.09,
            "submit_mid": 2.085,
            "submit_last": 2.09,
        },
    )
    return fake_execution


@pytest.mark.parametrize(
    ("ticker", "contract_symbol"),
    _PR514_LIVENESS_SHAPES,
)
def test_pr514_named_shapes_duplicate_confirmed_callbacks_are_idempotent(
    monkeypatch,
    ticker,
    contract_symbol,
):
    """A repeated confirmed callback cannot repeat claim, selector, or POST."""
    selector = _PR514SuccessfulSelector()
    osm, _broker, selector, _core, watcher, plan = _pr514_liveness_fixture(
        monkeypatch,
        ticker,
        contract_symbol,
        selector,
    )

    with patch.dict(
        sys.modules,
        {"ap.execution": _pr514_liveness_execution_module()},
    ), patch(
        "ap_entry_confirmation.check_entry_confirmation",
        return_value=_FakeConfirmResult(),
    ), patch("ap.db.conn", lambda: _NoopConn()), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        assert watcher.watch(plan, LOCAL_ORDER_ID) is True
        watched = watcher._pending[0]
        first_result = watcher.on_trigger(watched)
        second_result = watcher.on_trigger(watched)

    row = osm.get_order(LOCAL_ORDER_ID)
    assert first_result is None or first_result.get("disposition") in {
        None,
        "SUBMITTED",
    }
    assert second_result is None
    assert osm.claim_successes == 1
    assert selector.calls == 1
    assert len(osm.post_payloads) == 1
    assert row["contract"] == contract_symbol
    assert row["broker_order_id"] == "TR-323"


@pytest.mark.parametrize(
    ("ticker", "contract_symbol"),
    _PR514_LIVENESS_SHAPES,
)
def test_pr514_named_shapes_retry_waits_for_durable_clock_across_restart(
    monkeypatch,
    ticker,
    contract_symbol,
):
    """Retryable selector failure waits, then resumes once after restart."""
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_DELAY_SECONDS", "30")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "2359")
    monkeypatch.setenv("LIVE_CONFIRMATION_REQUIRED", "1")
    selector = _Selector()
    osm, broker, selector, _core, watcher, plan = _pr514_liveness_fixture(
        monkeypatch,
        ticker,
        contract_symbol,
        selector,
    )

    with patch.dict(
        sys.modules,
        {"ap.execution": _pr514_liveness_execution_module()},
    ), patch(
        "ap_entry_confirmation.check_entry_confirmation",
        return_value=_FakeConfirmResult(),
    ), patch("ap.db.conn", lambda: _NoopConn()), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        assert watcher.watch(plan, LOCAL_ORDER_ID) is True
        watched = watcher._pending[0]
        watched.overnight = False
        for _ in range(3):
            watcher._poll_active_signals(open_protect_active=False)

        row_after_failure = osm.get_order(LOCAL_ORDER_ID)
        retry_meta = row_after_failure["meta"]
        assert selector.calls == 1
        assert retry_meta["lifecycle_state"] == "RETRY_WAIT"
        assert retry_meta["materialization_status"] == "RETRY_PENDING"
        assert retry_meta["next_retry_at"] == retry_meta[
            "materialization_next_retry_at"
        ]
        assert datetime.fromisoformat(retry_meta["next_retry_at"]) > _now()

        # A confirmed watcher callback before its durable retry clock is due
        # must not create another selector/materialization attempt.
        for _ in range(3):
            watcher._poll_active_signals(open_protect_active=False)
        assert selector.calls == 1
        assert len(osm.post_payloads) == 0

        # Restart with no surviving in-memory owner. Recovery consumes the
        # durable due timestamp, advances the generation, and re-arms once.
        due_at = _iso(_now() - timedelta(seconds=1))
        osm.row["meta"]["next_retry_at"] = due_at
        osm.row["meta"]["materialization_next_retry_at"] = due_at
        core2 = _build_core(osm, broker, selector)
        core2._recover_plan_for_revalidation = lambda _watched: plan
        watcher2 = _build_watcher(osm, core2, ticker=ticker)
        _run_recovery(osm, broker, watcher2, core2)
        assert LOCAL_ORDER_ID in [
            w.signal["local_order_id"] for w in watcher2._pending
        ]
        for retry_watched in watcher2._pending:
            retry_watched.overnight = False
        for _ in range(3):
            watcher2._poll_active_signals(open_protect_active=False)

    row_after_retry = osm.get_order(LOCAL_ORDER_ID)
    assert selector.calls == 2
    assert osm.claim_successes == 2
    assert osm.claimed_generations == [1, 2]
    assert len(osm.post_payloads) == 1
    assert row_after_retry["contract"] == contract_symbol
    assert row_after_retry["broker_order_id"] == "TR-323"


@pytest.mark.parametrize(
    ("ticker", "contract_symbol"),
    _PR514_LIVENESS_SHAPES,
)
def test_pr514_named_shapes_active_owner_blocks_selector_duplicate(
    monkeypatch,
    ticker,
    contract_symbol,
):
    """A live materialization owner prevents another selector attempt."""
    selector = _PR514SuccessfulSelector()
    osm, _broker, selector, core, watcher, plan = _pr514_liveness_fixture(
        monkeypatch,
        ticker,
        contract_symbol,
        selector,
    )
    osm.row["meta"].update({
        "lifecycle_state": "MATERIALIZING",
        "materialization_status": "RUNNING",
        "materialization_in_flight": True,
        "materialization_owner": "materializer:active-owner",
        "materialization_generation": 7,
        "retry_attempt": 1,
        "materialization_lease_until": _iso(_now() + timedelta(seconds=30)),
    })
    plan.metadata.update({
        "lifecycle_state": "MATERIALIZING",
        "materialization_status": "RUNNING",
        "materialization_in_flight": True,
        "materialization_owner": "materializer:active-owner",
        "materialization_generation": 7,
        "retry_attempt": 1,
    })

    with patch.dict(
        sys.modules,
        {"ap.execution": _pr514_liveness_execution_module()},
    ), patch("ap.db.conn", lambda: _NoopConn()), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        assert watcher.watch(plan, LOCAL_ORDER_ID) is True
        result = watcher.on_trigger(watcher._pending[0])

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "MATERIALIZATION_ALREADY_OWNED"
    assert result["next_retry_at"] == osm.row["meta"]["materialization_lease_until"]
    assert selector.calls == 0
    assert osm.post_payloads == []
    assert osm.claim_successes == 0
    assert core.master_control is not None


@pytest.mark.parametrize(
    ("ticker", "contract_symbol"),
    _PR514_LIVENESS_SHAPES,
)
def test_pr514_named_shapes_stale_preclaim_generation_fails_closed(
    monkeypatch,
    ticker,
    contract_symbol,
):
    """A restart callback with a stale owner/generation cannot select or POST."""
    selector = _PR514SuccessfulSelector()
    osm, _broker, selector, _core, watcher, plan = _pr514_liveness_fixture(
        monkeypatch,
        ticker,
        contract_symbol,
        selector,
    )
    current_owner = "materializer:current-owner"
    osm.row["meta"].update({
        "lifecycle_state": "MATERIALIZING",
        "materialization_status": "RUNNING",
        "materialization_in_flight": True,
        "materialization_owner": current_owner,
        "materialization_generation": 2,
        "retry_attempt": 2,
    })
    plan.metadata.update({
        "lifecycle_state": "MATERIALIZING",
        "materialization_status": "RUNNING",
        "materialization_in_flight": True,
        "materialization_owner": current_owner,
        "materialization_generation": 2,
        "retry_attempt": 2,
    })

    with patch.dict(
        sys.modules,
        {"ap.execution": _pr514_liveness_execution_module()},
    ), patch("ap.db.conn", lambda: _NoopConn()), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        assert watcher.watch(plan, LOCAL_ORDER_ID) is True
        watched = watcher._pending[0]
        watched.signal.update({
            "_recovery_pre_claimed": True,
            "_recovery_pre_claimed_owner": "materializer:stale-owner",
            "_recovery_pre_claimed_generation": 1,
            "_recovery_pre_claimed_attempt": 1,
            "_recovery_pre_claimed_client_id": CLIENT_ID,
            "_recovery_pre_claimed_mode": "live",
        })
        result = watcher.on_trigger(watched)

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"
    assert selector.calls == 0
    assert osm.post_payloads == []
    assert osm.row["meta"]["materialization_generation"] == 2
    assert osm.row["meta"]["materialization_owner"] == current_owner


def test_pr514_paper_deferred_shape_does_not_call_live_capacity_authority(monkeypatch):
    """The deferred capacity resolver remains LIVE-only; PAPER is isolated."""
    selector = _PR514SuccessfulSelector()
    osm, broker, selector, _core, _watcher, plan = _pr514_liveness_fixture(
        monkeypatch,
        "C",
        "C260828C00133000",
        selector,
    )
    capacity_calls = []

    def _unexpected_live_capacity(**kwargs):
        capacity_calls.append(kwargs)
        raise AssertionError("PAPER must not call LIVE deferred capacity")

    master_control = types.SimpleNamespace(
        mode="PAPER",
        max_positions=5,
        _kill_switch_fn=lambda: False,
        get_entry_capacity=_unexpected_live_capacity,
        revalidate_exposure=lambda _plan, **_kwargs: types.SimpleNamespace(
            ok=True,
            reason_code="EXPOSURE_ALLOWED",
            reason="allowed",
        ),
    )
    core = _build_core(osm, broker, selector, master_control=master_control)
    core._recover_plan_for_revalidation = lambda _watched: plan
    core.paper = True
    core.mode = "PAPER"
    core.execution_mode = "paper"
    watcher = _build_watcher(osm, core, ticker="C")
    watcher.mode = "PAPER"
    osm.execution_mode = "paper"
    osm.row["execution_mode"] = "paper"
    osm.row["meta"].update({"execution_mode": "paper"})
    plan.execution_mode = "paper"
    plan.metadata.update({"execution_mode": "paper"})

    with patch.dict(
        sys.modules,
        {"ap.execution": _pr514_liveness_execution_module()},
    ), patch(
        "ap_entry_confirmation.check_entry_confirmation",
        return_value=_FakeConfirmResult(),
    ), patch("ap.db.conn", lambda: _NoopConn()), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        assert watcher.watch(plan, LOCAL_ORDER_ID) is True
        result = watcher.on_trigger(watcher._pending[0])

    assert capacity_calls == []
    assert selector.calls == 1
    assert len(osm.post_payloads) == 1
    assert result is None or result.get("disposition") in {None, "SUBMITTED"}


# ─────────────────────────────────────────────────────────────────────────────
# PR #514 follow-up — deferred materialization ownership boundary
# ─────────────────────────────────────────────────────────────────────────────


class _PR514OwnershipTraceOSM(_StatefulOSM):
    """Small durable-row double for ownership ordering and fail-closed tests."""

    def __init__(self):
        super().__init__()
        self.trace = []
        self.claim_calls = 0
        self.claim_return = True
        self.claim_error = None
        self.read_error = None
        self.missing_row = False
        self.expire_calls = 0
        self.cancel_calls = 0
        self.terminalize_return = True
        self.terminalization_kwargs = []
        self.proof_read_failures_remaining = 0

    def get_order(self, local_order_id):
        if self.read_error:
            raise RuntimeError(self.read_error)
        if self.missing_row:
            return None
        return super().get_order(local_order_id)

    def claim_deferred_materialization(self, local_order_id, **kwargs):
        self.trace.append("claim")
        self.claim_calls += 1
        if self.claim_error:
            raise RuntimeError(self.claim_error)
        if not self.claim_return:
            return False
        return super().claim_deferred_materialization(local_order_id, **kwargs)

    def terminalize_deferred_breach(self, local_order_id, **kwargs):
        self.trace.append("terminalize")
        self.terminalization_kwargs.append(dict(kwargs))
        if not self.terminalize_return:
            return False
        return super().terminalize_deferred_breach(local_order_id, **kwargs)

    def expire_pending_entry(self, local_order_id, reason):
        self.trace.append("expire")
        self.expire_calls += 1
        return False

    def cancel_pending_entry(self, local_order_id, reason):
        self.trace.append("cancel")
        self.cancel_calls += 1
        return False

    def _submit_order_with_retry(self, **kwargs):
        self.trace.append("broker_post")
        return super()._submit_order_with_retry(**kwargs)


class _PR514OwnershipTraceMC(_DeferredCapacityMC):
    def __init__(self, trace, **kwargs):
        super().__init__(**kwargs)
        self.trace = trace

    def get_entry_capacity(self, **kwargs):
        self.trace.append("capacity")
        return super().get_entry_capacity(**kwargs)

    def revalidate_exposure(self, plan, *, client_id):
        self.trace.append("revalidation")
        return super().revalidate_exposure(plan, client_id=client_id)


class _PR514OwnershipTraceSelector(_CSelector):
    def __init__(self, trace):
        super().__init__(execution_price_per_share=1.26)
        self.trace = trace

    def select(self, plan, *, request_context=None):
        self.trace.append("selector")
        return super().select(plan, request_context=request_context)


def _pr514_ownership_fixture(monkeypatch):
    trace = []
    osm = _PR514OwnershipTraceOSM()
    osm.trace = trace
    plan = _approved_plan()
    selector = _PR514OwnershipTraceSelector(trace)
    master_control = _PR514OwnershipTraceMC(trace)
    broker = _Broker()
    core = _build_core(osm, broker, selector, master_control=master_control)
    core._recover_plan_for_revalidation = lambda _watched: plan
    store_calls = {"status": [], "signal": []}
    core.store = types.SimpleNamespace(
        update_status=lambda *args, **kwargs: store_calls["status"].append((args, kwargs)),
        update_signal_fields=lambda *args, **kwargs: store_calls["signal"].append((args, kwargs)),
    )
    watcher = _build_watcher(osm, core)
    return osm, selector, master_control, core, watcher, plan, trace, store_calls


def _pr514_ownership_execution_module():
    fake_execution = types.ModuleType("ap.execution")
    fake_execution._refresh_ask_at_submit = lambda *_args, **_kwargs: (
        1.28,
        15,
        True,
        "ok",
        {
            "spread_pct": 0.02,
            "submit_bid": 1.25,
            "submit_ask": 1.28,
            "submit_mid": 1.265,
            "submit_last": 1.28,
        },
    )
    return fake_execution


def _run_pr514_scoped_callback(watcher, plan, *, signal_patch=None):
    confirmation_result = getattr(plan, "_test_confirmation_result", None)
    with patch.dict(
        sys.modules,
        {"ap.execution": _pr514_ownership_execution_module()},
    ), patch(
        "ap_entry_confirmation.check_entry_confirmation",
        return_value=confirmation_result or _FakeConfirmResult(),
    ), patch("ap.db.conn", lambda: _NoopConn()), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        assert watcher.watch(plan, LOCAL_ORDER_ID) is True
        watched = watcher._pending[0]
        if signal_patch:
            watched.signal.update(signal_patch)
        return watched, watcher.on_trigger(watched)


def _set_active_materialization(
    osm,
    *,
    owner="materializer:active-owner",
    generation=7,
    attempt=1,
    lease=None,
):
    osm.row["meta"].update({
        "lifecycle_state": "MATERIALIZING",
        "materialization_status": "RUNNING",
        "materialization_in_flight": True,
        "materialization_owner": owner,
        "materialization_generation": generation,
        "retry_attempt": attempt,
        "materialization_lease_until": lease or _iso(_now() + timedelta(seconds=30)),
    })


def _configure_pr524_ordinary_fixture(
    osm, core, watcher, plan, *, execution_mode="paper"
):
    """Turn the deferred harness into a normal, already-selected entry."""
    contract = "C260828C00133000"
    crossed_at = _iso(_now() - timedelta(seconds=10))
    mode = str(execution_mode).strip().lower()

    osm.execution_mode = mode
    osm.row.update({
        "symbol": "C",
        "execution_mode": mode,
        "contract": contract,
        "limit_price": 1.28,
        "qty": 1,
        "reserved_cost": 128.0,
    })
    osm.row["meta"] = {
        "execution_mode": mode,
        "contract_deferred": False,
        "trigger_crossed_at": crossed_at,
        "trigger_price": 130.0,
        "entry_cutoff_et": TEST_ENTRY_CUTOFF_ET,
    }

    plan.ticker = "C"
    plan.contract_symbol = contract
    plan.limit_price = 1.28
    plan.max_position_usd = 128.0
    plan.execution_mode = mode
    plan.metadata = {
        "execution_mode": mode,
        "contract_deferred": False,
        "trigger_crossed_at": crossed_at,
    }
    core.execution_mode = mode
    core.mode = mode.upper()
    core.paper = mode == "paper"
    core.master_control.mode = mode.upper()
    watcher.mode = mode.upper()


def test_pr514_fresh_deferred_claim_precedes_every_attempt_gate(monkeypatch):
    """Load-bearing order test: moving claim below any gate must fail this."""
    osm, selector, _mc, core, watcher, plan, trace, _stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    core._breach_risk_check = lambda _watched: (trace.append("risk") or True)

    _watched, result = _run_pr514_scoped_callback(watcher, plan)

    assert result is None or result.get("disposition") in {None, "SUBMITTED"}
    assert osm.claim_calls == 1
    assert selector.calls == 1
    assert trace.index("claim") < trace.index("risk")
    assert trace.index("claim") < trace.index("capacity")
    assert trace.index("claim") < trace.index("selector")
    assert trace.index("claim") < trace.index("revalidation")
    assert trace.index("claim") < trace.index("broker_post")
    assert trace.count("broker_post") == 1


def test_pr514_recovery_without_in_memory_plan_uses_recovered_cost(monkeypatch):
    osm, selector, _mc, core, _watcher, _plan, trace, _stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    core._recover_plan_for_revalidation = (
        core_mod.APExecutionCore._recover_plan_for_revalidation.__get__(
            core, type(core)
        )
    )
    watched = types.SimpleNamespace(
        signal={
            "signal_id": SIGNAL_ID,
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "live",
            "contract_deferred": True,
            "contract_symbol": "DEFERRED:SPY",
        },
        ticker="SPY",
        side="CALL",
        trigger_price=600.0,
        entry_trigger=600.0,
        stop_level=595.0,
        target_price=605.0,
        trigger_crossed_at=_now(),
    )
    core._breach_risk_check = lambda _watched: (trace.append("risk") or True)

    with patch.dict(
        sys.modules,
        {"ap.execution": _pr514_ownership_execution_module()},
    ), patch(
        "ap_entry_confirmation.check_entry_confirmation",
        return_value=_FakeConfirmResult(),
    ), patch("ap.db.conn", lambda: _NoopConn()), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        result = core._on_entry_trigger(watched)

    recovered = watched.signal["_approved_plan"]
    assert recovered.metadata["deferred_reservation_cost"] == pytest.approx(1.0)
    assert result is None or result.get("disposition") in {None, "SUBMITTED"}
    assert osm.claim_calls == 1
    assert selector.calls == 1
    assert trace.index("claim") < trace.index("risk")
    assert len(osm.post_payloads) == 1


def test_pr514_recovery_preclaim_is_proven_without_second_claim(monkeypatch):
    osm, selector, _mc, core, watcher, plan, trace, _stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    owner = "recovery_retry:owner"
    _set_active_materialization(osm, owner=owner, generation=7, attempt=1)
    plan.metadata["trigger_crossed_at"] = _iso(_now() - timedelta(seconds=10))
    core._breach_risk_check = lambda _watched: (trace.append("risk") or True)

    watched, result = _run_pr514_scoped_callback(
        watcher,
        plan,
        signal_patch={
            "_recovery_pre_claimed": True,
            "_recovery_pre_claimed_owner": owner,
            "_recovery_pre_claimed_generation": 7,
            "_recovery_pre_claimed_attempt": 1,
            "_recovery_pre_claimed_client_id": CLIENT_ID,
            "_recovery_pre_claimed_mode": "live",
        },
    )

    assert result is None or result.get("disposition") in {None, "SUBMITTED"}
    assert osm.claim_calls == 0
    assert selector.calls == 1
    assert trace.index("risk") < trace.index("selector")
    assert len(osm.post_payloads) == 1


def test_pr514_duplicate_active_owner_parks_at_exact_lease(monkeypatch):
    osm, selector, _mc, core, watcher, plan, trace, stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    lease = _iso(_now() + timedelta(seconds=45))
    _set_active_materialization(osm, owner="peer-owner", generation=9, attempt=0, lease=lease)
    row_before = copy.deepcopy(osm.row)
    osm.claim_return = False
    core._breach_risk_check = lambda _watched: pytest.fail("risk work after duplicate owner")

    _watched, result = _run_pr514_scoped_callback(watcher, plan)

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "MATERIALIZATION_ALREADY_OWNED"
    assert result["next_retry_at"] == lease
    assert osm.claim_calls == 1
    assert selector.calls == 0
    assert trace == ["claim"]
    assert osm.expire_calls == 0
    assert osm.cancel_calls == 0
    assert osm.row["meta"]["materialization_owner"] == "peer-owner"
    assert osm.row["meta"]["materialization_generation"] == 9
    assert osm.row["meta"]["retry_attempt"] == 0
    assert osm.row == row_before
    assert stores["status"] == []
    assert stores["signal"] == []


@pytest.mark.parametrize("block", ["kill_switch", "positions_full"])
def test_pr514_duplicate_owner_does_no_work_even_when_risk_would_block(monkeypatch, block):
    osm, selector, _mc, core, watcher, plan, trace, stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    lease = _iso(_now() + timedelta(seconds=45))
    _set_active_materialization(osm, owner="peer-owner", generation=9, attempt=0, lease=lease)
    osm.claim_return = False
    if block == "kill_switch":
        core._kill_switch = True
    else:
        core._current_open_position_count = lambda: 5
        core._current_pending_entry_count = lambda: 0
    core._breach_risk_check = lambda _watched: pytest.fail("duplicate reached risk work")

    _watched, result = _run_pr514_scoped_callback(watcher, plan)

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "MATERIALIZATION_ALREADY_OWNED"
    assert selector.calls == 0
    assert trace == ["claim"]
    assert stores["status"] == []
    assert stores["signal"] == []
    assert osm.expire_calls == 0
    assert osm.cancel_calls == 0


@pytest.mark.parametrize(
    ("case", "expected_reason"),
    [
        ("stale_owner", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
        ("expired_lease", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
        ("malformed_lease", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
        ("timezone_naive_lease", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
        ("missing_lease", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
        ("missing_owner", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
        ("generation_zero", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
        ("generation_negative", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
        ("generation_bool", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
        ("wrong_client_id", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
        ("wrong_execution_mode", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
        ("wrong_signal_id", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
        ("stale_generation", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
        ("wrong_owner", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
        ("malformed_preclaim_attempt", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
        ("missing_durable_row", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
        ("db_read_failure", "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"),
    ],
)
def test_pr514_preclaim_proof_failures_keep_watcher_and_do_zero_work(
    monkeypatch, case, expected_reason
):
    osm, selector, _mc, core, watcher, plan, trace, stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    owner = "preclaim-owner"
    _set_active_materialization(osm, owner=owner, generation=7, attempt=1)
    marker = {
        "_recovery_pre_claimed": True,
        "_recovery_pre_claimed_owner": owner,
        "_recovery_pre_claimed_generation": 7,
        "_recovery_pre_claimed_attempt": 1,
        "_recovery_pre_claimed_client_id": CLIENT_ID,
        "_recovery_pre_claimed_mode": "live",
    }
    if case == "stale_owner":
        osm.row["meta"]["materialization_owner"] = "old-owner"
    elif case == "expired_lease":
        osm.row["meta"]["materialization_lease_until"] = _iso(_now() - timedelta(seconds=1))
    elif case == "malformed_lease":
        osm.row["meta"]["materialization_lease_until"] = "not-a-lease"
    elif case == "timezone_naive_lease":
        osm.row["meta"]["materialization_lease_until"] = "2099-01-01T00:00:00"
    elif case == "missing_lease":
        osm.row["meta"].pop("materialization_lease_until", None)
    elif case == "missing_owner":
        osm.row["meta"]["materialization_owner"] = ""
    elif case == "generation_zero":
        osm.row["meta"]["materialization_generation"] = 0
    elif case == "generation_negative":
        osm.row["meta"]["materialization_generation"] = -1
    elif case == "generation_bool":
        osm.row["meta"]["materialization_generation"] = True
    elif case == "wrong_client_id":
        osm.row["client_id"] = "other@example.com"
    elif case == "wrong_execution_mode":
        osm.row["execution_mode"] = "paper"
    elif case == "wrong_signal_id":
        osm.row["signal_id"] = "other-signal"
    elif case == "stale_generation":
        osm.row["meta"]["materialization_generation"] = 8
    elif case == "wrong_owner":
        marker["_recovery_pre_claimed_owner"] = "wrong-owner"
    elif case == "malformed_preclaim_attempt":
        marker["_recovery_pre_claimed_attempt"] = True
    elif case == "missing_durable_row":
        osm.missing_row = True
    elif case == "db_read_failure":
        osm.read_error = "db_hiccup"
    row_before = copy.deepcopy(osm.row)
    core._breach_risk_check = lambda _watched: pytest.fail("invalid preclaim reached risk work")

    _watched, result = _run_pr514_scoped_callback(
        watcher, plan, signal_patch=marker
    )

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == expected_reason
    assert osm.row == row_before
    assert osm.claim_calls == 0
    assert selector.calls == 0
    assert trace == []
    assert osm.expire_calls == 0
    assert osm.cancel_calls == 0
    assert stores["status"] == []
    assert stores["signal"] == []


def test_pr514_claim_db_failure_keeps_watcher_before_selector(monkeypatch):
    osm, selector, _mc, core, watcher, plan, trace, stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    osm.claim_error = "db_write_failed"
    core._breach_risk_check = lambda _watched: pytest.fail("claim failure reached risk work")

    _watched, result = _run_pr514_scoped_callback(watcher, plan)

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "MATERIALIZATION_STATE_WRITE_FAILED"
    assert osm.claim_calls == 1
    assert selector.calls == 0
    assert trace == ["claim"]
    assert osm.expire_calls == 0
    assert osm.cancel_calls == 0
    assert stores["status"] == []
    assert stores["signal"] == []


def test_pr514_missing_client_is_not_inferred_from_runner(monkeypatch):
    osm, selector, _mc, core, watcher, plan, trace, stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    plan.client_id = ""
    core._breach_risk_check = lambda _watched: pytest.fail(
        "missing client identity reached risk work"
    )

    _watched, result = _run_pr514_scoped_callback(watcher, plan)

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "MATERIALIZATION_OWNERSHIP_UNPROVEN"
    assert osm.claim_calls == 0
    assert selector.calls == 0
    assert trace == []
    assert stores["status"] == []
    assert stores["signal"] == []


@pytest.mark.parametrize(
    ("column_mode", "meta_mode", "callback_mode", "expected_valid"),
    [
        ("live", "live", "live", True),
        (" live ", "live", "live", True),
        ("live", "", "live", True),
        ("paper", "", "paper", True),
        ("", "live", "live", True),
        ("", "paper", "paper", True),
        ("live", "paper", "live", False),
        ("paper", "live", "paper", False),
        ("", "", "live", False),
        ("", None, "live", False),
        ("", "staging", "live", False),
        ("live", "", "paper", False),
        ("paper", "", "live", False),
    ],
)
def test_pr514_durable_execution_mode_authority_matrix(
    monkeypatch, column_mode, meta_mode, callback_mode, expected_valid,
):
    """Durable column/meta mode is canonical, with no runner inference."""
    osm, selector, _mc, core, _watcher, _plan, trace, _stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    osm.row["execution_mode"] = column_mode
    if meta_mode is None:
        osm.row["meta"].pop("execution_mode", None)
    else:
        osm.row["meta"]["execution_mode"] = meta_mode
    watched = types.SimpleNamespace(
        signal={
            "signal_id": SIGNAL_ID,
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "execution_mode": callback_mode,
        },
        ticker="SPY",
        trigger_price=600.0,
        last_quote_ask=600.20,
        last_quote_bid=600.10,
        trigger_crossed_at=_now(),
    )

    result = core._claim_deferred_materialization_for_trigger(
        watched=watched,
        signal=watched.signal,
        local_order_id=LOCAL_ORDER_ID,
        ticker="SPY",
        client_id=CLIENT_ID,
        execution_mode=callback_mode,
        signal_id=SIGNAL_ID,
    )

    if expected_valid:
        assert result["disposition"] == "OWNED"
        assert osm.claim_calls == 1
    else:
        assert result["disposition"] == "KEEP_WATCHER"
        assert result["reason_code"] == "MATERIALIZATION_STATE_WRITE_FAILED"
        assert osm.claim_calls == 0
    assert selector.calls == 0
    assert osm.post_payloads == []
    assert osm.expire_calls == 0
    assert osm.cancel_calls == 0
    assert trace == (["claim"] if expected_valid else [])


@pytest.mark.parametrize(
    ("failure", "expected_reason"),
    [
        ("blocked", "exposure_revalidation_blocked"),
        ("error", "exposure_revalidation_error_live"),
    ],
)
def test_pr514_owned_deferred_exposure_reason_survives_terminal_cas(
    monkeypatch, failure, expected_reason,
):
    osm, selector, master_control, core, watcher, plan, trace, stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    # Keep this callback in the deferred ownership path while forcing the
    # existing exposure gate to execute, so the reason preservation is tested
    # at the same owner/generation terminal boundary as kill/capacity blocks.
    core._plan_is_deferred = lambda *_args: False
    diagnostics = []
    core._emit_breach_diag = lambda *_args, **kwargs: diagnostics.append(kwargs)
    if failure == "blocked":
        master_control.final_ok = False
        master_control.final_reason = "EXPOSURE_CAP"
    else:
        def _raise_exposure(*_args, **_kwargs):
            raise RuntimeError("exposure_db_unavailable")
        master_control.revalidate_exposure = _raise_exposure
    core._breach_risk_check = core_mod.APExecutionCore._breach_risk_check.__get__(
        core, type(core)
    )

    _watched, result = _run_pr514_scoped_callback(watcher, plan)

    assert result["disposition"] == "TERMINAL_DURABLE"
    assert result["reason_code"] == expected_reason
    assert osm.terminalization_kwargs[0]["reason_code"] == expected_reason
    assert osm.terminalization_kwargs[0]["diagnostics"]["failure_stage"] == "breach_risk_check"
    if failure == "blocked":
        assert osm.terminalization_kwargs[0]["diagnostics"]["mc_block_reason"] == "EXPOSURE_CAP"
    assert selector.calls == 0
    assert osm.post_payloads == []
    assert osm.expire_calls == 0
    assert osm.cancel_calls == 0
    assert stores["signal"]
    assert diagnostics and diagnostics[0]["reason"] == expected_reason
    assert trace.index("claim") < trace.index("terminalize")


@pytest.mark.parametrize(
    ("risk_block", "expected_reason"),
    [
        ("kill_switch", "kill_switch_active"),
        ("master_kill_switch", "master_control_kill_switch_active"),
        ("positions_full", "positions_full_at_breach"),
    ],
)
def test_pr514_owned_deferred_risk_failure_uses_exact_terminal_cas(
    monkeypatch, risk_block, expected_reason,
):
    osm, selector, _mc, core, watcher, plan, trace, stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    if risk_block == "kill_switch":
        core._kill_switch = True
    elif risk_block == "master_kill_switch":
        core.master_control._kill_switch_fn = lambda: True
    else:
        core._current_open_position_count = lambda: 5
        core._current_pending_entry_count = lambda: 0
    core._breach_risk_check = core_mod.APExecutionCore._breach_risk_check.__get__(
        core, type(core)
    )

    _watched, result = _run_pr514_scoped_callback(watcher, plan)

    assert result["disposition"] == "TERMINAL_DURABLE"
    assert osm.claim_calls == 1
    assert selector.calls == 0
    assert osm.expire_calls == 0
    assert osm.cancel_calls == 0
    assert trace.index("claim") < trace.index("terminalize")
    assert result["reason_code"] == expected_reason
    assert osm.terminalization_kwargs[0]["reason_code"] == expected_reason
    assert osm.terminalization_kwargs[0]["owner"] == osm.row["meta"]["materialization_owner"]
    assert osm.terminalization_kwargs[0]["generation"] == osm.row["meta"]["materialization_generation"]
    assert osm.terminalization_kwargs[0]["diagnostics"]["failure_stage"] == "breach_risk_check"
    assert stores["status"] == []
    assert stores["signal"]


def test_pr514_owned_terminal_cas_loss_keeps_watcher_without_generic_cleanup(monkeypatch):
    osm, selector, _mc, core, watcher, plan, trace, stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    osm.terminalize_return = False
    core._kill_switch = True
    diagnostics = []
    core._emit_breach_diag = lambda *_args, **kwargs: diagnostics.append(kwargs)
    core._breach_risk_check = core_mod.APExecutionCore._breach_risk_check.__get__(
        core, type(core)
    )

    watched, result = _run_pr514_scoped_callback(watcher, plan)

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "BREACH_TERMINAL_WRITE_FAILED"
    assert osm.claim_calls == 1
    assert selector.calls == 0
    assert trace == ["claim", "terminalize"]
    assert osm.expire_calls == 0
    assert osm.cancel_calls == 0
    assert stores["status"] == []
    assert stores["signal"] == []
    assert diagnostics == []
    assert watched.signal["_deferred_breach_risk_reason"] == "kill_switch_active"


def test_pr524_selector_terminal_cas_loss_is_returned_to_watcher(monkeypatch):
    osm, selector, _mc, core, watcher, plan, trace, _stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    osm.terminalize_return = False

    def _invalid_selector_result(_plan, *, request_context=None):
        selector.calls += 1
        trace.append("selector")
        return types.SimpleNamespace(
            contract_symbol=f"junk{REAL_OCC}",
            execution_price_per_share=1.25,
            affordable_contracts=1,
        )

    selector.select = _invalid_selector_result

    _watched, result = _run_pr514_scoped_callback(watcher, plan)

    assert result == {
        "disposition": "KEEP_WATCHER",
        "reason_code": "BREACH_TERMINAL_WRITE_FAILED",
        "retry_after_seconds": 5,
    }
    assert selector.calls == 1
    assert osm.post_payloads == []
    assert osm.terminalizations == []
    assert trace[-1] == "terminalize"


def test_pr514_stale_deferred_flag_on_real_contract_does_not_reclaim_or_select(monkeypatch):
    osm, selector, _mc, core, watcher, plan, trace, _stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    plan.contract_symbol = REAL_OCC
    plan.limit_price = 2.10
    plan.max_position_usd = 210.0
    plan.metadata.update({"contract_deferred": True})
    osm.row.update({
        "contract": REAL_OCC,
        "limit_price": 2.10,
        "reserved_cost": 210.0,
    })
    osm.row["meta"].update({
        "contract_deferred": True,
        "contract_symbol": REAL_OCC,
        "execution_mode": "live",
    })
    core._breach_risk_check = lambda _watched: (trace.append("risk") or True)

    _watched, _result = _run_pr514_scoped_callback(watcher, plan)

    assert osm.claim_calls == 0
    assert selector.calls == 0


def test_pr524_memory_real_durable_deferred_does_not_bypass_ownership(monkeypatch):
    osm, selector, _mc, core, watcher, plan, trace, stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    plan.contract_symbol = REAL_OCC
    plan.limit_price = 2.10
    plan.max_position_usd = 210.0
    plan.metadata.update({"contract_deferred": True})
    core._breach_risk_check = lambda _watched: pytest.fail(
        "durable DEFERRED truth reached risk work"
    )

    _watched, result = _run_pr514_scoped_callback(watcher, plan)

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "MATERIALIZATION_OWNERSHIP_UNPROVEN"
    assert osm.claim_calls == 0
    assert selector.calls == 0
    assert trace == []
    assert stores["status"] == []
    assert stores["signal"] == []


def test_pr524_memory_real_durable_same_real_continues_hydrated(monkeypatch):
    osm, selector, _mc, core, watcher, plan, trace, _stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    plan.contract_symbol = REAL_OCC
    plan.limit_price = 2.10
    plan.max_position_usd = 210.0
    plan.metadata.update({
        "contract_deferred": True,
        "trigger_crossed_at": _iso(_now() - timedelta(seconds=10)),
    })
    osm.row.update({
        "contract": REAL_OCC,
        "limit_price": 1.29,
        "reserved_cost": 129.0,
    })
    osm.row["meta"].update({
        "contract_deferred": False,
        "materialization_status": "SELECTED",
        "materialization_generation": 7,
        "broker_ready": True,
        "lifecycle_state": "BROKER_READY",
        "current_owner": "watcher:hydrated",
        "materialization_owner": "watcher:hydrated",
    })
    core._breach_risk_check = lambda _watched: (trace.append("risk") or True)

    _watched, result = _run_pr514_scoped_callback(watcher, plan)

    assert result is None or result.get("disposition") in {None, "SUBMITTED"}
    assert osm.claim_calls == 0
    assert selector.calls == 0
    assert trace.index("risk") >= 0
    assert trace.index("broker_post") >= 0
    assert len(osm.post_payloads) == 1


def test_pr524_ordinary_valid_plan_keeps_main_callback_path(monkeypatch):
    osm, selector, _mc, core, watcher, plan, trace, stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    _configure_pr524_ordinary_fixture(osm, core, watcher, plan)
    core._breach_risk_check = lambda _watched: (trace.append("risk") or True)
    submit_calls = []

    def _submit_existing_entry(**kwargs):
        trace.append("submit")
        submit_calls.append(kwargs)
        return {
            "ok": True,
            "local_order_id": LOCAL_ORDER_ID,
            "broker_order_id": "ordinary-paper-submit",
            "status": "ACK",
        }

    osm.submit_existing_entry = _submit_existing_entry

    _watched, result = _run_pr514_scoped_callback(watcher, plan)

    assert result is None or result.get("disposition") in {None, "SUBMITTED"}
    assert trace[0] == "risk"
    assert trace[-1] == "submit"
    assert "claim" not in trace
    assert selector.calls == 0
    assert stores["status"]
    assert len(submit_calls) == 1
    assert submit_calls[0]["plan"] is plan


def test_pr524_ordinary_missing_plan_uses_main_failure_path(monkeypatch):
    osm, selector, _mc, core, watcher, plan, trace, stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    _configure_pr524_ordinary_fixture(
        osm, core, watcher, plan, execution_mode="live"
    )
    core._breach_risk_check = lambda _watched: (trace.append("risk") or True)
    core._recover_plan_for_revalidation = lambda _watched: (
        trace.append("recover") or None
    )

    with patch.dict(
        sys.modules,
        {"ap.execution": _pr514_ownership_execution_module()},
    ), patch(
        "ap_entry_confirmation.check_entry_confirmation",
        return_value=_FakeConfirmResult(),
    ), patch("ap.db.conn", lambda: _NoopConn()), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        assert watcher.watch(plan, LOCAL_ORDER_ID) is True
        watched = watcher._pending[0]
        watched.signal.pop("_approved_plan", None)
        result = watcher.on_trigger(watched)

    assert result is None
    assert trace[0] == "risk"
    assert trace.index("risk") < trace.index("recover") < trace.index("expire")
    assert selector.calls == 0
    assert osm.post_payloads == []
    assert stores["status"]
    assert any(
        len(args) > 1
        and args[1].get("context_notes") == (
            "approved_plan_missing_after_revalidation"
        )
        for args, _kwargs in stores["signal"]
    )


def test_pr524_deferred_missing_plan_keeps_materialization_boundary(monkeypatch):
    osm, selector, _mc, core, watcher, plan, trace, stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    core._breach_risk_check = lambda _watched: pytest.fail(
        "deferred plan recovery failure reached ordinary risk work"
    )
    core._recover_plan_for_revalidation = lambda _watched: None

    with patch.dict(
        sys.modules,
        {"ap.execution": _pr514_ownership_execution_module()},
    ), patch(
        "ap_entry_confirmation.check_entry_confirmation",
        return_value=_FakeConfirmResult(),
    ), patch("ap.db.conn", lambda: _NoopConn()), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        assert watcher.watch(plan, LOCAL_ORDER_ID) is True
        watched = watcher._pending[0]
        watched.signal.pop("_approved_plan", None)
        result = watcher.on_trigger(watched)

    assert result == {
        "disposition": "KEEP_WATCHER",
        "reason_code": "MATERIALIZATION_PLAN_RECOVERY_FAILED",
        "retry_after_seconds": 5,
    }
    assert selector.calls == 0
    assert osm.post_payloads == []
    assert trace == []
    assert stores["status"] == []
    assert stores["signal"] == []


def test_pr524_recovery_materialization_broker_ready_gate_uses_exact_terminal_cas(
    monkeypatch,
):
    osm, selector, master_control, core, watcher, plan, trace, _stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    owner = "recovery_retry:broker-ready"
    _set_active_materialization(osm, owner=owner, generation=7, attempt=1)
    master_control.final_ok_sequence = [True, True]
    master_control.final_reason_sequence = [
        "EXPOSURE_ALLOWED",
        "EXPOSURE_ALLOWED",
    ]
    plan._test_confirmation_result = types.SimpleNamespace(
        passed=False,
        fail_reason="ENTRY_CONFIRMATION_REJECTED",
        metadata={"live_entry_ts": _iso()},
        to_meta=lambda **kwargs: {"passed": False, **kwargs},
    )
    marker = {
        "_recovery_pre_claimed": True,
        "_recovery_pre_claimed_owner": owner,
        "_recovery_pre_claimed_generation": 7,
        "_recovery_pre_claimed_attempt": 1,
        "_recovery_pre_claimed_client_id": CLIENT_ID,
        "_recovery_pre_claimed_mode": "live",
    }

    _watched, result = _run_pr514_scoped_callback(
        watcher, plan, signal_patch=marker
    )

    assert result["disposition"] == "TERMINAL_DURABLE"
    assert result["reason_code"] == "ENTRY_CONFIRMATION_REJECTED"
    assert selector.calls == 1
    assert osm.post_payloads == []
    assert len(osm.broker_ready_terminalization_calls) == 1
    assert osm.broker_ready_terminalization_calls[0]["owner"] == owner
    assert osm.broker_ready_terminalization_calls[0]["generation"] == 7
    assert osm.broker_ready_terminalization_calls[0]["retry_attempt"] == 1
    assert osm.broker_ready_terminalization_calls[0]["expected_direction"] == "CALL"
    assert (
        osm.broker_ready_terminalization_calls[0]["expected_contract_symbol"]
        == osm.row["contract"]
    )
    assert osm.terminalizations == [
        (result["reason_code"], "EXPIRED"),
    ]
    assert osm.row["status"] == "EXPIRED"


@pytest.mark.parametrize(
    ("terminalize_ok", "expected_disposition"),
    [(True, "TERMINAL_DURABLE"), (False, "KEEP_WATCHER")],
)
def test_pr524_owned_late_terminal_cas_disposition_reaches_callback(
    monkeypatch, terminalize_ok, expected_disposition
):
    osm, selector, master_control, core, watcher, plan, _trace, _stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    owner = "recovery_retry:late-terminal"
    _set_active_materialization(osm, owner=owner, generation=7, attempt=1)
    master_control.final_ok_sequence = [True, True]
    master_control.final_reason_sequence = [
        "EXPOSURE_ALLOWED",
        "EXPOSURE_ALLOWED",
    ]
    terminalization_calls = []
    original_terminalize = osm.terminalize_owned_broker_ready_materialization

    def _terminalize(local_order_id, **kwargs):
        terminalization_calls.append(dict(kwargs))
        if not terminalize_ok:
            return False
        return original_terminalize(local_order_id, **kwargs)

    osm.terminalize_owned_broker_ready_materialization = _terminalize

    def _raise_confirmation(*_args, **_kwargs):
        raise RuntimeError("confirmation_probe")

    marker = {
        "_recovery_pre_claimed": True,
        "_recovery_pre_claimed_owner": owner,
        "_recovery_pre_claimed_generation": 7,
        "_recovery_pre_claimed_attempt": 1,
        "_recovery_pre_claimed_client_id": CLIENT_ID,
        "_recovery_pre_claimed_mode": "live",
    }

    with patch.dict(
        sys.modules,
        {"ap.execution": _pr514_ownership_execution_module()},
    ), patch(
        "ap_entry_confirmation.check_entry_confirmation",
        side_effect=_raise_confirmation,
    ), patch("ap.db.conn", lambda: _NoopConn()), patch(
        "ap.db.run_with_retry",
        lambda fn, *a, **k: fn(),
    ):
        assert watcher.watch(plan, LOCAL_ORDER_ID) is True
        watched = watcher._pending[0]
        watched.signal.update(marker)
        result = watcher.on_trigger(watched)

    assert result["disposition"] == expected_disposition
    assert selector.calls == 1
    assert len(terminalization_calls) == 1
    assert osm.post_payloads == []
    if terminalize_ok:
        assert result["reason_code"] == "entry_confirm_error:confirmation_probe"
        assert osm.row["status"] == "EXPIRED"
    else:
        assert result == {
            "disposition": "KEEP_WATCHER",
            "reason_code": "BREACH_TERMINAL_WRITE_FAILED",
            "retry_after_seconds": 5,
        }
        assert osm.row["status"] == "PENDING_TRIGGER"
        assert osm.row["meta"]["lifecycle_state"] == "BROKER_READY"


@pytest.mark.parametrize(
    ("identity_field", "drifted_value"),
    [
        ("direction", "PUT"),
        ("contract", "SPY260717P00600000"),
    ],
)
def test_pr524_broker_ready_terminal_cas_rejects_economic_identity_drift(
    monkeypatch, identity_field, drifted_value,
):
    osm, selector, master_control, core, watcher, plan, _trace, _stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    owner = "recovery_retry:broker-ready"
    _set_active_materialization(osm, owner=owner, generation=7, attempt=1)
    master_control.final_ok_sequence = [True, True]
    master_control.final_reason_sequence = [
        "EXPOSURE_ALLOWED",
        "EXPOSURE_ALLOWED",
    ]
    plan._test_confirmation_result = types.SimpleNamespace(
        passed=False,
        fail_reason="ENTRY_CONFIRMATION_REJECTED",
        metadata={"live_entry_ts": _iso()},
        to_meta=lambda **kwargs: {"passed": False, **kwargs},
    )
    original_persist = osm.persist_deferred_broker_ready

    def _persist_then_drift(local_order_id, **kwargs):
        result = original_persist(local_order_id, **kwargs)
        if result:
            osm.row[identity_field] = drifted_value
        return result

    osm.persist_deferred_broker_ready = _persist_then_drift
    marker = {
        "_recovery_pre_claimed": True,
        "_recovery_pre_claimed_owner": owner,
        "_recovery_pre_claimed_generation": 7,
        "_recovery_pre_claimed_attempt": 1,
        "_recovery_pre_claimed_client_id": CLIENT_ID,
        "_recovery_pre_claimed_mode": "live",
    }

    _watched, result = _run_pr514_scoped_callback(
        watcher, plan, signal_patch=marker
    )

    assert result == {
        "disposition": "KEEP_WATCHER",
        "reason_code": "BREACH_TERMINAL_WRITE_FAILED",
        "retry_after_seconds": 5,
    }
    assert selector.calls == 1
    assert osm.post_payloads == []
    assert osm.terminalizations == []
    assert len(osm.broker_ready_terminalization_calls) == 1


def test_pr514_recovery_preclaim_on_hydrated_contract_is_still_proven(monkeypatch):
    osm, selector, _mc, core, watcher, plan, trace, stores = (
        _pr514_ownership_fixture(monkeypatch)
    )
    owner = "recovery_retry:hydrated"
    lease = _iso(_now() + timedelta(seconds=30))
    _set_active_materialization(osm, owner=owner, generation=7, attempt=1, lease=lease)
    osm.row["contract"] = REAL_OCC
    osm.row["limit_price"] = 2.10
    osm.row["reserved_cost"] = 210.0
    plan.contract_symbol = REAL_OCC
    plan.limit_price = 2.10
    plan.max_position_usd = 210.0
    plan.metadata.update({
        "contract_deferred": True,
        "trigger_crossed_at": _iso(_now() - timedelta(seconds=10)),
    })
    osm.submit_existing_entry = lambda **_kwargs: {
        "ok": True,
        "local_order_id": LOCAL_ORDER_ID,
        "broker_order_id": "hydrated-proof",
        "status": "ACK",
    }
    marker = {
        "_recovery_pre_claimed": True,
        "_recovery_pre_claimed_owner": owner,
        "_recovery_pre_claimed_generation": 7,
        "_recovery_pre_claimed_attempt": 1,
        "_recovery_pre_claimed_client_id": CLIENT_ID,
        "_recovery_pre_claimed_mode": "live",
    }
    core._breach_risk_check = lambda _watched: (trace.append("risk") or True)

    watched, result = _run_pr514_scoped_callback(
        watcher, plan, signal_patch=marker
    )

    assert result is None or result.get("disposition") in {None, "SUBMITTED"}
    assert watched.signal.get("_deferred_materialization_owned") is True
    assert osm.claim_calls == 0
    assert selector.calls == 0
    assert trace.index("risk") >= 0
    assert osm.post_payloads == []
    assert stores["status"] == []
