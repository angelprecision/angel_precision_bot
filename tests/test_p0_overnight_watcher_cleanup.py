from __future__ import annotations

import importlib
import logging
import os
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))

FIXED_ET = datetime(2026, 6, 12, 9, 15, tzinfo=ZoneInfo("America/New_York"))


class _FakeOrderStateMachine:
    def __init__(self, *, cleanup_succeeds: bool = True):
        self.orders: dict[str, dict] = {}
        self.create_calls = 0
        self.cleanup_succeeds = cleanup_succeeds
        self.expire_calls: list[tuple[str, str]] = []
        self.cancel_calls: list[tuple[str, str]] = []
        self.transition_calls: list[tuple[str, str, dict]] = []

    def create_entry_order(self, plan, initial_status="CREATED", execution_mode=None):
        self.create_calls += 1
        local_order_id = "local-ord-1"
        self.orders[local_order_id] = {
            "status": initial_status,
            "execution_mode": execution_mode,
            "contract": getattr(plan, "contract_symbol", None),
        }
        return local_order_id

    def mark_entry_pending_trigger(self, local_order_id: str) -> bool:
        self.orders.setdefault(local_order_id, {})["status"] = "PENDING_TRIGGER"
        return True

    def expire_pending_entry(self, local_order_id: str, *, reason: str = "") -> bool:
        self.expire_calls.append((local_order_id, reason))
        if not self.cleanup_succeeds:
            return False
        self.orders.setdefault(local_order_id, {})["status"] = "EXPIRED"
        self.orders[local_order_id]["last_error"] = reason
        return True

    def cancel_pending_entry(self, local_order_id: str, *, reason: str = "") -> bool:
        self.cancel_calls.append((local_order_id, reason))
        if not self.cleanup_succeeds:
            return False
        self.orders.setdefault(local_order_id, {})["status"] = "CANCELED"
        self.orders[local_order_id]["last_error"] = reason
        return True

    def transition(self, local_order_id: str, new_status: str, **kwargs) -> bool:
        self.transition_calls.append((local_order_id, new_status, kwargs))
        if not self.cleanup_succeeds:
            return False
        self.orders.setdefault(local_order_id, {})["status"] = new_status
        self.orders[local_order_id].update(kwargs)
        return True


class _FakeOpportunityLedger(types.ModuleType):
    class _Query:
        def __init__(self, storage: dict[tuple[str, str], dict]):
            self._storage = storage
            self._filters: dict[str, str] = {}

        def select(self, *_args, **_kwargs):
            return self

        def eq(self, key: str, value: str):
            self._filters[key] = value
            return self

        def limit(self, _n: int):
            return self

        def execute(self):
            key = (
                self._filters.get("canonical_signal_id"),
                self._filters.get("client_id"),
            )
            row = self._storage.get(key)
            return types.SimpleNamespace(data=[dict(row)] if row else [])

    class _SB:
        def __init__(self, storage: dict[tuple[str, str], dict]):
            self._storage = storage

        def table(self, name: str):
            assert name == "client_signal_opportunities"
            return _FakeOpportunityLedger._Query(self._storage)

    def __init__(self):
        super().__init__("ap.opportunity_ledger")
        self.rows: dict[tuple[str, str], dict] = {}
        self.invalidated_calls: list[dict] = []
        self.internal_error_calls: list[dict] = []

    def _get_sb(self):
        return self._SB(self.rows)

    def create_opportunities(
        self,
        signal_id: str,
        client_ids: list[str],
        payload: dict,
        canonical_signal_id: str | None = None,
        sb=None,
    ) -> int:
        canonical = canonical_signal_id or payload.get("canonical_signal_id") or signal_id
        for client_id in client_ids:
            self.rows.setdefault(
                (canonical, client_id),
                {
                    "signal_id": signal_id,
                    "canonical_signal_id": canonical,
                    "client_id": client_id,
                    "opportunity_status": "CREATED",
                    "miss_stage": None,
                    "miss_reason": None,
                    "metadata": {},
                },
            )
        return len(client_ids)

    def mark_watcher_invalidated(
        self,
        signal_id: str,
        client_id: str,
        reason: str,
        *,
        canonical_signal_id: str | None = None,
        order_local_id: str | None = None,
        extra_meta: dict | None = None,
        **_kwargs,
    ) -> bool:
        canonical = canonical_signal_id or signal_id
        row = self.rows.setdefault(
            (canonical, client_id),
            {
                "signal_id": signal_id,
                "canonical_signal_id": canonical,
                "client_id": client_id,
                "metadata": {},
            },
        )
        row.update(
            {
                "opportunity_status": "MISSED",
                "miss_stage": "WATCHER_ARM",
                "miss_reason": reason,
                "order_local_id": order_local_id,
                "metadata": {**(row.get("metadata") or {}), **(extra_meta or {})},
            }
        )
        self.invalidated_calls.append(dict(row))
        return True

    def mark_internal_error(
        self,
        signal_id: str,
        client_id: str,
        miss_reason: str,
        *,
        canonical_signal_id: str | None = None,
        order_local_id: str | None = None,
        extra_meta: dict | None = None,
        **_kwargs,
    ) -> bool:
        canonical = canonical_signal_id or signal_id
        row = self.rows.setdefault(
            (canonical, client_id),
            {
                "signal_id": signal_id,
                "canonical_signal_id": canonical,
                "client_id": client_id,
                "metadata": {},
            },
        )
        row.update(
            {
                "opportunity_status": "INTERNAL_ERROR",
                "miss_stage": "INTERNAL_ERROR",
                "miss_reason": miss_reason,
                "order_local_id": order_local_id,
                "metadata": {**(row.get("metadata") or {}), **(extra_meta or {})},
            }
        )
        self.internal_error_calls.append(dict(row))
        return True


def _install_reeval_stubs(monkeypatch, ledger: _FakeOpportunityLedger | None = None):
    fake_validator = types.ModuleType("ap.overnight_daily_validator")
    fake_validator.fetch_market_snapshot = lambda ticker, broker: {"last": 100.0}
    fake_validator.validate_overnight_daily_signal = (
        lambda **kwargs: types.SimpleNamespace(
            valid=True,
            reason_code="",
            reason_text="",
        )
    )
    fake_validator.InvalidationReason = object
    monkeypatch.setitem(sys.modules, "ap.overnight_daily_validator", fake_validator)

    fake_auth = types.ModuleType("ap.authorization")
    fake_auth.is_live_broker = lambda broker: False
    fake_auth.broker_live_mode_known = lambda broker: True
    fake_auth.check_live_authorization = lambda client_id: None
    fake_auth.authorization_gate_enforced = lambda: False
    fake_auth.LIVE_AUTHORIZATION_GATE_UNAVAILABLE = "LIVE_AUTHORIZATION_GATE_UNAVAILABLE"
    fake_auth.execution_mode_for_broker = lambda broker: "PAPER"
    monkeypatch.setitem(sys.modules, "ap.authorization", fake_auth)

    if ledger is not None:
        monkeypatch.setitem(sys.modules, "ap.opportunity_ledger", ledger)


def _make_plan():
    return types.SimpleNamespace(
        plan_id="plan-001",
        signal_id="sig-001",
        ticker="AAPL",
        side="CALL",
        direction="CALL",
        score=75.0,
        timeframe="1d",
        entry_trigger=101.0,
        trigger_price=101.0,
        trigger_type="breach",
        prior_day_high=100.0,
        prior_day_low=95.0,
        pattern="2-3",
        tier="A",
        contract_symbol="AAPL260619C00100000",
        contracts=1,
        limit_price=1.25,
        metadata={},
    )


def _make_signal():
    return {
        "signal_id": "sig-001",
        "canonical_signal_id": "CANON-001",
        "ticker": "AAPL",
        "symbol": "AAPL",
        "side": "CALL",
        "timeframe": "1d",
        "score": 75.0,
        "pattern": "2-3",
        "tier": "A",
        "entry_trigger": 101.0,
        "created_at": "2026-06-11T20:00:00+00:00",
    }


def _make_job(source: str) -> dict:
    signal = _make_signal()
    if source == "ap_signals":
        return {
            "id": "sup:sig-001",
            "signal_id": signal["signal_id"],
            "payload": signal,
            "_source": "ap_signals",
        }
    return {
        "id": "job-001",
        "signal_id": signal["signal_id"],
        "payload": signal,
        "_source": "trade_queue",
    }


def _run_reeval(
    monkeypatch,
    entry_watcher,
    *,
    source: str = "trade_queue",
    ledger: _FakeOpportunityLedger | None = None,
    cleanup_succeeds: bool = True,
):
    import ap_overnight_reeval as ov

    _install_reeval_stubs(monkeypatch, ledger=ledger)
    monkeypatch.setattr(ov, "_et_now", lambda: FIXED_ET)
    monkeypatch.setattr(
        ov,
        "_fetch_watching_signals",
        lambda client_id: [_make_job(source)],
    )

    rejected_calls: list[tuple[str, str, str]] = []
    error_calls: list[tuple[str, str, str]] = []
    monkeypatch.setattr(
        ov,
        "_mark_job_rejected",
        lambda job_id, client_id, reason: rejected_calls.append((str(job_id), client_id, reason)),
    )
    monkeypatch.setattr(
        ov,
        "_mark_job_error",
        lambda job_id, client_id, reason: error_calls.append((str(job_id), client_id, reason)),
    )

    broker = MagicMock()
    broker.get_prior_day_levels.return_value = {
        "prior_day_high": 100.0,
        "prior_day_low": 95.0,
    }

    master_control = MagicMock()
    master_control.evaluate.return_value = types.SimpleNamespace(
        ok=True,
        plan=_make_plan(),
        reason="approved",
        score=75.0,
    )

    contract_selector = MagicMock()
    contract_selector.select.return_value = "AAPL260619C00100000"

    osm = _FakeOrderStateMachine(cleanup_succeeds=cleanup_succeeds)
    result = ov.run_overnight_reeval(
        client_id="client-1",
        broker=broker,
        master_control=master_control,
        contract_selector=contract_selector,
        order_state_machine=osm,
        entry_watcher=entry_watcher,
        force=True,
    )
    return result, osm, rejected_calls, error_calls


def _make_pending_trigger_order(*, age_seconds: int) -> dict:
    created = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
    return {
        "local_order_id": "pending-1",
        "broker_order_id": None,
        "status": "PENDING_TRIGGER",
        "symbol": "AAPL",
        "contract": "DEFERRED:AAPL",
        "position_id": None,
        "signal_id": "sig-001",
        "plan_id": "plan-001",
        "created_ts": created.isoformat(),
        "submitted_ts": None,
        "limit_price": 1.25,
        "price": 1.25,
        "fill_price": None,
        "score": 75.0,
        "tier": "A",
        "trigger_price": 101.0,
        "meta": {},
    }


def test_pending_trigger_watchdog_preserves_active_watcher_owned_order(monkeypatch, caplog):
    from ap import order_monitor as om_mod

    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_ENABLED", True)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_MAX_AGE_SECONDS", 60)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_DRY_RUN", False)

    watcher = MagicMock()
    watcher.has_order.return_value = True
    osm = _FakeOrderStateMachine()
    monitor = om_mod.APOrderMonitor(
        client_id="client-1",
        broker=MagicMock(),
        order_state_machine=osm,
        position_manager=MagicMock(),
        entry_watcher=watcher,
    )
    monitor._emit_order_event = MagicMock()
    monitor._get_active_entry_orders = MagicMock(
        return_value=[_make_pending_trigger_order(age_seconds=120)]
    )

    caplog.set_level(logging.INFO, logger="ap.order_monitor")
    monitor._check_entry_orders()

    watcher.has_order.assert_called_once_with("pending-1")
    assert osm.expire_calls == []
    assert osm.transition_calls == []
    assert "PENDING_TRIGGER_WATCHDOG_SEEN" in caplog.text
    assert "watcher_owner_state=True" in caplog.text
    assert "ownership_check_available=True" in caplog.text
    assert "ownership_check_error=None" in caplog.text
    assert "cleanup_action=preserve_watcher_owned" in caplog.text
    assert "PENDING_TRIGGER_ORPHAN_EXPIRED" not in caplog.text


def test_pending_trigger_watchdog_expires_orphan_without_watcher_ownership(monkeypatch, caplog):
    from ap import order_monitor as om_mod

    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_ENABLED", True)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_MAX_AGE_SECONDS", 60)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_DRY_RUN", False)

    watcher = MagicMock()
    watcher.has_order.return_value = False
    osm = _FakeOrderStateMachine()
    monitor = om_mod.APOrderMonitor(
        client_id="client-1",
        broker=MagicMock(),
        order_state_machine=osm,
        position_manager=MagicMock(),
        entry_watcher=watcher,
    )
    monitor._emit_order_event = MagicMock()
    monitor._get_active_entry_orders = MagicMock(
        return_value=[_make_pending_trigger_order(age_seconds=120)]
    )

    caplog.set_level(logging.INFO, logger="ap.order_monitor")
    monitor._check_entry_orders()

    watcher.has_order.assert_called_once_with("pending-1")
    assert len(osm.expire_calls) == 1
    local_order_id, reason = osm.expire_calls[0]
    assert local_order_id == "pending-1"
    assert reason.startswith(
        "PENDING_TRIGGER_ORPHAN_EXPIRED: no_broker_order_id no_submitted_ts age="
    )
    assert osm.orders["pending-1"]["status"] == "EXPIRED"
    assert "watcher_owner_state=False" in caplog.text
    assert "ownership_check_available=True" in caplog.text
    assert "ownership_check_error=None" in caplog.text
    assert "PENDING_TRIGGER_ORPHAN_EXPIRED" in caplog.text


def test_pending_trigger_watchdog_preserves_order_when_entry_watcher_missing(monkeypatch, caplog):
    from ap import order_monitor as om_mod

    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_ENABLED", True)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_MAX_AGE_SECONDS", 60)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_DRY_RUN", False)

    osm = _FakeOrderStateMachine()
    monitor = om_mod.APOrderMonitor(
        client_id="client-1",
        broker=MagicMock(),
        order_state_machine=osm,
        position_manager=MagicMock(),
        entry_watcher=None,
    )
    monitor._emit_order_event = MagicMock()
    monitor._get_active_entry_orders = MagicMock(
        return_value=[_make_pending_trigger_order(age_seconds=120)]
    )

    caplog.set_level(logging.INFO, logger="ap.order_monitor")
    monitor._check_entry_orders()

    assert osm.expire_calls == []
    assert osm.transition_calls == []
    assert "PENDING_TRIGGER_ORPHAN_OWNERSHIP_UNKNOWN" in caplog.text
    assert "watcher_owner_state=unknown" in caplog.text
    assert "ownership_check_available=False" in caplog.text
    assert "ownership_check_error=entry_watcher_missing" in caplog.text
    assert "cleanup_action=preserve_ownership_unknown" in caplog.text
    assert "PENDING_TRIGGER_ORPHAN_EXPIRED" not in caplog.text


def test_pending_trigger_watchdog_preserves_order_when_ownership_check_raises(monkeypatch, caplog):
    from ap import order_monitor as om_mod

    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_ENABLED", True)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_MAX_AGE_SECONDS", 60)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_DRY_RUN", False)

    watcher = MagicMock()
    watcher.has_order.side_effect = RuntimeError("watcher registry unavailable")
    osm = _FakeOrderStateMachine()
    monitor = om_mod.APOrderMonitor(
        client_id="client-1",
        broker=MagicMock(),
        order_state_machine=osm,
        position_manager=MagicMock(),
        entry_watcher=watcher,
    )
    monitor._emit_order_event = MagicMock()
    monitor._get_active_entry_orders = MagicMock(
        return_value=[_make_pending_trigger_order(age_seconds=120)]
    )

    caplog.set_level(logging.INFO, logger="ap.order_monitor")
    monitor._check_entry_orders()

    watcher.has_order.assert_called_once_with("pending-1")
    assert osm.expire_calls == []
    assert osm.transition_calls == []
    assert "PENDING_TRIGGER_ORPHAN_OWNERSHIP_UNKNOWN" in caplog.text
    assert "watcher_owner_state=unknown" in caplog.text
    assert "ownership_check_available=True" in caplog.text
    assert "ownership_check_error=RuntimeError: watcher registry unavailable" in caplog.text
    assert "cleanup_action=preserve_ownership_unknown" in caplog.text
    assert "PENDING_TRIGGER_ORPHAN_EXPIRED" not in caplog.text


def test_overnight_watch_false_cleans_order_and_records_source_and_proof(monkeypatch, caplog):
    ledger = _FakeOpportunityLedger()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = False
    entry_watcher._last_reject_reason = "armed_false"

    caplog.set_level(logging.INFO, logger="ap.overnight_reeval")
    result, osm, rejected_calls, error_calls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
    )

    assert result["rejected"] == 1
    assert result["errors"] == 0
    assert error_calls == []
    assert rejected_calls == [
        ("job-001", "client-1", "overnight_watch_arm_failed:armed_false")
    ]
    assert osm.expire_calls == [
        ("local-ord-1", "overnight_watch_arm_failed:armed_false")
    ]
    proof = ledger.rows[("CANON-001", "client-1")]
    assert proof["opportunity_status"] == "MISSED"
    assert proof["miss_stage"] == "WATCHER_ARM"
    assert proof["miss_reason"] == "overnight_watch_arm_failed:armed_false"
    assert proof["metadata"]["overnight_watch_arm_failure"] is True
    assert proof["metadata"]["overnight_source_table"] == "trade_queue"
    assert proof["metadata"]["overnight_reeval_session_key"] == "2026-06-12"
    assert "OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_DONE" in caplog.text


def test_overnight_watch_false_cleanup_failure_marks_cleanup_failed_error_and_proof(monkeypatch, caplog):
    ledger = _FakeOpportunityLedger()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = False
    entry_watcher._last_reject_reason = "armed_false"

    caplog.set_level(logging.INFO, logger="ap.overnight_reeval")
    result, osm, rejected_calls, error_calls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
        cleanup_succeeds=False,
    )

    assert result["errors"] == 1
    assert result["rejected"] == 0
    assert rejected_calls == []
    assert error_calls == [
        (
            "job-001",
            "client-1",
            "overnight_watch_arm_failed_cleanup_failed:overnight_watch_arm_failed:armed_false",
        )
    ]
    assert osm.expire_calls == [
        ("local-ord-1", "overnight_watch_arm_failed:armed_false")
    ]
    assert osm.cancel_calls == [
        ("local-ord-1", "overnight_watch_arm_failed:armed_false")
    ]
    assert osm.transition_calls == [
        (
            "local-ord-1",
            "EXPIRED",
            {"last_error": "overnight_watch_arm_failed:armed_false"},
        )
    ]
    proof = ledger.rows[("CANON-001", "client-1")]
    assert proof["opportunity_status"] == "INTERNAL_ERROR"
    assert proof["miss_reason"] == (
        "overnight_watch_arm_failed_cleanup_failed:overnight_watch_arm_failed:armed_false"
    )
    assert proof["metadata"]["overnight_watch_arm_cleanup_failed"] is True
    assert proof["metadata"]["cleanup_method"] == "transition:EXPIRED"
    assert proof["metadata"]["cleanup_success"] is False
    assert proof["metadata"]["original_reason"] == "overnight_watch_arm_failed:armed_false"
    assert "OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_FAILED" in caplog.text
    assert "cleanup_success=False" in caplog.text
    assert "overnight_watch_arm_failed_cleanup_failed" in caplog.text


def test_shared_setup_does_not_create_repeated_local_orders_after_watch_arm_failure(monkeypatch):
    ledger = _FakeOpportunityLedger()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = False
    entry_watcher._last_reject_reason = "armed_false"

    first_result, first_osm, _, _ = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="ap_signals",
        ledger=ledger,
    )
    assert first_result["rejected"] == 1
    assert first_osm.create_calls == 1
    assert ledger.rows[("CANON-001", "client-1")]["opportunity_status"] == "MISSED"

    second_result, second_osm, _, _ = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="ap_signals",
        ledger=ledger,
    )
    assert second_result["skipped"] == 1
    assert second_osm.create_calls == 0
    assert entry_watcher.watch.call_count == 1, (
        "shared setup must not re-arm after a recorded per-client watch-arm failure"
    )


def test_shared_setup_previous_session_failure_does_not_block_retry(monkeypatch):
    ledger = _FakeOpportunityLedger()
    ledger.rows[("CANON-001", "client-1")] = {
        "signal_id": "sig-001",
        "canonical_signal_id": "CANON-001",
        "client_id": "client-1",
        "opportunity_status": "MISSED",
        "miss_stage": "WATCHER_ARM",
        "miss_reason": "overnight_watch_arm_failed:old_failure",
        "metadata": {
            "overnight_watch_arm_failure": True,
            "overnight_reeval_session_key": "2026-06-11",
        },
    }
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    result, osm, _, _ = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="ap_signals",
        ledger=ledger,
    )

    assert result["armed"] == 1
    assert result["skipped"] == 0
    assert osm.create_calls == 1
    entry_watcher.watch.assert_called_once()


def test_shared_setup_retryable_same_session_failure_does_not_block_retry(monkeypatch):
    ledger = _FakeOpportunityLedger()
    ledger.rows[("CANON-001", "client-1")] = {
        "signal_id": "sig-001",
        "canonical_signal_id": "CANON-001",
        "client_id": "client-1",
        "opportunity_status": "INTERNAL_ERROR",
        "miss_stage": "DATA_NOT_READY",
        "miss_reason": "overnight_watch_arm_retryable:data_not_ready",
        "metadata": {
            "overnight_watch_arm_failure": True,
            "overnight_reeval_session_key": "2026-06-12",
        },
    }
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    result, osm, _, _ = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="ap_signals",
        ledger=ledger,
    )

    assert result["armed"] == 1
    assert result["skipped"] == 0
    assert osm.create_calls == 1
    entry_watcher.watch.assert_called_once()


def test_overnight_watch_exception_cleans_order_marks_error_and_records_proof(monkeypatch, caplog):
    ledger = _FakeOpportunityLedger()
    entry_watcher = MagicMock()
    entry_watcher.watch.side_effect = RuntimeError("watcher boom")

    caplog.set_level(logging.INFO, logger="ap.overnight_reeval")
    result, osm, rejected_calls, error_calls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
    )

    assert result["errors"] == 1
    assert rejected_calls == []
    assert error_calls == [
        ("job-001", "client-1", "overnight_watch_arm_failed:exception:watcher boom")
    ]
    assert osm.expire_calls == [
        ("local-ord-1", "overnight_watch_arm_failed:exception:watcher boom")
    ]
    proof = ledger.rows[("CANON-001", "client-1")]
    assert proof["opportunity_status"] == "INTERNAL_ERROR"
    assert proof["miss_reason"] == "overnight_watch_arm_failed:exception:watcher boom"
    assert proof["metadata"]["overnight_source_table"] == "trade_queue"
    assert proof["metadata"]["overnight_reeval_session_key"] == "2026-06-12"
    assert "OVERNIGHT_WATCH_ARM_EXCEPTION_CLEANUP_DONE" in caplog.text


def test_overnight_watch_exception_cleanup_failure_marks_cleanup_failed_error_and_proof(monkeypatch, caplog):
    ledger = _FakeOpportunityLedger()
    entry_watcher = MagicMock()
    entry_watcher.watch.side_effect = RuntimeError("watcher boom")

    caplog.set_level(logging.INFO, logger="ap.overnight_reeval")
    result, osm, rejected_calls, error_calls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
        cleanup_succeeds=False,
    )

    assert result["errors"] == 1
    assert rejected_calls == []
    assert error_calls == [
        (
            "job-001",
            "client-1",
            "overnight_watch_arm_failed_cleanup_failed:overnight_watch_arm_failed:exception:watcher boom",
        )
    ]
    assert osm.expire_calls == [
        ("local-ord-1", "overnight_watch_arm_failed:exception:watcher boom")
    ]
    proof = ledger.rows[("CANON-001", "client-1")]
    assert proof["opportunity_status"] == "INTERNAL_ERROR"
    assert proof["miss_reason"] == (
        "overnight_watch_arm_failed_cleanup_failed:"
        "overnight_watch_arm_failed:exception:watcher boom"
    )
    assert proof["metadata"]["overnight_watch_arm_cleanup_failed"] is True
    assert proof["metadata"]["cleanup_method"] == "transition:EXPIRED"
    assert proof["metadata"]["cleanup_success"] is False
    assert proof["metadata"]["original_reason"] == "overnight_watch_arm_failed:exception:watcher boom"
    assert "OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_FAILED" in caplog.text
    assert "cleanup_success=False" in caplog.text


def test_hard_70_floor_unchanged(monkeypatch):
    monkeypatch.setenv("HYBRID_CLIENT_QUALITY_MODE", "true")
    monkeypatch.setenv("MIN_CLIENT_SCORE", "70")
    monkeypatch.setenv("ALLOW_CLIENT_TIER_B", "true")
    monkeypatch.setenv("DAILY_CLIENT_PATTERN_WHITELIST", "2-3,3-2-2,1-2_2D")
    monkeypatch.setenv("INTRADAY_CLIENT_PATTERN_WHITELIST", "")
    monkeypatch.setenv("ALLOW_FAILED_DIR_CLIENT", "false")
    monkeypatch.setenv("ENTRY_CONFIRM_SECONDS", "45")
    monkeypatch.setenv("MAX_CLIENT_TRADES_PER_DAY", "5")
    monkeypatch.setenv("MAX_CLIENT_DAILY_TRADES", "3")
    monkeypatch.setenv("MAX_CLIENT_INTRADAY_TRADES", "2")
    monkeypatch.setenv("MAX_CLIENT_SYMBOL_TRADES_PER_DAY", "1")
    monkeypatch.setenv("MAX_PRE_ENTRY_OPTION_FADE_PCT", "8")
    monkeypatch.setenv("MAX_PRE_ENTRY_UNDERLYING_REVERSAL_PCT", "0.25")

    gate_mod = importlib.import_module("ap_hybrid_client_quality_gate")
    gate_mod = importlib.reload(gate_mod)

    signal = {
        "symbol": "AAPL",
        "direction": "CALL",
        "timeframe": "1d",
        "pattern": "2-3",
        "tier": "A",
        "trigger_price": 180.0,
        "stop_underlying": 175.0,
        "target_underlying": 190.0,
    }
    empty_snap = {
        "trades_today": 0,
        "daily_trades": 0,
        "intraday_trades": 0,
        "symbol_trades": {},
    }

    blocked = gate_mod.evaluate_client_quality_gate(
        {**signal, "score": 69},
        "client-1",
        empty_snap,
    )
    allowed = gate_mod.evaluate_client_quality_gate(
        {**signal, "score": 70},
        "client-1",
        empty_snap,
    )

    assert blocked.allowed is False
    assert blocked.block_reason == "client_score_below_70"
    assert allowed.allowed is True
