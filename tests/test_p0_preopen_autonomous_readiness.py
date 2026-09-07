from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import ap.preopen_readiness as pr


REPO_ROOT = Path(__file__).resolve().parents[1]


class _Evt:
    def __init__(self, value: bool = True):
        self._value = value

    def is_set(self) -> bool:
        return self._value


class _Thread:
    def __init__(self, alive: bool = True):
        self._alive = alive

    def is_alive(self) -> bool:
        return self._alive


class _Watcher:
    def __init__(self, owned: set[str] | None = None):
        self._owned = owned or set()

    def has_order(self, local_order_id: str) -> bool:
        return local_order_id in self._owned


class _ExactWatcher(_Watcher):
    def __init__(
        self,
        *,
        local_order_id: str,
        signal_id: str,
        client_id: str,
        execution_mode: str,
    ):
        super().__init__({local_order_id})
        self._pending = [
            SimpleNamespace(
                signal={
                    "local_order_id": local_order_id,
                    "signal_id": signal_id,
                    "client_id": client_id,
                    "execution_mode": execution_mode,
                },
                state="PENDING",
                _ownership_quarantine=False,
            )
        ]
        self._dedup_set = {signal_id}


class _Runner:
    def __init__(self, *, mode: str = "live", watcher=None):
        self.email = "runner@example.com"
        self.mode = mode
        self.initialized = _Evt(True)
        self.worker_thread = _Thread(True)
        self.order_state_machine = object()
        self.position_manager = object()
        self.master_control = SimpleNamespace(mode=mode)
        self.core = SimpleNamespace(entry_watcher=watcher or _Watcher(), broker=object(), exit_eng=object())
        self.contract_selector = SimpleNamespace(
            data_broker=SimpleNamespace(
                cfg=SimpleNamespace(base_url="https://api.tradier.com" if mode == "live" else "https://sandbox.tradier.com")
            )
        )
        self.base_url = "https://api.tradier.com" if mode == "live" else "https://sandbox.tradier.com"
        self.account_id = "acct-1"
        self._resolved_tradier_token = "tok"
        self._last_overnight_reeval_date = "2026-06-22"
        self._token_getter_called = False

    def is_alive(self) -> bool:
        return True

    def _get_token(self):
        self._token_getter_called = True
        return "tok"


def _stub_common(monkeypatch, *, handoff=True, client_state=None):
    writes = []
    monkeypatch.setattr(pr, "_upsert_preopen_row", lambda **kwargs: writes.append(kwargs))
    monkeypatch.setattr(pr, "_morning_handoff_success_exists", lambda *args, **kwargs: handoff)
    monkeypatch.setattr(pr, "_post_overnight_reeval_success_exists", lambda *args, **kwargs: False)
    monkeypatch.setattr(
        pr,
        "_query_client_state",
        lambda client_id: client_state or {
            "stale_processing_ids": [],
            "watching_orphans": [],
            "pending_trigger_rows": [],
            "watching_count": 0,
        },
    )
    monkeypatch.setattr(pr, "_trading_date", lambda now=None: "2026-06-22")
    monkeypatch.setattr(pr, "_pod_mode", lambda: "live")
    monkeypatch.setattr(pr, "_after_929_et", lambda now=None: True)
    return writes


def test_all_green_readiness(monkeypatch):
    writes = _stub_common(monkeypatch)
    runner = _Runner(mode="live")

    result = pr.run_preopen_autonomous_readiness(
        "jason@example.com",
        "live",
        dry_run=True,
        runner=runner,
    )
    assert result["ok"] is True
    assert result["status"] == "OK"
    assert writes[-1]["mark_success"] is True


def test_readiness_credential_check_does_not_call_token_getter(monkeypatch):
    _stub_common(monkeypatch)
    runner = _Runner(mode="live")
    runner._resolved_tradier_token = "visible-token"

    result = pr.run_preopen_autonomous_readiness(
        "jason@example.com",
        "live",
        dry_run=True,
        runner=runner,
    )
    assert result["status"] == "OK"
    assert runner._token_getter_called is False


def test_missing_live_handoff_after_929_is_blocked(monkeypatch):
    _stub_common(monkeypatch, handoff=False)
    runner = _Runner(mode="live")

    result = pr.run_preopen_autonomous_readiness("jason@example.com", "live", dry_run=True, runner=runner)
    assert result["status"] == "BLOCKED"
    assert "morning_handoff_missing" in result["errors"]


def test_live_startup_with_watching_rows_does_not_degrade_for_missing_overnight(monkeypatch):
    _stub_common(monkeypatch, handoff=False, client_state={
        "stale_processing_ids": [],
        "watching_orphans": [],
        "pending_trigger_rows": [{"local_order_id": "L-1", "signal_id": "sig-1"}],
        "watching_count": 1,
    })
    runner = _Runner(
        mode="live",
        watcher=_ExactWatcher(
            local_order_id="L-1",
            signal_id="sig-1",
            client_id="jason@example.com",
            execution_mode="live",
        ),
    )
    runner.order_state_machine = SimpleNamespace(
        get_order=lambda oid: {
            "local_order_id": "L-1",
            "signal_id": "sig-1",
            "client_id": "jason@example.com",
            "execution_mode": "live",
            "status": "PENDING_TRIGGER",
            "meta": {},
        } if oid == "L-1" else None
    )
    runner._last_overnight_reeval_date = None

    result = pr.run_preopen_autonomous_readiness(
        "jason@example.com",
        "live",
        dry_run=True,
        runner=runner,
        stage="startup",
        now=datetime(2026, 6, 22, 9, 10, tzinfo=pr.ET),
    )
    assert result["status"] == "OK"
    assert "overnight_reeval_missing" not in result["errors"]
    assert result["details"]["overnight_reeval"]["status"] == "pending"
    assert "overnight_reeval_pending_startup" in result["warnings"]


def test_startup_stage_with_watching_rows_before_due_is_pending_not_missing(monkeypatch):
    _stub_common(monkeypatch, handoff=False, client_state={
        "stale_processing_ids": [],
        "watching_orphans": [],
        "pending_trigger_rows": [{"local_order_id": "L-1", "signal_id": "sig-1"}],
        "watching_count": 1,
    })
    runner = _Runner(
        mode="live",
        watcher=_ExactWatcher(
            local_order_id="L-1",
            signal_id="sig-1",
            client_id="jason@example.com",
            execution_mode="live",
        ),
    )
    runner.order_state_machine = SimpleNamespace(
        get_order=lambda oid: {
            "local_order_id": "L-1",
            "signal_id": "sig-1",
            "client_id": "jason@example.com",
            "execution_mode": "live",
            "status": "PENDING_TRIGGER",
            "meta": {},
        } if oid == "L-1" else None
    )
    runner._last_overnight_reeval_date = None

    result = pr.run_preopen_autonomous_readiness(
        "jason@example.com",
        "live",
        dry_run=True,
        runner=runner,
        stage="startup",
        now=datetime(2026, 6, 22, 9, 10, tzinfo=pr.ET),
    )
    assert result["details"]["overnight_reeval"]["status"] == "pending"
    assert "overnight_reeval_missing" not in result["errors"]


def test_post_due_missing_overnight_is_reported(monkeypatch):
    _stub_common(monkeypatch, handoff=True, client_state={
        "stale_processing_ids": [],
        "watching_orphans": [],
        "pending_trigger_rows": [{"local_order_id": "L-1", "signal_id": "sig-1"}],
        "watching_count": 1,
    })
    runner = _Runner(
        mode="live",
        watcher=_ExactWatcher(
            local_order_id="L-1",
            signal_id="sig-1",
            client_id="jason@example.com",
            execution_mode="live",
        ),
    )
    runner.order_state_machine = SimpleNamespace(
        get_order=lambda oid: {
            "local_order_id": "L-1",
            "signal_id": "sig-1",
            "client_id": "jason@example.com",
            "execution_mode": "live",
            "status": "PENDING_TRIGGER",
            "meta": {},
        } if oid == "L-1" else None
    )
    runner._last_overnight_reeval_date = None

    result = pr.run_preopen_autonomous_readiness(
        "jason@example.com",
        "live",
        dry_run=True,
        runner=runner,
        stage="manual",
        now=datetime(2026, 6, 22, 9, 30, tzinfo=pr.ET),
    )
    assert result["details"]["overnight_reeval"]["status"] == "missing"
    assert "overnight_reeval_missing" in result["errors"]


def test_missing_entry_watcher_is_blocked_for_live(monkeypatch):
    _stub_common(monkeypatch)
    runner = _Runner(mode="live")
    runner.core.entry_watcher = None

    result = pr.run_preopen_autonomous_readiness("jason@example.com", "live", dry_run=True, runner=runner)
    assert result["status"] == "BLOCKED"
    assert "entry_watcher_missing" in result["errors"]


def test_stale_processing_rows_are_reported(monkeypatch):
    _stub_common(monkeypatch, client_state={
        "stale_processing_ids": [11, 12],
        "watching_orphans": [],
        "pending_trigger_rows": [],
        "watching_count": 0,
    })
    runner = _Runner(mode="paper")
    monkeypatch.setattr(pr, "_pod_mode", lambda: "paper")

    result = pr.run_preopen_autonomous_readiness("paper@example.com", "paper", dry_run=True, runner=runner)
    assert result["status"] == "DEGRADED"
    assert "stale_processing_rows" in result["errors"]
    assert result["details"]["client_state"]["stale_processing_ids"] == [11, 12]


def test_watching_row_with_no_orders_recommends_new_rescue(monkeypatch):
    _stub_common(monkeypatch, client_state={
        "stale_processing_ids": [],
        "watching_orphans": [{"id": 41, "signal_id": "sig-41"}],
        "pending_trigger_rows": [],
        "watching_count": 1,
    })
    runner = _Runner(mode="paper")
    monkeypatch.setattr(pr, "_pod_mode", lambda: "paper")

    result = pr.run_preopen_autonomous_readiness("paper@example.com", "paper", dry_run=True, runner=runner)
    assert "watching_rows_missing_orders_recommend_new_rescue" in result["errors"]
    assert result["details"]["client_state"]["watching_orphans"][0]["id"] == 41


def test_pending_trigger_without_watcher_is_degraded(monkeypatch):
    _stub_common(monkeypatch, client_state={
        "stale_processing_ids": [],
        "watching_orphans": [],
        "pending_trigger_rows": [{"local_order_id": "L-1", "signal_id": "sig-1"}],
        "watching_count": 0,
    })
    runner = _Runner(mode="paper", watcher=_Watcher(set()))
    monkeypatch.setattr(pr, "_pod_mode", lambda: "paper")

    result = pr.run_preopen_autonomous_readiness("paper@example.com", "paper", dry_run=True, runner=runner)
    assert "pending_trigger_without_watcher_ownership" in result["errors"]
    assert result["details"]["pending_trigger_without_watcher"][0]["local_order_id"] == "L-1"


def test_exact_restart_rearm_retry_owner_blocks_live_until_watcher_or_terminal(monkeypatch):
    _stub_common(monkeypatch, client_state={
        "stale_processing_ids": [],
        "watching_orphans": [],
        "pending_trigger_rows": [{"local_order_id": "L-1", "signal_id": "sig-1"}],
        "watching_count": 0,
    })
    runner = _Runner(mode="live", watcher=_Watcher(set()))
    broker = MagicMock()
    runner.core.broker = broker
    now_utc = datetime.now(timezone.utc)
    row = {
        "local_order_id": "L-1",
        "signal_id": "sig-1",
        "client_id": "jason@example.com",
        "execution_mode": "live",
        "status": "PENDING_TRIGGER",
        "meta": {
            "restart_rearm_status": "RETRY_PENDING",
            "restart_rearm_owner": "restart_rearm:jason@example.com:live:L-1",
            "restart_rearm_reason": "regular_session_market_truth_not_yet_available",
            "restart_rearm_attempt": 1,
            "restart_rearm_next_at": (now_utc + timedelta(seconds=30)).isoformat(),
            "restart_rearm_deadline": (now_utc + timedelta(minutes=1)).isoformat(),
            "restart_rearm_first_failed_at": (now_utc - timedelta(minutes=2)).isoformat(),
            "restart_rearm_last_failed_at": (now_utc - timedelta(seconds=1)).isoformat(),
            "restart_rearm_client_id": "jason@example.com",
            "restart_rearm_execution_mode": "live",
            "restart_rearm_generation": 1,
            "late_attachment_policy_eligible": True,
        },
    }
    runner.order_state_machine = SimpleNamespace(get_order=lambda oid: dict(row) if oid == "L-1" else None)
    runner._overnight_reeval_success_date = "2026-06-22"

    result = pr.run_preopen_autonomous_readiness(
        "jason@example.com", "live", dry_run=True, runner=runner
    )

    assert result["status"] == "BLOCKED"
    assert "pending_trigger_without_watcher_ownership" in result["errors"]
    assert result["details"]["pending_trigger_retry_owned"] == [{
        "local_order_id": "L-1",
        "signal_id": "sig-1",
    }]
    assert result["details"]["pending_trigger_ownerless"] == []
    assert result["details"]["pending_trigger_without_watcher"] == result["details"]["pending_trigger_retry_owned"]
    assert row["meta"]["restart_rearm_status"] == "RETRY_PENDING"
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()
    broker.replace_order.assert_not_called()


def test_preopen_readiness_rejects_every_broker_handoff_marker(monkeypatch):
    """Readiness accepts only exact watcher/retry ownership, never handoff truth."""
    markers = [
        {"lifecycle_state": "SUBMITTING"},
        {"lifecycle_state": "SUBMITTED"},
        {"current_owner": "broker_submit:readiness-race"},
        {"recovery_submit_owner": "recovery:readiness-race"},
        {"materialization": {"lifecycle_state": "SUBMITTING"}},
        {"materialization": {"current_owner": "broker_submit:readiness-race"}},
        {"materialization": {"recovery_submit_owner": "recovery:readiness-race"}},
        {"broker_ready": True},
        {"submit_intent_at": "2026-09-07T16:00:00+00:00"},
        {"broker_submit_key": "readiness-submit-key"},
        {"broker_submit_payload_hash": "readiness-payload-hash"},
    ]

    for index, marker in enumerate(markers):
        runner = _Runner(mode="live", watcher=_Watcher(set()))
        local_order_id = f"L-marker-{index}"
        signal_id = f"sig-marker-{index}"
        row = {
            "local_order_id": local_order_id,
            "signal_id": signal_id,
            "client_id": "jason@example.com",
            "execution_mode": "live",
            "status": "PENDING_TRIGGER",
            "meta": dict(marker),
        }
        if index == len(markers) - 2:
            row["broker_order_id"] = "broker-readiness"
        if index == len(markers) - 1:
            row["submitted_ts"] = "2026-09-07T16:00:00+00:00"
        runner.order_state_machine = SimpleNamespace(
            get_order=lambda oid, durable=row: dict(durable)
            if oid == durable["local_order_id"]
            else None
        )

        pending = [{"local_order_id": local_order_id, "signal_id": signal_id}]
        unowned = pr._pending_trigger_without_watcher(
            runner,
            pending,
            client_id="jason@example.com",
            execution_mode="live",
        )
        assert unowned == pending, marker


def test_future_restart_rearm_lease_remains_unowned_for_readiness(monkeypatch):
    runner = _Runner(mode="live", watcher=_Watcher(set()))
    now_utc = datetime.now(timezone.utc)
    row = {
        "local_order_id": "L-future",
        "signal_id": "sig-future",
        "client_id": "jason@example.com",
        "execution_mode": "live",
        "status": "PENDING_TRIGGER",
        "meta": {
            "restart_rearm_status": "RETRY_PENDING",
            "restart_rearm_owner": "restart_rearm:jason@example.com:live:L-future",
            "restart_rearm_reason": "late_attachment_market_truth_unavailable_or_unresolved",
            "restart_rearm_attempt": 1,
            "restart_rearm_next_at": (now_utc + timedelta(days=3650)).isoformat(),
            "restart_rearm_deadline": (now_utc + timedelta(days=3650, minutes=3)).isoformat(),
            "restart_rearm_first_failed_at": (now_utc - timedelta(minutes=2)).isoformat(),
            "restart_rearm_last_failed_at": (now_utc - timedelta(seconds=1)).isoformat(),
            "restart_rearm_client_id": "jason@example.com",
            "restart_rearm_execution_mode": "live",
            "restart_rearm_generation": 1,
            "late_attachment_policy_eligible": True,
        },
    }
    runner.order_state_machine = SimpleNamespace(
        get_order=lambda oid: dict(row) if oid == "L-future" else None
    )

    unowned = pr._pending_trigger_without_watcher(
        runner,
        [{"local_order_id": "L-future", "signal_id": "sig-future"}],
        client_id="jason@example.com",
        execution_mode="live",
    )

    assert unowned == [{"local_order_id": "L-future", "signal_id": "sig-future"}]


def test_restart_rearm_owner_proof_failure_is_logged(caplog, monkeypatch):
    from ap.pending_trigger_restart_recovery import PendingTriggerRestartRecovery

    def _raise(*_args, **_kwargs):
        raise RuntimeError("canonical verifier unavailable")

    monkeypatch.setattr(
        PendingTriggerRestartRecovery,
        "prove_restart_rearm_retry_owner",
        _raise,
    )
    runner = _Runner(mode="live", watcher=_Watcher(set()))
    runner.order_state_machine = SimpleNamespace()
    pending = [{"local_order_id": "L-proof-error", "signal_id": "sig-proof-error"}]

    with caplog.at_level("WARNING", logger="ap.preopen_readiness"):
        unowned = pr._pending_trigger_without_watcher(
            runner,
            pending,
            client_id="jason@example.com",
            execution_mode="live",
        )

    assert unowned == pending
    assert any(
        "PREOPEN_RESTART_REARM_OWNER_PROOF_FAILED" in record.message
        for record in caplog.records
    )


def test_wrong_restart_rearm_owner_still_blocks_live_entries(monkeypatch):
    runner = _Runner(mode="live", watcher=_Watcher(set()))
    now_utc = datetime.now(timezone.utc)
    row = {
        "local_order_id": "L-1",
        "signal_id": "sig-1",
        "client_id": "jason@example.com",
        "execution_mode": "live",
        "status": "PENDING_TRIGGER",
        "meta": {
            "restart_rearm_status": "RETRY_PENDING",
            "restart_rearm_owner": "restart_rearm:other@example.com:live:L-1",
            "restart_rearm_reason": "regular_session_market_truth_not_yet_available",
            "restart_rearm_attempt": 1,
            "restart_rearm_next_at": (now_utc + timedelta(seconds=30)).isoformat(),
            "restart_rearm_deadline": (now_utc + timedelta(minutes=1)).isoformat(),
            "restart_rearm_first_failed_at": (now_utc - timedelta(minutes=2)).isoformat(),
            "restart_rearm_last_failed_at": (now_utc - timedelta(seconds=1)).isoformat(),
            "restart_rearm_client_id": "jason@example.com",
            "restart_rearm_execution_mode": "live",
        },
    }
    runner.order_state_machine = SimpleNamespace(get_order=lambda oid: dict(row) if oid == "L-1" else None)

    unowned = pr._pending_trigger_without_watcher(
        runner,
        [{"local_order_id": "L-1", "signal_id": "sig-1"}],
        client_id="jason@example.com",
        execution_mode="live",
    )

    assert unowned == [{"local_order_id": "L-1", "signal_id": "sig-1"}]


def test_restart_rearm_owner_with_wrong_signal_still_blocks_live_entries():
    runner = _Runner(mode="live", watcher=_Watcher(set()))
    now_utc = datetime.now(timezone.utc)
    durable_row = {
        "local_order_id": "L-1",
        "signal_id": "different-signal",
        "client_id": "jason@example.com",
        "execution_mode": "live",
        "status": "PENDING_TRIGGER",
        "meta": {
            "restart_rearm_status": "RETRY_PENDING",
            "restart_rearm_owner": "restart_rearm:jason@example.com:live:L-1",
            "restart_rearm_reason": "late_attachment_market_truth_unavailable_or_unresolved",
            "restart_rearm_attempt": 1,
            "restart_rearm_next_at": (now_utc + timedelta(seconds=30)).isoformat(),
            "restart_rearm_deadline": (now_utc + timedelta(minutes=1)).isoformat(),
            "restart_rearm_first_failed_at": (now_utc - timedelta(minutes=2)).isoformat(),
            "restart_rearm_last_failed_at": (now_utc - timedelta(seconds=1)).isoformat(),
            "restart_rearm_client_id": "jason@example.com",
            "restart_rearm_execution_mode": "live",
        },
    }
    runner.order_state_machine = SimpleNamespace(
        get_order=lambda oid: dict(durable_row) if oid == "L-1" else None
    )

    unowned = pr._pending_trigger_without_watcher(
        runner,
        [{"local_order_id": "L-1", "signal_id": "expected-signal"}],
        client_id="jason@example.com",
        execution_mode="live",
    )

    assert unowned == [{"local_order_id": "L-1", "signal_id": "expected-signal"}]


def test_mode_mismatch_is_critical(monkeypatch):
    _stub_common(monkeypatch)
    runner = _Runner(mode="paper")
    monkeypatch.setattr(pr, "_pod_mode", lambda: "live")

    result = pr.run_preopen_autonomous_readiness("jason@example.com", "live", dry_run=True, runner=runner)
    assert result["status"] == "BLOCKED"
    assert "pod_mode_client_mode_mismatch" in result["errors"]


def test_overnight_status_accepts_post_overnight_handoff_success(monkeypatch):
    _stub_common(monkeypatch, handoff=True, client_state={
        "stale_processing_ids": [],
        "watching_orphans": [],
        "pending_trigger_rows": [{"local_order_id": "L-1", "signal_id": "sig-1"}],
        "watching_count": 1,
    })
    runner = _Runner(
        mode="live",
        watcher=_ExactWatcher(
            local_order_id="L-1",
            signal_id="sig-1",
            client_id="jason@example.com",
            execution_mode="live",
        ),
    )
    runner.order_state_machine = SimpleNamespace(
        get_order=lambda oid: {
            "local_order_id": "L-1",
            "signal_id": "sig-1",
            "client_id": "jason@example.com",
            "execution_mode": "live",
            "status": "PENDING_TRIGGER",
            "meta": {},
        } if oid == "L-1" else None
    )
    runner._last_overnight_reeval_date = None
    monkeypatch.setattr(pr, "_post_overnight_reeval_success_exists", lambda *args, **kwargs: True)
    status, details = pr._overnight_status(
        runner,
        {
            "stale_processing_ids": [],
            "watching_orphans": [],
            "pending_trigger_rows": [{"local_order_id": "L-1", "signal_id": "sig-1"}],
            "watching_count": 1,
        },
        "2026-06-22",
        client_id="jason@example.com",
        execution_mode="live",
        stage="manual",
        now=datetime(2026, 6, 22, 9, 30, tzinfo=pr.ET),
    )
    assert status == "success"
    assert details["source"] == "handoff_run_locks.post_overnight_reeval"


def test_overnight_status_ignores_legacy_last_reeval_date(monkeypatch):
    _stub_common(monkeypatch, handoff=True, client_state={
        "stale_processing_ids": [],
        "watching_orphans": [],
        "pending_trigger_rows": [{"local_order_id": "L-1", "signal_id": "sig-1"}],
        "watching_count": 1,
    })
    runner = _Runner(mode="live")
    runner._last_overnight_reeval_date = "2026-06-22"
    monkeypatch.setattr(pr, "_post_overnight_reeval_success_exists", lambda *args, **kwargs: False)

    status, details = pr._overnight_status(
        runner,
        {
            "stale_processing_ids": [],
            "watching_orphans": [],
            "pending_trigger_rows": [{"local_order_id": "L-1", "signal_id": "sig-1"}],
            "watching_count": 1,
        },
        "2026-06-22",
        client_id="jason@example.com",
        execution_mode="live",
        stage="manual",
        now=datetime(2026, 6, 22, 9, 30, tzinfo=pr.ET),
    )
    assert status == "missing"
    assert details["source"] == "pending_trigger_without_watcher_ownership"


def test_startup_handoff_success_does_not_count_as_post_overnight_success(monkeypatch):
    _stub_common(monkeypatch, handoff=True, client_state={
        "stale_processing_ids": [],
        "watching_orphans": [],
        "pending_trigger_rows": [{"local_order_id": "L-1", "signal_id": "sig-1"}],
        "watching_count": 1,
    })
    runner = _Runner(mode="live")
    runner._last_overnight_reeval_date = None
    monkeypatch.setattr(pr, "_post_overnight_reeval_success_exists", lambda *args, **kwargs: False)

    status, details = pr._overnight_status(
        runner,
        {
            "stale_processing_ids": [],
            "watching_orphans": [],
            "pending_trigger_rows": [{"local_order_id": "L-1", "signal_id": "sig-1"}],
            "watching_count": 1,
        },
        "2026-06-22",
        client_id="jason@example.com",
        execution_mode="live",
        stage="manual",
        now=datetime(2026, 6, 22, 9, 30, tzinfo=pr.ET),
    )
    assert status == "missing"
    assert details["source"] == "pending_trigger_without_watcher_ownership"


def test_health_summary_exposes_preopen_status(monkeypatch):
    monkeypatch.setattr(pr, "_trading_date", lambda now=None: "2026-06-22")
    monkeypatch.setattr(pr, "_readiness_enforcement_active", lambda now=None: True)
    monkeypatch.setattr(pr, "_expected_clients_by_mode", lambda: {"paper": ["p@example.com"], "live": ["l@example.com"]})
    monkeypatch.setattr(
        pr,
        "_latest_preopen_rows",
        lambda trading_date: [
            {"client_id": "p@example.com", "execution_mode": "paper", "stage": "startup", "status": "degraded", "last_run_at": "1", "last_success_at": None, "last_error": "stale_processing_rows", "details": {"errors": ["stale_processing_rows"]}},
            {"client_id": "l@example.com", "execution_mode": "live", "stage": "manual", "status": "blocked", "last_run_at": "2", "last_success_at": None, "last_error": "morning_handoff_missing", "details": {"errors": ["morning_handoff_missing"]}},
        ],
    )
    health = pr.get_preopen_readiness_health()
    assert health["status"] == "BLOCKED"
    assert health["live"]["clients"]["l@example.com"]["status"] == "blocked"


def test_health_missing_rows_do_not_degrade_outside_enforcement_window(monkeypatch):
    monkeypatch.setattr(pr, "_trading_date", lambda now=None: "2026-06-22")
    monkeypatch.setattr(pr, "_readiness_enforcement_active", lambda now=None: False)
    monkeypatch.setattr(pr, "_expected_clients_by_mode", lambda: {"paper": ["p@example.com"], "live": ["l@example.com"]})
    monkeypatch.setattr(pr, "_latest_preopen_rows", lambda trading_date: [])
    health = pr.get_preopen_readiness_health()
    assert health["status"] == "OK"
    assert health["enforcement_active"] is False
    assert health["missing_expected_clients"] == {"paper": ["p@example.com"], "live": ["l@example.com"]}


def test_readiness_module_has_no_submit_cancel_or_state_mutation():
    src = (REPO_ROOT / "ap" / "preopen_readiness.py").read_text()
    assert "place_order(" not in src
    assert "cancel_order(" not in src
    assert "create_entry_order(" not in src
    assert ".transition(" not in src
    assert "UPDATE orders" not in src
    assert "UPDATE positions" not in src


def test_broker_credential_uncertainty_is_degraded_not_blocked(monkeypatch):
    _stub_common(monkeypatch)
    runner = _Runner(mode="live")
    runner._resolved_tradier_token = None
    runner.account_id = "acct-1"
    runner.base_url = "https://api.tradier.com"
    runner.core = SimpleNamespace(entry_watcher=_Watcher(), broker=object(), exit_eng=object())
    runner._last_overnight_reeval_date = "2026-06-22"

    result = pr.run_preopen_autonomous_readiness(
        "jason@example.com",
        "live",
        dry_run=True,
        runner=runner,
        stage="manual",
    )
    assert result["status"] == "DEGRADED"
    assert "broker_credentials_missing" in result["errors"]


def test_degraded_mode_path_only_clears_entries_not_exits():
    src = (REPO_ROOT / "client_runner.py").read_text()
    assert "self.entries_allowed.clear()" in src
    assert '"preopen_readiness_blocked:' in src
    assert "stop_runner=False" in src
    assert "exit_eng = getattr(self.core, \"exit_eng\", None)" in src


def test_source_marks_runner_overnight_success_date_after_admin_reeval():
    src = (REPO_ROOT / "app.py").read_text()
    assert "runner.run_overnight_reeval_attempt(" in src
    assert 'source="admin_sync"' in src
    assert 'source="admin_background"' in src


def test_source_wires_runner_endpoint_and_health():
    app_src = (REPO_ROOT / "app.py").read_text()
    runner_src = (REPO_ROOT / "client_runner.py").read_text()
    health_src = (REPO_ROOT / "ap_health_endpoints.py").read_text()
    assert '/admin/preopen_readiness' in app_src
    assert 'methods=["GET", "POST"]' in app_src
    assert 'run_preopen_autonomous_readiness(' in app_src
    assert 'Startup preopen readiness result' in runner_src
    assert 'Post-overnight preopen readiness result' in runner_src
    assert '@health_bp.route("/organs")' in health_src
    assert '"preopen_readiness":  get_preopen_readiness_health()' in health_src
    assert 'get_morning_handoff_health()' in health_src
    assert 'preopen_readiness.get("enforcement_active")' in app_src

def test_local_order_id_only_watcher_does_not_satisfy_readiness(monkeypatch):
    runner = _Runner(mode="live", watcher=_Watcher({"L-1"}))
    row = {
        "local_order_id": "L-1",
        "signal_id": "sig-1",
        "client_id": "jason@example.com",
        "execution_mode": "live",
        "status": "PENDING_TRIGGER",
        "meta": {},
    }
    runner.order_state_machine = SimpleNamespace(
        get_order=lambda oid: dict(row) if oid == "L-1" else None
    )

    unowned = pr._pending_trigger_without_watcher(
        runner,
        [row],
        client_id="jason@example.com",
        execution_mode="live",
    )

    assert unowned == [row]


def test_exact_registry_identity_satisfies_readiness(monkeypatch):
    runner = _Runner(
        mode="live",
        watcher=_ExactWatcher(
            local_order_id="L-1",
            signal_id="sig-1",
            client_id="jason@example.com",
            execution_mode="live",
        ),
    )
    row = {
        "local_order_id": "L-1",
        "signal_id": "sig-1",
        "client_id": "jason@example.com",
        "execution_mode": "live",
        "status": "PENDING_TRIGGER",
        "meta": {},
    }
    runner.order_state_machine = SimpleNamespace(
        get_order=lambda oid: dict(row) if oid == "L-1" else None
    )

    assert pr._pending_trigger_without_watcher(
        runner,
        [row],
        client_id="jason@example.com",
        execution_mode="live",
    ) == []
