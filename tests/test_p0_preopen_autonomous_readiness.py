from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import ap.preopen_readiness as pr


REPO_ROOT = Path(__file__).resolve().parents[1]


class _Evt:
    def __init__(self, value: bool = True):
        self._value = value

    def is_set(self) -> bool:
        return self._value

    def set(self) -> None:
        self._value = True

    def clear(self) -> None:
        self._value = False


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


class _MultiExactWatcher(_Watcher):
    """Minimal registry-shaped watcher for mixed watcher/retry readiness."""

    def __init__(self, rows):
        super().__init__({row["local_order_id"] for row in rows})
        self._pending = [
            SimpleNamespace(
                signal={
                    "local_order_id": row["local_order_id"],
                    "signal_id": row["signal_id"],
                    "client_id": row["client_id"],
                    "execution_mode": row["execution_mode"],
                },
                state="PENDING",
                _ownership_quarantine=False,
            )
            for row in rows
        ]
        self._dedup_set = {row["signal_id"] for row in rows}


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
        lambda client_id, execution_mode: client_state or {
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


class _ModeScopedReadinessConnection:
    """Small driver-faithful cursor for the real readiness SQL seam."""

    def __init__(self, pending_rows):
        self.pending_rows = list(pending_rows)
        self.calls = []
        self._result = []

    def __call__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params=()):
        normalized_sql = " ".join(str(sql).split()).lower()
        params = tuple(params or ())
        self.calls.append((normalized_sql, params))
        if "from orders" in normalized_sql and "pending_trigger" in normalized_sql:
            assert len(params) == 3
            requested_client, requested_mode, _cutoff = params
            self._result = [
                (row["local_order_id"], row["signal_id"])
                for row in self.pending_rows
                if row["client_id"] == requested_client
                and row["execution_mode"] == requested_mode
            ]
        elif "count(*)" in normalized_sql:
            self._result = [(0,)]
        else:
            self._result = []

    def fetchall(self):
        return list(self._result)

    def fetchone(self):
        return self._result[0] if self._result else None


def _mode_inventory_row(local_order_id, signal_id, execution_mode):
    return {
        "local_order_id": local_order_id,
        "signal_id": signal_id,
        "client_id": "jason@example.com",
        "execution_mode": execution_mode,
    }


def test_pending_trigger_inventory_is_mode_scoped_at_sql_boundary(monkeypatch):
    """The real readiness query returns only the requested client/mode rows."""
    import ap.db as db

    db_conn = _ModeScopedReadinessConnection([
        _mode_inventory_row("L-live", "sig-live", "live"),
        _mode_inventory_row("L-paper", "sig-paper", "paper"),
    ])
    monkeypatch.setattr(db, "conn", db_conn)
    monkeypatch.setattr(db, "run_with_retry", lambda fn: fn())

    live_state = pr._query_client_state("jason@example.com", "LIVE")
    paper_state = pr._query_client_state("jason@example.com", "PAPER")

    assert live_state["pending_trigger_rows"] == [{
        "local_order_id": "L-live",
        "signal_id": "sig-live",
    }]
    assert paper_state["pending_trigger_rows"] == [{
        "local_order_id": "L-paper",
        "signal_id": "sig-paper",
    }]
    pending_calls = [
        (sql, params)
        for sql, params in db_conn.calls
        if "from orders" in sql and "pending_trigger" in sql
    ]
    assert len(pending_calls) == 2
    assert all("execution_mode" in sql for sql, _params in pending_calls)
    assert [params[1] for _sql, params in pending_calls] == ["live", "paper"]


def test_paper_ownerless_pending_trigger_cannot_block_live_readiness(monkeypatch):
    """A malformed PAPER row is absent before LIVE ownership classification."""
    import ap.db as db

    live_row = {
        **_mode_inventory_row("L-live", "sig-live", "live"),
        "status": "PENDING_TRIGGER",
        "meta": {},
    }
    paper_row = {
        **_mode_inventory_row("L-paper", "sig-paper", "paper"),
        "status": "PENDING_TRIGGER",
        "meta": {"restart_rearm_status": "not-a-valid-lease"},
    }
    db_conn = _ModeScopedReadinessConnection([live_row, paper_row])
    monkeypatch.setattr(db, "conn", db_conn)
    monkeypatch.setattr(db, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(pr, "_upsert_preopen_row", lambda **_kwargs: None)
    monkeypatch.setattr(pr, "_morning_handoff_success_exists", lambda *args, **kwargs: True)
    monkeypatch.setattr(pr, "_post_overnight_reeval_success_exists", lambda *args, **kwargs: False)
    monkeypatch.setattr(pr, "_trading_date", lambda now=None: "2026-06-22")
    monkeypatch.setattr(pr, "_pod_mode", lambda: "live")

    runner = _Runner(
        mode="live",
        watcher=_ExactWatcher(
            local_order_id="L-live",
            signal_id="sig-live",
            client_id="jason@example.com",
            execution_mode="live",
        ),
    )
    runner.order_state_machine = SimpleNamespace(
        get_order=lambda oid: dict(live_row) if oid == "L-live" else None
    )
    runner._overnight_reeval_success_date = "2026-06-22"

    result = pr.run_preopen_autonomous_readiness(
        "jason@example.com",
        "live",
        dry_run=True,
        runner=runner,
        now=datetime(2026, 6, 22, 9, 30, tzinfo=pr.ET),
    )

    assert result["status"] == "OK"
    assert result["details"]["pending_trigger_watcher_owned"] == [{
        "local_order_id": "L-live",
        "signal_id": "sig-live",
    }]
    assert result["details"]["pending_trigger_ownerless"] == []
    assert result["details"]["client_state"]["pending_trigger_rows"] == [{
        "local_order_id": "L-live",
        "signal_id": "sig-live",
    }]


def test_readiness_inventory_rejects_invalid_mode_before_database_query(monkeypatch):
    """The query seam cannot be used without an exact runner mode."""
    import ap.db as db

    def _unexpected_database_open():
        raise AssertionError("invalid readiness mode must not open the database")

    monkeypatch.setattr(db, "conn", _unexpected_database_open)
    with pytest.raises(ValueError, match="execution_mode"):
        pr._query_client_state("jason@example.com", "staging")


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


def test_exact_restart_rearm_retry_owner_is_row_hold_not_live_block(monkeypatch):
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

    assert result["status"] == "OK"
    assert result["ok"] is True
    assert "pending_trigger_without_watcher_ownership" not in result["errors"]
    assert result["details"]["pending_trigger_retry_owned"] == [{
        "local_order_id": "L-1",
        "signal_id": "sig-1",
    }]
    assert result["details"]["pending_trigger_ownerless"] == []
    assert result["details"]["pending_trigger_without_watcher"] == []
    assert result["details"]["overnight_reeval"]["status"] == "pending"
    assert result["details"]["overnight_reeval"]["source"] == "pending_trigger_retry_owned"
    assert "overnight_reeval_retryable" in result["warnings"]
    assert row["meta"]["restart_rearm_status"] == "RETRY_PENDING"
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()
    broker.replace_order.assert_not_called()


def test_mixed_exact_watcher_and_retry_owner_keeps_live_tradeflow_available(monkeypatch):
    """A retry-owned row is held individually while a healthy watcher remains usable."""
    watcher_row = {
        "local_order_id": "L-watcher",
        "signal_id": "sig-watcher",
        "client_id": "jason@example.com",
        "execution_mode": "live",
        "status": "PENDING_TRIGGER",
        "meta": {},
    }
    now_utc = datetime.now(timezone.utc)
    retry_row = {
        "local_order_id": "L-retry",
        "signal_id": "sig-retry",
        "client_id": "jason@example.com",
        "execution_mode": "live",
        "status": "PENDING_TRIGGER",
        "meta": {
            "restart_rearm_status": "RETRY_PENDING",
            "restart_rearm_owner": "restart_rearm:jason@example.com:live:L-retry",
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
    rows_by_id = {
        row["local_order_id"]: row for row in (watcher_row, retry_row)
    }
    runner = _Runner(
        mode="live",
        watcher=_MultiExactWatcher([watcher_row]),
    )
    runner.order_state_machine = SimpleNamespace(
        get_order=lambda oid: dict(rows_by_id[oid]) if oid in rows_by_id else None
    )
    runner.core.broker = MagicMock()
    _stub_common(monkeypatch, client_state={
        "stale_processing_ids": [],
        "watching_orphans": [],
        "pending_trigger_rows": [
            {"local_order_id": watcher_row["local_order_id"], "signal_id": watcher_row["signal_id"]},
            {"local_order_id": retry_row["local_order_id"], "signal_id": retry_row["signal_id"]},
        ],
        "watching_count": 1,
    })

    result = pr.run_preopen_autonomous_readiness(
        "jason@example.com", "live", dry_run=True, runner=runner
    )

    assert result["status"] == "OK"
    assert result["ok"] is True
    assert result["errors"] == []
    assert [row["local_order_id"] for row in result["details"]["pending_trigger_watcher_owned"]] == ["L-watcher"]
    assert [row["local_order_id"] for row in result["details"]["pending_trigger_retry_owned"]] == ["L-retry"]
    assert result["details"]["pending_trigger_ownerless"] == []
    assert result["details"]["pending_trigger_without_watcher"] == []

    # Exercise the actual ClientRunner readiness interpretation as well: an
    # account with only valid watcher/retry ownership stays entry-available.
    from client_runner import ClientRunner

    live_runner = object.__new__(ClientRunner)
    live_runner.email = "jason@example.com"
    live_runner.mode = "LIVE"
    live_runner._degraded_lock = threading.Lock()
    live_runner.degraded_reasons = set()
    live_runner.degraded = _Evt(False)
    live_runner.entries_allowed = _Evt(True)
    live_runner.failed = _Evt(False)
    live_runner.stopping = _Evt(False)
    live_runner._set_entry_permission = live_runner.entries_allowed.set
    enforced = ClientRunner._enforce_post_overnight_readiness.__get__(
        live_runner, ClientRunner
    )(result, context="mixed_retry_readiness")
    assert enforced["status"] == "OK"
    assert live_runner.entries_allowed.is_set() is True
    assert live_runner.degraded.is_set() is False
    assert live_runner.degraded_reasons == set()
    runner.core.broker.submit_order.assert_not_called()
    runner.core.broker.cancel_order.assert_not_called()
    runner.core.broker.replace_order.assert_not_called()


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
