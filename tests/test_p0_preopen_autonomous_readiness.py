from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

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
        lambda client_id, **kwargs: client_state or {
            "stale_processing_ids": [],
            "watching_rows": [],
            "watching_historical_diagnostic_only": [],
            "watching_orphans": [],
            "watching_owned": [],
            "watching_blockers": [],
            "watching_blocker_reasons": [],
            "watching_classification_counts": {
                "historical_diagnostic_only": 0,
                "canonical_owner": 0,
                "blocking": 0,
            },
            "pending_trigger_rows": [],
            "watching_count": 0,
        },
    )
    monkeypatch.setattr(pr, "_trading_date", lambda now=None: "2026-06-22")
    monkeypatch.setattr(pr, "_pod_mode", lambda: "live")
    monkeypatch.setattr(pr, "_after_929_et", lambda now=None: True)
    return writes


def _watching_row(
    row_id: int | str,
    created_ts: datetime,
    *,
    signal_id: str | None = None,
    mode: str | None = None,
    owner_evidence: list[dict] | None = None,
    payload: dict | None = None,
) -> dict:
    row_payload = dict(payload or {})
    if mode is not None:
        row_payload["execution_mode"] = mode
    return {
        "id": row_id,
        "signal_id": signal_id or f"sig-{row_id}",
        "created_ts": created_ts,
        "payload": row_payload,
        "owner_evidence": list(owner_evidence or []),
    }


def _classified_client_state(rows: list[dict], *, mode: str = "live", now: datetime) -> dict:
    classified = pr._classify_watching_rows(rows, mode, now=now)
    return {
        "stale_processing_ids": [],
        "watching_rows": (
            classified["historical_diagnostic_only"]
            + classified["owned"]
            + classified["blockers"]
        ),
        "watching_historical_diagnostic_only": classified["historical_diagnostic_only"],
        "watching_orphans": classified["historical_diagnostic_only"],
        "watching_owned": classified["owned"],
        "watching_blockers": classified["blockers"],
        "watching_blocker_reasons": classified["blocker_reasons"],
        "watching_classification_counts": classified["counts"],
        "pending_trigger_rows": [],
        "watching_count": len(rows),
    }


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
    runner = _Runner(mode="live", watcher=_Watcher({"L-1"}))
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
    runner = _Runner(mode="live", watcher=_Watcher({"L-1"}))
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
    runner = _Runner(mode="live", watcher=_Watcher({"L-1"}))
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


def test_watching_row_with_no_orders_is_diagnostic_only(monkeypatch):
    _stub_common(monkeypatch, client_state={
        "stale_processing_ids": [],
        "watching_historical_diagnostic_only": [{"id": 41, "signal_id": "sig-41"}],
        "watching_orphans": [{"id": 41, "signal_id": "sig-41"}],
        "pending_trigger_rows": [],
        "watching_count": 1,
    })
    runner = _Runner(mode="paper")
    monkeypatch.setattr(pr, "_pod_mode", lambda: "paper")
    monkeypatch.setattr(pr, "_post_overnight_reeval_success_exists", lambda *args, **kwargs: True)

    result = pr.run_preopen_autonomous_readiness("paper@example.com", "paper", dry_run=True, runner=runner)
    assert result["status"] == "OK"
    assert "watching_historical_diagnostic_only" in result["warnings"]
    assert "watching_historical_diagnostic_only" not in result["errors"]
    assert result["details"]["client_state"]["watching_orphans"][0]["id"] == 41


def test_132_historical_rows_plus_one_current_ambiguous_row_blocks(monkeypatch):
    now = datetime(2026, 8, 17, 9, 30, tzinfo=pr.ET)
    historical_rows = [
        _watching_row(row_id, datetime(2026, 8, 13, 15, 0, tzinfo=pr.ET))
        for row_id in range(1, 133)
    ]
    current_ambiguous = _watching_row(133, datetime(2026, 8, 17, 9, 0, tzinfo=pr.ET))
    state = _classified_client_state(historical_rows + [current_ambiguous], now=now)
    _stub_common(monkeypatch, client_state=state)
    monkeypatch.setattr(pr, "_post_overnight_reeval_success_exists", lambda *args, **kwargs: True)
    runner = _Runner(mode="live")

    result = pr.run_preopen_autonomous_readiness(
        "live@example.com", "live", dry_run=True, runner=runner, now=now,
    )

    assert result["status"] == "BLOCKED"
    assert "watching_current_session_unresolved" in result["errors"]
    assert "watching_mode_evidence_missing" in result["errors"]
    assert "watching_historical_diagnostic_only" in result["warnings"]
    assert len(result["details"]["client_state"]["watching_orphans"]) == 132
    assert len(result["details"]["client_state"]["watching_blockers"]) == 1
    assert result["details"]["client_state"]["watching_classification_counts"] == {
        "historical_diagnostic_only": 132,
        "canonical_owner": 0,
        "blocking": 1,
    }


def test_historical_watching_rows_do_not_mask_unowned_pending_trigger(monkeypatch):
    now = datetime(2026, 8, 17, 9, 30, tzinfo=pr.ET)
    historical_rows = [
        _watching_row(row_id, datetime(2026, 8, 13, 15, 0, tzinfo=pr.ET))
        for row_id in range(1, 133)
    ]
    state = _classified_client_state(historical_rows, now=now)
    state["pending_trigger_rows"] = [{"local_order_id": "L-999", "signal_id": "sig-999"}]
    _stub_common(monkeypatch, client_state=state)
    monkeypatch.setattr(pr, "_post_overnight_reeval_success_exists", lambda *args, **kwargs: True)
    runner = _Runner(mode="live", watcher=_Watcher(set()))

    result = pr.run_preopen_autonomous_readiness(
        "live@example.com", "live", dry_run=True, runner=runner, now=now,
    )

    assert result["status"] == "BLOCKED"
    assert result["errors"] == ["pending_trigger_without_watcher_ownership"]
    assert "watching_historical_diagnostic_only" in result["warnings"]
    assert len(result["details"]["client_state"]["watching_orphans"]) == 132
    assert result["details"]["pending_trigger_without_watcher"][0]["local_order_id"] == "L-999"


def test_friday_to_monday_prior_session_is_blocking_without_exact_owner():
    now = datetime(2026, 8, 17, 9, 30, tzinfo=pr.ET)
    row = _watching_row(201, datetime(2026, 8, 14, 15, 45, tzinfo=pr.ET))

    classified = pr._classify_watching_rows([row], "live", now=now)

    assert classified["blockers"][0]["session_class"] == "prior_session"
    assert "watching_prior_session_ambiguous" in classified["blocker_reasons"]
    assert classified["historical_diagnostic_only"] == []


def test_holiday_gap_is_relevant_and_not_historical_debt(monkeypatch):
    monkeypatch.setattr(
        pr,
        "_nyse_is_trading_day",
        lambda value: value.weekday() < 5 and value.date().isoformat() != "2026-07-03",
    )
    now = datetime(2026, 7, 6, 9, 30, tzinfo=pr.ET)
    holiday_row = _watching_row(202, datetime(2026, 7, 3, 12, 0, tzinfo=pr.ET))
    prior_row = _watching_row(203, datetime(2026, 7, 2, 12, 0, tzinfo=pr.ET))
    old_row = _watching_row(204, datetime(2026, 7, 1, 12, 0, tzinfo=pr.ET))

    classified = pr._classify_watching_rows([holiday_row, prior_row, old_row], "live", now=now)

    assert classified["blockers"][0]["session_class"] == "relevant_gap"
    assert classified["blockers"][1]["session_class"] == "prior_session"
    assert [row["id"] for row in classified["historical_diagnostic_only"]] == [204]


def test_utc_timestamp_is_classified_by_et_session_boundary():
    now = datetime(2026, 8, 18, 0, 30, tzinfo=timezone.utc)  # Aug 17, 20:30 ET
    row = _watching_row(
        205,
        datetime(2026, 8, 18, 0, 15, tzinfo=timezone.utc),  # Aug 17, 20:15 ET
    )

    classified = pr._classify_watching_rows([row], "live", now=now)

    assert classified["blockers"][0]["session_class"] == "current_session"
    assert "watching_current_session_unresolved" in classified["blocker_reasons"]


def test_recent_missing_mode_evidence_is_hold():
    now = datetime(2026, 8, 17, 9, 30, tzinfo=pr.ET)
    row = _watching_row(206, datetime(2026, 8, 17, 9, 10, tzinfo=pr.ET))

    classified = pr._classify_watching_rows([row], "live", now=now)

    assert "watching_mode_evidence_missing" in classified["blocker_reasons"]
    assert classified["historical_diagnostic_only"] == []


def test_paper_evidence_cannot_authorize_live_readiness():
    now = datetime(2026, 8, 17, 9, 30, tzinfo=pr.ET)
    row = _watching_row(
        207,
        datetime(2026, 8, 17, 9, 10, tzinfo=pr.ET),
        mode="paper",
    )

    classified = pr._classify_watching_rows([row], "live", now=now)

    assert "watching_mode_conflict" in classified["blocker_reasons"]
    assert classified["historical_diagnostic_only"] == []


def test_conflicting_paper_live_evidence_is_hold():
    now = datetime(2026, 8, 17, 9, 30, tzinfo=pr.ET)
    row = _watching_row(
        209,
        datetime(2026, 8, 17, 9, 10, tzinfo=pr.ET),
        mode="paper",
        payload={"mode": "live"},
    )

    classified = pr._classify_watching_rows([row], "live", now=now)

    assert set(classified["blocker_reasons"]) == {
        "watching_mode_conflict",
        "watching_current_session_unresolved",
    }
    assert classified["historical_diagnostic_only"] == []


def test_entry_broker_fill_and_matching_position_are_canonical_owner():
    now = datetime(2026, 8, 17, 9, 30, tzinfo=pr.ET)
    row = _watching_row(
        208,
        datetime(2026, 8, 14, 15, 45, tzinfo=pr.ET),
        signal_id="sig-owner",
    )
    order = {
        "local_order_id": "local-owner",
        "broker_order_id": "broker-owner",
        "position_id": "position-owner",
        "kind": "ENTRY",
        "status": "FILLED",
        "signal_id": "sig-owner",
        "canonical_signal_id": "sig-owner",
        "execution_mode": "live",
        "submitted_ts": datetime(2026, 8, 14, 15, 46, tzinfo=pr.ET),
        "filled_ts": datetime(2026, 8, 14, 15, 47, tzinfo=pr.ET),
        "plan_id": "plan-owner",
    }
    position = {
        "id": "position-owner",
        "signal_id": "sig-owner",
        "plan_id": "plan-owner",
        "execution_mode": "live",
        "status": "OPEN",
    }
    row["owner_evidence"] = pr._watching_owner_evidence(
        row,
        orders=[order],
        positions=[position],
    )
    classified = pr._classify_watching_rows([row], "live", now=now)

    sources = {
        source
        for evidence in row["owner_evidence"]
        for source in evidence["evidence"]
    }
    assert {"entry_order", "broker_order", "submission", "fill", "matching_position"} <= sources
    assert len(classified["owned"]) == 1
    assert classified["owned"][0]["classification"] == "canonical_owner"
    assert classified["blockers"] == []
    assert classified["historical_diagnostic_only"] == []


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
    runner = _Runner(mode="live")
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
    assert details["source"] == "watching_or_pending_trigger_present_without_overnight_success"


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
    assert details["source"] == "watching_or_pending_trigger_present_without_overnight_success"


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
