from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ap.paper_restart_guard_rescue import build_paper_restart_guard_rescue_plan


def _queue_row(
    *,
    queue_id: int,
    client_id: str = "tradefluencehq@gmail.com",
    signal_id: str = "sig-1",
    status: str = "ARCHIVED",
    last_error: str = "restart_guard:overnight_skip",
    created_ts: datetime | None = None,
    ticker: str = "AAPL",
    side: str = "CALL",
):
    return {
        "id": queue_id,
        "client_id": client_id,
        "signal_id": signal_id,
        "status": status,
        "last_error": last_error,
        "created_ts": created_ts or datetime.now(timezone.utc),
        "ticker": ticker,
        "side": side,
    }


def _order_row(
    *,
    client_id: str = "tradefluencehq@gmail.com",
    signal_id: str = "sig-1",
    status: str = "PENDING_TRIGGER",
    local_order_id: str = "local-1",
    broker_order_id=None,
    submitted_ts=None,
    filled_ts=None,
):
    return {
        "client_id": client_id,
        "signal_id": signal_id,
        "kind": "ENTRY",
        "status": status,
        "local_order_id": local_order_id,
        "broker_order_id": broker_order_id,
        "submitted_ts": submitted_ts,
        "filled_ts": filled_ts,
    }


def test_restart_guard_row_without_order_is_restored_to_new():
    plan = build_paper_restart_guard_rescue_plan(
        queue_rows=[_queue_row(queue_id=1)],
        order_rows=[],
        active_paper_clients={"tradefluencehq@gmail.com"},
        now_utc=datetime.now(timezone.utc),
        lookback_hours=48,
    )
    assert len(plan) == 1
    assert plan[0].action == "NEW"
    assert plan[0].reason == "restart_guard_archived_no_order_row"


def test_watcher_owned_pending_trigger_row_is_restored_to_watching():
    plan = build_paper_restart_guard_rescue_plan(
        queue_rows=[_queue_row(queue_id=2)],
        order_rows=[_order_row()],
        active_paper_clients={"tradefluencehq@gmail.com"},
        now_utc=datetime.now(timezone.utc),
        lookback_hours=48,
    )
    assert len(plan) == 1
    assert plan[0].action == "WATCHING"
    assert plan[0].reason == "restart_guard_archived_watcher_owned_order"
    assert plan[0].order_local_id == "local-1"


def test_active_non_watcher_order_is_skipped():
    plan = build_paper_restart_guard_rescue_plan(
        queue_rows=[_queue_row(queue_id=3)],
        order_rows=[_order_row(status="SUBMITTED", broker_order_id="BRK-1")],
        active_paper_clients={"tradefluencehq@gmail.com"},
        now_utc=datetime.now(timezone.utc),
        lookback_hours=48,
    )
    assert len(plan) == 1
    assert plan[0].action == "SKIP"
    assert plan[0].reason == "active_entry_order_exists_non_watcher_owned"


def test_duplicate_stop_queue_rows_are_skipped():
    plan = build_paper_restart_guard_rescue_plan(
        queue_rows=[
            _queue_row(
                queue_id=4,
                last_error="restart_guard:overnight_skip|stop_queue_path_use_overnight_reeval_2026_06_22",
            )
        ],
        order_rows=[],
        active_paper_clients={"tradefluencehq@gmail.com"},
        now_utc=datetime.now(timezone.utc),
        lookback_hours=48,
    )
    assert len(plan) == 1
    assert plan[0].action == "SKIP"
    assert plan[0].reason == "duplicate_or_contract_selection_skip"


def test_outside_lookback_is_skipped():
    old_ts = datetime.now(timezone.utc) - timedelta(hours=72)
    plan = build_paper_restart_guard_rescue_plan(
        queue_rows=[_queue_row(queue_id=5, created_ts=old_ts)],
        order_rows=[],
        active_paper_clients={"tradefluencehq@gmail.com"},
        now_utc=datetime.now(timezone.utc),
        lookback_hours=48,
    )
    assert len(plan) == 1
    assert plan[0].action == "SKIP"
    assert plan[0].reason == "outside_lookback"


def test_non_paper_client_is_skipped():
    plan = build_paper_restart_guard_rescue_plan(
        queue_rows=[_queue_row(queue_id=6, client_id="jasoncosby1@gmail.com")],
        order_rows=[],
        active_paper_clients={"tradefluencehq@gmail.com"},
        now_utc=datetime.now(timezone.utc),
        lookback_hours=48,
    )
    assert len(plan) == 1
    assert plan[0].action == "SKIP"
    assert plan[0].reason == "inactive_or_non_paper_client"


def test_same_client_signal_only_one_actionable_row_is_emitted():
    now = datetime.now(timezone.utc)
    plan = build_paper_restart_guard_rescue_plan(
        queue_rows=[
            _queue_row(queue_id=8, created_ts=now - timedelta(minutes=1)),
            _queue_row(queue_id=7, created_ts=now - timedelta(minutes=2)),
        ],
        order_rows=[],
        active_paper_clients={"tradefluencehq@gmail.com"},
        now_utc=now,
        lookback_hours=48,
    )
    assert len(plan) == 2
    assert plan[0].action == "NEW"
    assert plan[1].action == "SKIP"
    assert plan[1].reason == "duplicate_signal_row"


def test_route_uses_dry_run_and_handoff_hook():
    src = (REPO_ROOT / "app.py").read_text()
    assert 'dry_run = bool(body.get("dry_run", True))' in src
    assert "_run_paper_restart_guard_handoff" in src
    assert '"watching_restored"' in src
