from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import threading
from unittest.mock import MagicMock

import pytest

from ap.exit_fill_truth_guard import (
    LifecycleProjectionError,
    PositionUpdateCardinalityError,
    ReconciliationIdentityError,
    _claim_reconciliation_retry,
    _failure_reason,
    _finish_reconciliation_attempt,
    _is_partial_result,
    _reconciliation_marker_retryable,
    _reconcile_exit_fill,
    _run_reconciliation_attempt,
    official_live_eligibility,
    project_position_from_exit_fills,
    retry_exit_fill_reconciliation,
    retry_pending_exit_fill_reconciliations,
)


def _position(*, qty: int, entry: float) -> dict:
    return {"qty": qty, "avg_fill": entry}


def test_spy_full_exit_projection_uses_all_broker_fills() -> None:
    projection = project_position_from_exit_fills(
        _position(qty=14, entry=0.61),
        [
            {"filled_ts": "2026-07-16T18:55:15Z", "filled_qty": 5, "fill_price": 1.26},
            {"filled_ts": "2026-07-16T18:55:21Z", "filled_qty": 3, "fill_price": 1.25},
            {"filled_ts": "2026-07-16T18:55:37Z", "filled_qty": 2, "fill_price": 1.25},
            {"filled_ts": "2026-07-16T18:55:52Z", "filled_qty": 1, "fill_price": 1.40},
            {"filled_ts": "2026-07-16T18:56:08Z", "filled_qty": 1, "fill_price": 1.35},
            {"filled_ts": "2026-07-16T18:56:23Z", "filled_qty": 1, "fill_price": 1.35},
            {"filled_ts": "2026-07-16T18:56:39Z", "filled_qty": 1, "fill_price": 1.35},
        ],
    )

    assert projection.exited_qty == 14
    assert projection.remaining_qty == 0
    assert projection.closed is True
    assert projection.weighted_exit_price == pytest.approx(1.285714, abs=1e-6)
    assert projection.realized_pnl == pytest.approx(946.0)
    assert projection.realized_pnl_pct == pytest.approx(110.7728)
    assert projection.final_fill_ts == "2026-07-16T18:56:39Z"


def test_spy_partial_exit_preserves_four_open_contracts() -> None:
    projection = project_position_from_exit_fills(
        _position(qty=14, entry=0.61),
        [
            {"filled_ts": "2026-07-16T18:55:13Z", "filled_qty": 5, "fill_price": 1.26},
            {"filled_ts": "2026-07-16T18:55:28Z", "filled_qty": 3, "fill_price": 1.23},
            {"filled_ts": "2026-07-16T18:55:44Z", "filled_qty": 2, "fill_price": 1.27},
        ],
    )

    assert projection.exited_qty == 10
    assert projection.remaining_qty == 4
    assert projection.closed is False
    assert projection.realized_pnl == pytest.approx(643.0)
    assert projection.weighted_exit_price == pytest.approx(1.253)


def test_partial_googl_exit_does_not_falsely_close_position() -> None:
    projection = project_position_from_exit_fills(
        _position(qty=4, entry=2.75),
        [{"filled_ts": "2026-07-16T17:47:54Z", "filled_qty": 2, "fill_price": 4.20}],
    )

    assert projection.exited_qty == 2
    assert projection.remaining_qty == 2
    assert projection.closed is False
    assert projection.realized_pnl == pytest.approx(290.0)
    assert projection.realized_pnl_pct == pytest.approx(52.7273)


def test_exit_overfill_is_quarantined_not_clamped() -> None:
    with pytest.raises(LifecycleProjectionError, match="exit_overfill"):
        project_position_from_exit_fills(
            _position(qty=1, entry=1.27),
            [{"filled_ts": "2026-07-16T18:40:51Z", "filled_qty": 2, "fill_price": 1.35}],
        )


def test_rows_without_positive_broker_fill_are_not_counted() -> None:
    with pytest.raises(LifecycleProjectionError, match="no_positive_exit_fills"):
        project_position_from_exit_fills(
            _position(qty=3, entry=2.63),
            [
                {"filled_qty": 0, "fill_price": 3.30},
                {"filled_qty": 1, "fill_price": None},
            ],
        )


def test_partial_result_detection_uses_normalized_or_broker_status() -> None:
    assert _is_partial_result({}, {"status": "EXIT_PARTIAL_FILL"}) is True
    assert _is_partial_result({}, {"status": "partially_filled"}) is True
    assert _is_partial_result({"status": "EXIT_PARTIAL_FILL"}, {}) is True
    assert _is_partial_result({}, {"status": "FILLED"}) is False


def test_fill_loader_never_sweeps_unrelated_synthetic_orders() -> None:
    source = __import__("inspect").getsource(
        __import__("ap.exit_fill_truth_guard", fromlist=["_load_exit_fills"])._load_exit_fills
    )
    assert "local_order_id=%s" in source
    assert "position_id IS NULL" not in source
    assert "broker-repair-%%" not in source


def test_partial_exit_path_invokes_canonical_sync() -> None:
    from pathlib import Path

    source = Path("ap/fill_monitor.py").read_text()
    partial_start = source.index('if mapped in ("PARTIAL_FILL", "EXIT_PARTIAL_FILL")')
    ack_start = source.index("# ── ACKNOWLEDGED", partial_start)
    partial_body = source[partial_start:ack_start]
    assert "partial_applied and kind == \"EXIT\"" in partial_body
    assert "_sync_exit_price(order, partial_result)" in partial_body


@pytest.mark.parametrize(
    (
        "execution_mode",
        "closed",
        "entry_id",
        "exit_id",
        "all_exit_fills_broker_backed",
        "expected",
    ),
    [
        ("live", True, "entry-1", "exit-1", True, True),
        ("LIVE", True, "entry-1", "exit-1", True, True),
        ("paper", True, "entry-1", "exit-1", True, False),
        ("unknown", True, "entry-1", "exit-1", True, False),
        ("live", False, "entry-1", "exit-1", True, False),
        ("live", True, "", "exit-1", True, False),
        ("live", True, "entry-1", "", True, False),
        ("live", True, "entry-1", "exit-2", False, False),
    ],
)
def test_official_live_proof_requires_complete_broker_lifecycle(
    execution_mode: str,
    closed: bool,
    entry_id: str,
    exit_id: str,
    all_exit_fills_broker_backed: bool,
    expected: bool,
) -> None:
    assert official_live_eligibility(
        execution_mode=execution_mode,
        closed=closed,
        entry_broker_order_id=entry_id,
        exit_broker_order_id=exit_id,
        all_exit_fills_broker_backed=all_exit_fills_broker_backed,
    ) is expected


def _exit_order(**overrides) -> dict:
    row = {
        "client_id": "jason@example.com",
        "execution_mode": "live",
        "local_order_id": "exit-local-1",
        "broker_order_id": "broker-exit-1",
        "position_id": "position-1",
        "contract": "SPY260717C00600000",
        "status": "EXIT_FILLED",
        "qty": 1,
        "filled_qty": 1,
        "fill_price": 1.35,
        "filled_ts": "2026-07-16T18:40:51Z",
        "meta": {},
    }
    row.update(overrides)
    return row


def test_reconciliation_exception_routes_to_durable_retry_required(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    finished = MagicMock()
    monkeypatch.setattr(guard, "_begin_reconciliation_attempt", lambda *_: 1)
    monkeypatch.setattr(guard, "_finish_reconciliation_attempt", finished)
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: (_ for _ in ()).throw(RuntimeError("db down")))

    with pytest.raises(RuntimeError, match="db down"):
        _reconcile_exit_fill(_exit_order(), {})

    error = finished.call_args.kwargs["error"]
    assert _failure_reason(error)[:2] == ("RETRY_REQUIRED", "RECONCILIATION_SQL_FAILURE")
    assert finished.call_args.kwargs["attempt_count"] == 1


def test_ambiguous_position_routes_to_durable_quarantine() -> None:
    error = ReconciliationIdentityError(
        "CANONICAL_POSITION_AMBIGUOUS", ["position-a", "position-b"]
    )
    assert _failure_reason(error) == (
        "QUARANTINED",
        "CANONICAL_POSITION_AMBIGUOUS",
        ["position-a", "position-b"],
    )


@pytest.mark.parametrize(
    ("error", "expected_status", "expected_reason"),
    [
        (RuntimeError("db down"), "RETRY_REQUIRED", "RECONCILIATION_SQL_FAILURE"),
        (
            ReconciliationIdentityError(
                "CANONICAL_POSITION_AMBIGUOUS", ["position-a", "position-b"]
            ),
            "QUARANTINED",
            "CANONICAL_POSITION_AMBIGUOUS",
        ),
    ],
)
def test_failure_marker_is_durably_written(
    monkeypatch, error, expected_status, expected_reason
) -> None:
    import ap.exit_fill_truth_guard as guard

    written = {}

    @contextmanager
    def _conn():
        yield object()

    monkeypatch.setattr(guard, "conn", _conn)
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(guard, "_read_marker", lambda *_: ({"attempt_count": 1}, "EXIT_FILLED"))
    monkeypatch.setattr(guard, "_write_marker", lambda _c, _client, _local, marker: written.update(marker))

    _finish_reconciliation_attempt(
        _exit_order(), {}, attempt_count=1, error=error
    )
    assert written["status"] == expected_status
    assert written["reason_code"] == expected_reason
    assert written["attempt_count"] == 1
    assert written["recorded_by"] == "canonical_exit_fill_reconciler"
    if expected_reason == "CANONICAL_POSITION_AMBIGUOUS":
        assert written["candidate_position_ids"] == ["position-a", "position-b"]


def test_successful_retry_resolves_durable_marker_without_resetting_attempts(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    written = {}

    @contextmanager
    def _conn():
        yield object()

    monkeypatch.setattr(guard, "conn", _conn)
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(
        guard,
        "_read_marker",
        lambda *_: ({"status": "IN_PROGRESS", "attempt_count": 4}, "EXIT_FILLED"),
    )
    monkeypatch.setattr(guard, "_write_marker", lambda _c, _client, _local, marker: written.update(marker))

    _finish_reconciliation_attempt(
        _exit_order(), {}, attempt_count=4, reconciled_position_id="position-1"
    )
    assert written["status"] == "RECONCILED"
    assert written["position_id"] == "position-1"
    assert written["attempt_count"] == 4
    assert written["candidate_position_ids"] == []
    assert written["reconciled_at"]


def test_stale_worker_cannot_finish_over_newer_retry_claim(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    written = MagicMock()

    @contextmanager
    def _conn():
        yield object()

    monkeypatch.setattr(guard, "conn", _conn)
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(
        guard,
        "_read_marker",
        lambda *_: ({"status": "IN_PROGRESS", "attempt_count": 3}, "EXIT_FILLED"),
    )
    monkeypatch.setattr(guard, "_write_marker", written)
    _finish_reconciliation_attempt(
        _exit_order(), {}, attempt_count=2, reconciled_position_id="position-1"
    )
    assert written.call_count == 0


def test_zero_row_position_update_stops_before_order_or_proof_mutation(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    class _Connection:
        def execute(self, *_args, **_kwargs):
            raise AssertionError("order relink or proof mutation must not execute")

    @contextmanager
    def _conn():
        yield _Connection()

    finished = MagicMock()
    monkeypatch.setattr(guard, "conn", _conn)
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(guard, "_begin_reconciliation_attempt", lambda *_: 1)
    monkeypatch.setattr(guard, "_finish_reconciliation_attempt", finished)
    monkeypatch.setattr(guard, "_resolve_position", lambda *_: {
        "id": "position-1", "client_id": "jason@example.com",
        "contract": "SPY260717C00600000", "qty": 1, "avg_fill": 1.27,
    })
    monkeypatch.setattr(guard, "_load_exit_fills", lambda *_: [{
        "local_order_id": "exit-local-1", "broker_order_id": "broker-exit-1",
        "filled_qty": 1, "fill_price": 1.35, "filled_ts": "2026-07-16T18:40:51Z",
    }])
    monkeypatch.setattr(guard, "_load_entry_order", lambda *_: {})
    monkeypatch.setattr(guard, "_table_columns", lambda *_: {
        "contracts_exited", "quantity_remaining", "exit_price", "realized_pnl",
        "realized_pnl_pct", "status", "exit_ts", "updated_at",
    })
    monkeypatch.setattr(guard, "_dynamic_update", lambda *_args, **_kwargs: 0)

    with pytest.raises(PositionUpdateCardinalityError, match="POSITION_UPDATE_ZERO_ROWS"):
        _reconcile_exit_fill(_exit_order(), {})
    assert isinstance(finished.call_args.kwargs["error"], PositionUpdateCardinalityError)


def test_retry_uses_stored_fill_and_is_idempotent(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    rows = [
        _exit_order(meta={"exit_fill_reconciliation": {"status": "RETRY_REQUIRED"}}),
        _exit_order(meta={"exit_fill_reconciliation": {
            "status": "RECONCILED", "position_id": "position-1",
        }}),
    ]
    reconcile = MagicMock(return_value={"position_id": "position-1"})

    class _Cursor:
        def __init__(self, row):
            self.row = row
        def execute(self, *_args, **_kwargs):
            return self
        def fetchone(self):
            return self.row

    @contextmanager
    def _conn():
        yield _Cursor(rows.pop(0))

    monkeypatch.setattr(guard, "conn", _conn)
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(guard, "_claim_reconciliation_retry", lambda *_args, **_kwargs: 2)
    monkeypatch.setattr(guard, "_run_reconciliation_attempt", reconcile)

    first = retry_exit_fill_reconciliation(
        client_id="jason@example.com", local_order_id="exit-local-1"
    )
    second = retry_exit_fill_reconciliation(
        client_id="jason@example.com", local_order_id="exit-local-1"
    )
    assert first == {"position_id": "position-1"}
    assert second == {"position_id": "position-1", "already_reconciled": True}
    reconcile.assert_called_once()
    stored_result = reconcile.call_args.args[1]
    assert stored_result["status"] == "EXIT_FILLED"
    assert stored_result["broker_order_id"] == "broker-exit-1"
    assert reconcile.call_args.kwargs["attempt_count"] == 2


def test_retry_path_preserves_broker_confirmed_order_status(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    observed = {}
    monkeypatch.setattr(guard, "_begin_reconciliation_attempt", lambda *_: 2)
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: {
        "position_id": "position-1", "projection": object()
    })
    monkeypatch.setattr(
        guard,
        "_finish_reconciliation_attempt",
        lambda order, *_args, **_kwargs: observed.update(status=order["status"]),
    )
    _reconcile_exit_fill(_exit_order(status="EXIT_FILLED"), {})
    assert observed["status"] == "EXIT_FILLED"


def test_partial_and_final_fills_share_canonical_reducer(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    calls = []
    monkeypatch.setattr(guard, "_begin_reconciliation_attempt", lambda *_: 1)
    monkeypatch.setattr(guard, "_finish_reconciliation_attempt", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: calls.append(fn) or {
        "position_id": "position-1", "projection": object()
    })
    _reconcile_exit_fill(_exit_order(status="EXIT_PARTIAL_FILL"), {"status": "PARTIAL_FILL"})
    _reconcile_exit_fill(_exit_order(status="EXIT_FILLED"), {"status": "FILLED"})
    assert len(calls) == 2


def test_fresh_in_progress_is_not_reclaimed() -> None:
    now = datetime.now(timezone.utc)
    marker = {"status": "IN_PROGRESS", "last_attempt_at": (now - timedelta(minutes=1)).isoformat()}
    assert _reconciliation_marker_retryable(marker, now) is False


def test_stale_in_progress_is_retryable_after_restart() -> None:
    now = datetime.now(timezone.utc)
    marker = {"status": "IN_PROGRESS", "last_attempt_at": (now - timedelta(minutes=6)).isoformat()}
    assert _reconciliation_marker_retryable(marker, now) is True


def test_crash_after_begin_does_not_permanently_strand_marker() -> None:
    now = datetime.now(timezone.utc)
    committed_before_crash = {
        "status": "IN_PROGRESS",
        "attempt_count": 1,
        "last_attempt_at": (now - timedelta(minutes=10)).isoformat(),
    }
    assert _reconciliation_marker_retryable(committed_before_crash, now) is True


def test_stale_retry_claim_increments_durable_attempt_count(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    now = datetime.now(timezone.utc)
    written = {}

    @contextmanager
    def _conn():
        yield object()

    monkeypatch.setattr(guard, "conn", _conn)
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(
        guard,
        "_read_marker",
        lambda *_: ({
            "status": "IN_PROGRESS",
            "attempt_count": 4,
            "last_attempt_at": (now - timedelta(minutes=6)).isoformat(),
        }, "EXIT_FILLED"),
    )
    monkeypatch.setattr(guard, "_write_marker", lambda _c, _client, _local, marker: written.update(marker))

    claimed = _claim_reconciliation_retry(_exit_order(), {}, now=now)
    assert claimed == 5
    assert written["status"] == "IN_PROGRESS"
    assert written["reason_code"] == "RECONCILIATION_RETRY_CLAIMED"
    assert written["attempt_count"] == 5
    assert written["last_attempt_at"] == now.isoformat()


def test_two_recovery_workers_cannot_claim_same_stale_attempt(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    now = datetime.now(timezone.utc)
    lock = threading.Lock()
    state = {
        "marker": {
            "status": "IN_PROGRESS",
            "attempt_count": 1,
            "last_attempt_at": (now - timedelta(minutes=6)).isoformat(),
        }
    }

    @contextmanager
    def _conn():
        with lock:
            yield object()

    monkeypatch.setattr(guard, "conn", _conn)
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(guard, "_read_marker", lambda *_: (dict(state["marker"]), "EXIT_FILLED"))
    monkeypatch.setattr(guard, "_write_marker", lambda _c, _client, _local, marker: state.update(marker=dict(marker)))

    claims = []
    threads = [
        threading.Thread(
            target=lambda: claims.append(
                _claim_reconciliation_retry(_exit_order(), {}, now=now)
            )
        )
        for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)
    assert sorted(value for value in claims if value is not None) == [2]
    assert claims.count(None) == 1
    assert state["marker"]["attempt_count"] == 2


def test_batch_recovery_discovers_stale_but_not_fresh_in_progress(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    now = datetime.now(timezone.utc)
    rows = [
        {
            "client_id": "fresh@example.com",
            "local_order_id": "fresh-exit",
            "meta": {"exit_fill_reconciliation": {
                "status": "IN_PROGRESS",
                "last_attempt_at": (now - timedelta(minutes=1)).isoformat(),
            }},
        },
        {
            "client_id": "stale@example.com",
            "local_order_id": "stale-exit",
            "meta": {"exit_fill_reconciliation": {
                "status": "IN_PROGRESS",
                "last_attempt_at": (now - timedelta(minutes=10)).isoformat(),
            }},
        },
    ]

    class _Cursor:
        def execute(self, *_args, **_kwargs):
            return self
        def fetchall(self):
            return rows

    @contextmanager
    def _conn():
        yield _Cursor()

    retry = MagicMock(return_value={"position_id": "position-1"})
    monkeypatch.setattr(guard, "conn", _conn)
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(guard, "retry_exit_fill_reconciliation", retry)
    outcomes = retry_pending_exit_fill_reconciliations(limit=10)
    assert [item["local_order_id"] for item in outcomes] == ["stale-exit"]
    retry.assert_called_once_with(
        client_id="stale@example.com", local_order_id="stale-exit"
    )


def test_successful_stale_recovery_finishes_reconciled(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    written = {}
    calls = 0

    @contextmanager
    def _conn():
        yield object()

    def _retry(fn):
        nonlocal calls
        calls += 1
        if calls == 1:
            return {"position_id": "position-1", "projection": object()}
        return fn()

    monkeypatch.setattr(guard, "conn", _conn)
    monkeypatch.setattr(guard, "run_with_retry", _retry)
    monkeypatch.setattr(
        guard,
        "_read_marker",
        lambda *_: ({"status": "IN_PROGRESS", "attempt_count": 2}, "EXIT_FILLED"),
    )
    monkeypatch.setattr(guard, "_write_marker", lambda _c, _client, _local, marker: written.update(marker))
    result = _run_reconciliation_attempt(_exit_order(), {}, attempt_count=2)
    assert result["position_id"] == "position-1"
    assert written["status"] == "RECONCILED"
    assert written["attempt_count"] == 2


def test_ambiguous_stale_recovery_returns_to_quarantine(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    written = {}
    calls = 0

    @contextmanager
    def _conn():
        yield object()

    def _retry(fn):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ReconciliationIdentityError(
                "CANONICAL_POSITION_AMBIGUOUS", ["position-a", "position-b"]
            )
        return fn()

    monkeypatch.setattr(guard, "conn", _conn)
    monkeypatch.setattr(guard, "run_with_retry", _retry)
    monkeypatch.setattr(
        guard,
        "_read_marker",
        lambda *_: ({"status": "IN_PROGRESS", "attempt_count": 2}, "EXIT_FILLED"),
    )
    monkeypatch.setattr(guard, "_write_marker", lambda _c, _client, _local, marker: written.update(marker))
    with pytest.raises(ReconciliationIdentityError):
        _run_reconciliation_attempt(_exit_order(), {}, attempt_count=2)
    assert written["status"] == "QUARANTINED"
    assert written["reason_code"] == "CANONICAL_POSITION_AMBIGUOUS"
    assert written["candidate_position_ids"] == ["position-a", "position-b"]


def test_retry_never_calls_broker_submit_or_cancel(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard
    from ap.brokers.tradier import TradierBroker

    order = _exit_order(meta={"exit_fill_reconciliation": {"status": "RETRY_REQUIRED"}})

    class _Cursor:
        def execute(self, *_args, **_kwargs):
            return self
        def fetchone(self):
            return order

    @contextmanager
    def _conn():
        yield _Cursor()

    place = MagicMock(side_effect=AssertionError("broker POST forbidden"))
    cancel = MagicMock(side_effect=AssertionError("broker cancel forbidden"))
    monkeypatch.setattr(TradierBroker, "place_order", place)
    monkeypatch.setattr(TradierBroker, "cancel_order", cancel)
    monkeypatch.setattr(guard, "conn", _conn)
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(guard, "_claim_reconciliation_retry", lambda *_args, **_kwargs: 2)
    monkeypatch.setattr(
        guard,
        "_run_reconciliation_attempt",
        lambda *_args, **_kwargs: {"position_id": "position-1"},
    )
    assert retry_exit_fill_reconciliation(
        client_id="jason@example.com", local_order_id="exit-local-1"
    ) == {"position_id": "position-1"}
    assert place.call_count == 0
    assert cancel.call_count == 0
