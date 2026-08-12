from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import threading
from types import SimpleNamespace
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
    _load_exit_fills,
    _select_canonical_proof_row,
    _update_canonical_proof_row,
    _reconciliation_marker_retryable,
    _reconcile_exit_fill,
    _run_reconciliation_attempt,
    official_live_eligibility,
    project_position_from_exit_fills,
    reconcile_confirmed_exit_fill,
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


def test_public_confirmed_exit_fill_entrypoint_delegates_to_canonical_reducer(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    order = {"local_order_id": "exit-1"}
    result = {"status": "EXIT_FILLED"}
    delegated = {"position_id": "position-1"}
    observed = {}

    def _delegate(arg_order, arg_result):
        observed["order"] = arg_order
        observed["result"] = arg_result
        return delegated

    monkeypatch.setattr(guard, "_reconcile_exit_fill", _delegate)
    assert reconcile_confirmed_exit_fill(order, result) == delegated
    assert observed == {"order": order, "result": result}


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


def test_fill_loader_includes_exact_current_partial_row_with_null_filled_ts_only_for_matching_local_id() -> None:
    class _Result:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    exit_rows = [
        {
            "local_order_id": "exit-current",
            "broker_order_id": "broker-current",
            "position_id": "position-1",
            "filled_qty": 2,
            "fill_price": 1.50,
            "filled_ts": None,
            "status": "EXIT_PARTIAL_FILL",
            "contract": "SPY260717C00600000",
            "client_id": "jason@example.com",
            "kind": "EXIT",
            "created_ts": "2026-07-17T16:01:00Z",
        },
        {
            "local_order_id": "exit-unrelated",
            "broker_order_id": "broker-unrelated",
            "position_id": "position-1",
            "filled_qty": 1,
            "fill_price": 1.60,
            "filled_ts": None,
            "status": "EXIT_PARTIAL_FILL",
            "contract": "SPY260717C00600000",
            "client_id": "jason@example.com",
            "kind": "EXIT",
            "created_ts": "2026-07-17T16:02:00Z",
        },
    ]

    class _Connection:
        def execute(self, sql, params):
            assert "OR (%s <> '' AND local_order_id=%s)" in sql
            assert "ORDER BY filled_ts ASC NULLS LAST, created_ts ASC" in sql
            (
                client_id,
                contract,
                statuses,
                entry_ts,
                _entry_ts_again,
                current_local_id,
                current_local_id_again,
                position_id,
                local_id_match_gate,
                local_id_match_again,
            ) = params
            assert current_local_id == current_local_id_again == "exit-current"
            assert local_id_match_gate == local_id_match_again == "exit-current"
            filtered = []
            for row in exit_rows:
                if row["client_id"] != client_id or row["kind"] != "EXIT":
                    continue
                if str(row["contract"]).upper() != str(contract).upper():
                    continue
                if row["status"] not in statuses:
                    continue
                if int(row["filled_qty"] or 0) <= 0 or row["fill_price"] is None:
                    continue
                timestamp_ok = entry_ts is None or (
                    row["filled_ts"] is not None and row["filled_ts"] >= entry_ts
                )
                exact_current_ok = bool(current_local_id) and row["local_order_id"] == current_local_id
                if not (timestamp_ok or exact_current_ok):
                    continue
                identity_ok = (
                    str(row["position_id"]) == str(position_id)
                    or (bool(local_id_match_gate) and row["local_order_id"] == local_id_match_gate)
                )
                if identity_ok:
                    filtered.append({
                        key: row[key]
                        for key in (
                            "local_order_id",
                            "broker_order_id",
                            "position_id",
                            "filled_qty",
                            "fill_price",
                            "filled_ts",
                            "status",
                        )
                    })
            filtered.sort(key=lambda row: ((row["filled_ts"] is None), row["filled_ts"], row["local_order_id"]))
            return _Result(filtered)

    rows = _load_exit_fills(
        _Connection(),
        {
            "id": "position-1",
            "contract": "SPY260717C00600000",
            "entry_ts": "2026-07-17T15:59:00Z",
        },
        _exit_order(
            local_order_id="exit-current",
            position_id="position-1",
            contract="SPY260717C00600000",
            status="EXIT_PARTIAL_FILL",
        ),
    )
    assert [row["local_order_id"] for row in rows] == ["exit-current"]


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


def test_partial_fill_projects_exact_durable_exit_ownership(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    captured = {}

    class _Result:
        rowcount = 1

    class _Connection:
        def execute(self, *_args, **_kwargs):
            return _Result()

    @contextmanager
    def _conn():
        yield _Connection()

    def _update(_c, table, updates, _where, _params):
        assert table == "positions"
        captured.update(updates)
        return 1

    monkeypatch.setattr(guard, "conn", _conn)
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(guard, "_finish_reconciliation_attempt", lambda *_a, **_k: None)
    monkeypatch.setattr(guard, "_resolve_position", lambda *_: {
        "id": "position-1",
        "client_id": "jason@example.com",
        "contract": "SPY260717C00600000",
        "qty": 4,
        "avg_fill": 1.00,
    })
    monkeypatch.setattr(guard, "_load_exit_fills", lambda *_: [{
        "local_order_id": "exit-local-1",
        "broker_order_id": "broker-exit-1",
        "filled_qty": 2,
        "fill_price": 1.50,
        "filled_ts": "2026-07-17T16:00:00Z",
    }])
    monkeypatch.setattr(guard, "_load_entry_order", lambda *_: {})
    monkeypatch.setattr(guard, "_table_columns", lambda *_: {
        "contracts_exited", "quantity_remaining", "exit_price", "realized_pnl",
        "realized_pnl_pct", "exit_in_flight", "pending_exit_qty",
        "pending_exit_local_order_id", "pending_exit_broker_order_id", "updated_at",
    })
    monkeypatch.setattr(guard, "_dynamic_update", _update)
    result = _run_reconciliation_attempt(
        _exit_order(
            status="EXIT_PARTIAL_FILL",
            qty=4,
            filled_qty=2,
            fill_price=1.50,
        ),
        {
            "status": "PARTIAL_FILL",
            "filled_qty": 2,
            "fill_price": 1.50,
            "broker_order_id": "broker-exit-1",
        },
        attempt_count=1,
    )
    assert captured["exit_in_flight"] is True
    assert captured["pending_exit_qty"] == 2
    assert captured["pending_exit_local_order_id"] == "exit-local-1"
    assert captured["pending_exit_broker_order_id"] == "broker-exit-1"
    assert result["exit_ownership"] == {
        "exit_in_flight": True,
        "pending_exit_local_order_id": "exit-local-1",
        "pending_exit_broker_order_id": "broker-exit-1",
        "pending_exit_qty": 2,
    }


def test_partial_fill_with_null_filled_ts_holds_before_position_mutation(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    position_updates_seen = []

    class _Result:
        rowcount = 1

        def __init__(self, rows=None, row=None):
            self._rows = rows or []
            self._row = row

        def fetchall(self):
            return self._rows

        def fetchone(self):
            return self._row

    class _Connection:
        def execute(self, sql, params=None):
            params = params or ()
            if "SELECT local_order_id, broker_order_id, position_id, filled_qty, fill_price, filled_ts, status" in sql:
                assert "OR (%s <> '' AND local_order_id=%s)" in sql
                return _Result(rows=[
                    {
                        "local_order_id": "exit-current",
                        "broker_order_id": "broker-current",
                        "position_id": "position-1",
                        "filled_qty": 2,
                        "fill_price": 1.50,
                        "filled_ts": None,
                        "status": "EXIT_PARTIAL_FILL",
                    }
                ])
            if "UPDATE orders SET position_id=%s, updated_ts=NOW() WHERE client_id=%s AND local_order_id IN %s" in sql:
                assert params[2] == ("exit-current",)
                return _Result()
            if "UPDATE orders SET position_id=%s, updated_ts=NOW() WHERE client_id=%s AND local_order_id=%s" in sql:
                raise AssertionError("entry order position rewrite should not run when entry order is missing")
            raise AssertionError(f"unexpected SQL: {sql}")

    @contextmanager
    def _conn():
        yield _Connection()

    def _update(_c, table, updates, _where, _params):
        assert table == "positions"
        position_updates_seen.append(dict(updates))
        return 1

    monkeypatch.setattr(guard, "conn", _conn)
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(guard, "_finish_reconciliation_attempt", lambda *_a, **_k: None)
    monkeypatch.setattr(guard, "_resolve_position", lambda *_: {
        "id": "position-1",
        "client_id": "jason@example.com",
        "contract": "SPY260717C00600000",
        "qty": 4,
        "avg_fill": 1.00,
        "entry_ts": "2026-07-17T15:59:00Z",
        "status": "OPEN",
    })
    monkeypatch.setattr(guard, "_load_entry_order", lambda *_: {})
    monkeypatch.setattr(guard, "_table_columns", lambda _c, table: (
        {
            "contracts_exited",
            "quantity_remaining",
            "exit_price",
            "realized_pnl",
            "realized_pnl_pct",
            "exit_in_flight",
            "pending_exit_qty",
            "pending_exit_local_order_id",
            "pending_exit_broker_order_id",
            "updated_at",
            "status",
            "exit_ts",
        }
        if table == "positions"
        else set()
    ))
    monkeypatch.setattr(guard, "_dynamic_update", _update)

    order = _exit_order(
        local_order_id="exit-current",
        broker_order_id="broker-current",
        position_id="position-1",
        contract="SPY260717C00600000",
        status="EXIT_PARTIAL_FILL",
        qty=4,
        filled_qty=2,
        fill_price=1.50,
        filled_ts=None,
    )
    result_payload = {
        "status": "EXIT_PARTIAL_FILL",
        "filled_qty": 2,
        "fill_price": 1.50,
        "broker_order_id": "broker-current",
        "filled_ts": None,
    }

    for _ in range(2):
        with pytest.raises(
            guard.LifecycleProjectionError,
            match="EXIT_FILL_TIMESTAMP_UNPROVEN",
        ):
            _run_reconciliation_attempt(order, result_payload, attempt_count=1)

    assert position_updates_seen == []


def test_exact_originating_entry_proof_identity_wins_over_position_fallback() -> None:
    class _Result:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class _Connection:
        def execute(self, sql, params):
            if "local_order_id=%s" in sql:
                assert params == ("jason@example.com", "entry-local-1")
                return _Result([{
                    "id": "proof-exact",
                    "local_order_id": "entry-local-1",
                    "position_id": None,
                }])
            raise AssertionError("position fallback must not run after exact ENTRY identity")

    row, quarantine = _select_canonical_proof_row(
        _Connection(),
        proof_columns={"id", "client_email", "local_order_id", "position_id"},
        client_id="jason@example.com",
        position_id="position-1",
        entry_local_order_id="entry-local-1",
    )
    assert row["id"] == "proof-exact"
    assert quarantine is None


def test_duplicate_position_proofs_are_quarantined_with_every_candidate_id() -> None:
    class _Result:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class _Connection:
        def execute(self, sql, _params):
            if "local_order_id=%s" in sql:
                return _Result([])
            assert "position_id::text=%s" in sql
            return _Result([
                {"id": "proof-a", "local_order_id": None, "position_id": "position-1"},
                {"id": "proof-b", "local_order_id": None, "position_id": "position-1"},
            ])

    row, quarantine = _select_canonical_proof_row(
        _Connection(),
        proof_columns={"id", "client_email", "local_order_id", "position_id"},
        client_id="jason@example.com",
        position_id="position-1",
        entry_local_order_id="entry-local-missing",
    )
    assert row is None
    assert quarantine["status"] == "QUARANTINED"
    assert quarantine["reason_code"] == "PROOF_IDENTITY_AMBIGUOUS"
    assert quarantine["candidate_proof_ids"] == ["proof-a", "proof-b"]


def test_canonical_proof_update_uses_exact_primary_key_and_client(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    update = MagicMock(return_value=1)
    monkeypatch.setattr(
        guard,
        "_select_canonical_proof_row",
        lambda *_args, **_kwargs: ({"id": "proof-exact"}, None),
    )
    monkeypatch.setattr(guard, "_dynamic_update", update)
    changed, diagnostic = _update_canonical_proof_row(
        object(),
        proof_columns={"id", "client_email", "local_order_id", "position_id"},
        client_id="jason@example.com",
        position_id="position-1",
        entry_local_order_id="entry-local-1",
        proof_updates={"win": True},
    )
    assert changed == 1
    assert diagnostic["status"] == "RECONCILED"
    assert diagnostic["proof_id"] == "proof-exact"
    assert update.call_args.args[3] == "id::text=%s AND client_email=%s"
    assert update.call_args.args[4] == ("proof-exact", "jason@example.com")


def test_proof_quarantine_retains_durable_position_reconciliation(monkeypatch) -> None:
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
        lambda *_: ({"status": "IN_PROGRESS", "attempt_count": 3}, "EXIT_FILLED"),
    )
    monkeypatch.setattr(
        guard,
        "_write_marker",
        lambda _c, _client, _local, marker: written.update(marker),
    )
    _finish_reconciliation_attempt(
        _exit_order(),
        {},
        attempt_count=3,
        reconciled_position_id="position-1",
        proof_reconciliation={
            "status": "QUARANTINED",
            "reason_code": "PROOF_IDENTITY_AMBIGUOUS",
            "error": "multiple proof rows match canonical position",
            "candidate_proof_ids": ["proof-a", "proof-b"],
            "rows_updated": 0,
        },
    )
    assert written["status"] == "QUARANTINED"
    assert written["reason_code"] == "PROOF_IDENTITY_AMBIGUOUS"
    assert written["position_id"] == "position-1"
    assert written["position_reconciled"] is True
    assert written["reconciled_at"] == ""
    assert written["candidate_proof_ids"] == ["proof-a", "proof-b"]


def test_successful_proof_retry_resolves_prior_proof_quarantine(monkeypatch) -> None:
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
        lambda *_: ({
            "status": "IN_PROGRESS",
            "reason_code": "RECONCILIATION_RETRY_CLAIMED",
            "candidate_proof_ids": ["proof-a", "proof-b"],
            "attempt_count": 4,
        }, "EXIT_FILLED"),
    )
    monkeypatch.setattr(
        guard,
        "_write_marker",
        lambda _c, _client, _local, marker: written.update(marker),
    )
    _finish_reconciliation_attempt(
        _exit_order(),
        {},
        attempt_count=4,
        reconciled_position_id="position-1",
        proof_reconciliation={
            "status": "RECONCILED",
            "reason_code": "CANONICAL_EXIT_PROOF_RECONCILED",
            "error": "",
            "proof_id": "proof-final",
            "candidate_proof_ids": [],
            "rows_updated": 1,
        },
    )
    assert written["status"] == "RECONCILED"
    assert written["reason_code"] == "CANONICAL_EXIT_FILL_RECONCILED"
    assert written["candidate_proof_ids"] == []
    assert written["proof_reconciliation"]["proof_id"] == "proof-final"
    assert written["reconciled_at"]


def test_pending_exit_ownership_migration_is_idempotent_on_postgres() -> None:
    test_url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")
    if not test_url or "test" not in test_url.lower():
        pytest.skip("PostgreSQL test database is unavailable")
    psycopg2 = pytest.importorskip("psycopg2")
    migration = Path(
        "migrations/20260717_positions_pending_exit_ownership.sql"
    ).read_text()
    db = psycopg2.connect(test_url)
    db.autocommit = True
    try:
        with db.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS public.positions "
                "(id TEXT PRIMARY KEY, exit_in_flight BOOLEAN DEFAULT FALSE)"
            )
            cursor.execute(migration)
            cursor.execute(migration)
            cursor.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name='positions' "
                "AND column_name = ANY(%s) ORDER BY column_name",
                ([
                    "pending_exit_qty",
                    "pending_exit_local_order_id",
                    "pending_exit_broker_order_id",
                    "pending_exit_action",
                    "pending_exit_reason",
                ],),
            )
            assert [row[0] for row in cursor.fetchall()] == [
                "pending_exit_action",
                "pending_exit_broker_order_id",
                "pending_exit_local_order_id",
                "pending_exit_qty",
                "pending_exit_reason",
            ]
    finally:
        db.close()


def test_postgres_duplicate_proofs_are_not_mutated_by_production_sql() -> None:
    from ap.db import _ConnWrapper

    test_url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")
    if not test_url or "test" not in test_url.lower():
        pytest.skip("PostgreSQL test database is unavailable")
    psycopg2 = pytest.importorskip("psycopg2")
    extras = pytest.importorskip("psycopg2.extras")
    db = psycopg2.connect(test_url)
    db.autocommit = True
    try:
        with db.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            wrapped = _ConnWrapper(db, cursor)
            cursor.execute("DROP SCHEMA IF EXISTS pr360_proof_test CASCADE")
            cursor.execute("CREATE SCHEMA pr360_proof_test")
            cursor.execute("SET search_path TO pr360_proof_test, public")
            cursor.execute(
                "CREATE TABLE proof_trades ("
                "id TEXT PRIMARY KEY, client_email TEXT NOT NULL, "
                "position_id TEXT, local_order_id TEXT, win BOOLEAN)"
            )
            cursor.execute(
                "INSERT INTO proof_trades "
                "(id, client_email, position_id, local_order_id, win) VALUES "
                "('proof-a','jason@example.com','position-1',NULL,FALSE),"
                "('proof-b','jason@example.com','position-1',NULL,FALSE)"
            )
            changed, diagnostic = _update_canonical_proof_row(
                wrapped,
                proof_columns={
                    "id", "client_email", "position_id", "local_order_id", "win"
                },
                client_id="jason@example.com",
                position_id="position-1",
                entry_local_order_id="entry-local-missing",
                proof_updates={"win": True},
            )
            assert changed == 0
            assert diagnostic["reason_code"] == "PROOF_IDENTITY_AMBIGUOUS"
            assert diagnostic["candidate_proof_ids"] == ["proof-a", "proof-b"]
            cursor.execute("SELECT count(*) AS count FROM proof_trades WHERE win IS TRUE")
            assert cursor.fetchone()["count"] == 0
    finally:
        with db.cursor() as cleanup:
            cleanup.execute("DROP SCHEMA IF EXISTS pr360_proof_test CASCADE")
        db.close()


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


def test_batch_recovery_reports_proof_quarantine_as_unresolved(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    now = datetime.now(timezone.utc)
    row = {
        "client_id": "jason@example.com",
        "local_order_id": "exit-local-1",
        "meta": {"exit_fill_reconciliation": {
            "status": "QUARANTINED",
            "last_attempt_at": (now - timedelta(minutes=10)).isoformat(),
        }},
    }

    class _Cursor:
        def execute(self, *_args, **_kwargs):
            return self

        def fetchall(self):
            return [row]

    @contextmanager
    def _conn():
        yield _Cursor()

    monkeypatch.setattr(guard, "conn", _conn)
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(
        guard,
        "retry_exit_fill_reconciliation",
        lambda **_kwargs: {
            "position_id": "position-1",
            "proof_reconciliation": {
                "status": "QUARANTINED",
                "reason_code": "PROOF_IDENTITY_AMBIGUOUS",
            },
        },
    )
    outcomes = retry_pending_exit_fill_reconciliations(
        client_id="jason@example.com",
        limit=100,
    )
    assert outcomes == [{
        "client_id": "jason@example.com",
        "local_order_id": "exit-local-1",
        "status": "QUARANTINED",
        "reconciled": False,
        "result": {
            "position_id": "position-1",
            "proof_reconciliation": {
                "status": "QUARANTINED",
                "reason_code": "PROOF_IDENTITY_AMBIGUOUS",
            },
        },
    }]


def test_startup_recovery_runs_client_scoped_exit_retry_before_position_and_exit_recovery(
    monkeypatch,
) -> None:
    import ap.exit_fill_truth_guard as guard
    from ap_recovery import APStartupRecovery

    events = []
    scan = MagicMock(return_value=[])
    monkeypatch.setattr(guard, "retry_pending_exit_fill_reconciliations", scan)
    recovery = APStartupRecovery.__new__(APStartupRecovery)
    recovery.client_id = "jason@example.com"
    recovery._execution_mode = lambda: "live"
    recovery._recover_deferred_breach_lifecycles = lambda _result: events.append("deferred")
    recovery._reconcile_stale_exit_generation_claims = lambda _result: events.append("stale_exit_claims")
    recovery._recover_exit_fills_that_occurred_during_downtime = lambda _result: (events.append("downtime_exit_fills") or [])
    recovery._recover_positions = lambda _result: events.append("positions")
    recovery._verify_pending_entries = lambda _result: events.append("entries")
    recovery._reattach_live_exit_protections = lambda _result, _live_exit_orders: events.append("exits")
    original_retry = recovery._retry_canonical_exit_fill_reconciliations

    def _retry(result):
        events.append("exit_fill_retry")
        return original_retry(result)

    recovery._retry_canonical_exit_fill_reconciliations = _retry
    recovery._recompute_buying_power = lambda _result: events.append("buying_power")
    recovery._reseed_dedup = lambda _result: events.append("dedup")

    result = recovery.run(include_watcher_reseed=False)
    assert events.index("exit_fill_retry") < events.index("downtime_exit_fills")
    assert events.index("stale_exit_claims") < events.index("downtime_exit_fills")
    assert events.index("downtime_exit_fills") < events.index("positions")
    assert events.index("exit_fill_retry") < events.index("positions")
    assert events.index("exit_fill_retry") < events.index("exits")
    assert events.count("exit_fill_retry") == 1
    scan.assert_called_once_with(client_id="jason@example.com", limit=100)
    assert result["exit_fill_reconciliations_attempted"] == 0
    assert result["errors"] == []


def test_startup_recovery_retries_canonical_exit_fill_before_loading_stale_runtime_state(
    monkeypatch,
) -> None:
    import ap.exit_fill_truth_guard as guard
    from ap_recovery import APStartupRecovery

    state = {"position_closed": False, "manual_resubmission": False}
    scan = MagicMock(side_effect=lambda **_kwargs: state.__setitem__("position_closed", True) or [])
    monkeypatch.setattr(guard, "retry_pending_exit_fill_reconciliations", scan)

    recovery = APStartupRecovery.__new__(APStartupRecovery)
    recovery.client_id = "jason@example.com"
    recovery.pm = SimpleNamespace(register_recovered_position=MagicMock())
    recovery.mc = SimpleNamespace(_position_count=0)
    recovery._execution_mode = lambda: "live"
    recovery._recover_deferred_breach_lifecycles = lambda _result: None
    recovery._reconcile_stale_exit_generation_claims = lambda _result: None
    recovery._recover_exit_fills_that_occurred_during_downtime = lambda _result: (state.__setitem__("position_closed", True) or [])
    recovery._verify_pending_entries = lambda _result: None
    recovery._reseed_dedup = lambda _result: None
    recovery._recover_positions = lambda result: (
        recovery.pm.register_recovered_position({"id": "stale-pos"})
        if not state["position_closed"] else None,
        result.__setitem__(
            "positions_recovered",
            result["positions_recovered"] + (0 if state["position_closed"] else 1),
        ),
        setattr(
            recovery.mc,
            "_position_count",
            recovery.mc._position_count + (0 if state["position_closed"] else 1),
        ),
    )
    recovery._reattach_live_exit_protections = lambda result, _live_exit_orders: (
        state.__setitem__("manual_resubmission", not state["position_closed"]),
        result.__setitem__(
            "exits_reattached",
            result["exits_reattached"] + (0 if state["position_closed"] else 1),
        ),
    )
    recovery._recompute_buying_power = lambda result: result.__setitem__(
        "buying_power_reserved",
        0.0 if state["position_closed"] else 250.0,
    )

    result = recovery.run(include_watcher_reseed=False)

    recovery.pm.register_recovered_position.assert_not_called()
    assert recovery.mc._position_count == 0
    assert state["manual_resubmission"] is False
    assert result["positions_recovered"] == 0
    assert result["exits_reattached"] == 0
    assert result["buying_power_reserved"] == 0.0


def test_startup_exit_retry_records_resolved_quarantined_failed_and_claimed(
    monkeypatch,
) -> None:
    import ap.exit_fill_truth_guard as guard
    from ap_recovery import APStartupRecovery

    monkeypatch.setattr(
        guard,
        "retry_pending_exit_fill_reconciliations",
        lambda **_kwargs: [
            {
                "local_order_id": "exit-ok",
                "status": "RECONCILED",
                "reconciled": True,
                "result": {"position_id": "position-1"},
            },
            {
                "local_order_id": "exit-quarantined",
                "status": "QUARANTINED",
                "reconciled": False,
                "result": {"proof_reconciliation": {
                    "reason_code": "PROOF_IDENTITY_AMBIGUOUS",
                }},
            },
            {
                "local_order_id": "exit-failed",
                "status": "FAILED",
                "reconciled": False,
                "error": "db down",
            },
            {
                "local_order_id": "exit-claimed",
                "status": "NOT_CLAIMED",
                "reconciled": False,
                "result": None,
            },
        ],
    )
    recovery = APStartupRecovery.__new__(APStartupRecovery)
    recovery.client_id = "jason@example.com"
    result = {
        "exit_fill_reconciliations_attempted": 0,
        "exit_fill_reconciliations_reconciled": 0,
        "exit_fill_reconciliations_quarantined": 0,
        "exit_fill_reconciliations_failed": 0,
        "exit_fill_reconciliations_skipped": 0,
        "errors": [],
    }
    recovery._retry_canonical_exit_fill_reconciliations(result)
    assert result["exit_fill_reconciliations_attempted"] == 4
    assert result["exit_fill_reconciliations_reconciled"] == 1
    assert result["exit_fill_reconciliations_quarantined"] == 1
    assert result["exit_fill_reconciliations_failed"] == 1
    assert result["exit_fill_reconciliations_skipped"] == 1
    assert result["errors"] == [
        "exit_fill_reconciliation_unresolved:exit-quarantined:PROOF_IDENTITY_AMBIGUOUS",
        "exit_fill_reconciliation_failed:exit-failed:db down",
    ]


def test_startup_exit_retry_discovery_failure_is_diagnostic_not_fatal(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard
    from ap_recovery import APStartupRecovery

    monkeypatch.setattr(
        guard,
        "retry_pending_exit_fill_reconciliations",
        MagicMock(side_effect=RuntimeError("catalog unavailable")),
    )
    recovery = APStartupRecovery.__new__(APStartupRecovery)
    recovery.client_id = "jason@example.com"
    recovery._execution_mode = lambda: "live"
    recovery._recover_deferred_breach_lifecycles = lambda _result: None
    recovery._reconcile_stale_exit_generation_claims = lambda _result: None
    recovery._recover_exit_fills_that_occurred_during_downtime = lambda _result: []
    recovery._recover_positions = lambda _result: None
    recovery._verify_pending_entries = lambda _result: None
    recovery._reattach_live_exit_protections = lambda _result, _live_exit_orders: None
    recovery._recompute_buying_power = lambda _result: None
    recovery._reseed_dedup = lambda _result: None

    result = recovery.run(include_watcher_reseed=False)
    assert result["exit_fill_reconciliations_failed"] == 1
    assert result["errors"] == ["exit_fill_reconciliation: catalog unavailable"]


def test_batch_recovery_reports_position_ambiguity_as_quarantined(monkeypatch) -> None:
    import ap.exit_fill_truth_guard as guard

    now = datetime.now(timezone.utc)
    row = {
        "client_id": "jason@example.com",
        "local_order_id": "exit-local-ambiguous",
        "meta": {"exit_fill_reconciliation": {
            "status": "RETRY_REQUIRED",
            "last_attempt_at": (now - timedelta(minutes=10)).isoformat(),
        }},
    }

    class _Cursor:
        def execute(self, *_args, **_kwargs):
            return self

        def fetchall(self):
            return [row]

    @contextmanager
    def _conn():
        yield _Cursor()

    monkeypatch.setattr(guard, "conn", _conn)
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(
        guard,
        "retry_exit_fill_reconciliation",
        MagicMock(
            side_effect=ReconciliationIdentityError(
                "CANONICAL_POSITION_AMBIGUOUS",
                ["position-a", "position-b"],
            )
        ),
    )

    outcomes = retry_pending_exit_fill_reconciliations(
        client_id="jason@example.com",
        limit=100,
    )

    assert outcomes == [{
        "client_id": "jason@example.com",
        "local_order_id": "exit-local-ambiguous",
        "status": "QUARANTINED",
        "reconciled": False,
        "result": {
            "reason_code": "CANONICAL_POSITION_AMBIGUOUS",
            "candidate_position_ids": ["position-a", "position-b"],
            "error": "CANONICAL_POSITION_AMBIGUOUS",
        },
        "error": "CANONICAL_POSITION_AMBIGUOUS",
    }]


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
