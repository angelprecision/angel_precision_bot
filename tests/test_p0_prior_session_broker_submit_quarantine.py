"""P0 regressions for prior-session DAY broker-submit quarantine (#574)."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:1/test")

import ap_execution_core
from ap.broker_submit_identity import (
    build_entry_submit_payload,
    canonical_broker_submit_key,
    entry_submit_payload_hash,
)
from ap.brokers.tradier import TradierBroker, TradierConfig


CLIENT = "jason@example.com"
NOW = datetime(2026, 9, 2, 16, 0, tzinfo=timezone.utc)
AAPL_ID = "33121850-ce16-43c0-a1e0-964c8bc608f1"
CSCO_ID = "386a608d-6282-4a08-a97f-2a6413f93064"


def _historical_row(
    *,
    local_order_id: str = AAPL_ID,
    symbol: str = "AAPL",
    contract: str = "AAPL260904C00230000",
    execution_mode: str = "live",
    intent_at: str | None = "2026-09-01T16:00:00+00:00",
    **overrides,
) -> dict:
    key = canonical_broker_submit_key(local_order_id)
    payload = build_entry_submit_payload(
        symbol=symbol,
        contract=contract,
        qty=1,
        limit_price=2.25,
        broker_submit_key=key,
    )
    row = {
        "local_order_id": local_order_id,
        "client_id": CLIENT,
        "execution_mode": execution_mode,
        "signal_id": f"sig-{local_order_id}",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "symbol": symbol,
        "contract": contract,
        "qty": 1,
        "limit_price": 2.25,
        "broker_order_id": None,
        "submitted_ts": None,
        "filled_ts": None,
        "position_id": None,
        "filled_qty": None,
        "created_ts": datetime(2026, 9, 1, 16, 0, tzinfo=timezone.utc),
        "meta": {
            "execution_mode": execution_mode,
            "lifecycle_state": "SUBMITTING",
            "materialization_generation": 4,
            "submit_intent_at": intent_at,
            "broker_submit_key": key,
            "broker_submit_payload_hash": entry_submit_payload_hash(payload),
            "current_owner": f"broker_submit:{key}",
        },
    }
    row.update(overrides)
    return row


def _core_for(row: dict, broker: MagicMock | None = None):
    osm = MagicMock()
    osm.get_order.return_value = row
    osm.retain_broker_submit_owner_for_reconciliation.return_value = True
    osm.update_order_meta.return_value = True

    broker = broker or MagicMock()
    broker.list_orders.return_value = []
    core = SimpleNamespace(
        client_id=CLIENT,
        email=CLIENT,
        execution_mode=str(row.get("execution_mode") or "live"),
        mode=str(row.get("execution_mode") or "live").upper(),
        order_state_machine=osm,
        broker=broker,
        entry_watcher=MagicMock(),
    )
    core._is_real_occ_contract = ap_execution_core.APExecutionCore._is_real_occ_contract
    core.reconcile_deferred_broker_intent = (
        ap_execution_core.APExecutionCore.reconcile_deferred_broker_intent.__get__(
            core, type(core)
        )
    )
    return core, osm, broker


def _assert_no_money_path_mutations(core, broker, osm):
    broker.place_order.assert_not_called()
    broker.cancel_order.assert_not_called()
    broker.list_positions.assert_not_called()
    broker.session.post.assert_not_called()
    broker.session.delete.assert_not_called()
    core.entry_watcher.watch.assert_not_called()
    for method in (
        "transition",
        "terminalize_deferred_breach",
        "terminalize_deferred_retry_if_unchanged",
        "submit_existing_entry",
    ):
        getattr(osm, method).assert_not_called()


def _assert_fail_closed_historical_ambiguity(
    result, local_order_id, row, core, broker, osm
):
    assert result["historical_nonblocking"] is False
    assert result.get("historical_quarantine_persisted") is not True
    assert result.get("broker_submit_owner_retention_persisted") is True
    assert local_order_id not in getattr(
        core, "_historical_quarantine_verified_ids", set()
    )
    osm.retain_broker_submit_owner_for_reconciliation.assert_called_once()
    retain_kwargs = osm.retain_broker_submit_owner_for_reconciliation.call_args.kwargs
    assert retain_kwargs["broker_submit_key"] == row["meta"]["broker_submit_key"]
    assert retain_kwargs["payload_hash"] == row["meta"]["broker_submit_payload_hash"]
    osm.update_order_meta.assert_not_called()
    _assert_no_money_path_mutations(core, broker, osm)


@pytest.mark.parametrize(
    ("local_order_id", "symbol", "contract"),
    [
        (AAPL_ID, "AAPL", "AAPL260904C00230000"),
        (CSCO_ID, "CSCO", "CSCO260904C00045000"),
    ],
)
def test_prior_session_empty_position_quarantines_without_order_poll_or_mutation(
    local_order_id, symbol, contract
):
    row = _historical_row(
        local_order_id=local_order_id,
        symbol=symbol,
        contract=contract,
    )
    broker = MagicMock()
    broker.list_positions_authoritative.return_value = []
    core, osm, broker = _core_for(row, broker)

    result = core.reconcile_deferred_broker_intent(
        local_order_id=local_order_id,
        now_utc=NOW,
    )

    assert result["disposition"] == "RECONCILE_PENDING"
    assert result["broker_truth"] == "NO_CURRENT_POSITION"
    assert result["historical_nonblocking"] is True
    assert result["historical_quarantine_persisted"] is True
    broker.list_positions_authoritative.assert_called_once_with()
    broker.list_orders.assert_not_called()
    osm.retain_broker_submit_owner_for_reconciliation.assert_called_once()
    retain_kwargs = osm.retain_broker_submit_owner_for_reconciliation.call_args.kwargs
    assert retain_kwargs["broker_submit_key"] == row["meta"]["broker_submit_key"]
    assert retain_kwargs["payload_hash"] == row["meta"]["broker_submit_payload_hash"]
    assert retain_kwargs["reason"] == "RECONCILE_PRIOR_SESSION_DAY_NO_CURRENT_POSITION"
    marker_patch = osm.update_order_meta.call_args.args[1]
    assert marker_patch["broker_submit_resolution_state"] == (
        "PRIOR_SESSION_DAY_NO_CURRENT_POSITION"
    )
    assert marker_patch["broker_submit_resolution_contract"] == contract
    assert marker_patch["broker_submit_resolution_execution_mode"] == "live"
    assert osm.update_order_meta.call_args.kwargs["expected_status"] == "PENDING_TRIGGER"
    assert osm.update_order_meta.call_args.kwargs["expected_execution_mode"] == "live"
    broker.list_positions.assert_not_called()
    _assert_no_money_path_mutations(core, broker, osm)


def test_prior_session_exact_position_remains_blocking_and_never_terminalizes():
    row = _historical_row()
    broker = MagicMock()
    broker.list_positions_authoritative.return_value = [
        {"symbol": row["contract"], "quantity": 1.0}
    ]
    core, osm, broker = _core_for(row, broker)

    result = core.reconcile_deferred_broker_intent(
        local_order_id=AAPL_ID,
        now_utc=NOW,
    )

    assert result["broker_truth"] == "POSITION_PRESENT"
    assert result["historical_nonblocking"] is False
    assert result["reason_code"] == "RECONCILE_PRIOR_SESSION_POSITION_PRESENT"
    broker.list_positions_authoritative.assert_called_once_with()
    broker.list_orders.assert_not_called()
    osm.update_order_meta.assert_not_called()
    _assert_no_money_path_mutations(core, broker, osm)


def test_prior_session_exact_zero_quantity_is_unknown_and_remains_blocking():
    row = _historical_row()
    broker = MagicMock()
    broker.list_positions_authoritative.return_value = [
        {"symbol": row["contract"], "quantity": 0}
    ]
    core, osm, broker = _core_for(row, broker)

    result = core.reconcile_deferred_broker_intent(
        local_order_id=AAPL_ID,
        now_utc=NOW,
    )

    assert result["broker_truth"] == "UNKNOWN"
    assert "RECONCILE_BROKER_POSITION_QUERY_FAILED" in result["reason_code"]
    broker.list_positions_authoritative.assert_called_once_with()
    broker.list_orders.assert_not_called()
    _assert_fail_closed_historical_ambiguity(
        result, AAPL_ID, row, core, broker, osm
    )


def test_prior_session_after_hours_timestamp_is_not_a_completed_session():
    # 19:00 ET on Tuesday is outside the regular session even though Tuesday
    # and Wednesday are both NYSE trading dates.
    row = _historical_row(intent_at="2026-09-01T23:00:00+00:00")
    broker = MagicMock()
    core, osm, broker = _core_for(row, broker)

    result = core.reconcile_deferred_broker_intent(
        local_order_id=AAPL_ID,
        now_utc=NOW,
    )

    assert result["reason_code"] == "RECONCILE_PRIOR_SESSION_CALENDAR_UNPROVEN"
    broker.list_positions_authoritative.assert_not_called()
    broker.list_orders.assert_not_called()
    _assert_fail_closed_historical_ambiguity(
        result, AAPL_ID, row, core, broker, osm
    )


def test_historical_quarantine_write_failure_remains_blocking_and_unmarked():
    row = _historical_row()
    broker = MagicMock()
    broker.list_positions_authoritative.return_value = []
    core, osm, broker = _core_for(row, broker)
    osm.update_order_meta.return_value = False

    result = core.reconcile_deferred_broker_intent(
        local_order_id=AAPL_ID,
        now_utc=NOW,
    )

    assert result["historical_nonblocking"] is False
    assert result["historical_quarantine_persisted"] is False
    assert result["reason_code"] == "RECONCILE_HISTORICAL_RESOLUTION_WRITE_FAILED"
    broker.list_orders.assert_not_called()
    _assert_no_money_path_mutations(core, broker, osm)


@pytest.mark.parametrize(
    "position_result",
    [
        RuntimeError("network timeout"),
        ValueError("malformed positions"),
        {"positions": []},
        [{"symbol": "AAPL260904C00230000"}],
        [{"symbol": "AAPL260904C00230000", "quantity": "nan"}],
    ],
)
def test_prior_session_position_truth_failure_stays_unknown_and_blocking(position_result):
    row = _historical_row()
    broker = MagicMock()
    if isinstance(position_result, Exception):
        broker.list_positions_authoritative.side_effect = position_result
    else:
        broker.list_positions_authoritative.return_value = position_result
    core, osm, broker = _core_for(row, broker)

    result = core.reconcile_deferred_broker_intent(
        local_order_id=AAPL_ID,
        now_utc=NOW,
    )

    assert result["broker_truth"] == "UNKNOWN"
    assert result.get("historical_nonblocking") is not True
    assert result["current_session_order_query"] == "NOT_REQUIRED"
    assert "RECONCILE_BROKER_POSITION_QUERY_FAILED" in result["reason_code"]
    broker.list_orders.assert_not_called()
    osm.update_order_meta.assert_not_called()
    _assert_no_money_path_mutations(core, broker, osm)


def test_same_session_still_uses_current_session_order_reconciliation():
    row = _historical_row(
        intent_at=(NOW - timedelta(minutes=5)).isoformat(),
    )
    broker = MagicMock()
    broker.list_orders.return_value = []
    core, osm, broker = _core_for(row, broker)

    result = core.reconcile_deferred_broker_intent(
        local_order_id=AAPL_ID,
        now_utc=NOW,
    )

    assert result["reason_code"] == "RECONCILE_BROKER_NO_MATCH_OBSERVED"
    broker.list_orders.assert_called_once_with()
    broker.list_positions_authoritative.assert_not_called()
    assert result["broker_submit_owner_retention_persisted"] is True
    _assert_no_money_path_mutations(core, broker, osm)


@pytest.mark.parametrize(
    "overrides",
    [
        {"client_id": "other@example.com"},
        {"execution_mode": "paper"},
        {"meta": {"execution_mode": "paper"}},
        {"meta": {"lifecycle_state": "RETRY_WAIT"}},
        {"contract": "AAPL-not-an-occ"},
        {"meta": {"broker_submit_key": "wrong-key"}},
        {"meta": {"current_owner": "recovery_submit:wrong"}},
        {"meta": {"broker_submit_payload_hash": "bad-hash"}},
        {"meta": {"submit_intent_at": "2026-09-03T16:00:00+00:00"}},
        {"meta": {"submit_intent_at": "2026-09-01T16:00:00"}},
    ],
)
def test_identity_mode_and_timestamp_failures_never_enter_historical_exception(overrides):
    row = _historical_row()
    row.update(overrides)
    if "meta" in overrides:
        row["meta"] = {**_historical_row()["meta"], **overrides["meta"]}
    broker = MagicMock()
    core, osm, broker = _core_for(row, broker)

    result = core.reconcile_deferred_broker_intent(
        local_order_id=AAPL_ID,
        now_utc=NOW,
    )

    assert result.get("historical_nonblocking") is not True
    broker.list_positions_authoritative.assert_not_called()
    broker.list_orders.assert_not_called()
    osm.update_order_meta.assert_not_called()


def test_calendar_ambiguity_fails_closed_without_position_or_order_query(monkeypatch):
    row = _historical_row()
    broker = MagicMock()
    core, osm, broker = _core_for(row, broker)
    monkeypatch.setattr(
        ap_execution_core,
        "_prior_session_classification",
        lambda *_args: "calendar_unproven",
    )

    result = core.reconcile_deferred_broker_intent(
        local_order_id=AAPL_ID,
        now_utc=NOW,
    )

    assert result["reason_code"] == "RECONCILE_PRIOR_SESSION_CALENDAR_UNPROVEN"
    assert result.get("historical_nonblocking") is not True
    broker.list_positions_authoritative.assert_not_called()
    broker.list_orders.assert_not_called()
    osm.update_order_meta.assert_not_called()


def test_quarantine_marker_survives_restart_and_revalidates_position_truth():
    row = _historical_row()
    broker = MagicMock()
    broker.list_positions_authoritative.return_value = []
    core, osm, broker = _core_for(row, broker)

    first = core.reconcile_deferred_broker_intent(
        local_order_id=AAPL_ID,
        now_utc=NOW,
    )
    assert first["historical_nonblocking"] is True
    row["meta"].update(osm.update_order_meta.call_args.args[1])
    osm.update_order_meta.reset_mock()

    second = core.reconcile_deferred_broker_intent(
        local_order_id=AAPL_ID,
        now_utc=NOW + timedelta(minutes=1),
    )

    assert second["historical_nonblocking"] is True
    assert broker.list_positions_authoritative.call_count == 2
    broker.list_orders.assert_not_called()
    assert osm.update_order_meta.call_count == 1
    _assert_no_money_path_mutations(core, broker, osm)

    broker.list_positions_authoritative.return_value = [
        {"symbol": row["contract"], "quantity": 1}
    ]
    third = core.reconcile_deferred_broker_intent(
        local_order_id=AAPL_ID,
        now_utc=NOW + timedelta(minutes=2),
    )
    assert third["historical_nonblocking"] is False
    assert third["historical_quarantine_invalidated"] is True
    invalidation = osm.update_order_meta.call_args.args[1]
    assert invalidation["broker_submit_resolution_state"] is None
    broker.list_orders.assert_not_called()
    _assert_no_money_path_mutations(core, broker, osm)


def test_existing_quarantine_marker_is_not_reused_after_position_truth_failure():
    row = _historical_row()
    broker = MagicMock()
    broker.list_positions_authoritative.return_value = []
    core, osm, broker = _core_for(row, broker)

    first = core.reconcile_deferred_broker_intent(
        local_order_id=AAPL_ID,
        now_utc=NOW,
    )
    assert first["historical_nonblocking"] is True
    row["meta"].update(osm.update_order_meta.call_args.args[1])
    assert AAPL_ID in core._historical_quarantine_verified_ids
    osm.update_order_meta.reset_mock()

    broker.list_positions_authoritative.side_effect = RuntimeError("position read failed")
    second = core.reconcile_deferred_broker_intent(
        local_order_id=AAPL_ID,
        now_utc=NOW + timedelta(minutes=1),
    )

    assert second["broker_truth"] == "UNKNOWN"
    assert second["historical_nonblocking"] is False
    assert AAPL_ID not in core._historical_quarantine_verified_ids
    _assert_no_money_path_mutations(core, broker, osm)


def test_position_present_with_marker_invalidation_cas_failure_stays_blocking():
    row = _historical_row()
    broker = MagicMock()
    broker.list_positions_authoritative.return_value = []
    core, osm, broker = _core_for(row, broker)

    first = core.reconcile_deferred_broker_intent(
        local_order_id=AAPL_ID,
        now_utc=NOW,
    )
    assert first["historical_nonblocking"] is True
    row["meta"].update(osm.update_order_meta.call_args.args[1])
    osm.update_order_meta.reset_mock()
    osm.update_order_meta.return_value = False
    broker.list_positions_authoritative.return_value = [
        {"symbol": row["contract"], "quantity": 1}
    ]

    result = core.reconcile_deferred_broker_intent(
        local_order_id=AAPL_ID,
        now_utc=NOW + timedelta(minutes=1),
    )

    assert result["broker_truth"] == "POSITION_PRESENT"
    assert result["historical_nonblocking"] is False
    assert result["historical_quarantine_invalidated"] is False
    assert AAPL_ID not in core._historical_quarantine_verified_ids
    _assert_no_money_path_mutations(core, broker, osm)


def test_readiness_excludes_only_currently_fenced_historical_quarantine_rows():
    source = Path(__file__).resolve().parents[1] / "ap" / "preopen_readiness.py"
    text = source.read_text(encoding="utf-8")
    assert "PRIOR_SESSION_DAY_NO_CURRENT_POSITION" in text
    assert "broker_submit_resolution_trading_date" in text
    assert "broker_submit:" in text
    assert "AND NOT (" in text
    assert "_historical_quarantine_verified_ids" in text
    assert "ANY(%s::text[])" in text
    assert "broker_submit_resolution_contract" in text
    assert "broker_submit_resolution_client_id" in text


def test_startup_cleanup_excludes_durable_broker_submit_handoffs():
    source = (Path(__file__).resolve().parents[1] / "client_runner.py").read_text(
        encoding="utf-8"
    )
    candidates = source[source.index("WITH candidates AS") : source.index("active_proof AS")]
    assert "AND NOT (" in candidates
    for field in (
        "submit_intent_at",
        "broker_submit_key",
        "broker_submit_payload_hash",
        "current_owner",
        "lifecycle_state",
        "broker_submit_resolution_state",
    ):
        assert f"o.meta->>'{field}'" in candidates


def test_readiness_query_uses_current_process_quarantine_fence(monkeypatch):
    from ap import db as ap_db
    from ap import preopen_readiness

    pending_row = {"local_order_id": AAPL_ID, "signal_id": "sig-aapl"}

    class Cursor:
        def __init__(self):
            self.rows = []
            self.calls = []

        def execute(self, sql, params=()):
            self.calls.append((sql, params))
            if "status = 'PROCESSING'" in sql:
                self.rows = []
            elif "SELECT q.id, q.signal_id" in sql:
                self.rows = []
            elif "SELECT local_order_id, signal_id" in sql:
                self.rows = [pending_row]
            elif "SELECT COUNT(*)" in sql:
                self.rows = [{"n": 0}]
            else:
                raise AssertionError(f"unexpected readiness SQL: {sql}")

        def fetchall(self):
            return self.rows

        def fetchone(self):
            return self.rows[0] if self.rows else None

    cursor = Cursor()

    class Connection:
        def __enter__(self):
            return cursor

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(ap_db, "conn", lambda: Connection())
    monkeypatch.setattr(ap_db, "run_with_retry", lambda fn, *args, **kwargs: fn())

    runner = SimpleNamespace(
        mode="LIVE",
        core=SimpleNamespace(_historical_quarantine_verified_ids={AAPL_ID}),
    )
    preopen_readiness._query_client_state(CLIENT, runner=runner)

    pending_call = next(sql_call for sql_call in cursor.calls if "SELECT local_order_id" in sql_call[0])
    assert "ANY(%s::text[])" in pending_call[0]
    assert pending_call[1][2] == [AAPL_ID]

    runner.core._historical_quarantine_verified_ids.clear()
    preopen_readiness._query_client_state(CLIENT, runner=runner)
    pending_call = [
        sql_call for sql_call in cursor.calls if "SELECT local_order_id" in sql_call[0]
    ][-1]
    assert pending_call[1][2] == []


def test_tradier_authoritative_positions_propagates_failure_and_accepts_empty_shape():
    cfg = TradierConfig(
        base_url="https://api.tradier.com",
        account_id="acct",
        access_token="token",
    )
    broker = TradierBroker(cfg)
    broker._get = MagicMock(return_value={"positions": {"position": []}})
    assert broker.list_positions_authoritative() == []

    broker._get.return_value = {
        "positions": {
            "position": {
                "symbol": "AAPL260904C00230000",
                "quantity": "1",
                "cost_basis": "225.00",
            }
        }
    }
    positions = broker.list_positions_authoritative()
    assert positions[0]["symbol"] == "AAPL260904C00230000"
    assert positions[0]["quantity"] == 1.0

    broker._get.side_effect = TimeoutError("broker unavailable")
    with pytest.raises(TimeoutError):
        broker.list_positions_authoritative()


@pytest.mark.parametrize(
    "payload",
    [
        {"positions": {}},
        {"positions": []},
    ],
)
def test_tradier_authoritative_positions_rejects_ambiguous_empty_roots(payload):
    cfg = TradierConfig(
        base_url="https://api.tradier.com",
        account_id="acct",
        access_token="token",
    )
    broker = TradierBroker(cfg)
    broker._get = MagicMock(return_value=payload)

    with pytest.raises(ValueError, match="TRADIER_POSITIONS_PAYLOAD_MALFORMED"):
        broker.list_positions_authoritative()


@pytest.mark.parametrize(
    "payload",
    [
        {"positions": {}},
        {"positions": []},
    ],
)
def test_prior_session_ambiguous_empty_position_roots_stay_blocking(
    payload,
):
    row = _historical_row()
    strict_broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            account_id="acct",
            access_token="token",
        )
    )
    strict_broker._get = MagicMock(return_value=payload)

    broker = MagicMock()
    broker.list_positions_authoritative.side_effect = (
        strict_broker.list_positions_authoritative
    )
    core, osm, broker = _core_for(row, broker)

    result = core.reconcile_deferred_broker_intent(
        local_order_id=AAPL_ID,
        now_utc=NOW,
    )

    assert result["broker_truth"] == "UNKNOWN"
    assert "RECONCILE_BROKER_POSITION_QUERY_FAILED" in result["reason_code"]
    broker.list_positions_authoritative.assert_called_once_with()
    strict_broker._get.assert_called_once()
    broker.list_orders.assert_not_called()
    _assert_fail_closed_historical_ambiguity(
        result, AAPL_ID, row, core, broker, osm
    )
