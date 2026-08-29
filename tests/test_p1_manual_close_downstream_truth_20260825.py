from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import os
from pathlib import Path
import types

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test",
)

import ap.manual_close_truth_guard as guard
from ap import db


CLIENT = "jasoncosby1@gmail.com"
POSITION_ID = "2fe10f52-e459-4bc5-a57a-1a80e3618040"
SIGNAL_ID = "f7dbe723-d342-406b-8650-fce428e9a0c0"
BROKER_EXIT_ID = "143318149"
EXTERNAL_LOCAL_ID = f"external-exit:{CLIENT}:{BROKER_EXIT_ID}"


class _Cursor:
    def __init__(
        self,
        *,
        external_row=None,
        proof_rows=None,
        identity_rows=None,
        queue_rows=None,
    ):
        self.external_row = external_row or {
            "local_order_id": EXTERNAL_LOCAL_ID,
            "broker_order_id": BROKER_EXIT_ID,
            "filled_ts": datetime(2026, 8, 25, 18, 19, 38, 780000, tzinfo=timezone.utc),
            "filled_qty": 1,
            "fill_price": 1.22,
            "execution_mode": "live",
            "contract": "NOW260828P00122000",
            "direction": "CALL",
            "position_contract": "NOW260828P00122000",
            "position_direction": "CALL",
            "meta": {
                "source": "manual_client_close_broker_fill",
                "external_broker_order": True,
                "adopted_without_submit": True,
                "exit_fill_timestamp_source": "broker_response",
                "exit_fill_timestamp_key": "last_fill_date",
            },
        }
        self.proof_rows = list(
            proof_rows
            if proof_rows is not None
            else [{"id": 1, "exit_local_order_id": None}]
        )
        self.identity_rows = list(
            identity_rows
            if identity_rows is not None
            else [{"execution_mode": "live", "signal_id": SIGNAL_ID}]
        )
        self.queue_rows = list(queue_rows if queue_rows is not None else [{"id": 41}])
        self.calls = []
        self.updates = []
        self.rowcount = 0
        self._one = None
        self._many = []

    def execute(self, sql, params=None):
        text = " ".join(str(sql).split())
        self.calls.append((text, params))
        self.rowcount = 0
        self._many = []
        self._one = None
        if (
            "FROM orders" in text
            and "LIKE %s" in text
            and "JOIN positions" in text
        ):
            self._one = dict(self.external_row)
        elif "SELECT id, exit_local_order_id" in text and "FROM proof_trades" in text:
            self._many = list(self.proof_rows)
        elif "SELECT DISTINCT p.execution_mode, o.signal_id" in text:
            self._many = list(self.identity_rows)
        elif "SELECT id FROM trade_queue" in text:
            self._many = list(self.queue_rows)
        elif "UPDATE trade_queue" in text:
            self.rowcount = 1
            self.updates.append((text, params))
        elif "UPDATE proof_trades" in text:
            self.rowcount = 1
            self.updates.append((text, params))
        return self

    def fetchone(self):
        value = self._one
        self._one = None
        return value

    def fetchall(self):
        return list(self._many)


@contextmanager
def _ctx(cursor):
    yield cursor


def _wire(monkeypatch, cursor):
    monkeypatch.setattr(db, "conn", lambda: _ctx(cursor))
    monkeypatch.setattr(db, "run_with_retry", lambda fn, *a, **k: fn())


def test_live_official_manual_close_remains_official_but_is_not_training(monkeypatch):
    cursor = _Cursor()
    _wire(monkeypatch, cursor)

    stamp = guard.quarantine_manual_external_close_stamp(
        {
            "execution_mode": "live",
            "official_live_performance_eligible": True,
            "performance_taxonomy": "LIVE_OFFICIAL",
            "training_eligible": True,
            "taxonomy_reason": "tradier_exit_proof_lock_passed",
            "broker_exit_order_id": BROKER_EXIT_ID,
        },
        client_id=CLIENT,
        position_id=POSITION_ID,
    )

    assert stamp["official_live_performance_eligible"] is True
    assert stamp["performance_taxonomy"] == "LIVE_OFFICIAL"
    assert stamp["broker_exit_order_id"] == BROKER_EXIT_ID
    assert stamp["training_eligible"] is False
    assert stamp["exit_local_order_id"] == EXTERNAL_LOCAL_ID
    assert stamp["manual_external_close"] is True
    assert "manual_external_close_training_excluded" in stamp["taxonomy_reason"]

    sql, params = next(call for call in cursor.calls if "FROM orders" in call[0])
    assert "LIKE %s" in sql
    assert "external-exit:%" not in sql
    assert params[4] == "external-exit:%"
    assert "updated" + "_ts" not in sql
    assert " id DESC" not in sql


def test_non_external_live_official_trade_is_not_quarantined(monkeypatch):
    monkeypatch.setattr(guard, "_external_exit_identity", lambda *a, **k: None)
    original = {
        "performance_taxonomy": "LIVE_OFFICIAL",
        "training_eligible": True,
        "taxonomy_reason": "tradier_exit_proof_lock_passed",
    }
    assert guard.quarantine_manual_external_close_stamp(
        original, client_id=CLIENT, position_id=POSITION_ID
    ) == original


def test_manual_close_proof_update_changes_training_not_realized_taxonomy(monkeypatch):
    cursor = _Cursor()
    _wire(monkeypatch, cursor)

    updated = guard._persist_manual_close_proof_truth(
        client_id=CLIENT,
        position_id=POSITION_ID,
        external_local_order_id=EXTERNAL_LOCAL_ID,
    )

    assert updated == 1
    sql, params = next(call for call in cursor.calls if "UPDATE proof_trades" in call[0])
    assert "training_eligible = FALSE" in sql
    assert "performance_taxonomy" not in sql
    assert "official_live_performance_eligible" not in sql
    assert EXTERNAL_LOCAL_ID in params
    assert CLIENT in params
    assert POSITION_ID in params


def test_stale_queue_terminalization_is_exact_signal_and_cas_guarded(monkeypatch):
    cursor = _Cursor()
    _wire(monkeypatch, cursor)

    updated = guard._terminalize_stale_queue_after_manual_close(
        client_id=CLIENT,
        position_id=POSITION_ID,
        broker_exit_order_id=BROKER_EXIT_ID,
    )

    assert updated == 1
    sql, params = next(call for call in cursor.calls if "UPDATE trade_queue" in call[0])
    assert "SET status='FILLED'" in sql
    assert "client_id=%s" in sql
    assert "signal_id=%s" in sql
    assert "payload->>'execution_mode'" in sql
    assert "WHERE id=%s" in sql
    assert "FOR UPDATE" in next(
        call[0] for call in cursor.calls if "SELECT id FROM trade_queue" in call[0]
    )
    assert "UPPER(COALESCE(status,'')) = ANY(%s)" in sql
    assert CLIENT in params
    assert SIGNAL_ID in params
    states = params[-1]
    assert set(states) == {"NEW", "PROCESSING", "WATCHING", "ARMED"}
    assert "FILLED" not in states
    assert "SUBMITTED" not in states
    assert "DONE" not in states


def test_proof_update_refuses_duplicate_live_candidates(monkeypatch):
    cursor = _Cursor(proof_rows=[
        {"id": 1, "exit_local_order_id": None},
        {"id": 2, "exit_local_order_id": None},
    ])
    _wire(monkeypatch, cursor)

    assert guard._persist_manual_close_proof_truth(
        client_id=CLIENT,
        position_id=POSITION_ID,
        external_local_order_id=EXTERNAL_LOCAL_ID,
    ) == 0
    assert not any("UPDATE proof_trades" in sql for sql, _ in cursor.updates)


def test_queue_cleanup_refuses_missing_or_ambiguous_mode_identity(monkeypatch):
    for queue_rows in ([], [{"id": 41}, {"id": 42}]):
        cursor = _Cursor(queue_rows=queue_rows)
        _wire(monkeypatch, cursor)

        assert guard._terminalize_stale_queue_after_manual_close(
            client_id=CLIENT,
            position_id=POSITION_ID,
            broker_exit_order_id=BROKER_EXIT_ID,
        ) == 0
        assert not any("UPDATE trade_queue" in sql for sql, _ in cursor.updates)


def test_external_identity_rejects_wrong_contract_direction_or_provenance(monkeypatch):
    base = _Cursor().external_row
    invalid_rows = (
        {**base, "contract": "NOW260828P00123000"},
        {**base, "direction": "PUT"},
        {**base, "execution_mode": "paper"},
        {**base, "fill_price": float("nan")},
        {**base, "meta": {**base["meta"], "adopted_without_submit": False}},
    )
    for invalid in invalid_rows:
        cursor = _Cursor(external_row=invalid)
        _wire(monkeypatch, cursor)
        assert guard._external_exit_identity(
            CLIENT, POSITION_ID, execution_mode="live"
        ) is None


def test_failed_manual_finalizer_does_not_mutate_downstream(monkeypatch):
    calls = {"proof": 0, "queue": 0}
    monkeypatch.setattr(
        guard,
        "_persist_manual_close_proof_truth",
        lambda **kwargs: calls.__setitem__("proof", calls["proof"] + 1),
    )
    monkeypatch.setattr(
        guard,
        "_terminalize_stale_queue_after_manual_close",
        lambda **kwargs: calls.__setitem__("queue", calls["queue"] + 1),
    )

    import ap.manual_close_reconciliation as manual

    monkeypatch.setattr(manual, "_finalize_position", lambda **kwargs: False)
    guard._install_manual_finalizer_patch()

    ok = manual._finalize_position(
        finalizer=lambda **kwargs: False,
        client_id=CLIENT,
        position_id=POSITION_ID,
        contract="NOW260828P00122000",
        evidence={
            "broker_order_id": BROKER_EXIT_ID,
            "broker_order_ids": [BROKER_EXIT_ID],
            "fill_price": 1.22,
            "filled_qty": 1,
            "filled_ts": "2026-08-25T18:19:38.780000+00:00",
        },
    )

    assert ok is False
    assert calls == {"proof": 0, "queue": 0}


def test_successful_manual_finalizer_stamps_proof_and_queue(monkeypatch):
    calls = {"proof": [], "queue": []}
    monkeypatch.setattr(
        guard,
        "_persist_manual_close_proof_truth",
        lambda **kwargs: calls["proof"].append(kwargs) or 1,
    )
    monkeypatch.setattr(
        guard,
        "_terminalize_stale_queue_after_manual_close",
        lambda **kwargs: calls["queue"].append(kwargs) or 1,
    )

    import ap.manual_close_reconciliation as manual

    monkeypatch.setattr(
        guard,
        "_external_exit_identity",
        lambda *a, **k: {
            "local_order_id": EXTERNAL_LOCAL_ID,
            "broker_order_id": BROKER_EXIT_ID,
        },
    )
    # Remove any previous test/install wrapper before installing this test wrapper.
    monkeypatch.setattr(manual, "_finalize_position", lambda **kwargs: True)
    guard._install_manual_finalizer_patch()

    ok = manual._finalize_position(
        finalizer=lambda **kwargs: True,
        client_id=CLIENT,
        position_id=POSITION_ID,
        contract="NOW260828P00122000",
        evidence={
            "broker_order_id": BROKER_EXIT_ID,
            "broker_order_ids": [BROKER_EXIT_ID],
            "fill_price": 1.22,
            "filled_qty": 1,
            "filled_ts": "2026-08-25T18:19:38.780000+00:00",
        },
    )

    assert ok is True
    assert len(calls["proof"]) == 1
    assert calls["proof"][0]["external_local_order_id"] == EXTERNAL_LOCAL_ID
    assert len(calls["queue"]) == 1
    assert calls["queue"][0]["client_id"] == CLIENT
    assert calls["queue"][0]["position_id"] == POSITION_ID
    assert calls["queue"][0]["broker_exit_order_id"] == BROKER_EXIT_ID


def test_restart_pass0_repairs_downstream_truth_after_proof_binding(monkeypatch):
    import ap.manual_close_reconciliation as manual

    class _PM:
        def repair_terminal_proof_from_persisted(self, position_id, **kwargs):
            return True, "proof_already_bound"

    runner = types.SimpleNamespace(
        email=CLIENT,
        mode="LIVE",
        broker=None,
        position_manager=_PM(),
        core=types.SimpleNamespace(exit_eng=None),
        _last_manual_close_check_ts=0.0,
    )
    fill = {
        "broker_order_id": BROKER_EXIT_ID,
        "filled_qty": 1,
        "fill_price": 1.22,
        "filled_at": datetime(2026, 8, 25, 18, 19, 38, tzinfo=timezone.utc),
        "fill_timestamp_source": manual.BROKER_FILL_TIMESTAMP_SOURCE,
        "fill_timestamp_key": "last_fill_date",
        "raw_status": "EXIT_FILLED",
        "raw_side": "sell_to_close",
        "db_contract": "NOW260828P00122000",
        "db_direction": "CALL",
    }
    monkeypatch.setattr(
        manual,
        "load_manual_close_state",
        lambda cid, mode: ([], set(), {POSITION_ID: [fill]}),
    )
    monkeypatch.setattr(
        manual,
        "load_terminal_recovery_candidates",
        lambda cid, mode: [{
            "id": POSITION_ID,
            "client_id": CLIENT,
            "execution_mode": "live",
            "contract": "NOW260828P00122000",
            "side": "CALL",
            "direction": "CALL",
            "qty": 1,
            "quantity_remaining": 0,
            "entry_ts": "2026-08-25T18:00:00+00:00",
        }],
    )
    calls = []
    monkeypatch.setattr(
        manual,
        "_recover_manual_close_downstream_truth",
        lambda **kwargs: calls.append(kwargs),
    )
    monkeypatch.setattr(
        manual.time,
        "time",
        lambda: datetime(2026, 8, 25, 18, 20, tzinfo=timezone.utc).timestamp(),
    )

    manual.detect_manual_closes(runner)

    assert calls == [{
        "client_id": CLIENT,
        "position_id": POSITION_ID,
        "broker_exit_order_id": BROKER_EXIT_ID,
        "execution_mode": "live",
    }]


def test_schema_attestation_and_migration_cover_guard_sql_contract():
    from ap.schema_attestation import REQUIRED_SCHEMA

    assert {"id", "execution_mode", "exit_local_order_id"}.issubset(
        REQUIRED_SCHEMA["proof_trades"]
    )
    assert {"direction", "execution_mode"}.issubset(REQUIRED_SCHEMA["positions"])
    assert "filled_ts" in REQUIRED_SCHEMA["orders"]
    assert "finished_ts" in REQUIRED_SCHEMA["trade_queue"]

    migration = (
        Path(__file__).resolve().parents[1]
        / "migrations"
        / "20260828_manual_close_downstream_truth.sql"
    ).read_text()
    assert "exit_local_order_id" in migration
    assert "filled_ts" in migration
    assert "finished_ts" in migration
    from ap.migration_runner import _top_level_transaction_control

    assert _top_level_transaction_control(migration) == []


def test_guard_is_registered_as_required_when_present():
    import ap.trade_lifecycle_guards as lifecycle

    entry = next(row for row in lifecycle._GUARDS if row[0] == "manual_close_downstream_truth")
    assert entry == (
        "manual_close_downstream_truth",
        "ap.manual_close_truth_guard",
        "install_manual_close_truth_guard",
        True,
    )
