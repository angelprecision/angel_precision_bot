from __future__ import annotations

from contextlib import contextmanager

import ap.manual_close_truth_guard as guard
from ap import db


CLIENT = "jasoncosby1@gmail.com"
POSITION_ID = "2fe10f52-e459-4bc5-a57a-1a80e3618040"
SIGNAL_ID = "f7dbe723-d342-406b-8650-fce428e9a0c0"
BROKER_EXIT_ID = "143318149"
EXTERNAL_LOCAL_ID = f"external-exit:{CLIENT}:{BROKER_EXIT_ID}"


class _Cursor:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.calls = []
        self.rowcount = 0
        self._one = None

    def execute(self, sql, params=None):
        text = " ".join(str(sql).split())
        self.calls.append((text, params))
        self.rowcount = 0
        if "FROM orders" in text and "external-exit:%" in text:
            self._one = {
                "local_order_id": EXTERNAL_LOCAL_ID,
                "broker_order_id": BROKER_EXIT_ID,
                "filled_ts": "2026-08-25T18:19:38.780000+00:00",
                "filled_qty": 1,
                "fill_price": 1.22,
            }
        elif "SELECT signal_id, execution_mode FROM positions" in text:
            self._one = {"signal_id": SIGNAL_ID, "execution_mode": "live"}
        elif "UPDATE trade_queue" in text:
            self.rowcount = 1
            self._one = None
        elif "UPDATE proof_trades" in text:
            self.rowcount = 1
            self._one = None
        else:
            self._one = None
        return self

    def fetchone(self):
        value = self._one
        self._one = None
        return value


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
    assert "UPPER(COALESCE(status,'')) = ANY(%s)" in sql
    assert CLIENT in params
    assert SIGNAL_ID in params
    states = params[-1]
    assert set(states) == {"NEW", "PROCESSING", "WATCHING", "ARMED"}
    assert "FILLED" not in states
    assert "SUBMITTED" not in states
    assert "DONE" not in states


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


def test_guard_is_registered_as_required_when_present():
    import ap.trade_lifecycle_guards as lifecycle

    entry = next(row for row in lifecycle._GUARDS if row[0] == "manual_close_downstream_truth")
    assert entry == (
        "manual_close_downstream_truth",
        "ap.manual_close_truth_guard",
        "install_manual_close_truth_guard",
        True,
    )
