from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from ap import morning_handoff


CLIENT_ID = "startup@example.com"
LOCAL_ORDER_ID = "startup-local-1"
SIGNAL_ID = "startup-signal-1"
CANONICAL_SIGNAL_ID = "startup-canonical-1"
TRIGGERED_AT = "2026-08-13T15:00:00+00:00"


def _trigger_ready_row() -> dict:
    return {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "paper",
        "signal_id": SIGNAL_ID,
        "canonical_signal_id": CANONICAL_SIGNAL_ID,
        "plan_id": "startup-plan-1",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "symbol": "SPY",
        "contract": "DEFERRED:SPY",
        "direction": "CALL",
        "qty": 1,
        "limit_price": None,
        "reserved_cost": None,
        "score": 85,
        "tier": "A",
        "trigger_price": 100.0,
        "stop_underlying": 95.0,
        "target_underlying": 110.0,
        "pattern": "2-3-2",
        "timeframe": "5m",
        "broker_order_id": None,
        "submitted_ts": None,
        "filled_ts": None,
        "created_ts": datetime.now(timezone.utc),
        "meta": {
            "watcher_audit": {"reason_code": "trigger_ready"},
            "trigger_crossed_at": TRIGGERED_AT,
            "trigger_crossed_at_provenance": {
                "canonical_signal_id": CANONICAL_SIGNAL_ID,
                "client_id": CLIENT_ID,
                "execution_mode": "paper",
                "local_order_id": LOCAL_ORDER_ID,
            },
            "canonical_signal_id": CANONICAL_SIGNAL_ID,
            "trigger_price": 100.0,
            "observed_underlying_price": 101.0,
            "contract_deferred": True,
            "score": 85,
            "tier": "A",
            "timeframe": "5m",
        },
        "_tq_status": "WATCHING",
        "_tq_last_error": None,
    }


class _Connection:
    def __init__(self, row: dict):
        self.row = row
        self.rowcount = 0
        self.sql = ""

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, _params=None):
        self.sql = str(sql)
        if "UPDATE trade_queue" in self.sql:
            self.rowcount = 0

    def fetchall(self):
        if "FROM orders o" in self.sql:
            row = dict(self.row)
            row["meta"] = dict(self.row["meta"])
            return [row]
        return []


class _OSM:
    client_id = CLIENT_ID

    def __init__(self, row: dict):
        self.row = row
        self.claim_calls = []
        self.cancel_calls = []

    def get_order(self, local_order_id):
        if local_order_id != LOCAL_ORDER_ID:
            return None
        row = dict(self.row)
        row["meta"] = dict(self.row["meta"])
        return row

    def get_positions_for_order(self, local_order_id):
        assert local_order_id == LOCAL_ORDER_ID
        return []

    def claim_deferred_materialization(self, local_order_id, **kwargs):
        self.claim_calls.append((local_order_id, kwargs))
        if local_order_id != LOCAL_ORDER_ID:
            return False
        self.row["meta"].update(
            {
                "lifecycle_state": "MATERIALIZING",
                "materialization_status": "RUNNING",
                "materialization_in_flight": True,
                "materialization_owner": kwargs["owner"],
                "materialization_generation": kwargs["new_generation"],
                "materialization_lease_until": kwargs["lease_until"],
                "retry_attempt": kwargs["retry_attempt"],
                "breach_attempt_count": kwargs["retry_attempt"],
                "materialization_attempts": kwargs["retry_attempt"],
                "broker_ready": False,
            }
        )
        return True

    def update_order_meta(self, local_order_id, patch):
        if local_order_id != LOCAL_ORDER_ID:
            return False
        self.row["meta"].update(dict(patch or {}))
        return True

    def cancel_pending_entry(self, local_order_id, **kwargs):
        self.cancel_calls.append((local_order_id, kwargs))
        return False


class _Watcher:
    owner_token = "startup-watcher-token"

    def __init__(self, osm: _OSM):
        self.osm = osm
        self.callback_calls = []

    def has_order(self, local_order_id):
        return False

    def on_trigger(self, watched):
        self.callback_calls.append(watched)
        self.osm.row["meta"].update(
            {
                "lifecycle_state": "RETRY_WAIT",
                "materialization_status": "RETRY_PENDING",
                "materialization_in_flight": False,
                "materialization_next_retry_at": (
                    datetime.now(timezone.utc) + timedelta(seconds=30)
                ).isoformat(),
                "broker_ready": False,
            }
        )
        return {"disposition": "RETRY_WAIT"}


def test_startup_handoff_reaches_real_restart_engine_with_complete_row(monkeypatch):
    row = _trigger_ready_row()
    osm = _OSM(row)
    watcher = _Watcher(osm)
    broker = SimpleNamespace(list_orders=lambda: [])
    core = SimpleNamespace(
        broker=broker,
        exit_eng=object(),
        entry_watcher=watcher,
        client_id=CLIENT_ID,
        client_email=CLIENT_ID,
        mode="PAPER",
        execution_mode="PAPER",
    )
    master_control = SimpleNamespace(client_id=CLIENT_ID, mode="paper")
    runner = SimpleNamespace(
        email=CLIENT_ID,
        mode="PAPER",
        core=core,
        order_state_machine=osm,
        position_manager=object(),
        master_control=master_control,
    )

    import ap.db as ap_db
    import ap.watching_readiness as readiness

    monkeypatch.setattr(morning_handoff, "_load_handoff_run_lock", lambda **kwargs: None)
    monkeypatch.setattr(morning_handoff, "_upsert_handoff_run_lock", lambda **kwargs: None)
    monkeypatch.setattr(
        morning_handoff,
        "_count_state",
        lambda _client_id: {"watching_rows": 0, "new_rows": 0, "pending_trigger_rows": 1},
    )
    monkeypatch.setattr(
        morning_handoff,
        "enqueue_watching_signals_to_trade_queue",
        lambda **kwargs: {
            "errors": [],
            "inserted": [],
            "skipped_duplicate": [],
            "rejected": [],
            "signals_found": 0,
        },
    )
    monkeypatch.setattr(
        readiness,
        "run_watching_readiness_pass",
        lambda *args, **kwargs: {"ok": True, "errors": []},
    )
    monkeypatch.setattr(ap_db, "conn", lambda: _Connection(row))
    monkeypatch.setattr(ap_db, "run_with_retry", lambda fn, *args, **kwargs: fn())

    result = morning_handoff.run_morning_handoff_audit(
        client_id=CLIENT_ID,
        execution_mode="paper",
        stage="startup",
        dry_run=False,
        runner=runner,
    )

    assert result["ok"] is True
    assert len(osm.claim_calls) == 1
    assert len(watcher.callback_calls) == 1
    assert row["status"] == "PENDING_TRIGGER"
    assert row["meta"]["lifecycle_state"] == "RETRY_WAIT"
    assert row["meta"]["materialization_status"] == "RETRY_PENDING"
    assert osm.cancel_calls == []
    assert result["summary"]["orders_with_verified_owner"] == 1
