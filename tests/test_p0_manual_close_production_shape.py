import json
import os
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

import ap.db as db_mod
from ap import manual_close_reconciliation as manual_mod
from ap.utils import BROKER_FILL_TIMESTAMP_SOURCE_KEY


CLIENT = "jasoncosby1@gmail.com"
POSITION_ID = "ca06eeca-f55b-4778-8756-66c91bae877b"
CONTRACT = "F260731C00014000"


class _Cursor:
    def __init__(self):
        self.rows: list[dict] = []
        self._fetchall: list[dict] = []
        self._fetchone = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=()):
        compact = " ".join(str(sql).split())
        self._fetchall = []
        self._fetchone = None
        if "pg_advisory_xact_lock" in compact:
            return self
        if "WHERE client_id=%s AND broker_order_id=%s" in compact:
            client_id, broker_order_id = params
            self._fetchall = [
                row
                for row in self.rows
                if row["client_id"] == client_id
                and row["broker_order_id"] == broker_order_id
            ]
            return self
        if compact.startswith("INSERT INTO orders"):
            (
                client_id,
                local_order_id,
                broker_order_id,
                position_id,
                symbol,
                contract,
                direction,
                qty,
                filled_qty,
                fill_price,
                created_ts,
                updated_ts,
                submitted_ts,
                filled_ts,
                meta,
                execution_mode,
            ) = params
            row = {
                "client_id": client_id,
                "local_order_id": local_order_id,
                "broker_order_id": broker_order_id,
                "position_id": position_id,
                "kind": "EXIT",
                "status": "EXIT_FILLED",
                "symbol": symbol,
                "contract": contract,
                "direction": direction,
                "qty": qty,
                "filled_qty": filled_qty,
                "fill_price": fill_price,
                "created_ts": created_ts,
                "updated_ts": updated_ts,
                "submitted_ts": submitted_ts,
                "filled_ts": filled_ts,
                "meta": json.loads(meta),
                "execution_mode": execution_mode,
            }
            self.rows.append(row)
            self._fetchone = row
            return self
        if "WHERE local_order_id=%s" in compact:
            local_order_id = params[0]
            self._fetchall = [
                row for row in self.rows if row["local_order_id"] == local_order_id
            ]
            return self
        raise AssertionError(f"unexpected SQL: {compact}")

    def fetchall(self):
        return list(self._fetchall)

    def fetchone(self):
        return self._fetchone


def test_jason_production_position_shape_uses_direction_when_side_is_null(monkeypatch):
    position = {
        "id": POSITION_ID,
        "client_id": CLIENT,
        "underlying": "F",
        "contract": CONTRACT,
        "side": None,
        "direction": "CALL",
        "qty": 2,
        "quantity_remaining": None,
        "entry_ts": "2026-07-21T15:26:58.911238+00:00",
        "execution_mode": "live",
    }
    filled_at = datetime(2026, 7, 21, 15, 57, 39, tzinfo=timezone.utc)
    evidence = {
        "fills": [
            {
                "broker_order_id": "137780001",
                "filled_qty": 2,
                "fill_price": 0.75,
                "filled_at": filled_at,
                "raw_status": "filled",
                "raw_side": "sell_to_close",
            }
        ]
    }
    cursor = _Cursor()
    monkeypatch.setattr(db_mod, "conn", lambda: cursor)
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn: fn())

    assert manual_mod.position_direction(position) == "CALL"

    ok, reason = manual_mod.adopt_external_exit_fills(
        client_id=CLIENT,
        execution_mode="live",
        position=position,
        evidence=evidence,
    )

    assert ok is True
    assert reason == "external_exit_adoption_complete"
    assert len(cursor.rows) == 1
    assert cursor.rows[0]["direction"] == "CALL"
    assert cursor.rows[0]["position_id"] == POSITION_ID
    assert cursor.rows[0]["execution_mode"] == "live"
    assert cursor.rows[0]["meta"][BROKER_FILL_TIMESTAMP_SOURCE_KEY] == "broker_response"
