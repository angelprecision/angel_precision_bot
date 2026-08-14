"""Read-only release-evidence tests for PR #423."""

from __future__ import annotations

from contextlib import contextmanager
import os

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

import ap.exit_retry_liveness_preflight as preflight


class _Cursor:
    def __init__(self, *, hold: bool = False):
        self.hold = hold
        self.result = []

    def execute(self, statement, params=()):
        query = " ".join(str(statement).split()).upper()
        if "INFORMATION_SCHEMA.COLUMNS" in query:
            self.result = [{
                "data_type": "text" if self.hold else "jsonb",
                "udt_name": "text" if self.hold else "jsonb",
                "is_nullable": "YES" if self.hold else "NO",
                "column_default": None if self.hold else "'{}'::jsonb",
            }]
        elif "TO_REGCLASS" in query:
            self.result = [] if self.hold else [{"to_regclass": "schema_migrations"}]
        elif "SCHEMA_MIGRATIONS" in query:
            self.result = [] if self.hold else [{
                "filename": preflight.MIGRATION_FILENAME,
                "checksum": preflight._migration_checksum(),
                "applied_at": "2026-08-11T00:00:00+00:00",
                "baselined": False,
            }]
        elif "FROM PG_INDEXES" in query:
            self.result = [] if self.hold else [{
                "indexname": "orders_active_exit_unique",
                "indexdef": (
                    "CREATE UNIQUE INDEX orders_active_exit_unique ON public.orders "
                    "(client_id, position_id) WHERE kind='EXIT' AND status IN "
                    "('EXIT_REQUESTED','EXIT_SUBMITTED','EXIT_ACKNOWLEDGED','EXIT_PARTIAL_FILL')"
                ),
            }]
        elif "JOIN PUBLIC.ORDERS" in query:
            self.result = [] if not self.hold else [{
                "position_id": "pos-1",
                "client_id": "client-1",
                "execution_mode": "paper",
                "meta": {},
                "local_order_id": "loc-1",
                "broker_order_id": "bro-1",
                "order_position_id": "pos-1",
                "kind": "EXIT",
                "order_status": "EXIT_PARTIAL_FILL",
                "order_execution_mode": "paper",
                "filled_qty": 1,
                "qty": 2,
            }]
        elif "HAVING COUNT" in query:
            self.result = [] if not self.hold else [{
                "client_id": "client-1",
                "position_id": "pos-1",
                "active_exit_count": 2,
            }]
        elif "FROM PUBLIC.POSITIONS" in query:
            self.result = [{
                "id": "pos-1",
                "client_id": "client-1",
                "execution_mode": "paper",
                "status": "OPEN",
                "quantity_remaining": 2,
                "meta": (
                    {"exit_retry_liveness": {"state": "BROKEN"}}
                    if self.hold
                    else {
                        "exit_retry_liveness": {
                            "state": "NONE",
                            "replace_attempt": 0,
                            "replacement_generation": 0,
                            "replace_quantity": 0,
                            "last_ack_identity": "",
                        }
                    }
                ),
            }]
        else:  # pragma: no cover - keeps unexpected writes visible
            raise AssertionError(f"unexpected preflight SQL: {statement}")
        return self

    def fetchone(self):
        return self.result[0] if self.result else None

    def fetchall(self):
        return list(self.result)


def _patch_db(monkeypatch, *, hold: bool):
    cursor = _Cursor(hold=hold)

    @contextmanager
    def _conn():
        yield cursor

    monkeypatch.setattr(preflight, "conn", _conn)
    monkeypatch.setattr(
        preflight,
        "run_with_retry",
        lambda fn, *args, **kwargs: fn(*args, **kwargs),
    )
    monkeypatch.setattr(
        preflight,
        "attest_schema",
        lambda strict=True: {"ok": True, "skipped": False},
    )


def test_preflight_passes_only_with_schema_index_and_clean_rows(monkeypatch):
    _patch_db(monkeypatch, hold=False)

    result = preflight.run_preflight()

    assert result["status"] == "PASS"
    assert result["safe"] is True
    assert result["findings"] == []
    assert result["migration"]["checksum_match"] is True
    assert result["active_exit_unique_indexes"]
    assert result["broker_calls"] == 0
    assert result["writes"] == 0


def test_preflight_holds_missing_proof_and_ambiguous_partial_fill(monkeypatch):
    _patch_db(monkeypatch, hold=True)

    result = preflight.run_preflight()

    assert result["status"] == "HOLD"
    assert result["safe"] is False
    assert "POSITIONS_META_JSONB_NOT_PROVEN" in result["findings"]
    assert "MIGRATION_NOT_LEDGERED" in result["findings"]
    assert "ACTIVE_EXIT_UNIQUE_INDEX_NOT_PROVEN" in result["findings"]
    assert "MALFORMED_OR_MISMATCHED_EXIT_RETRY_LIFECYCLE" in result["findings"]
    assert "AMBIGUOUS_ACTIVE_PARTIAL_FILL_ROWS" in result["findings"]
    assert "MULTIPLE_ACTIVE_EXITS_PER_POSITION" in result["findings"]
    assert result["broker_calls"] == 0
    assert result["writes"] == 0


def test_active_exit_index_proof_requires_complete_status_predicate():
    complete = {
        "indexdef": (
            "CREATE UNIQUE INDEX orders_active_exit_unique ON public.orders "
            "(client_id, position_id) WHERE kind='EXIT' AND status IN "
            "('EXIT_REQUESTED','EXIT_SUBMITTED','EXIT_ACKNOWLEDGED','EXIT_PARTIAL_FILL')"
        )
    }
    subset = {
        "indexdef": complete["indexdef"].replace(
            ",'EXIT_SUBMITTED','EXIT_ACKNOWLEDGED','EXIT_PARTIAL_FILL'", ""
        )
    }

    assert preflight._index_is_active_exit_unique(complete) is True
    assert preflight._index_is_active_exit_unique(subset) is False


def test_partial_fill_preflight_requires_order_mode_to_match_position_mode():
    row = {
        "position_id": "pos-1",
        "client_id": "client-1",
        "execution_mode": "paper",
        "order_execution_mode": "live",
        "local_order_id": "loc-1",
        "broker_order_id": "bro-1",
        "filled_qty": 1,
        "meta": {
            "exit_fill_consumption": {
                "position_id": "pos-1",
                "client_id": "client-1",
                "execution_mode": "paper",
                "local_order_id": "loc-1",
                "broker_order_id": "bro-1",
                "replacement_generation": 0,
                "applied_cumulative_qty": 1,
            }
        },
    }

    result = preflight._classify_partial_fill(row)

    assert result["safe"] is False
    assert "PARTIAL_FILL_ORDER_EXECUTION_MODE_MISMATCH" in result["findings"]
