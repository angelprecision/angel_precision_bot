"""PR #386 hardening regressions.

Covers three amendment surfaces:

1. Position-manager idempotency classification (Fix 1):
     * TERMINAL + remaining <= 0  → idempotent True, {"idempotent": True}
     * TERMINAL + remaining > 0   → False `terminal_position_has_remaining_quantity`
     * !TERMINAL + remaining <= 0 → False `nonterminal_position_has_zero_remaining`
     * Uses PositionStatus.TERMINAL so CLOSED_REPAIR / STOPPED / TAKEN_PROFIT
       are correctly classified terminal.

2. Idempotent no-op contract (Fix 2):
     * Second call with different exit_price / broker_order_id returns True.
     * Second call does NOT run proof_trades repair.
     * Second call does NOT invoke _ensure_terminal_close_proof.
     * The first finalization's economic truth is preserved.

3. Durable-fill identity hardening (Fix 3):
     * Missing db_contract          → rejected.
     * Missing db_direction         → rejected.
     * Missing raw_status           → rejected.
     * Invalid position direction   → entire durable set rejected.
     * filled_at non-datetime shape → rejected.
"""
from __future__ import annotations

import os
import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

import ap.db as db_mod
import ap.position_manager as pm_mod
from ap import manual_close_reconciliation as manual_mod


CLIENT = "jason@example.com"
CONTRACT = "F260731C00014000"
POSITION_ID = "pos-386-idem-001"


# ─────────────────────────────────────────────────────────────────────────────
# Fake DB layer for exercising close_position_from_exit_fill under FOR UPDATE.
# One row per position; execute() routes SELECT/UPDATE against it.
# ─────────────────────────────────────────────────────────────────────────────

class _PMCursor:
    def __init__(self, positions: dict, columns_by_table: dict):
        self.positions = positions          # position_id → dict
        self.columns_by_table = columns_by_table
        self._fetchone = None
        self._fetchall: list[dict] = []
        self.proof_updates: list[tuple] = []
        self.proof_selects: list[tuple] = []
        self.terminal_proof_inserts: list[tuple] = []
        self._sql_history: list[tuple] = []

    def __enter__(self): return self
    def __exit__(self, *a): return False

    def execute(self, sql, params=()):
        s = " ".join(str(sql).split())
        self._sql_history.append((s, params))
        self._fetchone = None
        self._fetchall = []
        if s.startswith("SELECT * FROM positions WHERE id=%s AND client_id=%s FOR UPDATE"):
            pid, cid = params
            row = self.positions.get(pid)
            if row and row.get("client_id") == cid:
                self._fetchone = dict(row)
            return self
        if s.startswith("UPDATE positions SET"):
            pid, cid = params[-2], params[-1]
            row = self.positions.get(pid)
            if not row or row.get("client_id") != cid:
                self._fetchone = None
                return self
            # Parse SETs — apply the ones we care about.
            # For test purposes: mark status/quantity_remaining from tail of
            # params; the finalizer builds SETs dynamically, so we just
            # capture the row it emits by re-scanning SET clause tokens.
            # Simpler: apply whatever the finalizer computed by inspecting
            # its own values. We rebuild by reading the SET clause.
            # For our tests, we only need the returned row to reflect a
            # transition — the finalizer's own computation of new_status
            # is a formal parameter, so we just take the second-to-last
            # named field pattern: after 'status=%s' the value.
            # To avoid brittle SQL parsing, we return a canned row.
            self._fetchone = {"id": pid, "status": "CLOSED"}
            return self
        if "FROM proof_trades" in s and s.strip().startswith("SELECT"):
            self.proof_selects.append((s, params))
            self._fetchall = []
            return self
        if s.strip().startswith("UPDATE proof_trades"):
            self.proof_updates.append((s, params))
            self._fetchall = []
            return self
        # _ensure_terminal_close_proof issues its own SQL; capture any hits.
        self.terminal_proof_inserts.append((s, params))
        return self

    def fetchall(self): return list(self._fetchall)
    def fetchone(self): return self._fetchone


class _APM(pm_mod.APPositionManager):
    """Minimal subclass that bypasses __init__ and stubs column probes."""

    def __new__(cls, *args, **kwargs):
        obj = object.__new__(cls)
        return obj

    def __init__(self, client_id: str):
        self.client_id = client_id

    def _has_position_column(self, col: str) -> bool:
        return True

    def _ensure_terminal_close_proof(self, **kwargs):
        # Sentinel: record the call and return True so the lock-wrapper's
        # binding re-read is exercised.
        self._terminal_proof_calls = getattr(self, "_terminal_proof_calls", [])
        self._terminal_proof_calls.append(kwargs)
        return True

    # PR #386 blocker 3: bypass the advisory-lock wrapper in unit tests
    # so the fake DB cursor does not need to model pg_advisory_xact_lock or
    # proof-binding-state SQL. Integration coverage of the lock lives in
    # tests/test_p0_manual_close_terminal_recovery.py.
    def _with_terminal_proof_lock(self, position_id, fn):
        return fn()

    def _proof_row_binding_state(self, **kwargs):
        return "bound_position"


def _install_pm_db(monkeypatch, positions_by_id):
    cursor = _PMCursor(positions_by_id, columns_by_table={})
    monkeypatch.setattr(pm_mod, "conn", lambda: cursor)
    monkeypatch.setattr(pm_mod, "run_with_retry", lambda fn: fn())
    return cursor


def _open_position(**overrides):
    row = {
        "id": POSITION_ID,
        "client_id": CLIENT,
        "status": "OPEN",
        "avg_fill": 0.73,
        "qty": 2,
        "quantity_remaining": 2,
        "exit_price": 0,
        "realized_pnl": 0,
        "realized_pnl_pct": 0,
        "contract": CONTRACT,
        "underlying": "F",
        "side": "CALL",
        "entry_ts": "2026-07-21T15:26:58.911238+00:00",
        "local_order_id": "entry-1",
    }
    row.update(overrides)
    return row


# ═══ Fix 1: 4-way canonical classification ══════════════════════════════════

def test_terminal_status_with_zero_remaining_is_idempotent_success(monkeypatch):
    row = _open_position(status="CLOSED", quantity_remaining=0,
                          exit_price=0.90, realized_pnl=34.0, realized_pnl_pct=23.3,
                          execution_mode="paper", exit_ts="2026-07-21T16:30:00+00:00")
    _install_pm_db(monkeypatch, {POSITION_ID: row})
    apm = _APM(CLIENT)
    ok = apm.close_position_from_exit_fill(
        position_id=POSITION_ID, exit_price=1.50, filled_qty=2,
        broker_order_id="LATE-CALLER", exit_reason="late_call_should_noop",
    )
    assert ok is True
    # PR #386 amendment: idempotent path MUST run proof repair (once) using
    # PERSISTED values only — never the second caller's evidence.
    calls = getattr(apm, "_terminal_proof_calls", [])
    assert len(calls) == 1
    kw = calls[0]
    assert kw["exit_option_price"] == 0.90         # persisted, not 1.50
    assert kw["exit_fill_price"] == 0.90
    assert kw["option_pnl_pct"] == 23.3
    # Original row untouched
    assert row["exit_price"] == 0.90


def test_terminal_status_with_remaining_quantity_refuses_mutation(monkeypatch):
    row = _open_position(status="CLOSED", quantity_remaining=2)
    _install_pm_db(monkeypatch, {POSITION_ID: row})
    apm = _APM(CLIENT)
    ok = apm.close_position_from_exit_fill(
        position_id=POSITION_ID, exit_price=1.20, filled_qty=2,
        broker_order_id="INVARIANT-1",
    )
    assert ok is False
    assert getattr(apm, "_terminal_proof_calls", []) == []


def test_nonterminal_status_with_zero_remaining_refuses_mutation(monkeypatch):
    row = _open_position(status="OPEN", quantity_remaining=0)
    _install_pm_db(monkeypatch, {POSITION_ID: row})
    apm = _APM(CLIENT)
    ok = apm.close_position_from_exit_fill(
        position_id=POSITION_ID, exit_price=1.20, filled_qty=2,
        broker_order_id="INVARIANT-2",
    )
    assert ok is False
    assert getattr(apm, "_terminal_proof_calls", []) == []


@pytest.mark.parametrize(
    ("exit_price", "filled_qty"),
    [
        (True, 2),
        (0.90, True),
        (0.90, 2.5),
        (float("nan"), 2),
        (float("inf"), 2),
    ],
)
def test_finalizer_rejects_malformed_external_scalars_before_db_mutation(
    monkeypatch, exit_price, filled_qty,
):
    cursor = _install_pm_db(monkeypatch, {POSITION_ID: _open_position()})
    apm = _APM(CLIENT)

    ok = apm.close_position_from_exit_fill(
        position_id=POSITION_ID,
        exit_price=exit_price,
        filled_qty=filled_qty,
        external_close=True,
    )

    assert ok is False
    assert not any(
        sql.startswith("UPDATE positions SET") for sql, _ in cursor._sql_history
    )


@pytest.mark.parametrize(
    "owner_fields",
    [
        {"exit_in_flight": True},
        {"pending_exit_broker_order_id": "bot-exit-460"},
        {"pending_exit_local_order_id": "local-exit-460"},
    ],
)
def test_external_finalizer_rechecks_current_exit_owner_under_lock(
    monkeypatch, owner_fields,
):
    cursor = _install_pm_db(monkeypatch, {POSITION_ID: _open_position(**owner_fields)})
    apm = _APM(CLIENT)

    ok = apm.close_position_from_exit_fill(
        position_id=POSITION_ID,
        exit_price=0.90,
        filled_qty=2,
        external_close=True,
    )

    assert ok is False
    assert not any(
        sql.startswith("UPDATE positions SET") for sql, _ in cursor._sql_history
    )


def test_external_finalizer_rechecks_remaining_quantity_under_lock(monkeypatch):
    cursor = _install_pm_db(
        monkeypatch,
        {POSITION_ID: _open_position(quantity_remaining=1)},
    )
    apm = _APM(CLIENT)

    ok = apm.close_position_from_exit_fill(
        position_id=POSITION_ID,
        exit_price=0.90,
        filled_qty=2,
        external_close=True,
    )

    assert ok is False
    assert not any(
        sql.startswith("UPDATE positions SET") for sql, _ in cursor._sql_history
    )


def test_external_finalizer_rejects_invalid_occ_contract_under_lock(monkeypatch):
    cursor = _install_pm_db(
        monkeypatch,
        {POSITION_ID: _open_position(contract="not-an-occ-symbol")},
    )
    apm = _APM(CLIENT)

    ok = apm.close_position_from_exit_fill(
        position_id=POSITION_ID,
        exit_price=0.90,
        filled_qty=2,
        external_close=True,
    )

    assert ok is False
    assert not any(
        sql.startswith("UPDATE positions SET") for sql, _ in cursor._sql_history
    )


def test_closed_repair_stopped_taken_profit_are_terminal_classified(monkeypatch):
    for status in ("CLOSED_REPAIR", "STOPPED", "TAKEN_PROFIT"):
        row = _open_position(status=status, quantity_remaining=0,
                              exit_price=0.80, realized_pnl=10.0,
                              execution_mode="paper",
                              exit_ts="2026-07-21T16:30:00+00:00")
        _install_pm_db(monkeypatch, {POSITION_ID: row})
        apm = _APM(CLIENT)
        ok = apm.close_position_from_exit_fill(
            position_id=POSITION_ID, exit_price=1.50, filled_qty=2,
            broker_order_id=f"LATE-{status}",
        )
        assert ok is True, f"{status} should be terminal-idempotent success"
        # PR #386 amendment: idempotent path performs one proof-repair call.
        calls = getattr(apm, "_terminal_proof_calls", [])
        assert len(calls) == 1, f"{status} expected exactly one proof-repair call"
        assert calls[0]["exit_option_price"] == 0.80
        # Original economics preserved
        assert row["exit_price"] == 0.80


# ═══ Fix 2: idempotent caller must not rewrite proof ════════════════════════

def test_duplicate_finalizer_with_different_caller_evidence_does_not_rewrite_proof(monkeypatch):
    row = _open_position(status="CLOSED", quantity_remaining=0,
                          exit_price=0.90, realized_pnl=34.0, realized_pnl_pct=23.3,
                          execution_mode="paper",
                          exit_ts="2026-07-21T16:30:00+00:00")
    cursor = _install_pm_db(monkeypatch, {POSITION_ID: row})
    apm = _APM(CLIENT)

    ok = apm.close_position_from_exit_fill(
        position_id=POSITION_ID,
        exit_price=99.99,                        # deliberately wrong
        filled_qty=2,
        broker_order_id="SECOND-CALLER",         # deliberately different
        exit_reason="second_call_wrong_evidence",
    )

    assert ok is True
    # No proof_trades SELECT/UPDATE — the idempotent path skips the generic
    # proof repair block entirely (that block would use CALLER evidence).
    assert cursor.proof_selects == []
    assert cursor.proof_updates == []
    # PR #386 amendment: one canonical proof-repair call IS expected, but it
    # MUST use PERSISTED values — never the second caller's 99.99 / SECOND-
    # CALLER / second_call_wrong_evidence.
    calls = getattr(apm, "_terminal_proof_calls", [])
    assert len(calls) == 1
    kw = calls[0]
    assert kw["exit_option_price"] == 0.90        # persisted, not 99.99
    assert kw["exit_fill_price"] == 0.90
    assert kw["exit_reason"] != "second_call_wrong_evidence"
    # First finalization's economics are still on the row.
    assert row["exit_price"] == 0.90
    assert row["realized_pnl"] == 34.0


# ═══ Fix 3: durable-fill identity hardening ═════════════════════════════════

def _pos(**overrides):
    row = {
        "id": POSITION_ID,
        "contract": CONTRACT,
        "side": "CALL",
        "direction": "CALL",
        "entry_ts": "2026-07-21T15:26:58.911238+00:00",
    }
    row.update(overrides)
    return row


def _fill(**overrides):
    row = {
        "broker_order_id": "BROK-1",
        "filled_qty": 2,
        "fill_price": 0.75,
        "filled_at": datetime(2026, 7, 21, 15, 57, 39, tzinfo=timezone.utc),
        "fill_timestamp_source": manual_mod.BROKER_FILL_TIMESTAMP_SOURCE,
        "fill_timestamp_key": "last_fill_date",
        "created_at": None,
        "raw_status": "EXIT_FILLED",
        "raw_side": "sell_to_close",
        "db_contract": CONTRACT,
        "db_direction": "CALL",
    }
    row.update(overrides)
    return row


DETECTED_AT = datetime(2026, 7, 21, 15, 58, 0, tzinfo=timezone.utc)


def test_durable_fill_missing_db_contract_is_rejected():
    valid = manual_mod._validate_durable_fills(
        [_fill(db_contract="")],
        position=_pos(),
        detected_at=DETECTED_AT,
    )
    assert valid == []


def test_durable_fill_wrong_db_contract_is_rejected():
    valid = manual_mod._validate_durable_fills(
        [_fill(db_contract="X260731C00099000")],
        position=_pos(),
        detected_at=DETECTED_AT,
    )
    assert valid == []


def test_durable_fill_missing_db_direction_is_rejected():
    valid = manual_mod._validate_durable_fills(
        [_fill(db_direction="")],
        position=_pos(),
        detected_at=DETECTED_AT,
    )
    assert valid == []


def test_durable_fill_invalid_db_direction_is_rejected():
    valid = manual_mod._validate_durable_fills(
        [_fill(db_direction="BULLISH")],
        position=_pos(),
        detected_at=DETECTED_AT,
    )
    assert valid == []


def test_durable_fill_mismatched_direction_is_rejected():
    valid = manual_mod._validate_durable_fills(
        [_fill(db_direction="PUT")],
        position=_pos(side="CALL"),
        detected_at=DETECTED_AT,
    )
    assert valid == []


def test_durable_fill_missing_raw_status_is_rejected():
    valid = manual_mod._validate_durable_fills(
        [_fill(raw_status="")],
        position=_pos(),
        detected_at=DETECTED_AT,
    )
    assert valid == []


def test_durable_fill_wrong_raw_status_is_rejected():
    valid = manual_mod._validate_durable_fills(
        [_fill(raw_status="ENTRY_FILLED")],
        position=_pos(),
        detected_at=DETECTED_AT,
    )
    assert valid == []


def test_durable_fill_position_direction_invalid_rejects_entire_set():
    """When the position row itself has no valid CALL/PUT direction, no
    durable fill can be proved aligned — reject the whole set."""
    valid = manual_mod._validate_durable_fills(
        [_fill(), _fill(broker_order_id="BROK-2")],
        position=_pos(side=None, direction=""),
        detected_at=DETECTED_AT,
    )
    assert valid == []


def test_durable_fill_non_datetime_filled_at_is_rejected():
    for bad in (
        "2026-07-21T15:57:39Z",   # string
        1690000000,                # epoch int
        1690000000.5,              # epoch float
        None,                      # missing
    ):
        valid = manual_mod._validate_durable_fills(
            [_fill(filled_at=bad)],
            position=_pos(),
            detected_at=DETECTED_AT,
        )
        assert valid == [], f"filled_at={bad!r} should have been rejected"


def test_durable_fill_valid_row_is_accepted():
    """Positive control: a fully-valid fill passes."""
    valid = manual_mod._validate_durable_fills(
        [_fill()],
        position=_pos(),
        detected_at=DETECTED_AT,
    )
    assert len(valid) == 1
    assert valid[0]["broker_order_id"] == "BROK-1"


@pytest.mark.parametrize(
    "overrides",
    [
        {"fill_timestamp_source": ""},
        {"fill_timestamp_source": "broker_response", "fill_timestamp_key": "transaction_date"},
        {"fill_timestamp_source": "broker_response", "fill_timestamp_key": "update_date"},
        {"filled_at": datetime(2026, 7, 21, 15, 57, 39)},
    ],
)
def test_durable_fill_requires_broker_timestamp_provenance(overrides):
    valid = manual_mod._validate_durable_fills(
        [_fill(**overrides)],
        position=_pos(),
        detected_at=DETECTED_AT,
    )
    assert valid == []


def test_durable_fill_stale_timestamp_is_rejected():
    valid = manual_mod._validate_durable_fills(
        [_fill(filled_at=datetime(2026, 7, 21, 15, 0, 0, tzinfo=timezone.utc))],
        position=_pos(entry_ts="2026-07-21T15:26:58+00:00"),
        detected_at=DETECTED_AT,
    )
    assert valid == []


def test_durable_fill_far_future_timestamp_is_rejected():
    valid = manual_mod._validate_durable_fills(
        [_fill(filled_at=DETECTED_AT + timedelta(seconds=301))],
        position=_pos(),
        detected_at=DETECTED_AT,
    )
    assert valid == []
