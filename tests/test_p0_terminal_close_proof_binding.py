import os
from pathlib import Path

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

import ap.db as db_mod
import ap.position_manager as pm_mod
import ap.queue as queue_mod
import ap_proof_logger as proof_mod
from ap.position_manager import APPositionManager


REPO = Path(__file__).resolve().parents[1]
PM_SRC = (REPO / "ap" / "position_manager.py").read_text()
RUNNER_SRC = (REPO / "client_runner.py").read_text()


class _FakeConn:
    def __init__(self, handler):
        self._handler = handler
        self.calls = []
        self._rows = []

    def __call__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        self._rows = list(self._handler(sql, params) or [])
        return self

    def fetchone(self):
        return dict(self._rows[0]) if self._rows else None

    def fetchall(self):
        return [dict(r) for r in self._rows]


def _func_body(src: str, name: str) -> str:
    start = src.find(f"def {name}(")
    assert start != -1, f"missing function: {name}"
    end = src.find("\n    def ", start + 1)
    if end == -1:
        end = len(src)
    return src[start:end]


def test_same_position_id_under_other_client_cannot_suppress_insertion(monkeypatch):
    pm = APPositionManager("client@example.com")
    fake = _FakeConn(
        lambda sql, params: (
            [{"id": 1}]
            if params == ("other@example.com", "POS-1")
            else []
        )
    )
    monkeypatch.setattr(pm_mod, "conn", fake)
    monkeypatch.setattr(pm_mod, "run_with_retry", lambda fn: fn())

    assert pm._proof_row_exists(position_id="POS-1") is False
    assert fake.calls[0][1] == ("client@example.com", "POS-1")


def test_same_local_order_id_under_other_client_cannot_suppress_insertion(monkeypatch):
    pm = APPositionManager("client@example.com")
    fake = _FakeConn(
        lambda sql, params: (
            [{"id": 1}]
            if params == ("other@example.com", "ENTRY-1")
            else []
        )
    )
    monkeypatch.setattr(pm_mod, "conn", fake)
    monkeypatch.setattr(pm_mod, "run_with_retry", lambda fn: fn())

    assert pm._proof_row_exists(local_order_id="ENTRY-1") is False
    assert fake.calls[0][1] == ("client@example.com", "ENTRY-1")


def test_paper_proof_cannot_become_live_through_runtime_mode(monkeypatch):
    pm = APPositionManager("client@example.com")
    captured = {}

    class _FakeProofLogger:
        def __init__(self, *, supabase_client, client_email, mode):
            captured["mode"] = mode

        def log_trade(self, **kwargs):
            captured["kwargs"] = kwargs
            return {"_proof_persisted": True, "_proof_persistence_error": None}

    monkeypatch.setattr(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
        "local_order_id": local_order_id,
        "client_id": client_id,
        "execution_mode": "paper",
        "direction": "PUT",
        "signal_id": "",
    })
    monkeypatch.setattr(queue_mod, "_get_sb_client", lambda: object())
    monkeypatch.setattr(proof_mod, "APProofLogger", _FakeProofLogger)
    monkeypatch.setattr(pm, "_load_signal_side", lambda signal_id: None)

    assert pm._write_missing_terminal_proof(
        position_id="POS-1",
        local_order_id="ENTRY-1",
        contract="NVDA260821C00100000",
        underlying="NVDA",
        side="PUT",
        opened_at="2026-07-18T15:00:00+00:00",
        closed_at="2026-07-18T16:00:00+00:00",
        entry_option_price=2.5,
        exit_option_price=3.0,
        contracts=1,
        exit_reason="broker_exit_fill",
        option_pnl_pct=20.0,
        setup_status="broker_exit_fill",
        execution_mode="live",
        exit_fill_price=3.0,
    )
    assert captured["mode"] == "paper"
    assert captured["kwargs"]["execution_mode"] == "paper"
    assert captured["kwargs"]["synthetic_entry"] is False


def test_missing_side_cannot_become_call(monkeypatch):
    pm = APPositionManager("client@example.com")
    captured = {}

    class _FakeProofLogger:
        def __init__(self, *, supabase_client, client_email, mode):
            pass

        def log_trade(self, **kwargs):
            captured["kwargs"] = kwargs
            return {"_proof_persisted": True, "_proof_persistence_error": None}

    monkeypatch.setattr(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
        "local_order_id": local_order_id,
        "client_id": client_id,
        "execution_mode": "paper",
        "direction": "",
        "signal_id": "",
    })
    monkeypatch.setattr(queue_mod, "_get_sb_client", lambda: object())
    monkeypatch.setattr(proof_mod, "APProofLogger", _FakeProofLogger)
    monkeypatch.setattr(pm, "_load_signal_side", lambda signal_id: None)

    assert pm._write_missing_terminal_proof(
        position_id="POS-2",
        local_order_id="ENTRY-2",
        contract="TSLA260821P00100000",
        underlying="TSLA",
        side="",
        opened_at="2026-07-18T15:00:00+00:00",
        closed_at="2026-07-18T16:00:00+00:00",
        entry_option_price=1.5,
        exit_option_price=1.7,
        contracts=1,
        exit_reason="broker_exit_fill",
        option_pnl_pct=13.0,
        setup_status="broker_exit_fill",
        execution_mode="paper",
        exit_fill_price=1.7,
    )
    assert captured["kwargs"]["side"] == "UNKNOWN_QUARANTINED"
    assert captured["kwargs"]["synthetic_entry"] is True


def test_ambiguous_side_produces_quarantine(monkeypatch):
    pm = APPositionManager("client@example.com")
    monkeypatch.setattr(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
        "local_order_id": local_order_id,
        "client_id": client_id,
        "execution_mode": "paper",
        "direction": "PUT",
        "signal_id": "",
    })
    monkeypatch.setattr(pm, "_load_signal_side", lambda signal_id: None)

    resolved = pm._resolve_terminal_proof_identity(
        position_id="POS-3",
        local_order_id="ENTRY-3",
        side="CALL",
    )
    assert resolved["resolved_side"] == "UNKNOWN_QUARANTINED"
    assert resolved["side_quarantined"] is True


def test_exact_originating_entry_resolves_side_and_execution_mode(monkeypatch):
    pm = APPositionManager("client@example.com")
    monkeypatch.setattr(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
        "local_order_id": local_order_id,
        "client_id": client_id,
        "execution_mode": "paper",
        "direction": "PUT",
        "signal_id": "sig-1",
    })
    monkeypatch.setattr(pm, "_load_signal_side", lambda signal_id: "PUT")

    resolved = pm._resolve_terminal_proof_identity(
        position_id="POS-4",
        local_order_id="ENTRY-4",
        side="",
    )
    assert resolved["entry_local_order_id"] == "ENTRY-4"
    assert resolved["resolved_side"] == "PUT"
    assert resolved["resolved_execution_mode"] == "paper"
    assert resolved["side_quarantined"] is False


def test_duplicate_eligible_proof_rows_do_not_permit_arbitrary_mutation(monkeypatch):
    pm = APPositionManager("client@example.com")

    def _handler(sql, params):
        if "SELECT id, local_order_id" in sql:
            return [{"id": 1, "local_order_id": ""}, {"id": 2, "local_order_id": ""}]
        raise AssertionError("update must not run when candidates are ambiguous")

    fake = _FakeConn(_handler)
    monkeypatch.setattr(pm_mod, "conn", fake)
    monkeypatch.setattr(pm_mod, "run_with_retry", lambda fn: fn())

    assert pm._claim_recent_broker_repair_proof(
        position_id="POS-5",
        contract="AAPL260821C00100000",
        closed_at="2026-07-18T16:00:00+00:00",
        local_order_id="ENTRY-5",
    ) is False
    assert len(fake.calls) == 1


def test_broker_repair_binding_mutates_exactly_one_expected_row(monkeypatch):
    pm = APPositionManager("client@example.com")

    def _handler(sql, params):
        if "SELECT id, local_order_id" in sql:
            return [{"id": 7, "local_order_id": ""}]
        if "UPDATE proof_trades" in sql:
            return [{"id": 7}]
        return []

    fake = _FakeConn(_handler)
    monkeypatch.setattr(pm_mod, "conn", fake)
    monkeypatch.setattr(pm_mod, "run_with_retry", lambda fn: fn())

    assert pm._claim_recent_broker_repair_proof(
        position_id="POS-6",
        contract="MSFT260821C00100000",
        closed_at="2026-07-18T16:00:00+00:00",
        local_order_id="ENTRY-6",
    ) is True
    assert len(fake.calls) == 2
    assert fake.calls[1][1][3] == 7
    assert fake.calls[1][1][4] == "client@example.com"


def test_private_persistence_diagnostics_are_not_inserted_as_db_columns(monkeypatch):
    pm = APPositionManager("client@example.com")
    captured = {}

    class _FakeProofLogger:
        def __init__(self, *, supabase_client, client_email, mode):
            pass

        def log_trade(self, **kwargs):
            captured["kwargs"] = kwargs
            return {"_proof_persisted": True, "_proof_persistence_error": "hidden"}

    monkeypatch.setattr(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
        "local_order_id": local_order_id,
        "client_id": client_id,
        "execution_mode": "paper",
        "direction": "CALL",
        "signal_id": "",
    })
    monkeypatch.setattr(queue_mod, "_get_sb_client", lambda: object())
    monkeypatch.setattr(proof_mod, "APProofLogger", _FakeProofLogger)
    monkeypatch.setattr(pm, "_load_signal_side", lambda signal_id: None)

    assert pm._write_missing_terminal_proof(
        position_id="POS-7",
        local_order_id="ENTRY-7",
        contract="AMD260821C00100000",
        underlying="AMD",
        side="CALL",
        opened_at="2026-07-18T15:00:00+00:00",
        closed_at="2026-07-18T16:00:00+00:00",
        entry_option_price=1.2,
        exit_option_price=1.5,
        contracts=1,
        exit_reason="broker_exit_fill",
        option_pnl_pct=25.0,
        setup_status="broker_exit_fill",
    )
    assert "_proof_persisted" not in captured["kwargs"]
    assert "_proof_persistence_error" not in captured["kwargs"]


def test_manual_close_without_broker_exit_fill_does_not_create_normal_proof_performance():
    body = _func_body(RUNNER_SRC, "_detect_manual_closes")
    assert "allow_fallback_insert=False" in body
    assert "MANUAL_CLIENT_CLOSE_UNVERIFIED" in body


def test_expiry_cleanup_remains_non_fabricating():
    body = _func_body(PM_SRC, "close_expired_position")
    assert "allow_fallback_insert=False" in body
    assert "EXPIRED_CLOSE_PROOF_SKIPPED_NO_BROKER_TRUTH" in body
    assert "option_pnl_pct=-100.0" not in body
    assert "exit_option_price=0.0" in body


def test_terminal_close_scope_does_not_touch_submit_cancel_queue_or_watcher_paths():
    for forbidden in ("submit_order(", "cancel_order(", "trade_queue", "watcher_"):
        assert forbidden not in _func_body(PM_SRC, "_write_missing_terminal_proof")
        assert forbidden not in _func_body(PM_SRC, "_ensure_terminal_close_proof")
