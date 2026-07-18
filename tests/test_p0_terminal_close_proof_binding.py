import os
import types
from pathlib import Path

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

import pytest

import ap.db as db_mod
import ap.position_manager as pm_mod
import ap.queue as queue_mod
import ap.proof_taxonomy_guard as guard_mod
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
        "kind": "ENTRY",
        "execution_mode": "paper",
        "direction": "PUT",
        "signal_id": "",
        "meta": {
            "pattern": "ORB",
            "timeframe": "5m",
            "tier": "B",
            "score": 72,
            "context_score": 61,
        },
    })
    monkeypatch.setattr("ap.queue._get_sb_client", lambda: object())
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
    assert captured["kwargs"]["pattern"] == "ORB"
    assert captured["kwargs"]["timeframe"] == "5m"
    assert captured["kwargs"]["tier"] == "B"
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
        "kind": "ENTRY",
        "execution_mode": "paper",
        "direction": "",
        "signal_id": "",
    })
    monkeypatch.setattr("ap.queue._get_sb_client", lambda: object())
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
    assert "TERMINAL_METADATA_QUARANTINED" in captured["kwargs"]["setup_status"]


def test_ambiguous_side_produces_quarantine(monkeypatch):
    pm = APPositionManager("client@example.com")
    monkeypatch.setattr(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
        "local_order_id": local_order_id,
        "client_id": client_id,
        "kind": "ENTRY",
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
        "kind": "ENTRY",
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
        if "SELECT id" in sql and "FROM proof_trades" in sql:
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
        execution_mode="paper",
        side="CALL",
        contracts=1,
        entry_option_price=1.25,
    ) is False
    assert len(fake.calls) == 1


def test_broker_repair_binding_mutates_exactly_one_expected_row(monkeypatch):
    pm = APPositionManager("client@example.com")

    def _handler(sql, params):
        if "SELECT id" in sql and "FROM proof_trades" in sql:
            return [{"id": 7}]
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
        execution_mode="paper",
        side="CALL",
        contracts=1,
        entry_option_price=1.25,
    ) is True
    assert len(fake.calls) == 2
    assert fake.calls[1][1][3] == 7
    assert fake.calls[1][1][4] == "client@example.com"
    assert fake.calls[1][1][6] == "ENTRY-6"
    assert fake.calls[1][1][7] == "paper"
    assert fake.calls[1][1][8] == "CALL"


def test_local_order_only_existing_repair_proof_must_be_claimed_before_success(monkeypatch):
    pm = APPositionManager("client@example.com")
    calls = {"claim": 0, "insert": 0}
    monkeypatch.setattr(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
        "local_order_id": local_order_id,
        "client_id": client_id,
        "kind": "ENTRY",
        "execution_mode": "paper",
        "direction": "CALL",
        "signal_id": "",
    })
    monkeypatch.setattr(pm, "_proof_row_binding_state", lambda **kwargs: "local_order_only")

    def _claim(**kwargs):
        calls["claim"] += 1
        assert kwargs["local_order_id"] == "ENTRY-REPAIR"
        assert kwargs["position_id"] == "POS-REPAIR"
        return True

    monkeypatch.setattr(pm, "_claim_recent_broker_repair_proof", _claim)
    monkeypatch.setattr(pm, "_write_missing_terminal_proof", lambda **kwargs: calls.__setitem__("insert", calls["insert"] + 1) or True)

    assert pm._ensure_terminal_close_proof(
        position_id="POS-REPAIR",
        local_order_id="ENTRY-REPAIR",
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
        allow_fallback_insert=True,
        missing_reason_code="BROKER_TRUTH_CLOSE_PROOF_WRITE_FAILED",
    ) is True
    assert calls == {"claim": 1, "insert": 0}


def test_local_order_only_nonrepair_proof_does_not_allow_duplicate_fallback(monkeypatch):
    pm = APPositionManager("client@example.com")
    calls = {"insert": 0}
    monkeypatch.setattr(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
        "local_order_id": local_order_id,
        "client_id": client_id,
        "kind": "ENTRY",
        "execution_mode": "paper",
        "direction": "CALL",
        "signal_id": "",
    })
    monkeypatch.setattr(pm, "_proof_row_binding_state", lambda **kwargs: "local_order_only")
    monkeypatch.setattr(pm, "_claim_recent_broker_repair_proof", lambda **kwargs: False)
    monkeypatch.setattr(pm, "_write_missing_terminal_proof", lambda **kwargs: calls.__setitem__("insert", calls["insert"] + 1) or True)

    assert pm._ensure_terminal_close_proof(
        position_id="POS-NONREPAIR",
        local_order_id="ENTRY-NONREPAIR",
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
        allow_fallback_insert=True,
        missing_reason_code="BROKER_TRUTH_CLOSE_PROOF_WRITE_FAILED",
    ) is False
    assert calls["insert"] == 0


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
        "kind": "ENTRY",
        "execution_mode": "paper",
        "direction": "CALL",
        "signal_id": "",
        "meta": {
            "pattern": "PULLBACK",
            "timeframe": "15m",
            "tier": "A+",
            "score": 88,
            "context_score": 77,
        },
    })
    monkeypatch.setattr("ap.queue._get_sb_client", lambda: object())
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
    assert captured["kwargs"]["pattern"] == "PULLBACK"
    assert captured["kwargs"]["tier"] == "A+"


def test_terminal_metadata_quarantine_forces_training_ineligible_stamp(monkeypatch):
    persisted = {}
    initial_payload = {}

    class _Proof:
        email = "client@example.com"

    def _original(
        self,
        *,
        position_id="",
        local_order_id="",
        setup_status="",
        execution_mode="",
        synthetic_entry=False,
    ):
        initial_payload.update(
            position_id=position_id,
            local_order_id=local_order_id,
            setup_status=setup_status,
            execution_mode=execution_mode,
            synthetic_entry=synthetic_entry,
        )
        return {
            "_proof_persisted": True,
            "position_id": position_id,
            "local_order_id": local_order_id,
            "setup_status": setup_status,
            "execution_mode": execution_mode,
            "synthetic_entry": synthetic_entry,
        }

    identity = types.SimpleNamespace(
        local_order_id="ENTRY-META",
        position_id="POS-META",
        execution_mode="live",
        synthetic_entry=False,
    )
    monkeypatch.setattr(guard_mod, "resolve_originating_entry_identity", lambda **kwargs: identity)
    monkeypatch.setattr(guard_mod, "_lifecycle_proof_stamp", lambda identity: {
        "execution_mode": "live",
        "official_live_performance_eligible": True,
        "performance_taxonomy": "LIVE_OFFICIAL",
        "training_eligible": True,
        "taxonomy_reason": "tradier_exit_proof_lock_passed",
    })
    monkeypatch.setattr(guard_mod, "_persist_stamp", lambda proof_logger, result, stamp: persisted.update(stamp))

    result = guard_mod.wrap_log_trade(_original)(
        _Proof(),
        position_id="POS-META",
        local_order_id="ENTRY-META",
        setup_status="broker_exit_fill|TERMINAL_METADATA_QUARANTINED",
        execution_mode="live",
        synthetic_entry=False,
    )

    assert result["official_live_performance_eligible"] is False
    assert result["training_eligible"] is False
    assert result["synthetic_entry"] is True
    assert result["performance_taxonomy"] == "UNKNOWN_QUARANTINED"
    assert initial_payload["synthetic_entry"] is True
    assert persisted["taxonomy_reason"] == "terminal_fallback_strategy_metadata_unproven"


def test_manual_close_without_broker_exit_fill_does_not_create_normal_proof_performance():
    body = _func_body(RUNNER_SRC, "_detect_manual_closes")
    assert "allow_fallback_insert=False" in body
    assert "MANUAL_CLIENT_CLOSE_UNVERIFIED" in body


def test_exit_local_order_identity_is_refused(monkeypatch):
    pm = APPositionManager("client@example.com")
    calls = {"proof_exists": [], "claim": 0, "insert": 0}

    monkeypatch.setattr(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
        "local_order_id": local_order_id,
        "client_id": client_id,
        "kind": "EXIT",
        "execution_mode": "live",
        "direction": "CALL",
        "signal_id": "",
    })

    def _proof_exists(**kwargs):
        calls["proof_exists"].append(kwargs)
        assert kwargs == {"position_id": "POS-EXIT"}
        return False

    monkeypatch.setattr(pm, "_proof_row_exists", _proof_exists)
    monkeypatch.setattr(pm, "_claim_recent_broker_repair_proof", lambda **kwargs: calls.__setitem__("claim", calls["claim"] + 1) or True)
    monkeypatch.setattr(pm, "_write_missing_terminal_proof", lambda **kwargs: calls.__setitem__("insert", calls["insert"] + 1) or True)

    assert pm._ensure_terminal_close_proof(
        position_id="POS-EXIT",
        local_order_id="EXIT-LOCAL-1",
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
        allow_fallback_insert=True,
        missing_reason_code="BROKER_TRUTH_CLOSE_PROOF_WRITE_FAILED",
    ) is False
    assert calls["proof_exists"] == [{"position_id": "POS-EXIT"}]
    assert calls["claim"] == 0
    assert calls["insert"] == 0


def test_write_missing_terminal_proof_refuses_exit_local_order(monkeypatch):
    pm = APPositionManager("client@example.com")
    monkeypatch.setattr(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
        "local_order_id": local_order_id,
        "client_id": client_id,
        "kind": "EXIT",
        "execution_mode": "live",
        "direction": "CALL",
        "signal_id": "",
    })
    monkeypatch.setattr("ap.queue._get_sb_client", lambda: (_ for _ in ()).throw(AssertionError("proof logger path must not be reached")))

    assert pm._write_missing_terminal_proof(
        position_id="POS-EXIT-WRITE",
        local_order_id="EXIT-LOCAL-WRITE",
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
    ) is False


def test_no_entry_identity_cannot_bind_by_repair_time_window(monkeypatch):
    pm = APPositionManager("client@example.com")
    fake = _FakeConn(lambda sql, params: (_ for _ in ()).throw(AssertionError("db must not be read without entry identity")))
    monkeypatch.setattr(pm_mod, "conn", fake)
    monkeypatch.setattr(pm_mod, "run_with_retry", lambda fn: fn())

    assert pm._claim_recent_broker_repair_proof(
        position_id="POS-NO-ENTRY",
        contract="AMD260821C00100000",
        closed_at="2026-07-18T16:00:00+00:00",
        local_order_id="",
        execution_mode="paper",
        side="CALL",
        contracts=1,
        entry_option_price=1.2,
    ) is False
    assert fake.calls == []


def test_broker_repair_update_repeats_candidate_identity_predicates(monkeypatch):
    pm = APPositionManager("client@example.com")

    def _handler(sql, params):
        if "SELECT id" in sql and "FROM proof_trades" in sql:
            assert "closed_at BETWEEN" not in sql
            assert "local_order_id = %s" in sql
            assert "NULLIF(BTRIM(execution_mode), '')" in sql
            assert "NULLIF(BTRIM(mode), '')" in sql
            assert "UPPER(COALESCE(side, '')) = %s" in sql
            return [{"id": 10}]
        if "UPDATE proof_trades" in sql:
            assert "local_order_id = %s" in sql
            assert "NULLIF(BTRIM(execution_mode), '')" in sql
            assert "NULLIF(BTRIM(mode), '')" in sql
            assert "UPPER(COALESCE(side, '')) = %s" in sql
            return []
        return []

    fake = _FakeConn(_handler)
    monkeypatch.setattr(pm_mod, "conn", fake)
    monkeypatch.setattr(pm_mod, "run_with_retry", lambda fn: fn())

    assert pm._claim_recent_broker_repair_proof(
        position_id="POS-CAS",
        contract="AMD260821C00100000",
        closed_at="2026-07-18T16:00:00+00:00",
        local_order_id="ENTRY-CAS",
        execution_mode="live",
        side="CALL",
        contracts=1,
        entry_option_price=1.2,
    ) is False


def test_same_client_same_contract_paper_live_repair_rows_are_ambiguous_without_exact_mode(monkeypatch):
    pm = APPositionManager("client@example.com")

    def _handler(sql, params):
        if "SELECT id" in sql and "FROM proof_trades" in sql:
            assert params[3] == "live"
            return []
        if "UPDATE proof_trades" in sql:
            raise AssertionError("no exact mode match means no update")
        return []

    fake = _FakeConn(_handler)
    monkeypatch.setattr(pm_mod, "conn", fake)
    monkeypatch.setattr(pm_mod, "run_with_retry", lambda fn: fn())

    assert pm._claim_recent_broker_repair_proof(
        position_id="POS-LIVE",
        contract="SPY260821C00500000",
        closed_at="2026-07-18T16:00:00+00:00",
        local_order_id="ENTRY-LIVE",
        execution_mode="live",
        side="CALL",
        contracts=1,
        entry_option_price=2.0,
    ) is False


def test_fill_repair_uses_exact_identity_and_confirms_single_row(monkeypatch):
    pm = APPositionManager("client@example.com")
    detail = {
        "status": "CLOSED",
        "exit_price": 4.2,
        "filled_qty": 2,
        "remaining": 0,
        "realized_pnl": 84.0,
        "realized_pnl_pct": 21.0,
        "contract": "NVDA260821C00100000",
        "underlying": "NVDA",
        "side": "CALL",
        "opened_at": "2026-07-18T15:00:00+00:00",
        "closed_at": "2026-07-18T16:00:00+00:00",
        "entry_option_price": 3.47,
        "local_order_id": "ENTRY-8",
    }
    sql_calls = []

    def _run(fn):
        if not sql_calls:
            sql_calls.append(("position_update", ()))
            return True, detail
        return fn()

    def _handler(sql, params):
        sql_calls.append((sql, params))
        if "SELECT id" in sql and "FROM proof_trades" in sql:
            assert "position_id IS NULL" not in sql
            assert "NOW() - INTERVAL '2 hours'" not in sql
            assert params == ("client@example.com", "POS-8", "POS-8", "ENTRY-8", "ENTRY-8")
            return [{"id": 8}]
        if "UPDATE proof_trades" in sql:
            assert "RETURNING id" in sql
            return [{"id": 8}]
        return []

    fake = _FakeConn(_handler)
    monkeypatch.setattr(pm_mod, "run_with_retry", _run)
    monkeypatch.setattr(pm_mod, "conn", fake)
    monkeypatch.setattr(pm, "_ensure_terminal_close_proof", lambda **kwargs: True)

    assert pm.close_position_from_exit_fill(
        position_id="POS-8",
        filled_qty=2,
        exit_price=4.2,
        exit_reason="broker_exit_fill",
    )


def test_fill_repair_blocks_duplicate_exact_candidates(monkeypatch):
    pm = APPositionManager("client@example.com")
    detail = {
        "status": "CLOSED",
        "exit_price": 4.2,
        "filled_qty": 2,
        "remaining": 0,
        "realized_pnl": 84.0,
        "realized_pnl_pct": 21.0,
        "contract": "NVDA260821C00100000",
        "underlying": "NVDA",
        "side": "CALL",
        "opened_at": "2026-07-18T15:00:00+00:00",
        "closed_at": "2026-07-18T16:00:00+00:00",
        "entry_option_price": 3.47,
        "local_order_id": "ENTRY-9",
    }

    def _run(fn):
        if not hasattr(_run, "called"):
            _run.called = True
            return True, detail
        return fn()

    def _handler(sql, params):
        if "SELECT id" in sql and "FROM proof_trades" in sql:
            return [{"id": 8}, {"id": 9}]
        if "UPDATE proof_trades" in sql:
            raise AssertionError("duplicate candidates must not be updated")
        return []

    fake = _FakeConn(_handler)
    monkeypatch.setattr(pm_mod, "run_with_retry", _run)
    monkeypatch.setattr(pm_mod, "conn", fake)
    monkeypatch.setattr(pm, "_ensure_terminal_close_proof", lambda **kwargs: True)

    assert pm.close_position_from_exit_fill(
        position_id="POS-9",
        filled_qty=2,
        exit_price=4.2,
        exit_reason="broker_exit_fill",
    )


def _postgres_connection_or_skip():
    psycopg2 = pytest.importorskip("psycopg2")
    url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL")
    if not url:
        if os.getenv("GITHUB_ACTIONS") == "true":
            pytest.fail("INTELLIGENCE_POSTGRES_TEST_URL is required in GitHub Actions")
        pytest.skip("INTELLIGENCE_POSTGRES_TEST_URL not set")
    return psycopg2.connect(url)


class _RealPostgresConn:
    def __init__(self, connection):
        self.connection = connection
        self.cursor = None

    def __call__(self):
        return self

    def __enter__(self):
        import psycopg2.extras

        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type:
            self.connection.rollback()
        else:
            self.connection.commit()
        self.cursor.close()
        self.cursor = None
        return False

    def execute(self, sql, params=()):
        self.cursor.execute(sql, params)
        return self

    def fetchone(self):
        row = self.cursor.fetchone()
        return dict(row) if row else None

    def fetchall(self):
        return [dict(row) for row in self.cursor.fetchall()]


def test_postgres_broker_repair_claim_cardinality_and_cas(monkeypatch):
    db = _postgres_connection_or_skip()
    try:
        with db.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS proof_trades")
            cur.execute(
                """
                CREATE TABLE proof_trades (
                    id SERIAL PRIMARY KEY,
                    client_email TEXT NOT NULL,
                    position_id TEXT,
                    local_order_id TEXT,
                    execution_mode TEXT,
                    mode TEXT,
                    side TEXT,
                    contracts INTEGER,
                    entry_option_price NUMERIC
                )
                """
            )
            cur.execute(
                """
                INSERT INTO proof_trades (
                    client_email, position_id, local_order_id, execution_mode,
                    mode, side, contracts, entry_option_price
                )
                VALUES
                    ('client@example.com', 'broker-repair-client@example.com-AMD260821C00100000',
                     'ENTRY-PG-1', 'paper', 'paper', 'CALL', 1, 1.20),
                    ('client@example.com', 'broker-repair-client@example.com-AMD260821C00100000',
                     'ENTRY-PG-2', 'live', 'live', 'CALL', 1, 1.20)
                """
            )
        db.commit()

        pm = APPositionManager("client@example.com")
        monkeypatch.setattr(pm_mod, "conn", _RealPostgresConn(db))
        monkeypatch.setattr(pm_mod, "run_with_retry", lambda fn: fn())

        assert pm._claim_recent_broker_repair_proof(
            position_id="POS-PG-1",
            contract="AMD260821C00100000",
            closed_at="2026-07-18T16:00:00+00:00",
            local_order_id="ENTRY-PG-1",
            execution_mode="paper",
            side="CALL",
            contracts=1,
            entry_option_price=1.2,
        ) is True

        with db.cursor() as cur:
            cur.execute(
                "SELECT position_id FROM proof_trades WHERE local_order_id='ENTRY-PG-1'"
            )
            assert cur.fetchone()[0] == "POS-PG-1"

        assert pm._claim_recent_broker_repair_proof(
            position_id="POS-PG-2",
            contract="AMD260821C00100000",
            closed_at="2026-07-18T16:00:00+00:00",
            local_order_id="ENTRY-PG-2",
            execution_mode="paper",
            side="CALL",
            contracts=1,
            entry_option_price=1.2,
        ) is False

        with db.cursor() as cur:
            cur.execute(
                "SELECT position_id FROM proof_trades WHERE local_order_id='ENTRY-PG-2'"
            )
            assert cur.fetchone()[0] == "broker-repair-client@example.com-AMD260821C00100000"
    finally:
        db.close()


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
