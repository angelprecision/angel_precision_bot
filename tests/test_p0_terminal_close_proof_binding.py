import os
from pathlib import Path

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

import ap.position_manager as pm_mod
from ap.position_manager import APPositionManager


REPO = Path(__file__).resolve().parents[1]
PM_SRC = (REPO / "ap" / "position_manager.py").read_text()
RUNNER_SRC = (REPO / "client_runner.py").read_text()


def _func_body(src: str, name: str) -> str:
    start = src.find(f"def {name}(")
    assert start != -1, f"missing function: {name}"
    end = src.find("\n    def ", start + 1)
    if end == -1:
        end = len(src)
    return src[start:end]


def test_position_manager_routes_terminal_fallbacks_through_proof_logger():
    body = _func_body(PM_SRC, "_write_missing_terminal_proof")
    assert "APProofLogger" in body
    assert "INSERT INTO proof_trades" not in body


def test_runner_manual_close_uses_shared_terminal_proof_helper():
    body = _func_body(RUNNER_SRC, "_detect_manual_closes")
    assert "_ensure_terminal_close_proof(" in body
    assert "avg_fill, qty, side, local_order_id, entry_ts" in body


def test_proof_row_exists_returns_none_when_lookup_errors(monkeypatch):
    pm = APPositionManager("client@example.com")

    def _boom(_fn):
        raise RuntimeError("db down")

    monkeypatch.setattr(pm_mod, "run_with_retry", _boom)
    assert pm._proof_row_exists(position_id="POS-1") is None


def test_close_position_from_exit_fill_uses_shared_terminal_helper(monkeypatch):
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
        "local_order_id": "EXIT-1",
    }
    run_results = iter([(True, detail), 0])
    helper_calls = []

    monkeypatch.setattr(pm_mod, "run_with_retry", lambda _fn: next(run_results))
    monkeypatch.setattr(pm, "_ensure_terminal_close_proof", lambda **kwargs: helper_calls.append(kwargs) or True)

    assert pm.close_position_from_exit_fill(
        position_id="POS-1",
        filled_qty=2,
        exit_price=4.2,
        exit_reason="manual_test",
    )
    assert len(helper_calls) == 1
    assert helper_calls[0]["allow_fallback_insert"] is True
    assert helper_calls[0]["missing_reason_code"] == "BROKER_TRUTH_CLOSE_PROOF_WRITE_FAILED"
    assert helper_calls[0]["exit_fill_price"] == 4.2


def test_close_expired_position_does_not_fabricate_proof_row(monkeypatch):
    pm = APPositionManager("client@example.com")
    detail = {
        "result": "expired",
        "contract": "TSLA260821P00100000",
        "underlying": "TSLA",
        "side": "PUT",
        "opened_at": "2026-07-18T15:00:00+00:00",
        "closed_at": "2026-07-18T16:00:00+00:00",
        "entry_option_price": 2.15,
        "contracts": 1,
        "local_order_id": "EXIT-EXP-1",
    }
    helper_calls = []

    monkeypatch.setattr(pm_mod, "run_with_retry", lambda _fn: (True, detail))
    monkeypatch.setattr(pm, "_ensure_terminal_close_proof", lambda **kwargs: helper_calls.append(kwargs) or False)

    assert pm.close_expired_position(position_id="POS-EXP-1")
    assert len(helper_calls) == 1
    assert helper_calls[0]["allow_fallback_insert"] is False
    assert helper_calls[0]["missing_reason_code"] == "EXPIRED_CLOSE_PROOF_SKIPPED_NO_BROKER_TRUTH"
    assert helper_calls[0]["option_pnl_pct"] == 0.0
