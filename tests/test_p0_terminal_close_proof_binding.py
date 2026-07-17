from pathlib import Path


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


def test_position_manager_has_terminal_proof_helpers():
    assert "def _claim_recent_broker_repair_proof(" in PM_SRC
    assert "def _write_missing_terminal_proof(" in PM_SRC


def test_close_position_from_exit_fill_claims_or_inserts_missing_proof():
    body = _func_body(PM_SRC, "close_position_from_exit_fill")
    assert "_claim_recent_broker_repair_proof(" in body
    assert "_write_missing_terminal_proof(" in body
    assert "proof_trades inserted from broker-truth close" in body


def test_close_expired_position_emits_expiry_proof_row():
    body = _func_body(PM_SRC, "close_expired_position")
    assert "_write_missing_terminal_proof(" in body
    assert "option_pnl_pct=-100.0" in body
    assert "exit_option_price=0.0" in body
    assert "proof_trades inserted from contract expiry" in body


def test_manual_close_claims_broker_repair_proof():
    idx = RUNNER_SRC.find("def _detect_manual_closes(")
    assert idx != -1
    end = RUNNER_SRC.find("\n    def ", idx + 1)
    body = RUNNER_SRC[idx:end]
    assert "_claim_recent_broker_repair_proof(" in body
    assert "MANUAL_CLOSE_PROOF_UNCLAIMED" in body
