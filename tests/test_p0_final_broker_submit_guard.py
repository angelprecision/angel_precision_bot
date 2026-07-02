from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test_final_submit_guard")
os.environ.setdefault("ENCRYPTION_KEY", "ap-final-submit-guard-2026")

from ap.execution import _final_submit_block_reason  # noqa: E402


REPO_ROOT = Path(__file__).resolve().parents[1]
EXEC_SRC = (REPO_ROOT / "ap" / "execution.py").read_text()


def _signal(**overrides):
    payload = {
        "underlying_at_signal": 201.15,
        "symbol": "AAPL",
        "direction": "CALL",
    }
    payload.update(overrides)
    return payload


def test_deferred_contract_cannot_broker_submit():
    reason = _final_submit_block_reason("DEFERRED:AAPL", 1.23, _signal())
    assert reason == "submit_blocked:deferred_contract_not_materialized"


def test_limit_price_point_zero_one_cannot_broker_submit():
    reason = _final_submit_block_reason("AAPL260717C00100000", 0.01, _signal())
    assert reason == "submit_blocked:limit_not_materialized"


def test_invalid_first_alias_but_valid_later_alias_passes():
    payload = _signal(
        underlying_at_signal="not-a-number",
        underlying_price=201.25,
    )
    reason = _final_submit_block_reason("AAPL260717C00100000", 1.05, payload)
    assert reason is None


def test_raw_underlying_passes_after_normalization():
    payload = _signal(underlying_at_signal=None, underlying=201.25)
    reason = _final_submit_block_reason("AAPL260717C00100000", 1.05, payload)
    assert reason is None


@pytest.mark.parametrize("alias_key", ["last", "close", "mark"])
def test_raw_quote_aliases_pass_after_normalization(alias_key):
    payload = _signal(underlying_at_signal=None, **{alias_key: 201.25})
    reason = _final_submit_block_reason("AAPL260717C00100000", 1.05, payload)
    assert reason is None


@pytest.mark.parametrize("alias_key", ["prior_day_close", "previous_close", "prev_close", "prior_close"])
def test_prior_close_aliases_do_not_satisfy_final_submit_proof(alias_key):
    payload = _signal(underlying_at_signal=None, **{alias_key: 201.25})
    reason = _final_submit_block_reason("AAPL260717C00100000", 1.05, payload)
    assert reason == "submit_blocked:metadata_invalid_zero_underlying"


@pytest.mark.parametrize("underlying_value", [None, 0.0])
def test_truly_missing_or_zero_underlying_still_blocks(underlying_value):
    payload = _signal()
    if underlying_value is None:
        payload.pop("underlying_at_signal", None)
    else:
        payload["underlying_at_signal"] = underlying_value
    reason = _final_submit_block_reason("AAPL260717C00100000", 1.05, payload)
    assert reason == "submit_blocked:metadata_invalid_zero_underlying"


def test_blocked_submit_preserves_null_broker_order_id_in_update_path():
    idx = EXEC_SRC.find("block_reason = _final_submit_block_reason(")
    assert idx > 0
    window = EXEC_SRC[idx:idx + 1800]
    assert 'update_order(local_order_id, status="REJECTED", last_error=block_reason)' in window
    assert "broker_order_id=broker_order_id" not in window
    assert '"ORDER_SUBMIT_BLOCKED"' in window
    assert '"error": "submit_blocked"' in window
    assert "positions" not in window
    assert "proof_trades" not in window


def test_blocked_submit_preserves_client_id_execution_mode_and_payload():
    payload = _signal(client_id="client@example.com", execution_mode="live")
    before = deepcopy(payload)
    reason = _final_submit_block_reason("DEFERRED:AAPL", 1.05, payload)
    assert reason == "submit_blocked:deferred_contract_not_materialized"
    assert payload == before


def test_valid_occ_contract_with_real_limit_continues_normal_path():
    reason = _final_submit_block_reason("AAPL260717C00100000", 1.05, _signal())
    assert reason is None


def test_guard_is_ordered_before_broker_submit_and_after_kill_recheck():
    idx_kill = EXEC_SRC.find('return {"ok": False, "error": "killed_before_submit"}')
    idx_guard = EXEC_SRC.find("block_reason = _final_submit_block_reason(")
    idx_submit = EXEC_SRC.rfind("_submit_order_with_retry(")
    assert idx_kill > 0
    assert idx_guard > idx_kill
    assert idx_submit > idx_guard
