from __future__ import annotations

import inspect
import os
from datetime import datetime, timedelta, timezone

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://fake")

from ap.contract_selector import (
    SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    _new_selector_request_context,
)
from ap.order_state_machine import APOrderStateMachine
from ap.selector_retry_policy import (
    load_selector_recovery_cursor,
    new_selector_recovery_cursor,
    record_selector_recovery_attempt,
    record_selector_structural_skip,
    selector_symbol_may_retry,
)
from ap_execution_core import _selector_cursor_retry_block_reason


IDENTITY = {
    "local_order_id": "order-401",
    "client_id": "client-401",
    "execution_mode": "live",
    "signal_id": "signal-401",
    "materialization_generation": 3,
}


def _load(candidate=None, **overrides):
    kwargs = {
        **IDENTITY,
        "selector_attempt_count": 2,
        **overrides,
    }
    return load_selector_recovery_cursor(candidate, **kwargs)


def test_attempt_one_persists_and_attempt_two_continues():
    cursor = new_selector_recovery_cursor(
        **IDENTITY,
        selector_attempt_count=1,
    )
    cursor = record_selector_recovery_attempt(
        cursor,
        symbol="SPY260731C00500000",
        attempt_number=1,
        expiration="2026-07-31",
        result_reason="DELTA_OUT_OF_RANGE",
        transient=False,
    )
    loaded, error = _load(cursor)
    assert error is None
    assert loaded["selector_attempt_count"] == 2
    ctx = _new_selector_request_context(
        "SPY",
        "live",
        selector_request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
        recovery_attempt_number=2,
        recovery_cursor=loaded,
    )
    assert "SPY260731C00500000" in ctx.revalidated_contracts


def test_transient_symbol_waits_for_refresh_then_may_retry():
    now = datetime.now(timezone.utc)
    record = {
        "transient": True,
        "attempted_at": now.isoformat(),
    }
    assert selector_symbol_may_retry(
        record, refresh_seconds=20, now=now + timedelta(seconds=19)
    ) is False
    assert selector_symbol_may_retry(
        record, refresh_seconds=20, now=now + timedelta(seconds=20)
    ) is True


def test_terminal_symbol_never_retries():
    assert selector_symbol_may_retry(
        {
            "transient": False,
            "attempted_at": datetime.now(timezone.utc).isoformat(),
        },
        refresh_seconds=5,
        now=datetime.now(timezone.utc) + timedelta(days=1),
    ) is False


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("local_order_id", "other-order"),
        ("client_id", "other-client"),
        ("execution_mode", "paper"),
        ("signal_id", "other-signal"),
        ("materialization_generation", 99),
    ],
)
def test_identity_mismatch_never_reuses_cursor(field, bad):
    cursor = new_selector_recovery_cursor(
        **IDENTITY,
        selector_attempt_count=1,
    )
    cursor[field] = bad
    loaded, error = _load(cursor)
    assert error == f"IDENTITY_MISMATCH:{field}"
    assert loaded["attempted_symbols"] == {}


@pytest.mark.parametrize("candidate", ["bad", [], 7])
def test_malformed_cursor_resets_safely(candidate):
    loaded, error = _load(candidate)
    assert error == "MALFORMED_CURSOR"
    assert loaded["local_order_id"] == IDENTITY["local_order_id"]
    assert loaded["attempted_symbols"] == {}


def test_claim_transition_may_rebind_only_previous_generation():
    previous = new_selector_recovery_cursor(
        **{**IDENTITY, "materialization_generation": 2},
        selector_attempt_count=1,
    )
    loaded, error = _load(previous, allow_previous_generation=True)
    assert error is None
    assert loaded["materialization_generation"] == 3
    _, blocked = _load(previous, allow_previous_generation=False)
    assert blocked == "IDENTITY_MISMATCH:materialization_generation"


def test_cursor_is_bounded():
    cursor = new_selector_recovery_cursor(
        **IDENTITY,
        selector_attempt_count=1,
    )
    for index in range(240):
        cursor = record_selector_recovery_attempt(
            cursor,
            symbol=f"SPY260731C{index:08d}",
            attempt_number=1,
            expiration="2026-07-31",
            result_reason="DIRECT_QUOTE_ZERO_BID_ASK",
            transient=True,
        )
        cursor = record_selector_structural_skip(
            cursor,
            symbol=f"SPY260731P{index:08d}",
            skip_reason="STRUCTURAL_CLEARLY_UNAFFORDABLE",
        )
    assert len(cursor["attempted_symbols"]) == 200
    assert len(cursor["structurally_skipped_symbols"]) == 200


def test_cursor_survives_retry_but_clears_on_terminal_and_selection():
    schedule_source = inspect.getsource(
        APOrderStateMachine.schedule_deferred_materialization_retry
    )
    selection_source = inspect.getsource(
        APOrderStateMachine.persist_deferred_broker_ready
    )
    terminal_source = inspect.getsource(
        APOrderStateMachine.terminalize_materialization_retry
    )
    transition_source = inspect.getsource(APOrderStateMachine.transition)
    assert '"selector_recovery_cursor_v1"] = selector_recovery_cursor' in schedule_source
    assert '"selector_recovery_cursor_v1": None' in selection_source
    assert '"selector_recovery_cursor_v1": None' in terminal_source
    assert "selector_recovery_cursor_v1" in transition_source
    assert "OrderStatus.is_terminal(new_status)" in transition_source


def test_cursor_writer_is_exact_identity_and_owner_fenced():
    source = inspect.getsource(APOrderStateMachine.persist_selector_recovery_cursor)
    for fragment in (
        "local_order_id = %s",
        "client_id = %s",
        "signal_id = %s",
        "execution_mode",
        "materialization_owner",
        "materialization_generation",
        "broker_order_id IS NULL",
        "submitted_ts IS NULL",
    ):
        assert fragment in source


def test_recovery_context_does_not_inherit_across_clients():
    cursor = new_selector_recovery_cursor(
        **IDENTITY,
        selector_attempt_count=1,
    )
    cursor = record_selector_recovery_attempt(
        cursor,
        symbol="SPY260731C00500000",
        attempt_number=1,
        expiration="2026-07-31",
        result_reason="DELTA_OUT_OF_RANGE",
        transient=False,
    )
    fresh, error = _load(cursor, client_id="different")
    assert error == "IDENTITY_MISMATCH:client_id"
    assert fresh["attempted_symbols"] == {}


@pytest.mark.parametrize(
    ("candidate", "load_reason", "expected"),
    [
        (None, None, "MISSING_CURSOR_ON_RETRY"),
        ("bad", "MALFORMED_CURSOR", "MALFORMED_CURSOR"),
        ({}, "IDENTITY_MISMATCH:client_id", "IDENTITY_MISMATCH:client_id"),
        (
            {},
            "IDENTITY_MISMATCH:materialization_generation",
            "IDENTITY_MISMATCH:materialization_generation",
        ),
    ],
)
def test_retry_cursor_failure_blocks_before_selector(
    candidate, load_reason, expected
):
    selector_calls = 0
    reason = _selector_cursor_retry_block_reason(
        cursor_enabled=True,
        selector_attempt_number=2,
        cursor_candidate=candidate,
        cursor_load_reason=load_reason,
    )
    if reason is None:
        selector_calls += 1
    assert reason == expected
    assert selector_calls == 0


def test_cursor_kill_switch_allows_fresh_retry_without_cursor():
    assert _selector_cursor_retry_block_reason(
        cursor_enabled=False,
        selector_attempt_number=2,
        cursor_candidate=None,
        cursor_load_reason=None,
    ) is None
