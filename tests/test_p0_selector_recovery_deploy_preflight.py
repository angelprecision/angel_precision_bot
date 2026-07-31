from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone

import pytest

import ap.selector_recovery_deploy_preflight as preflight
from ap.selector_retry_policy import new_selector_recovery_cursor


LOCAL_ORDER_ID = "order-preflight-401"
CLIENT_ID = "client-preflight-401"
SIGNAL_ID = "signal-preflight-401"


def _row(
    *,
    lifecycle_state: str = "RETRY_WAIT",
    execution_mode: str = "live",
    retry_attempt: int | str | None = 1,
    generation: int | str | None = 1,
    cursor_marker="missing",
    materialization_attempts: int | None = None,
    materialization_current_attempt: int | None = None,
    materialization_owner: str = "materializer-owner",
) -> dict:
    meta = {
        "lifecycle_state": lifecycle_state,
        "materialization_status": (
            "RETRY_PENDING"
            if lifecycle_state == "RETRY_WAIT"
            else "RUNNING"
        ),
        "materialization_owner": (
            ""
            if lifecycle_state == "RETRY_WAIT"
            else materialization_owner
        ),
        "retry_attempt": retry_attempt,
        "materialization_generation": generation,
        "next_retry_at": datetime.now(timezone.utc).isoformat(),
    }

    if materialization_attempts is not None:
        meta["materialization_attempts"] = materialization_attempts

    if materialization_current_attempt is not None:
        meta["materialization_current_attempt"] = (
            materialization_current_attempt
        )

    if cursor_marker != "missing":
        meta["selector_recovery_cursor_v1"] = cursor_marker

    return {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": execution_mode,
        "signal_id": SIGNAL_ID,
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "updated_ts": datetime.now(timezone.utc).isoformat(),
        "meta": meta,
    }


def _valid_cursor(
    *,
    execution_mode: str = "live",
    generation: int = 1,
    attempt: int = 1,
) -> dict:
    return new_selector_recovery_cursor(
        local_order_id=LOCAL_ORDER_ID,
        client_id=CLIENT_ID,
        execution_mode=execution_mode,
        signal_id=SIGNAL_ID,
        materialization_generation=generation,
        selector_attempt_count=attempt,
    )


def test_retry_wait_attempt_one_without_cursor_blocks_next_attempt_two():
    hazard = preflight.classify_selector_recovery_hazard(
        _row(
            lifecycle_state="RETRY_WAIT",
            retry_attempt=1,
            generation=1,
        )
    )

    assert hazard is not None
    assert hazard["persisted_attempt"] == 1
    assert hazard["selector_attempt_number"] == 2
    assert hazard["reason"] == "MISSING_CURSOR_ON_RETRY"


def test_retry_wait_valid_previous_generation_cursor_is_safe():
    row = _row(
        lifecycle_state="RETRY_WAIT",
        retry_attempt=1,
        generation=1,
        cursor_marker=_valid_cursor(
            generation=1,
            attempt=1,
        ),
    )

    # Recovery will claim generation 2. The exact generation-1 cursor is
    # permitted only through load_selector_recovery_cursor's explicit
    # previous-generation allowance.
    assert preflight.classify_selector_recovery_hazard(row) is None


def test_materializing_attempt_two_without_cursor_is_unsafe():
    hazard = preflight.classify_selector_recovery_hazard(
        _row(
            lifecycle_state="MATERIALIZING",
            retry_attempt=2,
            generation=2,
        )
    )

    assert hazard is not None
    assert hazard["selector_attempt_number"] == 2
    assert hazard["reason"] == "MISSING_CURSOR_ON_RETRY"


def test_materializing_initial_attempt_without_cursor_is_allowed():
    row = _row(
        lifecycle_state="MATERIALIZING",
        retry_attempt=0,
        generation=1,
        materialization_attempts=None,
        materialization_current_attempt=None,
    )

    assert preflight.classify_selector_recovery_hazard(row) is None


def test_materializing_attempt_precedence_matches_execution_core():
    row = _row(
        lifecycle_state="MATERIALIZING",
        retry_attempt=0,
        generation=2,
        materialization_attempts=2,
    )

    hazard = preflight.classify_selector_recovery_hazard(row)

    assert hazard is not None
    assert hazard["attempt_source"] == "materialization_attempts"
    assert hazard["selector_attempt_number"] == 2
    assert hazard["reason"] == "MISSING_CURSOR_ON_RETRY"


def test_malformed_cursor_is_unsafe():
    hazard = preflight.classify_selector_recovery_hazard(
        _row(
            lifecycle_state="MATERIALIZING",
            retry_attempt=2,
            generation=2,
            cursor_marker="not-a-cursor",
        )
    )

    assert hazard is not None
    assert hazard["reason"] == "MALFORMED_CURSOR"


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        (
            lambda cursor: cursor.update(
                {"client_id": "different-client"}
            ),
            "IDENTITY_MISMATCH:client_id",
        ),
        (
            lambda cursor: cursor.update(
                {"execution_mode": "paper"}
            ),
            "IDENTITY_MISMATCH:execution_mode",
        ),
        (
            lambda cursor: cursor.update(
                {"signal_id": "different-signal"}
            ),
            "IDENTITY_MISMATCH:signal_id",
        ),
        (
            lambda cursor: cursor.update(
                {"materialization_generation": 99}
            ),
            "IDENTITY_MISMATCH:materialization_generation",
        ),
    ],
)
def test_cursor_identity_mismatch_is_unsafe(mutation, expected_reason):
    cursor = _valid_cursor(
        generation=2,
        attempt=2,
    )
    mutation(cursor)

    hazard = preflight.classify_selector_recovery_hazard(
        _row(
            lifecycle_state="MATERIALIZING",
            retry_attempt=2,
            generation=2,
            cursor_marker=cursor,
        )
    )

    assert hazard is not None
    assert hazard["reason"] == expected_reason


@pytest.mark.parametrize("execution_mode", ["live", "paper"])
def test_live_and_paper_are_both_inspected(execution_mode):
    hazard = preflight.classify_selector_recovery_hazard(
        _row(
            lifecycle_state="RETRY_WAIT",
            execution_mode=execution_mode,
            retry_attempt=1,
            generation=1,
        )
    )

    assert hazard is not None
    assert hazard["execution_mode"] == execution_mode
    assert hazard["reason"] == "MISSING_CURSOR_ON_RETRY"


def test_materializing_retry_requires_owner():
    row = _row(
        lifecycle_state="MATERIALIZING",
        retry_attempt=2,
        generation=2,
        cursor_marker=_valid_cursor(
            generation=2,
            attempt=2,
        ),
        materialization_owner="",
    )

    hazard = preflight.classify_selector_recovery_hazard(row)

    assert hazard is not None
    assert hazard["reason"] == "MISSING_MATERIALIZATION_OWNER"


def test_malformed_retry_counter_fails_closed():
    hazard = preflight.classify_selector_recovery_hazard(
        _row(
            lifecycle_state="RETRY_WAIT",
            retry_attempt="invalid",
            generation=1,
        )
    )

    assert hazard is not None
    assert hazard["reason"] == "INVALID_COUNTER:retry_attempt"


def test_sql_is_strictly_read_only_and_identity_fenced():
    sql = " ".join(
        preflight.ACTIVE_SELECTOR_RECOVERY_ROWS_SQL.upper().split()
    )

    assert sql.startswith("SELECT ")
    assert "FROM ORDERS" in sql
    assert "KIND" in sql
    assert "PENDING_TRIGGER" in sql
    assert "BROKER_ORDER_ID" in sql
    assert "SUBMITTED_TS IS NULL" in sql
    assert "EXECUTION_MODE" in sql
    assert "RETRY_WAIT" in sql
    assert "MATERIALIZING" in sql
    assert "SUBMIT_INTENT_AT" in sql
    assert "BROKER_READY" in sql

    for forbidden in (
        "UPDATE ",
        "INSERT ",
        "DELETE ",
        "ALTER ",
        "DROP ",
        "TRUNCATE ",
        "MERGE ",
    ):
        assert forbidden not in sql


def test_cli_returns_zero_for_clean_state(monkeypatch, capsys):
    monkeypatch.setattr(
        preflight,
        "_fetch_active_rows",
        lambda: [],
    )

    exit_code = preflight.main()
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload == {
        "status": "PASS",
        "unsafe_row_count": 0,
        "rows": [],
    }


def test_cli_returns_two_and_exact_rows_for_hazard(
    monkeypatch,
    capsys,
):
    monkeypatch.setattr(
        preflight,
        "_fetch_active_rows",
        lambda: [
            _row(
                lifecycle_state="RETRY_WAIT",
                retry_attempt=1,
                generation=1,
            )
        ],
    )

    exit_code = preflight.main()
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "FAIL"
    assert payload["unsafe_row_count"] == 1
    assert payload["rows"][0]["local_order_id"] == LOCAL_ORDER_ID
    assert (
        payload["rows"][0]["reason"]
        == "MISSING_CURSOR_ON_RETRY"
    )


def test_cli_returns_three_when_preflight_cannot_query(
    monkeypatch,
    capsys,
):
    def _raise():
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(
        preflight,
        "_fetch_active_rows",
        _raise,
    )

    exit_code = preflight.main()
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 3
    assert payload["status"] == "ERROR"
    assert payload["unsafe_row_count"] is None
    assert payload["rows"] == []
    assert "RuntimeError" in payload["error"]


def test_module_has_no_runtime_trade_or_mutation_authority():
    source = inspect.getsource(preflight)

    for forbidden in (
        "submit_existing_entry",
        "submit_entry",
        "cancel_order",
        "transition(",
        "update_order_meta",
        "persist_selector_recovery_cursor",
        "schedule_deferred_materialization_retry",
        "terminalize_deferred",
        "UPDATE orders",
        "INSERT INTO orders",
        "DELETE FROM orders",
    ):
        assert forbidden not in source
