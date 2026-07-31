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


_UNSET = object()


def _row(
    *,
    lifecycle_state: str = "RETRY_WAIT",
    materialization_status=_UNSET,
    execution_mode: str = "live",
    retry_attempt: int | str | None = 1,
    generation: int | str | None = 1,
    cursor_marker="missing",
    materialization_attempts: int | None = None,
    materialization_current_attempt: int | None = None,
    materialization_owner: str = "materializer-owner",
) -> dict:
    if materialization_status is _UNSET:
        status_default = (
            "RETRY_PENDING" if lifecycle_state == "RETRY_WAIT" else "RUNNING"
        )
    else:
        status_default = materialization_status
    meta = {
        "lifecycle_state": lifecycle_state,
        "materialization_status": status_default,
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


def test_retry_wait_previous_generation_cursor_is_unsafe_without_preclaim():
    # Runtime only allows a generation N-1 cursor through
    # load_selector_recovery_cursor(allow_previous_generation=True), and only
    # sets that flag when _recovery_pre_claimed is True — a transactional
    # startup-recovery claim that a READ-ONLY preflight cannot prove from
    # orders.meta. The preflight therefore fails closed on prior-generation
    # cursors and reports the row for operator review.
    row = _row(
        lifecycle_state="RETRY_WAIT",
        retry_attempt=1,
        generation=1,
        cursor_marker=_valid_cursor(
            generation=1,
            attempt=1,
        ),
    )
    hazard = preflight.classify_selector_recovery_hazard(row)
    assert hazard is not None
    assert hazard["reason"] == "IDENTITY_MISMATCH:materialization_generation"


def test_retry_wait_exact_next_generation_cursor_is_safe():
    # Positive control: a cursor whose stored generation exactly matches the
    # generation runtime will claim next (N+1 for RETRY_WAIT) IS safe under
    # the read-only preflight, because acceptance does not depend on the
    # preclaim flag.
    row = _row(
        lifecycle_state="RETRY_WAIT",
        retry_attempt=1,
        generation=1,
        cursor_marker=_valid_cursor(
            generation=2,
            attempt=2,
        ),
    )
    assert preflight.classify_selector_recovery_hazard(row) is None


def test_materializing_previous_generation_cursor_is_unsafe_without_preclaim():
    # Same rule for MATERIALIZING: runtime only accepts a gen N-1 cursor when
    # preclaim is proven. The preflight must not silently accept it.
    row = _row(
        lifecycle_state="MATERIALIZING",
        retry_attempt=2,
        generation=2,
        cursor_marker=_valid_cursor(
            generation=1,
            attempt=1,
        ),
    )
    hazard = preflight.classify_selector_recovery_hazard(row)
    assert hazard is not None
    assert hazard["reason"] == "IDENTITY_MISMATCH:materialization_generation"


def test_materializing_exact_current_generation_cursor_is_safe():
    # Positive control: exact-current-generation cursor is safe.
    row = _row(
        lifecycle_state="MATERIALIZING",
        retry_attempt=2,
        generation=2,
        cursor_marker=_valid_cursor(
            generation=2,
            attempt=2,
        ),
    )
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


# ─────────────────────────────────────────────────────────────────────────────
# Canonical-lifecycle regressions (reviewer follow-up).
# Runtime startup recovery treats materialization_status as an equal peer of
# lifecycle_state. A read-only preflight that only reads lifecycle_state would
# silently omit real recovery-eligible rows and print a false PASS.
# ─────────────────────────────────────────────────────────────────────────────

def test_missing_lifecycle_with_retry_pending_status_is_unsafe():
    row = _row(
        lifecycle_state="",
        materialization_status="RETRY_PENDING",
        retry_attempt=1,
        generation=1,
    )
    hazard = preflight.classify_selector_recovery_hazard(row)
    assert hazard is not None
    assert hazard["lifecycle_state"] == "RETRY_WAIT"
    assert hazard["selector_attempt_number"] == 2
    assert hazard["reason"] == "MISSING_CURSOR_ON_RETRY"


def test_blank_lifecycle_with_running_status_is_materializing_and_inspected():
    row = _row(
        lifecycle_state="",
        materialization_status="RUNNING",
        retry_attempt=2,
        generation=2,
    )
    hazard = preflight.classify_selector_recovery_hazard(row)
    assert hazard is not None
    assert hazard["lifecycle_state"] == "MATERIALIZING"
    assert hazard["selector_attempt_number"] == 2
    assert hazard["reason"] == "MISSING_CURSOR_ON_RETRY"


def test_contradictory_lifecycle_and_materialization_status_is_conflict():
    # lifecycle_state says MATERIALIZING but materialization_status says
    # RETRY_PENDING. The canonical resolver keeps lifecycle_state (matches OSM
    # write order) and surfaces LIFECYCLE_STATUS_CONFLICT rather than silently
    # picking one field.
    row = _row(
        lifecycle_state="MATERIALIZING",
        materialization_status="RETRY_PENDING",
        retry_attempt=1,
        generation=1,
    )
    hazard = preflight.classify_selector_recovery_hazard(row)
    assert hazard is not None
    assert hazard["reason"] == "LIFECYCLE_STATUS_CONFLICT"


def test_status_only_attempt_two_missing_cursor_is_unsafe():
    # A legacy/compat row that carries only materialization_status must still
    # be inspected at attempt 2+. Confirms the SQL population expansion is
    # actually exercised by the classifier.
    row = _row(
        lifecycle_state="",
        materialization_status="RUNNING",
        retry_attempt=0,
        materialization_attempts=2,
        generation=2,
    )
    hazard = preflight.classify_selector_recovery_hazard(row)
    assert hazard is not None
    assert hazard["lifecycle_state"] == "MATERIALIZING"
    assert hazard["selector_attempt_number"] == 2
    assert hazard["attempt_source"] == "materialization_attempts"
    assert hazard["reason"] == "MISSING_CURSOR_ON_RETRY"


def test_non_preclaimed_materializing_row_with_only_previous_generation_cursor_is_unsafe():
    # Explicit reviewer regression: a MATERIALIZING attempt-2 row carrying only
    # a valid generation-1 cursor must NOT be accepted as safe. Runtime accepts
    # this only when _recovery_pre_claimed is True (the preflight cannot prove
    # that from a read-only SELECT), so the preflight must fail closed.
    row = _row(
        lifecycle_state="MATERIALIZING",
        retry_attempt=2,
        generation=2,
        cursor_marker=_valid_cursor(
            generation=1,
            attempt=1,
        ),
    )
    hazard = preflight.classify_selector_recovery_hazard(row)
    assert hazard is not None
    assert hazard["reason"] == "IDENTITY_MISMATCH:materialization_generation"


def test_exact_current_generation_positive_control_materializing():
    # Positive control for Fix 2: an exact-current-generation cursor is safe.
    row = _row(
        lifecycle_state="MATERIALIZING",
        retry_attempt=3,
        generation=3,
        cursor_marker=_valid_cursor(
            generation=3,
            attempt=3,
        ),
    )
    assert preflight.classify_selector_recovery_hazard(row) is None


def test_sql_covers_status_only_recovery_population():
    # Blocker 1 required the SQL to include materialization_status-only rows.
    sql = " ".join(
        preflight.ACTIVE_SELECTOR_RECOVERY_ROWS_SQL.upper().split()
    )
    assert "MATERIALIZATION_STATUS" in sql
    assert "RETRY_PENDING" in sql
    assert "RUNNING" in sql


def test_cli_flags_status_only_hazard():
    """CLI-level regression that a status-only recovery row is not silently
    dropped by the JSON output — a false PASS is precisely the failure mode
    Blocker 1 warned about."""
    import pytest as _pytest

    payload = None

    def _fake_rows():
        return [_row(
            lifecycle_state="",
            materialization_status="RETRY_PENDING",
            retry_attempt=1,
            generation=1,
        )]

    monkey = _pytest.MonkeyPatch()
    monkey.setattr(preflight, "_fetch_active_rows", _fake_rows)
    try:
        exit_code = preflight.main()
    finally:
        monkey.undo()
    # Just prove the classification path is exercised end-to-end here; the
    # dedicated capsys-based CLI tests above assert the JSON contents.
    assert exit_code == 2
