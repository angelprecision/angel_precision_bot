from __future__ import annotations

from datetime import datetime, timedelta, timezone

import ap.queue as queue


def _marker(ts: datetime, *, lookback_hours: int = 48) -> dict:
    return {
        "recovery_rescue": True,
        "recovery_rescue_ts": ts.isoformat(),
        "recovery_rescue_lookback_hours": lookback_hours,
        "execution_mode": "paper",
    }


def test_current_session_paper_recovery_bypasses_restart_guard() -> None:
    now = datetime.now(timezone.utc)
    payload = _marker(now)

    assert queue._is_current_session_paper_recovery(
        payload=payload,
        execution_mode="PAPER",
        now=now,
    )
    assert queue._manual_restart_guard_bypass_enabled(
        payload=payload,
        execution_mode="PAPER",
    )


def test_live_never_accepts_paper_recovery_marker() -> None:
    now = datetime.now(timezone.utc)
    payload = _marker(now)

    assert not queue._is_current_session_paper_recovery(
        payload=payload,
        execution_mode="LIVE",
        now=now,
    )
    assert not queue._manual_restart_guard_bypass_enabled(
        payload=payload,
        execution_mode="LIVE",
    )


def test_previous_session_marker_fails_closed() -> None:
    now = datetime.now(timezone.utc)
    payload = _marker(now - timedelta(days=1))

    assert not queue._is_current_session_paper_recovery(
        payload=payload,
        execution_mode="PAPER",
        now=now,
    )


def test_malformed_or_incomplete_marker_fails_closed() -> None:
    now = datetime.now(timezone.utc)

    malformed = _marker(now)
    malformed["recovery_rescue_ts"] = "not-a-timestamp"
    assert not queue._is_current_session_paper_recovery(
        payload=malformed,
        execution_mode="PAPER",
        now=now,
    )

    missing_timestamp = {
        "recovery_rescue": True,
        "recovery_rescue_lookback_hours": 48,
    }
    assert not queue._is_current_session_paper_recovery(
        payload=missing_timestamp,
        execution_mode="PAPER",
        now=now,
    )

    excessive_lookback = _marker(now, lookback_hours=49)
    assert not queue._is_current_session_paper_recovery(
        payload=excessive_lookback,
        execution_mode="PAPER",
        now=now,
    )


def test_future_marker_beyond_clock_skew_fails_closed() -> None:
    now = datetime.now(timezone.utc)
    payload = _marker(now + timedelta(minutes=6))

    assert not queue._is_current_session_paper_recovery(
        payload=payload,
        execution_mode="PAPER",
        now=now,
    )


def test_existing_manual_rescue_contract_is_preserved() -> None:
    assert queue._manual_restart_guard_bypass_enabled(
        job_result={"manual_rescue": True},
        payload={},
        execution_mode="PAPER",
    )
    assert queue._manual_restart_guard_bypass_enabled(
        job_last_error="manual_rescue_current_session",
        payload={},
        execution_mode="PAPER",
    )


def test_runtime_package_reexports_production_queue_surface() -> None:
    assert callable(queue.enqueue_signal)
    assert callable(queue.worker_loop)
    assert queue._base._manual_restart_guard_bypass_enabled is queue._manual_restart_guard_bypass_enabled
