from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import ap_overnight_reeval as overnight
from ap import order_monitor


@pytest.mark.xfail(
    strict=True,
    reason="P0 implementation pending: watcher Boolean results are not yet classified into explicit liveness outcomes",
)
def test_legacy_false_without_terminal_evidence_is_retryable_not_rejected():
    classify = getattr(overnight, "_classify_watch_arm_outcome", None)
    assert callable(classify), "missing structured watcher-arm outcome classifier"

    result = classify(
        watch_result=False,
        already_watching=False,
        terminal_conflict=False,
        exception=None,
    )

    assert result.disposition == "RETRYABLE_NOT_ARMED"
    assert result.terminal is False
    assert result.retryable is True


@pytest.mark.xfail(
    strict=True,
    reason="P0 implementation pending: pending-entry ownership must prove active same-client/same-mode ownership",
)
def test_terminal_pending_entry_does_not_block_new_candidate():
    owns = getattr(order_monitor, "_pending_entry_blocks_candidate", None)
    assert callable(owns), "missing canonical pending-entry ownership predicate"

    existing = {
        "local_order_id": "old-1",
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "symbol": "NOW",
        "side": "CALL",
        "status": "CANCELED",
        "created_ts": datetime.now(timezone.utc).isoformat(),
    }
    candidate = {
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "symbol": "NOW",
        "side": "CALL",
    }

    assert owns(existing, candidate, watcher_owned=False) is False


@pytest.mark.xfail(
    strict=True,
    reason="P0 implementation pending: stale orphan cleanup must unblock without weakening active-owner idempotency",
)
def test_stale_unowned_pending_entry_is_cleanup_eligible():
    classify = getattr(order_monitor, "_classify_pending_entry_ownership", None)
    assert callable(classify), "missing canonical pending-entry ownership classifier"

    existing = {
        "local_order_id": "orphan-1",
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "symbol": "SPY",
        "side": "PUT",
        "status": "PENDING_TRIGGER",
        "created_ts": (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat(),
    }

    result = classify(
        existing,
        watcher_owned=False,
        recovery_owned=False,
        broker_terminal=False,
    )

    assert result.disposition == "STALE_ORPHAN_CLEANUP"
    assert result.blocks_candidate is False
    assert result.cleanup_required is True


@pytest.mark.xfail(
    strict=True,
    reason="P0 implementation pending: PAPER ownership must never suppress a LIVE candidate",
)
def test_pending_entry_scope_isolated_by_execution_mode():
    owns = getattr(order_monitor, "_pending_entry_blocks_candidate", None)
    assert callable(owns), "missing canonical pending-entry ownership predicate"

    existing = {
        "local_order_id": "paper-1",
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "paper",
        "symbol": "QQQ",
        "side": "PUT",
        "status": "PENDING_TRIGGER",
        "created_ts": datetime.now(timezone.utc).isoformat(),
    }
    candidate = {
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "symbol": "QQQ",
        "side": "PUT",
    }

    assert owns(existing, candidate, watcher_owned=True) is False
