"""
P0 regression: LIVE preopen_readiness blocked_keys + NYSE calendar routing.

Contract enforced here
──────────────────────
1. LIVE runners with `overnight_reeval_missing` in errors must return
   status="BLOCKED", not "DEGRADED". Client runner enters degraded mode
   (blocks new entries) only on BLOCKED.

2. LIVE runners with `pending_trigger_without_watcher_ownership` in errors
   must return status="BLOCKED".

3. PAPER runners with either error must return "DEGRADED", not "BLOCKED"
   (paper sessions still surface diagnostics without freezing).

4. `_is_market_day`, `_after_929_et`, `_overnight_reeval_due` must all
   route through ap.flatline_alarm.is_trading_day so an NYSE holiday
   (e.g. July 4) is treated the same as a weekend.
"""
from __future__ import annotations

import datetime as dt
from unittest.mock import MagicMock, patch

import pytest


# ─── NYSE calendar routing ────────────────────────────────────────────────────

def test_is_market_day_routes_through_nyse_calendar_on_holiday(monkeypatch):
    """When ap.flatline_alarm.is_trading_day says False, _is_market_day
    must return False even if the date is a weekday."""
    from ap import preopen_readiness as pr

    # July 3, 2026 is a Friday (Independence Day observed by NYSE)
    friday_holiday = dt.datetime(2026, 7, 3, 9, 15, tzinfo=pr.ET)
    monkeypatch.setattr(
        "ap.flatline_alarm.is_trading_day",
        lambda d: False,  # simulate closure
    )
    assert pr._is_market_day(friday_holiday) is False


def test_is_market_day_normal_weekday_is_trading(monkeypatch):
    from ap import preopen_readiness as pr
    friday = dt.datetime(2026, 7, 24, 9, 15, tzinfo=pr.ET)
    monkeypatch.setattr("ap.flatline_alarm.is_trading_day", lambda d: True)
    assert pr._is_market_day(friday) is True


def test_is_market_day_weekend_is_not_trading(monkeypatch):
    from ap import preopen_readiness as pr
    saturday = dt.datetime(2026, 7, 25, 9, 15, tzinfo=pr.ET)
    monkeypatch.setattr("ap.flatline_alarm.is_trading_day", lambda d: False)
    assert pr._is_market_day(saturday) is False


def test_after_929_et_false_on_holiday(monkeypatch):
    """Even at 10:00 AM ET, on an NYSE-closed day _after_929_et is False —
    prevents the readiness enforcement path from firing on holidays."""
    from ap import preopen_readiness as pr
    friday_holiday_10am = dt.datetime(2026, 7, 3, 10, 0, tzinfo=pr.ET)
    monkeypatch.setattr("ap.flatline_alarm.is_trading_day", lambda d: False)
    assert pr._after_929_et(friday_holiday_10am) is False


def test_after_929_et_true_on_trading_day(monkeypatch):
    from ap import preopen_readiness as pr
    trading_day_930 = dt.datetime(2026, 7, 24, 9, 30, tzinfo=pr.ET)
    monkeypatch.setattr("ap.flatline_alarm.is_trading_day", lambda d: True)
    assert pr._after_929_et(trading_day_930) is True


def test_overnight_reeval_due_false_on_holiday(monkeypatch):
    from ap import preopen_readiness as pr
    friday_holiday = dt.datetime(2026, 7, 3, 9, 30, tzinfo=pr.ET)
    monkeypatch.setattr("ap.flatline_alarm.is_trading_day", lambda d: False)
    assert pr._overnight_reeval_due(friday_holiday) is False


def test_nyse_calendar_import_failure_falls_back_to_weekday(monkeypatch):
    """Fail-safe: if the calendar module is unavailable, _nyse_is_trading_day
    falls back to weekday-only. Preserves prior behavior on environments
    that don't ship ap.flatline_alarm."""
    from ap import preopen_readiness as pr
    import sys

    # Force the ap.flatline_alarm import inside _nyse_is_trading_day to raise.
    monkeypatch.setitem(sys.modules, "ap.flatline_alarm", None)

    friday = dt.datetime(2026, 7, 24, 9, 15, tzinfo=pr.ET)
    assert pr._nyse_is_trading_day(friday) is True
    saturday = dt.datetime(2026, 7, 25, 9, 15, tzinfo=pr.ET)
    assert pr._nyse_is_trading_day(saturday) is False


# ─── LIVE blocked_keys hardening ─────────────────────────────────────────────

def _build_runner(mode="live", *, alive=True, initialized=True, worker_alive=True):
    runner = MagicMock()
    runner.mode = mode
    runner.is_alive.return_value = alive
    runner.initialized = MagicMock()
    runner.initialized.is_set.return_value = initialized
    runner.worker_thread = MagicMock()
    runner.worker_thread.is_alive.return_value = worker_alive
    runner.order_state_machine = MagicMock()
    runner.core = MagicMock()
    runner.core.entry_watcher = MagicMock()
    runner.core.entry_watcher.has_order = MagicMock(return_value=True)
    runner.core.broker = MagicMock()
    runner.core.broker.cfg = MagicMock(base_url="https://api.tradier.com", account_id="ACCT", access_token="TOKEN")
    runner.core.broker.base_url = "https://api.tradier.com"
    runner.core.broker.account_id = "ACCT"
    runner.core.broker.access_token = "TOKEN"
    runner.contract_selector = MagicMock()
    runner.contract_selector.data_broker = runner.core.broker
    runner.master_control = MagicMock()
    runner.master_control.mode = mode
    runner.member = {"tradier_live_access_token": "T", "tradier_paper_access_token": "T"}
    runner._resolved_tradier_token = "T"
    runner._overnight_reeval_success_date = None
    return runner


def test_live_missing_overnight_reeval_returns_blocked(monkeypatch):
    """PR #388 core-promise gate: LIVE + overnight_reeval_missing → BLOCKED."""
    from ap import preopen_readiness as pr

    runner = _build_runner(mode="live")

    # Force the required "missing" conditions.
    monkeypatch.setattr(pr, "_resolve_runner", lambda cid: runner)
    monkeypatch.setattr(pr, "_upsert_preopen_row", lambda **kw: None)
    monkeypatch.setattr(pr, "_morning_handoff_success_exists",
                        lambda cid, mode, td: True)  # handoff OK
    monkeypatch.setattr(pr, "_query_client_state", lambda cid, mode: {
        "stale_processing_ids": [],
        "watching_orphans": [],
        "pending_trigger_rows": [],
        "watching_count": 3,  # nonzero so overnight_status is "missing"
    })
    monkeypatch.setattr(pr, "_post_overnight_reeval_success_exists",
                        lambda cid, mode, td: False)
    monkeypatch.setattr(pr, "_nyse_is_trading_day", lambda dt: True)
    # After 9:29 to force the missing overnight to become an error
    monkeypatch.setattr(pr, "_after_929_et", lambda now=None: True)

    result = pr.run_preopen_autonomous_readiness(
        "live-client@example.com", "live",
        dry_run=True, stage="post_overnight_reeval",
    )
    assert "overnight_reeval_missing" in result["errors"]
    assert result["status"] == "BLOCKED", (
        f"LIVE runner with overnight_reeval_missing must return BLOCKED "
        f"(triggers degraded mode); got {result['status']!r}. "
        f"errors={result['errors']!r}"
    )
    assert result["ok"] is False


def test_live_pending_trigger_without_watcher_returns_blocked(monkeypatch):
    """LIVE + pending_trigger_without_watcher_ownership → BLOCKED.

    Rows in the DB with status=PENDING_TRIGGER but no live in-memory watcher
    ownership are a direct violation of the PR's core promise. Live entries
    must be cut off."""
    from ap import preopen_readiness as pr

    runner = _build_runner(mode="live")
    # Watcher does NOT own the pending order.
    runner.core.entry_watcher.has_order = MagicMock(return_value=False)

    monkeypatch.setattr(pr, "_resolve_runner", lambda cid: runner)
    monkeypatch.setattr(pr, "_upsert_preopen_row", lambda **kw: None)
    monkeypatch.setattr(pr, "_morning_handoff_success_exists", lambda cid, mode, td: True)
    monkeypatch.setattr(pr, "_query_client_state", lambda cid, mode: {
        "stale_processing_ids": [],
        "watching_orphans": [],
        "pending_trigger_rows": [{"local_order_id": "loi-999", "signal_id": "sig-1"}],
        "watching_count": 0,  # so overnight status becomes explicit_noop
    })
    monkeypatch.setattr(pr, "_post_overnight_reeval_success_exists", lambda cid, mode, td: True)
    monkeypatch.setattr(pr, "_nyse_is_trading_day", lambda dt: True)

    result = pr.run_preopen_autonomous_readiness(
        "live-client@example.com", "live",
        dry_run=True, stage="post_overnight_reeval",
    )
    assert "pending_trigger_without_watcher_ownership" in result["errors"]
    assert result["status"] == "BLOCKED", (
        f"LIVE runner with unowned PENDING_TRIGGER must BLOCK; got "
        f"{result['status']!r}. errors={result['errors']!r}"
    )


def test_paper_missing_overnight_reeval_degrades_not_blocks(monkeypatch):
    """Guard is narrow to LIVE. PAPER + overnight_reeval_missing → DEGRADED.

    Paper sessions should still surface the diagnostic without freezing
    the runner. Only LIVE capital risk warrants a hard block."""
    from ap import preopen_readiness as pr

    runner = _build_runner(mode="paper")
    monkeypatch.setattr(pr, "_resolve_runner", lambda cid: runner)
    monkeypatch.setattr(pr, "_upsert_preopen_row", lambda **kw: None)
    monkeypatch.setattr(pr, "_morning_handoff_success_exists", lambda cid, mode, td: True)
    monkeypatch.setattr(pr, "_query_client_state", lambda cid, mode: {
        "stale_processing_ids": [],
        "watching_orphans": [],
        "pending_trigger_rows": [],
        "watching_count": 3,
    })
    monkeypatch.setattr(pr, "_post_overnight_reeval_success_exists", lambda cid, mode, td: False)
    monkeypatch.setattr(pr, "_nyse_is_trading_day", lambda dt: True)
    monkeypatch.setattr(pr, "_after_929_et", lambda now=None: True)

    result = pr.run_preopen_autonomous_readiness(
        "paper-client@example.com", "paper",
        dry_run=True, stage="post_overnight_reeval",
    )
    assert result["status"] == "DEGRADED", (
        f"PAPER must DEGRADE (not BLOCK) on overnight_reeval_missing; "
        f"got {result['status']!r}."
    )
