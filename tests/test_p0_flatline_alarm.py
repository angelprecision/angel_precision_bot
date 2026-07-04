# tests/test_p0_flatline_alarm.py
# P0 zero-submission trading-day alarm.
#
# Invariants:
#   T1  Calendar truth: weekends + NYSE 2026 holidays are non-trading days;
#       2026-07-03 (the holiday the bot traded through) is NOT a trading day;
#       MARKET_HOLIDAYS_EXTRA is honored; unknown years fail toward trading.
#   T2  Before checkpoint → SKIPPED_BEFORE_CHECKPOINT, no alerts.
#   T3  Jun-29..Jul-02 failure shape (signals flowed, zero submissions) →
#       FLATLINE_SIGNALS_NO_SUBMISSIONS per mode, worst_state escalates.
#   T4  Healthy day → OK, no alerts.
#   T5  Dead pipeline (no signals at all) → FLATLINE_NO_SIGNALS.
#   T6  Counts query raises → CHECK_ERROR, never silent OK (fail-loud).
#   T7  dry_run never sends alerts even on flatline.

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from ap.flatline_alarm import (
    ET,
    is_trading_day,
    run_flatline_check,
)

ET_TZ = ZoneInfo("America/New_York")


def _at(y, m, d, hh, mm):
    return datetime(y, m, d, hh, mm, tzinfo=ET_TZ)


# ── T1: calendar ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("d,expected", [
    (date(2026, 7, 3), False),   # Independence Day observed — the incident day
    (date(2026, 7, 4), False),   # Saturday
    (date(2026, 7, 5), False),   # Sunday
    (date(2026, 7, 6), True),    # Monday — trading resumes
    (date(2026, 6, 19), False),  # Juneteenth
    (date(2026, 11, 26), False), # Thanksgiving
    (date(2026, 7, 1), True),
    (date(2026, 7, 2), True),
])
def test_t1_nyse_2026_calendar(d, expected):
    assert is_trading_day(d) is expected


def test_t1b_extra_holidays_env(monkeypatch):
    monkeypatch.setenv("MARKET_HOLIDAYS_EXTRA", "2026-08-14, 2026-08-17")
    assert is_trading_day(date(2026, 8, 14)) is False
    assert is_trading_day(date(2026, 8, 17)) is False
    assert is_trading_day(date(2026, 8, 18)) is True


def test_t1c_unknown_year_fails_toward_trading():
    assert is_trading_day(date(2031, 3, 12)) is True   # weekday, no calendar
    assert is_trading_day(date(2031, 3, 15)) is False  # Saturday still blocked


# ── helpers ──────────────────────────────────────────────────────────────────

def _counts(live_sub, live_sig, paper_sub, paper_sig, orders_created=None):
    def fn(_d):
        return {
            "live": {"submissions": live_sub, "fills": 0,
                     "orders_created": orders_created if orders_created is not None else live_sig,
                     "signals_global": live_sig},
            "paper": {"submissions": paper_sub, "fills": 0,
                      "orders_created": paper_sig,
                      "signals_global": paper_sig},
        }
    return fn


class _AlertSpy:
    def __init__(self):
        self.calls = []
    def __call__(self, url, payload):
        self.calls.append(payload)
        return True


@pytest.fixture
def alert_spy(monkeypatch):
    spy = _AlertSpy()
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test/webhook")
    import ap.discord_reporter as dr
    monkeypatch.setattr(dr, "send_discord_webhook", spy)
    return spy


# ── T2: checkpoint gating ────────────────────────────────────────────────────

def test_t2_before_checkpoint_skips(alert_spy):
    res = run_flatline_check(
        now_et=_at(2026, 7, 1, 9, 45),
        counts_fn=_counts(0, 100, 0, 100),
    )
    assert res.skipped_reason == "SKIPPED_BEFORE_CHECKPOINT"
    assert res.worst_state == "SKIPPED_BEFORE_CHECKPOINT"
    assert alert_spy.calls == []


def test_t2b_holiday_skips(alert_spy):
    res = run_flatline_check(
        now_et=_at(2026, 7, 3, 11, 0),   # the actual incident-noise day
        counts_fn=_counts(0, 100, 0, 100),
    )
    assert res.skipped_reason == "SKIPPED_NOT_TRADING_DAY"
    assert alert_spy.calls == []


# ── T3: the Jun-29..Jul-02 failure shape ─────────────────────────────────────

def test_t3_signals_but_zero_submissions_alerts(alert_spy):
    res = run_flatline_check(
        now_et=_at(2026, 7, 1, 10, 45),
        counts_fn=_counts(live_sub=0, live_sig=208, paper_sub=0, paper_sig=208),
    )
    assert res.state_by_mode["live"] == "FLATLINE_SIGNALS_NO_SUBMISSIONS"
    assert res.state_by_mode["paper"] == "FLATLINE_SIGNALS_NO_SUBMISSIONS"
    assert res.worst_state == "FLATLINE_SIGNALS_NO_SUBMISSIONS"
    assert res.alerts_sent == ["discord"]
    assert "FLATLINE" in alert_spy.calls[0]["content"]
    assert "live" in alert_spy.calls[0]["content"]


def test_t3b_one_mode_flat_other_ok(alert_spy):
    res = run_flatline_check(
        now_et=_at(2026, 7, 1, 10, 45),
        counts_fn=_counts(live_sub=0, live_sig=50, paper_sub=12, paper_sig=50),
    )
    assert res.state_by_mode["live"] == "FLATLINE_SIGNALS_NO_SUBMISSIONS"
    assert res.state_by_mode["paper"] == "OK"
    assert res.worst_state == "FLATLINE_SIGNALS_NO_SUBMISSIONS"
    assert res.alerts_sent == ["discord"]


# ── T4: healthy day ──────────────────────────────────────────────────────────

def test_t4_healthy_day_ok_no_alert(alert_spy):
    res = run_flatline_check(
        now_et=_at(2026, 7, 1, 10, 45),
        counts_fn=_counts(live_sub=6, live_sig=120, paper_sub=9, paper_sig=120),
    )
    assert res.worst_state == "OK"
    assert res.alerts_sent == []
    assert alert_spy.calls == []


# ── T5: dead pipeline ────────────────────────────────────────────────────────

def test_t5_no_signals_at_all(alert_spy):
    res = run_flatline_check(
        now_et=_at(2026, 7, 1, 10, 45),
        counts_fn=_counts(0, 0, 0, 0, orders_created=0),
    )
    assert res.state_by_mode["live"] == "FLATLINE_NO_SIGNALS"
    assert res.worst_state in ("FLATLINE_NO_SIGNALS", "FLATLINE_SIGNALS_NO_SUBMISSIONS")
    assert res.alerts_sent == ["discord"]


# ── T6: fail-loud ────────────────────────────────────────────────────────────

def test_t6_counts_failure_is_check_error_not_silent_ok(alert_spy):
    def boom(_d):
        raise RuntimeError("db down")
    res = run_flatline_check(now_et=_at(2026, 7, 1, 10, 45), counts_fn=boom)
    assert res.error is not None
    assert res.worst_state == "CHECK_ERROR"
    assert res.alerts_sent == ["discord"]  # alarm about the alarm


# ── T7: dry_run sends nothing ────────────────────────────────────────────────

def test_t7_dry_run_never_alerts(alert_spy):
    res = run_flatline_check(
        now_et=_at(2026, 7, 1, 10, 45),
        dry_run=True,
        counts_fn=_counts(0, 208, 0, 208),
    )
    assert res.worst_state == "FLATLINE_SIGNALS_NO_SUBMISSIONS"
    assert res.alerts_sent == []
    assert alert_spy.calls == []
