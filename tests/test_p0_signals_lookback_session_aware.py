"""
tests/test_p0_signals_lookback_session_aware.py
================================================
PR #388 amendment — SIGNALS_LOOKBACK default must be session-aware, not 18h.

A Friday scanner setup MUST be visible in Monday morning's overnight reeval
inventory fetch. The prior default of 18h left Friday rows outside the cutoff
by Monday, defeating the primary purpose of the PR whenever
`SIGNALS_LOOKBACK` was not explicitly overridden on the deployment.

These tests run with SIGNALS_LOOKBACK ABSENT from the environment.
"""
from __future__ import annotations

import importlib
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

import ap_overnight_reeval as ov


ET = ZoneInfo("America/New_York")


@pytest.fixture(autouse=True)
def _strip_signals_lookback(monkeypatch):
    """Guarantee SIGNALS_LOOKBACK is not set for these tests."""
    monkeypatch.delenv("SIGNALS_LOOKBACK", raising=False)
    yield


def _iso_to_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def test_default_cutoff_reaches_back_to_prior_trading_session_on_monday():
    """Monday morning cutoff must be at/before Friday 00:00 ET so a Friday
    scanner setup created any time on Friday is still inside the window."""
    monday_930_et = datetime(2026, 7, 27, 9, 30, tzinfo=ET)  # Mon after Fri 7/24
    monday_930_utc = monday_930_et.astimezone(timezone.utc)
    cutoff = _iso_to_dt(ov._signals_lookback_cutoff_iso(now=monday_930_utc))
    friday_start_et = datetime(2026, 7, 24, 0, 0, tzinfo=ET)
    assert cutoff <= friday_start_et.astimezone(timezone.utc), (
        f"Monday cutoff {cutoff} must be at/before Friday 00:00 ET "
        f"{friday_start_et} — a Friday scanner setup would otherwise be lost."
    )


def test_default_cutoff_covers_a_friday_evening_created_signal():
    """Friday 20:00 ET signal MUST fall inside the Monday-morning cutoff."""
    monday_930_utc = datetime(2026, 7, 27, 9, 30, tzinfo=ET).astimezone(timezone.utc)
    cutoff = _iso_to_dt(ov._signals_lookback_cutoff_iso(now=monday_930_utc))
    friday_signal_created = datetime(2026, 7, 24, 20, 0, tzinfo=ET).astimezone(timezone.utc)
    assert friday_signal_created >= cutoff, (
        f"Friday-evening signal at {friday_signal_created} was outside the "
        f"Monday cutoff {cutoff} — the 18-hour bug has returned."
    )


def test_default_cutoff_survives_a_monday_holiday_across_weekend():
    """Tuesday-after-holiday morning: cutoff must reach back to the last
    trading session (Friday) so Friday rows survive both a weekend AND a
    Monday closure. NYSE calendar drives the walk-back."""
    # 2026-01-19 is MLK Day (NYSE closed). Tuesday morning 2026-01-20.
    tue_930_utc = datetime(2026, 1, 20, 9, 30, tzinfo=ET).astimezone(timezone.utc)
    cutoff = _iso_to_dt(ov._signals_lookback_cutoff_iso(now=tue_930_utc))
    friday_start_et = datetime(2026, 1, 16, 0, 0, tzinfo=ET)
    assert cutoff <= friday_start_et.astimezone(timezone.utc), (
        f"Tue-after-MLK cutoff {cutoff} must be at/before Friday 00:00 ET "
        f"{friday_start_et}; got a cutoff that would drop Friday inventory."
    )


def test_default_cutoff_is_not_18_hours_relative_to_now():
    """Sanity: on a Monday morning the cutoff must be strictly older than
    now-18h. If it isn't, the 18h default has silently returned."""
    monday_930_utc = datetime(2026, 7, 27, 9, 30, tzinfo=ET).astimezone(timezone.utc)
    cutoff = _iso_to_dt(ov._signals_lookback_cutoff_iso(now=monday_930_utc))
    eighteen_hours_ago = monday_930_utc - timedelta(hours=18)
    assert cutoff < eighteen_hours_ago, (
        f"Cutoff {cutoff} is inside the 18-hour window {eighteen_hours_ago} — "
        f"the old default has come back."
    )


def test_explicit_signals_lookback_env_override_is_still_honored(monkeypatch):
    """Operators can still pin an explicit hours window when needed."""
    monkeypatch.setenv("SIGNALS_LOOKBACK", "6")
    ref = datetime(2026, 7, 27, 15, 0, tzinfo=timezone.utc)
    cutoff = _iso_to_dt(ov._signals_lookback_cutoff_iso(now=ref))
    assert cutoff == ref - timedelta(hours=6)


def test_signals_lookback_zero_or_negative_env_falls_back_to_session_aware(monkeypatch):
    """A misconfigured non-positive value must not silently disable retention.
    Fall back to the session-aware default instead of using 0 as the window."""
    for bad in ("0", "-5", "abc", ""):
        monkeypatch.setenv("SIGNALS_LOOKBACK", bad)
        monday_930_utc = datetime(2026, 7, 27, 9, 30, tzinfo=ET).astimezone(timezone.utc)
        cutoff = _iso_to_dt(ov._signals_lookback_cutoff_iso(now=monday_930_utc))
        friday_start_et = datetime(2026, 7, 24, 0, 0, tzinfo=ET)
        assert cutoff <= friday_start_et.astimezone(timezone.utc), (
            f"SIGNALS_LOOKBACK={bad!r} must fall back to session-aware; "
            f"got cutoff {cutoff}"
        )


def test_old_module_level_constant_is_removed():
    """The old _SIGNALS_LOOKBACK_HOURS module-level constant is gone; only
    the session-aware helper survives. This prevents accidental references
    to the 18h default from returning."""
    assert not hasattr(ov, "_SIGNALS_LOOKBACK_HOURS"), (
        "The frozen 18h default constant must be removed so no caller can "
        "accidentally read the old value."
    )
    assert callable(getattr(ov, "_signals_lookback_cutoff_iso", None))
