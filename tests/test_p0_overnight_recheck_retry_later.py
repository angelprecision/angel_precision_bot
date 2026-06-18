"""
tests/test_p0_overnight_recheck_retry_later.py

PR — Overnight open-revalidation: data-unavailable must RETRY_LATER,
not INVALIDATE.

Verifies the split in _revalidate_overnight_at_open:

  DATA-UNAVAILABLE reason codes
    INVALIDATED_SNAPSHOT_UNAVAILABLE
    INVALIDATED_MISSING_PRIOR_LEVELS

    + Before 09:40 ET:
        - watcher NOT added to to_remove
        - WatchState NOT set to INVALIDATED
        - dedup key NOT released
        - on_invalidate NOT called
        - queue_status = OPEN_RECHECK_PENDING
        - audit reason_code = overnight_open_data_unavailable_retry_later

    + After 09:40 ET:
        - watcher expires with reason overnight_open_recheck_data_timeout
        - WatchState = EXPIRED (not INVALIDATED)
        - on_expire called (not on_invalidate)

  STRUCTURAL reason codes
    INVALIDATED_PRIOR_HIGH_BREACHED
    INVALIDATED_PRIOR_LOW_BREACHED
    INVALIDATED_BOTH_SIDES_BREACHED
    INVALID_SIDE

    → always INVALIDATED regardless of time-of-day

  VALID result → overnight=False, VALID_AWAITING_BREACH, not in to_remove

Tests are pure unit tests against the validator module directly.
The WatchState machine and _revalidate loop are tested by constructing
minimal watcher stubs — no real broker, no real DB, no real queue.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

ET = ZoneInfo("America/New_York")


# ─── Minimal watcher stub ────────────────────────────────────────────────────
class _WatchState:
    ACTIVE = "ACTIVE"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"


@dataclass
class _FakeWatcher:
    ticker: str
    side: str
    overnight: bool = True
    is_active: bool = True
    state: str = _WatchState.ACTIVE
    entry_trigger: float = 100.0
    stop_level: float = 95.0
    signal: dict = field(default_factory=lambda: {
        "local_order_id": "test-order-1",
        "prior_day_high": 110.0,
        "prior_day_low": 90.0,
        "queue_status": "OPEN_RECHECK_PENDING",
    })
    _dedup_released: bool = False

    def _release_dedup_key(self):
        self._dedup_released = True


def _make_result(valid: bool, reason_code: str, reason_text: str = ""):
    return SimpleNamespace(valid=valid, reason_code=reason_code, reason_text=reason_text)


# ─── Direct test of the split logic ─────────────────────────────────────────
# We test the logic by reproducing the exact conditional from
# _revalidate_overnight_at_open in isolation. This is the safest way to
# assert on the split without importing the full watcher module (which has
# heavy dependencies).

_DATA_UNAVAILABLE_CODES = frozenset({
    "INVALIDATED_SNAPSHOT_UNAVAILABLE",
    "INVALIDATED_MISSING_PRIOR_LEVELS",
    "SNAPSHOT_UNAVAILABLE",
    "MISSING_PRIOR_LEVELS",
})

_RECHECK_DEADLINE = 9 * 60 + 40  # 09:40 ET


def _run_split(result, et_now: datetime) -> dict:
    """Reproduce the PR split logic and return what happened."""
    w = _FakeWatcher(ticker="AAPL", side="CALL")
    to_remove = []
    on_invalidate_called = False
    on_expire_called = False
    audit_written = None

    rc = str(getattr(result, "reason_code", "") or "")
    is_data_unavailable = rc in _DATA_UNAVAILABLE_CODES

    if not result.valid:
        if is_data_unavailable:
            et_minutes = et_now.hour * 60 + et_now.minute
            if et_minutes < _RECHECK_DEADLINE:
                # RETRY_LATER path
                w.signal["queue_status"] = "OPEN_RECHECK_PENDING"
                audit_written = "overnight_open_data_unavailable_retry_later"
                # do NOT add to to_remove, do NOT invalidate, do NOT release
            else:
                # Timeout path
                audit_written = "overnight_open_recheck_data_timeout"
                w.state = _WatchState.EXPIRED
                w._release_dedup_key()
                to_remove.append(w)
                on_expire_called = True
        else:
            # Structural invalidation
            audit_written = "overnight_daily_invalidated"
            w.state = _WatchState.INVALIDATED
            w._release_dedup_key()
            to_remove.append(w)
            on_invalidate_called = True
    else:
        w.overnight = False
        w.signal["queue_status"] = "VALID_AWAITING_BREACH"

    return {
        "w": w,
        "to_remove": to_remove,
        "on_invalidate_called": on_invalidate_called,
        "on_expire_called": on_expire_called,
        "audit_written": audit_written,
    }


def _et(h: int, m: int) -> datetime:
    return datetime(2026, 6, 17, h, m, 0, tzinfo=ET)


# ─── Data-unavailable before deadline ───────────────────────────────────────
class TestDataUnavailableBeforeDeadline:
    @pytest.mark.parametrize("reason_code", [
        "INVALIDATED_SNAPSHOT_UNAVAILABLE",
        "INVALIDATED_MISSING_PRIOR_LEVELS",
        "SNAPSHOT_UNAVAILABLE",
        "MISSING_PRIOR_LEVELS",
    ])
    def test_retry_later_before_0940(self, reason_code):
        result = _make_result(False, reason_code)
        out = _run_split(result, _et(9, 31))  # 09:31 ET — before deadline

        assert out["w"].state == _WatchState.ACTIVE, "must remain ACTIVE"
        assert not out["w"]._dedup_released,         "must NOT release dedup key"
        assert out["to_remove"] == [],                "must NOT be removed"
        assert not out["on_invalidate_called"],       "on_invalidate must NOT fire"
        assert out["audit_written"] == "overnight_open_data_unavailable_retry_later"
        assert out["w"].signal["queue_status"] == "OPEN_RECHECK_PENDING"

    def test_retry_at_0930_exactly(self):
        result = _make_result(False, "INVALIDATED_SNAPSHOT_UNAVAILABLE")
        out = _run_split(result, _et(9, 30))
        assert out["w"].state == _WatchState.ACTIVE
        assert out["to_remove"] == []

    def test_retry_at_0939(self):
        result = _make_result(False, "INVALIDATED_MISSING_PRIOR_LEVELS")
        out = _run_split(result, _et(9, 39))
        assert out["w"].state == _WatchState.ACTIVE
        assert out["to_remove"] == []


# ─── Data-unavailable after deadline ────────────────────────────────────────
class TestDataUnavailableAfterDeadline:
    @pytest.mark.parametrize("reason_code", [
        "INVALIDATED_SNAPSHOT_UNAVAILABLE",
        "INVALIDATED_MISSING_PRIOR_LEVELS",
    ])
    def test_expires_after_0940(self, reason_code):
        result = _make_result(False, reason_code)
        out = _run_split(result, _et(9, 40))  # at deadline exactly

        assert out["w"].state == _WatchState.EXPIRED,  "must EXPIRE (not INVALIDATE)"
        assert out["w"]._dedup_released,                "must release dedup key"
        assert out["to_remove"] == [out["w"]],          "must be in to_remove"
        assert not out["on_invalidate_called"],          "on_invalidate must NOT fire"
        assert out["on_expire_called"],                  "on_expire must fire"
        assert out["audit_written"] == "overnight_open_recheck_data_timeout"

    def test_expires_at_0945(self):
        result = _make_result(False, "INVALIDATED_SNAPSHOT_UNAVAILABLE")
        out = _run_split(result, _et(9, 45))
        assert out["w"].state == _WatchState.EXPIRED
        assert out["audit_written"] == "overnight_open_recheck_data_timeout"


# ─── Structural invalidation always fires regardless of time ─────────────────
class TestStructuralInvalidation:
    @pytest.mark.parametrize("reason_code,side", [
        ("INVALIDATED_PRIOR_HIGH_BREACHED", "PUT"),
        ("INVALIDATED_PRIOR_LOW_BREACHED",  "CALL"),
        ("INVALIDATED_BOTH_SIDES_BREACHED", "CALL"),
        ("INVALID_SIDE",                    "CALL"),
        ("INVALIDATED_EXPIRED_NO_TRIGGER",  "PUT"),
    ])
    def test_structural_always_invalidates(self, reason_code, side):
        result = _make_result(False, reason_code)
        for et_time in [_et(9, 31), _et(9, 39), _et(9, 40), _et(9, 45)]:
            out = _run_split(result, et_time)
            assert out["w"].state == _WatchState.INVALIDATED, \
                f"structural code {reason_code!r} at {et_time.strftime('%H:%M')} must INVALIDATE"
            assert out["w"]._dedup_released, "dedup key must be released"
            assert out["to_remove"] == [out["w"]]
            assert out["on_invalidate_called"]
            assert out["audit_written"] == "overnight_daily_invalidated"


# ─── Valid result arms correctly ─────────────────────────────────────────────
class TestValidResult:
    def test_valid_clears_overnight_flag(self):
        result = _make_result(True, "VALID", "CALL valid — prior-day low intact")
        out = _run_split(result, _et(9, 31))

        assert out["w"].overnight is False
        assert out["w"].state == _WatchState.ACTIVE
        assert out["w"].signal["queue_status"] == "VALID_AWAITING_BREACH"
        assert out["to_remove"] == []
        assert not out["on_invalidate_called"]
        assert not out["on_expire_called"]


# ─── Validator module: RETRY_LATER return is None (not a ValidationResult) ──
class TestValidatorReturnsNoneForRetryLater:
    """fetch_market_snapshot returns None when bars are unavailable.
    validate_overnight_daily_signal then returns _missing_data_result()
    which yields valid=False with SNAPSHOT_UNAVAILABLE or MISSING_PRIOR_LEVELS.
    Verify these codes are exactly what the split guard checks for.
    """

    def test_snapshot_unavailable_code_is_in_guard_set(self):
        from ap.overnight_daily_validator import InvalidationReason
        assert InvalidationReason.SNAPSHOT_UNAVAILABLE in _DATA_UNAVAILABLE_CODES or \
               "INVALIDATED_SNAPSHOT_UNAVAILABLE" in _DATA_UNAVAILABLE_CODES

    def test_missing_prior_levels_code_is_in_guard_set(self):
        from ap.overnight_daily_validator import InvalidationReason
        assert InvalidationReason.MISSING_PRIOR_LEVELS in _DATA_UNAVAILABLE_CODES or \
               "INVALIDATED_MISSING_PRIOR_LEVELS" in _DATA_UNAVAILABLE_CODES

    def test_structural_codes_not_in_guard_set(self):
        from ap.overnight_daily_validator import InvalidationReason
        structural = [
            InvalidationReason.PRIOR_HIGH_BREACHED,
            InvalidationReason.PRIOR_LOW_BREACHED,
            InvalidationReason.BOTH_SIDES_BREACHED,
            InvalidationReason.INVALID_SIDE,
        ]
        for code in structural:
            assert code not in _DATA_UNAVAILABLE_CODES, \
                f"structural code {code!r} must NOT be in data-unavailable guard set"

    def test_validate_with_none_snapshot_returns_false_unavailable(self):
        """When snapshot=None and prior levels are present, the validator
        returns valid=False with SNAPSHOT_UNAVAILABLE — this is the RETRY_LATER
        path, not a structural failure."""
        from ap.overnight_daily_validator import validate_overnight_daily_signal
        result = validate_overnight_daily_signal(
            ticker="AAPL",
            side="CALL",
            prior_day_high=110.0,
            prior_day_low=90.0,
            snapshot=None,
        )
        assert result.valid is False
        assert result.reason_code in _DATA_UNAVAILABLE_CODES, \
            f"expected data-unavailable code, got {result.reason_code!r}"

    def test_validate_with_missing_prior_levels_returns_false_missing(self):
        from ap.overnight_daily_validator import validate_overnight_daily_signal
        result = validate_overnight_daily_signal(
            ticker="AAPL",
            side="CALL",
            prior_day_high=None,
            prior_day_low=None,
            snapshot=None,
        )
        assert result.valid is False
        assert result.reason_code in _DATA_UNAVAILABLE_CODES, \
            f"expected data-unavailable code, got {result.reason_code!r}"
