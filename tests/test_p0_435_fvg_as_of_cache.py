"""#435 BREACH 15m cache as-of completeness."""

from __future__ import annotations

from datetime import datetime, timezone

from ap.fvg_telemetry import (
    _completed_15m_bucket_key,
    cache_covers_as_of,
)


def test_live_path_uses_live_bucket():
    assert _completed_15m_bucket_key(None) == "live"


def test_completed_buckets_differ_across_new_15m_bar():
    earlier = datetime(2026, 9, 9, 14, 10, tzinfo=timezone.utc)
    later = datetime(2026, 9, 9, 14, 20, tzinfo=timezone.utc)
    assert _completed_15m_bucket_key(earlier) != _completed_15m_bucket_key(later)


def test_earlier_cache_cannot_cover_later_breach_as_of():
    # Last bar opens 14:00 → coverage through 14:15.
    # as_of 14:10 only needs the 14:00 boundary (covered).
    # as_of 14:30 needs the 14:15 completed boundary (not covered).
    bars = [{
        "time": "2026-09-09T14:00:00+00:00",
        "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0,
    }]
    assert cache_covers_as_of(bars, datetime(2026, 9, 9, 14, 10, tzinfo=timezone.utc))
    assert not cache_covers_as_of(bars, datetime(2026, 9, 9, 14, 30, tzinfo=timezone.utc))
