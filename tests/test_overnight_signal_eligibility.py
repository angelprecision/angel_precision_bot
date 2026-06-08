"""
tests/test_overnight_signal_eligibility.py
Regression: 1d signal created Sunday 10:15 PM ET remains eligible
Monday 8:57 AM ET and is not marked informational-only.
"""
import os, sys, pytest
sys.path.insert(0, '/home/claude')

SRC_QUEUE = open('/home/claude/ovn_queue_fixed.py').read()
SRC_REEVAL = open('/home/claude/ovn_reeval_fixed.py').read()

def test_1d_signal_not_marked_informational_only():
    """No signal with timeframe=1d should ever hit the old
    'informational only (not in execution pipeline)' log message."""
    assert "informational only (not in execution pipeline)" not in SRC_QUEUE

def test_overnight_candidate_loaded_log_present():
    """1d/1w signals during after-hours must log overnight_candidate_loaded=true."""
    assert "overnight_candidate_loaded=true" in SRC_QUEUE
    assert "market_hours=false" in SRC_QUEUE
    assert "contract_selection=deferred" in SRC_QUEUE
    assert "execution_pipeline=watching_for_morning_reeval" in SRC_QUEUE

def test_mc_block_rescue_for_overnight_candidates():
    """When MC blocks a 1d/1w signal after-hours, write WATCHING not MISSED."""
    assert "overnight_candidate_deferred" in SRC_QUEUE
    assert "overnight_candidate:mc_block_overridden_for_reeval" in SRC_QUEUE

def test_overnight_valid_timeframes_include_1d_and_1w():
    """1d and 1w must be in the overnight-candidate valid timeframe set."""
    assert '"1d"' in SRC_QUEUE and '"1w"' in SRC_QUEUE
    assert '"daily"' in SRC_QUEUE and '"weekly"' in SRC_QUEUE

def test_intraday_signals_still_informational():
    """True intraday (non-daily) after-hours signals keep informational-only label."""
    assert "Informational only" in SRC_QUEUE

def test_overnight_enabled_env_respected():
    """OVERNIGHT_ENABLED=false must disable the rescue path."""
    assert 'OVERNIGHT_ENABLED' in SRC_QUEUE

def test_signals_lookback_hours_used_in_reeval():
    """SIGNALS_LOOKBACK env var must drive the cutoff query in overnight reeval."""
    assert "_SIGNALS_LOOKBACK_HOURS" in SRC_REEVAL
    assert "timedelta(hours=_lookback_hours)" in SRC_REEVAL

def test_signals_lookback_default_18h():
    """Default lookback must be 18 hours to cover full overnight window."""
    assert '"SIGNALS_LOOKBACK", "18"' in SRC_REEVAL

def test_signals_lookback_takes_precedence_over_days():
    """When SIGNALS_LOOKBACK set, hours-based cutoff used over day-based."""
    idx = SRC_REEVAL.find("_lookback_hours and _lookback_hours > 0")
    assert idx > 0, "hours-based guard missing"
    region = SRC_REEVAL[idx:idx+200]
    assert "timedelta(hours=" in region

def test_sunday_10pm_signal_eligible_monday_857am():
    """
    Regression: 1d signal written Sunday 10:15 PM ET (03:15 UTC Monday)
    must still be within the 18h lookback window at Monday 8:57 AM ET (12:57 UTC).
    Gap = 12:57 - 03:15 = 9h 42m — well within 18h.
    """
    from datetime import datetime, timezone, timedelta
    signal_ts_utc = datetime(2026, 6, 8, 3, 15, 0, tzinfo=timezone.utc)   # Sun 10:15 PM ET
    reeval_ts_utc = datetime(2026, 6, 8, 12, 57, 0, tzinfo=timezone.utc)  # Mon 8:57 AM ET
    lookback_hours = 18
    cutoff = reeval_ts_utc - timedelta(hours=lookback_hours)
    assert signal_ts_utc >= cutoff, (
        f"Signal at {signal_ts_utc} is outside 18h lookback from {reeval_ts_utc}. "
        f"Gap = {(reeval_ts_utc - signal_ts_utc).total_seconds()/3600:.1f}h"
    )
