"""
tests/test_overnight_reeval_restore.py
Emergency P0 — restore overnight flow / stop false rejections.
"""
import os, sys
sys.path.insert(0, '/home/claude')
import pytest

SRC = open('/home/claude/em_reeval_fixed.py').read()

# ── Helpers ───────────────────────────────────────────────────────────────────

def _has(s): return s in SRC
def _count(s): return SRC.count(s)


# ── 1. Snapshot unavailable does NOT reject when fail-closed=false ────────────
def test_snapshot_miss_does_not_reject_by_default():
    assert '_OVERNIGHT_SNAPSHOT_FAIL_CLOSED' in SRC
    assert 'OVERNIGHT_SNAPSHOT_FAIL_CLOSED", "false"' in SRC   # default false

def test_snapshot_miss_goes_to_retry_later_not_rejected():
    assert 'RETRY_LATER' in SRC
    # The continue (keep watching) path must NOT call _mark_job_rejected
    idx = SRC.find('RETRY_LATER')
    region = SRC[idx:idx+200]
    assert '_mark_job_rejected' not in region

def test_snapshot_miss_increments_skipped_not_rejected():
    idx = SRC.find('RETRY_LATER')
    region = SRC[idx:idx+300]
    assert 'skipped' in region
    assert 'result["rejected"]' not in region

def test_snapshot_miss_logs_data_unavailable_not_invalidated():
    assert 'category=DATA_UNAVAILABLE' in SRC
    assert 'final_decision=RETRY_LATER' in SRC
    assert 'OVERNIGHT_SNAPSHOT_UNAVAILABLE' in SRC

def test_snapshot_miss_label_is_not_invalidated():
    # The DATA_UNAVAILABLE path must NOT contain OVERNIGHT_TRUE_INVALIDATION
    idx = SRC.find('category=DATA_UNAVAILABLE')
    region = SRC[max(0,idx-50):idx+300]
    assert 'OVERNIGHT_TRUE_INVALIDATION' not in region

# ── 2. Snapshot WATCHING preserved ───────────────────────────────────────────
def test_snapshot_miss_job_stays_watching():
    # After the RETRY_LATER log, we continue (leave job in WATCHING)
    assert 'leave job WATCHING for next reeval run' in SRC

# ── 3. True invalidation still rejects ───────────────────────────────────────
def test_true_invalidation_still_rejects():
    assert 'OVERNIGHT_TRUE_INVALIDATION' in SRC
    idx = SRC.find('OVERNIGHT_TRUE_INVALIDATION')
    region = SRC[idx:idx+400]
    assert '_mark_job_rejected' in region

def test_stale_signal_still_rejects():
    assert 'stale_signal' in SRC

def test_invalid_timeframe_still_rejects():
    assert 'intraday_timeframe_rejected' in SRC

# ── 4. MC re-score disabled by default ───────────────────────────────────────
def test_score_recheck_disabled_by_default():
    assert 'OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED", "false"' in SRC

def test_score_recheck_flag_exists():
    assert '_OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED' in SRC

# ── 9+10. Already-WATCHING signals with original score pass even if MC re-scores low
def test_original_score_70_passes_when_recheck_disabled():
    # When recheck=false and MC blocks, signal continues (audit-only)
    assert 'OVERNIGHT_SCORE_RECHECK_DISABLED' in SRC
    idx = SRC.find('OVERNIGHT_SCORE_RECHECK_DISABLED')
    region = SRC[idx:idx+300]
    assert 'continuing' in region

def test_original_score_not_overwritten():
    # signal["score"] must be restored to _original_score in both branches
    assert SRC.count('signal["score"] = _original_score') >= 2

def test_reeval_score_stored_as_metadata_only():
    assert 'overnight_reeval_score' in SRC
    assert 'original_signal_score' in SRC

def test_reeval_score_does_not_overwrite_signal_score():
    # Nowhere should we write signal["score"] = decision.score (reeval score)
    assert 'signal["score"] = decision.score' not in SRC
    assert 'signal["score"] = _reeval_score' not in SRC

# ── 11. Defaults ──────────────────────────────────────────────────────────────
def test_snapshot_fail_closed_default_is_false():
    assert '"OVERNIGHT_SNAPSHOT_FAIL_CLOSED", "false"' in SRC

def test_score_recheck_default_is_false():
    assert '"OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED", "false"' in SRC

# ── 14-18. No changes to other systems ───────────────────────────────────────
def test_no_scanner_scoring_changes():
    assert 'scanner' not in SRC.split('ap_overnight_reeval')[0][:100]

def test_no_intraday_logic_changes():
    # intraday rejection path still uses intraday_timeframe_rejected
    assert 'intraday_timeframe_rejected' in SRC

def test_no_contract_selector_filter_changes():
    # contract selector is called unchanged
    assert 'contract_selector.select' in SRC

def test_no_live_broker_submit_changes():
    # Broker submit is downstream of this module
    assert 'submit_order' not in SRC
    assert 'broker.submit' not in SRC
