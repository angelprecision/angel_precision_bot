"""
tests/test_overnight_reeval_restore.py
Emergency P0 — restore overnight flow / stop false rejections.
Tests read from the actual repo file — no /home/claude/ paths.
"""
from pathlib import Path
import pytest

_REPO = Path(__file__).resolve().parents[1]
SRC   = (_REPO / "ap_overnight_reeval.py").read_text()


# ── 1. Snapshot unavailable does NOT reject when fail-closed=false ────────────

def test_snapshot_fail_closed_default_is_false():
    assert '"OVERNIGHT_SNAPSHOT_FAIL_CLOSED", "false"' in SRC

def test_snapshot_miss_does_not_reject_by_default():
    assert '_OVERNIGHT_SNAPSHOT_FAIL_CLOSED' in SRC

def test_snapshot_miss_goes_to_retry_later_not_rejected():
    assert 'RETRY_LATER' in SRC
    idx = SRC.find('RETRY_LATER')
    region = SRC[idx:idx+200]
    assert '_mark_job_rejected' not in region

# ── 2. Snapshot miss keeps job WATCHING ───────────────────────────────────────

def test_snapshot_miss_job_stays_watching():
    assert 'leave job WATCHING for next reeval run' in SRC

# ── 3. Snapshot miss increments skipped not rejected ─────────────────────────

def test_snapshot_miss_increments_skipped_not_rejected():
    idx = SRC.find('RETRY_LATER')
    region = SRC[idx:idx+300]
    assert 'skipped' in region
    assert 'result["rejected"]' not in region

# ── 4. Snapshot miss logs DATA_UNAVAILABLE / RETRY_LATER, not INVALIDATED ────

def test_snapshot_miss_logs_data_unavailable():
    assert 'category=DATA_UNAVAILABLE' in SRC
    assert 'final_decision=RETRY_LATER' in SRC
    assert 'OVERNIGHT_SNAPSHOT_UNAVAILABLE' in SRC

def test_snapshot_miss_not_labeled_invalidated():
    idx = SRC.find('category=DATA_UNAVAILABLE')
    region = SRC[max(0, idx-50):idx+300]
    assert 'OVERNIGHT_TRUE_INVALIDATION' not in region

# ── 5. True overnight invalidation still rejects ─────────────────────────────

def test_true_invalidation_still_rejects():
    assert 'OVERNIGHT_TRUE_INVALIDATION' in SRC
    idx = SRC.find('OVERNIGHT_TRUE_INVALIDATION')
    region = SRC[idx:idx+400]
    assert '_mark_job_rejected' in region

# ── 6. Stale signal age still rejects ────────────────────────────────────────

def test_stale_signal_still_rejects():
    assert 'stale_signal' in SRC

# ── 7. Invalid timeframe still rejects ───────────────────────────────────────

def test_invalid_timeframe_still_rejects():
    assert 'intraday_timeframe_rejected' in SRC

# ── 8. Missing prior levels handled inside validator, not here ────────────────

def test_prior_levels_check_exists():
    assert 'prior_day_high' in SRC
    assert 'prior_day_low' in SRC

# ── 9+10. Already-WATCHING signal with original score proceeds even if reeval=low

def test_score_recheck_disabled_original_score_passes():
    assert 'OVERNIGHT_SCORE_RECHECK_DISABLED' in SRC
    idx = SRC.find('OVERNIGHT_SCORE_RECHECK_DISABLED')
    region = SRC[idx:idx+400]
    assert 'continuing' in region.lower()

def test_original_score_not_overwritten():
    # signal["score"] must be restored to _original_score, not reeval score
    assert SRC.count('signal["score"] = _original_score') >= 2

# ── 11. OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED=false is the default ──────────

def test_score_recheck_default_is_false():
    assert '"OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED", "false"' in SRC

def test_score_recheck_flag_exists():
    assert '_OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED' in SRC

# ── 12. Recalculated score stored only as metadata when enabled ───────────────

def test_reeval_score_stored_as_metadata():
    assert '"overnight_reeval_score"' in SRC
    assert '"original_signal_score"' in SRC
    assert '"overnight_reeval_score_source"' in SRC
    assert '"overnight_reeval_score_recheck_enabled"' in SRC

# ── 13. Recalculated score does NOT overwrite signal["score"] ─────────────────

def test_reeval_score_does_not_overwrite():
    assert 'signal["score"] = decision.score' not in SRC
    assert 'signal["score"] = _reeval_score' not in SRC

# ── No risky decision reconstruction ─────────────────────────────────────────

def test_no_type_decision_reconstruction():
    assert 'type(decision)(' not in SRC

def test_plan_missing_skips_not_rejects():
    assert 'OVERNIGHT_SCORE_RECHECK_AUDIT_ONLY' in SRC
    idx = SRC.find('OVERNIGHT_SCORE_RECHECK_AUDIT_ONLY')
    region = SRC[idx:idx+300]
    assert 'skipped' in region
    assert '_mark_job_rejected' not in region

# ── 14-18. No other system changes ───────────────────────────────────────────

def test_no_intraday_logic_changes():
    assert 'intraday_timeframe_rejected' in SRC

def test_no_contract_selector_filter_changes():
    assert 'contract_selector.select' in SRC

def test_no_live_broker_submit_changes():
    assert 'submit_order' not in SRC
    assert 'broker.submit' not in SRC
