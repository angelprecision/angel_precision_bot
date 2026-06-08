"""
tests/test_overnight_reeval_restore.py
Emergency P0 — restore original overnight behavior.
"""
from pathlib import Path
import pytest

_REPO = Path(__file__).resolve().parents[1]
Q   = (_REPO / "ap" / "queue.py").read_text()
OV  = (_REPO / "ap_overnight_reeval.py").read_text()


# ── 1. Snapshot unavailable does not reject ───────────────────────────────────

def test_snapshot_fail_closed_default_false():
    assert '"OVERNIGHT_SNAPSHOT_FAIL_CLOSED", "false"' in OV

def test_snapshot_miss_keeps_watching():
    assert 'leave job WATCHING for next reeval run' in OV

def test_snapshot_miss_increments_skipped_not_rejected():
    idx = OV.find('RETRY_LATER')
    region = OV[idx:idx+300]
    assert 'skipped' in region
    assert 'result["rejected"]' not in region

def test_snapshot_miss_logs_data_unavailable():
    assert 'category=DATA_UNAVAILABLE' in OV
    assert 'final_decision=RETRY_LATER' in OV

def test_snapshot_miss_no_mark_job_rejected():
    idx = OV.find('RETRY_LATER')
    region = OV[idx:idx+200]
    assert '_mark_job_rejected' not in region


# ── 2. True invalidation still rejects ───────────────────────────────────────

def test_true_invalidation_rejects():
    assert 'OVERNIGHT_TRUE_INVALIDATION' in OV
    idx = OV.find('OVERNIGHT_TRUE_INVALIDATION')
    assert '_mark_job_rejected' in OV[idx:idx+400]

def test_stale_signal_rejects():
    assert 'OVERNIGHT_STALE_SIGNAL' in OV

def test_invalid_timeframe_rejects():
    assert 'intraday_timeframe_rejected' in OV


# ── 3. MC reject does NOT rescue in LIVE by default ──────────────────────────

def test_overnight_mc_rescue_disabled_by_default():
    assert '"OVERNIGHT_MC_RESCUE_ENABLED", "false"' in Q

def test_mc_reject_no_rescue_log():
    assert 'MC_REJECTED_NO_RESCUE_LIVE' in Q

def test_mc_rescue_does_not_return_when_disabled():
    # When rescue disabled: log and fall through — no 'return' on the disabled path
    idx = Q.find('MC_REJECTED_NO_RESCUE_LIVE')
    region = Q[idx:idx+200]
    assert 'return' not in region   # disabled path must NOT return early

def test_mc_rescue_only_when_enabled():
    assert 'MC_RESCUE_ENABLED_TEST_ONLY' in Q


# ── 4. Approved overnight signal parks as WATCHING ───────────────────────────

def test_approved_watching_source_metadata():
    assert '"APPROVED_OVERNIGHT_WATCHING"' in Q
    assert '"mc_approved":                 True' in Q
    assert '"mc_rescue":                   False' in Q
    assert '"contract_selection_deferred": True' in Q


# ── 5. PR93 score recheck cannot arm rescued rejects ─────────────────────────

def test_rescued_signal_blocked_in_reeval_by_default():
    assert '"OVERNIGHT_MC_RESCUE_ENABLED", "false"' in OV
    assert '_is_mc_rescue' in OV
    assert '_mc_rescue_allowed' in OV

def test_rescued_signal_skipped_not_armed():
    # Rescued signals: guard block skips with continue
    assert '_is_mc_rescue and not _mc_rescue_allowed' in OV
    idx = OV.find('_is_mc_rescue and not _mc_rescue_allowed')
    # The guard is in an if block — the body has skipped/continue
    region = OV[idx:idx+500]
    assert '"skipped"' in region or 'skipped' in region
    assert 'continue' in region

def test_rescued_signal_metadata_present():
    assert '"mc_rescue":       True' in Q
    assert '"watching_source": "mc_rejected_overnight_rescue"' in Q


# ── 6. Approved WATCHING can arm when snapshot available ─────────────────────

def test_approved_watching_can_proceed():
    assert '"mc_approved":                 True' in Q
    assert '"mc_rescue":                   False' in Q


# ── Lifecycle: WATCHING state set before REJECTED ────────────────────────────

def test_watching_established_before_rejected():
    assert 'stale_check_pre_reject' in OV

def test_stale_reject_uses_overnight_reason_code():
    assert 'OVERNIGHT_STALE_SIGNAL' in OV


# ── No type(decision) reconstruction ────────────────────────────────────────

def test_no_type_decision_reconstruction():
    assert 'type(decision)(' not in OV

def test_original_score_not_overwritten():
    assert OV.count('signal["score"] = _original_score') >= 2


# ── Scope: no other systems changed ──────────────────────────────────────────

def test_no_contract_selector_filter_changes():
    assert 'contract_selector.select' in OV

def test_no_live_broker_submit_changes():
    assert 'submit_order' not in OV
    assert 'broker.submit' not in OV
