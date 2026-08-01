"""
tests/test_pr96_observe_only_and_overnight.py
PR#96: second/intel score observe-only + overnight reeval safety preserved.
"""
import sys
from pathlib import Path
import pytest

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

import importlib.util as _ilu

def _load(rel):
    spec = _ilu.spec_from_file_location("_mod", _REPO / rel)
    mod  = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_ib = _load("intelligence_bridge.py")
_map_result    = _ib._map_result
_count_missing = _ib._count_missing_fundamentals

OV_SRC = (_REPO / "ap_overnight_reeval.py").read_text()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _skip_result(scanner_score=70.0, intel_score=38.0, ticker="AAPL",
                 fund_signal="neutral", fund_confidence=50.0,
                 reasoning_override=None, risk_approved=True, risk_reason="approved"):
    return {
        "ticker": ticker,
        "action": "skip",
        "score":  intel_score,
        "confidence": 0.0,
        "contracts": 1,
        "reasoning": reasoning_override or
                     f"Score {intel_score:.1f} < 60 — insufficient edge (production mode)",
        "risk_detail": {"approved": risk_approved, "reason": risk_reason,
                        "contract_quality_passes": True},
        "signal_breakdown": {
            "fundamentals": {"signal": fund_signal, "confidence": fund_confidence},
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# GATE G — observe-only behavior (tests 1–8)
# ══════════════════════════════════════════════════════════════════════════════

def test_01_score70_intel38_no_hard_risk_approved():
    r = _skip_result(70.0, 38.0)
    g = _map_result(r, 70.0)
    assert g["approved"] is True
    assert g["score"] == 70.0
    assert g["intel_score"] == 38.0
    assert g.get("second_score_mode") == "observe_only"

def test_02_score72_intel45_no_hard_risk_approved():
    r = _skip_result(72.0, 45.0)
    g = _map_result(r, 72.0)
    assert g["approved"] is True
    assert g["score"] == 72.0

def test_03_score69_below_threshold_rejected(monkeypatch):
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    r = _skip_result(69.0, 45.0)
    g = _map_result(r, 69.0)
    assert g["approved"] is False

def test_04_risk_manager_reject_blocks(monkeypatch):
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    r = _skip_result(70.0, 45.0,
                     reasoning_override="Risk Manager: daily loss limit reached",
                     risk_approved=False, risk_reason="daily loss limit")
    g = _map_result(r, 70.0)
    assert g["approved"] is False

def test_05_contract_quality_failed_blocks(monkeypatch):
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    r = _skip_result(70.0, 45.0,
                     reasoning_override="Contract quality failed: spread too wide")
    g = _map_result(r, 70.0)
    assert g["approved"] is False

def test_06_scanner_neutral_blocks(monkeypatch):
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    r = _skip_result(70.0, 30.0,
                     reasoning_override="Scanner signal is neutral — no directional setup")
    g = _map_result(r, 70.0)
    assert g["approved"] is False

def test_07_hard_risk_veto_blocks(monkeypatch):
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    r = _skip_result(70.0, 40.0, risk_approved=False, risk_reason="risk veto: capital")
    g = _map_result(r, 70.0)
    assert g["approved"] is False

def test_08_case_variations_handled():
    """Mixed case must still detect hard blocks."""
    for phrase in ("INSUFFICIENT EDGE", "Insufficient Edge", "insufficient edge"):
        r = _skip_result(70.0, 38.0, reasoning_override=f"Score 38.0 < 60 — {phrase}")
        g = _map_result(r, 70.0)
        assert g["approved"] is True, f"phrase={phrase}"

def test_08b_case_variations_hard_block(monkeypatch):
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    for phrase in ("RISK MANAGER:", "risk manager:", "Risk Manager:"):
        r = _skip_result(70.0, 38.0,
                         reasoning_override=f"{phrase} daily loss limit",
                         risk_approved=False, risk_reason="daily loss")
        g = _map_result(r, 70.0)
        assert g["approved"] is False, f"phrase={phrase}"


# ══════════════════════════════════════════════════════════════════════════════
# Missing fundamentals — audit only (tests 9–13)
# ══════════════════════════════════════════════════════════════════════════════

def test_09_explicit_missing_count_logged():
    r = _skip_result()
    r["signal_breakdown"]["fundamentals"]["missing_fundamental_count"] = 5
    assert _count_missing(r) == 5

def test_10_unavailable_fields_list_counted():
    r = _skip_result()
    r["signal_breakdown"]["fundamentals"]["unavailable_fields"] = ["pe", "ev", "roe"]
    assert _count_missing(r) == 3

def test_11_fundamentals_available_false_counted():
    r = _skip_result()
    r["signal_breakdown"]["fundamentals"]["available"] = False
    assert _count_missing(r) == 7

def test_12_neutral_conf50_fallback_only():
    r = _skip_result(fund_signal="neutral", fund_confidence=50.0)
    assert _count_missing(r) == 7
    r2 = _skip_result(fund_signal="neutral", fund_confidence=75.0)
    assert _count_missing(r2) == 0

def test_13_missing_count_zero_does_NOT_block_fallback():
    """Even with missing_count=0, scanner fallback must still approve."""
    r = _skip_result(70.0, 38.0, fund_signal="neutral", fund_confidence=75.0)
    assert _count_missing(r) == 0
    g = _map_result(r, 70.0)
    assert g["approved"] is True, (
        "missing_count=0 must NOT block scanner fallback when insufficient edge "
        "+ no hard risk + scanner_score>=70"
    )


# ══════════════════════════════════════════════════════════════════════════════
# Overnight reeval — gating intel re-score but not hard safety (tests 14–19)
# ══════════════════════════════════════════════════════════════════════════════

def test_14_score_recheck_disabled_constant_exists():
    assert "_OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED" in OV_SRC

def test_15_intel_block_proceeds_to_arm_when_recheck_disabled():
    """Intel-only MC rejection + recheck disabled → PROCEED to arm, not skip/reject."""
    assert "OVERNIGHT_SCORE_RECHECK_DISABLED" in OV_SRC
    assert "PROCEED_TO_ARM" in OV_SRC
    assert "second_score_mode=observe_only" in OV_SRC
    idx = OV_SRC.find("decision=PROCEED_TO_ARM")
    region = OV_SRC[idx:idx+1800]
    assert "_mark_job_rejected" not in region.split("else:")[0], (
        "PROCEED branch must not reach _mark_job_rejected before else:"
    )
    assert "Fall through to Step 5" in region

def test_15a_score_block_proceeds_to_arm_when_recheck_disabled():
    """Morning blocked_score rechecks must stay observe-only for WATCHING rows."""
    for phrase in (
        "blocked_score",
        "rejected_low_score",
        "score_below_floor",
        "score_below_priority_floor",
        "context_below_floor",
        "tier_reject",
    ):
        assert phrase in OV_SRC, f"missing observe-only score phrase: {phrase}"
    assert "SCORE_BELOW_THRESHOLD" in OV_SRC

def test_15d_hydrate_plan_helper_exists():
    assert "_hydrate_plan_from_signal" in OV_SRC

def test_15e_hydrate_plan_carries_required_fields():
    """The hydrated plan must include all fields downstream code uses."""
    for attr in ("ticker", "side", "score", "timeframe", "entry_trigger",
                 "trigger_price", "prior_day_high", "prior_day_low",
                 "contract_symbol", "contracts", "metadata"):
        assert f'{attr}' in OV_SRC, f"hydrate plan missing field: {attr}"

def test_15f_intel_block_does_not_increment_skipped():
    """The intel-block PROCEED branch must not increment skipped."""
    idx = OV_SRC.find("decision=PROCEED_TO_ARM")
    end = OV_SRC.find("Fall through to Step 5", idx)
    branch_body = OV_SRC[idx:end]
    assert 'result["skipped"]' not in branch_body, (
        "Intel-block PROCEED branch must not increment skipped"
    )

def test_15g_score_recheck_reason_code_is_classified_observe_only():
    """Overnight score recheck should key off SCORE_BELOW_THRESHOLD explicitly."""
    assert "_observe_only_reason_codes" in OV_SRC
    idx = OV_SRC.find("_observe_only_reason_codes")
    region = OV_SRC[idx:idx+250]
    assert "SCORE_BELOW_THRESHOLD" in region
    assert "_reason_code in _observe_only_reason_codes" in OV_SRC

def test_15b_hard_safety_block_still_rejects():
    """Hard safety blocks (capital/kill switch/etc) must still reject."""
    assert "_is_hard_safety_block" in OV_SRC
    idx = OV_SRC.find('_is_hard_safety_block')
    region = OV_SRC[idx:idx+3500]
    assert "_mark_job_rejected" in region
    assert "Hard safety block OR recheck enabled" in OV_SRC

def test_15c_hard_safety_phrases_complete():
    """All required hard safety phrases must be present."""
    for phrase in ("capital", "kill_switch", "auth", "invalid_side",
                   "risk_limit", "daily_loss", "risk manager",
                   "contract quality failed", "scanner signal is neutral"):
        assert phrase in OV_SRC, f"missing hard safety phrase: {phrase}"

def test_16_snapshot_unavailable_keeps_watching():
    assert '"OVERNIGHT_SNAPSHOT_FAIL_CLOSED", "false"' in OV_SRC
    idx = OV_SRC.find("RETRY_LATER")
    region = OV_SRC[idx:idx+200]
    assert "_mark_job_rejected" not in region

def test_17_prior_levels_unavailable_retry_later():
    """Broker/history failure → RETRY_LATER, not reject. Side-specific."""
    assert "OVERNIGHT_PRIOR_LEVELS_UNAVAILABLE" in OV_SRC
    # RED-ON-MAIN CLEANUP (PR #265): the structured log line grew (marker
    # taxonomy from the paper-rescue PRs) past the old fixed 600-char
    # window. Bound the region at the branch's own terminating `continue`
    # instead of a fixed width — the invariant is that THIS branch skips
    # and continues without rejecting; downstream branches may legitimately
    # reject for other reasons.
    idx = OV_SRC.find("OVERNIGHT_PRIOR_LEVELS_UNAVAILABLE")
    end = OV_SRC.find("continue", idx) + len("continue")
    region = OV_SRC[idx:end]
    assert 'result["skipped"]' in region
    assert "continue" in region
    assert "_mark_job_rejected" not in region

def test_17b_prior_levels_check_is_side_specific():
    """CALL checks prior_day_high; PUT checks prior_day_low."""
    assert '_side_upper == "CALL"' in OV_SRC
    assert '_side_upper == "PUT"'  in OV_SRC
    idx = OV_SRC.find('_side_upper == "CALL"')
    region = OV_SRC[idx:idx+200]
    assert "prior_day_high" in region
    idx2 = OV_SRC.find('_side_upper == "PUT"')
    region2 = OV_SRC[idx2:idx2+200]
    assert "prior_day_low" in region2

def test_17c_call_missing_high_with_low_present_retries():
    """CALL signal: prior_day_high unavailable, prior_day_low present → RETRY_LATER.
    Behavioral check: the code path tests _side_upper == 'CALL' and prior_day_high is None
    independently of prior_day_low value."""
    idx = OV_SRC.find('_side_upper == "CALL"')
    region = OV_SRC[idx:idx+150]
    assert "prior_day_high is None" in region
    assert "prior_day_low is None" not in region

def test_17d_put_missing_low_with_high_present_retries():
    """PUT signal: prior_day_low unavailable, prior_day_high present → RETRY_LATER."""
    idx = OV_SRC.find('_side_upper == "PUT"')
    region = OV_SRC[idx:idx+150]
    assert "prior_day_low is None" in region
    assert "prior_day_high is None" not in region

def test_17e_explicit_entry_trigger_bypasses_levels_check():
    """If signal carries entry_trigger, no level fetch is required."""
    idx = OV_SRC.find('_missing_level = None')
    region = OV_SRC[idx:idx+400]
    assert "_has_trigger" in region

def test_17f_log_includes_side_and_missing_field():
    """Log line must include side= and missing= fields."""
    # RED-ON-MAIN CLEANUP (PR #265): the structured log line grew (marker
    # taxonomy from the paper-rescue PRs) past the old fixed 600-char
    # window. Bound the region at the branch's own terminating `continue`
    # instead of a fixed width — the invariant is that THIS branch skips
    # and continues without rejecting; downstream branches may legitimately
    # reject for other reasons.
    idx = OV_SRC.find("OVERNIGHT_PRIOR_LEVELS_UNAVAILABLE")
    end = OV_SRC.find("continue", idx) + len("continue")
    region = OV_SRC[idx:end]
    assert "side=%s missing=%s" in region or ("side=" in region and "missing=" in region)

def test_17g_prior_day_session_mismatch_fails_closed_before_arming():
    """Mismatched/missing broker prior_day_date must not arm from stale fresh levels."""
    assert "_PRIOR_LEVEL_CACHE_ENABLED" in OV_SRC
    assert "_prior_trading_session_date" in OV_SRC
    assert 'prior_levels.get("prior_day_date")' in OV_SRC
    assert "PRIOR_LEVEL_SESSION_MISMATCH" in OV_SRC
    idx = OV_SRC.find("PRIOR_LEVEL_SESSION_MISMATCH")
    region = OV_SRC[idx:idx+1200]
    assert "prior_day_high = None" in region
    assert "prior_day_low = None" in region
    assert "_get_cached_prior_levels" in OV_SRC


def test_17h_cache_fallback_requires_session_matched_cache_only():
    """Fallback after mismatch/null fresh levels must use session-matched cache only."""
    idx = OV_SRC.find("_get_cached_prior_levels")
    region = OV_SRC[idx:idx+700]
    assert 'rec.get("session_date") != expected_session.isoformat()' in region
    idx2 = OV_SRC.find("if _cached:")
    region2 = OV_SRC[idx2:idx2+700]
    assert 'prior_day_high = float(_cached.get("prior_day_high") or 0) or None' in region2
    assert 'prior_day_low = float(_cached.get("prior_day_low") or 0) or None' in region2
    assert "PRIOR_LEVEL_CACHE_FALLBACK_USED" in region2
    assert "_expected_session.isoformat()" in region2

def test_18_stale_signal_still_rejects():
    assert "stale_signal" in OV_SRC

def test_19_invalid_timeframe_still_rejects():
    assert "intraday_timeframe_rejected" in OV_SRC


# ══════════════════════════════════════════════════════════════════════════════
# Protect _fetch_watching_signals existing behavior (tests 20–22)
# ══════════════════════════════════════════════════════════════════════════════

def test_20_fetch_reads_both_trade_queue_and_ap_signals():
    assert "trade_queue" in OV_SRC
    assert "ap_signals" in OV_SRC
    assert "decision_status" in OV_SRC
    assert 'status = \'WATCHING\'' in OV_SRC or 'status="WATCHING"' in OV_SRC
    assert '"decision_status", "WATCHING"' in OV_SRC

def test_21_fetch_does_not_filter_ap_signals_by_client_email():
    """ap_signals query must not filter by client_email."""
    idx = OV_SRC.find('sb.table("ap_signals")')
    assert idx > 0
    region = OV_SRC[idx:idx+800]
    assert '.eq("client_email"' not in region
    assert "client_email=" not in region or "Do NOT filter ap_signals by client_email" in OV_SRC

def test_22_fetch_logs_total_trade_queue_ap_signals():
    """The fetch function logs '_fetch_watching_signals: total=X trade_queue=Y ap_signals=Z'."""
    assert "_fetch_watching_signals: total=" in OV_SRC
    assert "trade_queue=" in OV_SRC
    assert "ap_signals=" in OV_SRC

def test_22b_shared_ap_signals_dedup_by_setup_identity():
    """Shared ap_signals are deduped by (ticker, side, timeframe, entry_trigger)."""
    assert "_seen_setup_keys" in OV_SRC


# ══════════════════════════════════════════════════════════════════════════════
# Overnight order metadata preservation
# ══════════════════════════════════════════════════════════════════════════════

def test_23_overnight_create_entry_order_preserves_plan_metadata():
    """Overnight/deferred OSM create must pass plan metadata through."""
    assert "decision.plan.metadata" in OV_SRC
    assert "meta=_plan_meta" in OV_SRC
    idx = OV_SRC.find('_plan_meta = getattr(decision.plan, "metadata", None) or {}')
    region = OV_SRC[idx:idx+700]
    assert "create_entry_order(" in region
    assert "meta=_plan_meta" in region


def test_24_overnight_metadata_comment_lists_required_fields():
    """The required overnight metadata fields must stay called out in code."""
    idx = OV_SRC.find("Pass decision.plan.metadata into OSM")
    region = OV_SRC[idx:idx+500]
    for field in ("sizing_context", "contract_deferred", "risk_profile_source", "snapshot_at_eval"):
        assert field in region, f"missing overnight metadata field note: {field}"


# ══════════════════════════════════════════════════════════════════════════════
# Approved log includes all required fields
# ══════════════════════════════════════════════════════════════════════════════

def test_log_includes_required_fields(caplog):
    import logging
    with caplog.at_level(logging.INFO, logger="intelligence_bridge"):
        r = _skip_result(70.0, 38.0)
        _map_result(r, 70.0)
    combined = " ".join(caplog.messages)
    assert "scanner_score" in combined
    assert "intel_score" in combined
    assert "effective_score" in combined
    assert "second_score_mode=observe_only" in combined
    assert "missing_fundamental_count" in combined
    assert "GATE_G_SCANNER_APPROVED" in combined
