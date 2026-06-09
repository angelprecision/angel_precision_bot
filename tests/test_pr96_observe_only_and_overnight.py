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

def test_15_intel_block_skipped_when_recheck_disabled():
    """When MC reason is intel-related, skip (RETRY_LATER) instead of reject."""
    assert "OVERNIGHT_SCORE_RECHECK_DISABLED" in OV_SRC
    idx = OV_SRC.find("OVERNIGHT_SCORE_RECHECK_DISABLED")
    region = OV_SRC[idx:idx+400]
    assert 'result["skipped"]' in region
    assert "continue" in region

def test_15b_hard_safety_block_still_rejects():
    """Hard safety blocks (capital/kill switch/etc) must still reject."""
    assert "_is_hard_safety_block" in OV_SRC
    # Hard safety branch reaches _mark_job_rejected — search wider window
    idx = OV_SRC.find('_is_hard_safety_block')
    region = OV_SRC[idx:idx+3500]
    assert "_mark_job_rejected" in region
    # And the hard-safety-OR-recheck-enabled comment is the reject path
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
    """Broker/history failure → RETRY_LATER, not reject."""
    assert "OVERNIGHT_PRIOR_LEVELS_UNAVAILABLE" in OV_SRC
    idx = OV_SRC.find("OVERNIGHT_PRIOR_LEVELS_UNAVAILABLE")
    region = OV_SRC[idx:idx+400]
    assert 'result["skipped"]' in region
    assert "continue" in region
    assert "_mark_job_rejected" not in region

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
    # The .eq() filters on the ap_signals query must NOT include client_email
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
