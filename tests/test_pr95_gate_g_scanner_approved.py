"""
tests/test_pr95_gate_g_scanner_approved.py
PR#95 amendment: tighter fallback condition.
All tests are behavioral — they call the actual _map_result function.
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
_map_result          = _ib._map_result
_count_missing       = _ib._count_missing_fundamentals
INTEL_THRESHOLD      = _ib.INTEL_APPROVE_THRESHOLD       # 35.0
LIVE_MIN_ELIGIBLE    = _ib.GATE_G_SCANNER_MIN_ELIGIBLE   # 70.0

# ── Helpers ───────────────────────────────────────────────────────────────────

def _skip_result(scanner_score=70.0, intel_score=38.0, ticker="AAPL",
                 fund_signal="neutral", fund_confidence=50.0):
    """Portfolio manager returned skip with given fundamentals context."""
    return {
        "ticker":   ticker,
        "action":   "skip",
        "score":    intel_score,
        "confidence": 0.0,
        "contracts": 1,
        "reasoning": f"Score {intel_score:.1f} < 60 — insufficient edge (production mode)",
        "risk_detail": {"approved": True, "contract_quality_passes": True},
        "signal_breakdown": {
            "fundamentals": {"signal": fund_signal, "confidence": fund_confidence},
            "scanner":      {"signal": "bullish",   "confidence": scanner_score},
        },
    }

def _approved_result(ticker="AMZN", score=72.0):
    return {
        "ticker": ticker, "action": "execute", "score": score,
        "confidence": score / 100.0, "contracts": 2,
        "reasoning": "Strong setup",
        "risk_detail": {"approved": True, "contract_quality_passes": True},
        "signal_breakdown": {
            "fundamentals": {"signal": "bullish", "confidence": 80.0},
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# 1. score=70 + missing fundamentals + insufficient edge → approved fallback
# ══════════════════════════════════════════════════════════════════════════════

def test_score70_missing_fundamentals_insufficient_edge_approved():
    """Core case: scanner=70, intel=38, fundamentals missing → fallback approved."""
    result = _skip_result(scanner_score=70.0, intel_score=38.0,
                          fund_signal="neutral", fund_confidence=50.0)  # default = missing
    gate   = _map_result(result, fallback_score=70.0)
    assert gate["approved"] is True, (
        f"score=70 + missing fundamentals + insufficient edge must be approved. "
        f"Got approved={gate['approved']} status={gate.get('intel_status')}"
    )

def test_fallback_effective_score_equals_scanner_score():
    """Effective score must be the scanner score (70), not the intel score (38)."""
    result = _skip_result(scanner_score=70.0, intel_score=38.0,
                          fund_signal="neutral", fund_confidence=50.0)
    gate   = _map_result(result, fallback_score=70.0)
    assert gate["score"] == 70.0, (
        f"effective_score must be scanner_score=70.0, got {gate['score']}"
    )

def test_fallback_status_is_data_unavailable_scanner_fallback():
    result = _skip_result(scanner_score=70.0, intel_score=38.0,
                          fund_signal="neutral", fund_confidence=50.0)
    gate   = _map_result(result, fallback_score=70.0)
    assert gate["intel_status"] == "DATA_UNAVAILABLE_SCANNER_FALLBACK"

def test_score72_missing_fundamentals_also_approved():
    result = _skip_result(scanner_score=72.0, intel_score=46.0,
                          fund_signal="neutral", fund_confidence=50.0)
    gate   = _map_result(result, fallback_score=72.0)
    assert gate["approved"] is True
    assert gate["score"] == 72.0


# ══════════════════════════════════════════════════════════════════════════════
# 2. score=70 + real fundamentals present + insufficient edge → REJECTED
# ══════════════════════════════════════════════════════════════════════════════

def test_score70_real_fundamentals_insufficient_edge_rejected(monkeypatch):
    """Real fundamentals (confidence != 50) + low intel score → keep the reject."""
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    result = _skip_result(scanner_score=70.0, intel_score=42.0,
                          fund_signal="neutral", fund_confidence=75.0)  # real data
    gate   = _map_result(result, fallback_score=70.0)
    assert gate["approved"] is False, (
        "Real fundamentals present but intel score still < 60 must stay rejected. "
        f"Got approved={gate['approved']}"
    )

def test_missing_count_zero_when_real_fundamentals():
    """Confirm _count_missing returns 0 when confidence != 50."""
    result = _skip_result(fund_signal="neutral", fund_confidence=75.0)
    assert _count_missing(result) == 0

def test_missing_count_nonzero_when_default_confidence():
    """Confirm _count_missing returns > 0 when confidence == 50 (pipeline default)."""
    result = _skip_result(fund_signal="neutral", fund_confidence=50.0)
    assert _count_missing(result) > 0


# ══════════════════════════════════════════════════════════════════════════════
# 3. score=70 + risk manager reject → REJECTED
# ══════════════════════════════════════════════════════════════════════════════

def test_score70_risk_manager_reject_blocks(monkeypatch):
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    result = {
        "ticker": "SPY", "action": "skip", "score": 45.0, "confidence": 0.0,
        "contracts": 0,
        "reasoning": "Risk Manager: daily loss limit reached",
        "risk_detail": {"approved": False, "reason": "daily loss limit reached"},
        "signal_breakdown": {},
    }
    gate = _map_result(result, fallback_score=70.0)
    assert gate["approved"] is False, "Risk Manager rejection must block in LIVE"


# ══════════════════════════════════════════════════════════════════════════════
# 4. score=70 + neutral direction → REJECTED
# ══════════════════════════════════════════════════════════════════════════════

def test_score70_neutral_direction_blocks(monkeypatch):
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    result = {
        "ticker": "TSLA", "action": "skip", "score": 30.0, "confidence": 0.0,
        "contracts": 0,
        "reasoning": "Scanner signal is neutral — no directional setup",
        "risk_detail": {"approved": True},
        "signal_breakdown": {},
    }
    gate = _map_result(result, fallback_score=70.0)
    assert gate["approved"] is False, "Neutral direction must block in LIVE"


# ══════════════════════════════════════════════════════════════════════════════
# 5. score=70 + contract quality failed → REJECTED
# ══════════════════════════════════════════════════════════════════════════════

def test_score70_contract_quality_failed_blocks(monkeypatch):
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    result = {
        "ticker": "AAPL", "action": "skip", "score": 40.0, "confidence": 0.0,
        "contracts": 0,
        "reasoning": "Contract quality failed: spread too wide",
        "risk_detail": {"approved": True},
        "signal_breakdown": {},
    }
    gate = _map_result(result, fallback_score=70.0)
    assert gate["approved"] is False, "Contract quality failure must block in LIVE"


# ══════════════════════════════════════════════════════════════════════════════
# 6. effective_score=scanner_score ONLY when missing data is confirmed
# ══════════════════════════════════════════════════════════════════════════════

def test_effective_score_only_when_missing_data_confirmed(monkeypatch):
    """When fundamentals are present, effective_score must not be scanner_score."""
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    # Real fundamentals: confidence=75 → _missing_count=0 → fallback must NOT apply
    result = _skip_result(scanner_score=70.0, intel_score=42.0,
                          fund_signal="neutral", fund_confidence=75.0)
    gate   = _map_result(result, fallback_score=70.0)
    assert gate["score"] != 70.0 or gate["approved"] is False, (
        "When fundamentals are present, effective_score must not be scanner_score=70"
    )

def test_effective_score_is_scanner_score_when_missing_confirmed():
    """When data is confirmed missing, effective_score == scanner_score."""
    result = _skip_result(scanner_score=70.0, intel_score=38.0,
                          fund_signal="neutral", fund_confidence=50.0)
    gate   = _map_result(result, fallback_score=70.0)
    assert gate["approved"] is True
    assert gate["score"] == 70.0


# ══════════════════════════════════════════════════════════════════════════════
# Threshold: scanner score below GATE_G_SCANNER_MIN_ELIGIBLE (70) still blocked
# ══════════════════════════════════════════════════════════════════════════════

def test_scanner_score_below_live_min_eligible_still_blocked(monkeypatch):
    """scanner_score=65 < GATE_G_SCANNER_MIN_ELIGIBLE=70 → no fallback."""
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    result = _skip_result(scanner_score=65.0, intel_score=38.0,
                          fund_signal="neutral", fund_confidence=50.0)
    gate   = _map_result(result, fallback_score=65.0)
    assert gate["approved"] is False, (
        f"scanner_score=65 < GATE_G_SCANNER_MIN_ELIGIBLE={LIVE_MIN_ELIGIBLE} must block"
    )

def test_gate_g_scanner_min_eligible_default_is_70():
    assert LIVE_MIN_ELIGIBLE == 70.0, (
        f"GATE_G_SCANNER_MIN_ELIGIBLE must default to 70.0, got {LIVE_MIN_ELIGIBLE}"
    )

def test_intel_approve_threshold_not_used_for_fallback():
    """The 35.0 threshold must not be the fallback gate — 70 is."""
    assert LIVE_MIN_ELIGIBLE > INTEL_THRESHOLD, (
        "GATE_G_SCANNER_MIN_ELIGIBLE must be stricter than INTEL_APPROVE_THRESHOLD"
    )


# ══════════════════════════════════════════════════════════════════════════════
# Logging: required fields present
# ══════════════════════════════════════════════════════════════════════════════

def test_gate_g_approved_log_fields(caplog):
    import logging
    with caplog.at_level(logging.INFO, logger="intelligence_bridge"):
        result = _skip_result(scanner_score=70.0, intel_score=38.0,
                              fund_signal="neutral", fund_confidence=50.0)
        _map_result(result, fallback_score=70.0)
    combined = " ".join(caplog.messages)
    assert "scanner_score" in combined
    assert "intel_score"   in combined
    assert "missing_fundamental_count" in combined
    assert "GATE_G_SCANNER_APPROVED" in combined
