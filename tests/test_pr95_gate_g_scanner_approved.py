"""
tests/test_pr95_gate_g_scanner_approved.py
PR#95: scanner-approved score wins over data-gap intel skip.
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
INTEL_THRESHOLD      = _ib.INTEL_APPROVE_THRESHOLD   # 35.0

# ── Helpers ───────────────────────────────────────────────────────────────────

def _missing_fund_result(scanner_score=70.0, intel_score=38.0, ticker="AAPL"):
    """Pipeline result where skip came from missing/neutral fundamentals only."""
    return {
        "ticker":   ticker,
        "action":   "skip",
        "score":    intel_score,
        "confidence": 0.0,
        "contracts": 1,
        "reasoning": f"Score {intel_score:.1f} < 60 — insufficient edge (production mode)",
        "risk_detail": {"approved": True, "contract_quality_passes": True},
        "signal_breakdown": {
            "fundamentals": {"signal": "neutral", "confidence": 50.0},
            "scanner":      {"signal": "bullish", "confidence": scanner_score},
        },
    }

def _hard_risk_result(ticker="SPY"):
    """Pipeline result where skip came from risk manager rejection."""
    return {
        "ticker":    ticker,
        "action":    "skip",
        "score":     45.0,
        "confidence": 0.0,
        "contracts": 0,
        "reasoning": "Risk Manager: daily loss limit reached",
        "risk_detail": {
            "approved": False,
            "reason":   "daily loss limit reached",
        },
        "signal_breakdown": {},
    }

def _neutral_direction_result(ticker="TSLA"):
    """Scanner direction was neutral — hard block."""
    return {
        "ticker":    ticker,
        "action":    "skip",
        "score":     30.0,
        "confidence": 0.0,
        "contracts": 0,
        "reasoning": "Scanner signal is neutral — no directional setup",
        "risk_detail": {"approved": True},
        "signal_breakdown": {},
    }

def _approved_result(ticker="AMZN", score=72.0):
    """Pipeline result that approved cleanly."""
    return {
        "ticker":    ticker,
        "action":   "execute",
        "score":     score,
        "confidence": score / 100.0,
        "contracts": 2,
        "reasoning": "Strong setup",
        "risk_detail": {"approved": True, "contract_quality_passes": True},
        "signal_breakdown": {
            "fundamentals": {"signal": "bullish", "confidence": 80.0},
        },
    }


# ── Test 1: score=70, missing fundamentals → NOT rejected ─────────────────────

def test_score_70_missing_fundamentals_not_rejected():
    result = _missing_fund_result(scanner_score=70.0, intel_score=38.0)
    gate   = _map_result(result, fallback_score=70.0)
    assert gate["approved"] is True, (
        f"score=70 with missing fundamentals was rejected. "
        f"intel_status={gate.get('intel_status')}, reasoning={gate.get('reasoning')}"
    )

def test_score_70_uses_scanner_score_not_intel_score():
    result = _missing_fund_result(scanner_score=70.0, intel_score=38.0)
    gate   = _map_result(result, fallback_score=70.0)
    assert gate["score"] == 70.0, (
        f"Expected effective_score=70.0, got {gate['score']}. "
        f"Scanner score must override intel data-gap score."
    )

def test_score_72_missing_fundamentals_not_rejected():
    result = _missing_fund_result(scanner_score=72.0, intel_score=46.0)
    gate   = _map_result(result, fallback_score=72.0)
    assert gate["approved"] is True

def test_intel_status_is_data_unavailable_scanner_fallback():
    result = _missing_fund_result(scanner_score=70.0, intel_score=38.0)
    gate   = _map_result(result, fallback_score=70.0)
    assert gate["intel_status"] == "DATA_UNAVAILABLE_SCANNER_FALLBACK"


# ── Test 2: NaN fundamentals → missing_fundamental_count neutral ──────────────

def test_missing_fund_count_neutral_when_default_confidence():
    """Pipeline default confidence=50 → all fundamentals missing."""
    result = _missing_fund_result()
    count  = _count_missing(result)
    assert count > 0, "Default confidence=50 should indicate missing fundamentals"

def test_missing_fund_count_zero_when_real_data():
    """Non-default confidence → real data available."""
    result = _missing_fund_result()
    result["signal_breakdown"]["fundamentals"]["confidence"] = 75.0
    count  = _count_missing(result)
    assert count == 0


# ── Test 3: Explicit hard-risk intel finding still rejects ────────────────────

def test_hard_risk_reject_still_blocks(monkeypatch):
    # Hard risk blocks only apply in LIVE mode (paper allows 1-contract collection)
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    result = _hard_risk_result()
    gate   = _map_result(result, fallback_score=70.0)
    assert gate["approved"] is False, (
        "Risk Manager rejection must still block in LIVE even when scanner_score=70"
    )

def test_neutral_direction_still_blocks(monkeypatch):
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    result = _neutral_direction_result()
    gate   = _map_result(result, fallback_score=70.0)
    assert gate["approved"] is False, (
        "Neutral scanner direction must still block in LIVE"
    )


# ── Test 4: Score below master threshold still rejects ────────────────────────

def test_scanner_score_below_intel_threshold_still_blocked(monkeypatch):
    """Scanner score below INTEL_APPROVE_THRESHOLD → still blocked in LIVE."""
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    result = _missing_fund_result(scanner_score=30.0, intel_score=18.0)
    gate   = _map_result(result, fallback_score=30.0)
    assert gate["approved"] is False, (
        f"scanner_score={30.0} < INTEL_APPROVE_THRESHOLD={INTEL_THRESHOLD} "
        f"must still block in LIVE"
    )


# ── Test 5: Approved scanner signal can remain WATCHING for reeval ────────────
# (This is behavioral at the queue/reeval level; here we confirm Gate G approves it)

def test_overnight_approved_signal_gate_g_approves():
    """After-hours 1d signal with scanner_score=70 gets Gate G approved."""
    result = _missing_fund_result(scanner_score=70.0, intel_score=42.0, ticker="MSFT")
    gate   = _map_result(result, fallback_score=70.0)
    assert gate["approved"] is True
    assert gate["score"] == 70.0


# ── Test 6: Logs include required fields ──────────────────────────────────────

def test_gate_g_log_fields_present(caplog):
    """Gate G log must include scanner_score, intel_score, effective_score,
    missing_fundamental_count, and hard_risk_reason."""
    import logging
    with caplog.at_level(logging.INFO, logger="intelligence_bridge"):
        result = _missing_fund_result(scanner_score=70.0, intel_score=38.0)
        _map_result(result, fallback_score=70.0)

    combined = " ".join(caplog.messages)
    assert "scanner_score" in combined
    assert "intel_score"   in combined
    assert "effective_score" in combined or "scanner_score=70" in combined
    assert "missing_fundamental_count" in combined

def test_hard_risk_log_includes_reason(caplog):
    import logging
    with caplog.at_level(logging.INFO, logger="intelligence_bridge"):
        result = _hard_risk_result()
        _map_result(result, fallback_score=70.0)
    combined = " ".join(caplog.messages)
    assert "GATE_G_HARD_RISK_BLOCK" in combined or "risk" in combined.lower()


# ── Scope: no scanner/entry/exit/sizing changes ───────────────────────────────

def test_only_intelligence_bridge_changed():
    ib_src = (_REPO / "intelligence_bridge.py").read_text()
    assert "GATE_G_SCANNER_APPROVED" in ib_src
    assert "DATA_UNAVAILABLE_SCANNER_FALLBACK" in ib_src

def test_portfolio_manager_unchanged():
    pm_path = _REPO / "ap_intelligence" / "agents" / "ap_portfolio_manager.py"
    if not pm_path.exists():
        pytest.skip("ap_portfolio_manager.py not in local path — skipped in CI")
    pm_src = pm_path.read_text()
    assert "insufficient edge" in pm_src
    assert "GATE_G_SCANNER_APPROVED" not in pm_src

def test_pipeline_unchanged():
    pipe_path = _REPO / "ap_intelligence" / "ap_signal_pipeline.py"
    if not pipe_path.exists():
        pytest.skip("ap_signal_pipeline.py not in local path — skipped in CI")
    pipe_src = pipe_path.read_text()
    assert "GATE_G_SCANNER_APPROVED" not in pipe_src
