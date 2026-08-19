"""
tests/test_p0_master_control_sector_identity.py
PR #483 amendment — CANONICAL SECTOR AUTHORITY.

Master Control no longer maintains any local sector membership map.
ap.exposure_gate.get_sector() is the single canonical resolver for every
active-money sector decision. Master Control's own local SECTOR_MAP was
removed entirely because it was a second, smaller, competing taxonomy
that caused two proven defects:

    1. unmapped ticker -> synthetic "other" bucket -> false shared
       correlation -> 22 revalidate_sector_cap_other blocks in Jason's
       LIVE trade-flow audit (2026-08-06..2026-08-17).
    2. canonically-known tickers (QCOM, ARM, SMCI, NOW, AVGO, INTC,
       PLTR, ...) absent from the smaller legacy map resolved to
       "unknown" and silently lost sector-cap protection, even though
       the repository's own canonical resolver already knew them.

Invariant under test:
    canonical-known sector -> enforce existing sector-cap dollar math
    genuinely unknown sector -> sector=None, never aggregated with other
        unknowns, sector-specific cap skipped, every other independent
        risk gate still runs
    never: unknown -> "other"
    never: canonical-known ticker -> None merely because it was missing
        from a smaller, now-removed local map
"""
from __future__ import annotations

import inspect
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import ap_master_control as mc_mod
from ap.exposure_gate import get_sector as canonical_get_sector

_REPO = Path(__file__).resolve().parents[1]
MC_SRC = _REPO.joinpath("ap_master_control.py").read_text()


def _mc():
    """Minimal APMasterControl instance sufficient for the sector-only
    helper methods, without running __init__ (which requires a live
    DB/broker context)."""
    return mc_mod.APMasterControl.__new__(mc_mod.APMasterControl)


def _pos(ticker: str, price: float = 2.00, qty: int = 1) -> dict:
    return {"underlying": ticker, "avg_fill": price, "quantity_remaining": qty}


def _snap(open_positions=None, closing_positions=None):
    return {
        "_snapshot_ok": True,
        "_snapshot_ts": "2026-08-19T00:00:00Z",
        "_snapshot_age_sec": 0.1,
        "open_count": len(open_positions or []),
        "pending_entries": 0,
        "capital_deployed": 0.0,
        "calls_open": 0,
        "puts_open": 0,
        "trades_today": 0,
        "open_positions": open_positions or [],
        "closing_positions": closing_positions or [],
        "realized_pnl_today": 0.0,
        "total_trades": 0,
        "ticker_open_counts": {},
        "ticker_pending_counts": {},
    }


def _make_revalidation_mc(**overrides) -> mc_mod.APMasterControl:
    kwargs = dict(
        mode="paper",
        score_floor=60.0,
        context_floor=0.0,
        account_equity=5000.0,
        max_capital_pct=0.10,
        max_sector_pct=0.25,
        max_ticker_pct=0.10,
        pending_capital_fail_closed_live=False,
        require_snapshot_freshness_live=False,
    )
    kwargs.update(overrides)
    m = mc_mod.APMasterControl(**kwargs)
    m._kill_switch_fn = lambda: False
    m._equity_snapshot = MagicMock(return_value=(kwargs["account_equity"], 0.0))
    m._alert_degraded = MagicMock()
    m._pending_capital_from_snapshot_or_db = MagicMock(return_value=0.0)
    m._get_pending_capital_breakdown = MagicMock(
        return_value={
            "pending_submitted_entry_exposure": 0.0,
            "filled_unreconciled_exposure": 0.0,
            "pending_total_capital_reserved": 0.0,
        }
    )
    return m


def _plan(ticker: str, max_position_usd: float, contracts: int = 1) -> mc_mod.ApprovedExecutionPlan:
    return mc_mod.ApprovedExecutionPlan(
        plan_id="p-483", signal_id="s-483", client_id="jason@example.com",
        ticker=ticker, side="CALL", direction="CALL", pattern="BREAKOUT",
        timeframe="1d", contracts=contracts, max_position_usd=max_position_usd,
        tier="A", score=80.0, intel_score=0.0, confidence_bucket="standard_pool",
        trigger_type="breach", trigger_price=100.0, stop_underlying=99.0,
        target_underlying=105.0, metadata={"local_order_id": "lo-483"},
        mode="PAPER", paper_sim=True,
    )


def _revalidate(m, plan):
    def _run(fn, **kw):
        return fn()
    with patch("ap.db.conn", side_effect=RuntimeError("no db in test")), \
         patch("ap.db.run_with_retry", side_effect=_run):
        return m.revalidate_exposure(plan, client_id="jason@example.com")


# ══════════════════════════════════════════════════════════════════════════
# Canonical authority — no local map, single resolver, import-cycle safe
# ══════════════════════════════════════════════════════════════════════════

def test_master_control_has_no_local_sector_map():
    assert not hasattr(mc_mod.APMasterControl, "SECTOR_MAP"), (
        "Master Control must not maintain a second, competing sector "
        "membership map — ap.exposure_gate.get_sector() is the sole "
        "canonical authority"
    )


def test_master_control_imports_canonical_resolver():
    assert "from ap.exposure_gate import get_sector" in MC_SRC


def test_exposure_gate_has_no_dependency_back_on_master_control():
    """Import-cycle proof: ap.exposure_gate does not import
    ap_master_control (or anything that transitively would)."""
    import ap.exposure_gate as eg
    src = inspect.getsource(eg)
    assert "ap_master_control" not in src


def test_master_control_module_imports_cleanly():
    assert hasattr(mc_mod, "APMasterControl")
    assert hasattr(mc_mod.APMasterControl, "_resolve_sector")


def test_no_other_fallback_remains_in_authoritative_sector_code():
    assert '.get(ticker.upper(), "other")' not in MC_SRC
    assert '.get(ticker_in_pos.upper(), "other")' not in MC_SRC
    assert "SECTOR_MAP" not in MC_SRC.replace(
        "# PR #483 amendment: removed local SECTOR_MAP entirely. Sector", ""
    )


# ══════════════════════════════════════════════════════════════════════════
# A/B. QCOM / ARM / SMCI / NOW canonical technology protection
# ══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("ticker", ["QCOM", "ARM", "SMCI", "NOW", "AVGO", "INTC", "PLTR"])
def test_canonical_tech_tickers_resolve_through_master_control(ticker):
    """These tickers are known to the canonical resolver but were absent
    from Master Control's old local map — must resolve to tech, not
    None, and must not have sector-cap protection silently disabled."""
    resolved = mc_mod.APMasterControl._resolve_sector(ticker)
    assert resolved == "tech", f"{ticker} must resolve to tech via the canonical authority"


def test_qcom_technology_replay_counts_existing_tech_exposure_and_blocks():
    m = _make_revalidation_mc(
        max_position_pct=0.90, max_total_capital_pct=0.90, max_ticker_pct=0.90,
        max_sector_pct=0.10,  # sector cap = 500
    )
    existing_tech = [_pos("AAPL", price=2.0, qty=1), _pos("NVDA", price=2.0, qty=1)]  # $400 deployed
    m._get_snapshot = MagicMock(return_value=_snap(existing_tech))
    plan = _plan("QCOM", max_position_usd=200.0)  # 400 + 200 = 600 > 500
    decision = _revalidate(m, plan)
    assert decision.ok is False
    assert decision.reason_code == "SECTOR_CAP_BLOCK"
    assert "revalidate_sector_cap_tech" in decision.reason
    assert "sector=None" not in decision.reason
    assert "sector_cap_applied=False" not in decision.reason


def test_arm_smci_now_participate_in_same_tech_cap_math():
    mc = _mc()
    existing = [_pos("ARM", price=2.0, qty=1), _pos("SMCI", price=2.0, qty=1), _pos("NOW", price=2.0, qty=1)]
    deployed = mc._sector_capital_deployed(existing, "tech")
    assert deployed == 3 * 2.0 * 1 * 100


# ══════════════════════════════════════════════════════════════════════════
# C. SPY + QQQ index aggregation
# ══════════════════════════════════════════════════════════════════════════

def test_spy_qqq_resolve_to_index_and_aggregate():
    assert mc_mod.APMasterControl._resolve_sector("SPY") == "index"
    assert mc_mod.APMasterControl._resolve_sector("QQQ") == "index"
    mc = _mc()
    existing = [_pos("SPY", price=5.0, qty=2)]
    deployed = mc._sector_capital_deployed(existing, "index")
    assert deployed == 5.0 * 2 * 100


def test_spy_qqq_index_cap_still_applies():
    m = _make_revalidation_mc(
        max_position_pct=0.90, max_total_capital_pct=0.90, max_ticker_pct=0.90,
        max_sector_pct=0.10,  # sector cap = 500
    )
    existing_index = [_pos("SPY", price=4.0, qty=1)]  # $400 deployed, index
    m._get_snapshot = MagicMock(return_value=_snap(existing_index))
    plan = _plan("QQQ", max_position_usd=200.0)  # 400 + 200 = 600 > 500
    decision = _revalidate(m, plan)
    assert decision.ok is False
    assert "revalidate_sector_cap_index" in decision.reason


# ══════════════════════════════════════════════════════════════════════════
# D. IWM (index) does not pollute unrelated tech exposure
# ══════════════════════════════════════════════════════════════════════════

def test_iwm_index_does_not_pollute_tech_exposure():
    mc = _mc()
    assert mc_mod.APMasterControl._resolve_sector("IWM") == "index"
    existing = [_pos("IWM", price=100.0, qty=5)]  # large index exposure
    tech_deployed = mc._sector_capital_deployed(existing, "tech")
    assert tech_deployed == 0.0


# ══════════════════════════════════════════════════════════════════════════
# E. True unknown + true unknown never aggregate (fail-first proof)
# ══════════════════════════════════════════════════════════════════════════

def test_genuine_unknown_tickers_never_aggregate():
    mc = _mc()
    assert mc_mod.APMasterControl._resolve_sector("DHR") is None
    assert mc_mod.APMasterControl._resolve_sector("PCAR") is None
    positions = [_pos("DHR", price=5.00, qty=2)]
    assert mc._sector_capital_deployed(positions, "other") == 0.0
    assert mc._sector_capital_deployed(positions, None) == 0.0


def test_get_sector_exposure_does_not_bucket_unmapped_tickers_as_other():
    mc = _mc()
    positions = [_pos("DHR", price=5.00, qty=1), _pos("PCAR", price=6.00, qty=1)]
    exposure = mc.get_sector_exposure(positions)
    assert "other" not in exposure
    assert exposure.get("sector_unknown") == (5.00 * 1 * 100) + (6.00 * 1 * 100)


def test_sector_unknown_diagnostic_bucket_never_used_as_risk_authority():
    mc = _mc()
    positions = [_pos("DHR", price=5.00, qty=1), _pos("PCAR", price=6.00, qty=1)]
    assert mc._sector_capital_deployed(positions, "sector_unknown") == 0.0


# ══════════════════════════════════════════════════════════════════════════
# F/G/H. True unknown does not fail open other risk gates
# ══════════════════════════════════════════════════════════════════════════

def test_unknown_candidate_per_position_cap_still_blocks():
    m = _make_revalidation_mc(max_position_pct=0.10)  # budget=500
    m._get_snapshot = MagicMock(return_value=_snap([]))
    plan = _plan("DHR", max_position_usd=600.0)
    decision = _revalidate(m, plan)
    assert decision.ok is False
    assert decision.reason_code == "ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP"


def test_unknown_candidate_total_cap_still_blocks():
    m = _make_revalidation_mc(max_position_pct=0.05, max_total_capital_pct=0.20)
    m._get_snapshot = MagicMock(return_value=_snap([]))
    m._pending_capital_from_snapshot_or_db = MagicMock(return_value=850.0)
    plan = _plan("DHR", max_position_usd=200.0)
    decision = _revalidate(m, plan)
    assert decision.ok is False
    assert decision.reason_code in (
        "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED",
        "ACTUAL_CONTRACT_COST_EXCEEDS_REMAINING_TOTAL_CAPACITY",
    )


def test_unknown_candidate_ticker_cap_still_blocks():
    m = _make_revalidation_mc(max_position_pct=0.50, max_total_capital_pct=0.90, max_ticker_pct=0.05)
    existing = [_pos("DHR", price=2.0, qty=2)]  # $400 in this ticker
    m._get_snapshot = MagicMock(return_value=_snap(existing))
    plan = _plan("DHR", max_position_usd=100.0)
    decision = _revalidate(m, plan)
    assert decision.ok is False
    assert decision.reason_code == "TICKER_CAP_BLOCK"


def test_unknown_candidate_otherwise_valid_proceeds_beyond_sector_gate():
    m = _make_revalidation_mc(max_position_pct=0.50, max_total_capital_pct=0.90, max_ticker_pct=0.50)
    m._get_snapshot = MagicMock(return_value=_snap([]))
    plan = _plan("DHR", max_position_usd=100.0)
    decision = _revalidate(m, plan)
    assert decision.ok is True
    assert decision.reason_code == ""


# ══════════════════════════════════════════════════════════════════════════
# I/J. Known tech under/over cap
# ══════════════════════════════════════════════════════════════════════════

def test_known_tech_under_cap_passes():
    m = _make_revalidation_mc(
        max_position_pct=0.90, max_total_capital_pct=0.90, max_ticker_pct=0.90,
        max_sector_pct=0.50,  # sector cap = 2500, plenty of room
    )
    existing_tech = [_pos("AAPL", price=1.0, qty=1)]  # $100 deployed
    m._get_snapshot = MagicMock(return_value=_snap(existing_tech))
    plan = _plan("MSFT", max_position_usd=100.0)
    decision = _revalidate(m, plan)
    assert decision.ok is True


def test_known_tech_over_cap_blocks():
    m = _make_revalidation_mc(
        max_position_pct=0.90, max_total_capital_pct=0.90, max_ticker_pct=0.90,
        max_sector_pct=0.10,  # sector cap = 500
    )
    existing_tech = [_pos("AAPL", price=4.0, qty=1)]  # $400 deployed
    m._get_snapshot = MagicMock(return_value=_snap(existing_tech))
    plan = _plan("MSFT", max_position_usd=200.0)  # 400+200=600 > 500
    decision = _revalidate(m, plan)
    assert decision.ok is False
    assert decision.reason_code == "SECTOR_CAP_BLOCK"


# ══════════════════════════════════════════════════════════════════════════
# K. Blank/malformed ticker — existing upstream validity semantics
# ══════════════════════════════════════════════════════════════════════════

def test_blank_ticker_never_manufactures_a_sector():
    assert mc_mod.APMasterControl._resolve_sector("") is None
    assert mc_mod.APMasterControl._resolve_sector(None) is None


# ══════════════════════════════════════════════════════════════════════════
# L. Case/whitespace normalization
# ══════════════════════════════════════════════════════════════════════════

def test_case_and_whitespace_normalization():
    assert mc_mod.APMasterControl._resolve_sector("qcom") == mc_mod.APMasterControl._resolve_sector("QCOM")
    assert mc_mod.APMasterControl._resolve_sector(" QCOM ".strip()) == mc_mod.APMasterControl._resolve_sector("QCOM")


# ══════════════════════════════════════════════════════════════════════════
# Sector-decision telemetry (observability only)
# ══════════════════════════════════════════════════════════════════════════

def test_telemetry_known_sector():
    telemetry = mc_mod.APMasterControl._sector_telemetry("tech")
    assert telemetry == {
        "sector_resolution": "known",
        "sector_cap_applied": True,
        "sector_cap_skip_reason": None,
    }


def test_telemetry_unknown_sector():
    telemetry = mc_mod.APMasterControl._sector_telemetry(None)
    assert telemetry == {
        "sector_resolution": "unknown",
        "sector_cap_applied": False,
        "sector_cap_skip_reason": "unknown_sector_identity",
    }


# ══════════════════════════════════════════════════════════════════════════
# Static authority / mutation-freeze proofs
# ══════════════════════════════════════════════════════════════════════════

def test_no_revalidate_sector_cap_other_can_be_emitted():
    body = MC_SRC[MC_SRC.find("def revalidate_exposure("):]
    body = body[: body.find("\n    def _log_capital_utilization")]
    assert "if sector is not None and proj_sector > max_sector:" in body


def test_no_new_broker_submit_or_cancel_authority_introduced():
    body = MC_SRC[MC_SRC.find("def revalidate_exposure("):]
    body = body[: body.find("\n    def _log_capital_utilization")]
    assert "submit_order" not in body
    assert "cancel_order" not in body
    assert "broker.submit" not in body
    assert "broker.cancel" not in body
