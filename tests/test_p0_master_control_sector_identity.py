"""
tests/test_p0_master_control_sector_identity.py
PR #483 — SURGICAL FIX: remove Master Control's synthetic "other"
super-sector. Master Control's existing local SECTOR_MAP is UNCHANGED
(same tickers, same classifications, byte-identical to
main@462106c8839769ef6b3137867839a44aec39ad09). The only production
change is removing the `.get(ticker.upper(), "other")` fallback so an
unmapped ticker resolves to None instead of the literal string "other".

Production finding: Jason's LIVE trade-flow audit 2026-08-06..2026-08-17
found 22 revalidate_sector_cap_other blocks. Unrelated unmapped tickers
were treated as economically correlated because both
get_sector_exposure() and _sector_capital_deployed() defaulted every
unmapped ticker to the literal string "other".

Invariant under test:
    known sector (per the UNCHANGED SECTOR_MAP) -> enforce existing
        sector cap math exactly as before
    unknown sector -> never aggregate with other unknown tickers
    unknown sector -> never produce a "sector_cap_other" style reason
    unknown sector -> every other independent risk gate still runs
        (per-position cap, total cap, ticker cap, affordability, etc.)
"""
from __future__ import annotations

import inspect
from pathlib import Path
from unittest.mock import MagicMock, patch

import ap_master_control as mc_mod

_REPO = Path(__file__).resolve().parents[1]
MC_SRC = _REPO.joinpath("ap_master_control.py").read_text()

# The exact SECTOR_MAP membership on
# main@462106c8839769ef6b3137867839a44aec39ad09 — this PR must not add,
# remove, or reclassify a single entry.
_ORIGINAL_SECTOR_MAP = {
    "AAPL": "tech", "MSFT": "tech", "NVDA": "tech", "AMD": "tech",
    "GOOGL": "tech", "META": "tech", "CRM": "tech", "ORCL": "tech",
    "TSLA": "tech", "AMZN": "tech", "NFLX": "tech", "SNOW": "tech",
    "JPM": "financials", "BAC": "financials", "GS": "financials",
    "MS": "financials", "C": "financials", "WFC": "financials",
    "UNH": "healthcare", "JNJ": "healthcare", "PFE": "healthcare",
    "ABBV": "healthcare", "MRK": "healthcare", "LLY": "healthcare",
    "WMT": "consumer", "COST": "consumer", "TGT": "consumer",
    "LOW": "consumer", "HD": "consumer", "NKE": "consumer",
    "XOM": "energy", "CVX": "energy", "SLB": "energy",
    "CAT": "industrials", "DE": "industrials", "BA": "industrials",
    "CMCSA": "telecom", "VZ": "telecom", "T": "telecom",
    "DIS": "media",
}


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
        "_snapshot_ts": "2026-08-18T00:00:00Z",
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
# Control test 8 — SECTOR_MAP membership is byte-identical to main.
# No taxonomy consolidation. No additions, removals, or reclassifications.
# ══════════════════════════════════════════════════════════════════════════

def test_sector_map_membership_unchanged_from_main():
    assert dict(mc_mod.APMasterControl.SECTOR_MAP) == _ORIGINAL_SECTOR_MAP, (
        "PR #483 must not add, remove, or reclassify any SECTOR_MAP entry — "
        "only the '\"other\"' fallback is removed"
    )


def test_exposure_gate_module_untouched_by_this_pr():
    """PR #483 (narrow scope) does not touch ap/exposure_gate.py at all."""
    src = _REPO.joinpath("ap", "exposure_gate.py").read_text()
    assert "ap_master_control" not in src
    assert "def get_sector(symbol: str)" in src


# ══════════════════════════════════════════════════════════════════════════
# Control tests 1-2 — known-sector behavior unchanged
# ══════════════════════════════════════════════════════════════════════════

def test_control_1_aapl_msft_known_tech_still_aggregates_and_can_breach_cap():
    mc = _mc()
    positions = [_pos("AAPL", price=10.00, qty=2), _pos("MSFT", price=8.00, qty=2)]
    tech_deployed = mc._sector_capital_deployed(positions, "tech")
    assert tech_deployed == (10.00 * 2 * 100) + (8.00 * 2 * 100)


def test_control_2_jpm_bac_known_financials_still_aggregates():
    mc = _mc()
    positions = [_pos("JPM", price=5.00, qty=1), _pos("BAC", price=6.00, qty=1)]
    fin_deployed = mc._sector_capital_deployed(positions, "financials")
    assert fin_deployed == (5.00 * 1 * 100) + (6.00 * 1 * 100)


# ══════════════════════════════════════════════════════════════════════════
# Control test 3 — unknown + unknown never aggregate (fail-first proof)
# ══════════════════════════════════════════════════════════════════════════

def test_control_3_unmapped_candidate_does_not_share_bucket_with_unrelated_unmapped_position():
    """Fail-first proof: DHR is not in SECTOR_MAP (unmapped, like the
    production names Jason hit). Before this fix, both DHR and any other
    unmapped ticker fell back to 'other' and were summed together."""
    mc = _mc()
    positions = [_pos("DHR", price=5.00, qty=2)]
    assert mc_mod.APMasterControl.SECTOR_MAP.get("DHR") is None
    assert mc_mod.APMasterControl.SECTOR_MAP.get("QCOM") is None  # also unmapped in this local map
    deployed_other_bucket = mc._sector_capital_deployed(positions, "other")
    assert deployed_other_bucket == 0.0, (
        "'other' must not be a valid sector key any more — nothing can "
        "ever be attributed to it"
    )
    deployed_for_unknown_candidate = mc._sector_capital_deployed(positions, None)
    assert deployed_for_unknown_candidate == 0.0


def test_control_3b_second_unrelated_unmapped_pair_does_not_falsely_aggregate():
    mc = _mc()
    positions = [_pos("PCAR", price=3.00, qty=1), _pos("DDOG", price=4.00, qty=1)]
    assert mc._sector_capital_deployed(positions, None) == 0.0


def test_get_sector_exposure_does_not_bucket_unmapped_tickers_as_other():
    mc = _mc()
    positions = [_pos("DHR", price=5.00, qty=1), _pos("PCAR", price=6.00, qty=1)]
    exposure = mc.get_sector_exposure(positions)
    assert "other" not in exposure
    assert exposure.get("sector_unknown") == (5.00 * 1 * 100) + (6.00 * 1 * 100)


def test_sector_unknown_diagnostic_bucket_never_used_as_risk_authority():
    """get_sector_exposure()'s 'sector_unknown' key is a REPORTING label
    only. _sector_capital_deployed (the actual risk-gate math) must never
    read or honor it as an economic correlation key."""
    mc = _mc()
    positions = [_pos("DHR", price=5.00, qty=1), _pos("PCAR", price=6.00, qty=1)]
    assert mc._sector_capital_deployed(positions, "sector_unknown") == 0.0


# ══════════════════════════════════════════════════════════════════════════
# Control tests 4-7 — unknown sector does not fail open other risk gates
# ══════════════════════════════════════════════════════════════════════════

def test_control_4_unknown_candidate_oversized_per_position_cost_still_blocks():
    m = _make_revalidation_mc(max_position_pct=0.10)  # equity 5000 -> budget 500
    m._get_snapshot = MagicMock(return_value=_snap([]))
    plan = _plan("ZZZZFAKE", max_position_usd=600.0)  # over budget
    decision = _revalidate(m, plan)
    assert decision.ok is False
    assert decision.reason_code == "ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP"


def test_control_5_unknown_candidate_total_account_cap_exceeded_still_blocks():
    # per-position budget = 250 (0.05 * 5000); total cap = 1000 (0.20 * 5000)
    # real_cost=200 fits per-position budget but 850 pending + 200 > 1000 total cap
    m = _make_revalidation_mc(max_position_pct=0.05, max_total_capital_pct=0.20)
    m._get_snapshot = MagicMock(return_value=_snap([]))
    m._pending_capital_from_snapshot_or_db = MagicMock(return_value=850.0)
    plan = _plan("ZZZZFAKE2", max_position_usd=200.0)
    decision = _revalidate(m, plan)
    assert decision.ok is False
    assert decision.reason_code in (
        "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED",
        "ACTUAL_CONTRACT_COST_EXCEEDS_REMAINING_TOTAL_CAPACITY",
    )


def test_control_6_unknown_candidate_ticker_cap_exceeded_still_blocks():
    m = _make_revalidation_mc(
        max_position_pct=0.50, max_total_capital_pct=0.90, max_ticker_pct=0.05,
    )
    existing = [_pos("ZZZZFAKE3", price=2.0, qty=2)]  # $400 already deployed in this ticker
    m._get_snapshot = MagicMock(return_value=_snap(existing))
    plan = _plan("ZZZZFAKE3", max_position_usd=100.0)
    decision = _revalidate(m, plan)
    assert decision.ok is False
    assert decision.reason_code == "TICKER_CAP_BLOCK"
    assert "revalidate_ticker_cap_ZZZZFAKE3" in decision.reason


def test_control_7_unknown_candidate_otherwise_valid_proceeds_beyond_sector_gate():
    m = _make_revalidation_mc(
        max_position_pct=0.50, max_total_capital_pct=0.90, max_ticker_pct=0.50,
    )
    m._get_snapshot = MagicMock(return_value=_snap([]))
    plan = _plan("ZZZZFAKE4", max_position_usd=100.0)
    decision = _revalidate(m, plan)
    assert decision.ok is True
    assert decision.reason_code == ""


def test_known_sector_still_blocks_over_cap_via_revalidate_exposure():
    m = _make_revalidation_mc(
        max_position_pct=0.90, max_total_capital_pct=0.90, max_ticker_pct=0.90,
        max_sector_pct=0.10,  # sector cap = 500
    )
    existing_tech = [_pos("MSFT", price=4.0, qty=1)]  # $400 deployed, tech
    m._get_snapshot = MagicMock(return_value=_snap(existing_tech))
    plan = _plan("AAPL", max_position_usd=200.0)  # 400 + 200 = 600 > 500
    decision = _revalidate(m, plan)
    assert decision.ok is False
    assert decision.reason_code == "SECTOR_CAP_BLOCK"
    assert "revalidate_sector_cap_tech" in decision.reason
    assert "revalidate_sector_cap_other" not in decision.reason


def test_unknown_existing_position_does_not_pollute_known_candidate_sector():
    mc = _mc()
    positions = [_pos("DHR", price=100.0, qty=5)]  # large unmapped exposure
    tech_deployed = mc._sector_capital_deployed(positions, "tech")
    assert tech_deployed == 0.0


# ══════════════════════════════════════════════════════════════════════════
# Control tests 9-10 — no "other" fallback anywhere, no revalidate_sector_cap_other
# ══════════════════════════════════════════════════════════════════════════

def test_control_9_no_other_fallback_remains_in_authoritative_sector_code():
    assert '.get(ticker.upper(), "other")' not in MC_SRC
    assert '.get(ticker_in_pos.upper(), "other")' not in MC_SRC
    assert '.get(existing_ticker.upper(), "other")' not in MC_SRC


def test_control_10_no_revalidate_sector_cap_other_can_be_emitted():
    body = MC_SRC[MC_SRC.find("def revalidate_exposure("):]
    body = body[: body.find("\n    def _log_capital_utilization")]
    assert "if sector is not None and proj_sector > max_sector:" in body


# ══════════════════════════════════════════════════════════════════════════
# Sector-decision telemetry
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


def test_telemetry_is_observability_only_not_a_second_risk_engine():
    src = inspect.getsource(mc_mod.APMasterControl._sector_telemetry)
    assert "return self._block(" not in src
    assert "return ControlDecision(" not in src
    call_sites = [line for line in MC_SRC.splitlines() if "_sector_telemetry(" in line]
    assert call_sites, "expected at least one _sector_telemetry call site"
    for line in call_sites:
        assert "**self._sector_telemetry(" in line or "def _sector_telemetry" in line


# ══════════════════════════════════════════════════════════════════════════
# Normalization
# ══════════════════════════════════════════════════════════════════════════

def test_ticker_case_normalization_resolves_identically():
    m = mc_mod.APMasterControl.SECTOR_MAP
    assert m.get("aapl".upper()) == m.get("AAPL")
    assert m.get(" QCOM ".strip().upper()) == m.get("QCOM")


# ══════════════════════════════════════════════════════════════════════════
# Mutation / authority freeze (static proof)
# ══════════════════════════════════════════════════════════════════════════

def test_no_new_broker_submit_or_cancel_authority_introduced():
    body = MC_SRC[MC_SRC.find("def revalidate_exposure("):]
    body = body[: body.find("\n    def _log_capital_utilization")]
    assert "submit_order" not in body
    assert "cancel_order" not in body
    assert "broker.submit" not in body
    assert "broker.cancel" not in body


def test_master_control_module_imports_cleanly():
    import importlib
    importlib.reload(mc_mod)
    assert hasattr(mc_mod, "APMasterControl")
    assert hasattr(mc_mod.APMasterControl, "SECTOR_MAP")
