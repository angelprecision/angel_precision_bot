# =============================================================================
# tests/test_vol_exit_comparison.py — PR-G3
#
# Tests the pure logic of the R-multiple comparison harness: R computation,
# arm aggregation, attribution, CI math, and the conservative verdict gate.
# No DB required — all DB access is factored behind build_trade_records /
# compute_arm_stats which take plain inputs.
# =============================================================================
import math
import pytest

from ap.vol_exit_comparison import (
    TradeRecord, compute_arm_stats, render_verdict, build_trade_records,
    _mean, _median, _stddev, _mean_ci95, MIN_TRADES_PER_ARM,
)


def _rec(pid, mode, pnl, risk, r, **kw):
    return TradeRecord(
        position_id=pid, client_id="c", ticker="X", ladder_mode=mode,
        realized_pnl=pnl, planned_risk=risk, realized_r=r,
        pnl_pct=kw.get("pnl_pct"), mfe_pct=kw.get("mfe_pct"),
        mae_pct=kw.get("mae_pct"),
        exit_reason_code=kw.get("code", "TP"), exit_ts="", git_commit="abc",
    )


# ── pure stats ────────────────────────────────────────────────────────────────
def test_mean_median_std():
    assert _mean([1, 2, 3]) == 2
    assert _median([1, 2, 3]) == 2
    assert _median([1, 2, 3, 4]) == 2.5
    assert _mean([]) is None
    assert abs(_stddev([1, 2, 3]) - 1.0) < 1e-9
    assert _stddev([5]) is None


def test_ci_clears_zero_for_consistent_wins():
    lo, hi = _mean_ci95([1.0, 1.1, 0.9, 1.05, 0.95, 1.0])
    assert lo is not None and lo > 0


# ── R computation ─────────────────────────────────────────────────────────────
def test_build_records_computes_r_from_risk_basis():
    # entry $2.00/share ×100 ×qty2 ×sl_pct0.33 = $132 planned risk
    # realized +$66 → R = 66/132 = 0.5
    rows = [{
        "position_id": "p1", "client_id": "c", "symbol": "NVDA",
        "pnl_dollars": 66.0, "pnl_pct": 0.25, "mfe_pct": 0.4, "mae_pct": -0.1,
        "exit_reason_code": "PROFIT_PROTECT_W1", "exit_ts": "t", "git_commit": "g",
        "tr_entry_price": 2.0, "tr_qty": 2, "p_entry_price": 2.0,
        "p_avg_fill": 2.0, "p_qty": 2, "sl_pct": 0.33, "p_realized_pnl": 66.0,
        "execution_mode": "paper",
    }]
    recs, excluded = build_trade_records(rows, {"p1": "vol_scaled"})
    assert excluded == 0
    assert recs[0].ladder_mode == "vol_scaled"
    assert abs(recs[0].planned_risk - 132.0) < 1e-6
    assert abs(recs[0].realized_r - 0.5) < 1e-6


def test_build_records_excludes_when_no_risk_basis():
    rows = [{
        "position_id": "p2", "client_id": "c", "symbol": "X",
        "pnl_dollars": 50.0, "exit_reason_code": "TP", "exit_ts": "t",
        "git_commit": "g", "tr_entry_price": None, "tr_qty": 0,
        "p_entry_price": None, "p_avg_fill": None, "p_qty": 0,
        "sl_pct": None, "p_realized_pnl": 50.0, "execution_mode": "paper",
    }]
    recs, excluded = build_trade_records(rows, {})
    assert excluded == 1
    assert recs[0].realized_r is None
    assert recs[0].ladder_mode == "legacy"       # default attribution


def test_attribution_defaults_to_legacy_when_absent():
    rows = [{"position_id": "pX", "client_id": "c", "symbol": "X",
             "pnl_dollars": 10.0, "exit_reason_code": "TP", "exit_ts": "t",
             "git_commit": "g", "tr_entry_price": 1.0, "tr_qty": 1,
             "p_avg_fill": 1.0, "p_qty": 1, "sl_pct": 0.3,
             "p_realized_pnl": 10.0}]
    recs, _ = build_trade_records(rows, {})   # no mode entry for pX
    assert recs[0].ladder_mode == "legacy"


# ── arm aggregation ───────────────────────────────────────────────────────────
def test_arm_stats_expectancy_and_winrate():
    trades = [_rec("a", "legacy", 100, 100, 1.0),
              _rec("b", "legacy", -50, 100, -0.5),
              _rec("c", "legacy", 100, 100, 1.0),
              _rec("d", "legacy", -50, 100, -0.5)]
    st = compute_arm_stats("legacy", trades)
    assert st.n == 4 and st.n_with_r == 4
    assert st.wins == 2 and st.losses == 2
    assert abs(st.win_rate - 0.5) < 1e-9
    # expectancy = mean(1,-0.5,1,-0.5) = 0.25R
    assert abs(st.expectancy_r - 0.25) < 1e-9
    assert abs(st.total_r - 1.0) < 1e-9
    assert abs(st.avg_win_r - 1.0) < 1e-9
    assert abs(st.avg_loss_r - (-0.5)) < 1e-9


def test_arm_stats_counts_winrate_without_r_basis():
    trades = [_rec("a", "legacy", 100, None, None),   # no R basis
              _rec("b", "legacy", -20, None, None)]
    st = compute_arm_stats("legacy", trades)
    assert st.n == 2 and st.n_with_r == 0
    assert st.wins == 1 and st.losses == 1
    assert st.expectancy_r is None                    # no R basis → no expectancy


# ── verdict gate (the safety-critical logic) ─────────────────────────────────
def _arm(n, exp, ci_low):
    from ap.vol_exit_comparison import ArmStats
    return ArmStats(arm="x", n=n, n_with_r=n, expectancy_r=exp,
                    expectancy_r_ci_low=ci_low, expectancy_r_ci_high=exp + 0.1)


def test_verdict_not_enough_data():
    v, _ = render_verdict(_arm(5, 0.5, 0.3), _arm(30, 0.8, 0.6))
    assert v == "NOT_ENOUGH_DATA"


def test_verdict_hold_when_vol_worse():
    v, _ = render_verdict(_arm(30, 0.6, 0.4), _arm(30, 0.3, 0.1))
    assert v == "HOLD_LEGACY"


def test_verdict_hold_when_improvement_within_noise():
    # vol expectancy higher but CI low does NOT clear legacy point → inconclusive
    v, _ = render_verdict(_arm(30, 0.50, 0.45), _arm(30, 0.55, 0.48))
    assert v == "HOLD_INCONCLUSIVE"


def test_verdict_favored_only_when_ci_clears_legacy():
    # vol expectancy higher AND CI low (0.62) clears legacy point (0.50)
    v, detail = render_verdict(_arm(30, 0.50, 0.40), _arm(30, 0.80, 0.62))
    assert v == "VOL_SCALED_FAVORED"
    assert "REVIEW" in detail        # never claims auto-enable


def test_verdict_never_recommends_on_thin_vol_arm():
    # Even a great-looking vol arm with too few trades must not pass
    v, _ = render_verdict(_arm(30, 0.5, 0.4), _arm(MIN_TRADES_PER_ARM - 1, 2.0, 1.8))
    assert v == "NOT_ENOUGH_DATA"
