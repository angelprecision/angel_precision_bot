# =============================================================================
# tests/test_vol_scaled_exit_ladder.py — PR-G
#
# Coverage:
#   A. expected_move.py pure math (golden numbers, degenerate inputs, clamps)
#   B. exit_ladder resolver: legacy-identity, flag guards, live-lock,
#      missing-IV fallback, high/low-IV band widening, stamp contents
#   C. evaluate_exit integration: default legacy byte-identity, vol_scaled
#      paper widens bands, live stays legacy, missing meta ⇒ legacy
# =============================================================================
import math
import os
import importlib
import pytest


# ── A. expected_move pure math ───────────────────────────────────────────────
from ap.expected_move import (
    expected_move_1d_pct,
    expected_move_pct_over,
    expected_option_daily_range_pct,
    feasibility_ratio,
    OPTION_RANGE_CLAMP_LO,
    OPTION_RANGE_CLAMP_HI,
)


def test_em1_golden():
    # IV 0.3969 annualized → 1d = 0.3969/sqrt(252) = 0.0250 exactly (sqrt(252)≈15.8745)
    val, q = expected_move_1d_pct(0.3969)
    assert q == "ok"
    assert abs(val - (0.3969 / math.sqrt(252))) < 1e-12


def test_em1_missing_and_bounds():
    assert expected_move_1d_pct(None) == (None, "iv_missing")
    assert expected_move_1d_pct("abc") == (None, "iv_missing")
    assert expected_move_1d_pct(0) == (None, "iv_missing")       # non-positive
    assert expected_move_1d_pct(-0.2) == (None, "iv_missing")
    assert expected_move_1d_pct(6.0) == (None, "iv_out_of_bounds")


def test_em_over_days():
    v1, _ = expected_move_1d_pct(0.40)
    v4, q = expected_move_pct_over(0.40, 4)
    assert q == "ok"
    assert abs(v4 - v1 * 2.0) < 1e-12          # sqrt(4)=2
    assert expected_move_pct_over(0.40, 0) == (None, "days_invalid")


def test_option_range_golden():
    # delta 0.5, spot 100, IV such that em1=0.02 → underlying move=$2
    # premium 2.0 → range = 0.5 * 2 / 2.0 = 0.5
    iv = 0.02 * math.sqrt(252)
    val, q = expected_option_daily_range_pct(0.5, 2.0, 100.0, iv)
    assert q == "ok"
    assert abs(val - 0.5) < 1e-9


def test_option_range_clamps_high_and_low():
    # Far-OTM cheap contract → huge range → clamp hi
    iv = 0.05 * math.sqrt(252)
    hi_val, hi_q = expected_option_daily_range_pct(0.9, 0.05, 100.0, iv)
    assert hi_val == OPTION_RANGE_CLAMP_HI
    assert hi_q == "ok_clamped_hi"
    # Deep-ITM expensive contract → tiny range → clamp lo
    iv2 = 0.005 * math.sqrt(252)
    lo_val, lo_q = expected_option_daily_range_pct(0.1, 50.0, 100.0, iv2)
    assert lo_val == OPTION_RANGE_CLAMP_LO
    assert lo_q == "ok_clamped_lo"


def test_option_range_bad_inputs():
    good_iv = 0.30
    assert expected_option_daily_range_pct(None, 2.0, 100.0, good_iv)[1] == "delta_missing"
    assert expected_option_daily_range_pct(1.5, 2.0, 100.0, good_iv)[1] == "delta_missing"  # >1
    assert expected_option_daily_range_pct(0.5, 0, 100.0, good_iv)[1] == "premium_invalid"
    assert expected_option_daily_range_pct(0.5, 2.0, 0, good_iv)[1] == "underlying_invalid"
    assert expected_option_daily_range_pct(0.5, 2.0, 100.0, None)[1] == "iv_missing"


def test_feasibility_ratio():
    # spot 100, em 0.02 → expected move $2. target 103 from entry 100 → 3/2 = 1.5
    r, q = feasibility_ratio(100.0, 103.0, 0.02)
    assert q == "ok"
    assert abs(r - 1.5) < 1e-9
    # em passed as 0 fails the positive-float guard first → "em_invalid"
    assert feasibility_ratio(100, 103, 0)[1] == "em_invalid"


# ── B. exit_ladder resolver ──────────────────────────────────────────────────
import ap.exit_ladder as EL
from ap.exit_ladder import (
    resolve_exit_ladder, LegacyThresholds, ExitLadderConfig,
    MODE_LEGACY, MODE_VOL_SCALED,
)

LEGACY = LegacyThresholds(
    hard_stop=-0.33, immediate_tp=0.12, profit_lock=0.12, theta_stop=-0.35,
    w1_threshold=0.15, w2_threshold=0.25, w3_threshold=0.15,
)

VOL_META = {  # range 0.30 → predictable resolved values with default k
    "entry_atm_iv": 0.40, "delta": 0.5, "premium_per_share": 2.0,
    "underlying_price": 100.0, "expected_option_daily_range_pct": 0.30,
    "range_quality": "ok",
}


def _cfg(**kw):
    base = dict(mode=MODE_VOL_SCALED, enabled=True, paper_only=True,
                live_enabled=False, tp_k=0.5, w1_k=1.0, w2_k=0.6, w3_k=0.4,
                hard_stop_k=-0.75, theta_stop_k=-0.85)
    base.update(kw)
    return ExitLadderConfig(**base)


def test_legacy_mode_is_byte_identical():
    cfg = _cfg(mode=MODE_LEGACY)
    r = resolve_exit_ladder("paper", LEGACY, VOL_META, cfg)
    assert r.mode == MODE_LEGACY
    assert (r.hard_stop, r.immediate_tp, r.profit_lock, r.theta_stop,
            r.w1_threshold, r.w2_threshold, r.w3_threshold) == (
        LEGACY.hard_stop, LEGACY.immediate_tp, LEGACY.profit_lock,
        LEGACY.theta_stop, LEGACY.w1_threshold, LEGACY.w2_threshold,
        LEGACY.w3_threshold)


def test_disabled_flag_falls_back_legacy():
    cfg = _cfg(enabled=False)
    r = resolve_exit_ladder("paper", LEGACY, VOL_META, cfg)
    assert r.mode == MODE_LEGACY
    assert r.fallback_reason == "vol_exit_disabled"


def test_paper_enabled_uses_vol_scaled():
    cfg = _cfg()
    r = resolve_exit_ladder("paper", LEGACY, VOL_META, cfg)
    assert r.mode == MODE_VOL_SCALED
    # range 0.30, defaults: tp=.15, w1=.30, w2=.18, w3=.12, hard=-.225, theta=-.255
    assert abs(r.immediate_tp - 0.15) < 1e-9
    assert abs(r.w1_threshold - 0.30) < 1e-9
    assert abs(r.w2_threshold - 0.18) < 1e-9
    assert abs(r.w3_threshold - 0.12) < 1e-9
    assert abs(r.hard_stop - (-0.225)) < 1e-9
    assert abs(r.theta_stop - (-0.255)) < 1e-9


def test_live_stays_legacy_when_paper_only():
    cfg = _cfg(paper_only=True, live_enabled=False)
    for mode in ("live", "", "unknown", None):
        r = resolve_exit_ladder(mode, LEGACY, VOL_META, cfg)
        assert r.mode == MODE_LEGACY, f"mode={mode!r} should lock to legacy"
        assert r.fallback_reason == "live_locked"


def test_live_enabled_requires_both_flags():
    # paper_only still true blocks live even with live_enabled
    r = resolve_exit_ladder("live", LEGACY, VOL_META, _cfg(paper_only=True, live_enabled=True))
    assert r.mode == MODE_LEGACY
    # both correct → live uses vol_scaled
    r2 = resolve_exit_ladder("live", LEGACY, VOL_META, _cfg(paper_only=False, live_enabled=True))
    assert r2.mode == MODE_VOL_SCALED


def test_missing_iv_meta_falls_back_legacy():
    cfg = _cfg()
    # None/{} → missing_iv_meta; a dict lacking IV → recompute path → iv_missing
    expected = {None: "missing_iv_meta", "empty": "missing_iv_meta", "nofields": "iv_missing"}
    cases = {None: None, "empty": {}, "nofields": {"foo": "bar"}}
    for key, meta in cases.items():
        r = resolve_exit_ladder("paper", LEGACY, meta, cfg)
        assert r.mode == MODE_LEGACY
        assert r.fallback_reason == expected[key]


def test_high_iv_widens_low_iv_tightens():
    cfg = _cfg()
    hi = resolve_exit_ladder("paper", LEGACY, {**VOL_META, "expected_option_daily_range_pct": 0.60}, cfg)
    lo = resolve_exit_ladder("paper", LEGACY, {**VOL_META, "expected_option_daily_range_pct": 0.15}, cfg)
    assert hi.w1_threshold > lo.w1_threshold
    assert abs(hi.hard_stop) > abs(lo.hard_stop)


def test_recompute_from_primitives_when_precomputed_absent():
    meta = {"entry_atm_iv": 0.40, "delta": 0.5, "premium_per_share": 2.0,
            "underlying_price": 100.0}   # no precomputed range
    r = resolve_exit_ladder("paper", LEGACY, meta, _cfg())
    assert r.mode == MODE_VOL_SCALED
    assert r.expected_range is not None


def test_stamp_contents():
    r = resolve_exit_ladder("paper", LEGACY, VOL_META, _cfg())
    s = r.stamp()
    assert s["ladder_mode"] == MODE_VOL_SCALED
    assert "resolved" in s and "k" in s and "expected_range" in s


def test_resolver_never_raises_on_garbage():
    # Deliberately hostile meta values
    for meta in ({"expected_option_daily_range_pct": "NaN"},
                 {"delta": object()},
                 {"entry_atm_iv": float("inf")}):
        r = resolve_exit_ladder("paper", LEGACY, meta, _cfg())
        assert r.mode == MODE_LEGACY  # safe fallback


# ── C. evaluate_exit integration ─────────────────────────────────────────────
def _fresh_env(**overrides):
    keys = ["EXIT_LADDER_MODE", "VOL_EXIT_ENABLED", "VOL_EXIT_PAPER_ONLY",
            "VOL_EXIT_LIVE_ENABLED", "VOL_EXIT_TP_K", "VOL_EXIT_W1_K",
            "VOL_EXIT_W2_K", "VOL_EXIT_W3_K", "VOL_EXIT_HARD_STOP_K",
            "VOL_EXIT_THETA_STOP_K"]
    for k in keys:
        os.environ.pop(k, None)
    os.environ.update({k: str(v) for k, v in overrides.items()})


@pytest.fixture
def exit_mod():
    import ap_exit_engine as m
    return m


def _mk_pos(exit_mod, *, execution_mode="paper", vol_meta=None, pnl=0.0):
    P = exit_mod.ManagedPosition
    pos = P(
        ticker="NVDA", option_symbol="NVDA260117C00600000", side="CALL",
        quantity=3, entry_price=2.0, underlying_entry=600.0,
        underlying_target=610.0, underlying_stop=595.0,
    )
    pos.quantity_remaining = 3
    pos.execution_mode = execution_mode
    pos.vol_exit_meta = vol_meta
    pos.current_option_price = 2.0 * (1 + pnl)
    return pos


def test_evaluate_default_is_legacy(exit_mod):
    _fresh_env()  # nothing set → EXIT_LADDER_MODE defaults to legacy
    pos = _mk_pos(exit_mod, vol_meta=VOL_META)
    # Force a mid-morning W1 window where a vol change would be observable
    import datetime as dt
    now = dt.datetime(2026, 1, 15, 11, 30, tzinfo=exit_mod.ET)
    pos.current_option_price = 2.0 * 1.20  # +20%
    pos.scale_outs_done = 0
    d = exit_mod.evaluate_exit(pos, now_et=now)
    # Legacy W1 threshold 0.15 → +20% triggers scale. Stamp must say legacy.
    assert d._ladder.get("ladder_mode") == "legacy"


def test_evaluate_vol_scaled_paper_changes_threshold(exit_mod):
    # range 0.30, w1_k 1.0 → W1 threshold = 0.30 (vs legacy 0.15).
    # At +20% (0.20): legacy would SCALE at W1, vol_scaled should NOT (needs +30%).
    _fresh_env(EXIT_LADDER_MODE="vol_scaled", VOL_EXIT_ENABLED="true",
               VOL_EXIT_PAPER_ONLY="true")
    import datetime as dt
    now = dt.datetime(2026, 1, 15, 11, 30, tzinfo=exit_mod.ET)

    # Case 1: +20% is below the widened +30% scale-1 gate → HOLD (suppressed).
    # In legacy this would SCALE_1 at +15%; vol_scaled correctly holds.
    pos = _mk_pos(exit_mod, execution_mode="paper", vol_meta=VOL_META)
    pos.current_option_price = 2.0 * 1.20  # +20%
    pos.scale_outs_done = 0
    d = exit_mod.evaluate_exit(pos, now_et=now)
    assert "SCALE_1" not in d.reason and "PROFIT PROTECT W1" not in d.reason

    # Case 2: +35% clears the widened +30% gate → SCALE_1 fires WITH vol stamp.
    pos2 = _mk_pos(exit_mod, execution_mode="paper", vol_meta=VOL_META)
    pos2.current_option_price = 2.0 * 1.35  # +35%
    pos2.scale_outs_done = 0
    d2 = exit_mod.evaluate_exit(pos2, now_et=now)
    assert d2.action == "SCALE_OUT"
    assert d2._ladder.get("ladder_mode") == "vol_scaled"
    assert "+30%" in d2.reason  # resolved threshold shown honestly


def test_evaluate_live_stays_legacy(exit_mod):
    _fresh_env(EXIT_LADDER_MODE="vol_scaled", VOL_EXIT_ENABLED="true",
               VOL_EXIT_PAPER_ONLY="true")
    import datetime as dt
    now = dt.datetime(2026, 1, 15, 11, 30, tzinfo=exit_mod.ET)
    pos = _mk_pos(exit_mod, execution_mode="live", vol_meta=VOL_META)
    pos.current_option_price = 2.0 * 1.20
    pos.scale_outs_done = 0
    d = exit_mod.evaluate_exit(pos, now_et=now)
    assert d._ladder.get("ladder_mode") == "legacy"
    assert d._ladder.get("fallback_reason") == "live_locked"


def test_evaluate_missing_meta_legacy(exit_mod):
    _fresh_env(EXIT_LADDER_MODE="vol_scaled", VOL_EXIT_ENABLED="true",
               VOL_EXIT_PAPER_ONLY="true")
    import datetime as dt
    now = dt.datetime(2026, 1, 15, 11, 30, tzinfo=exit_mod.ET)
    pos = _mk_pos(exit_mod, execution_mode="paper", vol_meta=None)
    pos.current_option_price = 2.0 * 1.20
    pos.scale_outs_done = 0
    d = exit_mod.evaluate_exit(pos, now_et=now)
    assert d._ladder.get("ladder_mode") == "legacy"


def test_eod_hard_close_unaffected_by_ladder(exit_mod):
    # EOD must fire regardless of ladder mode — money-safety invariant.
    _fresh_env(EXIT_LADDER_MODE="vol_scaled", VOL_EXIT_ENABLED="true",
               VOL_EXIT_PAPER_ONLY="true")
    import datetime as dt
    now = dt.datetime(2026, 1, 15, 15, 55, tzinfo=exit_mod.ET)  # past 3:50
    pos = _mk_pos(exit_mod, execution_mode="paper", vol_meta=VOL_META)
    pos.current_option_price = 2.0  # flat
    d = exit_mod.evaluate_exit(pos, now_et=now)
    assert d.action == "CLOSE_ALL"
    assert "EOD" in d.reason


# ── D. PR-G3 attribution-coverage: EVERY exit path must carry _ladder ─────────
# Regression for the bug where only scale/window/theta paths stamped _ladder,
# causing #292 to mis-attribute vol_scaled positions (that exited via hard
# stop / target / EOD) as legacy and contaminate the R-multiple comparison.

def test_target_hit_carries_ladder_stamp(exit_mod):
    _fresh_env(EXIT_LADDER_MODE="vol_scaled", VOL_EXIT_ENABLED="true",
               VOL_EXIT_PAPER_ONLY="true")
    import datetime as dt
    now = dt.datetime(2026, 1, 15, 10, 30, tzinfo=exit_mod.ET)
    pos = _mk_pos(exit_mod, execution_mode="paper", vol_meta=VOL_META)
    pos.current_underlying = 611.0   # past target 610 → TARGET HIT (was unstamped)
    d = exit_mod.evaluate_exit(pos, now_et=now)
    assert "TARGET HIT" in d.reason
    assert d._ladder.get("ladder_mode") == "vol_scaled"   # was 'legacy' before fix


def test_hard_stop_carries_ladder_stamp(exit_mod):
    _fresh_env(EXIT_LADDER_MODE="vol_scaled", VOL_EXIT_ENABLED="true",
               VOL_EXIT_PAPER_ONLY="true")
    import datetime as dt
    now = dt.datetime(2026, 1, 15, 10, 30, tzinfo=exit_mod.ET)
    pos = _mk_pos(exit_mod, execution_mode="paper", vol_meta=VOL_META)
    # vol_scaled hard stop = -0.75 * range(0.30) = -0.225 → drive well past it.
    # The soft-loss tier may return a HOLD that stamps a breach for later
    # confirmation rather than an immediate STOP; either way the decision MUST
    # carry the ladder stamp (that is the property under test).
    pos.current_option_price = 2.0 * (1 - 0.40)   # -40%
    d = exit_mod.evaluate_exit(pos, now_et=now)
    assert d._ladder.get("ladder_mode") == "vol_scaled"


def test_eod_close_carries_ladder_stamp(exit_mod):
    _fresh_env(EXIT_LADDER_MODE="vol_scaled", VOL_EXIT_ENABLED="true",
               VOL_EXIT_PAPER_ONLY="true")
    import datetime as dt
    now = dt.datetime(2026, 1, 15, 15, 55, tzinfo=exit_mod.ET)   # past EOD
    pos = _mk_pos(exit_mod, execution_mode="paper", vol_meta=VOL_META)
    pos.current_option_price = 2.0   # flat
    d = exit_mod.evaluate_exit(pos, now_et=now)
    assert "EOD" in d.reason
    assert d._ladder.get("ladder_mode") == "vol_scaled"


def test_hold_carries_ladder_stamp(exit_mod):
    _fresh_env(EXIT_LADDER_MODE="vol_scaled", VOL_EXIT_ENABLED="true",
               VOL_EXIT_PAPER_ONLY="true")
    import datetime as dt
    now = dt.datetime(2026, 1, 15, 10, 15, tzinfo=exit_mod.ET)   # early, flat
    pos = _mk_pos(exit_mod, execution_mode="paper", vol_meta=VOL_META)
    pos.current_option_price = 2.0 * 1.05   # +5%, below every gate
    d = exit_mod.evaluate_exit(pos, now_et=now)
    assert d.action == "HOLD"
    assert d._ladder.get("ladder_mode") == "vol_scaled"


def test_all_exit_paths_stamp_ladder_when_vol_scaled(exit_mod):
    """Exhaustive: sweep representative states; NO returned decision may be
    missing _ladder once the ladder has been resolved for a vol_scaled pos."""
    _fresh_env(EXIT_LADDER_MODE="vol_scaled", VOL_EXIT_ENABLED="true",
               VOL_EXIT_PAPER_ONLY="true")
    import datetime as dt
    scenarios = [
        (dt.datetime(2026, 1, 15, 10, 30, tzinfo=exit_mod.ET), 611.0, 2.0),     # target
        (dt.datetime(2026, 1, 15, 10, 30, tzinfo=exit_mod.ET), 600.0, 2.0*0.60),# hard stop
        (dt.datetime(2026, 1, 15, 13, 30, tzinfo=exit_mod.ET), 600.0, 2.0*0.70),# theta
        (dt.datetime(2026, 1, 15, 11, 30, tzinfo=exit_mod.ET), 600.0, 2.0*1.40),# scale
        (dt.datetime(2026, 1, 15, 15, 55, tzinfo=exit_mod.ET), 600.0, 2.0),     # EOD
        (dt.datetime(2026, 1, 15, 10, 15, tzinfo=exit_mod.ET), 600.0, 2.0*1.05),# HOLD
    ]
    for now, undl, opt in scenarios:
        pos = _mk_pos(exit_mod, execution_mode="paper", vol_meta=VOL_META)
        pos.current_underlying = undl
        pos.current_option_price = opt
        d = exit_mod.evaluate_exit(pos, now_et=now)
        assert getattr(d, "_ladder", None), f"missing _ladder for reason={d.reason!r}"
        assert d._ladder.get("ladder_mode") in ("vol_scaled", "legacy")
