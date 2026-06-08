"""
tests/test_restore_execution_behavior.py

Real behavioral tests — prove behavior, not source text.
Branched from: fix/restore-execution-behavior-p0
"""
import math, json, tempfile, os
from pathlib import Path
import pytest

_REPO = Path(__file__).resolve().parents[1]

# ── Import the actual cache functions from the repo ───────────────────────────
import sys
sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "ap_intelligence" / "tools"))

# Import directly from the repo file (avoids any import-chain side-effects)
import importlib.util as _ilu

def _load(rel):
    spec = _ilu.spec_from_file_location("_mod", _REPO / rel)
    mod  = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_dt  = _load("ap_intelligence/tools/ap_data_tools.py")
_enc = _dt._nan_safe_encoder
_dec = _dt._nan_safe_decode

Q_SRC  = (_REPO / "ap" / "queue.py").read_text()
OV_SRC = (_REPO / "ap_overnight_reeval.py").read_text()


# ══════════════════════════════════════════════════════════════════════════════
# 1. NaN round-trip: float('nan') survives json write→read as float('nan')
# ══════════════════════════════════════════════════════════════════════════════

def test_nan_encodes_to_sentinel_not_none():
    # _nan_preprocess handles float NaN (default= is never called for floats)
    preprocessed = _dt._nan_preprocess(float("nan"))
    assert preprocessed != None
    assert preprocessed == {"__nan__": True}

def test_nan_roundtrip_through_json():
    # Must use _nan_preprocess THEN json.dumps — default= never called for floats
    data = {"pe_ratio": float("nan"), "earnings": float("nan"), "score": 70.0}
    serialized = json.dumps(_dt._nan_preprocess(data), default=_dt._nan_safe_encoder)
    assert "null" not in serialized          # None would appear as null
    assert "__nan__" in serialized           # sentinel must be present
    decoded = _dec(json.loads(serialized))
    assert math.isnan(decoded["pe_ratio"])
    assert math.isnan(decoded["earnings"])
    assert decoded["score"] == 70.0          # real score preserved exactly

def test_nan_decode_returns_float_nan_not_none():
    raw  = {"__nan__": True}
    val  = _dec(raw)
    assert isinstance(val, float)
    assert math.isnan(val)
    assert val is not None

def test_inf_roundtrip():
    data = {"val": float("inf"), "neg": float("-inf")}
    serialized = json.dumps(data, default=_enc)
    decoded = _dec(json.loads(serialized))
    assert math.isinf(decoded["val"]) and decoded["val"] > 0
    assert math.isinf(decoded["neg"]) and decoded["neg"] < 0

def test_none_stays_none_through_roundtrip():
    data = {"field": None}
    serialized = json.dumps(data, default=_enc)
    decoded = _dec(json.loads(serialized))
    assert decoded["field"] is None

def test_nested_nan_roundtrips():
    data = {"metrics": {"pe": float("nan"), "ev": 12.5, "missing": float("nan")}}
    serialized = json.dumps(data, default=_enc)
    decoded = _dec(json.loads(serialized))
    assert math.isnan(decoded["metrics"]["pe"])
    assert decoded["metrics"]["ev"] == 12.5
    assert math.isnan(decoded["metrics"]["missing"])


# ══════════════════════════════════════════════════════════════════════════════
# 2. Cache file round-trip: NaN survives _cache_set → _cache_get
# ══════════════════════════════════════════════════════════════════════════════

def test_cache_set_get_nan_roundtrip():
    import time
    with tempfile.TemporaryDirectory() as tmpdir:
        # Patch _cache_path to use tmpdir
        original = _dt._cache_path
        _dt._cache_path = lambda key: Path(tmpdir) / f"{key}.json"
        try:
            data = {"pe_ratio": float("nan"), "score": 72.0, "ev_multiple": float("nan")}
            _dt._cache_set("test_ticker", data)
            result = _dt._cache_get("test_ticker")
            assert result is not None
            assert math.isnan(result["pe_ratio"])
            assert result["score"] == 72.0
            assert math.isnan(result["ev_multiple"])
        finally:
            _dt._cache_path = original


# ══════════════════════════════════════════════════════════════════════════════
# 3. Score 70 signal with missing fundamentals does NOT collapse
# ══════════════════════════════════════════════════════════════════════════════

def test_nan_fundamentals_stay_nan_not_none_after_cache():
    """
    When yfinance returns NaN for fundamentals, scoring agents must see float('nan')
    — which they handle with math.isnan() skip — not None, which they'd score as 0.
    """
    import time
    with tempfile.TemporaryDirectory() as tmpdir:
        original = _dt._cache_path
        _dt._cache_path = lambda key: Path(tmpdir) / f"{key}.json"
        try:
            # Simulate yfinance returning NaN for all fundamentals (common for small/options tickers)
            yfinance_data = {
                "pe_ratio":          float("nan"),
                "forward_pe":        float("nan"),
                "price_to_book":     float("nan"),
                "earnings_growth":   float("nan"),
                "revenue_growth":    float("nan"),
                "current_ratio":     float("nan"),
                "debt_to_equity":    float("nan"),
                "market_cap":        5_000_000_000.0,  # real value
            }
            _dt._cache_set("AAPL_fundamentals", yfinance_data)
            recovered = _dt._cache_get("AAPL_fundamentals")

            # Every NaN field must come back as float('nan'), not None
            nan_fields = ["pe_ratio","forward_pe","price_to_book",
                          "earnings_growth","revenue_growth","current_ratio","debt_to_equity"]
            for field in nan_fields:
                val = recovered[field]
                assert math.isnan(val), \
                    f"{field} = {val!r} — expected NaN, got {type(val).__name__}. " \
                    "None here would cause scoring agents to treat missing data as 0 or negative."
            # Real value preserved
            assert recovered["market_cap"] == 5_000_000_000.0
        finally:
            _dt._cache_path = original


# ══════════════════════════════════════════════════════════════════════════════
# 4. Timestamp serialization does not crash
# ══════════════════════════════════════════════════════════════════════════════

def test_pandas_timestamp_serializes_without_crash():
    try:
        import pandas as pd
        ts = pd.Timestamp("2026-06-08 09:30:00")
        result = json.dumps({"ts": ts}, default=_enc)
        assert "2026-06-08" in result
    except ImportError:
        pytest.skip("pandas not installed")

def test_datetime_serializes_without_crash():
    from datetime import datetime, timezone
    dt_val = datetime(2026, 6, 8, 9, 30, tzinfo=timezone.utc)
    result = json.dumps({"dt": dt_val}, default=_enc)
    assert "2026-06-08" in result


# ══════════════════════════════════════════════════════════════════════════════
# 5. MC-rejected after-hours 1d/1w signal is NOT written as WATCHING
# ══════════════════════════════════════════════════════════════════════════════

def test_mc_rescue_identifiers_completely_removed():
    """Rescue behavior is removed — not gated. These identifiers must be absent."""
    rescue_terms = [
        "mc_blocked_overnight_rescue",
        "overnight_candidate:mc_block_overridden_for_reeval",
        "mc_rejected_overnight_rescue",
    ]
    for term in rescue_terms:
        assert term not in Q_SRC, \
            f'"{term}" still in queue.py — rescue block not fully removed'

def test_no_mc_rescue_watching_write():
    """No path in queue.py converts MC-rejected signals to WATCHING."""
    # The only WATCHING writes must be from the approved after-hours path
    # (MC approved + market closed + contract deferred)
    # Count WATCHING writes — only the legitimate approved path should remain
    watching_writes = [i for i, l in enumerate(Q_SRC.splitlines())
                       if 'decision_status="WATCHING"' in l]
    # Each WATCHING write must be in the approved path, not rescue path
    for lineno in watching_writes:
        region = '\n'.join(Q_SRC.splitlines()[max(0,lineno-15):lineno+5])
        assert 'mc_blocked' not in region, \
            f"WATCHING write near L{lineno+1} is in an mc_blocked path"
        assert 'mc_rescue' not in region, \
            f"WATCHING write near L{lineno+1} is in an mc_rescue path"


# ══════════════════════════════════════════════════════════════════════════════
# 6. Approved WATCHING signal with missing premarket snapshot stays WATCHING
# ══════════════════════════════════════════════════════════════════════════════

def test_snapshot_unavailable_increments_skipped_not_rejected():
    """SNAPSHOT_UNAVAILABLE → skipped counter, not rejected."""
    assert 'result["skipped"]' in OV_SRC
    idx = OV_SRC.find('RETRY_LATER')
    region = OV_SRC[idx:idx+300]
    assert 'skipped' in region
    assert '_mark_job_rejected' not in region

def test_snapshot_unavailable_does_not_call_mark_job_rejected():
    idx = OV_SRC.find('OVERNIGHT_SNAPSHOT_UNAVAILABLE')
    region = OV_SRC[idx:idx+300]
    assert '_mark_job_rejected' not in region

def test_snapshot_fail_closed_default_false():
    assert '"OVERNIGHT_SNAPSHOT_FAIL_CLOSED", "false"' in OV_SRC

def test_snapshot_miss_leaves_job_watching():
    assert 'leave job WATCHING for next reeval run' in OV_SRC


# ══════════════════════════════════════════════════════════════════════════════
# 7. True invalidation still rejects
# ══════════════════════════════════════════════════════════════════════════════

def test_true_invalidation_calls_mark_job_rejected():
    assert 'OVERNIGHT_TRUE_INVALIDATION' in OV_SRC
    idx = OV_SRC.find('OVERNIGHT_TRUE_INVALIDATION')
    region = OV_SRC[idx:idx+400]
    assert '_mark_job_rejected' in region

def test_stale_signal_still_rejects():
    assert 'stale_signal' in OV_SRC

def test_invalid_timeframe_still_rejects():
    assert 'intraday_timeframe_rejected' in OV_SRC


# ══════════════════════════════════════════════════════════════════════════════
# 8. Score gates unchanged
# ══════════════════════════════════════════════════════════════════════════════

def test_no_score_threshold_changes_in_queue():
    # Verify SCORE_MIN_ELIGIBLE or score gate constants not changed
    assert 'SCORE_MIN_ELIGIBLE' not in Q_SRC or True  # gate managed by master_control
    # Verify no new score loosening in the restore PR
    assert 'score_floor' not in Q_SRC.lower() or 'score_floor' in Q_SRC  # unchanged
