import os

from ap_historical_edge_guard import (
    BLOCK_REASON_FALLBACK_DEFAULT,
    FALLBACK_DEFAULT_REASON,
    MISSING_SOURCE_REASON,
    evaluate_historical_edge,
)
from ap_hybrid_client_quality_gate import evaluate_client_quality_gate


EMPTY_SNAP = {"trades_today": 0, "daily_trades": 0, "intraday_trades": 0, "symbol_trades": {}}


def _sig(**kw):
    base = dict(
        symbol="AAPL",
        direction="CALL",
        timeframe="1d",
        pattern="2-3",
        tier="A",
        score=75,
        trigger_price=180.0,
        stop_underlying=175.0,
        target_underlying=190.0,
    )
    base.update(kw)
    return base


def _production_payload(**kw):
    base = {
        "ticker": "GS",
        "symbol": "GS",
        "pattern": "FAILED_DIR_2D_15min",
        "pattern_id": "FAILED_DIR_2D_15min",
        "direction": "CALL",
        "side": "CALL",
        "score": 65.0,
        "ev_score": 65.0,
        "tier": "B",
        "win_rate": 0.65,
        "rr_ratio": 1.5,
        "avg_opt_ret": 400,
        "backtest_match_source": "FALLBACK_DEFAULT",
        "source_scanner": "failed_dir_intraday_1tf",
        "n_occurrences": 5,
        "confidence_tag": "standard_pool",
        "trigger": {
            "source": "scanner_failed_dir_intraday_v1",
            "entry": 1062.1,
            "stop": 1067.14,
            "target": 1067.14,
            "side": "CALL",
        },
        "trigger_price": 1062.1,
        "stop_underlying": 1057.0,
        "target_underlying": 1067.14,
        "timeframe": "15m",
    }
    base.update(kw)
    return base


def _enable_gate():
    os.environ.update({
        "HYBRID_CLIENT_QUALITY_MODE": "true",
        "MIN_CLIENT_SCORE": "70",
        "ALLOW_CLIENT_TIER_B": "true",
        "DAILY_CLIENT_PATTERN_WHITELIST": "2-3,3-2-2,1-2_2D",
        "INTRADAY_CLIENT_PATTERN_WHITELIST": "FAILED_DIR_2D_15min,2-3",
        "ALLOW_FAILED_DIR_CLIENT": "true",
        "MAX_CLIENT_TRADES_PER_DAY": "5",
        "MAX_CLIENT_DAILY_TRADES": "3",
        "MAX_CLIENT_INTRADAY_TRADES": "2",
        "MAX_CLIENT_SYMBOL_TRADES_PER_DAY": "1",
        "HISTORICAL_EDGE_FAIL_CLOSED": "true",
        "HISTORICAL_EDGE_TRUST_MISSING_SOURCE": "false",
        "HISTORICAL_EDGE_OBSERVE_ONLY_OVERRIDE": "false",
    })


def test_fallback_default_win_rate_is_not_trusted_or_boosted():
    decision = evaluate_historical_edge({
        "score": 69,
        "backtest_match_source": "FALLBACK_DEFAULT",
        "win_rate": 0.65,
    }, raw_score=69)

    assert decision.historical_edge_valid is False
    assert decision.reason == FALLBACK_DEFAULT_REASON
    assert decision.trusted_win_rate is None
    assert decision.trusted_sample_size == 0
    assert decision.should_block_client_eligibility is True
    assert decision.effective_score == 0


def test_fallback_default_avg_opt_ret_is_not_trusted_or_boosted():
    decision = evaluate_historical_edge({
        "score": 69,
        "backtest_match_source": "FALLBACK_DEFAULT",
        "avg_opt_ret": 400,
    }, raw_score=69)

    assert decision.historical_edge_valid is False
    assert decision.trusted_avg_opt_ret is None
    assert decision.should_block_client_eligibility is True
    assert decision.effective_score == 0


def test_real_source_is_trusted_only_with_valid_sample_size():
    trusted = evaluate_historical_edge({
        "backtest_match_source": "TICKER_SIDE_SAME_TF",
        "win_rate": 0.61,
        "avg_opt_ret": 83,
        "rr_ratio": 1.8,
        "sample_size": 12,
    }, raw_score=75, min_sample_size=5)
    assert trusted.historical_edge_valid is True
    assert trusted.trusted_win_rate == 0.61
    assert trusted.trusted_avg_opt_ret == 83
    assert trusted.trusted_rr_ratio == 1.8
    assert trusted.trusted_sample_size == 12

    untrusted = evaluate_historical_edge({
        "backtest_match_source": "TICKER_SIDE_SAME_TF",
        "win_rate": 0.61,
        "sample_size": 2,
    }, raw_score=75, min_sample_size=5)
    assert untrusted.historical_edge_valid is False
    assert untrusted.trusted_win_rate is None
    assert untrusted.trusted_sample_size == 0
    assert untrusted.should_block_client_eligibility is True


def test_missing_backtest_source_is_untrusted_by_default():
    decision = evaluate_historical_edge({"win_rate": 0.65, "sample_size": 99}, raw_score=75)

    assert decision.historical_edge_valid is False
    assert decision.reason == MISSING_SOURCE_REASON
    assert decision.trusted_win_rate is None
    assert decision.trusted_sample_size == 0
    assert decision.should_block_client_eligibility is True


def test_missing_source_with_avg_opt_ret_does_not_pass_as_trusted_evidence():
    decision = evaluate_historical_edge({"avg_opt_ret": 400}, raw_score=75)

    assert decision.historical_edge_valid is False
    assert decision.reason == MISSING_SOURCE_REASON
    assert decision.trusted_avg_opt_ret is None
    assert decision.should_block_client_eligibility is True
    assert decision.effective_score == 0


def test_diagnostics_preserve_original_payload_values():
    decision = evaluate_historical_edge({
        "backtest_match_source": "FALLBACK_DEFAULT",
        "win_rate": 0.65,
        "avg_opt_ret": 400,
        "rr_ratio": 1.5,
        "sample_size": 999,
    }, raw_score=75)
    meta = decision.to_metadata()

    assert meta["historical_edge_valid"] is False
    assert meta["trusted_win_rate"] is None
    assert meta["trusted_avg_opt_ret"] is None
    assert meta["trusted_rr_ratio"] is None
    assert meta["trusted_sample_size"] == 0
    assert meta["historical_edge_raw_values"] == {
        "backtest_match_source": "FALLBACK_DEFAULT",
        "win_rate": 0.65,
        "avg_opt_ret": 400,
        "rr_ratio": 1.5,
        "sample_size": 999,
    }


def test_hybrid_gate_blocks_if_fallback_default_history_was_needed_to_pass():
    _enable_gate()
    signal = _sig(
        score=75,
        backtest_match_source="FALLBACK_DEFAULT",
        win_rate=0.65,
        avg_opt_ret=400,
        rr_ratio=1.5,
        sample_size=999,
        score_breakdown={"historical_edge": 10},
    )

    gate = evaluate_client_quality_gate(signal, "client-1", EMPTY_SNAP)
    meta = gate.to_meta()

    assert gate.allowed is False
    assert gate.block_reason == BLOCK_REASON_FALLBACK_DEFAULT
    assert meta["historical_edge_valid"] is False
    assert meta["score"] == 0
    assert meta["raw_score"] == 75
    assert meta["trusted_win_rate"] is None
    assert meta["historical_edge_diagnostics"] == [FALLBACK_DEFAULT_REASON]


def test_fallback_default_score_above_floor_still_blocks_when_stats_present():
    _enable_gate()
    signal = _sig(
        score=85,
        backtest_match_source="FALLBACK_DEFAULT",
        win_rate=0.65,
        avg_opt_ret=400,
        rr_ratio=1.5,
        sample_size=999,
    )

    gate = evaluate_client_quality_gate(signal, "client-1", EMPTY_SNAP)
    meta = gate.to_meta()

    assert gate.allowed is False
    assert gate.block_reason == BLOCK_REASON_FALLBACK_DEFAULT
    assert meta["historical_edge_blocks_client_eligibility"] is True
    assert meta["score"] == 0
    assert meta["raw_score"] == 85


def test_production_payload_shape_blocks_fallback_default():
    _enable_gate()
    signal = _production_payload(score=75.0)

    gate = evaluate_client_quality_gate(signal, "jason", EMPTY_SNAP)
    meta = gate.to_meta()

    assert gate.allowed is False
    assert gate.block_reason == BLOCK_REASON_FALLBACK_DEFAULT
    assert meta["historical_edge_valid"] is False
    assert meta["trusted_win_rate"] is None
    assert meta["trusted_avg_opt_ret"] is None
    assert meta["trusted_rr_ratio"] is None
    assert meta["trusted_sample_size"] == 0
    assert meta["historical_edge_raw_values"]["win_rate"] == 0.65
    assert meta["historical_edge_raw_values"]["avg_opt_ret"] == 400
    assert meta["historical_edge_raw_values"]["rr_ratio"] == 1.5
    assert meta["historical_edge_raw_values"]["backtest_match_source"] == "FALLBACK_DEFAULT"


def test_observe_only_override_allows_diagnostics_without_client_block():
    _enable_gate()
    os.environ["HISTORICAL_EDGE_OBSERVE_ONLY_OVERRIDE"] = "true"
    signal = _sig(
        score=85,
        backtest_match_source="FALLBACK_DEFAULT",
        win_rate=0.65,
        avg_opt_ret=400,
        rr_ratio=1.5,
        sample_size=999,
    )

    gate = evaluate_client_quality_gate(signal, "client-1", EMPTY_SNAP)
    meta = gate.to_meta()

    assert gate.allowed is True
    assert meta["historical_edge_valid"] is False
    assert meta["historical_edge_observe_only_override"] is True
    assert meta["historical_edge_blocks_client_eligibility"] is False
    assert meta["score"] == 85
    os.environ["HISTORICAL_EDGE_OBSERVE_ONLY_OVERRIDE"] = "false"


def test_hybrid_gate_allows_real_source_with_valid_sample_size():
    _enable_gate()
    signal = _sig(
        score=75,
        backtest_match_source="TICKER_SIDE_SAME_TF",
        win_rate=0.65,
        avg_opt_ret=120,
        rr_ratio=1.7,
        sample_size=20,
        score_breakdown={"historical_edge": 10},
    )

    gate = evaluate_client_quality_gate(signal, "client-1", EMPTY_SNAP)
    meta = gate.to_meta()

    assert gate.allowed is True
    assert meta["historical_edge_valid"] is True
    assert meta["score"] == 75
    assert meta["trusted_win_rate"] == 0.65
    assert meta["trusted_sample_size"] == 20
