from __future__ import annotations


def _signal(**overrides):
    base = {
        "signal_id": "sig-profile-1",
        "ticker": "AAPL",
        "symbol": "AAPL",
        "side": "CALL",
        "direction": "CALL",
        "pattern": "2-3",
        "timeframe": "1d",
        "score": 82,
        "entry_price": 100.0,
        "trigger_price": 100.0,
        "stop_price": 98.0,
        "target_price": 106.0,
        "current_price": 101.0,
        "spread_pct": 0.05,
        "delta": 0.42,
        "open_interest": 1500,
        "option_volume": 400,
        "dte": 1,
        "relative_volume": 1.9,
        "win_rate": 0.78,
        "sample_size": 24,
        "avg_opt_ret": 0.21,
        "levels": {"daily_trigger": 100.05, "weekly_level": 100.10},
        "candles": {
            "monthly": [{"high": 99, "low": 90}, {"high": 104, "low": 96}],
            "weekly": [{"high": 99, "low": 95}, {"high": 103, "low": 97}],
            "daily": [{"high": 99.5, "low": 97}, {"high": 102, "low": 99}],
            "4h": [
                {"open": 98.0, "high": 99.0, "low": 97.7, "close": 98.5, "volume": 1000},
                {"open": 98.6, "high": 99.4, "low": 98.1, "close": 99.0, "volume": 1100},
                {"open": 99.7, "high": 101.0, "low": 99.4, "close": 100.5, "volume": 1900},
            ],
        },
    }
    base.update(overrides)
    return base


def test_trade_dossier_build_attaches_observe_only_profile_diagnostics():
    from ap import trade_dossier

    dossier = trade_dossier.build_trade_dossier(
        _signal(),
        client_id="client@example.com",
        execution_mode="LIVE",
        decision_context={
            "master_control_decision": "APPROVE",
            "decision_reason": "test approved",
            "approved": True,
            "git_commit": "sha-test",
            "config_hash": "cfg-test",
        },
    )

    profile = dossier["dossier"]["ap_trade_profile"]
    assert profile["profile_diagnostics_version"] == "ap_trade_profile_diagnostics_v1_observe_only"
    assert profile["observe_only"] is True
    assert profile["live_behavior_changed"] is False
    assert profile["diagnostics"]["broker_submit_touched"] is False
    assert profile["diagnostics"]["broker_cancel_touched"] is False
    assert profile["diagnostics"]["orders_mutated"] is False
    assert profile["diagnostics"]["positions_mutated"] is False
    assert profile["diagnostics"]["queue_mutated"] is False

    market_context = profile["market_context"]
    assert market_context["diagnostics"]["observe_only"] is True
    assert market_context["diagnostics"]["fake_data_used"] is False

    position_profile = profile["position_score_profile"]
    assert position_profile["profile_version"] == "position_score_profile_v1_observe_only"
    assert position_profile["observe_only"] is True
    assert position_profile["diagnostics"]["live_behavior_changed"] is False
    assert "historical_feedback" in position_profile["components"]


def test_trade_profile_wrapper_preserves_core_trade_dossier_shape():
    from ap import trade_dossier

    original = getattr(trade_dossier, "_AP_PROFILE_DIAGNOSTICS_ORIGINAL_BUILD")
    signal = _signal()
    kwargs = {
        "client_id": "client@example.com",
        "execution_mode": "LIVE",
        "decision_context": {
            "master_control_decision": "APPROVE",
            "decision_reason": "test approved",
            "approved": True,
            "git_commit": "sha-test",
            "config_hash": "cfg-test",
        },
    }

    baseline = original(signal, **kwargs)
    wrapped = trade_dossier.build_trade_dossier(signal, **kwargs)

    stable_top_level_keys = {
        "dossier_id",
        "signal_id",
        "canonical_signal_id",
        "client_id",
        "execution_mode",
        "trade_date",
        "ticker",
        "direction",
        "strategy",
        "timeframe",
        "dossier_status",
        "case_quality_score",
        "case_grade",
        "decision",
        "decision_reason",
        "primary_strength",
        "primary_risk",
        "git_commit",
        "config_hash",
        "schema_version",
    }
    for key in stable_top_level_keys:
        assert wrapped[key] == baseline[key]

    assert "ap_trade_profile" not in baseline["dossier"]
    assert "ap_trade_profile" in wrapped["dossier"]

    wrapped_without_profile = dict(wrapped["dossier"])
    wrapped_without_profile.pop("ap_trade_profile")
    for key in ("identity", "levels", "case_score", "decision_snapshot", "report_only"):
        assert wrapped_without_profile[key] == baseline["dossier"][key]
