from ap_proof_logger import (
    APProofLogger,
    classify_exit,
    is_positive_training_label,
    is_target_hit_option_loss,
    normalize_trade_outcome,
)


def _log_trade(**overrides):
    logger = APProofLogger(supabase_client=None, client_email="jasoncosby1@gmail.com", mode="live")
    payload = {
        "ticker": "WFC",
        "pattern": "1-2_2U",
        "side": "CALL",
        "timeframe": "1d",
        "score": 82.0,
        "tier": "A",
        "context_score": 70.0,
        "setup_status": "APPROVED",
        "entry_trigger": 77.62,
        "entry_option_price": 1.0,
        "exit_option_price": 1.12,
        "underlying_entry": 77.70,
        "underlying_exit": 78.56,
        "contracts": 1,
        "exit_reason": "TARGET HIT -- underlying target reached",
        "option_pnl_pct": 12.0,
        "underlying_pnl_pct": 1.1,
        "win": True,
    }
    payload.update(overrides)
    return logger.log_trade(**payload)


def test_target_hit_with_positive_option_pnl_remains_clean_win():
    row = _log_trade(option_pnl_pct=12.0, exit_option_price=1.12, win=True)

    assert row["win"] is True
    assert row["target_hit_option_loss"] is False
    assert row["exit_bucket"] == "WIN_BASE_HIT"
    assert classify_exit("TARGET HIT -- underlying target reached", 12.0, True) == "WIN_BASE_HIT"
    assert is_positive_training_label(row) is True


def test_target_hit_with_negative_option_pnl_is_not_clean_win():
    row = _log_trade(option_pnl_pct=-6.5, exit_option_price=0.935, win=True)

    assert is_target_hit_option_loss(row["exit_reason"], row["option_pnl_pct"]) is True
    assert row["win"] is False
    assert row["target_hit_option_loss"] is True
    assert row["exit_bucket"] == "TARGET_HIT_OPTION_LOSS"
    assert is_positive_training_label(row) is False


def test_training_label_excludes_target_hit_option_losses_even_if_raw_win_was_true():
    outcome = normalize_trade_outcome(
        "TARGET_HIT -- underlying target reached",
        option_pnl_pct=-0.01,
        win=True,
    )
    row = {
        "exit_reason": "TARGET_HIT -- underlying target reached",
        "option_pnl_pct": -0.01,
        **outcome,
    }

    assert outcome == {
        "win": False,
        "exit_bucket": "TARGET_HIT_OPTION_LOSS",
        "target_hit_option_loss": True,
    }
    assert is_positive_training_label(row) is False
