from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ap.live_submit_safety import (
    require_current_quote,
    require_fresh_trigger,
    require_live_identity,
    require_remaining_opportunity,
)


def test_live_identity_blocks_blank_execution_mode():
    decision = require_live_identity(client_id="client@example.com", execution_mode="")
    assert decision.ok is False
    assert decision.reason == "LIVE_SUBMIT_BLOCK_BLANK_OR_UNKNOWN_EXECUTION_MODE"


def test_live_identity_blocks_missing_client_id():
    decision = require_live_identity(client_id="", execution_mode="live")
    assert decision.ok is False
    assert decision.reason == "LIVE_SUBMIT_BLOCK_MISSING_CLIENT_ID"


def test_live_identity_allows_explicit_live():
    assert require_live_identity(client_id="client@example.com", execution_mode="live").ok is True


def test_fresh_trigger_blocks_old_breach():
    old = datetime.now(timezone.utc) - timedelta(seconds=300)
    decision = require_fresh_trigger(trigger_crossed_at=old, max_age_seconds=120)
    assert decision.ok is False
    assert decision.reason == "STALE_TRIGGER_BREACH"


def test_fresh_trigger_requires_timestamp():
    decision = require_fresh_trigger(trigger_crossed_at=None, max_age_seconds=120)
    assert decision.ok is False
    assert decision.reason == "LIVE_SUBMIT_BLOCK_MISSING_TRIGGER_CROSSED_AT"


def test_current_quote_fails_closed_when_missing():
    decision, mid = require_current_quote(bid=0, ask=0, last=0)
    assert decision.ok is False
    assert decision.reason == "LIVE_SUBMIT_BLOCK_MISSING_UNDERLYING_QUOTE"
    assert mid == 0


def test_call_blocks_after_target_already_touched_or_consumed():
    decision = require_remaining_opportunity(
        side="CALL",
        current_price=100,
        trigger_price=98,
        target_price=100,
    )
    assert decision.ok is False
    assert decision.reason == "TARGET_ALREADY_INVALID"


def test_call_blocks_when_no_longer_above_trigger():
    decision = require_remaining_opportunity(
        side="CALL",
        current_price=97,
        trigger_price=98,
        target_price=104,
    )
    assert decision.ok is False
    assert decision.reason == "CALL_NOT_STILL_ABOVE_TRIGGER"


def test_put_blocks_when_no_longer_below_trigger():
    decision = require_remaining_opportunity(
        side="PUT",
        current_price=101,
        trigger_price=100,
        target_price=95,
    )
    assert decision.ok is False
    assert decision.reason == "PUT_NOT_STILL_BELOW_TRIGGER"


def test_remaining_opportunity_allows_valid_call():
    decision = require_remaining_opportunity(
        side="CALL",
        current_price=99,
        trigger_price=98,
        target_price=104,
        min_remaining_fraction=0.25,
    )
    assert decision.ok is True
