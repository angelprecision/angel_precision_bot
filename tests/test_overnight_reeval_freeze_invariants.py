from __future__ import annotations

from pathlib import Path


_REPO = Path(__file__).resolve().parents[1]
_OVERNIGHT_SRC = (_REPO / "ap_overnight_reeval.py").read_text()


def _overnight_job_loop_prefix() -> str:
    start = _OVERNIGHT_SRC.find("for job in watching_signals:")
    assert start != -1, "overnight job loop not found"
    end = _OVERNIGHT_SRC.find("# Step 1: Fetch prior-day levels", start)
    assert end != -1, "prior-level fetch marker not found"
    return _OVERNIGHT_SRC[start:end]


def _step6b_block() -> str:
    start = _OVERNIGHT_SRC.find("# Step 6b:")
    assert start != -1, "Step 6b PENDING_TRIGGER marker not found"
    end = _OVERNIGHT_SRC.find("# Step 7:", start)
    assert end != -1, "Step 7 watcher-arm marker not found"
    return _OVERNIGHT_SRC[start:end]


def test_overnight_rejects_invalid_side_before_prior_levels_or_trigger_math():
    """Missing/invalid side must fail closed before broker levels or trigger derivation.

    Otherwise an invalid side can fall into the PUT-like prior_day_low branch before
    Master Control gets a chance to reject it.
    """
    prefix = _overnight_job_loop_prefix()

    assert "INVALID_OR_MISSING_SIDE" in prefix or "invalid_or_missing_side" in prefix
    assert "normalize_signal_side" in prefix or "CALL_ALIASES" in prefix
    assert "signal[\"side\"] = side" in prefix or "signal['side'] = side" in prefix
    assert "signal[\"direction\"] = side" in prefix or "signal['direction'] = side" in prefix
    assert "side = (signal.get(\"side\") or \"\").upper()" not in prefix


def test_overnight_trigger_derivation_does_not_treat_invalid_side_as_put():
    """Entry trigger fallback must be explicit CALL/PUT, never side!=CALL => PUT."""
    trigger_idx = _OVERNIGHT_SRC.find("# Step 2: Derive entry_trigger")
    assert trigger_idx != -1, "trigger derivation block not found"
    trigger_block = _OVERNIGHT_SRC[trigger_idx: trigger_idx + 700]

    assert "prior_day_high if side == \"CALL\" else prior_day_low" not in trigger_block
    assert "side == \"PUT\"" in trigger_block or "elif side == \"PUT\"" in trigger_block


def test_overnight_skips_redundant_pending_trigger_transition_when_already_pending():
    """Atomic create_entry_order(initial_status=PENDING_TRIGGER) must not require a second transition."""
    block = _step6b_block()

    assert "get_order" in block, "Step 6b must inspect the current OSM row before transitioning"
    assert "PENDING_TRIGGER" in block
    assert "_pt_ok = True" in block, "already-pending rows must be accepted as transition success"


def test_overnight_deferred_contract_keeps_selector_failure_metadata():
    """Pre-market selector failure should not reject, but the exact failure must survive downstream."""
    assert "pre_market_contract_selection_failed" in _OVERNIGHT_SRC
    assert "pre_market_selector_failure" in _OVERNIGHT_SRC
    assert "pre_market_selector_reason_code" in _OVERNIGHT_SRC
    assert "contract_selection_deferred_to" in _OVERNIGHT_SRC


def test_overnight_create_entry_order_stays_pending_trigger_and_does_not_submit():
    """This PR must preserve the no-broker-submit-before-breach invariant."""
    create_idx = _OVERNIGHT_SRC.find("order_state_machine.create_entry_order(")
    assert create_idx != -1, "create_entry_order call not found"
    create_block = _OVERNIGHT_SRC[create_idx: create_idx + 700]
    assert 'initial_status="PENDING_TRIGGER"' in create_block
    assert "execution_mode=" in create_block
    assert "meta=" in create_block

    pre_breach_region = _OVERNIGHT_SRC[: _OVERNIGHT_SRC.find("# Step 7:")]
    assert ".submit_order(" not in pre_breach_region
    assert ".submit_existing_entry(" not in pre_breach_region
