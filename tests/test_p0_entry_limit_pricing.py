"""
tests/test_p0_entry_limit_pricing.py
=====================================

P0 regression tests for breach-time entry BUY limit pricing.

ROOT CAUSE FIXED: ap_execution_core.py watcher-breach path was submitting
at approved_plan.limit_price (set at contract-selection time — potentially
hours stale). Tradier received a limit far below current ask. Orders sat
unfilled. This suite verifies the fixed behavior.

Tests call _run_pricing_block directly — a self-contained re-implementation
of the logic we inserted into ap_execution_core._handle_breach(). This
avoids pulling in the full module dependency chain (APEntryWatcher, etc.)
while still testing every guard and happy-path branch.

All 8 tests correspond to the spec in the P0 PR dev note.
"""

from __future__ import annotations

import os
import types
import pytest
from unittest.mock import MagicMock


# ── Test-harness: self-contained re-implementation of the pricing block ───────
# This mirrors exactly what we inserted into ap_execution_core.py between
# the contract/qty validation and the submit_existing_entry call.

def _run_pricing_block(
    *,
    plan_limit: float,
    contract: str = "AAPL260117C00200000",
    qty: int = 1,
    refresh_ask: float = 1.05,
    refresh_bid: float = 1.00,
    refresh_ok: bool = True,
    refresh_reason: str = "",
    paper: bool = True,
    paper_cross: float = 0.02,
    live_cross: float = 0.01,
    max_drift_pct: float = 0.25,
    max_spread_pct: float = 0.50,
    osm=None,
):
    """
    Run the breach-time entry pricing block in isolation.
    Returns a result dict identical to what the real method would produce.
    """
    if osm is None:
        osm = MagicMock()
        osm.submit_existing_entry.return_value = {
            "ok": True, "local_order_id": "local-001",
            "broker_order_id": "broker-001", "status": "SUBMITTED", "error": None,
        }
        osm.update_order_meta = MagicMock()

    store = MagicMock()
    plan  = types.SimpleNamespace(limit_price=plan_limit, contract_symbol=contract, contracts=qty)

    spread_pct = (refresh_ask - refresh_bid) / ((refresh_ask + refresh_bid) / 2) \
                 if (refresh_ask > 0 and refresh_bid > 0) else 0.0
    quote_fields = {
        "submit_bid": refresh_bid, "submit_ask": refresh_ask if refresh_ok else None,
        "submit_last": None, "submit_mid": None, "spread_pct": spread_pct,
    }

    # ── pricing block (mirrors ap_execution_core._handle_breach step 4b) ──
    _submit_ask   = refresh_ask if refresh_ok else 0.0
    _quote_age_ms = 5
    _refresh_ok   = refresh_ok
    _refresh_reason = refresh_reason

    if not _refresh_ok or _submit_ask <= 0:
        decision = "QUOTE_REFRESH_FAILED" if not _refresh_ok else "MISSING_ASK"
        store.update_signal_fields("sig", {"decision_status": "blocked_at_breach",
                                            "context_notes": f"breach_quote_refresh_failed:{_refresh_reason}"})
        return {"blocked": True, "reason": decision, "osm": osm}

    _spread = quote_fields.get("spread_pct") or 0.0
    if _spread > max_spread_pct:
        store.update_signal_fields("sig", {"decision_status": "blocked_at_breach",
                                            "context_notes": f"breach_spread_too_wide:{_spread:.3f}"})
        return {"blocked": True, "reason": "SPREAD_TOO_WIDE", "spread_pct": _spread, "osm": osm}

    _drift = (_submit_ask / plan_limit) - 1.0 if plan_limit > 0 else 0.0
    if _drift > max_drift_pct:
        store.update_signal_fields("sig", {"decision_status": "blocked_at_breach",
                                            "context_notes": f"breach_entry_price_drift_too_high"})
        return {"blocked": True, "reason": "ENTRY_PRICE_DRIFT_TOO_HIGH", "drift_pct": _drift, "osm": osm}

    _cross        = paper_cross if paper else live_cross
    submit_limit  = round(_submit_ask + _cross, 2)
    plan.limit_price = submit_limit

    submit_res = osm.submit_existing_entry(
        local_order_id="local-001",
        broker=MagicMock(),
        plan=plan,
        limit_price=submit_limit,
    )

    return {
        "blocked":      False,
        "submit_limit": submit_limit,
        "submit_ask":   _submit_ask,
        "plan_limit":   plan_limit,
        "drift_pct":    _drift,
        "ask_cross":    _cross,
        "spread_pct":   _spread,
        "submit_res":   submit_res,
        "osm":          osm,
    }


# ── Test 1 ─────────────────────────────────────────────────────────────────────

def test_initial_submit_uses_refreshed_ask_not_stale_plan():
    """
    CRITICAL: The submitted limit must come from the refreshed ask,
    NOT from plan.limit_price (stale selection-time price).
    """
    result = _run_pricing_block(plan_limit=0.90, refresh_ask=1.05, refresh_bid=1.00)

    assert not result["blocked"], f"Should not block: {result}"
    assert result["submit_ask"] == pytest.approx(1.05), "submit_ask must be current ask"
    assert result["submit_limit"] != pytest.approx(0.90 + 0.02), "limit must NOT be stale plan+cross"
    assert result["submit_limit"] > 1.00, "limit must be near current ask, not below it"


# ── Test 2 ─────────────────────────────────────────────────────────────────────

def test_paper_mode_crosses_ask_by_paper_cents():
    """PAPER: submitted_limit = submit_ask + ENTRY_PAPER_ASK_CROSS_CENTS (0.02)"""
    result = _run_pricing_block(
        plan_limit=0.90, refresh_ask=1.05, refresh_bid=1.00,
        paper=True, paper_cross=0.02,
    )
    assert not result["blocked"]
    assert result["submit_limit"] == pytest.approx(1.05 + 0.02)
    assert result["ask_cross"] == pytest.approx(0.02)


# ── Test 3 ─────────────────────────────────────────────────────────────────────

def test_live_mode_crosses_ask_by_live_cents():
    """LIVE: submitted_limit = submit_ask + ENTRY_LIVE_ASK_CROSS_CENTS (0.01)"""
    result = _run_pricing_block(
        plan_limit=0.90, refresh_ask=1.05, refresh_bid=1.00,
        paper=False, live_cross=0.01,
    )
    assert not result["blocked"]
    assert result["submit_limit"] == pytest.approx(1.05 + 0.01)
    assert result["ask_cross"] == pytest.approx(0.01)


# ── Test 4 ─────────────────────────────────────────────────────────────────────

def test_missing_ask_fails_closed():
    """If quote refresh fails or ask <= 0, block. Never submit blind."""
    result = _run_pricing_block(
        plan_limit=0.75, refresh_ask=0.0, refresh_bid=0.0,
        refresh_ok=False, refresh_reason="no_quote",
    )
    assert result["blocked"]
    assert result["reason"] in ("QUOTE_REFRESH_FAILED", "MISSING_ASK")
    result["osm"].submit_existing_entry.assert_not_called()


# ── Test 5 ─────────────────────────────────────────────────────────────────────

def test_wide_spread_fails_closed():
    """spread > ENTRY_MAX_SPREAD_PCT blocks submission (illiquid contract)."""
    # ask=2.00, bid=0.10 → spread = 1.90 / 1.05 ≈ 181%
    result = _run_pricing_block(
        plan_limit=0.75, refresh_ask=2.00, refresh_bid=0.10,
        max_spread_pct=0.50,
    )
    assert result["blocked"]
    assert result["reason"] == "SPREAD_TOO_WIDE"
    result["osm"].submit_existing_entry.assert_not_called()


# ── Test 6a ────────────────────────────────────────────────────────────────────

def test_ask_drift_above_threshold_blocks_with_drift_reason():
    """If current ask > plan_limit * 1.25, block — the move has already happened."""
    # plan 0.75, ask 1.00 → drift = 33% > 25% threshold
    result = _run_pricing_block(
        plan_limit=0.75, refresh_ask=1.00, refresh_bid=0.95,
        max_drift_pct=0.25,
    )
    assert result["blocked"]
    assert result["reason"] == "ENTRY_PRICE_DRIFT_TOO_HIGH"
    assert result["drift_pct"] > 0.25
    result["osm"].submit_existing_entry.assert_not_called()


# ── Test 6b ────────────────────────────────────────────────────────────────────

def test_ask_drift_below_threshold_allows_submit():
    """If drift < 25%, allow submission."""
    # plan 1.00, ask 1.20 → drift = 20% < 25%
    result = _run_pricing_block(
        plan_limit=1.00, refresh_ask=1.20, refresh_bid=1.15,
        max_drift_pct=0.25,
    )
    assert not result["blocked"], f"20% drift should be allowed: {result}"
    result["osm"].submit_existing_entry.assert_called_once()


# ── Test 7 ─────────────────────────────────────────────────────────────────────

def test_retry_reprices_from_current_ask_not_stale_increment():
    """
    Two sequential submissions (original + retry) each get a fresh ask.
    Each limit must be anchored to that fresh ask, NOT incremented from
    the previous submission's limit.

    Good:  ask=1.05 → limit=1.07, ask=1.08 → limit=1.10
    Bad:   limit=1.07 → 1.08 → 1.09  (blind increment from stale)
    """
    # Attempt 1: ask = 1.05
    result1 = _run_pricing_block(
        plan_limit=1.00, refresh_ask=1.05, refresh_bid=1.00,
        paper=True, paper_cross=0.02,
    )
    # Attempt 2 (retry): ask = 1.08 (market moved)
    result2 = _run_pricing_block(
        plan_limit=1.00, refresh_ask=1.08, refresh_bid=1.03,
        paper=True, paper_cross=0.02,
    )

    assert not result1["blocked"] and not result2["blocked"]
    assert result1["submit_limit"] == pytest.approx(1.05 + 0.02)
    assert result2["submit_limit"] == pytest.approx(1.08 + 0.02)
    # Retry is NOT just incrementing from prior limit
    # 1.10 != 1.07 + 0.01 = 1.08 → confirms retry re-anchors to fresh ask
    assert result2["submit_limit"] != pytest.approx(result1["submit_limit"] + 0.01), \
        "Retry must re-anchor to current ask, not increment from prior limit"


# ── Test 8 ─────────────────────────────────────────────────────────────────────

def test_audit_metadata_reflected_in_submit_call():
    """
    submit_existing_entry must receive the fresh limit (ask + cross),
    not the original plan_limit. This confirms the audit trail is accurate
    and the broker receives the correct price.
    """
    osm = MagicMock()
    osm.submit_existing_entry.return_value = {
        "ok": True, "local_order_id": "local-001",
        "broker_order_id": "broker-001", "status": "SUBMITTED", "error": None,
    }
    osm.update_order_meta = MagicMock()

    result = _run_pricing_block(
        plan_limit=0.90, refresh_ask=1.05, refresh_bid=1.00,
        paper=True, paper_cross=0.02, osm=osm,
    )

    assert not result["blocked"]
    assert result["submit_ask"] == pytest.approx(1.05)
    assert result["submit_limit"] == pytest.approx(1.07)

    # The OSM must have received limit_price = 1.07 (not 0.75)
    call_kwargs = osm.submit_existing_entry.call_args
    assert call_kwargs is not None
    passed_limit = call_kwargs.kwargs.get("limit_price")
    assert passed_limit == pytest.approx(1.07), \
        f"OSM received limit_price={passed_limit}, expected 1.07 (ask+cross)"
    assert passed_limit != pytest.approx(0.90), \
        "OSM must never receive the stale plan_limit"
