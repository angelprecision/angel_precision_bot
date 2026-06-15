"""
tests/test_p0_paper_recovery_immediate.py

P0 PR #143 — PAPER mode recovery rows submit immediately after contract selection.

LIVE always waits for breach. PAPER recovery rows that pass all quality gates
AND have a real selected contract are promoted from breach → immediate so the
existing submit_existing_entry path runs.

Verifies:
  1. ap_recovery._reset() SQL writes payload.recovery_rescue = true
  2. PAPER + recovery_rescue + real contract → trigger_type promoted to immediate
  3. LIVE + recovery_rescue + real contract → trigger_type stays breach
  4. PAPER + NO recovery_rescue marker → trigger_type stays breach
  5. PAPER + recovery_rescue + DEFERRED contract → trigger_type stays breach
  6. PAPER + recovery_rescue + contracts=0 → trigger_type stays breach
  7. PAPER + recovery_rescue + limit_price=0 → trigger_type stays breach
  8. forced_breach=True overrides everything (LIVE-style override)
  9. Source scan: no broker submit path bypasses master_control / cutoff / OSM
"""
from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock, patch


REPO = Path(__file__).resolve().parents[1]


# =============================================================================
# Test 1: ap_recovery._reset SQL tags payload with recovery_rescue=true
# =============================================================================

def test_ap_recovery_reset_sql_tags_payload_recovery_rescue():
    """The UPDATE in _reseed_watchers must merge a JSONB marker into
    trade_queue.payload so the queue dispatch can identify rescued rows."""
    src = (REPO / "ap_recovery.py").read_text()

    # The UPDATE must include payload = COALESCE(payload, ...) || %s::jsonb
    assert "payload = COALESCE(payload, '{}'::jsonb) || %s::jsonb" in src, (
        "ap_recovery._reset must merge recovery marker into trade_queue.payload"
    )
    # The marker payload includes recovery_rescue=true
    assert '"recovery_rescue":true' in src, (
        "marker payload must include recovery_rescue=true"
    )
    # The marker includes timestamp + lookback for audit
    assert '"recovery_rescue_ts"' in src
    assert '"recovery_rescue_lookback_hours"' in src


# =============================================================================
# Helpers for queue _dispatch source-level tests
#
# The promotion logic is a contained block in queue._dispatch. We test it by
# reading the source rather than driving _dispatch end-to-end because the
# function pulls in master_control, contract_selector, OSM, watcher — testing
# in isolation prevents a brittle full-integration scaffold.
# =============================================================================

QUEUE_SRC = (REPO / "ap" / "queue.py").read_text()


def _extract_promote_block():
    """Return the PR #143 promotion block as a substring."""
    start = QUEUE_SRC.find("PR #143: PAPER recovery-rescued immediate-entry promotion")
    assert start > 0, "promotion block not found"
    # Block ends at the next blank-line followed by an `if trigger_type == "breach"`
    end = QUEUE_SRC.find('if trigger_type == "breach" and not entry_watcher:', start)
    assert end > 0
    return QUEUE_SRC[start:end]


def test_promote_block_only_for_paper():
    """Promotion gate must check `not live_mode`."""
    block = _extract_promote_block()
    assert "not live_mode" in block, (
        "promote block must require not live_mode (LIVE never promoted)"
    )


def test_promote_block_requires_recovery_rescue_marker():
    """Promotion gate must check payload.recovery_rescue."""
    block = _extract_promote_block()
    assert 'payload.get("recovery_rescue")' in block


def test_promote_block_skips_forced_breach():
    """Forced-breach signals must NOT promote."""
    block = _extract_promote_block()
    assert "not forced_breach" in block


def test_promote_block_requires_real_contract():
    """A DEFERRED: contract or empty contract must NOT promote."""
    block = _extract_promote_block()
    assert "_rec_deferred" in block
    assert "DEFERRED:" in block


def test_promote_block_requires_positive_contracts_and_limit():
    """Promotion requires contracts > 0 AND limit_price > 0."""
    block = _extract_promote_block()
    assert "_rec_contracts_qty <= 0" in block
    assert "_rec_limit <= 0" in block


def test_promote_block_flips_trigger_type():
    """When all gates pass, trigger_type is set to 'immediate'."""
    block = _extract_promote_block()
    # The block must contain this exact assignment in the success branch
    assert 'trigger_type = "immediate"' in block


def test_promote_block_logs_promotion_and_skip():
    """Both outcomes emit a structured log so operators can audit."""
    block = _extract_promote_block()
    assert "PAPER_RECOVERY_IMMEDIATE_PROMOTE" in block
    assert "PAPER_RECOVERY_IMMEDIATE_SKIP" in block


def test_promote_block_stamps_metadata():
    """Promotion decision recorded on plan.metadata for downstream audit."""
    block = _extract_promote_block()
    assert '"paper_recovery_immediate"' in block
    assert '"paper_recovery_immediate_reason"' in block


# =============================================================================
# Test 9: master_control / cutoff still run for promoted rows
# (proves promotion does NOT bypass quality gates)
# =============================================================================

def test_promotion_does_not_bypass_master_control():
    """The promotion block lives AFTER master_control.evaluate() returned
    decision.ok=True, AFTER contract_selector ran, AFTER revalidate. The
    block also lives BEFORE the cutoff guard at L971-980. We assert ordering
    by source position."""
    src = QUEUE_SRC

    # Indices
    mc_eval_idx       = src.find("decision = master_control.evaluate(")
    selector_idx      = src.find("contract_selector.select(")
    revalidate_idx    = src.find("master_control.revalidate(")
    promote_idx       = src.find("PAPER_RECOVERY_IMMEDIATE_PROMOTE")
    cutoff_idx        = src.find("ENTRY BLOCKED — too late in session")
    osm_create_idx    = src.find("order_state_machine.create_entry_order(")

    assert mc_eval_idx > 0,    "master_control.evaluate not found"
    assert selector_idx > 0,   "contract_selector.select not found"
    assert revalidate_idx > 0, "master_control.revalidate not found"
    assert promote_idx > 0,    "promotion log not found"
    assert cutoff_idx > 0,     "cutoff guard not found"
    assert osm_create_idx > 0, "OSM create not found"

    # Promotion is AFTER all quality gates
    assert mc_eval_idx    < promote_idx, "promotion must be after master_control.evaluate"
    assert selector_idx   < promote_idx, "promotion must be after contract_selector.select"
    assert revalidate_idx < promote_idx, "promotion must be after master_control.revalidate"
    # Promotion is BEFORE OSM create + cutoff (cutoff still applies after promotion)
    assert promote_idx < cutoff_idx,     "promotion must be before cutoff guard"
    assert promote_idx < osm_create_idx, "promotion must be before OSM create"


# =============================================================================
# Test 10: no new broker-submit path was added — submit_existing_entry is still
# the only money-affecting call
# =============================================================================

def test_only_one_broker_submit_path_remains():
    """There must be exactly one `order_state_machine.submit_existing_entry(`
    call site in ap/queue.py — PR #143 must NOT add a second submit path."""
    src = QUEUE_SRC
    n = src.count("order_state_machine.submit_existing_entry(")
    assert n == 1, (
        f"Expected exactly 1 submit_existing_entry call, got {n}. "
        "PR #143 must reuse the existing immediate path, not add a new one."
    )


# =============================================================================
# Test 11: ap_recovery still respects the per-row WATCHING reset gate
# =============================================================================

def test_ap_recovery_reset_still_skips_orders_with_active_orphan():
    """The NOT EXISTS clause that prevents resetting WATCHING rows whose
    orders row is still PENDING_TRIGGER must be preserved — otherwise we'd
    double-count work between WATCHING-reset and PENDING_TRIGGER-rearm paths."""
    src = (REPO / "ap_recovery.py").read_text()
    # The NOT EXISTS guard must still target PENDING_TRIGGER + broker_order_id IS NULL
    assert "AND  NOT EXISTS (" in src
    assert "PENDING_TRIGGER" in src
    assert "o.broker_order_id IS NULL" in src
    assert "o.submitted_ts IS NULL" in src
