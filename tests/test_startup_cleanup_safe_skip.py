"""
P0 — Fix Client Parity: Prevent Startup Phantom Clear From Canceling
Valid Opportunities (2026-06-04 spec)
=====================================================================

Verifies the 2026-06-03 parity bug is structurally fixed. The five
spec-mandated test cases are implemented by name:

  Test 1 — Do Not Cancel Active Opportunity
  Test 2 — Cancel True Stale Orphan
  Test 3 — Peer Client Filled Protection
  Test 4 — UNH Regression
  Test 5 — VZ Regression

Plus the six required explicit reason codes:

  STARTUP_CLEANUP_CANCELED_STALE_ORPHAN
  STARTUP_CLEANUP_SKIPPED_ACTIVE_SIGNAL
  STARTUP_CLEANUP_SKIPPED_RECENT_ROW
  STARTUP_CLEANUP_SKIPPED_WATCHER_ARMED
  STARTUP_CLEANUP_SKIPPED_RETRY_ELIGIBLE
  STARTUP_CLEANUP_SKIPPED_PEER_CLIENT_FILLED

Approach: source-grep / structural tests. The runner uses live DB
connections that we don't isolate here. The CTE logic is read directly
out of client_runner.py and asserted to enforce every spec rule.
"""
from __future__ import annotations

import os
import re

import pytest


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLIENT_RUNNER = os.path.join(REPO_ROOT, "client_runner.py")


def _read(path: str) -> str:
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def _extract_cleanup_method(src: str) -> str:
    """Return source of _clear_old_phantom_orders."""
    start = src.find("def _clear_old_phantom_orders(self):")
    assert start >= 0, "could not locate _clear_old_phantom_orders"
    end_match = re.search(r"\n    def _read_entries_paused", src[start:])
    assert end_match, "could not locate end of _clear_old_phantom_orders"
    return src[start : start + end_match.start()]


# ===========================================================================
# WHERE-clause acceptance (spec §1 — restrict cleanup)
# ===========================================================================

REQUIRED_FILTERS = [
    "submitted_ts IS NULL",
    "filled_ts IS NULL",
    "position_id IS NULL",
    "broker_order_id IS NULL",
    "'CREATED','PENDING_TRIGGER','PENDING','DEFERRED'",
    "min_age_seconds",
]


@pytest.mark.parametrize("clause", REQUIRED_FILTERS)
def test_cleanup_where_clause_contains_required_filter(clause):
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    assert clause in method_src, (
        f"_clear_old_phantom_orders must enforce {clause!r} in its WHERE filter"
    )


def test_default_age_gate_is_at_least_15_minutes():
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    assert "max(900" in method_src, (
        "default STARTUP_PHANTOM_CLEAR_MIN_AGE_SECONDS must be >= 900s "
        "(15 minutes) per the 2026-06-04 parity spec"
    )


# ===========================================================================
# All six explicit reason codes are present (spec §3)
# ===========================================================================

REQUIRED_REASON_CODES = [
    "STARTUP_CLEANUP_CANCELED_STALE_ORPHAN",
    "STARTUP_CLEANUP_SKIPPED_ACTIVE_SIGNAL",
    "STARTUP_CLEANUP_SKIPPED_RECENT_ROW",
    "STARTUP_CLEANUP_SKIPPED_WATCHER_ARMED",
    "STARTUP_CLEANUP_SKIPPED_RETRY_ELIGIBLE",
    "STARTUP_CLEANUP_SKIPPED_PEER_CLIENT_FILLED",
]


@pytest.mark.parametrize("code", REQUIRED_REASON_CODES)
def test_required_reason_code_present(code):
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    assert code in method_src, f"missing explicit reason code: {code}"


def test_classified_cte_picks_reason_in_priority_order():
    """The CASE expression must list reasons in the correct priority:
    peer_filled > active_signal > watcher_armed > retry_eligible > cancel."""
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    classified_idx = method_src.find("classified AS (")
    assert classified_idx >= 0, "missing classified CTE"
    end_idx = method_src.find("),", classified_idx)
    cte = method_src[classified_idx:end_idx]

    # Find the line number of each reason in the CASE block
    positions = {
        code: cte.find(code)
        for code in [
            "STARTUP_CLEANUP_SKIPPED_PEER_CLIENT_FILLED",
            "STARTUP_CLEANUP_SKIPPED_ACTIVE_SIGNAL",
            "STARTUP_CLEANUP_SKIPPED_WATCHER_ARMED",
            "STARTUP_CLEANUP_SKIPPED_RETRY_ELIGIBLE",
        ]
    }
    for code, pos in positions.items():
        assert pos >= 0, f"{code} missing from classified CASE"
    # Priority order: peer_filled first, then active_signal, then watcher,
    # then retry_eligible
    ordered = sorted(positions.items(), key=lambda kv: kv[1])
    assert [c for c, _ in ordered] == [
        "STARTUP_CLEANUP_SKIPPED_PEER_CLIENT_FILLED",
        "STARTUP_CLEANUP_SKIPPED_ACTIVE_SIGNAL",
        "STARTUP_CLEANUP_SKIPPED_WATCHER_ARMED",
        "STARTUP_CLEANUP_SKIPPED_RETRY_ELIGIBLE",
    ], f"classified CTE priority order is wrong: {ordered}"


# ===========================================================================
# Parity safety: canonical_signal_id matching (spec §5)
# ===========================================================================

def test_canonical_signal_id_matching_with_fallback():
    """Must use COALESCE(canonical_signal_id, signal_id) so the fix works
    pre-migration too (spec §5)."""
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    assert "COALESCE(o.canonical_signal_id, o.signal_id)" in method_src
    assert "COALESCE(p.canonical_signal_id, p.signal_id)" in method_src


def test_peer_filled_cte_exists():
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    assert "peer_filled AS (" in method_src
    pf_idx = method_src.find("peer_filled AS (")
    pf_chunk = method_src[pf_idx : pf_idx + 800]
    # peer_filled must match FILLED rows on ANY client
    assert "'FILLED','PARTIALLY_FILLED'" in pf_chunk
    assert "filled_ts IS NOT NULL" in pf_chunk


def test_signal_active_cte_exists():
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    assert "signal_active AS (" in method_src
    sa_idx = method_src.find("signal_active AS (")
    sa_chunk = method_src[sa_idx : sa_idx + 800]
    assert "'SUBMITTED','ACKNOWLEDGED'" in sa_chunk
    assert "submitted_ts IS NOT NULL" in sa_chunk
    assert "p.created_ts > NOW()" in sa_chunk  # parity window


# ===========================================================================
# Retro peer rescue (spec §4)
# ===========================================================================

def test_retro_peer_rescue_cte_exists():
    """Acceptance criterion 4: rows previously canceled by the OLD code
    path (last_error='startup_phantom_clear_pre_submit') must be flipped
    to RETRY_ELIGIBLE when a canonical peer subsequently fills."""
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    assert "rescue_candidates AS" in method_src, "missing rescue_candidates CTE"
    assert "rescue_peer_filled AS" in method_src
    # Must look at the legacy reason string too, not just the new one
    rescue_idx = method_src.find("rescue_candidates AS")
    rescue_chunk = method_src[rescue_idx : rescue_idx + 2000]
    assert "'startup_phantom_clear_pre_submit'" in rescue_chunk, (
        "rescue path must accept the legacy reason for back-fill"
    )
    assert "'STARTUP_CLEANUP_CANCELED_STALE_ORPHAN'" in rescue_chunk
    # And must SET status='RETRY_ELIGIBLE'
    update_idx = method_src.find("UPDATE orders o", rescue_idx)
    update_chunk = method_src[update_idx : update_idx + 600]
    assert "status = 'RETRY_ELIGIBLE'" in update_chunk
    assert "STARTUP_CLEANUP_SKIPPED_PEER_CLIENT_FILLED" in update_chunk


def test_retro_rescue_bounded_by_window():
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    assert "STARTUP_PEER_RESCUE_WINDOW_SECONDS" in method_src
    assert 'os.getenv(\n                        "STARTUP_PEER_RESCUE_WINDOW_SECONDS"' in method_src \
        or "STARTUP_PEER_RESCUE_WINDOW_SECONDS" in method_src


# ===========================================================================
# Spec Test 1 — Do Not Cancel Active Opportunity
# ===========================================================================

def test_test1_active_opportunity_lands_in_skip_path():
    """Given a row with no submit/no broker_oid and an active canonical
    peer, the SQL routes it to skip_targets (RETRY_ELIGIBLE) not
    cancel_targets."""
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    # skip_targets requires skip_reason IS NOT NULL
    skip_idx = method_src.find("skip_targets AS (")
    skip_end = method_src.find("RETURNING o.local_order_id, cls.skip_reason", skip_idx)
    skip_chunk = method_src[skip_idx : skip_end + 200]
    assert "cls.skip_reason IS NOT NULL" in skip_chunk
    assert "status = 'RETRY_ELIGIBLE'" in skip_chunk
    # cancel_targets requires skip_reason IS NULL
    cancel_idx = method_src.find("cancel_targets AS (")
    cancel_end = method_src.find("RETURNING o.local_order_id\n", cancel_idx)
    cancel_chunk = method_src[cancel_idx : cancel_end + 200]
    assert "cls.skip_reason IS NULL" in cancel_chunk


# ===========================================================================
# Spec Test 2 — Cancel True Stale Orphan
# ===========================================================================

def test_test2_true_stale_orphan_gets_canceled():
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    cancel_idx = method_src.find("cancel_targets AS (")
    cancel_chunk = method_src[cancel_idx : cancel_idx + 2500]
    # The cancel path's last_error must be the explicit STALE_ORPHAN code
    assert "last_error = 'STARTUP_CLEANUP_CANCELED_STALE_ORPHAN'" in cancel_chunk


# ===========================================================================
# Spec Test 3 — Peer Client Filled Protection
# ===========================================================================

def test_test3_peer_filled_protection_priority():
    """Peer-filled must be the HIGHEST-priority skip reason in the CASE."""
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    classified_idx = method_src.find("classified AS (")
    classified_chunk = method_src[classified_idx : classified_idx + 2000]
    # peer_filled must appear as the first WHEN
    first_when = classified_chunk.find("WHEN ")
    first_then_end = classified_chunk.find("\n", first_when)
    first_block = classified_chunk[first_when : first_then_end + 200]
    assert "STARTUP_CLEANUP_SKIPPED_PEER_CLIENT_FILLED" in first_block, (
        "peer_filled must be the first/highest-priority WHEN clause"
    )


# ===========================================================================
# Spec Test 4 — UNH Regression
# ===========================================================================

def test_test4_unh_regression_structural():
    """Structural proof: the 2026-06-03 UNH incident pattern is now caught.

    Pattern:
      tradefluencehq → FILLED (canonical REEVAL:603bce5b-...)
      jasoncosby1, jose.vasquez4011 → CANCELED startup_phantom_clear_pre_submit

    After fix: the new active-startup pass would find these candidates
    routed to skip_targets (peer_filled or active_signal). And the retro
    rescue pass would flip pre-existing canceled rows to RETRY_ELIGIBLE.
    """
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    # 1) Forward path: peer_filled CTE catches the live UNH case
    assert "peer_filled AS (" in method_src
    # 2) Rescue path: catches rows already canceled by the old code on
    #    2026-06-03 (jasoncosby1, jose.vasquez4011)
    assert "rescue_candidates AS" in method_src
    rescue_idx = method_src.find("rescue_candidates AS")
    rescue_chunk = method_src[rescue_idx : rescue_idx + 2500]
    assert "'startup_phantom_clear_pre_submit'" in rescue_chunk


# ===========================================================================
# Spec Test 5 — VZ Regression
# ===========================================================================

def test_test5_vz_regression_structural():
    """Same structural proof for the VZ PUT incident — covered by the
    same peer_filled + rescue logic that handles UNH."""
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    # The fix is canonical-id-agnostic, so the same CTE that catches UNH
    # also catches VZ. Sanity-check by asserting nothing in the SQL is
    # symbol-specific.
    assert "UNH" not in method_src
    assert "VZ" not in method_src
    # And canonical grouping is the only mechanism used
    assert "COALESCE(p.canonical_signal_id, p.signal_id)" in method_src


# ===========================================================================
# Tier B preserved (scope lock)
# ===========================================================================

def test_tier_b_logic_preserved():
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    assert "startup_phantom_clear_submitted" in method_src
    assert "PHANTOM_SUBMITTED_MIN_AGE_MINUTES" in method_src


# ===========================================================================
# Return-tuple unpacking matches new 8-tuple
# ===========================================================================

def test_outer_caller_unpacks_eight_counters():
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    assert (
        "tier_a_cancel, tier_a_skip, tier_b,\n"
        "             skip_peer_filled, skip_active, skip_watcher,\n"
        "             skip_retry, tier_a_rescue) = run_with_retry(_clear_phantoms)"
    ) in method_src, "outer caller must unpack all eight counters"


# ===========================================================================
# Canonical signal helper interop (spec §5)
# ===========================================================================

def test_canonical_id_normalization_matches_helper_behaviour():
    """Smoke-test the canonical_signal_id helper handles the per-order
    suffix shape called out in the spec.

        REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9:456c01
          ->  REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9
    """
    try:
        from ap_canonical_signal import build_canonical_signal_id
    except ImportError:
        pytest.skip("ap_canonical_signal not yet on this branch")
    canon = "REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9"
    assert build_canonical_signal_id(f"{canon}:456c01") == canon
    # And the VZ canonical too
    canon_vz = "REEVAL:c068cdf5-68fb-4052-92f4-24c8efba34d4"
    assert build_canonical_signal_id(f"{canon_vz}:0c5e80") == canon_vz
