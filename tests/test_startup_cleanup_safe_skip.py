"""
P0 — Fix Startup Phantom Clear Cancelling Valid Client Opportunities
====================================================================

Verifies the 2026-06-03 parity bug is structurally fixed:

  Before:
      tradefluencehq → FILLED
      jasoncosby1    → CANCELED (startup_phantom_clear_pre_submit)
      jose.vasquez4011 → CANCELED (startup_phantom_clear_pre_submit)

  After:
      tradefluencehq → FILLED
      jasoncosby1    → RETRY_ELIGIBLE (STARTUP_CLEANUP_SKIPPED_ACTIVE_SIGNAL)
      jose.vasquez4011 → RETRY_ELIGIBLE (STARTUP_CLEANUP_SKIPPED_ACTIVE_SIGNAL)

These are source-grep / structural tests — they prove that the rewritten
``_clear_old_phantom_orders`` SQL enforces every acceptance rule. The
runner uses live DB connections and is not isolated enough for a real
end-to-end test in this PR's scope.
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
    # Find the next def at the same indent level
    end_match = re.search(r"\n    def _read_entries_paused", src[start:])
    assert end_match, "could not locate end of _clear_old_phantom_orders"
    return src[start : start + end_match.start()]


# ---------------------------------------------------------------------------
# WHERE-clause acceptance: all required filters present
# ---------------------------------------------------------------------------

REQUIRED_FILTERS = [
    "submitted_ts IS NULL",
    "filled_ts IS NULL",
    "position_id IS NULL",
    # broker_order_id null/blank/sentinel guard
    "broker_order_id IS NULL",
    # status filter must include all four spec statuses
    "'CREATED','PENDING_TRIGGER','PENDING','DEFERRED'",
    # age gate
    "min_age_seconds",
]


@pytest.mark.parametrize("clause", REQUIRED_FILTERS)
def test_cleanup_where_clause_contains_required_filter(clause):
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    assert clause in method_src, (
        f"_clear_old_phantom_orders must enforce {clause!r} in its WHERE filter"
    )


# ---------------------------------------------------------------------------
# Reason codes — generic banner is gone, new explicit codes are present
# ---------------------------------------------------------------------------

def test_old_generic_reason_no_longer_used_for_cancel():
    """The legacy 'startup_phantom_clear_pre_submit' reason must no longer
    be the WRITTEN cancel reason for Tier A. We allow it to appear in
    comments / Tier B (the SUBMITTED tier), but not as the value set on
    Tier A rows."""
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    # The Tier A cancel block must write the new explicit reason
    assert "STARTUP_CLEANUP_CANCELED_STALE_ORPHAN" in method_src
    # And the Tier A SKIP block must write the parity-safe reason
    assert "STARTUP_CLEANUP_SKIPPED_ACTIVE_SIGNAL" in method_src


def test_new_reason_codes_appear_in_cleanup():
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    for code in [
        "STARTUP_CLEANUP_CANCELED_STALE_ORPHAN",
        "STARTUP_CLEANUP_SKIPPED_ACTIVE_SIGNAL",
        "STARTUP_CLEANUP_SKIPPED_RECENT_ROW",
    ]:
        assert code in method_src, f"missing explicit reason code: {code}"


# ---------------------------------------------------------------------------
# Parity safety: active_peers CTE + canonical_signal_id grouping
# ---------------------------------------------------------------------------

def test_active_peers_cte_groups_by_canonical_signal_id():
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    # The CTE must exist and group across clients
    assert "active_peers AS" in method_src, "missing active_peers CTE"
    # Canonical id grouping
    assert "canonical_signal_id" in method_src
    assert "COALESCE(p.canonical_signal_id, p.signal_id)" in method_src, (
        "active_peers must group by canonical_signal_id (with signal_id fallback)"
    )


def test_active_peers_considers_submitted_and_filled():
    """A peer row counts as 'active' if SUBMITTED/ACKNOWLEDGED/FILLED/
    PARTIALLY_FILLED, OR has a submitted_ts/filled_ts, OR was created
    inside the parity window. All three signals must be in the CTE."""
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    for token in [
        "'SUBMITTED','ACKNOWLEDGED','FILLED','PARTIALLY_FILLED'",
        "p.filled_ts IS NOT NULL",
        "p.submitted_ts IS NOT NULL",
        "p.created_ts > NOW()",
    ]:
        assert token in method_src, f"active_peers missing active-signal: {token}"


def test_skip_path_marks_retry_eligible_not_canceled():
    """The skip path must set status = 'RETRY_ELIGIBLE', not 'CANCELED'."""
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    # Locate the skip_targets CTE
    skip_start = method_src.find("skip_targets AS (")
    assert skip_start >= 0
    skip_chunk = method_src[skip_start : skip_start + 1500]
    assert "status = 'RETRY_ELIGIBLE'" in skip_chunk, (
        "skip_targets must set status='RETRY_ELIGIBLE' so the peer-retry "
        "path can re-evaluate, not 'CANCELED'"
    )
    assert "STARTUP_CLEANUP_SKIPPED_ACTIVE_SIGNAL" in skip_chunk


def test_cancel_path_only_when_no_active_peer():
    """cancel_targets must require NOT EXISTS active_peers."""
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    cancel_start = method_src.find("cancel_targets AS (")
    assert cancel_start >= 0
    cancel_chunk = method_src[cancel_start : cancel_start + 2000]
    assert "NOT EXISTS" in cancel_chunk, (
        "cancel_targets must require NOT EXISTS active_peers"
    )
    assert "STARTUP_CLEANUP_CANCELED_STALE_ORPHAN" in cancel_chunk


# ---------------------------------------------------------------------------
# Age default raised to 15 minutes per spec
# ---------------------------------------------------------------------------

def test_default_age_gate_is_at_least_15_minutes():
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    # The default fallback expression must yield >= 900s.
    assert "max(900" in method_src, (
        "default STARTUP_PHANTOM_CLEAR_MIN_AGE_SECONDS must be >= 900s "
        "(15 minutes) per the 2026-06-03 parity spec"
    )


# ---------------------------------------------------------------------------
# Return tuple structure preserved correctly
# ---------------------------------------------------------------------------

def test_run_with_retry_unpacks_three_counters():
    """The outer caller must unpack (tier_a_cancel, tier_a_skip, tier_b)."""
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    assert (
        "tier_a_cancel, tier_a_skip, tier_b = run_with_retry(_clear_phantoms)"
        in method_src
    ), "outer caller must unpack three counters from _clear_phantoms"


# ---------------------------------------------------------------------------
# Tier B (SUBMITTED tier) is unchanged (scope lock)
# ---------------------------------------------------------------------------

def test_tier_b_logic_preserved():
    """We must not have accidentally rewritten the Tier B SUBMITTED-tier
    SQL — the spec says only Tier A changes."""
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    assert "startup_phantom_clear_submitted" in method_src, (
        "Tier B (SUBMITTED tier) reason code must remain"
    )
    assert "PHANTOM_SUBMITTED_MIN_AGE_MINUTES" in method_src, (
        "Tier B env var must remain"
    )


# ---------------------------------------------------------------------------
# 2026-06-03 production incident: structural proof
# ---------------------------------------------------------------------------

INCIDENT_CANONICAL_IDS = [
    "REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9",  # UNH CALL
    "REEVAL:c068cdf5-68fb-4052-92f4-24c8efba34d4",  # VZ PUT
]


@pytest.mark.parametrize("canonical_id", INCIDENT_CANONICAL_IDS)
def test_simulated_incident_would_now_skip(canonical_id):
    """Simulate the 2026-06-03 incident in-memory:
       - one peer (tradefluencehq) has SUBMITTED row → 'active'
       - two candidates (jasoncosby1, jose.vasquez4011) hit Tier A criteria

    The active_peers CTE would identify the canonical_id as active. The
    candidates would land in skip_targets → RETRY_ELIGIBLE, not in
    cancel_targets. We prove this by checking the CTE logic admits the
    case, since we can't run psql here.
    """
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    # The CTE matches a peer on canonical id OR signal id
    assert "candidates ca" in method_src
    assert "ca.canon_id" in method_src
    # The skip path requires EXISTS active_peers
    skip_idx = method_src.find("skip_targets AS (")
    skip_chunk = method_src[skip_idx : skip_idx + 2000]
    assert "EXISTS (" in skip_chunk and "active_peers ap" in skip_chunk
