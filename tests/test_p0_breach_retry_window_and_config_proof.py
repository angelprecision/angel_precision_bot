"""
P0 (2026-07-02): breach retry window + ladder config provability.

Covers two production defects found in the 2026-06-30 → 2026-07-02 forensics:

1. BREACH_SELECTOR_RETRY_CUTOFF_ET defaulted to 945 (9:45 AM ET), but the
   queue is not released until ~9:45 ET, so no breach could ever fire before
   the cutoff — the retry system was structurally disabled for the whole
   session. Every retryable death showed breach_attempt_count=1. The default
   is now 1530 (the system's own last-entry boundary).

2. The DTE ladder ran disabled in production for multiple sessions with no
   durable record of the effective flag. Every deferred selector audit must
   now carry `dte_ladder_enabled` and `ladder_eligible_marker`.
"""

import pathlib
import re

import pytest

import ap_execution_core as ec

SRC = pathlib.Path(ec.__file__).with_suffix(".py").read_text()


# ── 1. Cutoff helper semantics ────────────────────────────────────────────────

def test_default_cutoff_is_last_entry_boundary(monkeypatch):
    monkeypatch.delenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", raising=False)
    assert ec._breach_retry_cutoff_hhmm() == 1530


def test_env_override_still_works_as_kill_switch(monkeypatch):
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "0")
    assert ec._breach_retry_cutoff_hhmm() == 0


def test_env_override_arbitrary_value(monkeypatch):
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "1015")
    assert ec._breach_retry_cutoff_hhmm() == 1015


def test_invalid_env_falls_back_to_default_never_raises(monkeypatch):
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "not-a-number")
    assert ec._breach_retry_cutoff_hhmm() == 1530
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "")
    assert ec._breach_retry_cutoff_hhmm() == 1530


@pytest.mark.parametrize(
    "now_hhmm,expected_past",
    [
        (946, False),   # 9:46 ET — the exact time production terminalized
        (1017, False),  # mid-morning transient miss must remain retryable
        (1116, False),  # late-morning transient miss must remain retryable
        (1529, False),  # one minute before the boundary
        (1530, True),   # at the boundary — no NEW retries into the close
        (1545, True),
    ],
)
def test_intraday_breaches_are_inside_the_retry_window(
    monkeypatch, now_hhmm, expected_past
):
    """The forensic times that died with attempt=1 must now be retryable."""
    monkeypatch.delenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", raising=False)
    assert (now_hhmm >= ec._breach_retry_cutoff_hhmm()) is expected_past


def test_computation_site_uses_the_helper():
    """The Path-A retry gate must read the helper, not a hardcoded 945."""
    assert "_RETRY_CUTOFF_A     = _breach_retry_cutoff_hhmm()" in SRC
    # No live (non-comment) 945 default may remain anywhere in the module.
    for line in SRC.splitlines():
        code = line.split("#", 1)[0]
        assert 'BREACH_SELECTOR_RETRY_CUTOFF_ET", "945"' not in code


def test_classifier_contract_unchanged_cutoff_still_terminalizes():
    """
    Regression fence for PR #252: when past_cutoff IS true (after 15:30 ET),
    the retry decision must still terminalize with the explicit cutoff reason.
    This PR changes WHEN past_cutoff becomes true — never what it does.
    """
    decision = ec._classify_deferred_breach_retry_decision(
        "CHAIN_ROW_ZERO_BID_ASK",
        queue_local_order_id="q-1",
        attempt=1,
        max_attempts=3,
        past_cutoff=True,
        retry_enabled=True,
        ladder_retryable=False,
    )
    assert decision["action"] == "retry_cutoff"
    assert decision["terminal_reason"] == "breach_retry_cutoff:CHAIN_ROW_ZERO_BID_ASK"


def test_classifier_contract_unchanged_intraday_now_schedules():
    """With past_cutoff False at intraday times, retryable reasons schedule."""
    decision = ec._classify_deferred_breach_retry_decision(
        "CHAIN_ROW_ZERO_BID_ASK",
        queue_local_order_id="q-1",
        attempt=1,
        max_attempts=3,
        past_cutoff=False,
        retry_enabled=True,
        ladder_retryable=False,
    )
    assert decision["action"] == "retry_schedule"


# ── 2. Ladder config provability in the deferred selector audit ─────────────

def test_audit_records_effective_ladder_flag():
    """
    Every deferred selector audit must persist the effective
    dte_ladder_enabled flag and the plan's eligibility marker so config
    state is provable from Supabase without reading Render env.
    """
    assert '_audit["dte_ladder_enabled"]' in SRC
    assert '_audit["ladder_eligible_marker"]' in SRC
    # Flag must be read from the live selector instance, not the env,
    # so an operator sees what the running process actually used.
    assert re.search(
        r'_audit\["dte_ladder_enabled"\]\s*=\s*bool\(\s*'
        r'getattr\(self\.contract_selector,\s*"dte_ladder_enabled"',
        SRC,
    )


def test_audit_marker_reads_plan_metadata_not_env():
    assert re.search(
        r'ladder_eligible_marker.*\n.*isinstance\(_plan_meta_for_audit,\s*dict\)',
        SRC,
    )
