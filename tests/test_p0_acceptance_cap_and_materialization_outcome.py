"""P0 (monday-trade-flow-readiness, amended): acceptance ask-cap fail-closed
contract, canonical tri-outcome stamps, and warmup-window retry proof.

Proves (amendment requirements 3, 5, 7):
  • _acceptance_ask_cap() fail-closed contract:
      - flag off → feature disabled, no cap, no error
      - flag on + valid cap → enforce
      - flag on + missing / unparsable / <= 0 cap → (enabled, None, error):
        callers MUST block — never proceed uncapped
  • Canonical tri-outcome mapping:
      - BREACH_BROKER_SUBMITTED → MATERIALIZED_AND_SUBMITTED
      - every other terminal → TERMINAL_NO_TRADEABLE_CONTRACT
  • Retry-schedule meta stamps RETRY_LATER_DATA_UNAVAILABLE and terminal meta
    stamps TERMINAL_NO_TRADEABLE_CONTRACT, both with
    entry_path=DEFERRED_BREACH_MATERIALIZATION and preserved client_id /
    execution_mode.
  • Warmup proof: zero-quote / empty-chain reasons near market open classify
    as retry_schedule (rearm), NOT immediate terminal — and terminalize
    cleanly only on exhaustion or cutoff.
"""
from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

import pytest

import ap_execution_core as core


# ─────────────────────────────────────────────────────────────────────────────
# _acceptance_ask_cap — fail-closed contract
# ─────────────────────────────────────────────────────────────────────────────

def test_cap_feature_off_when_flag_unset(monkeypatch):
    monkeypatch.delenv("DEFERRED_SMALL_ACCOUNT_FALLBACK", raising=False)
    monkeypatch.setenv("MAX_CONTRACT_ASK_FOR_JASON_ACCEPTANCE", "1.90")
    enabled, cap, err = core._acceptance_ask_cap()
    assert (enabled, cap, err) == (False, None, None)


def test_cap_valid(monkeypatch):
    monkeypatch.setenv("DEFERRED_SMALL_ACCOUNT_FALLBACK", "1")
    monkeypatch.setenv("MAX_CONTRACT_ASK_FOR_JASON_ACCEPTANCE", "1.90")
    enabled, cap, err = core._acceptance_ask_cap()
    assert enabled is True
    assert cap == pytest.approx(1.90)
    assert err is None


def test_cap_missing_fails_closed(monkeypatch):
    monkeypatch.setenv("DEFERRED_SMALL_ACCOUNT_FALLBACK", "1")
    monkeypatch.delenv("MAX_CONTRACT_ASK_FOR_JASON_ACCEPTANCE", raising=False)
    enabled, cap, err = core._acceptance_ask_cap()
    assert enabled is True
    assert cap is None
    assert err == "acceptance_cap_missing"


def test_cap_unparsable_fails_closed(monkeypatch):
    monkeypatch.setenv("DEFERRED_SMALL_ACCOUNT_FALLBACK", "1")
    monkeypatch.setenv("MAX_CONTRACT_ASK_FOR_JASON_ACCEPTANCE", "one-ninety")
    enabled, cap, err = core._acceptance_ask_cap()
    assert enabled is True
    assert cap is None
    assert err.startswith("acceptance_cap_unparsable")


@pytest.mark.parametrize("bad", ["0", "-1.5", "0.0"])
def test_cap_nonpositive_fails_closed(monkeypatch, bad):
    monkeypatch.setenv("DEFERRED_SMALL_ACCOUNT_FALLBACK", "1")
    monkeypatch.setenv("MAX_CONTRACT_ASK_FOR_JASON_ACCEPTANCE", bad)
    enabled, cap, err = core._acceptance_ask_cap()
    assert enabled is True
    assert cap is None
    assert err.startswith("acceptance_cap_nonpositive")


# ─────────────────────────────────────────────────────────────────────────────
# Canonical tri-outcome mapping
# ─────────────────────────────────────────────────────────────────────────────

def test_submitted_maps_to_materialized():
    assert (
        core._canonical_materialization_outcome("BREACH_BROKER_SUBMITTED")
        == "MATERIALIZED_AND_SUBMITTED"
    )


@pytest.mark.parametrize("terminal", [
    "BREACH_SELECTOR_RETURNED_NONE",
    "BREACH_SELECTOR_EXCEPTION",
    "BREACH_SUBMISSION_SKIPPED",
    "NO_VALID_PLAYBOOK_DTE_CONTRACT",
    "UNTRADEABLE_FOR_ACCOUNT_SIZE",
    "DATA_MISSING_OI_VOLUME",
    "BREACH_RISK_CHECK_BLOCKED",
])
def test_all_other_terminals_map_to_terminal_no_tradeable(terminal):
    assert (
        core._canonical_materialization_outcome(terminal)
        == "TERMINAL_NO_TRADEABLE_CONTRACT"
    )


def test_retry_schedule_meta_carries_canonical_stamp_and_identity():
    meta = core._build_deferred_retry_schedule_meta(
        reason_code="CHAIN_ROW_ZERO_BID_ASK",
        selector_audit={"stage": "quality_filter"},
        attempt=1,
        max_attempts=3,
        delay_seconds=20,
        client_id="jason@example.com",
        execution_mode="LIVE",
        local_order_id="LOID-1",
        signal_id="SIG-1",
    )
    assert meta["entry_path"] == "DEFERRED_BREACH_MATERIALIZATION"
    assert meta["materialization_outcome"] == "RETRY_LATER_DATA_UNAVAILABLE"
    assert meta["materialization_detail"] == "CHAIN_ROW_ZERO_BID_ASK"
    # client identity / mode must survive the retry meta round-trip
    assert meta["client_id"] == "jason@example.com"
    assert meta["execution_mode"] == "LIVE"
    assert meta["local_order_id"] == "LOID-1"
    assert meta["signal_id"] == "SIG-1"


def test_retry_terminal_meta_carries_canonical_stamp_and_identity():
    meta = core._build_deferred_retry_terminal_meta(
        terminal_reason="breach_retry_exhausted:CHAIN_ROW_ZERO_BID_ASK",
        reason_code="CHAIN_ROW_ZERO_BID_ASK",
        selector_audit={},
        attempt=4,
        max_attempts=3,
        client_id="jason@example.com",
        execution_mode="LIVE",
        local_order_id="LOID-1",
        signal_id="SIG-1",
    )
    assert meta["entry_path"] == "DEFERRED_BREACH_MATERIALIZATION"
    assert meta["materialization_outcome"] == "TERMINAL_NO_TRADEABLE_CONTRACT"
    assert meta["materialization_detail"] == "CHAIN_ROW_ZERO_BID_ASK"
    assert meta["client_id"] == "jason@example.com"
    assert meta["execution_mode"] == "LIVE"


# ─────────────────────────────────────────────────────────────────────────────
# Warmup-window retry proof (amendment req 5)
# Zero-quote / empty-chain reasons near open must REARM, not expire.
# ─────────────────────────────────────────────────────────────────────────────

_WARMUP_REASONS = [
    "CHAIN_ROW_ZERO_BID_ASK",
    "DIRECT_QUOTE_ZERO_BID_ASK",
    "NO_CHAIN_DATA",
    "CHAIN_PROVIDER_EMPTY_EXPIRATIONS",
    "CHAIN_PROVIDER_EMPTY_OPTIONS",
]


@pytest.mark.parametrize("reason", _WARMUP_REASONS)
def test_zero_quote_near_open_schedules_retry_not_expiry(reason):
    decision = core._classify_deferred_breach_retry_decision(
        reason,
        queue_local_order_id="LOID-1",
        attempt=1,
        max_attempts=3,
        past_cutoff=False,   # inside the entry window (e.g. 9:31 ET warmup)
        retry_enabled=True,
    )
    assert decision["action"] == "retry_schedule"
    assert decision["retryable_reason"] is True


@pytest.mark.parametrize("reason", _WARMUP_REASONS)
def test_zero_quote_exhaustion_terminalizes_cleanly(reason):
    decision = core._classify_deferred_breach_retry_decision(
        reason,
        queue_local_order_id="LOID-1",
        attempt=4,           # past max attempts
        max_attempts=3,
        past_cutoff=False,
        retry_enabled=True,
    )
    assert decision["action"] == "retry_exhausted"
    assert decision["terminal_reason"] == f"breach_retry_exhausted:{reason}"


def test_structural_quality_reject_is_not_warmup_retryable():
    # OI_TOO_LOW is a contract-level verdict — retrying won't change it.
    decision = core._classify_deferred_breach_retry_decision(
        "OI_TOO_LOW",
        queue_local_order_id="LOID-1",
        attempt=1,
        max_attempts=3,
        past_cutoff=False,
        retry_enabled=True,
    )
    assert decision["action"] == "terminal_quality"
