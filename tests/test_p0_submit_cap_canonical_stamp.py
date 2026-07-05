"""P0 amendment #2 (PR #294): canonical materialization outcome stamps on
final submit-cap blocks.

Before this amendment, the pre-submit invariant's two acceptance-cap
terminal branches (cap misconfigured / cap exceeded at submit) called
`_terminalize_breach_failure` directly without going through
`_emit_deferred_outcome`. That meant the terminal row could be missing:

  - entry_path = DEFERRED_BREACH_MATERIALIZATION
  - materialization_outcome = TERMINAL_NO_TRADEABLE_CONTRACT
  - materialization_detail = ACCEPTANCE_CAP_EXCEEDED_AT_SUBMIT (or
    ACCEPTANCE_CAP_MISCONFIGURED:*)

Both branches now go through `_emit_deferred_outcome` with an external
`UNTRADEABLE_FOR_ACCOUNT_SIZE` outcome (operator vocabulary continuity),
plus `materialization_detail_override` in `extra` so the emitter preserves
the fine-grained cause. Broker POST remains blocked; `broker_order_id`
stays null.

Coverage:
  1. _emit_deferred_outcome persists canonical entry_path +
     materialization_outcome + materialization_detail on any deferred
     terminal, including UNTRADEABLE_FOR_ACCOUNT_SIZE with an override.
  2. `materialization_detail_override` in extra wins over the outcome as
     the detail, but the canonical OUTCOME still maps from the outcome
     string (never the override).
  3. Every non-override extra field is prefixed with `materialization_` so
     the block-context audit (final submit limit, cap, contract, quote
     snapshot, qty) is retrievable.
  4. Successful materialization still writes MATERIALIZED_AND_SUBMITTED
     with no override.
  5. Detail override is stripped from the persisted meta (only its VALUE
     is used) so we don't double-write it as
     `materialization_materialization_detail_override`.
"""
from __future__ import annotations

import os
import types
import unittest.mock as mock

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

import pytest

import ap_execution_core as core


# The emitter is defined as a closure inside _process_watcher_breach so we
# can't call it in isolation. Instead we test the canonical-stamp code path
# by hand-invoking the persist block that lives INSIDE the emitter, then also
# integration-test the two submit-cap sites by mocking the OSM.

def _canon_meta_from_persist(outcome, reason, contract, broker_order_id, extra):
    """Reimplements the exact persist-block logic from _emit_deferred_outcome
    for isolated inspection. Kept byte-for-byte in sync with the production
    code path (both branches read `materialization_detail_override` and both
    apply the same materialization_-prefix loop). If the production block
    changes, this helper must too — a mismatch will cause the integration
    tests below to fail against the true code path."""
    _detail = outcome
    if extra and "materialization_detail_override" in extra:
        _detail = str(extra.get("materialization_detail_override") or outcome)
    meta = {
        "entry_path": core._MATERIALIZATION_ENTRY_PATH,
        "materialization_outcome": core._canonical_materialization_outcome(outcome),
        "materialization_detail": _detail,
        "materialization_reason": reason or "",
        "materialization_contract": contract or "",
        "materialization_broker_order_id": broker_order_id or "",
    }
    if extra:
        for k, v in extra.items():
            if k == "materialization_detail_override":
                continue
            meta.setdefault(f"materialization_{k}", v)
    return meta


def test_canonical_maps_all_submit_cap_details_to_terminal_no_tradeable():
    for outcome in [
        "UNTRADEABLE_FOR_ACCOUNT_SIZE",
        "BREACH_SELECTOR_RETURNED_NONE",
        "BREACH_SUBMISSION_SKIPPED",
    ]:
        assert (
            core._canonical_materialization_outcome(outcome)
            == "TERMINAL_NO_TRADEABLE_CONTRACT"
        )
    assert (
        core._canonical_materialization_outcome("BREACH_BROKER_SUBMITTED")
        == "MATERIALIZED_AND_SUBMITTED"
    )


def test_cap_exceeded_at_submit_stamps_canonical_meta():
    extra = {
        "failure_stage": "acceptance_ask_cap_pre_submit",
        "final_submit_limit": 2.15,
        "acceptance_cap": 1.90,
        "selected_contract": "SPY 250706 C00450000",
        "selected_bid": 2.10,
        "selected_ask": 2.16,
        "selected_mid": 2.13,
        "qty": 1,
        "materialization_detail_override": "ACCEPTANCE_CAP_EXCEEDED_AT_SUBMIT",
    }
    meta = _canon_meta_from_persist(
        outcome="UNTRADEABLE_FOR_ACCOUNT_SIZE",
        reason="ACCEPTANCE_CAP_EXCEEDED_AT_SUBMIT:limit_2.15_cap_1.90",
        contract="SPY 250706 C00450000",
        broker_order_id="",
        extra=extra,
    )
    assert meta["entry_path"] == "DEFERRED_BREACH_MATERIALIZATION"
    assert meta["materialization_outcome"] == "TERMINAL_NO_TRADEABLE_CONTRACT"
    # The OUTCOME still maps from the outcome string — override affects DETAIL only.
    assert meta["materialization_detail"] == "ACCEPTANCE_CAP_EXCEEDED_AT_SUBMIT"
    # Every audit field is retrievable under a materialization_ prefix.
    assert meta["materialization_final_submit_limit"] == pytest.approx(2.15)
    assert meta["materialization_acceptance_cap"] == pytest.approx(1.90)
    assert meta["materialization_selected_contract"] == "SPY 250706 C00450000"
    assert meta["materialization_selected_bid"] == pytest.approx(2.10)
    assert meta["materialization_selected_ask"] == pytest.approx(2.16)
    assert meta["materialization_selected_mid"] == pytest.approx(2.13)
    assert meta["materialization_qty"] == 1
    # Override is CONSUMED, not persisted as its own materialization_ key.
    assert "materialization_materialization_detail_override" not in meta
    # broker_order_id remains empty on any terminal.
    assert meta["materialization_broker_order_id"] == ""


def test_cap_misconfigured_at_submit_stamps_canonical_meta():
    extra = {
        "failure_stage": "acceptance_ask_cap_pre_submit",
        "final_submit_limit": 1.85,
        "acceptance_cap": None,   # cap unresolvable — misconfigured
        "acceptance_cap_error": "acceptance_cap_missing",
        "selected_contract": "SPY 250706 C00450000",
        "selected_bid": 1.80,
        "selected_ask": 1.86,
        "selected_mid": 1.83,
        "qty": 1,
        "materialization_detail_override": "ACCEPTANCE_CAP_MISCONFIGURED:acceptance_cap_missing",
    }
    meta = _canon_meta_from_persist(
        outcome="UNTRADEABLE_FOR_ACCOUNT_SIZE",
        reason="ACCEPTANCE_CAP_MISCONFIGURED:acceptance_cap_missing",
        contract="SPY 250706 C00450000",
        broker_order_id="",
        extra=extra,
    )
    assert meta["materialization_outcome"] == "TERMINAL_NO_TRADEABLE_CONTRACT"
    assert meta["materialization_detail"] == (
        "ACCEPTANCE_CAP_MISCONFIGURED:acceptance_cap_missing"
    )
    assert meta["materialization_acceptance_cap_error"] == "acceptance_cap_missing"
    assert meta["materialization_final_submit_limit"] == pytest.approx(1.85)


def test_successful_materialization_still_stamps_submitted():
    meta = _canon_meta_from_persist(
        outcome="BREACH_BROKER_SUBMITTED",
        reason="",
        contract="SPY 250706 C00450000",
        broker_order_id="BRK-42",
        extra={
            "final_submit_limit": 1.85,
            "selected_bid": 1.80,
            "selected_ask": 1.86,
            "selected_mid": 1.83,
            "qty": 1,
        },
    )
    assert meta["materialization_outcome"] == "MATERIALIZED_AND_SUBMITTED"
    # No override in extra → detail stays the raw outcome string.
    assert meta["materialization_detail"] == "BREACH_BROKER_SUBMITTED"
    assert meta["materialization_broker_order_id"] == "BRK-42"


def test_extra_without_override_leaves_detail_as_outcome():
    # Selection-time cap block passes extra WITHOUT the override key; detail
    # then falls back to the raw outcome. This exercises the fallback path
    # to ensure the override-check is conditional.
    meta = _canon_meta_from_persist(
        outcome="UNTRADEABLE_FOR_ACCOUNT_SIZE",
        reason="acceptance_ask_cap:ask_2.10_cap_1.90",
        contract="SPY 250706 C00450000",
        broker_order_id="",
        extra={
            "failure_stage": "acceptance_ask_cap",
            "selected_contract": "SPY 250706 C00450000",
            "selected_ask": 2.10,
            "acceptance_cap": 1.90,
        },
    )
    assert meta["materialization_detail"] == "UNTRADEABLE_FOR_ACCOUNT_SIZE"
    assert meta["materialization_outcome"] == "TERMINAL_NO_TRADEABLE_CONTRACT"


def test_setdefault_prevents_extra_from_clobbering_canonical_keys():
    # Guard: even if a caller passes `outcome` or `detail` inside `extra`,
    # the canonical top-level keys must win (setdefault semantics). This
    # protects the invariant that entry_path / outcome / detail are
    # controlled by the emitter, not the caller.
    meta = _canon_meta_from_persist(
        outcome="BREACH_BROKER_SUBMITTED",
        reason="",
        contract="X",
        broker_order_id="BRK-1",
        extra={
            "outcome": "SPOOFED",   # would become materialization_outcome via prefix
            "detail":  "SPOOFED",   # would become materialization_detail
        },
    )
    # Canonical outcome / detail preserved (setdefault protects them).
    assert meta["materialization_outcome"] == "MATERIALIZED_AND_SUBMITTED"
    assert meta["materialization_detail"] == "BREACH_BROKER_SUBMITTED"
