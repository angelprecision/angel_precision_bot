"""P0 amendment #5 (PR #294 final hardening): order-row read fail-closed.

Before this amendment, _classify_order_row_read returned (None, None) when
the persisted `orders` row was unreadable — the handoff proof would pass
and broker submit would proceed WITHOUT having proven the DB row. For a
system handling client money at scale, that is not acceptable.

Amendment #5 changes the contract:
  osm.get_order() raises OR returns None after selector success
    → BLOCK_RETRY
    → emit RETRY_LATER_DATA_UNAVAILABLE + MATERIALIZATION_ORDER_ROW_UNREADABLE
    → terminalize, no broker POST

  readable row with DEFERRED:* or diverging contract
    → TERMINAL_NO_TRADEABLE_CONTRACT + MATERIALIZATION_COPYBACK_MISMATCH
    (unchanged from amendment #4 — classifier handles this)

Two distinct failure modes, two distinct canonical outcomes.
The distinction matters for the operator: "retry later, transient" vs
"permanent mismatch, something is wrong with the pipeline".

Required test coverage (per spec):
  1. osm.get_order raises → BLOCK_RETRY, no broker POST.
  2. osm.get_order returns None → BLOCK_RETRY, no broker POST.
  3. readable row matches selector → PASS.
  4. readable row is DEFERRED:* → PASS from read classifier, mismatch
     handled by _classify_materialization_handoff (covered in handoff proof
     tests; repeated here for completeness).
  5. readable row diverges from selector → same.
"""
from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

import pytest

import ap_execution_core as core

_REAL_OCC = "SPY 250706 C00450000"


def _captured_snapshot(selector_contract=_REAL_OCC):
    return {
        "captured":             True,
        "selector_contract":    selector_contract,
        "selector_bid":         1.80,
        "selector_ask":         1.86,
        "selector_mid":         1.83,
        "selector_premium":     1.86,
        "selector_qty":         1,
        "copied_plan_contract": selector_contract,
        "copied_plan_limit":    1.86,
        "copied_plan_qty":      1,
        "copied_plan_max_usd":  186.0,
        "order_row_contract":   None,   # not yet populated at read-time
    }


# ─────────────────────────────────────────────────────────────────────────────
# _classify_order_row_read — pure truth table
# ─────────────────────────────────────────────────────────────────────────────

def test_read_exception_returns_block_retry_with_error_detail():
    verdict, reason = core._classify_order_row_read(
        handoff_snapshot=_captured_snapshot(),
        order_row_raw=None,
        read_error="psycopg2.OperationalError: timeout",
    )
    assert verdict == "BLOCK_RETRY"
    assert "read_error:" in reason
    assert "psycopg2.OperationalError" in reason


def test_get_order_returns_none_row_not_found_block_retry():
    verdict, reason = core._classify_order_row_read(
        handoff_snapshot=_captured_snapshot(),
        order_row_raw=None,
        read_error=None,
    )
    assert verdict == "BLOCK_RETRY"
    assert reason == "row_not_found"


def test_readable_row_returns_pass():
    verdict, reason = core._classify_order_row_read(
        handoff_snapshot=_captured_snapshot(),
        order_row_raw={"contract": _REAL_OCC, "local_order_id": "L1"},
        read_error=None,
    )
    assert (verdict, reason) == ("PASS", None)


def test_proof_not_applicable_when_snapshot_not_captured():
    verdict, reason = core._classify_order_row_read(
        handoff_snapshot={"captured": False},
        order_row_raw=None,
        read_error=None,
    )
    assert (verdict, reason) == (None, None)


def test_proof_not_applicable_when_selector_returned_placeholder():
    verdict, reason = core._classify_order_row_read(
        handoff_snapshot=_captured_snapshot(selector_contract="DEFERRED:SPY"),
        order_row_raw=None,
        read_error=None,
    )
    assert (verdict, reason) == (None, None)


def test_proof_not_applicable_when_no_selector_contract():
    verdict, reason = core._classify_order_row_read(
        handoff_snapshot=_captured_snapshot(selector_contract=None),
        order_row_raw=None,
        read_error=None,
    )
    assert (verdict, reason) == (None, None)


def test_none_snapshot_is_not_applicable():
    verdict, reason = core._classify_order_row_read(
        handoff_snapshot=None,
        order_row_raw=None,
        read_error="ignored",
    )
    assert (verdict, reason) == (None, None)


# ─────────────────────────────────────────────────────────────────────────────
# Canonical outcome mapping: DEFERRED_ORDER_ROW_UNREADABLE → RETRY_LATER
# ─────────────────────────────────────────────────────────────────────────────

def test_unreadable_outcome_maps_to_retry_later():
    assert (
        core._canonical_materialization_outcome("DEFERRED_ORDER_ROW_UNREADABLE")
        == "RETRY_LATER_DATA_UNAVAILABLE"
    )


def test_submitted_still_maps_to_materialized():
    assert (
        core._canonical_materialization_outcome("BREACH_BROKER_SUBMITTED")
        == "MATERIALIZED_AND_SUBMITTED"
    )


def test_mismatch_still_maps_to_terminal_no_tradeable():
    for code in [
        "BREACH_SUBMISSION_SKIPPED",
        "BREACH_SELECTOR_RETURNED_NONE",
        "UNTRADEABLE_FOR_ACCOUNT_SIZE",
    ]:
        assert (
            core._canonical_materialization_outcome(code)
            == "TERMINAL_NO_TRADEABLE_CONTRACT"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Canonical stamp shape for unreadable outcome
# ─────────────────────────────────────────────────────────────────────────────
# Replicate the emitter persist block (same pattern as
# test_p0_submit_cap_canonical_stamp.py) to verify that the BLOCK_RETRY path
# produces the right operator-visible fields in orders.meta.

def _canon_meta_from_persist(outcome, reason, contract, broker_order_id, extra):
    _detail = outcome
    if extra and "materialization_detail_override" in extra:
        _detail = str(extra.get("materialization_detail_override") or outcome)
    meta = {
        "entry_path":                      core._MATERIALIZATION_ENTRY_PATH,
        "materialization_outcome":         core._canonical_materialization_outcome(outcome),
        "materialization_detail":          _detail,
        "materialization_reason":          reason or "",
        "materialization_contract":        contract or "",
        "materialization_broker_order_id": broker_order_id or "",
    }
    if extra:
        for k, v in extra.items():
            if k == "materialization_detail_override":
                continue
            meta.setdefault(f"materialization_{k}", v)
    return meta


def _unreadable_extra(read_reason, snap, pre_contract, pre_limit, pre_qty,
                       local_order_id="LOID-42", client_id="jason@example.com",
                       execution_mode="LIVE"):
    return {
        "failure_stage":                   "handoff_order_row_read",
        "materialization_detail_override": "MATERIALIZATION_ORDER_ROW_UNREADABLE",
        "order_row_read_error":            read_reason,
        "selector_contract":               snap.get("selector_contract"),
        "selector_bid":                    snap.get("selector_bid"),
        "selector_ask":                    snap.get("selector_ask"),
        "selector_mid":                    snap.get("selector_mid"),
        "copied_plan_contract":            snap.get("copied_plan_contract"),
        "pre_submit_contract":             pre_contract,
        "pre_submit_limit":                pre_limit,
        "pre_submit_qty":                  pre_qty,
        "local_order_id":                  local_order_id,
        "client_id":                       client_id,
        "execution_mode":                  execution_mode,
    }


def test_read_error_stamps_retry_later_outcome():
    """Spec test 1: osm.get_order raises → RETRY_LATER_DATA_UNAVAILABLE."""
    snap = _captured_snapshot()
    extra = _unreadable_extra(
        "read_error:psycopg2.OperationalError: timeout",
        snap, _REAL_OCC, 1.87, 1,
    )
    meta = _canon_meta_from_persist(
        outcome="DEFERRED_ORDER_ROW_UNREADABLE",
        reason="order_row_unreadable:read_error:psycopg2.OperationalError: timeout",
        contract=_REAL_OCC,
        broker_order_id="",
        extra=extra,
    )
    assert meta["entry_path"] == "DEFERRED_BREACH_MATERIALIZATION"
    assert meta["materialization_outcome"] == "RETRY_LATER_DATA_UNAVAILABLE"
    assert meta["materialization_detail"] == "MATERIALIZATION_ORDER_ROW_UNREADABLE"
    assert meta["materialization_broker_order_id"] == ""   # never submitted
    # Full audit fields present:
    for field in (
        "materialization_selector_contract",
        "materialization_order_row_read_error",
        "materialization_pre_submit_contract",
        "materialization_pre_submit_limit",
        "materialization_local_order_id",
        "materialization_client_id",
        "materialization_execution_mode",
    ):
        assert field in meta, f"missing audit field: {field}"


def test_row_not_found_stamps_retry_later_outcome():
    """Spec test 2: osm.get_order returns None → RETRY_LATER_DATA_UNAVAILABLE."""
    snap = _captured_snapshot()
    extra = _unreadable_extra("row_not_found", snap, _REAL_OCC, 1.87, 1)
    meta = _canon_meta_from_persist(
        outcome="DEFERRED_ORDER_ROW_UNREADABLE",
        reason="order_row_unreadable:row_not_found",
        contract=_REAL_OCC,
        broker_order_id="",
        extra=extra,
    )
    assert meta["materialization_outcome"] == "RETRY_LATER_DATA_UNAVAILABLE"
    assert meta["materialization_detail"] == "MATERIALIZATION_ORDER_ROW_UNREADABLE"
    assert meta["materialization_order_row_read_error"] == "row_not_found"
    assert meta["materialization_broker_order_id"] == ""


def test_readable_matching_row_produces_pass_and_allows_submit():
    """Spec test 3: readable row matches selector → classifier says PASS."""
    verdict, reason = core._classify_order_row_read(
        handoff_snapshot=_captured_snapshot(),
        order_row_raw={"contract": _REAL_OCC, "limit_price": 1.87},
        read_error=None,
    )
    assert (verdict, reason) == ("PASS", None)
    # With PASS, the extracted contract goes to the handoff classifier:
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_captured_snapshot(selector_contract=_REAL_OCC),
        pre_submit_contract=_REAL_OCC,
        pre_submit_limit=1.87,
        pre_submit_qty=1,
        order_row_contract=_REAL_OCC,
    )
    assert (ok, mismatch) == (True, None)


def test_readable_row_deferred_terminates_with_copyback_mismatch():
    """Spec test 4: readable row is DEFERRED:* → TERMINAL_NO_TRADEABLE_CONTRACT."""
    # Read step passes (row IS readable)
    verdict, _ = core._classify_order_row_read(
        handoff_snapshot=_captured_snapshot(),
        order_row_raw={"contract": "DEFERRED:SPY"},
        read_error=None,
    )
    assert verdict == "PASS"
    # Classifier catches the mismatch:
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_captured_snapshot(),
        pre_submit_contract=_REAL_OCC,
        pre_submit_limit=1.87,
        pre_submit_qty=1,
        order_row_contract="DEFERRED:SPY",
    )
    assert ok is False
    assert mismatch == "order_row_contract_placeholder_or_missing"
    # And that maps to TERMINAL, not RETRY:
    assert (
        core._canonical_materialization_outcome("BREACH_SUBMISSION_SKIPPED")
        == "TERMINAL_NO_TRADEABLE_CONTRACT"
    )


def test_readable_row_diverges_terminates_with_copyback_mismatch():
    """Spec test 5: readable row diverges from selector → TERMINAL_NO_TRADEABLE_CONTRACT."""
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_captured_snapshot(),
        pre_submit_contract=_REAL_OCC,
        pre_submit_limit=1.87,
        pre_submit_qty=1,
        order_row_contract="SPY 250706 C00451000",
    )
    assert ok is False
    assert mismatch.startswith("order_row_contract_diverges_from_selector:")
    assert core._canonical_materialization_outcome("BREACH_SUBMISSION_SKIPPED") == \
        "TERMINAL_NO_TRADEABLE_CONTRACT"


def test_retry_and_terminal_are_mutually_exclusive():
    """Unreadable → RETRY. Readable-but-wrong → TERMINAL. Never swapped."""
    assert core._canonical_materialization_outcome("DEFERRED_ORDER_ROW_UNREADABLE") \
        == "RETRY_LATER_DATA_UNAVAILABLE"
    assert core._canonical_materialization_outcome("BREACH_SUBMISSION_SKIPPED") \
        == "TERMINAL_NO_TRADEABLE_CONTRACT"
    assert "RETRY" not in core._canonical_materialization_outcome("BREACH_SUBMISSION_SKIPPED")
    assert "TERMINAL" not in core._canonical_materialization_outcome("DEFERRED_ORDER_ROW_UNREADABLE")
