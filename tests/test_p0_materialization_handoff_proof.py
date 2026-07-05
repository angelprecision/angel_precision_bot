"""P0 amendment #3 (PR #294): deferred materialization handoff integrity proof.

Proves the "trigger_ready → selector executes → selector result copies back →
execution core uses copied values at pre-submit" invariant. Even when the
selector succeeds with a real OCC contract, a bug anywhere between copy-back
and the pre-submit invariant could still cause the pre-submit view to read a
stale placeholder or a $0.01 limit — production would look identical to the
old failure even though the selector actually worked.

The classifier `_classify_materialization_handoff` (pure module-level helper)
is exercised directly through its full truth table. The canonical stamp for
handoff mismatches is exercised through the emitter persist block (same
pattern as the submit-cap canonical stamp tests).

Coverage matches the seven required test scenarios:
  1. Selector real + copy-back real + pre-submit real → handoff_ok=True.
  2. Copied plan still has DEFERRED:<SYMBOL> at pre-submit → False,
     mismatch=pre_submit_contract_placeholder_or_missing.
  3. Copied plan real but pre-submit contract diverges → False,
     mismatch=pre_submit_contract_diverges_from_selector.
  4. Selector real but pre-submit limit stuck at 0.01 → False,
     mismatch=pre_submit_limit_not_materialized.
  5. Selector real but pre-submit qty is 0 → False,
     mismatch=pre_submit_qty_not_materialized.
  6. Successful materialization writes MATERIALIZED_AND_SUBMITTED with the
     handoff proof fields present on orders.meta.
  7. When the snapshot was NOT captured (selector never ran on this
     trigger), the proof returns (None, None) so the standing
     DEFERRED_CONTRACT_NOT_MATERIALIZED / DEFERRED_LIMIT_NOT_MATERIALIZED
     invariants still fire — proof AUGMENTS, never REPLACES, them.

Additional integrity coverage:
  - Selector returned a DEFERRED:* placeholder itself → proof (None, None)
    (not applicable; existing invariants handle it).
  - Empty selector_contract → proof (None, None).
  - Corrupt snapshot (missing keys, wrong types) does not raise.
"""
from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

import pytest

import ap_execution_core as core


# ─────────────────────────────────────────────────────────────────────────────
# Pure classifier truth table
# ─────────────────────────────────────────────────────────────────────────────

_REAL_OCC = "SPY 250706 C00450000"


def _snapshot(**overrides):
    """A captured snapshot representing a successful selector run.

    Sensible defaults: real OCC contract, plausible quotes, qty=1, copy-back
    propagated back into the plan, AND the persisted `orders` row's
    contract matches (`order_row_contract`). Individual tests override only
    what they're stressing so the failure mode is unambiguous.
    """
    snap = {
        "captured":             True,
        "selector_contract":    _REAL_OCC,
        "selector_bid":         1.80,
        "selector_ask":         1.86,
        "selector_mid":         1.83,
        "selector_premium":     1.86,
        "selector_qty":         1,
        "copied_plan_contract": _REAL_OCC,
        "copied_plan_limit":    1.86,
        "copied_plan_qty":      1,
        "copied_plan_max_usd":  186.0,
        # P0 amendment #4 (PR #294): third view — the persisted `orders` row.
        "order_row_contract":   _REAL_OCC,
    }
    snap.update(overrides)
    return snap


def test_case_1_clean_handoff_returns_ok():
    # Amendment #4: three-view alignment — selector, plan, order-row all match.
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_snapshot(),
        pre_submit_contract=_REAL_OCC,
        pre_submit_limit=1.87,
        pre_submit_qty=1,
        order_row_contract=_REAL_OCC,
    )
    assert (ok, mismatch) == (True, None)


def test_case_2_pre_submit_still_deferred_placeholder():
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_snapshot(),
        pre_submit_contract="DEFERRED:SPY",
        pre_submit_limit=1.87,
        pre_submit_qty=1,
    )
    assert ok is False
    assert mismatch == "pre_submit_contract_placeholder_or_missing"


def test_case_2b_pre_submit_contract_missing():
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_snapshot(),
        pre_submit_contract="",
        pre_submit_limit=1.87,
        pre_submit_qty=1,
    )
    assert ok is False
    assert mismatch == "pre_submit_contract_placeholder_or_missing"


def test_case_3_pre_submit_contract_diverges_from_selector():
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_snapshot(),
        pre_submit_contract="SPY 250706 C00451000",   # +$1 strike drift
        pre_submit_limit=1.87,
        pre_submit_qty=1,
    )
    assert ok is False
    assert mismatch.startswith("pre_submit_contract_diverges_from_selector:")
    assert f"selector={_REAL_OCC}" in mismatch
    assert "pre_submit=SPY 250706 C00451000" in mismatch


def test_case_4_pre_submit_limit_stuck_at_penny():
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_snapshot(),
        pre_submit_contract=_REAL_OCC,
        pre_submit_limit=0.01,
        pre_submit_qty=1,
    )
    assert ok is False
    assert mismatch.startswith("pre_submit_limit_not_materialized:limit=")


def test_case_4b_pre_submit_limit_zero():
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_snapshot(),
        pre_submit_contract=_REAL_OCC,
        pre_submit_limit=0.0,
        pre_submit_qty=1,
    )
    assert ok is False
    assert mismatch.startswith("pre_submit_limit_not_materialized")


def test_case_5_pre_submit_qty_zero():
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_snapshot(),
        pre_submit_contract=_REAL_OCC,
        pre_submit_limit=1.87,
        pre_submit_qty=0,
    )
    assert ok is False
    assert mismatch.startswith("pre_submit_qty_not_materialized:qty=0")


def test_case_5b_pre_submit_qty_negative_treated_as_missing():
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_snapshot(),
        pre_submit_contract=_REAL_OCC,
        pre_submit_limit=1.87,
        pre_submit_qty=-1,
    )
    assert ok is False
    assert mismatch.startswith("pre_submit_qty_not_materialized")


def test_case_7_snapshot_not_captured_returns_not_applicable():
    """When the deferred-selection branch never ran to success, the snapshot
    is never populated. The proof must return (None, None) so downstream
    standing invariants (DEFERRED_CONTRACT_NOT_MATERIALIZED / LIMIT) still
    fire — proof AUGMENTS, never REPLACES them."""
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot={"captured": False},
        pre_submit_contract="DEFERRED:SPY",
        pre_submit_limit=0.01,
        pre_submit_qty=0,
    )
    assert (ok, mismatch) == (None, None)


def test_selector_placeholder_result_is_not_applicable():
    """If the selector itself returned a DEFERRED:* placeholder (which the
    copy-back path would have detected upstream), the proof abstains and
    lets DEFERRED_CONTRACT_NOT_MATERIALIZED handle it."""
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_snapshot(selector_contract="DEFERRED:SPY"),
        pre_submit_contract="DEFERRED:SPY",
        pre_submit_limit=0.01,
        pre_submit_qty=0,
    )
    assert (ok, mismatch) == (None, None)


def test_empty_selector_contract_is_not_applicable():
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_snapshot(selector_contract=None),
        pre_submit_contract=_REAL_OCC,
        pre_submit_limit=1.87,
        pre_submit_qty=1,
    )
    assert (ok, mismatch) == (None, None)


def test_corrupt_snapshot_shape_does_not_raise():
    # Missing keys — classifier must tolerate defensively.
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot={"captured": True, "selector_contract": _REAL_OCC},
        pre_submit_contract=_REAL_OCC,
        pre_submit_limit=1.87,
        pre_submit_qty=1,
    )
    assert (ok, mismatch) == (True, None)


def test_none_snapshot_returns_not_applicable():
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=None,      # type: ignore[arg-type]
        pre_submit_contract=_REAL_OCC,
        pre_submit_limit=1.87,
        pre_submit_qty=1,
    )
    assert (ok, mismatch) == (None, None)


# ─────────────────────────────────────────────────────────────────────────────
# Amendment #4: order_row_contract third-view checks
# ─────────────────────────────────────────────────────────────────────────────
# These cover the specific failure mode: selector succeeded AND the plan got
# the real contract, but the persisted `orders` row was never updated. In
# production that would look like "everything worked" until the OSM submit
# path read a stale DB contract. The proof must catch it before broker POST.

def test_case_3_order_row_still_deferred_while_plan_and_selector_real():
    """Requirement scenario #3: selector + plan agree on real contract, but
    order row still has DEFERRED — MUST block."""
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_snapshot(),
        pre_submit_contract=_REAL_OCC,
        pre_submit_limit=1.87,
        pre_submit_qty=1,
        order_row_contract="DEFERRED:SPY",
    )
    assert ok is False
    assert mismatch == "order_row_contract_placeholder_or_missing"


def test_case_3b_order_row_contract_empty():
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_snapshot(),
        pre_submit_contract=_REAL_OCC,
        pre_submit_limit=1.87,
        pre_submit_qty=1,
        order_row_contract="",
    )
    assert ok is False
    assert mismatch == "order_row_contract_placeholder_or_missing"


def test_order_row_contract_diverges_from_selector():
    """Even when plan and pre-submit agree with selector, if the persisted
    row disagrees (say, an earlier stale update leaked through), block."""
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_snapshot(),
        pre_submit_contract=_REAL_OCC,
        pre_submit_limit=1.87,
        pre_submit_qty=1,
        order_row_contract="SPY 250706 C00451000",   # +$1 strike drift
    )
    assert ok is False
    assert mismatch.startswith("order_row_contract_diverges_from_selector:")
    assert f"selector={_REAL_OCC}" in mismatch
    assert "order_row=SPY 250706 C00451000" in mismatch


def test_order_row_contract_none_degrades_gracefully_does_not_block():
    """Amendment #4 fail-graceful: a failed DB read (None) must not weaken
    the proof for OTHER dimensions but must not block on the order-row
    dimension alone. Selector + plan + pre-submit all agree; order-row
    unreadable → still OK. Standing DEFERRED_CONTRACT invariant runs after
    and catches genuine placeholder leakage."""
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_snapshot(order_row_contract=None),
        pre_submit_contract=_REAL_OCC,
        pre_submit_limit=1.87,
        pre_submit_qty=1,
        order_row_contract=None,
    )
    assert (ok, mismatch) == (True, None)


def test_order_row_check_does_not_precede_pre_submit_checks():
    """When both pre-submit and order-row are broken, the pre-submit
    mismatch reason wins — pre-submit is closer to the actual submit."""
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot=_snapshot(),
        pre_submit_contract="DEFERRED:SPY",
        pre_submit_limit=1.87,
        pre_submit_qty=1,
        order_row_contract="DEFERRED:SPY",
    )
    assert ok is False
    # pre_submit check fires first — deterministic ordering.
    assert mismatch == "pre_submit_contract_placeholder_or_missing"



# The emitter is a closure inside _process_watcher_breach so we replicate its
# persist logic byte-for-byte (same pattern as test_p0_submit_cap_canonical_stamp.py)
# to verify the mismatch path produces the canonical fields the amendment
# requires. If the emitter code changes, the sister canonical-stamp file also
# changes; this replica keeps the two in lockstep.

def _canon_meta_from_persist(outcome, reason, contract, broker_order_id, extra):
    _detail = outcome
    if extra and "materialization_detail_override" in extra:
        _detail = str(extra.get("materialization_detail_override") or outcome)
    meta = {
        "entry_path":                         core._MATERIALIZATION_ENTRY_PATH,
        "materialization_outcome":            core._canonical_materialization_outcome(outcome),
        "materialization_detail":             _detail,
        "materialization_reason":             reason or "",
        "materialization_contract":           contract or "",
        "materialization_broker_order_id":    broker_order_id or "",
    }
    if extra:
        for k, v in extra.items():
            if k == "materialization_detail_override":
                continue
            meta.setdefault(f"materialization_{k}", v)
    return meta


def _mismatch_extra(mismatch_reason, snapshot, pre_contract, pre_limit, pre_qty,
                     local_order_id="LOID-42", client_id="jason@example.com",
                     execution_mode="LIVE"):
    """Rebuilds the `extra` dict the production block passes to
    `_emit_deferred_outcome` on a mismatch. Kept in sync with the emitter
    call site — including the amendment #4 order_row_contract field."""
    return {
        "failure_stage":                   "materialization_handoff_proof",
        "materialization_detail_override": "MATERIALIZATION_COPYBACK_MISMATCH",
        "mismatch_reason":                 mismatch_reason,
        "selector_contract":               snapshot.get("selector_contract"),
        "selector_bid":                    snapshot.get("selector_bid"),
        "selector_ask":                    snapshot.get("selector_ask"),
        "selector_mid":                    snapshot.get("selector_mid"),
        "selector_premium":                snapshot.get("selector_premium"),
        "selector_qty":                    snapshot.get("selector_qty"),
        "copied_plan_contract":            snapshot.get("copied_plan_contract"),
        "copied_plan_limit":               snapshot.get("copied_plan_limit"),
        "copied_plan_qty":                 snapshot.get("copied_plan_qty"),
        "copied_plan_max_usd":             snapshot.get("copied_plan_max_usd"),
        "order_row_contract":              snapshot.get("order_row_contract"),
        "pre_submit_contract":             pre_contract,
        "pre_submit_limit":                pre_limit,
        "pre_submit_qty":                  pre_qty,
        "local_order_id":                  local_order_id,
        "client_id":                       client_id,
        "execution_mode":                  execution_mode,
    }


def test_mismatch_stamps_canonical_terminal_no_tradeable():
    snap = _snapshot()
    extra = _mismatch_extra(
        "pre_submit_contract_placeholder_or_missing",
        snap, pre_contract="DEFERRED:SPY", pre_limit=1.87, pre_qty=1,
    )
    meta = _canon_meta_from_persist(
        outcome="BREACH_SUBMISSION_SKIPPED",
        reason="materialization_copyback_mismatch:pre_submit_contract_placeholder_or_missing",
        contract="DEFERRED:SPY",
        broker_order_id="",
        extra=extra,
    )
    assert meta["entry_path"] == "DEFERRED_BREACH_MATERIALIZATION"
    assert meta["materialization_outcome"] == "TERMINAL_NO_TRADEABLE_CONTRACT"
    assert meta["materialization_detail"] == "MATERIALIZATION_COPYBACK_MISMATCH"
    # Every required audit field present under materialization_ prefix:
    for expected in (
        "materialization_selector_contract",
        "materialization_copied_plan_contract",
        "materialization_order_row_contract",   # amendment #4
        "materialization_pre_submit_contract",
        "materialization_selector_bid",
        "materialization_selector_ask",
        "materialization_selector_mid",
        "materialization_pre_submit_limit",
        "materialization_pre_submit_qty",
        "materialization_local_order_id",
        "materialization_client_id",
        "materialization_execution_mode",
        "materialization_mismatch_reason",
    ):
        assert expected in meta, f"missing required audit field {expected}"
    assert meta["materialization_broker_order_id"] == ""    # never submitted


@pytest.mark.parametrize("mismatch,pre_c,pre_l,pre_q,order_c", [
    ("pre_submit_contract_placeholder_or_missing",         "DEFERRED:SPY",         1.87, 1, _REAL_OCC),
    ("pre_submit_contract_diverges_from_selector:...",     "SPY 250706 C00451000", 1.87, 1, _REAL_OCC),
    ("pre_submit_limit_not_materialized:limit=0.0100",     _REAL_OCC,              0.01, 1, _REAL_OCC),
    ("pre_submit_qty_not_materialized:qty=0",              _REAL_OCC,              1.87, 0, _REAL_OCC),
    # Amendment #4: order-row mismatches produce the same canonical stamp
    # so dashboards can filter every handoff break the same way.
    ("order_row_contract_placeholder_or_missing",          _REAL_OCC,              1.87, 1, "DEFERRED:SPY"),
    ("order_row_contract_diverges_from_selector:...",      _REAL_OCC,              1.87, 1, "SPY 250706 C00451000"),
])
def test_all_mismatch_reasons_stamp_same_canonical_detail(
    mismatch, pre_c, pre_l, pre_q, order_c,
):
    snap = _snapshot(order_row_contract=order_c)
    extra = _mismatch_extra(mismatch, snap, pre_c, pre_l, pre_q)
    meta = _canon_meta_from_persist(
        outcome="BREACH_SUBMISSION_SKIPPED",
        reason=f"materialization_copyback_mismatch:{mismatch}",
        contract=pre_c,
        broker_order_id="",
        extra=extra,
    )
    assert meta["materialization_outcome"] == "TERMINAL_NO_TRADEABLE_CONTRACT"
    assert meta["materialization_detail"] == "MATERIALIZATION_COPYBACK_MISMATCH"
    assert meta["materialization_mismatch_reason"] == mismatch
    assert meta["materialization_broker_order_id"] == ""
    # Amendment #4: order_row_contract must always appear on any mismatch
    # canonical stamp (its VALUE tells operators which view broke).
    assert "materialization_order_row_contract" in meta


def test_successful_materialization_stamps_submitted_with_handoff_fields():
    """Requirement 6: successful path carries the same three-stage snapshot
    on orders.meta so a dashboard can prove any deferred trigger — success
    or terminal — has full proof lineage."""
    snap = _snapshot()
    extra = {
        "final_submit_limit":    1.87,
        "selected_bid":          1.80,
        "selected_ask":          1.86,
        "selected_mid":          1.83,
        "qty":                   1,
        "handoff_ok":            True,
        "selector_contract":     snap["selector_contract"],
        "copied_plan_contract":  snap["copied_plan_contract"],
        "order_row_contract":    snap["order_row_contract"],   # amendment #4
        "pre_submit_contract":   _REAL_OCC,
        "pre_submit_limit":      1.87,
        "pre_submit_qty":        1,
        "local_order_id":        "LOID-42",
        "client_id":             "jason@example.com",
        "execution_mode":        "LIVE",
    }
    meta = _canon_meta_from_persist(
        outcome="BREACH_BROKER_SUBMITTED",
        reason="",
        contract=_REAL_OCC,
        broker_order_id="BRK-1",
        extra=extra,
    )
    assert meta["materialization_outcome"] == "MATERIALIZED_AND_SUBMITTED"
    # Successful terminal preserves handoff proof lineage for dashboards.
    assert meta["materialization_selector_contract"] == _REAL_OCC
    assert meta["materialization_pre_submit_contract"] == _REAL_OCC
    assert meta["materialization_order_row_contract"] == _REAL_OCC   # amendment #4
    assert meta["materialization_handoff_ok"] is True
    assert meta["materialization_local_order_id"] == "LOID-42"
    assert meta["materialization_client_id"] == "jason@example.com"
    assert meta["materialization_execution_mode"] == "LIVE"


def test_existing_deferred_invariants_still_fire_when_proof_abstains():
    """Requirement 7: proof abstaining (snapshot not captured) MUST NOT
    weaken existing DEFERRED_CONTRACT_NOT_MATERIALIZED / LIMIT invariants.

    This is a structural test: verify the classifier returns (None, None)
    for the exact conditions the standing invariants exist to catch, so
    control flow falls through to them.
    """
    # Selector never ran → standing DEFERRED contract invariant catches
    # the placeholder pre-submit contract:
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot={"captured": False},
        pre_submit_contract="DEFERRED:SPY",
        pre_submit_limit=1.87,
        pre_submit_qty=1,
    )
    assert (ok, mismatch) == (None, None)

    # Selector never ran → standing DEFERRED limit invariant catches the
    # $0.01 placeholder limit:
    ok, mismatch = core._classify_materialization_handoff(
        handoff_snapshot={"captured": False},
        pre_submit_contract=_REAL_OCC,
        pre_submit_limit=0.01,
        pre_submit_qty=1,
    )
    assert (ok, mismatch) == (None, None)
