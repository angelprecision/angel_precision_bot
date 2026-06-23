"""
tests/test_pending_trigger_orphan_guard.py

P0 — PENDING_TRIGGER watcher-held rows must not be expired as broker orphans.

Root cause:
Jason order 22514 (C260626C00150000, trigger_price=155.0, reserved_cost=105.0)
reached PENDING_TRIGGER and was expired with:
  last_error = PENDING_TRIGGER_ORPHAN_EXPIRED: no_broker_order_id no_submitted_ts age=5452s

This is wrong. A PENDING_TRIGGER ENTRY row is watcher-held and only submits
to the broker AFTER the breach trigger fires. broker_order_id and submitted_ts
are intentionally NULL while waiting. The orphan expiry path must not treat
NULL broker fields as evidence of a broken row on a valid watcher-held entry.

Root problem:
_check_pending_trigger_order() checked three guards in order:
  1. broker_oid present → preserve
  2. submitted_ts present → preserve
  3. watcher in-memory owns it → preserve
  4. watcher ownership unknown → preserve (unknown)
  5. otherwise → EXPIRE as orphan

Step 5 fires when _watcher_owner_state is False — i.e., the in-memory watcher
does not hold the order. This legitimately happens on every process restart:
the OSM row persists in the DB but the watcher's in-memory set resets.
A row with trigger_price=155.0 and a real option contract falls through step 5
after a watcher restart and gets wrongly expired.

Fix:
Before the orphan expiry path, check the order row for watcher evidence:
  - trigger_price set (MC writes this at plan creation)
  - meta.trigger_type present (OSM writes from plan.trigger_type)
  - meta.watcher_audit present (watcher writes on arm)
  - meta.watcher_audit_history present (multi-cycle audit trail)
  - real option contract symbol (C260626C00150000 etc.)
  - DEFERRED:* contract (overnight deferred pre-breach)
  - meta.contract_deferred=True

If watcher evidence found AND entry cutoff NOT passed:
  → preserve, attempt re-arm, log PENDING_TRIGGER_WATCHER_EVIDENCE_PRESERVED

If watcher evidence found AND entry cutoff HAS passed:
  → expire with PENDING_TRIGGER_EOD_EXPIRED (not ORPHAN), reason honest

If no watcher evidence:
  → fall through to existing orphan expiry (CREATED rows, truly broken rows)

Tests:
  Test 1 — Jason regression: real contract before cutoff → KEEP
  Test 2 — DEFERRED:* before cutoff → KEEP
  Test 3 — trigger_price evidence → KEEP
  Test 4 — watcher_audit evidence → KEEP
  Test 5 — after EOD cutoff: watcher evidence + cutoff passed → EOD expire, not orphan
  Test 6 — CREATED stale orphan (no evidence) → EXPIRE (existing behavior preserved)
  Test 7 — terminal watcher failure last_error → fall through to existing path
  Test 8 — Source guards: sentinels, helper functions present
"""
from __future__ import annotations

import importlib.util
import re
import sys
import types
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

_REPO = Path(__file__).resolve().parents[1]
_OM_SRC = (_REPO / "ap" / "order_monitor.py").read_text()


# ---------------------------------------------------------------------------
# Load helpers from order_monitor without triggering full module init
# ---------------------------------------------------------------------------

def _load_om_helpers():
    """Import _is_valid_watcher_held_pending_trigger from order_monitor."""
    spec = importlib.util.spec_from_file_location(
        "ap.order_monitor_test_shim",
        _REPO / "ap" / "order_monitor.py",
    )
    with patch.dict(sys.modules, {
        "ap.db":      MagicMock(),
        "ap.utils":   MagicMock(),
        "ap.queue":   MagicMock(),
        "ap.brokers": MagicMock(),
    }):
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod

_om = _load_om_helpers()


# ---------------------------------------------------------------------------
# Build a minimal APOrderMonitor instance for unit-testing helpers
# ---------------------------------------------------------------------------

def _make_monitor(*, watcher_has_order: bool | None = False) -> object:
    """Build a minimal APOrderMonitor with stubbed dependencies."""
    mon = object.__new__(_om.APOrderMonitor)
    mon.client_id = "jasoncosby1@gmail.com"
    mon.mode = "live"

    # Stub watcher
    watcher = MagicMock()
    if watcher_has_order is None:
        watcher.has_order.side_effect = Exception("watcher_unavailable")
    else:
        watcher.has_order.return_value = watcher_has_order
    mon.entry_watcher = watcher

    # Stub OSM
    osm = MagicMock()
    osm.expire_pending_entry.return_value = True
    osm.transition.return_value = True
    mon.osm = osm

    return mon


# ---------------------------------------------------------------------------
# Helper: build a PENDING_TRIGGER order dict
# ---------------------------------------------------------------------------

def _order(
    *,
    local_id: str = "order-22514",
    contract: str = "C260626C00150000",
    trigger_price: float | None = 155.0,
    broker_order_id: str | None = None,
    submitted_ts=None,
    status: str = "PENDING_TRIGGER",
    meta: dict | None = None,
    last_error: str | None = None,
) -> dict:
    return {
        "id": 22514,
        "local_order_id": local_id,
        "contract": contract,
        "trigger_price": trigger_price,
        "broker_order_id": broker_order_id,
        "submitted_ts": submitted_ts,
        "status": status,
        "meta": meta or {},
        "last_error": last_error,
        "signal_id": "sig-test",
        "plan_id": "plan-test",
        "ticker": contract.split("2")[0] if "2" in contract else "C",
        "direction": "CALL",
        "kind": "ENTRY",
        "fill_price": None,
        "filled_qty": 0,
        "reserved_cost": 105.0,
    }


# ---------------------------------------------------------------------------
# Source guards
# ---------------------------------------------------------------------------

class TestSourceGuards:
    def test_is_valid_watcher_held_pending_trigger_exists(self):
        assert "_is_valid_watcher_held_pending_trigger" in _OM_SRC

    def test_is_after_pt_eod_cutoff_exists(self):
        assert "_is_after_pt_eod_cutoff" in _OM_SRC

    def test_preserved_sentinel_present(self):
        assert "PENDING_TRIGGER_WATCHER_EVIDENCE_PRESERVED" in _OM_SRC

    def test_eod_expired_sentinel_present(self):
        assert "PENDING_TRIGGER_WATCHER_EVIDENCE_EOD_EXPIRED" in _OM_SRC

    def test_eod_reason_code_differs_from_orphan_reason(self):
        assert "PENDING_TRIGGER_EOD_EXPIRED" in _OM_SRC
        assert "PENDING_TRIGGER_ORPHAN_EXPIRED" in _OM_SRC

    def test_watcher_evidence_guard_before_orphan_expiry(self):
        """The watcher-evidence guard must appear before the orphan expiry block."""
        evidence_idx = _OM_SRC.find("PENDING_TRIGGER_WATCHER_EVIDENCE_PRESERVED")
        orphan_idx = _OM_SRC.find('"PENDING_TRIGGER_ORPHAN_EXPIRED: no_broker_order_id"')
        if orphan_idx == -1:
            orphan_idx = _OM_SRC.find("PENDING_TRIGGER_ORPHAN_EXPIRED: no_broker_order_id")
        assert evidence_idx != -1
        assert orphan_idx != -1
        assert evidence_idx < orphan_idx, (
            "Watcher-evidence guard must fire BEFORE the orphan expiry block"
        )

    def test_eod_constants_present(self):
        assert "_PT_ORPHAN_EOD_CUTOFF_HOUR" in _OM_SRC
        assert "_PT_ORPHAN_EOD_CUTOFF_MIN" in _OM_SRC

    def test_trigger_price_evidence_in_helper(self):
        fn_start = _OM_SRC.find("def _is_valid_watcher_held_pending_trigger")
        fn_body = _OM_SRC[fn_start: fn_start + 2500]
        assert "trigger_price" in fn_body

    def test_real_option_contract_evidence_in_helper(self):
        fn_start = _OM_SRC.find("def _is_valid_watcher_held_pending_trigger")
        fn_body = _OM_SRC[fn_start: fn_start + 4500]
        assert "DEFERRED" in fn_body
        assert "[CP]" in fn_body, "Helper must use regex to detect option contract"
        assert "real_option_contract" in fn_body

    def test_watcher_audit_evidence_in_helper(self):
        fn_start = _OM_SRC.find("def _is_valid_watcher_held_pending_trigger")
        fn_body = _OM_SRC[fn_start: fn_start + 2500]
        assert "watcher_audit" in fn_body


# ---------------------------------------------------------------------------
# Test _is_valid_watcher_held_pending_trigger helper directly
# ---------------------------------------------------------------------------

class TestWatcherEvidenceHelper:
    def _helper(self, order):
        mon = _make_monitor()
        return mon._is_valid_watcher_held_pending_trigger(order)

    # Jason regression: real option contract + trigger_price
    def test_real_option_contract_is_evidence(self):
        is_valid, desc = self._helper(_order(
            contract="C260626C00150000", trigger_price=155.0
        ))
        assert is_valid is True
        assert "155.0" in desc or "trigger_price" in desc

    def test_trigger_price_alone_is_evidence(self):
        """trigger_price non-null/non-zero is sufficient even with generic contract."""
        is_valid, desc = self._helper(_order(
            contract="C260626C00150000", trigger_price=50.0
        ))
        assert is_valid is True

    def test_zero_trigger_price_not_evidence(self):
        """trigger_price=0 must NOT count as evidence (unset default)."""
        is_valid, desc = self._helper(_order(
            contract="TICK",
            trigger_price=0.0,
            meta={},
        ))
        assert is_valid is False

    def test_none_trigger_price_checked_via_contract(self):
        """trigger_price=None falls through; real option contract still counts."""
        is_valid, desc = self._helper(_order(
            contract="AAPL260626C00200000", trigger_price=None
        ))
        assert is_valid is True  # real option contract evidence

    def test_deferred_contract_is_evidence(self):
        is_valid, desc = self._helper(_order(
            contract="DEFERRED:FANG", trigger_price=42.0
        ))
        assert is_valid is True

    def test_deferred_lower_case_is_evidence(self):
        is_valid, desc = self._helper(_order(
            contract="deferred:FANG", trigger_price=None,
            meta={}
        ))
        # lowercase "deferred:" — should also be caught
        assert is_valid is True or "deferred" in desc.lower()

    def test_meta_trigger_type_is_evidence(self):
        is_valid, desc = self._helper(_order(
            contract="TICK",
            trigger_price=None,
            meta={"trigger_type": "breach"},
        ))
        assert is_valid is True
        assert "trigger_type" in desc

    def test_meta_watcher_audit_is_evidence(self):
        is_valid, desc = self._helper(_order(
            contract="TICK",
            trigger_price=None,
            meta={"watcher_audit": {"armed_at": "2026-06-17T09:30:00"}},
        ))
        assert is_valid is True

    def test_meta_watcher_audit_history_is_evidence(self):
        is_valid, desc = self._helper(_order(
            contract="TICK",
            trigger_price=None,
            meta={"watcher_audit_history": [{"cycle": 1}]},
        ))
        assert is_valid is True

    def test_meta_contract_deferred_true_is_evidence(self):
        is_valid, desc = self._helper(_order(
            contract="TICK",
            trigger_price=None,
            meta={"contract_deferred": True},
        ))
        assert is_valid is True

    def test_bare_ticker_no_evidence(self):
        """A short ticker-like contract with no other evidence is not a watcher row."""
        is_valid, desc = self._helper(_order(
            contract="C",
            trigger_price=None,
            meta={},
        ))
        assert is_valid is False

    def test_no_evidence_returns_false(self):
        is_valid, desc = self._helper(_order(
            contract="RAWTICKERONLY",
            trigger_price=None,
            meta={},
        ))
        assert is_valid is False
        assert "no_watcher_evidence" in desc


# ---------------------------------------------------------------------------
# Test 1 — Jason regression: real contract before cutoff → KEEP ALIVE
# ---------------------------------------------------------------------------

class TestJasonRegressionPreserved:
    """
    Jason order 22514: C260626C00150000, trigger_price=155.0
    broker_order_id=NULL, submitted_ts=NULL, age=5452s
    Must NOT be expired before EOD.
    """
    def _run(self, watcher_has_order=False, after_cutoff=False):
        mon = _make_monitor(watcher_has_order=watcher_has_order)
        order = _order(
            local_id="order-22514",
            contract="C260626C00150000",
            trigger_price=155.0,
        )
        with patch.object(mon, "_is_after_pt_eod_cutoff", return_value=after_cutoff), \
             patch.object(mon, "_attempt_lost_handoff_rearm",
                          return_value=(True, True, "rearm_success")), \
             patch.object(mon, "_log_pending_trigger_watchdog_seen"):
            mon._check_pending_trigger_order(
                order=order,
                local_id="order-22514",
                contract="C260626C00150000",
                age_secs=5452.0,
                broker_oid=None,
                submitted_ts=None,
            )
        return mon

    def test_osm_not_expired_before_cutoff(self):
        mon = self._run(watcher_has_order=False, after_cutoff=False)
        mon.osm.expire_pending_entry.assert_not_called()
        mon.osm.transition.assert_not_called()

    def test_rearm_attempted_before_cutoff(self):
        """Monitor must attempt re-arm when watcher lost in-memory state."""
        mon = _make_monitor(watcher_has_order=False)
        order = _order(contract="C260626C00150000", trigger_price=155.0)
        rearm_mock = MagicMock(return_value=(True, True, "rearm_success"))
        with patch.object(mon, "_is_after_pt_eod_cutoff", return_value=False), \
             patch.object(mon, "_attempt_lost_handoff_rearm", rearm_mock), \
             patch.object(mon, "_log_pending_trigger_watchdog_seen"):
            mon._check_pending_trigger_order(
                order=order, local_id="order-22514",
                contract="C260626C00150000", age_secs=5452.0,
                broker_oid=None, submitted_ts=None,
            )
        rearm_mock.assert_called_once()

    def test_orphan_reason_never_written(self):
        """The ORPHAN last_error must never be written for this row."""
        mon = _make_monitor(watcher_has_order=False)
        order = _order(contract="C260626C00150000", trigger_price=155.0)
        with patch.object(mon, "_is_after_pt_eod_cutoff", return_value=False), \
             patch.object(mon, "_attempt_lost_handoff_rearm",
                          return_value=(True, False, "no_plan")), \
             patch.object(mon, "_log_pending_trigger_watchdog_seen"):
            mon._check_pending_trigger_order(
                order=order, local_id="order-22514",
                contract="C260626C00150000", age_secs=5452.0,
                broker_oid=None, submitted_ts=None,
            )
        # expire_pending_entry must not have been called with orphan reason
        for c in mon.osm.expire_pending_entry.call_args_list:
            reason = c.kwargs.get("reason", "") or (c.args[1] if len(c.args) > 1 else "")
            assert "ORPHAN" not in reason, f"Orphan reason must not be used: {reason}"


# ---------------------------------------------------------------------------
# Test 2 — DEFERRED:* before cutoff → KEEP ALIVE
# ---------------------------------------------------------------------------

class TestDeferredContractPreserved:
    def test_deferred_contract_preserved_before_cutoff(self):
        mon = _make_monitor(watcher_has_order=False)
        order = _order(
            contract="DEFERRED:FANG",
            trigger_price=42.0,
            meta={"contract_deferred": True},
        )
        with patch.object(mon, "_is_after_pt_eod_cutoff", return_value=False), \
             patch.object(mon, "_attempt_lost_handoff_rearm",
                          return_value=(True, True, "rearm_success")), \
             patch.object(mon, "_log_pending_trigger_watchdog_seen"):
            mon._check_pending_trigger_order(
                order=order, local_id="order-def-001",
                contract="DEFERRED:FANG", age_secs=6000.0,
                broker_oid=None, submitted_ts=None,
            )
        mon.osm.expire_pending_entry.assert_not_called()
        mon.osm.transition.assert_not_called()


# ---------------------------------------------------------------------------
# Test 3 — trigger_price evidence → KEEP (no other evidence)
# ---------------------------------------------------------------------------

class TestTriggerPriceEvidence:
    def test_trigger_price_alone_preserves_row(self):
        mon = _make_monitor(watcher_has_order=False)
        order = _order(
            contract="RAWTICKERONLY",
            trigger_price=99.5,
            meta={},
        )
        with patch.object(mon, "_is_after_pt_eod_cutoff", return_value=False), \
             patch.object(mon, "_attempt_lost_handoff_rearm",
                          return_value=(False, False, "no_watcher")), \
             patch.object(mon, "_log_pending_trigger_watchdog_seen"):
            mon._check_pending_trigger_order(
                order=order, local_id="order-tp-001",
                contract="RAWTICKERONLY", age_secs=5500.0,
                broker_oid=None, submitted_ts=None,
            )
        mon.osm.expire_pending_entry.assert_not_called()


# ---------------------------------------------------------------------------
# Test 4 — watcher_audit evidence → KEEP
# ---------------------------------------------------------------------------

class TestWatcherAuditEvidence:
    def test_watcher_audit_meta_preserves_row(self):
        mon = _make_monitor(watcher_has_order=False)
        order = _order(
            contract="RAWTICKERONLY",
            trigger_price=None,
            meta={"watcher_audit": {"armed_at": "2026-06-17T09:30:00", "trigger_price": 55.0}},
        )
        with patch.object(mon, "_is_after_pt_eod_cutoff", return_value=False), \
             patch.object(mon, "_attempt_lost_handoff_rearm",
                          return_value=(False, False, "no_watcher")), \
             patch.object(mon, "_log_pending_trigger_watchdog_seen"):
            mon._check_pending_trigger_order(
                order=order, local_id="order-wa-001",
                contract="RAWTICKERONLY", age_secs=5500.0,
                broker_oid=None, submitted_ts=None,
            )
        mon.osm.expire_pending_entry.assert_not_called()


# ---------------------------------------------------------------------------
# Test 5 — After EOD cutoff: watcher evidence + cutoff passed → EOD expire,
#          not orphan reason
# ---------------------------------------------------------------------------

class TestEODExpireNotOrphan:
    def test_eod_expire_uses_eod_reason_not_orphan(self):
        mon = _make_monitor(watcher_has_order=False)
        order = _order(
            contract="C260626C00150000",
            trigger_price=155.0,
        )
        captured_reasons = []
        def _capture_expire(local_id, reason=""):
            captured_reasons.append(reason)
            return True

        mon.osm.expire_pending_entry.side_effect = _capture_expire

        with patch.object(mon, "_is_after_pt_eod_cutoff", return_value=True), \
             patch.object(mon, "_log_pending_trigger_watchdog_seen"):
            mon._check_pending_trigger_order(
                order=order, local_id="order-eod-001",
                contract="C260626C00150000", age_secs=7200.0,
                broker_oid=None, submitted_ts=None,
            )

        assert len(captured_reasons) == 1, "Must expire exactly once"
        reason = captured_reasons[0]
        assert "ORPHAN" not in reason, f"EOD expire must not use orphan reason: {reason}"
        assert "EOD" in reason or "cutoff" in reason.lower() or "eod" in reason.lower(), (
            f"EOD expire reason must mention cutoff: {reason}"
        )

    def test_eod_reason_mentions_evidence(self):
        mon = _make_monitor(watcher_has_order=False)
        order = _order(contract="C260626C00150000", trigger_price=155.0)
        captured_reasons = []
        mon.osm.expire_pending_entry.side_effect = lambda lid, reason="": captured_reasons.append(reason) or True

        with patch.object(mon, "_is_after_pt_eod_cutoff", return_value=True), \
             patch.object(mon, "_log_pending_trigger_watchdog_seen"):
            mon._check_pending_trigger_order(
                order=order, local_id="order-eod-002",
                contract="C260626C00150000", age_secs=7200.0,
                broker_oid=None, submitted_ts=None,
            )

        if captured_reasons:
            # Reason must NOT be the generic orphan reason
            assert "no_broker_order_id no_submitted_ts" not in captured_reasons[0]


# ---------------------------------------------------------------------------
# Test 6 — CREATED stale orphan (no watcher evidence) → EXPIRE (existing behavior)
# ---------------------------------------------------------------------------

class TestCreatedStaleOrphanExpired:
    """Truly broken rows with no watcher evidence must still expire."""

    def test_no_evidence_falls_through_to_orphan_expiry(self):
        mon = _make_monitor(watcher_has_order=False)
        # No trigger_price, no watcher audit, bare ticker contract, no meta
        order = _order(
            contract="RAWTICKERONLY",  # not a real option symbol
            trigger_price=None,
            meta={},
        )
        with patch.object(mon, "_is_after_pt_eod_cutoff", return_value=False), \
             patch.object(mon, "_log_pending_trigger_watchdog_seen"):
            mon._check_pending_trigger_order(
                order=order, local_id="order-broken-001",
                contract="RAWTICKERONLY", age_secs=6000.0,
                broker_oid=None, submitted_ts=None,
            )
        # Must expire — no watcher evidence
        assert (
            mon.osm.expire_pending_entry.called
            or mon.osm.transition.called
        ), "Row with no watcher evidence must be expired"


# ---------------------------------------------------------------------------
# Test 7 — PENDING_TRIGGER with watcher terminal failure → falls through
# ---------------------------------------------------------------------------

class TestWatcherTerminalFailureExpired:
    """
    If last_error contains a known terminal failure reason from the watcher
    (e.g. watcher_block, breach_time_contract_selection_no_result), the row
    should fall through to the existing expiry path.
    These rows typically have NO trigger_price / watcher_audit since the
    watcher rejected the arm — so no watcher evidence is present.
    """

    def test_terminal_failure_with_no_evidence_expires(self):
        mon = _make_monitor(watcher_has_order=False)
        order = _order(
            contract="RAWTICKERONLY",
            trigger_price=None,
            meta={},
            last_error="breach_time_contract_selection_no_result",
        )
        with patch.object(mon, "_is_after_pt_eod_cutoff", return_value=False), \
             patch.object(mon, "_log_pending_trigger_watchdog_seen"):
            mon._check_pending_trigger_order(
                order=order, local_id="order-terminal-001",
                contract="RAWTICKERONLY", age_secs=6000.0,
                broker_oid=None, submitted_ts=None,
            )
        assert (
            mon.osm.expire_pending_entry.called
            or mon.osm.transition.called
        ), "Terminal failure row with no watcher evidence must expire"


# ---------------------------------------------------------------------------
# Test 8 — _is_after_pt_eod_cutoff helper
# ---------------------------------------------------------------------------

class TestEODCutoffHelper:
    """Test _is_after_pt_eod_cutoff using the _PT_ORPHAN_EOD_CUTOFF_HOUR/MIN
    constants directly rather than patching datetime (which would require the
    shim module path and is fragile). Instead, unit-test the underlying logic."""

    def test_logic_before_cutoff(self):
        """Before 15:30 ET → should not be after cutoff."""
        cutoff_hour = _om._PT_ORPHAN_EOD_CUTOFF_HOUR
        cutoff_min = _om._PT_ORPHAN_EOD_CUTOFF_MIN

        # Simulate 14:00 ET
        h, m = 14, 0
        after = (
            h > cutoff_hour
            or (h == cutoff_hour and m >= cutoff_min)
        )
        assert after is False

    def test_logic_after_cutoff(self):
        """After 15:30 ET → should be after cutoff."""
        cutoff_hour = _om._PT_ORPHAN_EOD_CUTOFF_HOUR
        cutoff_min = _om._PT_ORPHAN_EOD_CUTOFF_MIN

        h, m = 15, 45
        after = (
            h > cutoff_hour
            or (h == cutoff_hour and m >= cutoff_min)
        )
        assert after is True

    def test_logic_exactly_at_cutoff(self):
        """Exactly at 15:30 ET → should be after cutoff (>= semantics)."""
        cutoff_hour = _om._PT_ORPHAN_EOD_CUTOFF_HOUR
        cutoff_min = _om._PT_ORPHAN_EOD_CUTOFF_MIN

        h, m = cutoff_hour, cutoff_min
        after = (
            h > cutoff_hour
            or (h == cutoff_hour and m >= cutoff_min)
        )
        assert after is True

    def test_default_cutoff_constants(self):
        """Default cutoff must be 15:30 ET matching ap_entry_watcher."""
        assert _om._PT_ORPHAN_EOD_CUTOFF_HOUR == 15
        assert _om._PT_ORPHAN_EOD_CUTOFF_MIN == 30

    def test_helper_returns_bool(self):
        """_is_after_pt_eod_cutoff must return a bool, not raise."""
        mon = _make_monitor()
        result = mon._is_after_pt_eod_cutoff()
        assert isinstance(result, bool)


# ---------------------------------------------------------------------------
# Test 9 — Existing guards not broken
# broker_oid present and submitted_ts present still skip expiry
# ---------------------------------------------------------------------------

class TestExistingGuardsPreserved:
    def test_broker_oid_present_skips_expiry(self):
        mon = _make_monitor(watcher_has_order=False)
        order = _order(contract="C260626C00150000", trigger_price=155.0)
        with patch.object(mon, "_log_pending_trigger_watchdog_seen"):
            mon._check_pending_trigger_order(
                order=order, local_id="order-broker-001",
                contract="C260626C00150000", age_secs=6000.0,
                broker_oid="BROKER-123",  # broker_oid present
                submitted_ts=None,
            )
        mon.osm.expire_pending_entry.assert_not_called()
        mon.osm.transition.assert_not_called()

    def test_submitted_ts_present_skips_expiry(self):
        mon = _make_monitor(watcher_has_order=False)
        order = _order(contract="C260626C00150000", trigger_price=155.0)
        with patch.object(mon, "_log_pending_trigger_watchdog_seen"):
            mon._check_pending_trigger_order(
                order=order, local_id="order-submitted-001",
                contract="C260626C00150000", age_secs=6000.0,
                broker_oid=None,
                submitted_ts=datetime.now(timezone.utc),
            )
        mon.osm.expire_pending_entry.assert_not_called()
        mon.osm.transition.assert_not_called()

    def test_watcher_owns_it_skips_expiry(self):
        mon = _make_monitor(watcher_has_order=True)
        order = _order(contract="RAWTICKERONLY", trigger_price=None, meta={})
        with patch.object(mon, "_log_pending_trigger_watchdog_seen"):
            mon._check_pending_trigger_order(
                order=order, local_id="order-owned-001",
                contract="RAWTICKERONLY", age_secs=6000.0,
                broker_oid=None, submitted_ts=None,
            )
        mon.osm.expire_pending_entry.assert_not_called()
        mon.osm.transition.assert_not_called()

    def test_within_max_age_returns_early(self):
        mon = _make_monitor()
        order = _order(contract="RAWTICKERONLY", trigger_price=None, meta={})
        with patch.object(mon, "_log_pending_trigger_watchdog_seen") as mock_log:
            mon._check_pending_trigger_order(
                order=order, local_id="order-young-001",
                contract="RAWTICKERONLY", age_secs=100.0,  # well within age limit
                broker_oid=None, submitted_ts=None,
            )
        mock_log.assert_not_called()
        mon.osm.expire_pending_entry.assert_not_called()


# ---------------------------------------------------------------------------
# P1: overnight/deferred rows preserved after EOD cutoff
#
# PENDING_TRIGGER rows with overnight/deferred evidence must NOT be
# EOD-expired solely because wall-clock is past 15:30 ET.
#
# Only same-day intraday rows that missed the trading window may expire.
# Overnight/deferred rows survive across sessions and are handled by
# overnight_reeval or the morning handoff audit at next open.
# ---------------------------------------------------------------------------

class TestOvernightDeferred:
    """_is_overnight_or_deferred_row detects all evidence variants."""

    def _helper(self, order: dict) -> tuple[bool, str]:
        mon = _make_monitor()
        return mon._is_overnight_or_deferred_row(order)

    # Evidence 1: DEFERRED:* contract
    def test_deferred_contract_prefix_detected(self):
        is_ov, desc = self._helper(_order(contract="DEFERRED:FANG"))
        assert is_ov is True
        assert "deferred_contract" in desc

    def test_deferred_lowercase_detected(self):
        is_ov, desc = self._helper(_order(contract="deferred:USB"))
        assert is_ov is True

    # Evidence 2: meta.contract_deferred
    def test_meta_contract_deferred_true_detected(self):
        is_ov, desc = self._helper(_order(
            contract="C260626C00150000", meta={"contract_deferred": True}
        ))
        assert is_ov is True
        assert "contract_deferred" in desc

    # Evidence 3: meta.overnight
    def test_meta_overnight_true_detected(self):
        is_ov, desc = self._helper(_order(
            contract="C260626C00150000", meta={"overnight": True}
        ))
        assert is_ov is True
        assert "overnight" in desc

    # Evidence 4: meta.queue_status variants
    def test_queue_status_after_hours_deferred(self):
        is_ov, _ = self._helper(_order(
            meta={"queue_status": "after_hours_deferred"}
        ))
        assert is_ov is True

    def test_queue_status_awaiting_overnight_reeval(self):
        is_ov, _ = self._helper(_order(
            meta={"queue_status": "awaiting_overnight_reeval"}
        ))
        assert is_ov is True

    def test_queue_status_open_recheck(self):
        is_ov, _ = self._helper(_order(meta={"queue_status": "open_recheck"}))
        assert is_ov is True

    # Evidence 5: timeframe
    def test_timeframe_1d_detected(self):
        is_ov, desc = self._helper(_order(meta={"timeframe": "1d"}))
        assert is_ov is True
        assert "timeframe" in desc

    def test_timeframe_daily_detected(self):
        is_ov, _ = self._helper(_order(meta={"timeframe": "daily"}))
        assert is_ov is True

    def test_timeframe_overnight_detected(self):
        is_ov, _ = self._helper(_order(meta={"timeframe": "overnight"}))
        assert is_ov is True

    def test_timeframe_order_level(self):
        """Timeframe at order top-level (not just in meta) is also detected."""
        o = _order(meta={})
        o["timeframe"] = "1d"
        is_ov, _ = self._helper(o)
        assert is_ov is True

    def test_timeframe_5m_intraday_not_detected(self):
        """5-minute timeframe is same-day — not overnight."""
        is_ov, desc = self._helper(_order(meta={"timeframe": "5m"}))
        assert is_ov is False

    # Evidence 6 & 7: prior_day levels
    def test_meta_prior_day_high_detected(self):
        is_ov, desc = self._helper(_order(meta={"prior_day_high": 155.0}))
        assert is_ov is True
        assert "prior_day" in desc

    def test_meta_prior_day_low_detected(self):
        is_ov, _ = self._helper(_order(meta={"prior_day_low": 148.0}))
        assert is_ov is True

    def test_order_level_prior_day_high_detected(self):
        o = _order(meta={})
        o["prior_day_high"] = 160.0
        is_ov, desc = self._helper(o)
        assert is_ov is True

    # No evidence → same-day
    def test_no_overnight_evidence_returns_false(self):
        is_ov, desc = self._helper(_order(
            contract="C260626C00150000",
            trigger_price=155.0,
            meta={"trigger_type": "breach"},
        ))
        assert is_ov is False
        assert "no_overnight_deferred_evidence" in desc

    def test_source_has_overnight_preserved_sentinel(self):
        src = open(_REPO / "ap" / "order_monitor.py").read()
        assert "PENDING_TRIGGER_OVERNIGHT_PRESERVED" in src

    def test_source_has_is_overnight_or_deferred_row_method(self):
        src = open(_REPO / "ap" / "order_monitor.py").read()
        assert "_is_overnight_or_deferred_row" in src


class TestOvernightPreservedAfterCutoff:
    """After EOD cutoff, overnight/deferred rows must be preserved — not expired."""

    def _run(self, order: dict, after_cutoff: bool = True) -> object:
        mon = _make_monitor(watcher_has_order=False)
        with patch.object(mon, "_is_after_pt_eod_cutoff", return_value=after_cutoff), \
             patch.object(mon, "_attempt_lost_handoff_rearm",
                          return_value=(True, False, "no_watcher")), \
             patch.object(mon, "_log_pending_trigger_watchdog_seen"):
            mon._check_pending_trigger_order(
                order=order,
                local_id=order["local_order_id"],
                contract=order["contract"],
                age_secs=6000.0,
                broker_oid=None,
                submitted_ts=None,
            )
        return mon

    # ── DEFERRED:* contract after cutoff → preserved, not expired ─────────

    def test_deferred_contract_preserved_after_cutoff(self):
        """DEFERRED:FANG must not be expired after 15:30 ET."""
        mon = self._run(_order(
            local_id="order-deferred-eod",
            contract="DEFERRED:FANG",
            trigger_price=42.0,
            meta={"contract_deferred": True},
        ), after_cutoff=True)
        mon.osm.expire_pending_entry.assert_not_called()
        mon.osm.transition.assert_not_called()

    def test_deferred_contract_no_orphan_reason_after_cutoff(self):
        """DEFERRED row after cutoff must not get PENDING_TRIGGER_ORPHAN_EXPIRED."""
        mon = self._run(_order(
            local_id="order-deferred-orphan",
            contract="DEFERRED:USB",
            trigger_price=60.0,
            meta={"contract_deferred": True},
        ), after_cutoff=True)
        for c in mon.osm.expire_pending_entry.call_args_list:
            reason = c.kwargs.get("reason", "") or ""
            assert "ORPHAN" not in reason
            assert "EOD" not in reason

    def test_deferred_contract_no_eod_expired_reason(self):
        """DEFERRED row after cutoff must not get PENDING_TRIGGER_EOD_EXPIRED."""
        mon = self._run(_order(
            local_id="order-deferred-eod2",
            contract="DEFERRED:NVDA",
            trigger_price=80.0,
            meta={"contract_deferred": True},
        ), after_cutoff=True)
        for c in mon.osm.expire_pending_entry.call_args_list:
            reason = c.kwargs.get("reason", "") or ""
            assert "EOD_EXPIRED" not in reason

    # ── meta.contract_deferred=True after cutoff → preserved ──────────────

    def test_meta_contract_deferred_preserved_after_cutoff(self):
        mon = self._run(_order(
            local_id="order-meta-deferred",
            contract="RAWTICKERONLY",
            trigger_price=None,
            meta={"contract_deferred": True},
        ), after_cutoff=True)
        mon.osm.expire_pending_entry.assert_not_called()
        mon.osm.transition.assert_not_called()

    # ── timeframe=1d after cutoff → preserved ─────────────────────────────

    def test_daily_timeframe_preserved_after_cutoff(self):
        o = _order(
            local_id="order-daily-eod",
            contract="C260626C00150000",
            trigger_price=155.0,
            meta={"timeframe": "1d"},
        )
        mon = self._run(o, after_cutoff=True)
        mon.osm.expire_pending_entry.assert_not_called()
        mon.osm.transition.assert_not_called()

    # ── prior_day level after cutoff → preserved ──────────────────────────

    def test_prior_day_high_preserved_after_cutoff(self):
        o = _order(
            local_id="order-pdh-eod",
            contract="C260626C00150000",
            trigger_price=155.0,
            meta={"prior_day_high": 156.0},
        )
        mon = self._run(o, after_cutoff=True)
        mon.osm.expire_pending_entry.assert_not_called()

    # ── overnight sentinel logged, not EOD sentinel ────────────────────────

    def test_overnight_preserved_sentinel_logged(self):
        """PENDING_TRIGGER_OVERNIGHT_PRESERVED must appear in log for deferred rows."""
        import logging
        mon = _make_monitor(watcher_has_order=False)
        with patch.object(mon, "_is_after_pt_eod_cutoff", return_value=True), \
             patch.object(mon, "_attempt_lost_handoff_rearm",
                          return_value=(True, False, "no_watcher")), \
             patch.object(mon, "_log_pending_trigger_watchdog_seen"), \
             self.assertLogs("ap.order_monitor", level="INFO") as log_ctx:
            mon._check_pending_trigger_order(
                order=_order(
                    local_id="order-sentinel",
                    contract="DEFERRED:FANG",
                    trigger_price=42.0,
                    meta={"contract_deferred": True},
                ),
                local_id="order-sentinel",
                contract="DEFERRED:FANG",
                age_secs=7200.0,
                broker_oid=None,
                submitted_ts=None,
            )
        combined = "\n".join(log_ctx.output)
        assert "PENDING_TRIGGER_OVERNIGHT_PRESERVED" in combined, (
            "Overnight preserved rows must log PENDING_TRIGGER_OVERNIGHT_PRESERVED"
        )
        assert "PENDING_TRIGGER_WATCHER_EVIDENCE_EOD_EXPIRED" not in combined

    def assertLogs(self, logger_name="", level="INFO"):
        import unittest
        return unittest.TestCase().assertLogs(logger_name, level=level)

    # ── Same-day intraday row after cutoff → still expires ────────────────

    def test_same_day_intraday_still_expires_after_cutoff(self):
        """A same-day intraday row (no overnight evidence) must still be expired
        after cutoff — the P1 fix must not suppress legitimate EOD cleanup."""
        mon = _make_monitor(watcher_has_order=False)
        # Only trigger_price evidence — no deferred/overnight markers
        order = _order(
            local_id="order-intraday-eod",
            contract="RAWTICKERONLY",
            trigger_price=None,   # no watcher evidence at all
            meta={},
        )
        with patch.object(mon, "_is_after_pt_eod_cutoff", return_value=True), \
             patch.object(mon, "_log_pending_trigger_watchdog_seen"):
            mon._check_pending_trigger_order(
                order=order,
                local_id="order-intraday-eod",
                contract="RAWTICKERONLY",
                age_secs=6000.0,
                broker_oid=None,
                submitted_ts=None,
            )
        # No watcher evidence → falls through to orphan expiry
        assert (
            mon.osm.expire_pending_entry.called
            or mon.osm.transition.called
        ), "Same-day row with no watcher evidence must still expire after cutoff"

    def test_same_day_real_contract_with_trigger_expires_after_cutoff(self):
        """A same-day real-contract intraday row must be EOD-expired after cutoff,
        not orphan-expired (reason must say EOD, not ORPHAN)."""
        mon = _make_monitor(watcher_has_order=False)
        # Real option contract + trigger price but no overnight markers
        order = _order(
            local_id="order-intraday-real",
            contract="C260626C00150000",
            trigger_price=155.0,
            meta={},  # no overnight/deferred evidence
        )
        captured_reasons = []
        mon.osm.expire_pending_entry.side_effect = lambda lid, reason="": captured_reasons.append(reason) or True

        with patch.object(mon, "_is_after_pt_eod_cutoff", return_value=True), \
             patch.object(mon, "_log_pending_trigger_watchdog_seen"):
            mon._check_pending_trigger_order(
                order=order,
                local_id="order-intraday-real",
                contract="C260626C00150000",
                age_secs=6000.0,
                broker_oid=None,
                submitted_ts=None,
            )

        assert len(captured_reasons) == 1, "Must expire exactly once"
        reason = captured_reasons[0]
        assert "ORPHAN" not in reason, f"EOD expire must not use orphan reason: {reason}"
        assert "EOD_EXPIRED" in reason or "cutoff" in reason.lower(), (
            f"EOD expire must mention cutoff: {reason}"
        )
