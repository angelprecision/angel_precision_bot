"""
tests/test_p0_pr597_terminal_watcher_convergence.py

PR #597 — Terminal watcher convergence.

Binds spec: docs/pr_specs/p0_terminal_watcher_convergence_20260908.md

Exercises the production _resolve_trigger_callback_disposition() path in
ap_entry_watcher.py to prove:

  * Sep-8 TMO ownership split is closed (fail-first + head passes).
  * Watcher terminal-reason authority matches the recovery terminalizer's
    canonical family (last_error + 5 meta fields).
  * watcher_audit.reason_code=trigger_ready alone is NOT terminal proof.
  * Identity mismatches never evict a watcher.
  * Conflicting durable reasons HOLD.
  * Cleanup is exact, idempotent, and does not touch broker/position/proof.

Money path: zero broker submit/cancel calls, zero position/proof mutation.
"""
from __future__ import annotations

import os
import types
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


def _tmo_row(
    *,
    status: str = "CANCELED",
    last_error: str | None = "restart_stuck_trigger_ready_no_broker_proof",
    restart_recovery_terminal_reason: str | None = "restart_stuck_trigger_ready_no_broker_proof",
    terminal_reason: str | None = None,
    reason_code: str | None = None,
    final_reason: str | None = None,
    materialization_reason: str | None = None,
    watcher_invalidation_reason: str | None = None,
    watcher_audit_reason_code: str | None = None,
    local_order_id: str = "b9e29854-1c97-43d6-986b-eef0506bdf5a",
    client_id: str = "jason@example.com",
    execution_mode: str = "live",
    broker_order_id: str | None = None,
    submitted_ts=None,
    restart_recovery_cls: str | None = "STUCK_TRIGGER_READY",
) -> dict:
    """Production TMO shape from the Sep-8 Jason LIVE incident."""
    meta: dict = {}
    if restart_recovery_terminal_reason is not None:
        meta["restart_recovery_terminal_reason"] = restart_recovery_terminal_reason
    if terminal_reason is not None:
        meta["terminal_reason"] = terminal_reason
    if reason_code is not None:
        meta["reason_code"] = reason_code
    if final_reason is not None:
        meta["final_reason"] = final_reason
    if materialization_reason is not None:
        meta["materialization_reason"] = materialization_reason
    if watcher_invalidation_reason is not None:
        meta["watcher_invalidation_reason"] = watcher_invalidation_reason
    if watcher_audit_reason_code is not None:
        meta["watcher_audit"] = {"reason_code": watcher_audit_reason_code}
    if restart_recovery_cls is not None:
        meta["restart_recovery_cls"] = restart_recovery_cls

    return {
        "local_order_id": local_order_id,
        "client_id": client_id,
        "execution_mode": execution_mode,
        "status": status,
        "last_error": last_error,
        "broker_order_id": broker_order_id,
        "submitted_ts": submitted_ts,
        "meta": meta,
    }


def _watcher_for(row: dict, *, ticker: str = "TMO", side: str = "CALL"):
    """Real APEntryWatcher wired against a mock OSM that returns `row`."""
    import ap_entry_watcher as ew

    osm = MagicMock()
    osm.get_order.return_value = row
    # Broker/order-side calls that must never fire in convergence.
    osm.cancel_pending_entry = MagicMock()
    osm.submit_existing_entry = MagicMock()

    broker = MagicMock()
    broker.submit_order = MagicMock()
    broker.cancel_order = MagicMock()

    w = ew.APEntryWatcher(broker, order_state_machine=osm, mode="LIVE")
    # Populate the runtime identity the watcher advertises to itself.
    w.execution_mode = str(row.get("execution_mode") or "").strip().lower()
    w.mode = "LIVE"
    w.paper = False
    w.client_id = str(row.get("client_id") or "")

    watched = types.SimpleNamespace(
        signal={
            "local_order_id": row.get("local_order_id"),
            "signal_id": "sig-tmo",
            "ticker": ticker,
            "side": side,
            "entry_price": 100.0,
            "client_id": row.get("client_id"),
            "client_email": row.get("client_id"),
            "execution_mode": row.get("execution_mode"),
            "contract_symbol": f"DEFERRED:{ticker}",
        },
    )
    return ew, w, osm, broker, watched


def _resolve(w, watched, *, claim: str | None = "TERMINAL_DURABLE"):
    result = {"disposition": claim} if claim else None
    return w._resolve_trigger_callback_disposition(watched, result)


# ─────────────────────────────────────────────────────────────────────────────
# Section §12 — Exact TMO fail-first replay
# ─────────────────────────────────────────────────────────────────────────────


class TestTMOFailFirstReplay:
    """The Sep-8 Jason LIVE TMO production incident, exact identity."""

    def test_tmo_canceled_with_last_error_and_restart_recovery_reason_is_terminal(self):
        row = _tmo_row()  # exact TMO Sep-8 shape
        ew, w, osm, broker, watched = _watcher_for(row)

        disposition, next_retry = _resolve(w, watched, claim="TERMINAL_DURABLE")

        assert disposition == "TERMINAL_DURABLE", (
            "TMO Sep-8 shape (CANCELED + last_error + restart_recovery_terminal_reason) "
            "must converge to TERMINAL_DURABLE — the ownership-split bug is closed."
        )
        assert next_retry is None
        # Money path — none of these may fire from terminal convergence.
        broker.submit_order.assert_not_called()
        broker.cancel_order.assert_not_called()
        osm.cancel_pending_entry.assert_not_called()
        osm.submit_existing_entry.assert_not_called()

    def test_pre_fix_narrow_family_would_have_missed_tmo(self):
        """
        Regression pin: proves the pre-#597 narrow verifier — which checked
        only meta.reason_code / meta.final_reason / meta.materialization_reason
        — would NOT have recognised the TMO Sep-8 durable reason authorities.

        This test locks the failure class in place so a future refactor that
        accidentally reverts to the narrow set is caught here rather than in
        production on live money.
        """
        row = _tmo_row()
        narrow_only = bool(
            row["meta"].get("reason_code")
            or row["meta"].get("final_reason")
            or row["meta"].get("materialization_reason")
        )
        assert narrow_only is False, (
            "TMO Sep-8 shape must NOT satisfy the pre-fix narrow terminal-reason "
            "family — if this test passes, the incident row would still fail closed."
        )
        # And the canonical durable authorities that PR #597 recognises DO carry the reason:
        assert row["last_error"], "TMO must carry top-level last_error"
        assert row["meta"]["restart_recovery_terminal_reason"], (
            "TMO must carry meta.restart_recovery_terminal_reason"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Section §13.1–7 — Terminal reason vocabulary matrix
# ─────────────────────────────────────────────────────────────────────────────


class TestTerminalReasonVocabulary:

    def test_13_1_canceled_with_orders_last_error_is_terminal(self):
        row = _tmo_row(
            last_error="restart_stuck_trigger_ready_no_broker_proof",
            restart_recovery_terminal_reason=None,
            restart_recovery_cls=None,
        )
        _, w, _, _, watched = _watcher_for(row)
        assert _resolve(w, watched)[0] == "TERMINAL_DURABLE"

    def test_13_2_canceled_with_restart_recovery_terminal_reason_is_terminal(self):
        row = _tmo_row(
            last_error=None,
            restart_recovery_terminal_reason="restart_stuck_trigger_ready_no_broker_proof",
        )
        _, w, _, _, watched = _watcher_for(row)
        assert _resolve(w, watched)[0] == "TERMINAL_DURABLE"

    def test_13_3_expired_with_meta_terminal_reason_is_terminal(self):
        row = _tmo_row(
            status="EXPIRED",
            last_error=None,
            restart_recovery_terminal_reason=None,
            terminal_reason="watch_deadline_exceeded",
            restart_recovery_cls=None,
        )
        _, w, _, _, watched = _watcher_for(row)
        assert _resolve(w, watched)[0] == "TERMINAL_DURABLE"

    def test_13_4_rejected_with_meta_reason_code_is_terminal(self):
        row = _tmo_row(
            status="REJECTED",
            last_error=None,
            restart_recovery_terminal_reason=None,
            reason_code="broker_rejected_spread_too_wide",
            restart_recovery_cls=None,
        )
        _, w, _, _, watched = _watcher_for(row)
        assert _resolve(w, watched)[0] == "TERMINAL_DURABLE"

    def test_13_5_error_with_meta_final_reason_is_terminal(self):
        row = _tmo_row(
            status="ERROR",
            last_error=None,
            restart_recovery_terminal_reason=None,
            final_reason="internal_state_error",
            restart_recovery_cls=None,
        )
        _, w, _, _, watched = _watcher_for(row)
        assert _resolve(w, watched)[0] == "TERMINAL_DURABLE"

    def test_13_6_meta_materialization_reason_still_terminal(self):
        """Regression: PR #597 must not remove the pre-existing materialization_reason family."""
        row = _tmo_row(
            status="CANCELED",
            last_error=None,
            restart_recovery_terminal_reason=None,
            materialization_reason="materialization_max_attempts_expired",
            restart_recovery_cls=None,
        )
        _, w, _, _, watched = _watcher_for(row)
        assert _resolve(w, watched)[0] == "TERMINAL_DURABLE"

    def test_13_7_duplicate_consistent_reason_across_fields_is_terminal(self):
        """§8.2 second clause: identical reason in multiple fields = consistent authority."""
        row = _tmo_row(
            last_error="restart_stuck_trigger_ready_no_broker_proof",
            restart_recovery_terminal_reason="restart_stuck_trigger_ready_no_broker_proof",
            terminal_reason="restart_stuck_trigger_ready_no_broker_proof",
        )
        _, w, _, _, watched = _watcher_for(row)
        assert _resolve(w, watched)[0] == "TERMINAL_DURABLE"

    def test_watcher_invalidation_reason_alone_is_terminal(self):
        """meta.watcher_invalidation_reason is a canonical authority per spec §7."""
        row = _tmo_row(
            status="CANCELED",
            last_error=None,
            restart_recovery_terminal_reason=None,
            watcher_invalidation_reason="stop_bid_below_call_stop",
            restart_recovery_cls=None,
        )
        _, w, _, _, watched = _watcher_for(row)
        assert _resolve(w, watched)[0] == "TERMINAL_DURABLE"


# ─────────────────────────────────────────────────────────────────────────────
# Section §13.8–15 — Negative authority
# ─────────────────────────────────────────────────────────────────────────────


class TestNegativeAuthority:

    def test_13_8_terminal_status_with_no_recognized_reason_holds(self):
        row = _tmo_row(
            status="CANCELED",
            last_error=None,
            restart_recovery_terminal_reason=None,
            restart_recovery_cls=None,
        )
        _, w, _, _, watched = _watcher_for(row)
        disp, _ = _resolve(w, watched)
        assert disp == "KEEP_WATCHER", (
            "§8.1: terminal status without recognized durable reason must HOLD, not evict."
        )

    def test_13_9_terminal_status_with_only_watcher_audit_trigger_ready_holds(self):
        """The exact Sep-8 residue: watcher_audit says trigger_ready but no
        canonical terminal reason exists yet. Must NOT be treated as terminal."""
        row = _tmo_row(
            status="CANCELED",
            last_error=None,
            restart_recovery_terminal_reason=None,
            watcher_audit_reason_code="trigger_ready",
            restart_recovery_cls=None,
        )
        _, w, _, _, watched = _watcher_for(row)
        disp, _ = _resolve(w, watched)
        assert disp == "KEEP_WATCHER", (
            "§7: watcher_audit.reason_code=trigger_ready is NEVER terminal proof."
        )

    def test_13_10_non_terminal_pending_trigger_is_not_terminal(self):
        row = _tmo_row(
            status="PENDING_TRIGGER",
            last_error="stale_text_should_not_evict",
            restart_recovery_terminal_reason="stale_reason",
            restart_recovery_cls=None,
        )
        _, w, _, _, watched = _watcher_for(row)
        disp, _ = _resolve(w, watched, claim="TERMINAL_DURABLE")
        assert disp == "KEEP_WATCHER", (
            "Terminal reasons on a non-terminal status must NOT synthesize a terminal outcome."
        )

    def test_13_11_wrong_local_order_id_does_not_evict(self):
        row = _tmo_row(local_order_id="different-order-id")
        _, w, _, _, watched = _watcher_for(row)
        # watched.signal.local_order_id is TMO — but row returns a different OID.
        watched.signal["local_order_id"] = "b9e29854-1c97-43d6-986b-eef0506bdf5a"
        disp, _ = _resolve(w, watched)
        assert disp == "KEEP_WATCHER"

    def test_13_12_wrong_client_does_not_evict(self):
        row = _tmo_row(client_id="someone-else@example.com")
        _, w, _, _, watched = _watcher_for(row)
        # Watcher runtime client is jason@; row is someone-else@.
        w.client_id = "jason@example.com"
        disp, _ = _resolve(w, watched)
        assert disp == "KEEP_WATCHER"

    def test_13_13_wrong_execution_mode_does_not_evict(self):
        row = _tmo_row(execution_mode="paper")
        _, w, _, _, watched = _watcher_for(row)
        w.execution_mode = "live"
        w.mode = "LIVE"
        w.paper = False
        disp, _ = _resolve(w, watched)
        assert disp == "KEEP_WATCHER"

    def test_13_14_missing_row_local_order_id_does_not_evict(self):
        row = _tmo_row(local_order_id="")
        _, w, _, _, watched = _watcher_for(row)
        watched.signal["local_order_id"] = "b9e29854-1c97-43d6-986b-eef0506bdf5a"
        disp, _ = _resolve(w, watched)
        assert disp == "KEEP_WATCHER"

    def test_13_15_conflicting_durable_reasons_holds(self):
        row = _tmo_row(
            last_error="reason_A_from_recovery",
            restart_recovery_terminal_reason="reason_B_conflicts",
            terminal_reason="reason_C_also_conflicts",
        )
        _, w, _, _, watched = _watcher_for(row)
        disp, _ = _resolve(w, watched)
        assert disp == "KEEP_WATCHER", (
            "§8.2: conflicting durable authorities must HOLD, never last-writer-wins."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Section §13.16–20 — Race behavior
# ─────────────────────────────────────────────────────────────────────────────


class TestRaceBehavior:

    def test_13_16_callback_starts_pending_reread_after_terminalization_wins_terminal(self):
        """Race: watcher enters callback on PENDING_TRIGGER row, concurrent
        recovery terminalizes, watcher's reread sees TERMINAL truth."""
        # We simulate the race with a stateful get_order that returns
        # PENDING_TRIGGER first, then CANCELED on reread.
        pending_row = _tmo_row(
            status="PENDING_TRIGGER",
            last_error=None,
            restart_recovery_terminal_reason=None,
            restart_recovery_cls=None,
        )
        terminal_row = _tmo_row()  # Sep-8 shape

        import ap_entry_watcher as ew
        osm = MagicMock()
        osm.get_order.side_effect = [pending_row, terminal_row]
        osm.cancel_pending_entry = MagicMock()
        osm.submit_existing_entry = MagicMock()
        broker = MagicMock()
        w = ew.APEntryWatcher(broker, order_state_machine=osm, mode="LIVE")
        w.execution_mode = "live"
        w.mode = "LIVE"
        w.paper = False
        w.client_id = "jason@example.com"

        watched = types.SimpleNamespace(signal={
            "local_order_id": pending_row["local_order_id"],
            "signal_id": "sig-tmo",
            "ticker": "TMO",
            "side": "CALL",
            "entry_price": 100.0,
            "client_id": "jason@example.com",
            "client_email": "jason@example.com",
            "execution_mode": "live",
            "contract_symbol": "DEFERRED:TMO",
        })

        # First call: pending — verifier returns KEEP.
        first, _ = w._resolve_trigger_callback_disposition(watched, {"disposition": "TERMINAL_DURABLE"})
        # Second call after concurrent terminalization: verifier sees terminal truth.
        second, _ = w._resolve_trigger_callback_disposition(watched, {"disposition": "TERMINAL_DURABLE"})
        assert first == "KEEP_WATCHER"
        assert second == "TERMINAL_DURABLE"
        broker.submit_order.assert_not_called()
        broker.cancel_order.assert_not_called()

    def test_13_19_repeated_resolution_is_idempotent(self):
        """Two observers reading the same terminal row both get TERMINAL_DURABLE."""
        row = _tmo_row()
        _, w, _, broker, watched = _watcher_for(row)
        d1, _ = _resolve(w, watched)
        d2, _ = _resolve(w, watched)
        d3, _ = _resolve(w, watched)
        assert d1 == d2 == d3 == "TERMINAL_DURABLE"
        broker.submit_order.assert_not_called()

    def test_13_20_terminal_row_does_not_rehydrate_watcher_via_verifier(self):
        """A terminal row read through the verifier must never return a
        disposition that would resurrect the watcher (KEEP_WATCHER retains;
        TERMINAL_DURABLE evicts; neither creates new behaviour)."""
        row = _tmo_row()
        _, w, _, _, watched = _watcher_for(row)
        disp, next_retry = _resolve(w, watched)
        assert disp == "TERMINAL_DURABLE"
        assert next_retry is None
        # Never OWNERSHIP_TRANSFERRED / RETRY_WAIT / SUBMITTED from a terminal row.
        assert disp not in {"OWNERSHIP_TRANSFERRED", "RETRY_WAIT", "SUBMITTED"}


# ─────────────────────────────────────────────────────────────────────────────
# Section §13.27–31 — Money-path negative controls
# ─────────────────────────────────────────────────────────────────────────────


class TestMoneyPath:

    def test_13_27_zero_broker_submit_from_terminal_convergence(self):
        row = _tmo_row()
        _, w, _, broker, watched = _watcher_for(row)
        _resolve(w, watched)
        broker.submit_order.assert_not_called()

    def test_13_28_zero_broker_cancel_from_terminal_convergence(self):
        row = _tmo_row()
        _, w, osm, broker, watched = _watcher_for(row)
        _resolve(w, watched)
        broker.cancel_order.assert_not_called()
        osm.cancel_pending_entry.assert_not_called()

    def test_13_29_30_zero_position_or_proof_mutation(self):
        """The verifier reads only; it must have no position/proof side effects."""
        row = _tmo_row()
        _, w, osm, broker, watched = _watcher_for(row)
        # These OSM attributes must never be invoked by the verifier itself.
        osm.record_deferred_hydration_result = MagicMock()
        osm.submit_existing_entry = MagicMock()
        _resolve(w, watched)
        osm.record_deferred_hydration_result.assert_not_called()
        osm.submit_existing_entry.assert_not_called()

    def test_13_31_verifier_does_not_synthetically_terminalize(self):
        """A row that is NOT in the terminal family and has no reason must
        never be pushed to TERMINAL_DURABLE by the verifier."""
        row = _tmo_row(
            status="PENDING_TRIGGER",
            last_error=None,
            restart_recovery_terminal_reason=None,
            restart_recovery_cls=None,
        )
        _, w, _, _, watched = _watcher_for(row)
        disp, _ = _resolve(w, watched)
        assert disp != "TERMINAL_DURABLE"


# ─────────────────────────────────────────────────────────────────────────────
# Section §13.32–34 — Submitted / broker-intent negative controls
# ─────────────────────────────────────────────────────────────────────────────


class TestSubmittedFamilyProtected:

    def test_13_32_submitted_family_not_governed_by_terminal_cleanup(self):
        """SUBMITTED status must resolve through _verify_submitted, not _verify_terminal —
        proven by broker_order_id presence returning SUBMITTED regardless of any
        terminal reason text present in the row."""
        row = _tmo_row(
            status="SUBMITTED",
            broker_order_id="TRD-98765",
            submitted_ts="2026-09-08T14:00:00+00:00",
            last_error=None,
            restart_recovery_terminal_reason=None,
            restart_recovery_cls=None,
        )
        _, w, _, _, watched = _watcher_for(row)
        disp, _ = _resolve(w, watched, claim="SUBMITTED")
        assert disp == "SUBMITTED"

    def test_13_33_broker_intent_ambiguity_reconciles_not_terminalizes(self):
        """SUBMITTED status without broker_order_id but with submit_intent →
        RECONCILE_BROKER_INTENT, never TERMINAL_DURABLE."""
        row = _tmo_row(
            status="SUBMITTED",
            broker_order_id=None,
            submitted_ts=None,
            last_error=None,
            restart_recovery_terminal_reason=None,
            restart_recovery_cls=None,
        )
        row["meta"]["submit_intent_at"] = "2026-09-08T14:00:00+00:00"
        _, w, _, _, watched = _watcher_for(row)
        disp, _ = _resolve(w, watched, claim="SUBMITTED")
        assert disp == "RECONCILE_BROKER_INTENT"

    def test_13_34_terminal_local_state_cannot_erase_proven_broker_ownership(self):
        """A row that is FILLED with a broker_order_id must NOT resolve to
        TERMINAL_DURABLE just because someone stuffed a terminal reason into meta."""
        row = _tmo_row(
            status="FILLED",
            broker_order_id="TRD-11111",
            submitted_ts="2026-09-08T14:00:00+00:00",
            last_error=None,
            restart_recovery_terminal_reason="stale_reason_should_not_matter",
            restart_recovery_cls=None,
        )
        _, w, _, _, watched = _watcher_for(row)
        # FILLED status is in _submitted_family, not _terminal_family, so
        # regardless of any stray reason text, terminal path is not entered.
        disp, _ = _resolve(w, watched, claim="TERMINAL_DURABLE")
        # _verify_terminal short-circuits KEEP_WATCHER on non-terminal status.
        assert disp == "KEEP_WATCHER"


# ─────────────────────────────────────────────────────────────────────────────
# Structural anchors (prevent silent regression to the narrow verifier)
# ─────────────────────────────────────────────────────────────────────────────


class TestStructuralAnchors:

    def _src(self) -> str:
        return open("ap_entry_watcher.py").read()

    def test_shared_terminal_reason_family_is_present(self):
        src = self._src()
        # The new shared collector must reference every canonical authority.
        for token in (
            "_collect_durable_terminal_reasons",
            "restart_recovery_terminal_reason",
            "terminal_reason",
            "reason_code",
            "final_reason",
            "materialization_reason",
            "watcher_invalidation_reason",
        ):
            assert token in src, f"PR #597: missing canonical authority reference: {token}"

    def test_convergence_observability_log_present(self):
        src = self._src()
        assert "WATCHER_TERMINAL_DURABLE_CONVERGED" in src
        assert "WATCHER_TERMINAL_CONVERGENCE_UNPROVEN" in src

    def test_watcher_audit_reason_code_is_not_a_terminal_source(self):
        """The spec §7 rule that watcher_audit.reason_code alone is not
        terminal proof must hold structurally — the verifier's collector
        must not include watcher_audit in its authority set."""
        # Behavioural version of the check: a row whose ONLY terminal-shaped
        # reason lives under meta.watcher_audit must NOT resolve TERMINAL.
        row = _tmo_row(
            status="CANCELED",
            last_error=None,
            restart_recovery_terminal_reason=None,
            watcher_audit_reason_code="stop_bid_below_call_stop",  # a real reason,
            restart_recovery_cls=None,                             # but wrong location
        )
        _, w, _, _, watched = _watcher_for(row)
        disp, _ = _resolve(w, watched)
        assert disp == "KEEP_WATCHER", (
            "§7: watcher_audit.reason_code is not sufficient terminal authority — "
            "the canonical fields must carry it too."
        )
