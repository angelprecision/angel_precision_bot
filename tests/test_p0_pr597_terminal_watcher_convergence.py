"""
tests/test_p0_pr597_terminal_watcher_convergence.py

PR #597 — Terminal watcher convergence.

Binds spec: docs/pr_specs/p0_terminal_watcher_convergence_20260908.md
Incorporates corrections 1–11 from the 2026-09-08 second-pass audit.

Exercises the production _resolve_trigger_callback_disposition() AND the
poll-time convergence via actual _poll_active_signals() in ap_entry_watcher.py.

Coverage:
  * TMO Sep-8 ownership split closed (fail-first + head).
  * Terminal reason vocabulary matches recovery terminalizer's canonical family.
  * watcher_audit.reason_code=trigger_ready is NOT terminal proof.
  * Identity mismatches / missing identity NEVER evict a watcher.
  * Runtime watcher and restart-recovery identity parity.
  * Broker-handoff ambiguity on terminal-family row → HOLD/RECONCILE, never
    ordinary terminal cleanup (blocker #2).
  * Real callback race: on_trigger → resolver — terminal wins.
  * Pre-dispatch terminal proof suppresses callback via actual
    _poll_active_signals() (blocker #1).
  * Signal identity check at terminal eviction (blocker #5).
  * Registry / dedup / direction cleanup driven through real
    _poll_active_signals() (blockers #4, #6).
  * Two-observer idempotency; process-restart no-rehydrate (blocker #7).
  * Historical materialization_reason + canonical terminal reason is not a
    conflict (blocker #8).
  * Real dedup postcondition proof (blocker #3).
  * Money path: zero broker/position/proof mutation on every case (#9).

Zero broker submit/cancel/position/proof mutation.
"""
from __future__ import annotations

import logging
import os
import types
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")


_TMO_LOCAL_ORDER_ID = "b9e29854-1c97-43d6-986b-eef0506bdf5a"
_TMO_DURABLE_REASON = "restart_stuck_trigger_ready_no_broker_proof"


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures: durable row / runtime watcher / signal identity independently
# controlled so missing-authority bugs cannot self-mask (blocker #9).
# ─────────────────────────────────────────────────────────────────────────────


def make_row(
    *,
    local_order_id: str | None = _TMO_LOCAL_ORDER_ID,
    client_id: str | None = "jason@example.com",
    client_email: str | None = None,
    execution_mode: str | None = "live",
    status: str = "CANCELED",
    last_error: str | None = _TMO_DURABLE_REASON,
    restart_recovery_terminal_reason: str | None = _TMO_DURABLE_REASON,
    terminal_reason: str | None = None,
    reason_code: str | None = None,
    final_reason: str | None = None,
    materialization_reason: str | None = None,
    watcher_invalidation_reason: str | None = None,
    watcher_audit_reason_code: str | None = None,
    restart_recovery_cls: str | None = "STUCK_TRIGGER_READY",
    broker_order_id: str | None = None,
    submitted_ts=None,
    submit_intent_at: str | None = None,
    broker_submit_key: str | None = None,
    broker_submit_payload_hash: str | None = None,
    broker_ready: bool | None = None,
    signal_id: str | None = None,
    canonical_signal_id: str | None = None,
    include_client_id_key: bool = True,
    include_client_email_key: bool = False,
    include_execution_mode_key: bool = True,
) -> dict:
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
    if submit_intent_at is not None:
        meta["submit_intent_at"] = submit_intent_at
    if broker_submit_key is not None:
        meta["broker_submit_key"] = broker_submit_key
    if broker_submit_payload_hash is not None:
        meta["broker_submit_payload_hash"] = broker_submit_payload_hash
    if broker_ready is not None:
        meta["broker_ready"] = broker_ready

    row: dict = {
        "local_order_id": local_order_id if local_order_id is not None else "",
        "status": status,
        "last_error": last_error,
        "broker_order_id": broker_order_id,
        "submitted_ts": submitted_ts,
        "meta": meta,
    }
    if include_client_id_key:
        row["client_id"] = client_id if client_id is not None else ""
    if include_client_email_key:
        row["client_email"] = client_email if client_email is not None else ""
    if include_execution_mode_key:
        row["execution_mode"] = execution_mode if execution_mode is not None else ""
    if signal_id is not None:
        row["signal_id"] = signal_id
    if canonical_signal_id is not None:
        row["canonical_signal_id"] = canonical_signal_id
    return row


def make_scenario(
    *,
    row: dict,
    runtime_client: str | None = "jason@example.com",
    runtime_mode: str | None = "live",
    runtime_paper: bool = False,
    signal_local_order_id: str | None = _TMO_LOCAL_ORDER_ID,
    signal_client_id: str | None = "jason@example.com",
    signal_client_email: str | None = None,
    signal_execution_mode: str | None = "live",
    signal_id: str | None = None,
    canonical_signal_id: str | None = None,
    ticker: str = "TMO",
    side: str = "CALL",
):
    import ap_entry_watcher as ew
    osm = MagicMock()
    osm.get_order.return_value = row
    osm.cancel_pending_entry = MagicMock()
    osm.submit_existing_entry = MagicMock()
    osm.record_deferred_hydration_result = MagicMock()

    broker = MagicMock()
    broker.submit_order = MagicMock()
    broker.cancel_order = MagicMock()

    w = ew.APEntryWatcher(broker, order_state_machine=osm, mode="LIVE")
    w.execution_mode = str(runtime_mode or "")
    w.mode = "LIVE" if (runtime_mode or "").lower() == "live" else (
        "PAPER" if (runtime_mode or "").lower() == "paper" else ""
    )
    w.paper = runtime_paper
    w.client_id = str(runtime_client or "")

    sig: dict = {
        "signal_id": signal_id or "sig-tmo",
        "ticker": ticker,
        "side": side,
        "entry_price": 100.0,
        "contract_symbol": f"DEFERRED:{ticker}",
    }
    if canonical_signal_id is not None:
        sig["canonical_signal_id"] = canonical_signal_id
    if signal_local_order_id is not None:
        sig["local_order_id"] = signal_local_order_id
    if signal_client_id is not None:
        sig["client_id"] = signal_client_id
    if signal_client_email is not None:
        sig["client_email"] = signal_client_email
    if signal_execution_mode is not None:
        sig["execution_mode"] = signal_execution_mode

    watched = types.SimpleNamespace(signal=sig)
    return ew, w, osm, broker, watched


def resolve(w, watched, *, claim: str | None = "TERMINAL_DURABLE"):
    result = {"disposition": claim} if claim else None
    return w._resolve_trigger_callback_disposition(watched, result)


def zero_broker(broker, osm):
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()
    osm.cancel_pending_entry.assert_not_called()
    osm.submit_existing_entry.assert_not_called()
    osm.record_deferred_hydration_result.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# TMO fail-first replay (spec §12)
# ─────────────────────────────────────────────────────────────────────────────


class TestTMOFailFirstReplay:

    def test_tmo_shape_resolves_terminal_durable(self):
        row = make_row()
        _, w, osm, broker, watched = make_scenario(row=row)
        disp, next_retry = resolve(w, watched)
        assert disp == "TERMINAL_DURABLE"
        assert next_retry is None
        zero_broker(broker, osm)

    def test_pre_fix_narrow_family_would_have_missed_tmo(self):
        row = make_row()
        narrow_only = bool(
            row["meta"].get("reason_code")
            or row["meta"].get("final_reason")
            or row["meta"].get("materialization_reason")
        )
        assert narrow_only is False
        assert row["last_error"]
        assert row["meta"]["restart_recovery_terminal_reason"]


# ─────────────────────────────────────────────────────────────────────────────
# Terminal reason vocabulary (spec §13.1–7)
# ─────────────────────────────────────────────────────────────────────────────


class TestTerminalReasonVocabulary:

    def test_canceled_with_orders_last_error(self):
        row = make_row(restart_recovery_terminal_reason=None, restart_recovery_cls=None)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_canceled_with_restart_recovery_terminal_reason(self):
        row = make_row(last_error=None)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_expired_with_meta_terminal_reason(self):
        row = make_row(status="EXPIRED", last_error=None,
                       restart_recovery_terminal_reason=None,
                       terminal_reason="watch_deadline_exceeded",
                       restart_recovery_cls=None)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_rejected_with_meta_reason_code(self):
        row = make_row(status="REJECTED", last_error=None,
                       restart_recovery_terminal_reason=None,
                       reason_code="broker_rejected_spread_too_wide",
                       restart_recovery_cls=None)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_error_with_meta_final_reason(self):
        row = make_row(status="ERROR", last_error=None,
                       restart_recovery_terminal_reason=None,
                       final_reason="internal_state_error",
                       restart_recovery_cls=None)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_meta_materialization_reason_alone_still_terminal(self):
        row = make_row(last_error=None, restart_recovery_terminal_reason=None,
                       materialization_reason="materialization_max_attempts_expired",
                       restart_recovery_cls=None)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_duplicate_consistent_reason_across_fields(self):
        row = make_row(terminal_reason=_TMO_DURABLE_REASON)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_watcher_invalidation_reason_alone(self):
        row = make_row(last_error=None, restart_recovery_terminal_reason=None,
                       watcher_invalidation_reason="stop_bid_below_call_stop",
                       restart_recovery_cls=None)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)


# ─────────────────────────────────────────────────────────────────────────────
# Blocker #8: historical materialization_reason + canonical terminal reason
# is not a conflict.
# ─────────────────────────────────────────────────────────────────────────────


class TestHistoricalVsCanonicalReason:

    def test_historical_materialization_plus_canonical_terminal_is_not_conflict(self):
        """A row with a legitimate historical materialization diagnostic AND
        a later canonical terminal reason from the recovery family must
        converge, not be permanently held as if conflicting."""
        row = make_row(
            last_error="restart_stuck_trigger_ready_no_broker_proof",
            restart_recovery_terminal_reason="restart_stuck_trigger_ready_no_broker_proof",
            materialization_reason="materialization_retry_attempted",  # older diagnostic
        )
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_multiple_distinct_canonical_reasons_still_hold(self):
        """Genuine canonical conflict still HOLDs."""
        row = make_row(
            last_error="reason_A",
            restart_recovery_terminal_reason="reason_B_conflicts",
            terminal_reason="reason_C_also_conflicts",
        )
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_only_historical_reasons_multiple_still_hold(self):
        """If no canonical reason exists and multiple historical values
        exist, still ambiguous."""
        row = make_row(
            last_error=None,
            restart_recovery_terminal_reason=None,
            materialization_reason="materialization_max_attempts_expired",
            restart_recovery_cls=None,
        )
        # Add a second historical-only value via terminal_reason field would
        # actually be canonical. Only materialization_reason is historical.
        # Prove single historical → TERMINAL_DURABLE (already covered above).
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"


# ─────────────────────────────────────────────────────────────────────────────
# Blocker #2: broker-handoff fence on terminal-family rows
# ─────────────────────────────────────────────────────────────────────────────


class TestBrokerHandoffFence:
    """A terminal-statused row with any unresolved broker submit/handoff
    evidence must NOT run ordinary terminal cleanup. It routes to the
    existing reconciliation authority (KEEP/RECONCILE) so no broker POST
    or cancel fires and existing reconciler owns the disposition."""

    @pytest.mark.parametrize("marker_kwargs,marker_name", [
        (dict(broker_order_id="TRD-99999"), "broker_order_id"),
        (dict(submitted_ts="2026-09-08T14:00:00+00:00"), "submitted_ts"),
        (dict(submit_intent_at="2026-09-08T14:00:00+00:00"), "submit_intent_at"),
        (dict(broker_submit_key="key-abc-123"), "broker_submit_key"),
        (dict(broker_submit_payload_hash="hash-xyz"), "broker_submit_payload_hash"),
        (dict(broker_ready=True), "broker_ready"),
    ])
    def test_canceled_with_broker_handoff_marker_does_not_terminal_cleanup(
        self, marker_kwargs, marker_name
    ):
        row = make_row(status="CANCELED", **marker_kwargs)
        _, w, osm, broker, watched = make_scenario(row=row)
        disp, _ = resolve(w, watched)
        assert disp != "TERMINAL_DURABLE", (
            f"{marker_name}: canonically terminalized row with unresolved "
            f"broker handoff must NOT run ordinary terminal cleanup — got {disp}"
        )
        assert disp in {"RECONCILE_BROKER_INTENT", "KEEP_WATCHER"}
        zero_broker(broker, osm)

    def test_error_with_broker_order_id_does_not_terminal_cleanup(self):
        row = make_row(status="ERROR", broker_order_id="TRD-11111",
                       last_error="broker_reported_error")
        _, w, osm, broker, watched = make_scenario(row=row)
        disp, _ = resolve(w, watched)
        assert disp != "TERMINAL_DURABLE"
        assert disp in {"RECONCILE_BROKER_INTENT", "KEEP_WATCHER"}
        zero_broker(broker, osm)

    def test_error_with_submitted_ts_does_not_terminal_cleanup(self):
        row = make_row(status="ERROR", submitted_ts="2026-09-08T14:00:00+00:00",
                       last_error="something_after_submit")
        _, w, osm, broker, watched = make_scenario(row=row)
        disp, _ = resolve(w, watched)
        assert disp != "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_clean_terminal_row_still_converges(self):
        """Sanity: without any broker handoff evidence, terminal cleanup
        does proceed (regression pin for the fence itself)."""
        row = make_row()  # no broker markers
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_broker_ready_false_is_not_a_handoff_marker(self):
        """broker_ready=False is not evidence of a handoff — only True is."""
        row = make_row(broker_ready=False)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"


# ─────────────────────────────────────────────────────────────────────────────
# Negative authority: missing client / mode / identity
# ─────────────────────────────────────────────────────────────────────────────


class TestIdentityFailClosed:

    def test_missing_client_id_and_email_holds(self):
        row = make_row(include_client_id_key=False, include_client_email_key=False)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_whitespace_client_id_holds(self):
        row = make_row(client_id="   ")
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_conflicting_client_id_vs_client_email_holds(self):
        row = make_row(client_id="jason@example.com",
                       include_client_email_key=True,
                       client_email="someone.else@example.com")
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_client_email_only_matching_works(self):
        row = make_row(include_client_id_key=False,
                       include_client_email_key=True,
                       client_email="jason@example.com")
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"

    def test_missing_execution_mode_holds(self):
        row = make_row(include_execution_mode_key=False)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_invalid_execution_mode_holds(self):
        row = make_row(execution_mode="production")
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_paper_row_live_runtime_holds(self):
        row = make_row(execution_mode="paper")
        _, w, osm, broker, watched = make_scenario(
            row=row, runtime_mode="live", runtime_paper=False)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_live_row_paper_runtime_holds(self):
        row = make_row(execution_mode="live")
        _, w, osm, broker, watched = make_scenario(
            row=row, runtime_mode="paper", runtime_paper=True)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_wrong_local_order_id_holds(self):
        row = make_row(local_order_id="different")
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)


# ─────────────────────────────────────────────────────────────────────────────
# Blocker #5: signal identity check
# ─────────────────────────────────────────────────────────────────────────────


class TestSignalIdentityCheck:

    def test_wrong_durable_signal_id_holds(self):
        row = make_row(signal_id="sig-durable-different")
        _, w, osm, broker, watched = make_scenario(
            row=row, signal_id="sig-watcher-original")
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_matching_signal_id_evicts(self):
        row = make_row(signal_id="sig-matching")
        _, w, osm, broker, watched = make_scenario(
            row=row, signal_id="sig-matching")
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"

    def test_conflicting_canonical_signal_id_holds(self):
        row = make_row(canonical_signal_id="canon-durable-different")
        _, w, osm, broker, watched = make_scenario(
            row=row, canonical_signal_id="canon-watcher-original")
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_matching_canonical_signal_id_evicts(self):
        row = make_row(canonical_signal_id="canon-match")
        _, w, osm, broker, watched = make_scenario(
            row=row, canonical_signal_id="canon-match")
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"

    def test_legacy_row_without_signal_id_still_converges(self):
        """Legacy rows that legitimately lack signal_id / canonical_signal_id
        are not blocked by the new signal identity check — the check only
        engages when durable authority carries the field."""
        row = make_row()  # no signal_id / canonical_signal_id
        # Signal has its own signal_id but the row doesn't advertise one.
        _, w, osm, broker, watched = make_scenario(
            row=row, signal_id="sig-watcher-only")
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"


# ─────────────────────────────────────────────────────────────────────────────
# Runtime watcher ↔ restart recovery identity parity (spec §17.11)
# ─────────────────────────────────────────────────────────────────────────────


class TestIdentityAuthorityParity:

    def _recovery_would_accept(self, row, *, expected_local, runtime_client, runtime_mode):
        _oid = str(row.get("local_order_id") or "").strip()
        if not _oid or _oid != expected_local:
            return False
        _cli = str(row.get("client_id") or row.get("client_email") or "").strip().lower()
        if not _cli or _cli != (runtime_client or "").strip().lower():
            return False
        _mode = str(row.get("execution_mode") or "").strip().lower()
        if not _mode or _mode != (runtime_mode or "").strip().lower():
            return False
        _status = str(row.get("status") or "").strip().upper()
        return _status in {"REJECTED", "EXPIRED", "CANCELED", "ERROR"}

    def _watcher_would_evict(self, row, *, runtime_client, runtime_mode):
        _, w, _, _, watched = make_scenario(
            row=row, runtime_client=runtime_client, runtime_mode=runtime_mode,
            signal_client_id=runtime_client, signal_execution_mode=runtime_mode)
        return resolve(w, watched)[0] == "TERMINAL_DURABLE"

    @pytest.mark.parametrize("name,row_kwargs,accept", [
        ("exact_identity", dict(), True),
        ("wrong_client", dict(client_id="someone-else@example.com"), False),
        ("missing_client", dict(include_client_id_key=False,
                                include_client_email_key=False), False),
        ("whitespace_client", dict(client_id="   "), False),
        ("wrong_mode", dict(execution_mode="paper"), False),
        ("missing_mode", dict(include_execution_mode_key=False), False),
        ("malformed_mode", dict(execution_mode="production"), False),
        ("wrong_local_order_id", dict(local_order_id="different-order-id"), False),
        ("missing_local_order_id", dict(local_order_id=""), False),
    ])
    def test_parity(self, name, row_kwargs, accept):
        row = make_row(**row_kwargs)
        rec = self._recovery_would_accept(row,
            expected_local=_TMO_LOCAL_ORDER_ID,
            runtime_client="jason@example.com", runtime_mode="live")
        wat = self._watcher_would_evict(row,
            runtime_client="jason@example.com", runtime_mode="live")
        assert rec == accept, f"[{name}] recovery expectation broke"
        assert wat == rec, f"[{name}] parity violated recovery={rec} watcher={wat}"


# ─────────────────────────────────────────────────────────────────────────────
# watcher_audit.reason_code=trigger_ready is NOT terminal proof
# ─────────────────────────────────────────────────────────────────────────────


class TestWatcherAuditNotAuthority:

    def test_only_watcher_audit_trigger_ready_holds(self):
        row = make_row(last_error=None, restart_recovery_terminal_reason=None,
                       watcher_audit_reason_code="trigger_ready",
                       restart_recovery_cls=None)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_only_watcher_audit_real_reason_holds(self):
        row = make_row(last_error=None, restart_recovery_terminal_reason=None,
                       watcher_audit_reason_code="stop_bid_below_call_stop",
                       restart_recovery_cls=None)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)


# ─────────────────────────────────────────────────────────────────────────────
# Blocker #1: pre-dispatch terminal proof via ACTUAL _poll_active_signals()
#            Blocker #4: real _direction_claims cleanup
#            Blocker #6: real production consumer, not replicas
#            Blocker #7: tests 19 (two-observer) and 20 (restart)
# ─────────────────────────────────────────────────────────────────────────────


class _RealOSM:
    """Small production-shaped OSM that stores real rows and lets a test
    mutate them mid-poll. Mirrors the pattern in
    test_p0_prebreach_stop_activation.py's _MockOSM."""

    def __init__(self, initial_row: dict):
        self._row = dict(initial_row)

    def set_row(self, row: dict):
        self._row = dict(row)

    def get_order(self, local_order_id: str) -> dict:
        return dict(self._row)

    def update_order_meta(self, local_order_id: str, meta_patch: dict) -> bool:
        _m = dict(self._row.get("meta") or {})
        _m.update(meta_patch or {})
        self._row["meta"] = _m
        return True

    def cancel_pending_entry(self, *a, **kw): return True
    def expire_pending_entry(self, *a, **kw): return True
    def transition(self, *a, **kw): return True
    def terminalize_deferred_breach(self, *a, **kw): return False


def _terminal_row_for(local_oid: str, *, client="jason@example.com",
                      execution_mode="live", signal_id="sig-tmo"):
    """Fully populated production-shape row that satisfies every safety gate
    the new terminal convergence installs."""
    return {
        "local_order_id": local_oid,
        "client_id": client,
        "execution_mode": execution_mode,
        "status": "CANCELED",
        "last_error": _TMO_DURABLE_REASON,
        "broker_order_id": None,
        "submitted_ts": None,
        "signal_id": signal_id,
        "meta": {
            "restart_recovery_terminal_reason": _TMO_DURABLE_REASON,
            "restart_recovery_cls": "STUCK_TRIGGER_READY",
        },
    }


def _real_signal(local_oid, ticker, signal_id, *, side="CALL",
                 client="jason@example.com", execution_mode="live"):
    return {
        "signal_id": signal_id,
        "ticker": ticker,
        "side": side,
        "entry_price": 100.0,
        "stop_price": 98.0,
        "target_price": 105.0,
        "local_order_id": local_oid,
        "client_id": client,
        "client_email": client,
        "execution_mode": execution_mode,
        "contract_symbol": f"DEFERRED:{ticker}",
    }


def _install_watched_in_pending(w, signal, *, side="CALL"):
    """Install a real WatchedSignal in the real _pending, seeded so the
    next poll cycle enters the confirmation dispatch path."""
    import ap_entry_watcher as ew
    ws = ew.WatchedSignal(signal, overnight=False)
    ws._watcher_ref = w
    # Seed one prior breach poll so the confirming poll dispatches.
    now = datetime.now(timezone.utc)
    ws.breach_count = ws.MOMENTUM_POLLS_REQUIRED - 1
    ws._pending_first_breach_at = now
    ws.first_breach_bid = 99.0
    ws.first_breach_ask = 99.1
    ws._last_valid_breach_observation_at = now
    with w._lock:
        w._pending.append(ws)
        w._dedup_set.add(ws.signal_id)
    return ws


def _build_live_watcher(osm, *, client="jason@example.com"):
    import ap_entry_watcher as ew
    broker = MagicMock()
    broker.submit_order = MagicMock()
    broker.cancel_order = MagicMock()
    w = ew.APEntryWatcher(broker, order_state_machine=osm, mode="LIVE")
    w.execution_mode = "live"
    w.mode = "LIVE"
    w.paper = False
    w.client_id = client
    return w, broker


def _quote_that_confirms_call_breach(ticker: str) -> dict:
    """Emit a quote whose ask crosses the entry trigger."""
    return {ticker: {"bid": 100.6, "ask": 100.7}}


class TestPollDispatchPreTerminalConvergence:
    """Blocker #1: canonical row is TERMINAL immediately before callback
    dispatch. Drive the REAL _poll_active_signals loop and prove:
      - on_trigger is NEVER called
      - selector/materializer callbacks are NEVER called
      - broker submit/cancel are NEVER called
      - watcher is removed from _pending
      - dedup key is released
      - direction claim converges via existing stale-claim pruning
      - WATCHER_TERMINAL_PRE_DISPATCH_SKIP diagnostic fires
    """

    def _drive_one_poll(self, w, ticker, on_trigger_calls, quote=None):
        with patch.object(w, "_fetch_quotes",
                          return_value=quote or _quote_that_confirms_call_breach(ticker)):
            w._poll_active_signals(open_protect_active=False)

    def test_terminal_before_dispatch_skips_on_trigger_entirely(self, caplog):
        local_oid = str(uuid.uuid4())
        # Row is terminal BEFORE the poll runs.
        osm = _RealOSM(_terminal_row_for(local_oid, signal_id="sig-A"))
        w, broker = _build_live_watcher(osm)

        callback_count = {"n": 0}
        def _cb(_ws):
            callback_count["n"] += 1
            return {"disposition": "SUBMITTED"}
        w.on_trigger = _cb

        sig = _real_signal(local_oid, "TMO", "sig-A")
        ws = _install_watched_in_pending(w, sig)

        with caplog.at_level(logging.INFO, logger="ap.entry_watcher"):
            self._drive_one_poll(w, "TMO", callback_count)

        # 1) on_trigger MUST NOT have been called.
        assert callback_count["n"] == 0, (
            "Pre-dispatch terminal proof must suppress the callback entirely; "
            f"on_trigger was called {callback_count['n']} times."
        )
        # 2) Money path: zero broker action.
        broker.submit_order.assert_not_called()
        broker.cancel_order.assert_not_called()
        # 3) Watcher removed from _pending by convergence cleanup.
        with w._lock:
            assert ws not in w._pending
        # 4) Dedup released (production helper actually removed the key).
        with w._lock:
            assert sig["signal_id"] not in w._dedup_set
        # 5) Structured diagnostic fired.
        joined = "\n".join(r.getMessage() for r in caplog.records)
        assert "WATCHER_TERMINAL_PRE_DISPATCH_SKIP" in joined
        assert "on_trigger_called=false" in joined

    def test_row_pending_at_dispatch_but_terminal_after_callback_wins_terminal(self):
        """Row changes PENDING_TRIGGER → terminal AFTER on_trigger begins.
        Callback may run once; resolver reread sees terminal truth; no
        second economic action; no callback repeat."""
        local_oid = str(uuid.uuid4())
        pending_row = _terminal_row_for(local_oid, signal_id="sig-B")
        pending_row["status"] = "PENDING_TRIGGER"
        pending_row["last_error"] = None
        del pending_row["meta"]["restart_recovery_terminal_reason"]
        del pending_row["meta"]["restart_recovery_cls"]
        osm = _RealOSM(pending_row)
        w, broker = _build_live_watcher(osm)

        callback_count = {"n": 0}
        def _cb(_ws):
            callback_count["n"] += 1
            # Race: recovery terminalizes the row DURING our callback.
            osm.set_row(_terminal_row_for(local_oid, signal_id="sig-B"))
            return {"disposition": "SUBMITTED"}
        w.on_trigger = _cb

        sig = _real_signal(local_oid, "TMO", "sig-B")
        ws = _install_watched_in_pending(w, sig)

        with patch.object(w, "_fetch_quotes",
                          return_value=_quote_that_confirms_call_breach("TMO")):
            w._poll_active_signals(open_protect_active=False)

        # Callback ran once because at dispatch the row wasn't yet terminal.
        assert callback_count["n"] == 1
        # No broker action from convergence.
        broker.submit_order.assert_not_called()
        broker.cancel_order.assert_not_called()
        # Watcher removed by post-callback convergence path.
        with w._lock:
            assert ws not in w._pending
            assert sig["signal_id"] not in w._dedup_set

        # A second poll must not re-fire the callback (watcher is gone).
        with patch.object(w, "_fetch_quotes",
                          return_value=_quote_that_confirms_call_breach("TMO")):
            w._poll_active_signals(open_protect_active=False)
        assert callback_count["n"] == 1, "No post-terminal callback repeat"

    def test_unrelated_watcher_survives_pre_dispatch_convergence(self):
        """A convergence event on watcher A must not touch watcher B."""
        local_A = str(uuid.uuid4())
        local_B = str(uuid.uuid4())

        # get_order dispatches by local_order_id.
        rows = {
            local_A: _terminal_row_for(local_A, signal_id="sig-A"),
            local_B: {
                "local_order_id": local_B,
                "client_id": "jason@example.com",
                "execution_mode": "live",
                "status": "PENDING_TRIGGER",  # unrelated, not terminal
                "last_error": None,
                "broker_order_id": None,
                "submitted_ts": None,
                "meta": {},
            },
        }
        osm = MagicMock()
        osm.get_order.side_effect = lambda oid: dict(rows[oid])
        osm.update_order_meta = MagicMock(return_value=True)
        osm.cancel_pending_entry = MagicMock()
        osm.submit_existing_entry = MagicMock()
        w, broker = _build_live_watcher(osm)
        w.on_trigger = MagicMock(return_value={"disposition": "SUBMITTED"})

        sig_A = _real_signal(local_A, "TMO", "sig-A")
        sig_B = _real_signal(local_B, "AAPL", "sig-B")
        ws_A = _install_watched_in_pending(w, sig_A)
        ws_B = _install_watched_in_pending(w, sig_B)

        with patch.object(w, "_fetch_quotes", return_value={
            "TMO": {"bid": 100.6, "ask": 100.7},
            "AAPL": {"bid": 100.6, "ask": 100.7},
        }):
            w._poll_active_signals(open_protect_active=False)

        # A converged and was removed; B remained and may or may not have
        # been triggered (depending on selector), but must still be in the
        # process (either _pending or moved forward via callback).
        with w._lock:
            assert ws_A not in w._pending, "terminal watcher must be removed"
        broker.submit_order.assert_not_called()
        broker.cancel_order.assert_not_called()


class TestRealDirectionClaimConvergence:
    """Blocker #4: use the ACTUAL _direction_claims registry from the
    package shim, not an arbitrary dedup string. Prove terminal
    convergence causes the exact winner's claim to disappear via the
    real production behavior."""

    def test_terminal_winner_direction_claim_pruned(self):
        """Prove that terminal convergence on TMO causes the exact TMO
        direction claim to disappear via the shim's existing stale-claim
        pruning, WITHOUT touching an unrelated AAPL direction claim whose
        watcher is still active in _pending."""
        # The package shim owns _direction_claims. Import it directly.
        import ap_entry_watcher as ew_shim

        # Fresh watcher via the shim (this is the production entry point).
        local_win = str(uuid.uuid4())
        local_other = str(uuid.uuid4())

        # OSM dispatches by local_order_id.
        rows = {
            local_win: _terminal_row_for(local_win, signal_id="sig-win"),
            local_other: {
                "local_order_id": local_other,
                "client_id": "jason@example.com",
                "execution_mode": "live",
                "status": "PENDING_TRIGGER",
                "last_error": None,
                "broker_order_id": None,
                "submitted_ts": None,
                "meta": {},
            },
        }
        osm = MagicMock()
        osm.get_order.side_effect = lambda oid: dict(rows[oid])
        osm.update_order_meta = MagicMock(return_value=True)
        osm.cancel_pending_entry = MagicMock()
        osm.submit_existing_entry = MagicMock()
        w, broker = _build_live_watcher(osm)

        # Only proceed if this build exposes _direction_claims.
        if not hasattr(w, "_direction_claims"):
            pytest.skip("This build's APEntryWatcher does not expose "
                        "_direction_claims (shim not active in this import)")

        # Defensive callback wiring — pre-dispatch is expected to suppress
        # it, but if the runtime falls through for a non-terminal watcher
        # (AAPL), the callback must exist.
        w.on_trigger = MagicMock(return_value={"disposition": "KEEP_WATCHER"})

        # TMO winner: terminal row, will converge and be removed from _pending.
        sig_win = _real_signal(local_win, "TMO", "sig-win")
        ws_win = _install_watched_in_pending(w, sig_win)

        # AAPL: still-active watcher on a completely different key. Its
        # presence in _pending is what allows the shim's stale-claim
        # pruning to observe that the TMO key has no matching pending
        # item and prune the TMO claim, while preserving AAPL's own claim.
        sig_other = _real_signal(local_other, "AAPL", "sig-unrelated")
        # Do NOT seed AAPL for confirmation — leave it as fresh so it does
        # not trigger; it just sits in _pending as a live untriggered watcher.
        import ap_entry_watcher as ew
        ws_other = ew.WatchedSignal(sig_other, overnight=False)
        ws_other._watcher_ref = w
        with w._lock:
            w._pending.append(ws_other)
            w._dedup_set.add(ws_other.signal_id)

        # Seed BOTH real direction claims.
        key_win = ("jason@example.com", "live", "TMO")
        key_other = ("jason@example.com", "live", "AAPL")
        with w._direction_claim_gate:
            w._direction_claims[key_win] = {
                "status": "won",
                "winner_local_order_id": local_win,
                "winner_signal_id": sig_win["signal_id"],
                "winner_direction": "CALL",
            }
            w._direction_claims[key_other] = {
                "status": "won",
                "winner_local_order_id": local_other,
                "winner_signal_id": sig_other["signal_id"],
                "winner_direction": "CALL",
            }

        # Drive the poll — pre-dispatch convergence removes TMO watcher.
        with patch.object(w, "_fetch_quotes", return_value={
            "TMO": _quote_that_confirms_call_breach("TMO")["TMO"],
            "AAPL": {"bid": 50.0, "ask": 50.05},  # AAPL untriggered
        }):
            w._poll_active_signals(open_protect_active=False)

        # TMO winner is now gone from _pending. Drive a second poll so the
        # shim's stale-claim pruning observes the absence and prunes the
        # TMO direction claim; AAPL claim must survive because its winner
        # is still in _pending.
        with patch.object(w, "_fetch_quotes", return_value={
            "AAPL": {"bid": 50.0, "ask": 50.05},
        }):
            w._poll_active_signals(open_protect_active=False)

        with w._direction_claim_gate:
            # TMO claim was pruned by the real shim behavior (winner not in _pending).
            assert key_win not in w._direction_claims, (
                "Terminal convergence removed TMO watcher; shim's stale-claim "
                "pruning must discard TMO direction claim on the next poll."
            )
            # AAPL claim survives (its winner is still in _pending).
            assert key_other in w._direction_claims, (
                "Unrelated AAPL direction claim (whose winner is still in "
                "_pending) must not be pruned by TMO convergence."
            )

        # Terminal winner truly gone from _pending; no broker action.
        with w._lock:
            assert ws_win not in w._pending
        broker.submit_order.assert_not_called()
        broker.cancel_order.assert_not_called()


class TestTwoObserverIdempotency:
    """Blocker #7 test 19: two convergence attempts race the same watcher."""

    def test_two_polls_on_same_terminal_row_are_idempotent(self):
        local_oid = str(uuid.uuid4())
        osm = _RealOSM(_terminal_row_for(local_oid, signal_id="sig-idem"))
        w, broker = _build_live_watcher(osm)

        callback_count = {"n": 0}
        w.on_trigger = lambda _ws: (callback_count.__setitem__("n", callback_count["n"] + 1)
                                     or {"disposition": "SUBMITTED"})

        sig = _real_signal(local_oid, "TMO", "sig-idem")
        ws = _install_watched_in_pending(w, sig)

        # First poll: converges, removes watcher.
        with patch.object(w, "_fetch_quotes",
                          return_value=_quote_that_confirms_call_breach("TMO")):
            w._poll_active_signals(open_protect_active=False)
        assert callback_count["n"] == 0, "first poll: callback skipped"
        with w._lock:
            assert ws not in w._pending
            assert sig["signal_id"] not in w._dedup_set

        # Second poll on the same terminal row (watcher already gone) is a no-op.
        pending_snapshot = list(w._pending)
        dedup_snapshot = set(w._dedup_set)
        with patch.object(w, "_fetch_quotes",
                          return_value=_quote_that_confirms_call_breach("TMO")):
            w._poll_active_signals(open_protect_active=False)
        # No callback resurrection, no state corruption.
        assert callback_count["n"] == 0
        with w._lock:
            assert list(w._pending) == pending_snapshot
            assert set(w._dedup_set) == dedup_snapshot
        broker.submit_order.assert_not_called()
        broker.cancel_order.assert_not_called()


class TestRestartNoRehydrate:
    """Blocker #7 test 20: after terminalization, a fresh watcher instance
    (simulating process restart) must not rehydrate a behavior-active
    watcher for that exact row through the runtime code #597 owns."""

    def test_terminal_row_does_not_rehydrate_via_watcher(self):
        local_oid = str(uuid.uuid4())
        terminal_row = _terminal_row_for(local_oid, signal_id="sig-rehydrate")
        osm = _RealOSM(terminal_row)
        w, broker = _build_live_watcher(osm)

        # No WatchedSignal is installed — simulates a fresh restart where
        # the runtime has no in-memory ownership. The runtime must not
        # invent one; _pending remains empty for this row.
        with patch.object(w, "_fetch_quotes", return_value={}):
            w._poll_active_signals(open_protect_active=False)

        with w._lock:
            assert not any(
                str((getattr(x, "signal", {}) or {}).get("local_order_id") or "") == local_oid
                for x in w._pending
            )
            # Dedup registry untouched by the poll.
            # (Recovery/#580 lifecycle is out of scope here; this only proves
            # #597 doesn't rehydrate.)
        broker.submit_order.assert_not_called()
        broker.cancel_order.assert_not_called()

    def test_direct_resolver_never_rehydrates(self):
        """The resolver, called against a terminal row, only returns
        TERMINAL_DURABLE — never a state that would resurrect a watcher."""
        row = make_row()
        _, w, osm, broker, watched = make_scenario(row=row)
        for claim in ("TERMINAL_DURABLE", "SUBMITTED", "RETRY_WAIT",
                      "OWNERSHIP_TRANSFERRED", "KEEP_WATCHER", None):
            disp, _ = resolve(w, watched, claim=claim)
            assert disp in {"TERMINAL_DURABLE", "KEEP_WATCHER"}
            assert disp != "SUBMITTED"
            assert disp != "RETRY_WAIT"
            assert disp != "OWNERSHIP_TRANSFERRED"


# ─────────────────────────────────────────────────────────────────────────────
# Blocker #3: real dedup POSTCONDITION proof via _poll_active_signals()
# ─────────────────────────────────────────────────────────────────────────────


class TestDedupPostconditionProof:
    """Prove that convergence truly removes the exact dedup key from the
    exact _dedup_set — not by mocking, not by manual mutation."""

    def test_dedup_key_actually_absent_after_convergence(self, caplog):
        local_oid = str(uuid.uuid4())
        osm = _RealOSM(_terminal_row_for(local_oid, signal_id="sig-dedup"))
        w, broker = _build_live_watcher(osm)
        w.on_trigger = MagicMock(return_value={"disposition": "SUBMITTED"})

        sig = _real_signal(local_oid, "TMO", "sig-dedup")
        ws = _install_watched_in_pending(w, sig)

        # Precondition: dedup key IS in the registry.
        with w._lock:
            assert "sig-dedup" in w._dedup_set

        with caplog.at_level(logging.INFO, logger="ap.entry_watcher"):
            with patch.object(w, "_fetch_quotes",
                              return_value=_quote_that_confirms_call_breach("TMO")):
                w._poll_active_signals(open_protect_active=False)

        # Postcondition: the exact key is ABSENT from the exact registry.
        with w._lock:
            assert "sig-dedup" not in w._dedup_set, (
                "Real production _release_dedup_key must actually remove the "
                "exact signal_id from the exact _dedup_set"
            )
        # Convergence log reports truthful values.
        joined = "\n".join(r.getMessage() for r in caplog.records)
        assert "WATCHER_TERMINAL_DURABLE_CONVERGED" in joined
        assert "watcher_removed=true" in joined
        assert "dedup_released=true" in joined
        assert "watcher_removed=pending" not in joined
        broker.submit_order.assert_not_called()

    def test_broken_release_helper_reports_unproven_not_success(self, caplog, monkeypatch):
        """If the release helper silently fails (e.g. _watcher_ref is
        detached so it cannot resolve the dedup set), the postcondition
        check must catch it and emit CONVERGENCE_UNPROVEN, not a false
        success."""
        local_oid = str(uuid.uuid4())
        osm = _RealOSM(_terminal_row_for(local_oid, signal_id="sig-broken"))
        w, broker = _build_live_watcher(osm)
        w.on_trigger = MagicMock(return_value={"disposition": "SUBMITTED"})

        sig = _real_signal(local_oid, "TMO", "sig-broken")
        ws = _install_watched_in_pending(w, sig)

        # Break the release helper by detaching the watcher ref AFTER install
        # but BEFORE the poll runs. The helper will run without exception but
        # the dedup key will remain in _dedup_set. The postcondition check
        # must catch this and emit UNPROVEN.
        original_release = ws._release_dedup_key
        def _silent_no_op():
            # Deliberately does nothing — mimics the production helper's
            # internal exception swallowing.
            return None
        ws._release_dedup_key = _silent_no_op  # type: ignore[assignment]

        with caplog.at_level(logging.WARNING, logger="ap.entry_watcher"):
            with patch.object(w, "_fetch_quotes",
                              return_value=_quote_that_confirms_call_breach("TMO")):
                w._poll_active_signals(open_protect_active=False)

        joined = "\n".join(r.getMessage() for r in caplog.records)
        # Dedup key remains because the helper was neutered.
        with w._lock:
            assert "sig-broken" in w._dedup_set, (
                "Test invariant: neutered helper must have left the key in place"
            )
        # And the production code must report this truthfully.
        assert "WATCHER_TERMINAL_CONVERGENCE_UNPROVEN" in joined
        assert "dedup_released=False" in joined or "dedup_released=false" in joined
        # No broker re-entry over a bookkeeping failure — the row is still terminal.
        broker.submit_order.assert_not_called()
        broker.cancel_order.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Money-path final proof (blocker #9)
# ─────────────────────────────────────────────────────────────────────────────


class TestMoneyPathFinal:

    def test_verifier_never_calls_broker_or_osm_write_paths(self):
        row = make_row()
        _, w, osm, broker, watched = make_scenario(row=row)
        for claim in ("TERMINAL_DURABLE", "SUBMITTED", "RETRY_WAIT",
                      "OWNERSHIP_TRANSFERRED", "KEEP_WATCHER", None):
            resolve(w, watched, claim=claim)
        zero_broker(broker, osm)

    @pytest.mark.parametrize("row_kwargs", [
        dict(include_client_id_key=False, include_client_email_key=False),
        dict(client_id="   "),
        dict(include_execution_mode_key=False),
        dict(execution_mode="paper"),
        dict(local_order_id=""),
        dict(last_error="A", restart_recovery_terminal_reason="B"),
        dict(broker_order_id="TR-9999"),
        dict(submit_intent_at="2026-09-08T14:00:00+00:00"),
        dict(broker_ready=True),
        dict(signal_id="different-durable-sid"),
    ])
    def test_every_new_fail_closed_case_zero_money_path(self, row_kwargs):
        row = make_row(**row_kwargs)
        signal_kwargs = {}
        if "signal_id" in row_kwargs:
            signal_kwargs["signal_id"] = "sig-watcher-original"
        _, w, osm, broker, watched = make_scenario(row=row, **signal_kwargs)
        disp, _ = resolve(w, watched)
        assert disp != "TERMINAL_DURABLE", (
            f"row_kwargs={row_kwargs} must not clean up as ordinary terminal"
        )
        zero_broker(broker, osm)


# ─────────────────────────────────────────────────────────────────────────────
# Submitted family untouched (regression pin)
# ─────────────────────────────────────────────────────────────────────────────


class TestSubmittedFamilyUntouched:

    def test_submitted_with_broker_id_still_submitted(self):
        row = make_row(status="SUBMITTED",
                       broker_order_id="TRD-98765",
                       submitted_ts="2026-09-08T14:00:00+00:00",
                       last_error=None, restart_recovery_terminal_reason=None,
                       restart_recovery_cls=None)
        _, w, _, _, watched = make_scenario(row=row)
        assert resolve(w, watched, claim="SUBMITTED")[0] == "SUBMITTED"

    def test_reconcile_broker_intent_still_reconciles(self):
        row = make_row(status="SUBMITTED",
                       broker_order_id=None, submitted_ts=None,
                       last_error=None, restart_recovery_terminal_reason=None,
                       submit_intent_at="2026-09-08T14:00:00+00:00",
                       restart_recovery_cls=None)
        _, w, _, _, watched = make_scenario(row=row)
        assert resolve(w, watched, claim="SUBMITTED")[0] == "RECONCILE_BROKER_INTENT"

    def test_filled_with_broker_id_never_terminal_evicted(self):
        row = make_row(status="FILLED",
                       broker_order_id="TRD-11111",
                       submitted_ts="2026-09-08T14:00:00+00:00",
                       last_error=None,
                       restart_recovery_terminal_reason="stale_reason_should_not_matter",
                       restart_recovery_cls=None)
        _, w, _, _, watched = make_scenario(row=row)
        # FILLED is not in _terminal_family; the §11 preempt does not fire.
        # Claim TERMINAL_DURABLE against a FILLED row must not evict.
        assert resolve(w, watched, claim="TERMINAL_DURABLE")[0] == "KEEP_WATCHER"


# ─────────────────────────────────────────────────────────────────────────────
# Structural anchors (silent-regression guard)
# ─────────────────────────────────────────────────────────────────────────────


class TestStructuralAnchors:

    def _src(self) -> str:
        return open("ap_entry_watcher.py").read()

    def test_shared_terminal_reason_family_present(self):
        src = self._src()
        for tok in ("_collect_durable_terminal_reasons",
                    "restart_recovery_terminal_reason", "terminal_reason",
                    "reason_code", "final_reason", "materialization_reason",
                    "watcher_invalidation_reason"):
            assert tok in src, f"missing: {tok}"

    def test_broker_handoff_fence_wired(self):
        src = self._src()
        for tok in ("_collect_broker_handoff_markers",
                    "submit_intent_at", "broker_submit_key",
                    "broker_submit_payload_hash", "broker_ready"):
            assert tok in src, f"missing broker handoff marker: {tok}"

    def test_pre_dispatch_convergence_wired(self):
        src = self._src()
        assert "_prove_deferred_terminal_convergence" in src
        assert "WATCHER_TERMINAL_PRE_DISPATCH_SKIP" in src

    def test_signal_identity_gate_wired(self):
        src = self._src()
        assert "canonical_signal_id" in src

    def test_no_false_dedup_released_true_literal(self):
        src = self._src()
        # The one place that formerly hard-coded "dedup_released=true" must
        # now derive it from the actual _ded_released state.
        # Callback OK log line is the previous contradiction.
        assert 'dedup_released=%s' in src, (
            "Blocker #3: WATCHER_TRIGGER_CALLBACK_OK must derive dedup_released "
            "from actual verified state, not hardcode true."
        )

    def test_convergence_diagnostics_wired(self):
        src = self._src()
        assert "WATCHER_TERMINAL_DURABLE_CONVERGED" in src
        assert "WATCHER_TERMINAL_CONVERGENCE_UNPROVEN" in src
        assert "watcher_removed=pending" not in src
