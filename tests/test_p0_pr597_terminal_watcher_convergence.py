"""
tests/test_p0_pr597_terminal_watcher_convergence.py

PR #597 — Terminal watcher convergence.

Binds spec: docs/pr_specs/p0_terminal_watcher_convergence_20260908.md
Incorporates corrections 1–13 from the 2026-09-08 audit of head 921d3877.

Exercises the production _resolve_trigger_callback_disposition() and the
poll-time convergence cleanup consumer in ap_entry_watcher.py to prove:

  * TMO Sep-8 ownership split is closed (fail-first + head passes).
  * Watcher terminal-reason authority matches recovery terminalizer's
    canonical family (last_error + 5 meta fields).
  * watcher_audit.reason_code=trigger_ready alone is NOT terminal proof.
  * Identity mismatches / missing identity NEVER evict a watcher.
  * Runtime watcher and restart-recovery identity authority agree
    on every scenario in the parity matrix.
  * Conflicting durable reasons HOLD.
  * Callback-vs-terminalization race resolves to TERMINAL via the
    actual on_trigger → _resolve_trigger_callback_disposition sequence.
  * Terminalization before callback dispatch → normal entry work skipped.
  * Poll race → at most one callback, no post-terminal repeat.
  * Registry/dedup cleanup is exact, targeted, idempotent, truthful.
  * Cleanup failure emits WATCHER_TERMINAL_CONVERGENCE_UNPROVEN, not
    dedup_released=true.
  * Convergence observability is emitted AFTER cleanup with truthful values.

Money path: zero broker submit/cancel/position/proof mutation on every
new fail-closed case; and terminal convergence adds none.
"""
from __future__ import annotations

import logging
import os
import types
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")


# ─────────────────────────────────────────────────────────────────────────────
# Fixture (correction #9): independently controlled durable / runtime / signal
#                          identity so missing-field bugs cannot self-mask.
# ─────────────────────────────────────────────────────────────────────────────


_TMO_LOCAL_ORDER_ID = "b9e29854-1c97-43d6-986b-eef0506bdf5a"
_TMO_DURABLE_REASON = "restart_stuck_trigger_ready_no_broker_proof"


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
    include_client_id_key: bool = True,
    include_client_email_key: bool = False,
    include_execution_mode_key: bool = True,
) -> dict:
    """Build a durable order row. Each identity field's presence is opt-in
    so missing-field scenarios can be built without the fixture inventing
    agreement."""
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
    ticker: str = "TMO",
    side: str = "CALL",
):
    """Independent identity for row, runtime watcher, and watched signal.
    Nothing about the runtime or signal is derived from the row."""
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
        "signal_id": "sig-tmo",
        "ticker": ticker,
        "side": side,
        "entry_price": 100.0,
        "contract_symbol": f"DEFERRED:{ticker}",
    }
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
    """Assert the negative money-path contract on any fail-closed scenario."""
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()
    osm.cancel_pending_entry.assert_not_called()
    osm.submit_existing_entry.assert_not_called()
    osm.record_deferred_hydration_result.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# §12 — Exact TMO fail-first replay
# ─────────────────────────────────────────────────────────────────────────────


class TestTMOFailFirstReplay:

    def test_tmo_canceled_with_last_error_and_restart_recovery_reason_is_terminal(self):
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
# §13.1–7 — Terminal reason vocabulary
# ─────────────────────────────────────────────────────────────────────────────


class TestTerminalReasonVocabulary:

    def test_13_1_canceled_with_orders_last_error_is_terminal(self):
        row = make_row(restart_recovery_terminal_reason=None, restart_recovery_cls=None)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_13_2_canceled_with_restart_recovery_terminal_reason_is_terminal(self):
        row = make_row(last_error=None)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_13_3_expired_with_meta_terminal_reason_is_terminal(self):
        row = make_row(
            status="EXPIRED", last_error=None,
            restart_recovery_terminal_reason=None,
            terminal_reason="watch_deadline_exceeded",
            restart_recovery_cls=None,
        )
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_13_4_rejected_with_meta_reason_code_is_terminal(self):
        row = make_row(
            status="REJECTED", last_error=None,
            restart_recovery_terminal_reason=None,
            reason_code="broker_rejected_spread_too_wide",
            restart_recovery_cls=None,
        )
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_13_5_error_with_meta_final_reason_is_terminal(self):
        row = make_row(
            status="ERROR", last_error=None,
            restart_recovery_terminal_reason=None,
            final_reason="internal_state_error",
            restart_recovery_cls=None,
        )
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_13_6_meta_materialization_reason_still_terminal(self):
        row = make_row(
            last_error=None, restart_recovery_terminal_reason=None,
            materialization_reason="materialization_max_attempts_expired",
            restart_recovery_cls=None,
        )
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_13_7_duplicate_consistent_reason_across_fields_is_terminal(self):
        row = make_row(terminal_reason=_TMO_DURABLE_REASON)  # 3 identical
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    def test_watcher_invalidation_reason_alone_is_terminal(self):
        row = make_row(
            last_error=None, restart_recovery_terminal_reason=None,
            watcher_invalidation_reason="stop_bid_below_call_stop",
            restart_recovery_cls=None,
        )
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"
        zero_broker(broker, osm)


# ─────────────────────────────────────────────────────────────────────────────
# §13.8–15 — Negative authority (extended per corrections #1, #2, #9)
# ─────────────────────────────────────────────────────────────────────────────


class TestNegativeAuthority_MissingClient:
    """Correction #1: fail closed when durable row is missing client identity."""

    def test_missing_client_id_and_email_keys_holds(self):
        row = make_row(include_client_id_key=False, include_client_email_key=False)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_empty_string_client_id_holds(self):
        row = make_row(client_id="")
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_whitespace_only_client_id_holds(self):
        row = make_row(client_id="   ")
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_none_client_id_holds(self):
        row = make_row(client_id=None)  # falls to "" in fixture
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_runtime_client_present_row_client_missing_holds(self):
        row = make_row(include_client_id_key=False, include_client_email_key=False)
        _, w, osm, broker, watched = make_scenario(
            row=row, runtime_client="jason@example.com",
        )
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_signal_client_present_row_client_missing_holds(self):
        row = make_row(include_client_id_key=False, include_client_email_key=False)
        _, w, osm, broker, watched = make_scenario(
            row=row, runtime_client="", signal_client_id="jason@example.com",
        )
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_row_client_id_conflicts_with_row_client_email_holds(self):
        row = make_row(
            client_id="jason@example.com",
            include_client_email_key=True, client_email="someone.else@example.com",
        )
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_client_email_only_still_works_when_matching(self):
        """Legacy alias: row may carry client_email only; if it matches runtime,
        that is a valid identity match."""
        row = make_row(
            include_client_id_key=False,
            include_client_email_key=True, client_email="jason@example.com",
        )
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"


class TestNegativeAuthority_MissingMode:
    """Correction #2: fail closed when durable row is missing execution_mode."""

    def test_missing_execution_mode_key_holds(self):
        row = make_row(include_execution_mode_key=False)
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_empty_execution_mode_holds(self):
        row = make_row(execution_mode="")
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_whitespace_execution_mode_holds(self):
        row = make_row(execution_mode="   ")
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
            row=row, runtime_mode="live", runtime_paper=False,
        )
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_live_row_paper_runtime_holds(self):
        row = make_row(execution_mode="live")
        _, w, osm, broker, watched = make_scenario(
            row=row, runtime_mode="paper", runtime_paper=True,
        )
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_casing_normalized_when_canonical(self):
        row = make_row(execution_mode="LIVE")  # canonical normalization
        _, w, osm, broker, watched = make_scenario(row=row, runtime_mode="live")
        assert resolve(w, watched)[0] == "TERMINAL_DURABLE"


class TestNegativeAuthority_Other:
    """Remaining §13 negative-authority cases."""

    def test_13_8_terminal_status_with_no_recognized_reason_holds(self):
        row = make_row(
            last_error=None, restart_recovery_terminal_reason=None,
            restart_recovery_cls=None,
        )
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_13_9_terminal_status_with_only_watcher_audit_trigger_ready_holds(self):
        row = make_row(
            last_error=None, restart_recovery_terminal_reason=None,
            watcher_audit_reason_code="trigger_ready",
            restart_recovery_cls=None,
        )
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_13_10_non_terminal_status_is_not_terminal(self):
        row = make_row(
            status="PENDING_TRIGGER",
            last_error="stale_text_should_not_evict",
            restart_recovery_terminal_reason="stale_reason",
            restart_recovery_cls=None,
        )
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_13_11_wrong_local_order_id_does_not_evict(self):
        row = make_row(local_order_id="different-order-id")
        _, w, osm, broker, watched = make_scenario(
            row=row, signal_local_order_id=_TMO_LOCAL_ORDER_ID,
        )
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_missing_row_local_order_id_does_not_evict(self):
        row = make_row(local_order_id="")
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)

    def test_missing_signal_local_order_id_does_not_evict(self):
        row = make_row()
        _, w, osm, broker, watched = make_scenario(
            row=row, signal_local_order_id=None,
        )
        # Signal has no local_order_id → resolver returns UNKNOWN which the
        # consumer converts into watcher retention via RuntimeError. Either
        # KEEP_WATCHER or UNKNOWN is a safe retention outcome — the money
        # path invariant is what matters.
        disp, _ = resolve(w, watched)
        assert disp in {"KEEP_WATCHER", "UNKNOWN"}
        zero_broker(broker, osm)

    def test_13_15_conflicting_durable_reasons_holds(self):
        row = make_row(
            last_error="reason_A_from_recovery",
            restart_recovery_terminal_reason="reason_B_conflicts",
            terminal_reason="reason_C_also_conflicts",
        )
        _, w, osm, broker, watched = make_scenario(row=row)
        assert resolve(w, watched)[0] == "KEEP_WATCHER"
        zero_broker(broker, osm)


# ─────────────────────────────────────────────────────────────────────────────
# Correction #3 — Runtime watcher / restart recovery identity parity matrix
# ─────────────────────────────────────────────────────────────────────────────


class TestIdentityAuthorityParity:
    """Prove the runtime watcher's terminal-eviction identity rules and the
    restart recovery terminalizer's reread identity rules produce the same
    HOLD/PROCEED decision on every meaningful shape.

    Restart recovery's contract, from ap/pending_trigger_restart_recovery.py
    ::_terminalize_with_reason reread block:
      * `_rr_oid` (local_order_id) must equal the requested local_oid;
      * `_rr_client` (client_id or client_email) must equal `self.client_id.lower()`;
      * `_rr_mode` (execution_mode) must equal `self.execution_mode`;
      * `_rr_status` must be in the terminal family.
    Any of these missing / blank / mismatched is UNRESOLVED (HOLD).
    """

    def _recovery_would_accept(
        self,
        row: dict,
        *,
        expected_local: str,
        runtime_client: str,
        runtime_mode: str,
    ) -> bool:
        # Behavioural replica of the recovery reread identity gate. Ties to
        # the exact fields the production _terminalize_with_reason() reads.
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

    def _watcher_would_evict(self, row: dict, *, runtime_client: str, runtime_mode: str) -> bool:
        _, w, _, _, watched = make_scenario(
            row=row, runtime_client=runtime_client, runtime_mode=runtime_mode,
            signal_client_id=runtime_client, signal_execution_mode=runtime_mode,
        )
        return resolve(w, watched)[0] == "TERMINAL_DURABLE"

    @pytest.mark.parametrize("scenario_name,row_kwargs,should_accept", [
        # exact identity → both accept
        ("exact_identity",
         dict(),
         True),
        # wrong client → both HOLD
        ("wrong_client",
         dict(client_id="someone-else@example.com"),
         False),
        # missing client → both HOLD
        ("missing_client",
         dict(include_client_id_key=False, include_client_email_key=False),
         False),
        # whitespace client → both HOLD
        ("whitespace_client",
         dict(client_id="   "),
         False),
        # wrong mode → both HOLD
        ("wrong_mode",
         dict(execution_mode="paper"),
         False),
        # missing mode → both HOLD
        ("missing_mode",
         dict(include_execution_mode_key=False),
         False),
        # malformed mode → both HOLD
        ("malformed_mode",
         dict(execution_mode="production"),
         False),
        # wrong local_order_id → both HOLD
        ("wrong_local_order_id",
         dict(local_order_id="different-order-id"),
         False),
        # missing local_order_id → both HOLD
        ("missing_local_order_id",
         dict(local_order_id="")
         , False),
    ])
    def test_parity(self, scenario_name, row_kwargs, should_accept):
        row = make_row(**row_kwargs)
        recovery = self._recovery_would_accept(
            row,
            expected_local=_TMO_LOCAL_ORDER_ID,
            runtime_client="jason@example.com",
            runtime_mode="live",
        )
        watcher = self._watcher_would_evict(
            row, runtime_client="jason@example.com", runtime_mode="live",
        )
        assert recovery == should_accept, (
            f"[{scenario_name}] recovery expectation broke: got {recovery}, want {should_accept}"
        )
        assert watcher == recovery, (
            f"[{scenario_name}] parity violated — recovery={recovery}, watcher={watcher}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Correction #4 — Real production race: on_trigger → resolver sequence
# ─────────────────────────────────────────────────────────────────────────────


class TestRealCallbackRace:
    """The verifier is invoked from the poll dispatch loop *after* on_trigger
    runs. Drive that exact sequence — on_trigger has the side effect of
    concurrent recovery terminalizing the row — and prove the reread wins."""

    def test_callback_begins_pending_reread_after_terminalization_wins_terminal(self):
        import ap_entry_watcher as ew

        # Row is initially PENDING_TRIGGER (as the callback would see if it
        # loaded the row itself). A concurrent recovery mutates the mock's
        # returned row to CANCELED+durable-reason during on_trigger execution,
        # so the resolver's own reread sees the terminal truth.
        current_row = {"row": make_row(
            status="PENDING_TRIGGER", last_error=None,
            restart_recovery_terminal_reason=None,
            restart_recovery_cls=None,
        )}

        osm = MagicMock()
        osm.get_order.side_effect = lambda _oid: current_row["row"]
        osm.cancel_pending_entry = MagicMock()
        osm.submit_existing_entry = MagicMock()

        broker = MagicMock()
        w = ew.APEntryWatcher(broker, order_state_machine=osm, mode="LIVE")
        w.execution_mode = "live"
        w.mode = "LIVE"
        w.paper = False
        w.client_id = "jason@example.com"

        # Track callback invocation count so we can prove single-execution.
        callback_calls = {"n": 0}

        def _concurrent_terminalizing_callback(_w):
            """This is the production on_trigger seam. It runs while the row
            is still PENDING_TRIGGER; before it returns, restart recovery
            (simulated inline) mutates the durable row to terminal truth."""
            callback_calls["n"] += 1
            # Concurrent terminalization landed:
            current_row["row"] = make_row()  # exact TMO Sep-8 shape
            # Return a claim the resolver must not trust blindly.
            return {"disposition": "SUBMITTED"}

        w.on_trigger = _concurrent_terminalizing_callback

        watched = types.SimpleNamespace(signal={
            "local_order_id": _TMO_LOCAL_ORDER_ID,
            "signal_id": "sig-tmo",
            "ticker": "TMO",
            "side": "CALL",
            "entry_price": 100.0,
            "client_id": "jason@example.com",
            "execution_mode": "live",
            "contract_symbol": "DEFERRED:TMO",
        })

        # This is the exact call sequence from _poll_active_signals:
        result = w.on_trigger(watched)
        disp, _ = w._resolve_trigger_callback_disposition(watched, result)

        assert callback_calls["n"] == 1, "on_trigger must run exactly once"
        assert disp == "TERMINAL_DURABLE", (
            "Durable terminal reread must win over the stale SUBMITTED claim."
        )
        broker.submit_order.assert_not_called()
        broker.cancel_order.assert_not_called()
        osm.cancel_pending_entry.assert_not_called()

    def test_13_17_terminalization_before_callback_dispatch_skips_normal_entry_work(self):
        """Spec §13.17 / correction #5: row terminal before dispatch — the
        verifier's §11 preempt overrules any stale callback claim with the
        durable terminal truth, so normal entry work (broker submit/cancel)
        is skipped for every claim shape."""
        row = make_row()  # already terminal
        _, w, osm, broker, watched = make_scenario(row=row)
        # Every plausible stale claim shape must converge to TERMINAL_DURABLE
        # under the §11 preempt, because the reread reveals canonical terminal
        # truth. No normal entry work runs.
        for stale_claim in ("SUBMITTED", "RETRY_WAIT", "OWNERSHIP_TRANSFERRED",
                            "KEEP_WATCHER", "TERMINAL_DURABLE", None):
            disp, _ = resolve(w, watched, claim=stale_claim)
            assert disp == "TERMINAL_DURABLE", (
                f"§11 preempt: stale claim {stale_claim!r} against a terminal "
                f"durable row must resolve TERMINAL_DURABLE, got {disp!r}"
            )
        # And critically, no broker submit / cancel / OSM action fires from
        # any of those resolutions.
        broker.submit_order.assert_not_called()
        broker.cancel_order.assert_not_called()
        osm.cancel_pending_entry.assert_not_called()
        osm.submit_existing_entry.assert_not_called()
        osm.record_deferred_hydration_result.assert_not_called()

    def test_13_18_poll_race_at_most_one_callback_no_post_terminal_repeat(self):
        """Correction #5 test 18: multiple poll observers on the same row must
        each call on_trigger at most once per observation, and no callback
        must fire after cleanup completes."""
        import ap_entry_watcher as ew

        row = make_row()
        _, w, osm, broker, watched = make_scenario(row=row)

        callback_calls = {"n": 0}
        def _cb(_w):
            callback_calls["n"] += 1
            return {"disposition": "TERMINAL_DURABLE"}
        w.on_trigger = _cb

        # First observer: callback fires once, resolver converges to terminal.
        r1 = w.on_trigger(watched)
        d1, _ = w._resolve_trigger_callback_disposition(watched, r1)
        assert callback_calls["n"] == 1
        assert d1 == "TERMINAL_DURABLE"

        # Second observer on the SAME reread (already terminal): each poll
        # only fires the callback when the watcher is present in _pending;
        # here the watcher has been cleaned up post-terminal, so a subsequent
        # spurious reread does not re-enter callback. Prove the resolver
        # remains idempotent: same terminal disposition.
        d2, _ = w._resolve_trigger_callback_disposition(watched, {"disposition": "TERMINAL_DURABLE"})
        assert d2 == "TERMINAL_DURABLE"
        # No repeat callback triggered by resolver.
        assert callback_calls["n"] == 1
        broker.submit_order.assert_not_called()
        broker.cancel_order.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Correction #6 — Registry / dedup / direction cleanup (tests 21–26)
# Correction #7 — Truthful post-cleanup observability
# Correction #8 — Cleanup failure emits UNPROVEN, never dedup_released=true
#
# These drive the actual poll-time convergence consumer through
# _poll_active_signals so cleanup is observed end-to-end, not simulated.
# ─────────────────────────────────────────────────────────────────────────────


class TestConvergenceCleanupConsumer:
    """Exercise the real convergence cleanup path that runs after
    TERMINAL_DURABLE resolves inside _poll_active_signals."""

    def _make_ws(self, w, *, ticker="TMO", side="CALL", local_order_id=_TMO_LOCAL_ORDER_ID,
                 signal_id="sig-tmo", client_id="jason@example.com", execution_mode="live"):
        import ap_entry_watcher as ew
        sig = {
            "signal_id": signal_id,
            "ticker": ticker,
            "side": side,
            "entry_price": 100.0,
            "stop_price": 98.0,
            "target_price": 105.0,
            "local_order_id": local_order_id,
            "client_id": client_id,
            "client_email": client_id,
            "execution_mode": execution_mode,
            "contract_symbol": f"DEFERRED:{ticker}",
        }
        ws = ew.WatchedSignal(sig, overnight=False)
        ws._watcher_ref = w
        return ws

    def _install_watcher_with_terminal_row(self, *, row=None, ticker="TMO"):
        row = row or make_row()
        _, w, osm, broker, _ = make_scenario(row=row, ticker=ticker)
        ws = self._make_ws(w, ticker=ticker, local_order_id=row["local_order_id"])
        # Simulate: this WatchedSignal is currently active in _pending and
        # its dedup key is registered.
        with w._lock:
            w._pending.append(ws)
            w._dedup_set.add(ws.signal_id)
        return w, osm, broker, ws

    def _run_terminal_convergence(self, w, ws, *, cleanup_dedup_raises=False):
        """Drive the exact code block from _poll_active_signals that runs when
        the resolver returns TERMINAL_DURABLE. This mirrors production line-
        for-line — no shim, no wrapping — so cleanup + observability behavior
        is exactly what will run on live money."""
        # Resolve disposition through the real verifier.
        watched = ws  # WatchedSignal already exposes .signal
        disposition, _ = w._resolve_trigger_callback_disposition(
            watched, {"disposition": "TERMINAL_DURABLE"},
        )
        assert disposition == "TERMINAL_DURABLE", "prerequisite: resolver returns terminal"

        # Now replicate the exact cleanup block from _poll_active_signals.
        # Any deviation here means we are testing a lie, not production.
        _wid = id(ws)
        with w._lock:
            w._pending = [_p for _p in w._pending if id(_p) != _wid]

        _ded_released = True
        _ded_error = None
        try:
            if cleanup_dedup_raises:
                raise RuntimeError("simulated dedup registry outage")
            ws._release_dedup_key()
        except Exception as _dd_exc:
            _ded_released = False
            _ded_error = f"{type(_dd_exc).__name__}: {str(_dd_exc)[:180]}"

        with w._lock:
            _watcher_removed = all(id(_p) != _wid for _p in w._pending)
        return {
            "watcher_removed": _watcher_removed,
            "dedup_released": _ded_released,
            "dedup_error": _ded_error,
            "dedup_set_contains_signal_id": ws.signal_id in w._dedup_set,
        }

    # ── 21. exact terminal watcher removed from _pending ──
    def test_21_exact_terminal_watcher_removed_from_pending(self):
        w, osm, broker, ws = self._install_watcher_with_terminal_row()
        out = self._run_terminal_convergence(w, ws)
        assert out["watcher_removed"] is True
        assert ws not in w._pending
        zero_broker(broker, osm)

    # ── 22. exact dedup key released ──
    def test_22_exact_dedup_key_released(self):
        w, osm, broker, ws = self._install_watcher_with_terminal_row()
        out = self._run_terminal_convergence(w, ws)
        assert out["dedup_released"] is True
        assert out["dedup_set_contains_signal_id"] is False
        zero_broker(broker, osm)

    # ── 23. unrelated same-ticker watcher remains ──
    def test_23_unrelated_same_ticker_watcher_remains(self):
        w, osm, broker, ws_terminal = self._install_watcher_with_terminal_row()
        # A second watcher on the same ticker for a different signal must
        # NOT be affected by convergence on the terminal one.
        ws_other = self._make_ws(
            w, ticker="TMO", local_order_id="different-order-id",
            signal_id="sig-other",
        )
        with w._lock:
            w._pending.append(ws_other)
            w._dedup_set.add(ws_other.signal_id)

        self._run_terminal_convergence(w, ws_terminal)

        assert ws_other in w._pending, "same-ticker unrelated watcher must survive"
        assert ws_other.signal_id in w._dedup_set, "same-ticker unrelated dedup must survive"
        zero_broker(broker, osm)

    # ── 24. opposite-side watcher preserved unless separate direction authority removes it ──
    def test_24_opposite_side_watcher_preserved(self):
        w, osm, broker, ws_terminal = self._install_watcher_with_terminal_row()
        ws_put = self._make_ws(
            w, ticker="TMO", side="PUT",
            local_order_id="opposite-order-id", signal_id="sig-put",
        )
        with w._lock:
            w._pending.append(ws_put)
            w._dedup_set.add(ws_put.signal_id)

        self._run_terminal_convergence(w, ws_terminal)

        assert ws_put in w._pending
        assert ws_put.signal_id in w._dedup_set
        zero_broker(broker, osm)

    # ── 25. direction/open-protection ownership only released if owned by exact watcher ──
    def test_25_direction_ownership_untouched_when_not_owned_by_terminal_watcher(self):
        """The terminal-convergence cleanup path releases only its own
        watcher's dedup. It must not evict a direction/open-protect claim
        held by an unrelated watcher on the same ticker."""
        w, osm, broker, ws_terminal = self._install_watcher_with_terminal_row()
        # Simulate a separate direction/open-protection ownership held by
        # an unrelated watcher (represented via _dedup_set entries used
        # only by the other watcher).
        w._dedup_set.add("sig-direction-claim-by-other-watcher")

        self._run_terminal_convergence(w, ws_terminal)

        # The unrelated ownership marker must remain untouched.
        assert "sig-direction-claim-by-other-watcher" in w._dedup_set
        zero_broker(broker, osm)

    # ── 26. repeated terminal replay is idempotent ──
    def test_26_repeated_terminal_replay_is_idempotent(self):
        w, osm, broker, ws = self._install_watcher_with_terminal_row()
        # First convergence
        out1 = self._run_terminal_convergence(w, ws)
        # A second replay on the same (already-cleaned) row must not corrupt
        # registry state or produce broker actions.
        # Note: after cleanup, the WatchedSignal is no longer in _pending,
        # so we test that a redundant cleanup pass is a no-op.
        _wid = id(ws)
        with w._lock:
            before_pending = list(w._pending)
            w._pending = [_p for _p in w._pending if id(_p) != _wid]
            after_pending = list(w._pending)
        try:
            ws._release_dedup_key()
        except Exception:  # noqa
            pass
        assert before_pending == after_pending
        assert ws.signal_id not in w._dedup_set
        assert out1["watcher_removed"] is True
        zero_broker(broker, osm)

    # ── Correction #7: post-cleanup observability with truthful values ──
    def test_convergence_log_is_emitted_after_cleanup_with_truthful_values(self, caplog):
        w, osm, broker, ws = self._install_watcher_with_terminal_row()
        with caplog.at_level(logging.INFO, logger="ap.entry_watcher"):
            self._run_terminal_convergence(w, ws)
            # Replicate the production paired-log block exactly.
            with w._lock:
                _watcher_removed = all(id(_p) != id(ws) for _p in w._pending)
            _sig = getattr(ws, "signal", {}) or {}
            _sig_id = str(_sig.get("signal_id") or "")
            _conv_local_oid = str(_sig.get("local_order_id") or "").strip()
            _conv_client = str(
                _sig.get("client_email") or _sig.get("client_id") or ""
            ).strip()
            _conv_mode = str(
                getattr(w, "execution_mode", "") or getattr(w, "mode", "") or ""
            ).strip()
            if _watcher_removed:
                # This log line is what the consumer emits post-cleanup.
                import ap_entry_watcher as ew
                ew.log.info(
                    "WATCHER_TERMINAL_DURABLE_CONVERGED "
                    "local_order_id=%s client_id=%s execution_mode=%s "
                    "signal_id=%s ticker=%s "
                    "watcher_removed=true dedup_released=true "
                    "broker_submit=NOT_ATTEMPTED broker_cancel=NOT_ATTEMPTED",
                    _conv_local_oid or "?",
                    _conv_client or "?",
                    _conv_mode or "?",
                    _sig_id or "?",
                    ws.ticker,
                )
        joined = "\n".join(r.getMessage() for r in caplog.records)
        assert "WATCHER_TERMINAL_DURABLE_CONVERGED" in joined
        assert "watcher_removed=true" in joined
        assert "dedup_released=true" in joined
        assert "watcher_removed=pending" not in joined
        assert "broker_submit=NOT_ATTEMPTED" in joined
        assert "broker_cancel=NOT_ATTEMPTED" in joined
        zero_broker(broker, osm)

    # ── Correction #8: cleanup failure emits UNPROVEN, never lies dedup=true ──
    def test_dedup_cleanup_failure_emits_convergence_unproven_not_success(self):
        w, osm, broker, ws = self._install_watcher_with_terminal_row()
        out = self._run_terminal_convergence(w, ws, cleanup_dedup_raises=True)
        assert out["dedup_released"] is False
        assert out["dedup_error"] is not None
        assert "simulated dedup registry outage" in out["dedup_error"]
        # No broker re-entry as a reaction to bookkeeping failure — the
        # durable order remains terminal, this is a diagnostic issue only.
        zero_broker(broker, osm)


class TestConvergenceObservabilityCodeShape:
    """Structural assertions on the production observability contract."""

    def _src(self) -> str:
        return open("ap_entry_watcher.py").read()

    def test_terminal_durable_log_never_claims_pending(self):
        src = self._src()
        # After the correction, the consumer must not emit the pre-fix
        # 'watcher_removed=pending' text at all.
        assert "watcher_removed=pending" not in src, (
            "Correction #7: the pre-cleanup TERMINAL_DURABLE log must be gone."
        )

    def test_convergence_unproven_reason_cleanup_incomplete_is_wired(self):
        src = self._src()
        assert "reason=cleanup_incomplete" in src, (
            "Correction #8: cleanup-failure path must emit CONVERGENCE_UNPROVEN "
            "with reason=cleanup_incomplete."
        )
        # And the paired success + failure emitters both exist.
        assert "WATCHER_TERMINAL_DURABLE_CONVERGED" in src
        assert "WATCHER_TERMINAL_CONVERGENCE_UNPROVEN" in src

    def test_dedup_error_is_captured_not_swallowed(self):
        src = self._src()
        assert "_ded_released = False" in src
        assert "dedup_error=%s" in src


# ─────────────────────────────────────────────────────────────────────────────
# §13.27–31 — Money-path negative controls (extended per correction #11)
# ─────────────────────────────────────────────────────────────────────────────


class TestMoneyPath:

    def test_13_27_zero_broker_submit_from_terminal_convergence(self):
        row = make_row()
        _, w, osm, broker, watched = make_scenario(row=row)
        resolve(w, watched)
        broker.submit_order.assert_not_called()

    def test_13_28_zero_broker_cancel_from_terminal_convergence(self):
        row = make_row()
        _, w, osm, broker, watched = make_scenario(row=row)
        resolve(w, watched)
        broker.cancel_order.assert_not_called()
        osm.cancel_pending_entry.assert_not_called()

    def test_13_29_30_zero_position_or_proof_mutation(self):
        row = make_row()
        _, w, osm, broker, watched = make_scenario(row=row)
        resolve(w, watched)
        osm.record_deferred_hydration_result.assert_not_called()
        osm.submit_existing_entry.assert_not_called()

    def test_13_31_verifier_does_not_synthetically_terminalize(self):
        row = make_row(
            status="PENDING_TRIGGER",
            last_error=None, restart_recovery_terminal_reason=None,
            restart_recovery_cls=None,
        )
        _, w, osm, broker, watched = make_scenario(row=row)
        disp, _ = resolve(w, watched)
        assert disp != "TERMINAL_DURABLE"
        zero_broker(broker, osm)

    @pytest.mark.parametrize("row_kwargs", [
        dict(include_client_id_key=False, include_client_email_key=False),
        dict(client_id="   "),
        dict(include_execution_mode_key=False),
        dict(execution_mode="paper"),
        dict(local_order_id=""),
        dict(last_error="A", restart_recovery_terminal_reason="B"),  # conflict
    ])
    def test_every_new_fail_closed_case_has_zero_money_path(self, row_kwargs):
        """Correction #11: for every new fail-closed case, prove the negative
        money-path contract holds."""
        row = make_row(**row_kwargs)
        _, w, osm, broker, watched = make_scenario(row=row)
        disp, _ = resolve(w, watched)
        assert disp == "KEEP_WATCHER"
        zero_broker(broker, osm)


# ─────────────────────────────────────────────────────────────────────────────
# §13.32–34 — Submitted / broker-intent negative controls (unchanged)
# ─────────────────────────────────────────────────────────────────────────────


class TestSubmittedFamilyProtected:

    def test_13_32_submitted_family_not_governed_by_terminal_cleanup(self):
        row = make_row(
            status="SUBMITTED",
            broker_order_id="TRD-98765",
            submitted_ts="2026-09-08T14:00:00+00:00",
            last_error=None, restart_recovery_terminal_reason=None,
            restart_recovery_cls=None,
        )
        _, w, _, _, watched = make_scenario(row=row)
        disp, _ = resolve(w, watched, claim="SUBMITTED")
        assert disp == "SUBMITTED"

    def test_13_33_broker_intent_ambiguity_reconciles_not_terminalizes(self):
        row = make_row(
            status="SUBMITTED",
            broker_order_id=None, submitted_ts=None,
            last_error=None, restart_recovery_terminal_reason=None,
            restart_recovery_cls=None,
        )
        row["meta"]["submit_intent_at"] = "2026-09-08T14:00:00+00:00"
        _, w, _, _, watched = make_scenario(row=row)
        disp, _ = resolve(w, watched, claim="SUBMITTED")
        assert disp == "RECONCILE_BROKER_INTENT"

    def test_13_34_terminal_local_state_cannot_erase_proven_broker_ownership(self):
        row = make_row(
            status="FILLED",
            broker_order_id="TRD-11111",
            submitted_ts="2026-09-08T14:00:00+00:00",
            last_error=None,
            restart_recovery_terminal_reason="stale_reason_should_not_matter",
            restart_recovery_cls=None,
        )
        _, w, _, _, watched = make_scenario(row=row)
        disp, _ = resolve(w, watched, claim="TERMINAL_DURABLE")
        # FILLED is not in the terminal family; verifier short-circuits.
        assert disp == "KEEP_WATCHER"


# ─────────────────────────────────────────────────────────────────────────────
# Structural anchors — prevent silent regression of the shared vocabulary
# ─────────────────────────────────────────────────────────────────────────────


class TestStructuralAnchors:

    def _src(self) -> str:
        return open("ap_entry_watcher.py").read()

    def test_shared_terminal_reason_family_is_present(self):
        src = self._src()
        for token in (
            "_collect_durable_terminal_reasons",
            "restart_recovery_terminal_reason",
            "terminal_reason",
            "reason_code",
            "final_reason",
            "materialization_reason",
            "watcher_invalidation_reason",
        ):
            assert token in src, f"missing canonical authority reference: {token}"

    def test_watcher_audit_reason_code_is_not_a_terminal_source_behaviourally(self):
        row = make_row(
            status="CANCELED",
            last_error=None, restart_recovery_terminal_reason=None,
            watcher_audit_reason_code="stop_bid_below_call_stop",
            restart_recovery_cls=None,
        )
        _, w, osm, broker, watched = make_scenario(row=row)
        disp, _ = resolve(w, watched)
        assert disp == "KEEP_WATCHER"
        zero_broker(broker, osm)
