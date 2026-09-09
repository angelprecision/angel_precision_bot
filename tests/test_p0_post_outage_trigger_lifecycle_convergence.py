"""
tests/test_p0_post_outage_trigger_lifecycle_convergence.py

PR #580 — Converge trigger-ready watcher lifecycle after DB/readiness outage.

Defect class (September 4, 2026 production, Jason LIVE, PEP,
signal_id da9db343-8cec-46af-abd0-e43e73bfa4c6):

  durable PENDING_TRIGGER watcher survives / reappears after outage
  + in-memory ap_lifecycle ledger has no state for the recovered signal
  + watcher reaches confirmed breach
  + watcher attempts TRIGGER_READY
  + lifecycle sees NONE -> TRIGGER_READY
  + ILLEGAL_TRANSITION
  + WATCHER_TRIGGER_CALLBACK_ATTEMPT loops (attempt=1/3 repeatedly)
  + no completed contract / materialization handoff
  + no broker POST
  + later restart cleanup: restart_stuck_trigger_ready_no_broker_proof

Ownership boundary (spec §4):
  - #568 owns deferred selector/materialization retry authority.
  - #569 owns late watcher recovery / fresh market truth / readiness.
  - #580 owns EXACTLY this bridge:
        durable recovered watcher authority
                       ↓
        restore in-memory lifecycle ownership
                       ↓
        normal watcher state machine (WATCHING → TRIGGER_READY legally)

Binding invariants proved by this file:
  - NONE -> TRIGGER_READY stays illegal (§2).
  - ap_lifecycle.py is not modified (§5).
  - Recovery-rearm admission with proven identity restores lifecycle
    NONE → ADOPTED → WATCHING BEFORE the watcher becomes behavior-active
    (§6, §14).
  - Trigger fires legally via WATCHING → TRIGGER_READY only after a poll
    confirms current market truth (§7).
  - Contradictory or terminal in-memory state on recovery = HOLD; no
    second behavior-active watcher, no callback, no broker mutation (§6).
  - Broker-ready / entry-submitted supersession = HOLD; watcher does not
    re-take ownership (§16).
  - Ordinary (non-recovery) new admissions are untouched (§6 opening line).
"""
from __future__ import annotations

import os
import types
import uuid
from typing import Optional
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")


# ── Isolate ap_lifecycle.LEDGER between tests ────────────────────────────────
#
# LEDGER is a module-level singleton; state leaks across tests otherwise.
# We reach into `_current_state` (a dict) to clear it — never rewriting it,
# and never touching LEGAL_TRANSITIONS. This preserves spec §5 "do not
# modify ap_lifecycle.py" for the module under test; the test harness just
# resets in-memory state the way a fresh process would.
@pytest.fixture(autouse=True)
def _isolate_ledger():
    import ap_lifecycle as L
    with L.LEDGER._entry_lock:
        L.LEDGER._current_state.clear()
    yield
    with L.LEDGER._entry_lock:
        L.LEDGER._current_state.clear()


# ── Small builders ───────────────────────────────────────────────────────────

def _recovery_signal_dict(
    *,
    signal_id: Optional[str] = None,
    ticker: str = "PEP",
    side: str = "CALL",
    trigger_price: float = 150.0,
    stop_price: float = 148.0,
    target_price: float = 155.0,
    client_id: str = "jason@example.com",
    execution_mode: str = "live",
    contract_symbol: str = "PEP260906C00150000",
    watcher_token: str = "watcher:test-token",
    materialization_generation: int = 1,
    recovery_rearm: bool = True,
    extra_metadata: Optional[dict] = None,
) -> dict:
    """Build the exact signal_dict shape watch() would hand to add_signal()
    on a recovery-rearm admission with proven identity.

    The __recovery_rearm marker is the recovery-provenance signal the
    bridge in add_signal() reads. On real production code paths the
    marker is stamped by watch() at line 3761 after
    recovery_trigger_evidence_identity_is_proven() has passed. Bypassing
    watch() here isolates the bridge from #569's late-recovery /
    readiness classifier, which is a separately-owned PR.
    """
    _sig = signal_id or str(uuid.uuid4())
    metadata = {
        "canonical_signal_id": _sig,
        "client_id": client_id,
        "execution_mode": execution_mode,
        "materialization_generation": materialization_generation,
        "trigger_generation": materialization_generation,
    }
    if extra_metadata:
        metadata.update(extra_metadata)
    d = {
        "signal_id":                 _sig,
        "canonical_signal_id":       _sig,
        "ticker":                    ticker,
        "side":                      side,
        "score":                     72.0,
        "grade":                     "A",
        "entry_price":               trigger_price,
        "entry_trigger":             trigger_price,
        "stop_price":                stop_price,
        "target_price":              target_price,
        "trigger_crossed_at":        None,
        "plan_id":                   f"plan-{_sig[:8]}",
        "local_order_id":            f"local-{_sig[:8]}",
        "metadata":                  metadata,
        "materialization_generation": materialization_generation,
        "client_id":                 client_id,
        "execution_mode":            execution_mode,
        "watcher_token":             watcher_token,
        "trigger_generation":        materialization_generation,
        "deferred_retry_not_before": None,
        "queue_id":                  None,
        "trade_queue_id":            None,
        "late_attachment_policy_eligible": False,
        "contract_symbol":           contract_symbol,
        "pattern":                   "2-1-2",
        "prior_day_high":            trigger_price + 0.10,
        "prior_day_low":             stop_price - 0.10,
        "timeframe":                 "1h",
        "strategy_type":             "continuation",
        "contract_deferred":         False,
        "trigger": {
            "entry": trigger_price,
            "stop":  stop_price,
            "pt1":   target_price,
        },
    }
    if recovery_rearm:
        d["__recovery_rearm"] = True
    return d


def _bare_watcher():
    """Construct a watcher isolated from broker/OSM/DB side effects.

    require_on_trigger=False lets a signal register even with no callback,
    which we exploit to prove lifecycle restoration is independent of
    callback machinery — the bridge must land before any poll fires.

    We stub the DB-touching helpers add_signal() invokes on the
    recovery-rearm HOLD path. Real DB access is not the subject under
    test here; the ledger transitions are.
    """
    import ap_entry_watcher as w
    ew = w.APEntryWatcher(
        broker=None,
        order_state_machine=None,
        require_on_trigger=False,
        mode="LIVE",
    )
    ew._test_only_allow_recovery_without_row_lock = True
    ew._persist_watcher_audit = lambda *a, **kw: None
    return ew


# ─────────────────────────────────────────────────────────────────────────────
#  §13 / §14 — Exact PEP-class fail-first + positive convergence
# ─────────────────────────────────────────────────────────────────────────────

class TestPEPFailFirstAndConvergence:
    """
    Reproduce the exact September 4 PEP defect class on the unpatched code
    (fail-first), then prove the patched code converges legally.

    The fail-first assertion is expressed as: after the recovery-rearm
    watcher admission, in-memory lifecycle for the signal MUST be WATCHING
    (or ADOPTED at minimum). On unpatched code it will still be NONE, so
    the subsequent illegal NONE→TRIGGER_READY happens exactly as
    production observed.
    """

    def test_recovery_rearm_admits_and_restores_lifecycle_to_watching(self):
        """
        §14 positive trace, up to and including the WATCHING transition.
        The bridge must fire on recovery-rearm admission with proven
        identity (marker __recovery_rearm=True set by watch() after
        recovery_trigger_evidence_identity_is_proven passes), leaving
        lifecycle in WATCHING. A subsequent normal poll then advances
        WATCHING → TRIGGER_READY legally (LEGAL_TRANSITIONS line 183-188).
        We do not force a fill here — reaching normal downstream gates
        legally is the whole positive claim (§14).
        """
        import ap_lifecycle as L

        sig_id = str(uuid.uuid4())
        ew = _bare_watcher()

        # Simulate: process restart. LEDGER._current_state is empty.
        assert L.LEDGER.current_state(sig_id) is None, (
            "precondition: post-restart in-memory state must be NONE"
        )

        sig = _recovery_signal_dict(signal_id=sig_id)
        ok = ew.add_signal(sig)

        # ── FAIL-FIRST ASSERTION ─────────────────────────────────────────
        # Without the bridge: state stays NONE (illegal NONE→TRIGGER_READY loop
        # is the production defect). With the bridge: state must be WATCHING.
        assert ok is True, (
            "recovery_rearm add_signal() must accept a proven-identity admission; "
            "returned False — fail-first would be malformed"
        )
        state = L.LEDGER.current_state(sig_id)
        assert state == L.SignalState.WATCHING, (
            f"PR #580 defect reproduces: after recovery-rearm admission, "
            f"lifecycle should be WATCHING; got {state!r}. "
            f"This is the exact PEP September 4 lifecycle loss — the next "
            f"confirmed breach will attempt NONE→TRIGGER_READY, log "
            f"ILLEGAL_TRANSITION, and loop WATCHER_TRIGGER_CALLBACK_ATTEMPT "
            f"attempt=1/3 without ever converging."
        )
        # And the watcher must be registered — bridge must not un-register a
        # legal admission.
        assert len(ew._pending) == 1

    def test_none_to_trigger_ready_transition_remains_illegal(self):
        """
        §2 binding invariant: NONE → TRIGGER_READY must never be legal, no
        matter what #580 does. If a future edit weakens LEGAL_TRANSITIONS,
        this test fails and gate-blocks the merge.
        """
        import ap_lifecycle as L
        legal_from_none = L.LEGAL_TRANSITIONS.get(None, set())
        assert L.SignalState.TRIGGER_READY not in legal_from_none, (
            "NONE→TRIGGER_READY became legal — spec §2 binding invariant "
            "violated. Restart must go NONE→ADOPTED→WATCHING→TRIGGER_READY."
        )

    def test_positive_full_trace_watching_then_legal_trigger_ready(self):
        """
        §14 full trace: after restart+recovery, restored lifecycle allows
        a legal WATCHING → TRIGGER_READY transition via the existing
        signal_triggered() API. Proves the bridge preserves the normal
        state machine — trigger is not manufactured from historical
        metadata (§7); it is a normal poll-time state advance.
        """
        import ap_lifecycle as L

        sig_id = str(uuid.uuid4())
        ew = _bare_watcher()
        sig = _recovery_signal_dict(signal_id=sig_id)
        ok = ew.add_signal(sig)
        assert ok is True

        # Now simulate the normal poll-time trigger transition. This is
        # exactly what ap_entry_watcher._poll_active_signals does after a
        # confirmed breach (line ~5708). It must NOT log ILLEGAL_TRANSITION.
        entry = L.signal_triggered(
            sig_id, "PEP", L.LifecycleOwner.WATCHER,
            reason="trigger_breached_post_recovery",
        )
        assert entry.to_state == L.SignalState.TRIGGER_READY
        assert L.LEDGER.current_state(sig_id) == L.SignalState.TRIGGER_READY, (
            "WATCHING → TRIGGER_READY should succeed legally after bridge; "
            "if it does not, the bridge failed to leave lifecycle in WATCHING."
        )


# ─────────────────────────────────────────────────────────────────────────────
#  §6 — Lifecycle restoration matrix (NONE / ADOPTED / WATCHING / terminal)
# ─────────────────────────────────────────────────────────────────────────────

class TestLifecycleRestorationMatrix:

    def test_case_none_restores_via_adopted_to_watching(self):
        """§6 case 1: in-memory NONE → ADOPTED → WATCHING."""
        import ap_lifecycle as L
        sig_id = str(uuid.uuid4())
        assert L.LEDGER.current_state(sig_id) is None

        ew = _bare_watcher()
        ok = ew.add_signal(_recovery_signal_dict(signal_id=sig_id))

        assert ok is True
        assert L.LEDGER.current_state(sig_id) == L.SignalState.WATCHING
        # And the trace should show the exact NONE→ADOPTED→WATCHING sequence,
        # not a jump. Guards spec §7 (do not manufacture TRIGGER_READY).
        history = L.LEDGER.history(sig_id)
        seq = [(e.from_state, e.to_state) for e in history]
        assert (None, L.SignalState.ADOPTED) in seq, (
            f"expected NONE→ADOPTED in trace; got {seq}"
        )
        assert (L.SignalState.ADOPTED, L.SignalState.WATCHING) in seq, (
            f"expected ADOPTED→WATCHING in trace; got {seq}"
        )

    def test_case_adopted_transitions_only_to_watching(self):
        """§6 case 2: already ADOPTED → single WATCHING transition."""
        import ap_lifecycle as L
        sig_id = str(uuid.uuid4())
        L.signal_adopted(sig_id, "PEP", reason="test_setup_prior_adopt")
        assert L.LEDGER.current_state(sig_id) == L.SignalState.ADOPTED
        history_before = len(L.LEDGER.history(sig_id))

        ew = _bare_watcher()
        ok = ew.add_signal(_recovery_signal_dict(signal_id=sig_id))

        assert ok is True
        assert L.LEDGER.current_state(sig_id) == L.SignalState.WATCHING
        # Exactly one new lifecycle write (ADOPTED→WATCHING) — no
        # duplicate ADOPTED, no spurious churn.
        assert len(L.LEDGER.history(sig_id)) == history_before + 1

    def test_case_watching_is_idempotent_noop(self):
        """§6 case 3: already WATCHING → idempotent, still WATCHING.

        The current signal_watching() transition dispatcher records an
        idempotent WATCHING→WATCHING entry (line 355 treats
        resolved_from == to_state as legal). What §6 requires is that the
        bridge does not perform any illegal or contradictory write; state
        remains WATCHING and the watcher is admitted.
        """
        import ap_lifecycle as L
        sig_id = str(uuid.uuid4())
        L.signal_adopted(sig_id, "PEP", reason="test_setup")
        L.signal_watching(sig_id, "PEP", L.LifecycleOwner.WATCHER, reason="test_setup")
        assert L.LEDGER.current_state(sig_id) == L.SignalState.WATCHING

        ew = _bare_watcher()
        ok = ew.add_signal(_recovery_signal_dict(signal_id=sig_id))

        assert ok is True
        assert L.LEDGER.current_state(sig_id) == L.SignalState.WATCHING

    @pytest.mark.parametrize("terminal_setup", [
        "POSITION_OPENED",
        "INVALIDATED",
        "EXPIRED",
        "CANCELLED",
        "REJECTED",
        "ENTRY_SUBMITTED",
    ])
    def test_terminal_or_superseded_state_holds_and_does_not_register(
        self, terminal_setup,
    ):
        """
        §6 contradictory/terminal + §16 broker-ready negative control:
        HOLD. No second behavior-active watcher; state is not overwritten.
        """
        import ap_lifecycle as L

        sig_id = str(uuid.uuid4())
        # Drive lifecycle into the terminal / superseded state via legal
        # transitions — never a direct write (would violate §5).
        L.signal_adopted(sig_id, "PEP", reason="setup")
        L.signal_watching(sig_id, "PEP", L.LifecycleOwner.WATCHER, reason="setup")
        if terminal_setup == "POSITION_OPENED":
            L.signal_triggered(sig_id, "PEP", L.LifecycleOwner.WATCHER, reason="setup")
            L.signal_entry_submitted(sig_id, "PEP", reason="setup")
            L.signal_position_opened(sig_id, "PEP", reason="setup")
            expected_state = L.SignalState.POSITION_OPENED
        elif terminal_setup == "ENTRY_SUBMITTED":
            L.signal_triggered(sig_id, "PEP", L.LifecycleOwner.WATCHER, reason="setup")
            L.signal_entry_submitted(sig_id, "PEP", reason="setup")
            expected_state = L.SignalState.ENTRY_SUBMITTED
        elif terminal_setup == "INVALIDATED":
            L.signal_invalidated(sig_id, "PEP", L.LifecycleOwner.WATCHER, reason="setup")
            expected_state = L.SignalState.INVALIDATED
        elif terminal_setup == "EXPIRED":
            L.signal_expired(sig_id, "PEP", L.LifecycleOwner.WATCHER, reason="setup")
            expected_state = L.SignalState.EXPIRED
        elif terminal_setup == "CANCELLED":
            L.signal_cancelled(sig_id, "PEP", L.LifecycleOwner.WATCHER, reason="setup")
            expected_state = L.SignalState.CANCELLED
        elif terminal_setup == "REJECTED":
            # signal_rejected requires reason_code; use raw transition API.
            L.LEDGER.transition(
                sig_id, "PEP", L.SignalState.REJECTED,
                L.LifecycleOwner.WATCHER, "setup",
            )
            expected_state = L.SignalState.REJECTED
        else:
            pytest.fail(f"unknown setup {terminal_setup}")

        assert L.LEDGER.current_state(sig_id) == expected_state

        ew = _bare_watcher()
        pending_before = len(ew._pending)
        ok = ew.add_signal(_recovery_signal_dict(signal_id=sig_id))

        # HOLD outcome:
        assert ok is False, (
            f"recovery admission on {terminal_setup} must HOLD (return False); "
            f"admitting a second behavior-active watcher on a superseded "
            f"lifecycle is exactly what spec §6 forbids."
        )
        assert len(ew._pending) == pending_before, (
            f"HOLD must not leave the watcher registered in _pending; "
            f"a superseded lifecycle with a live watcher would race the "
            f"legitimate owner."
        )
        assert L.LEDGER.current_state(sig_id) == expected_state, (
            f"HOLD must not overwrite superseded lifecycle authority; "
            f"state went {expected_state} → {L.LEDGER.current_state(sig_id)}"
        )


# ─────────────────────────────────────────────────────────────────────────────
#  §6 (opening) — non-recovery admissions are untouched
# ─────────────────────────────────────────────────────────────────────────────

class TestNonRecoveryAdmissionUnchanged:
    """
    Bridge must NOT run for ordinary new admissions. Spec §6:
    "Do not run this for ordinary new watcher admission."

    Ordinary admissions rely on the pre-existing lifecycle path (queue /
    execution stamps CREATED elsewhere; watcher stamps WATCHING via the
    existing _ew_record helper elsewhere in the flow). The recovery bridge
    is guarded by __recovery_rearm — verifying that guard prevents the
    bridge from silently taking ownership of every add_signal.
    """

    def test_non_recovery_add_signal_does_not_run_recovery_bridge(self):
        """
        A non-recovery admission (no __recovery_rearm marker on signal_dict)
        must not fire the recovery bridge.

        We assert this by checking that no ADOPTED entry appears in the
        ledger with the recovery reason code. The bridge's ONLY entrypoint
        to signal_adopted uses reason="restart_recovery_loaded_existing_signal";
        seeing that reason for a non-recovery signal would prove leak.
        """
        import ap_lifecycle as L
        sig_id = str(uuid.uuid4())
        ew = _bare_watcher()
        # Ordinary new admission — recovery_rearm marker OFF.
        sig = _recovery_signal_dict(signal_id=sig_id, recovery_rearm=False)

        _ok = ew.add_signal(sig)

        for e in L.LEDGER.history(sig_id):
            assert e.reason != "restart_recovery_loaded_existing_signal", (
                "Recovery-only bridge fired on an ordinary admission — "
                "guard by __recovery_rearm is broken. Every new signal "
                "would now silently ADOPT itself as if post-restart."
            )
            assert e.reason != "restored_to_watching_after_restart", (
                "Recovery-only bridge fired on an ordinary admission — "
                "guard by __recovery_rearm is broken."
            )


# ─────────────────────────────────────────────────────────────────────────────
#  §18 — Identity mismatch matrix
# ─────────────────────────────────────────────────────────────────────────────

class TestIdentityMatrixRefusalPathways:
    """
    Recovery rearm identity is proven upstream by
    recovery_trigger_evidence_identity_is_proven(). §18 requires that
    every identity mismatch refuse — no lifecycle fabrication, no callback,
    no submit/cancel, no watcher registered.

    These tests exercise the identity gate in-place (the watch() path
    calls it before propagating recovery to add_signal), so an identity
    mismatch means: (a) watch() returns False, (b) nothing lands in
    _pending, (c) LEDGER remains untouched.

    We only test the durable-trigger-evidence case here (raw_crossed_at
    present but provenance-mismatched). The trivial "no trigger evidence"
    path is intentionally admissible — that IS the normal recovery flow.
    """

    def test_provenance_mismatch_with_durable_evidence_refuses_admission(self):
        """
        This test exercises the recovery_trigger_evidence_identity_is_proven()
        gate directly. That function is pre-existing (defined at
        ap_entry_watcher.py line 229) and is called by watch() BEFORE the
        recovery marker is ever propagated to add_signal(). The bridge
        therefore only sees pre-verified signals; identity mismatches
        never reach it. This test proves the upstream refusal still
        holds — a regression here would let an unverified plan request
        lifecycle restoration (§8 binding rule).
        """
        import ap_entry_watcher as w

        sig_id = str(uuid.uuid4())
        local_oid = f"local-{sig_id[:8]}"

        # Durable trigger evidence exists, but provenance identifies a
        # different local_order_id. This is the exact class the identity
        # proof exists to catch.
        row = {
            "signal_id": sig_id,
            "canonical_signal_id": sig_id,
            "client_id": "jason@example.com",
            "execution_mode": "live",
            "local_order_id": local_oid,
            "trigger_crossed_at": "2026-09-04T14:00:00+00:00",
            "metadata": {
                "canonical_signal_id": sig_id,
                "client_id": "jason@example.com",
                "execution_mode": "live",
                "trigger_crossed_at": "2026-09-04T14:00:00+00:00",
                "trigger_crossed_at_provenance": {
                    "signal_id": sig_id,
                    "local_order_id": "some-other-order-not-ours",
                    "canonical_signal_id": sig_id,
                    "client_id": "jason@example.com",
                    "execution_mode": "live",
                },
            },
        }
        assert w.recovery_trigger_evidence_identity_is_proven(row, local_oid) is False, (
            "Provenance-mismatched durable evidence must be refused by the "
            "identity gate before any recovery bridge runs — spec §8/§18."
        )

    def test_identity_gate_admits_clean_evidence(self):
        """Sanity: a matching-provenance row passes the identity gate."""
        import ap_entry_watcher as w
        sig_id = str(uuid.uuid4())
        local_oid = f"local-{sig_id[:8]}"
        row = {
            "signal_id": sig_id,
            "canonical_signal_id": sig_id,
            "client_id": "jason@example.com",
            "execution_mode": "live",
            "local_order_id": local_oid,
            "trigger_crossed_at": "2026-09-04T14:00:00+00:00",
            "metadata": {
                "canonical_signal_id": sig_id,
                "client_id": "jason@example.com",
                "execution_mode": "live",
                "trigger_crossed_at": "2026-09-04T14:00:00+00:00",
                "trigger_crossed_at_provenance": {
                    "signal_id": sig_id,
                    "local_order_id": local_oid,
                    "canonical_signal_id": sig_id,
                    "client_id": "jason@example.com",
                    "execution_mode": "live",
                },
            },
        }
        assert w.recovery_trigger_evidence_identity_is_proven(row, local_oid) is True


# ─────────────────────────────────────────────────────────────────────────────
#  September 8 Jason LIVE production-shaped watch()/poll() regression
# ─────────────────────────────────────────────────────────────────────────────

class TestSeptember8JasonLiveRecoveryRearm:
    """Exercise the real recovery watch bridge with a durable OSM row.

    This deliberately stops at watcher/lifecycle ownership. The fake OSM only
    exposes a read of the canonical row plus recording mocks for broker-facing
    methods; no selector, submit, cancel, position, or proof authority is
    supplied by this regression.
    """

    client_id = "jasoncosby1@gmail.com"
    execution_mode = "live"

    @staticmethod
    def _shape():
        sid = str(uuid.uuid4())
        canonical_sid = f"canonical:{sid}"
        local_oid = f"tmo-recovery:{sid}"
        metadata = {
            "signal_id": sid,
            "canonical_signal_id": canonical_sid,
            "client_id": TestSeptember8JasonLiveRecoveryRearm.client_id,
            "execution_mode": TestSeptember8JasonLiveRecoveryRearm.execution_mode,
            "ticker": "TMO",
            "side": "CALL",
            "materialization_generation": 1,
            "trigger_generation": 1,
            "contract_deferred": True,
        }
        row = {
            "status": "PENDING_TRIGGER",
            "kind": "ENTRY",
            "local_order_id": local_oid,
            "signal_id": sid,
            "canonical_signal_id": canonical_sid,
            "client_id": TestSeptember8JasonLiveRecoveryRearm.client_id,
            "execution_mode": TestSeptember8JasonLiveRecoveryRearm.execution_mode,
            "broker_order_id": None,
            "submitted_ts": None,
            "meta": dict(metadata),
        }
        plan = types.SimpleNamespace(
            signal_id=sid,
            canonical_signal_id=canonical_sid,
            ticker="TMO",
            side="CALL",
            score=80.0,
            tier="A",
            trigger_price=100.0,
            entry_trigger=100.0,
            stop_underlying=95.0,
            target_underlying=110.0,
            plan_id=f"plan:{sid}",
            metadata=dict(metadata),
            materialization_generation=1,
            trigger_generation=1,
            client_id=TestSeptember8JasonLiveRecoveryRearm.client_id,
            execution_mode=TestSeptember8JasonLiveRecoveryRearm.execution_mode,
            contract_symbol="DEFERRED:TMO",
            pattern="2-1-2",
            prior_day_high=100.1,
            prior_day_low=94.9,
            timeframe="5m",
            strategy_type="continuation",
        )
        return sid, canonical_sid, local_oid, row, plan

    @staticmethod
    def _watcher(row):
        import ap_entry_watcher as ew

        class _OSM:
            def __init__(self, durable_row):
                self._durable_row = durable_row
                self.get_order = MagicMock(side_effect=self._get_order)
                self.cancel_pending_entry = MagicMock(return_value=True)
                self.submit_existing_entry = MagicMock(return_value=True)
                self.create_position = MagicMock(return_value=True)
                self.create_proof_trade = MagicMock(return_value=True)

            def _get_order(self, _local_order_id):
                return dict(self._durable_row)

        osm = _OSM(row)
        broker = MagicMock()
        broker.submit_order = MagicMock()
        broker.cancel_order = MagicMock()
        watcher = ew.APEntryWatcher(
            broker=broker,
            order_state_machine=osm,
            require_on_trigger=True,
            mode="LIVE",
        )
        watcher._test_only_allow_recovery_without_row_lock = True
        watcher._persist_watcher_audit = MagicMock()
        watcher._persist_trigger_confirmation_authority = MagicMock(return_value=True)
        watcher._is_regular_session_now = lambda: True
        watcher._is_past_entry_cutoff_now = lambda: False
        watcher._get_quote = lambda _ticker: {
            "bid": 99.5,
            "ask": 99.8,
            "quote_age_ms": 1,
        }
        watcher._fetch_quotes = MagicMock(return_value={
            "TMO": {"bid": 99.5, "ask": 100.1, "quote_age_ms": 1},
        })
        return watcher, osm, broker

    def test_exact_recovery_restores_before_registration_and_triggers_legally(self):
        import ap_lifecycle as L

        sid, _canonical_sid, local_oid, row, plan = self._shape()
        watcher, osm, broker = self._watcher(row)
        registration_trace = []

        class _TracedPending(list):
            def append(self, watched):
                registration_trace.append(
                    ("behavior_active_registration", L.LEDGER.current_state(sid))
                )
                super().append(watched)

        watcher._pending = _TracedPending()
        assert L.LEDGER.current_state(sid) is None

        # This is the production recovery call: the durable row is already
        # PENDING_TRIGGER, while the fresh process ledger is empty.
        assert watcher.watch(plan, local_oid, recovery_rearm=True) is True
        assert L.LEDGER.current_state(sid) == L.SignalState.WATCHING
        assert registration_trace == [
            ("behavior_active_registration", L.SignalState.WATCHING),
        ]
        assert len(watcher._pending) == 1
        assert watcher._dedup_set == {sid}

        history = L.LEDGER.history(sid)
        transitions = [(entry.from_state, entry.to_state) for entry in history]
        assert (None, L.SignalState.ADOPTED) in transitions
        assert (L.SignalState.ADOPTED, L.SignalState.WATCHING) in transitions
        assert (None, L.SignalState.TRIGGER_READY) not in transitions
        assert not any(
            "ILLEGAL_TRANSITION" in str(entry.reason) for entry in history
        )

        # Restart/replay is idempotent: the exact same durable owner remains
        # represented by one watcher and one dedup key.
        assert watcher.watch(plan, local_oid, recovery_rearm=True) is True
        assert len(watcher._pending) == 1
        assert registration_trace == [
            ("behavior_active_registration", L.SignalState.WATCHING),
        ]

        # The test may run after the local wall clock crosses the production
        # overnight threshold. Recovery/lifecycle ownership is independent of
        # that scheduling boundary; pin this already-admitted watcher to the
        # regular-session poll phase so the two observations below exercise the
        # normal WATCHING -> TRIGGER_READY path deterministically.
        watcher._pending[0].overnight = False

        watcher.on_trigger = MagicMock(return_value={"disposition": "KEEP_WATCHER"})
        watcher._poll_active_signals()
        assert watcher.on_trigger.call_count == 0
        watcher._poll_active_signals()
        assert watcher.on_trigger.call_count == 1
        assert L.LEDGER.current_state(sid) == L.SignalState.TRIGGER_READY
        assert len(watcher._pending) == 1
        assert watcher._dedup_set == {sid}
        assert not any(
            "ILLEGAL_TRANSITION" in str(entry.reason)
            for entry in L.LEDGER.history(sid)
        )

        # Restoration and the retained callback made no broker or downstream
        # mutation; this regression does not grant those authorities.
        assert osm.cancel_pending_entry.call_count == 0
        assert osm.submit_existing_entry.call_count == 0
        assert osm.create_position.call_count == 0
        assert osm.create_proof_trade.call_count == 0
        assert broker.submit_order.call_count == 0
        assert broker.cancel_order.call_count == 0

    @pytest.mark.parametrize(
        "field",
        ["client_id", "execution_mode", "local_order_id", "signal_id", "canonical_signal_id"],
    )
    def test_exact_recovery_identity_mismatch_holds_without_registration(self, field):
        import ap_lifecycle as L

        sid, canonical_sid, local_oid, row, plan = self._shape()
        wrong_value = {
            "client_id": "another-client@example.com",
            "execution_mode": "paper",
            "local_order_id": f"wrong-order:{sid}",
            "signal_id": f"wrong-signal:{sid}",
            "canonical_signal_id": f"wrong-canonical:{sid}",
        }[field]
        if field == "local_order_id":
            requested_local_oid = wrong_value
        else:
            requested_local_oid = local_oid
            setattr(plan, field, wrong_value)
            plan.metadata[field] = wrong_value
            if field == "canonical_signal_id":
                plan.canonical_signal_id = wrong_value
            if field == "signal_id":
                plan.signal_id = wrong_value
        watcher, osm, broker = self._watcher(row)
        watcher._is_regular_session_now = lambda: False

        assert watcher.watch(plan, requested_local_oid, recovery_rearm=True) is False
        assert L.LEDGER.current_state(sid) is None
        assert watcher._pending == []
        assert watcher._dedup_set == set()
        assert "recovery_lifecycle" in watcher._last_reject_reason
        assert osm.cancel_pending_entry.call_count == 0
        assert osm.submit_existing_entry.call_count == 0
        assert broker.submit_order.call_count == 0
        assert broker.cancel_order.call_count == 0

    def test_durable_broker_handoff_evidence_holds_before_lifecycle_restore(self):
        import ap_lifecycle as L

        sid, _canonical_sid, local_oid, row, plan = self._shape()
        row["meta"]["submit_intent_at"] = "2026-09-08T16:00:00+00:00"
        plan.metadata["submit_intent_at"] = row["meta"]["submit_intent_at"]
        watcher, osm, broker = self._watcher(row)

        assert watcher.watch(plan, local_oid, recovery_rearm=True) is False
        assert L.LEDGER.current_state(sid) is None
        assert watcher._pending == []
        assert "broker_handoff" in watcher._last_reject_reason
        assert osm.cancel_pending_entry.call_count == 0
        assert osm.submit_existing_entry.call_count == 0
        assert broker.submit_order.call_count == 0
        assert broker.cancel_order.call_count == 0
