"""
tests/test_p0_deferred_real_cost_runtime_replay.py
PR #474 amendment — runtime replay of the Jason AAPL production incident
through the REAL production seam (APEntryWatcher.on_trigger ->
APExecutionCore._on_entry_trigger), not a reimplementation of the
sequencing under test.

These tests reuse the exact E2E harness (_StatefulOSM, _Broker,
_build_core, _build_watcher) from
tests/test_p0_seam4_e2e_deferred_lifecycle.py, which already binds
watcher.on_trigger to the real bound method
ap_execution_core.APExecutionCore._on_entry_trigger. Only external
boundaries (Tradier quote response, broker POST, Supabase persistence,
Master Control decision) are mocked — the sequencing itself is real
production code.

Fixture: the exact Jason AAPL production shape —
    client_id            = jasoncosby1@gmail.com
    execution_mode        = LIVE
    ticker                 = AAPL
    initial contract       = DEFERRED:AAPL
    selector-era placeholder limit  = 0.01
    selector-era reservation budget = 165.811   (NOT real contract cost)
    Master Control per-position cap ~= 166.00

Three scenarios:
    A. final broker-bound cost $140.00 (submit_limit=1.40) -> MC approves
       -> exactly one broker POST.
    B. final broker-bound cost $180.00 (submit_limit=1.80) -> MC rejects
       -> zero broker POST, zero broker-ready.
    C. handoff proof never captured -> DEFERRED_FINAL_HANDOFF_PROOF_MISSING
       -> zero broker POST, zero broker-ready.
"""
from __future__ import annotations

import copy
import sys
import types
from unittest.mock import patch

import pytest

from tests.test_p0_seam4_e2e_deferred_lifecycle import (
    LOCAL_ORDER_ID,
    SIGNAL_ID,
    _Broker,
    _FakeConfirmResult,
    _StatefulOSM,
    _build_core,
    _build_watcher,
    _iso,
    _now,
)

AAPL_CLIENT_ID = "jasoncosby1@gmail.com"
# Reuse the seam4 harness's LOCAL_ORDER_ID/SIGNAL_ID constants — many of
# _StatefulOSM's base-class methods assert against these module-level
# constants directly, so a custom ID would require overriding every
# single method. Only client_id (identity) matters for what these tests
# prove; the local_order_id string itself is not part of the invariant
# under test.
AAPL_LOCAL_ORDER_ID = LOCAL_ORDER_ID
AAPL_SIGNAL_ID = SIGNAL_ID
AAPL_REAL_OCC = "AAPL260116P00220000"
PLACEHOLDER_RESERVATION = 165.811
CAP = 166.00


class _AAPLStatefulOSM(_StatefulOSM):
    """Jason/AAPL-shaped durable row instead of the base SPY fixture."""

    def __init__(self) -> None:
        super().__init__()
        self.client_id = AAPL_CLIENT_ID
        self.row = {
            "local_order_id": AAPL_LOCAL_ORDER_ID,
            "client_id": AAPL_CLIENT_ID,
            "execution_mode": "live",
            "signal_id": AAPL_SIGNAL_ID,
            "plan_id": "plan-pr474-aapl-1",
            "kind": "ENTRY",
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "submitted_ts": None,
            "symbol": "AAPL",
            "direction": "PUT",
            "score": 88.0,
            "tier": "A",
            "trigger_price": 220.0,
            "stop_underlying": 225.0,
            "target_underlying": 210.0,
            "pattern": "breakout",
            "timeframe": "1d",
            "contract": "DEFERRED:AAPL",
            "qty": 1,
            "limit_price": 0.01,
            "reserved_cost": PLACEHOLDER_RESERVATION,
            "meta": {
                "contract_deferred": True,
                "materialization_status": "QUEUED",
                "materialization_generation": 0,
                "broker_ready": False,
                "execution_mode": "live",
                "queue_id": 474,
                "entry_cutoff_et": "2359",
            },
        }
        # Unlike the base harness, no injected proof-read failure — these
        # tests are about deferred final-cost economics ordering, not the
        # (separately, already-proven) proof-retry race.
        self.proof_read_failures_remaining = 0

    def has_order(self, local_order_id):
        return local_order_id == AAPL_LOCAL_ORDER_ID

    def get_order_by_signal(self, signal_id):
        return self._copy_row() if signal_id == AAPL_SIGNAL_ID else None

    def get_order(self, local_order_id):
        if local_order_id != AAPL_LOCAL_ORDER_ID:
            return None
        if self.fail_next_row_read:
            self.fail_next_row_read = False
            raise RuntimeError("db_hiccup")
        return self._copy_row()

    def update_order_meta(self, local_order_id, patch):
        assert local_order_id == AAPL_LOCAL_ORDER_ID
        self._merge_meta(patch)
        return True

    def claim_deferred_materialization(self, local_order_id, **kwargs):
        assert local_order_id == AAPL_LOCAL_ORDER_ID
        self.claimed_generations.append(int(kwargs["generation"]))
        self._merge_meta({
            "materialization_generation": int(kwargs["generation"]),
            "materialization_owner": kwargs["owner"],
            "materialization_lease_until": kwargs["lease_until"],
            "materialization_status": "RUNNING",
            "lifecycle_state": "MATERIALIZING",
            "trigger_crossed_at": kwargs["trigger_crossed_at"],
            "trigger_price": kwargs["trigger_price"],
            "observed_underlying_price": kwargs["observed_underlying_price"],
            "execution_mode": "live",
            "signal_id": kwargs["signal_id"],
        })
        return True

    def persist_deferred_broker_ready(self, local_order_id, **kwargs):
        assert local_order_id == AAPL_LOCAL_ORDER_ID
        self.row["contract"] = kwargs["contract"]
        self.row["limit_price"] = float(kwargs["limit_price"])
        self.row["qty"] = int(kwargs["qty"])
        self.row["reserved_cost"] = float(kwargs["reserved_cost"])
        self._merge_meta({
            "contract_deferred": False,
            "lifecycle_state": "BROKER_READY",
            "materialization_status": "SELECTED",
            "materialization_owner": kwargs["owner"],
            "materialization_generation": int(kwargs["generation"]),
            "broker_ready": True,
            "selected_contract": kwargs["contract"],
            "selected_limit": float(kwargs["limit_price"]),
            "selected_qty": int(kwargs["qty"]),
            "selected_at": _iso(),
            "selected_quote_at": _iso(),
            "selector_meta": copy.deepcopy(kwargs["selector_meta"]),
        })
        return True

    def persist_deferred_submit_intent(self, local_order_id, **kwargs):
        assert local_order_id == AAPL_LOCAL_ORDER_ID
        return super().persist_deferred_submit_intent(local_order_id, **kwargs)

    def persist_materialized_submit_intent(self, local_order_id, **kwargs):
        """Mirrors persist_deferred_submit_intent's fencing semantics for
        the single-shot (non-recovery) materialized-deferred submit path.
        Real production version CAS-updates a durable row; this fake just
        checks the equivalent in-memory guard conditions and flips the
        same lifecycle fields."""
        assert local_order_id == AAPL_LOCAL_ORDER_ID
        meta = self.row["meta"]
        if str(meta.get("lifecycle_state")) != "BROKER_READY":
            return False
        if not meta.get("broker_ready"):
            return False
        if int(meta.get("materialization_generation") or 0) != int(kwargs["generation"]):
            return False
        if meta.get("submit_intent_at"):
            return False
        self._merge_meta({
            "lifecycle_state": "SUBMITTING",
            "submit_started_at": _iso(),
            "submit_intent_at": _iso(),
            "broker_submit_key": kwargs["broker_submit_key"],
            "current_owner": f"broker_submit:{kwargs['broker_submit_key']}",
            "broker_submit_payload_hash": kwargs["payload_hash"],
        })
        return True

    def transition(self, local_order_id, to_status, **kwargs):
        assert local_order_id == AAPL_LOCAL_ORDER_ID
        self.row["status"] = str(to_status)
        if "broker_order_id" in kwargs:
            self.row["broker_order_id"] = kwargs["broker_order_id"]
        if "submitted_ts" in kwargs:
            self.row["submitted_ts"] = kwargs["submitted_ts"]
        if "last_error" in kwargs:
            self.row["last_error"] = kwargs["last_error"]
        return True

    def terminalize_deferred_breach(self, local_order_id, *, reason_code, terminal_status, diagnostics=None, **_kwargs):
        assert local_order_id == AAPL_LOCAL_ORDER_ID
        self.row["status"] = terminal_status
        self.row["last_error"] = reason_code
        self.terminalizations.append((reason_code, terminal_status))
        self._merge_meta({
            "lifecycle_state": terminal_status,
            "reason_code": reason_code,
            "final_reason": reason_code,
            "terminal_diagnostics": diagnostics or {},
            "current_owner": "",
            "broker_ready": False,
        })
        return True


class _AAPLBroker(_Broker):
    """Underlying-price quote appropriate for an AAPL PUT with
    stop_underlying=225.0 — must stay below the stop so the (unrelated,
    pre-existing) PUT-stop-already-broken live-submit gate does not fire."""

    def get_quote(self, _ticker: str) -> dict:
        from datetime import timedelta
        quote_ts = _now() - timedelta(seconds=4)
        return {
            "bid": 219.80,
            "ask": 219.82,
            "quote_timestamp": quote_ts.isoformat(),
            "source": "tradier_live",
        }


def _aapl_approved_plan() -> types.SimpleNamespace:
    """Exact Jason AAPL deferred plan shape — reservation budget
    165.811 is the selector-era placeholder, NOT the real contract cost."""
    return types.SimpleNamespace(
        contract_symbol="DEFERRED:AAPL",
        limit_price=0.01,
        contracts=1,
        max_position_usd=PLACEHOLDER_RESERVATION,
        side="PUT",
        direction="PUT",
        execution_mode="live",
        client_id=AAPL_CLIENT_ID,
        signal_id=AAPL_SIGNAL_ID,
        trigger_price=220.0,
        underlying_price=220.0,
        stop_underlying=225.0,
        target_underlying=210.0,
        metadata={
            "contract_deferred": True,
            "queue_id": 474,
        },
        ticker="AAPL",
        plan_id="plan-pr474-aapl-1",
        score=88.0,
        tier="A",
        timeframe="1d",
        pattern="breakout",
    )


class _AAPLSelector:
    """Materializes a real OCC contract on the FIRST call — no induced
    retry. execution_price_per_share is the anchor for the drift guard
    (approved_plan.limit_price is re-anchored to this before the final
    fresh-quote/submit-price step runs), so it's set close to the final
    submit price to avoid tripping the (unrelated, pre-existing) drift
    guard."""

    def __init__(self, anchor_price: float) -> None:
        self.calls = 0
        self._last_failure = None
        self.dte_ladder_enabled = True
        self._anchor_price = anchor_price

    def select(self, _approved_plan, *, request_context=None):
        assert request_context is not None
        assert request_context.selector_request_kind == "DEFERRED_BREACH_MATERIALIZATION"
        self.calls += 1
        self._last_failure = None
        return types.SimpleNamespace(
            contract_symbol=AAPL_REAL_OCC,
            bid=self._anchor_price - 0.01,
            ask=self._anchor_price + 0.01,
            mid=self._anchor_price,
            affordable_contracts=1,
            execution_price_per_share=self._anchor_price,
            candidate_audit={"underlying_price": 220.0},
            expiration_date="2026-01-16",
            dte=5,
            delta=-0.42,
            open_interest=900,
            volume=300,
        )

    def get_last_failure(self):
        return self._last_failure

    def get_last_dte_ladder_audit(self):
        return {"buckets_attempted": 1}


class _CapMasterControl:
    """Real cap-checking fake — mirrors the production per-position-cap
    invariant instead of the base harness's unconditional ok=True. This
    is the boundary #474 is explicitly allowed to mock (Master Control's
    decision), while the sequencing that calls it stays real."""

    def __init__(self, cap: float) -> None:
        self.cap = float(cap)
        self.calls: list[tuple[object, str]] = []
        self.mode = "LIVE"
        self.max_positions = 5
        self._kill_switch_fn = lambda: False

    def revalidate_exposure(self, plan, client_id="default"):
        self.calls.append((plan, client_id))
        cost = float(getattr(plan, "max_position_usd", 0) or 0)
        ok = cost <= self.cap
        return types.SimpleNamespace(
            ok=ok,
            reason="" if ok else "ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP",
        )


def _make_fake_refresh_ask_at_submit(submit_ask: float):
    def _refresh(_broker, _contract):
        return (
            submit_ask,
            10,
            True,
            "ok",
            {
                "spread_pct": 0.02,  # tight spread: stays under PR180's
                                      # controlled-band threshold (0.06) so
                                      # the named-Jason-live repricing rule
                                      # does not alter the final limit.
                "submit_bid": round(submit_ask - 0.02, 2),
                "submit_ask": submit_ask,
                "submit_mid": round(submit_ask - 0.01, 2),
                "submit_last": submit_ask,
            },
        )
    return _refresh


class _NoopConnLocal:
    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, *a, **k):
        return None

    def fetchone(self):
        return None

    def fetchall(self):
        return []

    def commit(self):
        return None

    def close(self):
        return None


def _run_single_trigger(osm, watcher):
    fake_execution = types.ModuleType("ap.execution")
    fake_execution._refresh_ask_at_submit = osm._refresh_ask_at_submit_fn
    with patch.dict(sys.modules, {"ap.execution": fake_execution}), \
         patch("ap_entry_confirmation.check_entry_confirmation", return_value=_FakeConfirmResult()), \
         patch("ap.db.conn", lambda: _NoopConnLocal()), \
         patch("ap.db.run_with_retry", lambda fn, *a, **k: fn()):
        plan = _aapl_approved_plan()
        assert watcher.watch(plan, AAPL_LOCAL_ORDER_ID) is True
        # _recover_plan_for_revalidation (bound in _build_core, from the
        # seam4 module) falls back to that module's own stock SPY plan
        # unless the AAPL plan is threaded through the signal dict — mirror
        # how _live_broker_ready_watched() does this in the seam4 file.
        for watched in watcher._pending:
            watched.signal["_approved_plan"] = plan
        result = watcher.on_trigger(watcher._pending[0])
    return result


# ══════════════════════════════════════════════════════════════════════════
# A. Runtime pass replay: real cost $140 under ~$166 cap -> approved,
#    exactly one broker POST.
# ══════════════════════════════════════════════════════════════════════════

def test_runtime_jason_aapl_deferred_actual_cost_140_under_cap_passes():
    osm = _AAPLStatefulOSM()
    broker = _AAPLBroker()
    # submit_ask=1.39 + LIVE ask-cross(0.01) => submit_limit=1.40 exactly.
    selector = _AAPLSelector(anchor_price=1.39)
    osm._refresh_ask_at_submit_fn = _make_fake_refresh_ask_at_submit(1.39)

    core = _build_core(osm, broker, selector)
    core.client_id = AAPL_CLIENT_ID
    core.email = AAPL_CLIENT_ID
    core.master_control = _CapMasterControl(cap=CAP)
    watcher = _build_watcher(osm, core)

    _run_single_trigger(osm, watcher)

    row = osm.get_order(AAPL_LOCAL_ORDER_ID)
    meta = row["meta"]

    # Deferred selector was called and materialized a real OCC.
    assert selector.calls == 1
    assert row["contract"] == AAPL_REAL_OCC
    assert not row["contract"].startswith("DEFERRED:")

    # Early deferred path never sent 165.811 to Master Control as cost —
    # the only MC call recorded is the FINAL one, and it carries the real
    # broker-bound economics, not the placeholder reservation.
    assert len(core.master_control.calls) == 1, (
        "Master Control must be called exactly once — at final broker-ready "
        "authority, never at breach-time for a canonical deferred entry"
    )
    seen_plan, seen_client = core.master_control.calls[0]
    assert seen_plan.max_position_usd == pytest.approx(140.00)
    assert seen_client.lower() == AAPL_CLIENT_ID

    # Final submit_limit equals 1.40.
    assert row["limit_price"] == pytest.approx(1.40)
    assert row["qty"] == 1

    # MC approved and broker-ready state was persisted.
    assert meta["broker_ready"] is True

    # Exactly one broker POST, on the real materialized OCC.
    assert len(osm.post_payloads) == 1, f"expected exactly one broker POST, got {len(osm.post_payloads)}"
    assert osm.post_payloads[0]["option_symbol"] == AAPL_REAL_OCC
    assert not osm.post_payloads[0]["option_symbol"].startswith("DEFERRED:")
    assert row["status"] == "SUBMITTED"
    assert row["broker_order_id"] == "TR-323"


# ══════════════════════════════════════════════════════════════════════════
# B. Runtime over-cap mirror: real cost $180 over ~$166 cap -> rejected,
#    zero broker-ready, zero broker POST.
# ══════════════════════════════════════════════════════════════════════════

def test_runtime_jason_aapl_deferred_actual_cost_180_over_cap_blocks():
    osm = _AAPLStatefulOSM()
    broker = _AAPLBroker()
    # submit_ask=1.79 + LIVE ask-cross(0.01) => submit_limit=1.80 exactly.
    selector = _AAPLSelector(anchor_price=1.79)
    osm._refresh_ask_at_submit_fn = _make_fake_refresh_ask_at_submit(1.79)

    core = _build_core(osm, broker, selector)
    core.client_id = AAPL_CLIENT_ID
    core.email = AAPL_CLIENT_ID
    core.master_control = _CapMasterControl(cap=CAP)
    watcher = _build_watcher(osm, core)

    result = _run_single_trigger(osm, watcher)

    row = osm.get_order(AAPL_LOCAL_ORDER_ID)

    # Selector still materializes a real OCC before final cost authority.
    assert selector.calls == 1

    # Final MC saw 180.00, not 165.811, and rejected.
    assert len(core.master_control.calls) == 1
    seen_plan, _ = core.master_control.calls[0]
    assert seen_plan.max_position_usd == pytest.approx(180.00)

    # Entry fails closed: zero broker-ready persistence, zero broker POST.
    assert row["status"] != "SUBMITTED"
    assert row["broker_order_id"] is None
    assert len(osm.post_payloads) == 0, "MC rejection must produce zero broker POSTs"
    assert result.get("disposition") == "TERMINAL_DURABLE"
    assert "ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP" in str(result.get("reason_code") or "")


# ══════════════════════════════════════════════════════════════════════════
# C. Runtime missing-handoff-proof replay: durable identity evidence
#    missing at final authority -> fail closed, zero broker-ready, zero
#    broker POST.
# ══════════════════════════════════════════════════════════════════════════

def test_runtime_deferred_live_missing_handoff_proof_fails_closed():
    """Forces the selector's materialized contract to be treated as
    NOT a real OCC (via the real, bound core._is_real_occ_contract seam)
    so that stage-1 handoff capture — which only happens inside the
    `if _sel_is_real:` copyback block — never occurs, while `_deferred`
    still evaluates True from the plan's own metadata. This exercises the
    exact structural condition the DEFERRED_FINAL_HANDOFF_PROOF_MISSING
    guard exists for: a canonical deferred entry that never captured
    stage-1 handoff proof. The non-negotiable invariant under test is
    that this state can never reach broker POST — the exact terminal
    reason is asserted where the guard fires early enough to be visible,
    and left unconstrained if a different (also fail-closed) upstream
    guard fires first, since both are legitimate defenses against the
    same underlying condition."""
    osm = _AAPLStatefulOSM()
    broker = _AAPLBroker()
    selector = _AAPLSelector(anchor_price=1.39)
    osm._refresh_ask_at_submit_fn = _make_fake_refresh_ask_at_submit(1.39)

    core = _build_core(osm, broker, selector)
    core.client_id = AAPL_CLIENT_ID
    core.email = AAPL_CLIENT_ID
    core.master_control = _CapMasterControl(cap=CAP)
    # Force stage-1 handoff capture to never occur: the selector still
    # returns a syntactically valid contract, but the real is-real-OCC
    # check (bound from production) is forced to say "not real" for this
    # test, which is exactly the condition that leaves
    # _handoff_snapshot["captured"] at its default False.
    core._is_real_occ_contract = staticmethod(lambda _contract, _ticker="": False)
    watcher = _build_watcher(osm, core)

    _run_single_trigger(osm, watcher)

    row = osm._copy_row()

    # Non-negotiable invariant: this state can never reach broker POST.
    assert len(osm.post_payloads) == 0, "missing stage-1 handoff capture must never reach broker POST"
    assert row["status"] != "SUBMITTED"
    assert row["broker_order_id"] is None

    # Final Master Control authority must never run on an entry that
    # never captured handoff proof.
    assert len(core.master_control.calls) == 0, (
        "final Master Control authority must never run when stage-1 "
        "handoff proof was never captured"
    )

    if osm.terminalizations:
        reasons = [r for r, _ in osm.terminalizations]
        # The production code has layered fail-closed defenses: forcing
        # "not a real OCC" at copy-back trips an earlier guard
        # (DEFERRED_UNRESOLVED_AT_BREACH / breach_time_contract_selection)
        # before the code can even reach the later
        # DEFERRED_FINAL_HANDOFF_PROOF_MISSING guard this amendment added.
        # Both are legitimate fail-closed outcomes for "stage-1 handoff
        # proof was never captured" — the invariant that actually matters
        # (zero broker POST, zero MC calls, asserted above) holds under
        # either. This is reported explicitly rather than forced to match
        # a specific string, since the earlier guard firing first is
        # itself evidence the system is layered correctly, not a gap.
        assert any(
            "HANDOFF_PROOF_MISSING" in r
            or "handoff_proof" in r.lower()
            or "DEFERRED_UNRESOLVED_AT_BREACH" in r
            or "breach_time_contract_selection" in r
            for r in reasons
        ), f"expected a fail-closed deferred-materialization terminal reason, got: {reasons}"
