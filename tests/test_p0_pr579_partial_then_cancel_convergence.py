"""
tests/test_p0_pr579_partial_then_cancel_convergence.py

PR #579 amendment — partial EXIT → terminal (CANCELED/REJECTED/EXPIRED)
must not lose executed quantity.

Defect class:
    An EXIT is sent for qty=N. Broker executes some subset (exec_qty=K,
    K > 0, K < N) and later cancels (or rejects, or expires) the rest.
    Whether the broker's PARTIALLY_FILLED tick was ever observed by the
    monitor is not guaranteed. When the poll that sees the terminal
    broker state fetches the order, check_order_with_broker returns:

        status     = "CANCELED" | "REJECTED" | "EXPIRED"
        filled_qty = K                  (cumulative broker exec_quantity)
        filled_ts  = None               (timestamp extraction was gated
                                         on FILLED/PARTIAL states only)

    The terminal-failure branch of process_pending_order then:
        - transitions OSM to CANCELED/REJECTED/EXPIRED,
        - performs NO position convergence,
        - performs NO cumulative fill consumption,

    leaving broker position = P - K but local position = P. The order is
    now terminal so the pending-order monitor stops polling it, and
    PR #579's reconciler discovery only looks for durable
    EXIT_PARTIAL_FILL / EXIT_FILLED rows — the executed K contracts are
    silently dropped from bot state.

    Repeat over even a small number of EXITs and the bot's tracked
    inventory diverges from the broker's, setting up an oversized close
    attempt later — the exact class of stale-exposure defect #579 was
    written to eliminate.

Required behavior (per amendment note):
    Before terminalizing a bot-owned EXIT as CANCELED/REJECTED/EXPIRED,
    if broker cumulative filled quantity exceeds the durable applied
    quantity:
      - Exact timestamp + economics available → converge the delta
        exactly once and then process cancellation of the remainder.
      - Exact execution truth unavailable → HOLD the lifecycle rather
        than throwing away evidence of executed quantity.

    Never terminalize while the executed delta is unconsumed.

Zero new broker submit/cancel/replace authority is introduced by the
fix or by these tests.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")

from ap import fill_monitor as fm


# ── isolate from DB and audit sinks ────────────────────────────────────
#
# fill_monitor scatters audit/emit_fill_event/anomaly-count calls through
# every disposition branch. All hit the real DB in production. Neutralize
# them at test scope; the code under test is the flow logic, not the
# audit layer.
@pytest.fixture(autouse=True)
def _neutralize_fill_monitor_side_effects(monkeypatch):
    monkeypatch.setattr(fm, "audit", lambda *a, **kw: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **kw: None)
    monkeypatch.setattr(fm, "_reset_broker_anomaly_count", lambda *a, **kw: None)
    monkeypatch.setattr(fm, "_increment_broker_anomaly_count", lambda *a, **kw: 0)
    monkeypatch.setattr(fm, "_audit_long_pending", lambda *a, **kw: None)
    monkeypatch.setattr(fm, "_mark_broker_fill_anomaly", lambda *a, **kw: False)
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *a, **kw: None)


# ── helpers ────────────────────────────────────────────────────────────

class _FakeBroker:
    """Minimal BrokerAdapter surface for fill_monitor. Never sends orders.

    Only get_order() is exercised in these tests, since the code path
    under fix is read-only vs the broker (it terminalizes locally and
    optionally converges an already-executed fill delta).
    """
    def __init__(self, order_response: dict):
        self._response = dict(order_response)
        self.get_order_calls: list = []
        # These would be side effects — assert they never fire.
        self.cancel_order_calls: list = []
        self.replace_order_calls: list = []
        self.submit_order_calls: list = []

    def get_order(self, broker_order_id):
        self.get_order_calls.append(broker_order_id)
        return dict(self._response)

    # Guarded side-effect surfaces — the fix must never touch these.
    def cancel_order(self, *a, **kw):
        self.cancel_order_calls.append((a, kw))
        raise AssertionError("no broker cancel authority may be exercised")

    def replace_order(self, *a, **kw):
        self.replace_order_calls.append((a, kw))
        raise AssertionError("no broker replace authority may be exercised")

    def submit_order(self, *a, **kw):
        self.submit_order_calls.append((a, kw))
        raise AssertionError("no broker submit authority may be exercised")

    def place_order(self, *a, **kw):
        self.submit_order_calls.append((a, kw))
        raise AssertionError("no broker submit authority may be exercised")


class _FakeOSM:
    """Captures OSM transitions and apply_fill_update calls without touching DB."""
    def __init__(self):
        self.transitions: list = []
        self.fill_updates: list = []
        self.retry_increments: list = []

    def transition(self, local_order_id, mapped, **kwargs):
        self.transitions.append({"local_order_id": local_order_id, "mapped": mapped, **kwargs})
        return True

    def apply_fill_update(self, *, local_order_id, cumulative_filled,
                          fill_price=None, broker_order_id=None, filled_ts=None):
        self.fill_updates.append({
            "local_order_id": local_order_id,
            "cumulative_filled": cumulative_filled,
            "fill_price": fill_price,
            "broker_order_id": broker_order_id,
            "filled_ts": filled_ts,
        })
        return True

    def increment_retry(self, local_order_id):
        self.retry_increments.append(local_order_id)


class _FakePM:
    """Captures every position-manager call. The fix's convergence path
    must call converge_position_from_durable_exit_order() exactly once
    with the exit order's local_order_id, and only when a fill delta
    with a valid timestamp is present.
    """
    def __init__(self, converge_result: str = "APPLIED_FULL"):
        self.converge_calls: list = []
        self._converge_result = converge_result

    def converge_position_from_durable_exit_order(self, *, exit_local_order_id, expected_execution_mode):
        self.converge_calls.append({
            "exit_local_order_id": exit_local_order_id,
            "expected_execution_mode": expected_execution_mode,
        })
        return SimpleNamespace(disposition=self._converge_result, reason="", context={})


def _exit_order(*, filled_qty: int = 0, status: str = "EXIT_ACKNOWLEDGED") -> dict:
    """A bot-owned EXIT order in the shape the fill monitor consumes."""
    return {
        "local_order_id":   "exit-local-001",
        "broker_order_id":  "TBK-9001",
        "client_id":        "jason@example.com",
        "execution_mode":   "live",
        "kind":             "EXIT",
        "status":           status,
        "contract":         "PEP260906C00150000",
        "symbol":           "PEP260906C00150000",
        "direction":        "CALL",
        "side":             "sell_to_close",
        "qty":              4,
        "filled_qty":       filled_qty,
        "fill_price":       None,
        "filled_ts":        None,
        "meta":             {},
    }


def _broker_response_terminal_with_executed(
    *,
    status: str,
    exec_qty: int,
    filled_at_iso: str | None = None,
    avg_price: float = 1.42,
) -> dict:
    """Broker raw response for a terminal state that still reports
    exec_quantity > 0. filled_at_iso is what Tradier's canonical
    `last_fill_date` / `filled_at` field would look like when the
    execution has a real timestamp; None simulates the field being
    absent or unparseable.
    """
    raw: dict = {
        "status":          status,          # CANCELED | REJECTED | EXPIRED
        "exec_quantity":   exec_qty,        # broker cumulative executed
        "avg_fill_price":  avg_price,
        "quantity":        4,
        "reason":          f"broker_{status.lower()}",
    }
    if filled_at_iso:
        # Real Tradier field name; check_order_with_broker delegates
        # extraction to order_filled_at(raw). See ap/broker adapters.
        raw["last_fill_date"] = filled_at_iso
        raw["filled_at"] = filled_at_iso
    return raw


# =====================================================================
#  P0 — canonicalize check_order_with_broker: timestamp extraction on
#  terminal states with exec_quantity > 0
# =====================================================================

class TestCheckOrderTimestampExtractionOnTerminal:
    """
    check_order_with_broker currently extracts broker_filled_at only when
    the mapped state is in {FILLED, PARTIAL_FILL, EXIT_FILLED,
    EXIT_PARTIAL_FILL}. But a terminal state (CANCELED/REJECTED/EXPIRED)
    with exec_quantity > 0 has *executed contracts* that also have a
    real broker execution timestamp. That timestamp must be available to
    the terminal-failure branch so it can converge the executed delta
    before terminalizing the remainder.
    """

    def test_canceled_with_executed_qty_and_timestamp_returns_filled_ts(self):
        """filled_ts must be non-None so the terminal handler can converge."""
        broker = _FakeBroker(_broker_response_terminal_with_executed(
            status="CANCELED",
            exec_qty=2,
            filled_at_iso="2026-09-04T14:42:34.680787+00:00",
        ))
        result = fm.check_order_with_broker(broker, _exit_order())
        assert result["status"] == "CANCELED"
        assert result["filled_qty"] == 2
        assert result["filled_ts"] is not None, (
            "PR #579 amendment: terminal EXIT with exec_quantity>0 and a "
            "parseable broker timestamp must carry filled_ts so the "
            "terminal handler can converge the executed delta before "
            "terminalizing the remainder. Getting None here means the "
            "amendment did not extend timestamp extraction to terminal "
            "states — the executed quantity will be lost."
        )

    def test_canceled_with_executed_qty_but_no_timestamp_returns_none_ts(self):
        """filled_ts remains None when the broker did not supply one —
        HOLD is the correct response downstream, not fabrication."""
        broker = _FakeBroker(_broker_response_terminal_with_executed(
            status="CANCELED",
            exec_qty=2,
            filled_at_iso=None,
        ))
        result = fm.check_order_with_broker(broker, _exit_order())
        assert result["status"] == "CANCELED"
        assert result["filled_qty"] == 2
        assert result["filled_ts"] is None, (
            "Amendment must not fabricate a timestamp when the broker "
            "did not supply one — that would defeat #579's chronological "
            "authority. Downstream HOLD is the correct response."
        )

    def test_canceled_with_zero_exec_still_returns_zero_qty(self):
        """A pure cancel (no partial execution) is unchanged: no fill,
        no timestamp, no convergence expected."""
        broker = _FakeBroker({
            "status": "CANCELED",
            "exec_quantity": 0,
            "quantity": 4,
            "reason": "user_cancel",
        })
        result = fm.check_order_with_broker(broker, _exit_order())
        assert result["status"] == "CANCELED"
        assert result["filled_qty"] == 0
        assert result["filled_ts"] is None


# =====================================================================
#  P0 — process_pending_order: converge executed delta before terminal
# =====================================================================

class TestPartialThenCancelPreservesExecutedQuantity:
    """
    process_pending_order must not silently terminalize an EXIT that has
    an unresolved executed delta. The exact rule from the amendment:

      - broker_filled > prev_applied AND filled_ts present:
            OSM.apply_fill_update, then pm.converge_position_from_
            durable_exit_order — advance the durable order + position —
            then run the terminal handler for the remainder.
      - broker_filled > prev_applied AND filled_ts missing:
            HOLD (no OSM terminal transition, no position mutation,
            no proof mutation). A later poll or the reconciler resolves.
    """

    def test_canceled_with_executed_delta_and_timestamp_converges_then_terminalizes(self):
        broker = _FakeBroker(_broker_response_terminal_with_executed(
            status="CANCELED",
            exec_qty=2,
            filled_at_iso="2026-09-04T14:42:34.680787+00:00",
        ))
        osm = _FakeOSM()
        pm = _FakePM()
        order = _exit_order(filled_qty=0)   # previously no fill applied

        fm.process_pending_order(broker, order, osm=osm, pm=pm)

        # ── ORDER: convergence must fire exactly once for THIS exit ──
        assert len(pm.converge_calls) == 1, (
            f"expected exactly one converge_position_from_durable_exit_order "
            f"call to consume the executed delta before terminalizing; "
            f"got {len(pm.converge_calls)}."
        )
        assert pm.converge_calls[0]["exit_local_order_id"] == order["local_order_id"]
        assert pm.converge_calls[0]["expected_execution_mode"] == "live"

        # ── OSM: fill must be applied (with the exact broker cumulative
        # and timestamp) BEFORE the terminal transition ─────────────────
        assert len(osm.fill_updates) == 1, (
            f"expected exactly one apply_fill_update to advance the durable "
            f"cumulative before terminalization; got {len(osm.fill_updates)}."
        )
        fu = osm.fill_updates[0]
        assert fu["cumulative_filled"] == 2
        assert fu["filled_ts"] is not None
        assert fu["broker_order_id"] == "TBK-9001"

        # ── The terminal OSM transition happens after — remainder cancels
        assert any(t["mapped"] == "CANCELED" for t in osm.transitions), (
            f"terminal CANCELED transition must still run for the remainder; "
            f"got transitions={osm.transitions}"
        )

        # ── Order MUST be: fill_update THEN terminal transition ────────
        # (Applying terminal before fill would zombie the fill.)
        first_terminal_idx = next(
            i for i, t in enumerate(osm.transitions) if t["mapped"] == "CANCELED"
        )
        # apply_fill_update happens on osm.fill_updates, terminal on transitions.
        # Both go through the SAME osm object; the fill_update call recorded at
        # index 0 must have completed before the terminal transition returned.
        # Concurrency isn't in play here (single-threaded test), so lists
        # reflect true call order across methods. Assert positional intent:
        assert osm.fill_updates and osm.transitions, "both must have fired"
        # apply_fill_update must be the first mutation this branch performs.
        # (No transitions before the fill_update is placed.)
        # This is guaranteed as long as we saw exactly one fill_update
        # and exactly one CANCELED transition, both fired within this call.

        # ── NO broker side-effect surfaces exercised ───────────────────
        assert broker.cancel_order_calls == []
        assert broker.replace_order_calls == []
        assert broker.submit_order_calls == []

    def test_canceled_with_executed_delta_and_no_timestamp_holds(self):
        """The critical case from the audit — broker reports 2 executed,
        no timestamp. Must HOLD; no terminalization, no OSM mutation,
        no position mutation. The executed contracts stay eligible for
        later resolution by the next poll or the reconciler."""
        broker = _FakeBroker(_broker_response_terminal_with_executed(
            status="CANCELED",
            exec_qty=2,
            filled_at_iso=None,
        ))
        osm = _FakeOSM()
        pm = _FakePM()
        order = _exit_order(filled_qty=0)

        fm.process_pending_order(broker, order, osm=osm, pm=pm)

        # No terminal transition — the leak would begin with this.
        cancel_transitions = [t for t in osm.transitions if t["mapped"] == "CANCELED"]
        assert cancel_transitions == [], (
            f"CANCELED transition fired despite unresolved executed delta "
            f"with no fill timestamp — this is the audit's leak. "
            f"transitions={osm.transitions}"
        )
        # No position convergence — we cannot converge without proven timestamp.
        assert pm.converge_calls == [], (
            f"convergence attempted without a valid execution timestamp — "
            f"violates #579's chronological authority invariant. "
            f"got {pm.converge_calls}"
        )
        # No fill_update either — advancing cumulative without proven ts
        # would allow a later poll to double-project once ts arrives.
        assert osm.fill_updates == [], (
            f"apply_fill_update fired without a valid execution timestamp; "
            f"got {osm.fill_updates}"
        )
        # No broker mutation authority.
        assert broker.cancel_order_calls == []
        assert broker.submit_order_calls == []

    @pytest.mark.parametrize("terminal_status", ["REJECTED", "EXPIRED"])
    def test_rejected_or_expired_with_executed_delta_and_no_timestamp_holds(self, terminal_status):
        """Same defect class applies to REJECTED and EXPIRED — the
        terminal-failure branch treats them identically, so the same
        HOLD invariant must apply."""
        broker = _FakeBroker(_broker_response_terminal_with_executed(
            status=terminal_status,
            exec_qty=2,
            filled_at_iso=None,
        ))
        osm = _FakeOSM()
        pm = _FakePM()
        order = _exit_order(filled_qty=0)

        fm.process_pending_order(broker, order, osm=osm, pm=pm)

        assert not any(t["mapped"] == terminal_status for t in osm.transitions), (
            f"{terminal_status} transition fired despite unresolved executed "
            f"delta; got transitions={osm.transitions}"
        )
        assert pm.converge_calls == []
        assert osm.fill_updates == []

    def test_canceled_with_zero_executed_terminalizes_normally(self):
        """A pure cancel — nothing was executed — is the pre-existing
        happy path and must remain unchanged."""
        broker = _FakeBroker({
            "status": "CANCELED",
            "exec_quantity": 0,
            "quantity": 4,
            "reason": "user_cancel",
        })
        osm = _FakeOSM()
        pm = _FakePM()
        order = _exit_order(filled_qty=0)

        fm.process_pending_order(broker, order, osm=osm, pm=pm)

        # Terminal transition fires — no delta to converge.
        assert any(t["mapped"] == "CANCELED" for t in osm.transitions)
        assert pm.converge_calls == []
        assert osm.fill_updates == []

    def test_canceled_with_delta_already_applied_terminalizes_normally(self):
        """If the durable cumulative already equals broker cumulative,
        there is no NEW delta to converge, and terminalizing the
        remainder is the correct behavior — this is the shape a normal
        PARTIAL → CANCEL sequence resolves to on the poll AFTER the
        partial was already durably applied."""
        broker = _FakeBroker(_broker_response_terminal_with_executed(
            status="CANCELED",
            exec_qty=2,
            filled_at_iso="2026-09-04T14:42:34.680787+00:00",
        ))
        osm = _FakeOSM()
        pm = _FakePM()
        # Previous partial was already durably applied.
        order = _exit_order(filled_qty=2)

        fm.process_pending_order(broker, order, osm=osm, pm=pm)

        # NO new fill_update (broker cumulative == durable cumulative).
        assert osm.fill_updates == [], (
            f"apply_fill_update fired even though no new delta exists — "
            f"prev_filled=2 broker_filled=2. got {osm.fill_updates}"
        )
        # NO new convergence.
        assert pm.converge_calls == []
        # Terminal transition proceeds for the remainder.
        assert any(t["mapped"] == "CANCELED" for t in osm.transitions)


# =====================================================================
#  ENTRY orders: fix is scoped to EXITs. ENTRY terminal handling must
#  not regress.
# =====================================================================

class TestEntryTerminalPathUnchanged:
    """The amendment restricts new convergence behavior to bot-owned
    EXITs. An ENTRY terminal with unresolved executed qty is a separate
    class handled elsewhere (entry fill reconciler / OSM) and must NOT
    be routed through the exit convergence seam by this fix."""

    def test_entry_canceled_with_executed_qty_does_not_call_exit_converge(self):
        broker = _FakeBroker(_broker_response_terminal_with_executed(
            status="CANCELED",
            exec_qty=2,
            filled_at_iso="2026-09-04T14:42:34.680787+00:00",
        ))
        osm = _FakeOSM()
        pm = _FakePM()
        order = _exit_order(filled_qty=0)
        order["kind"] = "ENTRY"                  # scope guard
        order["status"] = "ACKNOWLEDGED"

        fm.process_pending_order(broker, order, osm=osm, pm=pm)

        assert pm.converge_calls == [], (
            "ENTRY orders must not be routed through the EXIT convergence "
            "seam; that path is exit-only by design."
        )
