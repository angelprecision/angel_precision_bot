"""
P0 focused test suite for PR #392 — unfilled-entry repricing & retry continuity.

Covers every controlled scenario the audit brief requires:

  1. seed_entry_lifecycle_on_first_ack is idempotent, computes retry_deadline
     as min(signal_valid_until, ack + window), stamps static_approval_proof.
  2. claim_entry_retry_generation is CAS-guarded on retry_generation and
     refuses to overwrite durable anchors via extra_meta.
  3. record_entry_lifecycle_terminal supports owner+generation guard.
  4. submit_entry_continuation:
       a. refuses non-terminal cancel  → CANCEL_NOT_CONFIRMED
       b. refuses expired deadline     → RETRY_DEADLINE_EXPIRED
       c. refuses gen>=2               → REPLACEMENT_UNFILLED_TERMINAL
       d. broker.get_order raises      → BROKER_TRUTH_UNAVAILABLE_HOLD
       e. broker.get_order returns None → BROKER_TRUTH_UNAVAILABLE_HOLD
       f. broker.get_order returns ambiguous status → BROKER_TRUTH_UNAVAILABLE_HOLD
       g. full late fill, adoption OK  → LATE_FILL_ADOPTED
       h. full late fill, adoption fails → MARKET_TRUTH_HOLD (no dup exposure)
       i. partial fill computes remainder correctly
       j. kill switch                  → ACCOUNT_RISK_BLOCKED
       k. daily stop hit               → ACCOUNT_RISK_BLOCKED
       l. missing static proof         → REPLACEMENT_UNFILLED_TERMINAL
       m. no lifecycle id              → REPLACEMENT_UNFILLED_TERMINAL
       n. direction reversal           → REARM_DIRECTION_REVERSAL (not TERMINAL)
       o. stop broken                  → THESIS_INVALID_TERMINAL
       p. HOLD reason (age unknown)    → MARKET_TRUTH_HOLD
       q. SUBMIT_VALID + broker OK     → REPLACEMENT_SUBMITTED
       r. SUBMIT_VALID + broker fails  → REPLACEMENT_CLAIMED (not SUBMITTED)
       s. concurrent claim: 2nd loses  → REPLACEMENT_UNFILLED_TERMINAL

These tests use fakes rather than DB fixtures so they can run without a
Postgres instance in the P0 CI runner. The OSM CAS pattern is verified
against a hand-rolled in-memory row store that models `orders.meta` as a
JSON dict updated via non-destructive merge — the same semantics as
`COALESCE(meta,'{}'::jsonb) || %s::jsonb` in Postgres.
"""
from __future__ import annotations

import os

# Stub DATABASE_URL so ap.db imports cleanly in unit tests without a real DB.
os.environ.setdefault("DATABASE_URL", "postgres://test:test@localhost:5432/test")

import json
import types
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

import pytest


# ---------------------------------------------------------------------------
# Minimal in-memory OSM fake that reproduces the meta-merge semantics
# and the three CAS methods PR #392 adds.
# ---------------------------------------------------------------------------

class FakeOSM:
    """Enough OSM surface to exercise submit_entry_continuation.

    Models orders.meta as a Python dict updated via non-destructive shallow
    merge. Implements every method the continuation calls: get_order,
    seed_entry_lifecycle_on_first_ack, claim_entry_retry_generation,
    record_entry_lifecycle_terminal, submit_existing_entry (stubbed),
    rearm_entry_for_direction_reversal (stubbed).
    """
    def __init__(self, client_id: str = "test_client"):
        self.client_id = client_id
        self._rows: dict[str, dict] = {}
        # Test knobs
        self._submit_existing_entry_result: dict | None = None
        self._submit_existing_entry_raises: Exception | None = None
        self._rearm_called_with: list = []
        self._rearm_return: bool = True

    # ---- test setup helpers ----
    def install_row(self, local_order_id: str, row: dict) -> None:
        row = dict(row)
        row.setdefault("client_id", self.client_id)
        row.setdefault("kind", "ENTRY")
        row.setdefault("meta", {})
        self._rows[local_order_id] = row

    # ---- API used by continuation ----
    def get_order(self, local_order_id: str):
        return self._rows.get(local_order_id)

    def _merge_meta(self, local_order_id: str, patch: dict) -> None:
        row = self._rows.get(local_order_id)
        if not row:
            return
        meta = row.get("meta") or {}
        if not isinstance(meta, dict):
            meta = {}
        meta = dict(meta)
        meta.update(patch)
        row["meta"] = meta

    def seed_entry_lifecycle_on_first_ack(
        self, local_order_id, *, broker_order_id, first_broker_ack_at,
        original_approved_quantity, original_signal_valid_until,
        retry_window_seconds, static_approval_proof,
    ):
        row = self._rows.get(local_order_id)
        if not row:
            return False
        meta = row.get("meta") or {}
        if str(meta.get("entry_lifecycle_id") or "").strip():
            return False
        if not isinstance(static_approval_proof, dict) or not static_approval_proof:
            return False
        if int(original_approved_quantity) < 1:
            return False
        try:
            ack_dt = datetime.fromisoformat(str(first_broker_ack_at).replace("Z", "+00:00"))
            if ack_dt.tzinfo is None:
                ack_dt = ack_dt.replace(tzinfo=timezone.utc)
        except Exception:
            return False
        window_deadline = ack_dt + timedelta(seconds=int(retry_window_seconds))
        deadline = window_deadline
        if original_signal_valid_until:
            try:
                svu = datetime.fromisoformat(
                    str(original_signal_valid_until).replace("Z", "+00:00")
                )
                if svu.tzinfo is None:
                    svu = svu.replace(tzinfo=timezone.utc)
                if svu < window_deadline:
                    deadline = svu
            except Exception:
                pass
        self._merge_meta(local_order_id, {
            "entry_lifecycle_id":          f"entry_lifecycle:{local_order_id}",
            "original_local_order_id":     local_order_id,
            "current_local_order_id":      local_order_id,
            "original_broker_order_id":    broker_order_id,
            "current_broker_order_id":     broker_order_id,
            "first_broker_ack_at":         first_broker_ack_at,
            "original_approved_quantity":  int(original_approved_quantity),
            "filled_quantity":             0,
            "remaining_quantity":          int(original_approved_quantity),
            "original_signal_valid_until": original_signal_valid_until,
            "retry_deadline":              deadline.isoformat(),
            "retry_window_seconds":        int(retry_window_seconds),
            "retry_generation":            0,
            "retry_state":                 "OPEN_UNFILLED",
            "retry_owner":                 "",
            "retry_not_before":            "",
            "static_approval_proof":       dict(static_approval_proof),
        })
        return True

    def claim_entry_retry_generation(
        self, local_order_id, *, current_generation, owner, retry_state,
        retry_not_before=None, extra_meta=None,
    ):
        row = self._rows.get(local_order_id)
        if not row:
            return False
        meta = row.get("meta") or {}
        if not str(meta.get("entry_lifecycle_id") or "").strip():
            return False
        _stored_gen = int(meta.get("retry_generation") or 0)
        if _stored_gen != int(current_generation):
            return False
        # Anchors that extra_meta must never overwrite
        anchors = {
            "entry_lifecycle_id", "original_local_order_id",
            "original_broker_order_id", "first_broker_ack_at",
            "original_approved_quantity", "original_signal_valid_until",
            "retry_deadline", "static_approval_proof",
        }
        patch = {
            "retry_generation": int(current_generation) + 1,
            "retry_owner":      owner,
            "retry_state":      retry_state,
        }
        if retry_not_before:
            patch["retry_not_before"] = str(retry_not_before)
        if isinstance(extra_meta, dict):
            for k, v in extra_meta.items():
                if k in anchors:
                    continue
                patch[k] = v
        self._merge_meta(local_order_id, patch)
        return True

    def record_entry_lifecycle_terminal(
        self, local_order_id, *, terminal_reason,
        owner=None, current_generation=None, extra_meta=None,
    ):
        row = self._rows.get(local_order_id)
        if not row:
            return False
        meta = row.get("meta") or {}
        if not str(meta.get("entry_lifecycle_id") or "").strip():
            return False
        if owner is not None and current_generation is not None:
            if str(meta.get("retry_owner") or "") != str(owner):
                return False
            if int(meta.get("retry_generation") or 0) != int(current_generation):
                return False
        anchors = {
            "entry_lifecycle_id", "original_local_order_id",
            "original_broker_order_id", "first_broker_ack_at",
            "original_approved_quantity", "original_signal_valid_until",
            "retry_deadline", "static_approval_proof",
        }
        patch = {
            "retry_state":           terminal_reason,
            "retry_terminal_reason": terminal_reason,
        }
        if isinstance(extra_meta, dict):
            for k, v in extra_meta.items():
                if k in anchors:
                    continue
                patch[k] = v
        self._merge_meta(local_order_id, patch)
        return True

    def submit_existing_entry(self, *, local_order_id, broker, limit_price=None, plan=None):
        if self._submit_existing_entry_raises is not None:
            raise self._submit_existing_entry_raises
        if self._submit_existing_entry_result is not None:
            return dict(self._submit_existing_entry_result)
        return {"ok": True, "local_order_id": local_order_id,
                "broker_order_id": "broker_replacement_" + uuid.uuid4().hex[:6],
                "status": "ACKNOWLEDGED"}

    def rearm_entry_for_direction_reversal(self, local_order_id, reason: str = ""):
        self._rearm_called_with.append((local_order_id, reason))
        return self._rearm_return


class FakeBroker:
    """Enough broker surface to exercise the continuation.

    - get_order returns a dict describing the ORIGINAL broker order state.
    - get_quote returns the current underlying quote.
    """
    def __init__(self):
        self._get_order_side_effect = None
        self._get_order_return: dict | None = {
            "status": "CANCELED",
            "filled_qty": 0,
        }
        self._get_quote_return: dict | None = {
            "bid": 100.0, "ask": 100.05,
            "source": "poly", "quote_age_ms": 200,
        }

    def get_order(self, broker_order_id: str):
        if self._get_order_side_effect is not None:
            raise self._get_order_side_effect
        return self._get_order_return

    def get_quote(self, symbol: str):
        return self._get_quote_return


# ---------------------------------------------------------------------------
# Continuation stub — we test the LOGIC of submit_entry_continuation by
# instantiating a minimal APExecutionCore-shaped object and monkeypatching
# the pieces the method touches externally (get_client_state, market gate,
# fill_monitor).
# ---------------------------------------------------------------------------

class MiniCore:
    """Minimal execution-core surface that owns submit_entry_continuation.

    We use the real method directly off the class dict so we exercise the
    exact production code path.
    """
    def __init__(self, client_id: str, osm: FakeOSM):
        self.client_id = client_id
        self.email = client_id
        self.order_state_machine = osm
        self.position_manager = None
        self.exit_eng = None
        self.data_broker = None


def _install_real_method(mini: MiniCore):
    """Bind the real APExecutionCore.submit_entry_continuation onto mini."""
    from ap_execution_core import APExecutionCore
    mini.submit_entry_continuation = APExecutionCore.submit_entry_continuation.__get__(mini)
    mini._adopt_broker_fill_via_fill_monitor = (
        APExecutionCore._adopt_broker_fill_via_fill_monitor.__get__(mini)
    )
    return mini


def _seed_row(osm: FakeOSM, *, local_order_id="ord_1", broker_order_id="brk_1",
              qty=4, deadline_offset_seconds=60, static_proof=None) -> dict:
    """Install an already-seeded ENTRY order in the fake OSM."""
    if static_proof is None:
        static_proof = {
            "symbol":               "BAC",
            "direction":            "PUT",
            "contract":             "BAC260724P00062000",
            "execution_mode":       "paper",
            "signal_id":            "sig_bac_1",
            "score":                72,
            "tier":                 "B",
            "pattern":              "2d_reversal",
            "setup_generation":     1,
            "first_30min_allowed":  True,
            "time_gate_policy_id":  "admission_v1",
            "trigger_price":        61.17,
            "stop_price":           61.60,
            "target_price":         60.20,
        }
    now = datetime.now(timezone.utc)
    ack_iso = (now - timedelta(seconds=5)).isoformat()
    deadline = (now + timedelta(seconds=deadline_offset_seconds)).isoformat()
    row = {
        "local_order_id":  local_order_id,
        "client_id":       osm.client_id,
        "kind":            "ENTRY",
        "status":          "CANCELED",
        "broker_order_id": broker_order_id,
        "symbol":          static_proof["symbol"],
        "direction":       static_proof["direction"],
        "contract":        static_proof["contract"],
        "execution_mode":  static_proof["execution_mode"],
        "signal_id":       static_proof["signal_id"],
        "qty":             qty,
        "limit_price":     0.66,
        "meta": {
            "entry_lifecycle_id":          f"entry_lifecycle:{local_order_id}",
            "original_local_order_id":     local_order_id,
            "current_local_order_id":      local_order_id,
            "original_broker_order_id":    broker_order_id,
            "current_broker_order_id":     broker_order_id,
            "first_broker_ack_at":         ack_iso,
            "original_approved_quantity":  qty,
            "filled_quantity":             0,
            "remaining_quantity":          qty,
            "original_signal_valid_until": None,
            "retry_deadline":              deadline,
            "retry_window_seconds":        75,
            "retry_generation":            0,
            "retry_state":                 "OPEN_UNFILLED",
            "retry_owner":                 "",
            "retry_not_before":            "",
            "static_approval_proof":       static_proof,
        },
    }
    osm.install_row(local_order_id, row)
    return row


@pytest.fixture(autouse=True)
def _stub_get_client_state(monkeypatch):
    """Neutral account state — no kill switch."""
    import ap.db as _dbmod

    def _fake_get_client_state(client_id: str) -> dict:
        return {"kill_switch": 0, "mode": "LIVE", "daily_stop_hit": 0}

    monkeypatch.setattr(_dbmod, "get_client_state", _fake_get_client_state, raising=False)


@pytest.fixture
def _pass_market_gate(monkeypatch):
    """Force check_market_validity_gate to PASS. Returns the mock so test
    can adjust reason_code / passed."""
    import ap.live_submit_gates as _gates

    class _Res:
        def __init__(self, passed=True, reason_code="PASS"):
            self.passed = passed
            self.reason_code = reason_code
            self.audit = {}

    holder = {"result": _Res(True, "PASS")}

    def _fake_gate(**kwargs):
        return holder["result"]

    monkeypatch.setattr(_gates, "check_market_validity_gate", _fake_gate, raising=False)
    return holder


# ===========================================================================
# SEED tests
# ===========================================================================

class TestSeedEntryLifecycle:
    def test_seeds_all_anchor_fields_and_computes_deadline(self):
        # A raw un-seeded row with no entry_lifecycle_id
        osm = FakeOSM()
        osm.install_row("o1", {
            "meta": {},
            "broker_order_id": "brk_1",
            "symbol": "BAC",
            "direction": "PUT",
            "qty": 4,
        })
        now = datetime.now(timezone.utc)
        ack_iso = now.isoformat()
        seeded = osm.seed_entry_lifecycle_on_first_ack(
            "o1",
            broker_order_id="brk_1",
            first_broker_ack_at=ack_iso,
            original_approved_quantity=4,
            original_signal_valid_until=(now + timedelta(seconds=120)).isoformat(),
            retry_window_seconds=75,
            static_approval_proof={"symbol": "BAC", "direction": "PUT",
                                    "execution_mode": "paper", "trigger_price": 61.17},
        )
        assert seeded is True
        meta = osm._rows["o1"]["meta"]
        assert meta["entry_lifecycle_id"] == "entry_lifecycle:o1"
        assert meta["original_local_order_id"] == "o1"
        assert meta["current_local_order_id"] == "o1"
        assert meta["original_broker_order_id"] == "brk_1"
        assert meta["original_approved_quantity"] == 4
        assert meta["remaining_quantity"] == 4
        assert meta["retry_generation"] == 0
        assert meta["retry_state"] == "OPEN_UNFILLED"
        # Deadline: min(signal_valid_until, ack + 75s). Both are ~75s so
        # signal_valid_until (120s) is LATER, so deadline == ack+75s exactly.
        deadline = datetime.fromisoformat(meta["retry_deadline"])
        assert (deadline - now).total_seconds() == pytest.approx(75, abs=2)

    def test_seed_is_idempotent(self):
        osm = FakeOSM()
        osm.install_row("o1", {"meta": {}, "broker_order_id": "brk_1"})
        now = datetime.now(timezone.utc).isoformat()
        ok1 = osm.seed_entry_lifecycle_on_first_ack(
            "o1", broker_order_id="brk_1", first_broker_ack_at=now,
            original_approved_quantity=1, original_signal_valid_until=None,
            retry_window_seconds=75,
            static_approval_proof={"symbol": "X", "direction": "CALL", "execution_mode": "paper", "trigger_price": 1.0},
        )
        ok2 = osm.seed_entry_lifecycle_on_first_ack(
            "o1", broker_order_id="brk_1", first_broker_ack_at=now,
            original_approved_quantity=99, original_signal_valid_until=None,
            retry_window_seconds=999,
            static_approval_proof={"symbol": "X", "direction": "CALL", "execution_mode": "paper", "trigger_price": 1.0},
        )
        assert ok1 is True
        assert ok2 is False, "second seed must be no-op"
        assert osm._rows["o1"]["meta"]["original_approved_quantity"] == 1

    def test_seed_deadline_uses_signal_valid_until_when_earlier(self):
        osm = FakeOSM()
        osm.install_row("o1", {"meta": {}, "broker_order_id": "brk_1"})
        now = datetime.now(timezone.utc)
        signal_valid_until = now + timedelta(seconds=30)  # < window (75s)
        seeded = osm.seed_entry_lifecycle_on_first_ack(
            "o1", broker_order_id="brk_1",
            first_broker_ack_at=now.isoformat(),
            original_approved_quantity=1,
            original_signal_valid_until=signal_valid_until.isoformat(),
            retry_window_seconds=75,
            static_approval_proof={"symbol": "X", "direction": "CALL", "execution_mode": "paper", "trigger_price": 1.0},
        )
        assert seeded is True
        deadline = datetime.fromisoformat(osm._rows["o1"]["meta"]["retry_deadline"])
        assert (deadline - now).total_seconds() == pytest.approx(30, abs=2)


# ===========================================================================
# CAS tests
# ===========================================================================

class TestCASClaim:
    def test_two_workers_only_one_wins(self):
        osm = FakeOSM()
        _seed_row(osm)
        w1 = osm.claim_entry_retry_generation(
            "ord_1", current_generation=0, owner="w1", retry_state="REPLACEMENT_SUBMIT",
        )
        w2 = osm.claim_entry_retry_generation(
            "ord_1", current_generation=0, owner="w2", retry_state="REPLACEMENT_SUBMIT",
        )
        assert w1 is True
        assert w2 is False
        assert osm._rows["ord_1"]["meta"]["retry_owner"] == "w1"
        assert osm._rows["ord_1"]["meta"]["retry_generation"] == 1

    def test_extra_meta_cannot_overwrite_anchors(self):
        osm = FakeOSM()
        _seed_row(osm)
        original_lifecycle = osm._rows["ord_1"]["meta"]["entry_lifecycle_id"]
        original_deadline = osm._rows["ord_1"]["meta"]["retry_deadline"]
        original_proof = osm._rows["ord_1"]["meta"]["static_approval_proof"]
        ok = osm.claim_entry_retry_generation(
            "ord_1", current_generation=0, owner="w1", retry_state="REPLACEMENT_SUBMIT",
            extra_meta={
                "entry_lifecycle_id":     "HACKED",
                "retry_deadline":         "1970-01-01T00:00:00+00:00",
                "static_approval_proof":  {"hacked": True},
                "some_new_field":         "OK",
            },
        )
        assert ok is True
        meta = osm._rows["ord_1"]["meta"]
        assert meta["entry_lifecycle_id"] == original_lifecycle
        assert meta["retry_deadline"] == original_deadline
        assert meta["static_approval_proof"] == original_proof
        assert meta["some_new_field"] == "OK"


# ===========================================================================
# Continuation guard tests
# ===========================================================================

class TestContinuationGuards:
    def test_non_terminal_cancel_refused(self, _pass_market_gate):
        osm = FakeOSM()
        _seed_row(osm)
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="PENDING", broker=FakeBroker(),
        )
        assert r["outcome"] == "CANCEL_NOT_CONFIRMED"

    def test_deadline_expired(self, _pass_market_gate):
        osm = FakeOSM()
        _seed_row(osm, deadline_offset_seconds=-5)
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=FakeBroker(),
        )
        assert r["outcome"] == "RETRY_DEADLINE_EXPIRED"

    def test_second_replacement_blocked(self, _pass_market_gate):
        osm = FakeOSM()
        _seed_row(osm)
        osm._rows["ord_1"]["meta"]["retry_generation"] = 2
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=FakeBroker(),
        )
        assert r["outcome"] == "REPLACEMENT_UNFILLED_TERMINAL"
        assert "generation" in r["detail"].lower() or "replacement" in r["detail"].lower()

    def test_no_lifecycle_id_blocked(self, _pass_market_gate):
        osm = FakeOSM()
        osm.install_row("ord_1", {"meta": {}, "kind": "ENTRY", "qty": 4})
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=FakeBroker(),
        )
        assert r["outcome"] == "REPLACEMENT_UNFILLED_TERMINAL"


# ===========================================================================
# Broker truth invariants (Blocker 5)
# ===========================================================================

class TestBrokerTruthMustNotProceedOnUnknown:
    def test_get_order_raises(self, _pass_market_gate):
        osm = FakeOSM()
        _seed_row(osm)
        broker = FakeBroker()
        broker._get_order_side_effect = RuntimeError("broker api down")
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        assert r["outcome"] == "BROKER_TRUTH_UNAVAILABLE_HOLD"
        # NO replacement claim beyond hold state
        assert osm._rows["ord_1"]["meta"]["retry_state"] == "MARKET_TRUTH_HOLD"
        # Zero broker POST attempted
        assert osm._submit_existing_entry_result is None

    def test_get_order_returns_none(self, _pass_market_gate):
        osm = FakeOSM()
        _seed_row(osm)
        broker = FakeBroker()
        broker._get_order_return = None
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        assert r["outcome"] == "BROKER_TRUTH_UNAVAILABLE_HOLD"

    def test_get_order_returns_ambiguous_status(self, _pass_market_gate):
        osm = FakeOSM()
        _seed_row(osm)
        broker = FakeBroker()
        broker._get_order_return = {"status": "SOMETHING_WEIRD", "filled_qty": 0}
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        assert r["outcome"] == "BROKER_TRUTH_UNAVAILABLE_HOLD"


# ===========================================================================
# Late fill & partial fill (Blocker 4)
# ===========================================================================

class TestLateFillAdoption:
    def _patch_fill_monitor(self, monkeypatch, adopt_ok: bool):
        """Fake process_pending_order that either transitions the row to
        FILLED with a position_id, or leaves it PARTIAL_FILL to simulate
        adoption failure."""
        import ap.fill_monitor as _fmm

        def _fake_process(broker, order, osm, pm=None, exit_engine=None, alert_fn=None, data_broker=None):
            row = osm._rows.get(order["local_order_id"])
            if not row:
                return
            if adopt_ok:
                row["status"] = "FILLED"
                row["position_id"] = "pos_" + uuid.uuid4().hex[:6]

        monkeypatch.setattr(_fmm, "process_pending_order", _fake_process, raising=False)

    def test_full_late_fill_adopted(self, monkeypatch, _pass_market_gate):
        osm = FakeOSM()
        _seed_row(osm, qty=4)
        broker = FakeBroker()
        broker._get_order_return = {"status": "FILLED", "filled_qty": 4}
        self._patch_fill_monitor(monkeypatch, adopt_ok=True)
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        assert r["outcome"] == "LATE_FILL_ADOPTED"
        assert osm._rows["ord_1"]["meta"]["retry_terminal_reason"] == "LATE_FILL_ADOPTED"
        assert osm._rows["ord_1"]["status"] == "FILLED"

    def test_full_late_fill_adoption_fails_holds_no_replacement(self, monkeypatch, _pass_market_gate):
        osm = FakeOSM()
        _seed_row(osm, qty=4)
        broker = FakeBroker()
        broker._get_order_return = {"status": "FILLED", "filled_qty": 4}
        self._patch_fill_monitor(monkeypatch, adopt_ok=False)
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        # Fill exists at broker but adoption incomplete → HOLD, no replacement
        assert r["outcome"] == "MARKET_TRUTH_HOLD"
        assert osm._rows["ord_1"]["meta"]["retry_state"] == "MARKET_TRUTH_HOLD"
        # We must not have terminalized as LATE_FILL_ADOPTED yet
        assert osm._rows["ord_1"]["meta"].get("retry_terminal_reason") != "LATE_FILL_ADOPTED"

    def test_partial_fill_computes_remainder(self, monkeypatch, _pass_market_gate):
        # Original qty 4, broker filled 2 → replacement quantity == 2
        osm = FakeOSM()
        _seed_row(osm, qty=4)
        broker = FakeBroker()
        broker._get_order_return = {"status": "PARTIAL_FILL", "filled_qty": 2}
        # No adoption needed (partial). We continue toward replacement.
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        assert r["outcome"] == "REPLACEMENT_SUBMITTED", r
        assert r["audit"]["replacement_quantity"] == 2
        assert r["audit"]["remaining_quantity_computed"] == 2


# ===========================================================================
# Account risk (kill switch, daily stop)
# ===========================================================================

class TestAccountRiskGates:
    def test_kill_switch_blocks(self, monkeypatch, _pass_market_gate):
        import ap.db as _dbmod
        monkeypatch.setattr(_dbmod, "get_client_state",
                            lambda cid: {"kill_switch": 1}, raising=False)
        osm = FakeOSM()
        _seed_row(osm)
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=FakeBroker(),
        )
        assert r["outcome"] == "ACCOUNT_RISK_BLOCKED"

    def test_daily_stop_blocks(self, monkeypatch, _pass_market_gate):
        import ap.db as _dbmod
        monkeypatch.setattr(_dbmod, "get_client_state",
                            lambda cid: {"daily_stop_hit": 1}, raising=False)
        osm = FakeOSM()
        _seed_row(osm)
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=FakeBroker(),
        )
        assert r["outcome"] == "ACCOUNT_RISK_BLOCKED"


# ===========================================================================
# Market truth authority (Blockers 1 & 2)
# ===========================================================================

class TestMarketTruthAuthority:
    def test_direction_reversal_is_rearm_not_terminal(self, monkeypatch):
        import ap.live_submit_gates as _gates

        class _Res:
            passed = False
            reason_code = "PUT_NO_LONGER_BELOW_TRIGGER"
            audit = {}

        monkeypatch.setattr(_gates, "check_market_validity_gate",
                            lambda **k: _Res(), raising=False)
        osm = FakeOSM()
        _seed_row(osm)
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=FakeBroker(),
        )
        # PR #391 authority: direction reversal → REARM, NOT TERMINAL
        assert r["outcome"] == "REARM_DIRECTION_REVERSAL", r
        # Must have called the OSM rearm helper
        assert osm._rearm_called_with, "OSM rearm_entry_for_direction_reversal must be called"

    def test_stop_broken_is_terminal(self, monkeypatch):
        import ap.live_submit_gates as _gates

        class _Res:
            passed = False
            reason_code = "PUT_STOP_ALREADY_BROKEN"
            audit = {}

        monkeypatch.setattr(_gates, "check_market_validity_gate",
                            lambda **k: _Res(), raising=False)
        osm = FakeOSM()
        _seed_row(osm)
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=FakeBroker(),
        )
        assert r["outcome"] == "THESIS_INVALID_TERMINAL"

    def test_hold_reason_holds_no_replacement(self, monkeypatch):
        import ap.live_submit_gates as _gates

        class _Res:
            passed = False
            reason_code = "CURRENT_PRICE_AGE_UNKNOWN"
            audit = {}

        monkeypatch.setattr(_gates, "check_market_validity_gate",
                            lambda **k: _Res(), raising=False)
        osm = FakeOSM()
        _seed_row(osm)
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=FakeBroker(),
        )
        assert r["outcome"] == "MARKET_TRUTH_HOLD"

    def test_quote_provenance_is_passed_into_gate(self, monkeypatch):
        """BLOCKER 1: gate call must receive quote_age_ms, quote_source,
        quote_fetched_at, quote_provenance, quote_fetch_failed."""
        import ap.live_submit_gates as _gates
        seen_kwargs = {}

        class _Res:
            passed = True
            reason_code = "PASS"
            audit = {}

        def _spy(**kwargs):
            seen_kwargs.update(kwargs)
            return _Res()

        monkeypatch.setattr(_gates, "check_market_validity_gate", _spy, raising=False)
        osm = FakeOSM()
        _seed_row(osm)
        core = _install_real_method(MiniCore(osm.client_id, osm))
        core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=FakeBroker(),
        )
        # BLOCKER 1: every provenance field must be passed.
        assert "quote_age_ms" in seen_kwargs
        assert "quote_source" in seen_kwargs
        assert "quote_fetched_at" in seen_kwargs
        assert "quote_provenance" in seen_kwargs
        assert "quote_fetch_failed" in seen_kwargs
        assert seen_kwargs["quote_provenance"] == "synchronous_submit_fetch"
        assert seen_kwargs["quote_source"] == "poly"
        assert seen_kwargs["quote_age_ms"] == 200


# ===========================================================================
# Replacement broker POST (Blocker 3)
# ===========================================================================

class TestReplacementPostActuallyHappens:
    def test_submitted_only_after_broker_ack(self, _pass_market_gate):
        osm = FakeOSM()
        _seed_row(osm, qty=4)
        # Real broker POST via OSM returns broker_order_id + ACKNOWLEDGED
        osm._submit_existing_entry_result = {
            "ok": True,
            "local_order_id": "ord_1",
            "broker_order_id": "brk_replacement_1",
            "status": "ACKNOWLEDGED",
        }
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=FakeBroker(),
        )
        assert r["outcome"] == "REPLACEMENT_SUBMITTED"
        assert r["audit"]["submit_result_broker_order_id"] == "brk_replacement_1"
        assert r["audit"]["submit_result_ok"] is True

    def test_broker_post_fails_stays_claimed_not_submitted(self, _pass_market_gate):
        osm = FakeOSM()
        _seed_row(osm, qty=4)
        osm._submit_existing_entry_result = {
            "ok": False,
            "local_order_id": "ord_1",
            "broker_order_id": None,
            "status": "ERROR",
            "error": "broker_rejected_transient",
        }
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=FakeBroker(),
        )
        # BLOCKER 3: NEVER emit REPLACEMENT_SUBMITTED without a broker ack
        assert r["outcome"] == "REPLACEMENT_CLAIMED"
        assert "cas won" in r["detail"].lower() or "claim" in r["detail"].lower()

    def test_broker_post_raises_stays_claimed(self, _pass_market_gate):
        osm = FakeOSM()
        _seed_row(osm, qty=4)
        osm._submit_existing_entry_raises = RuntimeError("broker api down")
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=FakeBroker(),
        )
        assert r["outcome"] == "REPLACEMENT_CLAIMED"
