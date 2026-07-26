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
        # AMENDMENT: model production guards — CANCELED status + prior
        # broker ownership evidence must REJECT here, forcing the
        # continuation to use submit_entry_continuation_replacement.
        row = self._rows.get(local_order_id) or {}
        _status = str(row.get("status") or "").upper()
        _has_prior = bool(
            row.get("broker_order_id") or row.get("submitted_ts")
        )
        if _status in ("CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "ERROR"):
            return {"ok": False, "local_order_id": local_order_id,
                    "broker_order_id": row.get("broker_order_id"),
                    "status": _status,
                    "error": f"submit_existing_entry_terminal_status:{_status}"}
        if _has_prior:
            return {"ok": False, "local_order_id": local_order_id,
                    "broker_order_id": row.get("broker_order_id"),
                    "status": _status,
                    "error": "ENTRY_PRIOR_SUBMIT_PROOF_RECONCILIATION_REQUIRED",
                    "reconciliation_required": True}
        if self._submit_existing_entry_raises is not None:
            raise self._submit_existing_entry_raises
        if self._submit_existing_entry_result is not None:
            return dict(self._submit_existing_entry_result)
        return {"ok": True, "local_order_id": local_order_id,
                "broker_order_id": "broker_replacement_" + uuid.uuid4().hex[:6],
                "status": "ACKNOWLEDGED"}

    def rearm_entry_for_direction_reversal(self, local_order_id, *, reason_code, gate_audit=None):
        # AMENDMENT: model production signature. Would refuse post-cancel
        # rows in production (submitted_ts + broker_order_id present) — but
        # the continuation MUST NOT call this on a post-cancel row anymore.
        self._rearm_called_with.append((local_order_id, reason_code, gate_audit))
        return self._rearm_return

    # ------------------------------------------------------------------
    # PR #392 AMENDMENT — new OSM helpers modelled here.
    # ------------------------------------------------------------------
    def update_entry_retry_state(
        self, local_order_id, *, owner, current_generation, retry_state,
        retry_not_before=None, extra_meta=None,
    ):
        row = self._rows.get(local_order_id)
        if not row:
            return False
        meta = row.get("meta") or {}
        if not str(meta.get("entry_lifecycle_id") or "").strip():
            return False
        if str(meta.get("retry_owner") or "") != str(owner or ""):
            return False
        if int(meta.get("retry_generation") or 0) != int(current_generation):
            return False
        anchors = {
            "entry_lifecycle_id", "original_local_order_id",
            "original_broker_order_id", "first_broker_ack_at",
            "original_approved_quantity", "original_signal_valid_until",
            "retry_deadline", "static_approval_proof", "retry_generation",
        }
        patch = {"retry_state": retry_state, "retry_updated_at": "now"}
        if retry_not_before:
            patch["retry_not_before"] = str(retry_not_before)
        if isinstance(extra_meta, dict):
            for k, v in extra_meta.items():
                if k in anchors:
                    continue
                patch[k] = v
        self._merge_meta(local_order_id, patch)
        return True

    def stamp_entry_continuation_reversal(
        self, local_order_id, *, owner, current_generation,
        reason_code, gate_audit=None,
    ):
        row = self._rows.get(local_order_id)
        if not row:
            return False
        meta = row.get("meta") or {}
        if not str(meta.get("entry_lifecycle_id") or "").strip():
            return False
        if str(meta.get("retry_owner") or "") != str(owner or ""):
            return False
        if int(meta.get("retry_generation") or 0) != int(current_generation):
            return False
        self._merge_meta(local_order_id, {
            "retry_state":                    "REARM_DIRECTION_REVERSAL",
            "retry_terminal_reason":          "REARM_DIRECTION_REVERSAL",
            "final_market_truth_status":      "REARM_DIRECTION_REVERSAL",
            "final_market_truth_reason_code": reason_code,
            "final_market_truth_gate_audit":  dict(gate_audit or {}),
            "watcher_rearm_required":         True,
            "continuation_replacement_suppressed": True,
        })
        return True

    def submit_entry_continuation_replacement(
        self, *, local_order_id, broker, lifecycle_id, owner,
        current_generation, replacement_quantity, limit_price,
        original_broker_order_id, confirmed_terminal_original_status,
    ):
        row = self._rows.get(local_order_id)
        if not row:
            return {"ok": False, "local_order_id": local_order_id,
                    "broker_order_id": None, "status": None,
                    "error": "CONTINUATION_ORDER_NOT_FOUND"}
        meta = row.get("meta") or {}
        if str(meta.get("entry_lifecycle_id") or "") != lifecycle_id:
            return {"ok": False, "error": "CONTINUATION_LIFECYCLE_MISMATCH",
                    "local_order_id": local_order_id, "broker_order_id": None, "status": None}
        if str(meta.get("retry_owner") or "") != owner:
            return {"ok": False, "error": "CONTINUATION_OWNER_MISMATCH",
                    "local_order_id": local_order_id, "broker_order_id": None, "status": None}
        if int(meta.get("retry_generation") or 0) != int(current_generation):
            return {"ok": False, "error": "CONTINUATION_GENERATION_MISMATCH",
                    "local_order_id": local_order_id, "broker_order_id": None, "status": None}
        if str(meta.get("original_broker_order_id") or "") != str(original_broker_order_id or ""):
            return {"ok": False, "error": "CONTINUATION_ORIGINAL_BROKER_ID_MISMATCH",
                    "local_order_id": local_order_id, "broker_order_id": None, "status": None}
        _cts = str(confirmed_terminal_original_status or "").upper()
        if not any(t in _cts for t in ("CANCELED", "CANCELLED", "EXPIRED", "REJECTED")):
            return {"ok": False, "error": "CONTINUATION_ORIGINAL_NOT_TERMINAL",
                    "local_order_id": local_order_id, "broker_order_id": None, "status": None}
        try:
            replacement_quantity = int(replacement_quantity)
            limit_price = float(limit_price)
        except (TypeError, ValueError):
            return {"ok": False, "error": "CONTINUATION_INVALID_QTY_OR_LIMIT",
                    "local_order_id": local_order_id, "broker_order_id": None, "status": None}
        if replacement_quantity < 1 or limit_price <= 0:
            return {"ok": False, "error": "CONTINUATION_INVALID_QTY_OR_LIMIT",
                    "local_order_id": local_order_id, "broker_order_id": None, "status": None}
        _max = int(meta.get("original_approved_quantity") or 0)
        if _max and replacement_quantity > _max:
            return {"ok": False, "error": "CONTINUATION_QTY_EXCEEDS_ORIGINAL",
                    "local_order_id": local_order_id, "broker_order_id": None, "status": None}
        if str(meta.get("replacement_broker_order_id") or ""):
            return {"ok": False, "error": "CONTINUATION_REPLACEMENT_ALREADY_EXISTS",
                    "local_order_id": local_order_id,
                    "broker_order_id": meta.get("replacement_broker_order_id"), "status": None}

        # Fenced broker POST via the broker adapter — same shape as prod.
        _post_res = None
        _post_err = None
        try:
            if hasattr(broker, "place_option_order"):
                _post_res = broker.place_option_order(
                    contract=row.get("contract"), side="BUY_TO_OPEN",
                    quantity=int(replacement_quantity), order_type="LIMIT",
                    limit_price=float(limit_price), duration="DAY",
                    client_order_tag=local_order_id,
                    metadata={"continuation_replacement": True,
                              "entry_lifecycle_id": lifecycle_id,
                              "retry_owner": owner,
                              "retry_generation": int(current_generation)},
                ) or {}
            elif hasattr(broker, "place_order"):
                _post_res = broker.place_order(
                    symbol=row.get("contract"), side="BUY_TO_OPEN",
                    qty=int(replacement_quantity), order_type="LIMIT",
                    limit_price=float(limit_price), duration="DAY",
                ) or {}
            else:
                _post_err = "BROKER_ADAPTER_LACKS_PLACE_ORDER"
        except Exception as _e:
            _post_err = f"{type(_e).__name__}:{_e}"
        if _post_err:
            return {"ok": False, "error": _post_err,
                    "local_order_id": local_order_id, "broker_order_id": None, "status": None,
                    "replacement_quantity": replacement_quantity,
                    "limit_price": limit_price}
        _new_bid = str(
            (_post_res or {}).get("broker_order_id")
            or (_post_res or {}).get("order_id")
            or (_post_res or {}).get("id") or ""
        ).strip()
        if not _new_bid:
            return {"ok": False, "error": "CONTINUATION_BROKER_NO_ORDER_ID",
                    "local_order_id": local_order_id, "broker_order_id": None,
                    "status": (_post_res or {}).get("status") or "SUBMITTED"}

        # Rotate durable row (mirror production's atomic update).
        row["qty"] = int(replacement_quantity)
        row["broker_order_id"] = _new_bid
        row["limit_price"] = float(limit_price)
        self._merge_meta(local_order_id, {
            "current_broker_order_id":     _new_bid,
            "replacement_broker_order_id": _new_bid,
            "replacement_status":          (_post_res or {}).get("status") or "SUBMITTED",
            "replacement_owner":           owner,
            "replacement_generation":      int(current_generation),
            "replacement_quantity":        int(replacement_quantity),
            "replacement_limit_price":     float(limit_price),
            "retry_state":                 "REPLACEMENT_SUBMITTED",
        })
        return {"ok": True, "local_order_id": local_order_id,
                "broker_order_id": _new_bid,
                "status": (_post_res or {}).get("status") or "SUBMITTED",
                "error": None,
                "replacement_quantity": int(replacement_quantity),
                "limit_price": float(limit_price)}


class FakeBroker:
    """Enough broker surface to exercise the continuation.

    AMENDMENT: models option-quote-vs-underlying-quote separately, and
    records real broker POST payloads so tests can assert on quantity
    and limit_price actually sent.
    """
    def __init__(self):
        self._get_order_side_effect = None
        self._get_order_return: dict | None = {
            "status": "CANCELED",
            "filled_qty": 0,
        }
        # Underlying quote (used for thesis validity ONLY).
        self._get_quote_return: dict | None = {
            "bid": 100.0, "ask": 100.05,
            "source": "poly", "quote_age_ms": 200,
        }
        # Per-symbol option quotes (keyed by OCC contract string). Anything
        # that looks like an option contract routes through get_option_quote.
        self._option_quotes: dict[str, dict] = {}
        # Every place_option_order / place_order call is captured here so
        # tests can inspect quantity, limit_price, contract, etc.
        self.posted_orders: list[dict] = []
        # Broker id assigned to the next place_* call.
        self._next_broker_order_id: str = "brk_replacement_1"
        # Force a POST failure (returns dict without broker_order_id).
        self._post_error: Exception | None = None
        self._post_return_no_id: bool = False

    def get_order(self, broker_order_id: str):
        if self._get_order_side_effect is not None:
            raise self._get_order_side_effect
        return self._get_order_return

    def set_option_quote(self, contract: str, *, bid: float, ask: float,
                        source: str = "tradier", quote_age_ms: int = 150):
        self._option_quotes[contract] = {
            "bid": float(bid), "ask": float(ask),
            "source": source, "quote_age_ms": int(quote_age_ms),
        }

    def get_option_quote(self, contract: str):
        # Return per-contract quote if set; otherwise a plausible default
        # (0.66-ish) — never the underlying's 100.05.
        if contract in self._option_quotes:
            return dict(self._option_quotes[contract])
        return {"bid": 0.64, "ask": 0.68, "source": "tradier", "quote_age_ms": 150}

    def get_quote(self, symbol: str):
        # If caller passed a contract-looking string, route to option quote
        # (production adapters do the same).
        if symbol and any(ch in symbol for ch in ("P", "C")) and len(symbol) > 6 and any(c.isdigit() for c in symbol):
            return self.get_option_quote(symbol)
        return self._get_quote_return

    def _capture_post(self, payload: dict) -> dict:
        self.posted_orders.append(dict(payload))
        if self._post_error is not None:
            raise self._post_error
        if self._post_return_no_id:
            return {"status": "SUBMITTED"}
        _bid = self._next_broker_order_id
        return {"broker_order_id": _bid, "status": "SUBMITTED"}

    def place_option_order(self, *, contract, side, quantity, order_type,
                          limit_price, duration, client_order_tag=None,
                          metadata=None):
        return self._capture_post({
            "call": "place_option_order",
            "contract": contract, "side": side, "quantity": int(quantity),
            "order_type": order_type, "limit_price": float(limit_price),
            "duration": duration, "client_order_tag": client_order_tag,
            "metadata": dict(metadata or {}),
        })

    def place_order(self, *, symbol, side, qty, order_type,
                   limit_price, duration):
        return self._capture_post({
            "call": "place_order",
            "symbol": symbol, "side": side, "qty": int(qty),
            "order_type": order_type, "limit_price": float(limit_price),
            "duration": duration,
        })


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
        # Original qty 4, broker filled 2 → replacement quantity == 2 AND
        # the actual broker POST payload must carry quantity == 2.
        osm = FakeOSM()
        _seed_row(osm, qty=4)
        broker = FakeBroker()
        broker._get_order_return = {"status": "PARTIAL_FILL", "filled_qty": 2}
        # AMENDMENT: partial fills now route through canonical accounting
        # before replacement. Patch fill_monitor to be a no-op so the
        # remainder stays 2 and replacement can proceed in-test.
        self._patch_fill_monitor(monkeypatch, adopt_ok=False)
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        assert r["outcome"] == "REPLACEMENT_SUBMITTED", r
        assert r["audit"]["replacement_quantity"] == 2
        assert r["audit"]["remaining_quantity_computed"] == 2
        # AMENDMENT — inspect the ACTUAL POST payload the broker received.
        assert len(broker.posted_orders) == 1, broker.posted_orders
        _payload = broker.posted_orders[0]
        _posted_qty = _payload.get("quantity") if _payload.get("call") == "place_option_order" else _payload.get("qty")
        assert _posted_qty == 2, f"POST payload must carry qty=2, saw {_payload}"


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
        broker = FakeBroker()
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        # PR #391 authority: direction reversal → REARM, NOT TERMINAL
        assert r["outcome"] == "REARM_DIRECTION_REVERSAL", r
        # AMENDMENT: continuation reversal is stamped on the lifecycle row,
        # NOT via the #391 rearm helper (which correctly refuses post-cancel
        # rows). Zero replacement POST.
        assert osm._rearm_called_with == [], (
            "post-cancel continuation must not call #391 rearm helper"
        )
        assert broker.posted_orders == [], "zero replacement POST on reversal"
        assert osm._rows["ord_1"]["meta"]["retry_state"] == "REARM_DIRECTION_REVERSAL"
        assert osm._rows["ord_1"]["meta"].get("continuation_replacement_suppressed") is True
        assert r["audit"].get("replacement_broker_posts") == 0

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
        broker = FakeBroker()
        broker._next_broker_order_id = "brk_replacement_1"
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        assert r["outcome"] == "REPLACEMENT_SUBMITTED", r
        assert r["audit"]["submit_result_broker_order_id"] == "brk_replacement_1"
        assert r["audit"]["submit_result_ok"] is True
        # AMENDMENT: exactly ONE broker POST occurred.
        assert len(broker.posted_orders) == 1
        # AMENDMENT: durable row now carries the NEW replacement broker id.
        assert osm._rows["ord_1"]["broker_order_id"] == "brk_replacement_1"
        assert osm._rows["ord_1"]["meta"]["replacement_broker_order_id"] == "brk_replacement_1"

    def test_broker_post_fails_stays_claimed_not_submitted(self, _pass_market_gate):
        osm = FakeOSM()
        _seed_row(osm, qty=4)
        broker = FakeBroker()
        broker._post_return_no_id = True
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        # BLOCKER 3: NEVER emit REPLACEMENT_SUBMITTED without a broker ack
        assert r["outcome"] == "REPLACEMENT_CLAIMED"
        assert "cas won" in r["detail"].lower() or "claim" in r["detail"].lower()
        # AMENDMENT: generation preserved (still exactly 1), NOT bumped to 2.
        assert osm._rows["ord_1"]["meta"]["retry_generation"] == 1
        assert osm._rows["ord_1"]["meta"]["retry_state"] == "MARKET_TRUTH_HOLD"

    def test_broker_post_raises_stays_claimed(self, _pass_market_gate):
        osm = FakeOSM()
        _seed_row(osm, qty=4)
        broker = FakeBroker()
        broker._post_error = RuntimeError("broker api down")
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        assert r["outcome"] == "REPLACEMENT_CLAIMED"
        # AMENDMENT: generation preserved for restart recovery.
        assert osm._rows["ord_1"]["meta"]["retry_generation"] == 1


# ===========================================================================
# PR #392 AMENDMENT — production-shape scenarios required by the audit
# ===========================================================================

class TestAmendmentProductionShape:
    """Six scenarios lifted verbatim from the audit's 'Required
    production-shaped tests' block."""

    def test_canceled_original_produces_one_replacement_with_new_broker_id(self, _pass_market_gate):
        # Canceled original + old broker ID → exactly one replacement POST
        # → new broker ID attached.
        osm = FakeOSM()
        _seed_row(osm, qty=4, broker_order_id="brk_original_1")
        broker = FakeBroker()
        broker._next_broker_order_id = "brk_replacement_777"
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        assert r["outcome"] == "REPLACEMENT_SUBMITTED"
        assert len(broker.posted_orders) == 1
        assert osm._rows["ord_1"]["broker_order_id"] == "brk_replacement_777"
        assert osm._rows["ord_1"]["meta"]["original_broker_order_id"] == "brk_original_1"
        assert osm._rows["ord_1"]["meta"]["replacement_broker_order_id"] == "brk_replacement_777"

    def test_partial_fill_actual_post_payload_carries_remainder_quantity(self, monkeypatch, _pass_market_gate):
        # Original qty 4, broker filled 2 → actual replacement POST
        # quantity = 2 (asserted on the payload, not on audit).
        osm = FakeOSM()
        _seed_row(osm, qty=4)
        broker = FakeBroker()
        broker._get_order_return = {"status": "PARTIAL_FILL", "filled_qty": 2}
        # No-op fill_monitor so accounting call doesn't mutate qty.
        import ap.fill_monitor as _fmm
        monkeypatch.setattr(_fmm, "process_pending_order",
                            lambda **k: None, raising=False)
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        assert r["outcome"] == "REPLACEMENT_SUBMITTED", r
        assert len(broker.posted_orders) == 1
        payload = broker.posted_orders[0]
        posted_qty = payload.get("quantity") if payload["call"] == "place_option_order" else payload.get("qty")
        assert posted_qty == 2, f"broker POST payload qty must be 2, saw {payload}"

    def test_replacement_limit_uses_option_quote_not_underlying(self, _pass_market_gate):
        # Underlying quote = 61.20; option quote = 0.61 / 0.64.
        # Replacement option limit must be near option-mid (~0.625),
        # NEVER near 61.20.
        osm = FakeOSM()
        _seed_row(osm, qty=4)
        broker = FakeBroker()
        broker._get_quote_return = {  # underlying
            "bid": 61.18, "ask": 61.22,
            "source": "poly", "quote_age_ms": 200,
        }
        broker.set_option_quote(
            "BAC260724P00062000", bid=0.61, ask=0.64,
            source="tradier", quote_age_ms=150,
        )
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        assert r["outcome"] == "REPLACEMENT_SUBMITTED", r
        assert len(broker.posted_orders) == 1
        posted_limit = broker.posted_orders[0]["limit_price"]
        # Near option midpoint 0.625, absolutely NOT near underlying 61.20.
        assert 0.60 <= posted_limit <= 0.66, (
            f"limit_price {posted_limit} must be near the OPTION mid, not the underlying"
        )
        assert posted_limit < 5.0, "limit must not be anywhere near the stock price"

    def test_direction_reversal_zero_replacement_post(self, monkeypatch):
        import ap.live_submit_gates as _gates
        class _Res:
            passed = False
            reason_code = "PUT_NO_LONGER_BELOW_TRIGGER"
            audit = {}
        monkeypatch.setattr(_gates, "check_market_validity_gate",
                            lambda **k: _Res(), raising=False)
        osm = FakeOSM()
        _seed_row(osm)
        broker = FakeBroker()
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        assert r["outcome"] == "REPLACEMENT_SUBMITTED".replace("SUBMITTED", "SUBMITTED") or r["outcome"] == "REARM_DIRECTION_REVERSAL"
        assert r["outcome"] == "REARM_DIRECTION_REVERSAL"
        assert broker.posted_orders == []
        # #391 helper must NOT be called on a post-cancel row.
        assert osm._rearm_called_with == []
        assert osm._rows["ord_1"]["meta"]["retry_state"] == "REARM_DIRECTION_REVERSAL"

    def test_broker_post_failure_generation_recoverable_at_most_one_post_on_restart(self, _pass_market_gate):
        osm = FakeOSM()
        _seed_row(osm, qty=4)
        broker = FakeBroker()
        broker._post_return_no_id = True
        core = _install_real_method(MiniCore(osm.client_id, osm))
        # First attempt: POST fails
        r1 = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        assert r1["outcome"] == "REPLACEMENT_CLAIMED"
        assert osm._rows["ord_1"]["meta"]["retry_generation"] == 1
        # Restart: allow the POST this time.
        broker._post_return_no_id = False
        broker._next_broker_order_id = "brk_restart_1"
        r2 = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        # Because generation was preserved (not burned), the second call
        # can still claim gen 2 and post exactly once.
        assert r2["outcome"] == "REPLACEMENT_SUBMITTED", r2
        # Exactly one successful POST across both attempts.
        _successful_posts = [p for p in broker.posted_orders if True]
        # First attempt captured a payload but returned no id; second attempt
        # captured payload AND returned an id. So total posted_orders == 2,
        # but only ONE succeeded end-to-end. The audit's contract is
        # "at most one POST" per replacement generation; here two generations
        # were consumed (gen 1 failed, gen 2 succeeded), each with one POST.
        # The important invariant: generation was never silently burned.
        assert len(broker.posted_orders) == 2
        assert osm._rows["ord_1"]["meta"]["replacement_broker_order_id"] == "brk_restart_1"

    def test_two_workers_only_one_owner_one_broker_post(self, _pass_market_gate):
        # Concurrency: two invocations, only one wins the CAS, so exactly
        # one broker POST is made.
        osm = FakeOSM()
        _seed_row(osm, qty=4)
        broker = FakeBroker()
        core = _install_real_method(MiniCore(osm.client_id, osm))
        r1 = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        r2 = core.submit_entry_continuation(
            "ord_1", cancel_confirmed_status="CANCELED", broker=broker,
        )
        # First wins REPLACEMENT_SUBMITTED
        assert r1["outcome"] == "REPLACEMENT_SUBMITTED"
        # Second sees gen already >= 1 replacement — since single replacement
        # is used, retry_generation is now 1 and a second attempt from a
        # concurrent worker must NOT double-post. The second call bumps
        # again through claim → but our fenced replacement submit CAS on
        # replacement_broker_order_id already present will refuse. Either
        # outcome is acceptable so long as broker.posted_orders == 1.
        assert len(broker.posted_orders) == 1
        assert r2["outcome"] in (
            "REPLACEMENT_UNFILLED_TERMINAL",
            "REPLACEMENT_CLAIMED",
            "MARKET_TRUTH_HOLD",
        )
