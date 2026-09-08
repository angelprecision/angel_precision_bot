"""
tests/test_p0_rearm_watcher_required_routing.py

Focused integration coverage for the REARM_WATCHER_REQUIRED handoff added to
PR #421 (fix/p0-direction-reversal-rearm), amendment "additional mandatory
constraints A-K".

Exercises the REAL production seam end-to-end:

    APStartupRecovery._recover_deferred_breach_lifecycles()
    → resume_deferred_materialization_retry()
    → synthetic watched object
    → real _on_entry_trigger()
    → real rearm_deferred_materialization_direction_reversal()
    → REARM_WATCHER_REQUIRED (exact-generation handoff)
    → ap_recovery.py's REARM_WATCHER_REQUIRED handler
    → PendingTriggerRestartRecovery.recover_one_row() (public entry point)

None of the following are monkeypatched to a prepared outcome — every test
in this module calls them for real:

    APStartupRecovery._recover_deferred_breach_lifecycles
    resume_deferred_materialization_retry
    _on_entry_trigger
    rearm_deferred_materialization_direction_reversal
    PendingTriggerRestartRecovery.recover_one_row

Only external DB transport, quote responses, broker adapters, and watcher
registry storage are faked.

Test classes (constraint I):
    TestQuoteAvailableRealWatcherAttached      — scenario 1
    TestQuoteUnavailableBoundedRetry           — scenario 2
    TestCrashAfterDurableWrite                 — scenario 3
    TestConcurrentGenerationAdvancement        — scenario 4
    TestRepeatedRecoveryPassIsIdempotent       — scenario 5
    TestMutationSurface                        — constraint H
"""
from __future__ import annotations

import json
import os
import threading

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import ap.db as ap_db_mod
import ap.order_state_machine as osm_mod
import ap_recovery as ap_recovery_mod
from ap.order_state_machine import APOrderStateMachine
from ap.pending_trigger_restart_recovery import _RowOutcome
from ap_execution_core import APExecutionCore
from ap_recovery import APStartupRecovery


@pytest.fixture(autouse=True)
def _open_deferred_retry_cutoff_for_lifecycle_tests(monkeypatch):
    """Keep restart-handoff lifecycle tests inside the retry window."""
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "2359")


# ─────────────────────────────────────────────────────────────────────────────
# Shared identity constants
# ─────────────────────────────────────────────────────────────────────────────

LOCAL_ORDER_ID = "order-rwr-001"
SIGNAL_ID = "sig-rwr-001"
CANONICAL_SIGNAL_ID = "canon-sig-rwr-001"
PLAN_ID = "plan-rwr-001"
CLIENT_ID = "jason@example.com"
EXEC_MODE = "paper"
GENERATION = 3           # durable generation before this retry claim
NEW_GEN = GENERATION + 1  # generation after resume_deferred's claim (4)
EXPECTED_ATT = 2
TRIGGER_PRICE = 100.0
TRIGGER_TS = "2026-08-06T14:00:00+00:00"
OWNER = f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:5"


def _select_columns_row(**overrides) -> dict:
    """Row shape matching _recover_deferred_breach_lifecycles' _load() SELECT."""
    base = {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "signal_id": SIGNAL_ID,
        "plan_id": PLAN_ID,
        "kind": "ENTRY",
        "symbol": "SPY",
        "contract": "",
        "direction": "CALL",
        "score": 75.0,
        "tier": "A",
        "trigger_price": TRIGGER_PRICE,
        "stop_underlying": None,
        "target_underlying": None,
        "pattern": "",
        "timeframe": "5m",
        "execution_mode": EXEC_MODE,
        "qty": 1,
        "limit_price": 2.50,
        "reserved_cost": 250.0,
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {},
        "created_ts": datetime.now(timezone.utc).isoformat(),
    }
    base.update(overrides)
    return base


def _due_retry_wait_meta(*, generation=GENERATION, next_retry_at=None) -> dict:
    provenance = {
        "canonical_signal_id": SIGNAL_ID,
        "client_id": CLIENT_ID,
        "execution_mode": EXEC_MODE,
        "local_order_id": LOCAL_ORDER_ID,
    }
    return {
        "lifecycle_state": "RETRY_WAIT",
        "materialization_status": "RETRY_LATER_DATA_UNAVAILABLE",
        "materialization_in_flight": False,
        "materialization_generation": generation,
        "materialization_owner": "",
        "retry_attempt": 1,
        "retry_max_attempts": 5,
        "breach_attempt_count": 1,
        "materialization_attempts": 1,
        "materialization_next_retry_at": (
            next_retry_at
            or (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
        ),
        "trigger_crossed_at": TRIGGER_TS,
        "trigger_crossed_at_provenance": dict(provenance),
        "trigger_confirmed_at": "2026-08-06T14:00:15+00:00",
        "first_breach_bid": 100.5,
        "first_breach_ask": 100.55,
    }


def _materializing_meta(*, generation=NEW_GEN, attempt=EXPECTED_ATT) -> dict:
    provenance = {
        "canonical_signal_id": SIGNAL_ID,
        "client_id": CLIENT_ID,
        "execution_mode": EXEC_MODE,
        "local_order_id": LOCAL_ORDER_ID,
    }
    return {
        "lifecycle_state": "MATERIALIZING",
        "materialization_status": "RUNNING",
        "materialization_in_flight": True,
        "materialization_generation": generation,
        "materialization_owner": OWNER,
        "materialization_lease_until": "2099-01-01T00:00:00+00:00",
        "retry_attempt": attempt,
        "retry_max_attempts": 5,
        "breach_attempt_count": attempt,
        "materialization_attempts": attempt,
        "trigger_crossed_at": TRIGGER_TS,
        "trigger_crossed_at_provenance": dict(provenance),
        "trigger_confirmed_at": "2026-08-06T14:00:15+00:00",
        "first_breach_bid": 100.5,
        "first_breach_ask": 100.55,
        "selector_recovery_cursor_v1": {
            "version": 1,
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID.lower(),
            "execution_mode": EXEC_MODE,
            "signal_id": SIGNAL_ID,
            "materialization_generation": generation,
            "selector_attempt_count": max(1, attempt - 1),
            "attempted_symbols": {},
            "structurally_skipped_symbols": {},
            "expirations_probed": [],
            "last_ranked_index_by_expiration": {},
        },
    }


class _FakeCursor:
    """Shared fake cursor for both ap.db and ap.order_state_machine conn()."""

    def __init__(self, select_rows=None, rowcount=1, on_write=None):
        self._select_rows = select_rows if select_rows is not None else []
        self.rowcount = rowcount
        self._on_write = on_write
        self.last_sql = ""
        self.last_params = None

    def execute(self, sql, params=None):
        self.last_sql = sql
        self.last_params = params
        _sql_upper = sql.strip().upper()
        if _sql_upper.startswith("SELECT"):
            pass  # fetchall() returns self._select_rows
        else:
            if self._on_write:
                self._on_write(sql, params)
        return self

    def fetchall(self):
        return list(self._select_rows)

    def fetchone(self):
        return self._select_rows[0] if self._select_rows else None


class _FakeConnCtx:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self._cursor

    def __exit__(self, *_exc):
        return False


class _FakeWatched:
    def __init__(self, signal, state):
        self.signal = signal
        self.state = state
        self._ownership_quarantine = False
        # PR #421 final amendment: real WatchedSignal stamps an immutable
        # _registration_token (uuid4) at construction — the exact-
        # registration identity ap_recovery.py's rollback fencing and
        # PTR's provenance reporting key off. This fake must carry the
        # same contract so tests exercising those real production code
        # paths against this double behave identically to production.
        import uuid as _uuid
        self._registration_token = _uuid.uuid4().hex


class _FakeWatcher:
    """Faithful-enough fake of APEntryWatcher for registry-proof purposes."""

    def __init__(self, owner_token="watcher-token-from-ptr"):
        self._pending = []
        self._dedup_set = set()
        # Real APEntryWatcher.watch() stamps signal_dict["watcher_token"] =
        # self.owner_token — a process-level token identifying this watcher
        # instance. ap_recovery.py's ownership-adoption step reads this
        # attribute directly to durably transfer ownership.
        self.owner_token = owner_token

    def has_order(self, oid):
        return any(
            str((getattr(w, "signal", {}) or {}).get("local_order_id") or "") == oid
            for w in self._pending
        )

    def prove_materialization_retry_owner(self, *a, **kw):
        return {"proven": False, "reason_code": "NO_MATCH"}

    def prove_restart_rearm_retry_owner(self, *a, **kw):
        return None

    def watch(self, plan, local_oid=None, recovery_rearm=False,
              registration_provenance_out=None, **kw):
        if registration_provenance_out is not None:
            registration_provenance_out["created_by_this_call"] = False
            registration_provenance_out["registration_token"] = None
        _sig = {
            "local_order_id": str(local_oid or ""),
            "signal_id": str(getattr(plan, "signal_id", "") or ""),
            "client_id": str(getattr(plan, "client_id", "") or "").strip().lower(),
            "execution_mode": str(
                getattr(plan, "execution_mode", "") or ""
            ).strip().lower(),
        }
        # Faithful to real APEntryWatcher.watch()'s recovery_rearm
        # "RECOVERY_REARM_LEFT_ALONE" path: if a matching watcher already
        # owns this exact row, watch() returns True WITHOUT creating a
        # new registration or reporting any creation provenance — success
        # here proves ownership, not authorship.
        for _existing in self._pending:
            _esig = getattr(_existing, "signal", {}) or {}
            if (
                str(_esig.get("local_order_id") or "") == _sig["local_order_id"]
                and str(_esig.get("signal_id") or "") == _sig["signal_id"]
                and str(_esig.get("client_id") or "") == _sig["client_id"]
                and str(_esig.get("execution_mode") or "") == _sig["execution_mode"]
            ):
                return True
        _watched = _FakeWatched(_sig, "REARM")
        self._pending.append(_watched)
        if _sig["signal_id"]:
            self._dedup_set.add(_sig["signal_id"])
        if registration_provenance_out is not None:
            registration_provenance_out["created_by_this_call"] = True
            registration_provenance_out["registration_token"] = (
                _watched._registration_token
            )
        return True

    def dedup_held(self, oid, **kw):
        return False


def _build_harness(monkeypatch, *, due_meta_overrides=None, quote_bid=98.0,
                    quote_ask=98.5, quote_fail=False):
    """Construct a real APStartupRecovery + APExecutionCore wired together,
    with exactly one due RETRY_WAIT row backing the orders table SELECT, and
    a real (unmocked) rearm SQL path against a shared in-memory row store.

    Returns (recovery, execution_core, fake_osm, row_store, fake_watcher,
             fake_selector, broker_calls) for assertion use.
    """
    due_meta = _due_retry_wait_meta(generation=GENERATION)
    if due_meta_overrides:
        due_meta.update(due_meta_overrides)
    due_row = _select_columns_row(meta=due_meta)

    # ── Shared mutable row store: SELECT reads reflect the latest write ────
    row_store = {"row": dict(due_row)}

    def _on_orders_write(sql, params):
        # The rearm SQL and update_order_meta writes are captured via OSM's
        # own methods below (which mutate row_store["row"]["meta"]); this
        # hook exists for the ap.db-level orders SELECT path only, so it's
        # a no-op unless a caller writes raw SQL through ap.db.conn (none do
        # in this flow — all writes go through ap.order_state_machine.conn).
        pass

    def _select_cursor():
        return _FakeCursor(select_rows=[dict(row_store["row"])])

    monkeypatch.setattr(ap_db_mod, "conn", lambda: _FakeConnCtx(_select_cursor()))
    monkeypatch.setattr(ap_db_mod, "run_with_retry", lambda fn: fn())

    # ── Real OSM instance backed by the shared row store ────────────────────
    real_osm = object.__new__(APOrderStateMachine)
    real_osm.client_id = CLIENT_ID

    def _osm_get_order(order_id):
        if str(order_id) != LOCAL_ORDER_ID:
            return None
        return dict(row_store["row"])

    def _osm_update_order_meta(order_id, patch, **_kwargs):
        if str(order_id) != LOCAL_ORDER_ID:
            return False
        row_store["row"]["meta"] = {**row_store["row"].get("meta", {}), **patch}
        return True

    def _osm_claim_deferred_materialization(order_id, *, owner, new_generation,
                                             lease_until, trigger_crossed_at,
                                             trigger_price, observed_underlying_price,
                                             signal_id, execution_mode, retry_attempt,
                                             generation=None,
                                             advance_retry_attempt=True,
                                             advance_after_market_truth=False):
        if str(order_id) != LOCAL_ORDER_ID:
            return False
        current = dict(row_store["row"].get("meta") or {})
        if advance_after_market_truth:
            if (
                current.get("materialization_owner") != owner
                or current.get("materialization_generation") != new_generation
                or current.get("materialization_market_truth_pending") is not True
            ):
                return False
            current.update({
                "retry_attempt": retry_attempt,
                "breach_attempt_count": retry_attempt,
                "materialization_attempts": retry_attempt,
                "materialization_market_truth_pending": False,
            })
            row_store["row"]["meta"] = current
            return True
        claimed_attempt = (
            retry_attempt
            if advance_retry_attempt
            else int(current.get("retry_attempt") or 0)
        )
        row_store["row"]["meta"] = _materializing_meta(
            generation=new_generation, attempt=claimed_attempt,
        )
        row_store["row"]["meta"].update({
            "materialization_owner": owner,
            "current_owner": owner,
            "watcher_token": owner,
            "materialization_market_truth_pending": not advance_retry_attempt,
        })
        return True

    real_osm.get_order = _osm_get_order
    real_osm.update_order_meta = _osm_update_order_meta
    real_osm.claim_deferred_materialization = _osm_claim_deferred_materialization
    real_osm.persist_selector_recovery_cursor = MagicMock(return_value=True)
    real_osm.terminalize_materialization_retry = MagicMock(return_value=True)

    # ── Real rearm SQL path: patch osm_mod.conn/run_with_retry so the real
    # rearm_deferred_materialization_direction_reversal() executes its real
    # SQL-shaped JSONB patch construction, applying it to the shared row.
    # Exact removal key list from rearm_deferred_materialization_direction_reversal's
    # real SQL `- 'key'` clauses (order_state_machine.py), applied AFTER the
    # jsonb merge — independent of what values the JSON patch itself carries.
    _REARM_SQL_REMOVED_KEYS = (
        "selector_recovery_cursor_v1",
        "trigger_crossed_at",
        "trigger_crossed_at_provenance",
        "triggered_at",
        "trigger_confirmed_at",
        "last_confirmed_trigger_at",
        "original_trigger_crossed_at",
        "first_breach_bid",
        "first_breach_ask",
        "last_trigger_confirmation_quote",
    )

    def _rearm_write(sql, params):
        if params and isinstance(params[0], str):
            try:
                patch = json.loads(params[0])
            except Exception:
                return
            _cur_meta = dict(row_store["row"].get("meta") or {})
            # Archival object (jsonb_strip_nulls(jsonb_build_object(...))):
            # preserve first-trigger evidence under the archived key names
            # before applying the patch, mirroring the real SQL's merge order.
            _archive = {
                "first_trigger_crossed_at": (
                    _cur_meta.get("first_trigger_crossed_at")
                    or _cur_meta.get("trigger_crossed_at")
                    or _cur_meta.get("triggered_at")
                ),
                "first_trigger_crossed_at_provenance": (
                    _cur_meta.get("first_trigger_crossed_at_provenance")
                    or _cur_meta.get("trigger_crossed_at_provenance")
                ),
                "first_trigger_confirmed_at": (
                    _cur_meta.get("first_trigger_confirmed_at")
                    or _cur_meta.get("trigger_confirmed_at")
                    or _cur_meta.get("last_confirmed_trigger_at")
                ),
                "first_trigger_breach_bid": (
                    _cur_meta.get("first_trigger_breach_bid")
                    or _cur_meta.get("first_breach_bid")
                ),
                "first_trigger_breach_ask": (
                    _cur_meta.get("first_trigger_breach_ask")
                    or _cur_meta.get("first_breach_ask")
                ),
            }
            _archive = {k: v for k, v in _archive.items() if v is not None}
            # Merge order: meta || patch || archive, then remove fixed keys.
            _cur_meta.update(patch)
            _cur_meta.update(_archive)
            for _rm_key in _REARM_SQL_REMOVED_KEYS:
                _cur_meta.pop(_rm_key, None)
            row_store["row"]["meta"] = _cur_meta

    def _rearm_cursor():
        return _FakeCursor(rowcount=1, on_write=_rearm_write)

    monkeypatch.setattr(osm_mod, "conn", lambda: _FakeConnCtx(_rearm_cursor()))
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())

    # ── Fake market data ─────────────────────────────────────────────────
    fake_db = MagicMock()
    fake_db.cfg.base_url = "https://api.tradier.com"
    if quote_fail:
        fake_db.get_quote.side_effect = RuntimeError("quote transport down")
    else:
        fake_db.get_quote.return_value = {
            "bid": quote_bid, "ask": quote_ask,
            "provider_timestamp": datetime.now(timezone.utc).isoformat(),
        }
    fake_selector = MagicMock()
    fake_selector.data_broker = fake_db

    broker_calls = {"post": [], "cancel": []}
    fake_broker = MagicMock()
    fake_broker.post.side_effect = lambda *a, **k: broker_calls["post"].append((a, k)) or {}
    fake_broker.cancel_order.side_effect = (
        lambda *a, **k: broker_calls["cancel"].append((a, k)) or {}
    )

    # ── Real APExecutionCore ────────────────────────────────────────────
    core = object.__new__(APExecutionCore)
    core.client_id = CLIENT_ID
    core.email = CLIENT_ID
    core.execution_mode = EXEC_MODE
    core.mode = "PAPER"
    core.paper = True
    core._kill_switch = False
    core.master_control = None
    core.store = MagicMock()
    core._max_positions = 5
    core.position_manager = MagicMock()
    core.position_manager.snapshot.return_value = {
        "open_count": 0, "pending_entries": 0,
    }
    core.order_state_machine = real_osm
    core.contract_selector = fake_selector
    core.broker = fake_broker
    core._pos_lock = threading.Lock()
    core._position_count = 0

    fake_watcher = _FakeWatcher()

    fake_mc = SimpleNamespace(mode="PAPER")

    recovery = APStartupRecovery(
        client_id=CLIENT_ID,
        broker=fake_broker,
        osm=real_osm,
        pm=MagicMock(),
        master_control=fake_mc,
        entry_watcher=fake_watcher,
        execution_core=core,
    )

    return recovery, core, real_osm, row_store, fake_watcher, fake_selector, broker_calls


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 1: quote available → real watcher attached
# ─────────────────────────────────────────────────────────────────────────────

class TestQuoteAvailableRealWatcherAttached:
    def test_real_watcher_attached_end_to_end(self, monkeypatch):
        # ask=98.5 < trigger=100.0 → CALL_NO_LONGER_ABOVE_TRIGGER →
        # REARM_DIRECTION_REVERSAL → REARM_WATCHER_REQUIRED. The PTR quote
        # check (a SEPARATE quote read from the market-truth check above)
        # reports "not yet through trigger" so PTR proceeds to rearm+verify.
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_available(broker, symbol, side, trigger):
            return False  # price below trigger, not yet through

        # PendingTriggerRestartRecovery is constructed fresh inside the
        # ap_recovery.py handler with self.entry_watcher / self.broker /
        # a default quote_check_fn. To control quote_check_fn for this
        # scenario, patch the handler's PTR construction indirectly by
        # patching the module-level default used when none is supplied.
        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_available,
        )

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        final_meta = row_store["row"]["meta"]
        real_token = watcher.owner_token

        # entry_watcher.has_order(local_order_id) == True
        assert watcher.has_order(LOCAL_ORDER_ID) is True
        assert len(watcher._pending) == 1
        assert watcher._pending[0].signal["local_order_id"] == LOCAL_ORDER_ID
        assert watcher._pending[0].signal["signal_id"] == SIGNAL_ID

        # Durable ownership must be truthfully transferred to the real
        # watcher token — not left split between runtime and durable state.
        assert final_meta.get("current_owner") == real_token
        assert final_meta.get("watcher_token") == real_token
        assert final_meta.get("recovery_owner") == ""
        assert final_meta.get("recovery_ownership") == ""
        assert final_meta.get("direction_reversal_rearm_requires_watcher") is False
        assert final_meta.get("materialization_status") == "WAITING_FOR_TRIGGER"

        # No stale selector cursor, retry counters, or retry schedule survive.
        assert "selector_recovery_cursor_v1" not in final_meta
        assert final_meta.get("retry_attempt") == 0
        assert final_meta.get("breach_attempt_count") == 0
        assert final_meta.get("materialization_attempts") == 0
        assert not final_meta.get("materialization_next_retry_at")

        # Exact identity preserved.
        assert row_store["row"]["client_id"] == CLIENT_ID
        assert row_store["row"]["execution_mode"] == EXEC_MODE

        assert not result.get("errors"), f"expected no errors, got {result['errors']}"
        # Zero broker calls anywhere in this flow — the next genuine breach
        # (not exercised by this recovery pass) is what starts selector
        # attempt 1, never this rearm/adoption handoff itself.
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []
        assert not selector.select.called
        assert not selector.select_contract.called


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 2: quote unavailable → bounded restart-rearm retry persisted
# ─────────────────────────────────────────────────────────────────────────────

class TestQuoteUnavailableBoundedRetry:
    def test_unavailable_quote_persists_exact_bounded_retry(self, monkeypatch):
        """When the quote is unavailable, PendingTriggerRestartRecovery's
        existing bounded restart-rearm retry must be durably established —
        not merely "no watcher and no crash". Assert the exact canonical
        retry fields/owner it writes, proving this is a real, executable
        bounded retry a later pass can pick up.
        """
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_unavailable(broker, symbol, side, trigger):
            return None  # unavailable

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_unavailable,
        )

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        final_meta = row_store["row"]["meta"]

        # No watcher attached.
        assert len(watcher._pending) == 0
        assert watcher.has_order(LOCAL_ORDER_ID) is False

        # The exact canonical restart-rearm retry fields PTR's
        # _enter_restart_rearm_retry() writes — proving this is a real,
        # identity-bound, executable bounded retry, not a placeholder.
        assert final_meta.get("restart_rearm_status") == "RETRY_PENDING"
        _expected_owner_prefix = f"restart_rearm:{CLIENT_ID}:{EXEC_MODE}:"
        assert str(final_meta.get("restart_rearm_owner") or "").startswith(
            _expected_owner_prefix
        ), (
            f"restart_rearm_owner must be identity-bound to this exact "
            f"client/mode/order, got {final_meta.get('restart_rearm_owner')!r}"
        )
        assert LOCAL_ORDER_ID in str(final_meta.get("restart_rearm_owner") or "")
        assert final_meta.get("restart_rearm_attempt") == 1
        assert final_meta.get("restart_rearm_next_at")
        assert final_meta.get("restart_rearm_deadline")
        assert final_meta.get("restart_rearm_reason")

        # Never appears as UNRESOLVED-because-no-watcher: the outcome IS a
        # durable bounded retry, not an absence of resolution.
        assert final_meta.get("restart_rearm_status") != ""

        # No fabricated watcher ownership.
        assert not str(final_meta.get("watcher_token") or "").strip()
        assert not str(final_meta.get("current_owner") or "").strip() or (
            final_meta.get("current_owner") == final_meta.get("restart_rearm_owner")
        )

        # No deferred materialization retry was (re)scheduled — this is the
        # restart-rearm retry subtype, not the materialization retry.
        assert not final_meta.get("materialization_next_retry_at")
        assert not final_meta.get("materialization_in_flight")

        # Generation was not incorrectly incremented by the bounded-retry
        # path itself — it must equal the post-rearm generation exactly.
        assert final_meta.get("materialization_generation") == NEW_GEN

        assert not selector.select.called
        assert not selector.select_contract.called
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 3: crash immediately after the durable OSM rearm write
# ─────────────────────────────────────────────────────────────────────────────

class TestCrashAfterDurableWrite:
    def test_fresh_recovery_instance_recovers_via_real_orphan_scan(
        self, monkeypatch,
    ):
        """Boundary: real rearm_deferred_materialization_direction_reversal()
        commits durably, then the process dies before REARM_WATCHER_REQUIRED
        is ever consumed — no in-memory callback result survives. A FRESH
        APStartupRecovery instance must still recover the row purely from
        durable state, via the real production path for exactly this row
        shape: _reseed_watchers()'s orphaned-PENDING_TRIGGER scan, which
        already calls PendingTriggerRestartRecovery.recover_one_row() for
        kind=ENTRY/status=PENDING_TRIGGER/no-broker/no-submit rows. This is
        not a new call site — it is pre-existing production code being
        exercised for real, not a prepared/mocked outcome.
        """
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        # ── Step 1: perform the REAL rearm, durably, via the real OSM/SQL
        # path already wired by _build_harness. ────────────────────────────
        real_owner = f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:{NEW_GEN + 1}"
        # Advance the row to MATERIALIZING first (the rearm's CAS WHERE
        # clause requires this), mirroring what resume_deferred's claim
        # would have done.
        row_store["row"]["meta"] = _materializing_meta(
            generation=NEW_GEN, attempt=EXPECTED_ATT,
        )
        row_store["row"]["meta"]["materialization_owner"] = real_owner

        ok = osm.rearm_deferred_materialization_direction_reversal(
            LOCAL_ORDER_ID,
            owner=real_owner,
            watcher_token="",  # synthetic recovery — no real watcher yet
            generation=NEW_GEN,
            signal_id=SIGNAL_ID,
            execution_mode=EXEC_MODE,
            market_truth_audit={"reason": "CALL_NO_LONGER_ABOVE_TRIGGER"},
        )
        assert ok is True, "the real durable rearm must succeed"

        # ── Step 2: post-write durable state must prove the exact corrected
        # semantics before anything else happens. ──────────────────────────
        post_write_meta = row_store["row"]["meta"]
        assert post_write_meta["lifecycle_state"] == ""
        assert post_write_meta["materialization_status"] == ""
        assert post_write_meta["current_owner"] == ""
        assert post_write_meta["watcher_token"] == ""
        assert post_write_meta["recovery_owner"] == real_owner
        assert post_write_meta["recovery_ownership"] == "recovery_scheduler"
        assert post_write_meta["direction_reversal_rearm_requires_watcher"] is True

        # ── Step 3: discard all process/callback state. The row_store dict
        # itself stands in for durable Postgres state surviving a crash;
        # nothing else from this point on may be reused. ───────────────────
        durable_row = dict(row_store["row"])
        durable_row["kind"] = "ENTRY"
        durable_row["filled_ts"] = None

        # ── Step 4: wire ap.db.conn/run_with_retry so _reseed_watchers()'s
        # real SQL (_reset() UPDATE trade_queue, _load_orphaned_pending_
        # trigger_orders() SELECT orders LEFT JOIN trade_queue) reflects
        # exactly this one durable row and nothing else.
        def _reseed_cursor():
            _cur = _FakeCursor(select_rows=[dict(durable_row)])

            def _exec(sql, params=None):
                _sql_upper = sql.strip().upper()
                if _sql_upper.startswith("UPDATE TRADE_QUEUE"):
                    _cur.rowcount = 0  # no WATCHING queue row to reset
                    _cur._select_rows = []
                elif _sql_upper.startswith("SELECT"):
                    _cur._select_rows = [dict(durable_row)]
                return _cur

            _cur.execute = _exec
            return _cur

        monkeypatch.setattr(
            ap_db_mod, "conn", lambda: _FakeConnCtx(_reseed_cursor()),
        )
        monkeypatch.setattr(ap_db_mod, "run_with_retry", lambda fn: fn())

        # ── Step 5: a fresh APStartupRecovery instance — proves no reliance
        # on any previous instance's in-memory state or the original
        # REARM_WATCHER_REQUIRED result object. ─────────────────────────────
        fresh_recovery = APStartupRecovery(
            client_id=CLIENT_ID,
            broker=recovery.broker,
            osm=osm,
            pm=MagicMock(),
            master_control=SimpleNamespace(mode="PAPER"),
            entry_watcher=watcher,
            execution_core=core,
        )

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check",
            lambda broker, symbol, side, trigger: False,  # quote available
        )

        result = {"errors": []}
        fresh_recovery._reseed_watchers(result)

        # ── Step 6: PR #421 Blocker 1 — WATCHER_OWNED alone is not
        # sufficient for a direction-reversal watcher-required row.
        # rearmed may only increment after durable ownership has
        # actually converged to the real watcher token: current_owner/
        # watcher_token = the exact token, recovery fields cleared,
        # materialization_status="WAITING_FOR_TRIGGER". A runtime watcher
        # alone (watcher_attached True) with the durable row still
        # showing recovery ownership must NOT count as success.
        watcher_attached = watcher.has_order(LOCAL_ORDER_ID)
        retry_meta = row_store["row"].get("meta") or {}
        bounded_retry_persisted = (
            str(retry_meta.get("restart_rearm_status") or "").upper()
            == "RETRY_PENDING"
        )
        assert watcher_attached or bounded_retry_persisted, (
            f"fresh recovery must attach a real watcher or persist a "
            f"bounded restart-rearm retry from durable state alone; "
            f"got watcher_attached={watcher_attached} "
            f"bounded_retry_persisted={bounded_retry_persisted} "
            f"meta={retry_meta}"
        )
        if watcher_attached:
            _real_token = watcher.owner_token
            assert retry_meta.get("current_owner") == _real_token, (
                "durable current_owner must equal the exact real watcher "
                "token before this counts as a successful startup rearm"
            )
            assert retry_meta.get("watcher_token") == _real_token
            assert retry_meta.get("recovery_owner") == ""
            assert retry_meta.get("recovery_ownership") == ""
            assert (
                retry_meta.get("direction_reversal_rearm_requires_watcher")
                is False
            )
            assert retry_meta.get("materialization_status") == (
                "WAITING_FOR_TRIGGER"
            )
            assert result.get("pending_trigger_watchers_rearmed") == 1, (
                "rearmed must not increment until complete durable "
                "convergence is proven"
            )
        assert row_store["row"]["local_order_id"] == LOCAL_ORDER_ID
        assert row_store["row"]["client_id"] == CLIENT_ID
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []


# ─────────────────────────────────────────────────────────────────────────────
# PR #421 Blocker 1 — fresh startup _reseed_watchers() must durably adopt
# watcher ownership, not merely count WATCHER_OWNED as success
# ─────────────────────────────────────────────────────────────────────────────


def _build_reseed_scenario(monkeypatch, *, quote_bid=98.0, quote_ask=98.5):
    """Shared setup for the startup-reseed durable-adoption regressions:
    a real durable rearm to the direction-reversal recovery-owned,
    no-watcher state, wired into a fresh APStartupRecovery's real
    _reseed_watchers() SQL path. Returns (fresh_recovery, osm, row_store,
    watcher, selector, broker_calls).
    """
    recovery, core, osm, row_store, watcher, selector, broker_calls = (
        _build_harness(monkeypatch, quote_bid=quote_bid, quote_ask=quote_ask)
    )

    real_owner = f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:{NEW_GEN + 1}"
    row_store["row"]["meta"] = _materializing_meta(
        generation=NEW_GEN, attempt=EXPECTED_ATT,
    )
    row_store["row"]["meta"]["materialization_owner"] = real_owner

    ok = osm.rearm_deferred_materialization_direction_reversal(
        LOCAL_ORDER_ID,
        owner=real_owner,
        watcher_token="",
        generation=NEW_GEN,
        signal_id=SIGNAL_ID,
        execution_mode=EXEC_MODE,
        market_truth_audit={"reason": "CALL_NO_LONGER_ABOVE_TRIGGER"},
    )
    assert ok is True, "setup: the real durable rearm must succeed"
    assert row_store["row"]["meta"]["direction_reversal_rearm_requires_watcher"] is True

    durable_row = dict(row_store["row"])
    durable_row["kind"] = "ENTRY"
    durable_row["filled_ts"] = None

    def _reseed_cursor():
        _cur = _FakeCursor(select_rows=[dict(durable_row)])

        def _exec(sql, params=None):
            _sql_upper = sql.strip().upper()
            if _sql_upper.startswith("UPDATE TRADE_QUEUE"):
                _cur.rowcount = 0
                _cur._select_rows = []
            elif _sql_upper.startswith("SELECT"):
                _cur._select_rows = [dict(durable_row)]
            return _cur

        _cur.execute = _exec
        return _cur

    monkeypatch.setattr(
        ap_db_mod, "conn", lambda: _FakeConnCtx(_reseed_cursor()),
    )
    monkeypatch.setattr(ap_db_mod, "run_with_retry", lambda fn: fn())

    fresh_recovery = APStartupRecovery(
        client_id=CLIENT_ID,
        broker=recovery.broker,
        osm=osm,
        pm=MagicMock(),
        master_control=SimpleNamespace(mode="PAPER"),
        entry_watcher=watcher,
        execution_core=core,
    )

    import ap.pending_trigger_restart_recovery as ptr_mod
    monkeypatch.setattr(
        ptr_mod, "_default_quote_check",
        lambda broker, symbol, side, trigger: False,
    )

    return fresh_recovery, osm, row_store, watcher, selector, broker_calls


def test_startup_cleanup_preserves_stale_active_materializer(monkeypatch):
    """Age-based startup cleanup must not terminalize an active owner."""
    recovery, _core, osm, row_store, _watcher, _selector, broker_calls = (
        _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
    )
    row_store["row"]["created_ts"] = (
        datetime.now(timezone.utc) - timedelta(days=4)
    ).isoformat()
    row_store["row"]["meta"] = _materializing_meta()
    osm.terminalize_deferred_breach = MagicMock(return_value=True)

    result = {"errors": []}
    recovery._recover_deferred_breach_lifecycles(result)

    osm.terminalize_deferred_breach.assert_not_called()
    assert result["materialization_in_flight_rows"] == 1
    assert broker_calls["post"] == []
    assert broker_calls["cancel"] == []


@pytest.mark.parametrize(
    "mutate,label",
    [
        (lambda m: m.update({"materialization_owner": ""}), "owner_missing"),
        (lambda m: m.pop("materialization_generation", None), "generation_missing"),
        (lambda m: m.update({"materialization_generation": 0}), "generation_zero"),
        (lambda m: m.update({"materialization_generation": -1}), "generation_negative"),
        (lambda m: m.update({"materialization_generation": True}), "generation_bool"),
        (lambda m: m.pop("materialization_lease_until", None), "lease_missing"),
        (
            lambda m: m.update({"materialization_lease_until": "not-a-date"}),
            "lease_malformed",
        ),
        (
            lambda m: m.update({"materialization_lease_until": "1999-01-01T00:00:00+00:00"}),
            "lease_expired",
        ),
        (
            lambda m: m.update({"materialization_lease_until": "2099-01-01T00:00:00"}),
            "lease_naive",
        ),
        (lambda m: m.update({"materialization_in_flight": "true"}), "in_flight_string"),
        (lambda m: m.update({"materialization_status": "QUEUED"}), "status_queued"),
        (lambda m: m.update({"lifecycle_state": "BROKER_READY"}), "lifecycle_broker_ready"),
        (lambda m: m.update({"materialization_outcome": "TERMINAL_ERROR"}), "terminal_outcome"),
        (lambda m: m.update({"broker_ready": True}), "broker_ready_true"),
        (lambda m: m.update({"submit_intent_at": "2026-08-27T12:00:00+00:00"}), "submit_intent"),
    ],
)
def test_startup_cleanup_does_not_protect_partial_or_malformed_materialization(
    monkeypatch, mutate, label
):
    """
    Amendment negative-control gate for the retained ap_recovery startup guard.
    Rows with partial/malformed/expired materialization proof must NOT enter
    the materialization_in_flight_rows bucket — they must fall through to
    the existing 72h aging / terminal-lifecycle authority. This is the exact
    guarantee the amendment demands: a crashed process leaving one stale
    RUNNING/QUEUED marker MUST NOT become immortal.
    """
    recovery, _core, osm, row_store, _watcher, _selector, broker_calls = (
        _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
    )
    # 4 days old — well past the 72h stale cutoff.  Without full canonical
    # proof, the row must be eligible for the existing terminalize path.
    row_store["row"]["created_ts"] = (
        datetime.now(timezone.utc) - timedelta(days=4)
    ).isoformat()
    meta = _materializing_meta()
    mutate(meta)
    row_store["row"]["meta"] = meta

    result = {"errors": []}
    recovery._recover_deferred_breach_lifecycles(result)

    # Whatever the outcome (terminalized, reconciled, or otherwise handled by
    # pre-existing authority), the amendment forbids this row from claiming
    # active-materializer protection.  Zero broker mutations must still hold.
    assert result.get("materialization_in_flight_rows", 0) == 0, (
        f"partial/malformed shape {label!r} incorrectly received "
        "active-materializer protection — this is exactly the immortality "
        "regression the amendment prohibits"
    )
    assert broker_calls["post"] == []
    assert broker_calls["cancel"] == []


class TestStartupFreshWatcherAdoptionSucceeds:
    """Test A — PTR creates a real watcher for a direction-reversal
    watcher-required row; durable adoption CAS succeeds; the reread
    confirms convergence. Only then does rearmed increment."""

    def test_fresh_watcher_adoption_and_verification_succeed(self, monkeypatch):
        fresh_recovery, osm, row_store, watcher, selector, broker_calls = (
            _build_reseed_scenario(monkeypatch)
        )

        result = {"errors": []}
        fresh_recovery._reseed_watchers(result)

        assert watcher.has_order(LOCAL_ORDER_ID) is True
        _real_token = watcher.owner_token
        _meta = row_store["row"]["meta"]
        assert _meta["current_owner"] == _real_token
        assert _meta["watcher_token"] == _real_token
        assert _meta["watcher_generation"] == NEW_GEN
        assert _meta["recovery_owner"] == ""
        assert _meta["recovery_ownership"] == ""
        assert _meta["direction_reversal_rearm_requires_watcher"] is False
        assert _meta["materialization_status"] == "WAITING_FOR_TRIGGER"
        assert result.get("pending_trigger_watchers_rearmed") == 1

        assert not selector.select.called
        assert not selector.select_contract.called
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []


class TestStartupFreshWatcherAdoptionCASLoses:
    """Test B — PTR creates a real watcher; durable adoption CAS returns
    False. Startup must not count success, and only the watcher THIS
    startup attempt created may be rolled back."""

    def test_adoption_cas_loss_rolls_back_only_own_registration(
        self, monkeypatch,
    ):
        fresh_recovery, osm, row_store, watcher, selector, broker_calls = (
            _build_reseed_scenario(monkeypatch)
        )

        monkeypatch.setattr(
            osm, "adopt_direction_reversal_watcher_ownership",
            lambda *a, **kw: False,
        )

        result = {"errors": []}
        fresh_recovery._reseed_watchers(result)

        # This attempt's own registration was rolled back — no orphaned
        # runtime watcher left dangling against a recovery-owned row, and
        # no broad/other watcher cleanup occurred (only one entry could
        # ever have existed here).
        assert watcher.has_order(LOCAL_ORDER_ID) is False
        assert len(watcher._pending) == 0
        assert result.get("pending_trigger_watchers_rearmed") in (0, None)
        assert any(
            "startup_reseed_ownership_adoption_failed" in e
            for e in result.get("errors", [])
        )
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []


class TestStartupPreExistingWatcherNeverEvicted:
    """Test C — a watcher already existed before this startup invocation
    (PTR's read-only fast path returns WATCHER_OWNED with zero
    registration). If durable adoption then fails, the pre-existing
    watcher must never be evicted — this invocation did not create it.

    Prior version of this test seeded a pre-existing watcher but never
    proved the adoption CAS was actually reached — the early
    entry_watcher.has_order() fast path in _reseed_watchers() would
    short-circuit past PTR entirely before this amendment, so the test
    passed without exercising the behavior it was named for. Fixed by
    asserting the adoption CAS call count directly.
    """

    def test_preexisting_watcher_reaches_adoption_cas_and_survives_loss(
        self, monkeypatch,
    ):
        fresh_recovery, osm, row_store, watcher, selector, broker_calls = (
            _build_reseed_scenario(monkeypatch)
        )

        _preexisting_sig = {
            "local_order_id": LOCAL_ORDER_ID,
            "signal_id": SIGNAL_ID,
            "client_id": CLIENT_ID.lower(),
            "execution_mode": EXEC_MODE,
        }
        _preexisting_watched = _FakeWatched(_preexisting_sig, "PENDING")
        watcher._pending.append(_preexisting_watched)
        watcher._dedup_set.add(SIGNAL_ID)
        _preexisting_token = _preexisting_watched._registration_token

        _adoption_call_count = {"n": 0}

        def _adopt_counts_and_fails(*a, **kw):
            _adoption_call_count["n"] += 1
            return False

        monkeypatch.setattr(
            osm, "adopt_direction_reversal_watcher_ownership",
            _adopt_counts_and_fails,
        )

        result = {"errors": []}
        fresh_recovery._reseed_watchers(result)

        # The defect this test exists to catch: the early has_order()
        # fast path must NOT have short-circuited past PTR/adoption for
        # this direction-reversal watcher-required row.
        assert _adoption_call_count["n"] == 1, (
            "the pre-existing watcher's row must reach the adoption CAS "
            "exactly once — the early runtime-only fast path must not "
            "have skipped it"
        )

        assert watcher.has_order(LOCAL_ORDER_ID) is True
        assert len(watcher._pending) == 1
        assert watcher._pending[0] is _preexisting_watched
        assert watcher._pending[0]._registration_token == _preexisting_token
        assert SIGNAL_ID in watcher._dedup_set
        assert result.get("pending_trigger_watchers_rearmed") in (0, None)
        assert any(
            "startup_reseed_ownership_adoption_failed" in e
            for e in result.get("errors", [])
        )
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []


class TestStartupPreExistingWatcherAdoptionSucceeds:
    """Explicit regression for the original remaining hole: a real
    runtime watcher exists BEFORE _reseed_watchers() runs, and durable
    adoption succeeds. Proves the early has_order() fast path does not
    swallow this row, and that a pre-existing (not just freshly
    registered) watcher can still reach full durable convergence."""

    def test_preexisting_watcher_durably_adopted_end_to_end(self, monkeypatch):
        fresh_recovery, osm, row_store, watcher, selector, broker_calls = (
            _build_reseed_scenario(monkeypatch)
        )

        _preexisting_sig = {
            "local_order_id": LOCAL_ORDER_ID,
            "signal_id": SIGNAL_ID,
            "client_id": CLIENT_ID.lower(),
            "execution_mode": EXEC_MODE,
        }
        _preexisting_watched = _FakeWatched(_preexisting_sig, "PENDING")
        watcher._pending.append(_preexisting_watched)
        watcher._dedup_set.add(SIGNAL_ID)

        _adoption_call_count = {"n": 0}
        _real_adopt = osm.adopt_direction_reversal_watcher_ownership

        def _adopt_counts_and_succeeds(*a, **kw):
            _adoption_call_count["n"] += 1
            return _real_adopt(*a, **kw)

        monkeypatch.setattr(
            osm, "adopt_direction_reversal_watcher_ownership",
            _adopt_counts_and_succeeds,
        )

        result = {"errors": []}
        fresh_recovery._reseed_watchers(result)

        assert _adoption_call_count["n"] == 1, (
            "already_verified_owner_rows must not have short-circuited "
            "this direction-reversal watcher-required row before PTR/"
            "adoption ran"
        )

        # Same pre-existing runtime watcher — no duplicate registered.
        assert watcher.has_order(LOCAL_ORDER_ID) is True
        assert len(watcher._pending) == 1
        assert watcher._pending[0] is _preexisting_watched

        _real_token = watcher.owner_token
        _meta = row_store["row"]["meta"]
        assert _meta["current_owner"] == _real_token
        assert _meta["watcher_token"] == _real_token
        assert _meta["watcher_generation"] == NEW_GEN
        assert _meta["recovery_owner"] == ""
        assert _meta["recovery_ownership"] == ""
        assert _meta["direction_reversal_rearm_requires_watcher"] is False
        assert _meta["materialization_status"] == "WAITING_FOR_TRIGGER"
        assert result.get("pending_trigger_watchers_rearmed") == 1

        assert not selector.select.called
        assert not selector.select_contract.called
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []


class TestStartupVerificationChecksGenerationAndFlag:
    """Test 9 — the durable reread must inspect watcher_generation and
    require direction_reversal_rearm_requires_watcher to be explicitly
    False. Either field being wrong/missing must block rearmed, exactly
    like a wrong current_owner/watcher_token would."""

    def test_wrong_watcher_generation_blocks_rearmed(self, monkeypatch):
        fresh_recovery, osm, row_store, watcher, selector, broker_calls = (
            _build_reseed_scenario(monkeypatch)
        )

        _real_adopt = osm.adopt_direction_reversal_watcher_ownership
        _real_get_order = osm.get_order
        _adopted_flag = {"done": False}

        def _adopt_and_flag(*a, **kw):
            ok = _real_adopt(*a, **kw)
            if ok:
                _adopted_flag["done"] = True
            return ok

        def _get_order_wrong_generation(order_id):
            _row = _real_get_order(order_id)
            if _adopted_flag["done"] and _row:
                _row = dict(_row)
                _row["meta"] = dict(_row.get("meta") or {})
                _row["meta"]["watcher_generation"] = NEW_GEN + 5
            return _row

        monkeypatch.setattr(
            osm, "adopt_direction_reversal_watcher_ownership", _adopt_and_flag,
        )
        monkeypatch.setattr(osm, "get_order", _get_order_wrong_generation)

        result = {"errors": []}
        fresh_recovery._reseed_watchers(result)

        assert result.get("pending_trigger_watchers_rearmed") in (0, None)
        assert watcher.has_order(LOCAL_ORDER_ID) is True  # not evicted
        assert any(
            "startup_reseed_adoption_verification_inconclusive" in e
            for e in result.get("errors", [])
        )
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []

    def test_watcher_required_flag_still_true_blocks_rearmed(
        self, monkeypatch,
    ):
        fresh_recovery, osm, row_store, watcher, selector, broker_calls = (
            _build_reseed_scenario(monkeypatch)
        )

        _real_adopt = osm.adopt_direction_reversal_watcher_ownership
        _real_get_order = osm.get_order
        _adopted_flag = {"done": False}

        def _adopt_and_flag(*a, **kw):
            ok = _real_adopt(*a, **kw)
            if ok:
                _adopted_flag["done"] = True
            return ok

        def _get_order_flag_still_true(order_id):
            _row = _real_get_order(order_id)
            if _adopted_flag["done"] and _row:
                _row = dict(_row)
                _row["meta"] = dict(_row.get("meta") or {})
                _row["meta"]["direction_reversal_rearm_requires_watcher"] = True
            return _row

        monkeypatch.setattr(
            osm, "adopt_direction_reversal_watcher_ownership", _adopt_and_flag,
        )
        monkeypatch.setattr(osm, "get_order", _get_order_flag_still_true)

        result = {"errors": []}
        fresh_recovery._reseed_watchers(result)

        assert result.get("pending_trigger_watchers_rearmed") in (0, None)
        assert watcher.has_order(LOCAL_ORDER_ID) is True
        assert any(
            "startup_reseed_adoption_verification_inconclusive" in e
            for e in result.get("errors", [])
        )

    def test_watcher_required_flag_missing_blocks_rearmed(self, monkeypatch):
        """Missing metadata must not satisfy the explicit-False
        invariant -- absence is not proof of convergence."""
        fresh_recovery, osm, row_store, watcher, selector, broker_calls = (
            _build_reseed_scenario(monkeypatch)
        )

        _real_adopt = osm.adopt_direction_reversal_watcher_ownership
        _real_get_order = osm.get_order
        _adopted_flag = {"done": False}

        def _adopt_and_flag(*a, **kw):
            ok = _real_adopt(*a, **kw)
            if ok:
                _adopted_flag["done"] = True
            return ok

        def _get_order_flag_missing(order_id):
            _row = _real_get_order(order_id)
            if _adopted_flag["done"] and _row:
                _row = dict(_row)
                _row["meta"] = dict(_row.get("meta") or {})
                _row["meta"].pop(
                    "direction_reversal_rearm_requires_watcher", None,
                )
            return _row

        monkeypatch.setattr(
            osm, "adopt_direction_reversal_watcher_ownership", _adopt_and_flag,
        )
        monkeypatch.setattr(osm, "get_order", _get_order_flag_missing)

        result = {"errors": []}
        fresh_recovery._reseed_watchers(result)

        assert result.get("pending_trigger_watchers_rearmed") in (0, None)
        assert watcher.has_order(LOCAL_ORDER_ID) is True
        assert any(
            "startup_reseed_adoption_verification_inconclusive" in e
            for e in result.get("errors", [])
        )


class TestStartupOrdinaryWatcherFastPathPreserved:
    """An ordinary row (never went through the direction-reversal
    recovery-owned state) with a pre-existing runtime watcher must still
    use the original duplicate-watcher fast path — this amendment must
    not route every ordinary startup watcher through adoption."""

    def test_ordinary_row_skips_adoption_entirely(self, monkeypatch):
        fresh_recovery, osm, row_store, watcher, selector, broker_calls = (
            _build_reseed_scenario(monkeypatch)
        )
        # Downgrade this row to an ordinary (non-RWR) already-owned row.
        row_store["row"]["meta"]["direction_reversal_rearm_requires_watcher"] = False
        row_store["row"]["meta"]["recovery_owner"] = ""
        row_store["row"]["meta"]["recovery_ownership"] = ""

        _preexisting_sig = {
            "local_order_id": LOCAL_ORDER_ID,
            "signal_id": SIGNAL_ID,
            "client_id": CLIENT_ID.lower(),
            "execution_mode": EXEC_MODE,
        }
        watcher._pending.append(_FakeWatched(_preexisting_sig, "PENDING"))
        watcher._dedup_set.add(SIGNAL_ID)

        _adoption_call_count = {"n": 0}

        def _adopt_counts(*a, **kw):
            _adoption_call_count["n"] += 1
            return True

        monkeypatch.setattr(
            osm, "adopt_direction_reversal_watcher_ownership", _adopt_counts,
        )

        result = {"errors": []}
        fresh_recovery._reseed_watchers(result)

        assert _adoption_call_count["n"] == 0, (
            "an ordinary already-owned row must never reach the "
            "direction-reversal adoption CAS"
        )
        assert len(watcher._pending) == 1  # no duplicate registered
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []
        assert not selector.select.called


class TestStartupReplacementWatcherSurvivesStaleRollback:
    """Test D — this startup invocation registers watcher A; before its
    own rollback runs, watcher A is replaced by watcher B (a concurrent
    actor); adoption fails; the stale startup cleanup must remove only
    the exact registration it created (A's token) and must never touch
    B, using the same exact-registration-identity fencing already
    proven for the due-retry RWR path."""

    def test_replacement_watcher_survives_stale_startup_cleanup(
        self, monkeypatch,
    ):
        fresh_recovery, osm, row_store, watcher, selector, broker_calls = (
            _build_reseed_scenario(monkeypatch)
        )

        def _adopt_loses_after_replacement(*a, **kw):
            # Simulate: between this attempt's registration and its own
            # adoption CAS, a concurrent actor's cleanup removed A and a
            # legitimate winner registered B for the same identity.
            watcher._pending.clear()
            watcher._dedup_set.discard(SIGNAL_ID)
            _sig = {
                "local_order_id": LOCAL_ORDER_ID,
                "signal_id": SIGNAL_ID,
                "client_id": CLIENT_ID.lower(),
                "execution_mode": EXEC_MODE,
            }
            _winner = _FakeWatched(_sig, "PENDING")
            watcher._pending.append(_winner)
            watcher._dedup_set.add(SIGNAL_ID)
            _adopt_loses_after_replacement.winner_token = (
                _winner._registration_token
            )
            return False

        monkeypatch.setattr(
            osm, "adopt_direction_reversal_watcher_ownership",
            _adopt_loses_after_replacement,
        )

        result = {"errors": []}
        fresh_recovery._reseed_watchers(result)

        winner_token = _adopt_loses_after_replacement.winner_token
        assert watcher.has_order(LOCAL_ORDER_ID) is True
        assert len(watcher._pending) == 1
        assert watcher._pending[0]._registration_token == winner_token
        assert SIGNAL_ID in watcher._dedup_set
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []


class TestStartupAdoptionVerificationRaises:
    """Test E — adoption CAS reports success, but the verification
    reread raises. rearmed must not increment, the watcher must not be
    evicted, recovery ownership must not be restored, and no generic
    retention helper may run."""

    def test_verification_read_exception_after_successful_cas(
        self, monkeypatch,
    ):
        fresh_recovery, osm, row_store, watcher, selector, broker_calls = (
            _build_reseed_scenario(monkeypatch)
        )

        _adoption_committed = {"flag": False}
        _real_adopt = osm.adopt_direction_reversal_watcher_ownership

        def _adopt_and_flag(*a, **kw):
            ok = _real_adopt(*a, **kw)
            if ok:
                _adoption_committed["flag"] = True
            return ok

        monkeypatch.setattr(
            osm, "adopt_direction_reversal_watcher_ownership", _adopt_and_flag,
        )

        _real_get_order = osm.get_order

        def _get_order_raises_after_adoption(order_id):
            if _adoption_committed["flag"]:
                raise RuntimeError(
                    "simulated transient read failure after successful CAS"
                )
            return _real_get_order(order_id)

        monkeypatch.setattr(osm, "get_order", _get_order_raises_after_adoption)

        result = {"errors": []}
        fresh_recovery._reseed_watchers(result)

        # The CAS itself did commit -- proving this is a genuine
        # "committed but unverifiable this pass" case.
        _real_token = watcher.owner_token
        _meta = row_store["row"]["meta"]
        assert _meta.get("current_owner") == _real_token
        assert _meta.get("watcher_token") == _real_token

        # Not counted as rearmed, watcher not evicted, no recovery
        # ownership restored (recovery fields stay exactly as the CAS
        # itself left them -- cleared, not reasserted).
        assert result.get("pending_trigger_watchers_rearmed") in (0, None)
        assert watcher.has_order(LOCAL_ORDER_ID) is True
        assert _meta.get("recovery_owner") == ""
        assert _meta.get("recovery_ownership") == ""
        assert any(
            "startup_reseed_adoption_verification_inconclusive" in e
            for e in result.get("errors", [])
        )
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []


class TestStartupAdoptionVerificationInconclusiveState:
    """Test F — same invariants as Test E, but the reread itself succeeds
    while returning a state that cannot conclusively prove convergence
    (e.g. a stale/lagging read that still shows blank current_owner)."""

    def test_verification_returns_unconvincing_state(self, monkeypatch):
        fresh_recovery, osm, row_store, watcher, selector, broker_calls = (
            _build_reseed_scenario(monkeypatch)
        )

        _adoption_committed = {"flag": False}
        _real_adopt = osm.adopt_direction_reversal_watcher_ownership

        def _adopt_and_flag(*a, **kw):
            ok = _real_adopt(*a, **kw)
            if ok:
                _adoption_committed["flag"] = True
            return ok

        monkeypatch.setattr(
            osm, "adopt_direction_reversal_watcher_ownership", _adopt_and_flag,
        )

        _real_get_order = osm.get_order

        def _get_order_stale_read_after_adoption(order_id):
            _row = _real_get_order(order_id)
            if _adoption_committed["flag"] and _row:
                # A read that "succeeds" but cannot prove convergence --
                # current_owner still reads blank despite the CAS commit.
                _row = dict(_row)
                _row["meta"] = dict(_row.get("meta") or {})
                _row["meta"]["current_owner"] = ""
            return _row

        monkeypatch.setattr(
            osm, "get_order", _get_order_stale_read_after_adoption,
        )

        result = {"errors": []}
        fresh_recovery._reseed_watchers(result)

        assert result.get("pending_trigger_watchers_rearmed") in (0, None)
        assert watcher.has_order(LOCAL_ORDER_ID) is True
        # The REAL underlying durable state (not the injected stale
        # read) was never reverted or double-written.
        _meta = row_store["row"]["meta"]
        assert _meta.get("recovery_owner") == ""
        assert _meta.get("recovery_ownership") == ""
        assert any(
            "startup_reseed_adoption_verification_inconclusive" in e
            for e in result.get("errors", [])
        )
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 4: concurrent generation advancement must fail closed
# ─────────────────────────────────────────────────────────────────────────────

class TestConcurrentGenerationAdvancement:
    def test_generation_mismatch_registers_no_watcher(self, monkeypatch):
        """A different worker advances materialization_generation past what
        this callback's REARM_WATCHER_REQUIRED expected. The handler must
        require an EXACT match (never >=) and must not register a watcher
        or fabricate a rearm using the stale expected_generation.
        """
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        # After the real rearm commits (generation NEW_GEN), simulate a
        # concurrent worker bumping the generation further before the
        # REARM_WATCHER_REQUIRED handler in ap_recovery.py re-reads the row.
        # The rearm write itself goes through raw SQL (osm_mod.conn), not
        # update_order_meta, so the bump is injected at the next read after
        # the rearm has landed (identified by the telltale cleared
        # lifecycle_state), which is exactly when a concurrent worker's
        # advancement would actually be observed.
        _original_get_order = osm.get_order
        _bumped = {"done": False}

        def _osm_get_order_then_bump(order_id):
            row = _original_get_order(order_id)
            if (
                not _bumped["done"]
                and row
                and (row.get("meta") or {}).get("lifecycle_state") == ""
            ):
                _bumped["done"] = True
                row_store["row"]["meta"]["materialization_generation"] = NEW_GEN + 1
                row = dict(row_store["row"])
            return row

        osm.get_order = _osm_get_order_then_bump

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        assert len(watcher._pending) == 0, (
            "no watcher may be registered when the durable generation no "
            "longer exactly matches the callback's expected_generation"
        )
        assert any(
            "generation_advanced" in e or "rearm_watcher_required" in e
            for e in result.get("errors", [])
        ), f"expected a structured generation-mismatch error, got {result.get('errors')}"
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []


class TestStrictDurableGenerationValidation:
    """Durable materialization_generation must reject Boolean and float
    values before ever calling PendingTriggerRestartRecovery.recover_one_row()
    — bare int(x) coercion would silently accept int(True)==1, int(1.9)==1.
    """

    def _run_with_durable_generation(self, monkeypatch, bad_value):
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )
        _original_get_order = osm.get_order
        _mangled = {"done": False}

        def _osm_get_order_then_mangle(order_id):
            row = _original_get_order(order_id)
            if (
                not _mangled["done"]
                and row
                and (row.get("meta") or {}).get("lifecycle_state") == ""
            ):
                _mangled["done"] = True
                row_store["row"]["meta"]["materialization_generation"] = bad_value
                row = dict(row_store["row"])
            return row

        osm.get_order = _osm_get_order_then_mangle

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)
        return watcher, selector, broker_calls, result

    def test_durable_generation_boolean_rejected(self, monkeypatch):
        watcher, selector, broker_calls, result = self._run_with_durable_generation(
            monkeypatch, True,
        )
        assert len(watcher._pending) == 0
        assert not selector.select.called
        assert not selector.select_contract.called
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []
        assert result.get("errors"), "expected a structured recovery error"

    def test_durable_generation_float_rejected(self, monkeypatch):
        watcher, selector, broker_calls, result = self._run_with_durable_generation(
            monkeypatch, 1.9,
        )
        assert len(watcher._pending) == 0
        assert not selector.select.called
        assert not selector.select_contract.called
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []
        assert result.get("errors"), "expected a structured recovery error"


class TestBrokerReadyCrashWindowRejection:
    def test_broker_ready_true_blocks_handoff_before_ptr(self, monkeypatch):
        """A refreshed row with authoritative broker_ready=true (durable
        crash-window evidence) must never reach
        PendingTriggerRestartRecovery.recover_one_row() at all — not merely
        produce a different PTR outcome.
        """
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )
        _original_get_order = osm.get_order
        _mangled = {"done": False}

        def _osm_get_order_then_set_broker_ready(order_id):
            row = _original_get_order(order_id)
            if (
                not _mangled["done"]
                and row
                and (row.get("meta") or {}).get("lifecycle_state") == ""
            ):
                _mangled["done"] = True
                row_store["row"]["meta"]["broker_ready"] = True
                row = dict(row_store["row"])
            return row

        osm.get_order = _osm_get_order_then_set_broker_ready

        import ap.pending_trigger_restart_recovery as ptr_mod
        _ptr_calls = []
        _original_recover_one_row = ptr_mod.PendingTriggerRestartRecovery.recover_one_row

        def _spy_recover_one_row(self, *a, **kw):
            _ptr_calls.append((a, kw))
            return _original_recover_one_row(self, *a, **kw)

        monkeypatch.setattr(
            ptr_mod.PendingTriggerRestartRecovery, "recover_one_row",
            _spy_recover_one_row,
        )

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        assert _ptr_calls == [], (
            "recover_one_row() must never be invoked when durable "
            "broker_ready evidence is present"
        )
        assert len(watcher._pending) == 0
        assert not selector.select.called
        assert not selector.select_contract.called
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []
        assert result.get("errors"), (
            "expected a structured recovery/crash-window error"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 5: repeated recovery pass is idempotent
# ─────────────────────────────────────────────────────────────────────────────

class TestRepeatedRecoveryPassIsIdempotent:
    def test_second_pass_does_not_duplicate_watcher_or_bump_generation(
        self, monkeypatch,
    ):
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_available(broker, symbol, side, trigger):
            return False

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_available,
        )

        result1 = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result1)
        _watcher_count_after_pass1 = len(watcher._pending)
        _generation_after_pass1 = row_store["row"]["meta"].get(
            "materialization_generation"
        )
        _owner_after_pass1 = row_store["row"]["meta"].get("current_owner")
        _token_after_pass1 = row_store["row"]["meta"].get("watcher_token")

        # Second pass against the SAME (now-owned) row. Since the row's
        # lifecycle is no longer RETRY_WAIT (it is watcher-owned or blank),
        # the due-retry branch should not re-fire, and no additional
        # watcher/generation/ownership mutation should occur.
        result2 = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result2)

        assert len(watcher._pending) == _watcher_count_after_pass1, (
            "a second recovery pass must not register a second watcher"
        )
        assert (
            row_store["row"]["meta"].get("materialization_generation")
            == _generation_after_pass1
        ), "a second recovery pass must not increment materialization_generation"
        assert (
            row_store["row"]["meta"].get("current_owner") == _owner_after_pass1
        ), "a second recovery pass must not create duplicate watcher ownership"
        assert (
            row_store["row"]["meta"].get("watcher_token") == _token_after_pass1
        ), "a second recovery pass must not create duplicate watcher ownership"
        assert not row_store["row"]["meta"].get("trigger_crossed_at"), (
            "a second recovery pass must not restore cleared active trigger evidence"
        )
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []
        assert not selector.select.called
        assert not selector.select_contract.called


class TestOwnershipAdoptionFailureEvictsWatcher:
    def test_cas_loss_evicts_just_registered_watcher(self, monkeypatch):
        """A real watcher registers in-process (PTR returns WATCHER_OWNED),
        but the durable ownership-adoption CAS deliberately loses (rowcount
        0). The handler must not report success, must evict exactly the
        watched entry it just registered, and must leave the row
        recovery-owned durably with zero selector/broker calls.
        """
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_available(broker, symbol, side, trigger):
            return False

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_available,
        )

        # Force ONLY the adoption's CAS UPDATE to lose (rowcount 0), while
        # leaving the rearm's own UPDATE (a different WHERE-clause shape)
        # succeeding normally, by distinguishing the two queries on their
        # distinctive WHERE-clause text.
        _original_rearm_write = None

        def _rearm_write_with_adoption_cas_loss(sql, params):
            if "recovery_scheduler" in sql and "watcher_token','') = ''" in sql:
                # This is adopt_direction_reversal_watcher_ownership's CAS —
                # simulate a concurrent loss: report zero rows matched, and
                # do not apply the patch.
                return
            # Otherwise this is the rearm's own UPDATE — apply normally via
            # the same emulation _build_harness already wires up.
            if params and isinstance(params[0], str):
                try:
                    patch = json.loads(params[0])
                except Exception:
                    return
                _cur_meta = dict(row_store["row"].get("meta") or {})
                _cur_meta.update(patch)
                for _rm_key in (
                    "selector_recovery_cursor_v1", "trigger_crossed_at",
                    "trigger_crossed_at_provenance", "triggered_at",
                    "trigger_confirmed_at",
                ):
                    _cur_meta.pop(_rm_key, None)
                row_store["row"]["meta"] = _cur_meta

        def _cas_loss_cursor():
            _cur = _FakeCursor(rowcount=1, on_write=None)
            _cur.execute = lambda sql, params=None: (
                _rearm_write_with_adoption_cas_loss(sql, params),
                setattr(_cur, "rowcount", (
                    0 if (
                        "recovery_scheduler" in sql
                        and "watcher_token','') = ''" in sql
                    ) else 1
                )),
                _cur,
            )[-1]
            return _cur

        monkeypatch.setattr(
            osm_mod, "conn", lambda: _FakeConnCtx(_cas_loss_cursor()),
        )
        monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        # The mandatory failure seam:
        assert watcher.has_order(LOCAL_ORDER_ID) is False, (
            "a just-registered runtime watcher must be evicted when its "
            "durable ownership adoption CAS loses"
        )
        assert len(watcher._pending) == 0
        assert SIGNAL_ID not in watcher._dedup_set

        final_meta = row_store["row"]["meta"]
        assert not str(final_meta.get("watcher_token") or "").strip(), (
            "no watcher token may be durably claimed after a failed adoption"
        )
        assert final_meta.get("recovery_ownership") == "recovery_scheduler"
        assert str(final_meta.get("recovery_owner") or "").strip(), (
            "the row must remain recovery-owned after a failed adoption"
        )

        assert any(
            "ownership_adoption" in e for e in result.get("errors", [])
        ), f"expected a structured adoption-failure error, got {result.get('errors')}"

        assert not selector.select.called
        assert not selector.select_contract.called
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []


class TestConcurrentRecoveryConvergence:
    def test_two_competing_recovery_instances_converge_to_one_owner(
        self, monkeypatch,
    ):
        """Two independently-constructed APStartupRecovery instances race
        against the SAME due row. recovery2 is triggered to run its entire
        pipeline reentrantly from inside recovery1's own claim step —
        before recovery1's claim has landed — so both instances genuinely
        read the same still-RETRY_WAIT row and each attempt the claim
        independently. The claim is CAS-gated against the row's actual
        durable state (not blindly accepted), so only whichever instance's
        claim executes first actually wins; the other observes the row has
        already moved and backs off. This proves convergence under real
        contention, not two serial no-op passes.
        """
        recovery1, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_available(broker, symbol, side, trigger):
            return False

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_available,
        )

        recovery2 = APStartupRecovery(
            client_id=CLIENT_ID,
            broker=recovery1.broker,
            osm=osm,
            pm=MagicMock(),
            master_control=SimpleNamespace(mode="PAPER"),
            entry_watcher=watcher,
            execution_core=core,
        )

        _original_claim = osm.claim_deferred_materialization
        _reentered = {"done": False}

        def _gated_claim(order_id, *, owner, new_generation, **kw):
            # CAS gate: only apply if the row is still durably RETRY_WAIT at
            # the exact expected prior generation at the moment this
            # specific call executes — a real claim's WHERE clause would
            # reject a stale attempt the same way.
            _row_meta = row_store["row"].get("meta") or {}
            if (
                str(_row_meta.get("lifecycle_state") or "") != "RETRY_WAIT"
                or int(_row_meta.get("materialization_generation") or 0)
                != GENERATION
            ):
                return False  # a racer already advanced this row — lose

            # Reentrant interleaving point: the FIRST time any claim call
            # reaches here, run recovery2's entire pipeline before this
            # call (recovery1's own claim) proceeds — modeling recovery2
            # observing and acting on the same still-untouched row first.
            if not _reentered["done"]:
                _reentered["done"] = True
                recovery2._recover_deferred_breach_lifecycles({"errors": []})

            # Re-check after the reentrant run: if recovery2 already won
            # the row in the interleaved call above, this (recovery1's
            # original) attempt must now correctly lose too.
            _row_meta_after = row_store["row"].get("meta") or {}
            if (
                str(_row_meta_after.get("lifecycle_state") or "") != "RETRY_WAIT"
                or int(_row_meta_after.get("materialization_generation") or 0)
                != GENERATION
            ):
                return False

            return _original_claim(
                order_id, owner=owner, new_generation=new_generation, **kw
            )

        osm.claim_deferred_materialization = _gated_claim

        result1 = {"errors": []}
        recovery1._recover_deferred_breach_lifecycles(result1)

        # Exactly one durable watcher owner — never two — despite two
        # independent actors both contending for the same row.
        assert len(watcher._pending) == 1, (
            "two competing recovery passes must converge to exactly one "
            "watcher registration under genuine CAS contention, not "
            "duplicate it"
        )
        assert watcher.has_order(LOCAL_ORDER_ID) is True
        final_meta = row_store["row"]["meta"]
        assert final_meta.get("current_owner") == watcher.owner_token
        assert final_meta.get("watcher_token") == watcher.owner_token
        assert final_meta.get("recovery_owner") == ""
        assert final_meta.get("recovery_ownership") == ""
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []


# ─────────────────────────────────────────────────────────────────────────────
# Mutation-surface proof (constraint H)
# ─────────────────────────────────────────────────────────────────────────────

class TestMutationSurface:
    def test_no_writes_to_positions_proof_trades_or_trade_queue(self, monkeypatch):
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_available(broker, symbol, side, trigger):
            return False

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_available,
        )

        # Real spies: wrap the already-wired ap.db.conn and
        # ap.order_state_machine.conn cursors so every SQL statement
        # actually executed during this recovery pass is captured verbatim
        # — not a tautological "not X.called or True" against an unrelated
        # MagicMock, but the real SQL traffic on the two connections this
        # flow uses.
        _captured_sql = []

        _orig_db_conn_factory = ap_db_mod.conn
        _orig_osm_conn_factory = osm_mod.conn

        def _spy_wrap(conn_factory):
            def _factory():
                _conn_ctx = conn_factory()
                _real_cursor = _conn_ctx._cursor if hasattr(
                    _conn_ctx, "_cursor"
                ) else None

                class _SpyConnCtx:
                    def __enter__(self_inner):
                        _cur = _conn_ctx.__enter__()
                        _orig_execute = _cur.execute

                        def _spy_execute(sql, params=None):
                            _captured_sql.append(sql)
                            return _orig_execute(sql, params)

                        _cur.execute = _spy_execute
                        return _cur

                    def __exit__(self_inner, *exc):
                        return _conn_ctx.__exit__(*exc)

                return _SpyConnCtx()
            return _factory

        monkeypatch.setattr(ap_db_mod, "conn", _spy_wrap(_orig_db_conn_factory))
        monkeypatch.setattr(osm_mod, "conn", _spy_wrap(_orig_osm_conn_factory))

        # pm (position manager) mutation surface — recovery must never call
        # any position-mutating method as a side effect of this handoff.
        pm = recovery.pm
        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        for _forbidden in (
            "open_position", "close_position", "create_position",
            "insert_position", "record_trade", "insert_proof_trade",
            "enqueue", "push", "insert_trade_queue",
        ):
            _attr = getattr(pm, _forbidden, None)
            if _attr is not None:
                assert not _attr.called, (
                    f"pm.{_forbidden}() must not be called by this handoff"
                )

        # Positive control: the spy must have actually captured real SQL
        # traffic (the rearm's UPDATE orders ...), proving this is a
        # meaningful assertion, not a vacuous pass because nothing ran.
        assert any(
            "orders" in sql.lower() for sql in _captured_sql
        ), f"expected to capture real orders-table SQL, got {_captured_sql!r}"

        # Negative control: no captured SQL statement references any of
        # the three forbidden tables.
        for _table in ("positions", "proof_trades", "trade_queue"):
            _hits = [sql for sql in _captured_sql if _table in sql.lower()]
            assert not _hits, (
                f"direction-reversal rearm + watcher-recovery handoff must "
                f"make zero {_table} writes, but captured: {_hits!r}"
            )

        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []
        assert not selector.select.called
        assert not selector.select_contract.called


# ─────────────────────────────────────────────────────────────────────────────
# PR #421 FINAL AMENDMENT — watcher provenance + post-adoption ownership
# safety (P0 races 1 and 2 found during merge audit)
# ─────────────────────────────────────────────────────────────────────────────
#
# Test A — pre-existing watcher survives adoption CAS loss
# Test B — successful adoption + verification read failure cannot recreate
#          recovery authority
# Test C — rollback remains valid for a watcher genuinely created by this
#          invocation (sanity counterpart to A/D, exercised through the
#          full real end-to-end seam rather than the lower-level unit
#          tests in test_p0_watcher_rollback_exact_registration_identity.py)
# Test D — replacement watcher is never removed: already covered by
#          test_stale_loser_cannot_evict_legitimate_winners_replacement_registration
#          in tests/test_p0_watcher_rollback_exact_registration_identity.py,
#          which exercises the exact same _evict_just_registered_watcher
#          fencing this amendment does not modify. Not duplicated here.
# Test E — _retain_recovery_ownership loses against committed watcher
#          authority


class TestPreExistingWatcherProvenance:
    """P0 race 1: PendingTriggerRestartRecovery's read-only fast path can
    return WATCHER_OWNED for a watcher that already existed BEFORE this
    recovery invocation ran — no registration happened in this call. A
    caller that assumed WATCHER_OWNED always means "I just registered
    this" could roll back (evict + release dedup) a real, currently-owned
    watcher that a live position may depend on, merely because this pass
    observed it and its own durable-adoption CAS happened to lose.
    """

    def test_preexisting_watcher_survives_adoption_cas_loss(self, monkeypatch):
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_available(broker, symbol, side, trigger):
            return False  # not yet through trigger — reaches WAITING_VALID

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_available,
        )

        # Pre-register a REAL, already-owned watcher for this exact row
        # BEFORE the recovery pass runs — simulating the watcher having
        # been registered by some earlier, unrelated pass or actor, not by
        # the invocation under test.
        _preexisting_sig = {
            "local_order_id": LOCAL_ORDER_ID,
            "signal_id": SIGNAL_ID,
            "client_id": CLIENT_ID.lower(),
            "execution_mode": EXEC_MODE,
        }
        _preexisting_watched = _FakeWatched(_preexisting_sig, "PENDING")
        watcher._pending.append(_preexisting_watched)
        watcher._dedup_set.add(SIGNAL_ID)
        _preexisting_token = _preexisting_watched._registration_token

        # Force the durable watcher-adoption CAS to lose/fail — this must
        # never be mistaken for permission to evict a watcher this pass
        # did not create.
        monkeypatch.setattr(
            osm, "adopt_direction_reversal_watcher_ownership",
            lambda *a, **kw: False,
        )

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        # The exact pre-existing watcher registration remains, untouched.
        assert watcher.has_order(LOCAL_ORDER_ID) is True
        assert len(watcher._pending) == 1
        assert watcher._pending[0] is _preexisting_watched
        assert watcher._pending[0]._registration_token == _preexisting_token
        assert SIGNAL_ID in watcher._dedup_set

        # No broker activity of any kind.
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []
        assert not selector.select.called
        assert not selector.select_contract.called


class TestSuccessfulAdoptionVerificationFailureNoAuthorityRecreated:
    """P0 race 2: once the durable watcher-adoption CAS reports success,
    watcher ownership is authoritative. A subsequent verification reread
    that raises or returns inconclusive must NOT evict the watcher and
    must NOT recreate recovery ownership on top of it — either outcome
    would produce a row with contradictory dual authority (or an
    unmonitored but "watcher-owned-looking" row).
    """

    def test_verification_read_exception_after_successful_cas(self, monkeypatch):
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_available(broker, symbol, side, trigger):
            return False

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_available,
        )

        # Let the real (harness-faked) CAS write succeed as normal, but
        # make the SEPARATE post-adoption verification reread raise —
        # targeted precisely at the reread that happens AFTER a successful
        # adoption, not any of the many earlier get_order() calls in the
        # resume_deferred_materialization_retry / _on_entry_trigger chain.
        _adoption_committed = {"flag": False}
        _real_adopt = osm.adopt_direction_reversal_watcher_ownership

        def _adopt_and_flag(*a, **kw):
            ok = _real_adopt(*a, **kw)
            if ok:
                _adoption_committed["flag"] = True
            return ok

        monkeypatch.setattr(
            osm, "adopt_direction_reversal_watcher_ownership", _adopt_and_flag,
        )

        _real_get_order = osm.get_order

        def _get_order_raises_after_adoption(order_id):
            if _adoption_committed["flag"]:
                raise RuntimeError(
                    "simulated transient read failure after successful CAS"
                )
            return _real_get_order(order_id)

        monkeypatch.setattr(osm, "get_order", _get_order_raises_after_adoption)

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        # The CAS itself did commit — durable state shows it, proving this
        # scenario is a genuine "committed but unverifiable this pass"
        # case, not merely "nothing happened."
        final_meta = row_store["row"]["meta"]
        real_token = watcher.owner_token
        assert final_meta.get("current_owner") == real_token
        assert final_meta.get("watcher_token") == real_token

        # No dual authority: the CAS's own patch already cleared recovery
        # fields, and the inconclusive verification must not have written
        # them again.
        assert final_meta.get("recovery_owner") == ""
        assert final_meta.get("recovery_ownership") == ""

        # The watcher this pass registered was never evicted merely
        # because the reread couldn't confirm what the CAS already
        # committed.
        assert watcher.has_order(LOCAL_ORDER_ID) is True
        assert len(watcher._pending) == 1
        assert SIGNAL_ID in watcher._dedup_set

        # Diagnostics recorded, but not as a hard failure that triggers
        # retention/eviction.
        assert any(
            "adoption_verification_inconclusive" in e
            for e in result.get("errors", [])
        )

        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []


class TestGenuineNewRegistrationRollbackStillWorksEndToEnd:
    """Sanity counterpart to Test A, through the full real seam rather than
    the lower-level unit tests: when NO watcher pre-exists and this exact
    invocation registers one, a genuine CAS failure must still roll it
    back — preserving the reason PR #421's original rollback fix exists.
    """

    def test_rollback_evicts_watcher_this_invocation_registered(self, monkeypatch):
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_available(broker, symbol, side, trigger):
            return False

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_available,
        )

        # No pre-existing watcher. Force the CAS to lose so this pass's
        # OWN just-registered watcher is the only thing rollback could
        # legitimately touch.
        monkeypatch.setattr(
            osm, "adopt_direction_reversal_watcher_ownership",
            lambda *a, **kw: False,
        )

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        # This pass's own registration was rolled back — no orphaned
        # runtime watcher left dangling against a recovery-owned row.
        assert watcher.has_order(LOCAL_ORDER_ID) is False
        assert len(watcher._pending) == 0
        assert SIGNAL_ID not in watcher._dedup_set

        assert any(
            "ownership_adoption_failed" in e for e in result.get("errors", [])
        )
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []


class TestRetainRecoveryOwnershipFencedAgainstCommittedWatcher:
    """§5: the fenced OSM write retain_recovery_ownership_if_no_watcher()
    must refuse — as a no-op, not a silent success — when the row already
    carries committed watcher authority (current_owner / watcher_token /
    watcher_generation all non-blank). Recovery authority must never be
    written on top of committed watcher authority.
    """

    def test_refuses_when_watcher_authority_already_committed(self, monkeypatch):
        real_osm = object.__new__(APOrderStateMachine)
        real_osm.client_id = CLIENT_ID

        row_store = {
            "meta": {
                "current_owner": "watcher-token-committed",
                "watcher_token": "watcher-token-committed",
                "watcher_generation": 4,
            }
        }

        class _C:
            rowcount = 0
            def execute(self, sql, params):
                _meta = row_store["meta"]
                _blank = (
                    not str(_meta.get("current_owner") or "")
                    and not str(_meta.get("watcher_token") or "")
                    and not str(_meta.get("watcher_generation") or "")
                )
                if _blank:
                    _patch = json.loads(params[0])
                    _meta.update(_patch)
                    self.rowcount = 1
                else:
                    self.rowcount = 0
                return self

        class _Conn:
            def __enter__(self):
                return _C()
            def __exit__(self, *a):
                return False

        monkeypatch.setattr(osm_mod, "conn", lambda: _Conn())
        monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())

        ok = real_osm.retain_recovery_ownership_if_no_watcher(
            LOCAL_ORDER_ID,
            recovery_owner=f"recovery_scheduler:{CLIENT_ID}",
            reason="test_committed_watcher_fence",
            recovery_retention_mode="paper",
        )

        assert ok is False
        # Watcher authority completely unchanged.
        assert row_store["meta"]["current_owner"] == "watcher-token-committed"
        assert row_store["meta"]["watcher_token"] == "watcher-token-committed"
        assert row_store["meta"]["watcher_generation"] == 4
        # No recovery authority was written on top of it.
        assert "recovery_ownership" not in row_store["meta"]
        assert "recovery_owner" not in row_store["meta"]

    def test_succeeds_when_no_watcher_authority_present(self, monkeypatch):
        """Positive control: the fence must not be simply broken/always-
        false — it must genuinely permit the write when watcher fields are
        blank, matching the exact pre-#421 behavior for the ordinary case.
        """
        real_osm = object.__new__(APOrderStateMachine)
        real_osm.client_id = CLIENT_ID

        row_store = {"meta": {"current_owner": "", "watcher_token": "", "watcher_generation": ""}}

        class _C:
            rowcount = 0
            def execute(self, sql, params):
                _meta = row_store["meta"]
                _blank = (
                    not str(_meta.get("current_owner") or "")
                    and not str(_meta.get("watcher_token") or "")
                    and not str(_meta.get("watcher_generation") or "")
                )
                if _blank:
                    _patch = json.loads(params[0])
                    _meta.update(_patch)
                    self.rowcount = 1
                else:
                    self.rowcount = 0
                return self

        class _Conn:
            def __enter__(self):
                return _C()
            def __exit__(self, *a):
                return False

        monkeypatch.setattr(osm_mod, "conn", lambda: _Conn())
        monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())

        ok = real_osm.retain_recovery_ownership_if_no_watcher(
            LOCAL_ORDER_ID,
            recovery_owner=f"recovery_scheduler:{CLIENT_ID}",
            reason="test_no_watcher_present",
            recovery_retention_mode="paper",
        )

        assert ok is True
        assert row_store["meta"]["recovery_ownership"] == "recovery_scheduler"
        assert row_store["meta"]["recovery_owner"] == f"recovery_scheduler:{CLIENT_ID}"


# ─────────────────────────────────────────────────────────────────────────────
# PR #421 FINAL P0 CORRECTION AMENDMENT
# ─────────────────────────────────────────────────────────────────────────────
#
# P0-1 — watcher creation provenance must be causal, not inferred from a
#        post-watch() registry lookup: a concurrent actor can register the
#        real watcher between PTR's initial ownership check and PTR's own
#        watch() call; watch() then observes it and returns True via its
#        "left alone" path WITHOUT creating anything. Proven against the
#        exact head this amendment corrects (6f5abf5b...) by verifying the
#        test fails without the fix.
#
# P0-2 — retain_recovery_ownership_if_no_watcher()'s watcher_generation
#        fence must accept the real production no-watcher shape
#        (watcher_generation=0, a JSON integer), not just the empty-string
#        shape the original positive-control test used.


class TestP01WatcherCreationProvenanceIsCausal:
    """The current invocation must never claim it created a watcher it
    merely observed. Reproduces the exact race: PTR's initial registry
    check finds nothing, a concurrent actor registers watcher A before
    PTR's own watch() call runs, watch() observes A and returns True
    without creating B — provenance must reflect that PTR created
    nothing, and rollback must never be able to touch A.
    """

    def test_concurrent_registration_between_initial_check_and_watch_call(
        self, monkeypatch,
    ):
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_available(broker, symbol, side, trigger):
            return False  # not yet through trigger — reaches WAITING_VALID

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_available,
        )

        # PTR's OWN internal ownership check (inside _recover_one, before
        # _rearm_and_verify is ever reached) must find NOTHING — the
        # harness's watcher starts empty, so this is naturally satisfied;
        # no watcher exists at that instant.
        assert watcher.has_order(LOCAL_ORDER_ID) is False

        # Inject the concurrent actor's registration at the exact moment
        # PTR calls watch() — simulating "a concurrent actor won the race
        # immediately after PTR's initial check passed, but before PTR's
        # own watch() call runs." _FakeWatcher.watch() (patched above to
        # be faithful to the real RECOVERY_REARM_LEFT_ALONE path) will
        # observe this injected entry and return True WITHOUT creating a
        # second registration or reporting any creation provenance.
        _real_watch = watcher.watch
        _injected = {"token": None, "done": False}

        def _watch_with_injection(plan, local_oid=None, recovery_rearm=False,
                                   registration_provenance_out=None, **kw):
            if not _injected["done"]:
                _injected["done"] = True
                _sig = {
                    "local_order_id": str(local_oid or ""),
                    "signal_id": str(getattr(plan, "signal_id", "") or ""),
                    "client_id": str(
                        getattr(plan, "client_id", "") or ""
                    ).strip().lower(),
                    "execution_mode": str(
                        getattr(plan, "execution_mode", "") or ""
                    ).strip().lower(),
                }
                _concurrent_watched = _FakeWatched(_sig, "PENDING")
                watcher._pending.append(_concurrent_watched)
                if _sig["signal_id"]:
                    watcher._dedup_set.add(_sig["signal_id"])
                _injected["token"] = _concurrent_watched._registration_token
            return _real_watch(
                plan, local_oid, recovery_rearm=recovery_rearm,
                registration_provenance_out=registration_provenance_out, **kw
            )

        monkeypatch.setattr(watcher, "watch", _watch_with_injection)

        # Force the durable watcher-adoption CAS to lose — the only way
        # to observe whether rollback incorrectly believes it may touch
        # watcher A.
        monkeypatch.setattr(
            osm, "adopt_direction_reversal_watcher_ownership",
            lambda *a, **kw: False,
        )

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        assert _injected["done"], "setup: the concurrent injection must have fired"
        assert _injected["token"] is not None

        # Watcher A survives, untouched, exact same token, dedup intact.
        assert watcher.has_order(LOCAL_ORDER_ID) is True
        assert len(watcher._pending) == 1
        assert watcher._pending[0]._registration_token == _injected["token"]
        assert SIGNAL_ID in watcher._dedup_set

        # No second watcher was ever created.
        assert len({id(w) for w in watcher._pending}) == 1

        assert any(
            "ownership_adoption_failed" in e for e in result.get("errors", [])
        )
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []
        assert not selector.select.called
        assert not selector.select_contract.called


class TestP02RetentionAcceptsRealZeroGenerationShape:
    """retain_recovery_ownership_if_no_watcher() must accept the actual
    production no-watcher shape: watcher_generation written as the JSON
    integer 0, not the empty string the original positive-control test
    used. Postgres's meta->>'watcher_generation' renders integer 0 as
    text "0", not "" — a fence requiring exact blank would reject #421's
    own legitimate no-watcher state.
    """

    @staticmethod
    def _cursor_class(row_store):
        import re as _re

        class _C:
            rowcount = 0
            def execute(self, sql, params):
                _meta = row_store["meta"]
                # Faithful text-conversion semantics of Postgres's
                # meta->>key on a JSONB column: every stored value
                # (string OR number) renders as its text form, exactly
                # like the real ->> operator — not Python truthiness.
                def _as_text(key):
                    if key not in _meta:
                        return ""
                    v = _meta[key]
                    if v is None:
                        return ""
                    return str(v)

                # Derive the accepted watcher_generation text set FROM
                # THE ACTUAL SQL STRING the production code generated —
                # not a hardcoded Python re-implementation. A regression
                # in the real SQL predicate (e.g. back to a bare
                # `= ''`) must make this fake's behavior regress too,
                # since it is reading that exact clause out of `sql`.
                _gen_clause_match = _re.search(
                    r"meta->>'watcher_generation'\s*,\s*''\)\s*(=|IN)\s*"
                    r"(\([^)]*\)|'[^']*')",
                    sql,
                )
                assert _gen_clause_match, (
                    "fake cursor could not find the watcher_generation "
                    "clause in the real SQL text — production predicate "
                    "shape changed; update this fake to match"
                )
                _op, _rhs = _gen_clause_match.groups()
                _accepted = set(_re.findall(r"'([^']*)'", _rhs))
                _gen_ok = _as_text("watcher_generation") in _accepted

                _blank = (
                    _as_text("current_owner") == ""
                    and _as_text("watcher_token") == ""
                    and _gen_ok
                )
                if _blank:
                    _patch = json.loads(params[0])
                    _meta.update(_patch)
                    self.rowcount = 1
                else:
                    self.rowcount = 0
                return self
        return _C

    def _conn_ctx(self, row_store, monkeypatch):
        _Cursor = self._cursor_class(row_store)
        class _Conn:
            def __enter__(self):
                return _Cursor()
            def __exit__(self, *a):
                return False
        monkeypatch.setattr(osm_mod, "conn", lambda: _Conn())
        monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())

    def test_real_zero_generation_no_watcher_shape_succeeds(self, monkeypatch):
        real_osm = object.__new__(APOrderStateMachine)
        real_osm.client_id = CLIENT_ID
        # The EXACT shape rearm_deferred_materialization_direction_reversal()
        # writes when there is no real watcher_token: watcher_generation
        # is the JSON integer 0, not "".
        row_store = {
            "meta": {
                "current_owner": "",
                "watcher_token": "",
                "watcher_generation": 0,
                "recovery_ownership": "recovery_scheduler",
                "recovery_owner": f"recovery_scheduler:{CLIENT_ID}:prior",
            }
        }
        self._conn_ctx(row_store, monkeypatch)

        _expected_owner = f"recovery_scheduler:{CLIENT_ID}"
        ok = real_osm.retain_recovery_ownership_if_no_watcher(
            LOCAL_ORDER_ID,
            recovery_owner=_expected_owner,
            reason="test_real_zero_generation_shape",
            recovery_retention_mode="paper",
        )

        assert ok is True
        assert row_store["meta"]["current_owner"] == ""
        assert row_store["meta"]["watcher_token"] == ""
        assert row_store["meta"]["watcher_generation"] == 0
        assert row_store["meta"]["recovery_ownership"] == "recovery_scheduler"
        assert row_store["meta"]["recovery_owner"] == _expected_owner

    @pytest.mark.parametrize("generation_value", [1, 4, "abc"])
    def test_nonzero_or_malformed_generation_refused(self, monkeypatch, generation_value):
        real_osm = object.__new__(APOrderStateMachine)
        real_osm.client_id = CLIENT_ID
        row_store = {
            "meta": {
                "current_owner": "",
                "watcher_token": "",
                "watcher_generation": generation_value,
            }
        }
        self._conn_ctx(row_store, monkeypatch)

        ok = real_osm.retain_recovery_ownership_if_no_watcher(
            LOCAL_ORDER_ID,
            recovery_owner=f"recovery_scheduler:{CLIENT_ID}",
            reason="test_nonzero_generation_refused",
            recovery_retention_mode="paper",
        )

        assert ok is False
        assert row_store["meta"]["watcher_generation"] == generation_value
        assert "recovery_ownership" not in row_store["meta"]

    def test_committed_watcher_authority_still_refused(self, monkeypatch):
        """Sanity re-check: the important half of the fence is unweakened."""
        real_osm = object.__new__(APOrderStateMachine)
        real_osm.client_id = CLIENT_ID
        row_store = {
            "meta": {
                "current_owner": "watcher-token-committed",
                "watcher_token": "watcher-token-committed",
                "watcher_generation": 4,
            }
        }
        self._conn_ctx(row_store, monkeypatch)

        ok = real_osm.retain_recovery_ownership_if_no_watcher(
            LOCAL_ORDER_ID,
            recovery_owner=f"recovery_scheduler:{CLIENT_ID}",
            reason="test_committed_watcher_still_refused",
            recovery_retention_mode="paper",
        )

        assert ok is False
        assert row_store["meta"]["current_owner"] == "watcher-token-committed"
        assert row_store["meta"]["watcher_token"] == "watcher-token-committed"
        assert "recovery_ownership" not in row_store["meta"]


# ─────────────────────────────────────────────────────────────────────────────
# PR #421 FINAL P0 CORRECTION — RWR recovery-retention CAS exact fencing
# ─────────────────────────────────────────────────────────────────────────────
#
# retain_recovery_ownership_if_no_watcher() only fences on committed WATCHER
# authority (current_owner/watcher_token/watcher_generation blank). That is
# not enough for REARM_WATCHER_REQUIRED: a stale actor whose own generation/
# identity/broker-state expectation no longer matches the durable row must
# not be able to write recovery ownership just because watcher fields
# happen to still read blank. These tests prove the new exact-fenced
# retain_rearm_watcher_required_recovery_ownership() closes that gap, and
# that authority-lost reasons never even attempt retention at all.


def _install_get_order_injector(osm, mutate_fn):
    """Wrap osm.get_order so the FIRST call whose row shows
    direction_reversal_rearm_requires_watcher=True (empirically confirmed
    to be the RWR block's own _rwr_row = self.osm.get_order(_exp_loid)
    validation read — the earlier in-memory _outcome/_exp_gen capture
    needs no DB read at all) is mutated by mutate_fn before being
    returned. All other calls pass through unmodified.
    """
    _fired = {"done": False}
    _real_get_order = osm.get_order

    def _wrapped(order_id):
        row = _real_get_order(order_id)
        if (
            not _fired["done"]
            and row
            and (row.get("meta") or {}).get(
                "direction_reversal_rearm_requires_watcher"
            ) is True
        ):
            _fired["done"] = True
            row = dict(row)
            row["meta"] = dict(row.get("meta") or {})
            mutate_fn(row["meta"])
        return row

    osm.get_order = _wrapped
    return _fired


def _install_fenced_rwr_retention(osm, row_store):
    """Attach a genuinely conditional (not blind-merge) fake for
    retain_rearm_watcher_required_recovery_ownership, mirroring the real
    SQL predicate against row_store directly. The harness's shared
    osm_mod.conn patch is a rearm-shaped blind-merge cursor that would
    otherwise make ANY CAS "succeed" regardless of its real WHERE clause
    — this fake restores genuine fencing for tests that need to prove the
    CAS itself refuses, not just that the caller chose not to call it.
    Returns a call-log list of every invocation for spying.
    """
    calls = []

    def _fake(local_order_id, *, recovery_owner, reason, recovery_retention_mode,
              client_id, signal_id, execution_mode, generation,
              expected_recovery_owner, canonical_signal_id=""):
        calls.append({
            "local_order_id": local_order_id, "reason": reason,
            "generation": generation, "client_id": client_id,
            "signal_id": signal_id, "execution_mode": execution_mode,
        })
        if local_order_id != LOCAL_ORDER_ID:
            return False
        _meta = row_store["row"].get("meta") or {}
        _row = row_store["row"]
        _ok = (
            str(_row.get("client_id") or "").strip().lower() == str(client_id).lower()
            and str(_row.get("signal_id") or "").strip() == str(signal_id)
            and str(_row.get("execution_mode") or "").strip().lower()
            == str(execution_mode).lower()
            and str(_row.get("kind") or "").strip().upper() == "ENTRY"
            and str(_row.get("status") or "").strip().upper() == "PENDING_TRIGGER"
            and not str(_row.get("broker_order_id") or "").strip()
            and not _row.get("submitted_ts")
            and not str(_meta.get("submit_intent_at") or "")
            and str(_meta.get("broker_ready") or "false").lower() in ("false", "")
            and int(_meta.get("materialization_generation") or 0) == int(generation)
            and str(_meta.get("recovery_ownership") or "") == "recovery_scheduler"
            and str(_meta.get("recovery_owner") or "") == str(expected_recovery_owner)
            and not str(_meta.get("current_owner") or "")
            and not str(_meta.get("watcher_token") or "")
            and str(_meta.get("watcher_generation") or "") in ("", "0")
        )
        if not _ok:
            return False
        _meta = dict(_meta)
        _meta.update({
            "recovery_ownership": "recovery_scheduler",
            "recovery_owner": recovery_owner,
        })
        row_store["row"]["meta"] = _meta
        return True

    osm.retain_rearm_watcher_required_recovery_ownership = _fake
    return calls


class TestRWRRetentionRefusedAfterGenerationAdvancement:
    """Test A — a newer pass already advanced the row's generation past
    what this actor expected. Retention must never even be attempted."""

    def test_stale_generation_performs_zero_ownership_mutation(self, monkeypatch):
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_available(broker, symbol, side, trigger):
            return False

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_available,
        )

        retention_calls = _install_fenced_rwr_retention(osm, row_store)

        def _advance_generation(meta):
            meta["materialization_generation"] = NEW_GEN + 1

        _install_get_order_injector(osm, _advance_generation)

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        # Retention was never even attempted for this reason.
        assert retention_calls == []
        # Definitive proof no retention write landed via ANY path
        # (including a pre-fix fallback to the older, less-fenced
        # retain_recovery_ownership_if_no_watcher): that field is
        # populated only by an actual retention CAS write, never by
        # the rearm write itself.
        assert "recovery_retained_at" not in row_store["row"]["meta"]
        # The underlying durable row was never mutated by the stale
        # actor — it still shows exactly the generation it had before
        # the injected concurrent-advance was observed (the injector
        # mutates only the copy returned to validation code, mirroring
        # "another actor's write already landed and this stale actor's
        # own read now sees it" without this test needing a second real
        # writer).
        assert row_store["row"]["meta"]["materialization_generation"] == NEW_GEN
        assert any(
            "generation_advanced_concurrently" in e
            for e in result.get("errors", [])
        )
        # No watcher/broker side effects from the stale actor either.
        assert watcher.has_order(LOCAL_ORDER_ID) is False
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []


class TestRWRRetentionRefusedAfterBrokerAdvancement:
    """Test B — the row gained broker/submission activity (crossed the
    crash window) since this actor's expectation was formed."""

    @pytest.mark.parametrize("field,value", [
        ("broker_order_id", "brk-12345"),
        ("submitted_ts", "2026-08-07T12:00:00Z"),
    ])
    def test_broker_column_advancement_performs_zero_mutation(
        self, monkeypatch, field, value,
    ):
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_available(broker, symbol, side, trigger):
            return False

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_available,
        )

        retention_calls = _install_fenced_rwr_retention(osm, row_store)

        _real_get_order = osm.get_order
        _fired = {"done": False}

        def _wrapped(order_id):
            row = _real_get_order(order_id)
            if (
                not _fired["done"]
                and row
                and (row.get("meta") or {}).get(
                    "direction_reversal_rearm_requires_watcher"
                ) is True
            ):
                _fired["done"] = True
                row = dict(row)
                row[field] = value
            return row

        osm.get_order = _wrapped

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        assert retention_calls == []
        # Definitive proof no retention write landed via ANY path
        # (including a pre-fix fallback to the older, less-fenced
        # retain_recovery_ownership_if_no_watcher): that field is
        # populated only by an actual retention CAS write, never by
        # the rearm write itself.
        assert "recovery_retained_at" not in row_store["row"]["meta"]
        assert row_store["row"].get(field) in (None, "", 0)
        assert any(
            "state_invalid_or_crash_window" in e for e in result.get("errors", [])
        )
        assert watcher.has_order(LOCAL_ORDER_ID) is False
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []

    def test_broker_ready_advancement_performs_zero_mutation(self, monkeypatch):
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_available(broker, symbol, side, trigger):
            return False

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_available,
        )

        retention_calls = _install_fenced_rwr_retention(osm, row_store)

        def _advance_broker_ready(meta):
            meta["broker_ready"] = True

        _install_get_order_injector(osm, _advance_broker_ready)

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        assert retention_calls == []
        # Definitive proof no retention write landed via ANY path
        # (including a pre-fix fallback to the older, less-fenced
        # retain_recovery_ownership_if_no_watcher): that field is
        # populated only by an actual retention CAS write, never by
        # the rearm write itself.
        assert "recovery_retained_at" not in row_store["row"]["meta"]
        assert row_store["row"]["meta"].get("broker_ready") in (None, False, "false")
        assert any(
            "state_invalid_or_crash_window" in e for e in result.get("errors", [])
        )
        assert watcher.has_order(LOCAL_ORDER_ID) is False


class TestRWRRetentionRefusedAfterIdentityAdvancement:
    """Test C — durable identity no longer matches what this actor
    expected (a different signal now occupies the row's identity slot,
    or execution_mode changed)."""

    def test_signal_id_mismatch_performs_zero_mutation(self, monkeypatch):
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_available(broker, symbol, side, trigger):
            return False

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_available,
        )

        retention_calls = _install_fenced_rwr_retention(osm, row_store)

        _real_get_order = osm.get_order
        _fired = {"done": False}

        def _wrapped(order_id):
            row = _real_get_order(order_id)
            if (
                not _fired["done"]
                and row
                and (row.get("meta") or {}).get(
                    "direction_reversal_rearm_requires_watcher"
                ) is True
            ):
                _fired["done"] = True
                row = dict(row)
                row["signal_id"] = "sig-DIFFERENT-001"
            return row

        osm.get_order = _wrapped

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        assert retention_calls == []
        # Definitive proof no retention write landed via ANY path
        # (including a pre-fix fallback to the older, less-fenced
        # retain_recovery_ownership_if_no_watcher): that field is
        # populated only by an actual retention CAS write, never by
        # the rearm write itself.
        assert "recovery_retained_at" not in row_store["row"]["meta"]
        assert row_store["row"]["signal_id"] == SIGNAL_ID  # unchanged
        assert any(
            "identity_mismatch" in e for e in result.get("errors", [])
        )
        assert watcher.has_order(LOCAL_ORDER_ID) is False
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []

    def test_execution_mode_mismatch_performs_zero_mutation(self, monkeypatch):
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_available(broker, symbol, side, trigger):
            return False

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_available,
        )

        retention_calls = _install_fenced_rwr_retention(osm, row_store)

        _real_get_order = osm.get_order
        _fired = {"done": False}

        def _wrapped(order_id):
            row = _real_get_order(order_id)
            if (
                not _fired["done"]
                and row
                and (row.get("meta") or {}).get(
                    "direction_reversal_rearm_requires_watcher"
                ) is True
            ):
                _fired["done"] = True
                row = dict(row)
                row["execution_mode"] = "live"
            return row

        osm.get_order = _wrapped

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        assert retention_calls == []
        # Definitive proof no retention write landed via ANY path
        # (including a pre-fix fallback to the older, less-fenced
        # retain_recovery_ownership_if_no_watcher): that field is
        # populated only by an actual retention CAS write, never by
        # the rearm write itself.
        assert "recovery_retained_at" not in row_store["row"]["meta"]
        assert row_store["row"]["execution_mode"] == EXEC_MODE  # unchanged
        assert any(
            "identity_mismatch" in e for e in result.get("errors", [])
        )
        assert watcher.has_order(LOCAL_ORDER_ID) is False


class TestRWRRetentionExactFenceCatchesLateAdvancementDuringAdoption:
    """Test D — watcher registration succeeds (this attempt's own
    provenance is genuine), but before durable adoption lands, another
    actor advances the row's generation. The adoption CAS naturally
    loses; this attempt's own watcher rollback fires correctly (exact-
    registration fencing, unregressed); and — the point of this test —
    the SEPARATE, later recovery-retention attempt must ALSO be refused
    by the exact-fenced CAS, even though nothing in the reason-based
    retain=False skip-list caught it (ownership_adoption_failed defaults
    to retain=True). This proves the CAS itself is a real second line of
    defense, not merely the caller's reason-based routing.
    """

    def test_late_advance_between_registration_and_adoption_blocks_retention(
        self, monkeypatch,
    ):
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_available(broker, symbol, side, trigger):
            return False

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_available,
        )

        retention_calls = _install_fenced_rwr_retention(osm, row_store)

        # Validation itself must PASS (generation still matches at that
        # point) — registration proceeds normally. The advance happens
        # ONLY at the adoption-CAS boundary: force the real adopt CAS to
        # report loss, and as a side effect of that exact call, advance
        # the durable row's generation — simulating a concurrent actor's
        # write landing in the gap between this attempt's registration
        # and its own adoption CAS.
        def _adopt_loses_and_advances(*a, **kw):
            row_store["row"]["meta"]["materialization_generation"] = NEW_GEN + 1
            return False

        monkeypatch.setattr(
            osm, "adopt_direction_reversal_watcher_ownership",
            _adopt_loses_and_advances,
        )

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        # This attempt's own watcher rollback still worked correctly —
        # unregressed exact-registration fencing.
        assert watcher.has_order(LOCAL_ORDER_ID) is False
        assert len(watcher._pending) == 0
        assert SIGNAL_ID not in watcher._dedup_set

        # Retention WAS attempted (ownership_adoption_failed defaults to
        # retain=True) ...
        assert len(retention_calls) == 1
        assert retention_calls[0]["generation"] == NEW_GEN
        # ... but the exact-fenced CAS itself refused it: the advanced
        # generation is untouched, and no recovery-owner write landed on
        # top of it from this stale attempt.
        assert row_store["row"]["meta"]["materialization_generation"] == (
            NEW_GEN + 1
        )
        assert any(
            "ownership_adoption_failed" in e for e in result.get("errors", [])
        )
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []


class TestRWROuterExceptionHandlerNeverRetainsStaleAuthority:
    """The broad outer `except Exception as _rwr_exc:` wraps the ENTIRE RWR
    validation+processing sequence. An unexpected exception there proves
    nothing about whether this actor's generation/identity/state
    expectation still matches the durable row -- it can fire well after a
    concurrent actor has already advanced authority. Falling back to the
    older, generic _retain_recovery_ownership() (fenced only on blank
    watcher fields, not on exact generation/identity/state) let a stale
    actor write recovery ownership onto a row it no longer owned. The
    fix removes that fallback entirely: the exception path must be fail-
    closed for durable ownership writes.
    """

    def test_unexpected_exception_after_stale_advance_performs_zero_mutation(
        self, monkeypatch,
    ):
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch, quote_bid=98.0, quote_ask=98.5)
        )

        def _quote_check_available(broker, symbol, side, trigger):
            return False

        import ap.pending_trigger_restart_recovery as ptr_mod
        monkeypatch.setattr(
            ptr_mod, "_default_quote_check", _quote_check_available,
        )

        retention_calls = _install_fenced_rwr_retention(osm, row_store)

        # At the exact moment this actor's own RWR validation read would
        # run (the same injection point empirically confirmed for Tests
        # A-D: the first get_order() call whose row shows
        # direction_reversal_rearm_requires_watcher=True), genuinely
        # advance the UNDERLYING durable generation (not merely a
        # returned copy -- this must remain visible after the exception
        # path runs) and then raise, landing in the broad outer
        # REARM_WATCHER_REQUIRED_HANDLER_EXCEPTION branch rather than any
        # reason-classified _rwr_fail() path.
        _fired = {"done": False}
        _real_get_order = osm.get_order

        def _wrapped(order_id):
            row = _real_get_order(order_id)
            if (
                not _fired["done"]
                and row
                and (row.get("meta") or {}).get(
                    "direction_reversal_rearm_requires_watcher"
                ) is True
            ):
                _fired["done"] = True
                row_store["row"]["meta"]["materialization_generation"] = (
                    NEW_GEN + 1
                )
                raise RuntimeError(
                    "simulated unexpected exception reaching the outer "
                    "REARM_WATCHER_REQUIRED exception handler"
                )
            return row

        osm.get_order = _wrapped

        result = {"errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        assert _fired["done"], "setup: the injected exception must have fired"

        # (4) The exact-fenced retention CAS was never invoked either.
        assert retention_calls == []
        # (5) No retention write of any kind landed -- this field is only
        # ever populated by an actual retention CAS write.
        assert "recovery_retained_at" not in row_store["row"]["meta"]
        # (6) The advanced durable generation is exactly what the
        # concurrent actor set it to -- untouched by the stale actor.
        assert row_store["row"]["meta"]["materialization_generation"] == (
            NEW_GEN + 1
        )
        # (7) Zero watcher side effects.
        assert watcher.has_order(LOCAL_ORDER_ID) is False
        assert len(watcher._pending) == 0
        # (8) Zero broker/submission side effects.
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []

        # The exception is still logged/recorded as an error, just
        # without any ownership mutation.
        assert any(
            "rearm_watcher_required_exception" in e
            for e in result.get("errors", [])
        )
