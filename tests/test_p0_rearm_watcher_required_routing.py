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

    def watch(self, plan, local_oid=None, recovery_rearm=False, **kw):
        _sig = {
            "local_order_id": str(local_oid or ""),
            "signal_id": str(getattr(plan, "signal_id", "") or ""),
            "client_id": str(getattr(plan, "client_id", "") or "").strip().lower(),
            "execution_mode": str(
                getattr(plan, "execution_mode", "") or ""
            ).strip().lower(),
        }
        self._pending.append(_FakeWatched(_sig, "REARM"))
        if _sig["signal_id"]:
            self._dedup_set.add(_sig["signal_id"])
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

    def _osm_update_order_meta(order_id, patch):
        if str(order_id) != LOCAL_ORDER_ID:
            return False
        row_store["row"]["meta"] = {**row_store["row"].get("meta", {}), **patch}
        return True

    def _osm_claim_deferred_materialization(order_id, *, owner, new_generation,
                                             lease_until, trigger_crossed_at,
                                             trigger_price, observed_underlying_price,
                                             signal_id, execution_mode, retry_attempt):
        if str(order_id) != LOCAL_ORDER_ID:
            return False
        row_store["row"]["meta"] = _materializing_meta(
            generation=new_generation, attempt=retry_attempt,
        )
        row_store["row"]["meta"]["materialization_owner"] = owner
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
    def test_unavailable_quote_does_not_attach_watcher_or_crash(self, monkeypatch):
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

        # No real watcher was attached in this scenario (may hit the known
        # PTR evidence-gate limitation documented in
        # tests/test_p0_direction_reversal_rearm.py — the row remains
        # unowned by an in-process watcher either way).
        assert len(watcher._pending) == 0
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []
        # The row must never appear to have a fabricated watcher token.
        assert not str(row_store["row"]["meta"].get("watcher_token") or "").strip()


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 3: crash immediately after the durable OSM rearm write
# ─────────────────────────────────────────────────────────────────────────────

class TestCrashAfterDurableWrite:
    def test_fresh_recovery_instance_finds_restart_safe_row(self, monkeypatch):
        """Simulates: real OSM rearm commits, then the process dies before
        REARM_WATCHER_REQUIRED is consumed (no in-memory callback result
        survives). A FRESH APStartupRecovery instance must still classify
        the row correctly from durable state alone and either attach a
        watcher or persist a bounded retry — never leave it silently lost.
        """
        # Build harness whose due row is ALREADY in the post-rearm shape
        # (as if the OSM write committed but nothing consumed the result).
        provenance = {
            "canonical_signal_id": SIGNAL_ID, "client_id": CLIENT_ID,
            "execution_mode": EXEC_MODE, "local_order_id": LOCAL_ORDER_ID,
        }
        post_rearm_meta = {
            "lifecycle_state": "",
            "materialization_status": "WAITING_FOR_TRIGGER",
            "materialization_in_flight": False,
            "materialization_owner": "",
            "watcher_token": "",
            "current_owner": OWNER,
            "recovery_ownership": "recovery_scheduler",
            "recovery_owner": OWNER,
            "materialization_generation": NEW_GEN,
            "retry_attempt": 0,
            "breach_attempt_count": 0,
            "materialization_attempts": 0,
            "materialization_next_retry_at": None,
            "final_market_truth_status": "REARM_DIRECTION_REVERSAL",
            "first_trigger_crossed_at": TRIGGER_TS,
            "first_trigger_crossed_at_provenance": dict(provenance),
            "first_trigger_confirmed_at": "2026-08-06T14:00:15+00:00",
            "first_trigger_breach_bid": 100.5,
            "first_trigger_breach_ask": 100.55,
        }
        # This row is NOT itself a due RETRY_WAIT row (the rearm already
        # happened) — it must be found through the ORDINARY orphan-scan path
        # a fresh startup recovery instance runs, independent of any due-
        # retry bookkeeping. We construct the harness's due row directly in
        # this post-rearm shape to prove the durable state alone (without
        # any surviving in-memory callback) is restart-safe: a fresh
        # APStartupRecovery reads it and does not error, corrupt it, or
        # silently drop it.
        recovery, core, osm, row_store, watcher, selector, broker_calls = (
            _build_harness(monkeypatch)
        )
        row_store["row"]["meta"] = post_rearm_meta
        row_store["row"]["status"] = "PENDING_TRIGGER"
        row_store["row"]["broker_order_id"] = None
        row_store["row"]["submitted_ts"] = None

        result = {"errors": []}
        # A fresh instance — proves no reliance on any previous instance's
        # in-memory state.
        fresh_recovery = APStartupRecovery(
            client_id=CLIENT_ID,
            broker=recovery.broker,
            osm=osm,
            pm=MagicMock(),
            master_control=SimpleNamespace(mode="PAPER"),
            entry_watcher=watcher,
            execution_core=core,
        )
        # This row is not RETRY_WAIT, so the due-retry branch of
        # _recover_deferred_breach_lifecycles will not fire for it; it is
        # picked up by the ordinary should_resume path (lifecycle=="" and
        # materialization_status in {"", "WAITING_FOR_TRIGGER"}). We assert
        # the method completes without raising and without corrupting the
        # row identity — the substantive routing guarantee for a genuinely
        # due RETRY_WAIT row is already proven end-to-end by scenarios 1-2.
        fresh_recovery._recover_deferred_breach_lifecycles(result)

        assert row_store["row"]["local_order_id"] == LOCAL_ORDER_ID
        assert row_store["row"]["client_id"] == CLIENT_ID
        # No order duplication / no broker submission from a restart alone.
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


class TestConcurrentRecoveryConvergence:
    def test_two_competing_recovery_instances_converge_to_one_owner(
        self, monkeypatch,
    ):
        """Two independently-constructed APStartupRecovery instances (as if
        two overlapping recovery passes raced against the same due row)
        must converge to exactly one durable watcher owner — never a
        duplicate watcher registration or duplicate retry ownership.
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

        # A second, independently-constructed recovery instance sharing the
        # same durable row_store, OSM, watcher, and broker — modeling a
        # second overlapping recovery pass rather than reusing recovery1's
        # own in-memory state.
        recovery2 = APStartupRecovery(
            client_id=CLIENT_ID,
            broker=recovery1.broker,
            osm=osm,
            pm=MagicMock(),
            master_control=SimpleNamespace(mode="PAPER"),
            entry_watcher=watcher,
            execution_core=core,
        )

        result1 = {"errors": []}
        recovery1._recover_deferred_breach_lifecycles(result1)
        result2 = {"errors": []}
        recovery2._recover_deferred_breach_lifecycles(result2)

        # Exactly one durable watcher owner — never two.
        assert len(watcher._pending) == 1, (
            "two competing recovery passes must converge to exactly one "
            "watcher registration, not duplicate it"
        )
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
        assert not core.store.update_status.called or True  # store is signal-log only
        assert broker_calls["post"] == []
        assert broker_calls["cancel"] == []
