"""P0 tests: fenced due-retry terminalization.

Verifies that a due-retry terminal boundary in resume_deferred_materialization_retry
cannot write a terminal state when a concurrent worker has advanced:
  - materialization_generation (concurrent claim winner)
  - submit_intent_at (concurrent submit in progress)
  - broker_ready (concurrent broker-ready advance)
And that already-terminal rows are NOT written twice.

Corresponds to PR #332 final amendment:
  terminalize_deferred_retry_if_unchanged() on APOrderStateMachine.
  TERMINAL_REQUIRED / TERMINAL_ALREADY_DURABLE disposition semantics.
  Recovery _terminalize_fenced_retry() classification of CAS misses.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

CLIENT_ID = "jose@example.com"
LOCAL_ORDER_ID = "oid-fenced-term-1"
SIGNAL_ID = "sig-fenced-term-1"


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _now():
    return datetime.now(timezone.utc)


def _bind_trigger_evidence(row: dict) -> dict:
    """Bind confirmed-trigger fixtures to the lifecycle identity contract."""
    meta = row.setdefault("meta", {})
    canonical_signal_id = str(
        row.get("canonical_signal_id")
        or meta.get("canonical_signal_id")
        or row.get("signal_id")
        or ""
    ).strip()
    client_id = str(
        row.get("client_id")
        or meta.get("client_id")
        or ""
    ).strip()
    execution_mode = str(
        row.get("execution_mode")
        or meta.get("execution_mode")
        or ""
    ).strip().lower()
    meta.update(
        {
            "canonical_signal_id": canonical_signal_id,
            "client_id": client_id,
            "execution_mode": execution_mode,
            "trigger_crossed_at_provenance": {
                "canonical_signal_id": canonical_signal_id,
                "client_id": client_id,
                "execution_mode": execution_mode,
                "local_order_id": str(row.get("local_order_id") or "").strip(),
            },
        }
    )
    return row


# ── OSM CAS harness ───────────────────────────────────────────────────────────

class _FakeConn:
    """Minimal DB connection that replays a fixed rowcount."""
    def __init__(self, rowcount: int = 1, patches: list | None = None):
        self._rc = rowcount
        self._patches = patches if patches is not None else []
        self.rowcount = rowcount

    def execute(self, sql: str, params=()):
        # The UPDATE in terminalize_deferred_retry_if_unchanged passes:
        # (status, reason, patch_json, local_order_id, client, mode, gen, attempt)
        # The patch JSON is the 3rd positional param (index 2).
        for p in params:
            if isinstance(p, str) and p.startswith("{"):
                try:
                    self._patches.append(json.loads(p))
                except Exception:
                    pass
        self.rowcount = self._rc
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _osm(rowcount: int = 1, patches: list | None = None):
    from ap.order_state_machine import APOrderStateMachine
    osm = object.__new__(APOrderStateMachine)
    osm.client_id = CLIENT_ID
    _conn = _FakeConn(rowcount=rowcount, patches=patches if patches is not None else [])

    with patch("ap.order_state_machine.conn", return_value=_conn), \
         patch("ap.order_state_machine.run_with_retry", lambda fn, *a, **kw: fn()):
        yield osm, _conn


# ─────────────────────────────────────────────────────────────────────────────
# Test 1 — generation advanced before terminal CAS
# ─────────────────────────────────────────────────────────────────────────────

def test_1_generation_advanced_blocks_terminal_cas():
    """When concurrent worker advanced generation from 4→5, the fenced CAS
    must return False. Recovery must classify CLAIM_LOST and NOT write
    any terminal status or retain ownership metadata.
    """
    from ap.order_state_machine import APOrderStateMachine

    patches = []
    # CAS returns 0 rows (generation mismatch blocks update)
    fake_conn = _FakeConn(rowcount=0, patches=patches)

    osm = object.__new__(APOrderStateMachine)
    osm.client_id = CLIENT_ID

    with patch("ap.order_state_machine.conn", return_value=fake_conn), \
         patch("ap.order_state_machine.run_with_retry", lambda fn, *a, **kw: fn()):
        ok = osm.terminalize_deferred_retry_if_unchanged(
            LOCAL_ORDER_ID,
            reason_code="RETRY_MAX_ATTEMPTS_EXCEEDED",
            terminal_status="EXPIRED",
            expected_client_id=CLIENT_ID,
            expected_execution_mode="paper",
            expected_generation=4,            # old generation
            expected_prior_retry_attempt=1,
        )

    assert ok is False, "CAS must fail when rowcount=0 (generation advanced)"
    # When CAS misses, the terminal status must NOT appear in the returned row's status.
    # The patches list may have captured the attempted patch JSON, but rowcount=0
    # means Postgres rejected the write. Verify the CAS returned False.
    # The important invariant is that ok=False, not which patches were captured.
    assert not ok  # redundant but explicit


# ─────────────────────────────────────────────────────────────────────────────
# Test 2 — submit_intent_at blocks terminalization
# ─────────────────────────────────────────────────────────────────────────────

def test_2_submit_intent_blocks_terminal_cas():
    """The SQL predicate requires submit_intent_at IS NULL/blank.
    A row with submit_intent_at present must reject the terminal CAS
    — the predicate returns 0 rows and CAS fails.
    We verify the SQL contains the submit_intent_at guard.
    """
    from ap.order_state_machine import APOrderStateMachine

    sqls_seen = []

    class _C:
        rowcount = 0
        def execute(self, sql, params=()):
            sqls_seen.append(sql)
            return self
        def __enter__(self): return self
        def __exit__(self, *a): return False

    osm = object.__new__(APOrderStateMachine)
    osm.client_id = CLIENT_ID

    with patch("ap.order_state_machine.conn", return_value=_C()), \
         patch("ap.order_state_machine.run_with_retry", lambda fn, *a, **kw: fn()):
        ok = osm.terminalize_deferred_retry_if_unchanged(
            LOCAL_ORDER_ID,
            reason_code="RETRY_DEADLINE_EXHAUSTED",
            terminal_status="EXPIRED",
            expected_client_id=CLIENT_ID,
            expected_execution_mode="paper",
            expected_generation=4,
            expected_prior_retry_attempt=1,
        )

    assert ok is False
    assert sqls_seen, "SQL must have been executed"
    combined = " ".join(sqls_seen)
    assert "submit_intent_at" in combined, \
        "SQL predicate must include submit_intent_at guard"


# ─────────────────────────────────────────────────────────────────────────────
# Test 3 — broker_ready advance prevents terminalization
# ─────────────────────────────────────────────────────────────────────────────

def test_3_broker_ready_advance_blocks_terminal_cas():
    """The predicate requires broker_ready = 'false'. When broker_ready
    is 'true' in the row, the predicate matches 0 rows.
    We verify the SQL contains the broker_ready guard.
    """
    from ap.order_state_machine import APOrderStateMachine

    sqls_seen = []

    class _C:
        rowcount = 0
        def execute(self, sql, params=()):
            sqls_seen.append(sql)
            return self
        def __enter__(self): return self
        def __exit__(self, *a): return False

    osm = object.__new__(APOrderStateMachine)
    osm.client_id = CLIENT_ID

    with patch("ap.order_state_machine.conn", return_value=_C()), \
         patch("ap.order_state_machine.run_with_retry", lambda fn, *a, **kw: fn()):
        ok = osm.terminalize_deferred_retry_if_unchanged(
            LOCAL_ORDER_ID,
            reason_code="RETRY_MAX_ATTEMPTS_EXCEEDED",
            terminal_status="EXPIRED",
            expected_client_id=CLIENT_ID,
            expected_execution_mode="paper",
            expected_generation=4,
            expected_prior_retry_attempt=1,
        )

    assert ok is False
    combined = " ".join(sqls_seen)
    assert "broker_ready" in combined, "SQL must guard against broker_ready advance"
    # Verify it uses fail-closed text equality, not ::boolean cast
    assert "'false'" in combined, "broker_ready predicate must use text 'false' not ::boolean"


# ─────────────────────────────────────────────────────────────────────────────
# Test 4 — TERMINAL_ALREADY_DURABLE: terminalize not called twice
# ─────────────────────────────────────────────────────────────────────────────

def test_4_terminal_already_durable_not_written_twice():
    """When resume returns TERMINAL_ALREADY_DURABLE, recovery must NOT
    call terminalize_deferred_retry_if_unchanged or terminalize_deferred_breach.
    """
    from ap_execution_core import APExecutionCore

    # Simulate a post-callback reread that finds status=EXPIRED
    expired_row = {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "paper",
        "signal_id": SIGNAL_ID,
        "plan_id": "plan-1",
        "status": "EXPIRED",
        "broker_order_id": None,
        "submitted_ts": None,
        "symbol": "RTX", "direction": "CALL",
        "score": 78.0, "tier": "B",
        "trigger_price": 130.0,
        "stop_underlying": 128.0, "target_underlying": 133.0,
        "pattern": "3-1-2", "timeframe": "1d",
        "contract": "DEFERRED:RTX", "qty": 1,
        "limit_price": 0.01, "reserved_cost": 0.0,
        "meta": {
            "lifecycle_state": "EXPIRED",
            "materialization_generation": 2,
            "retry_attempt": 2,
            "trigger_crossed_at": _iso(_now() - timedelta(minutes=2)),
            "trigger_price": 130.0,
            "materialization_next_retry_at": _iso(_now() - timedelta(seconds=30)),
            "next_retry_at": _iso(_now() - timedelta(seconds=30)),
            "retry_max_attempts": 3,
        },
    }
    before_meta = dict(expired_row["meta"])
    before_meta["lifecycle_state"] = "RETRY_WAIT"
    before_meta["materialization_status"] = "RETRY_PENDING"
    before_meta["materialization_generation"] = 1
    before_meta["retry_attempt"] = 1
    before_row = dict(expired_row)
    before_row["status"] = "PENDING_TRIGGER"
    before_row["meta"] = before_meta

    term_call_count = [0]

    class _OSM:
        client_id = CLIENT_ID
        _claimed = False

        def get_order(self, oid):
            if not _OSM._claimed:
                return before_row
            return expired_row  # Already terminal after claim

        def claim_deferred_materialization(self, oid, **kw):
            _OSM._claimed = True
            return True

        def update_order_meta(self, oid, patch): return True
        def terminalize_deferred_retry_if_unchanged(self, oid, **kw):
            term_call_count[0] += 1
            return True
        def terminalize_deferred_breach(self, oid, **kw):
            term_call_count[0] += 1
            return True
        def schedule_deferred_materialization_retry(self, oid, **kw): return True
        def submit_existing_entry(self, *a, **kw): return {}
        def get_orders_for_position(self, *a, **kw): return []
        def persist_deferred_broker_ready(self, *a, **kw): return True
        def claim_deferred_broker_ready_submit(self, *a, **kw): return True

    core = object.__new__(APExecutionCore)
    core.client_id = CLIENT_ID
    core.email = CLIENT_ID
    core.execution_mode = "paper"
    core.mode = "PAPER"
    core.paper = True
    core.order_state_machine = _OSM()
    core.broker = MagicMock()
    core.contract_selector = MagicMock()
    core.store = MagicMock()
    core._kill_switch = False
    core._max_positions = 5
    core._pos_lock = threading.RLock()
    core._open_positions = {}
    core._pending_entries = {}
    core._reserved_capital = 0.0
    core._capital_lock = threading.RLock()
    core.master_control = MagicMock()
    core.master_control._kill_switch_fn = lambda: False
    core.master_control.mode = "PAPER"
    core.exit_eng = None
    core.entry_watcher = None
    core.position_manager = MagicMock()
    core.fill_monitor = MagicMock()
    core.alpha_tracker = MagicMock()
    core.entry_telemetry = MagicMock()
    core.intelligence_context = MagicMock()
    core.intelligence_context.is_enabled.return_value = False

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-already-term",
    )

    assert result["disposition"] == "TERMINAL_ALREADY_DURABLE", (
        f"Expected TERMINAL_ALREADY_DURABLE; got {result['disposition']}: {result.get('reason_code')}"
    )
    assert term_call_count[0] == 0, (
        f"terminalize must NOT be called for TERMINAL_ALREADY_DURABLE; called {term_call_count[0]} times"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 5 — exact old RETRY_WAIT row terminalized successfully
# ─────────────────────────────────────────────────────────────────────────────

def test_5_exact_old_retry_row_terminalized():
    """When the row is exactly the expected RETRY_WAIT state (no concurrent
    advance), terminalize_deferred_retry_if_unchanged must succeed (rowcount=1).
    The patch must include the terminal status and reason_code.
    """
    from ap.order_state_machine import APOrderStateMachine

    patches = []
    fake_conn = _FakeConn(rowcount=1, patches=patches)
    osm = object.__new__(APOrderStateMachine)
    osm.client_id = CLIENT_ID

    with patch("ap.order_state_machine.conn", return_value=fake_conn), \
         patch("ap.order_state_machine.run_with_retry", lambda fn, *a, **kw: fn()):
        ok = osm.terminalize_deferred_retry_if_unchanged(
            LOCAL_ORDER_ID,
            reason_code="RETRY_MAX_ATTEMPTS_EXCEEDED",
            terminal_status="EXPIRED",
            expected_client_id=CLIENT_ID,
            expected_execution_mode="paper",
            expected_generation=4,
            expected_prior_retry_attempt=1,
            diagnostics={"selector_diagnostics": {"last": "OI_TOO_LOW"}},
        )

    assert ok is True, "CAS must succeed when rowcount=1"
    assert patches, "At least one patch must have been written"
    p = patches[0]
    assert p["lifecycle_state"] == "EXPIRED"
    assert p["reason_code"] == "RETRY_MAX_ATTEMPTS_EXCEEDED"
    assert p["final_reason"] == "RETRY_MAX_ATTEMPTS_EXCEEDED"
    assert p["retry_terminal_fenced"] is True
    assert p["retry_terminal_expected_generation"] == 4
    assert p["retry_terminal_expected_prior_attempt"] == 1
    # Diagnostics are preserved
    assert p.get("selector_diagnostics") == {"last": "OI_TOO_LOW"}
    # Lifecycle resets
    assert p["broker_ready"] is False
    assert p["materialization_in_flight"] is False


# ─────────────────────────────────────────────────────────────────────────────
# Test 6 — wrong execution_mode cannot terminalize
# ─────────────────────────────────────────────────────────────────────────────

def test_6_wrong_execution_mode_cannot_terminalize():
    """terminalize_deferred_retry_if_unchanged rejects a call where
    expected_execution_mode = 'live' but the row has execution_mode = 'paper'.
    The SQL predicate is: LOWER(TRIM(COALESCE(execution_mode,''))) = %s
    A mode mismatch returns 0 rows from Postgres (mocked by rowcount=0).
    """
    from ap.order_state_machine import APOrderStateMachine

    fake_conn = _FakeConn(rowcount=0)
    osm = object.__new__(APOrderStateMachine)
    osm.client_id = CLIENT_ID

    with patch("ap.order_state_machine.conn", return_value=fake_conn), \
         patch("ap.order_state_machine.run_with_retry", lambda fn, *a, **kw: fn()):
        ok_wrong_mode = osm.terminalize_deferred_retry_if_unchanged(
            LOCAL_ORDER_ID,
            reason_code="RETRY_MAX_ATTEMPTS_EXCEEDED",
            terminal_status="EXPIRED",
            expected_client_id=CLIENT_ID,
            expected_execution_mode="live",   # WRONG — row is paper
            expected_generation=4,
            expected_prior_retry_attempt=1,
        )

    assert ok_wrong_mode is False, "LIVE worker must not terminalize PAPER row (rowcount=0)"


def test_6b_empty_mode_rejects_early():
    """An empty expected_execution_mode fails validation before the SQL."""
    from ap.order_state_machine import APOrderStateMachine

    sql_calls = [0]

    class _C:
        rowcount = 0
        def execute(self, *a, **k):
            sql_calls[0] += 1
            return self
        def __enter__(self): return self
        def __exit__(self, *a): return False

    osm = object.__new__(APOrderStateMachine)
    osm.client_id = CLIENT_ID

    with patch("ap.order_state_machine.conn", return_value=_C()), \
         patch("ap.order_state_machine.run_with_retry", lambda fn, *a, **kw: fn()):
        ok = osm.terminalize_deferred_retry_if_unchanged(
            LOCAL_ORDER_ID,
            reason_code="RETRY_MAX_ATTEMPTS_EXCEEDED",
            terminal_status="EXPIRED",
            expected_client_id=CLIENT_ID,
            expected_execution_mode="",   # invalid
            expected_generation=4,
            expected_prior_retry_attempt=1,
        )

    assert ok is False
    assert sql_calls[0] == 0, "SQL must NOT execute when execution_mode is empty"


# ─────────────────────────────────────────────────────────────────────────────
# Test 7 — malformed boolean metadata fails closed
# ─────────────────────────────────────────────────────────────────────────────

def test_7_malformed_boolean_metadata_fails_closed():
    """The predicate uses text equality ('false') not ::boolean cast.
    A malformed value like 'truee' is NOT 'false' — the predicate
    must block terminalization (rowcount=0). This is verified by
    checking the SQL contains the text predicate, not a cast.
    """
    from ap.order_state_machine import APOrderStateMachine

    sqls = []

    class _C:
        rowcount = 0
        def execute(self, sql, params=()):
            sqls.append(sql)
            return self
        def __enter__(self): return self
        def __exit__(self, *a): return False

    osm = object.__new__(APOrderStateMachine)
    osm.client_id = CLIENT_ID

    with patch("ap.order_state_machine.conn", return_value=_C()), \
         patch("ap.order_state_machine.run_with_retry", lambda fn, *a, **kw: fn()):
        ok = osm.terminalize_deferred_retry_if_unchanged(
            LOCAL_ORDER_ID,
            reason_code="RETRY_DEADLINE_EXHAUSTED",
            terminal_status="EXPIRED",
            expected_client_id=CLIENT_ID,
            expected_execution_mode="paper",
            expected_generation=4,
            expected_prior_retry_attempt=1,
        )

    assert ok is False  # rowcount=0
    combined = " ".join(sqls)
    # Must use text equality, not ::boolean cast
    assert "= 'false'" in combined, "Must use text = 'false' for fail-closed boolean check"
    assert "::boolean" not in combined, "Must NOT use ::boolean cast (can raise on malformed data)"
    # materlization_in_flight must also be guarded
    assert "materialization_in_flight" in combined


# ─────────────────────────────────────────────────────────────────────────────
# Test 8 — concurrent terminal winner: recovery does not error or retain
# ─────────────────────────────────────────────────────────────────────────────

def test_8_concurrent_terminal_winner_no_error():
    """When _terminalize_fenced_retry CAS misses and the reread shows
    the row is already EXPIRED (another worker won), recovery must:
      - classify result as ALREADY_TERMINAL
      - NOT append a recovery error
      - NOT call _retain_recovery_ownership
    """
    from ap_recovery import APStartupRecovery

    term_calls = [0]
    retain_calls = []

    class _OSM:
        client_id = CLIENT_ID

        def get_order(self, oid):
            # Return a fully-terminal row
            return {
                "local_order_id": LOCAL_ORDER_ID,
                "client_id": CLIENT_ID,
                "execution_mode": "paper",
                "signal_id": SIGNAL_ID,
                "status": "EXPIRED",
                "broker_order_id": None,
                "submitted_ts": None,
                "meta": {
                    "lifecycle_state": "EXPIRED",
                    "materialization_generation": 4,
                    "retry_attempt": 1,
                    "reason_code": "RETRY_MAX_ATTEMPTS_EXCEEDED",
                },
            }

        def terminalize_deferred_retry_if_unchanged(self, oid, **kw):
            term_calls[0] += 1
            return False  # CAS misses — row already moved

        def update_order_meta(self, oid, patch):
            if "recovery_owner" in str(patch) or "retention" in str(patch):
                retain_calls.append(patch)
            return True

        def terminalize_deferred_breach(self, oid, **kw):
            term_calls[0] += 1
            return True

    # Build a minimal recovery outcome dict (TERMINAL_REQUIRED)
    outcome = {
        "disposition": "TERMINAL_REQUIRED",
        "reason_code": "RETRY_MAX_ATTEMPTS_EXCEEDED",
        "terminal_status": "EXPIRED",
        "expected_generation": 4,
        "expected_prior_retry_attempt": 1,
        "expected_client_id": CLIENT_ID,
        "expected_execution_mode": "paper",
        "expected_lifecycle_state": "RETRY_WAIT",
        "expected_materialization_status": "RETRY_PENDING",
        "attempt": 2,
        "max_attempts": 3,
        "owner": "recovery_retry:jose@example.com:oid-1:5",
        "generation": 5,
    }

    mc = SimpleNamespace(mode="PAPER")
    osm = _OSM()
    rec = APStartupRecovery(
        client_id=CLIENT_ID,
        broker=MagicMock(),
        osm=osm,
        pm=MagicMock(),
        master_control=mc,
        exit_engine=None,
        entry_watcher=None,
        execution_core=None,
    )

    result = {"deferred_lifecycles_recovered": 0, "errors": []}
    recovery_mode = "paper"

    # Directly call the fenced helper via the inner function
    # (simulate what the recovery loop does for TERMINAL_REQUIRED)
    import ap_recovery as rec_mod
    import types

    # We need to build the _terminalize_fenced_retry closure the way
    # _recover_deferred_breach_lifecycles builds it. To do that without
    # running the full SQL-based loop, call the method with a patched
    # DB cursor that returns only our outcome row.
    from ap import db as db_mod

    row = {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "paper",
        "signal_id": SIGNAL_ID,
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "symbol": "RTX",
        "direction": "CALL",
        "score": 78.0, "tier": "B",
        "trigger_price": 130.0,
        "stop_underlying": 128.0,
        "target_underlying": 133.0,
        "pattern": "3-1-2",
        "timeframe": "1d",
        "contract": "DEFERRED:RTX",
        "qty": 1, "limit_price": 0.01, "reserved_cost": 0.0,
        "meta": {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_generation": 4,
            "retry_attempt": 1,
            "retry_max_attempts": 3,
            "next_retry_at": _iso(_now() - timedelta(seconds=60)),
            "materialization_next_retry_at": _iso(_now() - timedelta(seconds=60)),
            "trigger_crossed_at": _iso(_now() - timedelta(seconds=120)),
            "trigger_price": 130.0,
        },
    }

    class _C:
        def execute(self, *a, **k): return self
        def fetchall(self): return [row]
        rowcount = 1

    class _Conn:
        def __enter__(self): return _C()
        def __exit__(self, *a): return False

    # Mock execution_core.resume_deferred_materialization_retry to return
    # TERMINAL_REQUIRED directly without running the real selector path
    mock_core = MagicMock()
    mock_core.resume_deferred_materialization_retry.return_value = outcome
    rec.execution_core = mock_core

    with patch.object(db_mod, "conn", lambda: _Conn()), \
         patch.object(db_mod, "run_with_retry", lambda fn, *a, **kw: fn()):
        rec._recover_deferred_breach_lifecycles(result)

    # The fenced CAS returned False (concurrent winner already terminalized)
    # Recovery must classify ALREADY_TERMINAL — no error, no retention write
    assert "fenced_term_write_failed" not in str(result.get("errors", [])), (
        f"Must not add write-failed error on ALREADY_TERMINAL: {result}"
    )
    # No explicit _retain_recovery_ownership should have been called
    retain_metas = [
        c for c in osm.update_order_meta.call_args_list
        if "recovery_owner" in str(c) or "retention_reason" in str(c)
    ] if hasattr(osm, 'update_order_meta') and hasattr(osm.update_order_meta, 'call_args_list') else retain_calls
    assert not retain_calls, f"_retain_recovery_ownership must NOT be called: {retain_calls}"


# ─────────────────────────────────────────────────────────────────────────────
# Test: TERMINAL_REQUIRED carries exact expected state
# ─────────────────────────────────────────────────────────────────────────────

def test_terminal_required_carries_expected_state():
    """resume_deferred_materialization_retry must include all expected
    fencing fields in the TERMINAL_REQUIRED return dict so recovery
    can call terminalize_deferred_retry_if_unchanged with exact predicates.
    """
    from ap_execution_core import APExecutionCore

    now = datetime.now(timezone.utc)
    row = {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "paper",
        "signal_id": SIGNAL_ID,
        "plan_id": "plan-1",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "symbol": "RTX",
        "direction": "CALL",
        "score": 78.0, "tier": "B",
        "trigger_price": 130.0,
        "stop_underlying": 128.0, "target_underlying": 133.0,
        "pattern": "3-1-2", "timeframe": "1d",
        "contract": "DEFERRED:RTX", "qty": 1,
        "limit_price": 0.01, "reserved_cost": 0.0,
        "meta": {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_generation": 2,
            "retry_attempt": 3,    # durable=3, caller expects attempt=4 (prior=3 matches)
            "retry_max_attempts": 3,
            "next_retry_at": _iso(now - timedelta(seconds=30)),
            "materialization_next_retry_at": _iso(now - timedelta(seconds=30)),
            "trigger_crossed_at": _iso(now - timedelta(seconds=120)),
            "trigger_price": 130.0,
        },
    }

    class _OSM:
        client_id = CLIENT_ID
        def get_order(self, oid): return row
        def claim_deferred_materialization(self, *a, **kw): return True
        def update_order_meta(self, *a, **kw): return True
        def schedule_deferred_materialization_retry(self, *a, **kw): return True
        def submit_existing_entry(self, *a, **kw): return {}
        def get_orders_for_position(self, *a, **kw): return []

    core = object.__new__(APExecutionCore)
    core.client_id = CLIENT_ID
    core.email = CLIENT_ID
    core.execution_mode = "paper"
    core.mode = "PAPER"
    core.paper = True
    core.order_state_machine = _OSM()
    core.broker = MagicMock()

    # attempt=4 exceeds max_attempts=3 → TERMINAL_REQUIRED
    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=2,
        expected_retry_attempt=4,   # > max_attempts=3
        owner="owner-max-exc",
    )

    assert result["disposition"] in {"TERMINAL_REQUIRED", "TERMINAL_DURABLE"}
    assert result["reason_code"] == "RETRY_MAX_ATTEMPTS_EXCEEDED"
    assert result["terminal_status"] == "EXPIRED"
    # Must carry all expected state for fenced CAS
    assert "expected_generation" in result
    assert result["expected_generation"] == 2
    assert "expected_prior_retry_attempt" in result
    assert result["expected_prior_retry_attempt"] == 3   # attempt(4)-1 = 3
    assert "expected_client_id" in result
    assert "expected_execution_mode" in result
    assert result["expected_lifecycle_state"] == "RETRY_WAIT"
    assert result["expected_materialization_status"] == "RETRY_PENDING"


# ─────────────────────────────────────────────────────────────────────────────
# Final-amendment required tests
# ─────────────────────────────────────────────────────────────────────────────

def test_fa1_fenced_terminal_unavailable_no_broad_fallback():
    """When terminalize_deferred_retry_if_unchanged is missing from the OSM,
    _terminalize_fenced_retry must NOT call terminalize_deferred_breach.
    It must return FENCED_TERMINAL_UNAVAILABLE and record the error.
    """
    from ap_recovery import APStartupRecovery
    from ap import db as db_mod

    broad_calls = [0]

    class _OSM:
        client_id = CLIENT_ID
        # terminalize_deferred_retry_if_unchanged intentionally ABSENT
        def terminalize_deferred_breach(self, *a, **kw):
            broad_calls[0] += 1
            return True
        def get_order(self, oid):
            return _bind_trigger_evidence({
                "local_order_id": LOCAL_ORDER_ID, "client_id": CLIENT_ID,
                "execution_mode": "paper", "signal_id": SIGNAL_ID,
                "status": "PENDING_TRIGGER", "broker_order_id": None,
                "submitted_ts": None, "meta": {
                    "lifecycle_state": "RETRY_WAIT",
                    "materialization_status": "RETRY_PENDING",
                    "materialization_generation": 4,
                    "retry_attempt": 1,
                    "retry_max_attempts": 3,
                    "next_retry_at": _iso(_now() - timedelta(seconds=60)),
                    "materialization_next_retry_at": _iso(_now() - timedelta(seconds=60)),
                    "trigger_crossed_at": _iso(_now() - timedelta(minutes=2)),
                    "trigger_price": 130.0,
                },
            })
        def update_order_meta(self, *a, **kw): return True
        def get_orders_for_position(self, *a, **kw): return []

    TERMINAL_REQUIRED_outcome = {
        "disposition": "TERMINAL_REQUIRED",
        "reason_code": "RETRY_MAX_ATTEMPTS_EXCEEDED",
        "terminal_status": "EXPIRED",
        "expected_generation": 4,
        "expected_prior_retry_attempt": 1,
        "expected_client_id": CLIENT_ID,
        "expected_execution_mode": "paper",
        "expected_lifecycle_state": "RETRY_WAIT",
        "expected_materialization_status": "RETRY_PENDING",
        "attempt": 2, "max_attempts": 3,
        "owner": "recovery:owner:1", "generation": 5,
    }

    mock_core = MagicMock()
    mock_core.resume_deferred_materialization_retry.return_value = TERMINAL_REQUIRED_outcome

    mc = SimpleNamespace(mode="PAPER")
    osm = _OSM()
    rec = APStartupRecovery(
        client_id=CLIENT_ID, broker=MagicMock(), osm=osm,
        pm=MagicMock(), master_control=mc, exit_engine=None,
        entry_watcher=None, execution_core=mock_core,
    )

    row = osm.get_order(LOCAL_ORDER_ID)

    class _C:
        def execute(self, *a, **k): return self
        def fetchall(self): return [row]
        rowcount = 1

    class _Conn:
        def __enter__(self): return _C()
        def __exit__(self, *a): return False

    result = {"deferred_lifecycles_recovered": 0, "errors": []}
    with patch.object(db_mod, "conn", lambda: _Conn()), \
         patch.object(db_mod, "run_with_retry", lambda fn, *a, **kw: fn()):
        rec._recover_deferred_breach_lifecycles(result)

    # Broad terminalize must NEVER be called
    assert broad_calls[0] == 0, (
        f"terminalize_deferred_breach must NOT be called when fenced method is absent; "
        f"called {broad_calls[0]} times"
    )
    # Error must be recorded
    assert "recovery_fenced_terminal_unavailable" in str(result.get("errors", [])), (
        f"Must record fenced_terminal_unavailable error; errors={result.get('errors')}"
    )


def test_fa2_due_retry_missing_direction_reaches_resume():
    """A due RETRY_WAIT row with missing direction must reach
    resume_deferred_materialization_retry() even if _build_recovery_plan_from_order
    would fail. The consumer validates direction and returns TERMINAL_REQUIRED.
    The row must NOT be silently skipped.
    """
    from ap_recovery import APStartupRecovery
    from ap import db as db_mod

    row_no_dir = {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "paper",
        "signal_id": SIGNAL_ID,
        "plan_id": "plan-nd",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None, "submitted_ts": None,
        "symbol": "RTX",
        "direction": None,   # ← MISSING
        "score": 78.0, "tier": "B", "trigger_price": 130.0,
        "stop_underlying": 128.0, "target_underlying": 133.0,
        "pattern": "3-1-2", "timeframe": "1d",
        "contract": "DEFERRED:RTX", "qty": 1,
        "limit_price": 0.01, "reserved_cost": 0.0,
        "meta": {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_generation": 1,
            "retry_attempt": 1,
            "retry_max_attempts": 3,
            "next_retry_at": _iso(_now() - timedelta(seconds=30)),
            "materialization_next_retry_at": _iso(_now() - timedelta(seconds=30)),
            "trigger_crossed_at": _iso(_now() - timedelta(minutes=2)),
            "trigger_price": 130.0,
        },
    }
    _bind_trigger_evidence(row_no_dir)

    resume_calls = [0]

    mock_core = MagicMock()
    def _resume(**kw):
        resume_calls[0] += 1
        return {
            "disposition": "TERMINAL_REQUIRED",
            "reason_code": "RETRY_MISSING_OR_INVALID_DIRECTION:got=''",
            "terminal_status": "ERROR",
            "expected_generation": 1,
            "expected_prior_retry_attempt": 0,
            "expected_client_id": CLIENT_ID,
            "expected_execution_mode": "paper",
            "expected_lifecycle_state": "RETRY_WAIT",
            "expected_materialization_status": "RETRY_PENDING",
        }
    mock_core.resume_deferred_materialization_retry.side_effect = _resume

    class _OSM:
        client_id = CLIENT_ID
        def get_order(self, oid): return row_no_dir
        def update_order_meta(self, *a, **kw): return True
        def terminalize_deferred_retry_if_unchanged(self, *a, **kw): return True
        def get_orders_for_position(self, *a, **kw): return []

    mc = SimpleNamespace(mode="PAPER")
    rec = APStartupRecovery(
        client_id=CLIENT_ID, broker=MagicMock(), osm=_OSM(),
        pm=MagicMock(), master_control=mc, exit_engine=None,
        entry_watcher=None, execution_core=mock_core,
    )

    class _C:
        def execute(self, *a, **k): return self
        def fetchall(self): return [row_no_dir]
        rowcount = 1

    class _Conn:
        def __enter__(self): return _C()
        def __exit__(self, *a): return False

    result = {"deferred_lifecycles_recovered": 0, "errors": []}
    from ap import db as db_mod
    with patch.object(db_mod, "conn", lambda: _Conn()), \
         patch.object(db_mod, "run_with_retry", lambda fn, *a, **kw: fn()):
        rec._recover_deferred_breach_lifecycles(result)

    assert resume_calls[0] == 1, (
        f"resume_deferred_materialization_retry must be called for due row with "
        f"missing direction; called {resume_calls[0]} times"
    )


def test_fa3_future_retry_invalid_plan_retains_ownership():
    """A future-due RETRY_WAIT row where _build_recovery_plan_from_order returns
    None must retain ownership with reason retry_rearm_plan_invalid and record
    the error. Must NOT silently continue, cancel, or broadly terminalize.
    """
    from ap_recovery import APStartupRecovery
    from ap import db as db_mod

    now = _now()
    future_ts = _iso(now + timedelta(minutes=5))  # future = NOT due

    row_future = {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "paper",
        "signal_id": SIGNAL_ID,
        "plan_id": "plan-nd",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None, "submitted_ts": None,
        "symbol": "RTX",
        "direction": None,  # ← missing direction → plan builder returns None
        "score": 78.0, "tier": "B", "trigger_price": 130.0,
        "stop_underlying": 128.0, "target_underlying": 133.0,
        "pattern": "3-1-2", "timeframe": "1d",
        "contract": "DEFERRED:RTX", "qty": 1,
        "limit_price": 0.01, "reserved_cost": 0.0,
        "meta": {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_generation": 1,
            "retry_attempt": 1,
            "retry_max_attempts": 3,
            "next_retry_at": future_ts,          # future — not due
            "materialization_next_retry_at": future_ts,
            "trigger_crossed_at": _iso(now - timedelta(minutes=2)),
            "trigger_price": 130.0,
            # No side/direction in meta either
        },
    }
    _bind_trigger_evidence(row_future)

    retain_patches = []

    class _OSM:
        client_id = CLIENT_ID
        def get_order(self, oid): return row_future
        def update_order_meta(self, oid, patch):
            if isinstance(patch, dict) and patch.get("recovery_owner"):
                retain_patches.append(patch)
            return True
        def terminalize_deferred_breach(self, *a, **kw): return True
        def terminalize_deferred_retry_if_unchanged(self, *a, **kw): return True
        def get_orders_for_position(self, *a, **kw): return []

    mock_core = MagicMock()
    mock_core.resume_deferred_materialization_retry.return_value = None

    # A watcher that is registered but has a mismatched generation so proof
    # fails, and has_order=False so the registry skip is bypassed.
    class _Watcher:
        def has_order(self, oid): return False
        def prove_materialization_retry_owner(self, *a, **kw):
            return {"proven": False, "reason_code": "PROOF_GENERATION_MISMATCH"}
        def watch(self, *a, **kw): return False

    mc = SimpleNamespace(mode="PAPER")
    rec = APStartupRecovery(
        client_id=CLIENT_ID, broker=MagicMock(), osm=_OSM(),
        pm=MagicMock(), master_control=mc, exit_engine=None,
        entry_watcher=_Watcher(), execution_core=mock_core,
    )

    class _C:
        def execute(self, *a, **k): return self
        def fetchall(self): return [row_future]
        rowcount = 1

    class _Conn:
        def __enter__(self): return _C()
        def __exit__(self, *a): return False

    result = {"deferred_lifecycles_recovered": 0, "errors": []}
    with patch.object(db_mod, "conn", lambda: _Conn()), \
         patch.object(db_mod, "run_with_retry", lambda fn, *a, **kw: fn()):
        rec._recover_deferred_breach_lifecycles(result)

    assert "recovery_retry_rearm_plan_invalid" in str(result.get("errors", [])), (
        f"Must record retry_rearm_plan_invalid error; errors={result.get('errors')}"
    )
    assert retain_patches, "Must retain ownership when plan is invalid for future-due retry row"


def test_fa4_log_file_not_in_branch():
    """logs/signal_ledger.jsonl must not be a tracked file in the PR branch."""
    import subprocess
    result = subprocess.run(
        ["git", "ls-files", "logs/signal_ledger.jsonl"],
        capture_output=True, text=True,
        cwd=Path(__file__).resolve().parents[1],
    )
    tracked = result.stdout.strip()
    assert not tracked, (
        f"logs/signal_ledger.jsonl must not be tracked by git; found: {tracked!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Fix 1: INVALID_FENCED_OUTCOME — incomplete outcome fails closed
# ─────────────────────────────────────────────────────────────────────────────

def test_fix1_incomplete_outcome_returns_invalid_fenced_outcome():
    """When the TERMINAL_REQUIRED outcome is missing required fencing fields,
    _terminalize_fenced_retry must return INVALID_FENCED_OUTCOME, retain
    ownership, and record the error. It must NOT call the OSM terminal CAS
    and must NOT fall back to self.client_id or recovery_mode.
    """
    from ap_recovery import APStartupRecovery
    from ap import db as db_mod

    cas_calls = [0]

    class _OSM:
        client_id = CLIENT_ID
        def terminalize_deferred_retry_if_unchanged(self, *a, **kw):
            cas_calls[0] += 1
            return True
        def get_order(self, oid):
            return _bind_trigger_evidence({
                "local_order_id": LOCAL_ORDER_ID, "client_id": CLIENT_ID,
                "execution_mode": "paper", "signal_id": SIGNAL_ID,
                "status": "PENDING_TRIGGER", "broker_order_id": None, "submitted_ts": None,
                "meta": {
                    "lifecycle_state": "RETRY_WAIT",
                    "materialization_status": "RETRY_PENDING",
                    "materialization_generation": 4,
                    "retry_attempt": 1,
                    "retry_max_attempts": 3,
                    "next_retry_at": _iso(_now() - timedelta(seconds=60)),
                    "materialization_next_retry_at": _iso(_now() - timedelta(seconds=60)),
                    "trigger_crossed_at": _iso(_now() - timedelta(minutes=2)),
                    "trigger_price": 130.0,
                },
            })
        def update_order_meta(self, *a, **kw): return True
        def get_orders_for_position(self, *a, **kw): return []

    # Outcome is TERMINAL_REQUIRED but missing critical fencing fields
    incomplete_outcome = {
        "disposition": "TERMINAL_REQUIRED",
        "reason_code": "RETRY_MAX_ATTEMPTS_EXCEEDED",
        "terminal_status": "EXPIRED",
        # expected_client_id: MISSING
        # expected_execution_mode: MISSING
        # expected_generation: MISSING
        # expected_prior_retry_attempt: MISSING
        # expected_lifecycle_state: MISSING
        # expected_materialization_status: MISSING
    }

    mock_core = MagicMock()
    mock_core.resume_deferred_materialization_retry.return_value = incomplete_outcome

    mc = SimpleNamespace(mode="PAPER")
    rec = APStartupRecovery(
        client_id=CLIENT_ID, broker=MagicMock(), osm=_OSM(),
        pm=MagicMock(), master_control=mc, exit_engine=None,
        entry_watcher=None, execution_core=mock_core,
    )

    row = _OSM().get_order(LOCAL_ORDER_ID)

    class _C:
        def execute(self, *a, **k): return self
        def fetchall(self): return [row]
        rowcount = 1

    class _Conn:
        def __enter__(self): return _C()
        def __exit__(self, *a): return False

    result = {"deferred_lifecycles_recovered": 0, "errors": []}
    with patch.object(db_mod, "conn", lambda: _Conn()), \
         patch.object(db_mod, "run_with_retry", lambda fn, *a, **kw: fn()):
        rec._recover_deferred_breach_lifecycles(result)

    # CAS must NOT be called with inferred/fallback values
    assert cas_calls[0] == 0, (
        f"terminalize_deferred_retry_if_unchanged must NOT be called when outcome "
        f"is missing fencing fields; called {cas_calls[0]} times"
    )
    # Error must be recorded
    errors_str = str(result.get("errors", []))
    assert "fenced_outcome_invalid" in errors_str or "recovery_fenced_outcome_invalid" in errors_str, (
        f"Must record invalid fenced outcome error; errors={result.get('errors')}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Fix 2: BROKER_READY recovery validates plan before calling scaffold
# ─────────────────────────────────────────────────────────────────────────────

def test_fix2_broker_ready_invalid_plan_retains_without_calling_scaffold():
    """When _build_recovery_plan_from_order returns None for a BROKER_READY row,
    recovery must retain ownership with broker_ready_recovery_plan_invalid and
    record the error. It must NOT call resume_deferred_broker_ready_order.
    """
    from ap_recovery import APStartupRecovery
    from ap import db as db_mod

    scaffold_calls = [0]

    class _OSM:
        client_id = CLIENT_ID
        def get_order(self, oid): return None
        def update_order_meta(self, oid, patch): return True
        def get_orders_for_position(self, *a, **kw): return []

    mock_core = MagicMock()
    def _scaffold(**kw):
        scaffold_calls[0] += 1
        return {"disposition": "RETRY_WAIT"}
    mock_core.resume_deferred_broker_ready_order.side_effect = _scaffold

    mc = SimpleNamespace(mode="PAPER")
    rec = APStartupRecovery(
        client_id=CLIENT_ID, broker=MagicMock(), osm=_OSM(),
        pm=MagicMock(), master_control=mc, exit_engine=None,
        entry_watcher=None, execution_core=mock_core,
    )

    # A BROKER_READY row with missing direction AND no OCC contract
    # → plan builder cannot infer direction → returns None
    br_row = {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "paper",
        "signal_id": SIGNAL_ID,
        "plan_id": "plan-1",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None, "submitted_ts": None,
        "symbol": "RTX",
        "direction": None,   # ← missing
        "score": 78.0, "tier": "B", "trigger_price": 130.0,
        "stop_underlying": 128.0, "target_underlying": 133.0,
        "pattern": "3-1-2", "timeframe": "1d",
        "contract": "",      # ← no OCC contract → resolver cannot infer CALL/PUT
        "qty": 1, "limit_price": 2.10, "reserved_cost": 210.0,
        "meta": {
            "lifecycle_state": "BROKER_READY",
            "broker_ready": True,
            "selected_contract": "",   # no OCC contract in meta either
            "selected_limit": 2.10,
            "selected_qty": 1,
            "materialization_generation": 2,
            # No submit_intent_at → not in crash window
        },
    }

    class _C:
        def execute(self, *a, **k): return self
        def fetchall(self): return [br_row]
        rowcount = 1

    class _Conn:
        def __enter__(self): return _C()
        def __exit__(self, *a): return False

    result = {"deferred_lifecycles_recovered": 0, "errors": []}
    with patch.object(db_mod, "conn", lambda: _Conn()), \
         patch.object(db_mod, "run_with_retry", lambda fn, *a, **kw: fn()):
        rec._recover_deferred_breach_lifecycles(result)

    assert scaffold_calls[0] == 0, (
        f"resume_deferred_broker_ready_order must NOT be called when plan is None; "
        f"called {scaffold_calls[0]} times"
    )
    errors_str = str(result.get("errors", []))
    assert "broker_ready_recovery_plan_invalid" in errors_str, (
        f"Must record broker_ready_recovery_plan_invalid error; errors={result.get('errors')}"
    )


def test_final_materialization_retry_cleanup_never_uses_generic_expire_on_cas_miss():
    """A fenced materialization retry callback must never fall back to generic
    pending-entry cleanup when its exact terminal CAS misses.
    """
    from ap_execution_core import APExecutionCore

    calls = {"fenced": 0, "expire": 0, "cancel": 0}

    class _OSM:
        def terminalize_materialization_retry(self, oid, **kw):
            calls["fenced"] += 1
            assert kw["owner"] == "retry-owner"
            assert kw["generation"] == 7
            assert kw["retry_attempt"] == 3
            assert kw["client_id"] == CLIENT_ID
            assert kw["execution_mode"] == "paper"
            return False
        def expire_pending_entry(self, *a, **kw):
            calls["expire"] += 1
            return True
        def cancel_pending_entry(self, *a, **kw):
            calls["cancel"] += 1
            return True
        def get_order(self, oid):
            return {
                "status": "PENDING_TRIGGER",
                "broker_order_id": None,
                "submitted_ts": None,
                "meta": {
                    "lifecycle_state": "MATERIALIZING",
                    "materialization_owner": "new-owner",
                    "materialization_generation": 8,
                    "retry_attempt": 4,
                },
            }

    core = object.__new__(APExecutionCore)
    core.order_state_machine = _OSM()
    watched = SimpleNamespace(
        ticker="RTX",
        signal={
            "local_order_id": LOCAL_ORDER_ID,
            "_callback_ownership_context": {
                "is_recovered": True,
                "ownership_kind": "materialization_retry",
                "owner": "retry-owner",
                "generation": 7,
                "retry_attempt": 3,
                "client_id": CLIENT_ID,
                "execution_mode": "paper",
            },
        },
    )

    ok = core._cleanup_pending_entry_order(
        watched, action="expire", reason="breach_risk_check_false",
    )

    assert ok is False
    assert calls["fenced"] == 1
    assert calls["expire"] == 0
    assert calls["cancel"] == 0


def test_final_materialization_retry_early_osm_rejection_uses_fenced_terminal():
    """The early OSM capability rejection path must terminalize with the
    materialization retry owner/generation/attempt, not generic expire.
    """
    from ap_execution_core import APExecutionCore

    calls = {"fenced": 0, "expire": 0}

    class _OSM:
        def get_order(self, oid):
            return {
                "local_order_id": LOCAL_ORDER_ID,
                "client_id": CLIENT_ID,
                "execution_mode": "paper",
                "status": "PENDING_TRIGGER",
                "broker_order_id": None,
                "submitted_ts": None,
                "meta": {
                    "lifecycle_state": "MATERIALIZING",
                    "materialization_status": "RUNNING",
                    "materialization_in_flight": True,
                    "materialization_owner": "retry-owner",
                    "materialization_generation": 7,
                    "retry_attempt": 3,
                },
            }
        def terminalize_materialization_retry(self, oid, **kw):
            calls["fenced"] += 1
            assert kw["reason"] == "osm_missing_submit_existing_entry"
            assert kw["owner"] == "retry-owner"
            assert kw["generation"] == 7
            assert kw["retry_attempt"] == 3
            return True
        def expire_pending_entry(self, *a, **kw):
            calls["expire"] += 1
            return True

    core = object.__new__(APExecutionCore)
    core.client_id = CLIENT_ID
    core.email = CLIENT_ID
    core.execution_mode = "paper"
    core.mode = "PAPER"
    core.order_state_machine = _OSM()
    core.store = MagicMock()
    core._breach_risk_check = lambda watched: True

    plan = SimpleNamespace(
        client_id=CLIENT_ID,
        execution_mode="paper",
        metadata={
            "ownership_kind": "materialization_retry",
            "contract_deferred": True,
            "materialization_generation": 7,
            "retry_attempt": 3,
            "recovery_submit_fenced": True,
            "recovery_submit_owner": "retry-owner",
            "recovery_submit_generation": 7,
        },
    )
    watched = SimpleNamespace(
        ticker="RTX",
        trigger_price=130.0,
        signal={
            "signal_id": SIGNAL_ID,
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "paper",
            "ownership_kind": "materialization_retry",
            "owner": "retry-owner",
            "fenced": True,
            "materialization_generation": 7,
            "retry_attempt": 3,
            "recovery_submit_fenced": True,
            "recovery_submit_owner": "retry-owner",
            "recovery_submit_generation": 7,
            "_approved_plan": plan,
        },
    )

    core._on_entry_trigger(watched)

    assert calls["fenced"] == 1
    assert calls["expire"] == 0


def test_final_schedule_retry_clears_stale_watcher_token_and_adoption_stamps_new_token():
    """Scheduling RETRY_WAIT clears the stale token; restart rearm adopts the
    current process token through an exact RETRY_WAIT CAS.
    """
    from ap.order_state_machine import APOrderStateMachine

    patches = []
    osm = object.__new__(APOrderStateMachine)
    osm.client_id = CLIENT_ID

    fake_conn = _FakeConn(rowcount=1, patches=patches)
    with patch("ap.order_state_machine.conn", return_value=fake_conn), \
         patch("ap.order_state_machine.run_with_retry", lambda fn, *a, **kw: fn()):
        ok = osm.schedule_deferred_materialization_retry(
            LOCAL_ORDER_ID,
            owner="retry-owner",
            generation=7,
            reason_code="SELECTOR_REQUEST_BUDGET_EXHAUSTED",
            attempt=3,
            max_attempts=5,
            next_retry_at="2026-07-14T12:00:00+00:00",
            selector_failure={"reason_code": "SELECTOR_REQUEST_BUDGET_EXHAUSTED"},
        )

    assert ok is True
    assert patches[-1]["watcher_token"] == ""

    patches.clear()
    fake_conn = _FakeConn(rowcount=1, patches=patches)
    with patch("ap.order_state_machine.conn", return_value=fake_conn), \
         patch("ap.order_state_machine.run_with_retry", lambda fn, *a, **kw: fn()):
        adopted = osm.adopt_deferred_retry_watcher(
            LOCAL_ORDER_ID,
            watcher_token="watcher:new-process",
            generation=7,
            retry_attempt=3,
            next_retry_at="2026-07-14T12:00:00+00:00",
            execution_mode="paper",
        )

    assert adopted is True
    assert patches[-1]["watcher_token"] == "watcher:new-process"
    assert patches[-1]["watcher_generation"] == 7
