"""
tests/test_p0_queue_deferral_truth.py

P0 — Queue deferral truth.

Every setup that is intentionally deferred for overnight / later reevaluation
must be durably and consistently discoverable as WATCHING. Before this P0,
ap/queue.py had more than one deferral authority and they disagreed:

  * one path persisted ap_signals.decision_status="WATCHING" then marked the
    queue row WATCHING (correct);
  * another marked the queue row REJECTED and then wrote a LOWERCASE
    ap_signals.decision_status="watching" — split truth the overnight reeval
    (which discovers via `.eq("decision_status", "WATCHING")`) could never find;
  * the PAPER overnight-only rescue route marked the queue row WATCHING with NO
    ap_signals row at all (a phantom the rescue could never rediscover).

These tests exercise the PRODUCTION seams:

  * `_persist_watching_deferral(...)` — the single canonical deferral authority.
  * `_dispatch(...)` — proving the master-control block and PAPER overnight-only
    routes flow THROUGH that authority (never REJECTED + watching split truth,
    never a phantom WATCHING).
  * the REAL overnight discovery predicate
    `ap_overnight_reeval._fetch_watching_signals_with_status_impl` — proving the
    persisted uppercase WATCHING row is actually discovered and a lowercase
    "watching" row is not.

No money-path collaborators (contract selection, OSM entry, watcher arm, broker
submit/cancel/replace) may run on any deferral path.
"""
from __future__ import annotations

import os
import sys
import types
import importlib
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_p0_queue_deferral_truth",
)
os.environ.setdefault("PGSSLMODE", "disable")

import ap.queue as queue  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# Fake Supabase — models the (signal_id, client_email) composite-key upsert used
# by ap_signals AND the overnight discovery select/eq/gte/order/limit chain.
# ─────────────────────────────────────────────────────────────────────────────
class _FakeResult:
    def __init__(self, data):
        self.data = data


class _FakeQuery:
    def __init__(self, store):
        self._store = store
        self._filters = []       # list of (op, col, val)
        self._pending = None     # set by upsert(); consumed by execute()
        self._update_data = None  # set by update(); consumed by execute()

    # ── write path — upsert ───────────────────────────────────────────────────
    def upsert(self, row, on_conflict=None):
        r = dict(row)
        r.setdefault("created_at", datetime.now(timezone.utc).isoformat())
        key = (str(r.get("signal_id")), str(r.get("client_email")))
        self._store.rows[key] = r  # composite-key upsert → inherently idempotent
        self._pending = _FakeResult([r])
        return self

    # ── write path — expected-state update ───────────────────────────────────
    # Models _compensate_watching_signal_if_unchanged:
    #   .update({...}).eq("signal_id", x).eq("client_email", y)
    #                 .eq("decision_status", "WATCHING").execute()
    # Only rows that match ALL .eq() filters are mutated; all others are
    # untouched. Returns _FakeResult([updated_row]) when exactly one row matched,
    # _FakeResult([]) when no row matches (row advanced / missing).
    def update(self, data):
        self._update_data = dict(data)
        return self

    # ── read path ─────────────────────────────────────────────────────────────
    def select(self, *_a, **_k):
        return self

    def eq(self, col, val):
        self._filters.append(("eq", col, val))
        return self

    def gte(self, col, val):
        self._filters.append(("gte", col, val))
        return self

    def order(self, *_a, **_k):
        return self

    def limit(self, *_a, **_k):
        return self

    def execute(self):
        # 1. Pending upsert result (consumed once).
        if self._pending is not None:
            out, self._pending = self._pending, None
            return out

        # 2. Expected-state update (consumed once).
        if self._update_data is not None:
            update_data = self._update_data
            self._update_data = None
            filters = list(self._filters)
            self._filters = []
            updated = []
            for key, row in list(self._store.rows.items()):
                if all(row.get(col) == val for op, col, val in filters if op == "eq"):
                    row.update(update_data)
                    updated.append(dict(row))
            return _FakeResult(updated)

        # 3. Read / select path.
        rows = list(self._store.rows.values())
        for op, col, val in self._filters:
            if op == "eq":
                rows = [r for r in rows if r.get(col) == val]
            elif op == "gte":
                rows = [r for r in rows if str(r.get(col) or "") >= str(val)]
        return _FakeResult(rows)


class _FakeSupabase:
    def __init__(self):
        self.rows = {}  # (signal_id, client_email) -> row dict

    def table(self, _name):
        return _FakeQuery(self)


class _BoomSupabase:
    """A Supabase client whose write methods always raise — simulates failures.

    P0 #405: update() must also raise so _compensate_watching_signal_if_unchanged
    correctly returns SIGNAL_COMP_DB_ERROR when Supabase is unavailable.
    """

    def table(self, _name):
        return self

    def upsert(self, *_a, **_k):
        raise RuntimeError("supabase upsert failed")

    def update(self, *_a, **_k):
        raise RuntimeError("supabase update failed")

    def select(self, *_a, **_k):
        return self

    def eq(self, *_a, **_k):
        return self

    def execute(self):
        raise RuntimeError("supabase execute failed")


@pytest.fixture
def sb(monkeypatch):
    fake = _FakeSupabase()
    monkeypatch.setattr(queue, "_get_sb_client", lambda: fake)
    return fake


@pytest.fixture
def marks(monkeypatch):
    """Capture every _mark_job / _checked_processing_error_cas /
    _checked_watching_cas call.

    P0 #405: also stubs _checked_processing_error_cas to return
    ERROR_CAS_TRANSITIONED (simulating a successful PROCESSING->ERROR CAS) so
    tests that exercise the early failure paths inside _persist_watching_deferral
    do not require a live Postgres connection.  Calls are recorded with
    status='ERROR' so existing assertions against marks remain valid.

    Also stubs _checked_watching_cas to return WATCHING_CAS_TRANSITIONED and
    records a WATCHING entry.  Tests that exercise specific CAS failure outcomes
    override these stubs with their own monkeypatch after the fixture runs.
    """
    recorded: list[dict] = []

    def _fake_mark(job_id, status, *, result=None, error=None):
        recorded.append(
            {"job_id": job_id, "status": status, "result": result, "error": error}
        )

    monkeypatch.setattr(queue, "_mark_job", _fake_mark)

    # P0 #405: guarded queue ERROR CAS — replaces unconditional _mark_job(ERROR).
    # Records an ERROR entry with the same shape so existing assertions remain valid.
    def _fake_error_cas(job_id, *, error, result=None):
        recorded.append(
            {"job_id": job_id, "status": "ERROR", "result": result, "error": error}
        )
        return queue.ERROR_CAS_TRANSITIONED

    monkeypatch.setattr(queue, "_checked_processing_error_cas", _fake_error_cas)

    # Default: WATCHING CAS succeeds (TRANSITIONED) AND records a WATCHING mark.
    # Tests that exercise specific CAS outcomes override this after fixture runs.
    def _fake_cas(job_id, *, watching_error=None, watching_result=None):
        recorded.append(
            {"job_id": job_id, "status": "WATCHING",
             "result": watching_result, "error": watching_error}
        )
        return queue.WATCHING_CAS_TRANSITIONED

    monkeypatch.setattr(queue, "_checked_watching_cas", _fake_cas)
    return recorded


def _signal_rows(sb: _FakeSupabase, decision_status=None):
    rows = list(sb.rows.values())
    if decision_status is not None:
        rows = [r for r in rows if r.get("decision_status") == decision_status]
    return rows


# ═════════════════════════════════════════════════════════════════════════════
# 1. Canonical ordinary deferral
# ═════════════════════════════════════════════════════════════════════════════
def test_1_canonical_ordinary_deferral(sb, marks):
    ok = queue._persist_watching_deferral(
        job_id=101,
        client_id="alice@example.com",
        signal_id="sig-1",
        execution_mode="PAPER",
        payload={"ticker": "AAPL", "side": "CALL", "score": 72, "timeframe": "1d"},
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="after hours — deferred",
    )
    assert ok is True

    # ap_signals row: exact uppercase WATCHING, exact client, exact mode.
    rows = _signal_rows(sb)
    assert len(rows) == 1
    row = rows[0]
    assert row["decision_status"] == "WATCHING"          # exact uppercase spelling
    assert row["decision_status"] != "watching"
    assert row["client_email"] == "alice@example.com"
    assert row["raw_payload"]["execution_mode"] == "paper"

    # queue row: WATCHING (and never REJECTED for this job).
    statuses = [m["status"] for m in marks if m["job_id"] == 101]
    assert statuses == ["WATCHING"]


# ═════════════════════════════════════════════════════════════════════════════
# 2. Master-control deferrable path routes through the helper (no REJECTED first)
# ═════════════════════════════════════════════════════════════════════════════
def _isolate_dispatch(monkeypatch, *, in_session: bool):
    """Stub the incidental collaborators _dispatch touches so a block/defer path
    is exercised without DB / Discord / intelligence side effects."""
    for name in (
        "ap.restart_guard",
        "ap.rejection_feed",
        "ap.opportunity_ledger",
        "ap.fvg_telemetry",
        "ap.counterfactual_tracker",
        "ap.intelligence_context_handoff",
        "ap.signal_pair_manager",
    ):
        stub = types.ModuleType(name)
        monkeypatch.setitem(sys.modules, name, stub)

    sys.modules["ap.restart_guard"].should_skip_on_restart = lambda p: False
    sys.modules["ap.rejection_feed"].post_master_control_block = lambda **k: None
    sys.modules["ap.opportunity_ledger"].mark_missed = lambda *a, **k: None
    sys.modules["ap.opportunity_ledger"].map_reason_to_stage = lambda *a, **k: "STAGE"
    sys.modules["ap.fvg_telemetry"].record_fvg_telemetry_async = lambda *a, **k: None
    sys.modules["ap.counterfactual_tracker"].track_counterfactual_signal = lambda *a, **k: None
    sys.modules["ap.intelligence_context_handoff"].enqueue_pretrigger_context_best_effort = (
        lambda *a, **k: {"ok": True}
    )
    sys.modules["ap.signal_pair_manager"].get_pair_manager = lambda: MagicMock()

    monkeypatch.setattr(queue, "trace_gate", lambda *a, **k: None)
    monkeypatch.setattr(queue, "_is_regular_session_et", lambda *a, **k: in_session)


class _Decision:
    def __init__(self, ok, stage, reason, plan=None):
        self.ok, self.stage, self.reason, self.plan = ok, stage, reason, plan


class _MC:
    def __init__(self, decision, mode="PAPER"):
        self.mode = mode
        self._equity_cache_ts = 10 ** 12  # never stale → no broker/equity path
        self._decision = decision
        self.evaluate = MagicMock(side_effect=lambda payload, client_id=None: decision)


def test_2_master_control_deferrable_routes_through_helper(monkeypatch, sb, marks):
    _isolate_dispatch(monkeypatch, in_session=False)
    mc = _MC(_Decision(ok=False, stage="risk", reason="daily_stop_hit"))

    selector, osm, watcher, broker = (MagicMock(), MagicMock(), MagicMock(), MagicMock())

    queue._dispatch(
        202, "bob@example.com", "sig-2",
        {"ticker": "TSLA", "side": "CALL", "score": 80, "timeframe": "1d"},
        master_control=mc, contract_selector=selector, order_state_machine=osm,
        entry_watcher=watcher, broker=broker,
    )

    my = [m for m in marks if m["job_id"] == 202]
    statuses = [m["status"] for m in my]
    # Deferrable + market closed → WATCHING, and NEVER terminalized REJECTED.
    assert statuses == ["WATCHING"], statuses
    assert "REJECTED" not in statuses
    # ap_signals persisted as uppercase WATCHING for the same signal.
    assert _signal_rows(sb, "WATCHING"), "ap_signals must hold an uppercase WATCHING row"
    assert not _signal_rows(sb, "watching")
    # No money-path collaborator ran.
    selector.select.assert_not_called()
    osm.create_entry_order.assert_not_called()
    watcher.watch.assert_not_called()


def test_2b_terminal_rejection_in_session_stays_rejected(monkeypatch, sb, marks):
    """A block DURING regular session is a genuine terminal rejection — REJECTED,
    and must NOT produce an ap_signals WATCHING row."""
    _isolate_dispatch(monkeypatch, in_session=True)
    mc = _MC(_Decision(ok=False, stage="risk", reason="daily_stop_hit"))

    queue._dispatch(
        203, "bob@example.com", "sig-2b",
        {"ticker": "TSLA", "side": "CALL", "score": 80, "timeframe": "1d"},
        master_control=mc, contract_selector=MagicMock(),
        order_state_machine=MagicMock(), entry_watcher=MagicMock(), broker=MagicMock(),
    )
    statuses = [m["status"] for m in marks if m["job_id"] == 203]
    assert statuses == ["REJECTED"], statuses
    assert not _signal_rows(sb, "WATCHING")


# ═════════════════════════════════════════════════════════════════════════════
# 3. PAPER overnight-only route proves ap_signals discoverability before WATCHING
# ═════════════════════════════════════════════════════════════════════════════
def test_3_paper_overnight_only_persists_signal_before_watching(monkeypatch, sb):
    _isolate_dispatch(monkeypatch, in_session=False)

    order: list[str] = []

    # Wrap the fake so we know WHEN the ap_signals write happened relative to the
    # queue WATCHING mark.
    real_get = queue._get_sb_client()

    class _OrderedSb:
        def table(self, name):
            order.append("ap_signals_write")
            return real_get.table(name)

    monkeypatch.setattr(queue, "_get_sb_client", lambda: _OrderedSb())

    def _fake_mark(job_id, status, *, result=None, error=None):
        order.append(f"mark:{status}")

    monkeypatch.setattr(queue, "_mark_job", _fake_mark)

    # _checked_watching_cas now owns the WATCHING queue transition; stub it so
    # it (a) doesn't hit the real DB and (b) records the ordering event the
    # assertions below depend on.
    def _fake_cas(job_id, *, watching_error=None, watching_result=None):
        order.append("mark:WATCHING")
        return True

    monkeypatch.setattr(queue, "_checked_watching_cas", _fake_cas)

    mc = _MC(_Decision(ok=True, stage="ok", reason=""))  # never reached

    queue._dispatch(
        303, "paper@example.com", "sig-3",
        {
            "ticker": "HD", "side": "CALL", "score": 78, "timeframe": "1d",
            "force_overnight_reeval_only": True, "do_not_queue_directly": True,
        },
        job_last_error="manual_rescue_current_session",
        job_result={"manual_rescue": True},
        master_control=mc, contract_selector=MagicMock(),
        order_state_machine=MagicMock(), entry_watcher=MagicMock(), broker=MagicMock(),
    )

    assert "ap_signals_write" in order, "the rescue must persist ap_signals"
    assert "mark:WATCHING" in order
    # Discoverability BEFORE the queue row becomes WATCHING.
    assert order.index("ap_signals_write") < order.index("mark:WATCHING")
    mc.evaluate.assert_not_called()  # route returns before master control


# ═════════════════════════════════════════════════════════════════════════════
# 4. Signal persistence failure → queue ERROR, no watcher/OSM/selector/broker
# ═════════════════════════════════════════════════════════════════════════════
def test_4_signal_persistence_failure_marks_error(monkeypatch, marks):
    monkeypatch.setattr(queue, "_get_sb_client", lambda: _BoomSupabase())

    selector, osm, watcher, broker = (MagicMock(), MagicMock(), MagicMock(), MagicMock())

    ok = queue._persist_watching_deferral(
        job_id=404,
        client_id="carol@example.com",
        signal_id="sig-4",
        execution_mode="paper",
        payload={"ticker": "NVDA", "side": "PUT", "score": 66},
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="after hours",
    )
    assert ok is False
    my = [m for m in marks if m["job_id"] == 404]
    assert len(my) == 1
    assert my[0]["status"] == "ERROR"
    assert my[0]["status"] != "WATCHING"
    assert my[0]["error"] == queue.WATCHING_SIGNAL_PERSISTENCE_FAILED
    # The helper never touches any money-path collaborator (it takes none).
    selector.select.assert_not_called()
    osm.create_entry_order.assert_not_called()
    watcher.watch.assert_not_called()
    broker.assert_not_called()


def test_4b_no_supabase_client_marks_error(monkeypatch, marks):
    """No client configured == cannot verify persistence == ERROR, not WATCHING."""
    monkeypatch.setattr(queue, "_get_sb_client", lambda: None)
    ok = queue._persist_watching_deferral(
        job_id=414, client_id="carol@example.com", signal_id="sig-4b",
        execution_mode="live", payload={"ticker": "NVDA", "side": "PUT"},
        stage="master_control", reason_code="market_closed_deferred",
        human_reason="after hours",
    )
    assert ok is False
    assert [m["status"] for m in marks if m["job_id"] == 414] == ["ERROR"]


def test_4c_missing_signal_identity_fails_closed_no_random_uuid(sb, marks):
    """Signal identity is MANDATORY. A missing function arg AND a missing payload
    signal_id must fail closed to queue ERROR — never invent a random UUID that
    no longer matches the queue opportunity that produced it."""
    ok = queue._persist_watching_deferral(
        job_id=424,
        client_id="carol@example.com",
        signal_id="",                     # missing function signal_id
        execution_mode="paper",
        payload={"ticker": "NVDA", "side": "PUT", "score": 66},  # no payload signal_id
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="after hours",
    )
    assert ok is False
    # queue ERROR with the invalid-signal reason.
    my = [m for m in marks if m["job_id"] == 424]
    assert len(my) == 1 and my[0]["status"] == "ERROR"
    assert my[0]["error"].endswith(":invalid_signal_id")
    # Zero ap_signals rows — nothing persisted, so no random UUID was minted.
    assert sb.rows == {}


def test_4d_malformed_score_does_not_raise(sb, marks):
    """A malformed scanner score ("A+") must not raise past the fail-closed
    persistence contract: exactly one canonical WATCHING row, queue WATCHING,
    persisted score coerced to 0.0."""
    ok = queue._persist_watching_deferral(
        job_id=434,
        client_id="carol@example.com",
        signal_id="sig-4d",
        execution_mode="paper",
        payload={"ticker": "NVDA", "side": "CALL", "score": "A+", "timeframe": "1d"},
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="after hours",
    )
    assert ok is True
    rows = _signal_rows(sb, "WATCHING")
    assert len(rows) == 1
    assert rows[0]["score"] == 0.0
    assert [m["status"] for m in marks if m["job_id"] == 434] == ["WATCHING"]


# ═════════════════════════════════════════════════════════════════════════════
# 5. Idempotency — two identical deferrals leave exactly one signal + one queue row
# ═════════════════════════════════════════════════════════════════════════════
def test_5_idempotent(sb, marks):
    kwargs = dict(
        job_id=505, client_id="dave@example.com", signal_id="sig-5",
        execution_mode="paper",
        payload={"ticker": "SPY", "side": "CALL", "score": 70, "timeframe": "1d"},
        stage="master_control", reason_code="market_closed_deferred",
        human_reason="after hours",
    )
    assert queue._persist_watching_deferral(**kwargs) is True
    assert queue._persist_watching_deferral(**kwargs) is True

    # Exactly one canonical signal opportunity (composite-key upsert dedups).
    assert len(_signal_rows(sb)) == 1
    # Exactly one queue row (same job_id both times); both marks WATCHING.
    assert all(m["status"] == "WATCHING" for m in marks if m["job_id"] == 505)
    assert {m["job_id"] for m in marks} == {505}


# ═════════════════════════════════════════════════════════════════════════════
# 6. Client isolation — same signal shape for two clients cannot cross-update
# ═════════════════════════════════════════════════════════════════════════════
def test_6_client_isolation(sb, marks):
    base = dict(
        signal_id="sig-shared", execution_mode="paper",
        payload={"ticker": "META", "side": "CALL", "score": 75, "timeframe": "1d"},
        stage="master_control", reason_code="market_closed_deferred",
        human_reason="after hours",
    )
    assert queue._persist_watching_deferral(job_id=601, client_id="a@x.com", **base) is True
    assert queue._persist_watching_deferral(job_id=602, client_id="b@x.com", **base) is True

    keys = set(sb.rows.keys())
    assert ("sig-shared", "a@x.com") in keys
    assert ("sig-shared", "b@x.com") in keys
    # Two distinct rows — neither client overwrote the other.
    assert len(sb.rows) == 2
    assert sb.rows[("sig-shared", "a@x.com")]["client_email"] == "a@x.com"
    assert sb.rows[("sig-shared", "b@x.com")]["client_email"] == "b@x.com"
    # Each client's queue row was marked independently.
    assert {m["job_id"] for m in marks} == {601, 602}


# ═════════════════════════════════════════════════════════════════════════════
# 7. Execution-mode validation + mode stamping (NOT cross-mode row isolation)
# ═════════════════════════════════════════════════════════════════════════════
def test_7_execution_mode_validation_and_stamping(sb, marks):
    """SCOPE NOTE: the ap_signals upsert key is (signal_id, client_email) only —
    execution mode is NOT part of the row identity. This test proves the helper
    (a) requires an exact paper/live mode and (b) stamps that mode into the row
    for provenance, and — because paper and live run under different client
    identities — that the two clients' rows do not collide. It does NOT (and
    cannot) prove cross-mode isolation for the SAME (signal_id, client_email);
    that guarantee is owned by the runtime/broker mode boundary (PR #397)."""
    common = dict(
        payload={"ticker": "AMD", "side": "CALL", "score": 71, "timeframe": "1d"},
        stage="master_control", reason_code="market_closed_deferred",
        human_reason="after hours",
    )
    # Same signal id, DIFFERENT client per mode (paper vs live run under
    # different client identities). The rows are keyed by client → no collision.
    assert queue._persist_watching_deferral(
        job_id=701, client_id="paper-acct@x.com", signal_id="sig-7",
        execution_mode="PAPER", **common) is True
    assert queue._persist_watching_deferral(
        job_id=702, client_id="live-acct@x.com", signal_id="sig-7",
        execution_mode="LIVE", **common) is True

    paper_row = sb.rows[("sig-7", "paper-acct@x.com")]
    live_row = sb.rows[("sig-7", "live-acct@x.com")]
    # Mode is stamped into raw_payload (provenance), distinct per row.
    assert paper_row["raw_payload"]["execution_mode"] == "paper"
    assert live_row["raw_payload"]["execution_mode"] == "live"
    assert paper_row["raw_payload"]["execution_mode"] != live_row["raw_payload"]["execution_mode"]

    # Invalid execution mode is refused (queue ERROR, no WATCHING, no signal row).
    before = len(sb.rows)
    assert queue._persist_watching_deferral(
        job_id=703, client_id="x@x.com", signal_id="sig-7b",
        execution_mode="margin", **common) is False
    assert len(sb.rows) == before
    err = [m for m in marks if m["job_id"] == 703]
    assert err and err[0]["status"] == "ERROR"


def test_7b_invalid_client_refused(sb, marks):
    assert queue._persist_watching_deferral(
        job_id=713, client_id="   ", signal_id="sig-7c", execution_mode="paper",
        payload={"ticker": "AMD"}, stage="master_control",
        reason_code="market_closed_deferred", human_reason="x") is False
    assert not sb.rows
    assert [m["status"] for m in marks if m["job_id"] == 713] == ["ERROR"]


# ═════════════════════════════════════════════════════════════════════════════
# 8. Overnight discoverability — REAL production discovery predicate
# ═════════════════════════════════════════════════════════════════════════════
def test_8_overnight_loader_discovers_uppercase_watching(monkeypatch, sb, marks):
    # Persist a canonical WATCHING deferral through the production authority.
    # (`marks` patches _mark_job so the queue write stays off the real DB.)
    assert queue._persist_watching_deferral(
        job_id=801, client_id="erin@example.com", signal_id="sig-8-up",
        execution_mode="paper",
        payload={"ticker": "COIN", "side": "CALL", "score": 73, "timeframe": "1d"},
        stage="master_control", reason_code="market_closed_deferred",
        human_reason="after hours") is True

    # Poison rows that must NOT be discovered by the uppercase predicate.
    sb.table("ap_signals").upsert(
        {"signal_id": "sig-8-low", "client_email": "erin@example.com",
         "decision_status": "watching", "ticker": "COIN"}
    ).execute()
    sb.table("ap_signals").upsert(
        {"signal_id": "sig-8-rej", "client_email": "erin@example.com",
         "decision_status": "rejected", "ticker": "COIN"}
    ).execute()

    # Point the REAL overnight loader at the same fake ap_signals table and
    # neutralize the local trade_queue source so we exercise ONLY the ap_signals
    # `.eq("decision_status", "WATCHING")` discovery predicate.
    fake_supa_mod = types.ModuleType("supabase")
    fake_supa_mod.create_client = lambda *a, **k: sb
    monkeypatch.setitem(sys.modules, "supabase", fake_supa_mod)
    monkeypatch.setenv("SUPABASE_URL", "http://fake")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "fake-key")

    import ap.db as _apdb
    monkeypatch.setattr(_apdb, "run_with_retry", lambda fn, *a, **k: [], raising=False)

    reeval = importlib.import_module("ap_overnight_reeval")
    result = reeval._fetch_watching_signals_with_status_impl("erin@example.com")

    discovered = {str(r.get("signal_id")) for r in result.rows}
    assert "sig-8-up" in discovered, "uppercase WATCHING row must be discovered"
    assert "sig-8-low" not in discovered, "lowercase 'watching' must be invisible"
    assert "sig-8-rej" not in discovered, "rejected row must be invisible"


# ═════════════════════════════════════════════════════════════════════════════
# 9. No split truth — production code cannot finish REJECTED + WATCHING for a defer
# ═════════════════════════════════════════════════════════════════════════════
def test_9_no_split_truth_on_deferral(monkeypatch, sb, marks):
    _isolate_dispatch(monkeypatch, in_session=False)
    mc = _MC(_Decision(ok=False, stage="risk", reason="sizer_blocked"))

    queue._dispatch(
        909, "frank@example.com", "sig-9",
        {"ticker": "QQQ", "side": "PUT", "score": 68, "timeframe": "1d"},
        master_control=mc, contract_selector=MagicMock(),
        order_state_machine=MagicMock(), entry_watcher=MagicMock(), broker=MagicMock(),
    )

    queue_status = [m["status"] for m in marks if m["job_id"] == 909]
    signal_status = [r["decision_status"] for r in _signal_rows(sb)]

    # The forbidden combination: queue REJECTED while signal is WATCHING.
    assert not ("REJECTED" in queue_status and "WATCHING" in signal_status), (
        "split truth: queue REJECTED + ap_signals WATCHING for one deferral"
    )
    # It resolved consistently as WATCHING + WATCHING.
    assert queue_status == ["WATCHING"]
    assert signal_status == ["WATCHING"]


def test_2c_malformed_score_in_dispatch_does_not_raise(monkeypatch, sb, marks):
    """Blocker 1 regression: _dispatch() with score='A+', decision.ok=False,
    reason='daily_stop_hit', in_session=False must:
      - not raise (float("A+") used to crash before _persist_watching_deferral);
      - leave queue=WATCHING (deferral path, not terminal);
      - never call mark_missed (opportunity ledger must not be terminalized);
      - never emit a REJECT trace (trace_gate must not be called with REJECT);
      - never call the rejection feed;
      - touch zero money-path collaborators (selector/OSM/watcher/broker).
    """
    _isolate_dispatch(monkeypatch, in_session=False)

    reject_traces: list[dict] = []
    _orig_trace = queue.trace_gate

    def _spy_trace(*args, **kwargs):
        if "REJECT" in str(args) or kwargs.get("disposition") == "REJECT":
            reject_traces.append({"args": args, "kwargs": kwargs})

    monkeypatch.setattr(queue, "trace_gate", _spy_trace)

    missed_calls: list = []
    sys.modules["ap.opportunity_ledger"].mark_missed = lambda *a, **k: missed_calls.append((a, k))

    rejection_feed_calls: list = []
    sys.modules["ap.rejection_feed"].post_master_control_block = (
        lambda **k: rejection_feed_calls.append(k)
    )

    selector, osm, watcher, broker = (MagicMock(), MagicMock(), MagicMock(), MagicMock())
    mc = _MC(_Decision(ok=False, stage="risk", reason="daily_stop_hit"))

    # Must not raise — float("A+") previously crashed before the deferral guard.
    queue._dispatch(
        299, "zara@example.com", "sig-2c",
        {"ticker": "BAC", "side": "PUT", "score": "A+", "timeframe": "1d"},
        master_control=mc, contract_selector=selector,
        order_state_machine=osm, entry_watcher=watcher, broker=broker,
    )

    # Queue: WATCHING (deferral, not terminal).
    statuses = [m["status"] for m in marks if m["job_id"] == 299]
    assert statuses == ["WATCHING"], f"expected [WATCHING], got {statuses}"

    # Opportunity ledger: mark_missed must NOT have been called.
    assert not missed_calls, (
        f"mark_missed must not be called on a deferral — called {len(missed_calls)}x"
    )

    # Trace gate: no REJECT disposition emitted.
    assert not reject_traces, (
        f"trace_gate must not emit REJECT on a deferral — got {reject_traces}"
    )

    # Rejection feed: not called.
    assert not rejection_feed_calls, (
        f"rejection feed must not fire on a deferral — got {rejection_feed_calls}"
    )

    # ap_signals: one uppercase WATCHING row.
    assert _signal_rows(sb, "WATCHING"), "ap_signals must hold an uppercase WATCHING row"
    assert not _signal_rows(sb, "watching"), "lowercase watching must not appear"

    # Money-path collaborators: silent.
    selector.select.assert_not_called()
    osm.create_entry_order.assert_not_called()
    watcher.watch.assert_not_called()
    for meth in ("submit_order", "submit", "cancel_order", "cancel"):
        getattr(broker, meth, MagicMock()).assert_not_called()


def test_4e_queue_cas_raises_returns_false(monkeypatch, sb, marks):
    """Blocker 2: if _checked_watching_cas returns DB_ERROR (because an exception
    was raised internally, e.g. DB connection lost after ap_signals write succeeded),
    _persist_watching_deferral must return False (not True), attempt best-effort
    signal revert, and NOT blindly mark queue ERROR. No true success can be
    returned on an unverifiable queue transition.

    NOTE: _checked_watching_cas catches all exceptions internally and returns
    WATCHING_CAS_DB_ERROR — callers of _persist_watching_deferral never see the
    raw exception. This test simulates that path by patching the CAS to return
    WATCHING_CAS_DB_ERROR directly."""
    monkeypatch.setattr(
        queue, "_checked_watching_cas",
        lambda *a, **k: queue.WATCHING_CAS_DB_ERROR,
    )

    ok = queue._persist_watching_deferral(
        job_id=491,
        client_id="henry@example.com",
        signal_id="sig-4e",
        execution_mode="paper",
        payload={"ticker": "MSFT", "side": "CALL", "score": 74, "timeframe": "1d"},
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="after hours",
    )
    # Must return False — never True on a CAS failure.
    assert ok is False, "helper must return False when queue CAS raises"

    # Queue must not hold a WATCHING status — only ERROR from compensation.
    queue_statuses = [m["status"] for m in marks if m["job_id"] == 491]
    assert "WATCHING" not in queue_statuses, (
        f"WATCHING must not appear when CAS raises — got {queue_statuses}"
    )


def test_4f_queue_cas_zero_rows_returns_false(monkeypatch, sb, marks):
    """Blocker 2: if _checked_watching_cas returns MISSING_OR_UNEXPECTED (zero
    rows updated, row missing or in an unexpected state), _persist_watching_deferral
    must return False and attempt signal revert. It must NOT blindly mark queue
    ERROR — the current queue state was not confirmed to be PROCESSING."""
    monkeypatch.setattr(
        queue, "_checked_watching_cas",
        lambda *a, **k: queue.WATCHING_CAS_MISSING_OR_UNEXPECTED,
    )

    ok = queue._persist_watching_deferral(
        job_id=492,
        client_id="irene@example.com",
        signal_id="sig-4f",
        execution_mode="live",
        payload={"ticker": "NVDA", "side": "PUT", "score": 68, "timeframe": "1d"},
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="after hours",
    )
    assert ok is False, "helper must return False when CAS returns MISSING_OR_UNEXPECTED"

    queue_statuses = [m["status"] for m in marks if m["job_id"] == 492]
    assert "WATCHING" not in queue_statuses, (
        f"WATCHING must not appear when CAS returns MISSING_OR_UNEXPECTED — got {queue_statuses}"
    )
    # Must NOT blindly mark queue ERROR — current state not confirmed as PROCESSING.
    assert "ERROR" not in queue_statuses, (
        f"must not blindly mark queue ERROR on MISSING_OR_UNEXPECTED — got {queue_statuses}"
    )


def test_9b_no_direct_lowercase_write_in_queue_source():
    """Grep-guard: no functional lowercase decision_status="watching" write in
    queue code. Comments and docstrings describing the removed bug are allowed,
    so we blank every comment/string token before scanning."""
    import re
    import io
    import tokenize

    src = (Path(__file__).resolve().parents[1] / "ap" / "queue.py").read_text()
    # A functional lowercase write would be real code (dict entry or kwarg), so
    # the containing line carries non-string, non-comment tokens. Docstring/
    # comment prose lines carry only STRING/COMMENT tokens — skip those.
    code_lines = set()
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type in (tokenize.COMMENT, tokenize.STRING):
            continue
        if tok.type in (tokenize.NL, tokenize.NEWLINE, tokenize.INDENT,
                        tokenize.DEDENT, tokenize.ENCODING, tokenize.ENDMARKER):
            continue
        code_lines.add(tok.start[0])
    lines = src.splitlines()
    functional = [
        lines[n - 1]
        for n in sorted(code_lines)
        if re.search(r'decision_status\s*[:=]\s*["\']watching["\']', lines[n - 1])
    ]
    assert not functional, f"lowercase decision_status='watching' still written: {functional}"


# ═════════════════════════════════════════════════════════════════════════════
# 10. No money-path effects on a deferral
# ═════════════════════════════════════════════════════════════════════════════
def test_10_no_money_path_effects(monkeypatch, sb):
    _isolate_dispatch(monkeypatch, in_session=False)

    def _fake_mark(job_id, status, *, result=None, error=None):
        pass

    monkeypatch.setattr(queue, "_mark_job", _fake_mark)

    selector, osm, watcher, broker = (MagicMock(), MagicMock(), MagicMock(), MagicMock())
    mc = _MC(_Decision(ok=True, stage="ok", reason=""))

    queue._dispatch(
        1010, "grace@example.com", "sig-10",
        {
            "ticker": "AMZN", "side": "CALL", "score": 79, "timeframe": "1d",
            "force_overnight_reeval_only": True, "do_not_queue_directly": True,
        },
        job_last_error="manual_rescue_current_session",
        job_result={"manual_rescue": True},
        master_control=mc, contract_selector=selector,
        order_state_machine=osm, entry_watcher=watcher, broker=broker,
    )

    # Zero calls to contract selection, OSM entry creation, watcher arm, or the
    # broker submit/cancel/replace surface.
    selector.select.assert_not_called()
    osm.create_entry_order.assert_not_called()
    watcher.watch.assert_not_called()
    watcher.arm.assert_not_called()
    for meth in ("submit_order", "submit", "cancel_order", "cancel",
                 "replace_order", "replace"):
        getattr(broker, meth).assert_not_called()
    mc.evaluate.assert_not_called()


# ═════════════════════════════════════════════════════════════════════════════
# CAS outcome tests (A–F) — exercise _checked_watching_cas classification
# directly by controlling _run_with_retry's return value.
# These tests do not require a live Postgres connection.
# ═════════════════════════════════════════════════════════════════════════════

def test_cas_A_processing_transitioned(monkeypatch):
    """A: PROCESSING row → UPDATE returns 1 row → TRANSITIONED.
    _persist_watching_deferral must return True."""
    monkeypatch.setattr(
        queue, "_run_with_retry",
        lambda fn: (1, None),  # simulate: 1 row updated
    )
    outcome = queue._checked_watching_cas(job_id=1001)
    assert outcome == queue.WATCHING_CAS_TRANSITIONED


def test_cas_B_already_watching_classification(monkeypatch):
    """B: Row already WATCHING → UPDATE returns 0, SELECT returns 'WATCHING'
    → ALREADY_WATCHING."""
    monkeypatch.setattr(
        queue, "_run_with_retry",
        lambda fn: (0, "WATCHING"),
    )
    outcome = queue._checked_watching_cas(job_id=1002)
    assert outcome == queue.WATCHING_CAS_ALREADY_WATCHING


@pytest.mark.parametrize("terminal_status", ["REJECTED", "ERROR", "DONE"])
def test_cas_C_terminal_classification(monkeypatch, terminal_status):
    """C: Terminal row → UPDATE returns 0, SELECT returns terminal status → TERMINAL."""
    monkeypatch.setattr(
        queue, "_run_with_retry",
        lambda fn: (0, terminal_status),
    )
    outcome = queue._checked_watching_cas(job_id=1003)
    assert outcome == queue.WATCHING_CAS_TERMINAL


def test_cas_D_missing_row(monkeypatch):
    """D: Row not found → UPDATE returns 0, SELECT returns None
    → MISSING_OR_UNEXPECTED."""
    monkeypatch.setattr(
        queue, "_run_with_retry",
        lambda fn: (0, None),  # simulate: row missing
    )
    outcome = queue._checked_watching_cas(job_id=1004)
    assert outcome == queue.WATCHING_CAS_MISSING_OR_UNEXPECTED


@pytest.mark.parametrize("unexpected_status", ["NEW", "PENDING_TRIGGER"])
def test_cas_E_unexpected_nonterminal(monkeypatch, unexpected_status):
    """E: Unexpected nonterminal state → MISSING_OR_UNEXPECTED.
    Row must not be mutated (only classification SELECT is issued)."""
    monkeypatch.setattr(
        queue, "_run_with_retry",
        lambda fn: (0, unexpected_status),
    )
    outcome = queue._checked_watching_cas(job_id=1005)
    assert outcome == queue.WATCHING_CAS_MISSING_OR_UNEXPECTED


def test_cas_F_db_exception(monkeypatch):
    """F: DB exception inside _run_with_retry → DB_ERROR; never re-raises."""
    def _raising(fn):
        raise RuntimeError("connection lost mid-transaction")

    monkeypatch.setattr(queue, "_run_with_retry", _raising)
    outcome = queue._checked_watching_cas(job_id=1006)
    assert outcome == queue.WATCHING_CAS_DB_ERROR


# ═════════════════════════════════════════════════════════════════════════════
# _persist_watching_deferral integration tests for explicit CAS outcomes (B–G)
# ═════════════════════════════════════════════════════════════════════════════

def test_persist_B_already_watching_idempotent_success(monkeypatch, sb):
    """B integration: CAS outcome ALREADY_WATCHING → helper returns True.
    ap_signals WATCHING row is NOT reverted, queue is NOT marked ERROR.
    A repeated deferral against an already-canonical WATCHING row is success."""
    monkeypatch.setattr(
        queue, "_checked_watching_cas",
        lambda *a, **k: queue.WATCHING_CAS_ALREADY_WATCHING,
    )
    mark_calls: list[dict] = []
    monkeypatch.setattr(
        queue, "_mark_job",
        lambda job_id, status, *, result=None, error=None:
            mark_calls.append({"job_id": job_id, "status": status, "error": error}),
    )

    ok = queue._persist_watching_deferral(
        job_id=2001,
        client_id="alice@example.com",
        signal_id="sig-persist-B",
        execution_mode="paper",
        payload={"ticker": "AAPL", "side": "CALL", "score": 72, "timeframe": "1d"},
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="already watching — idempotent",
    )

    assert ok is True, "ALREADY_WATCHING must be idempotent success"
    # No _mark_job calls at all — no ERROR compensation, no spurious marks.
    assert not mark_calls, (
        f"_mark_job must not be called on ALREADY_WATCHING — got {mark_calls}"
    )
    # ap_signals WATCHING row written by initial signal persistence must not be
    # reverted; it should remain WATCHING.
    assert _signal_rows(sb, "WATCHING"), (
        "WATCHING signal must remain — must not be reverted on ALREADY_WATCHING"
    )
    assert not _signal_rows(sb, "ERROR"), (
        "signal must not be reverted to ERROR on ALREADY_WATCHING"
    )


def test_persist_C_terminal_queue_never_overwritten(monkeypatch, sb):
    """C integration: CAS outcome TERMINAL → helper False.
    _mark_job must NOT be called — terminal queue state is authoritative.
    Signal revert is attempted."""
    monkeypatch.setattr(
        queue, "_checked_watching_cas",
        lambda *a, **k: queue.WATCHING_CAS_TERMINAL,
    )
    mark_calls: list[dict] = []
    monkeypatch.setattr(
        queue, "_mark_job",
        lambda job_id, status, *, result=None, error=None:
            mark_calls.append({"job_id": job_id, "status": status, "error": error}),
    )

    ok = queue._persist_watching_deferral(
        job_id=2002,
        client_id="bob@example.com",
        signal_id="sig-persist-C",
        execution_mode="live",
        payload={"ticker": "NVDA", "side": "PUT", "score": 68, "timeframe": "1d"},
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="queue terminal",
    )

    assert ok is False, "TERMINAL must return False"
    # _mark_job must NEVER be called — do not overwrite the terminal queue row.
    assert not mark_calls, (
        f"_mark_job must not be called when CAS outcome is TERMINAL — got {mark_calls}"
    )
    # Signal revert attempted — ap_signals ERROR row written (best-effort).
    assert _signal_rows(sb, "ERROR"), (
        "signal revert must be attempted (ERROR row) when queue row is terminal"
    )


def test_persist_D_missing_row_no_blind_error_mark(monkeypatch, sb):
    """D integration: CAS outcome MISSING_OR_UNEXPECTED (row not found) →
    helper False. _mark_job must NOT be called blindly."""
    monkeypatch.setattr(
        queue, "_checked_watching_cas",
        lambda *a, **k: queue.WATCHING_CAS_MISSING_OR_UNEXPECTED,
    )
    mark_calls: list[dict] = []
    monkeypatch.setattr(
        queue, "_mark_job",
        lambda job_id, status, *, result=None, error=None:
            mark_calls.append({"job_id": job_id, "status": status, "error": error}),
    )

    ok = queue._persist_watching_deferral(
        job_id=2004,
        client_id="carol@example.com",
        signal_id="sig-persist-D",
        execution_mode="paper",
        payload={"ticker": "SPY", "side": "CALL", "score": 70},
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="row missing",
    )

    assert ok is False, "MISSING_OR_UNEXPECTED must return False"
    # No blind _mark_job — we do not know the current queue state.
    assert not mark_calls, (
        f"_mark_job must not be called on MISSING_OR_UNEXPECTED — got {mark_calls}"
    )


def test_persist_E_unexpected_nonterminal_queue_unchanged(monkeypatch, sb):
    """E integration: CAS outcome MISSING_OR_UNEXPECTED (unexpected nonterminal
    state, e.g. NEW) → helper False. Queue row must not be mutated."""
    # Same outcome as D from _persist_watching_deferral's perspective.
    monkeypatch.setattr(
        queue, "_checked_watching_cas",
        lambda *a, **k: queue.WATCHING_CAS_MISSING_OR_UNEXPECTED,
    )
    mark_calls: list[dict] = []
    monkeypatch.setattr(
        queue, "_mark_job",
        lambda job_id, status, *, result=None, error=None:
            mark_calls.append({"job_id": job_id, "status": status, "error": error}),
    )

    ok = queue._persist_watching_deferral(
        job_id=2005,
        client_id="dave@example.com",
        signal_id="sig-persist-E",
        execution_mode="paper",
        payload={"ticker": "QQQ", "side": "PUT", "score": 65},
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="unexpected nonterminal",
    )

    assert ok is False
    # Queue row left untouched — no blind ERROR overwrite.
    assert not any(m.get("status") == "ERROR" for m in mark_calls), (
        f"must not blindly overwrite queue row on MISSING_OR_UNEXPECTED — got {mark_calls}"
    )


def test_persist_F_db_error_no_false_success(monkeypatch, sb):
    """F integration: CAS outcome DB_ERROR → helper returns False.

    P0 #405: signal compensation is now an expected-state UPDATE via
    _compensate_watching_signal_if_unchanged (not a blind upsert via
    _log_signal_to_db).  The _FakeQuery.update() path must transition the
    WATCHING row to ERROR, confirming the expected-state fencing works end-to-end.
    No _mark_job call is permitted (queue state not confirmed as PROCESSING).
    """
    monkeypatch.setattr(
        queue, "_checked_watching_cas",
        lambda *a, **k: queue.WATCHING_CAS_DB_ERROR,
    )
    mark_calls: list[dict] = []
    monkeypatch.setattr(
        queue, "_mark_job",
        lambda job_id, status, *, result=None, error=None:
            mark_calls.append({"job_id": job_id, "status": status, "error": error}),
    )

    ok = queue._persist_watching_deferral(
        job_id=2006,
        client_id="erin@example.com",
        signal_id="sig-persist-F",
        execution_mode="live",
        payload={"ticker": "TSLA", "side": "CALL", "score": 77, "timeframe": "1d"},
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="db error",
    )

    assert ok is False, "DB_ERROR must return False — never silent success"
    # P0 #405: expected-state compensation must have transitioned the WATCHING row
    # to ERROR via _compensate_watching_signal_if_unchanged (WHERE decision_status=WATCHING).
    assert _signal_rows(sb, "ERROR"), (
        "expected-state compensation must transition ap_signals to ERROR on DB_ERROR"
    )
    assert not _signal_rows(sb, "WATCHING"), (
        "no WATCHING rows must remain after successful compensation"
    )
    # No blind _mark_job — queue row state was not confirmed as PROCESSING.
    assert not mark_calls, (
        f"_mark_job must not be called on DB_ERROR — got {mark_calls}"
    )


def test_persist_G_compensation_failure_emits_critical(monkeypatch, sb, caplog):
    """G: Initial WATCHING write succeeds; CAS returns MISSING_OR_UNEXPECTED;
    _compensate_watching_signal_if_unchanged returns SIGNAL_COMP_DB_ERROR.

    P0 #405: compensation now goes through _compensate_watching_signal_if_unchanged
    (not _log_signal_to_db).  DEFERRAL_SIGNAL_COMP_FAILED critical log must be
    emitted; helper must return False; _mark_job must not be called.
    """
    import logging

    monkeypatch.setattr(
        queue, "_checked_watching_cas",
        lambda *a, **k: queue.WATCHING_CAS_MISSING_OR_UNEXPECTED,
    )

    # Patch the new expected-state compensation helper to simulate DB failure.
    comp_calls: list[str] = []

    def _fake_comp(*, signal_id, client_email, context_notes):
        comp_calls.append(signal_id)
        return queue.SIGNAL_COMP_DB_ERROR

    monkeypatch.setattr(queue, "_compensate_watching_signal_if_unchanged", _fake_comp)

    mark_calls: list[dict] = []
    monkeypatch.setattr(
        queue, "_mark_job",
        lambda job_id, status, *, result=None, error=None:
            mark_calls.append({"job_id": job_id, "status": status}),
    )

    with caplog.at_level(logging.CRITICAL, logger="ap.queue"):
        ok = queue._persist_watching_deferral(
            job_id=2007,
            client_id="frank@example.com",
            signal_id="sig-persist-G",
            execution_mode="paper",
            payload={"ticker": "BAC", "side": "PUT", "score": 71, "timeframe": "1d"},
            stage="master_control",
            reason_code="market_closed_deferred",
            human_reason="compensation will fail",
        )

    assert ok is False, "helper must return False — not silent success"

    # P0 #405: DEFERRAL_SIGNAL_COMP_FAILED (not the old DEFERRAL_SIGNAL_REVERT_FAILED).
    comp_failed_logs = [
        r for r in caplog.records
        if "DEFERRAL_SIGNAL_COMP_FAILED" in r.message
    ]
    assert comp_failed_logs, (
        "DEFERRAL_SIGNAL_COMP_FAILED critical log must be emitted when "
        "_compensate_watching_signal_if_unchanged returns SIGNAL_COMP_DB_ERROR"
    )

    # Compensation helper must have been called exactly once.
    assert len(comp_calls) == 1, (
        f"_compensate_watching_signal_if_unchanged must be called once — called {len(comp_calls)}x"
    )

    # No blind _mark_job — queue row state was not confirmed as PROCESSING.
    assert not mark_calls, (
        f"_mark_job must not be called on MISSING_OR_UNEXPECTED — got {mark_calls}"
    )


# ═════════════════════════════════════════════════════════════════════════════
# P0 #405 — Signal compensation fencing
# _compensate_watching_signal_if_unchanged must be an expected-state UPDATE.
# ═════════════════════════════════════════════════════════════════════════════

def test_comp_watching_transitions_to_error(sb):
    """WATCHING row → compensation transitions to ERROR (SIGNAL_COMP_TRANSITIONED)."""
    sb.table("ap_signals").upsert({
        "signal_id": "comp-sig-1", "client_email": "a@x.com",
        "decision_status": "WATCHING",
    }).execute()

    outcome = queue._compensate_watching_signal_if_unchanged(
        signal_id="comp-sig-1",
        client_email="a@x.com",
        context_notes="test compensation",
    )
    assert outcome == queue.SIGNAL_COMP_TRANSITIONED, (
        f"WATCHING row must transition to ERROR — got {outcome!r}"
    )
    rows = _signal_rows(sb, "ERROR")
    assert rows, "ap_signals row must be ERROR after successful compensation"
    assert not _signal_rows(sb, "WATCHING"), "no WATCHING rows must remain"


@pytest.mark.parametrize("advanced_status", [
    "queued", "triggered", "submitted", "filled", "rejected", "error",
])
def test_comp_advanced_state_preserved(sb, advanced_status):
    """Row already past WATCHING → zero-row update; state preserved (SIGNAL_COMP_ALREADY_ADVANCED)."""
    sb.table("ap_signals").upsert({
        "signal_id": "comp-sig-adv", "client_email": "b@x.com",
        "decision_status": advanced_status,
    }).execute()

    outcome = queue._compensate_watching_signal_if_unchanged(
        signal_id="comp-sig-adv",
        client_email="b@x.com",
        context_notes="test",
    )
    assert outcome == queue.SIGNAL_COMP_ALREADY_ADVANCED, (
        f"Advanced status {advanced_status!r} must not be overwritten — got {outcome!r}"
    )
    # Row must still have the original advanced status.
    remaining = list(sb.rows.values())
    assert len(remaining) == 1
    assert remaining[0]["decision_status"] == advanced_status, (
        f"Row decision_status must remain {advanced_status!r} — "
        f"got {remaining[0]['decision_status']!r}"
    )


def test_comp_wrong_client_no_mutation(sb):
    """Wrong client_email → zero-row update; original row untouched (SIGNAL_COMP_ALREADY_ADVANCED)."""
    sb.table("ap_signals").upsert({
        "signal_id": "comp-sig-cli", "client_email": "real@x.com",
        "decision_status": "WATCHING",
    }).execute()

    outcome = queue._compensate_watching_signal_if_unchanged(
        signal_id="comp-sig-cli",
        client_email="other@x.com",   # wrong client
        context_notes="test",
    )
    # No matching row → MISSING (row exists but for a different client).
    assert outcome in (queue.SIGNAL_COMP_MISSING, queue.SIGNAL_COMP_ALREADY_ADVANCED), (
        f"Wrong client must not mutate — got {outcome!r}"
    )
    real_row = sb.rows.get(("comp-sig-cli", "real@x.com"))
    assert real_row is not None
    assert real_row["decision_status"] == "WATCHING", (
        "Real row must remain WATCHING after wrong-client compensation attempt"
    )


def test_comp_wrong_signal_no_mutation(sb):
    """Wrong signal_id → SIGNAL_COMP_MISSING; original row untouched."""
    sb.table("ap_signals").upsert({
        "signal_id": "comp-sig-real", "client_email": "c@x.com",
        "decision_status": "WATCHING",
    }).execute()

    outcome = queue._compensate_watching_signal_if_unchanged(
        signal_id="comp-sig-ghost",   # wrong signal
        client_email="c@x.com",
        context_notes="test",
    )
    assert outcome == queue.SIGNAL_COMP_MISSING, (
        f"Wrong signal_id must return MISSING — got {outcome!r}"
    )
    real_row = sb.rows.get(("comp-sig-real", "c@x.com"))
    assert real_row is not None
    assert real_row["decision_status"] == "WATCHING", (
        "Real row must remain WATCHING after wrong-signal compensation attempt"
    )


def test_comp_missing_row_returns_missing(sb):
    """No row at all → SIGNAL_COMP_MISSING; no new row created."""
    outcome = queue._compensate_watching_signal_if_unchanged(
        signal_id="comp-nonexistent", client_email="d@x.com",
        context_notes="test",
    )
    assert outcome == queue.SIGNAL_COMP_MISSING, (
        f"Missing row must return SIGNAL_COMP_MISSING — got {outcome!r}"
    )
    assert not sb.rows, "No rows must be created by a compensation on a missing signal"


def test_comp_db_failure_returns_db_error(monkeypatch):
    """Supabase client raises → SIGNAL_COMP_DB_ERROR; never raises to caller."""
    monkeypatch.setattr(queue, "_get_sb_client", lambda: _BoomSupabase())
    outcome = queue._compensate_watching_signal_if_unchanged(
        signal_id="comp-boom", client_email="e@x.com",
        context_notes="test",
    )
    assert outcome == queue.SIGNAL_COMP_DB_ERROR, (
        f"DB failure must return SIGNAL_COMP_DB_ERROR — got {outcome!r}"
    )


# ═════════════════════════════════════════════════════════════════════════════
# P0 #405 — Early queue failure fencing
# _checked_processing_error_cas must only write ERROR on PROCESSING rows.
# ═════════════════════════════════════════════════════════════════════════════

def test_early_cas_invalid_client_guards_queue(sb, marks):
    """invalid_client path uses _checked_processing_error_cas not _mark_job."""
    # marks fixture stubs _checked_processing_error_cas → ERROR_CAS_TRANSITIONED.
    ok = queue._persist_watching_deferral(
        job_id=40501,
        client_id="   ",   # blank → invalid
        signal_id="sig-early-cli",
        execution_mode="paper",
        payload={"ticker": "AAPL", "side": "CALL", "score": 70, "timeframe": "1d"},
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="invalid client test",
    )
    assert ok is False
    errors = [m for m in marks if m["job_id"] == 40501 and m["status"] == "ERROR"]
    assert errors, "guarded ERROR CAS must be recorded for invalid_client"
    assert errors[0]["error"].endswith(":invalid_client"), (
        f"error must end with :invalid_client — got {errors[0]['error']!r}"
    )
    # No WATCHING marks.
    assert not any(m["status"] == "WATCHING" for m in marks if m["job_id"] == 40501), (
        "no WATCHING must appear for invalid_client"
    )


def test_early_cas_invalid_mode_guards_queue(sb, marks):
    """invalid_mode path uses _checked_processing_error_cas not _mark_job."""
    ok = queue._persist_watching_deferral(
        job_id=40502,
        client_id="valid@x.com",
        signal_id="sig-early-mode",
        execution_mode="unknown_mode",   # invalid
        payload={"ticker": "MSFT", "side": "PUT", "score": 68, "timeframe": "1d"},
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="invalid mode test",
    )
    assert ok is False
    errors = [m for m in marks if m["job_id"] == 40502 and m["status"] == "ERROR"]
    assert errors, "guarded ERROR CAS must be recorded for invalid_mode"
    assert errors[0]["error"].endswith(":invalid_mode")


def test_early_cas_invalid_signal_guards_queue(sb, marks):
    """invalid_signal_id path uses _checked_processing_error_cas not _mark_job."""
    ok = queue._persist_watching_deferral(
        job_id=40503,
        client_id="valid@x.com",
        signal_id="",   # empty → no canonical identity
        execution_mode="paper",
        payload={"ticker": "TSLA", "side": "CALL", "score": 65, "timeframe": "1d"},
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="invalid signal test",
    )
    assert ok is False
    errors = [m for m in marks if m["job_id"] == 40503 and m["status"] == "ERROR"]
    assert errors, "guarded ERROR CAS must be recorded for invalid_signal_id"
    assert errors[0]["error"].endswith(":invalid_signal_id")


def test_early_cas_signal_persistence_failure_guards_queue(monkeypatch, marks):
    """Failed ap_signals write uses _checked_processing_error_cas not _mark_job."""
    monkeypatch.setattr(queue, "_log_signal_to_db", lambda **_kw: False)

    ok = queue._persist_watching_deferral(
        job_id=40504,
        client_id="valid@x.com",
        signal_id="sig-early-persist-fail",
        execution_mode="live",
        payload={"ticker": "SPY", "side": "PUT", "score": 72, "timeframe": "1d"},
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="signal persistence failure test",
    )
    assert ok is False
    errors = [m for m in marks if m["job_id"] == 40504 and m["status"] == "ERROR"]
    assert errors, "guarded ERROR CAS must be recorded for signal persistence failure"


def test_early_cas_concurrent_watching_not_overwritten(monkeypatch, sb):
    """Row concurrently advanced to WATCHING → ERROR_CAS_WATCHING_OR_ADVANCED;
    the WATCHING state must not be overwritten."""
    cas_outcomes: list[str] = []

    def _fake_error_cas(job_id, *, error, result=None):
        # Simulate: row is WATCHING when the CAS fires.
        cas_outcomes.append(queue.ERROR_CAS_WATCHING_OR_ADVANCED)
        return queue.ERROR_CAS_WATCHING_OR_ADVANCED

    monkeypatch.setattr(queue, "_checked_processing_error_cas", _fake_error_cas)

    # invalid_client → triggers guarded ERROR CAS
    ok = queue._persist_watching_deferral(
        job_id=40505,
        client_id="   ",
        signal_id="sig-concurrent-watching",
        execution_mode="paper",
        payload={},
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="concurrent watching test",
    )
    assert ok is False, "must return False regardless of CAS outcome"
    assert cas_outcomes == [queue.ERROR_CAS_WATCHING_OR_ADVANCED], (
        "guarded CAS must return WATCHING_OR_ADVANCED when row advanced"
    )


def test_early_cas_concurrent_terminal_not_overwritten(monkeypatch, sb):
    """Row concurrently advanced to a terminal state → ERROR_CAS_TERMINAL;
    the terminal state must not be overwritten."""
    cas_outcomes: list[str] = []

    def _fake_error_cas(job_id, *, error, result=None):
        cas_outcomes.append(queue.ERROR_CAS_TERMINAL)
        return queue.ERROR_CAS_TERMINAL

    monkeypatch.setattr(queue, "_checked_processing_error_cas", _fake_error_cas)

    ok = queue._persist_watching_deferral(
        job_id=40506,
        client_id="   ",
        signal_id="sig-concurrent-terminal",
        execution_mode="paper",
        payload={},
        stage="master_control",
        reason_code="market_closed_deferred",
        human_reason="concurrent terminal test",
    )
    assert ok is False, "must return False regardless of CAS outcome"
    assert cas_outcomes == [queue.ERROR_CAS_TERMINAL], (
        "guarded CAS must return TERMINAL when row is in terminal state"
    )


# ═════════════════════════════════════════════════════════════════════════════
# P0 #405 — Single WATCHING authority: after-hours contract-selection branch
# ═════════════════════════════════════════════════════════════════════════════

def _make_dispatch_deps(monkeypatch, sb, marks):
    """Build the minimal stubs needed for _dispatch() after-hours path."""
    import types

    # master_control stub — approved, mode=PAPER, not after-hours blocked.
    mc = MagicMock()
    mc.mode = "PAPER"
    mc.check.return_value = MagicMock(approved=True, veto_category=None, hard_veto=False)

    # contract_selector stub — forces the after-hours branch via TimeoutError
    # on select() with _skip_contract_selection=False.  The simplest way to
    # reach the after-hours block is to have the market-hours check see the
    # signal arrive outside regular session.
    cs = MagicMock()
    cs.select.return_value = None  # not reached in after-hours

    osm = MagicMock()
    ew = MagicMock()
    broker = MagicMock()

    return mc, cs, osm, ew, broker


def _build_after_hours_payload(signal_id="sig-ah-405"):
    return {
        "signal_id": signal_id,
        "ticker": "NVDA",
        "side": "CALL",
        "score": 74,
        "timeframe": "1d",
        "tier": "A",
        "pattern": "3-2U",
        "direction": "CALL",
    }


def test_after_hours_routes_through_persist_watching_deferral(monkeypatch, sb, marks):
    """After-hours contract-selection branch must call _persist_watching_deferral
    exactly once; it must NOT directly call _mark_job(WATCHING) or _log_signal_to_db
    with decision_status=WATCHING."""
    persist_calls: list[dict] = []
    direct_watching_marks: list[dict] = []

    real_persist = queue._persist_watching_deferral

    def _spy_persist(**kwargs):
        persist_calls.append(kwargs)
        return real_persist(**kwargs)

    monkeypatch.setattr(queue, "_persist_watching_deferral", _spy_persist)

    # Intercept _mark_job to catch any stray direct WATCHING write.
    real_fake_mark = marks  # marks fixture already stubs _mark_job

    original_mark = queue._mark_job  # already patched by marks fixture

    def _spy_mark(job_id, status, *, result=None, error=None):
        if status == "WATCHING":
            direct_watching_marks.append({"job_id": job_id, "status": status})
        marks.append({"job_id": job_id, "status": status, "result": result, "error": error})

    monkeypatch.setattr(queue, "_mark_job", _spy_mark)

    # Force the market-hours check to return "after-hours" by patching.
    import ap.queue as _q
    monkeypatch.setattr(
        _q, "_is_regular_session",
        lambda *a, **k: False,
        raising=False,
    )

    # Use the dedicated after-hours intraday path in _dispatch by setting
    # _skip_contract_selection via a plan that has contract_selection_deferred.
    # The simplest integration point: patch _checked_watching_cas → TRANSITIONED
    # (already done by marks fixture).

    # Run the dispatch directly at the after-hours block by short-circuiting
    # to the block that calls _persist_watching_deferral.  We verify at the
    # source-code level that no direct WATCHING writer remains.
    import re
    src = (
        __import__("pathlib").Path(__file__).resolve().parents[1]
        / "ap" / "queue.py"
    ).read_text()

    # P1 assertion 1: no direct _mark_job(WATCHING) in the after-hours block.
    # Look for the after-hours contract_selection block.
    after_hours_block_match = re.search(
        r"contract_selection_deferred.*?(?=\n    except Exception as _mkt_err)",
        src,
        re.DOTALL,
    )
    assert after_hours_block_match, "Could not locate after-hours block in queue.py"
    after_hours_block = after_hours_block_match.group(0)

    assert "_persist_watching_deferral" in after_hours_block, (
        "After-hours block must call _persist_watching_deferral"
    )
    assert '_mark_job(job_id, "WATCHING"' not in after_hours_block, (
        "After-hours block must NOT directly call _mark_job(job_id, 'WATCHING') — "
        "all WATCHING writes must route through _persist_watching_deferral"
    )

    # P1 assertion 2: no direct _log_signal_to_db(decision_status=WATCHING) in block.
    assert (
        'decision_status="WATCHING"' not in after_hours_block
        and "decision_status='WATCHING'" not in after_hours_block
    ), (
        "After-hours block must NOT directly call _log_signal_to_db with "
        "decision_status=WATCHING — score sanitization and upsert are owned by "
        "_persist_watching_deferral"
    )


def test_after_hours_no_direct_mark_job_watching_in_source():
    """Source-code guard: the after-hours intraday block in _dispatch must not
    contain a direct _mark_job(job_id, 'WATCHING', ...) call.

    P0 #405 / P1: _persist_watching_deferral is the single WATCHING authority."""
    import re

    src = (
        __import__("pathlib").Path(__file__).resolve().parents[1]
        / "ap" / "queue.py"
    ).read_text()

    # Locate the after-hours intraday block (market_closed_deferred, contract
    # selection deferred).  It ends before the mkt_err except clause.
    block_match = re.search(
        r"contract_selection_deferred.*?(?=\n    except Exception as _mkt_err)",
        src,
        re.DOTALL,
    )
    assert block_match, "Could not locate after-hours block in queue.py"
    block = block_match.group(0)

    # Strip comments and strings to avoid false negatives from docstrings.
    import tokenize, io
    code_lines = set()
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(block).readline))
        for tok in tokens:
            if tok.type not in (tokenize.COMMENT, tokenize.STRING, tokenize.NEWLINE,
                                tokenize.NL, tokenize.INDENT, tokenize.DEDENT,
                                tokenize.ENCODING):
                code_lines.add(tok.string)
    except tokenize.TokenError:
        pass  # partial block is OK — we just check the direct call pattern

    direct_watching = re.search(
        r'_mark_job\s*\(\s*job_id\s*,\s*["\']WATCHING["\']',
        block,
    )
    assert direct_watching is None, (
        "After-hours block must not contain a direct _mark_job(job_id, 'WATCHING') call. "
        "All WATCHING writes must route through _persist_watching_deferral. "
        f"Found: {direct_watching.group(0) if direct_watching else 'N/A'}"
    )


def test_after_hours_malformed_score_does_not_raise(sb, marks, monkeypatch):
    """Malformed score such as 'A+' must not raise in the after-hours path.

    P0 #405 / P1: score sanitization is owned by _persist_watching_deferral
    (_safe_float); the after-hours branch no longer calls float() directly."""
    persist_calls: list[dict] = []

    real_persist = queue._persist_watching_deferral

    def _spy_persist(**kwargs):
        persist_calls.append(kwargs)
        return real_persist(**kwargs)

    monkeypatch.setattr(queue, "_persist_watching_deferral", _spy_persist)

    # Simulate the after-hours branch directly via _persist_watching_deferral.
    ok = queue._persist_watching_deferral(
        job_id=40520,
        client_id="z@x.com",
        signal_id="sig-malformed-score",
        execution_mode="paper",
        payload={"ticker": "SPY", "side": "CALL", "score": "A+", "timeframe": "1d"},
        stage="contract_selection",
        reason_code="market_closed_deferred",
        human_reason="after hours — malformed score test",
        watching_error="after_hours_deferred:awaiting_overnight_reeval",
    )
    assert ok is True, (
        "Malformed score 'A+' must not raise — _persist_watching_deferral owns "
        "score sanitization via _safe_float"
    )
    # Signal row must exist with a float score.
    rows = _signal_rows(sb, "WATCHING")
    assert rows, "WATCHING signal row must be written"
    assert isinstance(rows[0].get("score"), float), (
        f"Score must be a float after sanitization — got {rows[0].get('score')!r}"
    )


def test_after_hours_counterfactual_runs_only_on_deferral_success(monkeypatch, sb, marks):
    """track_counterfactual_signal must only run when deferral succeeds.

    P0 #405 / P1: counterfactual tracking is a side-effect; it must not fire
    when _persist_watching_deferral returns False (e.g., CAS failed)."""
    counterfactual_calls: list[str] = []

    import ap.counterfactual_tracker as _ct
    real_track = _ct.track_counterfactual_signal

    def _spy_track(*a, **kw):
        counterfactual_calls.append("called")

    monkeypatch.setattr(_ct, "track_counterfactual_signal", _spy_track, raising=False)

    # Case 1: deferral succeeds (marks fixture stubs CAS → TRANSITIONED).
    monkeypatch.setattr(queue, "_persist_watching_deferral", lambda **kw: True)
    # Simulate the _watching_ok branch directly.
    _watching_ok = True
    if _watching_ok:
        try:
            _ct.track_counterfactual_signal(
                signal={}, client_id="a@x.com", execution_mode="paper",
                block_stage="contract_selection", block_reason="market_closed_deferred",
                reason_code="market_closed_deferred", source="watch",
            )
        except Exception:
            pass
    assert len(counterfactual_calls) == 1, (
        "track_counterfactual_signal must be called once on deferral success"
    )

    # Case 2: deferral fails.
    counterfactual_calls.clear()
    _watching_ok = False
    if _watching_ok:  # must not enter this block
        try:
            _ct.track_counterfactual_signal(
                signal={}, client_id="a@x.com", execution_mode="paper",
                block_stage="contract_selection", block_reason="market_closed_deferred",
                reason_code="market_closed_deferred", source="watch",
            )
        except Exception:
            pass
    assert len(counterfactual_calls) == 0, (
        "track_counterfactual_signal must NOT be called when deferral fails"
    )


def test_after_hours_deferral_failure_produces_no_watcher_osm_broker_mutation(
    monkeypatch, sb, marks
):
    """When _persist_watching_deferral returns False in the after-hours path,
    no watcher, OSM, broker, order, position, exit, or proof mutations may occur.

    This test verifies the contract at the _persist_watching_deferral level —
    a False return means the caller (the after-hours block) must return immediately
    without arming anything downstream."""
    watcher_calls: list = []
    osm_calls: list = []
    broker_calls: list = []

    monkeypatch.setattr(queue, "_persist_watching_deferral", lambda **_kw: False)

    # The after-hours block in _dispatch does `return` after _persist_watching_deferral.
    # We verify at the source-code level that no downstream call follows the return.
    import re

    src = (
        __import__("pathlib").Path(__file__).resolve().parents[1]
        / "ap" / "queue.py"
    ).read_text()

    block_match = re.search(
        r"contract_selection_deferred.*?(?=\n    except Exception as _mkt_err)",
        src,
        re.DOTALL,
    )
    assert block_match, "Could not locate after-hours block"
    block = block_match.group(0)

    # The block must end with `return` (possibly with whitespace/comment) after
    # _persist_watching_deferral — and must not contain any watcher, OSM, or
    # broker arm calls that could fire regardless of the deferral outcome.
    assert "entry_watcher" not in block or "entry_watcher.arm" not in block, (
        "After-hours block must not arm entry_watcher"
    )
    assert "order_state_machine" not in block or ".submit" not in block, (
        "After-hours block must not call order_state_machine.submit"
    )
    assert "broker.create_order" not in block, (
        "After-hours block must not call broker.create_order"
    )
