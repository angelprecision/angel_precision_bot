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
        self._filters = []  # list of (op, col, val)

    # ── write path ────────────────────────────────────────────────────────────
    def upsert(self, row, on_conflict=None):
        r = dict(row)
        r.setdefault("created_at", datetime.now(timezone.utc).isoformat())
        key = (str(r.get("signal_id")), str(r.get("client_email")))
        self._store.rows[key] = r  # composite-key upsert → inherently idempotent
        self._pending = _FakeResult([r])
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
        if getattr(self, "_pending", None) is not None:
            out, self._pending = self._pending, None
            return out
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
    """A Supabase client whose upsert always raises — simulates write failure."""

    def table(self, _name):
        return self

    def upsert(self, *_a, **_k):
        raise RuntimeError("supabase upsert failed")

    def execute(self):  # pragma: no cover - never reached
        raise RuntimeError("never reached")


@pytest.fixture
def sb(monkeypatch):
    fake = _FakeSupabase()
    monkeypatch.setattr(queue, "_get_sb_client", lambda: fake)
    return fake


@pytest.fixture
def marks(monkeypatch):
    """Capture every _mark_job(job_id, status, error=, result=) call.

    Also stubs _checked_watching_cas to return True (simulating a successful
    single-row queue CAS) so tests that exercise _persist_watching_deferral or
    _dispatch do not require a live Postgres connection.  Tests that specifically
    exercise CAS failure (test_4e, test_4f) override this with their own
    monkeypatch after the fixture runs.
    """
    recorded: list[dict] = []

    def _fake_mark(job_id, status, *, result=None, error=None):
        recorded.append(
            {"job_id": job_id, "status": status, "result": result, "error": error}
        )

    monkeypatch.setattr(queue, "_mark_job", _fake_mark)

    # Default: CAS succeeds (TRANSITIONED) AND records a WATCHING mark so that
    # assertions like `[m["status"] for m in marks if m["job_id"] == X]` still
    # see ["WATCHING"] without requiring a live Postgres connection.
    # Tests that exercise specific CAS outcomes override this with their own
    # monkeypatch after the fixture runs.
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
    """F integration: CAS outcome DB_ERROR → helper False.
    Signal compensation attempted. No claim of successful deferral."""
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
    # Signal compensation attempted (ERROR row written to ap_signals).
    assert _signal_rows(sb, "ERROR"), (
        "signal compensation must be attempted on DB_ERROR"
    )
    # No blind _mark_job.
    assert not mark_calls, (
        f"_mark_job must not be called on DB_ERROR — got {mark_calls}"
    )


def test_persist_G_compensation_bool_failure_emits_critical(monkeypatch, sb, caplog):
    """G: Initial WATCHING write succeeds; CAS returns MISSING_OR_UNEXPECTED;
    compensation write returns False. Critical DEFERRAL_SIGNAL_REVERT_FAILED
    diagnostic must be emitted; helper must return False; no silent success."""
    import logging

    monkeypatch.setattr(
        queue, "_checked_watching_cas",
        lambda *a, **k: queue.WATCHING_CAS_MISSING_OR_UNEXPECTED,
    )

    call_count = [0]

    def _fake_log_signal(*, decision_status, **kwargs):
        call_count[0] += 1
        if decision_status == queue.DECISION_WATCHING:
            return True   # initial WATCHING write succeeds
        return False      # compensation write fails — returns False, not exception

    monkeypatch.setattr(queue, "_log_signal_to_db", _fake_log_signal)

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

    # DEFERRAL_SIGNAL_REVERT_FAILED critical log must be emitted because the
    # compensation write returned False (not an exception — must not rely on
    # try/except alone to detect this).
    revert_failed_logs = [
        r for r in caplog.records
        if "DEFERRAL_SIGNAL_REVERT_FAILED" in r.message
    ]
    assert revert_failed_logs, (
        "DEFERRAL_SIGNAL_REVERT_FAILED critical log must be emitted when "
        "compensation write returns False (not just when it raises)"
    )

    # Both initial write and compensation write must be attempted.
    assert call_count[0] >= 2, (
        f"_log_signal_to_db must be called at least twice (initial + compensation) "
        f"— called {call_count[0]}x"
    )

    # No blind _mark_job.
    assert not mark_calls, (
        f"_mark_job must not be called on MISSING_OR_UNEXPECTED — got {mark_calls}"
    )
