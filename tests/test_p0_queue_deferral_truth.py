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
    """Capture every _mark_job(job_id, status, error=, result=) call."""
    recorded: list[dict] = []

    def _fake_mark(job_id, status, *, result=None, error=None):
        recorded.append(
            {"job_id": job_id, "status": status, "result": result, "error": error}
        )

    monkeypatch.setattr(queue, "_mark_job", _fake_mark)
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
# 7. Execution-mode isolation — PAPER deferral cannot claim/satisfy a LIVE row
# ═════════════════════════════════════════════════════════════════════════════
def test_7_execution_mode_isolation(sb, marks):
    common = dict(
        payload={"ticker": "AMD", "side": "CALL", "score": 71, "timeframe": "1d"},
        stage="master_control", reason_code="market_closed_deferred",
        human_reason="after hours",
    )
    # Same signal id, DIFFERENT client per mode (paper vs live run under
    # different client identities). The rows must not collide or cross-satisfy.
    assert queue._persist_watching_deferral(
        job_id=701, client_id="paper-acct@x.com", signal_id="sig-7",
        execution_mode="PAPER", **common) is True
    assert queue._persist_watching_deferral(
        job_id=702, client_id="live-acct@x.com", signal_id="sig-7",
        execution_mode="LIVE", **common) is True

    paper_row = sb.rows[("sig-7", "paper-acct@x.com")]
    live_row = sb.rows[("sig-7", "live-acct@x.com")]
    assert paper_row["raw_payload"]["execution_mode"] == "paper"
    assert live_row["raw_payload"]["execution_mode"] == "live"
    # The PAPER write did not mutate the LIVE row's mode, and vice versa.
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
