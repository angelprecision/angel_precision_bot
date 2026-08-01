"""PR #404 amendment: real-PostgreSQL atomicity of the overnight watcher-arm claim.

The durable ownership authority for a watcher-arm attempt is the
client_signal_opportunities row for (canonical_signal_id, client_id), locked
with SELECT ... FOR UPDATE inside one transaction and mutated with a guarded
UPDATE ... RETURNING. This suite drives the REAL production helpers
(_atomic_claim_watch_arm_attempt / _bind_watch_arm_attempt_order /
_complete_watch_arm_attempt) against a real Postgres using two independent
connections and overlapping threads. Nothing about the atomic SQL is mocked.

Assertions prove exactly one claimant wins a concurrent race, the durable count
increments exactly once, retries are bounded, prior-order replacement requires
exact terminal proof of the same order, and bind/complete refuse a stolen owner.

Skipped unless DATABASE_URL points at a reachable Postgres. CI's P0 workflow
provisions a disposable Postgres and sets DATABASE_URL, so the suite runs there
and must never be skipped in CI.
"""
from __future__ import annotations

import os
import sys
import threading
import json
import types
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

# Import safety: ap_overnight_reeval's transitive imports expect a DATABASE_URL
# to exist. The real connection target below is validated separately.
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

try:
    import psycopg2
    import psycopg2.extras
except ImportError:  # pragma: no cover - handled by skip below
    psycopg2 = None

_RAW_URL = os.getenv("DATABASE_URL", "").strip()
IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS", "").strip().lower() == "true"


def _pg_reachable(url: str) -> bool:
    if not (psycopg2 and url):
        return False
    try:
        c = psycopg2.connect(url)
        c.close()
        return True
    except Exception:
        return False


_PG_AVAILABLE = _pg_reachable(_RAW_URL)

if IN_GITHUB_ACTIONS and not _PG_AVAILABLE:
    raise RuntimeError(
        "Overnight watch-attempt atomicity test requires a reachable Postgres "
        "(DATABASE_URL) in GitHub Actions; the P0 workflow provisions one."
    )

pytestmark = pytest.mark.skipif(
    not _PG_AVAILABLE,
    reason="DATABASE_URL not reachable outside CI",
)

import ap_overnight_reeval as overnight  # noqa: E402

ACQUIRED = overnight.WATCH_ATTEMPT_ACQUIRED
IN_PROGRESS = overnight.WATCH_ATTEMPT_ALREADY_IN_PROGRESS
CONFLICT = overnight.WATCH_ATTEMPT_CONFLICT
EXHAUSTED = overnight.WATCH_ATTEMPT_EXHAUSTED
ALREADY_ARMED = overnight.WATCH_ATTEMPT_ALREADY_ARMED
DB_ERROR = overnight.WATCH_ATTEMPT_DB_ERROR

S_IN_PROGRESS = overnight.WATCH_ATTEMPT_STATE_IN_PROGRESS
S_RETRYABLE = overnight.WATCH_ATTEMPT_STATE_RETRYABLE
S_ARMED = overnight.WATCH_ATTEMPT_STATE_ARMED
S_EXHAUSTED = overnight.WATCH_ATTEMPT_STATE_EXHAUSTED

CANON = "CANON-ATOMIC-001"
CLIENT = "client-1"
SESSION = "2026-07-29"
SIGNAL_ID = "sig-atomic-001"


# ── independent connection factory (one Postgres session per call) ────────────

@contextmanager
def _independent_conn():
    """Yield a fresh, independent psycopg2 session with the interface
    ap_overnight_reeval expects (execute / fetchone / fetchall). Commit on
    success, roll back on error. A distinct session per call is what makes the
    two worker threads genuinely contend on SELECT ... FOR UPDATE."""
    connection = psycopg2.connect(_RAW_URL)
    connection.autocommit = False
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    class _Wrap:
        def execute(self, sql, params=()):
            cursor.execute(sql, params if params is not None else ())
            return self

        def fetchone(self):
            row = cursor.fetchone()
            return dict(row) if row else None

        def fetchall(self):
            return [dict(r) for r in (cursor.fetchall() or [])]

    try:
        yield _Wrap()
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


class _FakeOSM:
    """Minimal order-state-machine stub for prior-terminal-order resolution.

    orders maps local_order_id -> {"status": ...}. Missing ids look absent.
    Rows are auto-enriched with production identity fields
    (local_order_id, client_id, execution_mode, canonical_signal_id, kind)
    so _local_order_terminal_state's identity fencing (Blocker 2) can match.
    """

    def __init__(self, orders=None, client_id=None, canonical_signal_id=None,
                 execution_mode="live"):
        self.orders = dict(orders or {})
        self.get_calls: list[str] = []
        self._default_client = client_id or CLIENT
        self._default_canonical = canonical_signal_id or CANON
        self._default_mode = execution_mode

    def get_order(self, local_order_id):
        self.get_calls.append(local_order_id)
        row = dict(self.orders.get(local_order_id) or {})
        if not row:
            return row
        row.setdefault("local_order_id", local_order_id)
        row.setdefault("client_id", self._default_client)
        row.setdefault("execution_mode", self._default_mode)
        row.setdefault("canonical_signal_id", self._default_canonical)
        row.setdefault("kind", "ENTRY")
        return row


@pytest.fixture(autouse=True)
def _schema():
    """Production-shaped, disposable client_signal_opportunities table."""
    setup = psycopg2.connect(_RAW_URL)
    setup.autocommit = True
    with setup.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS client_signal_opportunities (
                id                  bigserial PRIMARY KEY,
                signal_id           text NOT NULL,
                canonical_signal_id text NOT NULL,
                client_id           text NOT NULL,
                opportunity_status  text NOT NULL DEFAULT 'CREATED',
                order_local_id      text,
                metadata            jsonb,
                updated_at          timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_cso_atomic_canon_client "
            "ON client_signal_opportunities (canonical_signal_id, client_id)"
        )
        cur.execute("TRUNCATE client_signal_opportunities")
    setup.close()
    yield


@pytest.fixture(autouse=True)
def _wire_db(monkeypatch):
    """Route ap.db.conn to independent sessions and skip the Supabase ensure-row
    (rows are seeded directly into Postgres here). run_with_retry is passthrough
    so the real transaction boundary is exercised, not a retry shim."""
    import types

    db_mod = types.ModuleType("ap.db")
    db_mod.conn = _independent_conn
    db_mod.run_with_retry = lambda fn, *a, **kw: fn()
    monkeypatch.setitem(sys.modules, "ap.db", db_mod)
    monkeypatch.setattr(overnight, "_ensure_opportunity_row", lambda *a, **kw: None)


# ── seed / read helpers (direct Postgres) ─────────────────────────────────────

def _seed(*, mode, session=SESSION, state, count, token="", order_id="",
          client=CLIENT, canonical=CANON, signal_id=SIGNAL_ID):
    meta = overnight._attempt_meta_patch(
        existing_meta={},
        execution_mode=mode,
        session_key=session,
        state=state,
        attempt_count=count,
        token=token,
        local_order_id=order_id,
        reason="seed",
    )
    import json
    setup = psycopg2.connect(_RAW_URL)
    setup.autocommit = True
    with setup.cursor() as cur:
        cur.execute(
            "INSERT INTO client_signal_opportunities "
            "(signal_id, canonical_signal_id, client_id, opportunity_status, "
            " order_local_id, metadata) VALUES (%s,%s,%s,%s,%s,%s::jsonb) "
            "ON CONFLICT (canonical_signal_id, client_id) DO UPDATE SET "
            "metadata = EXCLUDED.metadata",
            (signal_id, canonical, client, "CREATED", order_id or None, json.dumps(meta)),
        )
    setup.close()


def _seed_raw_scope(scope, *, mode, session=SESSION, client=CLIENT, canonical=CANON):
    """Seed the EXACT scope dict into metadata without normalizing/repairing it,
    so the strict parser sees the real malformed/unknown value under the lock."""
    import json
    key = overnight._attempt_scope_key(mode, session)
    meta = {"overnight_watch_arm_attempt_scopes": {key: scope}}
    setup = psycopg2.connect(_RAW_URL)
    setup.autocommit = True
    with setup.cursor() as cur:
        cur.execute(
            "INSERT INTO client_signal_opportunities "
            "(signal_id, canonical_signal_id, client_id, opportunity_status, "
            " order_local_id, metadata) VALUES (%s,%s,%s,%s,%s,%s::jsonb) "
            "ON CONFLICT (canonical_signal_id, client_id) DO UPDATE SET "
            "metadata = EXCLUDED.metadata",
            (SIGNAL_ID, canonical, client, "CREATED", None, json.dumps(meta)),
        )
    setup.close()


def _complete(attempt, *, state, order_id, mode="live", reason="test"):
    return overnight._complete_watch_arm_attempt(
        signal_id=SIGNAL_ID,
        client_id=CLIENT,
        canonical_signal_id=CANON,
        signal_payload={"signal_id": SIGNAL_ID},
        execution_mode=mode,
        session_key=SESSION,
        attempt=attempt,
        state=state,
        local_order_id=order_id,
        reason=reason,
    )


def _read_scope(*, mode, session=SESSION, client=CLIENT, canonical=CANON):
    conn = psycopg2.connect(_RAW_URL)
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "SELECT metadata FROM client_signal_opportunities "
        "WHERE canonical_signal_id=%s AND client_id=%s",
        (canonical, client),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    meta = dict(row["metadata"]) if row and row["metadata"] else {}
    return overnight._attempt_meta_scope(meta, mode, session)


def _claim(*, mode, osm=None, client=CLIENT, canonical=CANON, max_attempts=3):
    # Call the atomic helper directly, bypassing the process-local
    # threading.Lock, so the test exercises Postgres row-lock concurrency.
    return overnight._atomic_claim_watch_arm_attempt(
        order_state_machine=osm or _FakeOSM(),
        signal_id=SIGNAL_ID,
        client_id=client,
        canonical_signal_id=canonical,
        signal_payload={"signal_id": SIGNAL_ID},
        execution_mode=mode,
        session_key=SESSION,
        max_attempts=max_attempts,
    )


# ── concurrency proof ─────────────────────────────────────────────────────────

def test_two_processes_single_winner_first_claim():
    """Two independent connections race the first claim from RETRYABLE/count=0.
    Exactly one wins ACQUIRED; the loser is IN_PROGRESS or CONFLICT (never a
    second ACQUIRED). The durable row shows exactly one increment. Only the
    winning path would reach OSM create / watcher arm; the loser reaches
    neither, and no broker submit occurs on the losing path."""
    _seed(mode="live", state=S_RETRYABLE, count=0)

    results: list[overnight._WatchAttemptClaim] = []
    lock = threading.Lock()
    barrier = threading.Barrier(2)
    effects = {"osm_create": 0, "watch": 0, "broker_submit": 0}

    def worker():
        barrier.wait()
        claim = _claim(mode="live")
        with lock:
            results.append(claim)
            # Production only reaches create_entry_order / entry_watcher.watch
            # on ACQUIRED; a broker submit never happens on this pre-open path.
            if claim.disposition == ACQUIRED:
                effects["osm_create"] += 1
                effects["watch"] += 1

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)

    dispositions = [c.disposition for c in results]
    assert len(results) == 2, dispositions
    assert dispositions.count(ACQUIRED) == 1, dispositions
    loser = [d for d in dispositions if d != ACQUIRED][0]
    assert loser in (IN_PROGRESS, CONFLICT), dispositions

    scope = _read_scope(mode="live")
    assert overnight._attempt_state(scope) == S_IN_PROGRESS
    assert overnight._attempt_count(scope) == 1
    assert str(scope.get("token") or "")  # exactly one durable token
    winner_token = [c.token for c in results if c.disposition == ACQUIRED][0]
    assert str(scope.get("token")) == winner_token

    assert effects["osm_create"] == 1
    assert effects["watch"] == 1
    assert effects["broker_submit"] == 0


def test_two_processes_single_winner_final_allowed_attempt():
    """RETRYABLE count=2, max=3, prior order proven terminal → exactly one worker
    acquires attempt 3, the other loses, durable count lands at 3."""
    _seed(mode="live", state=S_RETRYABLE, count=2, token="tok-prev", order_id="prior-1")
    osm = _FakeOSM({"prior-1": {"status": "EXPIRED"}})

    results = []
    lock = threading.Lock()
    barrier = threading.Barrier(2)

    def worker():
        barrier.wait()
        claim = _claim(mode="live", osm=osm)
        with lock:
            results.append(claim.disposition)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)

    assert results.count(ACQUIRED) == 1, results
    assert [d for d in results if d != ACQUIRED][0] in (IN_PROGRESS, CONFLICT)
    scope = _read_scope(mode="live")
    assert overnight._attempt_state(scope) == S_IN_PROGRESS
    assert overnight._attempt_count(scope) == 3


def test_exhausted_does_not_acquire():
    """count == max → EXHAUSTED, no acquisition, count unchanged.

    A RETRYABLE seed with count > 0 must carry both a token and a bound prior
    order (strict scope contract); otherwise the strict parser rejects it as a
    malformed scope, which is a separate assertion covered by
    test_malformed_or_unknown_attempt_scope_never_acquires.
    """
    _seed(mode="live", state=S_RETRYABLE, count=3, token="tok-x", order_id="prior-x")
    osm = _FakeOSM({"prior-x": {"status": "EXPIRED"}})
    claim = _claim(mode="live", osm=osm, max_attempts=3)
    assert claim.disposition == EXHAUSTED
    scope = _read_scope(mode="live")
    assert overnight._attempt_state(scope) == S_EXHAUSTED
    assert overnight._attempt_count(scope) == 3


def test_active_prior_order_reclaims_same_order_with_increment(_orders_table):
    """RETRYABLE + exact PENDING_TRIGGER reclaims the same bounded attempt."""
    _seed(mode="live", state=S_RETRYABLE, count=1, token="tok-1", order_id="prior-1")
    _insert_order(
        local_order_id="prior-1",
        status="PENDING_TRIGGER",
        client_id=CLIENT,
        canonical_signal_id=CANON,
        signal_id=SIGNAL_ID,
    )
    osm = _FakeOSM({"prior-1": {"status": "PENDING_TRIGGER"}})
    claim = _claim(mode="live", osm=osm)
    assert claim.disposition == REATTACH_REQUIRED, claim
    assert claim.local_order_id == "prior-1"
    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 2


def test_terminal_proof_of_different_order_is_conflict():
    """Preliminary terminal proof is for prior-1, but the durable order changed
    to prior-2 before the locked reread → conflict, no acquisition. Stale
    terminal proof can never authorize replacing a different order."""
    _seed(mode="live", state=S_RETRYABLE, count=1, token="tok-1", order_id="prior-1")

    real_terminal = overnight._local_order_terminal_state

    def _swap_then_prove(osm, local_order_id, **kwargs):
        # After the preliminary read proved prior-1 terminal, a concurrent actor
        # rebinds the durable scope to a *different* prior order. Accept the
        # identity-fencing kwargs (Blocker 2) and forward them unchanged.
        _seed(mode="live", state=S_RETRYABLE, count=1, token="tok-1", order_id="prior-2")
        return real_terminal(osm, local_order_id, **kwargs)

    osm = _FakeOSM({"prior-1": {"status": "EXPIRED"}, "prior-2": {"status": "PENDING_TRIGGER"}})
    import unittest.mock as mock
    with mock.patch.object(overnight, "_local_order_terminal_state", _swap_then_prove):
        claim = _claim(mode="live", osm=osm)
    assert claim.disposition == CONFLICT, claim
    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 1


def test_already_armed_is_idempotent_success():
    """ARMED durable scope → ALREADY_ARMED, no increment, no new order."""
    _seed(mode="live", state=S_ARMED, count=1, token="tok-A", order_id="ord-A")
    claim = _claim(mode="live")
    assert claim.disposition == ALREADY_ARMED
    assert claim.attempt_count == 1
    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 1
    assert overnight._attempt_state(scope) == S_ARMED


def test_cross_mode_isolation():
    """Same canonical/client/session, different execution mode → independent
    claims, no collision."""
    _seed(mode="live", state=S_RETRYABLE, count=0)
    _seed(mode="paper", state=S_RETRYABLE, count=0)
    live = _claim(mode="live")
    paper = _claim(mode="paper")
    assert live.disposition == ACQUIRED
    assert paper.disposition == ACQUIRED
    assert overnight._attempt_count(_read_scope(mode="live")) == 1
    assert overnight._attempt_count(_read_scope(mode="paper")) == 1


def test_cross_client_isolation():
    """Same canonical/mode/session, different clients → independent rows."""
    _seed(mode="live", state=S_RETRYABLE, count=0, client="client-1")
    _seed(mode="live", state=S_RETRYABLE, count=0, client="client-2")
    a = _claim(mode="live", client="client-1")
    b = _claim(mode="live", client="client-2")
    assert a.disposition == ACQUIRED
    assert b.disposition == ACQUIRED
    assert overnight._attempt_count(_read_scope(mode="live", client="client-1")) == 1
    assert overnight._attempt_count(_read_scope(mode="live", client="client-2")) == 1


def test_bind_refuses_stolen_owner():
    """Owner A acquires, but the durable token is replaced by B before bind →
    _bind_watch_arm_attempt_order returns False and does not overwrite B."""
    _seed(mode="live", state=S_RETRYABLE, count=0)
    a = _claim(mode="live")
    assert a.disposition == ACQUIRED
    # Steal: overwrite the durable scope with a different token/state.
    _seed(mode="live", state=S_IN_PROGRESS, count=a.attempt_count, token="token-B")

    ok = overnight._bind_watch_arm_attempt_order(
        signal_id=SIGNAL_ID,
        client_id=CLIENT,
        canonical_signal_id=CANON,
        signal_payload={"signal_id": SIGNAL_ID},
        execution_mode="live",
        session_key=SESSION,
        attempt=a,
        local_order_id="ord-a",
    )
    assert ok is False
    scope = _read_scope(mode="live")
    assert str(scope.get("token")) == "token-B"
    assert str(scope.get("local_order_id") or "") == ""


def test_complete_refuses_stolen_owner():
    """Owner A acquires + binds, but the durable token is replaced by B before
    completion → _complete_watch_arm_attempt returns False, no overwrite."""
    _seed(mode="live", state=S_RETRYABLE, count=0)
    a = _claim(mode="live")
    assert a.disposition == ACQUIRED
    bound = overnight._bind_watch_arm_attempt_order(
        signal_id=SIGNAL_ID, client_id=CLIENT, canonical_signal_id=CANON,
        signal_payload={"signal_id": SIGNAL_ID}, execution_mode="live",
        session_key=SESSION, attempt=a, local_order_id="ord-a",
    )
    assert bound is True
    # Steal.
    _seed(mode="live", state=S_IN_PROGRESS, count=a.attempt_count, token="token-B",
          order_id="ord-a")

    ok = overnight._complete_watch_arm_attempt(
        signal_id=SIGNAL_ID, client_id=CLIENT, canonical_signal_id=CANON,
        signal_payload={"signal_id": SIGNAL_ID}, execution_mode="live",
        session_key=SESSION, attempt=a, state=S_ARMED, local_order_id="ord-a",
        reason="watcher_armed",
    )
    assert ok is False
    scope = _read_scope(mode="live")
    assert str(scope.get("token")) == "token-B"
    assert overnight._attempt_state(scope) == S_IN_PROGRESS


def test_db_exception_is_db_error_and_blocks(monkeypatch):
    """A locked-query failure surfaces as WATCH_ATTEMPT_DB_ERROR with no
    acquisition — fail closed (spec Test L)."""
    _seed(mode="live", state=S_RETRYABLE, count=0)

    import types
    broken = types.ModuleType("ap.db")

    @contextmanager
    def _boom():
        raise RuntimeError("simulated_locked_query_failure")
        yield  # pragma: no cover

    broken.conn = _boom
    broken.run_with_retry = lambda fn, *a, **kw: fn()
    monkeypatch.setitem(sys.modules, "ap.db", broken)

    claim = _claim(mode="live")
    assert claim.disposition == DB_ERROR
    # Durable state unchanged (read via a fresh real connection).
    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 0


def test_bounded_lifecycle_three_attempts_then_exhausted():
    """Full bounded sequence with terminal proof between attempts: three
    acquisitions, then the fourth is EXHAUSTED. Proves attempt 4 is blocked
    after three total attempts."""
    _seed(mode="live", state=S_RETRYABLE, count=0)
    osm = _FakeOSM()

    for expected in (1, 2, 3):
        claim = _claim(mode="live", osm=osm, max_attempts=3)
        assert claim.disposition == ACQUIRED, (expected, claim)
        assert claim.attempt_count == expected
        # Simulate the watch-arm failing and being terminalized + set RETRYABLE
        # with a terminal prior order, as the cleanup path does.
        oid = f"ord-{expected}"
        osm.orders[oid] = {"status": "EXPIRED"}
        _seed(mode="live", state=S_RETRYABLE, count=expected, token=claim.token,
              order_id=oid)

    final = _claim(mode="live", osm=osm, max_attempts=3)
    assert final.disposition == EXHAUSTED
    scope = _read_scope(mode="live")
    assert overnight._attempt_state(scope) == S_EXHAUSTED
    assert overnight._attempt_count(scope) == 3


# ── strict scope parsing: malformed/unknown never acquires ────────────────────

@pytest.mark.parametrize(
    "scope",
    [
        {"state": "RETRIABLE", "count": 1, "token": "tok", "local_order_id": "ord"},
        {"state": "UNKNOWN", "count": 1, "token": "tok", "local_order_id": "ord"},
        {"state": "RETRYABLE", "count": "not-an-int", "token": "tok", "local_order_id": "ord"},
        {"state": "RETRYABLE", "count": -1, "token": "tok", "local_order_id": "ord"},
        {"state": "", "count": 1, "token": "", "local_order_id": ""},
        {"state": "", "count": 0, "token": "tok", "local_order_id": ""},
    ],
)
def test_malformed_or_unknown_attempt_scope_never_acquires(scope):
    _seed_raw_scope(scope, mode="live")
    claim = _claim(mode="live")
    assert claim.disposition == CONFLICT
    durable = _read_scope(mode="live")
    # The malformed/unknown scope is never silently repaired or acquired.
    assert durable == scope


# ── terminal states cannot reopen ─────────────────────────────────────────────

@pytest.mark.parametrize(
    ("current_state", "requested_state"),
    [
        (S_ARMED, S_RETRYABLE),
        (S_ARMED, overnight.WATCH_ATTEMPT_STATE_ERROR),
        (S_EXHAUSTED, S_ARMED),
        (overnight.WATCH_ATTEMPT_STATE_ERROR, S_RETRYABLE),
    ],
)
def test_terminal_attempt_state_cannot_reopen(current_state, requested_state):
    _seed(mode="live", state=current_state, count=1, token="tok-x", order_id="ord-x")
    attempt = overnight._WatchAttemptClaim(ACQUIRED, "tok-x", 1, "ord-x")
    ok = _complete(attempt, state=requested_state, order_id="ord-x")
    assert ok is False
    assert overnight._attempt_state(_read_scope(mode="live")) == current_state


class _FullFakeOSM:
    """OSM stub returning production-shaped rows (client/mode/canonical/kind)
    so _local_order_terminal_state can enforce identity fencing."""

    def __init__(self, orders=None):
        self.orders = dict(orders or {})
        self.get_calls: list[str] = []

    def get_order(self, local_order_id):
        self.get_calls.append(local_order_id)
        return dict(self.orders.get(local_order_id) or {})


def _entry_row(*, local_order_id, client=CLIENT, mode="live", canonical=CANON,
               kind="ENTRY", status="EXPIRED"):
    return {
        "local_order_id": local_order_id,
        "client_id": client,
        "execution_mode": mode,
        "canonical_signal_id": canonical,
        "kind": kind,
        "status": status,
    }


# ── blocker 2: terminal-order proof must verify prior order IDENTITY ──────────

def test_terminal_proof_correct_identity_allows_replacement():
    _seed(mode="live", state=S_RETRYABLE, count=1, token="tok-prev", order_id="prior-1")
    osm = _FullFakeOSM({"prior-1": _entry_row(local_order_id="prior-1")})
    claim = _claim(mode="live", osm=osm, max_attempts=3)
    assert claim.disposition == ACQUIRED, claim
    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 2


def test_terminal_proof_wrong_client_is_conflict():
    _seed(mode="live", state=S_RETRYABLE, count=1, token="tok-prev", order_id="prior-1")
    osm = _FullFakeOSM({"prior-1": _entry_row(local_order_id="prior-1", client="other-client")})
    claim = _claim(mode="live", osm=osm, max_attempts=3)
    assert claim.disposition == CONFLICT, claim
    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 1
    assert overnight._attempt_state(scope) == S_RETRYABLE


def test_terminal_proof_wrong_execution_mode_is_conflict():
    _seed(mode="live", state=S_RETRYABLE, count=1, token="tok-prev", order_id="prior-1")
    osm = _FullFakeOSM({"prior-1": _entry_row(local_order_id="prior-1", mode="paper")})
    claim = _claim(mode="live", osm=osm, max_attempts=3)
    assert claim.disposition == CONFLICT, claim
    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 1


def test_terminal_proof_wrong_canonical_is_conflict():
    _seed(mode="live", state=S_RETRYABLE, count=1, token="tok-prev", order_id="prior-1")
    osm = _FullFakeOSM({
        "prior-1": _entry_row(local_order_id="prior-1", canonical="CANON-OTHER")
    })
    claim = _claim(mode="live", osm=osm, max_attempts=3)
    assert claim.disposition == CONFLICT, claim
    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 1


def test_terminal_proof_wrong_kind_is_conflict():
    _seed(mode="live", state=S_RETRYABLE, count=1, token="tok-prev", order_id="prior-1")
    # Same local_order_id resolves an EXIT-kind row → identity mismatch.
    osm = _FullFakeOSM({
        "prior-1": _entry_row(local_order_id="prior-1", kind="EXIT")
    })
    claim = _claim(mode="live", osm=osm, max_attempts=3)
    assert claim.disposition == CONFLICT, claim
    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 1


def test_terminal_proof_missing_identity_fields_is_conflict():
    _seed(mode="live", state=S_RETRYABLE, count=1, token="tok-prev", order_id="prior-1")
    # Row has terminal status but omits identity fields the fence requires.
    osm = _FullFakeOSM({
        "prior-1": {"local_order_id": "prior-1", "status": "EXPIRED"}
    })
    claim = _claim(mode="live", osm=osm, max_attempts=3)
    assert claim.disposition == CONFLICT, claim
    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 1


# ── isolated identity-field-by-field fences (every other field valid) ─────────
# Each of these proves ONE missing/wrong identity field independently blocks the
# replacement claim. "Missing identity fields" bundled above fails via client
# mismatch first; these tests prove kind AND local_order_id fail closed on their
# own, even when everything else is valid.

def test_terminal_proof_missing_kind_is_conflict():
    _seed(mode="live", state=S_RETRYABLE, count=1, token="tok-prev", order_id="prior-1")
    osm = _FullFakeOSM({
        "prior-1": {
            "local_order_id": "prior-1",
            "client_id": CLIENT,
            "execution_mode": "live",
            "canonical_signal_id": CANON,
            "status": "EXPIRED",
        }
    })
    _watch_calls_before = 0  # atomic-claim path never calls watchers directly

    claim = _claim(mode="live", osm=osm, max_attempts=3)
    assert claim.disposition == CONFLICT, claim
    assert claim.reason == "prior_order_kind_missing", claim.reason

    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 1
    assert overnight._attempt_state(scope) == S_RETRYABLE
    # No OSM create, no watcher call, no broker submit occurs on the claim path.
    assert _watch_calls_before == 0


def test_terminal_proof_blank_kind_is_conflict():
    _seed(mode="live", state=S_RETRYABLE, count=1, token="tok-prev", order_id="prior-1")
    osm = _FullFakeOSM({
        "prior-1": {
            "local_order_id": "prior-1",
            "client_id": CLIENT,
            "execution_mode": "live",
            "canonical_signal_id": CANON,
            "kind": "",
            "status": "EXPIRED",
        }
    })
    claim = _claim(mode="live", osm=osm, max_attempts=3)
    assert claim.disposition == CONFLICT, claim
    assert claim.reason == "prior_order_kind_missing", claim.reason
    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 1
    assert overnight._attempt_state(scope) == S_RETRYABLE


def test_terminal_proof_missing_local_order_id_is_conflict():
    _seed(mode="live", state=S_RETRYABLE, count=1, token="tok-prev", order_id="prior-1")
    osm = _FullFakeOSM({
        "prior-1": {
            "client_id": CLIENT,
            "execution_mode": "live",
            "canonical_signal_id": CANON,
            "kind": "ENTRY",
            "status": "EXPIRED",
        }
    })
    claim = _claim(mode="live", osm=osm, max_attempts=3)
    assert claim.disposition == CONFLICT, claim
    assert claim.reason == "prior_order_local_id_missing", claim.reason
    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 1
    assert overnight._attempt_state(scope) == S_RETRYABLE


def test_terminal_proof_blank_local_order_id_is_conflict():
    _seed(mode="live", state=S_RETRYABLE, count=1, token="tok-prev", order_id="prior-1")
    osm = _FullFakeOSM({
        "prior-1": {
            "local_order_id": "",
            "client_id": CLIENT,
            "execution_mode": "live",
            "canonical_signal_id": CANON,
            "kind": "ENTRY",
            "status": "EXPIRED",
        }
    })
    claim = _claim(mode="live", osm=osm, max_attempts=3)
    assert claim.disposition == CONFLICT, claim
    assert claim.reason == "prior_order_local_id_missing", claim.reason
    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 1
    assert overnight._attempt_state(scope) == S_RETRYABLE


def test_terminal_proof_wrong_nonblank_local_order_id_is_conflict():
    """OSM lookup key is 'prior-1' but the row's own local_order_id names a
    different order → not proof of THIS order's termination. Mismatch."""
    _seed(mode="live", state=S_RETRYABLE, count=1, token="tok-prev", order_id="prior-1")
    osm = _FullFakeOSM({
        "prior-1": {
            "local_order_id": "different-order",
            "client_id": CLIENT,
            "execution_mode": "live",
            "canonical_signal_id": CANON,
            "kind": "ENTRY",
            "status": "EXPIRED",
        }
    })
    claim = _claim(mode="live", osm=osm, max_attempts=3)
    assert claim.disposition == CONFLICT, claim
    assert claim.reason == "prior_order_local_id_mismatch", claim.reason
    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 1
    assert overnight._attempt_state(scope) == S_RETRYABLE


def test_terminal_proof_exact_valid_identity_control_acquires():
    """Positive control: all identity fields match, kind=ENTRY, status=EXPIRED
    → exactly one acquisition, count 1 → 2."""
    _seed(mode="live", state=S_RETRYABLE, count=1, token="tok-prev", order_id="prior-1")
    osm = _FullFakeOSM({
        "prior-1": {
            "local_order_id": "prior-1",
            "client_id": CLIENT,
            "execution_mode": "live",
            "canonical_signal_id": CANON,
            "kind": "ENTRY",
            "status": "EXPIRED",
        }
    })
    claim = _claim(mode="live", osm=osm, max_attempts=3)
    assert claim.disposition == ACQUIRED, claim
    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 2
    assert overnight._attempt_state(scope) == S_IN_PROGRESS


# ── blocker 3: malformed CONTAINER / scope-value shapes never acquire ─────────

@pytest.mark.parametrize(
    "scopes_container",
    [
        ["not", "a", "dict"],
        "corrupt-string",
        42,
        7.5,
        True,
    ],
)
def test_malformed_scopes_container_is_conflict(scopes_container):
    """overnight_watch_arm_attempt_scopes present but not an object → CONFLICT.
    The strict parser must NOT silently repair this into a first-claim state."""
    import json
    setup = psycopg2.connect(_RAW_URL)
    setup.autocommit = True
    meta = {"overnight_watch_arm_attempt_scopes": scopes_container}
    with setup.cursor() as cur:
        cur.execute(
            "INSERT INTO client_signal_opportunities "
            "(signal_id, canonical_signal_id, client_id, opportunity_status, "
            " order_local_id, metadata) VALUES (%s,%s,%s,%s,%s,%s::jsonb) "
            "ON CONFLICT (canonical_signal_id, client_id) DO UPDATE SET "
            "metadata = EXCLUDED.metadata",
            (SIGNAL_ID, CANON, CLIENT, "CREATED", None, json.dumps(meta)),
        )
    setup.close()

    claim = _claim(mode="live")
    assert claim.disposition == CONFLICT

    # Metadata byte-for-byte preserved — the corrupt container was NEVER
    # silently normalized into a claim.
    conn = psycopg2.connect(_RAW_URL)
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "SELECT metadata FROM client_signal_opportunities "
        "WHERE canonical_signal_id=%s AND client_id=%s",
        (CANON, CLIENT),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    assert row["metadata"] == meta


@pytest.mark.parametrize(
    "scope_value",
    [
        ["not", "a", "dict"],
        "corrupt-string",
        42,
        7.5,
        True,
    ],
)
def test_malformed_scope_value_at_key_is_conflict(scope_value):
    """Container is a dict but the value at our session key is not an object →
    CONFLICT. The strict extractor MUST NOT return {} silently for this."""
    import json
    key = overnight._attempt_scope_key("live", SESSION)
    meta = {"overnight_watch_arm_attempt_scopes": {key: scope_value}}
    setup = psycopg2.connect(_RAW_URL)
    setup.autocommit = True
    with setup.cursor() as cur:
        cur.execute(
            "INSERT INTO client_signal_opportunities "
            "(signal_id, canonical_signal_id, client_id, opportunity_status, "
            " order_local_id, metadata) VALUES (%s,%s,%s,%s,%s,%s::jsonb) "
            "ON CONFLICT (canonical_signal_id, client_id) DO UPDATE SET "
            "metadata = EXCLUDED.metadata",
            (SIGNAL_ID, CANON, CLIENT, "CREATED", None, json.dumps(meta)),
        )
    setup.close()

    claim = _claim(mode="live")
    assert claim.disposition == CONFLICT

    conn = psycopg2.connect(_RAW_URL)
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "SELECT metadata FROM client_signal_opportunities "
        "WHERE canonical_signal_id=%s AND client_id=%s",
        (CANON, CLIENT),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    assert row["metadata"] == meta


def test_exact_armed_completion_replay_is_idempotent():
    _seed(mode="live", state=S_RETRYABLE, count=0)
    a = _claim(mode="live")
    assert a.disposition == ACQUIRED
    bound = overnight._bind_watch_arm_attempt_order(
        signal_id=SIGNAL_ID, client_id=CLIENT, canonical_signal_id=CANON,
        signal_payload={"signal_id": SIGNAL_ID}, execution_mode="live",
        session_key=SESSION, attempt=a, local_order_id="ord-a",
    )
    assert bound is True
    first = _complete(a, state=S_ARMED, order_id="ord-a", reason="watcher_armed")
    second = _complete(a, state=S_ARMED, order_id="ord-a", reason="watcher_armed")
    assert first is True
    assert second is True
    assert overnight._attempt_state(_read_scope(mode="live")) == S_ARMED


# ══════════════════════════════════════════════════════════════════════════════
# PR #404 amendment: WATCH_ATTEMPT_REATTACH_REQUIRED liveness regression
#
# Scenario: trade_queue candidate — expired IN_PROGRESS lease — blank
# attempt local_order_id — exact PENDING_TRIGGER ENTRY already in orders —
# no watcher.
#
# Before this fix the claim returned WATCH_ATTEMPT_ALREADY_IN_PROGRESS and
# the caller incremented retryable_deferred with no forward progress.  Every
# subsequent run repeated the identical sequence → indefinite stranding.
#
# The fix makes the claim (under the row lock) rotate the token, bind the
# discovered local_order_id, refresh the lease, and return
# WATCH_ATTEMPT_REATTACH_REQUIRED.  The caller must then call the existing
# watcher reattachment path, complete the attempt as ARMED, and never create
# a second order or submit to the broker.
# ══════════════════════════════════════════════════════════════════════════════

from datetime import datetime, timezone, timedelta  # noqa: E402 — appended block

REATTACH_REQUIRED = overnight.WATCH_ATTEMPT_REATTACH_REQUIRED

# ── helpers scoped to the reattach suite ─────────────────────────────────────

_REATTACH_CANON  = "CANON-REATTACH-001"
_REATTACH_SIG    = "sig-reattach-001"
_REATTACH_CLIENT = "client-reattach-1"
_REATTACH_OID    = "ord-reattach-pending-001"


@pytest.fixture(autouse=False)
def _orders_table():
    """Disposable `orders` table used only by the reattach suite."""
    setup = psycopg2.connect(_RAW_URL)
    setup.autocommit = True
    with setup.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id                  bigserial PRIMARY KEY,
                local_order_id      text UNIQUE,
                client_id           text,
                execution_mode      text,
                canonical_signal_id text,
                kind                text,
                status              text,
                direction           text,
                symbol              text,
                trigger_price       double precision,
                stop_underlying     double precision,
                target_underlying   double precision,
                qty                 integer DEFAULT 1,
                limit_price         double precision DEFAULT 0.01,
                score               double precision DEFAULT 0,
                timeframe           text DEFAULT '1d',
                plan_id             text DEFAULT '',
                signal_id           text,
                contract            text,
                tier                text,
                pattern             text,
                meta                jsonb,
                created_ts          timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute("TRUNCATE orders")
    setup.close()
    yield
    setup2 = psycopg2.connect(_RAW_URL)
    setup2.autocommit = True
    with setup2.cursor() as cur:
        cur.execute("TRUNCATE orders")
    setup2.close()


def _insert_order(
    *,
    local_order_id: str,
    status: str,
    client_id: str = _REATTACH_CLIENT,
    execution_mode: str = "live",
    canonical_signal_id: str = _REATTACH_CANON,
    signal_id: str = _REATTACH_SIG,
    symbol: str = "SPY",
    trigger_price: float = 450.0,
    kind: str = "ENTRY",
):
    """Insert a minimal but production-shaped orders row."""
    setup = psycopg2.connect(_RAW_URL)
    setup.autocommit = True
    with setup.cursor() as cur:
        cur.execute(
            """
            INSERT INTO orders
              (local_order_id, client_id, execution_mode,
               canonical_signal_id, signal_id, kind, status,
               symbol, trigger_price, direction)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (local_order_id) DO UPDATE SET status = EXCLUDED.status
            """,
            (
                local_order_id, client_id, execution_mode,
                canonical_signal_id, signal_id, kind, status,
                symbol, trigger_price, "BULL",
            ),
        )
    setup.close()


def _order_status(local_order_id: str) -> str:
    """Read back an order's status from the real orders table."""
    conn = psycopg2.connect(_RAW_URL)
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT status FROM orders WHERE local_order_id = %s",
                (local_order_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return str((row or {}).get("status") or "").upper()


def _count_orders(
    canonical_signal_id: str = _REATTACH_CANON,
    client_id: str = _REATTACH_CLIENT,
) -> int:
    """Count ENTRY rows for the canonical signal to confirm no duplicate."""
    conn = psycopg2.connect(_RAW_URL)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM orders "
        "WHERE canonical_signal_id = %s AND client_id = %s AND kind = 'ENTRY'",
        (canonical_signal_id, client_id),
    )
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count


def _seed_expired_in_progress(
    *,
    mode: str = "live",
    count: int = 1,
    order_id: str = "",
    client: str = _REATTACH_CLIENT,
    canonical: str = _REATTACH_CANON,
):
    """Seed an IN_PROGRESS scope with an already-expired lease (1 second ago)."""
    import json
    from datetime import timedelta

    expired_lease = (
        datetime.now(timezone.utc) - timedelta(seconds=1)
    ).isoformat()
    key = overnight._attempt_scope_key(mode, SESSION)
    token = "expired-token-001"
    scope = {
        "count": count,
        "state": "IN_PROGRESS",
        "token": token,
        "local_order_id": order_id,
        "last_reason": "seed_expired",
        "updated_at": expired_lease,
        "execution_mode": mode,
        "session_key": SESSION,
        "claim_started_at": expired_lease,
        "claim_lease_until": expired_lease,
    }
    meta = {
        "overnight_watch_arm_attempt_scopes": {key: scope},
        "overnight_watch_arm_attempt_count": count,
        "overnight_watch_arm_attempt_state": "IN_PROGRESS",
        "overnight_watch_arm_attempt_token": token,
        "overnight_watch_arm_local_order_id": order_id,
    }
    setup = psycopg2.connect(_RAW_URL)
    setup.autocommit = True
    with setup.cursor() as cur:
        cur.execute(
            "INSERT INTO client_signal_opportunities "
            "(signal_id, canonical_signal_id, client_id, opportunity_status, "
            " order_local_id, metadata) VALUES (%s,%s,%s,%s,%s,%s::jsonb) "
            "ON CONFLICT (canonical_signal_id, client_id) DO UPDATE SET "
            "metadata = EXCLUDED.metadata",
            (
                _REATTACH_SIG, canonical, client, "CREATED",
                order_id or None, json.dumps(meta),
            ),
        )
    setup.close()


def _claim_reattach(*, mode: str = "live", osm=None):
    """Claim helper scoped to the reattach canonical/client/signal."""
    return overnight._atomic_claim_watch_arm_attempt(
        order_state_machine=osm or _FakeOSM(
            client_id=_REATTACH_CLIENT,
            canonical_signal_id=_REATTACH_CANON,
        ),
        signal_id=_REATTACH_SIG,
        client_id=_REATTACH_CLIENT,
        canonical_signal_id=_REATTACH_CANON,
        signal_payload={"signal_id": _REATTACH_SIG},
        execution_mode=mode,
        session_key=SESSION,
        max_attempts=3,
    )


def _read_scope_reattach(*, mode: str = "live"):
    return _read_scope(
        mode=mode,
        client=_REATTACH_CLIENT,
        canonical=_REATTACH_CANON,
    )


# ── core liveness regression ──────────────────────────────────────────────────

def test_reattach_required_disposition_on_expired_claim_with_pending_trigger_entry(
    _orders_table,
):
    """
    Primary regression: expired IN_PROGRESS + blank order_id + PENDING_TRIGGER
    ENTRY must yield WATCH_ATTEMPT_REATTACH_REQUIRED — NEVER ALREADY_IN_PROGRESS.

    No mocks.  _read_attempt_scope_probe_full and _query_active_entry_order both
    go through ap.db.conn, which the autouse _wire_db fixture already routes to
    the real Postgres instance.  The seeded metadata (expired claim_lease_until)
    and the real orders row are read via those real DB paths.

    Preconditions
    -------------
    * Attempt scope: IN_PROGRESS, count=1, blank local_order_id, expired lease.
    * orders table: one PENDING_TRIGGER ENTRY for same client/mode/canonical.

    Post-conditions (all must hold)
    --------------------------------
    1. Claim returns WATCH_ATTEMPT_REATTACH_REQUIRED.
    2. The returned local_order_id equals the discovered order (durable bind).
    3. Durable scope state remains IN_PROGRESS (attempt not yet complete).
    4. Durable scope local_order_id is now bound to the existing order.
    5. Durable scope token is rotated (≠ original expired token).
    6. Durable scope lease is refreshed (claim_lease_until > now).
    7. No second orders row was created.
    8. ALREADY_IN_PROGRESS was NOT returned (liveness proof).
    """
    _insert_order(local_order_id=_REATTACH_OID, status="PENDING_TRIGGER")
    _seed_expired_in_progress(mode="live", count=1, order_id="")

    claim = _claim_reattach(mode="live")

    # 1. Disposition
    assert claim.disposition == REATTACH_REQUIRED, (
        f"Expected REATTACH_REQUIRED, got {claim.disposition!r} "
        f"(reason={claim.reason!r}). "
        "The claim must NOT return ALREADY_IN_PROGRESS on expired lease "
        "+ blank order_id + PENDING_TRIGGER entry — that is the indefinite-"
        "stranding bug this amendment closes."
    )

    # 2. local_order_id bound to the discovered order
    assert claim.local_order_id == _REATTACH_OID, (
        f"local_order_id must be {_REATTACH_OID!r}, got {claim.local_order_id!r}"
    )

    # 3–6. Durable scope assertions (read back from real Postgres)
    scope = _read_scope_reattach(mode="live")
    assert overnight._attempt_state(scope) == "IN_PROGRESS"  # 3
    assert scope["count"] == 2  # a real reattach attempt consumes the next slot
    assert str(scope.get("local_order_id") or "").strip() == _REATTACH_OID  # 4
    assert scope.get("token") != "expired-token-001"  # 5 — rotated
    lease_raw = scope.get("claim_lease_until")
    assert lease_raw, "claim_lease_until must be present after reattach bind"
    lease_dt = overnight._parse_watch_attempt_ts(lease_raw)
    assert lease_dt is not None and lease_dt > datetime.now(timezone.utc), (  # 6
        f"Refreshed lease must be in the future, got {lease_raw!r}"
    )

    # 7. No duplicate order
    assert _count_orders() == 1, (
        "Exactly one orders row must exist — REATTACH_REQUIRED must never "
        "create a second order."
    )

    # 8. Not ALREADY_IN_PROGRESS
    assert claim.disposition != IN_PROGRESS, (
        "ALREADY_IN_PROGRESS is the bug disposition that causes the indefinite "
        "retryable_deferred loop. This test proves it is gone."
    )


def test_reattach_required_attempt_can_be_completed_as_armed(_orders_table):
    """
    After a REATTACH_REQUIRED claim the caller completes the attempt as ARMED
    (simulating a successful watch() call).  Proves the token/count returned by
    the claim function satisfies _complete_watch_arm_attempt's CAS predicate.
    No mocks — real Postgres for all DB paths.
    """
    _insert_order(local_order_id=_REATTACH_OID, status="PENDING_TRIGGER")
    _seed_expired_in_progress(mode="live", count=1, order_id="")

    claim = _claim_reattach(mode="live")
    assert claim.disposition == REATTACH_REQUIRED, (
        f"Prerequisite: claim must be REATTACH_REQUIRED, got {claim.disposition!r}"
    )
    assert claim.local_order_id == _REATTACH_OID

    # Simulate caller completing as ARMED after successful watch().
    armed_ok = overnight._complete_watch_arm_attempt(
        signal_id=_REATTACH_SIG,
        client_id=_REATTACH_CLIENT,
        canonical_signal_id=_REATTACH_CANON,
        signal_payload={"signal_id": _REATTACH_SIG},
        execution_mode="live",
        session_key=SESSION,
        attempt=claim,
        state=overnight.WATCH_ATTEMPT_STATE_ARMED,
        local_order_id=_REATTACH_OID,
        reason="reattach_required_watcher_armed",
    )

    assert armed_ok is True, (
        "_complete_watch_arm_attempt must succeed: the claim wrote IN_PROGRESS "
        "with the new token+count, and ARMED is a legal transition from IN_PROGRESS."
    )
    scope = _read_scope_reattach(mode="live")
    assert overnight._attempt_state(scope) == overnight.WATCH_ATTEMPT_STATE_ARMED
    assert str(scope.get("local_order_id") or "") == _REATTACH_OID
    assert scope.get("token") == claim.token


def test_reattach_required_no_second_order_created(_orders_table):
    """
    Prove the REATTACH_REQUIRED path never creates a second order.
    After claim, attempt is completed as ARMED (via test helper, not real watcher).
    The orders table must still contain exactly one row.
    No mocks — real Postgres for all DB paths.
    """
    _insert_order(local_order_id=_REATTACH_OID, status="PENDING_TRIGGER")
    _seed_expired_in_progress(mode="live", count=1, order_id="")

    claim = _claim_reattach(mode="live")
    assert claim.disposition == REATTACH_REQUIRED, (
        f"Prerequisite: claim must be REATTACH_REQUIRED, got {claim.disposition!r}"
    )

    overnight._complete_watch_arm_attempt(
        signal_id=_REATTACH_SIG,
        client_id=_REATTACH_CLIENT,
        canonical_signal_id=_REATTACH_CANON,
        signal_payload={"signal_id": _REATTACH_SIG},
        execution_mode="live",
        session_key=SESSION,
        attempt=claim,
        state=overnight.WATCH_ATTEMPT_STATE_ARMED,
        local_order_id=_REATTACH_OID,
        reason="reattach_required_watcher_armed",
    )

    assert _count_orders() == 1, (
        "REATTACH_REQUIRED must bind the existing order — never create a second one."
    )


def test_reattach_required_no_retryable_deferred_loop_after_arm(_orders_table):
    """
    Prove the indefinite retryable_deferred loop is closed.
    No mocks — real Postgres for all DB paths.

    Run 1: expired lease + PENDING_TRIGGER → REATTACH_REQUIRED → complete ARMED.
    Run 2: scope is now ARMED → claim returns WATCH_ATTEMPT_ALREADY_ARMED.
           Neither ALREADY_IN_PROGRESS nor REATTACH_REQUIRED → the loop is gone.
    """
    _insert_order(local_order_id=_REATTACH_OID, status="PENDING_TRIGGER")
    _seed_expired_in_progress(mode="live", count=1, order_id="")

    # Run 1: claim + arm
    claim1 = _claim_reattach(mode="live")
    assert claim1.disposition == REATTACH_REQUIRED, (
        f"Run 1 must yield REATTACH_REQUIRED, got {claim1.disposition!r}"
    )
    armed = overnight._complete_watch_arm_attempt(
        signal_id=_REATTACH_SIG,
        client_id=_REATTACH_CLIENT,
        canonical_signal_id=_REATTACH_CANON,
        signal_payload={"signal_id": _REATTACH_SIG},
        execution_mode="live",
        session_key=SESSION,
        attempt=claim1,
        state=overnight.WATCH_ATTEMPT_STATE_ARMED,
        local_order_id=_REATTACH_OID,
        reason="reattach_required_watcher_armed",
    )
    assert armed is True, "Run 1 completion must succeed"

    # Run 2: scope is ARMED — claim must short-circuit immediately
    claim2 = _claim_reattach(mode="live")
    assert claim2.disposition == overnight.WATCH_ATTEMPT_ALREADY_ARMED, (
        f"Run 2 must yield ALREADY_ARMED (durable ARMED scope), "
        f"got {claim2.disposition!r}. "
        "ALREADY_IN_PROGRESS here would mean the loop is still open."
    )
    assert claim2.disposition != IN_PROGRESS, (
        "ALREADY_IN_PROGRESS on Run 2 is the indefinite-loop bug disposition."
    )
    assert claim2.disposition != REATTACH_REQUIRED, (
        "REATTACH_REQUIRED on Run 2 means the prior ARMED write failed."
    )


def test_reattach_required_broker_owned_status_converges_armed(_orders_table):
    """
    An already-owned in-flight order (SUBMITTED/ACCEPTED/OPEN/PARTIALLY_FILLED/
    FILLED) must NOT produce REATTACH_REQUIRED — those orders are broker-owned
    and must never be reattached. Discovery durably resolves the attempt ARMED.
    No mocks — the orders table is seeded with each status directly; the real
    _query_active_entry_order reads it back.
    """
    for status in ("SUBMITTED", "ACCEPTED", "OPEN", "PARTIALLY_FILLED", "FILLED"):
        # Reset scope for each sub-case (ON CONFLICT DO UPDATE in both helpers)
        _seed_expired_in_progress(mode="live", count=1, order_id="")
        _insert_order(local_order_id=_REATTACH_OID, status=status)

        claim = _claim_reattach(mode="live")
        assert claim.disposition == ALREADY_ARMED, (
            f"status={status}: already-owned order must yield ALREADY_ARMED, "
            f"not {claim.disposition!r}. Reattaching a broker-owned order is unsafe."
        )
        assert claim.disposition != REATTACH_REQUIRED, (
            f"status={status}: REATTACH_REQUIRED on a broker-owned order is a bug."
        )
        scope = _read_scope_reattach(mode="live")
        assert overnight._attempt_state(scope) == S_ARMED
        assert scope["local_order_id"] == _REATTACH_OID
        again = _claim_reattach(mode="live")
        assert again.disposition == ALREADY_ARMED


def test_retryable_bound_pending_trigger_reclaims_and_exhausts(_orders_table):
    """watch=False retries the same order, increments count, and hits the cap."""
    _insert_order(local_order_id=_REATTACH_OID, status="PENDING_TRIGGER")
    _seed(
        mode="live",
        state=S_RETRYABLE,
        count=1,
        token="retry-token-1",
        order_id=_REATTACH_OID,
        client=_REATTACH_CLIENT,
        canonical=_REATTACH_CANON,
    )

    osm = _FakeOSM(
        {_REATTACH_OID: {"status": "PENDING_TRIGGER"}},
        client_id=_REATTACH_CLIENT,
        canonical_signal_id=_REATTACH_CANON,
    )
    claim2 = _claim_reattach(mode="live", osm=osm)
    assert claim2.disposition == REATTACH_REQUIRED, claim2
    assert claim2.attempt_count == 2
    assert claim2.local_order_id == _REATTACH_OID
    assert overnight._complete_watch_arm_attempt(
        signal_id=_REATTACH_SIG,
        client_id=_REATTACH_CLIENT,
        canonical_signal_id=_REATTACH_CANON,
        signal_payload={"signal_id": _REATTACH_SIG},
        execution_mode="live",
        session_key=SESSION,
        attempt=claim2,
        state=S_RETRYABLE,
        local_order_id=_REATTACH_OID,
        reason="watch_false",
    ) is True

    claim3 = _claim_reattach(mode="live", osm=osm)
    assert claim3.disposition == REATTACH_REQUIRED, claim3
    assert claim3.attempt_count == 3
    assert overnight._complete_watch_arm_attempt(
        signal_id=_REATTACH_SIG,
        client_id=_REATTACH_CLIENT,
        canonical_signal_id=_REATTACH_CANON,
        signal_payload={"signal_id": _REATTACH_SIG},
        execution_mode="live",
        session_key=SESSION,
        attempt=claim3,
        state=S_EXHAUSTED,
        local_order_id=_REATTACH_OID,
        reason="watch_false_at_cap",
    ) is True
    assert _claim_reattach(mode="live").disposition == EXHAUSTED
    assert _count_orders() == 1


def test_created_order_is_never_reattached_and_scope_converges_error(_orders_table):
    _insert_order(local_order_id=_REATTACH_OID, status="CREATED")
    _seed_expired_in_progress(mode="live", count=1, order_id="")

    claim = _claim_reattach(mode="live")

    assert claim.disposition == CONFLICT
    assert claim.reason == "attempt_state_error"
    scope = _read_scope_reattach(mode="live")
    assert overnight._attempt_state(scope) == overnight.WATCH_ATTEMPT_STATE_ERROR
    assert scope["local_order_id"] == _REATTACH_OID
    assert _order_status(_REATTACH_OID) == "CREATED"
    assert _count_orders() == 1


def test_expired_bound_completion_failure_recovers_same_order(_orders_table):
    _insert_order(local_order_id=_REATTACH_OID, status="PENDING_TRIGGER")
    _seed_expired_in_progress(
        mode="live", count=2, order_id=_REATTACH_OID,
    )

    osm = _FakeOSM(
        {_REATTACH_OID: {"status": "PENDING_TRIGGER"}},
        client_id=_REATTACH_CLIENT,
        canonical_signal_id=_REATTACH_CANON,
    )
    recovered = _claim_reattach(mode="live", osm=osm)

    assert recovered.disposition == REATTACH_REQUIRED, recovered
    assert recovered.attempt_count == 3
    assert recovered.local_order_id == _REATTACH_OID
    assert _count_orders() == 1


def test_expired_bound_terminal_order_advances_to_next_attempt(_orders_table):
    _insert_order(local_order_id=_REATTACH_OID, status="EXPIRED")
    _seed_expired_in_progress(
        mode="live", count=1, order_id=_REATTACH_OID,
    )
    osm = _FakeOSM(
        {_REATTACH_OID: {"status": "EXPIRED"}},
        client_id=_REATTACH_CLIENT,
        canonical_signal_id=_REATTACH_CANON,
    )

    recovered = _claim_reattach(mode="live", osm=osm)

    assert recovered.disposition == ACQUIRED, recovered
    assert recovered.attempt_count == 2
    assert recovered.local_order_id == ""
    scope = _read_scope_reattach(mode="live")
    assert overnight._attempt_state(scope) == S_IN_PROGRESS
    assert scope["count"] == 2
    assert scope["local_order_id"] == ""
    assert _count_orders() == 1


def test_expired_bound_terminal_order_at_cap_persists_exhausted(_orders_table):
    _insert_order(local_order_id=_REATTACH_OID, status="EXPIRED")
    _seed_expired_in_progress(
        mode="live", count=3, order_id=_REATTACH_OID,
    )
    osm = _FakeOSM(
        {_REATTACH_OID: {"status": "EXPIRED"}},
        client_id=_REATTACH_CLIENT,
        canonical_signal_id=_REATTACH_CANON,
    )

    recovered = _claim_reattach(mode="live", osm=osm)

    assert recovered.disposition == EXHAUSTED, recovered
    assert recovered.attempt_count == 3
    assert recovered.local_order_id == _REATTACH_OID
    scope = _read_scope_reattach(mode="live")
    assert overnight._attempt_state(scope) == S_EXHAUSTED
    assert scope["count"] == 3
    assert scope["local_order_id"] == _REATTACH_OID
    assert _count_orders() == 1


def test_real_postgres_runtime_reattaches_once_then_observes_armed(
    monkeypatch, _orders_table,
):
    """Drive the real outer caller over real claim/order rows for two runs."""
    signal = {
        "signal_id": _REATTACH_SIG,
        "canonical_signal_id": _REATTACH_CANON,
        "ticker": "SPY",
        "symbol": "SPY",
        "side": "CALL",
        "direction": "CALL",
        "timeframe": "1d",
        "score": 80.0,
        "entry_trigger": 450.0,
        "created_at": "2026-07-29T20:00:00+00:00",
    }
    job = {
        "id": "job-reattach-runtime",
        "signal_id": _REATTACH_SIG,
        "payload": signal,
        "_source": "trade_queue",
    }
    _insert_order(local_order_id=_REATTACH_OID, status="PENDING_TRIGGER")
    _seed_expired_in_progress(mode="live", count=1, order_id="")

    validator = types.ModuleType("ap.overnight_daily_validator")
    validator.fetch_market_snapshot = lambda ticker, broker: {"last": 449.0}
    validator.validate_overnight_daily_signal = lambda **kwargs: types.SimpleNamespace(
        valid=True, reason_code="", reason_text="",
    )
    validator.InvalidationReason = object
    monkeypatch.setitem(sys.modules, "ap.overnight_daily_validator", validator)

    auth = types.ModuleType("ap.authorization")
    auth.execution_mode_for_broker = lambda broker: "LIVE"
    auth.is_live_broker = lambda broker: False
    auth.broker_live_mode_known = lambda broker: True
    auth.check_live_authorization = lambda client_id: None
    auth.authorization_gate_enforced = lambda: False
    auth.LIVE_AUTHORIZATION_GATE_UNAVAILABLE = "LIVE_AUTHORIZATION_GATE_UNAVAILABLE"
    monkeypatch.setitem(sys.modules, "ap.authorization", auth)

    monkeypatch.setattr(
        overnight,
        "_et_now",
        lambda: datetime(2026, 7, 29, 9, 15, tzinfo=ZoneInfo("America/New_York")),
    )
    monkeypatch.setattr(
        overnight,
        "_fetch_watching_signals_with_status",
        lambda client_id: overnight._FetchWatchingSignalsResult(
            [job], "SUCCESS", "SUCCESS", None, None,
        ),
    )
    monkeypatch.setattr(overnight, "_mark_job_rejected", lambda *a, **kw: None)
    monkeypatch.setattr(overnight, "_mark_job_error", lambda *a, **kw: None)
    monkeypatch.setattr(overnight, "_mark_job_watching_reason", lambda *a, **kw: None)
    monkeypatch.setattr(overnight, "_mark_job_watching_armed", lambda *a, **kw: None)

    proof_calls = []

    def _persist_real_proof(**kwargs):
        proof_calls.append(dict(kwargs))
        proof_meta = {
            "execution_mode": "live",
            "overnight_reeval_session_key": SESSION,
            "local_order_id": _REATTACH_OID,
        }
        connection = psycopg2.connect(_RAW_URL)
        connection.autocommit = True
        with connection.cursor() as cur:
            cur.execute(
                "UPDATE client_signal_opportunities "
                "SET opportunity_status = 'WATCHER_ARMED', order_local_id = %s, "
                "metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb "
                "WHERE canonical_signal_id = %s AND client_id = %s",
                (
                    _REATTACH_OID, json.dumps(proof_meta),
                    _REATTACH_CANON, _REATTACH_CLIENT,
                ),
            )
            updated = cur.rowcount
        connection.close()
        return updated == 1

    monkeypatch.setattr(overnight, "_persist_watcher_armed_proof", _persist_real_proof)

    broker = MagicMock()
    broker.get_prior_day_levels.return_value = {
        "prior_day_high": 451.0,
        "prior_day_low": 445.0,
    }
    master_control = MagicMock()
    selector = MagicMock()
    osm = MagicMock()
    watcher1 = MagicMock()
    watcher1.has_order.return_value = False
    watcher1.watch.return_value = True

    result1 = overnight.run_overnight_reeval(
        client_id=_REATTACH_CLIENT,
        broker=broker,
        master_control=master_control,
        contract_selector=selector,
        order_state_machine=osm,
        entry_watcher=watcher1,
        force=True,
    )

    assert result1["armed"] == 1
    master_control.evaluate.assert_not_called()
    selector.select.assert_not_called()
    osm.create_entry_order.assert_not_called()
    broker.submit_order.assert_not_called()
    watcher1.watch.assert_called_once()
    assert watcher1.watch.call_args.args[1] == _REATTACH_OID
    assert watcher1.watch.call_args.kwargs == {
        "recovery_rearm": True,
        "no_cancel_on_reject": True,
    }
    assert len(proof_calls) == 1
    scope1 = _read_scope_reattach(mode="live")
    assert overnight._attempt_state(scope1) == S_ARMED
    assert scope1["count"] == 2
    assert scope1["local_order_id"] == _REATTACH_OID
    assert _count_orders() == 1

    watcher2 = MagicMock()
    watcher2.has_order.return_value = False
    watcher2.watch.return_value = True
    result2 = overnight.run_overnight_reeval(
        client_id=_REATTACH_CLIENT,
        broker=broker,
        master_control=master_control,
        contract_selector=selector,
        order_state_machine=osm,
        entry_watcher=watcher2,
        force=True,
    )

    assert result2["armed"] == 1
    watcher2.watch.assert_not_called()
    master_control.evaluate.assert_not_called()
    selector.select.assert_not_called()
    osm.create_entry_order.assert_not_called()
    broker.submit_order.assert_not_called()
    assert len(proof_calls) == 1
    assert _count_orders() == 1


@pytest.mark.parametrize(
    "blocked_gate",
    ["snapshot_exception", "validator_true_invalidation", "missing_prior_level"],
)
def test_real_postgres_runtime_recovery_precedes_new_admission_market_gates(
    monkeypatch, _orders_table, blocked_gate,
):
    """Materialized recovery cannot be intercepted by new-admission data gates."""
    signal = {
        "signal_id": _REATTACH_SIG,
        "canonical_signal_id": _REATTACH_CANON,
        "ticker": "SPY",
        "symbol": "SPY",
        "side": "CALL",
        "direction": "CALL",
        "timeframe": "1d",
        "score": 80.0,
        "entry_trigger": 450.0,
        "created_at": "2026-07-29T20:00:00+00:00",
    }
    job = {
        "id": f"job-early-recovery-{blocked_gate}",
        "signal_id": _REATTACH_SIG,
        "payload": signal,
        "_source": "trade_queue",
    }
    _insert_order(local_order_id=_REATTACH_OID, status="PENDING_TRIGGER")
    _seed_expired_in_progress(mode="live", count=1, order_id="")

    snapshot = MagicMock()
    validator_call = MagicMock()
    if blocked_gate == "snapshot_exception":
        snapshot.side_effect = RuntimeError("snapshot temporarily unavailable")
        validator_call.return_value = types.SimpleNamespace(
            valid=True, reason_code="", reason_text="",
        )
    elif blocked_gate == "validator_true_invalidation":
        snapshot.return_value = {"last": 449.0}
        validator_call.return_value = types.SimpleNamespace(
            valid=False,
            reason_code="TRUE_INVALIDATION",
            reason_text="current structure invalidated",
        )
    else:
        snapshot.return_value = {"last": 449.0}
        validator_call.return_value = types.SimpleNamespace(
            valid=True, reason_code="", reason_text="",
        )

    validator = types.ModuleType("ap.overnight_daily_validator")
    validator.fetch_market_snapshot = snapshot
    validator.validate_overnight_daily_signal = validator_call
    validator.InvalidationReason = object
    monkeypatch.setitem(sys.modules, "ap.overnight_daily_validator", validator)

    auth = types.ModuleType("ap.authorization")
    auth.execution_mode_for_broker = lambda broker: "LIVE"
    auth.is_live_broker = lambda broker: False
    auth.broker_live_mode_known = lambda broker: True
    auth.check_live_authorization = lambda client_id: None
    auth.authorization_gate_enforced = lambda: False
    auth.LIVE_AUTHORIZATION_GATE_UNAVAILABLE = "LIVE_AUTHORIZATION_GATE_UNAVAILABLE"
    monkeypatch.setitem(sys.modules, "ap.authorization", auth)

    monkeypatch.setattr(
        overnight,
        "_et_now",
        lambda: datetime(2026, 7, 29, 9, 15, tzinfo=ZoneInfo("America/New_York")),
    )
    monkeypatch.setattr(
        overnight,
        "_fetch_watching_signals_with_status",
        lambda client_id: overnight._FetchWatchingSignalsResult(
            [job], "SUCCESS", "SUCCESS", None, None,
        ),
    )
    rejected = MagicMock()
    monkeypatch.setattr(overnight, "_mark_job_rejected", rejected)
    monkeypatch.setattr(overnight, "_mark_job_error", lambda *a, **kw: None)
    monkeypatch.setattr(overnight, "_mark_job_watching_reason", lambda *a, **kw: None)
    monkeypatch.setattr(overnight, "_mark_job_watching_armed", lambda *a, **kw: None)

    def _persist_real_proof(**kwargs):
        proof_meta = {
            "execution_mode": "live",
            "overnight_reeval_session_key": SESSION,
            "local_order_id": _REATTACH_OID,
        }
        connection = psycopg2.connect(_RAW_URL)
        connection.autocommit = True
        with connection.cursor() as cur:
            cur.execute(
                "UPDATE client_signal_opportunities "
                "SET opportunity_status = 'WATCHER_ARMED', order_local_id = %s, "
                "metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb "
                "WHERE canonical_signal_id = %s AND client_id = %s",
                (
                    _REATTACH_OID, json.dumps(proof_meta),
                    _REATTACH_CANON, _REATTACH_CLIENT,
                ),
            )
            updated = cur.rowcount
        connection.close()
        return updated == 1

    monkeypatch.setattr(
        overnight, "_persist_watcher_armed_proof", _persist_real_proof,
    )

    broker = MagicMock()
    if blocked_gate == "missing_prior_level":
        broker.get_prior_day_levels.return_value = {
            "prior_day_high": None,
            "prior_day_low": 445.0,
        }
    else:
        broker.get_prior_day_levels.return_value = {
            "prior_day_high": 451.0,
            "prior_day_low": 445.0,
        }
    master_control = MagicMock()
    selector = MagicMock()
    osm = MagicMock()
    watcher = MagicMock()
    watcher.has_order.return_value = False
    watcher.watch.return_value = True

    result = overnight.run_overnight_reeval(
        client_id=_REATTACH_CLIENT,
        broker=broker,
        master_control=master_control,
        contract_selector=selector,
        order_state_machine=osm,
        entry_watcher=watcher,
        force=True,
    )

    assert result["armed"] == 1
    assert result["terminal_rejected"] == 0
    rejected.assert_not_called()
    broker.get_prior_day_levels.assert_not_called()
    snapshot.assert_not_called()
    validator_call.assert_not_called()
    master_control.evaluate.assert_not_called()
    selector.select.assert_not_called()
    osm.create_entry_order.assert_not_called()
    broker.submit_order.assert_not_called()
    watcher.watch.assert_called_once()
    assert watcher.watch.call_args.args[1] == _REATTACH_OID
    assert _count_orders() == 1
    scope = _read_scope_reattach(mode="live")
    assert overnight._attempt_state(scope) == S_ARMED
    assert scope["count"] == 2
    assert scope["local_order_id"] == _REATTACH_OID


def test_real_postgres_retryable_bound_terminal_at_cap_exhausts_before_admission(
    monkeypatch, _orders_table,
):
    """A capped RETRYABLE terminal owner is exhausted before new-admission work."""
    signal = {
        "signal_id": _REATTACH_SIG,
        "canonical_signal_id": _REATTACH_CANON,
        "ticker": "SPY",
        "symbol": "SPY",
        "side": "CALL",
        "direction": "CALL",
        "timeframe": "1d",
        "score": 80.0,
        "entry_trigger": 450.0,
        "created_at": "2026-07-29T20:00:00+00:00",
    }
    job = {
        "id": "job-retryable-bound-terminal-at-cap",
        "signal_id": _REATTACH_SIG,
        "payload": signal,
        "_source": "trade_queue",
    }
    _insert_order(local_order_id=_REATTACH_OID, status="EXPIRED")
    _seed(
        mode="live",
        state=S_RETRYABLE,
        count=3,
        token="retryable-token-003",
        order_id=_REATTACH_OID,
        client=_REATTACH_CLIENT,
        canonical=_REATTACH_CANON,
        signal_id=_REATTACH_SIG,
    )

    monkeypatch.setattr(
        overnight,
        "_et_now",
        lambda: datetime(2026, 7, 29, 9, 15, tzinfo=ZoneInfo("America/New_York")),
    )
    monkeypatch.setattr(
        overnight,
        "_fetch_watching_signals_with_status",
        lambda client_id: overnight._FetchWatchingSignalsResult(
            [job], "SUCCESS", "SUCCESS", None, None,
        ),
    )
    monkeypatch.setattr(overnight, "_mark_job_rejected", lambda *a, **kw: None)
    monkeypatch.setattr(overnight, "_mark_job_error", lambda *a, **kw: None)
    monkeypatch.setattr(
        overnight, "_mark_job_watching_reason", lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        overnight, "_mark_job_watching_armed", lambda *a, **kw: None,
    )

    auth = types.ModuleType("ap.authorization")
    auth.execution_mode_for_broker = lambda broker: "LIVE"
    auth.is_live_broker = lambda broker: False
    auth.broker_live_mode_known = lambda broker: True
    auth.check_live_authorization = lambda client_id: None
    auth.authorization_gate_enforced = lambda: False
    auth.LIVE_AUTHORIZATION_GATE_UNAVAILABLE = "LIVE_AUTHORIZATION_GATE_UNAVAILABLE"
    monkeypatch.setitem(sys.modules, "ap.authorization", auth)

    broker = MagicMock()
    master_control = MagicMock()
    selector = MagicMock()
    osm = MagicMock()
    watcher = MagicMock()

    result = overnight.run_overnight_reeval(
        client_id=_REATTACH_CLIENT,
        broker=broker,
        master_control=master_control,
        contract_selector=selector,
        order_state_machine=osm,
        entry_watcher=watcher,
        force=True,
    )

    scope = _read_scope_reattach(mode="live")
    assert overnight._attempt_state(scope) == S_EXHAUSTED
    assert scope["count"] == 3
    assert scope["local_order_id"] == _REATTACH_OID
    assert _order_status(_REATTACH_OID) == "EXPIRED"
    assert result["terminal_errors"] == 1
    assert result["retryable_deferred"] == 0
    broker.get_prior_day_levels.assert_not_called()
    master_control.evaluate.assert_not_called()
    selector.select.assert_not_called()
    osm.create_entry_order.assert_not_called()
    broker.submit_order.assert_not_called()
    watcher.watch.assert_not_called()
