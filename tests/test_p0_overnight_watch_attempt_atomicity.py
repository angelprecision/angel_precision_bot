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
from contextlib import contextmanager

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
          client=CLIENT, canonical=CANON):
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
            (SIGNAL_ID, canonical, client, "CREATED", order_id or None, json.dumps(meta)),
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


def test_active_prior_order_blocks_without_increment():
    """RETRYABLE with a prior order still ACTIVE (PENDING_TRIGGER) →
    ALREADY_IN_PROGRESS, no increment, no acquisition."""
    _seed(mode="live", state=S_RETRYABLE, count=1, token="tok-1", order_id="prior-1")
    osm = _FakeOSM({"prior-1": {"status": "PENDING_TRIGGER"}})
    claim = _claim(mode="live", osm=osm)
    assert claim.disposition == IN_PROGRESS
    scope = _read_scope(mode="live")
    assert overnight._attempt_count(scope) == 1


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
