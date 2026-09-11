"""P0 tests for the atomic attempt-counter-mirror advance in
``APOrderStateMachine.claim_deferred_materialization``.

PR: fix/p0-attempt-mirror-atomic-advance-20260827

Binding invariant (per amendment):
    ``retry_attempt``, ``breach_attempt_count``, and
    ``materialization_attempts`` are one durable selector-attempt identity.
    A legitimate claim from attempt N to N+1 MUST advance all three atomically;
    any pre-existing split (2/1/1, 3/2/2, etc.) MUST fail closed.

Rows are exercised via the real ``claim_deferred_materialization`` writer
against an isolated PostgreSQL schema — no synthetic bypass of the CAS.

Environment:
  INTELLIGENCE_POSTGRES_TEST_URL — required to run. GitHub Actions sets this
  via the p0_regression workflow's postgres service. Local skip when absent.
"""
from __future__ import annotations

import json as _json
import os
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest

# The OSM module imports psycopg2 lazily but the ap.db bootstrap needs
# DATABASE_URL set to something valid before the import — the test URL works.
os.environ.setdefault(
    "DATABASE_URL",
    os.environ.get("INTELLIGENCE_POSTGRES_TEST_URL", "postgresql://mock/mock"),
)


# ── PostgreSQL infrastructure helpers ────────────────────────────────────────

class _Wrapper:
    """psycopg2 cursor wrapper matching the shape ap/db.conn() returns."""

    def __init__(self, cursor):
        self.cursor = cursor

    @property
    def rowcount(self):
        return self.cursor.rowcount

    def execute(self, sql, params=()):
        self.cursor.execute(sql, params)
        return self

    def fetchone(self):
        row = self.cursor.fetchone()
        return dict(row) if row else None

    def fetchall(self):
        return [dict(r) for r in self.cursor.fetchall()]


def _require_postgres():
    database_url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")
    if not database_url:
        if os.getenv("GITHUB_ACTIONS") == "true":
            pytest.fail("INTELLIGENCE_POSTGRES_TEST_URL is required in GitHub Actions")
        pytest.skip("disposable PostgreSQL URL not configured")
    psycopg2 = pytest.importorskip("psycopg2")
    return psycopg2, database_url


@contextmanager
def _isolated_schema():
    psycopg2, database_url = _require_postgres()
    import psycopg2.extras

    schema = f"attempt_mirror_{uuid.uuid4().hex}"

    @contextmanager
    def _pg_conn():
        db = psycopg2.connect(database_url)
        cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(f'SET search_path TO "{schema}"')
            yield _Wrapper(cur)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            cur.close()
            db.close()

    admin = psycopg2.connect(database_url)
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(
                f"""
                CREATE TABLE "{schema}".orders (
                    local_order_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    execution_mode TEXT NOT NULL,
                    signal_id TEXT,
                    canonical_signal_id TEXT,
                    broker_order_id TEXT,
                    submitted_ts TIMESTAMPTZ,
                    meta JSONB,
                    updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        yield schema, _pg_conn
    finally:
        with admin.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def _seed_row(pg_conn, schema, *, local_order_id, client_id, signal_id,
              execution_mode, meta):
    with pg_conn() as c:
        c.execute(
            f'INSERT INTO "{schema}".orders '
            "(local_order_id, client_id, kind, status, execution_mode, "
            "signal_id, canonical_signal_id, meta) VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
            (local_order_id, client_id, "ENTRY", "PENDING_TRIGGER",
             execution_mode, signal_id,
             meta.get("canonical_signal_id") or signal_id,
             _json.dumps(meta)),
        )


def _read_row(pg_conn, schema, local_order_id):
    with pg_conn() as c:
        return c.execute(
            f'SELECT meta, broker_order_id, submitted_ts, execution_mode, '
            f'status FROM "{schema}".orders WHERE local_order_id = %s',
            (local_order_id,),
        ).fetchone()


def _mirror_tuple(meta):
    return (meta.get("retry_attempt"),
            meta.get("breach_attempt_count"),
            meta.get("materialization_attempts"))


def _base_seed(*, generation, signal_id, execution_mode, client_id,
               ra=None, bac=None, mats=None, lifecycle="RETRY_WAIT",
               mat_status="RETRY_PENDING", extra=None):
    m = {
        "lifecycle_state": lifecycle,
        "materialization_status": mat_status,
        "materialization_in_flight": False,
        "materialization_generation": generation,
        "materialization_owner": "",
        "materialization_lease_until": "",
        "broker_ready": False,
        "signal_id": signal_id,
        "execution_mode": execution_mode,
        "client_id": client_id,
    }
    if ra is not None:
        m["retry_attempt"] = ra
    if bac is not None:
        m["breach_attempt_count"] = bac
    if mats is not None:
        m["materialization_attempts"] = mats
    if extra:
        m.update(extra)
    return m


@contextmanager
def _route_osm_writes(pg_conn):
    from ap.order_state_machine import APOrderStateMachine
    import ap.order_state_machine as osm_mod
    original = osm_mod.conn
    osm_mod.conn = pg_conn
    try:
        yield APOrderStateMachine
    finally:
        osm_mod.conn = original


def _claim_kwargs(**overrides):
    now = datetime.now(timezone.utc)
    base = dict(
        owner="materializer:mirror-test",
        new_generation=2,
        lease_until=(now + timedelta(seconds=60)).isoformat(),
        trigger_crossed_at=now.isoformat(),
        trigger_price=130.0,
        observed_underlying_price=130.05,
        signal_id="",             # filled by caller
        execution_mode="paper",
        retry_attempt=2,
    )
    base.update(overrides)
    return base


# ─────────────────────────────────────────────────────────────────────────────
# 1. Positive canonical advances
# ─────────────────────────────────────────────────────────────────────────────

def test_01_positive_1_1_1_to_2_2_2_atomic_advance():
    """1/1/1 → claim attempt 2 → 2/2/2 in the same JSONB merge."""
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=sid, execution_mode="paper",
                  meta=_base_seed(generation=1, signal_id=sid,
                                  execution_mode="paper", client_id=cid,
                                  ra=1, bac=1, mats=1))
        with _route_osm_writes(pg_conn) as OSM:
            ok = OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=2, new_generation=2).items() if k != "signal_id"})
        row = _read_row(pg_conn, schema, loid)
        assert ok is True
        assert _mirror_tuple(row["meta"]) == (2, 2, 2)
        assert row["meta"]["materialization_generation"] == 2


def test_02_positive_2_2_2_to_3_3_3_atomic_advance():
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=sid, execution_mode="paper",
                  meta=_base_seed(generation=2, signal_id=sid,
                                  execution_mode="paper", client_id=cid,
                                  ra=2, bac=2, mats=2))
        with _route_osm_writes(pg_conn) as OSM:
            ok = OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=3, new_generation=3).items() if k != "signal_id"})
        row = _read_row(pg_conn, schema, loid)
        assert ok is True
        assert _mirror_tuple(row["meta"]) == (3, 3, 3)


# ─────────────────────────────────────────────────────────────────────────────
# 3–7. Negative controls: pre-existing splits stay fail-closed
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("shape", [
    (2, 1, 1),  # production incident A
    (3, 2, 2),  # production incident B
    (1, 2, 1),
    (1, 1, 2),
    (3, 3, 2),
    (2, 3, 2),
    (2, 2, 3),
])
def test_03_pre_existing_split_shapes_reject(shape):
    """Every single-field split rejects a new claim; row is not mutated."""
    ra_seed, bac_seed, mats_seed = shape
    generation = 1
    # Ask to claim next attempt = max(shape)+1 so predicate can never trivially agree
    next_attempt = max(shape) + 1
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        seed_meta = _base_seed(generation=generation, signal_id=sid,
                               execution_mode="paper", client_id=cid,
                               ra=ra_seed, bac=bac_seed, mats=mats_seed)
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=sid, execution_mode="paper", meta=seed_meta)
        with _route_osm_writes(pg_conn) as OSM:
            ok = OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=next_attempt,
                    new_generation=generation + 1).items() if k != "signal_id"})
        row = _read_row(pg_conn, schema, loid)
        assert ok is False
        # Row untouched
        assert _mirror_tuple(row["meta"]) == shape
        assert row["meta"]["materialization_generation"] == generation


@pytest.mark.parametrize("malformed", [
    "true", "false", "1.5", "1.0", "1e2", "-1", "  ", "abc", "1 2",
])
def test_04_malformed_durable_mirror_rejects_claim(malformed):
    """A mirror stored as a non-integer text form fails closed via regex."""
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        seed = _base_seed(generation=1, signal_id=sid, execution_mode="paper",
                          client_id=cid, ra=1, bac=1, mats=1)
        seed["breach_attempt_count"] = malformed  # inject bad text form
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=sid, execution_mode="paper", meta=seed)
        with _route_osm_writes(pg_conn) as OSM:
            ok = OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=2, new_generation=2).items() if k != "signal_id"})
        assert ok is False


@pytest.mark.parametrize("bad_arg", [True, False, "2", 1.5, -1, 0])
def test_05_python_arg_boolean_or_bad_type_rejects_before_sql(bad_arg):
    """Bool/negative/zero/non-int ``retry_attempt`` argument fails at Python."""
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=sid, execution_mode="paper",
                  meta=_base_seed(generation=1, signal_id=sid,
                                  execution_mode="paper", client_id=cid,
                                  ra=1, bac=1, mats=1))
        with _route_osm_writes(pg_conn) as OSM:
            ok = OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=bad_arg,
                    new_generation=2).items() if k != "signal_id"})
        row = _read_row(pg_conn, schema, loid)
        assert ok is False
        # Row must be untouched — bad arg cannot mutate anything
        assert _mirror_tuple(row["meta"]) == (1, 1, 1)


# ─────────────────────────────────────────────────────────────────────────────
# 8. First-attempt shapes
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("shape", [
    (None, None, None),  # all absent — pure first attempt
    (0, 0, 0),           # all explicit zeros
])
def test_08_first_attempt_shape_advances_to_1(shape):
    """Absent-or-zero prior state accepts claim attempt=1."""
    ra, bac, mats = shape
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=sid, execution_mode="paper",
                  meta=_base_seed(generation=0, signal_id=sid,
                                  execution_mode="paper", client_id=cid,
                                  ra=ra, bac=bac, mats=mats,
                                  lifecycle="", mat_status=""))
        with _route_osm_writes(pg_conn) as OSM:
            ok = OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=1,
                    new_generation=1).items() if k != "signal_id"})
        row = _read_row(pg_conn, schema, loid)
        assert ok is True
        assert _mirror_tuple(row["meta"]) == (1, 1, 1)


def test_08b_positive_prior_missing_mirror_shapes_reject():
    """AMENDMENT §5/§8: for positive prior attempts, missing-mirror shapes
    MUST fail closed. Caller-supplied ``retry_attempt`` cannot manufacture
    positive durable authority from insufficient prior durable proof.

    For prior attempt 1 (claim attempt 2), every combination where one or
    more mirrors is absent while at least one is positive rejects.
    """
    missing_mirror_shapes = [
        # (retry_attempt, breach_attempt_count, materialization_attempts)
        (1,    None, 1),      # § retry present, breach absent, mat present
        (1,    1,    None),   # § retry+breach present, mat absent
        (1,    None, None),   # § retry alone
        (None, 1,    None),   # § breach alone
        (None, None, 1),      # § materialization_attempts alone
        (None, 1,    1),      # § canonical retry_attempt absent, siblings positive
                              #   — legacy NULL/N/N exception NOT retained
    ]
    for ra, bac, mats in missing_mirror_shapes:
        with _isolated_schema() as (schema, pg_conn):
            cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
            seed = _base_seed(generation=1, signal_id=sid,
                              execution_mode="paper", client_id=cid,
                              ra=ra, bac=bac, mats=mats)
            _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                      signal_id=sid, execution_mode="paper", meta=seed)
            with _route_osm_writes(pg_conn) as OSM:
                ok = OSM(client_id=cid).claim_deferred_materialization(
                    loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                        signal_id=sid, retry_attempt=2,
                        new_generation=2).items() if k != "signal_id"})
            row = _read_row(pg_conn, schema, loid)
            m = row["meta"]
            assert ok is False, f"expected reject for {(ra, bac, mats)}, got claim=True"
            # Row untouched — zero mutation on reject
            assert (m.get("retry_attempt"), m.get("breach_attempt_count"),
                    m.get("materialization_attempts")) == (ra, bac, mats), \
                f"mutation observed on rejected {(ra, bac, mats)}"
            assert m["materialization_generation"] == 1
            assert m["materialization_owner"] == ""
            assert m["materialization_status"] == "RETRY_PENDING"
            assert m["lifecycle_state"] == "RETRY_WAIT"


def test_08c_positive_prior_all_null_with_positive_claim_rejects():
    """AMENDMENT §7: caller memory alone cannot manufacture durable authority.

    Durable NULL/NULL/NULL + claim attempt=2 MUST reject. This is the most
    permissive shape the old predicate accepted and the audit blocker names
    it explicitly.
    """
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        # Absent all three attempt mirrors, but a generation exists — a
        # positive retry request against a row with no prior attempt proof.
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=sid, execution_mode="paper",
                  meta=_base_seed(generation=1, signal_id=sid,
                                  execution_mode="paper", client_id=cid))
        with _route_osm_writes(pg_conn) as OSM:
            ok = OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=2,
                    new_generation=2).items() if k != "signal_id"})
        m = _read_row(pg_conn, schema, loid)["meta"]
        assert ok is False
        assert m.get("retry_attempt") is None
        assert m.get("breach_attempt_count") is None
        assert m.get("materialization_attempts") is None
        assert m["materialization_generation"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# 9. Concurrency — two workers, only one may win
# ─────────────────────────────────────────────────────────────────────────────

def test_09_two_workers_race_exactly_one_wins():
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=sid, execution_mode="paper",
                  meta=_base_seed(generation=1, signal_id=sid,
                                  execution_mode="paper", client_id=cid,
                                  ra=1, bac=1, mats=1))

        barrier = threading.Barrier(2)
        results = []
        errors = []

        def worker(name):
            try:
                # Each thread needs its own OSM with the pg_conn route.
                # We reuse the module-level conn override for the whole run.
                barrier.wait(timeout=10)
                from ap.order_state_machine import APOrderStateMachine
                results.append((name, APOrderStateMachine(client_id=cid)
                    .claim_deferred_materialization(
                        loid, signal_id=sid,
                        **{k: v for k, v in _claim_kwargs(
                            signal_id=sid, owner=f"worker-{name}",
                            retry_attempt=2, new_generation=2).items()
                           if k != "signal_id"})))
            except BaseException as exc:
                errors.append(exc)

        with _route_osm_writes(pg_conn):
            threads = [threading.Thread(target=worker, args=("A",)),
                       threading.Thread(target=worker, args=("B",))]
            for t in threads: t.start()
            for t in threads: t.join(timeout=15)

        assert not errors, errors
        assert len(results) == 2
        winners = [n for n, ok in results if ok]
        assert len(winners) == 1
        row = _read_row(pg_conn, schema, loid)
        assert _mirror_tuple(row["meta"]) == (2, 2, 2)
        assert row["meta"]["materialization_generation"] == 2
        assert row["meta"]["materialization_owner"] == f"worker-{winners[0]}"


# ─────────────────────────────────────────────────────────────────────────────
# 10–17. Fence integrity — pre-existing conditions block the claim
# ─────────────────────────────────────────────────────────────────────────────

def _fence_test(*, seed_overrides=None, kwarg_overrides=None,
                seed_generation=1, seed_ra=1, seed_bac=1, seed_mats=1):
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        seed = _base_seed(generation=seed_generation, signal_id=sid,
                          execution_mode="paper", client_id=cid,
                          ra=seed_ra, bac=seed_bac, mats=seed_mats)
        if seed_overrides:
            seed.update(seed_overrides)
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=sid, execution_mode="paper", meta=seed)
        kwargs = _claim_kwargs(signal_id=sid, retry_attempt=2, new_generation=2)
        if kwarg_overrides:
            kwargs.update(kwarg_overrides)
        with _route_osm_writes(pg_conn) as OSM:
            ok = OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid,
                **{k: v for k, v in kwargs.items() if k != "signal_id"})
        row = _read_row(pg_conn, schema, loid)
        return ok, row


def test_10_generation_mismatch_zero_mutation():
    # Row has G=5 already; we ask new_generation=2 → expected_prev=1 ≠ 5 → False
    ok, row = _fence_test(seed_generation=5)
    assert ok is False
    assert _mirror_tuple(row["meta"]) == (1, 1, 1)


def test_11_client_mismatch_no_mutation():
    with _isolated_schema() as (schema, pg_conn):
        loid, sid = f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        _seed_row(pg_conn, schema, local_order_id=loid,
                  client_id="owner@x.io", signal_id=sid, execution_mode="paper",
                  meta=_base_seed(generation=1, signal_id=sid,
                                  execution_mode="paper", client_id="owner@x.io",
                                  ra=1, bac=1, mats=1))
        with _route_osm_writes(pg_conn) as OSM:
            # Different client_id runner
            ok = OSM(client_id="intruder@x.io").claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=2,
                    new_generation=2).items() if k != "signal_id"})
        assert ok is False


def test_12_signal_mismatch_no_mutation():
    with _isolated_schema() as (schema, pg_conn):
        cid, loid = "c@x.io", f"oid-{uuid.uuid4().hex}"
        real_sid = f"sig-real-{uuid.uuid4().hex}"
        wrong_sid = f"sig-wrong-{uuid.uuid4().hex}"
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=real_sid, execution_mode="paper",
                  meta=_base_seed(generation=1, signal_id=real_sid,
                                  execution_mode="paper", client_id=cid,
                                  ra=1, bac=1, mats=1))
        with _route_osm_writes(pg_conn) as OSM:
            ok = OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=wrong_sid,
                **{k: v for k, v in _claim_kwargs(
                    signal_id=wrong_sid, retry_attempt=2,
                    new_generation=2).items() if k != "signal_id"})
        assert ok is False


def test_13_execution_mode_mismatch_no_mutation():
    ok, row = _fence_test(kwarg_overrides={"execution_mode": "live"})
    assert ok is False
    assert _mirror_tuple(row["meta"]) == (1, 1, 1)


def test_14_missing_execution_mode_fails_closed():
    ok, row = _fence_test(kwarg_overrides={"execution_mode": ""})
    assert ok is False


def test_15_broker_order_id_present_blocks_claim():
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        with pg_conn() as c:
            c.execute(
                f'INSERT INTO "{schema}".orders '
                "(local_order_id,client_id,kind,status,execution_mode,signal_id,"
                "canonical_signal_id,broker_order_id,meta) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
                (loid, cid, "ENTRY", "PENDING_TRIGGER", "paper", sid,
                 sid,
                 "already-submitted-bkr-1",
                 _json.dumps(_base_seed(generation=1, signal_id=sid,
                                        execution_mode="paper", client_id=cid,
                                        ra=1, bac=1, mats=1))))
        with _route_osm_writes(pg_conn) as OSM:
            ok = OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=2,
                    new_generation=2).items() if k != "signal_id"})
        assert ok is False


def test_16_submitted_ts_present_blocks_claim():
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        with pg_conn() as c:
            c.execute(
                f'INSERT INTO "{schema}".orders '
                "(local_order_id,client_id,kind,status,execution_mode,signal_id,"
                "canonical_signal_id,submitted_ts,meta) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW(),%s::jsonb)",
                (loid, cid, "ENTRY", "PENDING_TRIGGER", "paper", sid,
                 sid,
                 _json.dumps(_base_seed(generation=1, signal_id=sid,
                                        execution_mode="paper", client_id=cid,
                                        ra=1, bac=1, mats=1))))
        with _route_osm_writes(pg_conn) as OSM:
            ok = OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=2,
                    new_generation=2).items() if k != "signal_id"})
        assert ok is False


def test_17_broker_ready_true_blocks_claim():
    ok, row = _fence_test(seed_overrides={"broker_ready": True})
    assert ok is False
    assert _mirror_tuple(row["meta"]) == (1, 1, 1)


# ─────────────────────────────────────────────────────────────────────────────
# 18–19. Restart & crash coherence
# ─────────────────────────────────────────────────────────────────────────────

def test_18_restart_after_committed_claim_reads_coherent_row():
    """After a claim commits, a fresh OSM read sees 2/2/2 — no split visible."""
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=sid, execution_mode="paper",
                  meta=_base_seed(generation=1, signal_id=sid,
                                  execution_mode="paper", client_id=cid,
                                  ra=1, bac=1, mats=1))
        with _route_osm_writes(pg_conn) as OSM:
            OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=2,
                    new_generation=2).items() if k != "signal_id"})
        # Simulated restart: fresh read
        row = _read_row(pg_conn, schema, loid)
        m = row["meta"]
        assert _mirror_tuple(m) == (2, 2, 2)
        assert m["materialization_generation"] == 2
        assert m["materialization_owner"] == "materializer:mirror-test"
        assert m["materialization_status"] == "RUNNING"
        assert m["lifecycle_state"] == "MATERIALIZING"


def test_19_crash_after_claim_leaves_coherent_row():
    """The write is a single JSONB merge; PostgreSQL commits it atomically.
    There is no interleaving where retry_attempt=N is durable while the sibling
    mirrors remain at N-1. Verify by reading immediately after the claim.
    """
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=sid, execution_mode="paper",
                  meta=_base_seed(generation=1, signal_id=sid,
                                  execution_mode="paper", client_id=cid,
                                  ra=1, bac=1, mats=1))
        with _route_osm_writes(pg_conn) as OSM:
            OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=2,
                    new_generation=2).items() if k != "signal_id"})
        m = _read_row(pg_conn, schema, loid)["meta"]
        assert _mirror_tuple(m) == (2, 2, 2)
        # generation + owner + attempt mirrors + lifecycle all present together
        assert m["materialization_generation"] == 2
        assert m["materialization_owner"] == "materializer:mirror-test"
        assert m["lifecycle_state"] == "MATERIALIZING"
        assert m["materialization_status"] == "RUNNING"
        assert m["materialization_in_flight"] is True


# ─────────────────────────────────────────────────────────────────────────────
# 23. Retry backoff observation must NOT increment
# ─────────────────────────────────────────────────────────────────────────────

def test_23_retry_attempt_none_on_positive_mirror_row_now_rejects():
    """AMENDMENT (issue #535, post-#530): passing ``retry_attempt=None`` on
    a row with positive mirrors NOW rejects.

    Prior to #535, this shape was silently accepted — the CAS advanced
    generation without touching mirrors and the row was left in a
    contradictory state (initial-materialization owner claiming a row that
    already had attempt history). #535 closes that seam: the
    ``retry_attempt=None`` calling convention requires the durable row to
    be in a legitimate first-attempt shape (mirrors absent or 0/0/0).

    Historical note: an earlier version of this test asserted the opposite
    (claim=True + mirrors unchanged). That assertion encoded the pre-#535
    seam and has been superseded by tests 24-27, which exercise the full
    stale-shape rejection matrix.
    """
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=sid, execution_mode="paper",
                  meta=_base_seed(generation=0, signal_id=sid,
                                  execution_mode="paper", client_id=cid,
                                  ra=1, bac=1, mats=1,
                                  lifecycle="", mat_status=""))
        with _route_osm_writes(pg_conn) as OSM:
            ok = OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=None,
                    new_generation=1).items() if k != "signal_id"})
        m = _read_row(pg_conn, schema, loid)["meta"]
        assert ok is False
        # Mirrors + generation untouched on rejection
        assert _mirror_tuple(m) == (1, 1, 1)
        assert m["materialization_generation"] == 0


# ─────────────────────────────────────────────────────────────────────────────
# 24–26. Issue #535 — first-attempt-shape invariant on retry_attempt=None path
# ─────────────────────────────────────────────────────────────────────────────
#
# The retry_attempt=None calling convention exists for the initial-materialization
# path from ap_execution_core::_claim_deferred_materialization_for_trigger. That
# path expects the durable row to be in a legitimate first-attempt shape (mirrors
# absent or 0/0/0). PR #530 fenced the retry_attempt-provided path but left this
# path structurally unfenced. Issue #535 closes that seam.

@pytest.mark.parametrize("stale_shape", [
    (1, 1, 1),  # nonzero mirrors but "coherent" — still not first-attempt
    (2, 2, 2),  # exact stale-from-prior-retry shape
    (5, 5, 5),
    (1, 0, 0),  # split
    (0, 1, 0),  # split
    (0, 0, 1),  # split
    (2, 1, 1),  # exact production incident shape from #530
])
def test_24_retry_attempt_none_rejects_stale_positive_mirrors(stale_shape):
    """#535: caller passes no retry_attempt (initial-materialization path).

    A row with ANY positive mirror value must fail closed. The row must
    not have generation advanced, must not have owner written, must retain
    its original mirrors exactly.
    """
    ra_seed, bac_seed, mats_seed = stale_shape
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        seed = _base_seed(generation=1, signal_id=sid, execution_mode="paper",
                          client_id=cid, ra=ra_seed, bac=bac_seed, mats=mats_seed,
                          lifecycle="", mat_status="")
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=sid, execution_mode="paper", meta=seed)
        with _route_osm_writes(pg_conn) as OSM:
            # NOTE: retry_attempt kwarg is deliberately absent — this is the
            # ap_execution_core::_claim_deferred_materialization_for_trigger path.
            ok = OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=None,
                    new_generation=2).items() if k != "signal_id"})
        m = _read_row(pg_conn, schema, loid)["meta"]
        assert ok is False, f"expected reject for stale {stale_shape}, got claim=True"
        # Row untouched — zero mutation on reject
        assert _mirror_tuple(m) == stale_shape, \
            f"mirrors mutated on reject for {stale_shape}: {_mirror_tuple(m)}"
        assert m["materialization_generation"] == 1, "generation must not advance"
        assert m["materialization_owner"] == "", "owner must not be written"


def test_25_retry_attempt_none_accepts_fresh_row_absent_mirrors():
    """#535: fresh row with mirrors absent — retry_attempt=None claim succeeds.

    Regression guard: the first-attempt-shape predicate must not break the
    normal initial-materialization path for a genuinely fresh row.
    """
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        # No attempt mirrors at all — pristine fresh row
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=sid, execution_mode="paper",
                  meta=_base_seed(generation=0, signal_id=sid,
                                  execution_mode="paper", client_id=cid,
                                  lifecycle="", mat_status=""))
        with _route_osm_writes(pg_conn) as OSM:
            ok = OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=None,
                    new_generation=1).items() if k != "signal_id"})
        m = _read_row(pg_conn, schema, loid)["meta"]
        assert ok is True
        # Mirrors stay absent — retry_attempt=None means don't touch them
        assert m.get("retry_attempt") is None
        assert m.get("breach_attempt_count") is None
        assert m.get("materialization_attempts") is None
        # But generation, owner, lifecycle DID advance
        assert m["materialization_generation"] == 1
        assert m["materialization_owner"] == "materializer:mirror-test"
        assert m["materialization_status"] == "RUNNING"
        assert m["lifecycle_state"] == "MATERIALIZING"


def test_26_retry_attempt_none_accepts_explicit_zero_mirrors():
    """#535: fresh row with mirrors explicitly 0/0/0 — retry_attempt=None claim succeeds.

    Some code paths seed 0 explicitly rather than leaving mirrors absent.
    Both must be accepted as legitimate first-attempt shape.
    """
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=sid, execution_mode="paper",
                  meta=_base_seed(generation=0, signal_id=sid,
                                  execution_mode="paper", client_id=cid,
                                  ra=0, bac=0, mats=0,
                                  lifecycle="", mat_status=""))
        with _route_osm_writes(pg_conn) as OSM:
            ok = OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=None,
                    new_generation=1).items() if k != "signal_id"})
        m = _read_row(pg_conn, schema, loid)["meta"]
        assert ok is True
        # Mirrors stay at 0 — retry_attempt=None doesn't overwrite them
        assert m.get("retry_attempt") == 0
        assert m.get("breach_attempt_count") == 0
        assert m.get("materialization_attempts") == 0
        assert m["materialization_generation"] == 1


@pytest.mark.parametrize("malformed", ["true", "false", "1.5", "-1", "abc"])
def test_27_retry_attempt_none_rejects_malformed_durable_mirrors(malformed):
    """#535: caller passes no retry_attempt AND a mirror is malformed text.

    Fresh-row semantics require valid absence or 0; garbage text fails closed
    just like it does on the retry_attempt-provided path.
    """
    with _isolated_schema() as (schema, pg_conn):
        cid, loid, sid = "c@x.io", f"oid-{uuid.uuid4().hex}", f"sig-{uuid.uuid4().hex}"
        seed = _base_seed(generation=0, signal_id=sid, execution_mode="paper",
                          client_id=cid, ra=0, bac=0, mats=0,
                          lifecycle="", mat_status="")
        seed["breach_attempt_count"] = malformed  # inject bad text form
        _seed_row(pg_conn, schema, local_order_id=loid, client_id=cid,
                  signal_id=sid, execution_mode="paper", meta=seed)
        with _route_osm_writes(pg_conn) as OSM:
            ok = OSM(client_id=cid).claim_deferred_materialization(
                loid, signal_id=sid, **{k: v for k, v in _claim_kwargs(
                    signal_id=sid, retry_attempt=None,
                    new_generation=1).items() if k != "signal_id"})
        assert ok is False
