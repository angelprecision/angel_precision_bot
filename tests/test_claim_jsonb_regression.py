"""
tests/test_claim_jsonb_regression.py

Regression test for the JSONB ? operator / db wrapper bug.

WHAT THIS TESTS:
    _ConnWrapper.execute() previously did sql.replace("?", "%s"), which
    corrupted the Postgres JSONB key-existence operator (payload ? 'score')
    into (payload %s 'score'). psycopg2 then saw a phantom bind placeholder
    and raised IndexError: tuple index out of range on every claim attempt,
    silently killing the queue worker without executing any trades.

    This test exercises the claim path through the actual _ConnWrapper so
    the bug cannot be reintroduced without immediately failing CI.

REQUIRES:
    A Postgres connection string in env var DATABASE_URL.
    The trade_queue and clients tables must exist (run migrations first).
    Skipped automatically when DATABASE_URL is absent (unit-test environments).
"""
import json
import os
import uuid
import pytest

DATABASE_URL = os.getenv("DATABASE_URL", "")

pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="DATABASE_URL not set — skipping integration test",
)


@pytest.fixture()
def db_conn():
    """Real psycopg2 connection wrapped in _ConnWrapper."""
    import psycopg2
    import psycopg2.extras
    from ap.db import _ConnWrapper

    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = False
    cur = conn.cursor()
    wrapper = _ConnWrapper(conn, cur)
    yield conn, wrapper
    conn.rollback()   # leave DB unchanged after every test
    conn.close()


def _insert_queue_row(wrapper, client_id: str, score: float) -> str:
    """Insert a NEW trade_queue row and return its id."""
    job_id = str(uuid.uuid4())
    payload = json.dumps({"ticker": "SPY", "side": "CALL", "score": score})
    wrapper.execute(
        """
        INSERT INTO trade_queue (id, client_id, signal_id, status, payload, created_ts)
        VALUES (%s, %s, %s, 'NEW', %s::jsonb, NOW())
        """,
        (job_id, client_id, str(uuid.uuid4()), payload),
    )
    return job_id


# ── TEST 1: JSONB ? operator does not raise ──────────────────────────────────
def test_jsonb_operator_does_not_raise(db_conn):
    """
    The core regression: executing a query containing payload ? 'score'
    through _ConnWrapper must not raise IndexError.
    Previously failed with: IndexError: tuple index out of range
    """
    conn, c = db_conn
    client_id = f"test-{uuid.uuid4().hex[:8]}@regression.test"

    # Ensure a clients row exists (FK requirement)
    c.execute(
        """
        INSERT INTO clients (client_id, name, broker_type, status,
            max_trades_per_day, max_concurrent_positions,
            daily_max_loss_pct, base_position_pct, created_at)
        VALUES (%s, %s, 'tradier', 'ACTIVE', 12, 4, 0.05, 0.10, NOW())
        ON CONFLICT (client_id) DO NOTHING
        """,
        (client_id, client_id),
    )

    _insert_queue_row(c, client_id, score=80.0)

    # This is the exact query that was crashing. Must not raise.
    c.execute(
        r"""
        WITH next_job AS (
            SELECT id
            FROM   trade_queue
            WHERE  status    = 'NEW'
              AND  client_id = %s
            ORDER BY
                CASE
                    WHEN payload ? 'score'
                     AND payload->>'score' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (payload->>'score')::numeric
                    ELSE 65
                END DESC,
                created_ts ASC
            LIMIT  1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE trade_queue tq
        SET    status     = 'PROCESSING',
               started_ts = NOW()
        FROM   next_job
        WHERE  tq.id = next_job.id
        RETURNING tq.id, tq.client_id, tq.signal_id, tq.payload
        """,
        (client_id,),   # exactly ONE bind parameter — must match exactly ONE %s
    )
    row = c.fetchone()
    assert row is not None, "Expected a claimed job row, got None"
    assert row["client_id"] == client_id
    assert row["payload"]["score"] == 80.0


# ── TEST 2: Score ranking — higher score claimed first ───────────────────────
def test_score_ranked_claim_order(db_conn):
    """
    Insert two NEW jobs for the same client: score 60 and score 90.
    The claim query should return the score-90 job first.
    """
    conn, c = db_conn
    client_id = f"test-{uuid.uuid4().hex[:8]}@regression.test"

    c.execute(
        """
        INSERT INTO clients (client_id, name, broker_type, status,
            max_trades_per_day, max_concurrent_positions,
            daily_max_loss_pct, base_position_pct, created_at)
        VALUES (%s, %s, 'tradier', 'ACTIVE', 12, 4, 0.05, 0.10, NOW())
        ON CONFLICT (client_id) DO NOTHING
        """,
        (client_id, client_id),
    )

    low_id  = _insert_queue_row(c, client_id, score=60.0)
    high_id = _insert_queue_row(c, client_id, score=90.0)

    c.execute(
        r"""
        WITH next_job AS (
            SELECT id
            FROM   trade_queue
            WHERE  status    = 'NEW'
              AND  client_id = %s
            ORDER BY
                CASE
                    WHEN payload ? 'score'
                     AND payload->>'score' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (payload->>'score')::numeric
                    ELSE 65
                END DESC,
                created_ts ASC
            LIMIT  1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE trade_queue tq
        SET    status = 'PROCESSING', started_ts = NOW()
        FROM   next_job
        WHERE  tq.id = next_job.id
        RETURNING tq.id, tq.payload
        """,
        (client_id,),
    )
    row = c.fetchone()
    assert row is not None
    assert row["id"] == high_id, (
        f"Expected high-score job {high_id} to be claimed first, got {row['id']}"
    )
    assert row["payload"]["score"] == 90.0


# ── TEST 3: Missing score falls back to 65 without crashing ─────────────────
def test_missing_score_uses_default(db_conn):
    """
    A payload without a 'score' key should fall back to 65, not crash.
    Previously the COALESCE + cast on non-numeric strings would explode.
    """
    conn, c = db_conn
    client_id = f"test-{uuid.uuid4().hex[:8]}@regression.test"

    c.execute(
        """
        INSERT INTO clients (client_id, name, broker_type, status,
            max_trades_per_day, max_concurrent_positions,
            daily_max_loss_pct, base_position_pct, created_at)
        VALUES (%s, %s, 'tradier', 'ACTIVE', 12, 4, 0.05, 0.10, NOW())
        ON CONFLICT (client_id) DO NOTHING
        """,
        (client_id, client_id),
    )

    # No 'score' key in payload
    job_id = str(uuid.uuid4())
    c.execute(
        """
        INSERT INTO trade_queue (id, client_id, signal_id, status, payload, created_ts)
        VALUES (%s, %s, %s, 'NEW', '{"ticker":"AAPL"}'::jsonb, NOW())
        """,
        (job_id, client_id, str(uuid.uuid4())),
    )

    # Must not raise — falls back to score=65
    c.execute(
        r"""
        WITH next_job AS (
            SELECT id FROM trade_queue
            WHERE  status = 'NEW' AND client_id = %s
            ORDER BY
                CASE
                    WHEN payload ? 'score'
                     AND payload->>'score' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (payload->>'score')::numeric
                    ELSE 65
                END DESC, created_ts ASC
            LIMIT 1 FOR UPDATE SKIP LOCKED
        )
        UPDATE trade_queue tq SET status='PROCESSING', started_ts=NOW()
        FROM next_job WHERE tq.id = next_job.id
        RETURNING tq.id
        """,
        (client_id,),
    )
    row = c.fetchone()
    assert row is not None
    assert row["id"] == job_id
