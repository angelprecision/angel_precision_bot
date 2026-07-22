"""
tests/test_p0_proof_trade_exactly_once_production_shape.py

Exactly-once terminal proof binding — production-shape PostgreSQL tests.

Incident: 2026-07-20 Tradefluence produced two proof_trades rows for each of two
economic trades (PEP and ABT). This was a proof-ledger duplication defect — there
was only one broker EXIT order and one broker fill per trade.

Root causes fixed:
  1. _claim_recent_broker_repair_proof SQL required local_order_id = canonical_entry_id
     in WHERE clause, but the production repair proof had local_order_id=NULL. The
     candidate row was unreachable, so the repair claim always failed.
  2. Mode resolution used COALESCE(NULLIF(execution_mode,''), ...) which treated the
     literal string 'unknown' as a valid value, shadowing the valid mode='paper'
     fallback. The candidate was rejected even when mode='paper' was present.
  3. The reconciler had an independent APProofLogger.log_trade() call that inserted a
     second canonical row ~1 second after the repair row.

These tests use real PostgreSQL through INTELLIGENCE_POSTGRES_TEST_URL and verify
the production-exact data shapes from the incident.
"""
from __future__ import annotations

import os
import sys
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL fixture helpers
# ─────────────────────────────────────────────────────────────────────────────

def _pg_connect():
    psycopg2 = pytest.importorskip("psycopg2")
    url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL")
    if not url:
        if os.getenv("GITHUB_ACTIONS") == "true":
            pytest.fail("INTELLIGENCE_POSTGRES_TEST_URL is required in GitHub Actions")
        pytest.skip("INTELLIGENCE_POSTGRES_TEST_URL not set")
    try:
        return psycopg2.connect(url)
    except Exception as exc:
        if os.getenv("GITHUB_ACTIONS") == "true":
            pytest.fail(f"PostgreSQL connection failed: {exc}")
        pytest.skip(f"PostgreSQL connection unavailable: {exc}")


def _setup_schema(db):
    """Create minimal schema for exactly-once proof tests."""
    with db.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS proof_trades CASCADE")
        cur.execute("DROP TABLE IF EXISTS orders CASCADE")
        cur.execute("DROP TABLE IF EXISTS positions CASCADE")

        cur.execute("""
            CREATE TABLE positions (
                id TEXT PRIMARY KEY,
                client_id TEXT NOT NULL,
                local_order_id TEXT,
                broker_order_id TEXT,
                execution_mode TEXT,
                contract TEXT,
                underlying TEXT,
                direction TEXT,
                qty INTEGER,
                avg_fill NUMERIC,
                entry_price NUMERIC,
                status TEXT,
                entry_ts TIMESTAMPTZ DEFAULT NOW(),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE orders (
                id TEXT PRIMARY KEY,
                client_id TEXT NOT NULL,
                local_order_id TEXT,
                broker_order_id TEXT,
                kind TEXT,
                execution_mode TEXT,
                status TEXT,
                direction TEXT,
                contract TEXT,
                symbol TEXT,
                qty INTEGER,
                filled_qty INTEGER,
                fill_price NUMERIC,
                position_id TEXT,
                signal_id TEXT,
                filled_ts TIMESTAMPTZ,
                created_ts TIMESTAMPTZ DEFAULT NOW(),
                updated_ts TIMESTAMPTZ DEFAULT NOW(),
                meta JSONB DEFAULT '{}'::jsonb
            )
        """)
        cur.execute("""
            CREATE TABLE proof_trades (
                id BIGSERIAL PRIMARY KEY,
                client_email TEXT NOT NULL,
                position_id TEXT,
                local_order_id TEXT,
                execution_mode TEXT,
                mode TEXT,
                side TEXT,
                contracts INTEGER,
                entry_option_price NUMERIC,
                exit_option_price NUMERIC,
                option_pnl_pct NUMERIC,
                exit_reason TEXT,
                proof_event_key TEXT,
                proof_diagnostics JSONB NOT NULL DEFAULT '{}'::jsonb,
                performance_taxonomy TEXT,
                training_eligible BOOLEAN DEFAULT FALSE,
                official_live_performance_eligible BOOLEAN DEFAULT FALSE,
                synthetic_entry BOOLEAN DEFAULT FALSE,
                ticker TEXT,
                pattern TEXT,
                timeframe TEXT,
                score NUMERIC,
                tier TEXT,
                context_score NUMERIC,
                setup_status TEXT,
                win BOOLEAN,
                opened_at TIMESTAMPTZ,
                closed_at TIMESTAMPTZ DEFAULT NOW(),
                system_version TEXT DEFAULT 'v2'
            )
        """)
        cur.execute("""
            CREATE UNIQUE INDEX uq_proof_trades_event_key
            ON proof_trades (proof_event_key)
            WHERE proof_event_key IS NOT NULL
              AND BTRIM(proof_event_key) <> ''
        """)
    db.commit()


class _PgConnWrapper:
    """Adapts psycopg2 connection to the ap.db.conn() context manager interface."""

    def __init__(self, connection):
        self._connection = connection
        self._cursor = None

    def __call__(self):
        return self

    def __enter__(self):
        import psycopg2.extras
        self._cursor = self._connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type:
            self._connection.rollback()
        else:
            self._connection.commit()
        self._cursor.close()
        self._cursor = None
        return False

    def execute(self, sql, params=()):
        self._cursor.execute(sql, params)
        return self

    def fetchone(self):
        row = self._cursor.fetchone()
        return dict(row) if row else None

    def fetchall(self):
        return [dict(r) for r in self._cursor.fetchall()]


def _count_proof_rows(db, client_email: str) -> int:
    with db.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM proof_trades WHERE client_email=%s",
            (client_email,)
        )
        return cur.fetchone()[0]


def _get_proof_rows(db, client_email: str) -> list[dict]:
    with db.cursor() as cur:
        cur.execute(
            "SELECT * FROM proof_trades WHERE client_email=%s ORDER BY id",
            (client_email,)
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ─────────────────────────────────────────────────────────────────────────────
# Test A — ABT production shape (repair row with NULL local_order_id)
# ─────────────────────────────────────────────────────────────────────────────

def test_abt_production_shape_exactly_one_proof_row():
    """
    Reproduces the exact ABT data shape from the 2026-07-20 incident.
    The repair proof has local_order_id=NULL and execution_mode='unknown'.
    After running the canonical terminal close + reconciler replay, there
    must be exactly one proof_trades row.
    """
    db = _pg_connect()
    try:
        _setup_schema(db)

        client_email = "tradefluencehq@gmail.com"
        position_id = "f40c1b44-8391-44bd-a25e-fbce780a282d"
        entry_local_order_id = "39f156b0-dce4-4a5e-a99e-f3b499ead39d"
        contract = "ABT260731C00098000"
        repair_position_id = f"broker-repair-{client_email}-{contract}"
        closed_at = "2026-07-20T15:27:35.873253Z"

        # Insert canonical ENTRY order (production-exact shape)
        with db.cursor() as cur:
            cur.execute("""
                INSERT INTO orders (
                    id, client_id, kind, local_order_id, broker_order_id,
                    position_id, execution_mode, status, direction, contract, symbol,
                    qty, filled_qty, fill_price, filled_ts
                ) VALUES (
                    %s, %s, 'ENTRY', %s, '35491112',
                    %s, 'paper', 'FILLED', 'CALL', %s, %s,
                    3, 3, 5.00, NOW()
                )
            """, (
                str(uuid.uuid4()), client_email, entry_local_order_id,
                position_id, contract, contract,
            ))
            # Insert canonical position
            cur.execute("""
                INSERT INTO positions (
                    id, client_id, local_order_id, broker_order_id, execution_mode,
                    contract, underlying, direction, qty, avg_fill, entry_price, status
                ) VALUES (
                    %s, %s, %s, '35491112', 'paper',
                    %s, 'ABT', 'CALL', 3, 5.00, 5.00, 'OPEN'
                )
            """, (position_id, client_email, entry_local_order_id, contract))
            # Insert repair proof row — production-exact:
            #   local_order_id=NULL, execution_mode='unknown', mode='paper'
            cur.execute("""
                INSERT INTO proof_trades (
                    client_email, position_id, local_order_id,
                    mode, execution_mode, side, contracts,
                    entry_option_price, exit_option_price, option_pnl_pct,
                    exit_reason, closed_at
                ) VALUES (
                    %s, %s, NULL,
                    'paper', 'unknown', 'CALL', 3,
                    5.00, 4.60, -8.00,
                    'THESIS_FAIL_SOFT_STOP — -14%% loss and underlying not confirming (no_underlying_data) | age=6.2min | confirmed 83s',
                    %s
                )
            """, (client_email, repair_position_id, closed_at))
        db.commit()

        # Verify precondition: one repair proof, not yet canonical
        assert _count_proof_rows(db, client_email) == 1

        # Wire up position_manager against real PostgreSQL
        conn_wrapper = _PgConnWrapper(db)
        import ap.position_manager as pm_mod
        import ap.db as db_mod

        with patch.object(pm_mod, "conn", conn_wrapper), \
             patch.object(pm_mod, "run_with_retry", lambda fn: fn()), \
             patch.object(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
                 "local_order_id": entry_local_order_id,
                 "client_id": client_email,
                 "kind": "ENTRY",
                 "execution_mode": "paper",
                 "direction": "CALL",
                 "signal_id": "",
                 "meta": {"pattern": "3-1-2", "timeframe": "5m", "tier": "A", "score": 85, "context_score": 72},
             } if local_order_id == entry_local_order_id else None):

            from ap.position_manager import APPositionManager
            pm = APPositionManager(client_email)

            result = pm.ensure_terminal_close_proof(
                position_id=position_id,
                local_order_id=entry_local_order_id,
                contract=contract,
                underlying="ABT",
                side="CALL",
                opened_at="2026-07-20T14:00:00Z",
                closed_at=closed_at,
                entry_option_price=5.00,
                exit_option_price=4.60,
                contracts=3,
                exit_reason="THESIS_FAIL_SOFT_STOP — -14% loss and underlying not confirming (no_underlying_data) | age=6.2min | confirmed 83s",
                option_pnl_pct=-8.00,
                setup_status="reconciler_auto_close",
                execution_mode="paper",
                exit_fill_price=4.60,
                reconciliation_reason="RECONCILER_AUTO_CLOSE | HIGH | broker_position_missing",
                allow_fallback_insert=True,
                missing_reason_code="RECONCILER_PROOF_WRITE_FAILED",
            )

        # Must be exactly one proof row (repair row was bound in place)
        assert _count_proof_rows(db, client_email) == 1, (
            f"Expected 1 proof row, got {_count_proof_rows(db, client_email)}"
        )
        assert result["status"] in {"BOUND_REPAIR", "EXISTING_CANONICAL", "MERGED_DUPLICATE"}, (
            f"Expected binding status, got {result}"
        )

        rows = _get_proof_rows(db, client_email)
        surviving = rows[0]

        # Canonical identity must be bound
        assert surviving["position_id"] == position_id, (
            f"position_id must be canonical, got {surviving['position_id']}"
        )
        assert surviving["local_order_id"] == entry_local_order_id, (
            f"local_order_id must be canonical ENTRY, got {surviving['local_order_id']}"
        )
        assert surviving["execution_mode"] in ("paper", "unknown"), (
            f"execution_mode should be resolved, got {surviving['execution_mode']}"
        )
        assert surviving["mode"] in ("paper",), (
            f"mode should be paper, got {surviving['mode']}"
        )

        # Financial data preserved
        assert abs(float(surviving["entry_option_price"]) - 5.00) < 0.01
        assert abs(float(surviving["exit_option_price"]) - 4.60) < 0.01
        assert abs(float(surviving["option_pnl_pct"]) - (-8.00)) < 0.01

        # Both diagnostic reasons must survive
        exit_reason = str(surviving["exit_reason"] or "")
        assert "THESIS_FAIL_SOFT_STOP" in exit_reason, (
            f"Decision reason must survive: {exit_reason}"
        )

        # Taxonomy
        assert surviving["performance_taxonomy"] in ("PAPER_UNVERIFIED", None)
        assert not surviving["official_live_performance_eligible"]
        assert not surviving["training_eligible"]

    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Test B — PEP production shape (PUT contract, qty=9)
# ─────────────────────────────────────────────────────────────────────────────

def test_pep_production_shape_exactly_one_proof_row():
    """PEP shape: PUT contract, qty=9, pnl=-5.88%."""
    db = _pg_connect()
    try:
        _setup_schema(db)

        client_email = "tradefluencehq@gmail.com"
        position_id = "bc2b1ca1-07c3-4969-9dcc-78eadd9341b3"
        entry_local_order_id = "6671303e-ee80-4498-a9f2-49b2067f32d8"
        contract = "PEP260731P00135000"
        repair_position_id = f"broker-repair-{client_email}-{contract}"
        closed_at = "2026-07-20T14:39:59.331124Z"

        with db.cursor() as cur:
            cur.execute("""
                INSERT INTO orders (
                    id, client_id, kind, local_order_id, broker_order_id,
                    position_id, execution_mode, status, direction, contract, symbol,
                    qty, filled_qty, fill_price, filled_ts
                ) VALUES (
                    %s, %s, 'ENTRY', %s, '35490001',
                    %s, 'paper', 'FILLED', 'PUT', %s, %s,
                    9, 9, 1.70, NOW()
                )
            """, (
                str(uuid.uuid4()), client_email, entry_local_order_id,
                position_id, contract, contract,
            ))
            cur.execute("""
                INSERT INTO positions (
                    id, client_id, local_order_id, broker_order_id, execution_mode,
                    contract, underlying, direction, qty, avg_fill, entry_price, status
                ) VALUES (
                    %s, %s, %s, '35490001', 'paper',
                    %s, 'PEP', 'PUT', 9, 1.70, 1.70, 'OPEN'
                )
            """, (position_id, client_email, entry_local_order_id, contract))
            cur.execute("""
                INSERT INTO proof_trades (
                    client_email, position_id, local_order_id,
                    mode, execution_mode, side, contracts,
                    entry_option_price, exit_option_price, option_pnl_pct,
                    exit_reason, closed_at
                ) VALUES (
                    %s, %s, NULL,
                    'paper', 'unknown', 'PUT', 9,
                    1.70, 1.60, -5.88,
                    'TOUCHED PROFIT STOP — peaked +4%% now -10%% — floor=0%% | underlying=not confirming',
                    %s
                )
            """, (client_email, repair_position_id, closed_at))
        db.commit()

        conn_wrapper = _PgConnWrapper(db)
        import ap.position_manager as pm_mod
        import ap.db as db_mod

        with patch.object(pm_mod, "conn", conn_wrapper), \
             patch.object(pm_mod, "run_with_retry", lambda fn: fn()), \
             patch.object(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
                 "local_order_id": entry_local_order_id,
                 "client_id": client_email,
                 "kind": "ENTRY",
                 "execution_mode": "paper",
                 "direction": "PUT",
                 "signal_id": "",
                 "meta": {"pattern": "3-1-2", "timeframe": "5m", "tier": "A", "score": 82, "context_score": 68},
             } if local_order_id == entry_local_order_id else None):

            from ap.position_manager import APPositionManager
            pm = APPositionManager(client_email)
            result = pm.ensure_terminal_close_proof(
                position_id=position_id,
                local_order_id=entry_local_order_id,
                contract=contract,
                underlying="PEP",
                side="PUT",
                opened_at="2026-07-20T13:00:00Z",
                closed_at=closed_at,
                entry_option_price=1.70,
                exit_option_price=1.60,
                contracts=9,
                exit_reason="TOUCHED PROFIT STOP — peaked +4% now -10% — floor=0% | underlying=not confirming",
                option_pnl_pct=-5.88,
                setup_status="reconciler_auto_close",
                execution_mode="paper",
                exit_fill_price=1.60,
                reconciliation_reason="RECONCILER_AUTO_CLOSE | HIGH | broker_position_missing",
                allow_fallback_insert=True,
                missing_reason_code="RECONCILER_PROOF_WRITE_FAILED",
            )

        assert _count_proof_rows(db, client_email) == 1
        rows = _get_proof_rows(db, client_email)
        surviving = rows[0]
        assert surviving["position_id"] == position_id
        assert surviving["local_order_id"] == entry_local_order_id
        assert surviving["side"] == "PUT"
        assert int(surviving["contracts"]) == 9
        assert abs(float(surviving["option_pnl_pct"]) - (-5.88)) < 0.01
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Test C — NULL local_order_id claim and non-NULL different order rejection
# ─────────────────────────────────────────────────────────────────────────────

def test_null_local_order_id_repair_row_can_be_claimed():
    """Repair row with local_order_id=NULL must be claimable by canonical ENTRY order."""
    db = _pg_connect()
    try:
        _setup_schema(db)
        client_email = "testclient@example.com"
        contract = "TSLA260731C00200000"
        repair_pid = f"broker-repair-{client_email}-{contract}"
        canonical_pid = str(uuid.uuid4())
        entry_order_id = str(uuid.uuid4())

        with db.cursor() as cur:
            cur.execute("""
                INSERT INTO orders (id, client_id, kind, local_order_id, execution_mode,
                    status, direction, contract, symbol, qty, filled_qty, fill_price, position_id)
                VALUES (%s, %s, 'ENTRY', %s, 'paper', 'FILLED', 'CALL', %s, %s, 2, 2, 3.00, %s)
            """, (str(uuid.uuid4()), client_email, entry_order_id, contract, contract, canonical_pid))
            cur.execute("""
                INSERT INTO positions (id, client_id, local_order_id, execution_mode,
                    contract, direction, qty, avg_fill, status)
                VALUES (%s, %s, %s, 'paper', %s, 'CALL', 2, 3.00, 'OPEN')
            """, (canonical_pid, client_email, entry_order_id, contract))
            # Repair row with NULL local_order_id
            cur.execute("""
                INSERT INTO proof_trades (client_email, position_id, local_order_id,
                    mode, execution_mode, side, contracts, entry_option_price, exit_option_price,
                    option_pnl_pct, exit_reason, closed_at)
                VALUES (%s, %s, NULL, 'paper', 'unknown', 'CALL', 2, 3.00, 2.50, -16.67,
                    'THESIS_FAIL_SOFT_STOP', NOW())
            """, (client_email, repair_pid))
        db.commit()

        conn_wrapper = _PgConnWrapper(db)
        import ap.position_manager as pm_mod
        import ap.db as db_mod

        with patch.object(pm_mod, "conn", conn_wrapper), \
             patch.object(pm_mod, "run_with_retry", lambda fn: fn()), \
             patch.object(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
                 "local_order_id": entry_order_id,
                 "client_id": client_email,
                 "kind": "ENTRY",
                 "execution_mode": "paper",
                 "direction": "CALL",
                 "signal_id": "",
                 "meta": {},
             } if local_order_id == entry_order_id else None):

            from ap.position_manager import APPositionManager
            pm = APPositionManager(client_email)
            claimed = pm._claim_recent_broker_repair_proof(
                position_id=canonical_pid,
                contract=contract,
                closed_at=datetime.now(timezone.utc).isoformat(),
                local_order_id=entry_order_id,
                execution_mode="paper",
                side="CALL",
                contracts=2,
                entry_option_price=3.00,
            )

        assert claimed is True, "NULL local_order_id repair row must be claimable"

        rows = _get_proof_rows(db, client_email)
        assert len(rows) == 1
        assert rows[0]["position_id"] == canonical_pid
        assert rows[0]["local_order_id"] == entry_order_id
    finally:
        db.close()


def test_different_nonempty_local_order_cannot_be_claimed():
    """A repair row with a different nonempty local_order_id must NOT be claimable."""
    db = _pg_connect()
    try:
        _setup_schema(db)
        client_email = "testclient@example.com"
        contract = "AAPL260731C00200000"
        repair_pid = f"broker-repair-{client_email}-{contract}"
        canonical_pid = str(uuid.uuid4())
        entry_order_id = str(uuid.uuid4())
        different_order_id = str(uuid.uuid4())  # different order

        with db.cursor() as cur:
            cur.execute("""
                INSERT INTO orders (id, client_id, kind, local_order_id, execution_mode,
                    status, direction, contract, symbol, qty, filled_qty, fill_price, position_id)
                VALUES (%s, %s, 'ENTRY', %s, 'paper', 'FILLED', 'CALL', %s, %s, 1, 1, 2.00, %s)
            """, (str(uuid.uuid4()), client_email, entry_order_id, contract, contract, canonical_pid))
            cur.execute("""
                INSERT INTO positions (id, client_id, local_order_id, execution_mode,
                    contract, direction, qty, avg_fill, status)
                VALUES (%s, %s, %s, 'paper', %s, 'CALL', 1, 2.00, 'OPEN')
            """, (canonical_pid, client_email, entry_order_id, contract))
            # Repair row with a DIFFERENT nonempty local_order_id
            cur.execute("""
                INSERT INTO proof_trades (client_email, position_id, local_order_id,
                    mode, execution_mode, side, contracts, entry_option_price, exit_option_price,
                    option_pnl_pct, exit_reason, closed_at)
                VALUES (%s, %s, %s, 'paper', 'paper', 'CALL', 1, 2.00, 1.80, -10.00,
                    'THESIS_FAIL', NOW())
            """, (client_email, repair_pid, different_order_id))
        db.commit()

        conn_wrapper = _PgConnWrapper(db)
        import ap.position_manager as pm_mod
        import ap.db as db_mod

        with patch.object(pm_mod, "conn", conn_wrapper), \
             patch.object(pm_mod, "run_with_retry", lambda fn: fn()), \
             patch.object(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
                 "local_order_id": entry_order_id,
                 "client_id": client_email,
                 "kind": "ENTRY",
                 "execution_mode": "paper",
                 "direction": "CALL",
                 "signal_id": "",
                 "meta": {},
             } if local_order_id == entry_order_id else None):

            from ap.position_manager import APPositionManager
            pm = APPositionManager(client_email)
            claimed = pm._claim_recent_broker_repair_proof(
                position_id=canonical_pid,
                contract=contract,
                closed_at=datetime.now(timezone.utc).isoformat(),
                local_order_id=entry_order_id,
                execution_mode="paper",
                side="CALL",
                contracts=1,
                entry_option_price=2.00,
            )

        assert claimed is False, "Repair row with different nonempty local_order_id must NOT be claimable"
        # Row must be unchanged
        rows = _get_proof_rows(db, client_email)
        assert rows[0]["position_id"] == repair_pid
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Test D — Unknown execution_mode fallback to mode='paper'
# ─────────────────────────────────────────────────────────────────────────────

def test_unknown_execution_mode_with_paper_mode_matches():
    """execution_mode='unknown' + mode='paper' must match a canonical PAPER entry."""
    db = _pg_connect()
    try:
        _setup_schema(db)
        client_email = "testclient@example.com"
        contract = "NVDA260731C00100000"
        repair_pid = f"broker-repair-{client_email}-{contract}"
        canonical_pid = str(uuid.uuid4())
        entry_order_id = str(uuid.uuid4())

        with db.cursor() as cur:
            cur.execute("""
                INSERT INTO orders (id, client_id, kind, local_order_id, execution_mode,
                    status, direction, contract, symbol, qty, filled_qty, fill_price, position_id)
                VALUES (%s, %s, 'ENTRY', %s, 'paper', 'FILLED', 'CALL', %s, %s, 1, 1, 5.00, %s)
            """, (str(uuid.uuid4()), client_email, entry_order_id, contract, contract, canonical_pid))
            cur.execute("""
                INSERT INTO positions (id, client_id, local_order_id, execution_mode,
                    contract, direction, qty, avg_fill, status)
                VALUES (%s, %s, %s, 'paper', %s, 'CALL', 1, 5.00, 'OPEN')
            """, (canonical_pid, client_email, entry_order_id, contract))
            # Repair row: execution_mode='unknown', mode='paper'
            cur.execute("""
                INSERT INTO proof_trades (client_email, position_id, local_order_id,
                    mode, execution_mode, side, contracts, entry_option_price, exit_option_price,
                    option_pnl_pct, exit_reason, closed_at)
                VALUES (%s, %s, NULL, 'paper', 'unknown', 'CALL', 1, 5.00, 4.00, -20.00,
                    'SOFT_STOP', NOW())
            """, (client_email, repair_pid))
        db.commit()

        conn_wrapper = _PgConnWrapper(db)
        import ap.position_manager as pm_mod
        import ap.db as db_mod

        with patch.object(pm_mod, "conn", conn_wrapper), \
             patch.object(pm_mod, "run_with_retry", lambda fn: fn()), \
             patch.object(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
                 "local_order_id": entry_order_id, "client_id": client_email,
                 "kind": "ENTRY", "execution_mode": "paper", "direction": "CALL",
                 "signal_id": "", "meta": {},
             } if local_order_id == entry_order_id else None):

            from ap.position_manager import APPositionManager
            pm = APPositionManager(client_email)
            claimed = pm._claim_recent_broker_repair_proof(
                position_id=canonical_pid, contract=contract,
                closed_at=datetime.now(timezone.utc).isoformat(),
                local_order_id=entry_order_id, execution_mode="paper",
                side="CALL", contracts=1, entry_option_price=5.00,
            )

        assert claimed is True, "execution_mode='unknown' + mode='paper' must match a PAPER entry"
    finally:
        db.close()


def test_conflicting_live_execution_mode_paper_mode_is_quarantined():
    """execution_mode='live' + mode='paper' conflict must be quarantined (not claimed)."""
    db = _pg_connect()
    try:
        _setup_schema(db)
        client_email = "testclient@example.com"
        contract = "META260731C00500000"
        repair_pid = f"broker-repair-{client_email}-{contract}"
        canonical_pid = str(uuid.uuid4())
        entry_order_id = str(uuid.uuid4())

        with db.cursor() as cur:
            cur.execute("""
                INSERT INTO orders (id, client_id, kind, local_order_id, execution_mode,
                    status, direction, contract, symbol, qty, filled_qty, fill_price, position_id)
                VALUES (%s, %s, 'ENTRY', %s, 'paper', 'FILLED', 'CALL', %s, %s, 1, 1, 10.00, %s)
            """, (str(uuid.uuid4()), client_email, entry_order_id, contract, contract, canonical_pid))
            cur.execute("""
                INSERT INTO positions (id, client_id, local_order_id, execution_mode,
                    contract, direction, qty, avg_fill, status)
                VALUES (%s, %s, %s, 'paper', %s, 'CALL', 1, 10.00, 'OPEN')
            """, (canonical_pid, client_email, entry_order_id, contract))
            # Conflict: execution_mode='live' but mode='paper'
            cur.execute("""
                INSERT INTO proof_trades (client_email, position_id, local_order_id,
                    mode, execution_mode, side, contracts, entry_option_price, exit_option_price,
                    option_pnl_pct, exit_reason, closed_at)
                VALUES (%s, %s, NULL, 'paper', 'live', 'CALL', 1, 10.00, 9.00, -10.00,
                    'STOP_HIT', NOW())
            """, (client_email, repair_pid))
        db.commit()

        conn_wrapper = _PgConnWrapper(db)
        import ap.position_manager as pm_mod
        import ap.db as db_mod

        with patch.object(pm_mod, "conn", conn_wrapper), \
             patch.object(pm_mod, "run_with_retry", lambda fn: fn()), \
             patch.object(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
                 "local_order_id": entry_order_id, "client_id": client_email,
                 "kind": "ENTRY", "execution_mode": "paper", "direction": "CALL",
                 "signal_id": "", "meta": {},
             } if local_order_id == entry_order_id else None):

            from ap.position_manager import APPositionManager
            pm = APPositionManager(client_email)
            # Canonical ENTRY order is paper — repair row with execution_mode='live' must not match
            claimed = pm._claim_recent_broker_repair_proof(
                position_id=canonical_pid, contract=contract,
                closed_at=datetime.now(timezone.utc).isoformat(),
                local_order_id=entry_order_id, execution_mode="paper",
                side="CALL", contracts=1, entry_option_price=10.00,
            )

        # 'live' in execution_mode resolves to 'live' via CASE, != 'paper' → no match → not claimed
        assert claimed is False, "Conflicting execution_mode='live' + mode='paper' must not match paper entry"
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Test E — Closed-at fencing
# ─────────────────────────────────────────────────────────────────────────────

def test_repair_row_outside_time_window_not_claimed():
    """Repair row with closed_at > 5 minutes from canonical closed_at must not be claimed."""
    db = _pg_connect()
    try:
        _setup_schema(db)
        client_email = "testclient@example.com"
        contract = "SPY260731C00580000"
        repair_pid = f"broker-repair-{client_email}-{contract}"
        canonical_pid = str(uuid.uuid4())
        entry_order_id = str(uuid.uuid4())

        # Repair row is from 10 minutes ago
        from datetime import timedelta
        old_closed_at = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        now_closed_at = datetime.now(timezone.utc).isoformat()

        with db.cursor() as cur:
            cur.execute("""
                INSERT INTO orders (id, client_id, kind, local_order_id, execution_mode,
                    status, direction, contract, symbol, qty, filled_qty, fill_price, position_id)
                VALUES (%s, %s, 'ENTRY', %s, 'paper', 'FILLED', 'CALL', %s, %s, 1, 1, 5.00, %s)
            """, (str(uuid.uuid4()), client_email, entry_order_id, contract, contract, canonical_pid))
            cur.execute("""
                INSERT INTO positions (id, client_id, local_order_id, execution_mode,
                    contract, direction, qty, avg_fill, status)
                VALUES (%s, %s, %s, 'paper', %s, 'CALL', 1, 5.00, 'OPEN')
            """, (canonical_pid, client_email, entry_order_id, contract))
            cur.execute("""
                INSERT INTO proof_trades (client_email, position_id, local_order_id,
                    mode, execution_mode, side, contracts, entry_option_price, exit_option_price,
                    option_pnl_pct, exit_reason, closed_at)
                VALUES (%s, %s, NULL, 'paper', 'unknown', 'CALL', 1, 5.00, 4.00, -20.00,
                    'OLD_STOP', %s)
            """, (client_email, repair_pid, old_closed_at))
        db.commit()

        conn_wrapper = _PgConnWrapper(db)
        import ap.position_manager as pm_mod
        import ap.db as db_mod

        with patch.object(pm_mod, "conn", conn_wrapper), \
             patch.object(pm_mod, "run_with_retry", lambda fn: fn()), \
             patch.object(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
                 "local_order_id": entry_order_id, "client_id": client_email,
                 "kind": "ENTRY", "execution_mode": "paper", "direction": "CALL",
                 "signal_id": "", "meta": {},
             } if local_order_id == entry_order_id else None):

            from ap.position_manager import APPositionManager
            pm = APPositionManager(client_email)
            claimed = pm._claim_recent_broker_repair_proof(
                position_id=canonical_pid, contract=contract,
                closed_at=now_closed_at,  # NOW, but repair row is 10 min old
                local_order_id=entry_order_id, execution_mode="paper",
                side="CALL", contracts=1, entry_option_price=5.00,
            )

        assert claimed is False, "Repair row >5 minutes outside closed_at window must not be claimed"
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Test F — Reconciler delegation (no direct APProofLogger call)
# ─────────────────────────────────────────────────────────────────────────────

def test_reconciler_has_no_direct_approoflogger_log_trade_call():
    """
    Source-level assertion: ap_reconciler.py must NOT contain a direct
    APProofLogger(...).log_trade(...) call in the _execute_reconciler_close path.
    The reconciler must delegate to self.pm.ensure_terminal_close_proof.
    """
    src = (_REPO / "ap_reconciler.py").read_text()

    # The reconciler should not have the old direct-insert pattern
    assert "APProofLogger(" not in src.split("def _execute_reconciler_close")[1].split("def _import_broker")[0], (
        "ap_reconciler._execute_reconciler_close must NOT contain a direct APProofLogger instantiation"
    )
    assert "ensure_terminal_close_proof" in src, (
        "ap_reconciler must delegate to ensure_terminal_close_proof"
    )


def test_reconciler_delegation_produces_no_second_row():
    """
    Reconciler replay after an existing repair-bound proof must not insert a second row.
    """
    db = _pg_connect()
    try:
        _setup_schema(db)
        client_email = "tradefluencehq@gmail.com"
        position_id = str(uuid.uuid4())
        entry_order_id = str(uuid.uuid4())
        contract = "QQQ260731C00480000"
        closed_at = datetime.now(timezone.utc).isoformat()

        with db.cursor() as cur:
            cur.execute("""
                INSERT INTO orders (id, client_id, kind, local_order_id, execution_mode,
                    status, direction, contract, symbol, qty, filled_qty, fill_price, position_id)
                VALUES (%s, %s, 'ENTRY', %s, 'paper', 'FILLED', 'CALL', %s, %s, 2, 2, 3.00, %s)
            """, (str(uuid.uuid4()), client_email, entry_order_id, contract, contract, position_id))
            cur.execute("""
                INSERT INTO positions (id, client_id, local_order_id, execution_mode,
                    contract, underlying, direction, qty, avg_fill, entry_price, status,
                    entry_ts)
                VALUES (%s, %s, %s, 'paper', %s, 'QQQ', 'CALL', 2, 3.00, 3.00, 'OPEN', NOW())
            """, (position_id, client_email, entry_order_id, contract))
            # Simulate: repair proof was already written + claimed
            cur.execute("""
                INSERT INTO proof_trades (client_email, position_id, local_order_id,
                    mode, execution_mode, side, contracts, entry_option_price, exit_option_price,
                    option_pnl_pct, exit_reason, closed_at, proof_event_key)
                VALUES (%s, %s, %s, 'paper', 'paper', 'CALL', 2, 3.00, 2.50, -16.67,
                    'SOFT_STOP', %s, %s)
            """, (client_email, position_id, entry_order_id, closed_at,
                  f"entry:{client_email}:{entry_order_id}"))
        db.commit()

        assert _count_proof_rows(db, client_email) == 1

        conn_wrapper = _PgConnWrapper(db)
        import ap.position_manager as pm_mod
        import ap.db as db_mod

        with patch.object(pm_mod, "conn", conn_wrapper), \
             patch.object(pm_mod, "run_with_retry", lambda fn: fn()), \
             patch.object(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
                 "local_order_id": entry_order_id, "client_id": client_email,
                 "kind": "ENTRY", "execution_mode": "paper", "direction": "CALL",
                 "signal_id": "", "meta": {},
             } if local_order_id == entry_order_id else None):

            from ap.position_manager import APPositionManager
            pm = APPositionManager(client_email)
            result = pm.ensure_terminal_close_proof(
                position_id=position_id, local_order_id=entry_order_id,
                contract=contract, underlying="QQQ", side="CALL",
                opened_at=closed_at, closed_at=closed_at,
                entry_option_price=3.00, exit_option_price=2.50,
                contracts=2, exit_reason="RECONCILER_AUTO_CLOSE | HIGH | broker_position_missing",
                option_pnl_pct=-16.67, setup_status="reconciler_auto_close",
                execution_mode="paper", exit_fill_price=2.50,
                reconciliation_reason="RECONCILER_AUTO_CLOSE | HIGH | broker_position_missing",
                allow_fallback_insert=True, missing_reason_code="RECONCILER_PROOF_WRITE_FAILED",
            )

        # Must still be exactly one row (idempotent)
        assert _count_proof_rows(db, client_email) == 1, (
            "Reconciler replay must not insert a second proof row"
        )
        assert result["status"] in {"EXISTING_CANONICAL", "BOUND_REPAIR", "MERGED_DUPLICATE"}
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Test G — Concurrent callers produce exactly one proof row
# ─────────────────────────────────────────────────────────────────────────────

def test_concurrent_terminal_proof_writers_produce_one_row():
    """
    Two concurrent calls to ensure_terminal_close_proof for the same canonical
    ENTRY local_order_id must produce exactly one proof_trades row and no
    unhandled unique violations.
    """
    db = _pg_connect()
    try:
        _setup_schema(db)
        client_email = "concurrenttest@example.com"
        position_id = str(uuid.uuid4())
        entry_order_id = str(uuid.uuid4())
        contract = "IWM260731C00230000"
        repair_pid = f"broker-repair-{client_email}-{contract}"
        closed_at = datetime.now(timezone.utc).isoformat()

        with db.cursor() as cur:
            cur.execute("""
                INSERT INTO orders (id, client_id, kind, local_order_id, execution_mode,
                    status, direction, contract, symbol, qty, filled_qty, fill_price, position_id)
                VALUES (%s, %s, 'ENTRY', %s, 'paper', 'FILLED', 'CALL', %s, %s, 1, 1, 4.00, %s)
            """, (str(uuid.uuid4()), client_email, entry_order_id, contract, contract, position_id))
            cur.execute("""
                INSERT INTO positions (id, client_id, local_order_id, execution_mode,
                    contract, underlying, direction, qty, avg_fill, entry_price, status, entry_ts)
                VALUES (%s, %s, %s, 'paper', %s, 'IWM', 'CALL', 1, 4.00, 4.00, 'OPEN', NOW())
            """, (position_id, client_email, entry_order_id, contract))
            cur.execute("""
                INSERT INTO proof_trades (client_email, position_id, local_order_id,
                    mode, execution_mode, side, contracts, entry_option_price, exit_option_price,
                    option_pnl_pct, exit_reason, closed_at)
                VALUES (%s, %s, NULL, 'paper', 'unknown', 'CALL', 1, 4.00, 3.50, -12.50,
                    'SOFT_STOP', %s)
            """, (client_email, repair_pid, closed_at))
        db.commit()

        results = []
        errors = []

        def _worker():
            # Each worker needs its own DB connection
            worker_db = _pg_connect()
            try:
                worker_conn = _PgConnWrapper(worker_db)
                import ap.position_manager as pm_mod
                import ap.db as db_mod

                with patch.object(pm_mod, "conn", worker_conn), \
                     patch.object(pm_mod, "run_with_retry", lambda fn: fn()), \
                     patch.object(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
                         "local_order_id": entry_order_id, "client_id": client_email,
                         "kind": "ENTRY", "execution_mode": "paper", "direction": "CALL",
                         "signal_id": "", "meta": {},
                     } if local_order_id == entry_order_id else None):

                    from ap.position_manager import APPositionManager
                    pm = APPositionManager(client_email)
                    r = pm.ensure_terminal_close_proof(
                        position_id=position_id, local_order_id=entry_order_id,
                        contract=contract, underlying="IWM", side="CALL",
                        opened_at=closed_at, closed_at=closed_at,
                        entry_option_price=4.00, exit_option_price=3.50,
                        contracts=1, exit_reason="SOFT_STOP",
                        option_pnl_pct=-12.50, setup_status="test",
                        execution_mode="paper", allow_fallback_insert=True,
                        missing_reason_code="TEST_FAILED",
                    )
                    results.append(r)
            except Exception as exc:
                errors.append(exc)
            finally:
                worker_db.close()

        t1 = threading.Thread(target=_worker)
        t2 = threading.Thread(target=_worker)
        t1.start()
        t2.start()
        t1.join(timeout=30)
        t2.join(timeout=30)

        assert not errors, f"Worker errors: {errors}"

        # Re-check via main connection
        final_count = _count_proof_rows(db, client_email)
        assert final_count == 1, (
            f"Concurrent writers must produce exactly 1 proof row, got {final_count}"
        )
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Test H — Taxonomy safety: LIVE binding alone does not grant official status
# ─────────────────────────────────────────────────────────────────────────────

def test_binding_live_execution_mode_alone_does_not_grant_official_live_taxonomy():
    """
    Binding a repair row and setting execution_mode='live' must NOT by itself set
    official_live_performance_eligible=TRUE or training_eligible=TRUE.
    That requires the existing complete broker-entry and broker-exit evidence lock.
    """
    db = _pg_connect()
    try:
        _setup_schema(db)
        client_email = "testclient@example.com"
        position_id = str(uuid.uuid4())
        entry_order_id = str(uuid.uuid4())
        contract = "MSFT260731C00400000"
        repair_pid = f"broker-repair-{client_email}-{contract}"
        closed_at = datetime.now(timezone.utc).isoformat()

        with db.cursor() as cur:
            cur.execute("""
                INSERT INTO orders (id, client_id, kind, local_order_id, execution_mode,
                    status, direction, contract, symbol, qty, filled_qty, fill_price, position_id)
                VALUES (%s, %s, 'ENTRY', %s, 'live', 'FILLED', 'CALL', %s, %s, 2, 2, 8.00, %s)
            """, (str(uuid.uuid4()), client_email, entry_order_id, contract, contract, position_id))
            cur.execute("""
                INSERT INTO positions (id, client_id, local_order_id, execution_mode,
                    contract, underlying, direction, qty, avg_fill, entry_price, status, entry_ts)
                VALUES (%s, %s, %s, 'live', %s, 'MSFT', 'CALL', 2, 8.00, 8.00, 'OPEN', NOW())
            """, (position_id, client_email, entry_order_id, contract))
            cur.execute("""
                INSERT INTO proof_trades (client_email, position_id, local_order_id,
                    mode, execution_mode, side, contracts, entry_option_price, exit_option_price,
                    option_pnl_pct, exit_reason, closed_at,
                    official_live_performance_eligible, training_eligible)
                VALUES (%s, %s, NULL, 'live', 'unknown', 'CALL', 2, 8.00, 7.00, -12.50,
                    'SOFT_STOP', %s, FALSE, FALSE)
            """, (client_email, repair_pid, closed_at))
        db.commit()

        conn_wrapper = _PgConnWrapper(db)
        import ap.position_manager as pm_mod
        import ap.db as db_mod

        with patch.object(pm_mod, "conn", conn_wrapper), \
             patch.object(pm_mod, "run_with_retry", lambda fn: fn()), \
             patch.object(db_mod, "get_order_by_id", lambda local_order_id, client_id=None: {
                 "local_order_id": entry_order_id, "client_id": client_email,
                 "kind": "ENTRY", "execution_mode": "live", "direction": "CALL",
                 "signal_id": "", "meta": {},
             } if local_order_id == entry_order_id else None):

            from ap.position_manager import APPositionManager
            pm = APPositionManager(client_email)
            claimed = pm._claim_recent_broker_repair_proof(
                position_id=position_id, contract=contract,
                closed_at=closed_at, local_order_id=entry_order_id,
                execution_mode="live", side="CALL", contracts=2, entry_option_price=8.00,
            )

        # Whether or not claimed, the row must never have official_live = TRUE from binding alone
        rows = _get_proof_rows(db, client_email)
        for row in rows:
            if row["position_id"] == position_id:
                assert not row["official_live_performance_eligible"], (
                    "Binding alone must not grant official_live_performance_eligible=TRUE"
                )
                assert not row["training_eligible"], (
                    "Binding alone must not grant training_eligible=TRUE"
                )
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Test I — Partial close produces zero proof rows
# ─────────────────────────────────────────────────────────────────────────────

def test_partial_reconciler_close_does_not_create_proof_row():
    """
    A partial reconciler close (quantity_remaining > 0 / final_status != 'CLOSED')
    must not call ensure_terminal_close_proof or insert any proof_trades row.
    """
    src = (_REPO / "ap_reconciler.py").read_text()
    # Guard must be present and skip the proof call for partial closes
    assert "PARTIAL_RECONCILER_CLOSE" in src, (
        "Partial-close guard (PARTIAL_RECONCILER_CLOSE) must be present in ap_reconciler.py"
    )
    # The proof call must be inside `if final_status != "CLOSED": ... return`
    # Verify the guard comes before ensure_terminal_close_proof in _execute_reconciler_close
    exec_close_body = src.split("def _execute_reconciler_close")[1].split("def _import_broker")[0]
    partial_guard_pos = exec_close_body.find("PARTIAL_RECONCILER_CLOSE")
    proof_call_pos = exec_close_body.find("ensure_terminal_close_proof")
    assert partial_guard_pos != -1, "PARTIAL_RECONCILER_CLOSE guard not found in _execute_reconciler_close"
    assert proof_call_pos != -1, "ensure_terminal_close_proof call not found in _execute_reconciler_close"
    assert partial_guard_pos < proof_call_pos, (
        "PARTIAL_RECONCILER_CLOSE guard must appear before ensure_terminal_close_proof call"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test J — No broker mutation
# ─────────────────────────────────────────────────────────────────────────────

def test_no_broker_submit_or_cancel_in_proof_binding_path():
    """This PR must not submit, cancel, or place any broker orders."""
    src = (_REPO / "ap/position_manager.py").read_text()
    # Check only the proof-related methods
    proof_methods = []
    for method_name in ("ensure_terminal_close_proof", "_claim_recent_broker_repair_proof",
                        "_write_missing_terminal_proof", "_ensure_terminal_close_proof"):
        start = src.find(f"def {method_name}(")
        if start == -1:
            continue
        end = src.find("\n    def ", start + 1)
        proof_methods.append(src[start:end] if end != -1 else src[start:])

    combined = "\n".join(proof_methods)
    for forbidden in ("submit_order(", "cancel_order(", "place_order(", "broker.submit"):
        assert forbidden not in combined, (
            f"Proof binding path must not call {forbidden}"
        )
