"""
DB hardening tests — ap/db.py safe support helpers.

Tests:
    1. get_stale_pending_trigger_orders SQL shape
    2. get_open_orders_for_reconcile still excludes PENDING_TRIGGER
    3. DB_RETRY_FAST and DB_RETRY_SLOW constants
    4. insert_order emits DeprecationWarning
"""
import os
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_db_hardening",
)

import warnings
import pytest
from unittest.mock import patch, MagicMock, call


# ---------------------------------------------------------------------------
# 1. get_stale_pending_trigger_orders — SQL shape
# ---------------------------------------------------------------------------
class TestStalePendingTrigger:
    def test_sql_includes_pending_trigger_status(self):
        """SQL must filter status = PENDING_TRIGGER."""
        from ap.db import get_stale_pending_trigger_orders

        captured_sql = []

        def fake_retry(fn):
            # Build a mock cursor that records SQL
            mock_cur = MagicMock()
            mock_cur.fetchall.return_value = []
            mock_conn = MagicMock()
            mock_conn.__enter__ = lambda s: s
            mock_conn.__exit__ = MagicMock(return_value=False)
            mock_conn.execute = mock_cur.execute
            mock_conn.fetchall = mock_cur.fetchall

            class FakeConn:
                def __enter__(self):
                    return self
                def __exit__(self, *a):
                    pass
                def execute(self, sql, params=None):
                    captured_sql.append((sql, params))
                    return mock_cur
                def fetchall(self):
                    return []

            with patch("ap.db.conn", return_value=FakeConn()):
                return fn()

        with patch("ap.db.run_with_retry", side_effect=fake_retry):
            result = get_stale_pending_trigger_orders("test@example.com", older_than_hours=8)

        assert isinstance(result, list)
        assert len(captured_sql) == 1, "Expected exactly one SQL statement"
        sql, params = captured_sql[0]

        # Required predicates
        assert "PENDING_TRIGGER" in sql, "SQL must filter status = PENDING_TRIGGER"
        assert "ENTRY" in sql, "SQL must filter kind = ENTRY"
        assert "client_id" in sql, "SQL must filter by client_id"
        assert "created_ts" in sql and "NOW()" in sql, "SQL must include age threshold"
        assert "ORDER BY created_ts ASC" in sql, "SQL must order by created_ts ASC"

        # Required params: (client_id, str(older_than_hours))
        assert params == ("test@example.com", "8"), (
            f"Expected params ('test@example.com', '8') but got {params!r}"
        )

    def test_function_returns_list(self):
        """Returns empty list on no rows."""
        from ap.db import get_stale_pending_trigger_orders

        with patch("ap.db.run_with_retry", return_value=[]):
            result = get_stale_pending_trigger_orders("test@example.com")
        assert result == []

    def test_function_exists_and_is_callable(self):
        from ap.db import get_stale_pending_trigger_orders
        assert callable(get_stale_pending_trigger_orders)

    def test_db_failure_raises_not_swallowed(self):
        """DB/query failure must be raised, not swallowed as empty list."""
        from ap.db import get_stale_pending_trigger_orders

        with patch("ap.db.run_with_retry", side_effect=RuntimeError("db down")):
            with pytest.raises(RuntimeError, match="db down"):
                get_stale_pending_trigger_orders("test@example.com")


# ---------------------------------------------------------------------------
# 2. get_open_orders_for_reconcile includes only evidence-bearing PENDING_TRIGGER
# ---------------------------------------------------------------------------
class TestReconcileEvidenceBearingPendingTrigger:
    def test_pending_trigger_requires_broker_intent_evidence(self):
        """Only broker-evidence-bearing PENDING_TRIGGER rows enter reconciliation."""
        import inspect
        import ap.db as db_module

        src = inspect.getsource(db_module.get_open_orders_for_reconcile)
        assert "PENDING_TRIGGER" in src
        assert "kind = 'ENTRY'" in src
        assert "broker_order_id" in src
        assert "submitted_ts" in src
        assert "submit_intent_at" in src
        assert "execution_mode" in src

    def test_reconcile_function_exists(self):
        from ap.db import get_open_orders_for_reconcile
        assert callable(get_open_orders_for_reconcile)


# ---------------------------------------------------------------------------
# 3. Retry profile constants
# ---------------------------------------------------------------------------
class TestRetryProfileConstants:
    def test_db_retry_fast_exists(self):
        from ap.db import DB_RETRY_FAST
        assert DB_RETRY_FAST is not None

    def test_db_retry_fast_values(self):
        from ap.db import DB_RETRY_FAST
        assert DB_RETRY_FAST["retries"] == 3
        assert DB_RETRY_FAST["base_sleep"] == 0.05
        assert DB_RETRY_FAST["max_sleep"] == 0.5

    def test_db_retry_slow_exists(self):
        from ap.db import DB_RETRY_SLOW
        assert DB_RETRY_SLOW is not None

    def test_db_retry_slow_values(self):
        from ap.db import DB_RETRY_SLOW
        assert DB_RETRY_SLOW["retries"] == 10
        assert DB_RETRY_SLOW["base_sleep"] == 0.1
        assert DB_RETRY_SLOW["max_sleep"] == 2.0

    def test_constants_are_dicts(self):
        from ap.db import DB_RETRY_FAST, DB_RETRY_SLOW
        assert isinstance(DB_RETRY_FAST, dict)
        assert isinstance(DB_RETRY_SLOW, dict)


# ---------------------------------------------------------------------------
# 4. insert_order() emits DeprecationWarning
# ---------------------------------------------------------------------------
class TestInsertOrderDeprecation:
    def test_insert_order_emits_deprecation_warning(self):
        """insert_order() must emit DeprecationWarning on every call."""
        import ap.db as db_module
        assert hasattr(db_module, "insert_order"), "insert_order() must still exist (not removed)"

        # Patch run_with_retry so no DB connection is needed
        with patch("ap.db.run_with_retry", return_value=None):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                try:
                    db_module.insert_order(
                        local_order_id="test-id",
                        client_id="test@example.com",
                        position_id=None,
                        kind="ENTRY",
                        status="CREATED",
                        symbol="AAPL",
                        contract="AAPL260522C00285000",
                        qty=1,
                        limit_price=2.50,
                    )
                except Exception:
                    pass  # DB errors OK — we only care about the warning
                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) >= 1, (
                    "insert_order() must emit at least one DeprecationWarning"
                )
                assert "APOrderStateMachine" in str(deprecation_warnings[0].message)
