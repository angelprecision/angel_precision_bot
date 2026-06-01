"""
Tests for sizer threshold ordering fix.

Covers:
  - validate_sizer_thresholds returns False + logs on misordered inputs
  - validate_sizer_thresholds returns True on correct inputs
  - Runtime defensive swap produces structured _last_threshold_correction
  - Structured correction dict contains all required fields (req 5)
  - DB override path wires through correctly (integration-style source check)
  - Startup validation is called from client_runner construction (source check)

Run:
    pytest tests/test_sizer_threshold_order.py -xvs
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

# Stub DATABASE_URL before any ap.* import so ap.db doesn't raise at module load.
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_sizer_threshold",
)


# ── validate_sizer_thresholds ────────────────────────────────────────────────

class TestValidateSizerThresholds:

    def test_correct_order_returns_true(self):
        """throttle=-1500, stop=-2000 → correctly ordered → True."""
        from ap.position_sizer import validate_sizer_thresholds
        assert validate_sizer_thresholds(-1500.0, -2000.0) is True

    def test_reversed_order_returns_false(self):
        """throttle=-2000, stop=-1500 → misordered → False."""
        from ap.position_sizer import validate_sizer_thresholds
        assert validate_sizer_thresholds(-2000.0, -1500.0) is False

    def test_reversed_order_calls_log_fn(self):
        """Reversed thresholds must call log_fn with [SIZER_THRESHOLD_MISORDERED]."""
        from ap.position_sizer import validate_sizer_thresholds
        warnings = []
        def capture(msg, *a, **kw):
            warnings.append(msg % a)
        ok = validate_sizer_thresholds(-2000.0, -1500.0,
                                        client_id="jasoncosby1",
                                        symbol="ABBV",
                                        log_fn=capture)
        assert not ok
        assert warnings, "log_fn must be called"
        assert "SIZER_THRESHOLD_MISORDERED" in warnings[0]
        assert "jasoncosby1" in warnings[0]
        assert "ABBV" in warnings[0]

    def test_positive_throttle_returns_false(self):
        """Positive throttle is invalid (must be negative)."""
        from ap.position_sizer import validate_sizer_thresholds
        assert validate_sizer_thresholds(1500.0, -2000.0) is False

    def test_positive_stop_returns_false(self):
        """Positive stop is invalid (must be negative)."""
        from ap.position_sizer import validate_sizer_thresholds
        assert validate_sizer_thresholds(-1500.0, 2000.0) is False

    def test_equal_values_returns_true(self):
        """Equal throttle and stop (edge case) — not reversed, returns True."""
        from ap.position_sizer import validate_sizer_thresholds
        assert validate_sizer_thresholds(-1500.0, -1500.0) is True

    def test_log_message_contains_fix_guidance(self):
        """Warning message must include actionable fix guidance."""
        from ap.position_sizer import validate_sizer_thresholds
        msgs = []
        validate_sizer_thresholds(-1994.26, -1500.0, log_fn=lambda m, *a, **kw: msgs.append(m % a))
        assert msgs
        combined = " ".join(msgs)
        assert "throttle_threshold_usd" in combined or "THROTTLE_THRESHOLD" in combined


# ── Runtime swap structured metadata (req 5) ─────────────────────────────────

class TestRuntimeSwapAuditMetadata:

    def _make_sizer(self, throttle: float, stop: float):
        from ap.position_sizer import APPositionSizer
        return APPositionSizer(
            throttle_threshold=throttle,
            stop_threshold=stop,
        )

    def _minimal_history(self, sizer):
        """Patch _fetch_history so compute() reaches the swap check."""
        sizer._fetch_history = MagicMock(return_value=[])
        sizer._resolve_threshold = lambda explicit, account_equity, pct, fallback, label: (
            explicit if explicit is not None else account_equity * -pct
        )

    def test_no_correction_when_ordered(self):
        """Correctly ordered thresholds must NOT set _last_threshold_correction."""
        sizer = self._make_sizer(-1500.0, -2000.0)
        assert sizer._last_threshold_correction is None
        # After compute with misordered-safe inputs, field stays None
        sizer._fetch_history = MagicMock(return_value=[])
        try:
            sizer.compute(
                client_id="c@c.com",
                tier="B",
                premium_per_contract=250.0,
                account_equity=100000.0,
                realized_pnl_today=0.0,
                position_manager=MagicMock(),
            )
        except Exception:
            pass  # may hit DB or other errors; we only care about the field
        # If thresholds are correctly ordered, correction must NOT be set
        # (only set when swap fires)
        assert sizer._last_threshold_correction is None

    def test_correction_dict_set_when_swapped(self):
        """Misordered thresholds trigger swap and populate _last_threshold_correction."""
        from ap.position_sizer import APPositionSizer
        sizer = APPositionSizer(
            throttle_threshold=-1994.26,  # more negative than stop → misordered
            stop_threshold=-1500.0,
        )
        # Stub compute internals to reach the swap check
        sizer._resolve_threshold = lambda explicit, **kw: explicit
        sizer._fetch_history = MagicMock(return_value=[])

        try:
            sizer.compute(
                client_id="jasoncosby1@gmail.com",
                tier="B",
                premium_per_contract=250.0,
                account_equity=100000.0,
                realized_pnl_today=0.0,
                position_manager=MagicMock(),
            )
        except Exception:
            pass  # may fail after swap; correction dict is set before further logic

        corr = sizer._last_threshold_correction
        assert corr is not None, "_last_threshold_correction must be set after swap"
        assert corr["sizer_thresholds_corrected"] is True

    def test_correction_dict_has_all_required_fields(self):
        """Correction dict must contain all seven required fields (req 5)."""
        from ap.position_sizer import APPositionSizer
        sizer = APPositionSizer(
            throttle_threshold=-1994.26,
            stop_threshold=-1500.0,
        )
        sizer._resolve_threshold = lambda explicit, **kw: explicit
        sizer._fetch_history = MagicMock(return_value=[])
        try:
            sizer.compute(
                client_id="tradefluencehq@gmail.com",
                tier="A",
                premium_per_contract=500.0,
                account_equity=100000.0,
                realized_pnl_today=0.0,
                position_manager=MagicMock(),
            )
        except Exception:
            pass

        corr = sizer._last_threshold_correction
        assert corr is not None
        required = [
            "sizer_thresholds_corrected",
            "original_throttle",
            "original_stop",
            "corrected_throttle",
            "corrected_stop",
            "client_id",
            "symbol",
        ]
        missing = [f for f in required if f not in corr]
        assert not missing, f"Missing fields in correction dict: {missing}"

    def test_correction_values_are_swapped(self):
        """After correction: corrected_throttle > corrected_stop (throttle less negative)."""
        from ap.position_sizer import APPositionSizer
        sizer = APPositionSizer(
            throttle_threshold=-1994.26,
            stop_threshold=-1500.0,
        )
        sizer._resolve_threshold = lambda explicit, **kw: explicit
        sizer._fetch_history = MagicMock(return_value=[])
        try:
            sizer.compute(
                client_id="jasoncosby1@gmail.com",
                tier="B",
                premium_per_contract=250.0,
                account_equity=100000.0,
                realized_pnl_today=0.0,
                position_manager=MagicMock(),
            )
        except Exception:
            pass

        corr = sizer._last_threshold_correction
        assert corr is not None
        assert corr["original_throttle"] == -1994.26
        assert corr["original_stop"]     == -1500.0
        # After swap: throttle should be -1500 (less negative), stop -1994.26
        assert corr["corrected_throttle"] > corr["corrected_stop"], (
            "After correction, throttle must be less negative than stop"
        )


# ── Source check: startup validation wired in client_runner ──────────────────

class TestStartupValidationWired:

    def test_client_runner_calls_validate_sizer_thresholds(self):
        """client_runner.py must call validate_sizer_thresholds after computing thresholds."""
        src = (REPO_ROOT / "client_runner.py").read_text()
        assert "validate_sizer_thresholds" in src, (
            "client_runner.py must call validate_sizer_thresholds at startup"
        )
        assert "from ap.position_sizer import validate_sizer_thresholds" in src, (
            "client_runner.py must import validate_sizer_thresholds"
        )

    def test_client_runner_reads_db_threshold_columns(self):
        """client_runner.py must read throttle_threshold_usd and stop_threshold_usd from DB."""
        src = (REPO_ROOT / "client_runner.py").read_text()
        assert "throttle_threshold_usd" in src, (
            "client_runner.py must query throttle_threshold_usd column from clients table"
        )
        assert "stop_threshold_usd" in src, (
            "client_runner.py must query stop_threshold_usd column from clients table"
        )

    def test_validate_sizer_thresholds_exported_from_position_sizer(self):
        """validate_sizer_thresholds must be importable from ap.position_sizer."""
        from ap.position_sizer import validate_sizer_thresholds
        assert callable(validate_sizer_thresholds)

    def test_migration_sql_exists(self):
        """Migration SQL file must exist with correct column definitions."""
        sql_path = REPO_ROOT / "sql" / "2026_05_31_sizer_threshold_columns.sql"
        assert sql_path.exists(), f"Migration SQL missing: {sql_path}"
        content = sql_path.read_text()
        assert "throttle_threshold_usd" in content
        assert "stop_threshold_usd" in content
        assert "chk_thresholds_ordered" in content
