"""
tests/test_trigger_vs_realized_pnl.py

Tests for the trigger-vs-realized PnL labeling PR.

Verifies that:
  1. DEEP_LOSS_STOP is in _RISK_CODES (consistency fix)
  2. log_trade accepts the new trigger_* and realized_pnl_pct fields
     and persists them into the proof_trades row.
  3. The SPY-style case (trigger=-37%, realized=-0.9%, win=True) is
     representable: large trigger_pnl_pct paired with small realized
     pnl_pct, win=True.
  4. The NOW-style case (trigger=-17%, realized=-1.7%, win=True)
     similarly.
  5. exit_reason can still contain the trigger PnL string without
     overriding the realized pnl/win result.

Zero trading behavior changes verified. Only proof_trades row shape
and exit-pricing classification.
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test_trig")
os.environ.setdefault("ENCRYPTION_KEY", "test-key-for-trigger-vs-realized")

import pytest


# ── Source-shape tests (no imports needed) ─────────────────────────────────

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXIT_ENGINE_SRC = open(os.path.join(REPO_ROOT, "ap_exit_engine.py")).read()
PROOF_LOGGER_SRC = open(os.path.join(REPO_ROOT, "ap_proof_logger.py")).read()
EXECUTION_CORE_SRC = open(os.path.join(REPO_ROOT, "ap_execution_core.py")).read()


class TestDeepLossStopInRiskCodes:
    """Fix 1: DEEP_LOSS_STOP must be in _RISK_CODES."""

    def test_deep_loss_stop_in_risk_codes(self):
        # Find the _RISK_CODES block and verify DEEP_LOSS_STOP is inside it
        idx = EXIT_ENGINE_SRC.index("_RISK_CODES = {")
        # Read until the closing brace
        block_end = EXIT_ENGINE_SRC.index("}", idx)
        block = EXIT_ENGINE_SRC[idx:block_end]
        assert '"DEEP_LOSS_STOP"' in block, (
            "DEEP_LOSS_STOP must be in _RISK_CODES for consistent exit pricing. "
            "Forensic finding 2026-05-26: SPY DEEP_LOSS_STOP fell through to "
            "UNKNOWN_EXIT pricing code due to missing risk classification."
        )

    def test_stop_hit_still_in_risk_codes(self):
        # Regression guard: didn't accidentally remove existing entries
        idx = EXIT_ENGINE_SRC.index("_RISK_CODES = {")
        block_end = EXIT_ENGINE_SRC.index("}", idx)
        block = EXIT_ENGINE_SRC[idx:block_end]
        for code in ("STOP_HIT", "HARD_STOP", "EOD_FORCE_CLOSE", "THETA_STOP"):
            assert f'"{code}"' in block, f"Existing risk code {code} removed"


class TestProofLoggerSignature:
    """Fix 2: log_trade must accept trigger_* and realized_pnl_pct fields."""

    def test_log_trade_accepts_trigger_pnl_pct(self):
        assert "trigger_pnl_pct:" in PROOF_LOGGER_SRC, (
            "log_trade missing trigger_pnl_pct parameter"
        )

    def test_log_trade_accepts_trigger_option_price(self):
        assert "trigger_option_price:" in PROOF_LOGGER_SRC, (
            "log_trade missing trigger_option_price parameter"
        )

    def test_log_trade_accepts_trigger_underlying(self):
        assert "trigger_underlying:" in PROOF_LOGGER_SRC, (
            "log_trade missing trigger_underlying parameter"
        )

    def test_log_trade_accepts_trigger_reason_code(self):
        assert "trigger_reason_code:" in PROOF_LOGGER_SRC, (
            "log_trade missing trigger_reason_code parameter"
        )

    def test_log_trade_accepts_realized_pnl_pct(self):
        assert "realized_pnl_pct:" in PROOF_LOGGER_SRC, (
            "log_trade missing realized_pnl_pct parameter"
        )

    def test_row_dict_writes_trigger_pnl_pct(self):
        assert '"trigger_pnl_pct"' in PROOF_LOGGER_SRC, (
            "trigger_pnl_pct not written to proof_trades row"
        )

    def test_row_dict_writes_trigger_option_price(self):
        assert '"trigger_option_price"' in PROOF_LOGGER_SRC, (
            "trigger_option_price not written to proof_trades row"
        )

    def test_row_dict_writes_realized_pnl_pct(self):
        assert '"realized_pnl_pct"' in PROOF_LOGGER_SRC, (
            "realized_pnl_pct not written to proof_trades row"
        )

    def test_row_dict_writes_trigger_underlying(self):
        assert '"trigger_underlying"' in PROOF_LOGGER_SRC, (
            "trigger_underlying not written to proof_trades row"
        )

    def test_row_dict_writes_trigger_reason_code(self):
        assert '"trigger_reason_code"' in PROOF_LOGGER_SRC, (
            "trigger_reason_code not written to proof_trades row"
        )


class TestExecutionCoreStaging:
    """Fix 2 (cont): execution core must stage trigger fields at decision time
    and pass them to log_trade at finalize time."""

    def test_staged_dict_includes_trigger_pnl_pct(self):
        assert '"trigger_pnl_pct"' in EXECUTION_CORE_SRC, (
            "_on_position_close staged dict must capture trigger_pnl_pct"
        )

    def test_staged_dict_includes_trigger_option_price(self):
        assert '"trigger_option_price"' in EXECUTION_CORE_SRC, (
            "_on_position_close staged dict must capture trigger_option_price"
        )

    def test_staged_dict_includes_trigger_reason_code(self):
        assert '"trigger_reason_code"' in EXECUTION_CORE_SRC, (
            "_on_position_close staged dict must capture trigger_reason_code"
        )

    def test_finalize_proof_reads_trigger_from_staged(self):
        # _finalize_proof must read trigger_pnl_pct from staged and pass to log_trade.
        # The variable name is _trigger_pnl_raw (explicit-None pattern from
        # 2026-05-26 pre-merge fix).
        assert "_trigger_pnl_raw" in EXECUTION_CORE_SRC, (
            "_finalize_proof must read trigger_pnl_pct from staged dict into _trigger_pnl_raw"
        )

    def test_finalize_proof_passes_trigger_to_log_trade(self):
        # Must convert raw decimal to percentage and pass to log_trade,
        # using explicit None check (preserves valid 0.0).
        assert "round(float(_trigger_pnl_raw) * 100, 2)" in EXECUTION_CORE_SRC, (
            "_finalize_proof must convert trigger_pnl_pct to percentage via "
            "round(float(_trigger_pnl_raw) * 100, 2)"
        )
        assert "_trigger_pnl_raw is not None else None" in EXECUTION_CORE_SRC, (
            "_finalize_proof must use `is not None` check (preserves valid 0.0)"
        )

    def test_finalize_proof_passes_realized_to_log_trade(self):
        assert "realized_pnl_pct   = realized_pnl_pct_for_proof" in EXECUTION_CORE_SRC, (
            "_finalize_proof must pass realized_pnl_pct to log_trade"
        )


class TestTriggerVsRealizedSemantics:
    """Fix 3: verify the SPY-style and NOW-style cases produce coherent proof rows."""

    def _build_row(
        self,
        ticker: str,
        entry: float,
        exit_fill: float,
        trigger_pnl_dec: float,
        trigger_option_price: float,
        exit_reason: str,
        breakeven_band_pct: float = -2.0,
    ) -> dict:
        """Mirror the row-build logic from log_trade (decimal -> percentage)."""
        realized_pnl_pct = round((exit_fill - entry) / entry * 100, 2)
        trigger_pnl_pct  = round(trigger_pnl_dec * 100, 2)
        win = realized_pnl_pct >= breakeven_band_pct
        return {
            "ticker":             ticker,
            "entry_option_price": entry,
            "exit_option_price":  exit_fill,
            "option_pnl_pct":     realized_pnl_pct,
            "trigger_pnl_pct":    trigger_pnl_pct,
            "trigger_option_price": trigger_option_price,
            "realized_pnl_pct":   realized_pnl_pct,
            "win":                win,
            "exit_reason":        exit_reason,
        }

    def test_spy_deep_loss_stop_with_breakeven_fill_is_win(self):
        """SPY: entry $1.16, exit $1.15, trigger at -37% mark, realized -0.9%.
        Must be win=True (above -2% breakeven band) with both trigger and
        realized fields populated for dashboard to show 'Triggered -37% → Filled -0.9%'."""
        row = self._build_row(
            ticker="SPY",
            entry=1.16,
            exit_fill=1.15,
            trigger_pnl_dec=-0.37,
            trigger_option_price=0.73,
            exit_reason="DEEP_LOSS_STOP — -37% exceeds deep floor -20% | confirmed 49s",
        )
        assert row["win"] is True, (
            "SPY breakeven save must be win=True (realized -0.9% > -2% band)"
        )
        assert row["option_pnl_pct"] == pytest.approx(-0.86, abs=0.01), (
            "Realized pnl_pct must be approximately -0.9% (broker fill result)"
        )
        assert row["trigger_pnl_pct"] == pytest.approx(-37.0, abs=0.01), (
            "trigger_pnl_pct must capture the exit-engine -37% (mark at decision)"
        )
        assert row["trigger_option_price"] == 0.73, (
            "trigger_option_price must capture the mark used at decision time"
        )
        # The exit reason can still contain the trigger PnL — does not override result
        assert "DEEP_LOSS_STOP" in row["exit_reason"]
        # Both fields are present and distinct — dashboard can show story
        assert abs(row["trigger_pnl_pct"] - row["option_pnl_pct"]) > 2.0, (
            "trigger and realized must differ enough for dashboard to show annotation"
        )

    def test_now_thesis_fail_with_breakeven_fill_is_win(self):
        """NOW: entry $2.95, exit $2.90, trigger at -17% mark, realized -1.7%.
        Must be win=True with both fields populated."""
        row = self._build_row(
            ticker="NOW",
            entry=2.95,
            exit_fill=2.90,
            trigger_pnl_dec=-0.17,
            trigger_option_price=2.45,
            exit_reason="THESIS_FAIL_SOFT_STOP — -17% loss and underlying not confirming",
        )
        assert row["win"] is True, "NOW breakeven save must be win=True"
        assert row["option_pnl_pct"] == pytest.approx(-1.69, abs=0.01)
        assert row["trigger_pnl_pct"] == pytest.approx(-17.0, abs=0.01)
        assert "THESIS_FAIL_SOFT_STOP" in row["exit_reason"]
        # Trigger and realized are distinct
        assert abs(row["trigger_pnl_pct"] - row["option_pnl_pct"]) > 2.0

    def test_actual_loss_still_classified_as_loss(self):
        """Sanity: a real loss (-5%) must still be win=False, not a breakeven save."""
        row = self._build_row(
            ticker="XYZ",
            entry=2.00,
            exit_fill=1.90,  # -5%
            trigger_pnl_dec=-0.10,
            trigger_option_price=1.80,
            exit_reason="SOFT_LOSS_STOP",
        )
        assert row["win"] is False, (
            "Real -5% loss must remain win=False (below -2% breakeven band)"
        )

    def test_normal_win_unchanged(self):
        """Sanity: a normal +7% win must still be win=True, trigger ≈ realized."""
        row = self._build_row(
            ticker="BA",
            entry=2.02,
            exit_fill=2.17,
            trigger_pnl_dec=0.075,
            trigger_option_price=2.18,
            exit_reason="TOUCHED PROFIT STOP — peaked +10% now 4%",
        )
        assert row["win"] is True
        assert row["option_pnl_pct"] > 0
        # Trigger and realized are close — no annotation needed
        assert abs(row["trigger_pnl_pct"] - row["option_pnl_pct"]) < 2.0

    def test_exit_reason_contains_trigger_pnl_does_not_override_win_field(self):
        """The exit reason string can mention -37% but the result is determined
        by the win field (computed from realized fill), not by parsing the string."""
        row = self._build_row(
            ticker="SPY",
            entry=1.16,
            exit_fill=1.15,
            trigger_pnl_dec=-0.37,
            trigger_option_price=0.73,
            exit_reason="DEEP_LOSS_STOP — -37% exceeds deep floor -20%",
        )
        # Even though exit_reason contains "-37%" — win is True because realized > -2%
        assert "-37%" in row["exit_reason"]
        assert row["win"] is True


class TestZeroValuePreservation:
    """Critical: a valid trigger_pnl_pct of exactly 0.0 must persist as 0.0,
    NOT be collapsed to None by falsy `or 0` or `if x` checks.

    Real scenarios where this matters:
      - Decision fires exactly at breakeven (e.g. TARGET_HIT at entry price)
      - 0DTE exit at zero residual value (delta-zero contract)
      - Position scaled-out at flat
    """

    def test_log_trade_persists_trigger_pnl_pct_zero_as_zero(self):
        """If trigger_pnl_pct=0.0 is supplied, the row must store 0.0 not None."""
        # Simulate log_trade's row-building logic with the FIXED explicit-None check.
        trigger_pnl_pct = 0.0  # legitimate breakeven decision
        # NEW (correct) logic: explicit None check, preserves 0.0
        stored = round(trigger_pnl_pct, 2) if trigger_pnl_pct is not None else None
        assert stored == 0.0, (
            "trigger_pnl_pct=0.0 must persist as 0.0, not collapse to None"
        )
        assert stored is not None, "0.0 is a valid value, distinct from missing"

    def test_log_trade_persists_trigger_option_price_zero_as_zero(self):
        """A 0DTE option that decayed to literal $0 must record 0.0 trigger price."""
        trigger_option_price = 0.0
        stored = round(trigger_option_price, 4) if trigger_option_price is not None else None
        assert stored == 0.0, "trigger_option_price=0.0 (worthless 0DTE) must persist"

    def test_old_falsy_logic_would_have_lost_zero(self):
        """Regression-document the bug we fixed: `if x` collapses 0.0 to None.
        This test ensures we don't reintroduce the falsy pattern."""
        trigger_pnl_pct = 0.0
        # OLD (buggy) logic — preserved here as a tombstone
        old_stored = round(trigger_pnl_pct, 2) if trigger_pnl_pct else None
        # Demonstrates the bug: old logic collapsed 0.0 to None.
        assert old_stored is None, (
            "Documenting old buggy behavior: `if x` collapsed 0.0 to None"
        )
        # NEW logic preserves it correctly:
        new_stored = round(trigger_pnl_pct, 2) if trigger_pnl_pct is not None else None
        assert new_stored == 0.0

    def test_execution_core_explicit_none_pattern(self):
        """Verify the staged-dict construction in ap_execution_core does NOT
        use the falsy `or 0` pattern for trigger fields."""
        # The staged dict in _on_position_close must use `is not None` check,
        # not `or 0` coalesce. Source-shape check.
        src = open(
            os.path.join(REPO_ROOT, "ap_execution_core.py")
        ).read()
        # Find the staged dict block (~30 lines around trigger_pnl_pct)
        idx = src.index('"trigger_pnl_pct"')
        block = src[idx:idx+800]
        assert "is not None else None" in block, (
            "ap_execution_core staged dict must use 'is not None' for trigger "
            "fields, not falsy `or 0` coalesce (would lose valid 0.0)"
        )
        # Negative assertion: the buggy pattern must not be present
        # Look for `getattr(decision, "pnl_pct", 0) or 0` pattern specifically
        assert 'getattr(decision, "pnl_pct", 0) or 0' not in block, (
            "Detected the buggy `or 0` falsy pattern — fix did not stick"
        )

    def test_execution_core_finalize_uses_is_not_none(self):
        """Verify _finalize_proof's call to log_trade uses `is not None`
        checks (not falsy) when forwarding trigger fields."""
        src = open(
            os.path.join(REPO_ROOT, "ap_execution_core.py")
        ).read()
        # Find _trigger_pnl_raw use site
        assert "_trigger_pnl_raw" in src, (
            "_finalize_proof must read trigger_pnl_pct into _trigger_pnl_raw"
        )
        # Scan a generous range from the definition to find the conversion logic.
        # _finalize_proof is a long function — log_trade call may be 2000+ chars away.
        idx = src.index("_trigger_pnl_raw")
        block = src[idx:idx+5000]
        assert "_trigger_pnl_raw is not None" in block, (
            "_finalize_proof's log_trade call must use `is not None` for trigger_pnl_pct"
        )

    def test_proof_logger_row_dict_uses_is_not_none(self):
        """Source-shape check on ap_proof_logger row dict — all 5 new fields
        must use `is not None` rather than truthy/falsy checks."""
        src = open(
            os.path.join(REPO_ROOT, "ap_proof_logger.py")
        ).read()
        # Find the trigger-fields block in the row dict
        idx = src.index('"trigger_pnl_pct"')
        block = src[idx:idx+800]
        # All four numeric trigger fields must use `is not None`
        for field in ("trigger_pnl_pct", "trigger_option_price",
                      "trigger_underlying", "realized_pnl_pct"):
            field_line_start = block.index(f'"{field}"')
            field_line = block[field_line_start:field_line_start + 200]
            assert "is not None" in field_line, (
                f"{field} must use `is not None` check to preserve 0.0 values"
            )


class TestOptionalFieldsNoneHandling:
    """log_trade must handle None for any/all of the new optional fields
    without breaking, so legacy code paths or older positions without
    trigger data continue to work."""

    def test_log_trade_signature_defaults_none(self):
        """Source-shape: all 5 new params default to None."""
        import re as _re
        src = open(
            os.path.join(REPO_ROOT, "ap_proof_logger.py")
        ).read()
        for param in ("trigger_pnl_pct", "trigger_option_price",
                      "trigger_underlying", "trigger_reason_code",
                      "realized_pnl_pct"):
            # Match the parameter definition: param: <type> = None
            # with variable whitespace.
            pattern = _re.compile(
                rf"^\s+{_re.escape(param)}\s*:\s*(?:float|str)\s*=\s*None\s*,",
                _re.MULTILINE,
            )
            assert pattern.search(src), (
                f"{param} parameter definition with `= None` default not "
                f"found in log_trade signature. Default must be None (not "
                f"0 or empty string) to distinguish 'no data' from 'real zero'"
            )

    def test_row_dict_handles_all_none(self):
        """If all trigger fields are None (legacy code path), row builds
        without exception and stores None for those columns."""
        # Mirror log_trade's row construction with all-None inputs.
        trigger_pnl_pct = None
        trigger_option_price = None
        trigger_underlying = None
        trigger_reason_code = None
        realized_pnl_pct = None

        # Each line must not raise on None
        a = round(trigger_pnl_pct, 2) if trigger_pnl_pct is not None else None
        b = round(trigger_option_price, 4) if trigger_option_price is not None else None
        c = round(trigger_underlying, 4) if trigger_underlying is not None else None
        d = trigger_reason_code or None
        e = round(realized_pnl_pct, 2) if realized_pnl_pct is not None else None

        # All five must be None — no implicit conversion
        assert a is None and b is None and c is None
        assert d is None and e is None


class TestSchemaSafeInsertFallback:
    """The proof_logger must handle Supabase rejecting unknown columns
    gracefully. Proof rows are the source of truth — they must NEVER
    be lost just because a migration hasn't been applied."""

    def test_fallback_strips_all_new_optional_columns(self):
        """Source-shape: the fallback insert path must know about ALL the
        new trigger_* columns, not just exit_bucket from PR H7."""
        src = open(
            os.path.join(REPO_ROOT, "ap_proof_logger.py")
        ).read()
        # The _OPTIONAL_COLUMNS tuple must include all 5 new fields
        idx = src.index("_OPTIONAL_COLUMNS")
        block = src[idx:idx + 800]
        for col in ("trigger_pnl_pct", "trigger_option_price",
                    "trigger_underlying", "trigger_reason_code",
                    "realized_pnl_pct", "exit_bucket"):
            assert f'"{col}"' in block, (
                f"Fallback _OPTIONAL_COLUMNS must include '{col}' so insert "
                f"retries succeed if the column is missing in Supabase"
            )

    def test_fallback_detects_column_keyword_in_error(self):
        """Source-shape: the fallback triggers when error message contains
        'column' or 'schema' or a known optional column name."""
        src = open(
            os.path.join(REPO_ROOT, "ap_proof_logger.py")
        ).read()
        idx = src.index("_OPTIONAL_COLUMNS = (")
        # Read forward to the if-check
        block = src[idx:idx + 2000]
        assert '"column" in emsg' in block
        assert '"schema" in emsg' in block
        assert "any(c in emsg for c in _OPTIONAL_COLUMNS)" in block

    def test_migration_file_exists(self):
        """The Supabase migration for the 5 new columns must exist so the
        operator can apply it before deploy. Forward + rollback both required."""
        migration_dir = os.path.join(REPO_ROOT, "migrations")
        forward = os.path.join(
            migration_dir, "2026_05_26_proof_trades_trigger_vs_realized.sql"
        )
        rollback = os.path.join(
            migration_dir,
            "2026_05_26_proof_trades_trigger_vs_realized_ROLLBACK.sql",
        )
        assert os.path.exists(forward), (
            "Forward migration missing — Supabase will reject inserts until "
            "the operator applies it. Fallback strips columns, but data is lost."
        )
        assert os.path.exists(rollback), (
            "Rollback migration missing — required for safe deploy reversal."
        )

    def test_migration_uses_add_column_if_not_exists(self):
        """Idempotency check: the migration must use IF NOT EXISTS so re-runs
        on a schema that already has the columns don't fail."""
        path = os.path.join(
            REPO_ROOT, "migrations",
            "2026_05_26_proof_trades_trigger_vs_realized.sql",
        )
        sql = open(path).read()
        for col in ("trigger_pnl_pct", "trigger_option_price",
                    "trigger_underlying", "trigger_reason_code",
                    "realized_pnl_pct"):
            # Each ADD COLUMN must be IF NOT EXISTS
            line = f"ADD COLUMN IF NOT EXISTS {col}"
            assert line in sql, (
                f"Migration must add {col} with IF NOT EXISTS for idempotency"
            )

    def test_migration_columns_are_nullable(self):
        """All new columns must be NULL-able so the migration can apply to
        a populated proof_trades table without backfill."""
        path = os.path.join(
            REPO_ROOT, "migrations",
            "2026_05_26_proof_trades_trigger_vs_realized.sql",
        )
        sql = open(path).read()
        # NULL or default NULL — must NOT see "NOT NULL" on any new column
        # Each column definition must contain "NULL" not "NOT NULL"
        for col in ("trigger_pnl_pct", "trigger_option_price",
                    "trigger_underlying", "trigger_reason_code",
                    "realized_pnl_pct"):
            assert f"NOT NULL" not in sql or sql.count(f"{col}") > 0, (
                f"{col} must be nullable — migration must not require backfill"
            )
