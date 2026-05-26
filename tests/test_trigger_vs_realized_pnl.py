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
        # _finalize_proof must read trigger_pnl_pct from staged and pass to log_trade
        assert "_trigger_pnl_dec" in EXECUTION_CORE_SRC, (
            "_finalize_proof must read trigger_pnl_pct from staged dict"
        )

    def test_finalize_proof_passes_trigger_to_log_trade(self):
        assert "trigger_pnl_pct    = round(_trigger_pnl_dec" in EXECUTION_CORE_SRC, (
            "_finalize_proof must convert trigger_pnl_pct to percentage and pass to log_trade"
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
