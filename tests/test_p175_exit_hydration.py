"""
tests/test_p175_exit_hydration.py
PR #175 — P0: Hydrate live exit context + live soft-exit guard

7 required test cases:
  1. NKE-style live PUT with underlying_entry missing → DATA_DEGRADED_HOLD, no broker sell.
  2. RIVN-style never-green with no_underlying_data → DATA_DEGRADED_HOLD, no broker sell.
  3. PUT down on option but underlying still below stop → UNDERLYING_CONFIRMING (HOLD).
  4. PUT with fresh underlying above stop for 2 polls → soft exit PROCEED.
  5. CALL inverse logic — confirming if above trigger, blocked if below stop.
  6. proof_trades preserves real positions.id (not broker-repair-* synthetic).
  7. execution_mode remains 'live', never 'unknown', for live positions.

Additional tests cover:
  - Option quote staleness blocking exit.
  - Spread-insane option quote blocking exit.
  - Minimum hold guard (< 4 min blocked, ≥ 4 min allowed).
  - Hard disaster stop bypasses hold + poll gates.
  - as_proof_fields() returns all required proof_trades columns.

Author: Angel Precision Intelligence
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from ap.exit_context_hydrator import ExitContext, ExitContextHydrator
from ap.live_exit_guard import (
    MINIMUM_LIVE_HOLD_SECS,
    OPTION_LOSS_CONFIRM_POLLS_REQUIRED,
    LiveExitGuard,
    _clear_option_loss_poll,
    _option_loss_poll_counts,
)

# ---------------------------------------------------------------------------
# Helpers to build ExitContext fixtures
# ---------------------------------------------------------------------------

def _pid() -> str:
    return str(uuid.uuid4())


def _make_ctx(
    *,
    position_id: Optional[str] = None,
    execution_mode: str = "live",
    direction: str = "PUT",
    contract: str = "NKE250620P00085000",
    underlying_symbol: str = "NKE",
    underlying_entry: Optional[float] = 88.50,
    current_underlying: Optional[float] = 86.00,
    stop_underlying: Optional[float] = 90.00,
    trigger_underlying: Optional[float] = 87.00,
    option_bid: Optional[float] = 1.10,
    option_ask: Optional[float] = 1.30,
    option_mid: Optional[float] = 1.20,
    option_quote_age_secs: Optional[float] = 5.0,
    option_spread_sane: bool = True,
    entry_price: Optional[float] = 1.50,
    hold_since: Optional[datetime] = None,
    broker_entry_order_id: Optional[str] = "BRK-001",
) -> ExitContext:
    """Factory for ExitContext test fixtures."""
    if position_id is None:
        position_id = _pid()
    if hold_since is None:
        hold_since = datetime.now(timezone.utc) - timedelta(seconds=MINIMUM_LIVE_HOLD_SECS + 60)

    return ExitContext(
        position_id=position_id,
        client_id="client-live-001",
        client_email="jasoncosby1@gmail.com",
        execution_mode=execution_mode,
        contract=contract,
        direction=direction,
        underlying_symbol=underlying_symbol,
        underlying_entry=underlying_entry,
        entry_price=entry_price,
        entry_bid=1.40,
        entry_ask=1.60,
        entry_mid=1.50,
        entry_price_source="TRADIER_SUBMIT",
        broker_entry_order_id=broker_entry_order_id,
        broker_entry_fill_ts="2025-06-20T09:32:00Z",
        watcher_audit={"underlying_price_at_trigger": underlying_entry},
        signal_id="sig-001",
        stop_underlying=stop_underlying,
        trigger_underlying=trigger_underlying,
        current_underlying=current_underlying,
        underlying_quote_age_secs=5.0,
        option_bid=option_bid,
        option_ask=option_ask,
        option_mid=option_mid,
        option_quote_age_secs=option_quote_age_secs,
        option_spread_sane=option_spread_sane,
        hold_since=hold_since,
    )


# ---------------------------------------------------------------------------
# Test 1 — NKE-style live PUT with underlying_entry missing
# ---------------------------------------------------------------------------

class TestNKEStyleMissingUnderlyingEntry:
    """
    Scenario: Live PUT on NKE.  underlying_entry was not captured at entry time
    (positions.underlying_entry = NULL, orders.meta has no watcher_audit).
    Exit engine wants to emit SOFT_LOSS.
    Expected: DATA_DEGRADED_HOLD — broker exit must NOT be submitted.
    """

    def test_data_degraded_hold_on_missing_underlying_entry(self):
        ctx = _make_ctx(underlying_entry=None)
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="SOFT_LOSS")

        assert decision.outcome == "DATA_DEGRADED_HOLD", (
            f"Expected DATA_DEGRADED_HOLD, got {decision.outcome}"
        )
        assert decision.block is True, "block must be True — broker exit must NOT fire"
        assert "underlying_entry" in decision.missing_fields, (
            "missing_fields must list 'underlying_entry'"
        )

    def test_data_degraded_hold_blocks_thesis_fail_soft_stop(self):
        ctx = _make_ctx(underlying_entry=None)
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="THESIS_FAIL_SOFT_STOP")
        assert decision.block is True
        assert decision.outcome == "DATA_DEGRADED_HOLD"

    def test_data_degraded_hold_blocks_never_green_stop(self):
        ctx = _make_ctx(underlying_entry=None)
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="NEVER_GREEN_STOP")
        assert decision.block is True
        assert decision.outcome == "DATA_DEGRADED_HOLD"

    def test_data_degraded_does_not_block_non_soft_exit(self):
        """A MANUAL_EXIT code (not in SOFT_EXIT_CODES) bypasses the guard entirely."""
        ctx = _make_ctx(underlying_entry=None)
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="MANUAL_EXIT")
        # MANUAL_EXIT is not a soft code — guard says PROCEED
        assert decision.outcome == "PROCEED"
        assert decision.block is False


# ---------------------------------------------------------------------------
# Test 2 — RIVN-style never-green with no_underlying_data
# ---------------------------------------------------------------------------

class TestRIVNStyleNoUnderlyingData:
    """
    Scenario: Live PUT on RIVN.  Both underlying_entry and current_underlying
    are missing — positions.underlying_entry = NULL, Polygon returned nothing.
    Exit engine wants to emit NEVER_GREEN_STOP.
    Expected: DATA_DEGRADED_HOLD.
    """

    def test_never_green_blocked_when_both_underlyings_missing(self):
        ctx = _make_ctx(
            contract="RIVN250620P00010000",
            underlying_symbol="RIVN",
            underlying_entry=None,
            current_underlying=None,
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="NEVER_GREEN_STOP")

        assert decision.outcome == "DATA_DEGRADED_HOLD"
        assert decision.block is True
        missing = decision.missing_fields
        assert "underlying_entry" in missing
        # current_underlying is also required
        assert any("current_underlying" in f or "option_mark" in f for f in missing) or \
               "current_underlying" in missing, (
            f"Expected current_underlying in missing, got: {missing}"
        )

    def test_no_option_data_also_degraded(self):
        ctx = _make_ctx(
            underlying_entry=None,
            current_underlying=None,
            option_bid=None,
            option_ask=None,
            option_mid=None,
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="NEVER_GREEN_STOP")
        assert decision.outcome == "DATA_DEGRADED_HOLD"
        assert decision.block is True
        assert "option_mark" in decision.missing_fields


# ---------------------------------------------------------------------------
# Test 3 — PUT down on option but underlying still below stop → HOLD
# ---------------------------------------------------------------------------

class TestPUTOptionLossButUnderlyingConfirming:
    """
    Scenario: Live PUT.  Option has lost value (current_option_mid < entry_price)
    but the underlying is still BELOW stop_underlying — the thesis is intact.
    Expected: UNDERLYING_CONFIRMING — do not exit.
    """

    def setup_method(self):
        # Clear any residual poll state
        _option_loss_poll_counts.clear()

    def test_put_underlying_below_stop_blocks_soft_exit(self):
        """
        PUT stop=90.00, current_underlying=86.00 (below stop → confirming).
        Option is down but we should HOLD.
        """
        ctx = _make_ctx(
            direction="PUT",
            stop_underlying=90.00,    # invalidated if current >= 90
            current_underlying=86.00,  # still below stop — confirming
            trigger_underlying=87.00,
            entry_price=1.50,
            option_mid=0.90,           # down 40% — but underlying is fine
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(
            ctx,
            proposed_exit_code="SOFT_LOSS",
            option_pnl_pct=-0.40,
        )
        assert decision.outcome == "UNDERLYING_CONFIRMING"
        assert decision.block is True

    def test_put_underlying_at_trigger_strongly_confirming(self):
        """Underlying at exactly trigger level — maximally confirming, must hold."""
        ctx = _make_ctx(
            direction="PUT",
            stop_underlying=90.00,
            current_underlying=87.00,  # exactly at trigger
            trigger_underlying=87.00,
            option_mid=0.80,
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="THESIS_FAIL_SOFT_STOP")
        assert decision.block is True
        assert decision.outcome == "UNDERLYING_CONFIRMING"

    def test_put_at_stop_allows_exit_after_polls_and_hold(self):
        """Underlying just crossed ABOVE stop — thesis invalidated — allow exit after confirmation."""
        pid = _pid()
        # Simulate 2 prior polls to satisfy option-loss confirmation
        _option_loss_poll_counts[pid] = OPTION_LOSS_CONFIRM_POLLS_REQUIRED - 1

        ctx = _make_ctx(
            position_id=pid,
            direction="PUT",
            stop_underlying=90.00,
            current_underlying=90.50,   # above stop → invalidated
            trigger_underlying=87.00,
            option_mid=0.30,
            entry_price=1.50,
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="SOFT_LOSS", option_pnl_pct=-0.80)
        assert decision.outcome == "PROCEED"
        assert decision.block is False


# ---------------------------------------------------------------------------
# Test 4 — PUT with fresh underlying above stop for 2 polls → PROCEED
# ---------------------------------------------------------------------------

class TestPUTTwoPolls:
    """
    Scenario: Live PUT.  Underlying has crossed stop.  Two consecutive polls
    confirm this.  Option loss is real.  Minimum hold has passed.
    Expected: PROCEED on the second poll.
    """

    def setup_method(self):
        _option_loss_poll_counts.clear()

    def test_first_poll_blocked(self):
        pid = _pid()
        ctx = _make_ctx(
            position_id=pid,
            direction="PUT",
            stop_underlying=90.00,
            current_underlying=91.00,  # above stop
            option_mid=0.30,
            entry_price=1.50,
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="SOFT_LOSS", option_pnl_pct=-0.80)
        # First poll — not yet confirmed
        assert decision.outcome == "OPTION_LOSS_UNCONFIRMED"
        assert decision.block is True

    def test_second_poll_proceeds(self):
        pid = _pid()
        # Pre-seed one poll
        _option_loss_poll_counts[pid] = OPTION_LOSS_CONFIRM_POLLS_REQUIRED - 1

        ctx = _make_ctx(
            position_id=pid,
            direction="PUT",
            stop_underlying=90.00,
            current_underlying=91.00,
            option_mid=0.30,
            entry_price=1.50,
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="SOFT_LOSS", option_pnl_pct=-0.80)
        # Second poll — confirmed
        assert decision.outcome == "PROCEED"
        assert decision.block is False

    def test_poll_counter_resets_when_no_longer_in_loss(self):
        pid = _pid()
        _option_loss_poll_counts[pid] = 2

        ctx = _make_ctx(
            position_id=pid,
            direction="PUT",
            stop_underlying=90.00,
            current_underlying=86.00,  # confirming again
            option_mid=1.80,
            entry_price=1.50,
        )
        guard = LiveExitGuard()
        # Underlying is now confirming — should block with UNDERLYING_CONFIRMING
        # and reset the poll counter
        guard.evaluate(ctx, proposed_exit_code="SOFT_LOSS", option_pnl_pct=0.20)
        assert _option_loss_poll_counts.get(pid, 0) == 0, (
            "poll counter should reset when option is no longer in loss"
        )


# ---------------------------------------------------------------------------
# Test 5 — CALL inverse logic
# ---------------------------------------------------------------------------

class TestCALLInverseLogic:
    """
    Scenario: Live CALL.
      - Confirming: current_underlying >= trigger (holding above entry zone).
      - Invalidated: current_underlying <= stop_underlying.
    """

    def setup_method(self):
        _option_loss_poll_counts.clear()

    def test_call_confirming_above_trigger_blocks_soft_exit(self):
        ctx = _make_ctx(
            direction="CALL",
            contract="NKE250620C00095000",
            stop_underlying=88.00,     # invalidated if current <= 88
            trigger_underlying=92.00,
            current_underlying=93.00,  # above trigger → confirming
            option_mid=1.80,
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="SOFT_LOSS")
        assert decision.outcome == "UNDERLYING_CONFIRMING"
        assert decision.block is True

    def test_call_between_stop_and_trigger_is_confirming(self):
        ctx = _make_ctx(
            direction="CALL",
            contract="NKE250620C00095000",
            stop_underlying=88.00,
            trigger_underlying=92.00,
            current_underlying=90.00,  # between stop and trigger → still confirming
            option_mid=1.20,
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="THESIS_FAIL_SOFT_STOP")
        assert decision.block is True
        assert decision.outcome == "UNDERLYING_CONFIRMING"

    def test_call_below_stop_is_invalidated_and_proceeds_after_polls(self):
        pid = _pid()
        _option_loss_poll_counts[pid] = OPTION_LOSS_CONFIRM_POLLS_REQUIRED - 1

        ctx = _make_ctx(
            position_id=pid,
            direction="CALL",
            contract="NKE250620C00095000",
            stop_underlying=88.00,
            current_underlying=87.50,  # below stop → invalidated
            option_mid=0.40,
            entry_price=1.50,
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="SOFT_LOSS", option_pnl_pct=-0.73)
        assert decision.outcome == "PROCEED"
        assert decision.block is False

    def test_call_below_stop_first_poll_blocked(self):
        pid = _pid()
        ctx = _make_ctx(
            position_id=pid,
            direction="CALL",
            contract="NKE250620C00095000",
            stop_underlying=88.00,
            current_underlying=87.50,
            option_mid=0.40,
            entry_price=1.50,
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="SOFT_LOSS", option_pnl_pct=-0.73)
        assert decision.outcome == "OPTION_LOSS_UNCONFIRMED"
        assert decision.block is True


# ---------------------------------------------------------------------------
# Test 6 — proof_trades preserves real positions.id
# ---------------------------------------------------------------------------

class TestProofTradesPreservesRealPositionID:
    """
    Verifies that ExitContextHydrator uses the DB positions.id and never
    replaces it with a broker-repair-* synthetic id, even when the caller
    passes a synthetic id as the position_id argument.

    Uses a mock DB to simulate the fallback query finding a real positions row.
    """

    REAL_POSITION_ID = str(uuid.uuid4())
    SYNTHETIC_POSITION_ID = "broker-repair-1234-5678"
    CLIENT_ID = "client-live-001"
    CONTRACT = "NKE250620P00085000"

    def _make_positions_row(self, real_id: str) -> dict:
        return {
            "id": real_id,
            "client_id": self.CLIENT_ID,
            "client_email": "jasoncosby1@gmail.com",
            "execution_mode": "live",
            "contract": self.CONTRACT,
            "direction": "PUT",
            "underlying_symbol": "NKE",
            "underlying_entry": 88.50,
            "opened_at": datetime.now(timezone.utc) - timedelta(minutes=10),
            "signal_id": None,
            "status": "open",
            "client_execution_mode": "live",
        }

    @patch("ap.exit_context_hydrator.ExitContextHydrator._fetch_underlying_quote")
    @patch("ap.exit_context_hydrator.ExitContextHydrator._fetch_option_quote")
    @patch("ap.exit_context_hydrator.ExitContextHydrator._fetch_signal")
    @patch("ap.exit_context_hydrator.ExitContextHydrator._fetch_entry_order_by_position")
    @patch("ap.exit_context_hydrator.ExitContextHydrator._fetch_position")
    def test_real_position_id_is_preserved(
        self,
        mock_fetch_pos,
        mock_fetch_order,
        mock_fetch_signal,
        mock_option_quote,
        mock_underlying_quote,
    ):
        mock_fetch_pos.return_value = self._make_positions_row(self.REAL_POSITION_ID)
        mock_fetch_order.return_value = None
        mock_fetch_signal.return_value = None
        mock_underlying_quote.return_value = (88.00, 5.0)
        mock_option_quote.return_value = (1.10, 1.30, 1.20, 5.0, True)

        hydrator = ExitContextHydrator()
        ctx = hydrator.hydrate(
            position_id=self.SYNTHETIC_POSITION_ID,
            client_id=self.CLIENT_ID,
        )

        assert ctx is not None, "hydration must succeed"
        assert ctx.position_id == self.REAL_POSITION_ID, (
            f"position_id must be the real DB id {self.REAL_POSITION_ID!r}, "
            f"not the synthetic {self.SYNTHETIC_POSITION_ID!r}. "
            f"Got: {ctx.position_id!r}"
        )
        assert not ctx.position_id.startswith("broker-repair-"), (
            "position_id must never start with 'broker-repair-' in ExitContext"
        )

    @patch("ap.exit_context_hydrator.ExitContextHydrator._fetch_underlying_quote")
    @patch("ap.exit_context_hydrator.ExitContextHydrator._fetch_option_quote")
    @patch("ap.exit_context_hydrator.ExitContextHydrator._fetch_signal")
    @patch("ap.exit_context_hydrator.ExitContextHydrator._fetch_entry_order_by_position")
    @patch("ap.exit_context_hydrator.ExitContextHydrator._fetch_position")
    def test_as_proof_fields_has_all_required_columns(
        self,
        mock_fetch_pos,
        mock_fetch_order,
        mock_fetch_signal,
        mock_option_quote,
        mock_underlying_quote,
    ):
        """as_proof_fields() must return all proof_trades columns required by PR #175."""
        mock_fetch_pos.return_value = self._make_positions_row(self.REAL_POSITION_ID)
        mock_fetch_order.return_value = {
            "id": "order-001",
            "broker_order_id": "BRK-001",
            "fill_price": 1.50,
            "filled_at": "2025-06-20T09:32:00Z",
            "kind": "ENTRY",
            "status": "filled",
            "position_id": self.REAL_POSITION_ID,
            "meta": {
                "submit_bid": 1.40,
                "submit_ask": 1.60,
                "submit_mid": 1.50,
                "entry_price_source": "TRADIER_SUBMIT",
            },
        }
        mock_fetch_signal.return_value = None
        mock_underlying_quote.return_value = (88.00, 5.0)
        mock_option_quote.return_value = (1.10, 1.30, 1.20, 5.0, True)

        hydrator = ExitContextHydrator()
        ctx = hydrator.hydrate(
            position_id=self.REAL_POSITION_ID,
            client_id=self.CLIENT_ID,
        )
        assert ctx is not None

        required_proof_fields = {
            "execution_mode",
            "underlying_entry",
            "underlying_exit",
            "exit_bid",
            "exit_ask",
            "exit_mid",
            "exit_price_source",
            "exit_pricing_tier",
            "broker_entry_order_id",
            "broker_exit_order_id",
            "broker_entry_fill_ts",
            "broker_exit_fill_ts",
        }
        proof_fields = ctx.as_proof_fields()
        missing_from_proof = required_proof_fields - set(proof_fields.keys())
        assert not missing_from_proof, (
            f"as_proof_fields() is missing required columns: {missing_from_proof}"
        )


# ---------------------------------------------------------------------------
# Test 7 — execution_mode remains 'live', never 'unknown'
# ---------------------------------------------------------------------------

class TestExecutionModeLive:
    """
    Verifies that ExitContextHydrator always resolves execution_mode to 'live'
    or 'paper' — never 'unknown'.
    """

    def _base_position_row(self, execution_mode_value):
        return {
            "id": _pid(),
            "client_id": "client-live-001",
            "client_email": "jasoncosby1@gmail.com",
            "execution_mode": execution_mode_value,
            "client_execution_mode": execution_mode_value,
            "contract": "NKE250620P00085000",
            "direction": "PUT",
            "underlying_symbol": "NKE",
            "underlying_entry": 88.50,
            "opened_at": datetime.now(timezone.utc),
            "signal_id": None,
            "status": "open",
        }

    def _hydrate_with_row(self, row: dict) -> ExitContext:
        with patch("ap.exit_context_hydrator.ExitContextHydrator._fetch_position", return_value=row), \
             patch("ap.exit_context_hydrator.ExitContextHydrator._fetch_entry_order_by_position", return_value=None), \
             patch("ap.exit_context_hydrator.ExitContextHydrator._fetch_signal", return_value=None), \
             patch("ap.exit_context_hydrator.ExitContextHydrator._fetch_underlying_quote", return_value=(88.0, 5.0)), \
             patch("ap.exit_context_hydrator.ExitContextHydrator._fetch_option_quote", return_value=(1.1, 1.3, 1.2, 5.0, True)):
            return ExitContextHydrator().hydrate(row["id"], row["client_id"])

    def test_live_position_returns_live_mode(self):
        row = self._base_position_row("live")
        ctx = self._hydrate_with_row(row)
        assert ctx is not None
        assert ctx.execution_mode == "live"
        assert ctx.is_live is True

    def test_unknown_execution_mode_defaults_to_paper(self):
        """'unknown' is not a valid mode — hydrator must default to 'paper'."""
        row = self._base_position_row("unknown")
        ctx = self._hydrate_with_row(row)
        assert ctx is not None
        assert ctx.execution_mode == "paper", (
            f"'unknown' must map to 'paper' for safety, got: {ctx.execution_mode!r}"
        )
        assert ctx.is_live is False

    def test_none_execution_mode_defaults_to_paper(self):
        row = self._base_position_row(None)
        row["client_execution_mode"] = None
        ctx = self._hydrate_with_row(row)
        assert ctx is not None
        assert ctx.execution_mode == "paper"

    def test_paper_position_is_not_live(self):
        row = self._base_position_row("paper")
        ctx = self._hydrate_with_row(row)
        assert ctx is not None
        assert ctx.execution_mode == "paper"
        assert ctx.is_live is False

    def test_guard_noop_for_paper(self):
        """Guard must be a no-op for paper positions — never blocks."""
        ctx = _make_ctx(execution_mode="paper", underlying_entry=None)  # degraded but paper
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="SOFT_LOSS")
        # Paper — guard does not apply
        assert decision.outcome == "PROCEED"
        assert decision.block is False


# ---------------------------------------------------------------------------
# Additional: minimum hold guard
# ---------------------------------------------------------------------------

class TestMinimumHoldGuard:
    """Exit engine cannot fire a live soft exit within the first 4 minutes."""

    def setup_method(self):
        _option_loss_poll_counts.clear()

    def test_blocked_within_4_minutes(self):
        ctx = _make_ctx(
            direction="PUT",
            stop_underlying=90.00,
            current_underlying=91.50,  # above stop — invalidated
            hold_since=datetime.now(timezone.utc) - timedelta(seconds=180),  # 3 min
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="SOFT_LOSS", option_pnl_pct=-0.50)
        assert decision.outcome == "MINIMUM_HOLD_NOT_MET"
        assert decision.block is True

    def test_allowed_after_4_minutes(self):
        pid = _pid()
        _option_loss_poll_counts[pid] = OPTION_LOSS_CONFIRM_POLLS_REQUIRED - 1

        ctx = _make_ctx(
            position_id=pid,
            direction="PUT",
            stop_underlying=90.00,
            current_underlying=91.50,
            hold_since=datetime.now(timezone.utc) - timedelta(seconds=MINIMUM_LIVE_HOLD_SECS + 30),
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="SOFT_LOSS", option_pnl_pct=-0.50)
        assert decision.outcome == "PROCEED"
        assert decision.block is False

    def test_hard_disaster_bypasses_minimum_hold(self):
        """HARD_DISASTER_STOP must exit regardless of hold time."""
        ctx = _make_ctx(
            direction="PUT",
            stop_underlying=90.00,
            current_underlying=91.50,
            hold_since=datetime.now(timezone.utc) - timedelta(seconds=30),  # 30s
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="HARD_DISASTER_STOP")
        assert decision.outcome == "PROCEED"
        assert decision.block is False


# ---------------------------------------------------------------------------
# Additional: option quote quality
# ---------------------------------------------------------------------------

class TestOptionQuoteQuality:
    """Option quote must be fresh and spread-sane for soft exit to proceed."""

    def setup_method(self):
        _option_loss_poll_counts.clear()

    def test_stale_option_quote_blocks_exit(self):
        ctx = _make_ctx(
            direction="PUT",
            stop_underlying=90.00,
            current_underlying=91.50,
            option_quote_age_secs=45.0,  # stale (> 30s)
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="SOFT_LOSS", option_pnl_pct=-0.50)
        assert decision.outcome == "OPTION_LOSS_UNCONFIRMED"
        assert decision.block is True

    def test_insane_spread_blocks_exit(self):
        ctx = _make_ctx(
            direction="PUT",
            stop_underlying=90.00,
            current_underlying=91.50,
            option_bid=0.10,
            option_ask=2.00,      # 95% spread — insane
            option_mid=1.05,
            option_spread_sane=False,
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="SOFT_LOSS", option_pnl_pct=-0.30)
        assert decision.outcome == "OPTION_LOSS_UNCONFIRMED"
        assert decision.block is True

    def test_missing_option_quote_degrades(self):
        ctx = _make_ctx(
            direction="PUT",
            stop_underlying=90.00,
            current_underlying=91.50,
            option_bid=None,
            option_ask=None,
            option_mid=None,
        )
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="SOFT_LOSS")
        assert decision.block is True
        # Either DATA_DEGRADED_HOLD (from missing option_mark) or OPTION_LOSS_UNCONFIRMED
        assert decision.outcome in ("DATA_DEGRADED_HOLD", "OPTION_LOSS_UNCONFIRMED")


# ---------------------------------------------------------------------------
# Additional: ExitContextHydrator._infer_underlying
# ---------------------------------------------------------------------------

class TestInferUnderlying:
    def test_nke_put_contract(self):
        result = ExitContextHydrator._infer_underlying("NKE250620P00085000")
        assert result == "NKE"

    def test_rivn_contract(self):
        result = ExitContextHydrator._infer_underlying("RIVN250620P00010000")
        assert result == "RIVN"

    def test_spx_contract(self):
        result = ExitContextHydrator._infer_underlying("SPX250620C05500000")
        assert result == "SPX"

    def test_empty_contract(self):
        assert ExitContextHydrator._infer_underlying("") is None
        assert ExitContextHydrator._infer_underlying(None) is None


# ---------------------------------------------------------------------------
# Additional: ExitContext.missing_live_critical_fields
# ---------------------------------------------------------------------------

class TestMissingLiveCriticalFields:
    def test_fully_populated_has_no_missing(self):
        ctx = _make_ctx()
        assert ctx.missing_live_critical_fields == []

    def test_missing_stop_underlying(self):
        ctx = _make_ctx(stop_underlying=None)
        missing = ctx.missing_live_critical_fields
        assert "stop_underlying" in missing

    def test_missing_direction(self):
        ctx = _make_ctx()
        ctx.direction = None
        missing = ctx.missing_live_critical_fields
        assert "direction" in missing

    def test_missing_contract(self):
        ctx = _make_ctx(contract=None)
        missing = ctx.missing_live_critical_fields
        assert "contract" in missing
