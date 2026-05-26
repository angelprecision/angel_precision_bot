"""
Exit-proof finalization validation tests (Commit 31, Task 7).

These tests pin down that proof / feedback / store / shadow writes happen
ONLY after a broker-confirmed exit fill - NEVER at submit time. The
regression we cannot ship is: a submitted-but-unfilled exit shows up as a
closed proof_trade.

Approach
--------
ap_execution_core is a heavyweight module that pulls the entire execution
engine at import time. We do not import it. Instead we use source-shape
proofs over its text:

    1. on_exit_fill_confirmed is wired to _finalize_proof.
    2. _on_position_close stages the proof (writes _proof_staged) and
       EXIT_SUBMITTED_PROOF_STAGED, but does NOT call:
         - self.proof.log_trade
         - self.feedback.record_outcome
         - self.store.update_status(..., "closed")
         - self.shadow.record_live_outcome
    3. _finalize_proof:
         - is the ONLY method that calls log_trade / record_outcome /
           update_status / record_live_outcome.
         - emits EXIT_FILL_CONFIRMED_PROOF_FINALIZED.
         - is idempotent: a second call when _proof_finalized is True
           emits EXIT_PROOF_FINALIZE_SKIPPED_ALREADY_LOGGED and writes
           nothing.
         - passes the broker-confirmed price as exit_option_price (and
           the staged estimate as exit_limit_placed for slippage math),
           NOT the estimate.

A separate behavioral test in TestFinalizeProofBehavior runs against an
extracted copy of the method to confirm idempotency + write-targeting on
actual code execution (with all four sinks mocked).

Run:
    pytest tests/test_exit_proof_finalization.py -v
"""
from __future__ import annotations

import inspect
import os
import re
import textwrap
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
EX_CORE = (REPO_ROOT / "ap_execution_core.py").read_text()

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_proof_finalize",
)


# ============================================================
# 1.  Source-shape proofs - the contract that lives in the code.
# ============================================================

class TestExitFillCallbackWiring:
    def test_callback_wired_to_finalize_proof(self):
        """on_exit_fill_confirmed must be the only entry point that
        triggers _finalize_proof."""
        # Find the wiring line.  ExecutionCore __init__ sets this.
        m = re.search(
            r"self\.exit_eng\.on_exit_fill_confirmed\s*=\s*self\._finalize_proof",
            EX_CORE,
        )
        assert m, (
            "ExecutionCore.__init__ must set "
            "self.exit_eng.on_exit_fill_confirmed = self._finalize_proof"
        )

    def test_finalize_proof_method_exists(self):
        m = re.search(
            r"def _finalize_proof\(self,\s*pos[^)]*,\s*actual_fill_price[^)]*\)",
            EX_CORE,
        )
        assert m, (
            "_finalize_proof(self, pos, actual_fill_price=0.0) signature missing"
        )


# ============================================================
# 2.  _on_position_close stages but does NOT write proof.
# ============================================================

class TestOnPositionCloseStagesOnly:
    """The 'close' callback fires at exit-submit time. At that moment we
    do NOT have a broker-confirmed fill, so it must only STAGE proof and
    log EXIT_SUBMITTED_PROOF_STAGED. The four write sinks must not run
    here."""

    @pytest.fixture
    def close_method_body(self):
        """Pull the body of _on_position_close so we can audit it."""
        # Locate '    def _on_position_close(' and slice to the next def
        # at the same indent.
        m = re.search(
            r"^    def _on_position_close\(self.*?(?=^    def )",
            EX_CORE, re.DOTALL | re.MULTILINE,
        )
        assert m, "_on_position_close method not found"
        return m.group(0)

    def test_stages_proof_dict(self, close_method_body):
        assert "_proof_staged" in close_method_body, (
            "_on_position_close must write pos._proof_staged"
        )

    def test_logs_exit_submitted_marker(self, close_method_body):
        assert "EXIT_SUBMITTED_PROOF_STAGED" in close_method_body, (
            "_on_position_close must log EXIT_SUBMITTED_PROOF_STAGED so "
            "ops can see the submit-time stage event"
        )

    @pytest.mark.parametrize("forbidden_call", [
        r"self\.proof\.log_trade\(",
        r"self\.feedback\.record_outcome\(",
        r"self\.store\.update_status\(",
        r"self\.shadow\.record_live_outcome\(",
    ])
    def test_does_not_call_write_sinks(self, close_method_body, forbidden_call):
        """Each of the four write sinks MUST NOT appear inside
        _on_position_close. Comments referencing the call are fine."""
        # Strip comments so '# self.proof.log_trade ...' notes don't trip us.
        stripped = re.sub(r"#[^\n]*", "", close_method_body)
        assert not re.search(forbidden_call, stripped), (
            f"_on_position_close calls {forbidden_call} - that breaks "
            "proof-at-fill. The call must move into _finalize_proof."
        )


# ============================================================
# 3.  _finalize_proof is the ONLY writer + is idempotent
# ============================================================

class TestFinalizeProofIsTheOnlyWriter:
    @pytest.fixture
    def finalize_body(self):
        m = re.search(
            r"^    def _finalize_proof\(self.*?(?=^    def )",
            EX_CORE, re.DOTALL | re.MULTILINE,
        )
        assert m, "_finalize_proof not found"
        return m.group(0)

    def test_idempotency_guard_present(self, finalize_body):
        """A second call to _finalize_proof must skip - we never want to
        double-log a single position's proof."""
        assert "_proof_finalized" in finalize_body, (
            "_finalize_proof must set/check pos._proof_finalized"
        )
        assert "EXIT_PROOF_FINALIZE_SKIPPED_ALREADY_LOGGED" in finalize_body, (
            "Duplicate-call path must log EXIT_PROOF_FINALIZE_SKIPPED_ALREADY_LOGGED"
        )

    def test_emits_finalized_marker(self, finalize_body):
        assert "EXIT_FILL_CONFIRMED_PROOF_FINALIZED" in finalize_body

    @pytest.mark.parametrize("required_call", [
        # log_trade is the canonical proof write
        r"self\.proof\.log_trade\(",
        # feedback record happens AFTER broker fill, never before
        r"self\.feedback\.record_outcome\(",
        # signal store flips to 'closed' only after fill
        r'self\.store\.update_status\(',
        # shadow live outcome records actual P/L
        r"self\.shadow\.record_live_outcome\(",
    ])
    def test_finalize_calls_each_sink_exactly_once(self, finalize_body, required_call):
        """Each sink must be called - once - from _finalize_proof. No other
        method should call them (caught by the test below)."""
        matches = re.findall(required_call, finalize_body)
        assert len(matches) == 1, (
            f"{required_call!r} should be called exactly once from "
            f"_finalize_proof; found {len(matches)}"
        )

    @pytest.mark.parametrize("sink_pattern", [
        r"self\.proof\.log_trade\(",
        r"self\.feedback\.record_outcome\(",
        r"self\.shadow\.record_live_outcome\(",
    ])
    def test_no_other_method_writes_proof(self, sink_pattern):
        """A sink call appearing OUTSIDE _finalize_proof's body would
        leak proof writes back to the submit-time path."""
        # Strip the body of _finalize_proof, then search what remains.
        m = re.search(
            r"^    def _finalize_proof\(self.*?(?=^    def )",
            EX_CORE, re.DOTALL | re.MULTILINE,
        )
        without_finalize = EX_CORE.replace(m.group(0), "")
        # Also strip comments so '# self.proof.log_trade' docstring lines pass.
        stripped = re.sub(r"#[^\n]*", "", without_finalize)
        # And strip docstrings (triple-quoted blocks) so doc references don't trip us.
        stripped = re.sub(r'"""[\s\S]*?"""', "", stripped)

        residual = re.findall(sink_pattern, stripped)
        assert not residual, (
            f"{sink_pattern!r} called outside _finalize_proof - this "
            f"breaks proof-at-fill. Move that call back into _finalize_proof. "
            f"Residual matches: {residual}"
        )

    def test_update_status_closed_only_inside_finalize(self):
        """Mark-as-closed against the signal store must happen only from
        _finalize_proof, after broker-confirmed fill."""
        m = re.search(
            r"^    def _finalize_proof\(self.*?(?=^    def )",
            EX_CORE, re.DOTALL | re.MULTILINE,
        )
        without_finalize = EX_CORE.replace(m.group(0), "")
        # Also strip comments + docstrings to avoid commentary lines.
        stripped = re.sub(r"#[^\n]*", "", without_finalize)
        stripped = re.sub(r'"""[\s\S]*?"""', "", stripped)
        # Allow update_status calls for OTHER values (e.g. 'closing'), but not 'closed'.
        for m2 in re.finditer(r'self\.store\.update_status\(([^)]*)\)', stripped):
            arg = m2.group(1)
            assert '"closed"' not in arg and "'closed'" not in arg, (
                f"store.update_status(..., 'closed') called outside _finalize_proof - "
                f"that would mark trades closed at submit time. Args: {arg!r}"
            )


# ============================================================
# 4.  log_trade is called with the broker fill price, not the estimate.
# ============================================================

class TestActualFillPriceUsed:
    """When the broker reports a real fill price, _finalize_proof must
    pass it as exit_option_price into log_trade. The staged estimate
    becomes exit_limit_placed (for slippage attribution)."""

    @pytest.fixture
    def finalize_body(self):
        m = re.search(
            r"^    def _finalize_proof\(self.*?(?=^    def )",
            EX_CORE, re.DOTALL | re.MULTILINE,
        )
        assert m
        return m.group(0)

    def test_final_exit_price_uses_actual_when_positive(self, finalize_body):
        """The pattern: final_exit_price = fill if fill > 0 else est."""
        assert re.search(
            r"final_exit_price\s*=\s*fill\s+if\s+fill\s*>\s*0\s+else\s+est",
            finalize_body,
        ), (
            "Expected 'final_exit_price = fill if fill > 0 else est' in "
            "_finalize_proof"
        )

    def test_log_trade_gets_final_exit_price(self, finalize_body):
        """log_trade(... exit_option_price=final_exit_price ...) — whitespace tolerant"""
        assert re.search(
            r"exit_option_price\s*=\s*final_exit_price",
            finalize_body,
        ), (
            "log_trade/feedback must receive final_exit_price as exit_option_price"
        )

    def test_log_trade_gets_actual_fill_in_dedicated_field(self, finalize_body):
        """log_trade(... exit_fill_price=fill ...)  - broker-confirmed
        price kept distinct from the staged estimate."""
        assert "exit_fill_price" in finalize_body, (
            "_finalize_proof must pass exit_fill_price into log_trade"
        )

    def test_log_trade_gets_staged_estimate_as_limit_placed(self, finalize_body):
        """exit_limit_placed = staged estimate so slippage math is correct."""
        assert "exit_limit_placed" in finalize_body, (
            "_finalize_proof must pass exit_limit_placed into log_trade so "
            "the proof row carries slippage vs estimate"
        )


# ============================================================
# 5.  Behavioral test against an extracted copy of _finalize_proof.
# ============================================================
#
# We cannot import ap_execution_core (heavy module load). Instead we
# extract just the method body, eval it inside a minimal class, and run
# it with all four sinks mocked. This proves the idempotency + write
# targeting at runtime, not just by source pattern.

@pytest.fixture(scope="module")
def finalize_proof_callable():
    body_match = re.search(
        r"^    def _finalize_proof\(self.*?(?=^    def )",
        EX_CORE, re.DOTALL | re.MULTILINE,
    )
    assert body_match, "could not locate _finalize_proof"
    body = textwrap.dedent(body_match.group(0))

    # PR B follow-up: _finalize_proof now references module-level
    # constants from ap_execution_core (FIX-8 BREAKEVEN_BAND_PCT
    # single-source-of-truth). Mirror those into the harness namespace
    # so the extracted method body resolves them. Source the value the
    # same way the real module does so behavior stays in sync.
    _be_pct_re = re.search(
        r'^BREAKEVEN_BAND_PCT\s*=\s*float\(os\.getenv\("BREAKEVEN_BAND_PCT",\s*"(-?\d+\.?\d*)"\)\)',
        EX_CORE, re.MULTILINE,
    )
    _be_default = _be_pct_re.group(1) if _be_pct_re else "-2.0"

    # Build a sandbox class with the method attached.
    # Module-scope names referenced by _finalize_proof's body:
    #   - log (module logger)
    #   - os (for getenv, indirectly)
    #   - BREAKEVEN_BAND_PCT (FIX-8 constant)
    #   - _record_intel_outcome (PR B FIX-6; falls back to None if the
    #     intelligence_bridge import fails. Safe default = None; the
    #     method's `if _record_intel_outcome:` guard handles None.)
    src = "import os\nimport logging\nlog = logging.getLogger('test_finalize')\n"
    src += f'BREAKEVEN_BAND_PCT = float(os.getenv("BREAKEVEN_BAND_PCT", "{_be_default}"))\n'
    src += "_record_intel_outcome = None\n"
    src += "class _Harness:\n"
    src += textwrap.indent(body, "    ")
    ns: dict = {}
    exec(src, ns)
    return ns["_Harness"]


def _build_harness(harness_cls):
    """Construct a fake ExecutionCore-shaped object with mockable sinks."""
    h = harness_cls()
    h.proof    = MagicMock()
    h.feedback = MagicMock()
    h.store    = MagicMock()
    h.shadow   = MagicMock()
    return h


def _build_pos(staged: dict, finalized: bool = False, current_underlying: float = 100.0):
    """Build a minimal pos object that _finalize_proof reads from."""
    return SimpleNamespace(
        _proof_staged       = staged,
        _proof_finalized    = finalized,
        ticker              = staged.get("ticker", "TEST"),
        current_underlying  = current_underlying,
    )


def _make_staged(**overrides) -> dict:
    base = {
        "ticker":             "QCOM",
        "pattern":             "BREAKOUT",
        "side":                "CALL",
        "timeframe":           "1d",
        "score":               92,
        "tier":                "A_PLUS",
        "context_score":       80,
        "setup_status":        "CONFIRMED",
        "entry_option_price":  3.00,
        "exit_option_price":   3.50,   # staged estimate
        "underlying_entry":    185.00,
        "underlying_exit":     186.00,
        "contracts":           9,
        "exit_reason":         "TAKE_PROFIT",
        "opt_pnl":             16.67,
        "spread_pct":          0.05,
        "chain_grade":         "A",
        "opened_at":           "2026-05-26T14:00:00Z",
        "synthetic_entry":     False,
        "position_id":         "pos-1",
        "local_order_id":      "loc-1",
        "signal":              {"signal_id": "sig-1"},
        "paper":               True,
    }
    base.update(overrides)
    return base


class TestFinalizeProofBehavior:
    def test_first_call_writes_all_sinks(self, finalize_proof_callable):
        h = _build_harness(finalize_proof_callable)
        pos = _build_pos(_make_staged())

        # Actual broker fill of $3.60 (better than the $3.50 staged estimate)
        h._finalize_proof(pos, actual_fill_price=3.60)

        # All four sinks called exactly once.
        assert h.proof.log_trade.call_count == 1
        assert h.feedback.record_outcome.call_count == 1
        assert h.store.update_status.call_count == 1
        assert h.shadow.record_live_outcome.call_count == 1

        # store.update_status(signal_id, "closed", ...) - the only place that
        # writes 'closed' on the signal.
        args, kwargs = h.store.update_status.call_args
        assert args[0] == "sig-1"
        assert args[1] == "closed"

        # log_trade got the actual broker fill price as exit_option_price.
        lt_kwargs = h.proof.log_trade.call_args.kwargs
        assert lt_kwargs["exit_option_price"] == pytest.approx(3.60), (
            "exit_option_price must be the broker-confirmed price, not the "
            f"staged estimate. Got {lt_kwargs['exit_option_price']!r}"
        )
        # exit_fill_price carries the same broker price for the proof row.
        assert lt_kwargs["exit_fill_price"] == pytest.approx(3.60)
        # exit_limit_placed = staged estimate so slippage math works.
        assert lt_kwargs["exit_limit_placed"] == pytest.approx(3.50)

        # Position is now flagged finalized.
        assert pos._proof_finalized is True

    def test_second_call_is_idempotent(self, finalize_proof_callable):
        h = _build_harness(finalize_proof_callable)
        pos = _build_pos(_make_staged())

        h._finalize_proof(pos, actual_fill_price=3.60)
        # Snapshot the call counts after the first finalize.
        first_lt = h.proof.log_trade.call_count
        first_fb = h.feedback.record_outcome.call_count
        first_st = h.store.update_status.call_count
        first_sh = h.shadow.record_live_outcome.call_count

        # Second call: must do NOTHING new.
        h._finalize_proof(pos, actual_fill_price=99.99)

        assert h.proof.log_trade.call_count == first_lt
        assert h.feedback.record_outcome.call_count == first_fb
        assert h.store.update_status.call_count == first_st
        assert h.shadow.record_live_outcome.call_count == first_sh

    def test_zero_fill_falls_back_to_estimate(self, finalize_proof_callable):
        """If the broker fill is 0 / None (paper sandbox), the finalize
        path must still finalize - using the staged estimate."""
        h = _build_harness(finalize_proof_callable)
        pos = _build_pos(_make_staged())

        h._finalize_proof(pos, actual_fill_price=0.0)

        assert h.proof.log_trade.call_count == 1
        lt_kwargs = h.proof.log_trade.call_args.kwargs
        # exit_option_price falls back to the staged estimate.
        assert lt_kwargs["exit_option_price"] == pytest.approx(3.50)
        # exit_fill_price is None because the broker didn't give us one.
        assert lt_kwargs["exit_fill_price"] is None

    def test_no_staged_dict_is_noop(self, finalize_proof_callable):
        """Position with no _proof_staged dict: finalize must be a no-op
        (defensive). Happens when on_exit_fill_confirmed fires for a
        position whose submit-time staging was skipped."""
        h = _build_harness(finalize_proof_callable)
        pos = SimpleNamespace(_proof_staged=None, _proof_finalized=False,
                              ticker="X", current_underlying=100)
        h._finalize_proof(pos, actual_fill_price=3.60)

        assert h.proof.log_trade.call_count == 0
        assert h.feedback.record_outcome.call_count == 0
        assert h.store.update_status.call_count == 0
        assert h.shadow.record_live_outcome.call_count == 0
