"""
tests/test_master_control_sizing_context_float_none.py
HOTFIX hotfix/master-control-sizing-context-float-none

Root cause:
  PR #43 added a `sizing_context` block to ap_master_control.py:1380-1407
  that reads `getattr(_sizing, "risk_pct", None)` and
  `getattr(_sizing, "budget_usd", None)`, then wraps the result in float().

  `SizingResult` (ap/position_sizer.py:52) is a frozen dataclass with
  fields {contracts, method, win_rate, kelly_raw, throttle_applied,
  drawdown_today, reason}. It has NEITHER `risk_pct` NOR `budget_usd`.

  Therefore `getattr(_sizing, "risk_pct", None)` → None, then
  `float(None)` raises:
      TypeError: float() argument must be a string or a real number, not 'NoneType'

  Production impact 2026-05-26: 100% of master_control.evaluate() calls
  since the 15:22 UTC deploy failed with this exception, freezing the bot
  for >75 minutes. Zero positions opened, zero signals processed.

  Why PR #43's tests didn't catch it:
  The existing test_position_lifecycle_integrity_and_sizing.py mocked
  `_sizing` with a MagicMock. MagicMock auto-returns truthy floatable
  attributes for ANY attribute name, so `float(mock.risk_pct)` returns
  a real number instead of raising. The bug was invisible in tests.

Fix:
  Use the same safe coercion pattern already in use elsewhere in the
  file (line 996, 1242, 1322 — float(x) or 0.0 / float(x or 0) form):
      float(getattr(_sizing, "risk_pct", None) or
            float(os.getenv("POSITION_RISK_PCT", "0.10")))
  This evaluates getattr first; if it returns None (or 0 or ''), the
  `or` falls through to the env-default. No NPE.

This test uses a REAL SizingResult dataclass (NOT a MagicMock) to prove
the field-name mismatch and catch the regression. Going forward any new
field added to sizing_context must use a coercion pattern that handles
the real-object case.

Run:
    DATABASE_URL=postgresql://x python3 -m pytest \
      tests/test_master_control_sizing_context_float_none.py -v
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))


def _real_sizing_result(contracts: int = 5, method: str = "kelly"):
    """Build a REAL SizingResult instance — NOT a MagicMock.

    MagicMock would silently auto-return truthy floatable attributes
    for any name (e.g. `mock.risk_pct → MagicMock()` that float()s
    without raising), which is exactly how the float(None) bug slipped
    through PR #43's test suite. Real dataclasses are strict.
    """
    from ap.position_sizer import SizingResult
    return SizingResult(
        contracts=contracts,
        method=method,
        win_rate=0.55,
        kelly_raw=0.08,
        throttle_applied=False,
        drawdown_today=0.0,
        reason="ok",
    )


class TestSizingResultDoesNotHaveRiskPctOrBudgetUsd:
    """
    Documents the exact shape of SizingResult so future PRs don't
    repeat the missing-attribute mistake. If SizingResult is ever
    extended with risk_pct/budget_usd this test will need an update
    AND the sizing_context block should switch to direct attribute
    access (no fallback needed).
    """

    def test_sizing_result_has_no_risk_pct_field(self):
        sr = _real_sizing_result()
        # getattr returns the default — confirms attribute does NOT exist
        assert getattr(sr, "risk_pct", None) is None, (
            "SizingResult.risk_pct DOES exist. Update sizing_context block "
            "in ap_master_control.py to read it directly and remove fallback."
        )

    def test_sizing_result_has_no_budget_usd_field(self):
        sr = _real_sizing_result()
        assert getattr(sr, "budget_usd", None) is None, (
            "SizingResult.budget_usd DOES exist. Update sizing_context block "
            "in ap_master_control.py to read it directly and remove fallback."
        )

    def test_sizing_result_method_field_exists(self):
        """method IS a real field — sizing_context reads it correctly."""
        sr = _real_sizing_result(method="kelly")
        assert sr.method == "kelly"


class TestSizingContextSourceFix:
    """
    Source-level proof that the sizing_context block uses a coercion
    pattern that survives `getattr(_sizing, missing_attr, None)`.

    The hotfix must rewrite the conditional expression so that None
    (the getattr fallback) is coerced to the env default BEFORE float()
    is called, not after.
    """

    def test_risk_pct_uses_safe_coercion(self):
        """The risk_pct line in sizing_context must not call float(None).

        Acceptable forms (any of):
          - `float(getattr(_sizing, "risk_pct", None) or <fallback>)`
          - `float(getattr(_sizing, "risk_pct", None) or 0) or <fallback>`
          - Direct attribute access ONLY if SizingResult is extended (see
            test above)
        Unacceptable:
          - `float(getattr(_sizing, "risk_pct", None))` with no `or` guard
        """
        src = (REPO_ROOT / "ap_master_control.py").read_text()
        # Locate the sizing_context block
        idx = src.find('"sizing_context":')
        assert idx > 0, "sizing_context block not found"
        # Extract a 2000-char window covering the block
        block = src[idx:idx + 2000]
        # The risk_pct line(s) must contain "risk_pct" and the words `or`
        # within a few lines so the None fallback can never reach float().
        import re
        # Find the risk_pct entry — capture next ~250 chars (multi-line expr)
        m = re.search(r'"risk_pct":\s*([\s\S]{0,300})', block)
        assert m, "risk_pct field missing in sizing_context"
        expr = m.group(1)
        # Either uses `or` for None-safety OR direct attribute access
        # (no getattr-with-default-None). Both are valid.
        if "getattr" in expr:
            assert " or " in expr, (
                f"risk_pct uses getattr(...,None) without `or` fallback — "
                f"will raise float(None) when _sizing lacks the attribute.\n"
                f"Expression:\n{expr}"
            )

    def test_budget_uses_safe_coercion(self):
        src = (REPO_ROOT / "ap_master_control.py").read_text()
        idx = src.find('"sizing_context":')
        assert idx > 0
        block = src[idx:idx + 2000]
        import re
        m = re.search(r'"budget":\s*([\s\S]{0,300})', block)
        assert m, "budget field missing in sizing_context"
        expr = m.group(1)
        if "getattr" in expr:
            assert " or " in expr, (
                f"budget uses getattr(...,None) without `or` fallback — "
                f"will raise float(None) when _sizing lacks the attribute.\n"
                f"Expression:\n{expr}"
            )


class TestBehavioralReproWithRealSizingResult:
    """
    Behavioral test: feed a REAL SizingResult through the exact
    expression shape used in ap_master_control.py and prove it does
    NOT raise.

    This is the test that PR #43 SHOULD have had.
    """

    def test_real_sizing_result_does_not_break_float_coercion(self):
        """Replay the exact expression that broke in production."""
        sr = _real_sizing_result()
        # Pre-fix expression (will raise — kept as a documented reference):
        with pytest.raises(TypeError, match="float.*NoneType"):
            float(
                getattr(sr, "risk_pct", None)
                if sr is not None
                else float(os.getenv("POSITION_RISK_PCT", "0.10"))
            )

        # Post-fix expression — must NOT raise
        result = float(
            getattr(sr, "risk_pct", None) or
            float(os.getenv("POSITION_RISK_PCT", "0.10"))
        )
        assert isinstance(result, float)
        assert result > 0.0  # falls through to env default 0.10

    def test_post_fix_budget_does_not_raise(self):
        sr = _real_sizing_result()
        account_equity = 25000.0
        # Post-fix expression
        result = float(
            getattr(sr, "budget_usd", None) or
            (account_equity * float(os.getenv("POSITION_RISK_PCT", "0.10")))
        )
        assert isinstance(result, float)
        # account_equity * 0.10 = 2500.0
        assert result == pytest.approx(2500.0)


class TestMockVsRealAuditTrail:
    """
    Sentinel test: prove that MagicMock is INSUFFICIENT for testing
    dataclass attribute access. This codifies the lesson learned today.

    Going forward, any test that exercises code reading attributes off
    a dataclass-shaped object MUST use the real type, not MagicMock.
    """

    def test_mock_silently_passes_what_real_object_fails(self):
        """MagicMock returns a floatable mock for ANY attribute name —
        which is exactly why PR #43 tests didn't catch float(None)."""
        sizing_mock = MagicMock()
        # Mock allows this — no exception
        v_mock = float(getattr(sizing_mock, "risk_pct", None) or 0.0)
        assert isinstance(v_mock, float)

        # Real object — getattr returns None for missing field
        sr = _real_sizing_result()
        v_default = getattr(sr, "risk_pct", None)
        assert v_default is None, (
            "Real SizingResult must return None for missing risk_pct; "
            "if this changes, sizing_context can use direct access."
        )
