"""
Phase 4 tests: account-equity-based position sizing.

Spec
----
  position_budget = account_equity * POSITION_RISK_PCT     # default 0.10
  qty             = floor(position_budget / (premium * 100))

  - MAX_TRADE_USD is an ABSOLUTE outer safety cap (default $50K)
  - MAX_CONTRACTS clamps qty (default 15 per PR #30; was 50 historically)
  - NO LIVE forced-1 anywhere
  - client.base_position_pct continues to win when set
  - returns sizing_reason_code so the dashboard can attribute the result

Acceptance examples (premium = $3.08, POSITION_RISK_PCT = 0.10):
  $10K  account \u2192 budget $1000  \u2192 1000/308 = 3.24 \u2192 3 contracts
  $30K  account \u2192 budget $3000  \u2192 3000/308 = 9.74 \u2192 9 contracts

Run:
    pytest tests/test_phase4_account_equity_sizing.py -xvs
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
EXEC_SRC = (REPO_ROOT / "ap" / "execution.py").read_text()

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_phase4",
)


# ============================================================
# 1. Constants & env vars
# ============================================================

class TestPhase4Constants:
    def test_position_risk_pct_default_010(self):
        import re
        assert re.search(
            r'POSITION_RISK_PCT\s*=\s*float\(os\.getenv\(\s*"POSITION_RISK_PCT"\s*,\s*"0\.10"\s*\)\)',
            EXEC_SRC,
        )

    def test_max_trade_usd_declared(self):
        assert "MAX_TRADE_USD" in EXEC_SRC
        assert 'os.getenv("MAX_TRADE_USD"' in EXEC_SRC

    def test_max_contracts_declared(self):
        assert "MAX_CONTRACTS" in EXEC_SRC
        assert 'os.getenv("MAX_CONTRACTS"' in EXEC_SRC


# ============================================================
# 2. _size_position unit tests
# ============================================================

@pytest.fixture
def size_fn():
    from ap.execution import _size_position
    return _size_position


class TestAcceptanceExamples:
    """The two examples called out in the audit prompt."""

    def test_10k_qcom_308_is_3_contracts(self, size_fn):
        qty, budget, reason = size_fn(10_000.0, 3.08)
        assert qty == 3, f"expected 3, got {qty} (budget={budget}, reason={reason})"
        assert budget == pytest.approx(1000.0)
        assert reason == "ACCOUNT_EQUITY_PCT"

    def test_30k_qcom_308_is_9_contracts(self, size_fn):
        qty, budget, reason = size_fn(30_000.0, 3.08)
        assert qty == 9, f"expected 9, got {qty} (budget={budget}, reason={reason})"
        assert budget == pytest.approx(3000.0)
        assert reason == "ACCOUNT_EQUITY_PCT"


class TestPctApplication:
    def test_100k_account_at_5_premium(self, size_fn):
        # 100000 * 0.10 = 10000 ; 10000 / (5 * 100) = 20 raw -> capped at
        # MAX_CONTRACTS=15 (PR #30 operational cap).
        qty, budget, reason = size_fn(100_000.0, 5.00)
        assert qty == 15
        assert budget == pytest.approx(10_000.0)
        assert reason == "MAX_CONTRACTS_CAP"

    def test_client_override_wins(self, size_fn):
        # client.base_position_pct = 0.02 -> only 2% of equity = $2000
        qty, budget, reason = size_fn(100_000.0, 3.08, client_cfg={"base_position_pct": 0.02})
        assert qty == 6   # 2000 / 308 = 6.49
        assert budget == pytest.approx(2_000.0)

    def test_client_override_zero_falls_back_to_env(self, size_fn):
        # Falsy value in client column must fall back to POSITION_RISK_PCT
        qty1, _, _ = size_fn(10_000.0, 3.08, client_cfg={"base_position_pct": 0})
        qty2, _, _ = size_fn(10_000.0, 3.08, client_cfg={"base_position_pct": None})
        qty3, _, _ = size_fn(10_000.0, 3.08)
        assert qty1 == qty2 == qty3 == 3


class TestMaxCaps:
    def test_max_trade_usd_clamps_budget(self, size_fn, monkeypatch):
        # Force a tight cap and verify the reason flips.
        monkeypatch.setattr("ap.execution.MAX_TRADE_USD", 500.0)
        qty, budget, reason = size_fn(100_000.0, 3.08)
        assert budget == pytest.approx(500.0)
        # 500 / 308 = 1.62 \u2192 1 contract
        assert qty == 1
        assert reason == "MAX_TRADE_USD_CAP"

    def test_max_contracts_clamps_qty(self, size_fn, monkeypatch):
        monkeypatch.setattr("ap.execution.MAX_CONTRACTS", 5)
        # 100k * 0.10 = $10k ; 10k / 308 = 32 raw ; clamp to 5
        qty, budget, reason = size_fn(100_000.0, 3.08)
        assert qty == 5
        assert reason == "MAX_CONTRACTS_CAP"


class TestEdgeCases:
    def test_zero_equity_returns_zero_qty(self, size_fn):
        qty, budget, reason = size_fn(0.0, 3.08)
        assert qty == 0
        assert reason == "INVALID_INPUTS"

    def test_negative_equity_returns_zero_qty(self, size_fn):
        qty, _, reason = size_fn(-10_000.0, 3.08)
        assert qty == 0
        assert reason == "INVALID_INPUTS"

    def test_zero_premium_returns_zero_qty(self, size_fn):
        qty, _, reason = size_fn(10_000.0, 0.0)
        assert qty == 0
        assert reason == "INVALID_INPUTS"

    def test_premium_too_high_for_budget(self, size_fn):
        # equity = $5K -> budget = $500. Premium $10.00 means cost = $1000/contract.
        # That's more than budget, so qty=0 with INSUFFICIENT_BUDGET.
        qty, budget, reason = size_fn(5_000.0, 10.00)
        assert qty == 0
        assert budget == pytest.approx(500.0)
        assert reason == "INSUFFICIENT_BUDGET"

    def test_invalid_inputs_string(self, size_fn):
        qty, _, reason = size_fn("not-a-number", 3.08)  # type: ignore[arg-type]
        assert qty == 0
        assert reason == "INVALID_INPUTS"


# ============================================================
# 3. Code-shape: no LIVE forced-1, call site uses _size_position
# ============================================================

class TestNoForcedOneInLive:
    def test_no_qty_equals_1_force_in_live_branch(self):
        """Spec: there must be NO forced qty=1 for LIVE mode. The only place
        qty=1 can appear is in error handling or paper-mode fallbacks; we
        assert that the words 'LIVE' and 'qty = 1' do not co-occur within
        a small window."""
        # Crude but effective: find all 'qty = 1' (with or without spaces)
        # and check that none of the surrounding 200 chars contain 'LIVE'.
        import re
        for m in re.finditer(r"\bqty\s*=\s*1\b", EXEC_SRC):
            start = max(0, m.start() - 200)
            end = min(len(EXEC_SRC), m.end() + 200)
            window = EXEC_SRC[start:end]
            assert "LIVE" not in window, (
                f"qty=1 force found near LIVE branch: ...{window}..."
            )

    def test_call_site_uses_size_position(self):
        assert "_size_position(\n            account_equity, premium, client_cfg=client\n        )" in EXEC_SRC, \
            "process_signal must call _size_position with account_equity and client_cfg"

    def test_call_site_does_not_use_old_calc_qty(self):
        """The old _calc_qty-based flow has been replaced. _calc_qty itself
        is kept for callers we don't control, but process_signal must not
        call it as its primary sizing path."""
        # Search the chunk of process_signal between 'Phase 4: account-equity sizing' and the next phase boundary.
        anchor = "PHASE 4: account-equity sizing"
        idx = EXEC_SRC.find(anchor)
        assert idx > 0
        # Look at the 4000 chars following \u2014 covers the sizing + insert region.
        window = EXEC_SRC[idx:idx + 4000]
        assert "_calc_qty(" not in window, (
            "process_signal Phase 4 region must not call _calc_qty; "
            "use _size_position instead"
        )


# ============================================================
# 4. Sizing telemetry persisted in meta + audit + response
# ============================================================

class TestSizingTelemetry:
    REQUIRED_FIELDS = (
        "account_equity",
        "position_budget",
        "final_qty",
        "sizing_reason_code",
    )

    def test_meta_carries_sizing_fields(self):
        # All four fields must appear in the _meta dict (Phase 4 block).
        for field in self.REQUIRED_FIELDS:
            assert f'"{field}":' in EXEC_SRC, f"meta missing field: {field}"

    def test_trade_executed_audit_carries_sizing_fields(self):
        idx = EXEC_SRC.find('"TRADE_EXECUTED"')
        assert idx > 0
        window = EXEC_SRC[idx:idx + 2000]
        for field in self.REQUIRED_FIELDS:
            assert field in window, f"TRADE_EXECUTED audit missing {field}"

    def test_position_too_small_response_carries_sizing_fields(self):
        # The early-return path when qty<1 must also carry the sizing context
        # so the dashboard can chart sizing rejects.
        idx = EXEC_SRC.find('"position_too_small"')
        assert idx > 0
        window = EXEC_SRC[idx:idx + 500]
        for field in ("account_equity", "position_budget", "sizing_reason_code"):
            assert field in window, f"position_too_small return missing {field}"


# ============================================================
# 5. Reason codes \u2014 each canonical code can be produced
# ============================================================

class TestReasonCodes:
    def test_account_equity_pct(self, size_fn):
        _, _, r = size_fn(10_000.0, 3.08)
        assert r == "ACCOUNT_EQUITY_PCT"

    def test_max_trade_usd_cap(self, size_fn, monkeypatch):
        monkeypatch.setattr("ap.execution.MAX_TRADE_USD", 500.0)
        _, _, r = size_fn(100_000.0, 3.08)
        assert r == "MAX_TRADE_USD_CAP"

    def test_max_contracts_cap(self, size_fn, monkeypatch):
        monkeypatch.setattr("ap.execution.MAX_CONTRACTS", 2)
        _, _, r = size_fn(100_000.0, 3.08)
        assert r == "MAX_CONTRACTS_CAP"

    def test_insufficient_budget(self, size_fn):
        _, _, r = size_fn(5_000.0, 10.00)
        assert r == "INSUFFICIENT_BUDGET"

    def test_invalid_inputs(self, size_fn):
        _, _, r = size_fn(0, 3.08)
        assert r == "INVALID_INPUTS"
