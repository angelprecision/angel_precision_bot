"""Read-only persisted request-scope replay for Jason LIVE Aug 19-21.

The fixture was extracted from public.orders without mutation.  It preserves
the exact persisted structural record stream plus independent known-eligible,
attempted, and eligible-unattempted identities.  Attempt outcomes were not
reconstructed where persistence did not map them unambiguously; those symbols
therefore remain deliberately unaccounted in attempted_results, which must
fail closed against request-level structural terminalization.
"""

from __future__ import annotations

import importlib.util
import json
from collections import Counter
from pathlib import Path

import pytest

from ap.selector_retry_policy import resolve_selector_recovery_final_reason

HERE = Path(__file__).parent
FIXTURES = json.loads(
    (HERE / "fixtures_jason_live_moneyness_20260819_21.json").read_text()
)

_spec = importlib.util.spec_from_file_location(
    "selector_retry_policy_pre_fix",
    HERE / "pr491_historical_reference" / "selector_retry_policy_pre_491.py",
)
_historical = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_historical)


def _evidence(row: dict) -> dict:
    return {
        "structural_skip_records": row["structural_records"],
        "structural_skip_results": {
            item["symbol"]: item["skip_reason"]
            for item in row["structural_records"]
        },
        "attempted_results": {},
        "eligible_unattempted_symbols": row["eligible_unattempted_symbols"],
        "direct_quote_known_eligible_symbols": row["known_eligible_symbols"],
        "quality_rejections": {},
        "market_truth_outcome": None,
        "market_truth_reason": None,
    }


@pytest.mark.parametrize("row", FIXTURES, ids=lambda row: f'{row["created_ts"][:10]}-{row["symbol"]}-{row["id"]}')
def test_jason_live_persisted_mixed_evidence_no_longer_terminalizes_moneyness(row):
    assert row["client_id"] == "jasoncosby1@gmail.com"
    assert row["execution_mode"] == "live"
    assert row["persisted_reason"] == "MONEYNESS_OUT_OF_RANGE"
    reasons = {item["skip_reason"] for item in row["structural_records"]}
    assert "STRUCTURAL_MONEYNESS_OUT_OF_RANGE" in reasons
    assert len(reasons) > 1 or set(row["known_eligible_symbols"]) - {
        item["symbol"] for item in row["structural_records"]
    }

    evidence = _evidence(row)
    historical = _historical.resolve_selector_recovery_final_reason(evidence)
    assert historical in {
        "MONEYNESS_OUT_OF_RANGE",
        "DELTA_OUT_OF_RANGE",
        "DTE_OUT_OF_RANGE",
    }
    assert resolve_selector_recovery_final_reason(evidence) not in {
        "MONEYNESS_OUT_OF_RANGE",
        "DELTA_OUT_OF_RANGE",
        "DTE_OUT_OF_RANGE",
    }


def test_fixture_spans_multiple_rows_on_all_three_sessions():
    counts = Counter(row["created_ts"][:10] for row in FIXTURES)
    assert counts["2026-08-19"] >= 2
    assert counts["2026-08-20"] >= 2
    assert counts["2026-08-21"] >= 2
    assert len(FIXTURES) >= 6
