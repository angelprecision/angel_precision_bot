"""P0 — PR #491 §18: exact production-shaped replay.

EVIDENCE-ONLY. This file makes NO production code changes. It replays the
resolver (both the pre-#491 historical version and the current #491
version) against exact, persisted production evidence pulled live from
Supabase (project jhawzqnhcihevkhehogm, `orders` table) on 2026-08-19/20 --
not synthetic, not abbreviated, not hand-generated.

Per binding spec docs/pr_specs/p0_selector_recovery_request_scope_truth_20260819.md
section 18, this replays three real post-Aug-6 production failure shapes and
records, for each:
  - the real, persisted candidate identities (exact OCC symbols) and their
    exact persisted structural skip reasons
  - the real persisted `direct_quote_unattempted_symbols` (all `[]` in these
    three records)
  - the historical/persisted final reason actually recorded in production
  - the pre-#491 resolver's replayed result (imported verbatim from git
    history at the exact rebase-base SHA, NOT hand-reconstructed, to avoid
    any risk of mis-transcribing the old defect)
  - the current (#491) resolver's replayed result
  - whether each result matches the persisted historical ground truth, and
    an honest note where it does not

Fixture data lives in tests/fixtures_pr491_production_replay.json, extracted
directly from the following real orders rows (queried live via the Supabase
MCP tool, not reconstructed from memory or fixtures/logs elsewhere in the
repo -- none existed):

  WDAY -- local_order_id=e066bb1d-8284-4721-b5eb-dfcd0bd138b7
          client_id=jasoncosby1@gmail.com execution_mode=live
          created_ts=2026-08-18T13:31:18.893503+00:00
          entry_path=DEFERRED_BREACH_MATERIALIZATION

  DDOG -- local_order_id=8a447f02-d166-4c14-a67a-6ff38ab04746
          client_id=tradefluencehq@gmail.com execution_mode=paper
          created_ts=2026-08-17T13:32:12.327742+00:00

  CRM  -- local_order_id=fa787602-2871-42fa-b579-df25feafb237
          client_id=jasoncosby1@gmail.com execution_mode=live
          created_ts=2026-08-12T13:32:12.875827+00:00

Each fixture's `structural_map` is the EXACT symbol -> skip_reason mapping
from that order's persisted `meta.deferred_selector_audit.
selection_diagnostics.structural_skips` array (top-level, most-complete
diagnostics object, not a truncated per-bucket snapshot) -- real OCC
identities, unmodified.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path

os.environ.setdefault("DATABASE_URL", "postgresql://fake")

import pytest

FIXTURES_PATH = Path(__file__).parent / "fixtures_pr491_production_replay.json"

# ── Import the CURRENT (#491) resolver, exactly as shipped in ap/. ─────────
from ap.selector_retry_policy import resolve_selector_recovery_final_reason as current_resolver

# ── Import the HISTORICAL (pre-#491) resolver verbatim from git history at
# the exact rebase-base SHA (b8e25dc1c19c969ac595685f3cc302d3b9c830da) --
# not hand-reconstructed, to eliminate any risk of mis-transcribing the old
# defect. The file is fully self-contained (no `ap` package imports), so it
# loads standalone without touching DB/schema machinery.
_HISTORICAL_MODULE_PATH = (
    Path(__file__).parent / "pr491_historical_reference" / "selector_retry_policy_pre_491.py"
)
_spec = importlib.util.spec_from_file_location(
    "selector_retry_policy_pre_491", _HISTORICAL_MODULE_PATH
)
_historical_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_historical_module)
historical_resolver = _historical_module.resolve_selector_recovery_final_reason


def _load_fixtures() -> dict:
    with open(FIXTURES_PATH) as f:
        return json.load(f)


def _evidence_from_fixture(fixture: dict) -> dict:
    """Build the evidence dict from the fixture's real persisted data,
    using ONLY fields we have high confidence faithfully map to the
    resolver's evidence schema:

    - structural_skip_results: the exact persisted symbol -> skip_reason
      map (selection_diagnostics.structural_skips), unmodified real OCC
      identities.
    - eligible_unattempted_symbols: the exact persisted
      direct_quote_unattempted_symbols list ([] in all three records).
    - attempted_results: {} -- selector_recovery_cursor_v1 was null in
      these records (they went through the DTE-ladder path rather than
      the durable breach-retry recovery cursor), so no attempted_results
      evidence ever existed for them; {} is the faithful mapping, not a
      simplification.
    - fallback_selector_reason: the persisted last_observed_selector_reason
      field -- the closest faithful analogue to the real production call
      site's `_obs_reason` (the selector's own pre-reducer observation,
      captured before any reduction). NOT canonical_selector_reason /
      reason_code, which are the resolver's OWN final output already
      persisted -- using either of those as an input would be circular.

    quality_rejections is deliberately left {} (empty): the persisted
    `top_reject_buckets` field is a per-candidate rejection-count histogram
    across the entire explored candidate universe (e.g. "4 candidates were
    BID_BELOW_MIN"), a materially different concept from the resolver's
    `quality_rejections` evidence field (a small set of specific reasons
    directly relevant to Step 2/4/8's full-set accounting). We do not have
    high confidence in a faithful field-for-field mapping between the two,
    and guessing one risks manufacturing a false replay result -- so it is
    left out rather than approximated. Steps 2 and 4 (terminal
    policy/quality veto, unrelated to and unmodified by PR #491) therefore
    do not fire in this replay, isolating the fixture's evidentiary weight
    on exactly what PR #491 changed: Step 3 (exhaustive structural proof)
    and Step 8.5 (fallback resurrection guard)."""
    summary = fixture["summary"]
    return {
        "structural_skip_results": fixture["structural_map"],
        "attempted_results": {},
        "eligible_unattempted_symbols": summary.get("direct_quote_unattempted_symbols") or [],
        "quality_rejections": {},
        "fallback_selector_reason": summary.get("last_observed_selector_reason"),
        "market_truth_outcome": None,
        "market_truth_reason": None,
    }


FIXTURES = _load_fixtures()


class TestExactProductionReplay:
    """Three exact production-shaped replays per spec §18. Each test
    documents: real candidate identities, real persisted historical result,
    pre-#491 replay result, current (#491) replay result, and whether each
    matches persisted ground truth."""

    def test_wday_20260818_jasoncosby1_live_mixed_structural(self):
        """WDAY, real LIVE Jason case, 38 real persisted structural
        candidates (5 delta-invalid + 33 moneyness-invalid, exact OCC
        symbols from the order's own selection_diagnostics), zero
        unattempted. Persisted historical result: MONEYNESS_OUT_OF_RANGE.

        Replaying the LITERAL pre-#491 code against this exact evidence
        reproduces MONEYNESS_OUT_OF_RANGE -- confirming this real request
        was a real instance of the defect. Replaying the current resolver
        against the SAME exact evidence must NOT reproduce
        MONEYNESS_OUT_OF_RANGE."""
        fixture = FIXTURES["wday"]
        evidence = _evidence_from_fixture(fixture)
        historical_ground_truth = fixture["summary"]["reason_code"]

        assert historical_ground_truth == "MONEYNESS_OUT_OF_RANGE"
        assert len(fixture["structural_map"]) == 38
        assert set(fixture["structural_map"].values()) == {
            "STRUCTURAL_DELTA_OUT_OF_RANGE",
            "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
        }

        pre_491_replay = historical_resolver(evidence)
        post_491_replay = current_resolver(evidence)

        # The literal historical code, replayed against this exact evidence,
        # reproduces what production actually did -- confirms this fixture
        # is a genuine, faithful instance of the defect, not a fabrication.
        assert pre_491_replay == historical_ground_truth == "MONEYNESS_OUT_OF_RANGE"

        # The current resolver, replayed against the SAME exact evidence,
        # must not reintroduce the false claim. It correctly falls through
        # to the truthful surviving reason: the selector's own last-
        # observed reason (CHAIN_ROW_ZERO_BID_ASK, RETRYABLE_DATA) is not
        # one of the four governed structural reasons, so it passes the
        # Step 8.5 guard and is honestly preserved.
        assert post_491_replay != "MONEYNESS_OUT_OF_RANGE"
        assert post_491_replay == "CHAIN_ROW_ZERO_BID_ASK"

    def test_ddog_20260817_tradefluencehq_paper_homogeneous_control(self):
        """DDOG, real paper-account case, 31 real persisted structural
        candidates, ALL homogeneous STRUCTURAL_MONEYNESS_OUT_OF_RANGE, zero
        unattempted. This is a genuinely EXHAUSTIVE case -- every known
        candidate agrees on the same structural reason. Persisted
        historical result: MONEYNESS_OUT_OF_RANGE (truthfully).

        Positive control: both the pre-#491 and current resolver must agree
        MONEYNESS_OUT_OF_RANGE here -- PR #491 must not weaken a genuinely
        exhaustive, truthful structural conclusion."""
        fixture = FIXTURES["ddog"]
        evidence = _evidence_from_fixture(fixture)
        historical_ground_truth = fixture["summary"]["reason_code"]

        assert historical_ground_truth == "MONEYNESS_OUT_OF_RANGE"
        assert len(fixture["structural_map"]) == 31
        assert set(fixture["structural_map"].values()) == {
            "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"
        }

        pre_491_replay = historical_resolver(evidence)
        post_491_replay = current_resolver(evidence)

        assert pre_491_replay == "MONEYNESS_OUT_OF_RANGE"
        assert post_491_replay == "MONEYNESS_OUT_OF_RANGE"
        assert post_491_replay == historical_ground_truth

    def test_crm_20260812_jasoncosby1_live_ambiguous_fail_closed(self):
        """CRM, real LIVE Jason case, 47 real persisted structural
        candidates (mixed delta-invalid + moneyness-invalid, same shape
        class as WDAY), zero unattempted. Persisted historical result:
        UNKNOWN_SELECTOR_RECOVERY_FAILURE.

        Honest finding: replaying the LITERAL pre-#491 code against this
        fixture's evidence (built from the same faithful, non-guessed field
        mapping used for WDAY/DDOG -- see _evidence_from_fixture) does NOT
        bit-for-bit reproduce CRM's persisted historical
        UNKNOWN_SELECTOR_RECOVERY_FAILURE outcome; it returns
        MONEYNESS_OUT_OF_RANGE, the same as it does for WDAY's equivalent
        mixed-structural shape. This means CRM's real historical outcome
        depended on production runtime state not captured in the persisted
        selection_diagnostics summary this fixture is built from (most
        likely the real quality_rejections/accounted-reasons evidence at
        the exact moment the deferred-breach reducer ran, which this
        replay deliberately leaves empty rather than guess at -- see
        _evidence_from_fixture's docstring). This is reported transparently
        rather than adjusted to fit a preferred narrative.

        What this fixture DOES establish, reliably, from evidence we are
        confident is faithful: given this exact real mixed-structural,
        non-exhaustive, zero-unattempted candidate set, the CURRENT
        resolver does not resurrect either MONEYNESS_OUT_OF_RANGE or
        DELTA_OUT_OF_RANGE -- the exhaustive-proof and fallback-guard
        invariants PR #491 exists to enforce hold for this real production
        evidence, even though exact historical bit-for-bit reproduction of
        CRM's specific final code was not achievable from the persisted
        diagnostics alone."""
        fixture = FIXTURES["crm"]
        evidence = _evidence_from_fixture(fixture)
        historical_ground_truth = fixture["summary"]["reason_code"]

        assert historical_ground_truth == "UNKNOWN_SELECTOR_RECOVERY_FAILURE"
        assert len(fixture["structural_map"]) == 47
        assert set(fixture["structural_map"].values()) == {
            "STRUCTURAL_DELTA_OUT_OF_RANGE",
            "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
        }

        pre_491_replay = historical_resolver(evidence)
        post_491_replay = current_resolver(evidence)

        # Honest disclosure: this replay's pre-#491 result does not match
        # CRM's persisted historical ground truth -- see docstring. Recorded
        # explicitly, asserted so any future silent change is caught rather
        # than papered over.
        assert pre_491_replay == "MONEYNESS_OUT_OF_RANGE"
        assert pre_491_replay != historical_ground_truth

        # What #491's own correctness claim requires: given this exact real
        # mixed-structural, non-exhaustive, zero-unattempted evidence, the
        # CURRENT resolver must not resurrect a specific structural reason
        # that was never exhaustively proven for it -- regardless of
        # whether it bit-for-bit matches this particular order's historical
        # final code.
        assert post_491_replay not in ("MONEYNESS_OUT_OF_RANGE", "DELTA_OUT_OF_RANGE")
        assert post_491_replay == "CHAIN_ROW_ZERO_BID_ASK"


class TestReplaySummaryTable:
    def test_print_replay_summary(self, capsys):
        """Not an assertion -- prints the full before/after table for the
        PR record. Run with `-s` to see output."""
        rows = []
        for name, fixture in FIXTURES.items():
            evidence = _evidence_from_fixture(fixture)
            historical_ground_truth = fixture["summary"]["reason_code"]
            pre_491 = historical_resolver(evidence)
            post_491 = current_resolver(evidence)
            rows.append(
                (
                    name.upper(),
                    fixture["execution_mode"],
                    len(fixture["structural_map"]),
                    historical_ground_truth,
                    pre_491,
                    post_491,
                )
            )
        print("\n\nPR #491 production replay summary (exact persisted evidence):")
        print(f"{'TICKER':<8}{'MODE':<8}{'N':<5}{'HISTORICAL':<32}{'PRE-491 REPLAY':<32}{'POST-491 REPLAY':<32}")
        for ticker, mode, n, hist, pre, post in rows:
            print(f"{ticker:<8}{mode:<8}{n:<5}{hist:<32}{pre:<32}{post:<32}")
        assert len(rows) == 3


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v", "-s"]))
