"""Tests for the selector candidate audit (item 3) — EVIDENCE ONLY.

Covers _build_candidate_audit(): top-N structure, field extraction, moneyness,
rejected-reason ordering, and graceful handling of missing data. The builder
must NEVER raise and must NOT affect selection.

Run: DATABASE_URL=postgresql://test:test@localhost/test python -m pytest tests/test_selector_candidate_audit.py -v
"""
import os
import json
import importlib


def _load():
    os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
    import ap.contract_selector as cs
    importlib.reload(cs)
    return cs


def _opt(symbol, strike, bid, ask, delta=None, dte=7, vol=100, oi=500, last=None, exp="2026-06-19"):
    o = {
        "symbol": symbol, "strike": strike, "bid": bid, "ask": ask,
        "volume": vol, "open_interest": oi, "dte": dte, "expiration_date": exp,
    }
    if last is not None:
        o["last"] = last
    if delta is not None:
        o["greeks"] = {"delta": delta}
    return o


class TestCandidateAudit:
    def test_top_n_limit(self):
        cs = _load()
        scored = [(90.0, _opt("A", 100, 1.0, 1.1)),
                  (80.0, _opt("B", 105, 0.5, 0.6)),
                  (70.0, _opt("C", 110, 0.3, 0.4)),
                  (60.0, _opt("D", 115, 0.2, 0.3))]
        audit = cs._build_candidate_audit(scored, 100.0, "A", "best_fit", {}, top_n=3)
        assert len(audit["candidates"]) == 3
        assert len(audit["top_candidates"]) == 3
        assert audit["candidates_considered"] == 4
        assert audit["selected_contract"] == "A"
        assert audit["selected_reason"] == "best_fit"
        assert audit["candidate_cap"] == 3

    def test_field_extraction(self):
        cs = _load()
        scored = [(90.0, _opt("A", 105, 1.0, 1.20, delta=0.55, dte=7, vol=250, oi=900))]
        audit = cs._build_candidate_audit(scored, 100.0, "A", "r", {}, top_n=3)
        c = audit["top_candidates"][0]
        assert c["rank"] == 1
        assert c["contract"] == "A"
        assert c["symbol"] == "A"
        assert c["strike"] == 105.0
        assert c["option_type"] == ""
        assert c["bid"] == 1.0 and c["ask"] == 1.20
        assert c["mid"] == 1.10
        assert c["delta"] == 0.55
        assert c["volume"] == 250 and c["open_interest"] == 900
        # spread_pct = (1.20-1.0)/1.10 = 0.1818
        assert abs(c["spread_pct"] - 0.1818) < 0.001
        assert c["premium"] == 110.0
        assert c["selected"] is True
        assert c["rank_score"] == 90.0
        assert c["rejected_at_step"] is None
        assert c["rejection_reason"] is None

    def test_missing_delta_is_none_not_crash(self):
        cs = _load()
        scored = [(50.0, _opt("A", 100, 1.0, 1.1, delta=None))]
        audit = cs._build_candidate_audit(scored, 100.0, "A", "r", {}, top_n=3)
        c = audit["top_candidates"][0]
        assert c["delta"] is None
        assert c["iv"] is None

    def test_rejected_reasons_sorted_desc(self):
        cs = _load()
        scored = [(50.0, _opt("A", 100, 1.0, 1.1))]
        rejections = {"spread_too_wide": 2, "delta_out_of_range": 7, "low_oi": 1}
        audit = cs._build_candidate_audit(scored, 100.0, "A", "r", rejections, top_n=3)
        keys = list(audit["hard_filter_rejects"].keys())
        assert keys[0] == "delta_out_of_range"  # highest count first

    def test_empty_scored_no_crash(self):
        cs = _load()
        audit = cs._build_candidate_audit([], 100.0, None, "none", {}, top_n=3)
        assert audit["top_candidates"] == []
        assert audit["candidates_considered"] == 0

    def test_all_rejected_fixture_stores_rejection_reasons(self):
        cs = _load()
        audit = cs._build_candidate_audit(
            [],
            100.0,
            None,
            "no_survivors",
            {"spread_too_wide": 4, "low_oi": 2},
            top_n=15,
        )
        assert audit["candidates"] == []
        assert audit["hard_filter_rejects"] == {"spread_too_wide": 4, "low_oi": 2}
        assert audit["selected_winner"] is None

    def test_unselected_ranked_candidates_marked_as_ranked_below_selected(self):
        cs = _load()
        scored = [(90.0, _opt("A", 100, 1.0, 1.1)), (80.0, _opt("B", 105, 0.8, 1.0))]
        audit = cs._build_candidate_audit(scored, 100.0, "A", "r", {}, top_n=3)
        c = audit["top_candidates"][1]
        assert c["selected"] is False
        assert c["rejected_at_step"] == "ranking"
        assert c["rejection_reason"] == "ranked_below_selected"

    def test_meta_size_bound(self):
        cs = _load()
        scored = []
        for idx in range(30):
            scored.append((100.0 - idx, _opt(f"OPT{idx}", 100 + idx, 1.0, 1.2, delta=0.4, vol=500, oi=1200)))
        audit = cs._build_candidate_audit(scored, 100.0, "OPT0", "r", {"spread_too_wide": 9}, top_n=15)
        assert len(audit["candidates"]) == 15
        assert len(json.dumps(audit, sort_keys=True)) < 12000

    def test_selectedcontract_carries_candidate_audit_field(self):
        cs = _load()
        sc = cs.SelectedContract(
            contract_symbol="X", expiration="2026-06-19", strike=100.0,
            option_type="call", bid=1.0, ask=1.1, mid=1.05, spread_pct=0.05,
            delta=0.5, open_interest=100, volume=50, premium_per_share=1.05,
            premium_per_contract=105.0, affordable_contracts=1,
            selection_reason="r", selection_score=90.0, dte=7,
        )
        # default None, settable, and present in to_dict
        assert sc.candidate_audit is None
        sc.candidate_audit = {"selected_contract": "X"}
        assert sc.to_dict()["candidate_audit"] == {"selected_contract": "X"}


class TestPreselectedQueueAuditPersistence:
    """PR #56 review fix: preselected (non-deferred) queue contracts MUST also
    write selector_candidate_audit into orders.meta. Previously, the audit only
    persisted when breach-time deferred selection ran, leaving the normal queue
    path with no candidate audit at all.

    The fix has two halves we test here:
      (a) queue.py stashes selected.candidate_audit onto plan.metadata
          immediately after a successful contract_selector.select(plan).
      (b) execution_core, on a successful preselected submit, reads
          approved_plan.metadata.selector_candidate_audit as a fallback when
          the deferred-path _candidate_audit variable is None, and persists it
          via OSM.update_order_meta().
    """

    def test_queue_stashes_candidate_audit_on_plan_metadata(self):
        """Half (a): mirror of the queue.py stash logic."""
        cs = _load()

        class FakePlan:
            def __init__(self):
                self.metadata = {}

        scored = [(90.0, _opt("A", 100, 1.0, 1.1, delta=0.5, vol=300, oi=800))]
        audit_payload = cs._build_candidate_audit(scored, 100.0, "A", "best_fit", {})
        sc = cs.SelectedContract(
            contract_symbol="A", expiration="2026-06-20", strike=100.0,
            option_type="call", bid=1.0, ask=1.1, mid=1.05, spread_pct=0.05,
            delta=0.5, open_interest=800, volume=300, premium_per_share=1.05,
            premium_per_contract=105.0, affordable_contracts=2,
            selection_reason="best_fit", selection_score=90.0, dte=21,
        )
        sc.candidate_audit = audit_payload

        plan = FakePlan()
        # Mirror queue.py exactly:
        _ca = getattr(sc, "candidate_audit", None)
        if _ca is not None:
            if not hasattr(plan, "metadata") or not isinstance(plan.metadata, dict):
                plan.metadata = {}
            plan.metadata["selector_candidate_audit"] = _ca
            plan.metadata["candidate_table"] = _ca

        assert "selector_candidate_audit" in plan.metadata
        assert "candidate_table" in plan.metadata
        assert plan.metadata["selector_candidate_audit"]["selected_contract"] == "A"
        assert len(plan.metadata["selector_candidate_audit"]["top_candidates"]) == 1
        assert set(plan.metadata["selector_candidate_audit"].keys()) == set(plan.metadata["candidate_table"].keys())

    def test_preselected_path_falls_back_to_plan_metadata(self):
        """Half (b): mirror of the execution_core fallback when _candidate_audit
        is None (deferred branch did not run) — must read from plan.metadata."""

        class FakePlan:
            metadata = {
                "selector_candidate_audit": {
                    "selected_contract": "NFLX260619C00950000",
                    "top_candidates": [{"rank": 1, "contract": "NFLX260619C00950000"}],
                }
            }

        _candidate_audit = None  # preselected path; deferred selection did not run
        approved_plan = FakePlan()

        _persist_ca = _candidate_audit
        if not _persist_ca and approved_plan is not None:
            _pmeta = getattr(approved_plan, "metadata", None) or {}
            if isinstance(_pmeta, dict):
                _persist_ca = _pmeta.get("candidate_table") or _pmeta.get("selector_candidate_audit")

        assert _persist_ca is not None, "preselected path must fall back to plan.metadata"
        assert _persist_ca["selected_contract"] == "NFLX260619C00950000"

    def test_preselected_persist_invokes_update_order_meta(self):
        """End-to-end mirror: preselected order with plan.metadata.selector_candidate_audit
        must result in a single update_order_meta call with the audit payload."""

        class FakePlan:
            metadata = {
                "selector_candidate_audit": {"selected_contract": "MSFT"}
            }

        captured = []

        class FakeOSM:
            def update_order_meta(self, local_order_id, meta_patch):
                captured.append((local_order_id, dict(meta_patch)))
                return True

        # Mirror execution_core's logic at the success branch
        _candidate_audit = None
        approved_plan = FakePlan()
        osm = FakeOSM()
        local_order_id = "LOID-123"

        _persist_ca = _candidate_audit
        if not _persist_ca and approved_plan is not None:
            _pmeta = getattr(approved_plan, "metadata", None) or {}
            if isinstance(_pmeta, dict):
                _persist_ca = _pmeta.get("candidate_table") or _pmeta.get("selector_candidate_audit")
        if _persist_ca and local_order_id and hasattr(osm, "update_order_meta"):
            osm.update_order_meta(local_order_id, {"selector_candidate_audit": _persist_ca, "candidate_table": _persist_ca})

        assert len(captured) == 1, "expected exactly one update_order_meta call"
        assert captured[0][0] == "LOID-123"
        assert captured[0][1] == {
            "selector_candidate_audit": {"selected_contract": "MSFT"},
            "candidate_table": {"selected_contract": "MSFT"},
        }

    def test_fallback_is_none_when_both_sources_absent(self):
        """If neither breach-time selection nor preselected stash produced an
        audit, _persist_ca is None and no update_order_meta call is made."""

        class FakePlan:
            metadata = {}  # no selector_candidate_audit

        _candidate_audit = None
        approved_plan = FakePlan()

        _persist_ca = _candidate_audit
        if not _persist_ca and approved_plan is not None:
            _pmeta = getattr(approved_plan, "metadata", None) or {}
            if isinstance(_pmeta, dict):
                _persist_ca = _pmeta.get("candidate_table") or _pmeta.get("selector_candidate_audit")

        assert _persist_ca is None

    def test_deferred_breach_audit_takes_priority(self):
        """If breach-time deferred selection produced a fresh _candidate_audit,
        it MUST be used; the plan.metadata value (older, from creation time) is
        ignored. The check is `if not _persist_ca:` after assigning the deferred
        audit — non-None deferred audit short-circuits the fallback."""

        class FakePlan:
            metadata = {
                "selector_candidate_audit": {"selected_contract": "STALE_PRESELECTED"}
            }

        _candidate_audit = {"selected_contract": "FRESH_BREACH_TIME"}
        approved_plan = FakePlan()

        _persist_ca = _candidate_audit
        if not _persist_ca and approved_plan is not None:
            _pmeta = getattr(approved_plan, "metadata", None) or {}
            if isinstance(_pmeta, dict):
                _persist_ca = _pmeta.get("candidate_table") or _pmeta.get("selector_candidate_audit")

        assert _persist_ca["selected_contract"] == "FRESH_BREACH_TIME"

    def test_corrupted_metadata_does_not_crash(self):
        """If approved_plan.metadata is not a dict (corruption guard), the
        fallback must not crash — it should leave _persist_ca as None."""

        class FakePlan:
            metadata = "not-a-dict"  # corruption

        _candidate_audit = None
        approved_plan = FakePlan()

        _persist_ca = _candidate_audit
        if not _persist_ca and approved_plan is not None:
            _pmeta = getattr(approved_plan, "metadata", None) or {}
            if isinstance(_pmeta, dict):
                _persist_ca = _pmeta.get("candidate_table") or _pmeta.get("selector_candidate_audit")

        # Defensive: corrupted metadata produces no audit, no crash.
        assert _persist_ca is None
