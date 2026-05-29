"""Tests for the selector candidate audit (item 3) — EVIDENCE ONLY.

Covers _build_candidate_audit(): top-N structure, field extraction, moneyness,
rejected-reason ordering, and graceful handling of missing data. The builder
must NEVER raise and must NOT affect selection.

Run: DATABASE_URL=postgresql://test:test@localhost/test python -m pytest tests/test_selector_candidate_audit.py -v
"""
import os
import importlib


def _load():
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
        assert len(audit["top_candidates"]) == 3
        assert audit["candidates_considered"] == 4
        assert audit["selected_contract"] == "A"
        assert audit["selected_reason"] == "best_fit"

    def test_field_extraction(self):
        cs = _load()
        scored = [(90.0, _opt("A", 105, 1.0, 1.20, delta=0.55, dte=7, vol=250, oi=900))]
        audit = cs._build_candidate_audit(scored, 100.0, "A", "r", {}, top_n=3)
        c = audit["top_candidates"][0]
        assert c["rank"] == 1
        assert c["contract"] == "A"
        assert c["strike"] == 105.0
        assert c["bid"] == 1.0 and c["ask"] == 1.20
        assert c["mid"] == 1.10
        assert c["delta"] == 0.55
        assert c["dte"] == 7
        assert c["volume"] == 250 and c["open_interest"] == 900
        # spread_pct = (1.20-1.0)/1.10 = 0.1818
        assert abs(c["spread_pct"] - 0.1818) < 0.001
        # moneyness = (105-100)/100 = 0.05
        assert abs(c["moneyness_pct"] - 0.05) < 0.0001
        assert abs(c["distance_from_underlying"] - 5.0) < 0.0001

    def test_missing_delta_is_none_not_crash(self):
        cs = _load()
        scored = [(50.0, _opt("A", 100, 1.0, 1.1, delta=None))]
        audit = cs._build_candidate_audit(scored, 100.0, "A", "r", {}, top_n=3)
        c = audit["top_candidates"][0]
        assert c["delta"] is None
        assert "delta_reason" in c  # records why it's missing

    def test_rejected_reasons_sorted_desc(self):
        cs = _load()
        scored = [(50.0, _opt("A", 100, 1.0, 1.1))]
        rejections = {"spread_too_wide": 2, "delta_out_of_range": 7, "low_oi": 1}
        audit = cs._build_candidate_audit(scored, 100.0, "A", "r", rejections, top_n=3)
        keys = list(audit["rejected_candidate_reasons"].keys())
        assert keys[0] == "delta_out_of_range"  # highest count first

    def test_empty_scored_no_crash(self):
        cs = _load()
        audit = cs._build_candidate_audit([], 100.0, None, "none", {}, top_n=3)
        assert audit["top_candidates"] == []
        assert audit["candidates_considered"] == 0

    def test_zero_underlying_moneyness_none(self):
        cs = _load()
        scored = [(50.0, _opt("A", 100, 1.0, 1.1))]
        audit = cs._build_candidate_audit(scored, 0.0, "A", "r", {}, top_n=3)
        c = audit["top_candidates"][0]
        assert c["moneyness_pct"] is None
        assert c["distance_from_underlying"] is None

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
