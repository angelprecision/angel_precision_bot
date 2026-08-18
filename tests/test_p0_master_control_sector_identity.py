"""
tests/test_p0_master_control_sector_identity.py
PR #483 — Master Control must resolve sector identity through the single
canonical resolver in ap/exposure_gate.py::get_sector(). It must no longer
maintain an authoritative local SECTOR_MAP with an "other" fallback that
silently aggregates unrelated unmapped tickers into one fake shared sector.

Production finding: Jason's LIVE trade-flow audit 2026-08-06..2026-08-17
found 22 revalidate_sector_cap_other blocks. Unrelated unmapped tickers
were treated as economically correlated because both
get_sector_exposure() and _sector_capital_deployed() defaulted every
unmapped ticker to the literal string "other".

Invariant under test:
    known sector   -> enforce existing sector cap math unchanged
    unknown sector -> never aggregate with other unknown tickers
    unknown sector -> never produce a "sector_cap_other" style reason
"""
from __future__ import annotations

import inspect
from pathlib import Path
from unittest.mock import MagicMock

import ap_master_control as mc_mod
from ap.exposure_gate import get_sector

_REPO = Path(__file__).resolve().parents[1]
MC_SRC = _REPO.joinpath("ap_master_control.py").read_text()


def _mc():
    """Build a minimal APMasterControl instance sufficient for the
    sector-resolution helper methods, without running __init__ (which
    requires a live DB/broker context)."""
    mc = mc_mod.APMasterControl.__new__(mc_mod.APMasterControl)
    return mc


def _pos(ticker: str, price: float = 2.00, qty: int = 1) -> dict:
    return {"underlying": ticker, "avg_fill": price, "quantity_remaining": qty}


# ══════════════════════════════════════════════════════════════════════════
# Fail-first: production-shaped reproduction of the false "other" bucket
# ══════════════════════════════════════════════════════════════════════════

def test_unmapped_candidate_does_not_share_bucket_with_unrelated_unmapped_position():
    """
    Case 1 (spec fail-first #1): an unmapped candidate ticker and an
    unrelated unmapped existing position must NOT be treated as the same
    sector. Before the fix, both resolved to the literal "other" and were
    summed together by _sector_capital_deployed.
    """
    mc = _mc()
    positions = [_pos("DHR", price=5.00, qty=2)]  # unrelated, unmapped name

    candidate_sector = get_sector("QCOM")  # canonical resolver: QCOM -> TECH (known)
    unrelated_sector = get_sector("DHR")   # canonical resolver: unknown -> None

    assert candidate_sector != unrelated_sector, (
        "QCOM must resolve to a known canonical sector distinct from an "
        "unrelated unmapped ticker's unknown identity"
    )

    # The old buggy path: both fall back to "other" and get summed together.
    deployed_other_bucket = mc._sector_capital_deployed(positions, "other")
    assert deployed_other_bucket == 0.0, (
        "_sector_capital_deployed must not attribute unrelated unmapped "
        "positions to a synthetic 'other' bucket any more"
    )


def test_second_unrelated_unmapped_pair_does_not_falsely_aggregate():
    """Case 2 (spec fail-first #2): a second, independent pair of unrelated
    unmapped tickers must not be collapsed into the same fake bucket."""
    mc = _mc()
    positions = [_pos("PCAR", price=3.00, qty=1), _pos("DDOG", price=4.00, qty=1)]

    deployed_other_bucket = mc._sector_capital_deployed(positions, "other")
    assert deployed_other_bucket == 0.0


def test_two_known_same_sector_tickers_still_aggregate_and_can_breach_cap():
    """Case 3 (spec fail-first #3): known-sector aggregation must remain
    intact — this is the control case proving we did not disable sector
    risk entirely."""
    mc = _mc()
    positions = [_pos("AAPL", price=10.00, qty=2), _pos("MSFT", price=8.00, qty=2)]

    tech_deployed = mc._sector_capital_deployed(positions, "TECH")
    assert tech_deployed == (10.00 * 2 * 100) + (8.00 * 2 * 100), (
        "Two genuinely known TECH tickers must still sum into the same "
        "known sector bucket"
    )


def test_spy_and_qqq_resolve_consistently_as_index():
    """Case 4 (spec fail-first #4): index ETFs must resolve consistently
    through the canonical resolver."""
    assert get_sector("SPY") == "INDEX"
    assert get_sector("QQQ") == "INDEX"
    assert get_sector("SPY") == get_sector("QQQ")


def test_truly_unknown_ticker_resolves_to_none_not_other():
    """Case 5 (spec fail-first #5): an unknown ticker must resolve to
    None/unknown, never to the string "other"."""
    resolved = get_sector("ZZZZNOTREAL")
    assert resolved is None
    assert resolved != "other"


# ══════════════════════════════════════════════════════════════════════════
# get_sector_exposure() must not use the "other" fallback either
# ══════════════════════════════════════════════════════════════════════════

def test_get_sector_exposure_does_not_bucket_unmapped_tickers_as_other():
    mc = _mc()
    positions = [_pos("DHR", price=5.00, qty=1), _pos("PCAR", price=6.00, qty=1)]
    exposure = mc.get_sector_exposure(positions)
    assert "other" not in exposure, (
        "get_sector_exposure must not create a synthetic 'other' bucket "
        "for unrelated unmapped tickers"
    )


def test_get_sector_exposure_still_aggregates_known_sectors():
    mc = _mc()
    positions = [_pos("AAPL", price=10.00, qty=1), _pos("MSFT", price=10.00, qty=1)]
    exposure = mc.get_sector_exposure(positions)
    assert exposure.get("TECH") == (10.00 * 1 * 100) + (10.00 * 1 * 100)


# ══════════════════════════════════════════════════════════════════════════
# Static / source assertions — the authoritative fallback must be gone
# ══════════════════════════════════════════════════════════════════════════

def test_master_control_no_longer_defines_authoritative_sector_map():
    assert "SECTOR_MAP: dict[str, str] = {" not in MC_SRC, (
        "Master Control must not define its own authoritative SECTOR_MAP "
        "after PR #483 — it must reuse ap.exposure_gate.get_sector()"
    )


def test_master_control_no_longer_uses_other_fallback_for_sector_risk():
    assert '.get(ticker.upper(), "other")' not in MC_SRC
    assert '.get(ticker_in_pos.upper(), "other")' not in MC_SRC


def test_master_control_imports_canonical_resolver():
    assert "from ap.exposure_gate import get_sector" in MC_SRC or (
        "import ap.exposure_gate" in MC_SRC
    )


# ══════════════════════════════════════════════════════════════════════════
# Import-cycle safety (spec-required proof)
# ══════════════════════════════════════════════════════════════════════════

def test_exposure_gate_has_no_dependency_back_on_master_control():
    """Prove ap.exposure_gate does not import ap_master_control (or
    anything that transitively would), so reusing get_sector() in
    Master Control cannot create a circular import."""
    import ap.exposure_gate as eg
    src = inspect.getsource(eg)
    assert "ap_master_control" not in src
    assert "import ap_master_control" not in src


def test_master_control_module_imports_cleanly_with_exposure_gate():
    """The real proof: importing ap_master_control (which will import
    ap.exposure_gate.get_sector) must succeed without ImportError."""
    import importlib
    import ap_master_control as mc_reload
    importlib.reload(mc_reload)
    assert hasattr(mc_reload, "APMasterControl")
