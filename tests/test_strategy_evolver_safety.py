from __future__ import annotations

import pytest

import ap_strategy_evolver


def test_legacy_evolver_requires_explicit_outcome_leakage_acknowledgement():
    with pytest.raises(RuntimeError, match="uses historical win_rate and avg_return"):
        ap_strategy_evolver.run_evolution_loop(source_csv="does-not-matter.csv", rounds=0)


def test_missing_real_dataset_never_falls_back_to_synthetic(tmp_path):
    missing = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError, match="Synthetic fallback is forbidden"):
        ap_strategy_evolver.load_historical_setups(str(missing))
