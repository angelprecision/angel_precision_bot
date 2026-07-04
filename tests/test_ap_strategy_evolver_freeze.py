import importlib
import json
import logging
import os
import pathlib
import subprocess
import sys


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load_evolver(monkeypatch, enabled):
    monkeypatch.chdir(REPO_ROOT)
    if enabled is None:
        monkeypatch.delenv("ENABLE_STRATEGY_EVOLVER", raising=False)
    else:
        monkeypatch.setenv("ENABLE_STRATEGY_EVOLVER", enabled)
    sys.modules.pop("ap_strategy_evolver", None)
    return importlib.import_module("ap_strategy_evolver")


def test_run_evolution_loop_is_inert_when_env_unset(monkeypatch, tmp_path, caplog):
    evolver = _load_evolver(monkeypatch, None)
    monkeypatch.chdir(tmp_path)

    with caplog.at_level(logging.WARNING):
        result = evolver.run_evolution_loop(rounds=1)

    assert result is None
    assert "evolver_frozen_pending_walkforward_rebuild" in caplog.text
    assert not (tmp_path / "results_evolver.tsv").exists()
    assert not (tmp_path / "best_evolver_config.json").exists()


def test_run_evolution_loop_false_writes_no_best_config(monkeypatch, tmp_path, caplog):
    evolver = _load_evolver(monkeypatch, "false")
    monkeypatch.chdir(tmp_path)

    with caplog.at_level(logging.WARNING):
        result = evolver.run_evolution_loop(rounds=1)

    assert result is None
    assert "evolver_frozen_pending_walkforward_rebuild" in caplog.text
    assert not (tmp_path / "best_evolver_config.json").exists()
    assert not (tmp_path / "results_evolver.tsv").exists()


def test_run_evolution_loop_true_keeps_research_behavior(monkeypatch, tmp_path):
    evolver = _load_evolver(monkeypatch, "true")
    monkeypatch.chdir(tmp_path)

    best_config, best_result = evolver.run_evolution_loop(rounds=1, source_csv="missing.csv")

    assert best_config is not None
    assert best_result is not None
    assert (tmp_path / "results_evolver.tsv").exists()

    payload = json.loads((tmp_path / "best_evolver_config.json").read_text())
    assert payload["runtime_eligible"] is False
    assert payload["objective_warning"] == "raw_win_rate_not_runtime_safe"
    assert "config" in payload


def test_cli_text_does_not_instruct_runtime_paste(tmp_path):
    script = REPO_ROOT / "ap_strategy_evolver.py"
    env = os.environ.copy()
    env["ENABLE_STRATEGY_EVOLVER"] = "false"

    result = subprocess.run(
        [sys.executable, str(script), "--rounds", "1"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    output = f"{result.stdout}\n{result.stderr}"
    assert result.returncode == 0
    assert "Paste 'config' block" not in output
    assert "evolver_frozen_pending_walkforward_rebuild" in output


def test_no_trading_path_imports_evolver():
    forbidden = ("import ap_strategy_evolver", "from ap_strategy_evolver import")

    for path in REPO_ROOT.rglob("*.py"):
        rel = path.relative_to(REPO_ROOT)
        if rel.parts[0] == "tests":
            continue
        if rel.name == "ap_strategy_evolver.py":
            continue
        text = path.read_text(encoding="utf-8")
        assert not any(token in text for token in forbidden), f"unexpected evolver import in {rel}"
