from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_live_mode_no_longer_blocks_when_ev_score_missing():
    src = (ROOT / "ap_master_control.py").read_text()
    assert 'return self._block(signal_id, ticker, client_id, "blocked_system", "live_mode_requires_ev_score")' not in src


def test_live_mode_uses_scanner_score_not_ev_score_as_authoritative_gate():
    src = (ROOT / "ap_master_control.py").read_text()
    assert 'effective_score = score' in src
    assert 'float(signal.get("ev_score") or score) if current_mode == "LIVE" else score' not in src
