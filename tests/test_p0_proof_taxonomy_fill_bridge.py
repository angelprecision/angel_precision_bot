from __future__ import annotations

import ap.proof_taxonomy_fill_bridge as bridge


def test_reconciled_live_official_fill_stamps_training_eligible(monkeypatch) -> None:
    captured = {}

    def original(order, result):
        return {
            "position_id": "position-1",
            "execution_mode": "live",
            "official_live_performance_eligible": True,
        }

    def stamp(*, client_id, position_id, stamp):
        captured.update(client_id=client_id, position_id=position_id, stamp=stamp)
        return 1

    monkeypatch.setattr(bridge, "_stamp_reconciled_taxonomy", stamp)
    wrapped = bridge.wrap_exit_fill_reconcile(original)
    result = wrapped({"client_id": "client@example.com"}, {})

    assert captured["client_id"] == "client@example.com"
    assert captured["position_id"] == "position-1"
    assert captured["stamp"]["performance_taxonomy"] == "LIVE_OFFICIAL"
    assert captured["stamp"]["training_eligible"] is True
    assert result["proof_taxonomy_rows_updated"] == 1


def test_reconciled_paper_fill_never_becomes_training_input(monkeypatch) -> None:
    captured = {}

    def original(order, result):
        return {
            "position_id": "position-paper",
            "execution_mode": "paper",
            "official_live_performance_eligible": False,
        }

    monkeypatch.setattr(
        bridge,
        "_stamp_reconciled_taxonomy",
        lambda **kwargs: captured.update(kwargs) or 2,
    )
    wrapped = bridge.wrap_exit_fill_reconcile(original)
    result = wrapped({"client_id": "paper@example.com"}, {})

    assert captured["stamp"]["performance_taxonomy"] == "PAPER_UNVERIFIED"
    assert captured["stamp"]["training_eligible"] is False
    assert captured["stamp"]["quote_domain_consistent"] is False
    assert result["proof_taxonomy_rows_updated"] == 2


def test_non_dict_reconciliation_result_is_preserved(monkeypatch) -> None:
    called = False

    def stamp(**kwargs):
        nonlocal called
        called = True
        return 0

    monkeypatch.setattr(bridge, "_stamp_reconciled_taxonomy", stamp)
    wrapped = bridge.wrap_exit_fill_reconcile(lambda order, result: None)
    assert wrapped({}, {}) is None
    assert called is False
