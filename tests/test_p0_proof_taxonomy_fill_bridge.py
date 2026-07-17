from __future__ import annotations

import ap.proof_taxonomy_fill_bridge as bridge
import ap.proof_taxonomy_guard as proof_guard
import ap.trade_lifecycle_guards as lifecycle_guards


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


def test_proof_before_fill_and_fill_before_proof_converge(monkeypatch) -> None:
    fill_stamp = {}
    monkeypatch.setattr(
        bridge,
        "_stamp_reconciled_taxonomy",
        lambda **kwargs: fill_stamp.update(kwargs["stamp"]) or 1,
    )
    fill_wrapped = bridge.wrap_exit_fill_reconcile(
        lambda _order, _result: {
            "position_id": "position-1",
            "execution_mode": "live",
            "official_live_performance_eligible": True,
        }
    )
    fill_wrapped({"client_id": "client@example.com"}, {})

    identity = proof_guard.EntryIdentity(
        client_id="client@example.com",
        position_id="position-1",
        local_order_id="entry-1",
        broker_order_id="entry-broker-1",
        execution_mode="live",
        signal_id="signal-1",
        canonical_signal_id="canonical-1",
        filled_qty=1,
        fill_price=1.0,
        filled_ts="2026-07-16T18:40:46Z",
        synthetic_entry=False,
    )
    monkeypatch.setattr(proof_guard, "resolve_originating_entry_identity", lambda **_kwargs: identity)
    monkeypatch.setattr(
        proof_guard,
        "_lifecycle_proof_stamp",
        lambda _identity: proof_guard.classify_performance_taxonomy({
            "execution_mode": "live",
            "official_live_performance_eligible": True,
        }),
    )
    monkeypatch.setattr(proof_guard, "_persist_stamp", lambda *_args, **_kwargs: None)
    proof_wrapped = proof_guard.wrap_log_trade(
        lambda self, ticker, **kwargs: {"_proof_persisted": False, **kwargs}
    )
    proof_result = proof_wrapped(
        type("Logger", (), {"email": "client@example.com"})(),
        "SPY",
        position_id="position-1",
        local_order_id="entry-1",
        execution_mode="live",
    )
    assert fill_stamp["performance_taxonomy"] == "LIVE_OFFICIAL"
    assert proof_result["performance_taxonomy"] == fill_stamp["performance_taxonomy"]
    assert proof_result["training_eligible"] is fill_stamp["training_eligible"] is True


def test_lifecycle_guards_install_in_final_stack_order(monkeypatch) -> None:
    installed = []

    class Module:
        def __init__(self, installer_name, guard_name):
            setattr(self, installer_name, lambda: installed.append(guard_name))

    modules = {
        module_name: Module(installer_name, guard_name)
        for guard_name, module_name, installer_name, _required in lifecycle_guards._GUARDS
    }
    monkeypatch.setattr(
        lifecycle_guards.importlib,
        "import_module",
        lambda module_name: modules[module_name],
    )
    lifecycle_guards.install_trade_lifecycle_guards()
    assert installed[:5] == [
        "canonical_exit_fill_truth",
        "exit_decision_idempotency",
        "proof_taxonomy",
        "proof_taxonomy_fill_bridge",
        "one_contract_policy",
    ]
