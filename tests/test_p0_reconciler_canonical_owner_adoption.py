"""Fail-first coverage for reconciler canonical exit-owner adoption."""

from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

import ap_reconciler as rec


CLIENT = "jasoncosby1@gmail.com"
CONTRACT = "NOW260828P00122000"
POSITION_ID = "2fe10f52-e459-4bc5-a57a-1a80e3618040"


class _ExitEngine:
    def __init__(self):
        self.adopt_calls = []
        self.add_calls = []

    def adopt_canonical_position_identity(self, **kwargs):
        self.adopt_calls.append(kwargs)
        return SimpleNamespace(
            disposition="ADOPTED",
            adopted=True,
            safe_to_seed=False,
            retryable=False,
        )

    def add_position(self, position):
        self.add_calls.append(position)


def _reconciler(engine):
    reconciler = rec.APBrokerReconciler.__new__(rec.APBrokerReconciler)
    reconciler.client_id = CLIENT
    reconciler.execution_mode = "live"
    reconciler.exit_engine = engine
    reconciler._alert = lambda _message: None
    reconciler._record_position_reseeded = lambda **_kwargs: None
    reconciler._heartbeat = lambda *_args, **_kwargs: None
    return reconciler


def test_canonical_position_routes_through_adoption_before_generic_add():
    engine = _ExitEngine()
    reconciler = _reconciler(engine)

    reconciler._seed_exit_engine_from_position(
        {
            "id": POSITION_ID,
            "client_id": CLIENT,
            "execution_mode": "live",
            "underlying": "NOW",
            "contract": CONTRACT,
            "direction": "PUT",
            "qty": 1,
            "avg_fill": 1.30,
            "underlying_entry": 127.425,
        }
    )

    assert engine.adopt_calls, "reconciler must try canonical adoption before add"
