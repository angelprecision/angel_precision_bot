"""P0 regression coverage for PR #526's blank-contract ownership seam.

The production invariant is deliberately tested at the callback boundary:
every approved plan that still needs contract materialization must prove the
durable owner before selector, copyback, terminal, or broker-facing work.
"""

from __future__ import annotations

import copy
import importlib
import os
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

from ap_execution_core import APExecutionCore


CLIENT_ID = "526-client@example.com"
LOCAL_ORDER_ID = "oid-pr526-blank"
SIGNAL_ID = "sig-pr526-blank"
TICKER = "SPY"
REAL_OCC = "SPY260828C00650000"


def _plan(*, contract_symbol: str = "", metadata: dict | None = None, client_id: str = CLIENT_ID,
          execution_mode: str = "live") -> types.SimpleNamespace:
    return types.SimpleNamespace(
        contract_symbol=contract_symbol,
        limit_price=0.01,
        contracts=1,
        max_position_usd=1.0,
        side="CALL",
        direction="CALL",
        execution_mode=execution_mode,
        client_id=client_id,
        signal_id=SIGNAL_ID,
        trigger_price=600.0,
        underlying_price=600.0,
        stop_underlying=595.0,
        target_underlying=605.0,
        metadata=dict(metadata or {}),
        ticker=TICKER,
        plan_id="plan-pr526-blank",
        score=92.0,
        tier="A",
        timeframe="1d",
        pattern="breakout",
    )


def _row(*, contract: str = "", client_id: str = CLIENT_ID, execution_mode: str = "live",
         meta: dict | None = None) -> dict:
    return {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": client_id,
        "execution_mode": execution_mode,
        "signal_id": SIGNAL_ID,
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "contract": contract,
        "meta": {
            "execution_mode": execution_mode,
            **(meta or {}),
        },
    }


class _PreflightOSM:
    """Small durable adapter double that records all ownership mutations."""

    def __init__(self, rows, *, claim_result: bool = False, read_error: Exception | None = None):
        self._rows = [copy.deepcopy(row) for row in rows]
        self.claim_result = claim_result
        self.read_error = read_error
        self.claim_calls: list[dict] = []
        self.get_order_calls = 0
        self.terminal_calls: list[dict] = []

    def get_order(self, local_order_id):
        assert local_order_id == LOCAL_ORDER_ID
        self.get_order_calls += 1
        if self.read_error is not None:
            raise self.read_error
        if self._rows:
            return copy.deepcopy(self._rows.pop(0))
        return None

    def claim_deferred_materialization(self, local_order_id, **kwargs):
        assert local_order_id == LOCAL_ORDER_ID
        self.claim_calls.append(dict(kwargs))
        return self.claim_result

    def terminalize_deferred_breach(self, local_order_id, **kwargs):
        assert local_order_id == LOCAL_ORDER_ID
        self.terminal_calls.append(dict(kwargs))
        return True


def _watched(plan, *, client_id: str = CLIENT_ID, execution_mode: str = "live"):
    return types.SimpleNamespace(
        signal={
            "_approved_plan": plan,
            "signal_id": SIGNAL_ID,
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": client_id,
            "execution_mode": execution_mode,
        },
        ticker=TICKER,
        side="CALL",
        trigger_price=600.0,
        entry_trigger=600.0,
        stop_level=595.0,
        target_price=605.0,
        trigger_crossed_at=datetime.now(timezone.utc),
        last_quote_ask=600.2,
        last_quote_bid=600.1,
    )


def _core(osm, selector=None, broker=None, *, execution_mode: str = "live"):
    runtime_mode = str(execution_mode).strip().lower()
    core = APExecutionCore.__new__(APExecutionCore)
    core.client_id = CLIENT_ID
    core.email = CLIENT_ID
    core.execution_mode = runtime_mode
    core.mode = runtime_mode.upper()
    core.paper = runtime_mode == "paper"
    core.order_state_machine = osm
    core.contract_selector = selector or MagicMock()
    core.broker = broker or MagicMock()
    core.master_control = MagicMock()
    core.store = MagicMock()
    core._max_positions = 5
    core._kill_switch = False
    core._emit_breach_diag = lambda *args, **kwargs: None
    core._on_entry_trigger = APExecutionCore._on_entry_trigger.__get__(core, type(core))
    return core


@pytest.mark.parametrize(
    ("contract_symbol", "metadata"),
    [
        ("", {}),
        ("DEFERRED:SPY", {}),
        ("", {"contract_deferred": True}),
    ],
    ids=["blank", "explicit-marker", "metadata-marker"],
)
def test_all_deferred_plan_shapes_claim_before_selector_or_broker(
    contract_symbol, metadata
):
    """Blank, explicit, and metadata-deferred plans share one preflight."""
    plan = _plan(contract_symbol=contract_symbol, metadata=metadata)
    row = _row(
        contract=contract_symbol,
        meta={"contract_deferred": bool(metadata.get("contract_deferred"))},
    )
    osm = _PreflightOSM([row, row], claim_result=False)
    selector = MagicMock()
    broker = MagicMock()
    core = _core(osm, selector=selector, broker=broker)
    core._breach_risk_check = lambda _watched: pytest.fail(
        "failed ownership proof must return before the risk/selector path"
    )

    result = core._on_entry_trigger(_watched(plan))

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "MATERIALIZATION_STATE_WRITE_FAILED"
    assert len(osm.claim_calls) == 1
    selector.select.assert_not_called()
    assert broker.method_calls == []
    assert osm.terminal_calls == []


def test_plan_classifier_matrix_preserves_real_occ_guard():
    assert APExecutionCore._plan_is_deferred(_plan(contract_symbol=""), TICKER)
    assert APExecutionCore._plan_is_deferred(
        _plan(contract_symbol="DEFERRED:SPY"), TICKER
    )
    assert APExecutionCore._plan_is_deferred(
        _plan(metadata={"contract_deferred": True}), TICKER
    )
    assert not APExecutionCore._plan_is_deferred(
        _plan(contract_symbol=REAL_OCC), TICKER
    )
    assert not APExecutionCore._plan_is_deferred(
        _plan(contract_symbol=REAL_OCC, metadata={"contract_deferred": True}), TICKER
    )


def test_unreadable_blank_durable_row_holds_without_mutation():
    plan = _plan()
    osm = _PreflightOSM([], read_error=RuntimeError("db unavailable"))
    selector = MagicMock()
    broker = MagicMock()
    core = _core(osm, selector=selector, broker=broker)

    result = core._on_entry_trigger(_watched(plan))

    assert result == {
        "disposition": "KEEP_WATCHER",
        "reason_code": "MATERIALIZATION_OWNERSHIP_UNPROVEN",
        "retry_after_seconds": 5,
    }
    assert osm.claim_calls == []
    assert osm.terminal_calls == []
    selector.select.assert_not_called()
    assert broker.method_calls == []


def test_client_mismatch_holds_before_claim_selector_or_broker():
    plan = _plan()
    mismatch_row = _row(client_id="other-client@example.com")
    osm = _PreflightOSM([mismatch_row, mismatch_row])
    selector = MagicMock()
    broker = MagicMock()
    core = _core(osm, selector=selector, broker=broker)

    result = core._on_entry_trigger(_watched(plan))

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "MATERIALIZATION_STATE_WRITE_FAILED"
    assert osm.claim_calls == []
    selector.select.assert_not_called()
    assert broker.method_calls == []


def test_paper_runtime_live_durable_row_holds_before_claim_selector_or_broker():
    """PAPER callbacks must not claim a durable LIVE materialization row."""
    plan = _plan(execution_mode="paper")
    row = _row(execution_mode="live", meta={"execution_mode": "live"})
    osm = _PreflightOSM([row, row])
    selector = MagicMock()
    broker = MagicMock()
    core = _core(
        osm,
        selector=selector,
        broker=broker,
        execution_mode="paper",
    )

    result = core._on_entry_trigger(_watched(plan, execution_mode="paper"))

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "MATERIALIZATION_STATE_WRITE_FAILED"
    assert osm.claim_calls == []
    selector.select.assert_not_called()
    assert broker.method_calls == []


@pytest.mark.parametrize(
    ("durable_owner", "durable_generation"),
    [("other-recovery-owner", 4), ("recovery-owner", 5)],
    ids=["owner-mismatch", "generation-mismatch"],
)
def test_recovered_blank_plan_requires_exact_owner_generation_before_selector_or_materialization(
    durable_owner, durable_generation
):
    """Restarted blank plans cannot bypass an unproven recovery preclaim."""
    owner = "recovery-owner"
    generation = 4
    lease = (datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat()
    plan = _plan(execution_mode="live")
    row = _row(
        execution_mode="live",
        meta={
            "lifecycle_state": "MATERIALIZING",
            "materialization_status": "RUNNING",
            "materialization_in_flight": True,
            "materialization_owner": durable_owner,
            "materialization_generation": durable_generation,
            "materialization_lease_until": lease,
            "retry_attempt": 1,
        },
    )
    osm = _PreflightOSM([row, row])
    selector = MagicMock()
    broker = MagicMock()
    core = _core(osm, selector=selector, broker=broker)
    core._breach_risk_check = lambda _watched: pytest.fail(
        "unproven recovery ownership must return before risk/selector/materialization"
    )
    watched = _watched(plan)
    watched.signal.update({
        "recovery_submit_fenced": True,
        "recovery_submit_owner": owner,
        "recovery_submit_generation": generation,
        "retry_attempt": 1,
        "ownership_kind": "materialization_retry",
        "_recovery_pre_claimed": True,
        "_recovery_pre_claimed_owner": owner,
        "_recovery_pre_claimed_generation": generation,
        "_recovery_pre_claimed_attempt": 1,
        "_recovery_pre_claimed_client_id": CLIENT_ID,
        "_recovery_pre_claimed_mode": "live",
    })

    result = core._on_entry_trigger(watched)

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED"
    assert osm.get_order_calls == 2  # callback preflight + exact preclaim proof
    assert osm.claim_calls == []
    assert osm.terminal_calls == []
    selector.select.assert_not_called()
    assert broker.method_calls == []


@pytest.mark.parametrize(
    ("row_mode", "meta_mode"),
    [("paper", "paper"), ("staging", "staging"), ("live", "paper")],
    ids=["live-paper-mismatch", "malformed", "contradictory"],
)
def test_invalid_durable_execution_mode_holds_before_materialization(row_mode, meta_mode):
    plan = _plan()
    osm = _PreflightOSM(
        [
            _row(execution_mode=row_mode, meta={"execution_mode": meta_mode}),
            _row(execution_mode=row_mode, meta={"execution_mode": meta_mode}),
        ]
    )
    selector = MagicMock()
    broker = MagicMock()
    core = _core(osm, selector=selector, broker=broker)

    result = core._on_entry_trigger(_watched(plan))

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "MATERIALIZATION_STATE_WRITE_FAILED"
    assert osm.claim_calls == []
    selector.select.assert_not_called()
    assert broker.method_calls == []


def test_duplicate_materialization_owner_parks_without_duplicate_work():
    lease = (datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat()
    row = _row(
        meta={
            "lifecycle_state": "MATERIALIZING",
            "materialization_status": "RUNNING",
            "materialization_in_flight": True,
            "materialization_owner": "other-worker",
            "materialization_generation": 4,
            "materialization_lease_until": lease,
            "retry_attempt": 1,
        }
    )
    osm = _PreflightOSM([row, row, row], claim_result=False)
    selector = MagicMock()
    broker = MagicMock()
    core = _core(osm, selector=selector, broker=broker)

    result = core._on_entry_trigger(_watched(_plan()))

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "MATERIALIZATION_ALREADY_OWNED"
    assert result["owner"] == "other-worker"
    assert result["generation"] == 4
    assert len(osm.claim_calls) == 1
    selector.select.assert_not_called()
    assert broker.method_calls == []


def test_real_occ_ordinary_plan_does_not_enter_deferred_preflight():
    plan = _plan(contract_symbol=REAL_OCC)
    osm = _PreflightOSM([_row(contract=REAL_OCC)])
    selector = MagicMock()
    broker = MagicMock()
    core = _core(osm, selector=selector, broker=broker)
    core._breach_risk_check = lambda _watched: False
    core._cleanup_pending_entry_order = MagicMock(return_value=True)

    result = core._on_entry_trigger(_watched(plan))

    assert result["reason_code"] == "breach_risk_check_false"
    assert osm.get_order_calls == 0
    assert osm.claim_calls == []
    selector.select.assert_not_called()
    assert broker.method_calls == []


def test_real_occ_with_stale_deferred_metadata_stays_ordinary():
    plan = _plan(
        contract_symbol=REAL_OCC,
        metadata={"contract_deferred": True},
    )
    osm = _PreflightOSM(
        [
            _row(
                contract=REAL_OCC,
                meta={"contract_deferred": True},
            )
        ]
    )
    selector = MagicMock()
    broker = MagicMock()
    core = _core(osm, selector=selector, broker=broker)
    core._breach_risk_check = lambda _watched: False
    core._cleanup_pending_entry_order = MagicMock(return_value=True)

    result = core._on_entry_trigger(_watched(plan))

    assert result["reason_code"] == "breach_risk_check_false"
    assert osm.get_order_calls == 1  # history is read, but not claimed
    assert osm.claim_calls == []
    selector.select.assert_not_called()
    assert broker.method_calls == []


def _load_seam4_fixtures():
    tests_dir = str(Path(__file__).resolve().parent)
    if tests_dir not in sys.path:
        sys.path.insert(0, tests_dir)
    return importlib.import_module("test_p0_seam4_e2e_deferred_lifecycle")


def test_blank_claim_materializes_occ_and_revalidates_actual_cost(monkeypatch):
    """Production-shaped happy path: claim -> selector -> copyback -> cost gate."""
    seam4 = _load_seam4_fixtures()
    osm = seam4._StatefulOSM()
    osm.proof_read_failures_remaining = 0
    osm.row["contract"] = ""
    osm.row["meta"].pop("contract_deferred", None)
    osm.row["meta"]["execution_mode"] = "live"
    plan = seam4._approved_plan()
    plan.contract_symbol = ""
    plan.metadata = {"queue_id": 526, "execution_mode": "live"}
    selector = seam4._CSelector(execution_price_per_share=1.26)
    capacity = seam4._DeferredCapacityMC()
    broker = seam4._Broker()
    core = seam4._build_core(osm, broker, selector, master_control=capacity)

    watched = types.SimpleNamespace(
        signal={
            "_approved_plan": plan,
            "signal_id": seam4.SIGNAL_ID,
            "local_order_id": seam4.LOCAL_ORDER_ID,
            "client_id": seam4.CLIENT_ID,
            "execution_mode": "live",
            "ticker": "SPY",
            "side": "CALL",
            "contract_symbol": "",
        },
        ticker="SPY",
        side="CALL",
        trigger_price=600.0,
        entry_trigger=600.0,
        stop_level=595.0,
        target_price=605.0,
        trigger_crossed_at=datetime.now(timezone.utc),
        last_quote_ask=600.2,
        last_quote_bid=600.1,
    )

    fake_execution = types.ModuleType("ap.execution")
    fake_execution._refresh_ask_at_submit = lambda *_args, **_kwargs: (
        1.28,
        15,
        True,
        "ok",
        {
            "spread_pct": 0.02,
            "submit_bid": 1.25,
            "submit_ask": 1.28,
            "submit_mid": 1.265,
            "submit_last": 1.28,
        },
    )
    with patch.dict(sys.modules, {"ap.execution": fake_execution}), patch(
        "ap_entry_confirmation.check_entry_confirmation",
        return_value=seam4._FakeConfirmResult(),
    ), patch("ap.db.conn", lambda: seam4._NoopConn()), patch(
        "ap.db.run_with_retry", lambda fn, *args, **kwargs: fn()
    ):
        result = core._on_entry_trigger(watched)

    assert result is None or result.get("disposition") in {None, "SUBMITTED"}
    assert len(osm.claimed_generations) == 1
    assert selector.calls == 1
    assert len(capacity.final_calls) == 2
    assert capacity.final_calls[0]["actual_selected_cost"] == pytest.approx(126.0)
    assert capacity.final_calls[1]["actual_selected_cost"] == pytest.approx(129.0)
    assert osm.row["contract"] == "C260828C00133000"
    assert osm.row["client_id"] == seam4.CLIENT_ID
    assert osm.row["execution_mode"] == "live"
    assert len(osm.post_payloads) == 1
    assert osm.post_payloads[0]["option_symbol"] == "C260828C00133000"


def test_pr526_execution_core_seam_has_no_direct_broker_or_state_mutation_surface():
    source = (Path(__file__).resolve().parents[1] / "ap_execution_core.py").read_text()
    start = source.index("    def _on_entry_trigger")
    end = source.find("\n    def ", start + 5)
    seam = source[start:] if end < 0 else source[start:end]

    assert ".submit_order(" not in seam
    assert ".cancel_order(" not in seam
    assert "INSERT INTO positions" not in seam
    assert "UPDATE positions" not in seam
    assert "proof_trades" not in seam
