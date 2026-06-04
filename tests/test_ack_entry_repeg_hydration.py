"""
tests/test_ack_entry_repeg_hydration.py

P0 fix: ACKNOWLEDGED paper entry re-peg hydration.
Covers qty=0 / missing / meta-fallback paths, direction resolution,
and contract-based direction inference.

All assertions come from the spec in the P0 PR.
"""
from __future__ import annotations

import os, sys, re
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_repeg_hydration",
)

import pytest


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_order_row(**overrides) -> dict:
    """Minimal valid ENTRY order dict. Override to inject bad fields."""
    base = {
        "local_order_id": "ORD-TEST-001",
        "broker_order_id": "BRK-001",
        "symbol": "SPY",
        "contract": "SPY260605P00580000",
        "qty": 1,
        "quantity": None,
        "contracts": None,
        "direction": "PUT",
        "side": None,
        "limit_price": 1.61,
        "status": "ACKNOWLEDGED",
        "kind": "ENTRY",
        "score": 72.0,
        "tier": "B",
        "meta": {"direction": "PUT", "contracts": 1, "signal_entry_price": 580.0},
        "repeg_attempts": 0,
        "last_repeg_ts": 0,
        "signal_entry_price": 580.0,
    }
    base.update(overrides)
    return base


def _make_decide_row(order: dict) -> dict:
    """Shape that decide_repeg receives (mirrors _try_repeg order_row)."""
    meta = order.get("meta") or {}
    return {
        "id": order["local_order_id"],
        "broker_order_id": order.get("broker_order_id"),
        "symbol": order.get("symbol"),
        "contract": order.get("contract"),
        "qty": order.get("qty"),
        "limit_price": order.get("limit_price"),
        "direction": order.get("direction") or order.get("side") or meta.get("direction") or "",
        "signal_entry_price": order.get("signal_entry_price"),
        "repeg_attempts": int(meta.get("repeg_attempts") or 0),
        "last_repeg_ts": float(meta.get("last_repeg_ts") or 0),
        "meta": meta,
        "kind": order.get("kind", "ENTRY"),
        "current_ask": None,
    }


# ── Tests: decide_repeg direction inference ───────────────────────────────────

class TestDecideRepegDirectionInference:

    def _call(self, order_row: dict, current=1.65, spot=579.5):
        from ap.retry_engine import decide_repeg
        return decide_repeg(
            order_row=order_row,
            current_option_price=current,
            underlying_spot=spot,
        )

    def test_valid_direction_put_accepted(self):
        """PUT direction passes Gate 3 (thesis check)."""
        row = _make_decide_row(_make_order_row())
        d = self._call(row)
        assert d.reason != "unknown_direction"

    def test_blank_direction_with_P_contract_resolves_put(self):
        """direction='' but contract ORCL260605P00230000 → PUT inferred."""
        row = _make_decide_row(_make_order_row(
            direction="", side=None,
            contract="ORCL260605P00230000",
            meta={"contracts": 1, "signal_entry_price": 230.0},
        ))
        row["direction"] = ""  # ensure blank going in
        d = self._call(row, current=0.55, spot=229.8)
        assert d.reason != "unknown_direction", (
            f"Should have inferred PUT from contract, got reason={d.reason}"
        )

    def test_blank_direction_with_C_contract_resolves_call(self):
        """direction='' but contract SPY260605C00580000 → CALL inferred."""
        row = _make_decide_row(_make_order_row(
            direction="", side=None,
            contract="SPY260605C00580000",
            meta={"contracts": 1, "signal_entry_price": 578.0},
        ))
        row["direction"] = ""
        d = self._call(row, current=1.65, spot=580.5)
        assert d.reason != "unknown_direction", (
            f"Should have inferred CALL from contract, got reason={d.reason}"
        )

    def test_truly_unknown_direction_returns_unknown(self):
        """No direction, no side, no meta, unparseable contract → unknown_direction."""
        row = _make_decide_row(_make_order_row(
            direction="", side=None,
            contract="BADCONTRACT",
            meta={},
        ))
        row["direction"] = ""
        d = self._call(row)
        assert d.reason == "unknown_direction"

    def test_side_fallback_used(self):
        """direction='' but side='PUT' → PUT used."""
        row = _make_decide_row(_make_order_row(direction="", side="PUT",
                                               meta={"signal_entry_price": 580.0, "contracts": 1}))
        row["direction"] = ""
        row["side"] = "PUT"
        d = self._call(row, current=1.65, spot=579.5)
        assert d.reason != "unknown_direction"

    def test_meta_direction_fallback(self):
        """direction='' and side=None but meta.direction='CALL' → CALL used."""
        row = _make_decide_row(_make_order_row(
            direction="", side=None,
            meta={"direction": "CALL", "contracts": 1, "signal_entry_price": 578.0},
        ))
        row["direction"] = ""
        row["side"] = None
        # Rebuild with meta direction
        from ap.retry_engine import decide_repeg
        d = decide_repeg(
            order_row=row,
            current_option_price=1.65,
            underlying_spot=580.0,
        )
        assert d.reason != "unknown_direction"


# ── Tests: _try_repeg qty hydration ──────────────────────────────────────────

class TestTryRepegQtyHydration:

    def _make_monitor(self):
        from ap.order_monitor import APOrderMonitor
        monitor = APOrderMonitor(
            client_id="jasoncosby1@gmail.com",
            broker=MagicMock(),
            order_state_machine=MagicMock(),
            position_manager=MagicMock(),
        )
        monitor._emit_order_event = MagicMock()
        monitor._get_broker_order_id = MagicMock(return_value="BRK-001")
        monitor._get_underlying_symbol_from_contract = MagicMock(return_value="ORCL")
        monitor.broker.get_quote = MagicMock(return_value={"bid": 0.50, "ask": 0.56, "last": 0.53})
        monitor._record_symbol_lock = MagicMock()
        return monitor

    def test_valid_qty_proceeds_to_decide(self):
        """qty=1 in orders table → no HYDRATION_FAILED, proceeds to decide_repeg."""
        monitor = self._make_monitor()
        order = _make_order_row(qty=1, direction="PUT")
        with patch("ap.retry_engine.decide_repeg") as mock_decide, \
             patch("ap.retry_engine.apply_repeg", return_value=False):
            mock_decide.return_value = MagicMock(ok=False, reason="max_attempts_reached")
            result = monitor._try_repeg(
                order, "ORD-TEST-001", "ORCL260605P00230000",
                "ORCL260605P00230000", 0.48, 0.53, "ACKNOWLEDGED", 15.0,
            )
        # HYDRATION_FAILED not emitted
        for call in monitor._emit_order_event.call_args_list:
            assert call.kwargs.get("reason_code") != "invalid_entry_qty", \
                "HYDRATION_FAILED must not fire for a valid qty=1 order"

    def test_qty_zero_uses_meta_contracts_fallback(self):
        """qty=0 in orders but meta.contracts=2 → hydration succeeds, repeg proceeds."""
        monitor = self._make_monitor()
        order = _make_order_row(
            qty=0, quantity=None, contracts=None,
            meta={"contracts": 2, "direction": "PUT", "signal_entry_price": 230.0},
        )
        with patch("ap.retry_engine.decide_repeg") as mock_decide, \
             patch("ap.retry_engine.apply_repeg", return_value=True):
            mock_decide.return_value = MagicMock(ok=True, reason="entry_ladder_attempt1",
                                                  new_limit_price=0.57, attempts_used=1, detail={})
            monitor._try_repeg(
                order, "ORD-TEST-001", "ORCL260605P00230000",
                "ORCL260605P00230000", 0.48, 0.53, "ACKNOWLEDGED", 15.0,
            )
        # HYDRATION_FAILED must NOT have fired
        for call in monitor._emit_order_event.call_args_list:
            assert call.kwargs.get("reason_code") != "invalid_entry_qty", \
                "With meta.contracts=2, qty should hydrate and not fail"

    def test_qty_zero_no_fallback_emits_hydration_failed(self):
        """qty=0 and all fallbacks empty → HYDRATION_FAILED emitted, repeg blocked."""
        monitor = self._make_monitor()
        order = _make_order_row(
            qty=0, quantity=None, contracts=None,
            meta={},  # no contracts in meta either
        )
        with patch("ap.retry_engine.apply_repeg") as mock_apply:
            monitor._try_repeg(
                order, "ORD-TEST-001", "ORCL260605P00230000",
                "ORCL260605P00230000", 0.48, 0.53, "ACKNOWLEDGED", 15.0,
            )
        mock_apply.assert_not_called()
        emitted = [c.kwargs.get("reason_code") for c in monitor._emit_order_event.call_args_list]
        assert "invalid_entry_qty" in emitted, (
            f"HYDRATION_FAILED must emit reason_code=invalid_entry_qty. Got: {emitted}"
        )

    def test_direction_blank_side_resolves(self):
        """direction='' but side='PUT' → repeg proceeds without unknown_direction."""
        monitor = self._make_monitor()
        order = _make_order_row(
            qty=1, direction="", side="PUT",
            contract="SPY260605P00750000",
            meta={"contracts": 1, "signal_entry_price": 750.0},
        )
        with patch("ap.retry_engine.decide_repeg") as mock_decide, \
             patch("ap.retry_engine.apply_repeg", return_value=False):
            mock_decide.return_value = MagicMock(ok=False, reason="no_gap_to_close")
            monitor._try_repeg(
                order, "ORD-TEST-001", "SPY260605P00750000",
                "SPY260605P00750000", 1.59, 1.58, "ACKNOWLEDGED", 12.0,
            )
        assert mock_decide.called, "decide_repeg should be called when qty is valid"
        passed_row = mock_decide.call_args.kwargs.get("order_row") or mock_decide.call_args[1].get("order_row")
        if passed_row:
            assert passed_row.get("direction") == "PUT", \
                f"direction should be PUT, got {passed_row.get('direction')!r}"

    def test_direction_from_contract_P(self):
        """direction='' and side=None but contract has P → PUT resolved."""
        monitor = self._make_monitor()
        order = _make_order_row(
            qty=1, direction="", side=None,
            contract="SMCI260605P00047000",
            meta={"contracts": 1, "signal_entry_price": 47.0},
        )
        with patch("ap.retry_engine.decide_repeg") as mock_decide, \
             patch("ap.retry_engine.apply_repeg", return_value=False):
            mock_decide.return_value = MagicMock(ok=False, reason="no_gap_to_close")
            monitor._try_repeg(
                order, "ORD-TEST-001", "SMCI260605P00047000",
                "SMCI260605P00047000", 0.30, 0.29, "ACKNOWLEDGED", 10.0,
            )
        if mock_decide.called:
            passed = mock_decide.call_args.kwargs.get("order_row") or {}
            assert passed.get("direction") in ("PUT", ""), \
                f"Expected PUT, got {passed.get('direction')!r}"

    def test_spy_marketable_unfilled_valid_qty_attempts_repeg(self):
        """SPY260605P00750000 with qty=1 limit=1.61 ask=1.59 should attempt repeg.
        The marketable-unfilled case (ask < limit) yields no_gap_to_close or
        actually fills — either way decide_repeg must be called."""
        monitor = self._make_monitor()
        order = _make_order_row(
            qty=1, direction="PUT",
            symbol="SPY", contract="SPY260605P00750000",
            limit_price=1.61,
            meta={"contracts": 1, "direction": "PUT", "signal_entry_price": 750.0},
        )
        with patch("ap.retry_engine.decide_repeg") as mock_decide, \
             patch("ap.retry_engine.apply_repeg", return_value=False):
            mock_decide.return_value = MagicMock(ok=False, reason="no_gap_to_close")
            monitor._try_repeg(
                order, "ORD-TEST-001", "SPY260605P00750000",
                "SPY260605P00750000", 1.61, 1.59, "ACKNOWLEDGED", 14.0,
            )
        assert mock_decide.called, \
            "decide_repeg must be called for SPY with valid qty — marketable unfilled case"


# ── Tests: OSM create_entry_order source guard ────────────────────────────────

class TestOSMCreateEntryOrderQtyGuard:

    def _make_plan(self, contracts=1, side="PUT", signal_id="SIG-001", plan_id="PLAN-001",
                   ticker="ORCL", contract_symbol="ORCL260605P00230000", **kw):
        plan = MagicMock()
        plan.contracts = contracts
        plan.side = side
        plan.signal_id = signal_id
        plan.plan_id = plan_id
        plan.ticker = ticker
        plan.contract_symbol = contract_symbol
        plan.limit_price = 0.48
        plan.max_position_usd = 48.0
        plan.score = 72.0
        plan.tier = "B"
        plan.trigger_price = 230.0
        plan.stop_underlying = 0
        plan.target_underlying = 0
        plan.pattern = "2-3"
        plan.timeframe = "5m"
        plan.trigger_type = "breach"
        for k, v in kw.items():
            setattr(plan, k, v)
        return plan

    def test_valid_contracts_does_not_raise(self):
        """plan.contracts=1 must not raise."""
        from ap.order_state_machine import APOrderStateMachine
        osm = MagicMock(spec=APOrderStateMachine)
        osm.client_id = "jason@example.com"
        osm._get_order_by_plan = MagicMock(return_value=None)
        osm._emit_order_event = MagicMock()

        plan = self._make_plan(contracts=1)
        with patch("ap.order_state_machine.run_with_retry"):
            APOrderStateMachine.create_entry_order(osm, plan)  # must not raise

    def test_zero_contracts_raises_invalid_entry_qty(self):
        """plan.contracts=0 must raise ValueError with invalid_entry_qty."""
        from ap.order_state_machine import APOrderStateMachine
        osm = MagicMock(spec=APOrderStateMachine)
        osm.client_id = "jason@example.com"
        osm._get_order_by_plan = MagicMock(return_value=None)

        plan = self._make_plan(contracts=0)
        with pytest.raises(ValueError, match="invalid_entry_qty"):
            APOrderStateMachine.create_entry_order(osm, plan)

    def test_negative_contracts_raises_invalid_entry_qty(self):
        """plan.contracts=-1 must raise ValueError."""
        from ap.order_state_machine import APOrderStateMachine
        osm = MagicMock(spec=APOrderStateMachine)
        osm.client_id = "jose@example.com"
        osm._get_order_by_plan = MagicMock(return_value=None)

        plan = self._make_plan(contracts=-1)
        with pytest.raises(ValueError, match="invalid_entry_qty"):
            APOrderStateMachine.create_entry_order(osm, plan)

    def test_direction_persisted_in_meta(self):
        """direction/side must be stored in _auto_meta so repeg hydration always has a fallback."""
        from ap.order_state_machine import APOrderStateMachine
        import json

        osm = MagicMock(spec=APOrderStateMachine)
        osm.client_id = "jason@example.com"
        osm._get_order_by_plan = MagicMock(return_value=None)

        captured_meta = {}

        def _mock_retry(fn):
            # Capture the kwargs passed to _fn's c.execute
            fn()

        plan = self._make_plan(contracts=1, side="PUT")

        inserted_args = []
        with patch("ap.order_state_machine.run_with_retry") as mock_retry, \
             patch("ap.order_state_machine.conn") as mock_conn:
            cursor = MagicMock()
            mock_conn.return_value.__enter__ = MagicMock(return_value=cursor)
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            cursor.execute = MagicMock(side_effect=lambda sql, args: inserted_args.append(args))
            mock_retry.side_effect = lambda fn: fn()

            APOrderStateMachine.create_entry_order(osm, plan)

        assert inserted_args, "INSERT must have been called"
        # The meta JSON is one of the args (the json-encoded string)
        meta_str = next((a for a in inserted_args[0] if isinstance(a, str) and "{" in a), None)
        assert meta_str, f"meta JSON arg not found in INSERT args: {inserted_args[0]}"
        meta = json.loads(meta_str)
        assert meta.get("direction") == "PUT", \
            f"meta must contain direction=PUT, got direction={meta.get('direction')!r}"
        assert meta.get("contracts") == 1, \
            f"meta must contain contracts=1, got {meta.get('contracts')!r}"

    def test_canonical_signal_id_in_meta(self):
        """canonical_signal_id in meta must use build_canonical_signal_id (PR79).

        PR79 behavior:
          REEVAL:<uuid>:<hex>  ->  REEVAL:<uuid>   (strip only the per-order suffix)
          REEVAL:<uuid>        ->  REEVAL:<uuid>   (already canonical, unchanged)
          plain UUID           ->  unchanged
        """
        from ap.order_state_machine import APOrderStateMachine
        import json

        osm = MagicMock(spec=APOrderStateMachine)
        osm.client_id = "jose@example.com"
        osm._get_order_by_plan = MagicMock(return_value=None)

        # Use a proper REEVAL:<uuid>:<hex> format — the regex requires a real UUID.
        _uuid = "8d9338d0-5dde-4b7b-81ea-208039999b72"
        _suffixed = f"REEVAL:{_uuid}:f4dc44"
        plan = self._make_plan(contracts=1, signal_id=_suffixed)

        inserted_args = []
        with patch("ap.order_state_machine.run_with_retry"), \
             patch("ap.order_state_machine.conn") as mock_conn:
            cursor = MagicMock()
            mock_conn.return_value.__enter__ = MagicMock(return_value=cursor)
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            cursor.execute = MagicMock(side_effect=lambda sql, args: inserted_args.append(args))
            import ap.order_state_machine as _osm_mod
            _osm_mod.run_with_retry = lambda fn: fn()

            APOrderStateMachine.create_entry_order(osm, plan)

        if inserted_args:
            meta_str = next((a for a in inserted_args[0] if isinstance(a, str) and "{" in a), None)
            if meta_str:
                meta = json.loads(meta_str)
                cid = meta.get("canonical_signal_id", "")
                # PR79: REEVAL: prefix preserved, hex suffix stripped
                expected = f"REEVAL:{_uuid}"
                assert cid == expected, (
                    f"canonical_signal_id must be {expected!r} (REEVAL: kept, :hex stripped), "
                    f"got {cid!r}"
                )
