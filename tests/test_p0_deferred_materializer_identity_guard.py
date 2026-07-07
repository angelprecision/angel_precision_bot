from __future__ import annotations

from ap.deferred_materializer import is_broker_ready_from_meta, stamp_selected


class FakeOSM:
    def __init__(self):
        self.meta = {}

    def update_order_meta(self, local_order_id, patch):
        self.meta.update(patch)
        return True


def test_stamp_selected_blocks_blank_execution_mode():
    osm = FakeOSM()
    ok = stamp_selected(
        osm,
        "order-1",
        client_id="client@example.com",
        execution_mode="",
        symbol="META",
        direction="CALL",
        contract="META260717C00100000",
        bid=1.0,
        ask=1.1,
        mid=1.05,
        limit_price=1.1,
        qty=1,
        reserved_cost=110,
    )
    assert ok is False
    assert osm.meta["broker_ready"] is False
    assert osm.meta["materialization_status"] == "FAILED_TERMINAL"
    assert osm.meta["materialization_identity_ok"] is False


def test_stamp_selected_allows_explicit_live_identity():
    osm = FakeOSM()
    ok = stamp_selected(
        osm,
        "order-2",
        client_id="client@example.com",
        execution_mode="live",
        symbol="META",
        direction="CALL",
        contract="META260717C00100000",
        bid=1.0,
        ask=1.1,
        mid=1.05,
        limit_price=1.1,
        qty=1,
        reserved_cost=110,
    )
    assert ok is True
    assert osm.meta["broker_ready"] is True
    assert osm.meta["materialization_identity_ok"] is True
    assert is_broker_ready_from_meta(osm.meta) is True


def test_broker_ready_meta_rejects_failed_identity():
    assert is_broker_ready_from_meta({"broker_ready": True, "materialization_identity_ok": False}) is False
