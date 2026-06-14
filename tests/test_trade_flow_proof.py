from __future__ import annotations

import json
import logging

from ap.trade_flow_proof import emit_trade_flow_proof


def test_emit_trade_flow_proof_is_structured_and_stable(caplog):
    caplog.set_level(logging.INFO, logger="ap.trade_flow")

    emit_trade_flow_proof(
        "master_control",
        client_id="jasoncosby1@gmail.com",
        signal_id="sig-123",
        ticker="AAPL",
        status="blocked",
        reason="capital_limit_no_remaining",
        execution_mode="LIVE",
        nested={"ok": True},
    )

    assert len(caplog.records) == 1
    message = caplog.records[0].message
    assert message.startswith("TRADE_FLOW_PROOF ")

    payload = json.loads(message.removeprefix("TRADE_FLOW_PROOF "))
    assert payload["event"] == "TRADE_FLOW_PROOF"
    assert payload["stage"] == "master_control"
    assert payload["client_id"] == "jasoncosby1@gmail.com"
    assert payload["signal_id"] == "sig-123"
    assert payload["ticker"] == "AAPL"
    assert payload["status"] == "blocked"
    assert payload["reason"] == "capital_limit_no_remaining"
    assert payload["execution_mode"] == "LIVE"
    assert payload["nested"] == {"ok": True}
