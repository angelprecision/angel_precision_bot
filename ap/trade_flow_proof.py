from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("ap.trade_flow")


def _safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    try:
        json.dumps(value)
        return value
    except Exception:
        return str(value)


def emit_trade_flow_proof(
    stage: str,
    *,
    client_id: str | None = None,
    signal_id: str | None = None,
    ticker: str | None = None,
    status: str | None = None,
    reason: str | None = None,
    **fields: Any,
) -> None:
    """Emit one stable breadcrumb for the scanner-to-submit entry pipeline."""
    payload = {
        "event": "TRADE_FLOW_PROOF",
        "stage": str(stage or "unknown"),
        "status": str(status or "unknown"),
        "client_id": str(client_id or ""),
        "signal_id": str(signal_id or ""),
        "ticker": str(ticker or ""),
        "reason": str(reason or ""),
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    for key, value in fields.items():
        if value is not None:
            payload[str(key)] = _safe(value)
    log.info("TRADE_FLOW_PROOF %s", json.dumps(payload, sort_keys=True, separators=(",", ":")))
