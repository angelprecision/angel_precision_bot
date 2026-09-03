"""Canonical broker-submit identity shared by payload and recovery paths."""

import hashlib
import json

BROKER_SUBMIT_KEY_MAX_LENGTH = 32


def canonical_broker_submit_key(local_order_id: str) -> str:
    """Return the exact client tag sent to Tradier for broker reconciliation."""
    return str(local_order_id or "").strip()[:BROKER_SUBMIT_KEY_MAX_LENGTH]


def build_entry_submit_payload(
    *,
    symbol: str,
    contract: str,
    qty: int,
    limit_price: float,
    broker_submit_key: str,
) -> dict:
    """Build the canonical Tradier ENTRY payload used for intent hashing.

    Reconciliation must prove the same payload identity that the existing
    submit path persisted before POST.  Keeping this small payload builder in
    the identity module prevents a recovery path from inventing a second
    broker-submit shape.
    """
    return {
        "class": "option",
        "symbol": str(symbol or ""),
        "option_symbol": str(contract or ""),
        "side": "buy_to_open",
        "quantity": int(qty),
        "type": "limit",
        "price": round(float(limit_price), 2),
        "duration": "day",
        "tag": canonical_broker_submit_key(broker_submit_key),
    }


def entry_submit_payload_hash(payload: dict) -> str:
    """Return the stable hash persisted with a canonical ENTRY intent."""
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
