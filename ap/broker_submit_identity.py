"""Canonical broker-submit identity shared by payload and recovery paths."""

BROKER_SUBMIT_KEY_MAX_LENGTH = 32


def canonical_broker_submit_key(local_order_id: str) -> str:
    """Return the exact client tag sent to Tradier for broker reconciliation."""
    return str(local_order_id or "").strip()[:BROKER_SUBMIT_KEY_MAX_LENGTH]
