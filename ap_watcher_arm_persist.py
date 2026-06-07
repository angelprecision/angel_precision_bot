# =============================================================================
# ap_watcher_arm_persist.py  —  PR #92 Audit Persistence Helper
# =============================================================================
# Single responsibility: safely merge a watcher_arm_audit object into the
# orders.meta JSONB column for a given local_order_id.
#
# Best-effort: every write is wrapped in try/except so a meta-write failure
# can NEVER block the existing arm-failure cleanup path in ap/queue.py.
# The function signature is intentionally simple to keep the queue.py
# integration small.
#
# This module is the ONLY new code that talks to Supabase. The audit
# classifier and retry loop are pure.
# =============================================================================

from __future__ import annotations

import json
import logging
from typing import Any, Callable, Optional

from ap_watcher_arm_audit import merge_into_order_meta, redact

log = logging.getLogger("ap.watcher_arm_persist")


def _default_conn_factory() -> Optional[Callable[[], Any]]:
    try:
        from ap.db import conn  # type: ignore
        return conn
    except Exception:
        return None


def persist_watcher_arm_audit(
    *,
    local_order_id: Any,
    audit: dict[str, Any],
    conn_factory: Optional[Callable[[], Any]] = None,
) -> bool:
    """Merge `audit` into orders.meta.watcher_arm_audit for this order.

    Behavior:
      * Reads existing orders.meta.
      * Calls merge_into_order_meta to do the safe merge (existing
        non-watcher_arm_audit keys are preserved).
      * Writes the result back.

    Returns True on success, False on any failure. Failures are logged
    at WARNING level — never at ERROR — because this is reporting code
    and must not look like a trading error in dashboards.

    The function NEVER raises.
    """
    if not local_order_id:
        return False
    try:
        cf = conn_factory if conn_factory is not None else _default_conn_factory()
        if cf is None:
            return False
        # Sanitize the audit one more time at the boundary as a defense in
        # depth — by contract the audit builder already redacted, but if a
        # caller crafted their own audit dict we still scrub here.
        safe_audit = redact(audit)
        with cf() as cur:
            cur.execute(
                "SELECT meta FROM orders WHERE local_order_id=%s",
                [str(local_order_id)],
            )
            row = cur.fetchone()
            if row is None:
                # No order row to attach the audit to; nothing we can do
                # without creating a duplicate. Return False quietly.
                return False
            existing_meta = row[0] if not isinstance(row, dict) else row.get("meta")
            merged = merge_into_order_meta(existing_meta, safe_audit)
            cur.execute(
                "UPDATE orders SET meta=%s WHERE local_order_id=%s",
                [json.dumps(merged, default=str), str(local_order_id)],
            )
        return True
    except Exception as e:
        log.warning(
            "persist_watcher_arm_audit failed for local_order_id=%s: %s",
            local_order_id, e,
        )
        return False


__all__ = ["persist_watcher_arm_audit"]
