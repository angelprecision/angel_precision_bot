from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from ap.broker_submit_identity import canonical_broker_submit_key

log = logging.getLogger("ap.broker_submit_reconciliation_guard")

_OCC_RE = re.compile(r"[A-Z0-9.]{1,6}\d{6}[CP]\d{8}")
_SETTLEMENT_SECONDS = 8.0
_CONFIRM_SECONDS = 3.0
_TRADIER_PAGE_LIMIT = 1500
_TRADIER_MAX_PAGES = 20


def _meta(row: dict) -> dict:
    value = row.get("meta") or {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return {}
    return value if isinstance(value, dict) else {}


def _parse_ts(value: Any) -> datetime | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _strict_positive_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed > 0 else None


def _exact_live_submit_identity(subject, row: dict) -> tuple[dict | None, str]:
    """Prove the exact durable LIVE submit-intent shape before liveness work."""
    if not isinstance(row, dict):
        return None, "row_missing"

    from ap.order_state_machine import _durable_execution_mode

    meta = _meta(row)
    expected_client = str(
        getattr(subject, "client_id", None)
        or getattr(subject, "email", None)
        or ""
    ).strip().lower()
    row_client = str(row.get("client_id") or "").strip().lower()
    if not expected_client or row_client != expected_client:
        return None, "client_id_mismatch"

    mode = _durable_execution_mode(row, meta)
    runtime_mode = str(
        getattr(subject, "execution_mode", None)
        or getattr(subject, "mode", None)
        or ""
    ).strip().lower()
    if runtime_mode not in {"live", "paper"}:
        return None, "runtime_mode_invalid"
    if mode != "live" or runtime_mode != "live":
        return None, "not_live"
    if str(row.get("kind") or "").strip().upper() != "ENTRY":
        return None, "kind_mismatch"
    if str(row.get("status") or "").strip().upper() != "PENDING_TRIGGER":
        return None, "status_mismatch"
    if str(row.get("broker_order_id") or "").strip() or row.get("submitted_ts"):
        return None, "broker_identity_already_present"

    contract = str(row.get("contract") or "").strip().upper()
    qty = _strict_positive_int(row.get("qty"))
    try:
        limit_price = float(row.get("limit_price") or 0)
    except (TypeError, ValueError, OverflowError):
        limit_price = 0.0
    if not _OCC_RE.fullmatch(contract):
        return None, "contract_invalid"
    if qty is None or limit_price <= 0:
        return None, "pricing_invalid"

    if str(meta.get("lifecycle_state") or "").strip().upper() != "SUBMITTING":
        return None, "lifecycle_not_submitting"
    submit_intent_at = str(meta.get("submit_intent_at") or "").strip()
    submit_dt = _parse_ts(submit_intent_at)
    raw_key = str(meta.get("broker_submit_key") or "").strip()
    payload_hash = str(meta.get("broker_submit_payload_hash") or "").strip()
    generation = _strict_positive_int(meta.get("materialization_generation"))
    if submit_dt is None or not raw_key or not payload_hash or generation is None:
        return None, "submit_intent_proof_incomplete"

    submit_key = canonical_broker_submit_key(raw_key)
    expected_owner = f"broker_submit:{submit_key}"
    if str(meta.get("current_owner") or "").strip() != expected_owner:
        return None, "broker_submit_owner_mismatch"

    selected_contract = str(meta.get("selected_contract") or "").strip().upper()
    if selected_contract and selected_contract != contract:
        return None, "selected_contract_mismatch"
    selected_qty_raw = meta.get("selected_qty")
    if selected_qty_raw not in (None, ""):
        selected_qty = _strict_positive_int(selected_qty_raw)
        if selected_qty != qty:
            return None, "selected_qty_mismatch"

    first_no_match_at = str(
        meta.get("broker_reconcile_no_match_observed_at") or ""
    ).strip()
    first_no_match_matches = bool(
        first_no_match_at
        and str(meta.get("broker_reconcile_no_match_submit_key") or "").strip()
            == submit_key
        and str(meta.get("broker_reconcile_no_match_payload_hash") or "").strip()
            == payload_hash
        and _strict_positive_int(meta.get("broker_reconcile_no_match_generation"))
            == generation
    )

    return {
        "client_id": row_client,
        "execution_mode": mode,
        "contract": contract,
        "qty": qty,
        "limit_price": limit_price,
        "submit_intent_at": submit_intent_at,
        "submit_dt": submit_dt,
        "broker_submit_key": submit_key,
        "payload_hash": payload_hash,
        "generation": generation,
        "current_owner": expected_owner,
        "first_no_match_at": first_no_match_at if first_no_match_matches else "",
    }, "ok"


def _patch_tradier_list_orders() -> None:
    from ap.brokers.tradier import TradierBroker

    current = TradierBroker.list_orders
    if getattr(current, "_ap_exact_tag_query", False):
        return

    def list_orders(self):
        """Return a complete current-session order set with Tradier tags."""
        all_orders: list[dict] = []
        path = f"/v1/accounts/{self.cfg.account_id}/orders"
        for page in range(1, _TRADIER_MAX_PAGES + 1):
            payload = self._get(
                path,
                params={
                    "includeTags": "true",
                    "limit": _TRADIER_PAGE_LIMIT,
                    "page": page,
                },
            )
            if not isinstance(payload, dict) or "orders" not in payload:
                raise ValueError("TRADIER_ORDERS_PAYLOAD_MALFORMED:root")
            node = payload.get("orders")
            orders = node.get("order") if isinstance(node, dict) else node
            if orders is None:
                batch: list[dict] = []
            elif isinstance(orders, str) and orders.strip().lower() in {"", "null"}:
                batch = []
            elif isinstance(orders, dict):
                batch = [orders]
            elif isinstance(orders, list):
                if any(not isinstance(order, dict) for order in orders):
                    raise ValueError("TRADIER_ORDERS_PAYLOAD_MALFORMED:list_item")
                batch = list(orders)
            else:
                raise ValueError(
                    f"TRADIER_ORDERS_PAYLOAD_MALFORMED:{type(orders).__name__}"
                )
            all_orders.extend(batch)
            if len(batch) < _TRADIER_PAGE_LIMIT:
                return all_orders
        raise RuntimeError("TRADIER_ORDERS_PAGINATION_EXHAUSTED")

    list_orders._ap_exact_tag_query = True
    list_orders._ap_original = current
    TradierBroker.list_orders = list_orders


def _record_first_no_match(local_order_id: str, identity: dict, observed_at: str) -> bool:
    from ap.db import conn, run_with_retry
    from ap.order_state_machine import _DURABLE_EXECUTION_MODE_SQL

    patch = json.dumps({
        "broker_reconcile_no_match_observed_at": observed_at,
        "broker_reconcile_no_match_submit_key": identity["broker_submit_key"],
        "broker_reconcile_no_match_payload_hash": identity["payload_hash"],
        "broker_reconcile_no_match_generation": identity["generation"],
    })

    def _write():
        with conn() as c:
            cur = c.execute(
                """
                UPDATE orders
                SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                    updated_ts = NOW()
                WHERE local_order_id = %s
                  AND client_id = %s
                  AND """ + _DURABLE_EXECUTION_MODE_SQL + """
                  AND kind = 'ENTRY'
                  AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                  AND (broker_order_id IS NULL OR broker_order_id = '')
                  AND submitted_ts IS NULL
                  AND UPPER(COALESCE(contract,'')) = %s
                  AND qty = %s
                  AND limit_price > 0
                  AND COALESCE(meta->>'lifecycle_state','') = 'SUBMITTING'
                  AND COALESCE(meta->>'submit_intent_at','') = %s
                  AND COALESCE(meta->>'broker_submit_key','') = %s
                  AND COALESCE(meta->>'broker_submit_payload_hash','') = %s
                  AND COALESCE(meta->>'current_owner','') = %s
                  AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                  AND COALESCE(meta->>'broker_reconcile_no_match_observed_at','') = ''
                """,
                (
                    patch,
                    local_order_id,
                    identity["client_id"],
                    identity["execution_mode"],
                    identity["contract"],
                    identity["qty"],
                    identity["submit_intent_at"],
                    identity["broker_submit_key"],
                    identity["payload_hash"],
                    identity["current_owner"],
                    identity["generation"],
                ),
            )
            return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

    try:
        return bool(run_with_retry(_write) > 0)
    except Exception:
        return False


def _release_after_proven_absence(
    local_order_id: str,
    identity: dict,
    *,
    first_no_match_at: str,
    proven_at: str,
) -> bool:
    """Restore BROKER_READY only after two complete broker no-match observations."""
    from ap.db import conn, run_with_retry
    from ap.order_state_machine import _DURABLE_EXECUTION_MODE_SQL

    patch = json.dumps({
        "lifecycle_state": "BROKER_READY",
        "broker_ready": True,
        "submit_intent_at": "",
        "broker_submit_key": "",
        "broker_submit_payload_hash": "",
        "current_owner": "",
        "recovery_submit_owner": "",
        "recovery_submit_lease_until": "",
        "broker_reconcile_absence_proven_at": proven_at,
        "broker_reconcile_absence_first_observed_at": first_no_match_at,
        "broker_reconcile_absence_submit_key": identity["broker_submit_key"],
        "broker_reconcile_absence_payload_hash": identity["payload_hash"],
        "broker_reconcile_absence_generation": identity["generation"],
        "broker_reconcile_resume_authorized": True,
    })

    def _write():
        with conn() as c:
            cur = c.execute(
                """
                UPDATE orders
                SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                    updated_ts = NOW()
                WHERE local_order_id = %s
                  AND client_id = %s
                  AND """ + _DURABLE_EXECUTION_MODE_SQL + """
                  AND kind = 'ENTRY'
                  AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                  AND (broker_order_id IS NULL OR broker_order_id = '')
                  AND submitted_ts IS NULL
                  AND UPPER(COALESCE(contract,'')) = %s
                  AND qty = %s
                  AND limit_price > 0
                  AND COALESCE(meta->>'lifecycle_state','') = 'SUBMITTING'
                  AND COALESCE(meta->>'submit_intent_at','') = %s
                  AND COALESCE(meta->>'broker_submit_key','') = %s
                  AND COALESCE(meta->>'broker_submit_payload_hash','') = %s
                  AND COALESCE(meta->>'current_owner','') = %s
                  AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                  AND COALESCE(meta->>'broker_reconcile_no_match_observed_at','') = %s
                  AND COALESCE(meta->>'broker_reconcile_no_match_submit_key','') = %s
                  AND COALESCE(meta->>'broker_reconcile_no_match_payload_hash','') = %s
                  AND COALESCE((meta->>'broker_reconcile_no_match_generation')::int, 0) = %s
                """,
                (
                    patch,
                    local_order_id,
                    identity["client_id"],
                    identity["execution_mode"],
                    identity["contract"],
                    identity["qty"],
                    identity["submit_intent_at"],
                    identity["broker_submit_key"],
                    identity["payload_hash"],
                    identity["current_owner"],
                    identity["generation"],
                    first_no_match_at,
                    identity["broker_submit_key"],
                    identity["payload_hash"],
                    identity["generation"],
                ),
            )
            return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

    try:
        return bool(run_with_retry(_write) > 0)
    except Exception:
        return False


def _patch_reconciler() -> None:
    import ap_execution_core

    cls = ap_execution_core.APExecutionCore
    original = cls.reconcile_deferred_broker_intent
    if getattr(original, "_ap_live_submit_liveness", False):
        return

    def reconcile_deferred_broker_intent(self, *, local_order_id: str) -> dict:
        result = original(self, local_order_id=local_order_id) or {}
        if str(result.get("reason_code") or "") != "RECONCILE_BROKER_NO_MATCH_HELD":
            return result

        osm = getattr(self, "order_state_machine", None) or getattr(self, "osm", None)
        if osm is None:
            return {**result, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_OSM_UNAVAILABLE"}
        try:
            row = osm.get_order(local_order_id)
        except Exception as exc:
            return {**result, "disposition": "RECONCILE_PENDING", "reason_code": f"RECONCILE_ROW_READ_ERROR:{type(exc).__name__}"}

        identity, reason = _exact_live_submit_identity(self, row)
        if identity is None:
            return {**result, "disposition": "RECONCILE_PENDING", "reason_code": f"RECONCILE_IDENTITY_UNPROVEN:{reason}"}

        now = datetime.now(timezone.utc)
        intent_age = (now - identity["submit_dt"]).total_seconds()
        first_raw = identity["first_no_match_at"]
        first_dt = _parse_ts(first_raw)
        if not first_raw:
            observed_at = now.isoformat()
            if not _record_first_no_match(local_order_id, identity, observed_at):
                return {**result, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_NO_MATCH_PROOF_CAS_LOST"}
            return {
                **result,
                "disposition": "RECONCILE_PENDING",
                "reason_code": "RECONCILE_BROKER_NO_MATCH_SETTLING",
                "next_retry_after_seconds": max(1, int(_SETTLEMENT_SECONDS)),
            }
        if first_dt is None:
            return {**result, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_NO_MATCH_PROOF_MALFORMED"}

        confirm_age = (now - first_dt).total_seconds()
        if intent_age < _SETTLEMENT_SECONDS or confirm_age < _CONFIRM_SECONDS:
            return {
                **result,
                "disposition": "RECONCILE_PENDING",
                "reason_code": "RECONCILE_BROKER_NO_MATCH_SETTLING",
                "next_retry_after_seconds": max(
                    1,
                    int(max(_SETTLEMENT_SECONDS - intent_age, _CONFIRM_SECONDS - confirm_age, 1)),
                ),
            }

        if not _release_after_proven_absence(
            local_order_id,
            identity,
            first_no_match_at=first_raw,
            proven_at=now.isoformat(),
        ):
            return {**result, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_ABSENCE_RELEASE_CAS_LOST"}

        # Do not invoke the restart helper from inside the reconciler. Its caller
        # owns some terminal dispositions. Restoring the exact row to BROKER_READY
        # is the durable handoff; the existing watcher/recovery path then re-enters
        # the canonical LIVE gates and canonical submit implementation.
        return {
            **result,
            "disposition": "RECONCILE_PENDING",
            "reason_code": "RECONCILE_BROKER_ABSENCE_PROVEN_CANONICAL_RESUME_REQUIRED",
            "canonical_resume_required": True,
            "next_retry_after_seconds": 1,
        }

    reconcile_deferred_broker_intent._ap_live_submit_liveness = True
    reconcile_deferred_broker_intent._ap_original = original
    cls.reconcile_deferred_broker_intent = reconcile_deferred_broker_intent


def _patch_entry_callback() -> None:
    import ap_execution_core

    cls = ap_execution_core.APExecutionCore
    original = cls._on_entry_trigger
    if getattr(original, "_ap_submit_intent_router", False):
        return

    def _reconcile_if_owned(self, watched):
        sig = getattr(watched, "signal", {}) or {}
        local_order_id = str(sig.get("local_order_id") or "").strip()
        if not local_order_id:
            return None
        osm = getattr(self, "order_state_machine", None) or getattr(self, "osm", None)
        if osm is None:
            return None
        try:
            row = osm.get_order(local_order_id)
        except Exception:
            return None
        identity, _ = _exact_live_submit_identity(self, row)
        if identity is None:
            return None
        rec = self.reconcile_deferred_broker_intent(local_order_id=local_order_id) or {}
        if str(rec.get("disposition") or "").upper() == "ALREADY_RECONCILED":
            return {**rec, "disposition": "SUBMITTED"}
        return {
            "disposition": "KEEP_WATCHER",
            "reason_code": str(rec.get("reason_code") or "BROKER_INTENT_RECONCILE_PENDING"),
            "retry_after_seconds": int(rec.get("next_retry_after_seconds") or 5),
            "local_order_id": local_order_id,
            "canonical_resume_required": bool(rec.get("canonical_resume_required")),
        }

    def _on_entry_trigger(self, watched):
        # Already-SUBMITTING means broker truth owns the next action. Never rerun
        # hydration, selector, LIVE gates, or submit-intent creation first.
        routed = _reconcile_if_owned(self, watched)
        if routed is not None:
            return routed

        result = original(self, watched)

        # A normal callback can atomically transfer BROKER_READY ownership to
        # broker_submit:* and then observe the transfer instead of a broker id.
        # Re-read immediately so that handoff enters reconciliation now, rather
        # than restarting ordinary entry work on the next poll.
        routed = _reconcile_if_owned(self, watched)
        return routed if routed is not None else result

    _on_entry_trigger._ap_submit_intent_router = True
    _on_entry_trigger._ap_original = original
    cls._on_entry_trigger = _on_entry_trigger


def _patch_recovery_retention() -> None:
    from ap.order_state_machine import APOrderStateMachine

    original = APOrderStateMachine.retain_recovery_ownership_if_no_watcher
    if getattr(original, "_ap_broker_submit_owner_retained", False):
        return

    def retain_recovery_ownership_if_no_watcher(
        self,
        local_order_id: str,
        *,
        recovery_owner: str,
        reason: str,
        recovery_retention_mode: str,
    ) -> bool:
        try:
            row = self.get_order(local_order_id)
        except Exception:
            row = None
        identity, _ = (
            _exact_live_submit_identity(self, row)
            if isinstance(row, dict)
            else (None, "row_missing")
        )
        if identity is not None:
            log.warning(
                "BROKER_SUBMIT_OWNER_RETAINED_PENDING_RECONCILIATION "
                "client_id=%s order=%s submit_key=%s reason=%s",
                self.client_id,
                local_order_id,
                identity["broker_submit_key"],
                reason,
            )
            return True
        return original(
            self,
            local_order_id,
            recovery_owner=recovery_owner,
            reason=reason,
            recovery_retention_mode=recovery_retention_mode,
        )

    retain_recovery_ownership_if_no_watcher._ap_broker_submit_owner_retained = True
    retain_recovery_ownership_if_no_watcher._ap_original = original
    APOrderStateMachine.retain_recovery_ownership_if_no_watcher = retain_recovery_ownership_if_no_watcher


def install_broker_submit_reconciliation_guard() -> None:
    """Install the P0 LIVE submit-intent liveness repair exactly once."""
    _patch_tradier_list_orders()
    _patch_reconciler()
    _patch_entry_callback()
    _patch_recovery_retention()
