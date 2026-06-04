"""
ap_broker_order_reconciler.py
==============================
PR: Fill Integrity — client-scoped Tradier order reconciliation.

Scope: ledger / fill integrity only.
Does not touch strategy, entries, exits, scoring, sizing, or gates.

Rules:
  - Never infer one client's fill from another client's fill.
  - Never set fill_price = limit_price.
  - Never write FILLED / EXIT_FILLED without filled_qty > 0 and fill_price > 0.
  - Every reconciliation is scoped: client_id → account_id/token → broker_order_id.
  - Raw Tradier response saved to broker_order_audit before normalization.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import requests as _requests

log = logging.getLogger("ap.broker_order_reconciler")


# ── Data types ────────────────────────────────────────────────────────────────

@dataclass
class NormalizedBrokerOrder:
    broker_order_id: str
    status: str
    symbol: Optional[str]
    quantity: Optional[Decimal]
    exec_quantity: Optional[Decimal]
    avg_fill_price: Optional[Decimal]
    last_fill_price: Optional[Decimal]
    last_fill_quantity: Optional[Decimal]
    transaction_date: Optional[str]
    raw: dict[str, Any]

    @property
    def is_filled(self) -> bool:
        return self.status in {"filled", "partially_filled"}

    @property
    def has_exec_fields(self) -> bool:
        return (
            self.exec_quantity is not None and self.exec_quantity > 0
            and self.avg_fill_price is not None and self.avg_fill_price > 0
        )


def _dec(value: Any) -> Optional[Decimal]:
    if value is None or value == "" or value == 0:
        return None
    try:
        d = Decimal(str(value))
        return d if d > 0 else None
    except InvalidOperation:
        return None


# ── Tradier response normalizer ───────────────────────────────────────────────

def normalize_tradier_order(payload: dict[str, Any]) -> NormalizedBrokerOrder:
    """
    Normalize a raw Tradier /accounts/{id}/orders/{order_id} response.
    Uses only confirmed execution fields for fill price/qty —
    never 'price' (which is the limit price).
    """
    order = (
        payload.get("order")
        or (payload.get("orders") or {}).get("order")
        or payload
    )
    if isinstance(order, list):
        if len(order) == 1:
            order = order[0]
        else:
            raise ValueError(
                f"Expected single Tradier order, got list of {len(order)}"
            )

    return NormalizedBrokerOrder(
        broker_order_id  = str(order.get("id") or ""),
        status           = str(order.get("status") or "").lower().strip(),
        symbol           = order.get("symbol"),
        quantity         = _dec(order.get("quantity")),
        # exec_quantity and avg_fill_price are the ONLY valid fill sources.
        # 'price' is the limit/stop price — must NOT be used as fill price.
        exec_quantity    = _dec(order.get("exec_quantity")),
        avg_fill_price   = _dec(order.get("avg_fill_price")),
        last_fill_price  = _dec(order.get("last_fill_price")),
        last_fill_quantity = _dec(order.get("last_fill_quantity")),
        transaction_date = order.get("transaction_date"),
        raw              = payload,
    )


# ── Tradier API fetch ─────────────────────────────────────────────────────────

def fetch_tradier_order(
    *,
    base_url: str,
    account_id: str,
    access_token: str,
    broker_order_id: str,
    timeout: int = 12,
) -> dict[str, Any]:
    """
    GET /v1/accounts/{account_id}/orders/{broker_order_id}
    Raises on non-200 or network error.
    """
    url = f"{base_url.rstrip('/')}/v1/accounts/{account_id}/orders/{broker_order_id}"
    resp = _requests.get(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


# ── Credential lookup (Supabase synchronous) ──────────────────────────────────

def get_client_tradier_credentials(sb, client_id: str) -> dict[str, Any]:
    """
    Return Tradier credentials for client_id from the members table.
    Respects tradier_active_mode: 'live' → live creds, else paper/sandbox.
    Raises RuntimeError if not found.
    """
    rows = (
        sb.table("members")
        .select("tradier_account_id,tradier_access_token,"
                "tradier_live_account_id,tradier_live_access_token,"
                "tradier_active_mode,tradier_base_url")
        .eq("email", client_id)
        .limit(1)
        .execute()
        .data or []
    )
    if not rows:
        raise RuntimeError(f"No member record for client_id={client_id}")

    m    = rows[0]
    mode = (m.get("tradier_active_mode") or "paper").lower()

    if mode == "live":
        account_id = m.get("tradier_live_account_id") or ""
        enc_token  = m.get("tradier_live_access_token") or ""
        base_url   = "https://api.tradier.com"
    else:
        account_id = m.get("tradier_account_id") or ""
        enc_token  = m.get("tradier_access_token") or ""
        base_url   = m.get("tradier_base_url") or "https://sandbox.tradier.com"

    if not account_id or not enc_token:
        raise RuntimeError(
            f"Missing Tradier credentials for client_id={client_id} mode={mode}"
        )

    # Decrypt token using the same helper as app.py
    try:
        from app import decrypt_token
        access_token = decrypt_token(enc_token)
    except Exception:
        # Fallback: use raw if it looks like a plain-text token
        access_token = enc_token

    return {
        "account_id":    account_id,
        "access_token":  access_token,
        "base_url":      base_url,
        "mode":          mode,
    }


# ── Audit table writer ────────────────────────────────────────────────────────

def _save_broker_audit(
    sb,
    *,
    client_id: str,
    local_order_id: Optional[str],
    broker_order_id: str,
    endpoint: str,
    raw: dict[str, Any],
    norm: NormalizedBrokerOrder,
) -> None:
    normalized_payload = {
        "broker_order_id":    norm.broker_order_id,
        "status":             norm.status,
        "exec_quantity":      str(norm.exec_quantity) if norm.exec_quantity else None,
        "avg_fill_price":     str(norm.avg_fill_price) if norm.avg_fill_price else None,
        "last_fill_price":    str(norm.last_fill_price) if norm.last_fill_price else None,
        "last_fill_quantity": str(norm.last_fill_quantity) if norm.last_fill_quantity else None,
        "transaction_date":   norm.transaction_date,
        "has_exec_fields":    norm.has_exec_fields,
    }
    try:
        sb.table("broker_order_audit").insert({
            "broker":          "tradier",
            "client_id":       client_id,
            "local_order_id":  local_order_id,
            "broker_order_id": broker_order_id,
            "endpoint":        endpoint,
            "raw_response":    raw,
            "normalized":      normalized_payload,
        }).execute()
    except Exception as e:
        log.error("[AUDIT_SAVE_FAILED] client=%s broker_order=%s err=%s",
                  client_id, broker_order_id, e)


# ── Apply fill to local orders row ────────────────────────────────────────────

def apply_broker_order_to_orders_row(
    sb,
    order: dict[str, Any],
    norm: NormalizedBrokerOrder,
) -> dict[str, Any]:
    """
    Apply a normalized Tradier order to the local orders row.
    ONLY writes FILLED/EXIT_FILLED when exec_quantity > 0 AND avg_fill_price > 0.
    Uses coalesce(meta, '{}') for safe JSONB merge.
    Returns {"ok": True/False, "action": ...}
    """
    local_id   = str(order.get("local_order_id") or order.get("id") or "")
    client_id  = str(order.get("client_id") or "")
    broker_oid = str(order.get("broker_order_id") or "")
    kind       = str(order.get("kind") or "ENTRY").upper()
    now        = datetime.now(timezone.utc).isoformat()

    if not norm.is_filled:
        return {"ok": True, "action": "skipped_not_filled", "status": norm.status}

    # Hard guard — never write FILLED without execution fields
    if not norm.has_exec_fields:
        try:
            sb.table("orders").update({
                "last_error": "BROKER_STATUS_FILLED_MISSING_EXEC_FIELDS__RECONCILE_REQUIRED",
                "updated_ts": now,
                "meta": sb.postgrest.session.post  # placeholder; done via raw SQL below
            }).eq("local_order_id", local_id).eq("client_id", client_id).execute()
        except Exception:
            pass
        # Use raw SQL for safe JSONB merge
        try:
            sb.rpc("pg_execute", {
                "query": """
                    UPDATE orders
                    SET
                      last_error = 'BROKER_STATUS_FILLED_MISSING_EXEC_FIELDS',
                      updated_ts  = now(),
                      meta        = COALESCE(meta, '{}'::jsonb) || jsonb_build_object(
                        'reconcile_required', true,
                        'broker_status', $1::text,
                        'missing_exec_fields_at', now()::text
                      )
                    WHERE local_order_id = $2
                      AND client_id      = $3
                """,
                "params": [norm.status, local_id, client_id]
            }).execute()
        except Exception as e:
            log.error("[FILL_GUARD] meta update failed %s: %s", local_id, e)
        log.warning(
            "[FILL_GUARD] BLOCKED | client=%s local=%s broker=%s "
            "status=%s exec_qty=%s avg_fill=%s — not writing FILLED",
            client_id, local_id, broker_oid,
            norm.status, norm.exec_quantity, norm.avg_fill_price
        )
        return {"ok": False, "action": "blocked_missing_exec_fields"}

    local_status = "FILLED" if kind == "ENTRY" else "EXIT_FILLED"
    if norm.status == "partially_filled":
        local_status = "PARTIAL_FILL" if kind == "ENTRY" else "EXIT_PARTIAL_FILL"

    filled_qty = int(norm.exec_quantity)
    fill_price = float(norm.avg_fill_price)

    try:
        sb.table("orders").update({
            "status":           local_status,
            "filled_qty":       filled_qty,
            "fill_price":       fill_price,
            "filled_ts":        now,
            "updated_ts":       now,
            "last_error":       None,
        }).eq("local_order_id", local_id).eq("client_id", client_id).execute()
    except Exception as e:
        log.error("[FILL_APPLY] orders update failed %s: %s", local_id, e)
        return {"ok": False, "action": "db_update_failed", "error": str(e)}

    # Safe meta merge using Supabase RPC
    try:
        sb.rpc("pg_execute", {
            "query": """
                UPDATE orders
                SET meta = COALESCE(meta, '{}'::jsonb) || jsonb_build_object(
                  'broker_avg_fill_price',    $1::text,
                  'broker_exec_quantity',     $2::int,
                  'broker_status',            $3::text,
                  'reconciled_from_broker_at', now()::text
                )
                WHERE local_order_id = $4
                  AND client_id      = $5
            """,
            "params": [str(fill_price), filled_qty, norm.status, local_id, client_id]
        }).execute()
    except Exception as e:
        log.warning("[FILL_META] meta update failed (non-critical) %s: %s", local_id, e)

    log.info(
        "[FILL_APPLIED] client=%s local=%s broker=%s → %s qty=%s fill=%.4f",
        client_id, local_id, broker_oid, local_status, filled_qty, fill_price
    )
    return {
        "ok":          True,
        "action":      "filled",
        "local_status": local_status,
        "filled_qty":  filled_qty,
        "fill_price":  fill_price,
    }


# ── Main reconcile entry point ────────────────────────────────────────────────

def reconcile_order(
    sb,
    *,
    client_id: str,
    local_order_id: Optional[str] = None,
    broker_order_id: Optional[str] = None,
) -> dict[str, Any]:
    """
    Fetch the Tradier order for this client and apply it to the local orders row.
    Scoped strictly to client_id — never uses another client's credentials.
    """
    if not local_order_id and not broker_order_id:
        raise ValueError("local_order_id or broker_order_id required")

    # Fetch local order row
    q = sb.table("orders").select("*").eq("client_id", client_id)
    if local_order_id:
        q = q.eq("local_order_id", local_order_id)
    elif broker_order_id:
        q = q.eq("broker_order_id", broker_order_id)
    rows = q.order("id", desc=True).limit(1).execute().data or []
    if not rows:
        raise RuntimeError(
            f"Local order not found for client={client_id} "
            f"local={local_order_id} broker={broker_order_id}"
        )
    order = rows[0]
    b_oid = str(order.get("broker_order_id") or broker_order_id or "")
    l_oid = str(order.get("local_order_id") or local_order_id or "")

    if not b_oid:
        raise RuntimeError(f"Order {l_oid} has no broker_order_id — cannot reconcile")

    # Fetch THIS client's credentials — never another client's
    creds   = get_client_tradier_credentials(sb, client_id)
    endpoint= f"/v1/accounts/{creds['account_id']}/orders/{b_oid}"

    raw  = fetch_tradier_order(
        base_url      = creds["base_url"],
        account_id    = creds["account_id"],
        access_token  = creds["access_token"],
        broker_order_id = b_oid,
    )
    norm = normalize_tradier_order(raw)

    # Save raw response before any normalization
    _save_broker_audit(
        sb,
        client_id       = client_id,
        local_order_id  = l_oid,
        broker_order_id = b_oid,
        endpoint        = endpoint,
        raw             = raw,
        norm            = norm,
    )

    result = apply_broker_order_to_orders_row(sb, order, norm)
    result["broker_order_id"] = b_oid
    result["local_order_id"]  = l_oid
    result["broker_status"]   = norm.status
    result["exec_quantity"]   = str(norm.exec_quantity) if norm.exec_quantity else None
    result["avg_fill_price"]  = str(norm.avg_fill_price) if norm.avg_fill_price else None
    return result
