from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Iterable, Optional
from zoneinfo import ZoneInfo


log = logging.getLogger("ap.morning_jobs")

ET = ZoneInfo("America/New_York")

DEFAULT_LIVE_CLIENT = "jasoncosby1@gmail.com"
DEFAULT_PAPER_CLIENTS = (
    "jose.vasquez4011@gmail.com",
    "tradefluencehq@gmail.com",
)
DEFAULT_EXECUTION_MODE = "live"

OVERNIGHT_REEVAL_JOB = "overnight_reeval"
MORNING_HANDOFF_PRIMARY_JOB = "morning_handoff_primary"
MORNING_HANDOFF_BACKUP_JOB = "morning_handoff_backup"

OVERNIGHT_REEVAL_ENDPOINT = "/admin/overnight_reeval"
MORNING_HANDOFF_AUDIT_ENDPOINT = "/admin/morning_handoff_audit"

JOB_TARGET_MINUTE = {
    OVERNIGHT_REEVAL_JOB: (9, 18),
    MORNING_HANDOFF_PRIMARY_JOB: (9, 25),
    MORNING_HANDOFF_BACKUP_JOB: (9, 31),
}


def build_overnight_reeval_payload(
    *,
    client_id: str = DEFAULT_LIVE_CLIENT,
    execution_mode: str | None = None,
) -> dict:
    payload = {
        "force": True,
        "clients": [str(client_id).strip()],
        "max_clients": 1,
        "time_budget_seconds": 120,
        "async_background": False,
    }
    mode = str(execution_mode or "").strip().lower()
    if mode:
        payload["execution_mode"] = mode
    return payload


def build_morning_handoff_payload(
    *,
    client_id: str = DEFAULT_LIVE_CLIENT,
    execution_mode: str = DEFAULT_EXECUTION_MODE,
    dry_run: bool = False,
) -> dict:
    return {
        "client_id": str(client_id).strip(),
        "execution_mode": str(execution_mode).strip().lower(),
        "dry_run": bool(dry_run),
    }


def json_body_bytes(payload: dict) -> bytes:
    return json.dumps(payload).encode("utf-8")


def sign_hmac(secret: bytes | str, timestamp: str, body: bytes) -> str:
    if isinstance(secret, str):
        secret = secret.encode("utf-8")
    msg = timestamp.encode("utf-8") + b"." + body
    return hmac.new(secret, msg, hashlib.sha256).hexdigest()


def build_job_window_key(
    job_name: str,
    *,
    client_id: str = DEFAULT_LIVE_CLIENT,
    execution_mode: str = DEFAULT_EXECUTION_MODE,
    now: Optional[datetime] = None,
) -> str:
    now_et = (now or datetime.now(ET)).astimezone(ET)
    return (
        f"{job_name}:"
        f"{str(client_id).strip().lower()}:"
        f"{str(execution_mode).strip().lower()}:"
        f"{now_et.date().isoformat()}"
    )


def build_hmac_headers(
    *,
    secret: bytes | str,
    payload: dict,
    timestamp: Optional[str] = None,
    idempotency_key: Optional[str] = None,
) -> tuple[dict, bytes]:
    body = json_body_bytes(payload)
    ts = str(timestamp or int(time.time()))
    headers = {
        "Content-Type": "application/json",
        "X-AP-Timestamp": ts,
        "X-AP-Signature": sign_hmac(secret, ts, body),
    }
    if idempotency_key:
        headers["Idempotency-Key"] = str(idempotency_key)
    return headers, body


def summarize_response_json(data: object) -> str:
    if isinstance(data, dict):
        keys = ("ok", "armed", "total_armed", "rejected", "total_rejected", "rescued", "errors", "watchers_requeued")
        parts = [f"{k}={data.get(k)!r}" for k in keys if k in data]
        if "results" in data and isinstance(data["results"], dict):
            parts.append(f"results={len(data['results'])}")
        return ", ".join(parts) or json.dumps(data, default=str)[:400]
    return str(data)[:400]


def select_runner_items(
    runners: dict,
    *,
    requested_clients: Optional[Iterable[str]] = None,
    max_clients: Optional[int] = None,
) -> list[tuple[str, object]]:
    items = list((runners or {}).items())
    if requested_clients:
        wanted = [str(x).strip().lower() for x in requested_clients if str(x).strip()]
        by_email = {str(email).strip().lower(): (email, runner) for email, runner in items}
        items = [by_email[email] for email in wanted if email in by_email]
    if max_clients and max_clients > 0:
        items = items[: int(max_clients)]
    return items


def should_run_now(job_name: str, *, now: Optional[datetime] = None, tolerance_minutes: int = 4) -> bool:
    now_et = (now or datetime.now(ET)).astimezone(ET)
    target = JOB_TARGET_MINUTE.get(job_name)
    if not target:
        return True
    target_hour, target_minute = target
    delta = abs((now_et.hour * 60 + now_et.minute) - (target_hour * 60 + target_minute))
    return delta <= tolerance_minutes


def call_admin_endpoint(
    *,
    bot_url: str,
    endpoint: str,
    secret: bytes | str,
    payload: dict,
    job_name: str,
    client_id: str,
    execution_mode: str,
    timeout_seconds: int = 120,
    now: Optional[datetime] = None,
    urlopen=urllib.request.urlopen,
) -> dict:
    idempotency_key = build_job_window_key(
        job_name,
        client_id=client_id,
        execution_mode=execution_mode,
        now=now,
    )
    headers, body = build_hmac_headers(
        secret=secret,
        payload=payload,
        idempotency_key=idempotency_key,
    )
    req = urllib.request.Request(
        f"{str(bot_url).rstrip('/')}{endpoint}",
        data=body,
        headers=headers,
        method="POST",
    )
    started = time.time()
    try:
        with urlopen(req, timeout=timeout_seconds) as resp:
            raw = resp.read().decode("utf-8")
            elapsed = round(time.time() - started, 3)
            data = json.loads(raw) if raw else {}
            log.info(
                "job=%s client=%s execution_mode=%s status_code=%s elapsed_seconds=%.3f success=true summary=%s",
                job_name,
                client_id,
                execution_mode,
                getattr(resp, "status", 200),
                elapsed,
                summarize_response_json(data),
            )
            return {
                "ok": True,
                "status_code": getattr(resp, "status", 200),
                "elapsed_seconds": elapsed,
                "body": data,
                "idempotency_key": idempotency_key,
            }
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        elapsed = round(time.time() - started, 3)
        log.error(
            "job=%s client=%s execution_mode=%s status_code=%s elapsed_seconds=%.3f success=false body=%s",
            job_name,
            client_id,
            execution_mode,
            exc.code,
            elapsed,
            raw[:800],
        )
        return {
            "ok": False,
            "status_code": exc.code,
            "elapsed_seconds": elapsed,
            "body": raw,
            "idempotency_key": idempotency_key,
        }
    except Exception as exc:  # noqa: BLE001
        elapsed = round(time.time() - started, 3)
        log.error(
            "job=%s client=%s execution_mode=%s status_code=NETWORK_ERROR elapsed_seconds=%.3f success=false error=%s",
            job_name,
            client_id,
            execution_mode,
            elapsed,
            exc,
        )
        return {
            "ok": False,
            "status_code": None,
            "elapsed_seconds": elapsed,
            "body": str(exc),
            "idempotency_key": idempotency_key,
        }
