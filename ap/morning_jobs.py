from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
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

OVERNIGHT_REEVAL_BATCH_JOB = "overnight_reeval_batch"
MORNING_HANDOFF_PRIMARY_JOB = "morning_handoff_primary"
MORNING_HANDOFF_BACKUP_JOB = "morning_handoff_backup"
MORNING_RECOVERY_JOB = "morning_recovery"

OVERNIGHT_REEVAL_ENDPOINT = "/admin/overnight_reeval"
MORNING_HANDOFF_AUDIT_ENDPOINT = "/admin/morning_handoff_audit"
RELEASE_AFTER_HOURS_DEFERRED_ENDPOINT = "/admin/release_after_hours_deferred"
PAPER_RESTART_GUARD_ENDPOINT = "/admin/paper_rescue_restart_guard"

# The authoritative overnight pass begins only once the regular session is open.
# Premarket can still leave WATCHING inventory staged, but morning automation no
# longer assumes Tradier has the current-session data required to finish the pass.
JOB_TARGET_MINUTE = {
    OVERNIGHT_REEVAL_BATCH_JOB: (9, 30),
    MORNING_HANDOFF_PRIMARY_JOB: (9, 30),  # compatibility/manual alias only
    # The evaluator has a 120-second HTTP work budget. Keep the reconciliation
    # watchdog outside that budget so it cannot race a still-running 09:30 pass.
    MORNING_HANDOFF_BACKUP_JOB: (9, 34),
    MORNING_RECOVERY_JOB: (9, 36),
}


@dataclass(frozen=True)
class MorningJobCall:
    job_name: str
    endpoint: str
    payload: dict
    client_scope: str
    execution_mode: str


def _coerce_et_datetime(now: Optional[datetime] = None) -> datetime:
    if now is None:
        return datetime.now(ET)
    if now.tzinfo is None:
        return now.replace(tzinfo=ET)
    return now.astimezone(ET)


def _normalize_clients(clients: Optional[Iterable[str]]) -> list[str]:
    return [str(client).strip() for client in clients or [] if str(client).strip()]


def build_overnight_reeval_payload(
    *,
    clients: Iterable[str],
    force: bool = True,
    max_clients: Optional[int] = None,
    time_budget_seconds: int = 120,
    async_background: bool = False,
) -> dict:
    client_list = _normalize_clients(clients)
    return {
        "force": bool(force),
        "clients": client_list,
        "max_clients": int(max_clients or len(client_list) or 1),
        "time_budget_seconds": int(time_budget_seconds),
        "async_background": bool(async_background),
    }


def build_morning_handoff_payload(
    *,
    clients: Optional[Iterable[str]] = None,
    mode: Optional[str] = None,
    client_id: Optional[str] = None,
    execution_mode: Optional[str] = None,
    dry_run: bool = False,
    triggered_by: Optional[str] = None,
    run_lock_scope: Optional[str] = None,
) -> dict:
    payload = {"dry_run": bool(dry_run)}
    client_list = _normalize_clients(clients)
    if client_list:
        payload["clients"] = client_list
    if client_id and str(client_id).strip():
        payload["client_id"] = str(client_id).strip()
    normalized_mode = str(mode or execution_mode or DEFAULT_EXECUTION_MODE).strip().lower()
    if normalized_mode:
        payload["mode"] = normalized_mode
    if execution_mode and str(execution_mode).strip():
        payload["execution_mode"] = str(execution_mode).strip().lower()
    if triggered_by and str(triggered_by).strip():
        payload["triggered_by"] = str(triggered_by).strip()
    if run_lock_scope and str(run_lock_scope).strip():
        payload["run_lock_scope"] = str(run_lock_scope).strip()
    return payload


def build_release_after_hours_deferred_payload(
    *,
    clients: Iterable[str],
    force: bool = True,
    lookback_h: int = 36,
) -> dict:
    return {
        "force": bool(force),
        "clients": _normalize_clients(clients),
        "lookback_h": int(lookback_h),
    }


def build_paper_rescue_restart_guard_payload(
    *,
    clients: Iterable[str],
    lookback_h: int = 36,
    dry_run: bool = False,
) -> dict:
    return {
        "clients": _normalize_clients(clients),
        "lookback_h": int(lookback_h),
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
    client_scope: str,
    execution_mode: str,
    now: Optional[datetime] = None,
) -> str:
    now_et = _coerce_et_datetime(now)
    return (
        f"{job_name}:"
        f"{str(client_scope).strip().lower()}:"
        f"{str(execution_mode).strip().lower()}:"
        f"{now_et.date().isoformat()}"
    )


def build_hmac_headers(
    *,
    secret: bytes | str,
    payload: dict,
    timestamp: Optional[str] = None,
) -> tuple[dict, bytes]:
    body = json_body_bytes(payload)
    ts = str(timestamp or int(time.time()))
    headers = {
        "Content-Type": "application/json",
        "X-AP-Timestamp": ts,
        "X-AP-Signature": sign_hmac(secret, ts, body),
    }
    return headers, body


def summarize_response_json(data: object) -> str:
    if isinstance(data, dict):
        keys = (
            "ok",
            "armed",
            "total_armed",
            "rejected",
            "total_rejected",
            "watchers_requeued",
            "rescued",
            "released",
            "clients_audited",
            "errors",
        )
        parts = [f"{key}={data.get(key)!r}" for key in keys if key in data]
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
        wanted = [str(item).strip().lower() for item in requested_clients if str(item).strip()]
        by_email = {str(email).strip().lower(): (email, runner) for email, runner in items}
        items = [by_email[email] for email in wanted if email in by_email]
    if max_clients and max_clients > 0:
        items = items[: int(max_clients)]
    return items


def should_run_now(
    job_name: str,
    *,
    now: Optional[datetime] = None,
    tolerance_minutes: int = 45,
) -> bool:
    now_et = _coerce_et_datetime(now)
    target = JOB_TARGET_MINUTE.get(job_name)
    if not target:
        return True
    target_hour, target_minute = target
    delta = abs((now_et.hour * 60 + now_et.minute) - (target_hour * 60 + target_minute))
    return delta <= tolerance_minutes


def build_job_calls(
    job_name: str,
    *,
    live_client: str = DEFAULT_LIVE_CLIENT,
    paper_clients: Optional[Iterable[str]] = None,
) -> list[MorningJobCall]:
    live_client = str(live_client).strip()
    paper_client_list = _normalize_clients(paper_clients or DEFAULT_PAPER_CLIENTS)
    if job_name == OVERNIGHT_REEVAL_BATCH_JOB:
        return [
            MorningJobCall(
                job_name="overnight_reeval_live",
                endpoint=OVERNIGHT_REEVAL_ENDPOINT,
                payload=build_overnight_reeval_payload(clients=[live_client]),
                client_scope=live_client,
                execution_mode="live",
            ),
            MorningJobCall(
                job_name="overnight_reeval_paper",
                endpoint=OVERNIGHT_REEVAL_ENDPOINT,
                payload=build_overnight_reeval_payload(clients=paper_client_list),
                client_scope=",".join(paper_client_list),
                execution_mode="paper",
            ),
        ]
    if job_name in (MORNING_HANDOFF_PRIMARY_JOB, MORNING_HANDOFF_BACKUP_JOB):
        return [
            MorningJobCall(
                job_name=f"{job_name}_live",
                endpoint=MORNING_HANDOFF_AUDIT_ENDPOINT,
                payload=build_morning_handoff_payload(
                    clients=[live_client],
                    mode="live",
                    dry_run=False,
                    triggered_by="scheduler",
                    run_lock_scope=f"handoff_live:{live_client}",
                ),
                client_scope=live_client,
                execution_mode="live",
            ),
            MorningJobCall(
                job_name=f"{job_name}_paper",
                endpoint=MORNING_HANDOFF_AUDIT_ENDPOINT,
                payload=build_morning_handoff_payload(
                    clients=paper_client_list,
                    mode="paper",
                    dry_run=False,
                    triggered_by="scheduler",
                    run_lock_scope="handoff_paper:" + ",".join(sorted(paper_client_list)),
                ),
                client_scope=",".join(paper_client_list),
                execution_mode="paper",
            ),
        ]
    if job_name == MORNING_RECOVERY_JOB:
        # LIVE recovery must reuse the canonical overnight evaluator. A successful
        # evaluator attempt already invokes the post-overnight handoff/readiness
        # path, so this recovers both fresh-data evaluation and watcher ownership
        # without introducing a second LIVE release authority.
        calls: list[MorningJobCall] = []
        if live_client:
            calls.append(
                MorningJobCall(
                    job_name="morning_recovery_live_overnight_reeval",
                    endpoint=OVERNIGHT_REEVAL_ENDPOINT,
                    payload=build_overnight_reeval_payload(clients=[live_client]),
                    client_scope=live_client,
                    execution_mode="live",
                )
            )

        # Preserve the existing paper recovery behavior. These calls remain
        # paper-only and can never survive a LIVE execution-mode filter.
        if paper_client_list:
            calls.extend(
                [
                    MorningJobCall(
                        job_name="release_after_hours_deferred_paper",
                        endpoint=RELEASE_AFTER_HOURS_DEFERRED_ENDPOINT,
                        payload=build_release_after_hours_deferred_payload(
                            clients=paper_client_list,
                            force=True,
                            lookback_h=36,
                        ),
                        client_scope=",".join(paper_client_list),
                        execution_mode="paper",
                    ),
                    MorningJobCall(
                        job_name="paper_rescue_restart_guard",
                        endpoint=PAPER_RESTART_GUARD_ENDPOINT,
                        payload=build_paper_rescue_restart_guard_payload(
                            clients=paper_client_list,
                            lookback_h=36,
                            dry_run=False,
                        ),
                        client_scope=",".join(paper_client_list),
                        execution_mode="paper",
                    ),
                ]
            )
        return calls
    raise ValueError(f"unsupported MORNING_JOB={job_name!r}")


def call_admin_endpoint(
    *,
    bot_url: str,
    endpoint: str,
    secret: bytes | str,
    payload: dict,
    job_name: str,
    client_scope: str,
    execution_mode: str,
    timeout_seconds: int = 120,
    now: Optional[datetime] = None,
    urlopen=urllib.request.urlopen,
) -> dict:
    job_window_key = build_job_window_key(
        job_name,
        client_scope=client_scope,
        execution_mode=execution_mode,
        now=now,
    )
    headers, body = build_hmac_headers(secret=secret, payload=payload)
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
                "job=%s job_window_key=%s client_scope=%s execution_mode=%s status_code=%s elapsed_seconds=%.3f success=true summary=%s",
                job_name,
                job_window_key,
                client_scope,
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
                "job_window_key": job_window_key,
            }
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        elapsed = round(time.time() - started, 3)
        log.error(
            "job=%s job_window_key=%s client_scope=%s execution_mode=%s status_code=%s elapsed_seconds=%.3f success=false body=%s",
            job_name,
            job_window_key,
            client_scope,
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
            "job_window_key": job_window_key,
        }
    except Exception as exc:  # noqa: BLE001
        elapsed = round(time.time() - started, 3)
        log.error(
            "job=%s job_window_key=%s client_scope=%s execution_mode=%s status_code=NETWORK_ERROR elapsed_seconds=%.3f success=false error=%s",
            job_name,
            job_window_key,
            client_scope,
            execution_mode,
            elapsed,
            exc,
        )
        return {
            "ok": False,
            "status_code": None,
            "elapsed_seconds": elapsed,
            "body": str(exc),
            "job_window_key": job_window_key,
        }
