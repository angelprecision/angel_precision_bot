"""Startup rescue for WATCHING rows armed with DEFERRED:* but missing orders.

This module repairs the narrow case where overnight reeval armed a deferred
watcher row (writing last_error like ``armed:contract=DEFERRED:<ticker>``) but
the process died before OSM materialized the ENTRY order row.

The repair path is intentionally narrow:
  - load WATCHING trade_queue rows in an explicit UTC session window
  - require last_error armed:contract=DEFERRED:<ticker>
  - require no matching ENTRY order for the same signal/client/mode
  - reconstruct ApprovedExecutionPlan from payload
  - call OSM create_entry_order(... initial_status='PENDING_TRIGGER' ...)
  - call entry_watcher.watch(... recovery_rearm=True, no_cancel_on_reject=True)

It never submits/cancels through the broker, never inserts directly into
orders, and never touches positions.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import uuid
from datetime import date, datetime, timezone
from typing import Any, Optional

log = logging.getLogger("ap.armed_deferred_rescue")

_MIN_SCORE_FOR_REPAIR = float(os.getenv("ARMED_DEFERRED_RESCUE_MIN_SCORE", "60"))
_ARMED_RE = re.compile(r"armed:contract=deferred:([A-Z.\-]+)$", re.IGNORECASE)


def _parse_payload(payload: Any) -> dict:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str) and payload.strip():
        try:
            loaded = json.loads(payload)
            return loaded if isinstance(loaded, dict) else {}
        except Exception:
            return {}
    return {}


def _coerce_utc_ts(value: str | datetime) -> str:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value or "").strip()
        if not text:
            raise ValueError("empty_timestamp")
        if len(text) == 10:
            dt = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
        else:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def _date_to_utc_start(value: str | date) -> str:
    if isinstance(value, date):
        d = value
    else:
        d = date.fromisoformat(str(value))
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).isoformat()


def _extract_last_error_ticker(last_error: str) -> str:
    m = _ARMED_RE.search(str(last_error or "").strip().upper())
    return str(m.group(1) if m else "")


def _ticker_from_payload(payload: dict) -> str:
    return str(payload.get("ticker") or payload.get("symbol") or "").strip().upper()


def _plan_from_payload(
    payload: dict,
    *,
    signal_id: str,
    client_id: str,
    execution_mode: str,
    queue_id: int,
) -> Optional[Any]:
    try:
        from ap_master_control import ApprovedExecutionPlan
    except ImportError as exc:
        log.error("armed_deferred_rescue: cannot import ApprovedExecutionPlan: %s", exc)
        return None

    trigger = payload.get("trigger") or {}
    ticker = _ticker_from_payload(payload)
    if not ticker:
        return None

    raw_side = payload.get("side") or payload.get("direction") or trigger.get("side") or ""
    side = str(raw_side).strip().upper()
    if side not in {"CALL", "PUT"}:
        return None

    def _f(*vals):
        for val in vals:
            try:
                fv = float(val or 0)
                if fv > 0:
                    return fv
            except (TypeError, ValueError):
                continue
        return None

    trigger_price = _f(trigger.get("entry"), payload.get("trigger_price"), payload.get("entry_trigger"))
    if trigger_price is None:
        return None

    stop_underlying = _f(trigger.get("stop"), payload.get("stop_underlying"), payload.get("stop_price"))
    target_underlying = _f(
        trigger.get("pt1"),
        trigger.get("pt2"),
        trigger.get("pt3"),
        payload.get("target_underlying"),
        payload.get("target_price"),
    )

    score = 0.0
    try:
        score = float(payload.get("score") or 0)
    except (TypeError, ValueError):
        score = 0.0

    contracts = int(os.getenv("MIN_CONTRACTS_PER_POSITION", "2"))
    try:
        raw_contracts = int(payload.get("contracts") or 0)
        if raw_contracts > 0:
            contracts = raw_contracts
    except (TypeError, ValueError):
        pass

    max_position_usd = 0.0
    try:
        max_position_usd = float(payload.get("max_position_usd") or 0)
    except (TypeError, ValueError):
        max_position_usd = 0.0

    metadata = dict(payload.get("metadata") or {})
    metadata.update({
        "contract_deferred": True,
        "deferred_breach_selection": True,
        "overnight": True,
        "queue_id": queue_id,
        "signal_id": signal_id,
        "armed_deferred_rescue": True,
        "rescue_ts": datetime.now(timezone.utc).isoformat(),
    })

    plan = ApprovedExecutionPlan(
        plan_id=str(uuid.uuid4()),
        signal_id=signal_id,
        client_id=client_id,
        ticker=ticker,
        side=side,
        direction=side,
        pattern=str(payload.get("pattern_id") or payload.get("pattern") or payload.get("signal_type") or "").strip(),
        timeframe=str(payload.get("timeframe") or "1d").strip() or "1d",
        contracts=contracts,
        max_position_usd=max_position_usd,
        tier=str(payload.get("tier") or "B").strip() or "B",
        score=score,
        intel_score=score,
        confidence_bucket=str(payload.get("confidence_tag") or "standard_pool"),
        trigger_type="breach",
        trigger_price=trigger_price,
        stop_underlying=stop_underlying,
        target_underlying=target_underlying,
        contract_symbol=f"DEFERRED:{ticker}",
        limit_price=0.01,
        mode=str(execution_mode).upper(),
        paper_sim=str(execution_mode).lower() != "live",
        reasoning="armed_deferred_rescue",
        intel_available=False,
        stage="APPROVED",
        metadata=metadata,
    )

    try:
        plan.prior_day_high = _f(trigger.get("pt1"), payload.get("prior_day_high"))
        plan.prior_day_low = _f(trigger.get("pt3"), payload.get("prior_day_low"))
    except Exception:
        pass
    return plan


def _load_eligible_rows(client_id: str, execution_mode: str, start_ts_utc: str, end_ts_utc: str) -> list[dict]:
    from ap.db import conn, run_with_retry

    def _fn():
        with conn() as c:
            c.execute(
                """
                SELECT
                    tq.id,
                    tq.signal_id,
                    tq.last_error,
                    tq.created_ts,
                    tq.payload
                FROM trade_queue tq
                WHERE tq.client_id = %s
                  AND tq.status = 'WATCHING'
                  AND tq.last_error LIKE 'armed:contract=DEFERRED:%%'
                  AND tq.last_error <> 'after_hours_deferred:awaiting_overnight_reeval'
                  AND tq.created_ts >= %s
                  AND tq.created_ts < %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM orders o
                      WHERE o.signal_id = tq.signal_id
                        AND o.client_id = tq.client_id
                        AND o.kind = 'ENTRY'
                        AND o.execution_mode = %s
                  )
                ORDER BY tq.created_ts ASC
                """,
                (client_id, start_ts_utc, end_ts_utc, str(execution_mode).lower()),
            )
            return [dict(r) for r in (c.fetchall() or [])]

    return run_with_retry(_fn) or []


def _order_already_exists(signal_id: str, client_id: str, execution_mode: str) -> Optional[dict]:
    from ap.db import conn, run_with_retry

    def _fn():
        with conn() as c:
            c.execute(
                """
                SELECT local_order_id, status, contract, created_ts
                FROM orders
                WHERE signal_id = %s
                  AND client_id = %s
                  AND kind = 'ENTRY'
                  AND execution_mode = %s
                LIMIT 1
                """,
                (signal_id, client_id, str(execution_mode).lower()),
            )
            row = c.fetchone()
            return dict(row) if row else None

    return run_with_retry(_fn)


def _write_tq_result(queue_id: int, client_id: str, *, last_error: str, result_json: dict) -> None:
    from ap.db import conn, run_with_retry

    def _fn():
        with conn() as c:
            c.execute(
                """
                UPDATE trade_queue
                SET last_error = %s,
                    result_json = %s
                WHERE id = %s
                  AND client_id = %s
                """,
                (str(last_error or "")[:500], json.dumps(result_json, default=str), queue_id, client_id),
            )

    try:
        run_with_retry(_fn)
    except Exception as exc:
        log.warning("armed_deferred_rescue: result write failed queue_id=%s: %s", queue_id, exc)


def _repair_row(
    row: dict,
    *,
    client_id: str,
    execution_mode: str,
    entry_watcher,
    osm,
    dry_run: bool,
) -> dict:
    queue_id = int(row.get("id") or 0)
    signal_id = str(row.get("signal_id") or "").strip()
    payload = _parse_payload(row.get("payload"))
    ticker = _ticker_from_payload(payload)
    last_error_raw = str(row.get("last_error") or "")

    result: dict[str, Any] = {
        "queue_id": queue_id,
        "signal_id": signal_id,
        "ticker": ticker or "?",
        "action": None,
        "local_order_id": None,
        "watcher_armed": None,
        "skip_reason": None,
        "dry_run": dry_run,
        "last_error_orig": last_error_raw,
        "error": None,
    }

    if not payload:
        result["action"] = "skipped"
        result["skip_reason"] = "empty_payload"
        return result

    if str(last_error_raw) == "after_hours_deferred:awaiting_overnight_reeval":
        result["action"] = "skipped"
        result["skip_reason"] = "awaiting_overnight_reeval"
        return result

    armed_ticker = _extract_last_error_ticker(last_error_raw)
    payload_ticker = ticker
    if not armed_ticker:
        result["action"] = "skipped"
        result["skip_reason"] = "missing_armed_ticker"
        return result
    if armed_ticker != payload_ticker:
        result["action"] = "skipped"
        result["skip_reason"] = "ticker_mismatch"
        result["armed_ticker"] = armed_ticker
        return result

    try:
        score = float(payload.get("score") or 0)
    except (TypeError, ValueError):
        score = 0.0
    if score < _MIN_SCORE_FOR_REPAIR:
        result["action"] = "skipped"
        result["skip_reason"] = f"score_below_rescue_floor:{score}"
        return result

    existing = _order_already_exists(signal_id, client_id, execution_mode)
    if existing:
        result["action"] = "already_repaired"
        result["local_order_id"] = str(existing.get("local_order_id") or "")
        result["skip_reason"] = f"order_exists:{existing.get('status')}"
        return result

    plan = _plan_from_payload(
        payload,
        signal_id=signal_id,
        client_id=client_id,
        execution_mode=execution_mode,
        queue_id=queue_id,
    )
    if plan is None:
        result["action"] = "skipped"
        result["skip_reason"] = "plan_reconstruction_failed"
        return result

    if dry_run:
        result["action"] = "dry_run_would_repair"
        result["plan_preview"] = {
            "ticker": plan.ticker,
            "side": plan.side,
            "contract_symbol": plan.contract_symbol,
            "trigger_price": plan.trigger_price,
            "contracts": plan.contracts,
            "limit_price": plan.limit_price,
            "execution_mode": execution_mode,
        }
        return result

    try:
        local_order_id = osm.create_entry_order(
            plan,
            initial_status="PENDING_TRIGGER",
            execution_mode=str(execution_mode).lower(),
            meta=plan.metadata,
        )
    except Exception as exc:
        result["action"] = "failed"
        result["error"] = f"create_entry_order:{type(exc).__name__}:{exc}"
        _write_tq_result(
            queue_id,
            client_id,
            last_error=f"rescue_failed:create_entry_order:{type(exc).__name__}",
            result_json=result,
        )
        return result

    if not local_order_id:
        result["action"] = "failed"
        result["error"] = "create_entry_order:no_local_order_id_returned"
        _write_tq_result(
            queue_id,
            client_id,
            last_error="rescue_failed:no_local_order_id",
            result_json=result,
        )
        return result

    result["local_order_id"] = str(local_order_id)

    watcher_armed = False
    try:
        watcher_armed = bool(entry_watcher.watch(
            plan,
            str(local_order_id),
            recovery_rearm=True,
            no_cancel_on_reject=True,
        ))
    except Exception as exc:
        result["watcher_armed"] = False
        result["watcher_error"] = f"{type(exc).__name__}:{exc}"
    else:
        result["watcher_armed"] = watcher_armed

    result["action"] = "repaired"
    _write_tq_result(
        queue_id,
        client_id,
        last_error=f"rescue_ok:order={local_order_id}:watcher={result['watcher_armed']}",
        result_json=result,
    )
    return result


def _log_summary(summary: dict) -> None:
    rows = summary.get("rows") or []
    preview = [
        {"queue_id": r.get("queue_id"), "ticker": r.get("ticker"), "action": r.get("action"), "skip_reason": r.get("skip_reason")}
        for r in rows[:20]
    ]
    log.info(
        "armed_deferred_rescue summary client=%s mode=%s dry_run=%s scanned=%s repaired=%s already_repaired=%s skipped=%s failed=%s preview=%s",
        summary.get("client_id"),
        summary.get("execution_mode"),
        summary.get("dry_run"),
        summary.get("scanned"),
        summary.get("repaired"),
        summary.get("already_repaired"),
        summary.get("skipped"),
        summary.get("failed"),
        preview,
    )


def run_armed_deferred_rescue(
    *,
    client_id: str,
    execution_mode: str,
    entry_watcher,
    osm,
    dry_run: bool = True,
    start_ts_utc: str | datetime,
    end_ts_utc: str | datetime,
) -> dict:
    _client = str(client_id or "").strip()
    _mode = str(execution_mode or "").strip().lower()
    summary = {
        "ok": True,
        "dry_run": dry_run,
        "client_id": _client,
        "execution_mode": _mode,
        "start_ts_utc": _coerce_utc_ts(start_ts_utc),
        "end_ts_utc": _coerce_utc_ts(end_ts_utc),
        "scanned": 0,
        "repaired": 0,
        "already_repaired": 0,
        "skipped": 0,
        "failed": 0,
        "rows": [],
        "errors": [],
    }

    if not _client:
        summary["ok"] = False
        summary["errors"] = ["client_id_required"]
        return summary
    if _mode not in {"live", "paper"}:
        summary["ok"] = False
        summary["errors"] = [f"invalid_execution_mode:{_mode!r}"]
        return summary
    if not dry_run:
        if osm is None:
            summary["ok"] = False
            summary["errors"] = ["osm_required"]
            return summary
        if entry_watcher is None:
            summary["ok"] = False
            summary["errors"] = ["entry_watcher_required"]
            return summary

    try:
        rows = _load_eligible_rows(_client, _mode, summary["start_ts_utc"], summary["end_ts_utc"])
    except Exception as exc:
        summary["ok"] = False
        summary["errors"] = [f"row_load_failed:{exc}"]
        return summary

    summary["scanned"] = len(rows)
    for row in rows:
        row_result = _repair_row(
            row,
            client_id=_client,
            execution_mode=_mode,
            entry_watcher=entry_watcher,
            osm=osm,
            dry_run=dry_run,
        )
        action = str(row_result.get("action") or "")
        if action in {"repaired", "dry_run_would_repair"}:
            summary["repaired"] += 1
        elif action == "already_repaired":
            summary["already_repaired"] += 1
        elif action == "skipped":
            summary["skipped"] += 1
        elif action in {"failed", "error"}:
            summary["failed"] += 1
            summary["ok"] = False
        summary["rows"].append(row_result)

    _log_summary(summary)
    return summary


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="ap_armed_deferred_rescue")
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--execution-mode", required=True, choices=("live", "paper"))
    parser.add_argument("--start-date", required=True, help="UTC date, e.g. 2026-07-01")
    parser.add_argument("--end-date", required=True, help="UTC date, e.g. 2026-07-02")
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    if not args.dry_run:
        raise SystemExit("CLI is dry-run only. Use run_armed_deferred_rescue(...) from a live runtime for repairs.")
    summary = run_armed_deferred_rescue(
        client_id=args.client_id,
        execution_mode=args.execution_mode,
        entry_watcher=None,
        osm=None,
        dry_run=True,
        start_ts_utc=_date_to_utc_start(args.start_date),
        end_ts_utc=_date_to_utc_start(args.end_date),
    )
    print(json.dumps(summary, indent=2, default=str))
    return 0 if summary.get("ok", False) else 1


if __name__ == "__main__":
    raise SystemExit(main())
