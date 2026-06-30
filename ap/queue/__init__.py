from __future__ import annotations

import copy as _copy
import importlib.util as _importlib_util
import sys as _sys
import uuid as _uuid
from pathlib import Path as _Path
from typing import Any as _Any, Optional as _Optional

_BASE_PATH = _Path(__file__).resolve().parent.parent / "queue.py"
_BASE_MODULE_NAME = "_ap_queue_base"
_spec = _importlib_util.spec_from_file_location(_BASE_MODULE_NAME, _BASE_PATH)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Unable to load legacy queue module from {_BASE_PATH}")
_base = _importlib_util.module_from_spec(_spec)
_sys.modules[_BASE_MODULE_NAME] = _base
_spec.loader.exec_module(_base)

for _name in dir(_base):
    if _name.startswith("__") and _name not in {"__doc__"}:
        continue
    globals()[_name] = getattr(_base, _name)

_ORIG_DISPATCH = _base._dispatch
_ORIG_MARK_JOB = _base._mark_job

_SIDE_ALIASES = {
    "BUY": "CALL", "LONG": "CALL", "CALLS": "CALL", "BULL": "CALL", "BULLISH": "CALL",
    "SELL": "PUT", "SHORT": "PUT", "PUTS": "PUT", "BEAR": "PUT", "BEARISH": "PUT",
}
_SELECTOR_FAILURE_RESULT_KEYS = (
    "queue_reason_code", "chain_rows", "survivor_count", "top_reject_buckets",
    "tradier_status_code", "retryable", "data_base_url", "selector_stage",
)
_selector_failure_by_job: dict[int, dict] = {}


def _normalize_queue_side(payload: dict | None) -> tuple[str | None, str | None]:
    if not isinstance(payload, dict):
        return None, "invalid_or_missing_side:payload_not_dict"
    raw = payload.get("side") or payload.get("direction")
    side = str(raw or "").upper().strip()
    side = _SIDE_ALIASES.get(side, side)
    if side in {"CALL", "PUT"}:
        return side, None
    return None, f"invalid_or_missing_side:{raw!r}"


def _safe_float(value: _Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _positive_float_or_none(value: _Any) -> float | None:
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    return val if val > 0 else None


def enqueue_signal(sig, client_id: str = "default", idempotency_key: str | None = None) -> bool:
    if isinstance(sig, dict):
        payload = _copy.deepcopy(sig)
    elif hasattr(sig, "model_dump"):
        payload = sig.model_dump()
    elif hasattr(sig, "dict"):
        payload = sig.dict()
    else:
        raise TypeError(f"Unsupported signal type: {type(sig)}")

    if not payload.get("ticker") and payload.get("symbol"):
        payload["ticker"] = payload["symbol"]
    if not payload.get("symbol") and payload.get("ticker"):
        payload["symbol"] = payload["ticker"]
    if payload.get("score") is None:
        payload["score"] = 65.0
    if payload.get("ev_score") is None:
        payload["ev_score"] = payload["score"]

    side, side_error = _normalize_queue_side(payload)
    if side_error:
        payload["side_validation_error"] = side_error
        payload.pop("side", None)
        payload.pop("direction", None)
    else:
        payload["side"] = side
        payload["direction"] = side

    signal_id = payload.get("signal_id") or f"signal_{_uuid.uuid4().hex[:12]}_{_base._now_iso()}"
    if not idempotency_key:
        idempotency_key = f"{client_id}:{signal_id}"

    def _ins():
        with _base._conn()() as c:
            c.execute(
                """
                INSERT INTO trade_queue (
                    client_id, signal_id, created_ts, status, payload, idempotency_key
                )
                VALUES (%s, %s, NOW(), 'NEW', %s, %s)
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                (client_id, signal_id, _base._json_dumps(payload), idempotency_key),
            )
            return getattr(c, "rowcount", None)

    try:
        rowcount = _base._run_with_retry(_ins)
        if rowcount == 0:
            _base.log.debug("Duplicate ignored: %s", signal_id)
            return False
        _base.log.info("Enqueued: %s client=%s", signal_id, client_id)
        return True
    except Exception as e:
        msg = str(e).lower()
        if "unique" in msg or "conflict" in msg:
            _base.log.debug("Duplicate ignored (exception path): %s", signal_id)
            return False
        raise


def _log_signal_to_db(signal_id: str, client_id: str, ticker: str, side: str, score: float,
                      stage: str, reason_code: str, human_reason: str, payload: dict, *,
                      decision_status: str = "rejected", queued_at: str | None = None) -> bool:
    try:
        sbc = _base._get_sb_client()
        if sbc is None:
            return False
        payload_for_row = dict(payload or {})
        normalized_side, side_error = _normalize_queue_side(
            {"side": side or payload_for_row.get("side"), "direction": payload_for_row.get("direction")}
        )
        row_side = normalized_side or "UNKNOWN"
        if side_error:
            payload_for_row["side_validation_error"] = side_error
            payload_for_row.pop("side", None)
            payload_for_row.pop("direction", None)
        row: dict = {
            "signal_id": str(signal_id or _uuid.uuid4()),
            "client_email": str(client_id),
            "system_version": "v2",
            "ticker": str(ticker),
            "side": row_side,
            "score": _safe_float(score or payload_for_row.get("score") or 0),
            "tier": str(payload_for_row.get("tier") or "B"),
            "pattern": str(payload_for_row.get("pattern") or ""),
            "timeframe": str(payload_for_row.get("timeframe") or "1d"),
            "decision_status": str(decision_status or "rejected").strip() or "rejected",
            "context_notes": f"stage={stage} | code={reason_code} | {human_reason}",
            "raw_payload": {
                "stage": stage,
                "reason_code": reason_code,
                "human_reason": human_reason,
                **{k: v for k, v in payload_for_row.items() if k not in ("raw_payload", "signal_payload") and not callable(v)},
            },
        }
        for src_key, dst_key in (
            ("entry_trigger", "entry_trigger"), ("entry_price", "entry_trigger"),
            ("stop_price", "stop_price"), ("stop_underlying", "stop_price"),
            ("target_price", "target_price"), ("underlying_at_signal", "underlying_at_signal"),
            ("underlying_price", "underlying_at_signal"),
        ):
            val = _positive_float_or_none(payload_for_row.get(src_key))
            if val is not None and dst_key not in row:
                row[dst_key] = val
        if queued_at:
            row["queued_at"] = queued_at
        sbc.table("ap_signals").upsert(row, on_conflict="signal_id").execute()
        return True
    except Exception as exc:
        _base.log.debug("_log_signal_to_db failed (non-fatal): %s", exc)
        return False


def _log_rejection_to_db(signal_id: str, client_id: str, ticker: str, side: str, score: float,
                         stage: str, reason_code: str, human_reason: str, payload: dict) -> None:
    _log_signal_to_db(signal_id, client_id, ticker, side, score, stage, reason_code, human_reason, payload,
                      decision_status="rejected")


def _selector_failure_payload(failure: dict | None) -> dict | None:
    if not isinstance(failure, dict):
        return None
    nested = failure.get("selector_failure")
    if isinstance(nested, dict):
        return _copy.deepcopy(nested)
    picked = {key: _copy.deepcopy(failure[key]) for key in _SELECTOR_FAILURE_RESULT_KEYS if key in failure}
    return picked or _copy.deepcopy(failure)


def _mark_job(job_id: int, status: str, *, result: dict | None = None, error: str | None = None):
    enriched = result
    try:
        job_key = int(job_id)
    except Exception:
        job_key = job_id
    try:
        if isinstance(result, dict) and str(result.get("stage") or "") == "contract_selection":
            selector_failure = _selector_failure_payload(_selector_failure_by_job.pop(job_key, None))
            if selector_failure:
                enriched = dict(result)
                enriched["selector_failure"] = selector_failure
                for key in _SELECTOR_FAILURE_RESULT_KEYS:
                    if key in selector_failure and key not in enriched:
                        enriched[key] = selector_failure.get(key)
                if "stage" in selector_failure and "selector_stage" not in enriched:
                    enriched["selector_stage"] = selector_failure.get("stage")
    except Exception:
        enriched = result
    return _ORIG_MARK_JOB(job_id, status, result=enriched, error=error)


class _SelectorFailureProxy:
    def __init__(self, inner: _Any, queue_id: int):
        self._inner = inner
        self._queue_id = queue_id

    def __getattr__(self, name: str) -> _Any:
        return getattr(self._inner, name)

    def select(self, plan):
        selected = self._inner.select(plan)
        if selected is None:
            try:
                get_failure = getattr(self._inner, "get_last_failure", None)
                failure = get_failure() if callable(get_failure) else None
                if isinstance(failure, dict):
                    _selector_failure_by_job[int(self._queue_id)] = _copy.deepcopy(failure)
            except Exception:
                pass
        return selected

    def get_last_failure(self):
        get_failure = getattr(self._inner, "get_last_failure", None)
        return get_failure() if callable(get_failure) else None


def write_breach_last_error(queue_id: _Optional[int], *, reason_code: str, explanation: str = "",
                            attempt: int = 0, client_id: str = "", ticker: str = "",
                            label: str = "BREACH_ENTRY_FAILED") -> None:
    if not queue_id:
        return
    try:
        rc = str(reason_code or "BREACH_ENTRY_FAILED").strip()
        event_label = str(label or "BREACH_ENTRY_FAILED").strip() or "BREACH_ENTRY_FAILED"
        full_label = f"{event_label}:{rc}"
        if attempt > 0:
            full_label = f"{full_label}:attempt_{attempt}"
        detail = str(explanation or "").strip()[:400]
        last_error = f"{full_label} - {detail}" if detail else full_label

        def _write():
            with _base._conn()() as c:
                c.execute(
                    """
                    UPDATE public.trade_queue
                       SET last_error = %s
                     WHERE id = %s
                       AND UPPER(COALESCE(status, '')) NOT IN (
                           'REJECTED', 'ERROR', 'SUBMITTED', 'FILLED',
                           'CANCELED', 'CANCELLED', 'EXPIRED', 'DONE', 'ARCHIVED'
                       )
                    """,
                    (last_error, int(queue_id)),
                )
        _base._run_with_retry(_write)
        _base.log.info("[%s] breach_last_error_written ticker=%s queue_id=%s label=%s reason=%s attempt=%d",
                       client_id or "?", ticker or "?", queue_id, event_label, rc, attempt)
    except Exception as exc:
        _base.log.debug("[%s] write_breach_last_error failed (non-critical): %s", client_id or "?", exc)


def write_deferred_breach_last_error(queue_id: _Optional[int], *, reason_code: str, explanation: str = "",
                                     attempt: int = 0, client_id: str = "", ticker: str = "") -> None:
    return write_breach_last_error(queue_id, reason_code=reason_code, explanation=explanation,
                                   attempt=attempt, client_id=client_id, ticker=ticker,
                                   label="DEFERRED_BREACH_CONTRACT_FAILED")


def _dispatch(job_id: int, client_id: str, signal_id: str, payload: dict, *,
              job_last_error: str | None = None, job_result: dict | None = None,
              master_control, contract_selector, order_state_machine, entry_watcher,
              position_manager=None, exit_eng=None, broker=None, on_split_brain=None):
    ticker = (payload or {}).get("ticker") or (payload or {}).get("symbol", "?")
    execution_mode = str(getattr(master_control, "mode", "PAPER") or "PAPER").upper()
    if execution_mode == "LIVE" and bool(getattr(_base, "ALLOW_IMMEDIATE_EXECUTION", False)):
        _base.log.critical("[%s] LIVE_FATAL_IMMEDIATE_EXECUTION_ENABLED", ticker)
        _mark_job(job_id, "ERROR", error="LIVE_FATAL_IMMEDIATE_EXECUTION_ENABLED")
        return

    side, side_error = _normalize_queue_side(payload)
    if side_error:
        clean_payload = dict(payload or {})
        clean_payload["side_validation_error"] = side_error
        clean_payload.pop("side", None)
        clean_payload.pop("direction", None)
        _base.log.error("[%s] QUEUE_SIDE_VALIDATION_REJECTED signal_id=%s client_id=%s reason=%s",
                        ticker, signal_id, client_id, side_error)
        _mark_job(job_id, "REJECTED",
                  result={"stage": "queue_side_validation", "reason": "INVALID_OR_MISSING_SIDE",
                          "reason_code": "INVALID_OR_MISSING_SIDE", "details": side_error},
                  error="queue_side_validation:INVALID_OR_MISSING_SIDE")
        _log_rejection_to_db(signal_id, client_id, ticker, "", _safe_float(clean_payload.get("score") or 0),
                             "queue_side_validation", "INVALID_OR_MISSING_SIDE", side_error, clean_payload)
        try:
            from ap.rejection_feed import post_master_control_block
            post_master_control_block(ticker=ticker, side="", stage="queue_side_validation",
                                      reason="INVALID_OR_MISSING_SIDE",
                                      score=_safe_float(clean_payload.get("score") or 0),
                                      pattern=clean_payload.get("pattern_id") or clean_payload.get("pattern", ""))
        except Exception:
            pass
        return

    payload_for_base = dict(payload or {})
    payload_for_base["side"] = side
    payload_for_base["direction"] = side
    selector_for_base = _SelectorFailureProxy(contract_selector, job_id) if contract_selector is not None else None
    return _ORIG_DISPATCH(job_id, client_id, signal_id, payload_for_base,
                          job_last_error=job_last_error, job_result=job_result,
                          master_control=master_control, contract_selector=selector_for_base,
                          order_state_machine=order_state_machine, entry_watcher=entry_watcher,
                          position_manager=position_manager, exit_eng=exit_eng,
                          broker=broker, on_split_brain=on_split_brain)


_base.enqueue_signal = enqueue_signal
_base._log_signal_to_db = _log_signal_to_db
_base._log_rejection_to_db = _log_rejection_to_db
_base._mark_job = _mark_job
_base._dispatch = _dispatch
_base.write_breach_last_error = write_breach_last_error
_base.write_deferred_breach_last_error = write_deferred_breach_last_error

globals().update({
    "enqueue_signal": enqueue_signal,
    "_normalize_queue_side": _normalize_queue_side,
    "_log_signal_to_db": _log_signal_to_db,
    "_log_rejection_to_db": _log_rejection_to_db,
    "_mark_job": _mark_job,
    "_dispatch": _dispatch,
    "write_breach_last_error": write_breach_last_error,
    "write_deferred_breach_last_error": write_deferred_breach_last_error,
    "worker_loop": _base.worker_loop,
})

__all__ = [name for name in globals() if not name.startswith("_")]
