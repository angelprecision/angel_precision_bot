from __future__ import annotations

from ap import queue_original as _orig
from ap.failed_dir_admission import (
    failed_dir_block_result,
    is_failed_dir_blocked,
    payload_pattern_id,
)

_ORIGINAL_DISPATCH = _orig._dispatch


def _dispatch(
    job_id: int,
    client_id: str,
    signal_id: str,
    payload: dict,
    *,
    job_last_error: str | None = None,
    job_result: dict | None = None,
    master_control,
    contract_selector,
    order_state_machine,
    entry_watcher,
    position_manager=None,
    exit_eng=None,
    broker=None,
    on_split_brain=None,
):
    payload = payload or {}
    ticker = payload.get("ticker") or payload.get("symbol", "?")
    execution_mode = str(getattr(master_control, "mode", "PAPER")).upper()

    if is_failed_dir_blocked(payload):
        result = failed_dir_block_result(
            payload,
            client_id=client_id,
            execution_mode=execution_mode,
            signal_id=signal_id,
            ticker=ticker,
        )
        _orig.log.warning(
            "[%s] FAILED_DIR_DISABLED client=%s mode=%s signal_id=%s pattern=%s",
            ticker,
            client_id,
            execution_mode,
            signal_id,
            payload_pattern_id(payload),
        )
        _orig._mark_job(
            job_id,
            "REJECTED",
            result=result,
            error="blocked_pattern:FAILED_DIR_DISABLED",
        )
        try:
            _orig._log_rejection_to_db(
                signal_id=signal_id,
                client_id=client_id,
                ticker=ticker,
                side=payload.get("side") or payload.get("direction") or "",
                score=float(payload.get("score") or 0),
                stage="admission",
                reason_code="BLOCKED_PATTERN_FAILED_DIR_DISABLED",
                human_reason="FAILED_DIR disabled pending higher-timeframe confluence validation",
                payload=payload,
            )
        except Exception:
            pass
        try:
            from ap.rejection_feed import post_master_control_block
            post_master_control_block(
                ticker=ticker,
                side=payload.get("side") or payload.get("direction") or "",
                stage="admission",
                reason="BLOCKED_PATTERN_FAILED_DIR_DISABLED",
                score=float(payload.get("score") or 0),
                pattern=payload_pattern_id(payload),
            )
        except Exception:
            pass
        return

    return _ORIGINAL_DISPATCH(
        job_id,
        client_id,
        signal_id,
        payload,
        job_last_error=job_last_error,
        job_result=job_result,
        master_control=master_control,
        contract_selector=contract_selector,
        order_state_machine=order_state_machine,
        entry_watcher=entry_watcher,
        position_manager=position_manager,
        exit_eng=exit_eng,
        broker=broker,
        on_split_brain=on_split_brain,
    )


_orig._dispatch = _dispatch

for _name in dir(_orig):
    if _name.startswith("__"):
        continue
    globals().setdefault(_name, getattr(_orig, _name))

globals()["_dispatch"] = _dispatch
