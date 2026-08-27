"""Fail-closed selector cursor persistence classification for PR #401.

The original order-state-machine method returned ``False`` for every failure:
invalid identity, malformed generation, JSON serialization errors, database
outages, unknown rowcount, and a genuine exact-owner CAS miss. Execution core
therefore mislabeled infrastructure failures as ownership loss. This guard
preserves ``False`` for one condition only: the exact fenced UPDATE matched zero
rows. Every other failure raises ``SelectorRecoveryCursorPersistFailed``.
"""
from __future__ import annotations

import json
import re
from typing import Any, Callable

from ap.logger import get_logger
from ap.selector_retry_policy import SelectorRecoveryCursorPersistFailed

log = get_logger("ap.selector_cursor_persistence_guard")

_PATCHED_ATTR = "_AP_SELECTOR_CURSOR_PERSIST_GUARD_PATCHED"
_ORIGINAL_ATTR = "_AP_SELECTOR_CURSOR_PERSIST_GUARD_ORIGINAL"
_MAX_CURSOR_BYTES = 256_000


def _db_conn():
    # Lazy import keeps package import/startup verification independent of an
    # already-initialized connection pool. The actual write still fails closed.
    from ap.db import conn

    return conn()


def _run_db_write(fn: Callable[[], Any]):
    from ap.db import run_with_retry

    return run_with_retry(fn)


def _positive_generation(value: Any) -> int:
    if isinstance(value, bool) or isinstance(value, float):
        raise SelectorRecoveryCursorPersistFailed(
            "selector recovery cursor generation malformed"
        )
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and re.fullmatch(r"[1-9]\d*", value.strip()):
        parsed = int(value.strip())
    else:
        raise SelectorRecoveryCursorPersistFailed(
            "selector recovery cursor generation malformed"
        )
    if parsed < 1:
        raise SelectorRecoveryCursorPersistFailed(
            "selector recovery cursor generation malformed"
        )
    return parsed


def _guarded_persist_selector_recovery_cursor(
    self,
    local_order_id: str,
    *,
    owner: str,
    generation: int,
    signal_id: str,
    execution_mode: str,
    cursor: dict,
) -> bool:
    local_raw = str(local_order_id or "")
    owner_raw = str(owner or "")
    signal_raw = str(signal_id or "")
    # Capture and validate runner identity before any database access.
    # A blank or whitespace-padded self.client_id must never reach the CAS
    # query where it would cause a zero-row UPDATE that is then misclassified
    # as ordinary ownership loss.  Invalid runner identity raises immediately.
    client_raw = str(getattr(self, "client_id", "") or "")
    local_id = local_raw.strip()
    durable_owner = owner_raw.strip()
    durable_signal = signal_raw.strip()
    durable_client = client_raw.strip()
    durable_mode = str(execution_mode or "").strip().lower()

    if (
        not local_id
        or not durable_owner
        or not durable_signal
        or not durable_client
        or local_raw != local_id
        or owner_raw != durable_owner
        or signal_raw != durable_signal
        or client_raw != durable_client
        or durable_mode not in {"live", "paper"}
        or not isinstance(cursor, dict)
    ):
        raise SelectorRecoveryCursorPersistFailed(
            "selector recovery cursor identity or payload invalid"
        )

    durable_generation = _positive_generation(generation)
    try:
        cursor_json = json.dumps(
            cursor,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise SelectorRecoveryCursorPersistFailed(
            "selector recovery cursor serialization failed"
        ) from exc

    if len(cursor_json.encode("utf-8")) > _MAX_CURSOR_BYTES:
        raise SelectorRecoveryCursorPersistFailed(
            "selector recovery cursor payload exceeds 256000 bytes"
        )

    def _persist():
        with _db_conn() as c:
            cur = c.execute(
                """
                UPDATE orders
                SET meta = COALESCE(meta, '{}'::jsonb)
                           || jsonb_build_object(
                                'selector_recovery_cursor_v1',
                                %s::jsonb
                              ),
                    updated_ts = NOW()
                WHERE local_order_id = %s
                  AND client_id = %s
                  AND signal_id = %s
                  AND LOWER(BTRIM(COALESCE(
                        NULLIF(BTRIM(execution_mode), ''),
                        NULLIF(BTRIM(meta->>'execution_mode'), ''),
                        ''
                      ))) = %s
                  AND LOWER(BTRIM(COALESCE(
                        NULLIF(BTRIM(execution_mode), ''),
                        NULLIF(BTRIM(meta->>'execution_mode'), ''),
                        ''
                      ))) IN ('live', 'paper')
                  AND (
                        NULLIF(BTRIM(execution_mode), '') IS NULL
                     OR NULLIF(BTRIM(meta->>'execution_mode'), '') IS NULL
                     OR LOWER(BTRIM(execution_mode)) = LOWER(BTRIM(meta->>'execution_mode'))
                  )
                  AND UPPER(BTRIM(COALESCE(kind,''))) = 'ENTRY'
                  AND UPPER(BTRIM(COALESCE(status,''))) = 'PENDING_TRIGGER'
                  AND (broker_order_id IS NULL OR broker_order_id = '')
                  AND submitted_ts IS NULL
                  AND COALESCE(meta->>'lifecycle_state','') = 'MATERIALIZING'
                  AND COALESCE(meta->>'materialization_owner','') = %s
                  AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                """,
                (
                    cursor_json,
                    local_id,
                    durable_client,
                    durable_signal,
                    durable_mode,
                    durable_owner,
                    durable_generation,
                ),
            )
            return getattr(cur, "rowcount", getattr(c, "rowcount", None))

    try:
        rowcount = _run_db_write(_persist)
    except Exception as exc:
        log.critical(
            "[%s] SELECTOR_RECOVERY_CURSOR_PERSIST_FAILED order=%s "
            "generation=%s error=%s",
            durable_client,
            local_id,
            durable_generation,
            exc,
        )
        raise SelectorRecoveryCursorPersistFailed(
            "selector recovery cursor database write failed"
        ) from exc

    if rowcount is None or rowcount == -1:
        raise SelectorRecoveryCursorPersistFailed(
            "selector recovery cursor rowcount unconfirmed"
        )
    if isinstance(rowcount, bool):
        raise SelectorRecoveryCursorPersistFailed(
            "selector recovery cursor rowcount malformed"
        )
    try:
        normalized_rowcount = int(rowcount)
    except (TypeError, ValueError) as exc:
        raise SelectorRecoveryCursorPersistFailed(
            "selector recovery cursor rowcount malformed"
        ) from exc

    if normalized_rowcount == 0:
        # The only False outcome: exact owner/generation/identity CAS missed.
        return False
    if normalized_rowcount != 1:
        raise SelectorRecoveryCursorPersistFailed(
            f"selector recovery cursor unexpected rowcount={normalized_rowcount}"
        )
    return True


def install_selector_cursor_persistence_guard() -> None:
    """Install the classified persistence implementation exactly once."""
    from ap.order_state_machine import APOrderStateMachine

    if getattr(APOrderStateMachine, _PATCHED_ATTR, False):
        return
    original = getattr(APOrderStateMachine, "persist_selector_recovery_cursor")
    setattr(APOrderStateMachine, _ORIGINAL_ATTR, original)
    APOrderStateMachine.persist_selector_recovery_cursor = (
        _guarded_persist_selector_recovery_cursor
    )
    setattr(APOrderStateMachine, _PATCHED_ATTR, True)
