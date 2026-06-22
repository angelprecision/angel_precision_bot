from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable


_ACTIVE_ENTRY_STATUSES = frozenset({
    "CREATED",
    "PENDING_TRIGGER",
    "SUBMITTED",
    "ACKNOWLEDGED",
    "PARTIAL_FILL",
})

_WATCHER_OWNED_ENTRY_STATUSES = frozenset({
    "PENDING_TRIGGER",
})

_SKIP_ERROR_PREFIXES = (
    "contract_selection:NO_CHAIN_DATA",
    "contract_selection:OI_TOO_LOW",
    "archived_contract_selection_failed_direct_queue",
)

_SKIP_ERROR_SUBSTRINGS = (
    "stop_queue_path_use_overnight_reeval",
)


@dataclass(frozen=True)
class RescueDecision:
    queue_id: int
    client_id: str
    signal_id: str
    action: str
    reason: str
    prior_status: str
    prior_last_error: str
    ticker: str = ""
    side: str = ""
    order_local_id: str = ""
    order_status: str = ""


def _parse_created_ts(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _string(value) -> str:
    return str(value or "").strip()


def _is_skip_error(last_error: str) -> bool:
    return any(last_error.startswith(prefix) for prefix in _SKIP_ERROR_PREFIXES) or any(
        part in last_error for part in _SKIP_ERROR_SUBSTRINGS
    )


def _is_watcher_owned_order(order: dict) -> bool:
    status = _string(order.get("status")).upper()
    if status not in _WATCHER_OWNED_ENTRY_STATUSES:
        return False
    if _string(order.get("broker_order_id")):
        return False
    if order.get("submitted_ts") is not None:
        return False
    if order.get("filled_ts") is not None:
        return False
    return True


def _active_entry_orders(order_rows: Iterable[dict], *, client_id: str, signal_id: str) -> list[dict]:
    out: list[dict] = []
    for row in order_rows:
        if _string(row.get("client_id")) != client_id:
            continue
        if _string(row.get("signal_id")) != signal_id:
            continue
        if _string(row.get("kind")).upper() != "ENTRY":
            continue
        if _string(row.get("status")).upper() not in _ACTIVE_ENTRY_STATUSES:
            continue
        out.append(row)
    return out


def build_paper_restart_guard_rescue_plan(
    *,
    queue_rows: Iterable[dict],
    order_rows: Iterable[dict],
    active_paper_clients: set[str],
    now_utc: datetime | None = None,
    lookback_hours: int = 48,
) -> list[RescueDecision]:
    now_utc = now_utc or datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=int(lookback_hours))
    decisions: list[RescueDecision] = []
    action_seen: set[tuple[str, str]] = set()

    rows_sorted = sorted(
        list(queue_rows),
        key=lambda row: (
            _parse_created_ts(row.get("created_ts")) or datetime.min.replace(tzinfo=timezone.utc),
            int(row.get("id") or 0),
        ),
        reverse=True,
    )

    for row in rows_sorted:
        queue_id = int(row.get("id") or 0)
        client_id = _string(row.get("client_id"))
        signal_id = _string(row.get("signal_id"))
        status = _string(row.get("status")).upper()
        last_error = _string(row.get("last_error"))
        created_ts = _parse_created_ts(row.get("created_ts"))
        ticker = _string(row.get("ticker") or row.get("symbol"))
        side = _string(row.get("side")).upper()
        key = (client_id, signal_id)

        if client_id not in active_paper_clients:
            decisions.append(RescueDecision(queue_id, client_id, signal_id, "SKIP", "inactive_or_non_paper_client", status, last_error, ticker, side))
            continue
        if status not in {"REJECTED", "ARCHIVED"}:
            decisions.append(RescueDecision(queue_id, client_id, signal_id, "SKIP", "status_not_eligible", status, last_error, ticker, side))
            continue
        if not last_error.startswith("restart_guard:overnight_skip"):
            decisions.append(RescueDecision(queue_id, client_id, signal_id, "SKIP", "last_error_not_restart_guard", status, last_error, ticker, side))
            continue
        if created_ts is None or created_ts < cutoff:
            decisions.append(RescueDecision(queue_id, client_id, signal_id, "SKIP", "outside_lookback", status, last_error, ticker, side))
            continue
        if _is_skip_error(last_error):
            decisions.append(RescueDecision(queue_id, client_id, signal_id, "SKIP", "duplicate_or_contract_selection_skip", status, last_error, ticker, side))
            continue
        if key in action_seen:
            decisions.append(RescueDecision(queue_id, client_id, signal_id, "SKIP", "duplicate_signal_row", status, last_error, ticker, side))
            continue

        matching_orders = _active_entry_orders(order_rows, client_id=client_id, signal_id=signal_id)
        if not matching_orders:
            action_seen.add(key)
            decisions.append(
                RescueDecision(
                    queue_id, client_id, signal_id, "NEW",
                    "restart_guard_archived_no_order_row",
                    status, last_error, ticker, side,
                )
            )
            continue

        watcher_owned = next((row for row in matching_orders if _is_watcher_owned_order(row)), None)
        if watcher_owned is not None:
            action_seen.add(key)
            decisions.append(
                RescueDecision(
                    queue_id, client_id, signal_id, "WATCHING",
                    "restart_guard_archived_watcher_owned_order",
                    status, last_error, ticker, side,
                    order_local_id=_string(watcher_owned.get("local_order_id")),
                    order_status=_string(watcher_owned.get("status")).upper(),
                )
            )
            continue

        first = matching_orders[0]
        decisions.append(
            RescueDecision(
                queue_id, client_id, signal_id, "SKIP",
                "active_entry_order_exists_non_watcher_owned",
                status, last_error, ticker, side,
                order_local_id=_string(first.get("local_order_id")),
                order_status=_string(first.get("status")).upper(),
            )
        )

    return decisions
