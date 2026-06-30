from __future__ import annotations

import inspect
import json
import logging
import re
import types
from typing import Any, Optional

from ap.db import conn, run_with_retry

log = logging.getLogger("ap.order_monitor_safety_guard")

_INSTALLED = False
_ORIGINALS: dict[str, Any] = {}


class _QuoteRoutingBroker:
    """Route market-data reads to data_broker while preserving execution broker actions.

    OrderMonitor has legacy paths that call self.broker.get_quote() inside the
    same helpers that may later cancel/replace broker orders. This proxy keeps
    cancel/place/get_order on the execution broker, but routes get_quote() to
    the explicit data broker when one exists.
    """

    def __init__(self, execution_broker: Any, data_broker: Any):
        self._execution_broker = execution_broker
        self._data_broker = data_broker

    def get_quote(self, *args, **kwargs):
        broker = self._data_broker or self._execution_broker
        method = getattr(broker, "get_quote", None)
        if callable(method):
            return method(*args, **kwargs)
        return None

    def __getattr__(self, name: str):
        return getattr(self._execution_broker, name)


def _parse_meta(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _normalize_direction(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"CALL", "PUT"}:
        return raw
    if raw in {"BUY", "LONG", "CALLS", "BULLISH"}:
        return "CALL"
    if raw in {"SELL", "SHORT", "PUTS", "BEARISH"}:
        return "PUT"
    return ""


def _direction_from_contract(contract: Any) -> str:
    symbol = str(contract or "").strip().upper()
    if not symbol:
        return ""
    match = re.search(r"\d{6}([CP])\d{5,8}", symbol) or re.search(r"\d([CP])\d", symbol)
    if not match:
        return ""
    return "CALL" if match.group(1) == "C" else "PUT"


def _find_runner_data_broker(client_id: str | None = None):
    """Best-effort recovery for current ClientRunner construction shape.

    client_runner resolves a dedicated data_broker before constructing
    APOrderMonitor, but the current constructor call does not pass it. While the
    direct call site is being reviewed, recover the already-resolved object from
    the caller frame or active runner so quote reads do not silently fall back to
    the sandbox execution broker in PAPER.
    """
    try:
        frame = inspect.currentframe()
        if frame is None:
            return None
        frame = frame.f_back
        while frame is not None:
            loc = frame.f_locals
            candidate = loc.get("data_broker") or loc.get("databroker")
            if candidate is not None:
                return candidate
            runner = loc.get("self")
            if runner is not None:
                runner_email = getattr(runner, "email", None) or getattr(runner, "client_id", None)
                if not client_id or not runner_email or str(runner_email) == str(client_id):
                    candidate = getattr(runner, "data_broker", None) or getattr(runner, "databroker", None)
                    if candidate is not None:
                        return candidate
            frame = frame.f_back
    except Exception:
        return None
    finally:
        try:
            del frame
        except Exception:
            pass

    try:
        import client_runner as _client_runner
        runners = getattr(_client_runner, "_active_runners", {}) or {}
        runner = runners.get(client_id) if client_id else None
        if runner is not None:
            return getattr(runner, "data_broker", None) or getattr(runner, "databroker", None)
    except Exception:
        return None
    return None


def _quote_broker_for(monitor: Any):
    broker = getattr(monitor, "broker", None)
    return (
        getattr(monitor, "data_broker", None)
        or getattr(broker, "data_broker", None)
        or broker
    )


def _with_quote_routing(monitor: Any, fn, *args, **kwargs):
    execution_broker = getattr(monitor, "broker", None)
    quote_broker = _quote_broker_for(monitor)
    if not execution_broker or not quote_broker or quote_broker is execution_broker:
        return fn(monitor, *args, **kwargs)

    proxy = _QuoteRoutingBroker(execution_broker, quote_broker)
    monitor.broker = proxy
    try:
        return fn(monitor, *args, **kwargs)
    finally:
        monitor.broker = execution_broker


def _guarded_init(self, *args, data_broker=None, **kwargs):
    _ORIGINALS["__init__"](self, *args, **kwargs)
    broker = getattr(self, "broker", None)
    recovered_data_broker = (
        data_broker
        or getattr(broker, "data_broker", None)
        or _find_runner_data_broker(getattr(self, "client_id", None))
    )
    self.data_broker = recovered_data_broker
    if broker is not None and recovered_data_broker is not None and getattr(broker, "data_broker", None) is None:
        try:
            setattr(broker, "data_broker", recovered_data_broker)
        except Exception:
            pass


def _guarded_advance_from_broker_status(self, local_order_id: str, broker_status, contract: str):
    s = self._normalize_broker_status(broker_status)
    if s in {"filled", "partially_filled"}:
        order = self.osm.get_order(local_order_id) or {}
        kind = str(order.get("kind") or "").upper()
        fill_status = "EXIT_FILLED" if kind == "EXIT" and s == "filled" else (
            "EXIT_PARTIAL_FILL" if kind == "EXIT" else (
                "FILLED" if s == "filled" else "PARTIAL_FILL"
            )
        )
        log.warning(
            "[%s] BROKER_FILL_SEEN_DEFER_TO_FILL_MONITOR | local=%s contract=%s "
            "kind=%s broker_status=%s blocked_status=%s",
            getattr(self, "client_id", "?"),
            local_order_id,
            contract,
            kind or "ENTRY",
            s,
            fill_status,
        )
        try:
            self._emit_order_event(
                local_order_id=local_order_id,
                stage="order_monitor",
                decision="ALERT",
                reason_code="BROKER_FILL_SEEN_DEFER_TO_FILL_MONITOR",
                explanation=(
                    "Broker reported a fill/partial fill, but order_monitor only has "
                    "status-level data. Fill monitor owns fill finalization because it "
                    "hydrates filled_qty/avg_fill and creates/syncs positions."
                ),
                contract=contract,
                position_id=(order or {}).get("position_id"),
                inputs={
                    "broker_status": s,
                    "kind": kind or "ENTRY",
                    "blocked_status": fill_status,
                    "requires_fill_monitor": True,
                },
            )
        except Exception:
            pass
        return None

    return _ORIGINALS["_advance_from_broker_status"](
        self, local_order_id, broker_status, contract
    )


def _guarded_build_lost_handoff_plan_from_order(self, order: dict):
    try:
        meta = _parse_meta(order.get("meta"))

        ticker = str(
            order.get("symbol")
            or meta.get("symbol")
            or meta.get("ticker")
            or ""
        ).upper()
        if not ticker:
            return None

        trigger = (
            order.get("trigger_price")
            if order.get("trigger_price") is not None
            else meta.get("signal_entry_price")
        )
        try:
            trigger = float(trigger) if trigger is not None else 0.0
        except (TypeError, ValueError):
            trigger = 0.0
        if trigger <= 0:
            return None

        contract = (
            order.get("contract")
            or meta.get("selected_contract")
            or meta.get("contract_symbol")
            or ""
        )
        direction = (
            _normalize_direction(order.get("direction"))
            or _normalize_direction(meta.get("direction"))
            or _normalize_direction(meta.get("side"))
            or _direction_from_contract(contract)
        )
        if direction not in {"CALL", "PUT"}:
            local_order_id = order.get("local_order_id") or order.get("id") or ""
            log.warning(
                "[%s] LOST_HANDOFF_REARM_BLOCKED_INVALID_SIDE | local=%s contract=%s "
                "raw_order_direction=%r raw_meta_direction=%r raw_meta_side=%r",
                getattr(self, "client_id", "?"),
                local_order_id,
                contract,
                order.get("direction"),
                meta.get("direction"),
                meta.get("side"),
            )
            try:
                self._emit_order_event(
                    local_order_id=local_order_id,
                    stage="order_monitor",
                    decision="BLOCK",
                    reason_code="LOST_HANDOFF_REARM_FAILED_INVALID_OR_MISSING_SIDE",
                    explanation=(
                        "Lost-handoff watcher rearm blocked because direction/side was "
                        "missing or invalid and could not be safely derived from OCC C/P."
                    ),
                    contract=str(contract or ""),
                    inputs={
                        "order_direction": order.get("direction"),
                        "meta_direction": meta.get("direction"),
                        "meta_side": meta.get("side"),
                        "contract": contract,
                    },
                )
            except Exception:
                pass
            return None

        stop = (
            order.get("stop_underlying")
            if order.get("stop_underlying") is not None
            else meta.get("stop_underlying")
        )
        target = (
            order.get("target_underlying")
            if order.get("target_underlying") is not None
            else meta.get("target_underlying")
        )

        plan = types.SimpleNamespace(
            signal_id=str(
                order.get("signal_id")
                or meta.get("signal_id")
                or order.get("local_order_id")
                or ""
            ),
            plan_id=str(order.get("plan_id") or meta.get("plan_id") or ""),
            ticker=ticker,
            symbol=ticker,
            direction=direction,
            side=direction,
            trigger_price=trigger,
            stop_underlying=stop,
            target_underlying=target,
            contract_symbol=str(contract or ""),
            contracts=int(order.get("qty") or meta.get("contracts") or 0) or 1,
            tier=str(order.get("tier") or meta.get("tier") or "B"),
            score=float(order.get("score") or meta.get("score") or 0) or 0.0,
            timeframe=str(meta.get("timeframe") or "1d"),
            pattern=str(meta.get("pattern") or ""),
            limit_price=float(order.get("limit_price") or 0) or None,
            metadata=dict(meta) if isinstance(meta, dict) else {},
        )
        if str(plan.contract_symbol).upper().startswith("DEFERRED:"):
            plan.metadata["contract_deferred"] = True
        return plan
    except Exception as exc:
        log.warning(
            "[%s] LOST_HANDOFF_PLAN_REBUILD_FAILED | local=%s error=%s",
            getattr(self, "client_id", "?"),
            order.get("local_order_id"),
            exc,
        )
        return None


def _guarded_get_active_entry_orders(self) -> list[dict]:
    def _fn():
        with conn() as c:
            c.execute(
                """
                SELECT local_order_id, broker_order_id, status, symbol,
                       contract, position_id, signal_id, plan_id,
                       created_ts, submitted_ts,
                       limit_price,
                       limit_price AS price,
                       fill_price,
                       qty, direction, execution_mode, reserved_cost,
                       stop_underlying, target_underlying,
                       score, tier, trigger_price, meta
                FROM orders
                WHERE client_id=%s
                  AND kind='ENTRY'
                  AND status IN ('CREATED','PENDING_TRIGGER','SUBMITTED','ACKNOWLEDGED','PARTIAL_FILL')
                ORDER BY created_ts ASC
                """,
                (self.client_id,),
            )
            return c.fetchall()

    try:
        return run_with_retry(_fn)
    except Exception as e:
        log.error(f"[{self.client_id}] Failed to fetch entry orders: {e}")
        return []


def _guarded_get_option_price(self, symbol: str) -> Optional[float]:
    quote_broker = _quote_broker_for(self)
    if not symbol or not quote_broker:
        return None
    try:
        if hasattr(quote_broker, "get_quote"):
            q = quote_broker.get_quote(symbol)
            if isinstance(q, dict):
                bid = float(q.get("bid") or 0)
                ask = float(q.get("ask") or 0)
                if bid > 0 and ask > 0:
                    return (bid + ask) / 2

        if hasattr(quote_broker, "session") and hasattr(quote_broker, "cfg"):
            cfg = quote_broker.cfg
            base = getattr(cfg, "base_url", None)
            if not base or "sandbox" in str(base).lower():
                log.warning(
                    "[%s] ORDER_MONITOR_MARKET_DATA_BASE_BLOCKED | symbol=%s base_url=%s",
                    getattr(self, "client_id", "?"),
                    symbol,
                    base or "missing",
                )
                return None
            token = getattr(cfg, "access_token", None) or getattr(cfg, "token", "")
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            resp = quote_broker.session.get(
                f"{base}/v1/markets/quotes",
                params={"symbols": symbol, "greeks": "false"},
                headers=headers,
                timeout=(3.05, 5),
            )
            if resp.status_code == 200:
                q = resp.json().get("quotes", {}).get("quote", {})
                if isinstance(q, dict):
                    bid = float(q.get("bid") or 0)
                    ask = float(q.get("ask") or 0)
                    if bid > 0 and ask > 0:
                        return (bid + ask) / 2
    except Exception as e:
        log.debug(
            "[%s] _get_option_price(%s) failed via quote broker: %s",
            getattr(self, "client_id", "?"),
            symbol,
            e,
        )
    return None


def _guarded_try_repeg(self, *args, **kwargs):
    return _with_quote_routing(self, _ORIGINALS["_try_repeg"], *args, **kwargs)


def _guarded_paper_entry_retry_and_fallback(self, *args, **kwargs):
    return _with_quote_routing(
        self, _ORIGINALS["_paper_entry_retry_and_fallback"], *args, **kwargs
    )


def _guarded_maybe_arm_post_cancel_retry(self, *args, **kwargs):
    return _with_quote_routing(
        self, _ORIGINALS["_maybe_arm_post_cancel_retry"], *args, **kwargs
    )


def install_order_monitor_safety_guard() -> None:
    """Install active OrderMonitor hardening guards.

    This is intentionally active-on-import through ap.__init__, matching the
    existing guard pattern in this repo. It does not submit/cancel broker orders;
    it only blocks status-only fill finalization, makes lost-handoff side
    fail-closed, hydrates entry context, and routes quote reads through an
    explicit data broker when one is configured.
    """
    global _INSTALLED
    if _INSTALLED:
        return

    from ap.order_monitor import APOrderMonitor

    _ORIGINALS.setdefault("__init__", APOrderMonitor.__init__)
    _ORIGINALS.setdefault(
        "_advance_from_broker_status", APOrderMonitor._advance_from_broker_status
    )
    _ORIGINALS.setdefault(
        "_build_lost_handoff_plan_from_order",
        APOrderMonitor._build_lost_handoff_plan_from_order,
    )
    _ORIGINALS.setdefault("_get_active_entry_orders", APOrderMonitor._get_active_entry_orders)
    _ORIGINALS.setdefault("_get_option_price", APOrderMonitor._get_option_price)
    _ORIGINALS.setdefault("_try_repeg", APOrderMonitor._try_repeg)
    _ORIGINALS.setdefault(
        "_paper_entry_retry_and_fallback", APOrderMonitor._paper_entry_retry_and_fallback
    )
    _ORIGINALS.setdefault(
        "_maybe_arm_post_cancel_retry", APOrderMonitor._maybe_arm_post_cancel_retry
    )

    APOrderMonitor.__init__ = _guarded_init
    APOrderMonitor._advance_from_broker_status = _guarded_advance_from_broker_status
    APOrderMonitor._build_lost_handoff_plan_from_order = (
        _guarded_build_lost_handoff_plan_from_order
    )
    APOrderMonitor._get_active_entry_orders = _guarded_get_active_entry_orders
    APOrderMonitor._get_option_price = _guarded_get_option_price
    APOrderMonitor._try_repeg = _guarded_try_repeg
    APOrderMonitor._paper_entry_retry_and_fallback = _guarded_paper_entry_retry_and_fallback
    APOrderMonitor._maybe_arm_post_cancel_retry = _guarded_maybe_arm_post_cancel_retry

    _INSTALLED = True
    log.info("ORDER_MONITOR_SAFETY_GUARD_INSTALLED")
