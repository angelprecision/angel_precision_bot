from __future__ import annotations

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

try:
    from ap.observability import (
        emit_decision_event,
        get_git_commit,
        make_config_hash,
        new_run_id,
    )
except Exception:
    emit_decision_event = None

    def new_run_id(prefix: str = "ap") -> str:
        return f"{prefix}-unknown"

    def make_config_hash(config: dict) -> str:
        return "unknown"

    def get_git_commit(default: str = "unknown") -> str:
        return default


log = logging.getLogger("ap.master_control")

# QUARTERLY REVIEW REQUIRED: These estimates are used for capital gate projections
# before real contract pricing is known. If ATM premiums diverge significantly
# (e.g. NVDA drops from 9.50 to 4.00 in low-vol regime), the capital gate will
# over-block valid signals. Update before each live trading quarter.
_PREMIUM_ESTIMATES: dict[str, float] = {
    "NVDA": 9.50,
    "TSLA": 7.00,
    "META": 6.00,
    "NFLX": 8.00,
    "AMD": 4.50,
    "MSFT": 5.00,
    "AAPL": 3.00,
    "SPY": 2.00,
    "QQQ": 3.50,
    "IWM": 1.50,
    "DIA": 2.50,
    "COIN": 5.00,
    "PLTR": 1.50,
    "MSTR": 18.00,
    "AMZN": 4.50,
    "GOOG": 4.00,
    "GOOGL": 4.00,
    "GS": 4.00,
    "ORCL": 3.00,
    "WFC": 2.50,
    "MS": 3.50,
}
_DEFAULT_PREMIUM_FALLBACK = 3.50

_PRIORITY_TICKERS = {"SPY", "QQQ", "IWM", "SPX", "NDX", "DIA"}
_PRIORITY_FLOOR = 40.0
_INDEX_TO_ETF = {"^GSPC": "SPY", "^NDX": "QQQ", "^RUT": "IWM", "^DJI": "DIA"}

# Canonical active ENTRY order states that reserve capital / represent pending exposure.
# Keep this aligned with APOrderStateMachine.PENDING_ENTRY_STATUSES and
# APPositionManager._PENDING_ENTRY_STATUSES. Master Control prefers
# position_manager.snapshot()["pending_entry_capital"], but this fallback must
# use the same canonical lifecycle so fallback risk math cannot drift.
try:
    from ap.order_state_machine import PENDING_ENTRY_STATUSES as _OSM_PENDING_ENTRY_STATUSES
except Exception:
    _OSM_PENDING_ENTRY_STATUSES = None

_ENTRY_CAPITAL_RESERVED_STATUSES = tuple(
    str(s).upper().strip()
    for s in (_OSM_PENDING_ENTRY_STATUSES or (
        "CREATED",
        "PENDING_TRIGGER",
        "SUBMITTED",
        "ACKNOWLEDGED",
        "PARTIAL_FILL",
    ))
)


def _estimate_premium(ticker: str) -> float:
    return _PREMIUM_ESTIMATES.get(str(ticker).upper().strip(), _DEFAULT_PREMIUM_FALLBACK)


DEFAULT_PREMIUM_ESTIMATE = _DEFAULT_PREMIUM_FALLBACK


@dataclass
class ApprovedExecutionPlan:
    plan_id: str
    signal_id: str
    client_id: str
    ticker: str
    side: str
    direction: str
    pattern: str
    timeframe: str
    contracts: int
    max_position_usd: float
    tier: str
    score: float
    intel_score: float
    confidence_bucket: str
    trigger_type: str
    trigger_price: Optional[float]
    stop_underlying: Optional[float]
    target_underlying: Optional[float]
    contract_symbol: Optional[str] = None
    limit_price: Optional[float] = None
    mode: str = "paper"
    paper_sim: bool = True
    reasoning: str = ""
    intel_available: bool = False
    stage: str = "APPROVED"
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_signal_dict(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "ticker": self.ticker,
            "symbol": self.ticker,
            "side": self.side,
            "direction": self.direction,
            "pattern": self.pattern,
            "pattern_id": self.pattern,
            "timeframe": self.timeframe,
            "score": self.score,
            "ev_score": self.score,
            "tier": self.tier,
            "confidence_tag": self.confidence_bucket,
            "trigger": {
                "entry": self.trigger_price,
                "stop": self.stop_underlying,
                "pt1": self.target_underlying,
            },
            "_approved_plan": self,
        }


@dataclass
class ControlDecision:
    ok: bool
    stage: str
    reason: str = ""
    reason_code: str = ""
    plan: Optional[ApprovedExecutionPlan] = None
    signal_id: str = ""
    ticker: str = ""
    client_id: str = "default"


class APMasterControl:
    """Single decision authority for whether a signal may become a trade."""

    SECTOR_MAP: dict[str, str] = {
        "AAPL": "tech",
        "MSFT": "tech",
        "NVDA": "tech",
        "AMD": "tech",
        "GOOGL": "tech",
        "META": "tech",
        "CRM": "tech",
        "ORCL": "tech",
        "TSLA": "tech",
        "AMZN": "tech",
        "NFLX": "tech",
        "SNOW": "tech",
        "JPM": "financials",
        "BAC": "financials",
        "GS": "financials",
        "MS": "financials",
        "C": "financials",
        "WFC": "financials",
        "UNH": "healthcare",
        "JNJ": "healthcare",
        "PFE": "healthcare",
        "ABBV": "healthcare",
        "MRK": "healthcare",
        "LLY": "healthcare",
        "WMT": "consumer",
        "COST": "consumer",
        "TGT": "consumer",
        "LOW": "consumer",
        "HD": "consumer",
        "NKE": "consumer",
        "XOM": "energy",
        "CVX": "energy",
        "SLB": "energy",
        "CAT": "industrials",
        "DE": "industrials",
        "BA": "industrials",
        "CMCSA": "telecom",
        "VZ": "telecom",
        "T": "telecom",
        "DIS": "media",
    }

    def __init__(
        self,
        *,
        mode: str = "paper",
        score_floor: float = 60.0,
        context_floor: float = 6.0,
        max_positions: int = 7,
        max_capital_pct: float = 0.40,
        max_sector_pct: float = 0.25,
        max_ticker_pct: float = 0.10,
        max_calls: int = 5,
        max_puts: int = 5,
        max_trades_today: int = 10,
        max_daily_loss: float = -500.0,
        account_equity: float = 25000.0,
        position_manager=None,
        position_sizer=None,
        supabase_client=None,
        signal_store=None,
        tier_engine=None,
        feedback_loop=None,
        client_id: str = "default",
        max_snapshot_age_sec: float = 15.0,
        require_snapshot_freshness_live: bool = True,
        pending_capital_fail_closed_live: bool = True,
    ):
        self.mode = mode.upper()
        self.paper = self.mode != "LIVE"
        self.score_floor = score_floor
        self.context_floor = context_floor
        self.max_positions = max_positions
        self.max_capital_pct = max_capital_pct
        self.max_sector_pct = max_sector_pct
        self.max_ticker_pct = max_ticker_pct
        self.max_calls = max_calls
        self.max_puts = max_puts
        self.max_trades_today = max_trades_today
        self.max_daily_loss = max_daily_loss
        self.account_equity = account_equity
        self._startup_equity = float(account_equity)  # used to scale max_daily_loss proportionally
        self.pm = position_manager
        self._client_id = client_id
        self.sizer = position_sizer
        self.sb = supabase_client
        self.store = signal_store
        self.tier_eng = tier_engine
        self.feedback = feedback_loop
        self.max_snapshot_age_sec = float(max_snapshot_age_sec)
        self.require_snapshot_freshness_live = bool(require_snapshot_freshness_live)
        self.pending_capital_fail_closed_live = bool(pending_capital_fail_closed_live)

        # Optional callback wired by runner/ops layer. Signature is flexible:
        # alert_fn(event=str, severity=str, client_id=str, ticker=str, signal_id=str, details=dict)
        self._equity_cache_ts: float = 0.0   # initialized here; set by _dispatch() after broker call
        self._alert_fn = None
        self._degraded_counts: dict[str, int] = {}

        self.run_id = os.getenv("AP_RUN_ID", new_run_id("ap"))
        self.strategy_version = os.getenv("AP_STRATEGY_VERSION", "ap_live_beta")
        self.git_commit = get_git_commit()
        self.config_hash = make_config_hash(
            {
                "mode": self.mode,
                "score_floor": self.score_floor,
                "context_floor": self.context_floor,
                "max_positions": self.max_positions,
                "max_capital_pct": self.max_capital_pct,
                "max_sector_pct": self.max_sector_pct,
                "max_ticker_pct": self.max_ticker_pct,
                "max_calls": self.max_calls,
                "max_puts": self.max_puts,
                "max_trades_today": self.max_trades_today,
                "max_daily_loss": self.max_daily_loss,
                "account_equity": self.account_equity,
                "max_snapshot_age_sec": self.max_snapshot_age_sec,
                "require_snapshot_freshness_live": self.require_snapshot_freshness_live,
                "pending_capital_fail_closed_live": self.pending_capital_fail_closed_live,
                "entry_capital_reserved_statuses": sorted(_ENTRY_CAPITAL_RESERVED_STATUSES),
            }
        )

        self._kill_switch_fn = None
        self._mode_fn = None
        self._seen_signals: dict[str, float] = {}  # key -> inserted_ts, expires after 1800s
        self._trade_cooldowns: dict[str, float] = {}
        self._seed_dedup_from_db(client_id=getattr(self, "_client_id", "default"))
        log.info(
            "APMasterControl initialized | mode=%s | score_floor=%s | ctx_floor=%s | max_pos=%s | max_cap=%.0f%% | max_sector=%.0f%% | max_ticker=%.0f%% | max_calls=%s | max_puts=%s | max_trades_today=%s | max_daily_loss=%s",
            self.mode,
            self.score_floor,
            self.context_floor,
            self.max_positions,
            self.max_capital_pct * 100,
            self.max_sector_pct * 100,
            self.max_ticker_pct * 100,
            self.max_calls,
            self.max_puts,
            self.max_trades_today,
            self.max_daily_loss,
        )

    def wire(self, *, kill_switch_fn=None, mode_fn=None, position_count_fn=None, alert_fn=None, **kwargs):
        if kill_switch_fn:
            self._kill_switch_fn = kill_switch_fn
        if mode_fn:
            self._mode_fn = mode_fn
        if alert_fn:
            self._alert_fn = alert_fn

    def _current_mode(self) -> str:
        try:
            return (self._mode_fn() if self._mode_fn else self.mode).upper()
        except Exception:
            return self.mode.upper()

    def _is_live_mode(self) -> bool:
        return self._current_mode() == "LIVE"


    def _parse_snapshot_ts(self, value: Any) -> Optional[datetime]:
        """Parse common snapshot timestamp formats into an aware UTC datetime."""
        if value is None:
            return None
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, (int, float)):
            try:
                dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
            except Exception:
                return None
        elif isinstance(value, str):
            raw = value.strip()
            if not raw:
                return None
            try:
                if raw.endswith("Z"):
                    raw = raw[:-1] + "+00:00"
                dt = datetime.fromisoformat(raw)
            except Exception:
                try:
                    dt = datetime.fromtimestamp(float(raw), tz=timezone.utc)
                except Exception:
                    return None
        else:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _validate_snapshot_freshness(
        self,
        snap: dict[str, Any],
        *,
        client_id: str,
        ticker: str = "",
        signal_id: str = "",
    ) -> dict[str, Any]:
        """
        In LIVE, a successful snapshot must also be recent enough to trust.
        A stale successful snapshot is treated as unavailable and callers block.
        """
        if not self._is_live_mode() or not self.require_snapshot_freshness_live:
            snap.setdefault("_snapshot_ok", True)
            snap.setdefault("_snapshot_error", "")
            return snap

        ts_value = (
            snap.get("snapshot_ts")
            or snap.get("snapshot_at")
            or snap.get("as_of")
            or snap.get("updated_at")
            or snap.get("ts")
            or snap.get("timestamp")
            or snap.get("generated_at")
        )
        ts = self._parse_snapshot_ts(ts_value)
        if ts is None:
            err = "snapshot_missing_freshness_timestamp"
            self._alert_degraded(
                "SNAPSHOT_STALE_OR_UNVERIFIED",
                severity="CRITICAL",
                client_id=client_id,
                ticker=ticker,
                signal_id=signal_id,
                details={"error": err, "available_keys": sorted(map(str, snap.keys()))},
            )
            snap["_snapshot_ok"] = False
            snap["_snapshot_error"] = err
            return snap

        age = (datetime.now(timezone.utc) - ts).total_seconds()
        snap["_snapshot_ts"] = ts.isoformat()
        snap["_snapshot_age_sec"] = age

        if age < -5 or age > self.max_snapshot_age_sec:
            err = f"snapshot_stale_or_future_dated age_sec={age:.2f} max={self.max_snapshot_age_sec:.2f}"
            self._alert_degraded(
                "SNAPSHOT_STALE_OR_UNVERIFIED",
                severity="CRITICAL",
                client_id=client_id,
                ticker=ticker,
                signal_id=signal_id,
                details={"error": err, "snapshot_ts": ts.isoformat(), "age_sec": age},
            )
            snap["_snapshot_ok"] = False
            snap["_snapshot_error"] = err
            return snap

        snap["_snapshot_ok"] = True
        snap.setdefault("_snapshot_error", "")
        return snap

    def _alert_degraded(
        self,
        event: str,
        *,
        severity: str = "WARNING",
        client_id: str = "default",
        ticker: str = "",
        signal_id: str = "",
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Best-effort degraded-subsystem alert hook. Never blocks trading by itself.
        Safety decisions still happen in the caller, e.g. snapshot failure blocks LIVE.
        """
        details = details or {}
        key = f"{event}:{client_id}:{ticker or '-'}"
        self._degraded_counts[key] = self._degraded_counts.get(key, 0) + 1
        payload = {
            "event": event,
            "severity": severity,
            "client_id": client_id,
            "ticker": ticker,
            "signal_id": signal_id,
            "count": self._degraded_counts[key],
            "run_id": self.run_id,
            "strategy_version": self.strategy_version,
            "config_hash": self.config_hash,
            "git_commit": self.git_commit,
            "details": details,
        }

        log_fn = log.critical if severity.upper() in {"CRITICAL", "HIGH"} else log.warning
        log_fn("[%s] DEGRADED | %s | ticker=%s signal=%s count=%s details=%s",
               client_id, event, ticker or "-", signal_id or "-", payload["count"], details)

        try:
            if self._alert_fn:
                self._alert_fn(
                    event=event,
                    severity=severity,
                    client_id=client_id,
                    ticker=ticker,
                    signal_id=signal_id,
                    details=payload,
                )
        except Exception as e:
            log.debug("alert_fn failed (non-critical): %s", e)

        try:
            from ap.db import conn, run_with_retry

            def _insert():
                with conn() as c:
                    c.execute(
                        """
                        INSERT INTO audit_log (client_id, level, event, payload, ts)
                        VALUES (%s, %s, %s, %s, NOW())
                        """,
                        (client_id, severity.upper(), event, json.dumps(payload)),
                    )

            run_with_retry(_insert)
        except Exception as e:
            log.debug("degraded alert audit insert failed (non-critical): %s", e)

    def set_account_equity(self, equity: float, client_id: str = ""):
        old = self.account_equity
        self.account_equity = float(equity)
        # Recompute max_daily_loss proportionally to new equity.
        # max_capital/sector/ticker pct gates recompute inline — no action needed.
        # max_daily_loss is a fixed dollar set at startup; scale it with equity.
        if self._startup_equity and self._startup_equity > 0:
            loss_pct = abs(self.max_daily_loss / self._startup_equity)
            self.max_daily_loss = -abs(self.account_equity * loss_pct)
        label = f"[{client_id}] " if client_id else ""
        if abs(old - self.account_equity) > 1:
            log.info(
                "%sAccount equity updated: $%.0f -> $%.0f | max_capital=$%.0f max_sector=$%.0f max_daily_loss=$%.0f",
                label,
                old,
                self.account_equity,
                self.account_equity * self.max_capital_pct,
                self.account_equity * self.max_sector_pct,
                self.max_daily_loss,
            )

    @staticmethod
    def _position_price_for_exposure(pos: dict) -> float:
        """Best available option premium per share for an active position.

        Keep this aligned with APPositionManager._active_position_capital_sql():
        avg_fill first, then entry/import/reconstruction price fields. This
        prevents sector/ticker exposure gates from undercounting partially healed
        live rows that do not yet have avg_fill populated.
        """
        for key in (
            "avg_fill",
            "entry_price",
            "entry_option_price",
            "premium_per_share",
            "limit_price",
        ):
            try:
                value = pos.get(key)
                if value is not None and float(value) > 0:
                    return float(value)
            except Exception:
                continue
        return 0.0

    @staticmethod
    def _position_qty_for_exposure(pos: dict) -> int:
        """Best available active/remaining contract quantity for exposure.

        quantity_remaining is preferred when available so partial exits reduce
        risk exposure; qty/contracts remain backward-compatible fallbacks.
        """
        for key in ("quantity_remaining", "qty", "contracts"):
            try:
                value = pos.get(key)
                if value is not None and int(value) > 0:
                    return int(value)
            except Exception:
                continue
        return 0

    @classmethod
    def _position_capital_for_exposure(cls, pos: dict) -> float:
        return cls._position_price_for_exposure(pos) * cls._position_qty_for_exposure(pos) * 100

    def _sector_capital_deployed(self, positions: list, sector: str) -> float:
        total = 0.0
        for pos in positions:
            ticker_in_pos = str(pos.get("underlying") or pos.get("ticker") or "")
            pos_sector = self.SECTOR_MAP.get(ticker_in_pos.upper(), "other")
            if pos_sector == sector:
                try:
                    total += self._position_capital_for_exposure(pos)
                except Exception:
                    pass
        return total

    def _pending_orders_capital(self, client_id: str) -> Optional[float]:
        """
        Sum capital reserved by entry orders that may still become exposure.

        LIVE policy: if this query fails, callers can fail closed because pending
        capital is part of the hard capital gate. The status list is intentionally
        canonical and aligned with OSM/PositionManager so fallback risk math cannot
        drift from the snapshot truth path.
        """
        try:
            from ap.db import conn, run_with_retry

            statuses = tuple(sorted(_ENTRY_CAPITAL_RESERVED_STATUSES))

            def _fn():
                with conn() as c:
                    c.execute(
                        """
                        SELECT COALESCE(SUM(
                            COALESCE(
                                NULLIF(reserved_cost, 0),
                                CASE
                                    WHEN limit_price IS NOT NULL AND limit_price > 0
                                    THEN limit_price * qty * 100
                                    ELSE 0
                                END
                            )
                        ), 0) AS pending_capital
                        FROM orders
                        WHERE client_id = %s
                          AND kind = 'ENTRY'
                          AND UPPER(COALESCE(status, '')) = ANY(%s)
                          AND UPPER(COALESCE(status, '')) NOT IN ('CANCELLED', 'CANCELED', 'REJECTED', 'ERROR', 'FAILED', 'FILLED', 'CLOSED')
                        """,
                        (client_id, list(statuses)),
                    )
                    row = c.fetchone()
                    pending_capital = float((row or {}).get("pending_capital") or 0)

                    # LIVE integrity guard: an active ENTRY order with neither
                    # reserved_cost nor limit_price has unknown dollar exposure.
                    # Do not silently count it as $0 in live capital gates.
                    if self._is_live_mode() and self.pending_capital_fail_closed_live:
                        c.execute(
                            """
                            SELECT COUNT(*) AS missing_cost
                            FROM orders
                            WHERE client_id = %s
                              AND kind = 'ENTRY'
                              AND UPPER(COALESCE(status, '')) = ANY(%s)
                              AND UPPER(COALESCE(status, '')) NOT IN ('CANCELLED', 'CANCELED', 'REJECTED', 'ERROR', 'FAILED', 'FILLED', 'CLOSED')
                              AND (reserved_cost IS NULL OR reserved_cost <= 0)
                              AND (limit_price IS NULL OR limit_price <= 0)
                            """,
                            (client_id, list(statuses)),
                        )
                        missing = int((c.fetchone() or {}).get("missing_cost") or 0)
                        if missing > 0:
                            raise RuntimeError(
                                f"pending_capital_integrity_failure: {missing} active ENTRY order(s) missing reserved_cost and limit_price"
                            )

                    return pending_capital

            return run_with_retry(_fn)
        except Exception as e:
            log.debug("_pending_orders_capital failed (non-critical): %s", e)
            self._alert_degraded(
                "PENDING_ORDERS_CAPITAL_UNAVAILABLE",
                severity="CRITICAL" if self._is_live_mode() and self.pending_capital_fail_closed_live else "WARNING",
                client_id=client_id,
                details={"error": str(e), "reserved_statuses": sorted(_ENTRY_CAPITAL_RESERVED_STATUSES)},
            )
            return None

    def _pending_capital_from_snapshot_or_db(self, snap: dict[str, Any], client_id: str) -> Optional[float]:
        """Return pending ENTRY dollar exposure using the strongest available source.

        Prefer APPositionManager.snapshot()["pending_entry_capital"] because it is
        computed from the same active entry lifecycle states as pending_entries
        in one consistent snapshot read. Fall back to the local DB query only for
        older position-manager versions that do not expose that field.
        """
        if isinstance(snap, dict) and snap.get("pending_entry_capital") is not None:
            try:
                return float(snap.get("pending_entry_capital") or 0.0)
            except Exception as e:
                self._alert_degraded(
                    "SNAPSHOT_PENDING_ENTRY_CAPITAL_INVALID",
                    severity="CRITICAL" if self._is_live_mode() else "WARNING",
                    client_id=client_id,
                    details={"value": repr(snap.get("pending_entry_capital")), "error": str(e)},
                )
                if self._is_live_mode() and self.pending_capital_fail_closed_live:
                    return None
        return self._pending_orders_capital(client_id)

    def _ticker_capital_deployed(self, positions: list, ticker: str) -> float:
        total = 0.0
        for pos in positions:
            t = str(pos.get("underlying") or pos.get("ticker") or "")
            if t.upper() == ticker.upper():
                try:
                    total += self._position_capital_for_exposure(pos)
                except Exception:
                    pass
        return total

    def get_sector_exposure(self, positions: list) -> dict[str, float]:
        exposure: dict[str, float] = {}
        for pos in positions:
            ticker_in_pos = str(pos.get("underlying") or pos.get("ticker") or "")
            sector = self.SECTOR_MAP.get(ticker_in_pos.upper(), "other")
            try:
                exposure[sector] = exposure.get(sector, 0.0) + self._position_capital_for_exposure(pos)
            except Exception:
                pass
        return exposure

    def evaluate(self, signal: dict, client_id: str = "default") -> ControlDecision:
        bootstrap_mode = False
        total_trades = 0
        ticker = signal.get("ticker", signal.get("symbol", "?"))

        if ticker and ticker.upper() in _INDEX_TO_ETF:
            mapped = _INDEX_TO_ETF[ticker.upper()]
            orig = ticker
            log.info("[%s] Index ticker normalized to %s at ingest (prevents duplicate orders)", orig, mapped)
            ticker = mapped
            signal["ticker"] = mapped
            signal["symbol"] = mapped
            signal["_original_index_ticker"] = orig

        score = float(signal.get("score", 0) or 0)
        signal_id = str(signal.get("signal_id") or uuid.uuid4())
        signal["signal_id"] = signal_id

        raw_side = signal.get("side") or signal.get("direction") or "CALL"
        norm_side = str(raw_side).upper().strip()
        if norm_side in {"BUY", "LONG", "CALLS", "BULLISH"}:
            norm_side = "CALL"
        elif norm_side in {"SELL", "SHORT", "PUTS", "BEARISH"}:
            norm_side = "PUT"
        signal["side"] = norm_side
        signal["direction"] = norm_side

        log.info("[%s] evaluate | score=%.1f | side=%s | client=%s", ticker, score, norm_side, client_id)

        if getattr(self, "exit_engine_down", False):
            return self._block(signal_id, ticker, client_id, "blocked_system", "exit_engine_down__protective_systems_unavailable")
        if self._kill_switch_fn and self._kill_switch_fn():
            return self._block(signal_id, ticker, client_id, "blocked_system", "kill_switch_active")

        current_mode = self._current_mode()
        if current_mode == "READ_ONLY":
            return self._block(signal_id, ticker, client_id, "blocked_system", "mode_read_only")
        if current_mode == "LIVE":
            ev_score = float(signal.get("ev_score") or 0)
            if ev_score <= 0:
                return self._block(signal_id, ticker, client_id, "blocked_system", "live_mode_requires_ev_score")
        # In LIVE mode, use ev_score (backtested EV) as the authoritative gate score.
        effective_score = float(signal.get("ev_score") or score) if current_mode == "LIVE" else score

        direction_raw = norm_side
        timeframe_raw = signal.get("timeframe", "1d")
        setup_key = f"{client_id}:{ticker.upper()}:{direction_raw}:{timeframe_raw}"
        signal_key = f"sig:{signal_id}:{client_id}"
        _now_ts = time.time()
        if len(self._seen_signals) > 500:
            self._seen_signals = {k: v for k, v in self._seen_signals.items() if _now_ts - v < 1800}
        if signal_key in self._seen_signals:
            return self._block(signal_id, ticker, client_id, "blocked_system", "duplicate_signal_id")
        if setup_key in self._seen_signals and (_now_ts - self._seen_signals[setup_key]) < 1800:
            return self._block(signal_id, ticker, client_id, "blocked_system", f"duplicate_setup ({ticker} {direction_raw} {timeframe_raw})")

        snap = self._get_snapshot(client_id, ticker=ticker, signal_id=signal_id)
        if current_mode == "LIVE" and not snap.get("_snapshot_ok", True):
            return self._block(
                signal_id,
                ticker,
                client_id,
                "blocked_system",
                f"snapshot_unavailable_live_blocked ({snap.get('_snapshot_error', 'unknown')})",
                reason_code="SNAPSHOT_UNAVAILABLE_LIVE_BLOCKED",
            )

        total_trades = int(snap.get("total_trades") or 0)
        bootstrap_mode = total_trades < 20

        if snap["open_count"] >= self.max_positions:
            return self._block(signal_id, ticker, client_id, "blocked_risk", f"max_positions ({snap['open_count']}/{self.max_positions})")
        effective_count = snap["open_count"] + snap["pending_entries"]
        if effective_count >= self.max_positions:
            return self._block(signal_id, ticker, client_id, "blocked_risk", f"max_positions_with_pending ({effective_count}/{self.max_positions})")

        estimated_contracts_pre = max(1, self._base_contracts(effective_score))
        estimated_new_cost_pre = estimated_contracts_pre * 100 * _estimate_premium(ticker)
        pending_capital_real = self._pending_capital_from_snapshot_or_db(snap, client_id)
        if pending_capital_real is None:
            if current_mode == "LIVE" and self.pending_capital_fail_closed_live:
                return self._block(
                    signal_id,
                    ticker,
                    client_id,
                    "blocked_system",
                    "pending_capital_unavailable_live_blocked",
                    reason_code="PENDING_CAPITAL_UNAVAILABLE",
                )
            pending_capital_real = 0.0
        projected_total = snap["capital_deployed"] + pending_capital_real + estimated_new_cost_pre
        max_capital = self.account_equity * self.max_capital_pct
        if projected_total > max_capital:
            return self._block(signal_id, ticker, client_id, "blocked_risk", f"capital_limit (projected ${projected_total:.0f} > ${max_capital:.0f})")

        sector = self.SECTOR_MAP.get(ticker.upper(), "other")
        sector_deployed = self._sector_capital_deployed(snap["open_positions"] + snap["closing_positions"], sector)
        estimated_contracts = max(1, self._base_contracts(effective_score))
        estimated_new_cost = estimated_contracts * 100 * _estimate_premium(ticker)
        projected_sector = sector_deployed + estimated_new_cost
        effective_equity = self.account_equity
        max_sector_capital = effective_equity * self.max_sector_pct
        if projected_sector > max_sector_capital:
            return self._block(signal_id, ticker, client_id, "blocked_risk", f"sector_cap_{sector} (projected ${projected_sector:.0f} > ${max_sector_capital:.0f})")

        ticker_deployed = self._ticker_capital_deployed(snap["open_positions"] + snap["closing_positions"], ticker)
        estimated_new_cost_ticker = estimated_contracts * 100 * _estimate_premium(ticker)
        projected_ticker = ticker_deployed + estimated_new_cost_ticker
        max_ticker_capital = effective_equity * self.max_ticker_pct
        if projected_ticker > max_ticker_capital:
            return self._block(signal_id, ticker, client_id, "blocked_risk", f"ticker_cap_{ticker.upper()} (projected ${projected_ticker:.0f} > ${max_ticker_capital:.0f})")

        if norm_side == "CALL" and snap["calls_open"] >= self.max_calls:
            return self._block(signal_id, ticker, client_id, "blocked_risk", f"max_calls ({snap['calls_open']}/{self.max_calls})")
        if norm_side == "PUT" and snap["puts_open"] >= self.max_puts:
            return self._block(signal_id, ticker, client_id, "blocked_risk", f"max_puts ({snap['puts_open']}/{self.max_puts})")
        if snap["trades_today"] >= self.max_trades_today:
            return self._block(signal_id, ticker, client_id, "blocked_risk", f"max_trades_today ({snap['trades_today']}/{self.max_trades_today})")
        if snap["realized_pnl_today"] <= self.max_daily_loss:
            return self._block(signal_id, ticker, client_id, "blocked_risk", f"daily_loss_limit (${snap['realized_pnl_today']:.2f} <= ${self.max_daily_loss:.2f})")
        _open_only_tickers = {str(p.get("underlying") or p.get("ticker") or "").upper() for p in snap["open_positions"]}
        if _open_only_tickers and ticker.upper() in _open_only_tickers:
            return self._block(signal_id, ticker, client_id, "blocked_risk", f"ticker_already_active ({ticker})")

        cooldown_key = f"{ticker.upper()}:{direction_raw}:cooldown"
        if cooldown_key in self._trade_cooldowns:
            elapsed = time.time() - self._trade_cooldowns[cooldown_key]
            if elapsed < 1800:
                return self._block(signal_id, ticker, client_id, "blocked_risk", f"same_setup_cooldown ({ticker} {direction_raw}, {int(1800 - elapsed)}s remaining)")

        if self.pm:
            try:
                if self.pm.has_pending_entry(ticker):
                    return self._block(signal_id, ticker, client_id, "blocked_risk", f"pending_entry_exists ({ticker})")
            except Exception as e:
                log.warning("[%s] has_pending_entry check failed: %s", ticker, e)

        if ticker.upper() in _PRIORITY_TICKERS:
            if effective_score < _PRIORITY_FLOOR:
                self._store_update(signal_id, "rejected", f"priority score {score:.1f} < floor {_PRIORITY_FLOOR}")
                return self._block(signal_id, ticker, client_id, "blocked_score", f"score_below_priority_floor ({score:.1f}<{_PRIORITY_FLOOR})")
        else:
            if effective_score < self.score_floor:
                self._store_update(signal_id, "rejected", f"score {effective_score:.1f} < floor {self.score_floor}")
                return self._block(signal_id, ticker, client_id, "blocked_score", f"score_below_floor ({effective_score:.1f}<{self.score_floor})")

        score_breakdown = signal.get("score_breakdown") or {}
        if "real_time_ctx" in score_breakdown:
            ctx = float(score_breakdown.get("real_time_ctx", 0) or 0)
            if ctx < self.context_floor:
                self._store_update(signal_id, "context_blocked", f"ctx={ctx:.1f} < floor {self.context_floor}")
                return self._block(signal_id, ticker, client_id, "blocked_score", f"context_below_floor (ctx={ctx:.1f}<{self.context_floor})")

        try:
            from ap_tier_engine import Tier
            tier = Tier.from_score(score)
        except Exception:
            tier = self._fallback_tier(score)

        if tier in ("REJECT", "reject"):
            self._store_update(signal_id, "rejected", f"tier=REJECT score={score:.1f}")
            return self._block(signal_id, ticker, client_id, "blocked_score", f"tier_reject (score={score:.1f})")
        if tier in ("SHADOW", "shadow"):
            tier = "C"

        intel = self._run_intelligence(signal)
        intel_score = float(intel.get("score", 0))
        intel_approve = intel.get("approved", True)
        intel_reason = intel.get("reasoning", "")
        intel_avail = intel.get("_available", False)
        if intel_avail and not intel_approve:
            self._store_update(signal_id, "rejected", f"intel_blocked: {intel_reason[:100]}")
            return self._block(signal_id, ticker, client_id, "blocked_intel", f"intel_rejected: {intel_reason[:80]}")

        intel_contracts = int(intel.get("contracts", 1) or 1)
        if bootstrap_mode:
            intel_contracts = 1

        feedback_mod = 1.0
        setup_status = "LEARNING"
        if self.feedback:
            try:
                feedback_mod = self.feedback.get_size_modifier(
                    ticker=ticker,
                    pattern=signal.get("pattern", ""),
                    timeframe=signal.get("timeframe", "1d"),
                    side=signal.get("side", "CALL"),
                )
                setup_status = self.feedback.get_setup_status(
                    ticker,
                    signal.get("pattern", ""),
                    signal.get("timeframe", "1d"),
                    signal.get("side", "CALL"),
                )
                if setup_status == "DOWNGRADED" and not self.paper:
                    return self._block(signal_id, ticker, client_id, "blocked_intel", "setup_downgraded_live_blocked")
            except Exception as e:
                log.warning("[%s] Feedback modifier failed: %s", ticker, e)

        placeholder_premium = 100 * _estimate_premium(ticker)
        raw_pnl = snap.get("realized_pnl_today", 0.0)
        in_session = True
        try:
            from zoneinfo import ZoneInfo
            from datetime import time as dt_time
            now_et = datetime.now(ZoneInfo("America/New_York"))
            in_session = dt_time(9, 30) <= now_et.time() <= dt_time(16, 0)
        except Exception:
            in_session = True
        pnl_today = raw_pnl if in_session else 0.0

        _sizing = None
        if self.sizer and not bootstrap_mode:
            try:
                _sizing = self.sizer.compute(
                    client_id=client_id,
                    tier=str(tier),
                    premium_per_contract=placeholder_premium,
                    account_equity=self.account_equity,
                    realized_pnl_today=pnl_today,
                    position_manager=self.pm,
                )
                contracts = _sizing.contracts
                if contracts <= 0:
                    return self._block(signal_id, ticker, client_id, "blocked_risk", f"sizer_blocked: {_sizing.reason}")
                if intel_avail and intel_contracts > 0:
                    contracts = min(contracts, intel_contracts)
            except Exception as e:
                log.warning("[%s] Sizer failed (%s) -- falling back to tier", ticker, e)
                contracts = self._base_contracts(effective_score)
        else:
            if str(tier).upper() == "B":
                # B-tier: score-driven, cap at 4 (budget gate handles the real ceiling)
                contracts = self._base_contracts(effective_score)
                contracts = min(contracts, 4)  # B-tier cap
            else:
                tier_mult = 1.0 if str(tier).upper() == "A+" else 0.6
                base = self._base_contracts(effective_score)
                contracts = max(1, round(base * feedback_mod * tier_mult))
                if intel_avail and intel_contracts > 0:
                    contracts = min(contracts, intel_contracts)

        if bootstrap_mode:
            contracts = 1

        trigger = signal.get("trigger") or {}
        entry_price = signal.get("entry_price") or trigger.get("entry")
        stop_price = signal.get("stop_price") or trigger.get("stop")
        target_price = signal.get("target_price") or trigger.get("pt1") or trigger.get("pt2")
        trigger_type = "breach" if entry_price else "immediate"

        plan = ApprovedExecutionPlan(
            plan_id=str(uuid.uuid4()),
            signal_id=signal_id,
            client_id=client_id,
            ticker=ticker,
            side=signal.get("side", "CALL"),
            direction=signal.get("direction", signal.get("side", "CALL")),
            pattern=signal.get("pattern", signal.get("pattern_id", "")),
            timeframe=signal.get("timeframe", "1d"),
            contracts=contracts,
            max_position_usd=(1 if bootstrap_mode else contracts) * 100 * _estimate_premium(ticker),
            tier=str(tier),
            score=score,
            intel_score=intel_score,
            confidence_bucket=signal.get("confidence_tag", "standard_pool"),
            trigger_type=trigger_type,
            trigger_price=float(entry_price) if entry_price else None,
            stop_underlying=float(stop_price) if stop_price else None,
            target_underlying=float(target_price) if target_price else None,
            mode="live" if not self.paper else "paper",
            paper_sim=self.paper,
            reasoning=intel_reason or f"tier={tier} score={score:.1f} feedback={feedback_mod:.2f} setup={setup_status}",
            intel_available=intel_avail,
            stage="APPROVED",
            metadata={
                "setup_status": setup_status,
                "feedback_mod": feedback_mod,
                "sizing_method": _sizing.method if _sizing is not None else "tier_fallback",
                "sizing_reason": _sizing.reason if _sizing is not None else "",
                "intel_result": intel,
                "sector": self.SECTOR_MAP.get(ticker.upper(), "other"),
                "snapshot_at_eval": {
                    "open_count": snap["open_count"],
                    "capital_deployed": snap["capital_deployed"],
                    "pending_entries": snap["pending_entries"],
                    "calls_open": snap["calls_open"],
                    "puts_open": snap["puts_open"],
                    "total_trades": total_trades,
                    "bootstrap_mode": bootstrap_mode,
                    "snapshot_ok": snap.get("_snapshot_ok"),
                    "snapshot_ts": snap.get("_snapshot_ts"),
                    "snapshot_age_sec": snap.get("_snapshot_age_sec"),
                    "snapshot_error": snap.get("_snapshot_error"),
                },
            },
        )

        # Final race-condition guard before dedup persistence / queue approval.
        if self._kill_switch_fn and self._kill_switch_fn():
            return self._block(signal_id, ticker, client_id, "blocked_system", "kill_switch_active_pre_commit")

        # Persist dedup first. Only then add in-memory keys and mark queued.
        # In LIVE mode a dedup-persist failure blocks the signal (fail-closed).
        # In paper mode a transient DB hiccup should not kill a valid signal.
        try:
            self._persist_dedup(signal_id, ticker, direction_raw, timeframe_raw, client_id)
        except Exception as _dedup_err:
            if self._is_live_mode():
                return self._block(
                    signal_id, ticker, client_id,
                    "blocked_system",
                    f"dedup_persist_failed_live: {_dedup_err}",
                    reason_code="DEDUP_PERSIST_FAILED",
                )
            log.warning("[%s] Dedup persist failed in paper — proceeding: %s", ticker, _dedup_err)
        self._seen_signals[signal_key] = time.time()
        self._seen_signals[setup_key] = time.time()
        self._store_update(signal_id, "queued", timestamp_flag="queued_at")

        try:
            if emit_decision_event:
                emit_decision_event(
                    run_id=self.run_id,
                    candidate_id=signal_id,
                    client_id=client_id,
                    stage="master_control",
                    decision="APPROVE",
                    explanation="Signal approved by master control",
                    symbol=ticker,
                    setup_type=signal.get("pattern"),
                    timeframe=signal.get("timeframe"),
                    strategy_version=self.strategy_version,
                    config_hash=self.config_hash,
                    git_commit=self.git_commit,
                    inputs={
                        "score": score,
                        "ev_score": signal.get("ev_score"),
                        "open_count": snap.get("open_count"),
                        "pending_entries": snap.get("pending_entries"),
                        "capital_deployed": snap.get("capital_deployed"),
                        "total_trades": snap.get("total_trades"),
                        "snapshot_ts": snap.get("_snapshot_ts"),
                        "snapshot_age_sec": snap.get("_snapshot_age_sec"),
                    },
                    thresholds={
                        "priority_floor": _PRIORITY_FLOOR,
                        "score_floor": self.score_floor,
                        "context_floor": self.context_floor,
                        "max_positions": self.max_positions,
                        "max_capital_pct": self.max_capital_pct,
                    },
                    context={
                        "tier": plan.tier,
                        "contracts": plan.contracts,
                        "trigger_type": plan.trigger_type,
                        "bootstrap_mode": bootstrap_mode,
                    },
                )
        except Exception as e:
            log.debug("Approval event emit failed (non-critical): %s", e)
            self._alert_degraded(
                "APPROVAL_EVENT_EMIT_FAILED",
                severity="WARNING",
                client_id=client_id,
                ticker=ticker,
                signal_id=signal_id,
                details={"error": str(e)},
            )

        return ControlDecision(ok=True, stage="approved", reason="", plan=plan, signal_id=signal_id, ticker=ticker, client_id=client_id)

    def _zero_snapshot(self, *, snapshot_ok: bool = True, snapshot_error: str = "") -> dict[str, Any]:
        return {
            "open_count": 0,
            "open_tickers": set(),
            "calls_open": 0,
            "puts_open": 0,
            "capital_deployed": 0.0,
            "pending_entries": 0,
            "pending_exits": 0,
            "trades_today": 0,
            "realized_pnl_today": 0.0,
            "total_trades": 0,
            "open_positions": [],
            "closing_positions": [],
            "_snapshot_ok": snapshot_ok,
            "_snapshot_error": snapshot_error,
        }

    def _get_snapshot(self, client_id: str, ticker: str = "", signal_id: str = "") -> dict[str, Any]:
        """
        Pull position snapshot. In LIVE mode, callers block if _snapshot_ok is False.
        This prevents snapshot failure from being mistaken for zero exposure.
        """
        if self.pm:
            try:
                if hasattr(self.pm, "client_id") and self.pm.client_id != client_id:
                    msg = f"master_control client_id={client_id} but pm.client_id={self.pm.client_id}"
                    live = self._is_live_mode()
                    log.error(
                        "SNAPSHOT MISMATCH: %s -- %s",
                        msg,
                        "LIVE will block" if live else "using pm data in non-live mode",
                    )
                    self._alert_degraded(
                        "SNAPSHOT_CLIENT_MISMATCH",
                        severity="CRITICAL" if live else "WARNING",
                        client_id=client_id,
                        ticker=ticker,
                        signal_id=signal_id,
                        details={"message": msg},
                    )
                    if live:
                        return self._zero_snapshot(
                            snapshot_ok=False,
                            snapshot_error="snapshot_client_mismatch_live_blocked",
                        )
                snap = self.pm.snapshot()
                if not isinstance(snap, dict):
                    raise TypeError(f"snapshot() returned {type(snap).__name__}, expected dict")
                snap.setdefault("open_count", 0)
                snap.setdefault("open_tickers", set())
                snap.setdefault("calls_open", 0)
                snap.setdefault("puts_open", 0)
                snap.setdefault("capital_deployed", 0.0)
                snap.setdefault("pending_entries", 0)
                snap.setdefault("pending_exits", 0)
                snap.setdefault("trades_today", 0)
                snap.setdefault("realized_pnl_today", 0.0)
                snap.setdefault("total_trades", 0)
                snap.setdefault("open_positions", [])
                snap.setdefault("closing_positions", [])
                snap["_snapshot_ok"] = True
                return self._validate_snapshot_freshness(snap, client_id=client_id, ticker=ticker, signal_id=signal_id)
            except Exception as e:
                self._alert_degraded(
                    "SNAPSHOT_UNAVAILABLE",
                    severity="CRITICAL" if self._is_live_mode() else "WARNING",
                    client_id=client_id,
                    ticker=ticker,
                    signal_id=signal_id,
                    details={"error": str(e)},
                )
                log.warning("[%s] snapshot() failed: %s -- %s", client_id, e, "LIVE will block" if self._is_live_mode() else "using zeros")
                return self._zero_snapshot(snapshot_ok=False, snapshot_error=str(e))

        msg = "position_manager_missing"
        if self._is_live_mode():
            self._alert_degraded(
                "SNAPSHOT_UNAVAILABLE",
                severity="CRITICAL",
                client_id=client_id,
                ticker=ticker,
                signal_id=signal_id,
                details={"error": msg},
            )
            return self._zero_snapshot(snapshot_ok=False, snapshot_error=msg)

        return self._zero_snapshot(snapshot_ok=True, snapshot_error="")

    def _run_intelligence(self, signal: dict) -> dict[str, Any]:
        try:
            from intelligence_bridge import INTELLIGENCE_AVAILABLE, run_intelligence_check
            if not INTELLIGENCE_AVAILABLE:
                return {"approved": True, "score": 0, "contracts": 1, "reasoning": "intel_unavailable", "_available": False}
            trigger = signal.get("trigger") or {}
            price = signal.get("entry_price") or trigger.get("entry") or signal.get("current_price") or 100.0
            result = run_intelligence_check(signal, underlying_price=float(price))
            result["_available"] = True
            return result
        except Exception as e:
            log.debug("Intelligence unavailable: %s", e)
            return {"approved": True, "score": 0, "contracts": 1, "reasoning": f"intel_error: {e}", "_available": False}

    def _fallback_tier(self, score: float) -> str:
        if score >= 85:
            return "A+"
        if score >= 75:
            return "A"
        if score >= 60:
            return "B"
        if score >= 35:
            return "C"
        return "REJECT"

    def _base_contracts(self, score: float) -> int:
        """Contract count from signal score.
        Budget is 10% of account ($1800) — actual contracts depend on premium.
        At $1.06 premium: $1800 / $106 = ~16 raw, capped at 5 by capital gates.
        At $4.30 premium: $1800 / $430 = ~4 contracts.
        Higher scores get more. Capital gate is the real ceiling, not this function.
        """
        if score >= 95: return 5
        if score >= 90: return 4
        if score >= 85: return 3
        if score >= 75: return 3
        if score >= 60: return 2   # daily scanner range
        return 1

    def revalidate_exposure(self, plan, client_id: str = "default") -> ControlDecision:
        """
        Re-check capital-sensitive gates after contract selection has updated
        plan.max_position_usd with real premium. This version logs blocked and
        approved capital-utilization rows before returning.
        """
        ticker = plan.ticker
        real_cost = float(plan.max_position_usd)
        equity = self.account_equity
        signal_id = plan.signal_id
        snap = self._get_snapshot(client_id, ticker=ticker, signal_id=signal_id)
        if self._is_live_mode() and not snap.get("_snapshot_ok", True):
            return self._block(
                signal_id,
                ticker,
                client_id,
                "blocked_system",
                f"snapshot_unavailable_live_blocked ({snap.get('_snapshot_error', 'unknown')})",
                reason_code="SNAPSHOT_UNAVAILABLE_LIVE_BLOCKED",
            )

        if self._kill_switch_fn and self._kill_switch_fn():
            return self._block(signal_id, ticker, client_id, "blocked_system", "kill_switch_active_post_snapshot")

        pending_cap = self._pending_capital_from_snapshot_or_db(snap, client_id)
        if pending_cap is None:
            if self._is_live_mode() and self.pending_capital_fail_closed_live:
                return self._block(
                    signal_id,
                    ticker,
                    client_id,
                    "blocked_system",
                    "pending_capital_unavailable_live_blocked",
                    reason_code="PENDING_CAPITAL_UNAVAILABLE",
                )
            pending_cap = 0.0
        proj_total = snap["capital_deployed"] + pending_cap + real_cost
        max_capital = equity * self.max_capital_pct
        pct_used = proj_total / equity * 100 if equity > 0 else 0

        sector = self.SECTOR_MAP.get(ticker.upper(), "other")
        sector_deployed = self._sector_capital_deployed(snap["open_positions"] + snap["closing_positions"], sector)
        proj_sector = sector_deployed + real_cost
        max_sector = equity * self.max_sector_pct

        ticker_deployed = self._ticker_capital_deployed(snap["open_positions"] + snap["closing_positions"], ticker)
        proj_ticker = ticker_deployed + real_cost
        max_ticker = equity * self.max_ticker_pct

        def _log_revalidation(blocked: bool, block_reason: str = "") -> None:
            self._log_capital_utilization(
                client_id=client_id,
                ticker=ticker,
                signal_id=signal_id,
                deployed=snap["capital_deployed"],
                pending=pending_cap,
                new_cost=real_cost,
                projected=proj_total,
                limit=max_capital,
                pct_used=pct_used,
                sector=sector,
                sector_deployed=sector_deployed,
                sector_projected=proj_sector,
                sector_limit=max_sector,
                ticker_deployed=ticker_deployed,
                ticker_projected=proj_ticker,
                ticker_limit=max_ticker,
                blocked=blocked,
                block_reason=block_reason,
            )

        if proj_total > max_capital:
            reason = f"capital_limit: ${proj_total:.0f} > ${max_capital:.0f}"
            _log_revalidation(True, reason)
            return self._block(
                signal_id,
                ticker,
                client_id,
                "blocked_risk",
                f"revalidate_capital_limit (${proj_total:.0f} > ${max_capital:.0f} | deployed=${snap['capital_deployed']:.0f} pending=${pending_cap:.0f} real_cost=${real_cost:.0f})",
            )

        if proj_sector > max_sector:
            reason = f"sector_cap_{sector}: ${proj_sector:.0f} > ${max_sector:.0f}"
            _log_revalidation(True, reason)
            return self._block(
                signal_id,
                ticker,
                client_id,
                "blocked_risk",
                f"revalidate_sector_cap_{sector} (${proj_sector:.0f} > ${max_sector:.0f} | real_cost=${real_cost:.0f})",
            )

        if proj_ticker > max_ticker:
            reason = f"ticker_cap_{ticker.upper()}: ${proj_ticker:.0f} > ${max_ticker:.0f}"
            _log_revalidation(True, reason)
            return self._block(
                signal_id,
                ticker,
                client_id,
                "blocked_risk",
                f"revalidate_ticker_cap_{ticker.upper()} (${proj_ticker:.0f} > ${max_ticker:.0f} | real_cost=${real_cost:.0f})",
            )

        _log_revalidation(False, "")
        log.info(
            "[%s] Re-validation passed | real_cost=$%.0f total_proj=$%.0f/%.0f sector_%s=$%.0f/%.0f ticker=$%.0f/%.0f",
            ticker,
            real_cost,
            proj_total,
            max_capital,
            sector,
            proj_sector,
            max_sector,
            proj_ticker,
            max_ticker,
        )
        return ControlDecision(ok=True, stage="revalidated", signal_id=signal_id, ticker=ticker, client_id=client_id)

    def _block(self, signal_id, ticker, client_id, stage, reason, reason_code: str = "") -> ControlDecision:
        reason_code = reason_code or self._reason_code_from_block(stage, reason)
        log.info("[%s] BLOCKED | stage=%s | reason=%s | reason_code=%s", ticker, stage, reason, reason_code)
        try:
            if emit_decision_event:
                emit_decision_event(
                    run_id=self.run_id,
                    candidate_id=signal_id,
                    client_id=client_id,
                    stage=stage,
                    decision="REJECT",
                    reason_code=reason_code,
                    explanation=reason,
                    symbol=ticker,
                    strategy_version=self.strategy_version,
                    config_hash=self.config_hash,
                    git_commit=self.git_commit,
                )
        except Exception as e:
            log.debug("Decision event emit failed (non-critical): %s", e)
            self._alert_degraded(
                "DECISION_EVENT_EMIT_FAILED",
                severity="WARNING",
                client_id=client_id,
                ticker=ticker,
                signal_id=signal_id,
                details={"error": str(e), "stage": stage, "reason": reason},
            )
        return ControlDecision(ok=False, stage=stage, reason=reason, reason_code=reason_code, signal_id=signal_id, ticker=ticker, client_id=client_id)

    def _reason_code_from_block(self, stage: str, reason: str) -> str:
        r = (reason or "").lower()
        if "pending_capital_unavailable" in r:
            return "PENDING_CAPITAL_UNAVAILABLE"
        if "snapshot_client_mismatch" in r:
            return "SNAPSHOT_CLIENT_MISMATCH_LIVE_BLOCKED"
        if "kill_switch" in r:
            return "KILL_SWITCH_ACTIVE"
        if "duplicate" in r or "dedup" in r:
            return "DEDUP_BLOCK"
        if "capital_limit" in r:
            return "CAPITAL_UTIL_BLOCK"
        if "sector_cap" in r:
            return "SECTOR_CAP_BLOCK"
        if "ticker_cap" in r:
            return "TICKER_CAP_BLOCK"
        if "max_positions" in r:
            return "POSITION_LIMIT_REACHED"
        if "max_calls" in r:
            return "MAX_CALLS"
        if "max_puts" in r:
            return "MAX_PUTS"
        if "daily_loss_limit" in r:
            return "DAILY_STOP_ACTIVE"
        if "score" in r or "tier_reject" in r or "context_below_floor" in r:
            return "SCORE_BELOW_THRESHOLD"
        if "live_mode_requires_ev_score" in r:
            return "LIVE_REQUIRES_EV_SCORE"
        if "exit_engine_down" in r:
            return "PROTECTIVE_SYSTEM_DOWN"
        if "snapshot_unavailable" in r or "snapshot" in r:
            return "SNAPSHOT_UNAVAILABLE_LIVE_BLOCKED"
        return "SESSION_RULE_BLOCK"

    def _store_update(self, signal_id: str, status: str, context_notes: str = "", timestamp_flag: str = ""):
        if not self.store:
            return
        try:
            if timestamp_flag:
                self.store.update_status(signal_id, status, timestamp_flag=timestamp_flag)
            else:
                self.store.update_status(signal_id, status, context_notes=context_notes)
        except Exception as e:
            log.debug("store_update failed: %s", e)

    def _log_capital_utilization(
        self,
        *,
        client_id: str,
        ticker: str,
        signal_id: str,
        deployed: float,
        pending: float,
        new_cost: float,
        projected: float,
        limit: float,
        pct_used: float,
        sector: str,
        sector_deployed: float,
        sector_projected: float,
        sector_limit: float,
        ticker_deployed: float,
        ticker_projected: float,
        ticker_limit: float,
        blocked: bool,
        block_reason: str,
    ):
        record = {
            "event": "capital_utilization",
            "ticker": ticker,
            "signal_id": signal_id,
            "deployed": round(deployed, 2),
            "pending": round(pending, 2),
            "new_cost": round(new_cost, 2),
            "projected": round(projected, 2),
            "limit": round(limit, 2),
            "pct_used": round(pct_used, 1),
            "headroom": round(limit - projected, 2),
            "sector": sector,
            "sector_deployed": round(sector_deployed, 2),
            "sector_projected": round(sector_projected, 2),
            "sector_limit": round(sector_limit, 2),
            "ticker_deployed": round(ticker_deployed, 2),
            "ticker_projected": round(ticker_projected, 2),
            "ticker_limit": round(ticker_limit, 2),
            "blocked": blocked,
            "block_reason": block_reason,
            "account_equity": round(self.account_equity, 2),
        }
        log.info(
            "[%s] CAPITAL_UTIL | %s | deployed=$%.0f pending=$%.0f new=$%.0f projected=$%.0f/%.0f (%.1f%%) headroom=$%.0f | %s",
            client_id,
            ticker,
            deployed,
            pending,
            new_cost,
            projected,
            limit,
            pct_used,
            limit - projected,
            f"BLOCKED: {block_reason}" if blocked else "APPROVED",
        )
        try:
            from ap.db import conn, run_with_retry

            def _insert():
                with conn() as c:
                    c.execute(
                        """
                        INSERT INTO audit_log (client_id, level, event, payload, ts)
                        VALUES (%s, 'INFO', 'capital_utilization', %s, NOW())
                        """,
                        (client_id, json.dumps(record)),
                    )

            run_with_retry(_insert)
        except Exception as e:
            log.debug("Capital utilization log failed (non-critical): %s", e)
            self._alert_degraded(
                "CAPITAL_UTILIZATION_LOG_FAILED",
                severity="WARNING",
                client_id=client_id,
                ticker=ticker,
                signal_id=signal_id,
                details={"error": str(e), "blocked": blocked, "block_reason": block_reason},
            )

    def reset_session(self, client_id: str = ""):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        client_prefix = client_id or (self.pm.client_id if hasattr(self.pm, "client_id") else "")
        self._seen_signals.clear()  # dict.clear() — same interface
        try:
            from ap.db import conn, run_with_retry

            def _clear():
                with conn() as c:
                    if client_prefix:
                        c.execute(
                            """
                            DELETE FROM kv
                            WHERE (k LIKE %s OR k LIKE %s)
                              AND updated_at::date = %s::date
                            """,
                            (
                                f"dedup:sig:%:{client_prefix}",
                                f"dedup:setup:{client_prefix}:%",
                                today,
                            ),
                        )
                    else:
                        log.warning("reset_session called without client_id -- skipping DB clear to protect other clients")

            run_with_retry(_clear)
        except Exception as e:
            log.debug("Dedup DB clear failed (non-critical): %s", e)

    def _persist_dedup(self, signal_id: str, ticker: str, direction: str, timeframe: str, client_id: str):
        try:
            from ap.db import conn, run_with_retry

            ts = datetime.now(timezone.utc).isoformat()
            keys = [
                f"dedup:sig:{signal_id}:{client_id}",
                f"dedup:setup:{client_id}:{ticker.upper()}:{direction}:{timeframe}",
            ]

            def _upsert():
                with conn() as c:
                    for key in keys:
                        c.execute(
                            """
                            INSERT INTO kv (k, v, updated_at)
                            VALUES (%s, %s, NOW())
                            ON CONFLICT (k) DO UPDATE SET v=%s, updated_at=NOW()
                            """,
                            (key, ts, ts),
                        )

            run_with_retry(_upsert)
        except Exception as e:
            log.debug("Dedup persist failed: %s", e)
            self._alert_degraded(
                "DEDUP_PERSIST_FAILED",
                severity="CRITICAL" if self._is_live_mode() else "WARNING",
                client_id=client_id,
                ticker=ticker,
                signal_id=signal_id,
                details={"error": str(e), "direction": direction, "timeframe": timeframe},
            )
            raise

    def _seed_dedup_from_db(self, client_id: str = "default"):
        try:
            from ap.db import conn, run_with_retry

            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            def _load():
                with conn() as c:
                    c.execute(
                        """
                        SELECT DISTINCT underlying, direction
                        FROM positions
                        WHERE client_id = %s
                          AND status = 'OPEN'
                          AND entry_ts::date = %s::date
                        """,
                        (client_id, today),
                    )
                    return c.fetchall()

            rows = run_with_retry(_load)
            for row in rows:
                underlying = str(row.get("underlying") or row.get("ticker") or "")
                direction = str(row.get("direction") or "CALL").upper()
                for tf in ("1d", "60m", "30m", "15m"):
                    self._seen_signals[f"{client_id}:{underlying.upper()}:{direction}:{tf}"] = time.time()
        except Exception as e:
            log.debug("Dedup seed failed (non-critical): %s", e)
            self._alert_degraded(
                "DEDUP_SEED_FAILED",
                severity="WARNING",
                client_id=client_id,
                details={"error": str(e)},
            )
