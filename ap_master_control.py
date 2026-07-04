from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from ap.admission_thresholds import (
    build_interrogation_packet_threshold_trace,
    build_threshold_trace,
    log_admission_thresholds,
    resolve_admission_thresholds,
)

try:
    from ap.counterfactual_tracker import track_counterfactual_signal
except Exception:  # pragma: no cover
    track_counterfactual_signal = None

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

# PR E / FIX-6: elevate intelligence_bridge to a module-level defensive
# import resolved ONCE at module load. Previously _run_intelligence did
# `from intelligence_bridge import ...` on every signal evaluation — a
# per-tick sys.modules lookup that adds zero functional value. Fail-open
# semantics preserved: if the import fails OR INTELLIGENCE_AVAILABLE is
# False, _run_intelligence returns approved=True with reasoning that
# attributes the decision to intel-unavailable.
try:
    from intelligence_bridge import (
        INTELLIGENCE_AVAILABLE as _INTEL_AVAILABLE,
        run_intelligence_check as _run_intel_check,
    )
except Exception as _intel_import_err:  # pragma: no cover
    _INTEL_AVAILABLE = False
    _run_intel_check = None
    log.info(
        "intelligence_bridge import unavailable at module load: %s — "
        "_run_intelligence will fail open (approved=True) for all signals.",
        _intel_import_err,
    )

MIN_CONTRACTS_PER_POSITION = int(os.getenv("MIN_CONTRACTS_PER_POSITION", "2"))
TRADE_DOSSIER_CACHE_TTL_SEC = float(os.getenv("TRADE_DOSSIER_CACHE_TTL_SEC", "1800"))
TRADE_DOSSIER_CACHE_MAX = int(os.getenv("TRADE_DOSSIER_CACHE_MAX", "500"))
TRADE_DOSSIER_QUEUE_MAX = int(os.getenv("TRADE_DOSSIER_QUEUE_MAX", "256"))
TRADE_DOSSIER_DB_HEALTH_TTL_SEC = float(os.getenv("TRADE_DOSSIER_DB_HEALTH_TTL_SEC", "900"))

# QUARTERLY REVIEW REQUIRED: These estimates are used for capital gate projections
# before real contract pricing is known. If ATM premiums diverge significantly
# (e.g. NVDA drops from 9.50 to 4.00 in low-vol regime), the capital gate will
# over-block valid signals. Update before each live trading quarter.
_PREMIUM_ESTIMATES: dict[str, float] = {
    # Calibrated to typical 2-4% OTM options, 2-5 DTE
    # These drive contract sizing in pre-selection gates only.
    # Once a real contract is selected, its limit_price overrides these.
    "NVDA":  4.50,
    "TSLA":  3.50,
    "META":  3.50,
    "NFLX":  4.00,
    "AMD":   2.00,
    "MSFT":  2.50,
    "AAPL":  1.50,
    "SPY":   1.50,
    "QQQ":   2.00,
    "IWM":   1.00,
    "DIA":   1.50,
    "COIN":  3.00,
    "PLTR":  1.00,
    "MSTR":  8.00,
    "AMZN":  2.50,
    "GOOG":  2.50,
    "GOOGL": 2.50,
    "GS":    2.50,
    "JPM":   2.00,
    "ORCL":  1.50,
    "WFC":   1.50,
    "MS":    2.00,
    "AVGO":  3.00,
    "CRM":   2.00,
    "PYPL":  1.50,
    "ORLY":  4.00,
    "AEP":   1.00,
}
_DEFAULT_PREMIUM_FALLBACK = 3.50

_PRIORITY_TICKERS = {"SPY", "QQQ", "IWM", "SPX", "NDX", "DIA"}
_PRIORITY_FLOOR = 40.0

# PR B — universal hard score floor.
# Below SCORE_MIN_ELIGIBLE: log/watch only (REJECTED_LOW_SCORE), no broker order.
# At/above:  eligible for normal admission path.
# Preferred / strongest tiers are informational — used by the dashboard and
# meta but do not change admission; eligibility is binary at SCORE_MIN_ELIGIBLE.
# Env-overridable so you can lower without redeploy if the day is dead.
SCORE_MIN_ELIGIBLE = float(os.getenv("SCORE_MIN_ELIGIBLE", "70"))
SCORE_PREFERRED    = float(os.getenv("SCORE_PREFERRED",    "75"))
SCORE_STRONGEST    = float(os.getenv("SCORE_STRONGEST",    "80"))

# ── SCORE-65 STRUCTURED ELIGIBILITY (refinement, 2026-05-xx) ──────────────────
# The universal hard floor at SCORE_MIN_ELIGIBLE=70 was too blunt: it rejected
# clean non-0DTE score-65 setups on META/NFLX/QQQ/COIN even though the class we
# actually wanted to stop was weak 0DTE / late-day index flow (esp. SPY 0DTE).
#
# This introduces a narrow exception band [SCORE65_FLOOR, SCORE_MIN_ELIGIBLE)
# that is admitted ONLY when the setup is structurally safer:
#   - NOT 0DTE
#   - NOT a late-day index 0DTE (covered by the not-0DTE rule + PR C anyway)
#   - spread_pct under SCORE65_MAX_SPREAD_PCT (when spread data is present)
#   - delta not weak/far-OTM when delta data is present
#   - timeframe is daily/overnight OR explicitly non-0DTE intraday
#
# Everything below SCORE65_FLOOR is still hard-rejected. 0DTE at score 65 is
# still hard-rejected. The feature is OFF by default (SCORE65_ALLOW=false) so
# nothing changes until the operator opts in after reviewing the count query.
SCORE65_ALLOW            = os.getenv("SCORE65_ALLOW", "false").strip().lower() in ("1", "true", "yes", "on")
SCORE65_FLOOR            = float(os.getenv("SCORE65_FLOOR", "65"))
SCORE65_MAX_SPREAD_PCT   = float(os.getenv("SCORE65_MAX_SPREAD_PCT", "0.08"))   # 8% — same band as submit chase
SCORE65_MIN_DELTA        = float(os.getenv("SCORE65_MIN_DELTA", "0.35"))        # reject weak/far-OTM if delta present

# PR B — the index/0DTE bucket reuses _PRIORITY_TICKERS above; we do not
# need a separate set. These tickers must NEVER trade below the hard floor
# regardless of mode (paper or live). Today's screenshot showed SPY score-65
# setups reaching execution; we cut that path at the root in evaluate().

# PR C — SPY/QQQ late-day 0DTE cutoff (minimize realized-loss exposure on
# the worst-performing setup class in our paper proof window).
#
# Rule:
#   - For _PRIORITY_TICKERS (SPY/QQQ/IWM/SPX/NDX/DIA), if the signal's
#     expiration is TODAY (0 DTE) and current time is at/after
#     INDEX_0DTE_CUTOFF_ET, REJECT with REJECTED_0DTE_CUTOFF.
#   - Between INDEX_LATE_DAY_CAUTION_ET and INDEX_0DTE_CUTOFF_ET, 0DTE
#     setups must clear INDEX_LATE_DAY_SCORE_FLOOR (default 75).
#   - Non-0DTE expirations (1+ DTE) are untouched — normal admission path.
# Env-overridable for live tuning without redeploy.
INDEX_0DTE_CUTOFF_ET         = os.getenv("INDEX_0DTE_CUTOFF_ET",         "14:30")  # 2:30 PM ET
INDEX_LATE_DAY_CAUTION_ET    = os.getenv("INDEX_LATE_DAY_CAUTION_ET",    "13:30")  # 1:30 PM ET
INDEX_LATE_DAY_SCORE_FLOOR   = float(os.getenv("INDEX_LATE_DAY_SCORE_FLOOR", "75"))
_INDEX_TO_ETF = {"^GSPC": "SPY", "^NDX": "QQQ", "^RUT": "IWM", "^DJI": "DIA"}


def _score_allows_entry(
    *,
    score: float,
    hard_floor: float,
    is_0dte: bool,
    is_index: bool,
    timeframe: str,
    spread_pct: float | None,
    delta: float | None,
    dte_known: bool = True,
) -> tuple[bool, str]:
    """Structured score eligibility — refinement of the blunt < hard_floor reject.

    Returns (allowed, reason_code).

    Decision order:
      1. score >= hard_floor (70)         -> ALLOWED (normal path; caller's
                                              downstream 0DTE/quote/watcher gates
                                              still apply). reason_code="".
      2. score < SCORE65_FLOOR (65)       -> REJECTED_LOW_SCORE_UNDER_65
      3. score in [65, hard_floor):
           - feature off (SCORE65_ALLOW)  -> REJECTED_LOW_SCORE (unchanged behavior)
           - is_0dte                      -> REJECTED_SCORE65_0DTE
           - spread present & too wide     -> REJECTED_SCORE65_WIDE_SPREAD
           - delta present & too weak      -> REJECTED_SCORE65_WEAK_CONTRACT
           - otherwise                     -> ALLOWED_SCORE65_NON_0DTE_*
                                              (DAILY if timeframe daily/overnight,
                                               else CLEAN for non-0DTE intraday)

    NOTE: quote_ok / submit_ask are NOT available at this point in the
    pipeline (they're resolved downstream in contract_selector.select() and at
    submit). The submit-time quote-refresh gate (QUOTE_REFRESH_FAILED_AT_SUBMIT,
    PR-H on main) already fails closed if the quote can't be refreshed, so a
    score-65 setup that passes here still cannot submit on a stale/failed quote.
    We deliberately do NOT duplicate that check here against data we don't have.
    """
    # 1. Normal path — at or above the hard floor.
    if score >= hard_floor:
        return True, ""

    # 2. Hard floor for the exception band itself.
    if score < SCORE65_FLOOR:
        return False, "REJECTED_LOW_SCORE_UNDER_65"

    # 3. score in [SCORE65_FLOOR, hard_floor)
    if not SCORE65_ALLOW:
        # Feature disabled — preserve existing blunt behavior exactly.
        return False, "REJECTED_LOW_SCORE"

    # UNKNOWN-DTE GUARD: the entire score-65 exception depends on PROVING the
    # setup is non-0DTE. A missing or unparseable DTE must NEVER silently
    # become non-0DTE — that would let unknown-expiry setups slip through.
    # The caller tracks _dte_known separately from _is_0dte_for_gate:
    #   True  → DTE parsed cleanly or expiration date matched YYYY-MM-DD
    #   False → DTE absent or unparseable
    # We reject unknown before the 0DTE check so ordering is safe.
    if not dte_known:
        return False, "REJECTED_SCORE65_UNKNOWN_DTE"

    # 0DTE at score 65 is always rejected (index or single-name).
    if is_0dte:
        return False, "REJECTED_SCORE65_0DTE"

    # Spread gate — only when spread data is present and meaningful (>0).
    if spread_pct is not None and spread_pct > 0 and spread_pct > SCORE65_MAX_SPREAD_PCT:
        return False, "REJECTED_SCORE65_WIDE_SPREAD"

    # Delta gate — only when delta data is present and meaningful (>0).
    # abs() so PUT deltas (often negative) compare on magnitude.
    if delta is not None and delta != 0 and abs(delta) < SCORE65_MIN_DELTA:
        return False, "REJECTED_SCORE65_WEAK_CONTRACT"

    # Clean non-0DTE score-65 — admit. Label by timeframe for the audit trail.
    tf = (timeframe or "").lower()
    if tf in ("1d", "1day", "daily", "overnight", "1wk", "1week", "weekly"):
        return True, "ALLOWED_SCORE65_NON_0DTE_DAILY"
    return True, "ALLOWED_SCORE65_NON_0DTE_CLEAN"

# Canonical active ENTRY order states that reserve capital / represent pending exposure.
# Keep this aligned with APOrderStateMachine.PENDING_ENTRY_STATUSES and
# APPositionManager._PENDING_ENTRY_STATUSES. Master Control prefers
# position_manager.snapshot()["filled_unreconciled_entry_capital"] and falls
# back to snapshot()["pending_entry_capital"] only for legacy compatibility.
# The broker-proof submitted-order path below must still use the same canonical
# lifecycle so fallback risk math cannot drift.
try:
    from ap.order_state_machine import PENDING_ENTRY_STATUSES as _OSM_PENDING_ENTRY_STATUSES
except Exception:
    _OSM_PENDING_ENTRY_STATUSES = None

# PR E / FIX-5: hardcoded fallback default. Must NEVER produce an empty
# tuple, otherwise pending-capital SUM silently returns zero and the
# hard capital gate is bypassed. The fallback is the canonical pre-fill
# entry lifecycle aligned with APOrderStateMachine.PENDING_ENTRY_STATUSES
# and APPositionManager._PENDING_ENTRY_STATUSES.
_DEFAULT_ENTRY_CAPITAL_RESERVED_STATUSES = (
    "CREATED",
    "PENDING_TRIGGER",
    "SUBMITTED",
    "ACKNOWLEDGED",
    "PARTIAL_FILL",
)

_ENTRY_CAPITAL_RESERVED_STATUSES = tuple(
    str(s).upper().strip()
    for s in (_OSM_PENDING_ENTRY_STATUSES or _DEFAULT_ENTRY_CAPITAL_RESERVED_STATUSES)
)

# PR E / FIX-5: a defensive belt-and-suspenders check. If for any reason
# the resolved tuple is empty (import returned an empty iterable, or a
# future refactor breaks the fallback chain), log CRITICAL and force the
# hardcoded default. Operators must see this in Render logs immediately
# — an empty status tuple silently disables the capital gate.
if not _ENTRY_CAPITAL_RESERVED_STATUSES:
    log.critical(
        "_ENTRY_CAPITAL_RESERVED_STATUSES resolved to EMPTY tuple — "
        "PENDING_ENTRY_STATUSES import broken (got=%r). Forcing "
        "hardcoded defaults to keep the capital gate functional. "
        "Fix the OSM import path or canonical status list.",
        _OSM_PENDING_ENTRY_STATUSES,
    )
    _ENTRY_CAPITAL_RESERVED_STATUSES = _DEFAULT_ENTRY_CAPITAL_RESERVED_STATUSES


def _estimate_premium(ticker: str) -> float:
    return _PREMIUM_ESTIMATES.get(str(ticker).upper().strip(), _DEFAULT_PREMIUM_FALLBACK)


DEFAULT_PREMIUM_ESTIMATE = _DEFAULT_PREMIUM_FALLBACK


def _env_true(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() not in {"0", "false", "no", "off", "disabled"}


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


# PR #229: Canonical signal-side normalizer for admission (APMasterControl.evaluate).
#
# This is the single source of truth for translating a raw signal `side` /
# `direction` field into the bot's internal {"CALL", "PUT"} contract. It exists
# at module scope so:
#   1. It can be unit-tested in isolation without spinning up APMasterControl.
#   2. Future consolidation can converge the four divergent `_normalize_side()`
#      implementations elsewhere in the codebase onto this function rather than
#      adding a fifth.
#
# Semantics: fail-CLOSED. Returns None for any value that is missing, empty,
# or not in the recognized alias set. Call sites MUST treat None as a block
# condition (no silent default to CALL — that was the pre-#229 bug).
_SIDE_ALIASES: dict[str, str] = {
    "CALL": "CALL",
    "BUY": "CALL",
    "LONG": "CALL",
    "CALLS": "CALL",
    "BULLISH": "CALL",
    "PUT": "PUT",
    "SELL": "PUT",
    "SHORT": "PUT",
    "PUTS": "PUT",
    "BEARISH": "PUT",
}


def _normalize_signal_side(raw: Any) -> Optional[str]:
    """Normalize a raw signal side/direction to 'CALL' or 'PUT'.

    Returns None if `raw` is missing, empty, or not a recognized alias.
    Callers must fail closed on None — never substitute a default direction.
    """
    if raw is None:
        return None
    side = str(raw).strip().upper()
    if not side:
        return None
    return _SIDE_ALIASES.get(side)


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
    # PR E / FIX-2: uppercase default. The rest of the system normalizes
    # mode to uppercase ("LIVE" / "PAPER"); the construction site below
    # in evaluate() also assigns uppercase.
    mode: str = "PAPER"
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
        daily_profit_target_usd: float = 0.0,
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
        # PR #155 — split per-position cap from total exposure cap.
        # max_position_pct: per-trade budget as fraction of equity.
        #   If not supplied, falls back to max_capital_pct for backward compat.
        # max_total_capital_pct: total portfolio exposure cap as fraction of equity.
        #   If not supplied, uses DEFAULT_MAX_TOTAL_CAPITAL_PCT env/default.
        #   Must not be lower than max_position_pct (enforced at init).
        #   Never silently made unlimited — minimum floor enforced.
        max_position_pct: float | None = None,
        max_total_capital_pct: float | None = None,
    ):
        self.mode = mode.upper()
        self.paper = self.mode != "LIVE"
        self.score_floor = score_floor
        self.context_floor = context_floor
        self.max_positions = max_positions
        self.max_capital_pct = max_capital_pct  # preserved for backward compat
        self.max_sector_pct = max_sector_pct
        self.max_ticker_pct = max_ticker_pct
        self.max_calls = max_calls
        self.max_puts = max_puts
        self.max_trades_today = max_trades_today
        self.max_daily_loss = max_daily_loss

        # PR #155 — split per-position cap from total portfolio exposure cap.
        # Backward compatibility:
        #   max_position_pct  → falls back to max_capital_pct if not set
        #   max_total_capital_pct → falls back to DEFAULT_MAX_TOTAL_CAPITAL_PCT
        #                           env/hardcoded, never left unlimited
        _DEFAULT_MAX_POSITION_PCT = float(os.getenv("DEFAULT_MAX_POSITION_PCT", "0.10"))
        _DEFAULT_MAX_TOTAL_CAPITAL_PCT = float(os.getenv("DEFAULT_MAX_TOTAL_CAPITAL_PCT", "0.40"))

        # Per-trade budget cap
        if max_position_pct is not None:
            self.max_position_pct = float(max_position_pct)
        else:
            # Legacy path: treat max_capital_pct as both caps, but apply the
            # known per-trade default when max_capital_pct is its own default.
            # If the caller explicitly set max_capital_pct to something < default
            # (e.g. 0.10 for Jason), honour it as the per-position cap.
            self.max_position_pct = float(max_capital_pct)

        # Total portfolio exposure cap — never unlimited
        if max_total_capital_pct is not None:
            self.max_total_capital_pct = float(max_total_capital_pct)
        else:
            self.max_total_capital_pct = _DEFAULT_MAX_TOTAL_CAPITAL_PCT

        # Safety: total cap must be >= per-position cap (otherwise every trade blocks)
        if self.max_total_capital_pct < self.max_position_pct:
            log.warning(
                "[%s] APMasterControl: max_total_capital_pct=%.2f < max_position_pct=%.2f "
                "— clamping total cap to per-position cap to avoid permanent block",
                client_id, self.max_total_capital_pct, self.max_position_pct,
            )
            self.max_total_capital_pct = self.max_position_pct
        # H8: once realized P&L for the session reaches this dollar target,
        # block NEW entries for the rest of the day so the bot does not give
        # back a green day chasing more trades. 0.0 = disabled (unlimited).
        # Exits are unaffected — master_control only gates entries.
        self.daily_profit_target_usd = float(daily_profit_target_usd or 0.0)
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
        self._counterfactual_ctx = threading.local()

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
        self.admission_thresholds = resolve_admission_thresholds(
            mc_score_floor=self.score_floor,
            mc_priority_floor=_PRIORITY_FLOOR,
            context_floor=self.context_floor,
        )
        self.admission_threshold_config_hash = self.admission_thresholds.config_hash
        log_admission_thresholds(
            log,
            component=f"APMasterControl:{self.mode}",
            resolved=self.admission_thresholds,
        )

        self._kill_switch_fn = None
        # H5: per-client entries pause. Distinct from kill switch:
        #   kill switch  = full READ_ONLY (no entries, protective mode)
        #   entries pause = THIS client's new entries off, exits keep running,
        #                   other clients unaffected. Operator-toggled from the
        #                   admin dashboard; read fresh each evaluate() so it
        #                   takes effect with no bot restart.
        self._entries_paused_fn = None
        self._mode_fn = None
        self._seen_signals: dict[str, float] = {}  # key -> inserted_ts, expires after 1800s
        self._trade_dossier_signal_cache: dict[str, dict[str, Any]] = {}
        self._trade_dossier_signal_cache_ts: dict[str, float] = {}
        self._trade_dossier_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=TRADE_DOSSIER_QUEUE_MAX)
        self._trade_dossier_worker: Optional[threading.Thread] = None
        self._trade_dossier_worker_lock = threading.Lock()
        self._trade_dossier_db_healthy: Optional[bool] = None
        self._trade_dossier_db_last_ok_ts: Optional[float] = None
        self._trade_dossier_db_last_err_ts: Optional[float] = None
        self._trade_cooldowns: dict[str, float] = {}
        # PR E / FIX-3: protect concurrent equity / max_daily_loss reads &
        # writes. set_account_equity() updates both fields from the
        # equity-sync thread while evaluate() reads them from the worker
        # thread — a non-atomic two-field update without this lock.
        self._equity_lock = threading.Lock()
        # PR E / FIX-4: protect concurrent _trade_cooldowns access.
        # ap_execution_core.py writes to master_control._trade_cooldowns
        # from the exit-callback thread (line ~1164) while evaluate()
        # reads it from the worker thread. The dict field is preserved
        # for backward compat with execution-core; this lock just
        # serializes the read in evaluate() and the clear in
        # reset_session(). New writers should call set_cooldown()
        # below, which acquires the lock.
        self._cooldown_lock = threading.Lock()
        # P0-3: Daily-loss force-close state.
        # When daily loss limit is hit, the entry gate blocks new trades AND
        # this flag is set. The exit engine reads it on each tick and force-
        # closes every open position via its existing authority (which uses
        # the P0-1 retry/idempotency path). One source of truth here; close
        # authority stays with the exit engine.
        #   None                       → not requested
        #   (ts: float, reason: str)   → requested at ts for reason
        self._force_close_all_state: tuple[float, str] | None = None
        # Guard so we only fire the request once per session even though the
        # daily-loss check runs on every entry signal.
        self._daily_loss_force_close_fired: bool = False
        self._start_trade_dossier_worker()
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

        # PR #224 amendment: surface FINAL_QUALITY_MODE disabled at startup so
        # an operator sees immediately why no trades are flowing, instead of
        # discovering it only via a stream of FINAL_QUALITY_MODE_DISABLED
        # rejections in the decision log.
        self.final_quality_mode_enabled = _env_true("FINAL_QUALITY_MODE_ENABLED", True)
        if not self.final_quality_mode_enabled:
            log.warning(
                "FINAL_QUALITY_MODE_DISABLED_BLOCKS_ALL_ENTRIES=true | mode=%s "
                "client=%s — every entry signal will be REJECTED with "
                "FINAL_QUALITY_MODE_DISABLED until FINAL_QUALITY_MODE_ENABLED "
                "is re-enabled.",
                self.mode, getattr(self, "_client_id", "default"),
            )

    def wire(self, *, kill_switch_fn=None, mode_fn=None, position_count_fn=None, alert_fn=None, entries_paused_fn=None, **kwargs):
        if kill_switch_fn:
            self._kill_switch_fn = kill_switch_fn
        if entries_paused_fn:
            self._entries_paused_fn = entries_paused_fn
        if mode_fn:
            self._mode_fn = mode_fn
        if alert_fn:
            self._alert_fn = alert_fn

    def _has_durable_duplicate_signal(
        self,
        client_id: str,
        signal_id: str,
        current_queue_id: Optional[int] = None,
    ) -> tuple[bool, str, str]:
        """Return (is_duplicate, source, detail) checking durable DB state.

        Returns (True, source, detail) when same client_id+signal_id has any
        of the following provably-active rows in Postgres:
          - trade_queue row in active status (NEW/PROCESSING/WATCHING/
            PENDING_TRIGGER/DEFERRED) excluding current_queue_id
          - orders row with kind='ENTRY' in non-terminal status
            (CREATED/PENDING_TRIGGER/SUBMITTED/ACCEPTED/ACKNOWLEDGED/OPEN/
             PARTIAL_FILL/PARTIALLY_FILLED/FILLED)
          - open positions row (if signal_id linkage exists)

        Returns (False, "", "") when only matching rows are:
          - the current row being re-evaluated (excluded by current_queue_id)
          - terminal failures (REJECTED, ERROR, EXPIRED, CANCELED)
          - prior duplicate-rejection rows (last_error containing
            'duplicate_signal_id')

        Returns (False, "check_unavailable", reason) when the DB read itself
        fails. Caller decides whether LIVE should fail-closed via separate
        duplicate_check_unavailable_live_blocked code path.

        This is the durable replacement for the volatile self._seen_signals
        check, which retains in-memory state across runs that may not match
        the durable DB truth (e.g. after restart, after retry, after recovery).
        """
        try:
            from ap.db import conn, run_with_retry
        except Exception as _imp_exc:
            return (False, "check_unavailable", f"db_import_failed:{_imp_exc}")

        def _check():
            with conn() as c:
                # 1) Active trade_queue row for same client+signal,
                #    excluding the current row being evaluated.
                if current_queue_id is not None:
                    c.execute(
                        """
                        SELECT id, status, last_error
                        FROM public.trade_queue
                        WHERE client_id = %s
                          AND signal_id = %s
                          AND id <> %s
                          AND UPPER(COALESCE(status, '')) IN
                              ('NEW','PROCESSING','WATCHING','PENDING_TRIGGER','DEFERRED')
                        LIMIT 1
                        """,
                        (client_id, signal_id, current_queue_id),
                    )
                else:
                    c.execute(
                        """
                        SELECT id, status, last_error
                        FROM public.trade_queue
                        WHERE client_id = %s
                          AND signal_id = %s
                          AND UPPER(COALESCE(status, '')) IN
                              ('NEW','PROCESSING','WATCHING','PENDING_TRIGGER','DEFERRED')
                        LIMIT 1
                        """,
                        (client_id, signal_id),
                    )
                row = c.fetchone()
                if row:
                    return ("trade_queue",
                            f"id={row['id']} status={row['status']}")

                # 2) Non-terminal ENTRY order for same client+signal.
                c.execute(
                    """
                    SELECT id, status
                    FROM public.orders
                    WHERE client_id = %s
                      AND signal_id = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status, '')) IN
                          ('CREATED','PENDING_TRIGGER','SUBMITTED','ACCEPTED',
                           'ACKNOWLEDGED','OPEN','PARTIAL_FILL',
                           'PARTIALLY_FILLED','FILLED')
                    LIMIT 1
                    """,
                    (client_id, signal_id),
                )
                row = c.fetchone()
                if row:
                    return ("orders",
                            f"id={row['id']} status={row['status']}")

                # 3) Open position linked to same signal_id, if linkage exists.
                try:
                    c.execute(
                        """
                        SELECT id, status
                        FROM public.positions
                        WHERE client_id = %s
                          AND signal_id = %s
                          AND UPPER(COALESCE(status, '')) IN ('OPEN','CLOSING','PARTIAL')
                        LIMIT 1
                        """,
                        (client_id, signal_id),
                    )
                    row = c.fetchone()
                    if row:
                        return ("positions",
                                f"id={row['id']} status={row['status']}")
                except Exception:
                    # signal_id column may not exist on positions in some
                    # schema versions — silently skip rather than false-block.
                    pass

                return ("", "")

        try:
            source, detail = run_with_retry(_check)
        except Exception as _q_exc:
            return (False, "check_unavailable", f"db_query_failed:{_q_exc}")

        if source:
            return (True, source, detail)
        return (False, "", "")

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
        # PR E / FIX-3: hold _equity_lock for the compound two-field update.
        # Without it a worker thread reading max_daily_loss between the
        # account_equity assignment and the max_daily_loss assignment sees
        # an inconsistent snapshot (old equity, scaled-to-new-equity loss).
        with self._equity_lock:
            old = self.account_equity
            self.account_equity = float(equity)
            # Recompute max_daily_loss proportionally to new equity.
            # max_capital/sector/ticker pct gates recompute inline — no action needed.
            # max_daily_loss is a fixed dollar set at startup; scale it with equity.
            if self._startup_equity and self._startup_equity > 0:
                loss_pct = abs(self.max_daily_loss / self._startup_equity)
                self.max_daily_loss = -abs(self.account_equity * loss_pct)
            _new_equity = self.account_equity
            _new_loss = self.max_daily_loss
        label = f"[{client_id}] " if client_id else ""
        if abs(old - _new_equity) > 1:
            log.info(
                "%sAccount equity updated: $%.0f -> $%.0f | max_capital=$%.0f max_sector=$%.0f max_daily_loss=$%.0f",
                label,
                old,
                _new_equity,
                _new_equity * self.max_capital_pct,
                _new_equity * self.max_sector_pct,
                _new_loss,
            )

    def _equity_snapshot(self) -> tuple[float, float]:
        """PR E FIX-3 (reader-side patch): atomic read of the
        (account_equity, max_daily_loss) pair under _equity_lock.

        set_account_equity() updates these two fields sequentially under
        the same lock. Without locking the READ as well, a worker thread
        can observe a half-updated pair (new equity, old loss) between
        the two write assignments — a classic reader/writer race that a
        write-only lock does NOT prevent.

        Callers (evaluate, revalidate_exposure) must take ONE snapshot
        at the top of the risk-check section, then use local variables
        for all subsequent capital / daily-loss math. NEVER hold this
        lock across DB / broker / PM / intelligence calls — the snapshot
        is intended to be sub-microsecond.

        Returns:
            (account_equity, max_daily_loss) — both as float, consistent
            with the most recent completed set_account_equity() update.
        """
        with self._equity_lock:
            return float(self.account_equity), float(self.max_daily_loss)

    def set_cooldown(self, key: str, ts: float | None = None, reason: str = "") -> None:
        """PR E / FIX-4: public, thread-safe setter for trade cooldowns.

        Replaces the previous pattern of mutating master_control.
        _trade_cooldowns directly from external threads. Acquires
        self._cooldown_lock for the write, so concurrent reads in
        evaluate() are guaranteed to see a fully-written entry.

        Args:
            key:    cooldown key, e.g. f"{ticker.upper()}:{direction}:cooldown".
            ts:     timestamp (epoch seconds). Defaults to time.time().
            reason: optional human-readable reason (logged at info level).

        Backward compatibility: the existing ap_execution_core code path
        that does `self.master_control._trade_cooldowns[key] = time.time()`
        is left untouched in this PR (per scope) and still works because
        the dict object is preserved.
        """
        if not key:
            return
        _ts = float(ts) if ts is not None else time.time()
        with self._cooldown_lock:
            self._trade_cooldowns[str(key)] = _ts
        if reason:
            log.info("cooldown set: %s @ %.0f reason=%s", key, _ts, reason)

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

    def _pending_orders_capital(
        self,
        client_id: str,
        *,
        exclude_local_order_id: Optional[str] = None,
        runtime_execution_mode: Optional[str] = None,
        with_diagnostics: bool = False,
    ) -> Optional[float] | dict:
        """
        Sum capital reserved by entry orders that may still become exposure.

        LIVE policy: if this query fails, callers can fail closed because pending
        capital is part of the hard capital gate. The status list is intentionally
        canonical and aligned with OSM/PositionManager so fallback risk math cannot
        drift from the snapshot truth path.

        P0 FIX (2026-05-21): exclude phantom orders. A CREATED/PENDING_TRIGGER
        order older than PENDING_ENTRY_PHANTOM_GRACE_SEC (default 30s) with no
        broker_order_id never reached the broker and is not real exposure. Without
        this exclusion, a single stale CREATED row from a prior session can pin
        projected_total above max_capital forever — observed in production
        2026-05-21 blocking 113 consecutive signals for tradefluencehq.

        PR E / FIX-1 (BUG-MC-1): support exclude_local_order_id so
        revalidate_exposure can subtract the current plan's reserved-cost
        row from the SUM. process_signal inserts the OSM row (with
        reserved_cost) BEFORE calling revalidate_exposure; without
        exclusion the plan's cost is double-counted (once via this SUM,
        once via revalidate_exposure's `+ real_cost`).
        """
        try:
            from ap.db import conn, run_with_retry
            import os as _os

            _phantom_grace_sec = int(_os.getenv("PENDING_ENTRY_PHANTOM_GRACE_SEC", "30"))

            # PR E / FIX-1: optional exclusion of one specific local_order_id
            # (the current plan's row). Built as an explicit `AND local_order_id != %s`
            # clause so the SQL stays a pure SUM with no Python-side post-filter.
            _exclude_clause = ""
            _exclude_params: tuple = ()
            if exclude_local_order_id:
                _exclude_clause = " AND local_order_id != %s"
                _exclude_params = (str(exclude_local_order_id),)

            # Resolve the runtime execution_mode for THIS client. Required so
            # historical mode='paper' rows do not count when client is now
            # running live (and vice versa). NULL mode rows are NEVER counted
            # — they predate the execution_mode column and are not provable.
            _rmode = (runtime_execution_mode or '').strip().lower() or None

            def _fn():
                with conn() as c:
                    # COUNTED rows: broker-proof submitted entries for this
                    # client AND runtime execution_mode.
                    c.execute(
                        """
                        SELECT
                            local_order_id,
                            broker_order_id,
                            UPPER(COALESCE(status, '')) AS status_uc,
                            COALESCE(execution_mode, '') AS execution_mode,
                            COALESCE(
                                NULLIF(reserved_cost, 0),
                                CASE
                                    WHEN COALESCE(limit_price, 0) > 0
                                         AND COALESCE(qty, 0) > 0
                                    THEN limit_price * qty * 100
                                    ELSE 0
                                END
                            ) AS row_cost
                        FROM orders
                        WHERE client_id = %s
                          AND kind = 'ENTRY'
                          AND UPPER(COALESCE(status, '')) IN (
                              'SUBMITTED', 'ACCEPTED', 'OPEN',
                              'PARTIALLY_FILLED', 'PARTIAL_FILL',
                              'PENDING_SUBMIT', 'ACKNOWLEDGED'
                          )
                          AND (
                            (broker_order_id IS NOT NULL AND broker_order_id <> '')
                            OR submitted_ts IS NOT NULL
                          )
                          AND COALESCE(filled_qty, 0) = 0
                          AND fill_price IS NULL
                          AND UPPER(COALESCE(contract, '')) NOT LIKE 'DEFERRED:%%'
                          AND COALESCE(execution_mode, '') = %s
                          AND COALESCE(last_error, '') NOT ILIKE %s
                          AND COALESCE(last_error, '') NOT ILIKE %s
                        """ + _exclude_clause,
                        (
                            client_id,
                            _rmode or '',
                            '%watcher_invalidated%',
                            '%STARTUP_CLEANUP_CANCELED_STALE_ORPHAN%',
                        ) + _exclude_params,
                    )
                    counted_rows = c.fetchall() or []
                    counted_total = sum(float(r['row_cost'] or 0) for r in counted_rows)

                    if not with_diagnostics:
                        return counted_total

                    # Build IGNORED diagnostic — every reserved_cost row that
                    # was NOT counted, grouped by reject bucket. Surfaced into
                    # capital_limit_no_remaining result_json so operators can
                    # see why $1551 of canceled reservations did not block.
                    c.execute(
                        """
                        SELECT
                            local_order_id,
                            broker_order_id,
                            UPPER(COALESCE(status, '')) AS status_uc,
                            COALESCE(execution_mode, '__null__') AS execution_mode,
                            COALESCE(last_error, '') AS last_error,
                            COALESCE(reserved_cost, 0) AS reserved_cost,
                            submitted_ts
                        FROM orders
                        WHERE client_id = %s
                          AND kind = 'ENTRY'
                          AND COALESCE(reserved_cost, 0) > 0
                          AND local_order_id NOT IN (
                              SELECT local_order_id FROM orders
                              WHERE client_id = %s
                                AND kind = 'ENTRY'
                                AND UPPER(COALESCE(status, '')) IN (
                                    'SUBMITTED', 'ACCEPTED', 'OPEN',
                                    'PARTIALLY_FILLED', 'PARTIAL_FILL',
                                    'PENDING_SUBMIT', 'ACKNOWLEDGED'
                                )
                                AND (
                                  (broker_order_id IS NOT NULL AND broker_order_id <> '')
                                  OR submitted_ts IS NOT NULL
                                )
                                AND COALESCE(filled_qty, 0) = 0
                                AND fill_price IS NULL
                                AND COALESCE(execution_mode, '') = %s
                          )
                        """,
                        (
                            client_id,
                            client_id,
                            _rmode or '',
                        ),
                    )
                    ignored_rows = c.fetchall() or []

                    # Group ignored rows by reject reason for diagnostics.
                    ignored_reserved_cost_by_status: dict = {}
                    ignored_order_ids_by_status:     dict = {}
                    for r in ignored_rows:
                        status_uc = r['status_uc'] or 'UNKNOWN'
                        emode     = r['execution_mode']
                        last_err  = (r['last_error'] or '').lower()
                        broker_id = r['broker_order_id']
                        submitted = r['submitted_ts']

                        if 'watcher_invalidated' in last_err:
                            bucket = 'watcher_invalidated'
                        elif 'startup_cleanup_canceled_stale_orphan' in last_err.lower():
                            bucket = 'startup_cleanup_canceled_stale_orphan'
                        elif emode == '__null__' or emode == '':
                            bucket = 'execution_mode_null_or_missing'
                        elif _rmode and emode != _rmode:
                            bucket = f'execution_mode_mismatch:{emode}'
                        elif (not broker_id) and (submitted is None):
                            bucket = 'never_submitted_no_broker_id_no_submit_ts'
                        elif status_uc in ('CANCELED', 'CANCELLED'):
                            bucket = 'canceled'
                        elif status_uc in ('REJECTED', 'ERROR', 'FAILED'):
                            bucket = f'terminal:{status_uc}'
                        elif status_uc == 'EXPIRED':
                            bucket = 'expired'
                        elif status_uc in ('CREATED', 'PENDING_TRIGGER'):
                            bucket = f'local_only:{status_uc}'
                        else:
                            bucket = f'other:{status_uc}'

                        ignored_reserved_cost_by_status[bucket] = (
                            ignored_reserved_cost_by_status.get(bucket, 0.0)
                            + float(r['reserved_cost'] or 0)
                        )
                        ignored_order_ids_by_status.setdefault(bucket, []).append(
                            r['local_order_id']
                        )

                    return {
                        'pending_submitted_entry_exposure': counted_total,
                        'counted_order_ids':                [r['local_order_id'] for r in counted_rows],
                        'counted_order_statuses':           [r['status_uc']      for r in counted_rows],
                        'counted_broker_order_ids':         [r['broker_order_id'] for r in counted_rows],
                        'ignored_reserved_cost_by_status':  ignored_reserved_cost_by_status,
                        'ignored_order_ids_by_status':      ignored_order_ids_by_status,
                        'runtime_execution_mode':           _rmode,
                        'active_broker_statuses': [
                            'SUBMITTED', 'ACCEPTED', 'OPEN',
                            'PARTIALLY_FILLED', 'PARTIAL_FILL',
                            'PENDING_SUBMIT', 'ACKNOWLEDGED',
                        ],
                    }

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

    def _get_pending_capital_breakdown(
        self,
        snap: dict[str, Any],
        client_id: str,
        *,
        exclude_local_order_id: Optional[str] = None,
        runtime_execution_mode: Optional[str] = None,
    ) -> Optional[dict]:
        """
        Return a dict with independent pending exposure components.

        Keys:
          pending_submitted_entry_exposure  — broker-proof unfilled submitted orders
          filled_unreconciled_entry_capital — fill-truth unreconciled fills (snap)
          pending_total_capital_reserved    — sum of both (use for cap math)

        Returns None on LIVE fail-closed when DB is unavailable.
        Never derives submitted pending by subtracting from total.
        Each component comes directly from its authoritative source.
        """
        # Component 1: broker-proof submitted-but-unfilled orders.
        # PR #121: request the diagnostic dict so we can surface counted/
        # ignored bucket detail when blocking with capital_limit_no_remaining.
        #
        # PR #121 amend: if caller did not pass runtime_execution_mode, fall
        # back to self._current_mode().lower(). This protects every call site
        # — including legacy ones not yet updated — from under-counting real
        # broker-submitted pending exposure due to a NULL/empty mode filter.
        _resolved_runtime_mode = runtime_execution_mode
        if not _resolved_runtime_mode:
            try:
                _resolved_runtime_mode = str(self._current_mode() or '').lower() or None
            except Exception:
                _resolved_runtime_mode = None

        _diag = self._pending_orders_capital(
            client_id,
            exclude_local_order_id=exclude_local_order_id,
            runtime_execution_mode=_resolved_runtime_mode,
            with_diagnostics=True,
        )
        if _diag is None:
            if self._is_live_mode() and self.pending_capital_fail_closed_live:
                return None
            _diag = {
                "pending_submitted_entry_exposure": 0.0,
                "counted_order_ids":                [],
                "counted_order_statuses":           [],
                "counted_broker_order_ids":         [],
                "ignored_reserved_cost_by_status":  {},
                "ignored_order_ids_by_status":      {},
                "runtime_execution_mode":           (runtime_execution_mode or "").lower() or None,
                "active_broker_statuses":           [],
            }
        elif isinstance(_diag, (int, float)):
            # Old call site path (no diagnostics requested) — wrap.
            _diag = {"pending_submitted_entry_exposure": float(_diag)}
        pending_submitted = float(_diag.get("pending_submitted_entry_exposure") or 0.0)

        # Component 2: fill-truth unreconciled fills (position_manager snapshot)
        filled_unreconciled = 0.0
        _filled_key = None
        _filled_value = None
        if isinstance(snap, dict):
            if snap.get("filled_unreconciled_entry_capital") is not None:
                _filled_key = "filled_unreconciled_entry_capital"
                _filled_value = snap.get("filled_unreconciled_entry_capital")
            elif snap.get("pending_entry_capital") is not None:
                _filled_key = "pending_entry_capital"
                _filled_value = snap.get("pending_entry_capital")
        if _filled_key is not None:
            try:
                filled_unreconciled = float(_filled_value or 0.0)
            except Exception as e:
                self._alert_degraded(
                    "SNAPSHOT_FILLED_UNRECONCILED_ENTRY_CAPITAL_INVALID",
                    severity="CRITICAL" if self._is_live_mode() else "WARNING",
                    client_id=client_id,
                    details={"field": _filled_key, "value": repr(_filled_value), "error": str(e)},
                )
                if self._is_live_mode() and self.pending_capital_fail_closed_live:
                    return None

        # Merge: keep all diagnostic fields so callers can surface them.
        return {
            "pending_submitted_entry_exposure": pending_submitted,
            "filled_unreconciled_entry_capital": filled_unreconciled,
            "filled_unreconciled_exposure":      filled_unreconciled,
            "pending_total_capital_reserved":   pending_submitted + filled_unreconciled,
            "counted_order_ids":                _diag.get("counted_order_ids", []),
            "counted_order_statuses":           _diag.get("counted_order_statuses", []),
            "counted_broker_order_ids":         _diag.get("counted_broker_order_ids", []),
            "ignored_reserved_cost_by_status":  _diag.get("ignored_reserved_cost_by_status", {}),
            "ignored_order_ids_by_status":      _diag.get("ignored_order_ids_by_status", {}),
            "ignored_already_reconciled_fill_capital": float(
                (snap or {}).get("ignored_already_reconciled_fill_capital") or 0.0
            ),
            "ignored_already_reconciled_order_ids": list(
                (snap or {}).get("ignored_already_reconciled_order_ids") or []
            ),
            "ignored_reconciled_match_keys": list(
                (snap or {}).get("ignored_reconciled_match_keys") or []
            ),
            "capital_deployed": float((snap or {}).get("capital_deployed") or 0.0),
            "position_capital_deployed": float(
                (snap or {}).get("position_capital_deployed")
                or (snap or {}).get("capital_deployed")
                or 0.0
            ),
            "open_position_ids": list((snap or {}).get("open_position_ids") or []),
            "runtime_execution_mode":           _diag.get("runtime_execution_mode"),
            "active_broker_statuses":           _diag.get("active_broker_statuses", []),
        }

    def _log_capital_breakdown_proof(
        self,
        *,
        ticker: str,
        breakdown: dict[str, Any],
    ) -> None:
        if not isinstance(breakdown, dict):
            return
        log.info(
            "[%s] CAPITAL_BREAKDOWN_PROOF "
            "capital_deployed=%.0f position_capital_deployed=%.0f "
            "pending_submitted_entry_exposure=%.0f filled_unreconciled_entry_capital=%.0f "
            "ignored_already_reconciled_fill_capital=%.0f "
            "counted_order_ids=%s ignored_order_ids_by_status=%s "
            "ignored_already_reconciled_order_ids=%s ignored_reconciled_match_keys=%s "
            "open_position_ids=%s runtime_execution_mode=%s",
            ticker,
            float(breakdown.get("capital_deployed", 0.0) or 0.0),
            float(breakdown.get("position_capital_deployed", 0.0) or 0.0),
            float(breakdown.get("pending_submitted_entry_exposure", 0.0) or 0.0),
            float(breakdown.get("filled_unreconciled_entry_capital", 0.0) or 0.0),
            float(breakdown.get("ignored_already_reconciled_fill_capital", 0.0) or 0.0),
            breakdown.get("counted_order_ids", []),
            breakdown.get("ignored_order_ids_by_status", {}),
            breakdown.get("ignored_already_reconciled_order_ids", []),
            breakdown.get("ignored_reconciled_match_keys", []),
            breakdown.get("open_position_ids", []),
            breakdown.get("runtime_execution_mode"),
        )

    def _pending_capital_from_snapshot_or_db(
        self,
        snap: dict[str, Any],
        client_id: str,
        *,
        exclude_local_order_id: Optional[str] = None,
        runtime_execution_mode: Optional[str] = None,
    ) -> Optional[float]:
        """Return total pending ENTRY dollar exposure (for capital-gate math).

        Thin wrapper over _get_pending_capital_breakdown that preserves the
        float-returning contract expected by all existing callers.

        PR #121 amend: runtime_execution_mode is forwarded to the breakdown
        so the underlying SQL filters orders.execution_mode = runtime. When
        omitted, the breakdown helper defaults runtime_execution_mode to
        self._current_mode().lower() so existing callers do not under-count
        broker-submitted pending orders for the active runtime.

        Returns None on LIVE fail-closed when DB is unavailable.
        """
        bd = self._get_pending_capital_breakdown(
            snap, client_id,
            exclude_local_order_id=exclude_local_order_id,
            runtime_execution_mode=runtime_execution_mode,
        )
        if bd is None:
            return None
        return bd["pending_total_capital_reserved"]

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

    # ── P0-3: Daily-loss force-close circuit breaker ─────────────────────────
    def check_daily_loss_breach(self, client_id: str = "") -> tuple[bool, dict]:
        """Read-only check: has daily realized P&L breached the loss limit?

        Returns (breached, snap). `breached=True` means realized_pnl_today is
        at or below max_daily_loss. The snap dict is the same shape used by
        the entry gate (with realized_pnl_today, open_positions, etc.).

        Pure read — does not mutate any state, does not trigger force-close.
        Use request_force_close_all() to actually flip the breaker.
        """
        try:
            snap = self._get_snapshot(client_id or self._client_id)
        except Exception as e:
            log.warning("daily_loss_check_snapshot_failed: %s", e)
            return False, {"realized_pnl_today": 0.0, "open_positions": []}
        pnl_today = float(snap.get("realized_pnl_today", 0.0))
        breached = pnl_today <= self.max_daily_loss
        return breached, snap

    def request_force_close_all(self, reason: str = "daily_loss_limit") -> bool:
        """Idempotently request that all open positions be closed.

        Sets `_force_close_all_state` to (timestamp, reason). The exit engine
        reads this each tick and force-closes any position not already exiting.

        Returns True if this call flipped the flag, False if it was already set
        (idempotent — multiple calls per session are safe and only the first
        triggers the alert).
        """
        if self._force_close_all_state is not None:
            log.debug(
                "force_close_all already requested at ts=%.0f reason=%s; ignoring new reason=%s",
                self._force_close_all_state[0], self._force_close_all_state[1], reason,
            )
            return False
        self._force_close_all_state = (time.time(), str(reason))
        log.critical(
            "FORCE_CLOSE_ALL REQUESTED | client=%s reason=%s | exit engine will "
            "close every open position on its next tick",
            self._client_id, reason,
        )
        # Best-effort alert
        try:
            if getattr(self, "_alert_fn", None):
                self._alert_fn(
                    f"[CRITICAL] FORCE_CLOSE_ALL triggered for {self._client_id} | "
                    f"reason={reason} | every open position will be closed"
                )
        except Exception as e:
            log.warning("force_close_all_alert_failed: %s", e)
        return True

    def is_force_close_requested(self) -> bool:
        """True when force-close-all has been requested for this session."""
        return self._force_close_all_state is not None

    def get_force_close_state(self) -> tuple[float, str] | None:
        """Returns (timestamp, reason) tuple if force-close requested, else None."""
        return self._force_close_all_state

    def clear_force_close(self, reason: str = "manual_clear") -> bool:
        """Clear the force-close flag. Used by reset_session and admin endpoints.

        Returns True if the flag was set and is now cleared, False if no-op.
        """
        if self._force_close_all_state is None:
            return False
        prev = self._force_close_all_state
        self._force_close_all_state = None
        self._daily_loss_force_close_fired = False
        log.warning(
            "force_close_all_cleared | client=%s previous_reason=%s clear_reason=%s",
            self._client_id, prev[1], reason,
        )
        return True

    def get_sector_exposure(self, positions: list) -> dict[str, float]:
        exposure: dict[str, float] = {}
        for pos in positions:
            ticker_in_pos = str(pos.get("underlying") or pos.get("ticker") or "")
            sector = self.SECTOR_MAP.get(ticker_in_pos.upper(), "other")
            try:
                exposure[sector] = exposure.get(sector, 0.0) + self._position_capital_for_exposure(pos)
            except Exception as _exp_err:
                log.error("Failed to calculate exposure for sector %s: %s", sector, _exp_err)
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
        self._cache_trade_dossier_signal(signal_id, signal)
        try:
            self._counterfactual_ctx.signal = dict(signal or {})
            self._counterfactual_ctx.signal["signal_id"] = signal_id
            self._counterfactual_ctx.client_id = str(client_id or "default")
            self._counterfactual_ctx.execution_mode = str(self._current_mode() or "PAPER")
        except Exception:
            pass

        # PR #229: fail-CLOSED on missing/invalid side. Pre-#229 this defaulted
        # to "CALL" which meant a malformed signal would silently route as a
        # bullish trade. Now blocked at admission with a stable reason_code so
        # the operator dashboard can filter it. See _normalize_signal_side.
        raw_side = signal.get("side") or signal.get("direction")
        norm_side = _normalize_signal_side(raw_side)
        if norm_side is None:
            return self._block(
                signal_id,
                ticker,
                client_id,
                "blocked_system",
                f"invalid_or_missing_side ({raw_side!r})",
                reason_code="INVALID_OR_MISSING_SIDE",
            )
        signal["side"] = norm_side
        signal["direction"] = norm_side

        log.info("[%s] evaluate | score=%.1f | side=%s | client=%s", ticker, score, norm_side, client_id)

        if getattr(self, "exit_engine_down", False):
            return self._block(signal_id, ticker, client_id, "blocked_system", "exit_engine_down__protective_systems_unavailable")
        if self._kill_switch_fn and self._kill_switch_fn():
            # SEMANTIC CLARIFICATION (P0-3 audit 2026-05-21):
            # kill_switch ON means full halt — block entries AND force-close
            # existing positions. This is different from pause_entries which
            # is "block new entries, keep exits running" (cool-off after a bad
            # session, not an emergency).
            #
            # Trigger is idempotent — request_force_close_all() returns False
            # on subsequent calls. Safe to call on every signal that hits this
            # gate; only the first one fires the actual close-all.
            if not self.is_force_close_requested():
                self.request_force_close_all(reason="kill_switch_activated")
            return self._block(signal_id, ticker, client_id, "blocked_system", "kill_switch_active")
        # H5: per-client entries pause. Only blocks NEW entries for THIS client
        # — evaluate() never gates exits, and other clients' runners have their
        # own master_control instance, so this is fully isolated.
        if self._entries_paused_fn:
            try:
                if self._entries_paused_fn():
                    return self._block(
                        signal_id, ticker, client_id, "blocked_system",
                        "client_entries_paused — operator paused this client; "
                        "exits still active",
                        reason_code="CLIENT_ENTRIES_PAUSED",
                    )
            except Exception as _ep_err:
                log.warning("[%s] entries_paused check failed (fail-open): %s",
                            ticker, _ep_err)

        current_mode = self._current_mode()
        if current_mode == "READ_ONLY":
            return self._block(signal_id, ticker, client_id, "blocked_system", "mode_read_only")
        # Live trading must not depend on backtest EV metadata being present.
        # Scanner score remains the gating score for both paper and live.
        effective_score = score

        direction_raw = norm_side
        timeframe_raw = signal.get("timeframe", "1d")
        setup_key = f"{client_id}:{ticker.upper()}:{direction_raw}:{timeframe_raw}"
        signal_key = f"sig:{signal_id}:{client_id}"
        _now_ts = time.time()
        if len(self._seen_signals) > 500:
            self._seen_signals = {k: v for k, v in self._seen_signals.items() if _now_ts - v < 1800}
        self._prune_trade_dossier_signal_cache(_now_ts)

        # Durable per-client duplicate check.
        # _seen_signals is a fast in-memory HINT only — unsafe as sole
        # authority because process restart wipes it, it can flag a signal as
        # duplicate before the queue row reached a durable accepted state
        # (false positive on retry/recovery), and it persists for 30 min even
        # when no active path exists. Block only when DB proves the same
        # client already has an active path. Terminal REJECTED/ERROR/EXPIRED/
        # CANCELED rows do NOT count.
        _current_qid = None
        try:
            _qid_raw = signal.get("_queue_id")
            _current_qid = int(_qid_raw) if _qid_raw is not None else None
        except (TypeError, ValueError):
            _current_qid = None

        _dur_dup, _dur_source, _dur_detail = self._has_durable_duplicate_signal(
            client_id=client_id,
            signal_id=signal_id,
            current_queue_id=_current_qid,
        )
        if _dur_dup:
            log.warning(
                "durable_duplicate_signal_id client_id=%s signal_id=%s "
                "source=%s detail=%s",
                client_id, signal_id, _dur_source, _dur_detail,
            )
            return self._block(
                signal_id, ticker, client_id,
                "blocked_system",
                f"duplicate_signal_id (durable:{_dur_source})",
            )

        # DB read failure: LIVE fails closed with distinct reason; PAPER
        # proceeds (no capital risk).
        if _dur_source == "check_unavailable":
            log.error(
                "duplicate_check_unavailable client_id=%s signal_id=%s detail=%s",
                client_id, signal_id, _dur_detail,
            )
            if current_mode == "LIVE":
                return self._block(
                    signal_id, ticker, client_id,
                    "blocked_system",
                    f"duplicate_check_unavailable_live_blocked ({_dur_detail})",
                )

        # Memory hint disagreed with durable truth: log it and proceed.
        if signal_key in self._seen_signals:
            log.info(
                "duplicate_memory_ignored_no_durable_active_path "
                "client_id=%s signal_id=%s",
                client_id, signal_id,
            )

        # Setup-level dedup unchanged — same-ticker/direction within 30 min
        # is intentionally per-process. Durable per-client setup dedup is a
        # separate concern from per-signal_id and is out of scope.
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

        # PR: sizing-bootstrap-fix
        # bootstrap_mode = qty=1 safety guard for brand-new LIVE deployments.
        # LIVE: stays active until total_trades >= BOOTSTRAP_TRADES_THRESHOLD
        #       (default 20) so we don't fire 10% sizing on day one.
        # PAPER: completely bypassed — paper has no live capital risk and
        #        proof-week needs realistic sizing to be a credible proof.
        # See _compute_bootstrap_mode helper for the canonical decision.
        total_trades = int(snap.get("total_trades") or 0)
        bootstrap_mode = self._compute_bootstrap_mode(total_trades=total_trades)

        # H8 (corrected): daily_profit_target_usd is a MINIMUM/milestone, NOT a
        # stop. The business goal is to make clients money — once the floor is
        # cleared the bot KEEPS taking quality setups to maximize the day. The
        # only behavior change after clearing the floor is OPTIONAL extra
        # selectivity (protect-profit score bump) so post-target trades have to
        # be a bit cleaner — opportunity stays open, the green day is protected
        # by quality, not by quitting. Disabled when target is 0.0.
        _post_target_score_bump = 0.0
        if self.daily_profit_target_usd and self.daily_profit_target_usd > 0:
            _pnl_today = float(snap.get("realized_pnl_today") or 0.0)
            if _pnl_today >= self.daily_profit_target_usd:
                _post_target_score_bump = float(
                    os.getenv("POST_TARGET_SCORE_BUMP", "5.0")
                )
                log.info(
                    "[%s] Daily floor cleared ($%.0f >= $%.0f) — continuing to "
                    "trade with +%.0f selectivity bump to protect the day",
                    ticker, _pnl_today, self.daily_profit_target_usd,
                    _post_target_score_bump,
                )

        # Slot check: only actual broker-submitted/filled exposure counts.
        # PENDING_TRIGGER / WATCHING / DEFERRED watcher rows are excluded from
        # pending_entries since the position_manager fix. Log when watchers are
        # present so ops can confirm the accounting is correct.
        _watcher_count = snap.get("watcher_count", 0)
        if _watcher_count > 0:
            log.info(
                "[%s] watcher_allowed_not_counted_as_position: "
                "client=%s watcher_rows=%d open=%d real_slots=%d "
                "entry_attempt_locks=%d max=%d "
                "— WATCHING/DEFERRED/pre-fill rows do not consume real position slots",
                ticker, client_id, _watcher_count,
                snap["open_count"], snap["pending_entries"],
                snap.get("entry_attempt_lock_count", 0), self.max_positions,
            )

        # Log entry-attempt locks separately — they are not position slots
        _lock_count = snap.get("entry_attempt_lock_count", 0)
        if _lock_count > 0:
            log.info(
                "[%s] submitted_unfilled_not_counted_as_position: "
                "client=%s entry_attempt_locks=%d reserved=$%.0f "
                "— in-flight submits are duplicate-submit protection only; "
                "not position slots",
                ticker, client_id, _lock_count,
                snap.get("entry_attempt_reserved_cost", 0.0),
            )

        # real_filled_slots = reconciled positions + unreconciled broker fills.
        # pending_entries is deduplicated: only counts filled orders where
        # position_id IS NULL (not yet in positions table), so the same
        # broker-confirmed fill is never counted twice.
        _open       = int(snap.get("open_count")      or 0)
        _unreconciled = int(snap.get("pending_entries") or 0)
        _locks      = int(snap.get("entry_attempt_lock_count", 0))
        _real_slots = _open + _unreconciled

        if _real_slots >= self.max_positions:
            return self._block(
                signal_id, ticker, client_id, "blocked_risk",
                f"blocked_actual_position_limit "
                f"open_positions={_open} "
                f"filled_orders_unreconciled={_unreconciled} "
                f"real_filled_slots={_real_slots} "
                f"max_positions={self.max_positions} "
                f"entry_attempt_locks={_locks}",
            )

        # PR p0/bootstrap-affordable-selection (2026-06-05):
        # In LIVE bootstrap mode we intend to buy ONE contract and the real
        # premium is unknown until the contract selector picks a strike.
        # The old code multiplied a static $3.50 fallback premium by
        # MIN_CONTRACTS_PER_POSITION (>=2 by default) and rejected the
        # signal before the selector could find an affordable contract
        # priced under the cap. Spec rule (5): "do not hard-reject a signal
        # solely because a static preselection premium estimate exceeds the
        # cap when an affordable contract may exist."
        #
        # Outside bootstrap, behaviour is unchanged: the static estimate
        # still gates pre-selection to avoid wasted selector work.
        # PR #121 amend: explicit runtime_execution_mode so the broker-proof
        # SQL filter excludes wrong-mode and NULL-mode rows. current_mode is
        # 'LIVE' / 'PAPER' / 'READ_ONLY' — lowercased to match the
        # orders.execution_mode value space ('live' / 'paper' / 'unknown').
        pending_capital_real = self._pending_capital_from_snapshot_or_db(
            snap,
            client_id,
            runtime_execution_mode=str(current_mode or '').lower(),
        )
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

        # PR E FIX-3 (reader-side patch): take ONE atomic snapshot of
        # (account_equity, max_daily_loss) under _equity_lock here.
        # Use the local variables for ALL subsequent risk math below.
        account_equity, max_daily_loss = self._equity_snapshot()
        # PR #155 — split per-position cap from total portfolio exposure cap.
        #
        # OLD (single cap):
        #   max_capital = equity * max_capital_pct   (acts as BOTH per-trade AND total)
        #   remaining   = max_capital - deployed - pending
        #
        # NEW (split caps):
        #   per_trade_budget       = equity * max_position_pct
        #   total_capital_cap      = equity * max_total_capital_pct
        #   current_total_exposure = capital_deployed + pending_capital
        #   remaining_total_cap    = total_capital_cap - current_total_exposure
        #   selector_budget        = min(per_trade_budget, remaining_total_cap)
        #
        # Existing exposure only shrinks selector_budget when the TOTAL
        # portfolio exposure cap is near full — not on every new trade.
        # Backward compat: when max_position_pct == max_total_capital_pct
        # (legacy single-value path), behaviour is identical to the old formula.
        #
        # Block reasons emitted below:
        #   capital_limit_total_exposure_cap_reached — total cap at/over limit
        #   capital_limit_no_remaining               — selector_budget <= 0

        per_trade_budget       = account_equity * self.max_position_pct
        total_capital_cap      = account_equity * self.max_total_capital_pct
        current_total_exposure = snap["capital_deployed"] + pending_capital_real
        remaining_total_cap    = total_capital_cap - current_total_exposure

        # Hard block: total portfolio exposure cap is at or over limit.
        # This is a separate, earlier block so the reason code is unambiguous.
        if remaining_total_cap <= 0.0:
            _block_meta_total = {
                "client_id":                 client_id,
                "execution_mode":            str(current_mode or "").lower(),
                "account_equity":            float(account_equity),
                "max_position_pct":          float(self.max_position_pct),
                "max_total_capital_pct":     float(self.max_total_capital_pct),
                "per_trade_budget":          float(per_trade_budget),
                "total_capital_cap":         float(total_capital_cap),
                "capital_deployed":          float(snap.get("capital_deployed", 0)),
                "pending_capital":           float(pending_capital_real),
                "current_total_exposure":    float(current_total_exposure),
                "remaining_total_cap":       float(remaining_total_cap),
                "selector_budget":           0.0,
                "legacy_max_capital_pct":    float(self.max_capital_pct),
            }
            log.warning(
                "[%s] CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED "
                "client=%s execution_mode=%s equity=%.0f "
                "total_capital_cap=%.0f current_total_exposure=%.0f "
                "remaining_total_cap=%.0f per_trade_budget=%.0f",
                ticker, client_id, current_mode, account_equity,
                total_capital_cap, current_total_exposure,
                remaining_total_cap, per_trade_budget,
            )
            return self._block(
                signal_id, ticker, client_id, "blocked_risk",
                f"capital_limit_total_exposure_cap_reached "
                f"total_cap=${total_capital_cap:.0f} "
                f"exposure=${current_total_exposure:.0f} "
                f"remaining=${remaining_total_cap:.0f}",
                reason_code="CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED",
                meta=_block_meta_total,
            )

        # selector_budget = tighter of: per-trade cap vs remaining total capacity.
        # Floor at 0 — negative means cap already exceeded.
        remaining_capital_for_this_trade = max(
            0.0,
            min(per_trade_budget, remaining_total_cap),
        )

        # Keep max_capital as an alias for downstream code that reads it
        # (logging, sizing_context, revalidate_exposure). Set to per_trade_budget
        # so those paths see the per-trade limit, not the total portfolio cap.
        max_capital = per_trade_budget

        # Two pre-block strategies:
        #   - LIVE  (bootstrap OR post-bootstrap): use the affordability
        #     flow. Skip the static-premium pre-block whenever
        #     remaining_capital > 0; let the selector search the chain for
        #     a contract whose real ask fits. revalidate_exposure() does
        #     the final real-cost check.
        #   - PAPER (non-bootstrap): keep the static-estimate pre-block
        #     so paper proof-week sizing stays comparable to historical
        #     behaviour. PAPER bootstrap (rare) follows the LIVE path.
        #
        # Review fix #2 (2026-06-05): a small LIVE account that crosses
        # the bootstrap threshold (default 20 trades) must NOT regress to
        # the static-fallback false-reject path. The condition below is
        # is_live_mode — not bootstrap_mode — so the affordability flow
        # stays active for the entire life of any LIVE client.
        _use_affordability_flow = bootstrap_mode or self._is_live_mode()
        if _use_affordability_flow:
            # intended_contracts is 1 in bootstrap (rule 7); for post-
            # bootstrap LIVE we still defer to the selector and let the
            # final-cost revalidation enforce the cap. Pre-block only when
            # remaining_capital cannot fund even the lowest-cost contract.
            estimated_contracts_pre = 1 if bootstrap_mode else 0
            estimated_new_cost_pre = 0.0   # unknown until selector runs
            if remaining_capital_for_this_trade <= 0.0:
                # PR #121: pass runtime_execution_mode so historical NULL-mode
                # and wrong-mode rows are excluded from the breakdown. Also
                # capture diagnostic buckets (ignored_*) and surface into
                # result_json so the operator can immediately see why pending
                # exposure blocked the trade.
                _bd_eval = self._get_pending_capital_breakdown(
                    snap, client_id,
                    runtime_execution_mode=str(current_mode or '').lower(),
                ) or {}
                _pending_submitted_real = float(
                    _bd_eval.get("pending_submitted_entry_exposure", 0.0)
                )
                _filled_unreconciled_entry_capital = float(
                    _bd_eval.get("filled_unreconciled_entry_capital", 0.0)
                )
                _pending_total_real     = float(
                    _bd_eval.get("pending_total_capital_reserved", pending_capital_real)
                )
                self._log_capital_breakdown_proof(ticker=ticker, breakdown=_bd_eval)

                log.warning(
                    "[%s] SMALL_ACCOUNT_CONTRACT_UNAFFORDABLE "
                    "client_email=%s execution_mode=%s client_cap=%.0f "
                    "capital_deployed=%.0f pending_submitted_entry_exposure=%.0f "
                    "filled_unreconciled_entry_capital=%.0f "
                    "pending_total_capital_reserved=%.0f "
                    "remaining_capital=%.0f "
                    "counted_order_ids=%s ignored_buckets=%s "
                    "candidate_limit=N/A computed_qty=0 original_qty=N/A final_qty=0 "
                    "reason=capital_limit_no_remaining",
                    ticker, client_id, current_mode,
                    max_capital,
                    float(snap.get("capital_deployed", 0)),
                    _pending_submitted_real,
                    _filled_unreconciled_entry_capital,
                    _pending_total_real,
                    remaining_capital_for_this_trade,
                    _bd_eval.get("counted_order_ids", []),
                    list((_bd_eval.get("ignored_reserved_cost_by_status") or {}).keys()),
                )

                # PR #121: full diagnostic payload into result_json.
                _block_meta = {
                    "client_id":                          client_id,
                    "execution_mode":                     str(current_mode or '').lower(),
                    "client_cap":                         float(max_capital),
                    "deployed_exposure":                  float(snap.get("capital_deployed", 0)),
                    "position_capital_deployed":          float(
                        snap.get("position_capital_deployed", snap.get("capital_deployed", 0))
                    ),
                    "pending_submitted_entry_exposure":   _pending_submitted_real,
                    "filled_unreconciled_entry_capital":  _filled_unreconciled_entry_capital,
                    "filled_unreconciled_exposure":       _filled_unreconciled_entry_capital,
                    "pending_total_capital_reserved":     _pending_total_real,
                    "ignored_already_reconciled_fill_capital": float(
                        _bd_eval.get("ignored_already_reconciled_fill_capital", 0.0)
                    ),
                    "ignored_already_reconciled_order_ids":   _bd_eval.get("ignored_already_reconciled_order_ids", []),
                    "ignored_reconciled_match_keys":          _bd_eval.get("ignored_reconciled_match_keys", []),
                    "remaining_capital":                  float(remaining_capital_for_this_trade),
                    "counted_order_ids":                  _bd_eval.get("counted_order_ids", []),
                    "counted_order_statuses":             _bd_eval.get("counted_order_statuses", []),
                    "counted_broker_order_ids":           _bd_eval.get("counted_broker_order_ids", []),
                    "ignored_reserved_cost_by_status":    _bd_eval.get("ignored_reserved_cost_by_status", {}),
                    "ignored_order_ids_by_status":        _bd_eval.get("ignored_order_ids_by_status", {}),
                    "open_position_ids":                  _bd_eval.get("open_position_ids", []),
                    "runtime_execution_mode":             _bd_eval.get("runtime_execution_mode"),
                    "active_broker_statuses":             _bd_eval.get("active_broker_statuses", []),
                }

                return self._block(
                    signal_id, ticker, client_id, "blocked_risk",
                    f"capital_limit_no_remaining "
                    f"client_cap=${max_capital:.0f} "
                    f"deployed=${snap['capital_deployed']:.0f} "
                    f"pending_total=${_pending_total_real:.0f} "
                    f"remaining=${remaining_capital_for_this_trade:.0f}",
                    reason_code="CAPITAL_LIMIT_NO_REMAINING",
                    meta=_block_meta,
                )
        else:
            # PAPER non-bootstrap: keep the existing static-estimate
            # pre-check so selector work is skipped when the projection
            # clearly exceeds the cap. Final cost is still revalidated
            # post-selection (revalidate_exposure runs in PAPER too).
            estimated_contracts_pre = max(
                MIN_CONTRACTS_PER_POSITION,
                self._base_contracts(effective_score, _estimate_premium(ticker)),
            )
            estimated_new_cost_pre = estimated_contracts_pre * 100 * _estimate_premium(ticker)
            projected_total = snap["capital_deployed"] + pending_capital_real + estimated_new_cost_pre
            if projected_total > max_capital:
                return self._block(
                    signal_id, ticker, client_id, "blocked_risk",
                    f"capital_limit (projected ${projected_total:.0f} > ${max_capital:.0f})",
                )

        sector = self.SECTOR_MAP.get(ticker.upper(), "other")
        sector_deployed = self._sector_capital_deployed(snap["open_positions"] + snap["closing_positions"], sector)
        # PR E FIX-3: use the snapshot value, not the instance field.
        effective_equity = account_equity
        max_sector_capital = effective_equity * self.max_sector_pct
        max_ticker_capital = effective_equity * self.max_ticker_pct
        ticker_deployed = self._ticker_capital_deployed(snap["open_positions"] + snap["closing_positions"], ticker)

        # Sector / ticker caps: same split as the capital cap above.
        # Affordability flow (LIVE always, plus any PAPER bootstrap) only
        # pre-blocks when a bucket is fully saturated. Final per-trade
        # contribution is checked against real_cost in
        # revalidate_exposure() after the selector picks a real strike.
        if _use_affordability_flow:
            if sector_deployed >= max_sector_capital:
                return self._block(
                    signal_id, ticker, client_id, "blocked_risk",
                    f"sector_cap_{sector}_saturated (deployed=${sector_deployed:.0f} >= cap=${max_sector_capital:.0f})",
                )
            if ticker_deployed >= max_ticker_capital:
                return self._block(
                    signal_id, ticker, client_id, "blocked_risk",
                    f"ticker_cap_{ticker.upper()}_saturated (deployed=${ticker_deployed:.0f} >= cap=${max_ticker_capital:.0f})",
                )
            estimated_contracts = 1 if bootstrap_mode else 0
            estimated_new_cost = 0.0
        else:
            estimated_contracts = max(MIN_CONTRACTS_PER_POSITION, self._base_contracts(effective_score, _estimate_premium(ticker)))
            estimated_new_cost = estimated_contracts * 100 * _estimate_premium(ticker)
            projected_sector = sector_deployed + estimated_new_cost
            if projected_sector > max_sector_capital:
                return self._block(signal_id, ticker, client_id, "blocked_risk", f"sector_cap_{sector} (projected ${projected_sector:.0f} > ${max_sector_capital:.0f})")

            estimated_new_cost_ticker = estimated_contracts * 100 * _estimate_premium(ticker)
            projected_ticker = ticker_deployed + estimated_new_cost_ticker
            if projected_ticker > max_ticker_capital:
                return self._block(signal_id, ticker, client_id, "blocked_risk", f"ticker_cap_{ticker.upper()} (projected ${projected_ticker:.0f} > ${max_ticker_capital:.0f})")

        _real_calls = int(snap.get("calls_open") or 0) + int(snap.get("filled_unreconciled_calls") or 0)
        _real_puts  = int(snap.get("puts_open")  or 0) + int(snap.get("filled_unreconciled_puts")  or 0)
        if norm_side == "CALL" and _real_calls >= self.max_calls:
            return self._block(
                signal_id, ticker, client_id, "blocked_risk",
                f"blocked_actual_position_limit "
                f"open_calls={snap.get('calls_open',0)} "
                f"unreconciled_calls={snap.get('filled_unreconciled_calls',0)} "
                f"real_calls={_real_calls}/{self.max_calls}",
            )
        if norm_side == "PUT" and _real_puts >= self.max_puts:
            return self._block(
                signal_id, ticker, client_id, "blocked_risk",
                f"blocked_actual_position_limit "
                f"open_puts={snap.get('puts_open',0)} "
                f"unreconciled_puts={snap.get('filled_unreconciled_puts',0)} "
                f"real_puts={_real_puts}/{self.max_puts}",
            )
        if snap["trades_today"] >= self.max_trades_today:
            return self._block(signal_id, ticker, client_id, "blocked_risk", f"max_trades_today ({snap['trades_today']}/{self.max_trades_today})")
        # PR E FIX-3: daily-loss check uses the snapshot value (local
        # `max_daily_loss` from the helper above), not the instance
        # field directly. The block uses the local variable
        # consistently in the comparison, the force-close-all reason,
        # and the user-facing block message.
        if snap["realized_pnl_today"] <= max_daily_loss:
            # P0-3: also trigger force-close-all so existing positions don't
            # keep bleeding. Entry gate blocks NEW trades; this circuit breaker
            # closes EXISTING ones. Once-per-session (idempotent).
            if not self._daily_loss_force_close_fired:
                self._daily_loss_force_close_fired = True
                self.request_force_close_all(
                    reason=(
                        f"daily_loss_limit ${snap['realized_pnl_today']:.2f} "
                        f"<= ${max_daily_loss:.2f}"
                    )
                )
            return self._block(signal_id, ticker, client_id, "blocked_risk", f"daily_loss_limit (${snap['realized_pnl_today']:.2f} <= ${max_daily_loss:.2f})")
        _open_only_tickers = {str(p.get("underlying") or p.get("ticker") or "").upper() for p in snap["open_positions"]}
        if _open_only_tickers and ticker.upper() in _open_only_tickers:
            return self._block(signal_id, ticker, client_id, "blocked_risk", f"ticker_already_active ({ticker})")

        cooldown_key = f"{ticker.upper()}:{direction_raw}:cooldown"
        # PR E / FIX-4: snapshot the cooldown timestamp under _cooldown_lock
        # so it cannot change between the membership check and the read.
        # ap_execution_core writes to _trade_cooldowns from a separate
        # thread; without this lock we could see a partially-mutated
        # dict (rare but real on CPython during the dict resize).
        with self._cooldown_lock:
            _cooldown_ts = self._trade_cooldowns.get(cooldown_key)
        if _cooldown_ts is not None:
            elapsed = time.time() - _cooldown_ts
            if elapsed < 1800:
                return self._block(signal_id, ticker, client_id, "blocked_risk", f"same_setup_cooldown ({ticker} {direction_raw}, {int(1800 - elapsed)}s remaining)")

        if self.pm:
            try:
                if self.pm.has_pending_entry(ticker):
                    return self._block(signal_id, ticker, client_id, "blocked_risk", f"pending_entry_exists ({ticker})")
            except Exception as e:
                log.warning("[%s] has_pending_entry check failed: %s", ticker, e)

        # H8: after the daily floor is cleared, require a slightly higher score
        # so post-target trades are cleaner — keeps the bot in opportunity while
        # protecting the green day with quality rather than by stopping.
        _eff_priority_floor = _PRIORITY_FLOOR + _post_target_score_bump
        _eff_score_floor    = self.score_floor + _post_target_score_bump
        _interrogation_trace = build_interrogation_packet_threshold_trace(
            signal.get("decision_packet") or signal.get("_decision_packet") or signal.get("interrogation_packet"),
            resolved=self.admission_thresholds,
        )

        # PR B + SCORE-65 REFINEMENT — structured score eligibility.
        # Runs BEFORE every other admission gate. The universal hard floor
        # (SCORE_MIN_ELIGIBLE=70) still applies, but score-65..69 setups get a
        # structured second look: clean non-0DTE setups may pass while 0DTE,
        # wide-spread, and weak-contract 65s stay blocked. Feature is gated by
        # SCORE65_ALLOW (default false) — when off, behavior is identical to
        # the previous blunt reject.
        _hard_floor = SCORE_MIN_ELIGIBLE + _post_target_score_bump

        # Resolve 0DTE once here (reused by PR C below). Cheap + side-effect free.
        _is_0dte_for_gate = False
        _dte_known        = False  # item-2: True only when DTE is provable
        try:
            from zoneinfo import ZoneInfo as _ZI_sg
            from datetime import datetime as _dt_sg
            _today_str_sg = _dt_sg.now(_ZI_sg("America/New_York")).strftime("%Y-%m-%d")
            _exp_sg = (
                signal.get("expiration")
                or signal.get("expiration_date")
                or (signal.get("trigger") or {}).get("expiration")
                or ""
            )
            _dte_sg = signal.get("dte")
            if _dte_sg is not None:
                try:
                    _is_0dte_for_gate = int(_dte_sg) == 0
                    _dte_known = True   # parsed cleanly
                except (TypeError, ValueError):
                    _is_0dte_for_gate = False
                    _dte_known = False  # present but unparseable
            # A parseable YYYY-MM-DD expiration also proves DTE.
            # Use strptime to actually validate the date — checking dash
            # positions alone accepts garbage like "2026-ab-cd" or "2026-99-99"
            # which would set _dte_known=True on data that is not a real date.
            if isinstance(_exp_sg, str) and len(_exp_sg) >= 10:
                try:
                    _exp_date = _dt_sg.strptime(_exp_sg[:10], "%Y-%m-%d").date()
                    _today_date = _dt_sg.now(_ZI_sg("America/New_York")).date()
                    if _exp_date == _today_date:
                        _is_0dte_for_gate = True
                    _dte_known = True   # strptime succeeded → real date proven
                except (ValueError, TypeError):
                    pass  # unparseable expiration — leave _dte_known as-is
        except Exception:
            _is_0dte_for_gate = False
            _dte_known = False

        _spread_for_gate = signal.get("spread_pct")
        try:
            _spread_for_gate = float(_spread_for_gate) if _spread_for_gate is not None else None
        except (TypeError, ValueError):
            _spread_for_gate = None
        _delta_for_gate = signal.get("delta")
        try:
            _delta_for_gate = float(_delta_for_gate) if _delta_for_gate is not None else None
        except (TypeError, ValueError):
            _delta_for_gate = None

        _score_ok, _score_reason = _score_allows_entry(
            score=effective_score,
            hard_floor=_hard_floor,
            is_0dte=_is_0dte_for_gate,
            is_index=(ticker.upper() in _PRIORITY_TICKERS),
            timeframe=signal.get("timeframe", "1d"),
            spread_pct=_spread_for_gate,
            delta=_delta_for_gate,
            dte_known=_dte_known,
        )
        _threshold_trace = {
            "scanner_floor": build_threshold_trace(
                threshold_name="scanner_floor",
                score_value=float(score),
                floor_value=self.admission_thresholds.thresholds.scanner_floor,
                passed=float(score) >= self.admission_thresholds.thresholds.scanner_floor,
                source=self.admission_thresholds.sources["scanner_floor"],
            ),
            "interrogation_floor": _interrogation_trace,
            "mc_score_floor": build_threshold_trace(
                threshold_name="mc_score_floor",
                score_value=float(effective_score),
                floor_value=float(_eff_score_floor),
                passed=float(effective_score) >= float(_eff_score_floor),
                source=self.admission_thresholds.sources["mc_score_floor"],
            ),
            "mc_priority_floor": build_threshold_trace(
                threshold_name="mc_priority_floor",
                score_value=float(effective_score),
                floor_value=float(_eff_priority_floor),
                passed=float(effective_score) >= float(_eff_priority_floor),
                source=self.admission_thresholds.sources["mc_priority_floor"],
            ),
        }
        if not _score_ok:
            self._store_update(
                signal_id, "rejected_low_score",
                f"score {effective_score:.1f} < min_eligible {_hard_floor:.1f} ({_score_reason})",
            )
            return self._block(
                signal_id, ticker, client_id, "blocked_score",
                f"{_score_reason} (score={effective_score:.1f} "
                f"min_eligible={_hard_floor:.1f})",
                meta={
                    "threshold_trace": _threshold_trace,
                    "threshold_config_hash": self.admission_threshold_config_hash,
                },
            )
        elif _score_reason:
            # Admitted via the score-65 exception band — record WHY for the audit.
            self._store_update(
                signal_id, "score65_admitted",
                f"score {effective_score:.1f} admitted via {_score_reason}",
            )

        # PR C — SPY/QQQ index 0DTE late-day cutoff.
        # Runs AFTER the PR B hard floor (so a score-65 SPY 0DTE still hits
        # REJECTED_LOW_SCORE first, cheaper) and BEFORE the legacy
        # priority/score floor checks (so a 0DTE SPY signal arriving at
        # 2:31 PM ET never reaches the broker no matter what its score is
        # within the PR B floor).
        if ticker.upper() in _PRIORITY_TICKERS:
            try:
                from zoneinfo import ZoneInfo as _ZI
                from datetime import datetime as _dt, time as _dt_time
                _now_et = _dt.now(_ZI("America/New_York"))
                _today_str = _now_et.strftime("%Y-%m-%d")

                # Resolve DTE from any of the common signal shapes.
                _exp_raw = (
                    signal.get("expiration")
                    or signal.get("expiration_date")
                    or (signal.get("trigger") or {}).get("expiration")
                    or ""
                )
                _dte_raw = signal.get("dte")
                _is_0dte = False
                if _dte_raw is not None:
                    try:
                        _is_0dte = int(_dte_raw) == 0
                    except (TypeError, ValueError):
                        _is_0dte = False
                if not _is_0dte and isinstance(_exp_raw, str) and _exp_raw[:10] == _today_str:
                    _is_0dte = True

                if _is_0dte:
                    try:
                        _ch, _cm = (int(x) for x in INDEX_0DTE_CUTOFF_ET.split(":"))
                        _wh, _wm = (int(x) for x in INDEX_LATE_DAY_CAUTION_ET.split(":"))
                        _cutoff   = _dt_time(_ch, _cm)
                        _caution  = _dt_time(_wh, _wm)
                    except Exception:
                        _cutoff   = _dt_time(14, 30)
                        _caution  = _dt_time(13, 30)

                    _now_t = _now_et.time()
                    if _now_t >= _cutoff:
                        # Hard block — no 0DTE on indexes after cutoff.
                        self._store_update(
                            signal_id, "rejected_0dte_cutoff",
                            f"{ticker} 0DTE after {INDEX_0DTE_CUTOFF_ET} ET",
                        )
                        return self._block(
                            signal_id, ticker, client_id, "blocked_time",
                            (
                                f"REJECTED_0DTE_CUTOFF ({ticker} 0DTE "
                                f"now={_now_t.strftime('%H:%M')} ET "
                                f"cutoff={INDEX_0DTE_CUTOFF_ET} ET)"
                            ),
                        )
                    if _now_t >= _caution and effective_score < INDEX_LATE_DAY_SCORE_FLOOR:
                        # Soft window: 0DTE allowed but only at higher score.
                        self._store_update(
                            signal_id, "rejected_0dte_caution",
                            (
                                f"{ticker} 0DTE score {effective_score:.1f} < "
                                f"late-day floor {INDEX_LATE_DAY_SCORE_FLOOR:.1f} "
                                f"after {INDEX_LATE_DAY_CAUTION_ET} ET"
                            ),
                        )
                        return self._block(
                            signal_id, ticker, client_id, "blocked_score",
                            (
                                f"REJECTED_0DTE_CAUTION_PERIOD "
                                f"(score={effective_score:.1f} < {INDEX_LATE_DAY_SCORE_FLOOR:.1f} "
                                f"after {INDEX_LATE_DAY_CAUTION_ET} ET)"
                            ),
                        )
            except Exception as _e:
                # Defensive: if the gate itself fails (timezone import, weird
                # signal shape, etc.) we DO NOT block on "unknown" — fall
                # through to existing checks. We log so the operator sees it.
                log.warning(
                    "[%s] PR-C 0DTE gate raised, falling through: %s",
                    ticker, _e,
                )

        if ticker.upper() in _PRIORITY_TICKERS:
            if effective_score < _eff_priority_floor:
                self._store_update(signal_id, "rejected", f"priority score {score:.1f} < floor {_eff_priority_floor}")
                return self._block(
                    signal_id,
                    ticker,
                    client_id,
                    "blocked_score",
                    f"score_below_priority_floor ({score:.1f}<{_eff_priority_floor})",
                    meta={
                        "threshold_trace": _threshold_trace,
                        "threshold_config_hash": self.admission_threshold_config_hash,
                    },
                )
        else:
            if effective_score < _eff_score_floor:
                self._store_update(signal_id, "rejected", f"score {effective_score:.1f} < floor {_eff_score_floor}")
                return self._block(
                    signal_id,
                    ticker,
                    client_id,
                    "blocked_score",
                    f"score_below_floor ({effective_score:.1f}<{_eff_score_floor})",
                    meta={
                        "threshold_trace": _threshold_trace,
                        "threshold_config_hash": self.admission_threshold_config_hash,
                    },
                )

        score_breakdown = signal.get("score_breakdown") or {}
        if "real_time_ctx" in score_breakdown:
            ctx = float(score_breakdown.get("real_time_ctx", 0) or 0)
            _threshold_trace["context_floor"] = build_threshold_trace(
                threshold_name="context_floor",
                score_value=ctx,
                floor_value=float(self.context_floor),
                passed=ctx >= float(self.context_floor),
                source=self.admission_thresholds.sources["context_floor"],
            )
            if ctx < self.context_floor:
                self._store_update(signal_id, "context_blocked", f"ctx={ctx:.1f} < floor {self.context_floor}")
                return self._block(
                    signal_id,
                    ticker,
                    client_id,
                    "blocked_score",
                    f"context_below_floor (ctx={ctx:.1f}<{self.context_floor})",
                    meta={
                        "threshold_trace": _threshold_trace,
                        "threshold_config_hash": self.admission_threshold_config_hash,
                    },
                )
        else:
            _threshold_trace["context_floor"] = build_threshold_trace(
                threshold_name="context_floor",
                score_value=None,
                floor_value=float(self.context_floor),
                passed=None,
                source="unavailable:score_breakdown.real_time_ctx_missing",
            )

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

        # ── PR-72: Quality Mode gate ──────────────────────────────────────
        # Runs AFTER score-floor and intel gate so we operate on the final
        # approved effective_score.  Gate is a no-op when
        # QUALITY_MODE_ENABLED != true; all caps and cooldowns are enforced
        # only in quality mode.  Does NOT touch exits, broker, or pricing.
        #
        # quality_mode_result is the canonical output of this module.
        # It is attached to:
        #   - plan.metadata["quality_mode_result"]      (approved AND blocked)
        #   - plan.metadata["score_audit"]["quality_mode_result"]  (approved)
        # PR-73 reads these fields; it does not recompute QM outcome.
        # If QM is disabled, build_disabled_result() provides a null-safe
        # object so PR-73 can render gracefully before PR-72 is merged.
        _qm_verdict = None
        try:
            import ap_quality_mode as _qm_mod
            _snap_for_qm = self._get_snapshot(client_id, ticker=ticker, signal_id=signal_id)
            _qm_verdict = _qm_mod.check(
                signal=signal,
                client_id=client_id,
                intel_status=intel.get("intel_status"),
                daily_trades_today=int(_snap_for_qm.get("trades_today", 0) or 0),
                open_positions=int(
                    (_snap_for_qm.get("open_count") or 0)
                    + (_snap_for_qm.get("pending_entries") or 0)
                ),
                approved_score=effective_score,
            )
            if not _qm_verdict.allowed:
                self._store_update(
                    signal_id, "rejected",
                    f"quality_mode_blocked: {_qm_verdict.quality_mode_reason}",
                )
                _qm_block = self._block(
                    signal_id, ticker, client_id,
                    "blocked_quality_mode",
                    _qm_verdict.quality_mode_reason,
                    reason_code=_qm_verdict.log_code,
                )
                # Attach quality_mode_result + signal context to the block
                # plan so the rejection feed and proof logger can read it.
                if _qm_block.plan is None:
                    from decision_packet import APTradePlan
                    _block_meta = {
                        **_qm_verdict.meta,
                        "signal_id":           signal_id,
                        "quality_mode_result": _qm_verdict.quality_mode_result,
                    }
                    _qm_block = _qm_block._replace(
                        plan=APTradePlan(
                            symbol=ticker,
                            direction=signal.get("direction", ""),
                            contracts=0,
                            metadata=_block_meta,
                        )
                    ) if hasattr(_qm_block, "_replace") else _qm_block
                return _qm_block
        except ImportError:
            log.warning("[%s] ap_quality_mode not found — quality gate skipped", ticker)
        except Exception as _qm_err:
            log.error("[%s] Quality mode gate raised (fail-open): %s", ticker, _qm_err)
        # ── end PR-72 ─────────────────────────────────────────────────────
        # Build a null-safe disabled result for the approved-plan metadata
        # path below. Used when QM is off OR when the gate raised/skipped.
        try:
            import ap_quality_mode as _qm_mod_fb
            _qm_disabled_result = _qm_mod_fb.build_disabled_result()
        except Exception:
            _qm_disabled_result = None

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
        # Use actual contract limit_price if the signal already has one selected
        # This gives accurate contract sizing instead of falling back to estimates
        _actual_limit = float(signal.get("limit_price") or signal.get("contract_premium") or 0)
        if _actual_limit > 0:
            placeholder_premium = _actual_limit * 100
            log.debug("[%s] Using actual contract premium $%.2f for sizing", ticker, _actual_limit)
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
                # PR E FIX-3: pass the snapshot equity value (local
                # `account_equity` from the helper above), not the
                # instance field directly. The snapshot was taken before
                # the risk-gate checks; the sizer benefits from the
                # same atomic value.
                _sizing = self.sizer.compute(
                    client_id=client_id,
                    tier=str(tier),
                    premium_per_contract=placeholder_premium,
                    account_equity=account_equity,
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
                contracts = self._base_contracts(effective_score, _estimate_premium(ticker))
        else:
            if str(tier).upper() == "B":
                # B-tier: score-driven, cap at 4 (budget gate handles the real ceiling)
                contracts = self._base_contracts(effective_score, _estimate_premium(ticker))
                contracts = min(contracts, 4)  # B-tier cap
            else:
                tier_mult = 1.0 if str(tier).upper() == "A+" else 0.6
                base = self._base_contracts(effective_score, _estimate_premium(ticker))
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
            # PR p0/bootstrap-affordable-selection (2026-06-05) + review fix #2:
            # For ANY LIVE client (bootstrap or post-bootstrap) we pass
            # remaining_capital as the selector budget. The selector reads
            # plan.max_position_usd and only returns contracts whose real
            # ask*100 fits inside it. revalidate_exposure() then enforces
            # the actual cost against the same cap before broker submission.
            #
            # PAPER non-bootstrap continues to use the static MAX_TRADE_USD
            # env so paper proof-week sizing is comparable to historical runs.
            max_position_usd=(
                remaining_capital_for_this_trade
                if _use_affordability_flow
                else float(os.getenv("MAX_TRADE_USD", "1800"))
            ),
            tier=str(tier),
            score=score,
            intel_score=intel_score,
            confidence_bucket=signal.get("confidence_tag", "standard_pool"),
            trigger_type=trigger_type,
            trigger_price=float(entry_price) if entry_price else None,
            stop_underlying=float(stop_price) if stop_price else None,
            target_underlying=float(target_price) if target_price else None,
            # PR E / FIX-2: normalize mode to uppercase LIVE/PAPER so it
            # matches the rest of the system (master_control.mode,
            # APEntryWatcher.mode, ap_execution_core.mode, client_runner.mode
            # all use uppercase). Previously lowercase "live"/"paper" caused
            # silent string mismatches in any downstream `if plan.mode ==
            # "LIVE":` check.
            mode="LIVE" if not self.paper else "PAPER",
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
                # PR: sizing-bootstrap-fix — dedicated, named bucket so
                # future audits can answer "why N contracts?" from a single
                # JSON path in orders.meta. Persisted unconditionally on
                # every approved plan, paper or live.
                "sizing_context": {
                    "account_equity":   float(account_equity),
                    # HOTFIX hotfix/master-control-sizing-context-float-none:
                    # SizingResult dataclass has NO risk_pct / budget_usd
                    # fields. PR #43 used `getattr(..., None)` inside float()
                    # which raised TypeError on every signal evaluation after
                    # 2026-05-26 15:22 UTC. We coerce the getattr result with
                    # `or <fallback>` so None never reaches float().
                    "risk_pct":         float(
                        getattr(_sizing, "risk_pct", None)
                        or float(os.getenv("POSITION_RISK_PCT", "0.10"))
                    ),
                    "contracts":        int(contracts),
                    "budget":           float(
                        getattr(_sizing, "budget_usd", None)
                        or (account_equity * float(os.getenv("POSITION_RISK_PCT", "0.10")))
                    ),
                    "total_trades":     int(total_trades),
                    "bootstrap_mode":   bool(bootstrap_mode),
                    "mode":             "LIVE" if not self.paper else "PAPER",
                    "score":            float(score),
                    "tier":             str(tier),
                    "premium_estimate": float(placeholder_premium / 100.0),
                    "static_premium_estimate": float(_estimate_premium(ticker)),
                    "max_position_usd": float(
                        remaining_capital_for_this_trade
                        if _use_affordability_flow
                        else float(os.getenv("MAX_TRADE_USD", "1800"))
                    ),
                    "max_contracts_cap": int(os.getenv("MAX_CONTRACTS", "15")),
                    "sizing_method":    _sizing.method if _sizing is not None else "tier_fallback",
                    # PR p0/bootstrap-affordable-selection telemetry per spec.
                    # These fields surface every input the post-mortem needs
                    # to answer "why did the selector see this budget?".
                    "account_equity":          float(account_equity),
                    "max_capital_pct":         float(self.max_capital_pct),
                    "max_capital_allowed":     float(max_capital),
                    "capital_deployed":        float(snap["capital_deployed"]),
                    "pending_capital":         float(pending_capital_real),
                    "remaining_capital":       float(remaining_capital_for_this_trade),
                    "intended_contracts":      int(contracts if bootstrap_mode else estimated_contracts_pre),
                    "max_affordable_premium": (
                        round(remaining_capital_for_this_trade / 100.0, 4)
                        if _use_affordability_flow and remaining_capital_for_this_trade > 0
                        else None
                    ),
                    "affordability_flow":  bool(_use_affordability_flow),
                    # PR #155 — split-cap fields for full capital math audit.
                    "max_position_pct":        float(self.max_position_pct),
                    "max_total_capital_pct":   float(self.max_total_capital_pct),
                    "per_trade_budget":        float(per_trade_budget),
                    "total_capital_cap":       float(total_capital_cap),
                    "current_total_exposure":  float(current_total_exposure),
                    "remaining_total_cap":     float(remaining_total_cap),
                    "selector_budget":         float(remaining_capital_for_this_trade),
                    "legacy_max_capital_pct":  float(self.max_capital_pct),
                },
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
                # PR-72: Quality Mode result — the canonical audit object.
                # Populated on every approved signal (enabled or disabled).
                # PR-73 reads this path; it does not recompute QM outcome.
                # score_audit.quality_mode_result mirrors this for callers
                # that navigate via score_audit.
                "quality_mode_result": (
                    _qm_verdict.quality_mode_result
                    if _qm_verdict is not None
                    else _qm_disabled_result
                ),
                # PR73: score_audit — complete scoring visibility.
                # Extends the skeleton written by PR-72 with all gate fields.
                # Read-only — zero changes to scoring logic, tiers, or floors.
                "score_audit": {
                    "final_score":          effective_score,
                    "raw_score":            score,
                    "ev_score":             float(signal.get("ev_score") or 0),
                    "score_floor":          float(_eff_score_floor),
                    "post_target_bump":     float(_post_target_score_bump),
                    "tier":                 str(tier),
                    "gate_status":          "APPROVED",
                    "approved":             True,
                    "reject_reasons":       [],
                    "score_reason":         _score_reason or "",
                    "score_components":     signal.get("score_breakdown") or {},
                    "threshold_trace":      _threshold_trace,
                    "threshold_config_hash": self.admission_threshold_config_hash,
                    "setup_status":         setup_status,
                    # quality_mode_result: canonical output from PR-72.
                    # PR-73 reads this; it does not recompute QM outcome.
                    "quality_mode_result":  (
                        _qm_verdict.quality_mode_result
                        if _qm_verdict is not None
                        else _qm_disabled_result
                    ),
                    "intel_result": {
                        "approved":  intel_approve,
                        "score":     intel_score,
                        "reason":    intel_reason[:200] if intel_reason else "",
                        "available": intel_avail,
                    } if intel_avail else None,
                    "mode":                 "LIVE" if not self.paper else "PAPER",
                    "effective_mode":       current_mode,
                },
            },
        )

        # Final race-condition guard before dedup persistence / queue approval.
        if self._kill_switch_fn and self._kill_switch_fn():
            return self._block(signal_id, ticker, client_id, "blocked_system", "kill_switch_active_pre_commit")

        _final_quality_block = self._run_final_quality_gates(
            signal=signal,
            signal_id=signal_id,
            ticker=ticker,
            client_id=client_id,
            plan=plan,
            intel=intel,
            snap=snap,
        )
        if _final_quality_block is not None:
            if _final_quality_block.plan is None:
                _final_quality_block = ControlDecision(
                    ok=False,
                    stage=_final_quality_block.stage,
                    reason=_final_quality_block.reason,
                    reason_code=_final_quality_block.reason_code,
                    plan=plan,
                    signal_id=signal_id,
                    ticker=ticker,
                    client_id=client_id,
                )
            elif _final_quality_block.plan.metadata is None:
                _final_quality_block.plan.metadata = plan.metadata or {}
            self._emit_trade_dossier(
                signal,
                client_id=client_id,
                decision_context={
                    "master_control_decision": "REJECT",
                    "decision_reason": _final_quality_block.reason,
                    "block_reason": _final_quality_block.reason,
                    "approved": False,
                    "git_commit": self.git_commit,
                    "config_hash": self.config_hash,
                },
                background=True,
                cache_signal_id=signal_id,
            )
            return _final_quality_block

        # Persist dedup first. Only then add in-memory keys.
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
        self._emit_trade_dossier(
            signal,
            client_id=client_id,
            decision_context={
                "master_control_decision": "APPROVE",
                "decision_reason": "Signal approved by master control",
                "approved": True,
                "git_commit": self.git_commit,
                "config_hash": self.config_hash,
            },
            background=True,
            cache_signal_id=signal_id,
        )

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
                        "scanner_floor": self.admission_thresholds.thresholds.scanner_floor,
                        "interrogation_floor": self.admission_thresholds.thresholds.interrogation_floor,
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
                        "threshold_trace": _threshold_trace,
                        "threshold_config_hash": self.admission_threshold_config_hash,
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

    def _emit_trade_dossier(
        self,
        signal: dict,
        *,
        client_id: str,
        decision_context: Optional[dict] = None,
        background: bool = True,
        cache_signal_id: Optional[str] = None,
        require_db_health: bool = False,
    ) -> None:
        if cache_signal_id:
            self._pop_trade_dossier_signal(cache_signal_id)
        if os.getenv("ENABLE_TRADE_DOSSIER", "true").lower() != "true":
            return
        if background:
            if require_db_health and not self._trade_dossier_db_health_allows_write():
                self._log_trade_dossier_db_unavailable(
                    signal=signal or {},
                    client_id=client_id,
                    err="health_gated",
                )
                return
            self._enqueue_trade_dossier_write(
                signal=signal or {},
                client_id=client_id,
                decision_context=decision_context or {},
                require_db_health=require_db_health,
            )
            return
        self._write_trade_dossier_now(
            signal=signal or {},
            client_id=client_id,
            decision_context=decision_context or {},
        )

    def _start_trade_dossier_worker(self) -> None:
        if os.getenv("ENABLE_TRADE_DOSSIER", "true").lower() != "true":
            return
        with self._trade_dossier_worker_lock:
            if self._trade_dossier_worker and self._trade_dossier_worker.is_alive():
                return
            self._trade_dossier_worker = threading.Thread(
                target=self._trade_dossier_worker_loop,
                daemon=True,
                name=f"trade-dossier-{self._client_id}",
            )
            self._trade_dossier_worker.start()

    def _trade_dossier_worker_loop(self) -> None:
        while True:
            try:
                payload = self._trade_dossier_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                self._process_trade_dossier_payload(payload)
            except Exception:
                pass

    def _process_trade_dossier_payload(self, payload: dict[str, Any]) -> None:
        signal = payload.get("signal") or {}
        client_id = str(payload.get("client_id") or self._client_id)
        decision_context = payload.get("decision_context") or {}
        require_db_health = bool(payload.get("require_db_health"))
        if require_db_health and not self._trade_dossier_db_health_allows_write():
            self._log_trade_dossier_db_unavailable(
                signal=signal,
                client_id=client_id,
                err="health_gated",
            )
            return
        self._write_trade_dossier_now(
            signal=signal,
            client_id=client_id,
            decision_context=decision_context,
        )

    def _enqueue_trade_dossier_write(
        self,
        *,
        signal: dict,
        client_id: str,
        decision_context: dict,
        require_db_health: bool = False,
    ) -> None:
        self._start_trade_dossier_worker()
        try:
            self._trade_dossier_queue.put_nowait(
                {
                    "signal": dict(signal or {}),
                    "client_id": client_id,
                    "decision_context": dict(decision_context or {}),
                    "require_db_health": bool(require_db_health),
                }
            )
        except queue.Full:
            log.warning(
                "trade_dossier_write_skipped_queue_full ticker=%s client_id=%s execution_mode=%s canonical_signal_id=%s",
                (signal or {}).get("ticker") or (signal or {}).get("symbol") or "?",
                client_id,
                "LIVE" if self._is_live_mode() else "PAPER",
                (signal or {}).get("canonical_signal_id") or (signal or {}).get("signal_id") or "",
            )

    def _write_trade_dossier_now(
        self,
        *,
        signal: dict,
        client_id: str,
        decision_context: dict,
    ) -> None:
        try:
            from ap.db import conn
            from ap.trade_dossier import build_and_persist_trade_dossier

            execution_mode = "LIVE" if self._is_live_mode() else "PAPER"
            with conn() as c:
                build_and_persist_trade_dossier(
                    c,
                    signal or {},
                    client_id=client_id,
                    execution_mode=execution_mode,
                    decision_context=decision_context or {},
                )
            self._mark_trade_dossier_db_health(True)
        except Exception as exc:
            self._mark_trade_dossier_db_health(False)
            self._log_trade_dossier_db_unavailable(
                signal=signal or {},
                client_id=client_id,
                err=exc,
            )

    def _mark_trade_dossier_db_health(self, healthy: bool) -> None:
        now_ts = time.time()
        self._trade_dossier_db_healthy = bool(healthy)
        if healthy:
            self._trade_dossier_db_last_ok_ts = now_ts
        else:
            self._trade_dossier_db_last_err_ts = now_ts

    def _trade_dossier_db_health_allows_write(self) -> bool:
        if self._trade_dossier_db_healthy is not True:
            return False
        if self._trade_dossier_db_last_ok_ts is None:
            return False
        return (time.time() - self._trade_dossier_db_last_ok_ts) <= TRADE_DOSSIER_DB_HEALTH_TTL_SEC

    def _log_trade_dossier_db_unavailable(self, *, signal: dict, client_id: str, err: Any) -> None:
        log.warning(
            "trade_dossier_write_skipped_db_unavailable ticker=%s client_id=%s execution_mode=%s canonical_signal_id=%s err=%s",
            (signal or {}).get("ticker") or (signal or {}).get("symbol") or "?",
            client_id,
            "LIVE" if self._is_live_mode() else "PAPER",
            (signal or {}).get("canonical_signal_id") or (signal or {}).get("signal_id") or "",
            err,
        )

    def _cache_trade_dossier_signal(self, signal_id: str, signal: dict) -> None:
        if not signal_id:
            return
        now_ts = time.time()
        self._trade_dossier_signal_cache[signal_id] = dict(signal or {})
        self._trade_dossier_signal_cache_ts[signal_id] = now_ts
        self._prune_trade_dossier_signal_cache(now_ts)

    def _pop_trade_dossier_signal(self, signal_id: str) -> dict[str, Any]:
        cached = dict(self._trade_dossier_signal_cache.pop(signal_id, {}) or {})
        self._trade_dossier_signal_cache_ts.pop(signal_id, None)
        return cached

    def _prune_trade_dossier_signal_cache(self, now_ts: Optional[float] = None) -> None:
        now_ts = now_ts if now_ts is not None else time.time()
        stale_ids = [
            sid for sid, inserted_ts in self._trade_dossier_signal_cache_ts.items()
            if now_ts - inserted_ts >= TRADE_DOSSIER_CACHE_TTL_SEC
        ]
        for sid in stale_ids:
            self._trade_dossier_signal_cache.pop(sid, None)
            self._trade_dossier_signal_cache_ts.pop(sid, None)
        if len(self._trade_dossier_signal_cache) <= TRADE_DOSSIER_CACHE_MAX:
            return
        keep_ids = sorted(
            self._trade_dossier_signal_cache_ts.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:TRADE_DOSSIER_CACHE_MAX]
        keep = {sid for sid, _ in keep_ids}
        self._trade_dossier_signal_cache = {
            sid: sig for sid, sig in self._trade_dossier_signal_cache.items() if sid in keep
        }
        self._trade_dossier_signal_cache_ts = {
            sid: ts for sid, ts in self._trade_dossier_signal_cache_ts.items() if sid in keep
        }

    def _compute_bootstrap_mode(self, *, total_trades: int) -> bool:
        """
        Decide whether bootstrap_mode (qty=1 safety force) is active.

        PR: sizing-bootstrap-fix

        Decision matrix:
          PAPER mode → always False. Paper has no live capital risk; forcing
            qty=1 on every order kills proof-week credibility.
          LIVE mode  → True if total_trades < BOOTSTRAP_TRADES_THRESHOLD
            (default 20). The threshold can be lowered per-deployment via
            env var but cannot be disabled in LIVE — a brand-new LIVE
            service must prove itself with real fills before sizing up.

        Why a method (not inline): centralizes the live-vs-paper decision in
        one auditable place, makes it directly unit-testable, and prevents
        future PRs from re-introducing the silent qty=1 force.
        """
        if self.paper:
            return False
        try:
            threshold = int(os.getenv("BOOTSTRAP_TRADES_THRESHOLD", "20"))
        except (TypeError, ValueError):
            threshold = 20
        return int(total_trades or 0) < threshold

    def _zero_snapshot(self, *, snapshot_ok: bool = True, snapshot_error: str = "") -> dict[str, Any]:
        return {
            "open_count": 0,
            "open_tickers": set(),
            "open_position_ids": [],
            "calls_open": 0,
            "puts_open": 0,
            "capital_deployed": 0.0,
            "position_capital_deployed": 0.0,
            "pending_entry_capital": 0.0,
            "filled_unreconciled_entry_capital": 0.0,
            "pending_entries": 0,
            "entry_attempt_lock_count":    0,
            "entry_attempt_reserved_cost": 0.0,
            "filled_unreconciled_calls":   0,
            "filled_unreconciled_puts":    0,
            "ignored_already_reconciled_fill_capital": 0.0,
            "ignored_already_reconciled_order_ids": [],
            "ignored_reconciled_match_keys": [],
            "watcher_count":  0,
            "pending_exits":  0,
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
                snap = self.pm.snapshot(mode=self.mode.lower())
                if not isinstance(snap, dict):
                    raise TypeError(f"snapshot() returned {type(snap).__name__}, expected dict")
                snap.setdefault("open_count", 0)
                snap.setdefault("open_tickers", set())
                snap.setdefault("open_position_ids", [])
                snap.setdefault("calls_open", 0)
                snap.setdefault("puts_open", 0)
                snap.setdefault("capital_deployed", 0.0)
                snap.setdefault("position_capital_deployed", snap.get("capital_deployed", 0.0))
                snap.setdefault("pending_entry_capital", 0.0)
                snap.setdefault("filled_unreconciled_entry_capital", snap.get("pending_entry_capital", 0.0))
                snap.setdefault("pending_entries", 0)
                snap.setdefault("entry_attempt_lock_count",    0)
                snap.setdefault("entry_attempt_reserved_cost", 0.0)
                snap.setdefault("filled_unreconciled_calls",   0)
                snap.setdefault("filled_unreconciled_puts",    0)
                snap.setdefault("ignored_already_reconciled_fill_capital", 0.0)
                snap.setdefault("ignored_already_reconciled_order_ids", [])
                snap.setdefault("ignored_reconciled_match_keys", [])
                snap.setdefault("watcher_count",  0)
                snap.setdefault("pending_exits",  0)
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
        # PR E / FIX-6: read module-level _INTEL_AVAILABLE / _run_intel_check
        # resolved once at module load. No lazy per-signal import.
        # Fail-open contract preserved: if intel is unavailable or any
        # error occurs, return approved=True so a broken intel layer
        # never blocks signals (intelligence is advisory, not a gate).
        if not _INTEL_AVAILABLE or _run_intel_check is None:
            return {
                "approved": True,
                "score": 0,
                "contracts": 1,
                "reasoning": "intel_unavailable",
                "_available": False,
            }
        try:
            trigger = signal.get("trigger") or {}
            price = (
                signal.get("entry_price")
                or trigger.get("entry")
                or signal.get("current_price")
                or 100.0
            )
            result = _run_intel_check(signal, underlying_price=float(price))
            result["_available"] = True
            return result
        except Exception as e:
            log.debug("Intelligence unavailable: %s", e)
            return {
                "approved": True,
                "score": 0,
                "contracts": 1,
                "reasoning": f"intel_error: {e}",
                "_available": False,
            }

    def _run_final_quality_gates(
        self,
        *,
        signal: dict,
        signal_id: str,
        ticker: str,
        client_id: str,
        plan: ApprovedExecutionPlan,
        intel: dict[str, Any],
        snap: dict[str, Any],
    ) -> Optional[ControlDecision]:
        if not _env_true("FINAL_QUALITY_MODE_ENABLED", True):
            self._store_update(signal_id, "rejected", "final_quality_mode_disabled")
            return self._block(
                signal_id,
                ticker,
                client_id,
                "REJECTED",
                "final_quality_mode_disabled",
                reason_code="FINAL_QUALITY_MODE_DISABLED",
            )

        direction = str(
            getattr(plan, "direction", None)
            or signal.get("direction")
            or signal.get("side")
            or ""
        ).upper()
        timeframe = str(
            getattr(plan, "timeframe", None)
            or signal.get("timeframe")
            or ""
        ).strip().lower()
        trigger_price = getattr(plan, "trigger_price", None)
        stop_price = getattr(plan, "stop_underlying", None)
        target_price = getattr(plan, "target_underlying", None)
        # PR #224 amendment: entry_price must NEVER be used as a current-price
        # fallback. entry_price is the planned/historical entry level, not a
        # live market read. Using it would let stale/no-data signals pass
        # current-price-dependent gates (TARGET_ALREADY_INVALID,
        # REMAINING_OPPORTUNITY_TOO_SMALL) on a fabricated "current" price.
        current_price = (
            signal.get("current_price")
            or signal.get("current_underlying")
            or signal.get("underlying_price")
            or signal.get("last_price")
        )

        try:
            current_price = float(current_price) if current_price is not None else None
        except Exception:
            current_price = None

        if current_price is None:
            # PR #224 amendment: do not fabricate a current price. Record why
            # current-price-dependent gates (TARGET_ALREADY_INVALID,
            # REMAINING_OPPORTUNITY_TOO_SMALL) are being skipped so operators
            # can see the gap rather than have it pass silently.
            if plan.metadata is None:
                plan.metadata = {}
            plan.metadata["final_gate_diagnostics"] = {
                **(plan.metadata.get("final_gate_diagnostics") or {}),
                "current_price_missing": True,
                "current_price_dependent_gates_skipped": [
                    "TARGET_ALREADY_INVALID",
                    "REMAINING_OPPORTUNITY_TOO_SMALL",
                ],
            }

        try:
            trigger_price = float(trigger_price) if trigger_price is not None else None
        except Exception:
            trigger_price = None
        try:
            stop_price = float(stop_price) if stop_price is not None else None
        except Exception:
            stop_price = None
        try:
            target_price = float(target_price) if target_price is not None else None
        except Exception:
            target_price = None

        if direction in {"CALL", "PUT"} and trigger_price and stop_price and target_price:
            _geometry_invalid = (
                (direction == "CALL" and not (stop_price < trigger_price < target_price))
                or (direction == "PUT" and not (target_price < trigger_price < stop_price))
            )
            if _geometry_invalid or _truthy(signal.get("trigger_geometry_invalid")):
                self._store_update(signal_id, "rejected", "trigger_geometry_invalid")
                return self._block(
                    signal_id,
                    ticker,
                    client_id,
                    "REJECTED",
                    "trigger_geometry_invalid",
                    reason_code="TRIGGER_GEOMETRY_INVALID",
                )

        if timeframe in {"1d", "d", "day", "daily", "overnight"} and (
            _truthy(signal.get("stale_daily_plan"))
            or _truthy(signal.get("daily_plan_stale"))
        ):
            self._store_update(signal_id, "rejected", "stale_daily_plan")
            return self._block(
                signal_id,
                ticker,
                client_id,
                "REJECTED",
                "stale_daily_plan",
                reason_code="STALE_DAILY_PLAN",
            )

        if current_price is not None and target_price is not None and direction in {"CALL", "PUT"}:
            _target_invalid = (
                (direction == "CALL" and current_price >= target_price)
                or (direction == "PUT" and current_price <= target_price)
            )
            if _target_invalid or _truthy(signal.get("target_already_invalid")):
                self._store_update(signal_id, "rejected", "target_already_invalid")
                return self._block(
                    signal_id,
                    ticker,
                    client_id,
                    "REJECTED",
                    "target_already_invalid",
                    reason_code="TARGET_ALREADY_INVALID",
                )

        if (
            current_price is not None
            and target_price is not None
            and trigger_price is not None
            and direction in {"CALL", "PUT"}
        ):
            initial_opportunity = abs(target_price - trigger_price)
            remaining_opportunity = abs(target_price - current_price)
            min_remaining_pct = float(os.getenv("FINAL_MIN_REMAINING_OPPORTUNITY_PCT", "0.15"))
            if (
                initial_opportunity > 0
                and remaining_opportunity / initial_opportunity < min_remaining_pct
            ) or _truthy(signal.get("remaining_opportunity_too_small")):
                self._store_update(signal_id, "rejected", "remaining_opportunity_too_small")
                return self._block(
                    signal_id,
                    ticker,
                    client_id,
                    "REJECTED",
                    "remaining_opportunity_too_small",
                    reason_code="REMAINING_OPPORTUNITY_TOO_SMALL",
                )

        risk_detail = (signal.get("risk_detail") or intel.get("risk_detail") or {})
        if (
            risk_detail.get("contract_quality_passes") is False
            or _truthy(signal.get("contract_quality_failed"))
            or _truthy(signal.get("contract_quality_block"))
        ):
            self._store_update(signal_id, "rejected", "contract_quality_block")
            return self._block(
                signal_id,
                ticker,
                client_id,
                "REJECTED",
                "contract_quality_block",
                reason_code="CONTRACT_QUALITY_BLOCK",
            )

        if not self.paper:
            # PR #224 amendment: explicit rollout-safe env gate.
            # FINAL_ENTRY_INTELLIGENCE_REQUIRED defaults to True for live so
            # the existing fail-closed behavior is preserved out of the box.
            # Setting it false is an explicit emergency rollback switch —
            # intel unavailability is then only OBSERVED (logged + recorded
            # in plan.metadata) and does not block the entry.
            intel_required = _env_true("FINAL_ENTRY_INTELLIGENCE_REQUIRED", True)
            if not bool(intel.get("_available")):
                if intel_required:
                    self._store_update(signal_id, "rejected", "entry_intelligence_missing")
                    return self._block(
                        signal_id,
                        ticker,
                        client_id,
                        "REJECTED",
                        "entry_intelligence_missing",
                        reason_code="ENTRY_INTELLIGENCE_MISSING",
                    )
                log.warning(
                    "[%s] ENTRY_INTELLIGENCE_MISSING_OBSERVED — intel unavailable "
                    "but FINAL_ENTRY_INTELLIGENCE_REQUIRED=false (emergency "
                    "rollback active) — proceeding without block",
                    ticker,
                )
                if plan.metadata is None:
                    plan.metadata = {}
                plan.metadata["final_gate_diagnostics"] = {
                    **(plan.metadata.get("final_gate_diagnostics") or {}),
                    "entry_intelligence_missing_observed": True,
                    "reason_code": "ENTRY_INTELLIGENCE_MISSING_OBSERVED",
                }
            else:
                try:
                    intel_score = float(intel.get("intel_score", intel.get("score", 0)) or 0)
                except Exception:
                    intel_score = 0.0
                intel_min_score = float(
                    os.getenv(
                        "FINAL_ENTRY_INTELLIGENCE_MIN_SCORE",
                        os.getenv("INTEL_APPROVE_THRESHOLD", "35.0"),
                    )
                )
                if intel_score < intel_min_score:
                    self._store_update(
                        signal_id,
                        "rejected",
                        f"entry_intelligence_score_too_low:{intel_score:.1f}",
                    )
                    return self._block(
                        signal_id,
                        ticker,
                        client_id,
                        "REJECTED",
                        f"entry_intelligence_score_too_low ({intel_score:.1f}<{intel_min_score:.1f})",
                        reason_code="ENTRY_INTELLIGENCE_SCORE_TOO_LOW",
                    )

        try:
            from ap_hybrid_client_quality_gate import evaluate_client_quality_gate

            if _env_true("HYBRID_CLIENT_QUALITY_MODE", False):
                _hcqg_snap = {
                    "trades_today": int(snap.get("total_trades", 0) or 0),
                    "daily_trades": int(
                        snap.get("daily_trades", 0) or snap.get("total_trades", 0) or 0
                    ),
                    "intraday_trades": int(snap.get("intraday_trades", 0) or 0),
                    "symbol_trades": snap.get("symbol_trades") or {},
                }
                _hcqg = evaluate_client_quality_gate(
                    signal=signal,
                    client_id=client_id,
                    snapshot=_hcqg_snap,
                    live_quote=None,
                    underlying_price=None,
                )
                if plan.metadata is None:
                    plan.metadata = {}
                plan.metadata["hybrid_client_quality_gate"] = _hcqg.to_meta()
                if not _hcqg.allowed:
                    self._store_update(
                        signal_id,
                        "rejected",
                        f"hybrid_client_quality_block: {_hcqg.block_reason}",
                    )
                    return self._block(
                        signal_id,
                        ticker,
                        client_id,
                        "REJECTED",
                        _hcqg.block_reason or "hybrid_client_quality_block",
                        reason_code="HYBRID_CLIENT_QUALITY_BLOCK",
                    )
        except ImportError:
            log.debug("ap_hybrid_client_quality_gate not found — gate skipped (install module)")
        except Exception as _hcqg_err:
            log.error("[%s] HYBRID_GATE ERROR — blocking as safety: %s", ticker, _hcqg_err)
            self._store_update(signal_id, "rejected", "hybrid_client_quality_gate_error")
            return self._block(
                signal_id,
                ticker,
                client_id,
                "REJECTED",
                f"hybrid_gate_error: {_hcqg_err}",
                reason_code="HYBRID_CLIENT_QUALITY_BLOCK",
            )

        return None

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

    def _base_contracts(self, score: float, premium_estimate: float = 0.0) -> int:
        """
        Contract count based on budget, not arbitrary score tiers.

        Score controls what FRACTION of the budget to deploy:
          score 85+  → 100% of budget
          score 75-84 → 80% of budget
          score 60-74 → 65% of budget
          score < 60  → 50% of budget (shouldn't reach here normally)

        Budget is MAX_TRADE_USD (default $1,800 = 10% of account).
        Contracts = floor(budget_fraction × MAX_TRADE_USD / (premium × 100))

        Minimum: 2 contracts (need at least 2 to scale out)
        Maximum: 10 contracts (hard cap for risk control)
        """
        try:
            max_usd = float(os.getenv("MAX_TRADE_USD", "1800"))
        except Exception:
            max_usd = 1800.0

        # Score-based budget fraction
        if score >= 85:
            fraction = 1.00
        elif score >= 75:
            fraction = 0.80
        elif score >= 60:
            fraction = 0.65
        else:
            fraction = 0.50

        effective_budget = max_usd * fraction

        # If we have a premium estimate, use it for contract math
        if premium_estimate and premium_estimate > 0:
            raw = int(effective_budget / (premium_estimate * 100))
        else:
            # Fall back to score tiers as rough estimate when no premium known yet
            if score >= 85: raw = 5
            elif score >= 75: raw = 4
            elif score >= 60: raw = 3
            else: raw = 2

        # Minimum 2 (need 2 to scale out at all), maximum 10
        return max(2, min(10, raw))

    def revalidate_exposure(self, plan, client_id: str = "default") -> ControlDecision:
        """
        Re-check capital-sensitive gates after contract selection has updated
        plan.max_position_usd with real premium. This version logs blocked and
        approved capital-utilization rows before returning.
        """
        try:
            _cf_signal = plan.to_signal_dict() if hasattr(plan, "to_signal_dict") else {}
            _plan_meta = getattr(plan, "metadata", None) or {}
            if isinstance(_plan_meta, dict) and _plan_meta.get("canonical_signal_id"):
                _cf_signal["canonical_signal_id"] = _plan_meta.get("canonical_signal_id")
            _cf_signal["signal_id"] = str(
                getattr(plan, "signal_id", None) or _cf_signal.get("signal_id") or ""
            )
            self._counterfactual_ctx.signal = _cf_signal
            self._counterfactual_ctx.client_id = str(client_id or "default")
            self._counterfactual_ctx.execution_mode = str(getattr(plan, "mode", None) or self._current_mode() or "PAPER")
        except Exception:
            pass
        ticker = plan.ticker
        execution_mode = "live" if self._is_live_mode() else "paper"

        # ---------------------------------------------------------------
        # PR p0/bootstrap-affordable-selection — review fix #1 (2026-06-05):
        # Bootstrap quantity is INVIOLATE = 1. The contract selector
        # (ap/contract_selector.py:1240, 1259) mutates plan.contracts to
        # selected.affordable_contracts — which can be 2, 3, 4+ for cheap
        # tickers. That escape would let a bootstrap trade buy multiple
        # contracts while still under the dollar cap, violating the qty=1
        # bootstrap safety guarantee.
        #
        # We re-read bootstrap_mode from the plan's sizing_context (stamped
        # by evaluate() before selection) and clamp BEFORE computing
        # real_cost. This is the single chokepoint between selector
        # mutation and broker submission, so the clamp is sufficient.
        # ---------------------------------------------------------------
        _plan_meta_for_clamp = getattr(plan, "metadata", None) or {}
        _sizing_ctx          = _plan_meta_for_clamp.get("sizing_context") or {}
        _bootstrap_now       = bool(_sizing_ctx.get("bootstrap_mode", False))
        if _bootstrap_now and self._is_live_mode():
            _selector_meta = getattr(plan, "selector_metadata", None) or {}
            _ppc = _selector_meta.get("premium_per_contract")
            if _ppc is None:
                # Fall back to the per-share execution price * 100 if the
                # selector didn't surface premium_per_contract. Either way,
                # plan.contracts is forced to 1.
                _exec_pps = getattr(plan, "selector_execution_price", None)
                if _exec_pps is not None:
                    _ppc = float(_exec_pps) * 100.0
            if _ppc is not None and _ppc > 0:
                if plan.contracts != 1:
                    log.info(
                        "[%s] bootstrap_clamp: selector wanted %s contracts, "
                        "forcing to 1 (premium_per_contract=$%.2f)",
                        ticker, plan.contracts, float(_ppc),
                    )
                plan.contracts = 1
                plan.max_position_usd = float(_ppc)  # 1 × premium_per_contract
            else:
                # Defensive: if the selector didn't populate premium info,
                # still clamp the qty so we never submit > 1 contract in
                # bootstrap. The capital revalidation below will use the
                # current plan.max_position_usd as-is.
                if plan.contracts != 1:
                    log.warning(
                        "[%s] bootstrap_clamp: forcing plan.contracts=1 but "
                        "selector premium_per_contract missing — max_position_usd "
                        "left at $%.2f for revalidation",
                        ticker, float(plan.max_position_usd),
                    )
                plan.contracts = 1

        real_cost = float(plan.max_position_usd)
        # PR E FIX-3 (reader-side patch): take atomic snapshot of
        # (account_equity, max_daily_loss) under _equity_lock.
        # revalidate_exposure uses equity for capital / sector / ticker
        # caps; max_daily_loss is not used here but the snapshot is the
        # consistent reader API. The lock is NEVER held across
        # _get_snapshot / _pending_capital_from_snapshot_or_db / DB calls.
        equity, _ = self._equity_snapshot()
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

        # PR E / FIX-1 (BUG-MC-1): if the OSM row for this plan already
        # exists (queue.py stashed the local_order_id on plan.metadata
        # after create_entry_order), pass exclude_local_order_id to the
        # pending-capital helper so the SUM does NOT include the current
        # plan's reserved_cost. Without this exclusion, the plan's cost
        # is double-counted: once via the SUM and again via the `+ real_cost`
        # addition below.
        _plan_metadata = getattr(plan, "metadata", None)
        _exclude_local_order_id: Optional[str] = None
        if isinstance(_plan_metadata, dict):
            _lo = _plan_metadata.get("local_order_id")
            if _lo:
                _exclude_local_order_id = str(_lo)
        # PR #121 amend: revalidate_exposure runs the same capital math as
        # evaluate() but after the selector. Pass runtime_execution_mode so
        # the broker-proof pending sum matches what the gate computed earlier.
        pending_cap = self._pending_capital_from_snapshot_or_db(
            snap, client_id,
            exclude_local_order_id=_exclude_local_order_id,
            runtime_execution_mode=str(self._current_mode() or '').lower(),
        )
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
        # PR #155 — split-cap revalidate_exposure: two independent gates.
        #
        # Gate 1 — per-position cap:
        #   Fires when: real_cost > per_trade_budget
        #   Resize headroom: per_trade_budget  (independent of deployed/pending)
        #   Reason: ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP
        #
        # Gate 2 — total exposure cap:
        #   Fires when: projected_total_exposure > total_capital_cap
        #   Resize headroom: remaining_total_capacity = total_cap - deployed - pending
        #   Reason: CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED (at/over limit)
        #            ACTUAL_CONTRACT_COST_EXCEEDS_REMAINING_TOTAL_CAPACITY (near limit)
        #
        # CRITICAL: do NOT compare projected_total_exposure to per_trade_budget.
        # That apples-vs-oranges comparison is what produced the false block on
        # second trades (proj_total=366 > per_trade_cap=198 when deployed=183,
        # real_cost=183) and the $15 resize bug (_remaining = 198 - 183 = 15).

        per_trade_budget         = equity * self.max_position_pct
        total_capital_cap        = equity * self.max_total_capital_pct
        current_total_exposure   = snap["capital_deployed"] + pending_cap   # excludes new real_cost
        projected_total_exposure = current_total_exposure + real_cost        # includes new real_cost
        remaining_total_capacity = total_capital_cap - current_total_exposure
        # max_capital alias for _log_capital_utilization (expects a single limit field).
        # Set to per_trade_budget — the primary per-position cap.
        max_capital = per_trade_budget
        pct_used = projected_total_exposure / equity * 100 if equity > 0 else 0

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
                execution_mode=execution_mode,
                ticker=ticker,
                signal_id=signal_id,
                deployed=snap["capital_deployed"],
                pending=pending_cap,
                new_cost=real_cost,
                projected=projected_total_exposure,
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

        # ── PR #155 split-cap revalidate_exposure ─────────────────────────────
        # Two independent gates, each with the correct headroom and reason code.
        # Do NOT compare projected_total_exposure to per_trade_budget — that is
        # the apples-vs-oranges bug that caused the false-block on second trades.
        #
        # Gate 1 — per-position cap:
        #   Question: does this one trade cost more than the per-trade budget?
        #   Headroom for resize: per_trade_budget
        #   Reason code: ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP
        #
        # Gate 2 — total exposure cap:
        #   Question: does adding this trade push total portfolio exposure over the cap?
        #   Headroom for resize: remaining_total_capacity = total_cap - deployed - pending
        #   Reason code: ACTUAL_CONTRACT_COST_EXCEEDS_REMAINING_TOTAL_CAPACITY
        #
        # Resize uses the headroom appropriate to the gate that fired.
        # A trade can be resized by Gate 1 and then still fail Gate 2 (edge case:
        # resized trade fits per-trade budget but total cap is almost full).
        # ─────────────────────────────────────────────────────────────────────

        # ── Gate 1: per-position cap ────────────────────────────────────────
        _resized_for_affordable_qty = False
        if real_cost > per_trade_budget:
            if plan.contracts > 0 and real_cost > 0:
                _original_qty_g1      = int(plan.contracts)
                _cost_per_contract_g1 = real_cost / _original_qty_g1
                # Resize headroom is per_trade_budget — the single-trade limit.
                # This is independent of how much is already deployed.
                _remaining_g1         = per_trade_budget
                _computed_qty_g1      = (
                    int(_remaining_g1 // _cost_per_contract_g1)
                    if _cost_per_contract_g1 > 0 else 0
                )
                _candidate_limit_g1   = _cost_per_contract_g1 / 100.0

                if _computed_qty_g1 >= 1:
                    _final_qty_g1         = min(_original_qty_g1, _computed_qty_g1)
                    plan.contracts        = _final_qty_g1
                    plan.max_position_usd = float(_final_qty_g1 * _cost_per_contract_g1)
                    real_cost             = plan.max_position_usd
                    projected_total_exposure = current_total_exposure + real_cost
                    pct_used              = projected_total_exposure / equity * 100 if equity > 0 else 0
                    proj_sector           = sector_deployed + real_cost
                    proj_ticker           = ticker_deployed + real_cost
                    _resized_for_affordable_qty = True
                    _bd_g1 = self._get_pending_capital_breakdown(snap, client_id) or {}
                    log.info(
                        "[%s] SMALL_ACCOUNT_AFFORDABLE_QTY_RESIZE gate=per_position_cap "
                        "client_email=%s execution_mode=%s "
                        "per_trade_budget=%.0f total_capital_cap=%.0f "
                        "current_total_exposure=%.0f remaining_total_capacity=%.0f "
                        "capital_deployed=%.0f pending=%.0f "
                        "pending_submitted_entry_exposure=%.0f "
                        "filled_unreconciled_exposure=%.0f "
                        "candidate_limit=%.4f computed_qty=%d original_qty=%d "
                        "final_qty=%d",
                        ticker, client_id, execution_mode,
                        per_trade_budget, total_capital_cap,
                        current_total_exposure, remaining_total_capacity,
                        float(snap.get("capital_deployed", 0)), pending_cap,
                        _bd_g1.get("pending_submitted_entry_exposure", 0.0),
                        _bd_g1.get("filled_unreconciled_exposure", 0.0),
                        _candidate_limit_g1, _computed_qty_g1, _original_qty_g1, _final_qty_g1,
                    )
                else:
                    _bd_g1 = self._get_pending_capital_breakdown(snap, client_id) or {}
                    reason = (
                        f"ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP "
                        f"client_email={client_id} execution_mode={execution_mode} "
                        f"real_cost=${real_cost:.0f} per_trade_budget=${per_trade_budget:.0f} "
                        f"total_capital_cap=${total_capital_cap:.0f} "
                        f"current_total_exposure=${current_total_exposure:.0f} "
                        f"remaining_total_capacity=${remaining_total_capacity:.0f} "
                        f"capital_deployed=${snap['capital_deployed']:.0f} "
                        f"pending=${pending_cap:.0f} "
                        f"pending_submitted_entry_exposure="
                        f"${_bd_g1.get('pending_submitted_entry_exposure', 0.0):.0f} "
                        f"candidate_limit={_candidate_limit_g1:.4f} "
                        f"computed_qty={_computed_qty_g1} original_qty={_original_qty_g1}"
                    )
                    log.warning(
                        "[%s] SMALL_ACCOUNT_CONTRACT_UNAFFORDABLE gate=per_position_cap "
                        "real_cost=%.0f per_trade_budget=%.0f computed_qty=%d",
                        ticker, real_cost, per_trade_budget, _computed_qty_g1,
                    )
                    _log_revalidation(True, reason)
                    return self._block(
                        signal_id, ticker, client_id, "blocked_risk", reason,
                        reason_code="ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP",
                    )

        # ── Gate 2: total exposure cap ──────────────────────────────────────
        if projected_total_exposure > total_capital_cap:
            if plan.contracts > 0 and real_cost > 0:
                _original_qty_g2      = int(plan.contracts)
                _cost_per_contract_g2 = real_cost / _original_qty_g2
                # Resize headroom is remaining_total_capacity — how much portfolio
                # room is left before hitting the total exposure ceiling.
                _remaining_g2         = max(0.0, remaining_total_capacity)
                _computed_qty_g2      = (
                    int(_remaining_g2 // _cost_per_contract_g2)
                    if _cost_per_contract_g2 > 0 else 0
                )
                _candidate_limit_g2   = _cost_per_contract_g2 / 100.0

                if _computed_qty_g2 >= 1:
                    _final_qty_g2         = min(_original_qty_g2, _computed_qty_g2)
                    plan.contracts        = _final_qty_g2
                    plan.max_position_usd = float(_final_qty_g2 * _cost_per_contract_g2)
                    real_cost             = plan.max_position_usd
                    projected_total_exposure = current_total_exposure + real_cost
                    pct_used              = projected_total_exposure / equity * 100 if equity > 0 else 0
                    proj_sector           = sector_deployed + real_cost
                    proj_ticker           = ticker_deployed + real_cost
                    _resized_for_affordable_qty = True
                    _bd_g2 = self._get_pending_capital_breakdown(snap, client_id) or {}
                    log.info(
                        "[%s] SMALL_ACCOUNT_AFFORDABLE_QTY_RESIZE gate=total_exposure_cap "
                        "client_email=%s execution_mode=%s "
                        "per_trade_budget=%.0f total_capital_cap=%.0f "
                        "current_total_exposure=%.0f remaining_total_capacity=%.0f "
                        "capital_deployed=%.0f pending=%.0f "
                        "pending_submitted_entry_exposure=%.0f "
                        "filled_unreconciled_exposure=%.0f "
                        "candidate_limit=%.4f computed_qty=%d original_qty=%d "
                        "final_qty=%d",
                        ticker, client_id, execution_mode,
                        per_trade_budget, total_capital_cap,
                        current_total_exposure, remaining_total_capacity,
                        float(snap.get("capital_deployed", 0)), pending_cap,
                        _bd_g2.get("pending_submitted_entry_exposure", 0.0),
                        _bd_g2.get("filled_unreconciled_exposure", 0.0),
                        _candidate_limit_g2, _computed_qty_g2, _original_qty_g2, _final_qty_g2,
                    )
                else:
                    _bd_g2 = self._get_pending_capital_breakdown(snap, client_id) or {}
                    _is_total_cap_reached = remaining_total_capacity <= 0.0
                    reason = (
                        f"{'CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED' if _is_total_cap_reached else 'ACTUAL_CONTRACT_COST_EXCEEDS_REMAINING_TOTAL_CAPACITY'} "
                        f"client_email={client_id} execution_mode={execution_mode} "
                        f"projected_total_exposure=${projected_total_exposure:.0f} "
                        f"total_capital_cap=${total_capital_cap:.0f} "
                        f"per_trade_budget=${per_trade_budget:.0f} "
                        f"current_total_exposure=${current_total_exposure:.0f} "
                        f"remaining_total_capacity=${remaining_total_capacity:.0f} "
                        f"real_cost=${real_cost:.0f} "
                        f"capital_deployed=${snap['capital_deployed']:.0f} "
                        f"pending=${pending_cap:.0f} "
                        f"pending_submitted_entry_exposure="
                        f"${_bd_g2.get('pending_submitted_entry_exposure', 0.0):.0f} "
                        f"computed_qty={_computed_qty_g2} original_qty={_original_qty_g2}"
                    )
                    reason_code = (
                        "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED"
                        if _is_total_cap_reached
                        else "ACTUAL_CONTRACT_COST_EXCEEDS_REMAINING_TOTAL_CAPACITY"
                    )
                    log.warning(
                        "[%s] TOTAL_EXPOSURE_CAP_BLOCK gate=total_exposure_cap "
                        "projected_total=%.0f total_cap=%.0f remaining=%.0f computed_qty=%d",
                        ticker, projected_total_exposure, total_capital_cap,
                        remaining_total_capacity, _computed_qty_g2,
                    )
                    _log_revalidation(True, reason)
                    return self._block(
                        signal_id, ticker, client_id, "blocked_risk", reason,
                        reason_code=reason_code,
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
            "[%s] Re-validation passed | real_cost=$%.0f "
            "projected_total=$%.0f/total_cap=$%.0f "
            "per_trade_budget=$%.0f remaining_total_capacity=$%.0f "
            "sector_%s=$%.0f/%.0f ticker=$%.0f/%.0f",
            ticker,
            real_cost,
            projected_total_exposure,
            total_capital_cap,
            per_trade_budget,
            remaining_total_capacity,
            sector,
            proj_sector,
            max_sector,
            proj_ticker,
            max_ticker,
        )
        return ControlDecision(ok=True, stage="revalidated", signal_id=signal_id, ticker=ticker, client_id=client_id)

    def _block(self, signal_id, ticker, client_id, stage, reason, reason_code: str = "",
               meta: Optional[dict] = None) -> ControlDecision:
        """
        Centralized block emitter.

        PR #121: meta is an optional dict carried as `context` into the
        decision event. Used by capital_limit_no_remaining to surface
        counted_order_ids / ignored_reserved_cost_by_status / etc into
        result_json so operators can audit pending-exposure rejections.
        """
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
                    context=(meta or None),
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
        try:
            dossier_signal = self._pop_trade_dossier_signal(signal_id)
            if not dossier_signal:
                dossier_signal = {"signal_id": signal_id, "ticker": ticker, "symbol": ticker}
            dossier_signal.setdefault("signal_id", signal_id)
            dossier_signal.setdefault("ticker", ticker)
            dossier_signal.setdefault("symbol", ticker)
            self._emit_trade_dossier(
                dossier_signal,
                client_id=client_id,
                decision_context={
                    "master_control_decision": "REJECT",
                    "decision_reason": reason,
                    "block_reason": reason,
                    "approved": False,
                    "git_commit": self.git_commit,
                    "config_hash": self.config_hash,
                },
                background=True,
                require_db_health=True,
            )
            if track_counterfactual_signal:
                _cf_signal = getattr(self._counterfactual_ctx, "signal", None) or {
                    "signal_id": signal_id,
                    "ticker": ticker,
                }
                _cf_signal["signal_id"] = str(_cf_signal.get("signal_id") or signal_id)
                track_counterfactual_signal(
                    signal=_cf_signal,
                    client_id=str(getattr(self._counterfactual_ctx, "client_id", client_id) or client_id),
                    execution_mode=str(getattr(self._counterfactual_ctx, "execution_mode", self._current_mode()) or "PAPER"),
                    block_stage=stage,
                    block_reason=reason,
                    reason_code=reason_code,
                    source="block",
                )
        except Exception:
            pass
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
        execution_mode: str,
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
            "execution_mode": execution_mode,
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
            "[%s] CAPITAL_UTIL | mode=%s | %s | deployed=$%.0f pending=$%.0f new=$%.0f projected=$%.0f/%.0f (%.1f%%) headroom=$%.0f | %s",
            client_id,
            execution_mode,
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
        self._trade_dossier_signal_cache.clear()
        self._trade_dossier_signal_cache_ts.clear()
        # PR E / FIX-4: clear _trade_cooldowns under _cooldown_lock so a new
        # trading day starts with no stale cooldowns AND the clear is atomic
        # w.r.t. any concurrent set_cooldown / execution-core writes.
        with self._cooldown_lock:
            self._trade_cooldowns.clear()
        # P0-3: clear force-close breaker on new session so the new trading day
        # starts fresh. Without this, a -$500 day would leave the breaker tripped
        # forever and the bot would close every position the next morning before
        # any trades could be made.
        self.clear_force_close(reason="reset_session")
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
