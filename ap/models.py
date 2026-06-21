from pydantic import BaseModel, Field
from typing import Literal, Optional, Dict, Any

Mode = Literal["SIM", "PAPER", "LIVE", "READ_ONLY"]


class Signal(BaseModel):
    signal_id: str
    symbol: str
    ticker: Optional[str] = None       # alias for symbol -- standardize at model level
    score: float = 65.0                # signal confidence score -- default B tier (65)
    ev_score: Optional[float] = None   # scanner ev_score -- passed through for live mode gate
    tier: Optional[str] = None         # optional pre-classified tier from scanner
    side: Optional[Literal["CALL", "PUT"]] = None
    direction: Literal["CALL", "PUT"]
    pattern: Optional[str] = None
    pattern_id: str
    timeframe: Optional[str] = None
    confidence_tag: Literal["elite_pool", "standard_pool"] = "standard_pool"
    timestamp_iso: str
    entry_price: Optional[float] = None
    entry_trigger: Optional[float] = None
    stop_price: Optional[float] = None
    target_price: Optional[float] = None
    prior_day_high: Optional[float] = None
    prior_day_low: Optional[float] = None
    signal_bar_date: Optional[str] = None
    generated_at: Optional[str] = None
    trigger: Dict[str, Any] = Field(default_factory=dict)

    def __init__(self, **data):
        # Normalize: ensure ticker mirrors symbol when not explicitly set.
        if not data.get("ticker") and data.get("symbol"):
            data["ticker"] = data["symbol"]
        if not data.get("symbol") and data.get("ticker"):
            data["symbol"] = data["ticker"]

        # Preserve explicit zero while still defaulting missing scanner ev_score.
        if data.get("ev_score") is None:
            data["ev_score"] = data.get("score", 65.0)

        # Maintain side/direction compatibility for scanner payloads.
        if not data.get("direction") and data.get("side"):
            data["direction"] = data["side"]
        if not data.get("side") and data.get("direction"):
            data["side"] = data["direction"]

        # Maintain pattern/pattern_id compatibility.
        if not data.get("pattern_id") and data.get("pattern"):
            data["pattern_id"] = data["pattern"]
        if not data.get("pattern") and data.get("pattern_id"):
            data["pattern"] = data["pattern_id"]

        # Preserve scanner-derived trigger levels and synthesize trigger payload
        # so downstream queue/watcher paths keep the night-before breach level.
        trigger = dict(data.get("trigger") or {})
        derived_entry = (
            data.get("entry_trigger")
            if data.get("entry_trigger") is not None
            else data.get("entry_price")
        )
        if trigger.get("entry") is None and derived_entry is not None:
            trigger["entry"] = derived_entry
        if trigger.get("stop") is None and data.get("stop_price") is not None:
            trigger["stop"] = data.get("stop_price")
        if trigger.get("target") is None and data.get("target_price") is not None:
            trigger["target"] = data.get("target_price")
        if trigger.get("pt1") is None and data.get("target_price") is not None:
            trigger["pt1"] = data.get("target_price")
        data["trigger"] = trigger

        if data.get("entry_price") is None and data.get("entry_trigger") is not None:
            data["entry_price"] = data.get("entry_trigger")
        if data.get("entry_trigger") is None and data.get("entry_price") is not None:
            data["entry_trigger"] = data.get("entry_price")

        super().__init__(**data)


class Quote(BaseModel):
    bid: float
    ask: float
    mid: float
    ts_iso: str


class OrderPlan(BaseModel):
    plan_id: str
    symbol: str
    contract: str
    direction: Literal["CALL", "PUT"]
    qty: int
    position_pct: float
    tp_pct: float
    sl_pct: float
    limit_price: Optional[float] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class GateResult(BaseModel):
    ok: bool
    reason: str = ""
    details: Dict[str, Any] = Field(default_factory=dict)
    # =========================
# MULTI-CLIENT MODELS
# =========================


class ClientConfig(BaseModel):
    """Client configuration"""
    client_id: str
    name: str
    broker_type: str  # 'tradier' or 'ibkr'
    broker_account_id: str
    broker_token: str  # Encrypted in production!
    broker_base_url: str = "https://sandbox.tradier.com"
    initial_equity: float
    status: str = "ACTIVE"  # ACTIVE | PAUSED | CLOSED
    created_at: str

    # Risk limits per client
    max_trades_per_day: int = 5
    max_concurrent_positions: int = 3
    daily_max_loss_pct: float = 0.05
    base_position_pct: float = 0.10


class ClientState(BaseModel):
    """Per-client state tracking"""
    client_id: str
    current_equity: float
    starting_equity_today: float
    realized_pnl_today: float
    trades_taken_today: int
    daily_stop_hit: bool
    kill_switch: bool
    mode: str  # SIM | PAPER | LIVE | READ_ONLY
    last_heartbeat_ts: str
