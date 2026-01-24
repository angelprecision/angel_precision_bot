from pydantic import BaseModel, Field
from typing import Literal, Optional, Dict, Any

Mode = Literal["SIM", "PAPER", "LIVE", "READ_ONLY"]

class Signal(BaseModel):
    signal_id: str
    symbol: str
    direction: Literal["CALL", "PUT"]
    pattern_id: str
    confidence_tag: Literal["elite_pool", "standard_pool"] = "standard_pool"
    timestamp_iso: str
    trigger: Dict[str, Any] = Field(default_factory=dict)

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

