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

