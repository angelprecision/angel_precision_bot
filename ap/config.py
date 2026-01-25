import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    # Database
    DB_FILE: str = os.getenv("BOT_DB_FILE", "ap_state.db")
    
    # Bot mode
    BOT_MODE: str = os.getenv("BOT_MODE", "SIM").upper()
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
    
    # Trading limits
    MAX_TRADES_PER_DAY: int = int(os.getenv("MAX_TRADES_PER_DAY", "4"))
    MAX_CONCURRENT_POSITIONS: int = int(os.getenv("MAX_CONCURRENT_POSITIONS", "2"))
    BASE_POSITION_PCT: float = float(os.getenv("BASE_POSITION_PCT", "0.15"))
    
    # Risk parameters
    TAKE_PROFIT_PCT: float = float(os.getenv("TAKE_PROFIT_PCT", "0.30"))
    STOP_LOSS_PCT: float = float(os.getenv("STOP_LOSS_PCT", "0.50"))
    DAILY_MAX_LOSS_PCT: float = float(os.getenv("DAILY_MAX_LOSS_PCT", "0.06"))
    
    # Growth throttles
    GROWTH_THROTTLE_50: float = float(os.getenv("GROWTH_THROTTLE_50", "0.50"))
    GROWTH_THROTTLE_75: float = float(os.getenv("GROWTH_THROTTLE_75", "0.75"))
    GROWTH_THROTTLE_90: float = float(os.getenv("GROWTH_THROTTLE_90", "0.90"))
    
    # Reduced risk at growth thresholds
    REDUCED_POSITION_PCT_50: float = float(os.getenv("REDUCED_POSITION_PCT_50", "0.12"))
    REDUCED_MAX_TRADES_75: int = int(os.getenv("REDUCED_MAX_TRADES_75", "2"))
    
    # Drawdown protection
    DRAWDOWN_STOP_DAY: float = float(os.getenv("DRAWDOWN_STOP_DAY", "0.12"))
    DRAWDOWN_KILL: float = float(os.getenv("DRAWDOWN_KILL", "0.20"))
    
    # Trading hours
    ENTRY_START_ET: str = os.getenv("ENTRY_START_ET", "09:35")
    ENTRY_END_ET: str = os.getenv("ENTRY_END_ET", "13:30")
    NO_ENTRY_LAST_MINUTES: int = int(os.getenv("NO_ENTRY_LAST_MINUTES", "10"))
    COOLDOWN_AFTER_EXIT_MINUTES: int = int(os.getenv("COOLDOWN_AFTER_EXIT_MINUTES", "3"))
    
    # Quote quality
    MAX_SPREAD_PCT: float = float(os.getenv("MAX_SPREAD_PCT", "0.10"))
    MAX_QUOTE_AGE_SECONDS: int = int(os.getenv("MAX_QUOTE_AGE_SECONDS", "5"))
    
    # Tradier credentials (for default client and backward compatibility)
    TRADIER_ACCOUNT_ID: str = os.getenv("TRADIER_ACCOUNT_ID", "")
    TRADIER_ACCESS_TOKEN: str = os.getenv("TRADIER_ACCESS_TOKEN", "")
    TRADIER_BASE_URL: str = os.getenv("TRADIER_BASE_URL", "https://sandbox.tradier.com")
