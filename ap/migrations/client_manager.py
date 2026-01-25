# ap/client_manager.py
"""
Client management and broker factory
Creates per-client broker instances with their credentials
"""
import uuid
from ap.db import conn, run_with_retry, create_client, get_client, update_client
from ap.crypto import encrypt_token, decrypt_token
from ap.auth import generate_api_key
from ap.logger import get_logger
from ap.brokers.tradier import TradierBroker, TradierConfig
from ap.utils import now_utc_iso

log = get_logger("ap.client_manager")


def create_new_client(
    name: str,
    tradier_account_id: str,
    tradier_access_token: str,
    tradier_base_url: str = "https://sandbox.tradier.com",
    initial_equity: float = 100000.0,
    **risk_params
) -> dict:
    """
    Create a new client with encrypted credentials
    
    Args:
        name: Client name
        tradier_account_id: Their Tradier account ID
        tradier_access_token: Their Tradier access token (will be encrypted)
        tradier_base_url: Tradier API URL (sandbox or production)
        initial_equity: Starting equity
        **risk_params: max_trades_per_day, max_concurrent_positions, etc.
    
    Returns:
        {
            "client_id": "...",
            "api_key": "ak_live_...",
            "name": "...",
            ...
        }
    """
    # Generate IDs
    client_id = f"client_{uuid.uuid4().hex[:12]}"
    api_key = generate_api_key()
    
    # Encrypt the Tradier token
    encrypted_token = encrypt_token(tradier_access_token)
    
    # Create client in database
    client = create_client(
        client_id=client_id,
        name=name,
        broker_type="tradier",
        broker_account_id=tradier_account_id,
        broker_token=encrypted_token,  # Store encrypted!
        broker_base_url=tradier_base_url,
        initial_equity=initial_equity,
        **risk_params
    )
    
    # Add API key to client record
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "UPDATE clients SET api_key=? WHERE client_id=?",
            (api_key, client_id)
        ))
    
    log.info(f"✅ Created client: {client_id} ({name})")
    
    return {
        "client_id": client_id,
        "api_key": api_key,
        "name": name,
        "broker_type": "tradier",
        "broker_account_id": tradier_account_id,
        "initial_equity": initial_equity,
        "status": "ACTIVE",
        "created_at": client["created_at"]
    }


def get_client_broker(client_id: str):
    """
    Get a broker instance for a specific client
    Decrypts their credentials and creates Tradier broker
    
    Returns: TradierBroker instance configured for this client
    """
    try:
        client = get_client(client_id)
        
        if client["status"] != "ACTIVE":
            raise ValueError(f"Client {client_id} is not active (status: {client['status']})")
        
        if client["broker_type"] != "tradier":
            raise NotImplementedError(f"Broker type {client['broker_type']} not yet supported")
        
        # Decrypt the access token
        access_token = decrypt_token(client["broker_token"])
        
        # Create Tradier broker instance
        broker = TradierBroker(TradierConfig(
            base_url=client["broker_base_url"],
            access_token=access_token,
            account_id=client["broker_account_id"]
        ))
        
        log.debug(f"Created broker instance for client {client_id}")
        
        return broker
        
    except Exception as e:
        log.error(f"Failed to create broker for client {client_id}: {e}")
        raise


def list_all_clients(status: str = None) -> list:
    """
    List all clients (admin only)
    Does NOT return encrypted tokens or API keys
    """
    from ap.db import get_all_clients
    
    clients = get_all_clients(status)
    
    # Strip sensitive data
    safe_clients = []
    for c in clients:
        safe_clients.append({
            "client_id": c["client_id"],
            "name": c["name"],
            "broker_type": c["broker_type"],
            "broker_account_id": c["broker_account_id"],
            "initial_equity": c["initial_equity"],
            "status": c["status"],
            "created_at": c["created_at"],
            "max_trades_per_day": c["max_trades_per_day"],
            "max_concurrent_positions": c["max_concurrent_positions"],
            "daily_max_loss_pct": c["daily_max_loss_pct"],
            "base_position_pct": c["base_position_pct"]
        })
    
    return safe_clients


def update_client_status(client_id: str, status: str) -> dict:
    """
    Update client status (ACTIVE, PAUSED, CLOSED)
    """
    if status not in ("ACTIVE", "PAUSED", "CLOSED"):
        raise ValueError(f"Invalid status: {status}")
    
    updated = update_client(client_id, status=status)
    
    log.info(f"Updated client {client_id} status to {status}")
    
    return updated


def regenerate_api_key(client_id: str) -> str:
    """
    Regenerate API key for a client (e.g., if compromised)
    """
    new_api_key = generate_api_key()
    
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "UPDATE clients SET api_key=? WHERE client_id=?",
            (new_api_key, client_id)
        ))
    
    log.warning(f"⚠️ Regenerated API key for client {client_id}")
    
    return new_api_key
