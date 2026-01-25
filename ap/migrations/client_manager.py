# ap/client_manager.py
import uuid
from ap.db import conn, run_with_retry, create_client, get_client, update_client, get_all_clients
from ap.crypto import encrypt_token, decrypt_token
from ap.auth import generate_api_key
from ap.logger import get_logger
from ap.brokers.tradier import TradierBroker, TradierConfig

log = get_logger("ap.client_manager")


def create_new_client(
    name: str,
    tradier_account_id: str,
    tradier_access_token: str,
    tradier_base_url: str = "https://sandbox.tradier.com",
    initial_equity: float = 100000.0,
    max_trades_per_day: int = 5,
    max_concurrent_positions: int = 3,
    daily_max_loss_pct: float = 0.05,
    base_position_pct: float = 0.10,
) -> dict:
    client_id = f"client_{uuid.uuid4().hex[:12]}"
    api_key = generate_api_key()

    encrypted_token = encrypt_token(tradier_access_token)

    create_client(
        client_id=client_id,
        name=name,
        broker_type="tradier",
        broker_account_id=tradier_account_id,
        broker_token=encrypted_token,
        broker_base_url=tradier_base_url,
        initial_equity=initial_equity,
        max_trades_per_day=max_trades_per_day,
        max_concurrent_positions=max_concurrent_positions,
        daily_max_loss_pct=daily_max_loss_pct,
        base_position_pct=base_position_pct,
    )

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
        "broker_base_url": tradier_base_url,
        "initial_equity": initial_equity,
        "status": "ACTIVE",
        "max_trades_per_day": max_trades_per_day,
        "max_concurrent_positions": max_concurrent_positions,
        "daily_max_loss_pct": daily_max_loss_pct,
        "base_position_pct": base_position_pct,
    }


def get_client_broker(client_id: str):
    client = get_client(client_id)
    if client["status"] != "ACTIVE":
        raise ValueError(f"Client {client_id} is not ACTIVE")

    if client["broker_type"] != "tradier":
        raise NotImplementedError(f"Unsupported broker_type: {client['broker_type']}")

    access_token = decrypt_token(client["broker_token"])

    return TradierBroker(TradierConfig(
        base_url=client["broker_base_url"],
        access_token=access_token,
        account_id=client["broker_account_id"]
    ))


def list_all_clients(status: str = None) -> list:
    clients = get_all_clients(status)
    safe = []
    for c in clients:
        safe.append({
            "client_id": c["client_id"],
            "name": c["name"],
            "status": c["status"],
            "broker_type": c["broker_type"],
            "broker_account_id": c["broker_account_id"],
            "broker_base_url": c["broker_base_url"],
            "initial_equity": c["initial_equity"],
            "max_trades_per_day": c["max_trades_per_day"],
            "max_concurrent_positions": c["max_concurrent_positions"],
            "daily_max_loss_pct": c["daily_max_loss_pct"],
            "base_position_pct": c["base_position_pct"],
            "created_at": c["created_at"],
        })
    return safe


def update_client_status(client_id: str, status: str) -> dict:
    if status not in ("ACTIVE", "PAUSED", "CLOSED"):
        raise ValueError("status must be ACTIVE|PAUSED|CLOSED")
    return update_client(client_id, status=status)


def regenerate_api_key(client_id: str) -> str:
    new_key = generate_api_key()
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "UPDATE clients SET api_key=? WHERE client_id=?",
            (new_key, client_id)
        ))
    return new_key
