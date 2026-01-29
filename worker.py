# worker.py - Background worker that processes queue (AUTO-BOOTSTRAP)

import os
import sys

from ap.queue import worker_loop
from ap.client_manager import get_client_broker
from ap.logger import get_logger
from ap.db import init_db, get_client, create_client

log = get_logger("worker")

def ensure_client_exists(client_id: str):
    try:
        get_client(client_id)
        return
    except Exception:
        pass

    # Pull broker credentials from env (set these in Render Worker env)
    broker_type = os.getenv("BROKER_TYPE", "tradier")
    broker_account_id = os.getenv("BROKER_ACCOUNT_ID", "demo")
    broker_token = os.getenv("BROKER_TOKEN", "demo")
    broker_base_url = os.getenv("BROKER_BASE_URL", "https://sandbox.tradier.com")
    initial_equity = float(os.getenv("INITIAL_EQUITY", "5000"))

    log.warning(f"⚠️ Client '{client_id}' missing — creating from env vars")

    create_client(
        client_id=client_id,
        name=f"{client_id} Client",
        broker_type=broker_type,
        broker_account_id=broker_account_id,
        broker_token=broker_token,
        broker_base_url=broker_base_url,
        initial_equity=initial_equity,
    )

    log.info(f"✅ Client '{client_id}' created")


if __name__ == "__main__":
    log.info("🤖 Worker starting...")

    # Use env var; default to "default"
    client_id = os.getenv("BOT_CLIENT_ID", "default").strip() or "default"

    try:
        # ✅ Always create tables first
        init_db()

        # ✅ Ensure the client exists
        ensure_client_exists(client_id)

        # ✅ Now broker can load
        broker = get_client_broker(client_id)
        log.info(f"✅ Broker initialized for client_id={client_id}")

        worker_loop(broker, poll_seconds=1)

    except KeyboardInterrupt:
        log.info("Worker stopped by user")
        sys.exit(0)
    except Exception as e:
        log.error(f"Worker crashed: {e}", exc_info=True)
        sys.exit(1)
