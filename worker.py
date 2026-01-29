# worker.py - Background worker that processes queue (production-safe)
import os
import sys

from ap.queue import worker_loop
from ap.client_manager import get_client_broker
from ap.logger import get_logger
from ap.db import init_db, get_client, create_client

log = get_logger("worker")


def ensure_default_client():
    """
    Ensure a 'default' client exists so the worker can create a broker.
    Uses env vars if it needs to create the client.
    """
    try:
        get_client("default")
        return
    except Exception:
        pass

    # These MUST be set in Render env for the Worker (and usually Web too)
    broker_type = os.getenv("BROKER_TYPE", "tradier")
    broker_account_id = os.getenv("BROKER_ACCOUNT_ID", "demo")
    broker_token = os.getenv("BROKER_TOKEN", "demo")
    broker_base_url = os.getenv("BROKER_BASE_URL", "https://sandbox.tradier.com")
    initial_equity = float(os.getenv("INITIAL_EQUITY", "5000"))

    log.warning("⚠️ default client missing — creating it from env vars")

    create_client(
        client_id="default",
        name="Default Client",
        broker_type=broker_type,
        broker_account_id=broker_account_id,
        broker_token=broker_token,
        broker_base_url=broker_base_url,
        initial_equity=initial_equity,
    )

    log.info("✅ default client created")


if __name__ == "__main__":
    log.info("🤖 Worker starting...")

    try:
        # ✅ Critical: create tables before any DB reads (clients, trade_queue, etc.)
        init_db()

        # ✅ Ensure default client exists (otherwise get_client_broker crashes)
        ensure_default_client()

        broker = get_client_broker("default")
        log.info("✅ Broker initialized")

        # Run worker loop forever
        worker_loop(broker, poll_seconds=1)

    except KeyboardInterrupt:
        log.info("Worker stopped by user")
        sys.exit(0)
    except Exception as e:
        log.error(f"Worker crashed: {e}", exc_info=True)
        sys.exit(1)
