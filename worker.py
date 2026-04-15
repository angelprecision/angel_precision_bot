# worker.py - DEPRECATED: Use client_runner.py instead.
# This legacy path bypasses all 9 risk management gates in APMasterControl.

import sys
print("ERROR: worker.py is DEPRECATED. Use client_runner.py instead.", file=sys.stderr)
sys.exit(1)

import os

from ap.queue import worker_loop
from ap.client_manager import get_client_broker
from ap.logger import get_logger
from ap.db import init_db, get_client, create_client
from ap.crypto import encrypt_token  # ✅ encrypt before storing (matches get_client_broker decrypt)

log = get_logger("worker")


def _get_env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def ensure_client_exists(client_id: str):
    # If client exists, done
    try:
        get_client(client_id)
        return
    except Exception:
        pass

    # Pull broker credentials from env (set these in Render Worker env)
    broker_type = _get_env("BROKER_TYPE", "tradier") or "tradier"

    # Support both naming styles (your env uses TRADIER_*)
    broker_account_id = _get_env("BROKER_ACCOUNT_ID") or _get_env("TRADIER_ACCOUNT_ID") or "demo"

    token_plain = (
        _get_env("BROKER_TOKEN")
        or _get_env("TRADIER_ACCESS_TOKEN")   # ✅ your real env var name
        or _get_env("TRADIER_TOKEN")
        or "demo"
    )

    broker_base_url = _get_env("BROKER_BASE_URL") or _get_env("TRADIER_BASE_URL") or "https://sandbox.tradier.com"
    initial_equity = float(_get_env("INITIAL_EQUITY", "5000") or "5000")

    # If you're using real creds, require them
    if token_plain == "demo" or broker_account_id == "demo":
        log.warning("⚠️ Using demo broker creds. Set TRADIER_ACCESS_TOKEN and BROKER_ACCOUNT_ID in Worker env for live/sandbox trading.")

    # ✅ Encrypt token before storing so get_client_broker() can decrypt
    try:
        token_encrypted = encrypt_token(token_plain) if token_plain else ""
    except Exception as e:
        raise RuntimeError(
            f"Failed to encrypt token. Make sure ENCRYPTION_KEY is set in Worker env. Error={e}"
        )

    log.warning(f"⚠️ Client '{client_id}' missing — creating from env vars (encrypted token)")

    create_client(
        client_id=client_id,
        name=f"{client_id} Client",
        broker_type=broker_type,
        broker_account_id=broker_account_id,
        broker_token=token_encrypted,  # ✅ store encrypted
        broker_base_url=broker_base_url,
        initial_equity=initial_equity,
    )

    log.info(f"✅ Client '{client_id}' created")


if __name__ == "__main__":
    log.info("🤖 Worker starting...")

    # Use env var; default to "default"
    client_id = _get_env("BOT_CLIENT_ID", "default") or "default"

    try:
        # ✅ Always create tables first
        init_db()

        # ✅ Ensure the client exists
        ensure_client_exists(client_id)

        # ✅ Now broker can load (decrypts token)
        broker = get_client_broker(client_id)
        log.info(f"✅ Broker initialized for client_id={client_id}")

        worker_loop(broker, poll_seconds=1)

    except KeyboardInterrupt:
        log.info("Worker stopped by user")
        sys.exit(0)
    except Exception as e:
        log.error(f"Worker crashed: {e}", exc_info=True)
        sys.exit(1)
