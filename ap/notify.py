# ap/notify.py
import os
import requests
from ap.logger import get_logger

log = get_logger("ap.notify")

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

def post_discord(content: str):
    if not DISCORD_WEBHOOK_URL:
        log.warning("DISCORD_WEBHOOK_URL not set; skipping Discord post")
        return
    try:
        r = requests.post(DISCORD_WEBHOOK_URL, json={"content": content}, timeout=10)
        r.raise_for_status()
    except Exception as e:
        log.error(f"Discord post failed: {e}")
