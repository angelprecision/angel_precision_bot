# gunicorn.conf.py — Angel Precision Bot
# Single-worker threading model:
#   workers=1 prevents multi-process state divergence (rate limits,
#   idempotency cache, _active_runners, kill switch all live in one process).
#   Increase GUNICORN_THREADS for concurrent request handling within that process.
import os

workers     = 1
threads     = int(os.getenv("GUNICORN_THREADS", "4"))
worker_class = "gthread"
timeout     = 120
keepalive   = 5
bind        = f"0.0.0.0:{os.getenv('PORT', '5000')}"
preload_app = False
accesslog   = "-"
errorlog    = "-"
loglevel    = os.getenv("LOG_LEVEL", "info").lower()
