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


def post_worker_init(worker):
    """Called by gunicorn after the worker process is fully initialized.
    This is the guaranteed way to start background threads in a preload_app=False
    setup — the app module is already imported and all globals are set.
    """
    if os.getenv("RUN_SUPERVISOR") != "1":
        return
    try:
        from client_runner import (
            _active_runners, _registry_lock, ClientRunner,
            _fetch_active_members, SUPABASE_URL, SUPABASE_SERVICE_KEY,
        )
        from supabase import create_client
        import logging
        log = logging.getLogger("gunicorn.error")

        if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
            log.warning("post_worker_init: no Supabase credentials, skipping runner start")
            return

        sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        members = _fetch_active_members(sb)
        log.info(f"post_worker_init: found {len(members)} active members")

        started = []
        for member in members:
            email = member["email"]
            with _registry_lock:
                existing = _active_runners.get(email)
                if existing and existing.is_alive():
                    log.info(f"post_worker_init: runner already alive for {email}")
                    continue
                runner = ClientRunner(member)
                _active_runners[email] = runner
                runner.start()
                started.append(email)
                log.info(f"post_worker_init: started runner for {email}")

        log.info(f"post_worker_init: done — started={started}")
    except Exception as exc:
        import logging
        logging.getLogger("gunicorn.error").error(f"post_worker_init runner start failed: {exc}", exc_info=True)
