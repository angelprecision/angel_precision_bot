# worker.py - Background worker that processes queue
import os
import sys
import time
from ap.queue import worker_loop
from ap.client_manager import get_client_broker
from ap.logger import get_logger

log = get_logger("worker")

if __name__ == "__main__":
    log.info("🤖 Worker starting...")
    
    try:
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
