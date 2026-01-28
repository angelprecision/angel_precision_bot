# worker.py - Background worker that processes queue
import os
from ap.queue import worker_loop
from ap.client_manager import get_client_broker

if __name__ == "__main__":
    print("🤖 Worker starting...")
    broker = get_client_broker("default")
    worker_loop(broker)
