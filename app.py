import threading
from flask import Flask, request, jsonify

from ap.config import Config
from ap.db import init_db
from ap.logger import get_logger
from ap.models import Signal
from ap.queue import enqueue_signal, worker_loop
from ap.state import load_state, update_state
from ap.broker import SimBroker

cfg = Config()
log = get_logger("app")

app = Flask(__name__)

BROKER = SimBroker(starting_equity=10000.0)

@app.get("/health")
def health():
    st = load_state()
    return jsonify({"ok": True, "mode": st["mode"], "kill_switch": st["kill_switch"], "heartbeat": st["last_heartbeat_ts"]})

@app.get("/state")
def state():
    return jsonify(load_state())

@app.post("/kill_switch/on")
def kill_on():
    update_state({"kill_switch": True, "mode": "READ_ONLY"})
    return jsonify({"ok": True, "kill_switch": True, "mode": "READ_ONLY"})

@app.post("/kill_switch/off")
def kill_off():
    # Only re-enable if you intend to resume
    update_state({"kill_switch": False})
    return jsonify({"ok": True, "kill_switch": False})

@app.post("/mode")
def set_mode():
    body = request.get_json(force=True)
    mode = str(body.get("mode", "")).upper()
    if mode not in ("SIM", "PAPER", "LIVE", "READ_ONLY"):
        return jsonify({"ok": False, "error": "Invalid mode"}), 400
    update_state({"mode": mode})
    return jsonify({"ok": True, "mode": mode})

@app.post("/signal")
def signal():
    body = request.get_json(force=True)
    sig = Signal(**body)
    enqueue_signal(sig)
    return jsonify({"ok": True, "queued": True, "signal_id": sig.signal_id})

def start_worker():
    t = threading.Thread(target=worker_loop, args=(BROKER,), daemon=True)
    t.start()

if __name__ == "__main__":
    init_db()
    # bootstrap mode from env into state
    update_state({"mode": cfg.BOT_MODE})
    start_worker()
    app.run(host="0.0.0.0", port=5000, debug=False)

