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
from ap.parsers import parse_scanner_text
import uuid
from datetime import datetime, timezone

@app.post("/scanner/discord")
def scanner_discord():
    body = request.get_json(force=True)
    # expected: {"content": "...discord message text..."}
    text = body.get("content") or ""
    parsed = parse_scanner_text(text)
    queued = 0

    for msg in parsed:
        # pick CALLS leg by default for now; later add logic (or send both as separate signals)
        if msg.calls and msg.calls.strike:
            sig = Signal(
                signal_id=str(uuid.uuid4()),
                symbol=msg.symbol,
                direction="CALL",
                pattern_id="SCANNER_V1",
                confidence_tag="standard_pool",
                timestamp_iso=datetime.now(timezone.utc).isoformat(),
                trigger={
                    "source": "discord",
                    "scanned_at": msg.scanned_at,
                    "current": msg.current,
                    "entry": msg.calls.entry,
                    "stop": msg.calls.stop,
                    "pt1": msg.calls.pt1,
                    "pt2": msg.calls.pt2,
                    "pt3": msg.calls.pt3,
                    "strike": msg.calls.strike,
                    "expiry_hint": msg.calls.expiry_hint,
                    "raw_strike": msg.calls.raw_strike_line
                }
            )
            enqueue_signal(sig)
            queued += 1

        if msg.puts and msg.puts.strike:
            sig = Signal(
                signal_id=str(uuid.uuid4()),
                symbol=msg.symbol,
                direction="PUT",
                pattern_id="SCANNER_V1",
                confidence_tag="standard_pool",
                timestamp_iso=datetime.now(timezone.utc).isoformat(),
                trigger={
                    "source": "discord",
                    "scanned_at": msg.scanned_at,
                    "current": msg.current,
                    "entry": msg.puts.entry,
                    "stop": msg.puts.stop,
                    "pt1": msg.puts.pt1,
                    "pt2": msg.puts.pt2,
                    "pt3": msg.puts.pt3,
                    "strike": msg.puts.strike,
                    "expiry_hint": msg.puts.expiry_hint,
                    "raw_strike": msg.puts.raw_strike_line
                }
            )
            enqueue_signal(sig)
            queued += 1

    return jsonify({"ok": True, "parsed": len(parsed), "queued": queued})

