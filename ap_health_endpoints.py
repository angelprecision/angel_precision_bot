"""
ap_health_endpoints.py
======================
Flask Blueprint exposing health, lifecycle trace, and kill switch endpoints.

Register in app.py:
    from ap_health_endpoints import health_bp
    app.register_blueprint(health_bp)

Endpoints:
    GET  /health/status               — full organ health snapshot + system state
    GET  /health/compact              — lightweight name→status map
    GET  /health/quotes               — quote authority snapshot (all tracked contracts)
    GET  /health/lifecycle            — lifecycle ledger summary (state counts)
    GET  /health/signal/<id>/trace    — full trace for a specific signal_id (JSON)
    GET  /health/rejections           — all rejection records (last N)
    GET  /health/command-center       — Signal Trace Command Center (HTML dashboard)
    POST /health/kill                 — trigger local kill switch {"reason": "..."}
    POST /health/reset                — reset local kill switch (manual operator action)
"""
from __future__ import annotations

import logging

from flask import Blueprint, jsonify, request

log = logging.getLogger("ap.health_endpoints")

health_bp = Blueprint("ap_health", __name__, url_prefix="/health")


@health_bp.route("/status")
def status():
    """Full organ health snapshot + kill switch state."""
    from ap_health_registry import HEALTH
    from ap_kill_switch import KILL
    from ap_quote_authority import QUOTES

    return jsonify({
        "system_healthy":     HEALTH.is_system_healthy(),
        "killed":             KILL.is_killed(),
        "kill_reason":        KILL.reason,
        "quote_rejects":      QUOTES.reject_count,
        "unhealthy_critical": HEALTH.unhealthy_critical_organs(),
        "organs":             HEALTH.snapshot(),
    })


@health_bp.route("/compact")
def compact():
    """Lightweight name→status map for dashboard polling."""
    from ap_health_registry import HEALTH
    from ap_kill_switch import KILL
    return jsonify({
        "system_healthy": HEALTH.is_system_healthy(),
        "killed":         KILL.is_killed(),
        "organs":         HEALTH.compact_snapshot(),
    })


@health_bp.route("/quotes")
def quotes():
    """All contracts currently tracked by the quote authority."""
    from ap_quote_authority import QUOTES
    return jsonify(QUOTES.snapshot_all())


@health_bp.route("/lifecycle")
def lifecycle():
    """Lifecycle ledger summary — state counts and rejection counts."""
    from ap_lifecycle import LEDGER
    return jsonify(LEDGER.snapshot())


@health_bp.route("/signal/<signal_id>/trace")
def signal_trace(signal_id: str):
    """Full lifecycle + rejection trace for a specific signal_id."""
    from ap_lifecycle import LEDGER
    entries    = LEDGER.history(signal_id)
    rejections = LEDGER.rejection_history(signal_id)
    current    = LEDGER.current_state(signal_id)
    return jsonify({
        "signal_id":     signal_id,
        "current_state": current.value if current else None,
        "trace": [
            {
                "ts":     e.timestamp_iso,
                "from":   e.from_state,
                "to":     e.to_state,
                "owner":  e.owner,
                "reason": e.reason,
            }
            for e in entries
        ],
        "rejections": [
            {
                "ts":       r.timestamp_iso,
                "category": r.category,
                "severity": r.severity,
                "code":     r.reason_code,
                "reason":   r.human_reason,
                "owner":    r.owner,
            }
            for r in rejections
        ],
    })


@health_bp.route("/rejections")
def rejections():
    """All rejection records. Optional ?limit=N query param."""
    from ap_lifecycle import LEDGER
    limit  = int(request.args.get("limit", 200))
    all_r  = LEDGER.all_rejections()
    recent = all_r[-limit:] if len(all_r) > limit else all_r
    return jsonify({
        "total":      len(all_r),
        "returned":   len(recent),
        "rejections": [
            {
                "ts":       r.timestamp_iso,
                "signal":   r.signal_id,
                "ticker":   r.ticker,
                "category": r.category,
                "severity": r.severity,
                "code":     r.reason_code,
                "reason":   r.human_reason,
                "owner":    r.owner,
            }
            for r in recent
        ],
    })


@health_bp.route("/command-center")
def command_center():
    """
    Signal Trace Command Center — embedded HTML page for the admin dashboard.
    Access at /health/command-center from any browser.
    Embed in the dashboard via iframe or link.
    """
    base_url = request.host_url.rstrip("/")
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AP Signal Trace — Command Center</title>
<style>
  :root {{
    --bg: #0a0a0a; --surface: #111; --border: #222; --gold: #c9a84c;
    --gold-dim: #8a6f2e; --green: #22c55e; --red: #ef4444;
    --amber: #f59e0b; --text: #e5e7eb; --muted: #6b7280;
    --mono: 'IBM Plex Mono', 'Courier New', monospace;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: var(--mono); font-size: 13px; padding: 16px; }}
  h1 {{ color: var(--gold); font-size: 18px; letter-spacing: 2px; text-transform: uppercase; margin-bottom: 16px; border-bottom: 1px solid var(--gold-dim); padding-bottom: 8px; }}
  h2 {{ color: var(--gold-dim); font-size: 12px; letter-spacing: 1px; text-transform: uppercase; margin-bottom: 8px; }}

  .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 16px; }}
  .card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 6px; padding: 12px; }}
  .card.full {{ grid-column: 1 / -1; }}

  .organ-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 6px; }}
  .organ {{ display: flex; justify-content: space-between; align-items: center; padding: 6px 8px; background: #0d0d0d; border: 1px solid var(--border); border-radius: 4px; font-size: 11px; }}
  .organ-name {{ color: var(--muted); }}
  .badge {{ padding: 2px 6px; border-radius: 3px; font-size: 10px; font-weight: bold; }}
  .HEALTHY  {{ background: #14532d; color: var(--green); }}
  .BOOTING  {{ background: #1c1c00; color: var(--amber); }}
  .STALE    {{ background: #3b1c00; color: var(--amber); }}
  .DEGRADED {{ background: #3b1c00; color: var(--amber); }}
  .FAILED   {{ background: #3b0000; color: var(--red); }}
  .STOPPED  {{ background: #1a1a1a; color: var(--muted); }}

  .search-row {{ display: flex; gap: 8px; margin-bottom: 12px; }}
  input {{ background: #0d0d0d; border: 1px solid var(--border); color: var(--text); padding: 8px 10px; border-radius: 4px; font-family: var(--mono); font-size: 13px; flex: 1; outline: none; }}
  input:focus {{ border-color: var(--gold-dim); }}
  button {{ background: var(--gold-dim); color: #000; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer; font-family: var(--mono); font-size: 12px; font-weight: bold; text-transform: uppercase; letter-spacing: 1px; white-space: nowrap; }}
  button:hover {{ background: var(--gold); }}
  button.danger {{ background: #7f1d1d; color: var(--red); }}
  button.danger:hover {{ background: #991b1b; }}

  .timeline {{ position: relative; }}
  .event {{ display: grid; grid-template-columns: 160px 120px 120px 1fr; gap: 8px; padding: 7px 8px; border-bottom: 1px solid var(--border); align-items: center; font-size: 11px; }}
  .event:hover {{ background: #0f0f0f; }}
  .event-ts {{ color: var(--muted); font-size: 10px; }}
  .event-state {{ display: flex; gap: 4px; align-items: center; }}
  .arrow {{ color: var(--muted); }}
  .state-from {{ color: var(--muted); }}
  .state-to {{ color: var(--gold); font-weight: bold; }}
  .REJECTED .state-to   {{ color: var(--red); }}
  .INVALIDATED .state-to {{ color: var(--amber); }}
  .WATCHING .state-to   {{ color: var(--green); }}
  .REMOVED .state-to    {{ color: var(--muted); }}
  .event-owner {{ color: #818cf8; font-size: 10px; }}
  .event-reason {{ color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}

  .rejection {{ background: #1a0000; border-left: 3px solid var(--red); padding: 8px 10px; margin-bottom: 4px; border-radius: 0 4px 4px 0; font-size: 11px; }}
  .rejection-header {{ display: flex; gap: 12px; margin-bottom: 4px; }}
  .rejection-reason {{ color: var(--muted); }}
  .sev-CRITICAL {{ color: var(--red); font-weight: bold; }}
  .sev-WARNING  {{ color: var(--amber); }}
  .sev-INFO     {{ color: var(--muted); }}

  .current-state {{ display: inline-block; padding: 4px 10px; border-radius: 4px; font-size: 12px; font-weight: bold; margin-bottom: 12px; border: 1px solid var(--gold-dim); color: var(--gold); }}
  .empty {{ color: var(--muted); padding: 20px; text-align: center; font-size: 12px; }}
  .system-ok {{ color: var(--green); }} .system-bad {{ color: var(--red); }}
  .kill-bar {{ background: #3b0000; border: 1px solid var(--red); border-radius: 4px; padding: 8px 12px; color: var(--red); margin-bottom: 12px; font-size: 12px; display: none; }}
</style>
</head>
<body>
<h1>⚡ Angel Precision — Signal Trace Command Center</h1>

<div id="kill-bar" class="kill-bar">🚨 KILL SWITCH ARMED — <span id="kill-reason"></span></div>

<div class="grid">
  <div class="card">
    <h2>System Health</h2>
    <div id="system-status" style="margin-bottom:10px;font-size:12px;">Loading...</div>
    <div class="organ-grid" id="organs"></div>
  </div>
  <div class="card">
    <h2>Recent Rejections</h2>
    <div id="rejections-summary"></div>
  </div>
</div>

<div class="card full" style="margin-bottom:16px;">
  <h2>Signal Lifecycle Trace</h2>
  <div class="search-row">
    <input id="sig-input" placeholder="Enter signal_id to trace..." />
    <button onclick="fetchTrace()">Trace</button>
    <button onclick="clearTrace()" style="background:#1a1a1a;color:var(--muted);">Clear</button>
  </div>
  <div id="trace-result"></div>
</div>

<div class="card full">
  <h2>Lifecycle Snapshot</h2>
  <div id="lifecycle-snap"></div>
</div>

<script>
const BASE = '{base_url}';

async function api(path) {{
  const r = await fetch(BASE + path);
  return r.json();
}}

function fmt(iso) {{
  if (!iso) return '';
  const d = new Date(iso);
  return d.toLocaleTimeString('en-US', {{hour12:false}}) + '.' + String(d.getMilliseconds()).padStart(3,'0');
}}

async function loadStatus() {{
  try {{
    const d = await api('/health/compact');
    const el = document.getElementById('system-status');
    const healthy = d.system_healthy;
    el.innerHTML = `System: <span class="${{healthy ? 'system-ok' : 'system-bad'}}">${{healthy ? '✅ HEALTHY' : '❌ UNHEALTHY'}}</span>`;

    const kb = document.getElementById('kill-bar');
    if (d.killed) {{
      kb.style.display = 'block';
      document.getElementById('kill-reason').textContent = d.kill_reason || '';
    }} else {{
      kb.style.display = 'none';
    }}

    const organs = document.getElementById('organs');
    organs.innerHTML = '';
    for (const [name, status] of Object.entries(d.organs || {{}})) {{
      const short = name.replace('ap_','').replace(/_/g,' ');
      organs.innerHTML += `<div class="organ"><span class="organ-name">${{short}}</span><span class="badge ${{status}}">${{status}}</span></div>`;
    }}
  }} catch(e) {{ console.error(e); }}
}}

async function loadRejections() {{
  try {{
    const d = await api('/health/rejections?limit=10');
    const el = document.getElementById('rejections-summary');
    if (!d.rejections || d.rejections.length === 0) {{
      el.innerHTML = '<div class="empty">No rejections recorded this session</div>';
      return;
    }}
    el.innerHTML = d.rejections.slice().reverse().map(r => `
      <div class="rejection">
        <div class="rejection-header">
          <span class="event-ts">${{fmt(r.ts)}}</span>
          <span style="color:var(--gold-dim)">${{r.ticker}}</span>
          <span class="sev-${{r.severity}}">${{r.severity}}</span>
          <span style="color:#818cf8">${{r.owner}}</span>
        </div>
        <div class="rejection-reason">${{r.code}}: ${{r.reason}}</div>
      </div>`).join('');
  }} catch(e) {{ console.error(e); }}
}}

async function loadLifecycle() {{
  try {{
    const d = await api('/health/lifecycle');
    const el = document.getElementById('lifecycle-snap');
    const states = d.state_counts || {{}};
    const rejects = d.rejection_counts_by_code || {{}};
    let html = '<div style="display:flex;gap:24px;flex-wrap:wrap;">';
    html += '<div><div style="color:var(--gold-dim);font-size:10px;margin-bottom:4px;">STATE COUNTS</div>';
    for (const [s,c] of Object.entries(states)) {{
      html += `<div style="display:flex;justify-content:space-between;gap:16px;padding:2px 0;font-size:11px;"><span style="color:var(--muted)">${{s}}</span><span style="color:var(--gold)">${{c}}</span></div>`;
    }}
    html += '</div>';
    if (Object.keys(rejects).length > 0) {{
      html += '<div><div style="color:var(--gold-dim);font-size:10px;margin-bottom:4px;">REJECTIONS BY CODE</div>';
      for (const [code,c] of Object.entries(rejects)) {{
        html += `<div style="display:flex;justify-content:space-between;gap:16px;padding:2px 0;font-size:11px;"><span style="color:var(--muted)">${{code}}</span><span style="color:var(--red)">${{c}}</span></div>`;
      }}
      html += '</div>';
    }}
    html += `<div style="font-size:11px;color:var(--muted);margin-top:4px;">Total entries: ${{d.entries_count || 0}} | Rejections: ${{d.rejections_count || 0}}</div>`;
    html += '</div>';
    el.innerHTML = html;
  }} catch(e) {{ console.error(e); }}
}}

async function fetchTrace() {{
  const sigId = document.getElementById('sig-input').value.trim();
  if (!sigId) return;
  const el = document.getElementById('trace-result');
  el.innerHTML = '<div class="empty">Loading...</div>';
  try {{
    const d = await api(`/health/signal/${{encodeURIComponent(sigId)}}/trace`);
    if (!d.trace || d.trace.length === 0) {{
      el.innerHTML = `<div class="empty">No lifecycle trace found for signal_id: ${{sigId}}</div>`;
      return;
    }}
    let html = `<div class="current-state">Current: ${{d.current_state || 'UNKNOWN'}}</div>`;
    html += '<div class="timeline">';
    html += '<div class="event" style="font-size:10px;color:var(--muted);border-bottom:1px solid var(--border);padding-bottom:4px;"><span>TIMESTAMP</span><span>FROM → TO</span><span>OWNER</span><span>REASON</span></div>';
    for (const e of d.trace) {{
      const toClass = e.to || '';
      html += `<div class="event ${{toClass}}">
        <span class="event-ts">${{fmt(e.ts)}}</span>
        <span class="event-state"><span class="state-from">${{e.from || 'NONE'}}</span><span class="arrow"> → </span><span class="state-to">${{e.to}}</span></span>
        <span class="event-owner">${{e.owner}}</span>
        <span class="event-reason" title="${{e.reason}}">${{e.reason}}</span>
      </div>`;
    }}
    html += '</div>';
    if (d.rejections && d.rejections.length > 0) {{
      html += '<div style="margin-top:12px;"><h2 style="margin-bottom:8px;">Rejections for this signal</h2>';
      for (const r of d.rejections) {{
        html += `<div class="rejection">
          <div class="rejection-header">
            <span class="event-ts">${{fmt(r.ts)}}</span>
            <span class="sev-${{r.severity}}">${{r.severity}}</span>
            <span style="color:#818cf8">${{r.owner}}</span>
            <span style="color:var(--gold-dim)">${{r.category}}</span>
          </div>
          <div class="rejection-reason">${{r.code}}: ${{r.reason}}</div>
        </div>`;
      }}
      html += '</div>';
    }}
    el.innerHTML = html;
  }} catch(e) {{
    el.innerHTML = `<div class="empty" style="color:var(--red)">Error: ${{e.message}}</div>`;
  }}
}}

function clearTrace() {{
  document.getElementById('trace-result').innerHTML = '';
  document.getElementById('sig-input').value = '';
}}

document.getElementById('sig-input').addEventListener('keydown', e => {{
  if (e.key === 'Enter') fetchTrace();
}});

// Initial load
loadStatus();
loadRejections();
loadLifecycle();

// Auto-refresh every 15 seconds
setInterval(() => {{
  loadStatus();
  loadRejections();
  loadLifecycle();
}}, 15000);
</script>
</body>
</html>"""
    from flask import Response
    return Response(html, mimetype="text/html")




# ── PR-110: Per-client live-control organ health ───────────────────────────────
# GET /health/organs and /health/clients both return the same JSON response.
# Never returns HTML. Always valid JSON, even on errors.

_REQUIRED_LIVE_ORGANS = [
    "client_runner",
    "exit_engine",
    "fill_monitor",
    "reconciler",
    "broker_precheck",
]

# Stale threshold for broker precheck (seconds since last successful run)
_BROKER_PRECHECK_STALE_S = float(
    __import__("os").getenv("BROKER_PRECHECK_STALE_S", "120")
)


def _build_client_organ_report(email: str, runner) -> dict:
    """
    Build a single client organ health dict from a live ClientRunner instance.
    Reads runner state directly — no external calls.
    """
    import time as _time

    mode        = str(getattr(runner, "mode", "UNKNOWN")).upper()
    account_id  = str(getattr(runner, "account_id", ""))
    pod_id      = __import__("os").getenv("AP_POD_ID", "live-pod-001")

    # ── Thread liveness ──────────────────────────────────────────────────
    runner_thread  = getattr(runner, "runner_thread", None) or getattr(runner, "_thread", None)
    worker_thread  = getattr(runner, "worker_thread", None)
    fill_thread    = getattr(runner, "fill_monitor_thread", None)
    core           = getattr(runner, "core", None)
    exit_eng       = getattr(core, "exit_eng", None) if core else None
    reconciler     = getattr(runner, "reconciler", None)

    _runner_alive   = runner.is_alive() if hasattr(runner, "is_alive") else False
    _fill_alive     = bool(fill_thread and fill_thread.is_alive())
    _worker_alive   = bool(worker_thread and worker_thread.is_alive())
    _exit_present   = exit_eng is not None
    _reconciler_ok  = reconciler is not None

    # ── Heartbeat ages ───────────────────────────────────────────────────
    now = _time.time()
    _fm_last  = getattr(runner, "last_fill_monitor_heartbeat_ts", 0) or 0
    _wk_last  = getattr(runner, "last_worker_heartbeat_ts", 0) or 0
    _eq_last  = getattr(runner, "last_equity_heartbeat_ts", 0) or 0

    cr_age   = round(now - _wk_last, 1) if _wk_last else None
    fm_age   = round(now - _fm_last, 1) if _fm_last else None
    # Exit engine heartbeat: use health registry if registered
    ee_age   = None
    try:
        from ap_health_registry import HEALTH
        snap = HEALTH.snapshot()
        if "ap_exit_engine" in snap:
            ee_age = round(snap["ap_exit_engine"]["heartbeat_age_s"], 1)
    except Exception:
        pass

    # ── Broker precheck metrics ──────────────────────────────────────────
    bp_last_ts     = getattr(exit_eng, "_broker_precheck_last_ts", 0) or 0
    bp_last_ok     = getattr(exit_eng, "_broker_precheck_last_ok", False)
    bp_http_status = getattr(exit_eng, "_broker_precheck_last_http_status", 0)
    bp_pos_count   = getattr(exit_eng, "_broker_precheck_last_position_count", 0)
    bp_ran         = bp_last_ts > 0
    bp_age         = round(now - bp_last_ts, 1) if bp_ran else None
    bp_stale       = (not bp_ran) or (bp_age is not None and bp_age > _BROKER_PRECHECK_STALE_S)
    bp_online      = bp_ran and bp_last_ok and not bp_stale

    # ── Organ online/stale booleans ──────────────────────────────────────
    # "online" = alive + heartbeating recently
    _fill_stale_threshold = float(
        __import__("os").getenv("FILL_MONITOR_GRACE_SEC", "300")
    )
    fill_online = _fill_alive and (fm_age is None or fm_age < _fill_stale_threshold)
    cr_online   = _runner_alive and (cr_age is None or cr_age < 120)
    ee_online   = _exit_present and (ee_age is None or ee_age < 90)
    rec_online  = _reconciler_ok

    # ── Broker symbols ───────────────────────────────────────────────────
    broker_symbols = []
    if exit_eng is not None:
        try:
            with exit_eng._lock:
                broker_symbols = [
                    str(getattr(p, "option_symbol", "") or "")
                    for p in exit_eng._positions
                    if not p.closed and int(p.quantity_remaining or 0) > 0
                ]
        except Exception:
            pass

    # ── Blocking reasons + live_control_ready ────────────────────────────
    blocking: list[str] = []

    if not cr_online:
        if not _runner_alive:
            blocking.append("client_runner_dead")
        elif cr_age is not None and cr_age >= 120:
            blocking.append("client_runner_stale")
        else:
            blocking.append("client_runner_missing")

    if not ee_online:
        if not _exit_present:
            blocking.append("exit_engine_missing")
        elif ee_age is not None and ee_age >= 90:
            blocking.append("exit_engine_stale")
        else:
            blocking.append("exit_engine_degraded")

    if not fill_online:
        if not _fill_alive:
            blocking.append("fill_monitor_missing")
        else:
            blocking.append("fill_monitor_stale")

    if not rec_online:
        blocking.append("reconciler_missing")

    if not bp_online:
        if not bp_ran:
            blocking.append("broker_precheck_never_ran")
        elif not bp_last_ok:
            status_suffix = str(bp_http_status) if bp_http_status and bp_http_status > 0 else "unknown"
            blocking.append(f"broker_precheck_http_{status_suffix}")
        elif bp_stale:
            blocking.append("broker_precheck_stale")
        else:
            blocking.append("broker_position_check_unavailable")

    # Additional: degraded mode
    if getattr(runner, "degraded", None) and runner.degraded.is_set():
        degraded_reasons = sorted(getattr(runner, "degraded_reasons", set()))
        for r in degraded_reasons:
            blocking.append(f"degraded_{r}")

    # For LIVE clients all organs required; PAPER clients only block on
    # runner/exit/fill (broker precheck is advisory in paper mode)
    if mode == "LIVE":
        live_control_ready = len(blocking) == 0
    else:
        critical_blocking = [b for b in blocking
                             if not b.startswith(("broker_precheck", "reconciler"))]
        live_control_ready = len(critical_blocking) == 0

    return {
        "client_email":               email,
        "account_id":                 account_id,
        "execution_mode":             mode.lower(),
        "pod_id":                     pod_id,

        "client_runner_online":       cr_online,
        "exit_engine_online":         ee_online,
        "fill_monitor_online":        fill_online,
        "reconciler_online":          rec_online,
        "broker_precheck_online":     bp_online,
        "broker_precheck_last_http_status": bp_http_status,
        "broker_position_count":      bp_pos_count if bp_ran else 0,
        "broker_symbols":             broker_symbols,

        "last_heartbeat_age_seconds": {
            "client_runner":  cr_age,
            "exit_engine":    ee_age,
            "fill_monitor":   fm_age,
            "reconciler":     None,    # reconciler is event-driven, no heartbeat ts
            "broker_precheck": bp_age,
        },

        "live_control_ready": live_control_ready,
        "blocking_reasons":   blocking,

        # additional diagnostics
        "entries_allowed":    bool(
            getattr(runner, "entries_allowed", None)
            and runner.entries_allowed.is_set()
        ),
        "initialized":        bool(
            getattr(runner, "initialized", None)
            and runner.initialized.is_set()
        ),
        "degraded":           bool(
            getattr(runner, "degraded", None)
            and runner.degraded.is_set()
        ),
    }


@health_bp.route("/organs")
@health_bp.route("/clients")
def organs():
    """
    PR-110: Per-client live-control organ health.
    Returns JSON always — never HTML. Never 404.
    Used by operator console to verify live machine is safe before entries.
    Alias: /health/clients
    """
    import time as _time

    try:
        from client_runner import _active_runners
    except Exception as _import_err:
        return jsonify({
            "ok": False,
            "error": f"runner_module_unavailable: {_import_err}",
            "clients": [],
        })

    from ap_health_registry import HEALTH

    client_reports = []
    overall_ok = True

    try:
        runners_snapshot = dict(_active_runners)
    except Exception as _e:
        return jsonify({
            "ok": False,
            "error": f"runner_registry_unavailable: {_e}",
            "clients": [],
        })

    for email, runner in runners_snapshot.items():
        try:
            report = _build_client_organ_report(email, runner)
        except Exception as _re:
            log.warning("[health/organs] error building report for %s: %s", email, _re)
            report = {
                "client_email":       email,
                "execution_mode":     "unknown",
                "live_control_ready": False,
                "blocking_reasons":   [f"report_build_error:{_re}"],
                "error":              str(_re),
            }
        if not report.get("live_control_ready"):
            overall_ok = False
        client_reports.append(report)

    # Top-level mode: LIVE if any client is live
    has_live = any(
        r.get("execution_mode", "").lower() == "live"
        for r in client_reports
    )

    return jsonify({
        "ok":      overall_ok,
        "mode":    "LIVE" if has_live else "PAPER",
        "clients": client_reports,
        "summary": {
            "total_clients":      len(client_reports),
            "live_control_ready": sum(1 for r in client_reports if r.get("live_control_ready")),
            "live_control_blocked": sum(1 for r in client_reports if not r.get("live_control_ready")),
        },
        "generated_at": _time.time(),
    })


@health_bp.route("/kill", methods=["POST"])
def kill():
    """Trigger local kill switch. Body: {"reason": "manual operator halt"}"""
    from ap_kill_switch import KILL
    reason = (request.json or {}).get("reason", "manual_operator_halt")
    KILL.kill(reason)
    log.critical("[HEALTH_ENDPOINT] Kill switch triggered via API: %s", reason)
    return jsonify({"killed": True, "reason": reason})


@health_bp.route("/reset", methods=["POST"])
def reset():
    """Reset local kill switch. Requires manual operator action."""
    from ap_kill_switch import KILL
    KILL.reset()
    log.info("[HEALTH_ENDPOINT] Kill switch reset via API")
    return jsonify({"killed": False})
