"""
PR: fix(master-control): unblock paper sizing from permanent bootstrap + persist entry/exit quote evidence

Pre-LIVE regression tests. These assert structural invariants that prevent
the qty=1 perma-bootstrap bug from regressing and that capture sizing/
quote evidence so future fill-conversion incidents are diagnosable from
DB state alone.

NOT TOUCHED by this PR (and asserted to remain intact via targeted
regression on the existing test files):
  - exit engine decision logic
  - entry watcher trigger logic
  - broker submit behavior
  - retry/repeg behavior
  - order monitor cancel windows
  - client_runner, queue, OSM state transitions

Bugs this PR addresses:
  1. position_manager snapshot does NOT include `total_trades` →
     master_control reads 0 → bootstrap_mode permanently True →
     contracts forced to 1 for every order. The 10% POSITION_RISK_PCT
     path is completely bypassed.
  2. The bootstrap quantity guard applies unconditionally (live and
     paper). Paper has no live capital risk; forcing qty=1 in paper
     prevents proof-week from generating realistic P&L.
  3. Approved plan/order metadata does NOT carry sizing context
     (account_equity, risk_pct, contracts, budget, total_trades,
     bootstrap_mode). When a fill misses or P&L looks wrong, there is
     no DB trail to diagnose what the bot believed about itself.
  4. Quote evidence (bid/ask/mid/last/spread) at submit is not
     persisted. After NFLX/MO incidents we cannot prove from DB state
     whether ask was above limit, whether mid was stale, or whether
     contract premium had widened between selection and submit.
  5. open_position() does NOT write entry_price column (only avg_fill).
     Exit engine uses entry_price for pnl_pct; null entry_price means
     option_pnl_pct=0 always, profit-take never triggers.
  6. reconciler._broker_position_underlying falls back to contract[:6]
     when Tradier omits underlying — that puts garbage like 'MO2605'
     into positions.underlying, hiding the position from the exit
     engine (queries by `underlying = 'MO'`).
"""
from __future__ import annotations

import importlib
import pathlib
import re
import sys
from typing import Any

import pytest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
PM_PATH   = REPO_ROOT / "ap" / "position_manager.py"
MC_PATH   = REPO_ROOT / "ap_master_control.py"
RC_PATH   = REPO_ROOT / "ap_reconciler.py"


def _src(p: pathlib.Path) -> str:
    return p.read_text(encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# FIX-1: position_manager snapshot includes total_trades
# ─────────────────────────────────────────────────────────────────────────────

def test_position_manager_snapshot_includes_total_trades_field():
    """
    snapshot() must include a `total_trades` key. Otherwise
    master_control reads 0 → bootstrap_mode True forever → qty=1.
    """
    src = _src(PM_PATH)
    # Find the snapshot() function body — loose match (return type annotation
    # may or may not be present depending on deployment vintage).
    snap_match = re.search(
        r"def\s+snapshot\s*\(\s*self\s*\)[^:]*:(.*?)(?=\n    def\s+\w|\nclass\s+\w|\Z)",
        src,
        re.DOTALL,
    )
    assert snap_match, "Could not locate def snapshot(self) in ap/position_manager.py"
    body = snap_match.group(1)
    # The body must have a "total_trades": expression somewhere.
    assert re.search(r'["\']total_trades["\']\s*:', body), (
        "snapshot() must include a 'total_trades' key in its returned dict. "
        "Without it, master_control reads 0 and bootstrap_mode is True forever, "
        "forcing qty=1 on every order."
    )


def test_position_manager_snapshot_total_trades_query_uses_filled_orders():
    """
    The total_trades count must reflect durable historical trade activity
    for that client. We define it as COUNT(*) FROM orders WHERE
    status IN ('FILLED','EXIT_FILLED') AND client_id = $1 — i.e. real
    completed broker fills, not paper queue entries.

    Asserting this at source level keeps the definition fixed and
    auditable. If a future PR wants to change the definition, the
    test must be updated explicitly.
    """
    src = _src(PM_PATH)
    # Look for a SQL fragment that counts FILLED orders. We accept either
    # COUNT(*) FROM orders WHERE ... status IN ('FILLED','EXIT_FILLED')
    # or a tuple/list of those statuses passed as parameters.
    has_orders_filled_count = (
        "FROM orders" in src
        and "FILLED" in src
        and "EXIT_FILLED" in src
        and "total_trades" in src
    )
    assert has_orders_filled_count, (
        "ap/position_manager.py must count total_trades from orders where "
        "status IN ('FILLED','EXIT_FILLED'). Use the durable orders truth, "
        "not the positions.status field (which gets reset by reconciler imports)."
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX-2: master_control bootstrap honors paper flag
# ─────────────────────────────────────────────────────────────────────────────

def test_bootstrap_mode_skipped_for_paper():
    """
    bootstrap_mode must NOT force qty=1 when self.paper is True.
    Paper has no live capital risk; forcing tiny qty kills proof-week.

    The check must be source-anchored — we look for a guard like
    `bootstrap_mode = (not self.paper) and (total_trades < ...)` OR a
    branch that explicitly bypasses the qty=1 force for paper.
    """
    src = _src(MC_PATH)
    # Accept either inline guard against self.paper, OR delegation to the
    # canonical _compute_bootstrap_mode helper (whose body must contain a
    # paper bypass).
    patterns = [
        r"bootstrap_mode\s*=\s*\(?\s*not\s+self\.paper\s*\)?\s*and\s+",
        r"bootstrap_mode\s*=\s*False\s+if\s+self\.paper",
        r"if\s+self\.paper\s*:\s*\n\s*bootstrap_mode\s*=\s*False",
        r"bootstrap_mode\s*=\s*self\._compute_bootstrap_mode\s*\(",
    ]
    assert any(re.search(p, src, re.DOTALL) for p in patterns), (
        "master_control bootstrap_mode must skip qty=1 force when self.paper is True. "
        "Either inline guard or delegation to _compute_bootstrap_mode helper."
    )
    # If delegating, verify the helper itself has the paper bypass.
    if re.search(r"bootstrap_mode\s*=\s*self\._compute_bootstrap_mode\s*\(", src):
        helper_match = re.search(
            r"def\s+_compute_bootstrap_mode\s*\(.*?(?=\n    def\s+\w)",
            src,
            re.DOTALL,
        )
        assert helper_match, "Could not locate _compute_bootstrap_mode body"
        helper_body = helper_match.group(0)
        assert re.search(r"if\s+self\.paper\s*:\s*\n\s*return\s+False", helper_body), (
            "_compute_bootstrap_mode must return False when self.paper is True."
        )


def test_bootstrap_mode_still_gates_live_when_total_trades_below_threshold():
    """
    LIVE mode must STILL enter bootstrap when total_trades < threshold.
    We don't change LIVE safety in this PR. Assert the live-side guard
    remains intact.
    """
    src = _src(MC_PATH)
    # The decision path must still reference total_trades for live path
    # via either a < comparison or an env-tunable threshold check.
    has_live_guard = (
        re.search(r"bootstrap_mode\s*=.*total_trades\s*<", src)
        or re.search(r"total_trades\s*<\s*(?:\d+|self\._bootstrap_threshold|BOOTSTRAP_TRADES_THRESHOLD)", src)
    )
    assert has_live_guard, (
        "LIVE bootstrap guard must remain: bootstrap_mode must still compare "
        "total_trades to a threshold so a brand-new LIVE deployment cannot "
        "instantly fire 10% trades before proving itself."
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX-3: sizing context persisted on the approved plan / order meta
# ─────────────────────────────────────────────────────────────────────────────

def test_master_control_persists_sizing_context_on_plan():
    """
    When a plan is approved, master_control must attach sizing context to
    plan.metadata so future audits can trace WHY contracts=N was chosen.

    Required keys:
      account_equity, risk_pct, contracts, budget, total_trades,
      bootstrap_mode
    """
    src = _src(MC_PATH)
    required_keys = [
        "account_equity",
        "risk_pct",
        "contracts",
        "budget",
        "total_trades",
        "bootstrap_mode",
    ]
    # The sizing_context lives inside the plan metadata dict literal as the
    # value of the "sizing_context" key. Loose match: slice 2000 chars
    # forward from the marker and grep the required keys inside.
    sc_idx = src.find('"sizing_context":')
    assert sc_idx >= 0, (
        "master_control must include a \"sizing_context\" key in plan metadata "
        "with sizing inputs so downstream audits can diagnose qty/budget decisions."
    )
    ctx_body = src[sc_idx : sc_idx + 2000]
    missing = [k for k in required_keys if f'"{k}"' not in ctx_body and f"'{k}'" not in ctx_body]
    assert not missing, (
        f"sizing_context is missing required keys: {missing}. "
        f"Need all of: {required_keys}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX-4: position row writes entry_price (not just avg_fill)
# ─────────────────────────────────────────────────────────────────────────────

def test_open_position_writes_entry_price_column():
    """
    open_position() builds the positions INSERT. It MUST include
    entry_price in the columns list, otherwise:
      - positions.entry_price stays NULL
      - ManagedPosition.option_pnl_pct returns 0.0 forever
      - profit-take and trailing-stop are dead
      - dashboard shows P&L = 0% even on +50% trades
    """
    src = _src(PM_PATH)
    # Slice from def open_position through end of the function body.
    op_match = re.search(
        r"def\s+open_position\s*\(.*?(?=\n    def\s+\w)",
        src,
        re.DOTALL,
    )
    assert op_match, "Could not locate open_position function body"
    body = op_match.group(0)
    # Accept either base-columns inclusion or a guarded append.
    has_entry_price = (
        '"entry_price"' in body or "'entry_price'" in body
    )
    assert has_entry_price, (
        "open_position() must include 'entry_price' in the INSERT "
        "(either in base columns or via _has_position_column-guarded append). "
        "Without it, restart-recovery loses entry_price and option_pnl_pct = 0%."
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX-5: reconciler underlying field — no contract[:6] truncation
# ─────────────────────────────────────────────────────────────────────────────

def test_reconciler_does_not_truncate_contract_to_6_chars():
    """
    ap_reconciler._broker_position_underlying must NOT fall back to
    `c_sym[:6]` when Tradier omits the underlying field. The 6-char
    slice produces garbage like 'MO2605' for MO260529P00074000, which
    is then stored as positions.underlying — hiding the position from
    the exit engine (queries by underlying='MO').

    Correct fallback: use _norm_underlying(c_sym) which knows how to
    parse the OCC option symbol back to the root ticker.
    """
    src = _src(RC_PATH)
    bpu_match = re.search(
        r"def\s+_broker_position_underlying\s*\(.*?(?=\n    def\s+\w)",
        src,
        re.DOTALL,
    )
    assert bpu_match, "Could not locate _broker_position_underlying"
    body = bpu_match.group(0)
    # Strip comments — a historical mention of `c_sym[:6]` in the
    # explanatory comment block must NOT trip this guard. Executable
    # code only.
    code_only = re.sub(r"#[^\n]*", "", body)
    assert "c_sym[:6]" not in code_only, (
        "_broker_position_underlying still has executable `c_sym[:6]` — "
        "that creates corrupted underlyings like 'MO2605' from "
        "'MO260529P00074000', hiding the position from the exit engine. "
        "Use _norm_underlying(c_sym)."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Behavioral: master_control bootstrap behavior with paper flag
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mc_module():
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    if "ap_master_control" in sys.modules:
        importlib.reload(sys.modules["ap_master_control"])
    import ap_master_control as mc
    return mc


def test_paper_master_control_does_not_set_bootstrap_when_total_trades_zero(mc_module):
    """
    Construct a paper master_control with total_trades=0 in the snapshot.
    The computed bootstrap_mode must be False (paper bypass).
    """
    # We can't run the full evaluate() path in unit scope (needs supabase,
    # position manager, etc.). We assert via a direct constructor + a small
    # helper that exposes the bootstrap decision. If the helper doesn't
    # exist, this test documents the requirement that paper-mode
    # bootstrap_mode resolves to False even when total_trades=0.
    has_helper = hasattr(mc_module.APMasterControl, "_compute_bootstrap_mode")
    if not has_helper:
        pytest.skip(
            "APMasterControl does not yet expose _compute_bootstrap_mode helper — "
            "skipping behavioral test; source-level checks cover the invariant."
        )
    mc_instance = mc_module.APMasterControl(
        mode="paper",
        client_id="test@example.com",
        score_floor=70,
        context_floor=0,
        max_positions=10,
        max_capital_pct=0.40,
        max_sector_pct=0.30,
        max_ticker_pct=0.20,
        max_calls=10,
        max_puts=10,
        max_trades_today=20,
        max_daily_loss=-1500,
        account_equity=25000,
        position_manager=None,
        position_sizer=None,
        supabase_client=None,
    )
    bm = mc_instance._compute_bootstrap_mode(total_trades=0)
    assert bm is False, "Paper mode with total_trades=0 must compute bootstrap_mode=False"


def test_live_master_control_keeps_bootstrap_when_total_trades_low(mc_module):
    """
    LIVE master_control with total_trades=0 MUST keep bootstrap_mode=True.
    LIVE safety is unchanged.
    """
    has_helper = hasattr(mc_module.APMasterControl, "_compute_bootstrap_mode")
    if not has_helper:
        pytest.skip("APMasterControl does not yet expose _compute_bootstrap_mode helper")
    mc_instance = mc_module.APMasterControl(
        mode="live",
        client_id="test@example.com",
        score_floor=70,
        context_floor=0,
        max_positions=10,
        max_capital_pct=0.40,
        max_sector_pct=0.30,
        max_ticker_pct=0.20,
        max_calls=10,
        max_puts=10,
        max_trades_today=20,
        max_daily_loss=-1500,
        account_equity=25000,
        position_manager=None,
        position_sizer=None,
        supabase_client=None,
    )
    bm = mc_instance._compute_bootstrap_mode(total_trades=0)
    assert bm is True, "LIVE mode with total_trades=0 must keep bootstrap_mode=True (safety)"


def test_live_master_control_clears_bootstrap_when_total_trades_high(mc_module):
    """LIVE with total_trades >= threshold must clear bootstrap_mode."""
    has_helper = hasattr(mc_module.APMasterControl, "_compute_bootstrap_mode")
    if not has_helper:
        pytest.skip("APMasterControl does not yet expose _compute_bootstrap_mode helper")
    mc_instance = mc_module.APMasterControl(
        mode="live",
        client_id="test@example.com",
        score_floor=70,
        context_floor=0,
        max_positions=10,
        max_capital_pct=0.40,
        max_sector_pct=0.30,
        max_ticker_pct=0.20,
        max_calls=10,
        max_puts=10,
        max_trades_today=20,
        max_daily_loss=-1500,
        account_equity=25000,
        position_manager=None,
        position_sizer=None,
        supabase_client=None,
    )
    bm = mc_instance._compute_bootstrap_mode(total_trades=999)
    assert bm is False, "LIVE mode with total_trades=999 must compute bootstrap_mode=False (graduated)"


# ─────────────────────────────────────────────────────────────────────────────
# FIX-2 (P0): QPM persists live quote state to positions table
# ─────────────────────────────────────────────────────────────────────────────

def test_qpm_has_persist_quote_to_db_method():
    """
    APPositionQuoteMonitor must expose a `_persist_quote_to_db` method.
    Without it, the positions table never reflects live current_option_price,
    current_underlying, or option_pnl_pct on any open position — and the
    dashboard / audit / restart-recovery are blind to live state.
    """
    sys.path.insert(0, str(REPO_ROOT))
    from ap.position_quote_monitor import APPositionQuoteMonitor
    assert hasattr(APPositionQuoteMonitor, "_persist_quote_to_db"), (
        "APPositionQuoteMonitor must define `_persist_quote_to_db` so the "
        "positions table reflects live quote/PnL on every poll."
    )


def test_qpm_persist_path_uses_throttle_and_client_scope():
    """
    Source-level guard:
      - `_persist_quote_to_db` must throttle by either time elapsed OR
        material price-delta to avoid DB hammering.
      - The SQL UPDATE must scope by BOTH `id` and `client_id` to prevent
        cross-client writes.
    """
    src = _src(REPO_ROOT / "ap" / "position_quote_monitor.py")
    fn_match = re.search(
        r"def\s+_persist_quote_to_db\s*\(.*?(?=\n    def\s+\w|\nclass\s+\w)",
        src,
        re.DOTALL,
    )
    assert fn_match, "Could not locate _persist_quote_to_db body"
    body = fn_match.group(0)
    assert (
        "QPM_DB_PERSIST_THROTTLE_SEC" in body
        and "QPM_DB_PERSIST_PRICE_DELTA_PCT" in body
    ), "_persist_quote_to_db must reference both throttle constants"
    assert (
        "UPDATE positions" in body
        and "WHERE id" in body
        and "AND client_id" in body
    ), "_persist_quote_to_db must UPDATE positions scoped by id AND client_id"


def test_qpm_refresh_calls_persist():
    """The QPM main refresh path must actually invoke _persist_quote_to_db."""
    src = _src(REPO_ROOT / "ap" / "position_quote_monitor.py")
    refresh_match = re.search(
        r"def\s+_refresh_once\s*\(.*?(?=\n    def\s+\w|\nclass\s+\w)",
        src,
        re.DOTALL,
    )
    assert refresh_match, "Could not locate _refresh_once body"
    body = refresh_match.group(0)
    assert "self._persist_quote_to_db(" in body, (
        "_refresh_once must call self._persist_quote_to_db so live quote "
        "state propagates to the positions table."
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX-3 (P0): Exit engine persists peak / max_profit_seen / touched_profit
# ─────────────────────────────────────────────────────────────────────────────

def test_exit_engine_has_persist_peak_state_method():
    """
    APExitEngine must expose `_persist_peak_state_to_db`. Without it
    peak_pnl_pct/max_profit_seen/touched_profit live only in memory; the
    positions table reports 0 on every winning trade.
    """
    sys.path.insert(0, str(REPO_ROOT))
    from ap_exit_engine import APExitEngine
    assert hasattr(APExitEngine, "_persist_peak_state_to_db"), (
        "APExitEngine must define `_persist_peak_state_to_db` so profit "
        "protection state propagates to the positions table."
    )


def test_exit_engine_persist_path_uses_throttle_and_client_scope():
    """Source-level guard on _persist_peak_state_to_db semantics."""
    src = _src(REPO_ROOT / "ap_exit_engine.py")
    fn_match = re.search(
        r"def\s+_persist_peak_state_to_db\s*\(.*?(?=\n    def\s+\w|\nclass\s+\w)",
        src,
        re.DOTALL,
    )
    assert fn_match, "Could not locate _persist_peak_state_to_db body"
    body = fn_match.group(0)
    assert "_EXIT_DB_PERSIST_THROTTLE_SEC" in body, (
        "_persist_peak_state_to_db must honor _EXIT_DB_PERSIST_THROTTLE_SEC."
    )
    assert (
        "UPDATE positions" in body
        and "WHERE id" in body
        and "AND client_id" in body
    ), "_persist_peak_state_to_db must UPDATE positions scoped by id AND client_id"
    assert "peak_pnl_pct" in body, "Must write peak_pnl_pct"
    assert "max_profit_seen" in body, "Must write max_profit_seen"
    assert "touched_profit" in body, "Must write touched_profit"


def test_exit_engine_invokes_persist_after_peak_update():
    """
    The exit-engine main evaluation loop must invoke
    `_persist_peak_state_to_db(pos)` after updating the in-memory peak.
    """
    src = _src(REPO_ROOT / "ap_exit_engine.py")
    assert "self._persist_peak_state_to_db(pos)" in src, (
        "Exit-engine evaluation loop must call _persist_peak_state_to_db(pos) "
        "after the in-memory peak/touched update."
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX-1 behavioral: OCC underlying normalization correctness
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def reconciler_norm_helper():
    """
    Construct just enough of APBrokerReconciler to call _broker_position_underlying.
    We bypass __init__ by creating an empty instance via __new__ and assigning
    the methods we actually exercise.
    """
    sys.path.insert(0, str(REPO_ROOT))
    from ap_reconciler import APBrokerReconciler
    inst = APBrokerReconciler.__new__(APBrokerReconciler)
    return inst


@pytest.mark.parametrize("bp,expected", [
    ({"symbol": "MO260529P00074000"},      "MO"),
    ({"symbol": "AAPL260529P00215000"},    "AAPL"),
    ({"symbol": "SPY260529P00520000"},     "SPY"),
    ({"symbol": "BAC260529C00052000"},     "BAC"),
    ({"symbol": "NVDA260529P00215000"},    "NVDA"),
    ({"symbol": "SMCI260529C00036500"},    "SMCI"),
    ({"underlying": "META", "symbol": "META260515C00615000"}, "META"),  # explicit underlying wins
    ({"root_symbol": "QQQ", "symbol": "QQQ260530C00440000"},  "QQQ"),
    ({"ticker": "TSLA",   "symbol": "TSLA260515C00452500"},   "TSLA"),
    ({"symbol": "AAPL"},  "AAPL"),  # plain ticker passthrough
])
def test_broker_position_underlying_returns_root_not_truncation(reconciler_norm_helper, bp, expected):
    """
    The fixed reconciler must return the true root ticker for every OCC
    option symbol it sees, NOT a 6-char prefix. Today's MO/NOW/BA/NVDA
    incidents (positions.underlying='MO2605', 'NOW260', 'NVDA26', etc.)
    must not be reproducible.
    """
    got = reconciler_norm_helper._broker_position_underlying(bp)
    assert got == expected, (
        f"_broker_position_underlying({bp}) returned {got!r}; expected {expected!r}. "
        f"Reconciler is still corrupting positions.underlying."
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX-4 behavioral: open_position must accept entry_price and persist it
# ─────────────────────────────────────────────────────────────────────────────

def test_open_position_signature_accepts_entry_price():
    """open_position must accept entry_price as a keyword argument."""
    sys.path.insert(0, str(REPO_ROOT))
    import inspect
    from ap.position_manager import APPositionManager
    sig = inspect.signature(APPositionManager.open_position)
    assert "entry_price" in sig.parameters, (
        "APPositionManager.open_position must accept entry_price kw."
    )
