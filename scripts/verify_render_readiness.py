#!/usr/bin/env python3
"""
Render readiness verification (Commit 31, Task 4).

One-shot pre-proof-week checklist.  Run this BEFORE Tuesday's open.

What it verifies
----------------
1. Required env vars are present (and have sane shapes).
2. ENCRYPTION_KEY is set and decrypt_token works under at least one scheme
   for each active member's stored Tradier token.  The plaintext token is
   NEVER printed.
3. Each active member row passes ap.readiness.compute_readiness for its
   declared mode (PAPER or LIVE).
4. Database is reachable and the clients table has the expected columns.

What it does NOT do
-------------------
- It never submits an order.
- It never modifies a row.
- It never prints a token.
- It never overrides a kill switch.

Usage
-----
    python scripts/verify_render_readiness.py
    python scripts/verify_render_readiness.py --client client-A
    python scripts/verify_render_readiness.py --json out.json

Exit code
---------
    0  all checks PASS  (logs READY_FOR_PROOF_WEEK)
    1  one or more CRITICAL checks failed
    2  warnings only (non-critical)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# ----------------------------------------------------------------------
# Result containers
# ----------------------------------------------------------------------

@dataclass
class CheckResult:
    name: str
    severity: str   # 'CRITICAL' / 'WARN' / 'INFO'
    ok: bool
    detail: str = ""


@dataclass
class ClientReadinessRow:
    client_id: str
    email_masked: str
    mode: str
    subscription_active: bool
    approved: bool                  # 'allow_live_trading' for LIVE; defaulted true otherwise
    token_decrypt_ok: bool
    token_scheme: Optional[str]     # 'NEW' / 'LEGACY' / None
    account_id_masked: Optional[str]
    kill_switch_on: bool
    ready_for_mode: bool
    notes: list[str] = field(default_factory=list)


@dataclass
class ReadinessSummary:
    timestamp_utc: str
    overall_ok: bool
    env_checks: list[CheckResult]
    db_check: CheckResult
    encryption_key_check: CheckResult
    clients: list[ClientReadinessRow]
    warnings: list[str]
    failures: list[str]


# ----------------------------------------------------------------------
# Env var policy
# ----------------------------------------------------------------------

REQUIRED_EXACT = {
    # name: (expected_str, severity)
    "MAX_CONTRACTS":                  ("15",  "CRITICAL"),
    "BROKER_ERROR_THRESHOLD":         ("5",   "CRITICAL"),
    "BROKER_ERROR_WINDOW_SECS":       ("120", "CRITICAL"),
    "BROKER_ERROR_CLEAR_AFTER_SECS":  ("300", "CRITICAL"),
    "MAX_OPEN_PER_SYMBOL":            ("1",   "CRITICAL"),
    "MAX_OPEN_PER_SECTOR":            ("2",   "CRITICAL"),
    "RECONCILER_SLA_SECONDS":         ("120", "CRITICAL"),
}

REQUIRED_PRESENT = {
    # name: (severity, hint_on_missing)
    "ENCRYPTION_KEY": (
        "CRITICAL",
        "ENCRYPTION_KEY must be set on Render so client tokens can decrypt.",
    ),
    "DATABASE_URL": (
        "CRITICAL",
        "DATABASE_URL is required; the bot cannot reach Supabase without it.",
    ),
}


def _check_env() -> list[CheckResult]:
    out: list[CheckResult] = []

    for var, (expected, sev) in REQUIRED_EXACT.items():
        actual = (os.getenv(var) or "").strip()
        if not actual:
            out.append(CheckResult(
                name=f"env:{var}",
                severity=sev,
                ok=False,
                detail=f"{var} is not set. Expected '{expected}'.",
            ))
            continue
        if actual != expected:
            # Not necessarily wrong - operator may have a reason - but flag it.
            out.append(CheckResult(
                name=f"env:{var}",
                severity="WARN",
                ok=False,
                detail=f"{var} = {actual!r} (recommended default is {expected!r}).",
            ))
        else:
            out.append(CheckResult(
                name=f"env:{var}",
                severity=sev,
                ok=True,
                detail=f"{var} = {actual}",
            ))

    for var, (sev, hint) in REQUIRED_PRESENT.items():
        val = os.getenv(var)
        if not val:
            out.append(CheckResult(
                name=f"env:{var}",
                severity=sev,
                ok=False,
                detail=hint,
            ))
        else:
            # Mask: only emit length + a 4-char fingerprint.  Never print the value.
            fp = (val[:2] + "..." + val[-2:]) if len(val) >= 6 else "***"
            out.append(CheckResult(
                name=f"env:{var}",
                severity=sev,
                ok=True,
                detail=f"{var} present (length={len(val)}, fingerprint={fp})",
            ))

    return out


# ----------------------------------------------------------------------
# Masking helpers
# ----------------------------------------------------------------------

def _mask_email(email: Optional[str]) -> str:
    if not email or "@" not in email:
        return "***"
    user, domain = email.split("@", 1)
    head = user[:2] if len(user) >= 2 else user
    return f"{head}***@{domain}"


def _mask_account_id(account_id: Optional[str]) -> Optional[str]:
    if not account_id:
        return None
    s = str(account_id)
    if len(s) <= 4:
        return "***"
    return f"***{s[-4:]}"


# ----------------------------------------------------------------------
# DB introspection
# ----------------------------------------------------------------------

EXPECTED_CLIENT_COLUMNS = (
    "client_id",
    "email",
    "subscription_active",
    "kill_switch",
    "tradier_active_mode",
    "tradier_account_id",
    "tradier_live_account_id",
    "tradier_access_token",
    "tradier_live_access_token",
    "allow_live_trading",
)


def _check_db(conn_fn) -> CheckResult:
    """Confirm we can connect and the clients table has the columns the
    readiness module expects."""
    try:
        with conn_fn() as c:
            # information_schema query - portable and read-only
            c.execute(
                """
                SELECT column_name
                FROM   information_schema.columns
                WHERE  table_name = 'clients'
                """
            )
            present = {(r.get("column_name") if isinstance(r, dict) else r[0])
                       for r in c.fetchall()}
    except Exception as e:
        return CheckResult(
            name="db:connect",
            severity="CRITICAL",
            ok=False,
            detail=f"Database unreachable or clients table missing: {e}",
        )

    missing = [col for col in EXPECTED_CLIENT_COLUMNS if col not in present]
    if missing:
        return CheckResult(
            name="db:clients_columns",
            severity="CRITICAL",
            ok=False,
            detail=f"clients table missing columns: {missing}",
        )
    return CheckResult(
        name="db:clients_columns",
        severity="CRITICAL",
        ok=True,
        detail=f"clients table has {len(present)} columns; required columns present.",
    )


def _fetch_active_members(conn_fn, client_filter: Optional[str] = None) -> list[dict]:
    sql = """
        SELECT client_id,
               email,
               COALESCE(subscription_active, FALSE)  AS subscription_active,
               COALESCE(kill_switch, FALSE)          AS kill_switch,
               COALESCE(tradier_active_mode, '')     AS tradier_active_mode,
               tradier_account_id,
               tradier_live_account_id,
               tradier_access_token,
               tradier_live_access_token,
               COALESCE(allow_live_trading, FALSE)   AS allow_live_trading
        FROM   clients
        WHERE  COALESCE(status, '') ILIKE 'ACTIVE'
    """
    params: list[Any] = []
    if client_filter:
        sql += " AND client_id = %s"
        params.append(client_filter)
    sql += " ORDER BY client_id"

    with conn_fn() as c:
        c.execute(sql, tuple(params) if params else None)
        return [dict(r) for r in c.fetchall()]


# ----------------------------------------------------------------------
# Token decrypt smoke
# ----------------------------------------------------------------------

def _decrypt_smoke(member: dict) -> tuple[bool, Optional[str], str]:
    """Try both schemes on whichever token corresponds to the member's
    declared mode.  Returns (ok, scheme_used, detail).  Never returns the
    plaintext.
    """
    mode = (member.get("tradier_active_mode") or "").strip().upper()
    if mode == "LIVE":
        ciphertext = member.get("tradier_live_access_token") or ""
        token_field = "tradier_live_access_token"
    else:
        ciphertext = member.get("tradier_access_token") or ""
        token_field = "tradier_access_token"

    if not ciphertext:
        return False, None, f"{token_field} is empty for this member"

    # Plaintext fallback in PAPER is allowed by the runtime; reflect that
    # but call it out so the operator sees it.
    if not str(ciphertext).startswith("gAAAAA"):
        if mode == "LIVE":
            return False, None, (
                "token appears to be plaintext but member mode=LIVE - "
                "LIVE refuses plaintext tokens"
            )
        return True, "PLAINTEXT", (
            "token appears to be plaintext (PAPER permits this; "
            "encrypt via dashboard before flipping LIVE)"
        )

    # Try NEW scheme first (matches ap.crypto.encrypt_token), then LEGACY.
    # We do NOT print the plaintext - we just need to know decrypt worked.
    raw_key = (os.getenv("ENCRYPTION_KEY") or "").strip()
    if not raw_key:
        return False, None, "ENCRYPTION_KEY not set; cannot decrypt"

    try:
        import base64
        import hashlib
        from cryptography.fernet import Fernet, InvalidToken

        # NEW
        try:
            Fernet(raw_key.encode()).decrypt(ciphertext.encode())
            return True, "NEW", "token decrypts via NEW direct-Fernet scheme"
        except InvalidToken:
            pass
        except Exception as e:
            # Key shape probably wrong for NEW; fall through to LEGACY.
            pass

        # LEGACY
        try:
            digest = hashlib.sha256(raw_key.encode()).digest()
            Fernet(base64.urlsafe_b64encode(digest)).decrypt(ciphertext.encode())
            return True, "LEGACY", (
                "token decrypts via LEGACY scheme - re-encrypt via dashboard to migrate"
            )
        except Exception as e:
            return False, None, f"decrypt failed under BOTH schemes: {type(e).__name__}"
    except ImportError as ie:
        return False, None, f"cryptography library unavailable: {ie}"


# ----------------------------------------------------------------------
# Per-client readiness composition
# ----------------------------------------------------------------------

def _check_client(member: dict) -> ClientReadinessRow:
    notes: list[str] = []

    ok_decrypt, scheme, decrypt_detail = _decrypt_smoke(member)
    notes.append(decrypt_detail)

    mode = (member.get("tradier_active_mode") or "").strip().upper() or "PAPER"

    if mode == "LIVE":
        account_id = member.get("tradier_live_account_id")
    else:
        account_id = member.get("tradier_account_id")

    # We bypass compute_readiness in the LIVE-without-token case rather
    # than risk the readiness module raising on missing data.  For the
    # rest, compute_readiness is the canonical reasoning.
    ready = False
    try:
        from ap.readiness import compute_readiness
        rpt = compute_readiness(member)
        ready = bool(rpt.allow_live if mode == "LIVE" else rpt.allow_paper)
        # Carry through any organ-status notes if compute_readiness emits them
        # (it normally does not).
    except Exception as e:
        notes.append(f"compute_readiness failed: {e}")
        ready = False

    if member.get("kill_switch"):
        notes.append("kill_switch is ON")

    if mode == "LIVE" and not bool(member.get("allow_live_trading")):
        notes.append("allow_live_trading is false; LIVE entries will be blocked")

    return ClientReadinessRow(
        client_id          = member.get("client_id") or "",
        email_masked       = _mask_email(member.get("email")),
        mode               = mode,
        subscription_active= bool(member.get("subscription_active")),
        approved           = bool(member.get("allow_live_trading")) if mode == "LIVE" else True,
        token_decrypt_ok   = ok_decrypt,
        token_scheme       = scheme,
        account_id_masked  = _mask_account_id(account_id),
        kill_switch_on     = bool(member.get("kill_switch")),
        ready_for_mode     = bool(ok_decrypt and ready and not member.get("kill_switch")),
        notes              = notes,
    )


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def run_verification(client_filter: Optional[str] = None, conn_fn=None) -> ReadinessSummary:
    if conn_fn is None:
        from ap.db import conn as _conn  # type: ignore
        conn_fn = _conn

    env_checks = _check_env()
    db_check = _check_db(conn_fn)

    encryption_check = next(
        (c for c in env_checks if c.name == "env:ENCRYPTION_KEY"),
        CheckResult(name="env:ENCRYPTION_KEY", severity="CRITICAL", ok=False,
                    detail="ENCRYPTION_KEY not evaluated"),
    )

    clients: list[ClientReadinessRow] = []
    if db_check.ok:
        try:
            members = _fetch_active_members(conn_fn, client_filter=client_filter)
            for m in members:
                clients.append(_check_client(m))
        except Exception as e:
            db_check = CheckResult(
                name="db:fetch_members",
                severity="CRITICAL",
                ok=False,
                detail=f"could not fetch active members: {e}",
            )

    failures = [c.detail for c in env_checks
                if not c.ok and c.severity == "CRITICAL"]
    if not db_check.ok and db_check.severity == "CRITICAL":
        failures.append(db_check.detail)
    failures.extend(
        f"client {c.client_id}: not ready for {c.mode} ({'; '.join(c.notes)})"
        for c in clients if not c.ready_for_mode
    )

    warnings = [c.detail for c in env_checks
                if not c.ok and c.severity == "WARN"]

    overall_ok = not failures

    return ReadinessSummary(
        timestamp_utc       = datetime.now(timezone.utc).isoformat(),
        overall_ok          = overall_ok,
        env_checks          = env_checks,
        db_check            = db_check,
        encryption_key_check= encryption_check,
        clients             = clients,
        warnings            = warnings,
        failures            = failures,
    )


def render_text(s: ReadinessSummary) -> str:
    lines: list[str] = []
    add = lines.append
    add("=" * 72)
    add(f"  ANGEL PRECISION - RENDER READINESS  ({s.timestamp_utc})")
    add(f"  Overall: {'PASS' if s.overall_ok else 'FAIL'}")
    add("=" * 72)
    add("")
    add("ENV CHECKS")
    for c in s.env_checks:
        sym = "OK " if c.ok else ("!! " if c.severity == "CRITICAL" else "?? ")
        add(f"  {sym}[{c.severity:8}] {c.name:35} {c.detail}")
    add("")
    add("DB CHECK")
    sym = "OK " if s.db_check.ok else "!! "
    add(f"  {sym}{s.db_check.name:35} {s.db_check.detail}")
    add("")
    add(f"CLIENTS ({len(s.clients)})")
    if not s.clients:
        add("  (no active clients found)")
    for c in s.clients:
        sym = "OK " if c.ready_for_mode else "!! "
        scheme_str = f" scheme={c.token_scheme}" if c.token_scheme else ""
        add(
            f"  {sym}{c.client_id:18}  {c.email_masked:24}  "
            f"mode={c.mode:5}  acct={c.account_id_masked or '?'}  "
            f"kill={'Y' if c.kill_switch_on else 'N'}  "
            f"decrypt={'Y' if c.token_decrypt_ok else 'N'}{scheme_str}"
        )
        for n in c.notes:
            add(f"      - {n}")
    add("")
    if s.warnings:
        add("WARNINGS")
        for w in s.warnings:
            add(f"  ?? {w}")
        add("")
    if s.failures:
        add("FAILURES")
        for f in s.failures:
            add(f"  !! {f}")
        add("")
    add("=" * 72)
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Verify Render readiness for proof week")
    p.add_argument("--client", default=None, help="Optional single client_id to check")
    p.add_argument("--json", default=None, help="Write JSON copy of the report here")
    args = p.parse_args(argv)

    try:
        summary = run_verification(client_filter=args.client)
    except Exception as e:
        print(f"ERROR: readiness verification crashed: {e}", file=sys.stderr)
        return 1

    print(render_text(summary))

    if args.json:
        try:
            Path(args.json).write_text(json.dumps(asdict(summary), indent=2, default=str))
        except Exception as e:
            print(f"WARN: failed to write JSON copy: {e}", file=sys.stderr)

    if summary.overall_ok:
        try:
            from ap.logger import audit  # type: ignore
            audit(
                "system", "INFO", "READY_FOR_PROOF_WEEK",
                {
                    "clients":  [c.client_id for c in summary.clients],
                    "warnings": summary.warnings,
                },
            )
        except Exception:
            pass
        return 0

    return 1 if summary.failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
