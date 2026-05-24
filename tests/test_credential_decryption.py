"""
Tests for client_runner.decrypt_token dual-scheme support
(fix/credential-auth-client-scope-hardening).

These prove:
  1. Tokens encrypted via ap.crypto.encrypt_token (NEW direct-Fernet
     scheme) decrypt correctly through client_runner.decrypt_token.
  2. Tokens encrypted via the LEGACY SHA256-derived scheme still decrypt.
  3. LIVE mode rejects plaintext.
  4. LIVE mode rejects invalid encrypted tokens after BOTH schemes fail.
  5. PAPER mode accepts plaintext.
  6. PAPER mode preserves the existing 'return ciphertext as plaintext'
     fallback on invalid encrypted input.

Run:
    pytest tests/test_credential_decryption.py -xvs
"""
from __future__ import annotations

import base64
import hashlib
import os
import sys
import types
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

REPO_ROOT = Path(__file__).resolve().parents[1]

# Set env BEFORE importing client_runner (it reads ENCRYPTION_KEY at module
# load into _raw_key, and ap.db raises if DATABASE_URL is missing).
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_credentials",
)
os.environ.setdefault("ENCRYPTION_KEY", "angel-test-key-for-pytest-2026")

# client_runner pulls in supabase at module load. We don't need a real
# supabase here — decrypt_token never touches it — so install a stub.
if "supabase" not in sys.modules:
    _supa_stub = types.ModuleType("supabase")
    _supa_stub.create_client = lambda *a, **kw: None
    _supa_stub.Client = type("Client", (), {})
    sys.modules["supabase"] = _supa_stub


# ---- key helpers (mirror what the module does) -------------------------

def _legacy_fernet(raw_key: str) -> Fernet:
    """LEGACY: SHA256(raw_key) -> urlsafe-b64 -> Fernet key."""
    digest = hashlib.sha256(raw_key.encode()).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def _new_fernet_key() -> str:
    """A fresh 44-char base64 Fernet key (NEW scheme uses this directly)."""
    return Fernet.generate_key().decode()


# ---- fixtures ---------------------------------------------------------

@pytest.fixture(scope="module")
def client_runner_module():
    import client_runner
    return client_runner


@pytest.fixture
def isolated_key(monkeypatch, client_runner_module):
    """Yield a fresh raw ENCRYPTION_KEY value and patch client_runner._raw_key
    to match. Restores the previous value automatically.
    """
    new_key = _new_fernet_key()
    monkeypatch.setattr(client_runner_module, "_raw_key", new_key)
    yield new_key


# ============================================================
# 1) NEW scheme round-trip via ap.crypto.encrypt_token
# ============================================================

class TestNewSchemeDecrypt:
    def test_ap_crypto_encrypt_then_runner_decrypt(self, monkeypatch, client_runner_module):
        """A token produced by ap.crypto.encrypt_token must decrypt
        through client_runner.decrypt_token."""
        new_key = _new_fernet_key()
        monkeypatch.setenv("ENCRYPTION_KEY", new_key)
        monkeypatch.setattr(client_runner_module, "_raw_key", new_key)

        # Force ap.crypto to re-read ENCRYPTION_KEY (it reads at call time).
        import ap.crypto as crypto
        plaintext = "tradier_token_ABC123"
        encrypted = crypto.encrypt_token(plaintext)

        # PAPER mode (no policy enforcement) — round-trip must work.
        recovered = client_runner_module.decrypt_token(encrypted, mode="PAPER")
        assert recovered == plaintext

        # LIVE mode — same round-trip must succeed (NEW scheme is the
        # canonical production path).
        recovered_live = client_runner_module.decrypt_token(encrypted, mode="LIVE")
        assert recovered_live == plaintext

    def test_new_scheme_does_not_emit_legacy_warning(
            self, monkeypatch, client_runner_module, caplog
    ):
        """If NEW scheme works, no LEGACY warning fires."""
        import logging
        new_key = _new_fernet_key()
        monkeypatch.setenv("ENCRYPTION_KEY", new_key)
        monkeypatch.setattr(client_runner_module, "_raw_key", new_key)

        import ap.crypto as crypto
        encrypted = crypto.encrypt_token("token-xyz")

        with caplog.at_level(logging.WARNING):
            client_runner_module.decrypt_token(encrypted, mode="PAPER")
        for rec in caplog.records:
            assert "LEGACY scheme" not in rec.getMessage(), \
                "LEGACY warning must not fire when NEW scheme succeeds"


# ============================================================
# 2) LEGACY scheme still works
# ============================================================

class TestLegacySchemeDecrypt:
    def test_legacy_token_decrypts_under_paper(
            self, monkeypatch, client_runner_module, caplog
    ):
        """A token encrypted under the LEGACY SHA256 scheme must still
        decrypt (so existing live deployments don't break)."""
        import logging
        raw = "legacy-passphrase-not-a-fernet-key"
        monkeypatch.setattr(client_runner_module, "_raw_key", raw)
        plaintext = "live_tradier_legacy_token"
        encrypted = _legacy_fernet(raw).encrypt(plaintext.encode()).decode()

        with caplog.at_level(logging.WARNING):
            recovered = client_runner_module.decrypt_token(encrypted, mode="PAPER")
        assert recovered == plaintext

        # The LEGACY warning MUST fire so operators see the migration nudge.
        assert any(
            "LEGACY scheme" in r.getMessage() for r in caplog.records
        ), "LEGACY decryption must log the migration warning"

    def test_legacy_token_decrypts_under_live(
            self, monkeypatch, client_runner_module
    ):
        """A token encrypted under LEGACY must still decrypt in LIVE mode
        (we cannot break live deployments during the migration window)."""
        raw = "legacy-live-passphrase"
        monkeypatch.setattr(client_runner_module, "_raw_key", raw)
        plaintext = "live_tradier_legacy_token"
        encrypted = _legacy_fernet(raw).encrypt(plaintext.encode()).decode()
        assert client_runner_module.decrypt_token(encrypted, mode="LIVE") == plaintext


# ============================================================
# 3) LIVE mode rejects plaintext
# ============================================================

class TestLiveModeRejectsPlaintext:
    def test_plaintext_token_raises_in_live(self, isolated_key, client_runner_module):
        with pytest.raises(RuntimeError) as e:
            client_runner_module.decrypt_token("PT-not-encrypted", mode="LIVE")
        assert "LIVE" in str(e.value)
        assert "Plaintext" in str(e.value) or "plaintext" in str(e.value).lower()

    def test_empty_plaintext_raises_in_any_mode(self, client_runner_module):
        with pytest.raises(ValueError):
            client_runner_module.decrypt_token("", mode="LIVE")
        with pytest.raises(ValueError):
            client_runner_module.decrypt_token("", mode="PAPER")


# ============================================================
# 4) LIVE mode rejects invalid encrypted tokens (both schemes fail)
# ============================================================

class TestLiveModeRejectsBadCiphertext:
    def test_invalid_ciphertext_raises_with_both_errors(
            self, isolated_key, client_runner_module
    ):
        # Looks-encrypted (gAAAAA...) but is actually garbage.
        bad = "gAAAAA" + "X" * 100
        with pytest.raises(RuntimeError) as e:
            client_runner_module.decrypt_token(bad, mode="LIVE")
        msg = str(e.value)
        # Spec: error must reference BOTH scheme failures.
        assert "new_scheme_error" in msg, \
            "LIVE failure must surface the NEW scheme error"
        assert "legacy_scheme_error" in msg, \
            "LIVE failure must surface the LEGACY scheme error"
        assert "BOTH" in msg or "both" in msg.lower()

    def test_missing_encryption_key_in_live_raises(
            self, monkeypatch, client_runner_module
    ):
        """If ENCRYPTION_KEY is empty AND the token looks encrypted, LIVE
        must fail closed."""
        monkeypatch.setattr(client_runner_module, "_raw_key", "")
        with pytest.raises(RuntimeError) as e:
            client_runner_module.decrypt_token("gAAAAAlooks_encrypted", mode="LIVE")
        assert "ENCRYPTION_KEY" in str(e.value)


# ============================================================
# 5) PAPER mode accepts plaintext (existing behavior)
# ============================================================

class TestPaperModeAcceptsPlaintext:
    def test_plaintext_returned_verbatim(self, isolated_key, client_runner_module):
        assert client_runner_module.decrypt_token(
            "plaintext-tradier-sandbox-token", mode="PAPER"
        ) == "plaintext-tradier-sandbox-token"

    def test_paper_default_mode_accepts_plaintext(
            self, isolated_key, client_runner_module
    ):
        # Default mode is PAPER \u2014 plaintext must round-trip without error.
        assert client_runner_module.decrypt_token("PT") == "PT"


# ============================================================
# 6) PAPER mode preserves fallback for invalid encrypted input
# ============================================================

class TestPaperModeFallback:
    def test_invalid_ciphertext_returns_raw_in_paper(
            self, isolated_key, client_runner_module, caplog
    ):
        """Existing behavior preserved: an unparseable encrypted-looking
        value in PAPER mode logs loudly and returns the raw value rather
        than raising. This keeps dev/sandbox iteration alive."""
        import logging
        bad = "gAAAAA" + "X" * 100
        with caplog.at_level(logging.ERROR):
            out = client_runner_module.decrypt_token(bad, mode="PAPER")
        assert out == bad, "PAPER fallback must return the raw ciphertext"
        # And it must shout in the log.
        assert any(
            "decrypt_token" in r.getMessage() and "PAPER mode" in r.getMessage()
            for r in caplog.records
        ), "PAPER fallback must log an error so the operator sees it"

    def test_missing_encryption_key_in_paper_returns_raw(
            self, monkeypatch, client_runner_module
    ):
        """When ENCRYPTION_KEY is unset AND token looks encrypted, PAPER
        returns the raw value (existing fallback)."""
        monkeypatch.setattr(client_runner_module, "_raw_key", "")
        assert client_runner_module.decrypt_token(
            "gAAAAAencrypted-looking", mode="PAPER"
        ) == "gAAAAAencrypted-looking"
