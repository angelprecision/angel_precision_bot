# ap/crypto.py
"""
Encryption utilities for storing sensitive client credentials (Tradier tokens).
Uses Fernet (symmetric encryption) with key from ENCRYPTION_KEY env var.

Supports two key derivation schemes:
  - NEW: ENCRYPTION_KEY is a raw 44-char base64 Fernet key used directly
  - LEGACY: SHA256(ENCRYPTION_KEY) → base64 → Fernet key
decrypt_token() tries both so existing tokens still work during migration.
"""
import os
import hashlib
import base64
from cryptography.fernet import Fernet
from ap.logger import get_logger

log = get_logger("ap.crypto")


def _get_encryption_key() -> bytes:
    """Return raw key from env — must be valid 44-char base64 Fernet key."""
    key = os.getenv("ENCRYPTION_KEY", "").strip()
    if not key:
        raise RuntimeError("ENCRYPTION_KEY is not set (required for production).")
    return key.encode() if isinstance(key, str) else key


def _get_legacy_encryption_key() -> bytes:
    """Old scheme: SHA256(raw_string) → base64 → Fernet key."""
    raw = os.getenv("ENCRYPTION_KEY", "").strip()
    if not raw:
        raise RuntimeError("ENCRYPTION_KEY is not set (required for production).")
    digest = hashlib.sha256(raw.encode()).digest()
    return base64.urlsafe_b64encode(digest)


def encrypt_token(plaintext: str) -> str:
    """Encrypt with the NEW (direct Fernet key) scheme."""
    if not plaintext:
        return ""
    key = _get_encryption_key()
    f = Fernet(key)
    return f.encrypt(plaintext.encode()).decode()


def decrypt_token(ciphertext: str) -> str:
    """Try new scheme first, fall back to legacy SHA256 scheme."""
    if not ciphertext:
        return ""
    token_bytes = ciphertext.encode() if isinstance(ciphertext, str) else ciphertext

    # Try new scheme first
    try:
        f = Fernet(_get_encryption_key())
        return f.decrypt(token_bytes).decode()
    except Exception:
        pass

    # Fall back to legacy SHA256-derived key
    try:
        f = Fernet(_get_legacy_encryption_key())
        result = f.decrypt(token_bytes).decode()
        log.warning("Token decrypted with LEGACY scheme — re-encrypt to migrate")
        return result
    except Exception as e:
        log.error("Token decrypt failed with BOTH schemes: %s", e)
        raise ValueError(f"Token decrypt failed: {e}") from e


def generate_encryption_key() -> str:
    return Fernet.generate_key().decode()
