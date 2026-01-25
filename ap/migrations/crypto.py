# ap/crypto.py
"""
Encryption utilities for storing sensitive client credentials (Tradier tokens).
Uses Fernet (symmetric encryption) with key from ENCRYPTION_KEY env var.
"""
import os
from cryptography.fernet import Fernet
from ap.logger import get_logger

log = get_logger("ap.crypto")


def _get_encryption_key() -> bytes:
    key = os.getenv("ENCRYPTION_KEY", "").strip()
    if not key:
        raise RuntimeError("ENCRYPTION_KEY is not set (required for production).")
    return key.encode() if isinstance(key, str) else key


def encrypt_token(plaintext: str) -> str:
    if not plaintext:
        return ""
    key = _get_encryption_key()
    f = Fernet(key)
    return f.encrypt(plaintext.encode()).decode()


def decrypt_token(ciphertext: str) -> str:
    if not ciphertext:
        return ""
    key = _get_encryption_key()
    f = Fernet(key)
    return f.decrypt(ciphertext.encode()).decode()


def generate_encryption_key() -> str:
    return Fernet.generate_key().decode()
