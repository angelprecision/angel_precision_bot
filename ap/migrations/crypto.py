# ap/crypto.py
"""
Encryption utilities for storing sensitive client credentials
Uses Fernet (symmetric encryption) with key from environment variable
"""
import os
import base64
from cryptography.fernet import Fernet
from ap.logger import get_logger

log = get_logger("ap.crypto")


def _get_encryption_key() -> bytes:
    """
    Get encryption key from environment variable.
    If not set, generate a new one (DEV ONLY - not for production!)
    """
    key = os.getenv("ENCRYPTION_KEY", "").strip()
    
    if not key:
        log.warning("⚠️ ENCRYPTION_KEY not set! Generating temporary key (DEV ONLY)")
        # Generate new key (only for development)
        key = Fernet.generate_key().decode()
        log.warning(f"Generated key: {key}")
        log.warning("⚠️ Set this as ENCRYPTION_KEY environment variable!")
    
    # Handle both raw bytes and base64 encoded strings
    if isinstance(key, str):
        key = key.encode()
    
    return key


def encrypt_token(plaintext: str) -> str:
    """
    Encrypt a string (e.g., Tradier access token)
    Returns: Base64-encoded ciphertext
    """
    if not plaintext:
        return ""
    
    try:
        key = _get_encryption_key()
        f = Fernet(key)
        ciphertext = f.encrypt(plaintext.encode())
        return ciphertext.decode()
    except Exception as e:
        log.error(f"Encryption failed: {e}")
        raise ValueError(f"Failed to encrypt token: {e}")


def decrypt_token(ciphertext: str) -> str:
    """
    Decrypt a token
    Returns: Original plaintext string
    """
    if not ciphertext:
        return ""
    
    try:
        key = _get_encryption_key()
        f = Fernet(key)
        plaintext = f.decrypt(ciphertext.encode())
        return plaintext.decode()
    except Exception as e:
        log.error(f"Decryption failed: {e}")
        raise ValueError(f"Failed to decrypt token: {e}")


def generate_encryption_key() -> str:
    """
    Generate a new encryption key for use as ENCRYPTION_KEY env var
    Run this once and save the output!
    """
    key = Fernet.generate_key()
    return key.decode()


if __name__ == "__main__":
    # Generate a new key for production use
    print("=" * 60)
    print("ENCRYPTION KEY GENERATOR")
    print("=" * 60)
    key = generate_encryption_key()
    print(f"\nYour encryption key (save this!):\n{key}\n")
    print("Add to Render environment variables:")
    print(f"ENCRYPTION_KEY={key}")
    print("=" * 60)
