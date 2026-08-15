"""API key generation and lookup.

Keys look like `tk_<32url-safe-chars>`. We store the SHA-256 hash + a short
prefix so we can display "tk_abcd..." without ever storing the plaintext key.

API keys are already high-entropy (>=192 bits), so a plain SHA-256 is enough —
bcrypt is only for low-entropy passwords.
"""
import hashlib
import hmac
import secrets
from dataclasses import dataclass
from typing import Optional


KEY_PREFIX_LEN = 8  # chars of the raw key to keep for display


def generate_api_key() -> tuple[str, str, str]:
    """Return (plaintext_key, key_prefix, key_hash)."""
    raw = "tk_" + secrets.token_urlsafe(32)
    prefix = raw[:KEY_PREFIX_LEN]
    key_hash = hashlib.sha256(raw.encode()).hexdigest()
    return raw, prefix, key_hash


def hash_key(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def constant_time_equals(a: str, b: str) -> bool:
    return hmac.compare_digest(a, b)


@dataclass
class AuthenticatedUser:
    user_id: int
    email: str
    plan: str
    api_key_id: int
