from cryptography.fernet import Fernet

from .env import get_env

__all__ = ["decrypt_token", "encrypt_token"]


def _get_cipher() -> Fernet:
    key = get_env("OAUTH_ENCRYPTION_KEY")
    return Fernet(key.encode())


def encrypt_token(token: str) -> str:
    cipher = _get_cipher()
    return cipher.encrypt(token.encode()).decode()


def decrypt_token(encrypted_token: str) -> str:
    cipher = _get_cipher()
    return cipher.decrypt(encrypted_token.encode()).decode()
