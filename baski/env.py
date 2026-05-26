"""Typed environment variable access with boolean/int coercion."""

import os
import secrets

__all__ = ["get_env", "is_cloud", "is_debug", "is_test", "port", "project_id", "token"]


class EnvValue:
    """Wrapper around an env var value that coerces to str/int/bool on demand."""

    def __init__(self, name: str, value: str | None) -> None:
        """Store the value or raise if missing."""
        if not (isinstance(value, str) and value):
            available = ", ".join(sorted(k for k in os.environ if k.isupper() and not k.startswith("_")))
            raise ValueError(f"Environment variable {name} is not set. Available: {available}")
        self._value = value
        self._name = name

    def __str__(self) -> str:
        """Return the raw value stripped of whitespace."""
        return str(self._value).strip()

    def __bool__(self) -> bool:
        """Coerce the value to bool, raising on non-boolean strings."""
        if self._value.lower() not in {"1", "true", "yes", "0", "false", "no"}:
            raise ValueError(f"Environment variable {self._name} can't be cast to boolean")
        return self._value.lower() in {"1", "true", "yes"}

    def __int__(self) -> int:
        """Coerce the value to int."""
        return int(self._value)

    def __eq__(self, other: object) -> bool:
        """Compare against bool/str/numeric without forcing a cast."""
        if isinstance(other, bool):
            return bool(self) == other
        if isinstance(other, str):
            return self._value == other
        if isinstance(other, (int, float)):
            return self._value == str(other)
        return NotImplemented

    def __hash__(self) -> int:
        """Hash by raw value."""
        return hash(self._value)


def get_env(name: str, default: str | int | bool | None = None) -> EnvValue:  # noqa: FBT001 — default accepts any scalar including bool by design
    """Read an env var, falling back to default if unset."""
    return EnvValue(name, os.environ.get(name, str(default) if default is not None else None))


def token() -> EnvValue:
    """Return the TOKEN env var, generating a random one if unset."""
    return get_env("TOKEN", secrets.token_urlsafe())


def port() -> EnvValue:
    """Return the PORT env var, defaulting to 8000."""
    return get_env("PORT", 8000)


def is_cloud() -> EnvValue:
    """Return the CLOUD env var, defaulting to False."""
    return get_env("CLOUD", False)  # noqa: FBT003 — boolean default for env var with bool semantics


def is_debug() -> EnvValue:
    """Return the DEBUG env var, defaulting to False."""
    return get_env("DEBUG", False)  # noqa: FBT003 — boolean default for env var with bool semantics


def is_test() -> EnvValue:
    """Return the TEST env var, defaulting to False."""
    return get_env("TEST", False)  # noqa: FBT003 — boolean default for env var with bool semantics


def project_id() -> EnvValue:
    """Return the GOOGLE_CLOUD_PROJECT env var."""
    return get_env("GOOGLE_CLOUD_PROJECT")
