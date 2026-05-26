import os
import secrets

__all__ = ["get_env", "is_cloud", "is_debug", "is_test", "port", "project_id", "token"]


class EnvValue:
    def __init__(self, name: str, value: str | None) -> None:
        if not (isinstance(value, str) and value):
            available = ", ".join(sorted(k for k in os.environ if k.isupper() and not k.startswith("_")))
            raise ValueError(f"Environment variable {name} is not set. Available: {available}")
        self._value = value
        self._name = name

    def __str__(self) -> str:
        return str(self._value).strip()

    def __bool__(self) -> bool:
        if self._value.lower() not in {"1", "true", "yes", "0", "false", "no"}:
            raise ValueError(f"Environment variable {self._name} can't be cast to boolean")
        return self._value.lower() in {"1", "true", "yes"}

    def __int__(self) -> int:
        return int(self._value)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return self._value == other

        if isinstance(other, bool):
            return bool(self) == other

        if isinstance(other, (int, float)):
            return self._value == str(other)

        raise ValueError(f"Equal operator for type {type(other)} is not supported")

    def __hash__(self) -> int:
        return hash(self._value)


def get_env(name: str, default: str | int | bool | None = None) -> EnvValue:
    return EnvValue(name, os.environ.get(name, str(default) if default is not None else None))


def token() -> EnvValue:
    return get_env("TOKEN", secrets.token_urlsafe())


def port() -> EnvValue:
    return get_env("PORT", 8000)


def is_cloud() -> EnvValue:
    return get_env("CLOUD", False)


def is_debug() -> EnvValue:
    return get_env("DEBUG", False)


def is_test() -> EnvValue:
    return get_env("TEST", False)


def project_id() -> EnvValue:
    return get_env("GOOGLE_CLOUD_PROJECT")
