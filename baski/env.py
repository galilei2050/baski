import os
import secrets

__all__ = ['is_cloud', 'is_debug', 'is_test', 'token', 'project_id', 'get_env']


class EnvValue(object):

    def __init__(self, name, value):
        assert isinstance(value, str), f"Environment variable {name} is not set"
        self._value = value
        self._name = name

    def __str__(self):
        return str(self._value)

    def __bool__(self):
        if self._value.lower() not in {'1', 'true', 'yes', '0', 'false', 'no'}:
            raise ValueError(f"Environment variable {self._name} can't be cast to boolean")
        return self._value.lower() in {'1', 'true', 'yes'}

    def __int__(self):
        return int(self._value)

    def __eq__(self, other):
        if isinstance(other, str):
            return self._value == other

        if isinstance(other, bool):
            return bool(self) == other

        if isinstance(other, (int, float)):
            return self._value == str(other)

        raise ValueError(f"Equal operator for type {type(other)} is not supported")


def get_env(name, default=None):
    return EnvValue(name, os.environ.get(name, str(default) if default is not None else None))


def token():
    return get_env("TOKEN", secrets.token_urlsafe())


def port():
    return get_env("PORT", 8080)


def is_cloud():
    return get_env("CLOUD", False)


def is_debug():
    return get_env("DEBUG", False)


def is_test():
    return get_env("TEST", False)


def project_id():
    return get_env("GOOGLE_CLOUD_PROJECT")
