import typing
from string import ascii_letters, digits

__all__ = ['escape_text', 'format_num', 'normalize']

_ALLOWED_CHARS = set(ascii_letters + digits + " ")


def normalize(text: typing.AnyStr) -> str:
    if not text:
        return ""
    return ("".join([ch for ch in str(text).strip().lower() if ch in _ALLOWED_CHARS])).replace("  ", " ")


def escape_text(text):
    text = str(text)
    # https://core.telegram.org/bots/api#markdownv2-style
    for symbol in ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']:
        text = text.replace(symbol, f'\\{symbol}')
    return text


def format_num(number):
    if not isinstance(number, (int, float)):
        return 'null'
    return escape_text(f'{number:_.2f}'.replace('_', ' '))
