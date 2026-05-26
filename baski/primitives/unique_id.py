"""UUID-based short id generation that avoids collisions with a known set."""

import uuid
from collections.abc import Iterable

__all__ = ["unique_id"]


def unique_id(existing: Iterable[str] | None = None) -> str | None:
    """Return a hex id (4-char prefix, falling back to full) not present in existing."""
    existing_set = set(existing or [])
    s = f"{int(uuid.uuid4()):032x}"
    for i in (4, len(s)):
        p = s[:i]
        if p not in existing_set:
            return p
    return None
