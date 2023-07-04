import uuid

__all__ = ['unique_id']


def unique_id(existing=None):
    existing = set(existing or [])
    s = '%032x' % int(uuid.uuid4())
    for i in (4, len(s)):
        p = s[:i]
        if p not in existing:
            return p
