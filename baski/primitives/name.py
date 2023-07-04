def fn_name(fn) -> str:
    """
    Returns callable dotted name including module
    :param fn:
    :return:
    """
    parts = [fn.__module__]
    if hasattr(fn, '__qualname__'):
        parts.append(fn.__qualname__)
    return '.'.join(parts)


def obj_name(obj) -> str:
    """
    Returns callable dotted name including module
    :param obj:
    :return:
    """
    parts = [obj.__module__]
    if hasattr(obj, '__class__'):
        cls = obj.__class__
        if hasattr(cls, '__name__'):
            parts.append(cls.__name__)
    return '.'.join(parts)
