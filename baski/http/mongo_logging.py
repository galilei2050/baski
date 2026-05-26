import time

from pymongo.monitoring import CommandFailedEvent, CommandListener, CommandStartedEvent, CommandSucceededEvent

from ..server.logger import Logger

__all__ = ["MongoQueryLogger"]


# durationMs above this threshold flips the entry to severity=WARNING and sets
# slow=True. A typical IXSCAN find is single-digit ms; over 200ms is worth
# eyeballing. Adjust if the noise floor shifts.
_SLOW_QUERY_MS = 200.0


def _extract_collection(*, command: dict, command_name: str) -> str:
    # For find/aggregate/insert/update/delete, command[command_name] is the
    # collection name (string). For admin commands like ping/buildInfo, it's
    # the int 1 — fall back to command_name itself so the label is meaningful.
    target = command.get(command_name)
    if isinstance(target, str):
        return target
    # getMore carries collection on a separate field; fall back to that.
    collection = command.get("collection")
    if isinstance(collection, str):
        return collection
    return command_name


def _extract_filter(*, command: dict, command_name: str) -> dict:
    if command_name == "aggregate":
        for stage in command.get("pipeline") or []:
            if "$match" in stage:
                return stage["$match"]
        return {}
    return command.get("filter") or command.get("q") or {}


def _extract_sort(*, command: dict, command_name: str) -> dict:
    if command_name == "aggregate":
        for stage in command.get("pipeline") or []:
            if "$sort" in stage:
                return stage["$sort"]
        return {}
    return command.get("sort") or {}


def _extract_stages(*, command: dict) -> list[str]:
    # Compact one-line summary of an aggregation pipeline, e.g.
    # ["$match", "$lookup:customers", "$lookup:services", "$unwind:customer", "$sort"].
    # Looking at this list is usually enough to spot the unindexed-lookup
    # smoking gun when an aggregate is slow.
    out: list[str] = []
    for stage in command.get("pipeline") or []:
        if not isinstance(stage, dict) or not stage:
            continue
        op = next(iter(stage))
        spec = stage[op]
        if op == "$lookup" and isinstance(spec, dict):
            target = spec.get("from")
            out.append(f"$lookup:{target}" if isinstance(target, str) else op)
        elif op == "$unwind":
            path = spec.get("path") if isinstance(spec, dict) else spec
            if isinstance(path, str) and path.startswith("$"):
                out.append(f"$unwind:{path[1:]}")
            else:
                out.append(op)
        else:
            out.append(op)
    return out


def _extract_projection_keys(*, command: dict, command_name: str) -> list[str]:
    if command_name == "aggregate":
        for stage in command.get("pipeline") or []:
            if "$project" in stage:
                return sorted((stage["$project"] or {}).keys())
        return []
    return sorted((command.get("projection") or {}).keys())


def _extract_n_returned(*, reply: dict) -> int:
    cursor = reply.get("cursor")
    if cursor is not None:
        return len(cursor.get("firstBatch") or cursor.get("nextBatch") or [])
    return int(reply.get("n") or 0)


def _build_labels(*, command: dict, command_name: str, duration_ms: float) -> dict:
    labels: dict = {
        "collection": _extract_collection(command=command, command_name=command_name),
        "operation": command_name,
        "filter": _extract_filter(command=command, command_name=command_name),
        "sort": _extract_sort(command=command, command_name=command_name),
        "durationMs": duration_ms,
        "slow": duration_ms >= _SLOW_QUERY_MS,
    }
    if command_name == "aggregate":
        labels["stages"] = _extract_stages(command=command)
    projection = _extract_projection_keys(command=command, command_name=command_name)
    if projection:
        labels["projection"] = projection
    if "limit" in command:
        labels["limit"] = command["limit"]
    if "skip" in command:
        labels["skip"] = command["skip"]
    if "hint" in command:
        labels["hint"] = command["hint"]
    return labels


class MongoQueryLogger(CommandListener):
    def __init__(self, *, logger: Logger) -> None:
        self._logger = logger
        # Maps request_id → (start_monotonic, command_dict, command_name)
        self._starts: dict[int, tuple[float, dict, str]] = {}

    def started(self, event: CommandStartedEvent) -> None:
        self._starts[event.request_id] = (time.monotonic(), dict(event.command), event.command_name)

    def succeeded(self, event: CommandSucceededEvent) -> None:
        start = self._starts.pop(event.request_id, None)
        if start is None:
            self._logger.warning("mongo.query missing started event", labels={"requestId": event.request_id})
            return
        _, command, command_name = start
        duration_ms = round(event.duration_micros / 1000, 3)
        labels = _build_labels(command=command, command_name=command_name, duration_ms=duration_ms)
        labels["nReturned"] = _extract_n_returned(reply=dict(event.reply))
        # Slow queries are surfaced at WARNING so a single severity filter
        # in Cloud Logging shows everything worth investigating.
        if labels["slow"]:
            self._logger.warning("mongo.query", labels=labels)
        else:
            self._logger.info("mongo.query", labels=labels)

    def failed(self, event: CommandFailedEvent) -> None:
        start = self._starts.pop(event.request_id, None)
        if start is None:
            self._logger.warning("mongo.query missing started event", labels={"requestId": event.request_id})
            return
        _, command, command_name = start
        duration_ms = round(event.duration_micros / 1000, 3)
        labels = _build_labels(command=command, command_name=command_name, duration_ms=duration_ms)
        labels["error"] = str(event.failure)
        self._logger.warning("mongo.query", labels=labels)
