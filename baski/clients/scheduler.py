"""Cloud Tasks-backed scheduler for enqueuing HTTP tasks (immediate or delayed)."""

__all__ = ["CloudTasksConfig", "CloudTasksScheduler", "Scheduler"]

from typing import Protocol

from google.api_core.exceptions import AlreadyExists
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
from pydantic import BaseModel

from ..primitives import datetime


class Scheduler(Protocol):
    """Protocol for task schedulers that enqueue HTTP tasks."""

    async def enqueue(  # noqa: PLR0913 — an HTTP task legitimately needs endpoint, name, body, headers, schedule
        self,
        *,
        endpoint: str,
        task_name: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
        schedule_time: datetime.datetime | None = None,
    ) -> bool:
        """Enqueue an HTTP POST task with `payload` as the raw body; immediate when schedule_time is None.

        `headers` default to `Content-Type: application/json` when None. Returns True if
        the task was created, False if one with this name already exists (dedup). Raises
        on any other error (fail-loud per project convention).
        """
        ...


class CloudTasksConfig(BaseModel):
    """Connection settings for CloudTasksScheduler."""

    model_config = {"arbitrary_types_allowed": True}

    client: tasks_v2.CloudTasksAsyncClient
    project_id: str
    location: str
    queue: str
    invoker_sa_email: str


class CloudTasksScheduler:
    """Cloud Tasks-backed scheduler. One long-lived client per server instance."""

    def __init__(self, config: CloudTasksConfig) -> None:
        """Unpack Cloud Tasks connection settings from the config bundle."""
        self._client = config.client
        self._project_id = config.project_id
        self._location = config.location
        self._queue = config.queue
        self._invoker_sa_email = config.invoker_sa_email

    async def enqueue(  # noqa: PLR0913 — an HTTP task legitimately needs endpoint, name, body, headers, schedule
        self,
        *,
        endpoint: str,
        task_name: str,
        payload: bytes,
        dispatch_deadline: datetime.timedelta,
        headers: dict[str, str] | None = None,
        schedule_time: datetime.datetime | None = None,
    ) -> bool:
        """Create an HTTP task POSTing payload to endpoint; False if task_name already exists.

        `dispatch_deadline` bounds how long Cloud Tasks waits for the worker (HTTP range 15s-30min).
        """
        parent = self._client.queue_path(self._project_id, self._location, self._queue)
        task = tasks_v2.Task(
            name=f"{parent}/tasks/{task_name}",
            http_request=tasks_v2.HttpRequest(
                http_method=tasks_v2.HttpMethod.POST,
                url=endpoint,
                headers=headers if headers is not None else {"Content-Type": "application/json"},
                body=payload,
                oidc_token=tasks_v2.OidcToken(service_account_email=self._invoker_sa_email),
            ),
            dispatch_deadline=dispatch_deadline,
        )
        if schedule_time is not None:
            ts = timestamp_pb2.Timestamp()
            ts.FromDatetime(schedule_time)
            task.schedule_time = ts

        try:
            await self._client.create_task(parent=parent, task=task)
        except AlreadyExists:
            return False
        else:
            return True
