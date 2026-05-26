import argparse
import asyncio
import logging as local_logging
import logging.config
import signal
import traceback
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property
from sys import _current_frames
from typing import Any

from google.cloud import firestore
from google.cloud import logging as cloud_logging

from ..env import get_env, is_cloud, is_debug, is_test, port, project_id
from .config import AppConfig
from .logger import CloudLogger, LocalLogger, Logger

__all__ = ["AsyncServer"]


logger = local_logging.getLogger(__name__)


def handler(signum: int, frame: Any) -> None:  # noqa: ARG001 — `frame` is required by signal.signal handler signature
    print("====================================================\n")  # noqa: T201 — SIGINT stack-dump goes to stdout; logging may not be flushed
    print("*** STACKTRACE - START ***")  # noqa: T201 — see above
    code = []
    for thread_id, stack in _current_frames().items():
        code.append(f"\n# ThreadID: {thread_id}")
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append(f'File: "{filename}:{lineno}", in {name}')
            if line:
                code.append(f"  {line.strip()}")

    for line in code:
        print(line)  # noqa: T201 — see above
    print("\n*** STACKTRACE - END ***")  # noqa: T201 — see above
    print("====================================================\n")  # noqa: T201 — see above
    raise KeyboardInterrupt


class AsyncServer:
    def __init__(self) -> None:
        signal.signal(signal.SIGINT, handler)
        _ = self.logging_client
        logger.info("Init %s", self.name)

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        pass

    @cached_property
    def logging_client(self) -> cloud_logging.Client | None:
        if self.config["cloud"]:
            local_logging.root.handlers.clear()
            logging_client = cloud_logging.Client()
            logging_client.get_default_handler()
            logging_client.setup_logging(log_level=local_logging.DEBUG if self.config["debug"] else local_logging.INFO)
            local_logging.getLogger("httpx").setLevel(local_logging.WARNING)
            return logging_client

        local_logging.root.handlers.clear()
        ch = local_logging.StreamHandler()
        ch.setLevel(local_logging.DEBUG if self.config["debug"] else local_logging.INFO)
        ch.setFormatter(local_logging.Formatter(style="{", fmt="{asctime} {levelname:7} {message}", datefmt="%H:%M:%S"))

        local_logging.root.addHandler(ch)
        local_logging.root.setLevel(local_logging.DEBUG if self.config["debug"] else local_logging.INFO)
        local_logging.getLogger("httpx").setLevel(local_logging.WARNING)
        return None

    @cached_property
    def logger(self) -> Logger:
        # Process-scoped structured logger for background components that have
        # no Request (e.g. the Mongo CommandListener). Request-scoped logging
        # still goes through dependencies.get_logger(request).
        if self.logging_client is not None:
            return CloudLogger(logger_client=self.logging_client, project_id=self.config["project_id"])
        return LocalLogger()

    @cached_property
    def loop_executor(self) -> ThreadPoolExecutor:
        # Default to 64 threads for I/O-bound operations (GCS uploads, etc.)
        return ThreadPoolExecutor(max_workers=self.config.concurrency or 64)

    @cached_property
    def args(self) -> dict[str, Any]:
        parser = argparse.ArgumentParser(prog=self.name)
        parser.add_argument("-d", "--debug", help="Run in debug mode", default=bool(is_debug()), action="store_true")
        parser.add_argument("-c", "--config", help="Path to config file", default="config.yml")
        parser.add_argument("-p", "--port", help="Port to listen", default=int(port()), type=int)
        parser.add_argument("--cloud", help="Run in cloud mode", default=bool(is_cloud()), action="store_true")
        parser.add_argument("--dry-run", help="Run in dry-run mode", default=bool(is_test()), action="store_true")
        parser.add_argument("--project-id", help="Google Cloud project ID", default=str(str(project_id())))
        parser.add_argument(
            "--region", help="Google Cloud region", default=str(get_env("GOOGLE_CLOUD_REGION", "us-central1"))
        )
        self.add_arguments(parser)
        args, _ = parser.parse_known_args()
        return dict(vars(args))

    @cached_property
    def config(self) -> AppConfig:
        cfg = AppConfig()
        cfg.load_yml(self.args["config"])
        for a in ["debug", "cloud", "project_id", "region"]:
            cfg[a] = self.args[a]
        return cfg

    async def check_config_periodically(self, interval_seconds: float = 300) -> None:
        """Background task: poll Firestore for config updates, SIGTERM on change.

        Must be scheduled on the running loop (e.g. via asyncio.create_task in an async
        lifespan), so the running loop actually executes it.
        """
        client = firestore.Client()
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                if self.config.load_db(client):
                    logger.critical("Config update detected. Stop")
                    signal.raise_signal(signal.SIGTERM)
                    return
                logger.debug("Config is the same.")
            except Exception as error:  # noqa: BLE001 — periodic config refresh must never tear down the server; warn and keep running
                logger.warning("An error occurred when updating the config - %s", error)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def __call__(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ARG002 — accepts arbitrary forwards from CLI entry point; ignored by design
        return self.run()

    def run(self) -> int:
        try:
            if self.args["cloud"]:
                logger.warning("Start %s", self.name)
            else:
                logger.warning("Start %s\n %s", self.name, self.config)
            if self.args["dry_run"]:
                logger.info("Dry run of %s complete", self.name)
                return 0

            with self.loop_executor:
                return self.execute()
        except KeyboardInterrupt:
            logger.info("Interrupted %s", self.name)
        except Exception as err:
            logger.exception("Unhandled exception in %s", self.name, exc_info=err)
            raise
        return 1

    def execute(self) -> int:
        raise NotImplementedError("Subclass must implement execute()")
