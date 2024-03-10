import abc
import argparse
import asyncio
import logging as local_logging
import logging.config
import os
import signal
import traceback
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property
from sys import _current_frames

from google.cloud import firestore
from google.cloud import logging as cloud_logging

from ..config import AppConfig
from ..env import is_debug, is_test, is_cloud, port, get_env

__all__ = ['AsyncServer']


def handler(signum, frame):
    print("====================================================\n")
    print("*** STACKTRACE - START ***")
    code = []
    for threadId, stack in _current_frames().items():
        code.append("\n# ThreadID: %s" % threadId)
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append(f'File: "{filename}:{lineno}", in {name}')
            if line:
                code.append("  %s" % (line.strip()))

    for line in code:
        print(line)
    print("\n*** STACKTRACE - END ***")
    print("====================================================\n")
    raise KeyboardInterrupt()


signal.signal(signal.SIGINT, handler)


class AsyncServer(metaclass=abc.ABCMeta):

    def __init__(self):
        logging.info('Init %s', self.name)
        self.logging_client = None

    def add_arguments(self, parser: argparse.ArgumentParser):
        '''
        Добавить аргументы для парсера cmd аргументов. Результат в методе handle можно прочитать из self.args
        :param parser:
        :return:
        '''
        pass

    def init(self, db=None):
        if self.config['cloud']:
            local_logging.root.handlers.clear()
            self._setup_cloud_logging(self.config['debug'])
        else:
            local_logging.root.handlers.clear()
            self._setup_local_logging(self.config['debug'])

    @cached_property
    def loop(self):
        loop = asyncio.get_event_loop()
        loop.set_default_executor(self.loop_executor)
        loop.add_signal_handler(signal.SIGTERM, self.stop)
        return loop

    @cached_property
    def loop_executor(self):
        return ThreadPoolExecutor(max_workers=self.config.concurrency or os.cpu_count())

    @cached_property
    def db(self):
        return firestore.AsyncClient()

    @cached_property
    def args(self):
        parser = argparse.ArgumentParser(prog=self.name)
        parser.add_argument('-d', '--debug', help='Run in debug mode', default=bool(is_debug()), action='store_true')
        parser.add_argument('-c', '--config', help="Path to config file", default='config.yml')
        parser.add_argument('-p', '--port', help="Port to listen", default=int(port()))
        parser.add_argument('--cloud', help="Run in cloud mode", default=bool(is_cloud()), action='store_true')
        parser.add_argument('--dry-run', help='Run in dry-run mode', default=bool(is_test()), action='store_true')
        parser.add_argument('--project-id', help='Google Cloud project ID', default=str(get_env('GOOGLE_CLOUD_PROJECT', '')))
        self.add_arguments(parser)
        return dict(vars(parser.parse_args()))

    @cached_property
    def config(self):
        cfg = AppConfig()
        cfg.load_yml(self.args['config'])
        cfg.load_db(firestore.Client())
        for a in ['debug', 'cloud']:
            cfg[a] = self.args[a]
        logging.info('Config file %s loaded', self.args['config'])
        return cfg

    def update_config(self) -> None:
        self.config.load_yml(self.args['config'])
        self.config.load_db(firestore.Client())

    def get_all_config_values(self) -> list:
        return [cfg for cfg in self.config.values()]

    async def check_update_config(self):
        current_config = self.get_all_config_values()
        while True:
            self.update_config()
            new_config = self.get_all_config_values()
            if new_config != current_config:
                logging.info('Config file update detected. Stop and close all tasks!')
                self.check_tasks_and_stop()
                break
            await asyncio.sleep(60)

    @property
    def name(self):
        return self.__class__.__name__

    def __call__(self, *args, **kwargs):
        return self.run()

    def stop(self):
        logging.warning("Got SIGTERM signal. Graceful shutdown start")
        loop = self.loop
        loop.call_later(1, self.check_tasks_and_stop)

    def should_wait_task(self, t: asyncio.Task):
        return False

    def check_tasks_and_stop(self):
        loop = self.loop
        running_tasks = [t.get_coro() for t in asyncio.all_tasks(loop) if self.should_wait_task(t)]
        if running_tasks:
            logging.warning(f"Wait for tasks complete: {running_tasks}")
            loop.call_later(60, self.check_tasks_and_stop)
            return
        self.loop.stop()

    def run(self) -> int:
        try:
            self.init()
            if self.args['cloud']:
                logging.info(f'Start {self.name}')
            else:
                logging.info(f'Start {self.name}\n {self.config}')
            if self.args['dry_run']:
                logging.info('Dry run of %s complete', self.name)
                return 0

            self.loop.create_task(self.check_update_config())
            self.execute()

        except KeyboardInterrupt:
            logging.info('Interrupted %s', self.name)
        except Exception as err:
            logging.error(err)
            raise
        return 1

    def _setup_cloud_logging(self, debug=False):
        self.logging_client = cloud_logging.Client()
        self.logging_client.get_default_handler()
        self.logging_client.setup_logging(log_level=logging.DEBUG if debug else logging.INFO)

    def _setup_local_logging(self, debug=False):
        ch = local_logging.StreamHandler()
        ch.setLevel(logging.DEBUG if debug else logging.INFO)
        ch.setFormatter(logging.Formatter(style='{', fmt='{levelname:5}{lineno:4}:{filename:30}{message}'))

        local_logging.root.addHandler(ch)
        local_logging.root.setLevel(logging.DEBUG if debug else logging.INFO)

    def execute(self):
        with self.loop_executor:
            try:
                return self.loop.run_forever()
            finally:
                self.loop.close()

