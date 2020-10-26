#!/usr/bin/env python

import logging as lg
import multiprocessing
import socket

from ...common.config.config import Config
from ...common.config.config import TanitConfigurationException

_logger = lg.getLogger(__name__)


class WorkerConfig(Config):
    default_bind_port = 9093
    default_bind_address = "0.0.0.0"
    default_thrift_threads = 25
    default_executor_threads = multiprocessing.cpu_count()

    def __init__(self, path=None):
        super(WorkerConfig, self).__init__(path)

    def load(self):
        if self.config.has_option("master", "worker-service_address"):
            master = self.config.get("master", "worker-service_address")
        else:
            raise TanitConfigurationException(
                "Missing master rpc worker service address from configuration"
            )

        if len(master.split(":")) != 2:
            raise TanitConfigurationException(
                "Master rpc worker service address should be formatted as host:port."
            )

        self.master_host = master.split(":")[0]
        self.master_port = int(master.split(":")[1])

        if self.config.has_option("worker", "bind_address"):
            self.bind_address = self.config.get("worker", "bind_address")
        else:
            self.bind_address = WorkerConfig.default_bind_address

        if self.config.has_option("worker", "bind_port"):
            self.bind_port = self.config.getint("worker", "bind_port")
        else:
            self.bind_port = WorkerConfig.default_bind_port

        if self.config.has_option("worker", "thrift_threads"):
            self.thrift_threads = self.config.getint("worker", "thrift_threads")
        else:
            self.thrift_threads = WorkerConfig.default_thrift_threads

        if self.config.has_option("worker", "executor_threads"):
            self.executor_threads = self.config.getint("worker", "executor_threads")
        else:
            self.executor_threads = WorkerConfig.default_executor_threads

        if self.config.has_option("worker", "service_address"):
            worker = self.config.get("worker", "worker_service_address")
            if len(worker.split(":")) != 2:
                raise TanitConfigurationException(
                    "Worker rpc service address should be formatted as host:port."
                )

            self.worker_host = master.split(":")[0]
            self.worker_port = int(master.split(":")[1])
        else:
            self.worker_host = socket.gethostname()
            self.worker_port = self.bind_port
