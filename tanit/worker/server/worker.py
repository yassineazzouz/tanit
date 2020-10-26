import logging as lg
import time
from threading import Thread

from six.moves.queue import Queue

from ...common.model.worker import WorkerStatus
from ...filesystem.filesystem_factory import FileSystemFactory
from ...master.client.client import ClientType
from ...master.client.client import ThriftClientFactory
from ..core.execution.task_factory import TaskFactory
from ..core.executor_factory import ExecutorFactory
from ..core.executor_pool import ExecutorPool
from ..filesystem.service import LocalFileSystemService

_logger = lg.getLogger(__name__)


class Worker(object):
    def __init__(self):
        self.lqueue = Queue()
        self.stopped = False

    def configure(self, config):
        self.address = config.worker_host
        self.port = config.worker_port

        self.wid = "tanit-worker-%s-%s" % (self.address, self.port)

        client_factory = ThriftClientFactory(config.master_host, config.master_port)
        self.master = client_factory.create_client(ClientType.WORKER_SERVICE)

        self.executor = ExecutorPool(
            self.wid,
            ExecutorFactory(client_factory, self.lqueue, config.executor_threads),
            self.lqueue,
            config.executor_threads,
        )

        self.task_factory = TaskFactory()

        self.reporter = WorkerHearbeatReporter(self)
        self.reporter.setDaemon(True)

        self.filesystem = LocalFileSystemService()

    def submit(self, task):
        task_exec = self.task_factory.create_task(task)
        if not self.stopped:
            self.lqueue.put(task_exec)
        else:
            raise WorkerStoppedException(
                "Can not submit task [ %s ] to [ %s ] : worker stopped.",
                task_exec.tid,
                self.wid,
            )

    def get_stats(self):
        return WorkerStatus(
            self.wid,
            self.executor.num_running(),
            self.executor.num_pending(),
            self.executor.num_available(),
        )

    def register_filesystem(self, name, filesystem):
        if self.stopped:
            raise WorkerStoppedException(
                "Can not register filesystem [ %s ] : worker server stopped.", name
            )

        _logger.info("Registering new filesystem [ %s ].", name)
        filesystem["name"] = name
        # register the worker as a filesystem
        FileSystemFactory.getInstance().register_filesystem(filesystem)
        _logger.info("Filesystem [ %s ] registered.", name)

    def start(self):
        _logger.info("Starting tanit worker [%s].", self.wid)
        self.stopped = False
        try:
            self.master.start()
        except Exception as e:
            _logger.error(
                "Could not connect to master on [%s:%s]", self.address, self.port
            )
            raise e

        self.executor.start()

        # register the worker
        self.master.register_worker(self.wid, self.address, self.port)

        # start the filesystem service
        self.filesystem.start()
        # register the file system
        if self.filesystem:
            # register the file system with the master
            self.master.register_filesystem(
                "local:%s" % self.address,
                {
                    "type": "local",
                    "address": str(self.address),
                    "port": str(self.filesystem.bind_port),
                },
            )

        self.reporter.start()

    def stop(self):
        _logger.info("Stopping Tanit worker [ %s ].", self.wid)
        self.stopped = True
        self.executor.stop()
        # unregister the worker
        try:
            self.master.unregister_worker(self.wid, self.address, self.port)
        except Exception:
            _logger.error("Could not unregister worker from master, exiting.")

        self.reporter.stop()
        self.reporter.join()

        self.master.stop()
        _logger.info("Tanit worker [ %s ] stopped.", self.wid)


class WorkerHearbeatReporter(Thread):
    # in seconds
    heartbeat_interval = 3

    def __init__(self, worker):
        super(WorkerHearbeatReporter, self).__init__()
        self.worker = worker
        self.client = worker.master
        self.stopped = False

    def stop(self):
        _logger.info("Stopping tanit worker hearbeat reporter.")
        self.stopped = True

    def run(self):
        _logger.info("Started tanit worker hearbeat reporter.")
        while not self.stopped:
            try:
                self.client.register_heartbeat(
                    self.worker.wid, self.worker.address, self.worker.port
                )
            except Exception:
                _logger.exception(
                    "Could not send heartbeat to master, is the server running !"
                )
            time.sleep(self.heartbeat_interval)
        _logger.info("Tanit worker hearbeat reporter stopped.")


class WorkerStoppedException(Exception):
    """Raised when trying to submit a task to a stopped worker."""

    pass
