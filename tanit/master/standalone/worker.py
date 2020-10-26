import logging as lg
import multiprocessing

from six.moves.queue import Queue

from ...common.model.task import Task
from ...common.model.worker import WorkerStatus
from ...worker.core.execution.task_factory import TaskFactory
from ...worker.core.executor_factory import ExecutorFactory
from ...worker.core.executor_pool import ExecutorPool
from ..client.client import LocalClientFactory
from ..core.worker.worker import WorkerIFace

_logger = lg.getLogger(__name__)


class LocalWorker(WorkerIFace):
    def __init__(self, wid, master):
        super(LocalWorker, self).__init__(wid, None, None)
        self.lqueue = Queue()
        self.stopped = False
        self.task_factory = TaskFactory()

        client_factory = LocalClientFactory(master)

        self.executor = ExecutorPool(
            self.wid,
            ExecutorFactory(client_factory, self.lqueue, multiprocessing.cpu_count()),
            self.lqueue,
            multiprocessing.cpu_count(),
        )

    def submit(self, task):
        task_exec = self.task_factory.create_task(
            Task(task.tid, task.etype, task.params)
        )
        if not self.stopped:
            self.lqueue.put(task_exec)
        else:
            raise WorkerStoppedException(
                "Can not submit task [ %s ] to [ %s ] : worker stopped.",
                task_exec.tid,
                self.wid,
            )

    def register_filesystem(self, name, filesystem):
        # The file system is defined in the master already
        pass

    def status(self):
        return WorkerStatus(
            self.wid,
            self.executor.num_running(),
            self.executor.num_pending(),
            self.executor.num_available(),
        )

    def start(self):
        _logger.info("Starting tanit worker [%s].", self.wid)
        self.stopped = False
        self.executor.start()
        _logger.info("Tanit worker [%s] started.", self.wid)

    def stop(self):
        _logger.info("Stopping Tanit worker [ %s ].", self.wid)
        self.stopped = True
        self.executor.stop()
        _logger.info("Tanit worker [ %s ] stopped.", self.wid)


class WorkerStoppedException(Exception):
    """Raised when trying to submit a task to a stopped worker."""

    pass
