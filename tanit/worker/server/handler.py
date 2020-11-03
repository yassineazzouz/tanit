import logging as lg

from ...common.model.task import Task
from ...thrift.worker.service.ttypes import WorkerStatus

_logger = lg.getLogger(__name__)


class WorkerServiceHandler(object):
    def __init__(self, worker):
        self.worker = worker

    def submit(self, task):
        self.worker.submit(Task(task.tid, task.operation, task.params))

    def worker_status(self):
        status = self.worker.get_stats()
        return WorkerStatus(
            status.wid, status.running, status.pending, status.available
        )

    def register_filesystem(self, filesystem):
        self.worker.register_filesystem(filesystem.name, filesystem.parameters)
