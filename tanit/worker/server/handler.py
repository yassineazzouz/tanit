import logging as lg
import json
import ast

from ...common.model.task import Task
from ...filesystem.model import FileSystem
from ...thrift.common.model.ttypes import WorkerExecutionStatus as TWorkerExecutionStatus

_logger = lg.getLogger(__name__)


class WorkerServiceHandler(object):
    def __init__(self, worker):
        self.worker = worker

    def submit(self, task):
        self.worker.submit(Task(task.tid, task.operation, task.params))

    def worker_status(self):
        status = self.worker.get_stats()
        return TWorkerExecutionStatus(
            status.wid, status.running, status.pending, status.available
        )

    def register_filesystem(self, filesystem):
        name = filesystem.name
        tpe = filesystem.type
        try:
            parameters = json.loads(filesystem.parameters)
            if type(parameters) is str:
                parameters = ast.literal_eval(parameters)
        except Exception as e:
            _logger.error("Error parsing filesystem json parameters specification.")
            raise e
        self.worker.register_filesystem(FileSystem(name, tpe, parameters))
