from ....common.model.execution_type import ExecutionType
from .execution_task import CopyTaskExecution
from .execution_task import MockTaskExecution
from .execution_task import UploadTaskExecution


class TaskFactory(object):
    def create_task(self, task):
        if task.etype == ExecutionType.COPY:
            return CopyTaskExecution(task.tid, task.params)
        elif task.etype == ExecutionType.UPLOAD:
            return UploadTaskExecution(task.tid, task.params)
        elif task.etype == ExecutionType.MOCK:
            return MockTaskExecution(task.tid, task.params)
        else:
            raise UnknownTaskTypeException("Unknown task type %s" % task.etype)


class UnknownTaskTypeException(Exception):
    pass
