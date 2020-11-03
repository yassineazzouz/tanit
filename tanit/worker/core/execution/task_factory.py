from .execution_task import CopyTaskExecution
from .execution_task import MockTaskExecution


class TaskFactory(object):

    def create_task(self, task):
        if task.operation == "COPY":
            return CopyTaskExecution(task.tid, task.params)
        elif task.etype == "MOCK":
            return MockTaskExecution(task.tid, task.params)
        else:
            raise UnknownTaskTypeException("Unknown task type %s" % task.etype)


class UnknownTaskTypeException(Exception):
    pass
