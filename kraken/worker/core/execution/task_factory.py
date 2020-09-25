#!/usr/bin/env python
# encoding: utf-8

from .execution_task import CopyTaskExecution, MockTaskExecution, UploadTaskExecution
from ....common.model.execution_type import ExecutionType

class TaskFactory(object):
        
    def create_task(self, task):
        if (task.etype == ExecutionType.COPY):
            return CopyTaskExecution(task.tid, task.params)
        elif (task.etype == ExecutionType.UPLOAD):
            return UploadTaskExecution(task.tid, task.params)
        elif (task.etype == ExecutionType.MOCK):
            return MockTaskExecution(task.tid, task.params)
        else:
            raise UnknownTaskTypeException("Unknown task type %s" % job.type)

class UnknownTaskTypeException(Exception):
    pass