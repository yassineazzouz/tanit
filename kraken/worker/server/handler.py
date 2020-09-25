#!/usr/bin/env python

import logging as lg
from ...common.model.task import Task
from ...common.model.execution_type import ExecutionType
from ..thrift.ttypes import WorkerStatus, TaskType

_logger = lg.getLogger(__name__)

class WorkerServiceHandler(object):

    def __init__(self, worker):
        self.worker = worker

    def submit(self, task):

        if task.type == TaskType.COPY:
            etype = ExecutionType.COPY
        elif task.type == TaskType.UPLOAD:
            etype = ExecutionType.UPLOAD
        elif task.type == TaskType.MOCK:
            etype = ExecutionType.MOCK
        else:
            # should raise exception here
            pass
        
        self.worker.submit(Task( task.tid, etype, task.params) )

    def worker_status(self):
        status = self.worker.get_stats()
        return WorkerStatus( 
                   status.wid,
                   status.running,
                   status.pending,
                   status.available)
