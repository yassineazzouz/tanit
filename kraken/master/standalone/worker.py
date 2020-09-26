#!/usr/bin/env python

import multiprocessing

from ..core.worker.worker import WorkerIFace
from ...worker.core.executor_pool import ExecutorPool
from ...worker.core.executor import Executor
from ...worker.core.execution.task_factory import TaskFactory
from ...common.model.worker import WorkerStatus
from ...common.model.task import Task

import logging as lg
from Queue import Queue

_logger = lg.getLogger(__name__)

class LocalWorker(WorkerIFace):

    def __init__(self, wid, master):
        super(LocalWorker, self).__init__(wid, None, None)
        self.lqueue = Queue()
        self.stopped = False
        self.task_factory = TaskFactory()
        self.executor = LocalExecutorPool(self.wid, master ,self.lqueue, multiprocessing.cpu_count())

    def submit(self, task):
        task_exec = self.task_factory.create_task(Task(task.tid, task.etype, task.params))
        if (not self.stopped):
            self.lqueue.put(task_exec)
        else:
            raise WorkerStoppedException("Can not submit task [ %s ] to [ %s ] : worker stopped.", task_exec.tid, self.wid)

    def status(self):
        return WorkerStatus(
            self.wid,
            self.executor.num_running(),
            self.executor.num_pending(),
            self.executor.num_available())
    
    def start(self):
        _logger.info("Starting kraken worker [%s].", self.wid)
        self.stopped = False
        self.executor.start()
        _logger.info("Kraken worker [%s] started.", self.wid)
    
    def stop(self):
        _logger.info("Stopping Kraken worker [ %s ].", self.wid)
        self.stopped = True
        self.executor.stop()
        _logger.info("Kraken worker [ %s ] stopped.", self.wid)
        
class WorkerStoppedException(Exception):
    """Raised when trying to submit a task to a stopped worker"""
    pass

class LocalExecutorPool(ExecutorPool):
    
    def __init__(self, wid, master, cqueue, concurrency):
        self.cqueue = cqueue
        self.concurrency = concurrency
        self.wid = wid
        self.executors = []
        self.stopped = True
        
        for i in range(0, concurrency):
            self.executors.append(LocalExecutor("%s-executor-%s" % (wid,i), cqueue, master))

class LocalExecutor(Executor):
    
    def __init__(self, eid, cqueue, master):
        super(Executor, self).__init__()
        self.cqueue = cqueue
        self.eid = eid
        self.stopped = False
        self.idle = True    
        self.current_task = None
        
        self.master = master

    def run(self):
        self._run()
        