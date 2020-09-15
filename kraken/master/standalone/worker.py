#!/usr/bin/env python

import time
import multiprocessing

from ...worker.core.executor_pool import ExecutorPool
from ...worker.core.executor import Executor
from ...common.model.worker import WorkerStatus
from ..core.worker.worker import WorkerIFace

import logging as lg
from Queue import Queue, Empty

_logger = lg.getLogger(__name__)

class LocalWorker(WorkerIFace):

    def __init__(self, master):
        self.wid = "kraken-local-worker"
        self.lqueue = Queue()
        self.stopped = False
        self.executor = LocalExecutorPool(self.wid, master ,self.lqueue, multiprocessing.cpu_count())

    def submit(self, task):
        if (not self.stopped):
            self.lqueue.put(task)
        else:
            raise WorkerStoppedException("Can not submit task [ %s ] to [ %s ] : worker stopped.", task.tid, self.wid)
    
    
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
        while True:
            self.idle = True
            try:
                task = self.cqueue.get(timeout=0.5)
                self.idle = False
                self.master.task_start(str(task.tid))
                self.current_task = task
                self._run(task)
                self.master.task_success(str(task.tid))
                self.cqueue.task_done()
            except Empty:
                if (self.stopped):
                    break
                time.sleep(0.5)
            except Exception as e:
                _logger.error("Error executing task [%s]", str(task.tid))
                _logger.error(e, exc_info=True)
                self.master.task_failure(str(task.tid))
                self.cqueue.task_done()     
        