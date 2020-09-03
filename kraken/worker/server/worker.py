#!/usr/bin/env python

from ...master.client.client import MasterWorkerClient
from ..core.executor_pool import ExecutorPool
from ...common.model.worker import WorkerStatus

import logging as lg
from Queue import Queue

_logger = lg.getLogger(__name__)

class Worker(object):

    def __init__(self, address, port, concurrency = 25):
        self.wid = "kraken-worker-%s-%s" % (address, port)
        self.address = address
        self.port = port
        self.lqueue = Queue()
        self.master = MasterWorkerClient("localhost", 9091)
        self.executor = ExecutorPool(self.wid, self.lqueue, concurrency)
        self.stopped = False
        
    
    def submit(self, task):
        if (not self.stopped):
            self.lqueue.put(task)
        else:
            raise WorkerStoppedException("Can not submit task [ %s ] to [ %s ] : worker stopped.", task.tid, self.wid)
    
    
    def get_stats(self):
        return WorkerStatus(
            self.wid,
            self.executor.num_running(),
            self.executor.num_pending(),
            self.executor.num_available())
    
    def start(self):
        _logger.info("Starting kraken worker [%s].", self.wid)
        self.stopped = False
        self.master.start()
        self.executor.start()
        
        #register the worker
        self.master.register_worker(self.wid, self.address, self.port)
        
    
    def stop(self):
        _logger.info("Stopping Kraken worker [ %s ].", self.wid)
        self.stopped = True
        self.executor.stop()
        self.master.stop()
        _logger.info("Kraken worker [ %s ] stopped.", self.wid)
        
class WorkerStoppedException(Exception):
    """Raised when trying to submit a task to a stopped worker"""
    pass