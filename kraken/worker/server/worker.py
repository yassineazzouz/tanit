#!/usr/bin/env python

from ...master.client.client_factory import ClientFactory
from ..core.executor_pool import ExecutorPool
from ...common.model.worker import WorkerStatus

import logging as lg
from Queue import Queue

_logger = lg.getLogger(__name__)

class Worker(object):

    def __init__(self):
        self.lqueue = Queue()
        self.stopped = False
        
    def configure(self, config):
        self.address = config.worker_host
        self.port = config.worker_port
        
        self.wid = "kraken-worker-%s-%s" % (self.address, self.port)
        factory = ClientFactory(config.master_host, config.master_port)
        self.master = factory.create_client('master-worker')
        
        self.executor = ExecutorPool(self.wid, factory ,self.lqueue, config.executor_threads)

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
        try:
            self.master.start()
        except Exception as e:
            _logger.error("Could not connect to master on [%s:%s]", self.address, self.port)
            _logger.error(e, exc_info=True)
            raise e
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