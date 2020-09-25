#!/usr/bin/env python

from ...master.client.client import ClientFactory
from ..core.executor_pool import ExecutorPool
from ..core.executor_factory import ExecutorFactory
from ...common.model.worker import WorkerStatus
from ..core.execution.task_factory import TaskFactory
from threading import Thread
import time

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
        client_factory = ClientFactory(config.master_host, config.master_port)
        self.master = client_factory.create_client('worker-service')
        
        self.executor = ExecutorPool( self.wid,
                                      ExecutorFactory(client_factory ,self.lqueue, config.executor_threads),
                                      self.lqueue,
                                      config.executor_threads)
        
        self.task_factory = TaskFactory()
        
        self.reporter = WorkerHearbeatReporter(self)
        self.reporter.setDaemon(True)

    def submit(self, task):
        task_exec = self.task_factory.create_task(task)
        if (not self.stopped):
            self.lqueue.put(task_exec)
        else:
            raise WorkerStoppedException("Can not submit task [ %s ] to [ %s ] : worker stopped.", task_exec.tid, self.wid)
    
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
            raise e
        
        self.executor.start()
        
        #register the worker
        try:
            self.master.register_worker(self.wid, self.address, self.port)
        except:
            _logger.error("Could not register worker to the master server. exiting")
            self.stop()
            raise
            
        self.reporter.start()
        
    
    def stop(self):
        _logger.info("Stopping Kraken worker [ %s ].", self.wid)
        self.stopped = True
        self.executor.stop()
        #unregister the worker
        try:
            self.master.unregister_worker(self.wid, self.address, self.port)
        except:
            _logger.error("Could not unregister worker from master, exiting.")
        
        self.reporter.stop()
        self.reporter.join()
        
        self.master.stop()
        _logger.info("Kraken worker [ %s ] stopped.", self.wid)
    

class WorkerHearbeatReporter(Thread):
    
    # in seconds
    heartbeat_interval = 3
    
    def __init__(self, worker):
        super(WorkerHearbeatReporter, self).__init__()
        self.worker = worker
        self.client = worker.master
        self.stopped = False

    def stop(self):
        _logger.info("Stopping kraken worker hearbeat reporter.")
        self.stopped = True

    def run(self):
        _logger.info("Started kraken worker hearbeat reporter.")
        while (not self.stopped):
            try:
                self.client.register_heartbeat(self.worker.wid, self.worker.address, self.worker.port)
            except:
                _logger.exception("Could not send heartbeat to master, is the server running !")
            time.sleep(self.heartbeat_interval)
        _logger.info("Kraken worker hearbeat reporter stopped.")

class WorkerStoppedException(Exception):
    """Raised when trying to submit a task to a stopped worker"""
    pass