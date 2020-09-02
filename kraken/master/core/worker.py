#!/usr/bin/env python
# The execution engine controls the execution environement
# encoding: utf-8

from multiprocessing.pool import ThreadPool

from ...worker.client.client import WorkerClient

import logging as lg
_logger = lg.getLogger(__name__)

class RemoteThriftWorker(object):
    def __init__(self, wid, address, port):
        self.wid = wid
        self.client = WorkerClient(address, port)
        
        self.stopped = True
        
    def start(self):
        self.stopped = False
        self.client.start()
        
    def stop(self):
        self.stopped = True
        self.client.stop()

    def submit(self, task_exec):
        if (not self.stopped):
            self.client.submit(task_exec.task)
        else :
            raise WorkerStoppedException("Can not submit task [ %s ] to [ %s ] : worker stopped.", task_exec.task.tid, self.wid)
        
    def status(self):
        return self.client.worker_status()
         
class ThreadPoolWorker(object):

    def __init__(self, wid, engine, concurrency = 25):
        
        self.wid = wid
        
        self.pool = ThreadPool(concurrency)
        self.concurrency = 25
        self.engine = engine
        
        self.stopped = False
        self.running = []
        self.pending = []
        
    
    def submit(self, task):
        def _run(task):
            _logger.debug("Start Running task [ %s ] on worker [ %s ]", task.tid, self.wid)
            task.on_start()
            self.pending.remove(task)
            self.running.append(task)
            try:
                self.engine.run_task(task)
                _logger.debug("Finished Running task [ %s ] on worker [ %s ]", task.tid, self.wid)
                task.on_finish()
            except Exception:
                _logger.exception("Failed Running task [ %s ] on worker [ %s ]", task.tid, self.wid)
                task.on_fail()
            finally:
                self.running.remove(task)

        if (not self.stopped):
            self.pool.map_async(_run, (task,))
            self.pending.append(task)
        else:
            raise WorkerStoppedException("Can not submit task [ %s ] to [ %s ] : worker stopped.", task.tid, self.wid)
    
    
    def num_running(self):
        return len(self.running)
    
    def num_pending(self):
        return len(self.pending)
    
    def num_available(self):
        return self.concurrency - self.num_running()
    
    def stop(self):
        _logger.info("Stopping kraken worker [%s].", self.wid)
        self.stopped = True
        self.pool.join()
        
class WorkerStoppedException(Exception):
    """Raised when trying to submit a task to a stopped worker"""
    pass