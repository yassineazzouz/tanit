#!/usr/bin/env python
# The execution engine controls the execution environement
# encoding: utf-8

from multiprocessing.pool import ThreadPool

import logging as lg
_logger = lg.getLogger(__name__)

class ThreadPoolWorker(object):

    def __init__(self, wid, engine, concurrency = 25):
        self.pool = ThreadPool(concurrency)
        self.concurrency = 25
        self.engine = engine
        self.wid = wid
        
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