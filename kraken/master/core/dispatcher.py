#!/usr/bin/env python
# The execution engine controls the execution environement
# encoding: utf-8

import abc
import time
from threading import Thread

import logging as lg
_logger = lg.getLogger(__name__)

class Dispatcher(object):
    __metaclass__ = abc.ABCMeta
    '''
    The Dispatcher distribute scheduled tasks between workers.
    Initially tasks are queued when submitted to the dispatcher
    then the dispatch thread continuously poll tasks and select
    the appropriate worker for execution.
    '''
    
    def __init__(self, cqueue):
        self.workers = []
        self.cqueue = cqueue
        self.stopped = False

    def _run(self):
        while True:
            if (not self.cqueue.empty()):
                worker = self.get_worker()
                if (worker == None):
                    _logger.warn("Failed to dispatch tasks : no workers found !")
                    time.sleep(2)
                else:
                    task_exec = self.cqueue.get()
                    _logger.debug("Dispatching next task [ %s ] for execution.", task_exec.task.tid)
                    task_exec.on_dispatch()
                    worker.submit(task_exec.task)
            else:
                _logger.debug("No new tasks to dispatch, sleeping for %s seconds...", 2)
                time.sleep(2)
    
            if (self.stopped and self.cqueue.empty()):
                _logger.debug("No new tasks to dispatch, terminating dispatcher thread.")
                return
        
    def register_worker(self, worker):
        worker.start()
        self.workers.append(worker)
    
    @abc.abstractmethod
    def get_worker(self):
        return

    def start(self):
        _logger.info("Stating kraken dispatcher.")
        self.daemon = Thread(target=self._run, args=())
        self.daemon.setDaemon(True)
        self.daemon.start()
        _logger.info("Kraken dispatcher started.")

    def stop(self):
        _logger.info("Stopping kraken dispatcher.")
        self.stopped = True
        self.daemon.join()
        for worker in self.workers:
            worker.stop()
        _logger.info("kraken dispatcher Stopped.")
        
    
class FairDispatcher(Dispatcher):
        
    def get_worker(self):
        if(len(self.workers) == 0):
            return None
        
        next_worker = self.workers[0]
        next_status = next_worker.status()  
        for worker in self.workers[1:len(self.workers)]:
            status = worker.status()
            if (status.pending < next_status.pending):
                next_worker = worker
                next_status = status
                continue
            elif (status.pending == next_status.pending):
                if (status.available > next_status.available):
                    next_worker = worker
                    next_status = status
                    continue
        return next_worker