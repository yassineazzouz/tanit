#!/usr/bin/env python
# encoding: utf-8

import time
from .executor import Executor
import logging as lg

_logger = lg.getLogger(__name__)

class ExecutorPool(object):
    
    def __init__(self, wid, factory, cqueue, concurrency):
        self.cqueue = cqueue
        self.concurrency = concurrency
        self.wid = wid
        self.executors = []
        self.stopped = True
        
        for i in range(0, concurrency):
            self.executors.append(Executor("%s-executor-%s" % (wid,i), cqueue, factory))
    
    def start(self):
        _logger.info("Starting Kraken worker executor pool [ %s ].", self.wid)
        self.stopped = False
        for executor in self.executors:
            _logger.info("Starting executor [ %s ]", executor.eid)
            executor.start()
            _logger.info("Executor [ %s ] started", executor.eid)
        _logger.info("Kraken worker executor pool [ %s ] started, using %s concurrent executors", self.wid, self.concurrency)
    
    def stop(self):
        _logger.info("Stopping Kraken worker executor pool [ %s ].", self.wid)
        
        self.stopped = True
        for executor in self.executors:
            _logger.info("Stopping executor [ %s ]", executor.eid)
            executor.stop()
        for executor in self.executors:
            executor.join()
            _logger.info("Executor [ %s ] stopped", executor.eid)
        
        _logger.info("Kraken worker executor pool [ %s ] stopped.", self.wid)
    
    def num_running(self):
        return self.concurrency - self.num_available()
    
    def num_pending(self):
        return self.cqueue.qsize()
    
    def num_available(self):
        #if self.num_pending > 0:
        #    return self.concurrency
        idl=0
        for executor in self.executors:
            if (executor.isIdle()):
                idl+=1
        return idl
    
    def log_stats(self):
        while not self.stopped:
            _logger.info("worker starts { running : %s, pending : %s, available: %s}", self.num_running(), self.num_pending(), self.num_available())
            if (self.executors[0].current_task != None):
                _logger.info("task %s , %s", self.executors[0].current_task.src_path, self.executors[0].current_task.dest_path)
            time.sleep(2)