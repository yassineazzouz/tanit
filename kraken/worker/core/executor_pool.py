#!/usr/bin/env python
# encoding: utf-8

from .executor import Executor
import logging as lg

_logger = lg.getLogger(__name__)

class ExecutorPool(object):
    
    def __init__(self, master, cqueue, concurrency):
        self.cqueue = cqueue
        self.concurrency = concurrency
        self.executors = []
        for i in range(0, concurrency):
            self.executors.append(Executor(cqueue, master))
    
    def start(self):
        _logger.info("Starting kraken worker executor pool using %s concurrent threads", self.concurrency)
        for executor in self.executors:
            executor.start()
    
    def stop(self):
        _logger.info("Stopping kraken worker executor pool.")
        for executor in self.executors:
            executor.stop()
        for executor in self.executors:
            executor.join()
    
    def num_running(self):
        return self.concurrency - self.num_available()
    
    def num_pending(self):
        return self.cqueue.qsize()
    
    def num_available(self):
        if self.num_pending > 0:
            return 0
        
        idl=0
        for executor in self.executors:
            if (executor.isIdle()):
                idl+=1
        return idl