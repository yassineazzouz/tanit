#!/usr/bin/env python
# encoding: utf-8

import abc
from datetime import datetime
from ....worker.client.client import WorkerClient

import logging as lg
_logger = lg.getLogger(__name__)

class WorkerIFace(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def start(self):
        pass
    
    @abc.abstractmethod    
    def stop(self):
        pass

    @abc.abstractmethod
    def submit(self, task):
        pass
    
    @abc.abstractmethod
    def status(self):
        pass

class RemoteThriftWorker(WorkerIFace):
    def __init__(self, wid, address, port):
        self.wid = wid
        self.address = address
        self.port = port
        self.last_hear_beat = datetime.now()
        
        self.client = WorkerClient(address, port)
        
        self.stopped = True
        
    def start(self):
        self.stopped = False
        self.client.start()
        
    def stop(self):
        self.stopped = True
        self.client.stop()

    def submit(self, task):
        if (not self.stopped):
            self.client.submit(task)
        else :
            raise WorkerStoppedException("Can not submit task [ %s ] to [ %s ] : worker stopped.", task.tid, self.wid)
        
    def status(self):
        return self.client.worker_status()
             
class WorkerStoppedException(Exception):
    """Raised when trying to submit a task to a stopped worker"""
    pass