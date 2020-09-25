#!/usr/bin/env python
# encoding: utf-8

import abc
import logging as lg
from datetime import datetime
from ....worker.client.client import WorkerClient

_logger = lg.getLogger(__name__)

class WorkerIFace(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, wid, address, port):
        self.wid = wid
        self.address = address
        self.port = port
        self.last_hear_beat = datetime.now()
        
        self.stopped = True
    
    def start(self):
        self.stopped = False
      
    def stop(self):
        self.stopped = True

    @abc.abstractmethod
    def submit(self, task):
        pass
    
    @abc.abstractmethod
    def status(self):
        pass

class RemoteThriftWorker(WorkerIFace):

    def __init__(self, wid, address, port):
        super(RemoteThriftWorker, self).__init__(wid, address, port)
        self.client = WorkerClient(address, port)

    def start(self):
        super(RemoteThriftWorker, self).start()
        self.client.start()

    def stop(self):
        super(RemoteThriftWorker, self).stop()
        self.client.stop()

    def submit(self, task_exec):
        if (not self.stopped):
            self.client.submit(task_exec.tid, task_exec.etype, task_exec.params)
        else:
            raise WorkerStoppedException("Can not submit task [ %s ] to [ %s ] : worker stopped.", task_exec.tid, self.wid)

    def status(self):
        return self.client.worker_status()

class WorkerStoppedException(Exception):
    """Raised when trying to submit a task to a stopped worker"""
    pass