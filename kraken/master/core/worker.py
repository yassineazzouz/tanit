#!/usr/bin/env python
# The execution engine controls the execution environement
# encoding: utf-8

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