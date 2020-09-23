#!/usr/bin/env python
# encoding: utf-8

from .worker import RemoteThriftWorker

class WorkerFactory(object):
        
    def create_worker(self, worker):
        return RemoteThriftWorker(worker.wid,worker.address, worker.port)