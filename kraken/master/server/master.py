#!/usr/bin/env python

from Queue import Queue
from ..core.dispatcher import FairDispatcher
from ..core.engine import Engine
from ..core.scheduler import SimpleScheduler

import logging as lg
from kraken.master.core.worker import ThreadPoolWorker, RemoteThriftWorker

_logger = lg.getLogger(__name__)

class Master(object):

    def __init__(self):
        self.jobs = []
        self.started = False

        # Lister queue
        self.lqueue = Queue()
        # Call queue
        self.cqueue = Queue()
        # execution engine
        self.engine = Engine()
        # scheduler
        self.scheduler = SimpleScheduler(self.lqueue, self.cqueue)
        # dispatcher
        self.dispatcher  = FairDispatcher(self.cqueue)
    
    def submit_job(self, job):
        if (not self.started):
            raise MasterStoppedException("Can not submit job [ %s ] : master server stopped.", job.jid)
        
        _logger.info("Received new job [ %s ].", job.jid)
        
        _logger.info("Configuring job [ %s ].", job.jid)
        self.engine.setup_job(job)
        
        _logger.info("Submitting job [ %s ] for execution.", job.jid)
        for task in job.tasks:
            self.lqueue.put(task)
        _logger.info("Submitted %s tasks for execution in job [ %s ].", len(job.tasks) ,job.jid)
        
        self.jobs.append(job)

    def list_jobs(self):
        return self.jobs
    
    def get_job(self, jid):
        for job in self.jobs:
            if (job.jid == jid):
                return job
        return None
            
    def register_worker(self, worker):
        if (not self.started):
            raise MasterStoppedException("Can not register worker [ %s ] : master server stopped.", worker.wid)
        
        _logger.info("Registering new Worker [ %s ].", worker.wid)
        if (worker.address == None):
            self.dispatcher.register_worker(ThreadPoolWorker(worker.wid,self.engine))
        else:
            self.dispatcher.register_worker(RemoteThriftWorker(worker.wid,worker.address, worker.port))
        _logger.info("Worker [ %s ] registered.", worker.wid)   
            
    def start(self):
        _logger.info("Stating Kraken master.")
        self.dispatcher.start()
        self.scheduler.start()
        self.started = True
        _logger.info("Kraken master started.")
        
    def stop(self):
        _logger.info("Stopping Kraken master.")
        self.started = False
        self.scheduler.stop()
        self.dispatcher.stop()
        _logger.info("Kraken master stopped.")
        
class MasterStoppedException(Exception):
    """Raised when trying to submit a task to a stopped master"""
    pass
    