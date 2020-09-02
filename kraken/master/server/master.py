#!/usr/bin/env python

import socket

from Queue import Queue
from ...common.core.engine import Engine
from ..core.dispatcher import FairDispatcher
from ..core.scheduler import SimpleScheduler
from ..core.worker import ThreadPoolWorker, RemoteThriftWorker
from ..core.job import JobExecution

import logging as lg

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
        job_exec = JobExecution(job)
        job_exec.setup()
        
        _logger.info("Submitting job [ %s ] for execution.", job.jid)
        for tid in job_exec.tasks:
            self.lqueue.put(job_exec.tasks[tid])
        _logger.info("Submitted %s tasks for execution in job [ %s ].", len(job_exec.tasks) ,job.jid)
        
        self.jobs.append(job_exec)

    def list_jobs(self):
        return self.jobs
    
    def get_job(self, jid):
        for job_exec in self.jobs:
            if (job_exec.job.jid == jid):
                return job_exec
        return None
  
    def task_start(self, tid):
        for job_exec in self.jobs:
            if tid in job_exec.tasks:
                task_exec = job_exec.tasks[tid]
                task_exec.on_start()
                break
      
    def task_success(self, tid):
        for job_exec in self.jobs:
            if tid in job_exec.tasks:
                task_exec = job_exec.tasks[tid]
                task_exec.on_finish()
                break
    
    def task_failure(self, tid):
        for job_exec in self.jobs:
            if tid in job_exec.tasks:
                task_exec = job_exec.tasks[tid]
                task_exec.on_fail()
                break
            
    def register_worker(self, worker):
        if (not self.started):
            raise MasterStoppedException("Can not register worker [ %s ] : master server stopped.", worker.wid)
        
        _logger.info("Registering new Worker [ %s ].", worker.wid)
        wkr = RemoteThriftWorker(worker.wid,worker.address, worker.port)
        wkr.start()
        self.dispatcher.register_worker(wkr)
        _logger.info("Worker [ %s ] registered.", worker.wid)   
            
    def start(self):
        _logger.info("Stating Kraken master services.")
        self.dispatcher.start()
        self.scheduler.start()
        self.started = True
        _logger.info("Kraken master services started.")
        
    def stop(self):
        _logger.info("Stopping Kraken master services.")
        self.started = False
        self.scheduler.stop()
        self.dispatcher.stop()
        _logger.info("Kraken master services stopped.")

class StandaloneMaster(Master):
    def __init__(self):
        super(StandaloneMaster, self).__init__()
        self.worker = ThreadPoolWorker("worker-local-%s-1" % socket.gethostname(),self.engine)
        
    def start(self):
        super(StandaloneMaster, self).start()
        _logger.info("Registering new local Worker [ %s ].", self.worker.wid)
        self.dispatcher.register_worker(self.worker)
        _logger.info("Worker [ %s ] registered.", self.worker.wid)
         
    
                
class MasterStoppedException(Exception):
    """Raised when trying to submit a task to a stopped master"""
    pass
    