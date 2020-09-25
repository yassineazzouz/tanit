#!/usr/bin/env python

from .execution.execution_manager import ExecutionManager
from .worker.worker_factory import WorkerFactory
from .worker.worker_manager import WorkerManager
from .worker.worker_decommissioner import WorkerDecommissioner
from ...common.model.worker import Worker

import logging as lg

_logger = lg.getLogger(__name__)

class Master(object):

    def __init__(self):
        # workers factory
        self.workers_factory = WorkerFactory(self)
        # workers manager
        self.workers_manager = WorkerManager(self.workers_factory)
        # execution manager
        self.execution_manager = ExecutionManager(self.workers_manager)
        # decommissioner
        self.decommissioner = WorkerDecommissioner(self.execution_manager, self.workers_manager)
        
        self.started = False
    
    def configure(self, config):
        pass
    
    def submit_job(self, job):
        if (not self.started):
            raise MasterStoppedException("Can not submit job, master server stopped.")
        return self.execution_manager.submit_job(job)
        
    def list_jobs(self):
        return self.execution_manager.list_jobs()
    
    def get_job(self, jid):
        return self.execution_manager.get_job(jid)
  
    def task_start(self, tid):
        self.execution_manager.task_start(tid)
      
    def task_success(self, tid):
        self.execution_manager.task_finish(tid)
    
    def task_failure(self, tid):
        self.execution_manager.task_failure(tid)

    def list_workers(self):
        _logger.info("Listing Workers.")
        wkr_list = []
        for wkr in self.workers_manager.list_live_workers():
            wkr_list.append(Worker(wkr.wid, wkr.address, wkr.port))
        return wkr_list
                    
    def register_worker(self, worker):
        if (not self.started):
            raise MasterStoppedException("Can not register worker [ %s ] : master server stopped.", worker.wid)
        
        _logger.info("Registering new Worker [ %s ].", worker.wid)
        self.workers_manager.register_worker(worker)
        _logger.info("Worker [ %s ] registered.", worker.wid)   

    def register_heartbeat(self, worker):
        _logger.debug("Received heart beat from Worker [ %s ].", worker.wid)
        self.workers_manager.register_heartbeat(worker)
            
    def unregister_worker(self, worker):
        if (not self.started):
            raise MasterStoppedException("Can not register worker [ %s ] : master server stopped.", worker.wid)
        
        # This will prevent any future tasks from being sent to the worker
        self.workers_manager.decommission_worker(worker.wid)
        
    def start(self):
        _logger.info("Stating Kraken master services.")
        self.started = True
        self.workers_manager.start()
        self.execution_manager.start()
        self.decommissioner.start()
        _logger.info("Kraken master services started.")
        
    def stop(self):
        _logger.info("Stopping Kraken master services.")
        self.started = False
        self.decommissioner.stop()
        self.decommissioner.join()
        self.execution_manager.stop()
        self.workers_manager.stop()
        _logger.info("Kraken master services stopped.")
          
class MasterStoppedException(Exception):
    """Raised when trying to submit a task to a stopped master"""
    pass
    