#!/usr/bin/env python

from .execution.execution_manager import ExecutionManager
from ...client.client_factory import ClientFactory
from ...common.model.worker import Worker

import logging as lg

_logger = lg.getLogger(__name__)

class Master(object):

    def __init__(self):
        # client factory
        self.client_factory = ClientFactory()
        # execution manager
        self.execution_manager = ExecutionManager()
        
        self.started = False
    
    def configure(self, config):
        pass
    
    def submit_job(self, conf):
        if (not self.started):
            raise MasterStoppedException("Can not submit job [ %s ] : master server stopped.", conf.jid)
        self.execution_manager.submit_job(conf)
        
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
        workers_manager = self.execution_manager.workers_manager
        wkr_list = []
        for wkr in workers_manager.list_workers():
            wkr_list.append(Worker(wkr.wid, wkr.address, wkr.port))
        return wkr_list
                    
    def register_worker(self, worker):
        if (not self.started):
            raise MasterStoppedException("Can not register worker [ %s ] : master server stopped.", worker.wid)
        
        workers_manager = self.execution_manager.workers_manager
        
        _logger.info("Registering new Worker [ %s ].", worker.wid)
        workers_manager.register_worker(worker)
        _logger.info("Worker [ %s ] registered.", worker.wid)   

    def register_heartbeat(self, worker):
        _logger.debug("Received heart beat from Worker [ %s ].", worker.wid)
        workers_manager = self.execution_manager.workers_manager
        workers_manager.register_heartbeat(worker)
            
    def unregister_worker(self, worker):
        if (not self.started):
            raise MasterStoppedException("Can not register worker [ %s ] : master server stopped.", worker.wid)
        
        # This will prevent any future tasks from being sent to the worker
        workers_manager = self.execution_manager.workers_manager
        workers_manager.decommission_worker(worker.wid)
        
    def start(self):
        _logger.info("Stating Kraken master services.")
        self.started = True
        self.execution_manager.start()
        _logger.info("Kraken master services started.")
        
    def stop(self):
        _logger.info("Stopping Kraken master services.")
        self.started = False
        self.execution_manager.stop()
        _logger.info("Kraken master services stopped.")
          
class MasterStoppedException(Exception):
    """Raised when trying to submit a task to a stopped master"""
    pass
    