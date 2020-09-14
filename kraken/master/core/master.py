#!/usr/bin/env python

import time
from Queue import Queue
from threading import Thread
from .dispatcher import FairDispatcher
from .scheduler import SimpleScheduler
from .worker_manager import WorkerManager
from .job import JobExecution
from ...common.core.engine import Engine
from ...common.model.worker import Worker

import logging as lg

_logger = lg.getLogger(__name__)

class Master(object):

    def __init__(self):
        # list of jobs
        self.jobs = []
          
        self.started = False

        # Lister queue
        self.lqueue = Queue()
        # Call queue
        self.cqueue = Queue()
        # execution engine
        self.engine = Engine()
        # workers manager
        self.workers_manager = WorkerManager()
        # scheduler
        self.scheduler = SimpleScheduler(self.lqueue, self.cqueue)
        # dispatcher
        self.dispatcher  = FairDispatcher(self.cqueue, self.workers_manager)
        # dicommissionner
        self.dicommissionner = WorkerDicommission(self)
        self.dicommissionner.setDaemon(True)
    
    def configure(self, config):
        pass
    
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

    def list_workers(self):
        _logger.info("Listing Workers.")
        wkr_list = []
        for wkr in self.workers_manager.list_workers():
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
        self.workers_manager.start()
        self.dispatcher.start()
        self.scheduler.start()
        self.dicommissionner.start()
        
        
        self.started = True
        _logger.info("Kraken master services started.")
        
    def stop(self):
        _logger.info("Stopping Kraken master services.")
        self.started = False
        self.scheduler.stop()
        self.dispatcher.stop()
        self.workers_manager.stop()
        self.dicommissionner.stop()
        self.dicommissionner.join()
        _logger.info("Kraken master services stopped.")



class WorkerDicommission(Thread):
    
    def __init__(self, master):
        super(WorkerDicommission, self).__init__()
        self.master = master
        self.workers_manager = master.workers_manager
        self.stopped = False
        
    def run(self):
        while not self.stopped:
            for worker in self.workers_manager.list_decommissioning_workers():
                self._decommission_worker(worker)
                self.workers_manager.on_worker_decommissioned(worker.wid)
    
    def stop(self):
        self.stopped = True

    def _decommission_worker(self, worker):
        worker_tasks = []
        for job_exec in self.master.jobs:
            if (job_exec.state not in ["FINISHED", "FAILED"]):
                for task_exec in job_exec.get_tasks():
                    if (task_exec.worker == worker.wid):
                        worker_tasks.append(task_exec)
        
        
        while(not self.stopped):
            # While the worker is up, wait for the tasks to finish
            # check if the worker is still alive
            try:
                worker.status()
            except:
                break
            # check how many tasks are still alive
            for task_exec in worker_tasks:
                if (task_exec.state in ["FAILED", "FINISHED"]):
                    worker_tasks.pop(task_exec)
            
            if (len(worker_tasks) == 0):
                break
            else:
                time.sleep(1.0)
    
        # check if all tasks actually
        if (len(worker_tasks) == 0):
            _logger.info("Worker [ %s ] have no more active tasks.", worker.wid)
        else:
            _logger.info("Worker [ %s ] still have %s active tasks.", worker.wid, len(worker_tasks))
            # reschedule the tasks
            for task_exec in worker_tasks:
                job_exec = task_exec.job
                tid = task_exec.task.tid
                
                job_exec.reset_task(tid)
                self.master.lqueue.put(job_exec.tasks[tid])
          
class MasterStoppedException(Exception):
    """Raised when trying to submit a task to a stopped master"""
    pass
    