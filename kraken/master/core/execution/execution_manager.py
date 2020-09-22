#!/usr/bin/env python

import logging as lg

from Queue import Queue
from ..dispatcher import FairDispatcher
from ..scheduler import SimpleScheduler
from .job_factory import JobFactory
from ..worker.worker_manager import WorkerManager

_logger = lg.getLogger(__name__)

class ExecutionManager(object):
    '''
    The ExecutionManager monitor the state of execution of jobs and tasks.
    It interacts with the different components involved in the execution pipeline
    and ensure the execution state is properly updated and reflect the real progress.
    '''
    
    def __init__(self, jobs_factory = None, workers_manager = None):
        # jobs list
        self.jobs = []
        # job factory
        self.jobs_factory = jobs_factory if jobs_factory != None else JobFactory()
        # Lister queue
        lqueue = Queue()
        # Call queue
        cqueue = Queue()
        # workers manager
        self.workers_manager = workers_manager if workers_manager else WorkerManager(self)
        # scheduler
        self.scheduler = SimpleScheduler(lqueue, cqueue, self.task_schedule )
        # dispatcher
        self.dispatcher  = FairDispatcher(cqueue, self.workers_manager, self.task_dispatch)

    def configure(self, config):
        pass

    def start(self):
        _logger.info("Stating Kraken master services.")
        self.workers_manager.start()
        self.dispatcher.start()
        self.scheduler.start()
        _logger.info("Kraken master services started.")
        
    def stop(self):
        _logger.info("Stopping Kraken master services.")
        self.scheduler.stop()
        self.dispatcher.stop()
        self.workers_manager.stop()
        _logger.info("Kraken master services stopped.")

    def submit_job(self, conf):       
        _logger.info("Submitting job [ %s ] for execution.", conf.jid)
        job = self.jobs_factory.create_job(conf)
        job.setup()

        self.jobs.append(job)
        for task_exec in self.get_tasks(jid = conf.jid):
            self.scheduler.schedule(task_exec)
            
        _logger.info("Submitted %s tasks for execution in job [ %s ].", len(self.get_tasks(jid = conf.jid)) ,conf.jid)

    def cancel_job(self, conf):
        pass
       
    def list_jobs(self):
        return self.jobs
    
    def get_job(self, jid):
        for job_exec in self.jobs:
            if job_exec.job.jid == jid:
                return job_exec
        return None
    
    def get_tasks(self, jid = None, states = [], worker = None):
        target_jobs = []
        if jid != None:
            job = self.get_job(jid)
            if (job != None):
                target_jobs.append(job)
            else:
                return []
        else:
            target_jobs = self.jobs
        
        target_tasks = []
        for job in target_jobs:
            for task in job.get_tasks():
                valid = True
                if len(states) != 0:
                    if task.state not in states:
                        valid = False
                if worker != None:
                    if (task.worker != worker):
                        valid = False
                if valid:
                    target_tasks.append(task)
        return target_tasks   

    def get_task(self, tid):
        for job_exec in self.jobs:
            task = job_exec.get_task(tid)
            if (task != None):
                return task
        return None

    def task_schedule(self, tid):
        task = self.get_task(tid)
        if (task != None):
            task.on_schedule()
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)

    def task_dispatch(self, tid, worker = None):
        task = self.get_task(tid)
        if (task != None):
            task.on_dispatch(worker)
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)
        
    def task_start(self, tid):
        task = self.get_task(tid)
        if (task != None):
            task.on_start()
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)
      
    def task_finish(self, tid):
        task = self.get_task(tid)
        if (task != None):
            task.on_finish()
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)
    
    def task_failure(self, tid):
        task = self.get_task(tid)
        if (task != None):
            task.on_fail()
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)

    def task_reset(self, tid):
        task = self.get_task(tid)
        if (task != None):
            task.on_reset()
            self.scheduler.schedule(task)
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)

class NoSuchJobException(Exception):
    pass

class NoSuchTaskException(Exception):
    pass