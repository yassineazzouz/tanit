#!/usr/bin/env python

import logging as lg

_logger = lg.getLogger(__name__)

class ExecutionManager(object):
    '''The ExecutionManager manage the state of execution of jobs and tasks'''
    
    def __init__(self):
        # jobs list
        self.jobs = []

    def register_job(self, job):
        self.jobs.append(job)

    def register_task(self, jid, task):
        job = self.get_job(jid)
        if (job != None):
            job.add_task(task, job)
        else:
            raise NoSuchJobException("No such job [ %s ]", jid)
       
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
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)

class NoSuchJobException(Exception):
    pass

class NoSuchTaskException(Exception):
    pass