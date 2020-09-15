#!/usr/bin/env python

import logging as lg
from datetime import datetime
from threading import Lock
from .execution_state import ExecutionState

_logger = lg.getLogger(__name__)

class TaskExecution(object):
    '''TaskExecution represent the execution flow of a task in the master'''
    
    def __init__(self, task, job):
        self.state = ExecutionState.SUBMITTED
        self.task = task
        self.job = job
        self.worker = None
        
    def get_state(self):
        return self.state

class JobExecution(object):
    '''
    JobExecution represent the execution flow of a job in the master
    The job state diagram is as follow:
    ----------------------------------------------------------------------
    |   SUBMITTED --> SCHEDULED --> DISPATCHED --> RUNNING --> FINISHED  |
    |                                                |                   |
    |                                                |                   |
    |                                              FAILED                |
    ---------------------------------------------------------------------|
    '''

    def __init__(self, job):
        
        self.state = ExecutionState.SUBMITTED
        
        self.submission_time = datetime.now()
        self.schedule_time = None
        self.dispatch_time = None
        self.start_time = None
        self.finish_time = None
        self.execution_time_s = -1

        self.tasks = {}
        
        self.scheduled_tasks = 0
        self.dispatched_tasks = 0
        self.started_tasks = 0
        self.finished_tasks = 0
        self.failed_tasks = 0

        self.job = job
        self.jlock = Lock()


    def get_task(self, tid):
        if tid in self.tasks:
            return self.tasks[tid]
        else:
            return None

    def get_tasks(self):
        return self.tasks.values()
    
    def add_task(self, task):
        self.tasks[task.tid] = TaskExecution(task, self)

    def get_state(self):
        return self.state