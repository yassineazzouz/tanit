#!/usr/bin/env python

from ..core.job import Job
from ..core.worker import Worker
from ..thrift.ttypes import JobStatus, JobState

import logging as lg

_logger = lg.getLogger(__name__)

class MasterClientServiceHandler(object):

    def __init__(self, master):
        self.master = master

    def submit_job(self, conf):
        job = Job(conf)
        self.master.submit_job(job)

    def list_jobs(self):
        status = []
        for job in self.master.list_jobs():
            status.append(JobStatus(job.jid, JobState._NAMES_TO_VALUES[job.state], job.submission_time))
        return status

    def job_status(self, jid):
        job = self.master.get_job(jid)
        if (job == None):
            raise JobNotFoundException("No such job [ %s ]", jid)
        return JobStatus(job.jid, JobState._NAMES_TO_VALUES[job.state], job.submission_time)

class MasterWorkerServiceHandler(object):

    def __init__(self, master):
        self.master = master
        
    def register_worker(self, worker):
        wker = Worker(worker.wid, worker.address, worker.port)
        self.master.register_worker(self, wker)

    def send_heartbeat(self, worker):
        pass        
        
    
class JobNotFoundException(Exception):
    """Raised when trying to submit a task to a stopped master"""
    pass