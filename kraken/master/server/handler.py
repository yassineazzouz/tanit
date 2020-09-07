#!/usr/bin/env python
import time
from hashlib import md5

from ...common.model.job import Job
from ...common.model.worker import Worker
from ..thrift.ttypes import JobStatus, JobState

import logging as lg

_logger = lg.getLogger(__name__)

class MasterClientServiceHandler(object):

    def __init__(self, master):
        self.master = master

    def submit_job(self, job):
        jid = "job-%s-%s" % (
            md5("{}-{}-{}-{}".format(job.src, job.src_path, job.dest, job.dest_path)).hexdigest(),
            time.strftime("%d%m%Y%H%M%S")
        )
        job = Job( jid,
                   job.src,
                   job.dest,
                   job.src_path,
                   job.dest_path,
                   job.include_pattern,
                   job.min_size,
                   job.preserve,
                   job.force,
                   job.checksum,
                   job.files_only,
                   job.part_size,
                   job.buffer_size)
        self.master.submit_job(job)

    def list_jobs(self):
        status = []
        for job_exec in self.master.list_jobs():
            status.append(JobStatus(job_exec.job.jid, JobState._NAMES_TO_VALUES[job_exec.state], job_exec.submission_time))
        return status

    def job_status(self, jid):
        job_exec = self.master.get_job(jid)
        if (job_exec == None):
            raise JobNotFoundException("No such job [ %s ]", jid)
        return JobStatus(job_exec.job.jid, JobState._NAMES_TO_VALUES[job_exec.state], job_exec.submission_time)

class MasterWorkerServiceHandler(object):

    def __init__(self, master):
        self.master = master
        
    def register_worker(self, worker):
        wker = Worker(worker.wid, worker.address, worker.port)
        self.master.register_worker(wker)
     
    def task_start(self, tid):
        self.master.task_start(tid)
           
    def task_success(self, tid):
        self.master.task_success(tid)
    
    def task_failure(self, tid):
        self.master.task_failure(tid)

    def send_heartbeat(self, worker):
        pass
        
    
class JobNotFoundException(Exception):
    """Raised when trying to submit a task to a stopped master"""
    pass