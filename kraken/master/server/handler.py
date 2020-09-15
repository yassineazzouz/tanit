#!/usr/bin/env python

from datetime import datetime
from hashlib import md5

from ...common.model.job import Job
from ...common.model.worker import Worker
from ..core.execution.execution_state import ExecutionState
from ..thrift import ttypes

import logging as lg

_logger = lg.getLogger(__name__)

class MasterClientServiceHandler(object):

    def __init__(self, master):
        self.master = master

    def submit_job(self, job):
        jid = "job-%s-%s" % (
            md5("{}-{}-{}-{}".format(job.src, job.src_path, job.dest, job.dest_path)).hexdigest(),
            str(datetime.now().strftime("%d%m%Y%H%M%S"))
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
        return jid

    def list_jobs(self):
        status = []
        for job_exec in self.master.list_jobs():
            status.append( 
                ttypes.JobStatus(
                    job_exec.job.jid,
                    ttypes.JobState._NAMES_TO_VALUES[ExecutionState._VALUES_TO_NAMES[job_exec.state]],
                    job_exec.submission_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "-" if (job_exec.start_time == None) else job_exec.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "-" if (job_exec.finish_time == None) else job_exec.finish_time.strftime("%Y-%m-%d %H:%M:%S"),
                    job_exec.execution_time_s
                )
            )
        return status

    def job_status(self, jid):
        job_exec = self.master.get_job(jid)
        if (job_exec == None):
            raise ttypes.JobNotFoundException("No such job [ %s ]" % jid)
        return  ttypes.JobStatus(
                    job_exec.job.jid,
                    ttypes.JobState._NAMES_TO_VALUES[ExecutionState._VALUES_TO_NAMES[job_exec.state]],
                    job_exec.submission_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "-" if (job_exec.start_time == None) else job_exec.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "-" if (job_exec.finish_time == None) else job_exec.finish_time.strftime("%Y-%m-%d %H:%M:%S"),
                    job_exec.execution_time_s
                )
        
class MasterWorkerServiceHandler(object):

    def __init__(self, master):
        self.master = master

    def list_workers(self):
        workers = []
        for wkr in self.master.list_workers():
            workers.append(ttypes.Worker(wkr.wid,wkr.address, wkr.port))
        return workers

    def register_heartbeat(self, worker):
        wker = Worker(worker.wid, worker.address, worker.port)
        self.master.register_heartbeat(wker)
        
    def register_worker(self, worker):
        wker = Worker(worker.wid, worker.address, worker.port)
        self.master.register_worker(wker)

    def unregister_worker(self, worker):
        wker = Worker(worker.wid, worker.address, worker.port)
        self.master.unregister_worker(wker)
     
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