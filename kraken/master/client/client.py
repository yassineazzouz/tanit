#!/usr/bin/env python
# encoding: utf-8

from ..thrift import MasterClientService, MasterWorkerService
from ..thrift import ttypes

from ...common.model.job import JobStatus
from ...common.model.worker import Worker

# Thrift files
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import logging as lg

_logger = lg.getLogger(__name__)

class MasterClient(object):

    def __init__(self, master_host, master_port):
        self.master_host = master_host
        self.master_port = master_port
        
    def start(self):
        # Init thrift connection and protocol handlers
        socket = TSocket.TSocket( self.master_host , self.master_port)
        self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        
        # Set client to our Example
        self.client = MasterClientService.Client(protocol)

        # Connect to server
        self.transport.open()
    
    def list_jobs(self):
        jobs = []
        for job in self.client.list_jobs():
            jobs.append(
                JobStatus( job.id, ttypes.JobState._VALUES_TO_NAMES[job.state], job.submission_time, job.start_time, job.finish_time, job.execution_time))
        return jobs
    
    def job_status(self, jid):
        try:
            st = self.client.job_status(jid)
            return JobStatus(st.id, ttypes.JobState._VALUES_TO_NAMES[st.state], st.submission_time, st.start_time, st.finish_time, st.execution_time)
        except ttypes.JobNotFoundException:
            return None
        
    def submit_job(self, job_spec):
        _logger.info("Submitting new job.")
        try:
            job = ttypes.Job(**job_spec)
        except Exception as e:
            _logger.error("Error resolving job parameters.")
            raise e
        
        id = self.client.submit_job(job)
        _logger.info("Job submitted : %s.", id)
        return id
        
    def stop(self):
        self.transport.close()
        

class MasterWorkerClient(object):
    
    def __init__(self, master_host, master_port):
        self.master_host = master_host
        self.master_port = master_port

    def start(self):
        
        # Init thrift connection and protocol handlers
        socket = TSocket.TSocket( self.master_host , self.master_port)
        self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        
        # Set client to our Example
        self.client = MasterWorkerService.Client(protocol)
        
        # Connect to server
        self.transport.open()

    def list_workers(self):
        wkr_list = []
        for wkr in self.client.list_workers():
            wkr_list.append(Worker(wkr.wid, wkr.address, wkr.port))
        return wkr_list

    def register_worker(self, wid, address, port):
        return self.client.register_worker(ttypes.Worker(wid, address, port))

    def unregister_worker(self, wid, address, port):
        return self.client.unregister_worker(ttypes.Worker(wid, address, port))

    def register_heartbeat(self, wid, address, port):
        self.client.register_heartbeat(ttypes.Worker(wid, address, port))
            
    def task_start(self, tid):
        self.client.task_start(tid) 
        
    def task_success(self, tid):
        self.client.task_success(tid)  
    
    def task_failure(self, tid):
        self.client.task_failure(tid)
    
    def stop(self):
        self.transport.close()