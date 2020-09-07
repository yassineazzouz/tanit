#!/usr/bin/env python
# encoding: utf-8

from ..thrift import MasterClientService, MasterWorkerService
from ..thrift.ttypes import Job, Worker

import json

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
        return self.client.list_jobs()
    
    def job_status(self, jid):
        return self.client.job_status(jid)
        
    def submit_job(self, job_spec):
        try:
            job = Job(**job_spec)
        except Exception as e:
            _logger.error("Error resolving job parameters.")
            raise e
        
        self.client.submit_job(job)
        
    
    def dummy_job(self):
        job = Job("swh", "swhstg", "/user/hive/warehouse/swh.db/wws_30002", "/user/yassine.azzouz/data/wws_30002", preserve=False)
        self.client.submit_job(job)
        
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

    def register_worker(self, wid, address, port):
        return self.client.register_worker(Worker(wid, address, port))
    
    def task_start(self, tid):
        self.client.task_start(tid) 
        
    def task_success(self, tid):
        self.client.task_success(tid)  
    
    def task_failure(self, tid):
        self.client.task_failure(tid)
    
    def stop(self):
        self.transport.close()