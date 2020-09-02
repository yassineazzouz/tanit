#!/usr/bin/env python
# encoding: utf-8

from ..thrift import MasterClientService, MasterWorkerService
from ..thrift.ttypes import Job, Worker

# Thrift files
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

class MasterClient(object):

    def start(self, host = "localhost", port = 9090):
        # Init thrift connection and protocol handlers
        socket = TSocket.TSocket( host , port)
        self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        
        # Set client to our Example
        self.client = MasterClientService.Client(protocol)

        # Connect to server
        self.transport.open()
    
    def list_jobs(self):
        return self.client.list_jobs()
    
    def dummy_job(self):
        job = Job("swh", "swhstg", "/user/yassine.azzouz/wws_30002", "/user/yassine.azzouz/data/wws_30002", preserve=False)
        self.client.submit_job(job)
        
    def stop(self):
        self.transport.close()
        

class MasterWorkerClient(object):
    
    def __init__(self, host = "localhost", port = 9091):
        # Init thrift connection and protocol handlers
        socket = TSocket.TSocket( host , port)
        self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        
        # Set client to our Example
        self.client = MasterWorkerService.Client(protocol)
        
    def start(self):
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