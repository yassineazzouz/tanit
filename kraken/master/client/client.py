#!/usr/bin/env python
# encoding: utf-8

from ..thrift import MasterClientService, MasterWorkerService
from ..thrift.ttypes import JobConf, Worker

# Thrift files
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol
from thrift.server import TNonblockingServer

class MasterClient(object):

    def start(self, host = "localhost", port = 9090):
        # Init thrift connection and protocol handlers
        socket = TSocket.TSocket( host , port)
        self.transport = TTransport.TFramedTransport(socket)
        protocol = TCompactProtocol.TCompactProtocol(self.transport)
        
        # Set client to our Example
        self.client = MasterClientService.Client(protocol)

        # Connect to server
        self.transport.open()
    
    def list_jobs(self):
        return self.client.list_jobs()
    
    def dummy_job(self):
        conf = JobConf("swh", "swhstg", "/user/hive/warehouse/swh.db/wws_30002", "/user/yassine.azzouz/data/wws_31000", preserve=False)
        self.client.submit_job(conf)
        
    def stop(self):
        self.transport.close()
        

class MasterWorkerClient(object):
    
    def start(self, host = "localhost", port = 9091):
        # Init thrift connection and protocol handlers
        socket = TSocket.TSocket( host , port)
        self.transport = TTransport.TFramedTransport(socket)
        protocol = TCompactProtocol.TCompactProtocol(self.transport)
        
        # Set client to our Example
        self.client = MasterWorkerService.Client(protocol)

        # Connect to server
        self.transport.open()

    def register_worker(self, wid, address, port):
        return self.client.register_worker(Worker(wid, address, port))
    
    def stop(self):
        self.transport.close()