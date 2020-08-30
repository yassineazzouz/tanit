#!/usr/bin/env python
# encoding: utf-8

host = "localhost"
port = 9090

import sys

# Example files
from ..thrift.KrakenService import Client
from ..thrift.ttypes import *

# Thrift files
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

class KrakenClient(object):

    def start(self):
        # Init thrift connection and protocol handlers
        socket = TSocket.TSocket( host , port)
        self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        
        # Set client to our Example
        self.client = Client(protocol)

        # Connect to server
        self.transport.open()
    
    def list_jobs(self):
        return self.client.list_jobs()
    
    def dummy_job(self):
        conf = JobConf("swh", "swhstg", "/user/hive/warehouse/swh.db/wws_30002", "/user/yassine.azzouz/data/wws_31000", preserve=False)
        self.client.submit_job(conf)
        
    def stop(self):
        self.transport.close()