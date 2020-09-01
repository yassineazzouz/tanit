#!/usr/bin/env python

import time

from .handler import MasterClientServiceHandler, MasterWorkerServiceHandler
from kraken.master.server.master import Master, StandaloneMaster
from ..thrift import MasterClientService, MasterWorkerService
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol
from thrift.server import TNonblockingServer

from threading import Thread

import logging as lg

_logger = lg.getLogger(__name__)

class MasterWorkerServer(Thread):
    
    def __init__(self, master, listen_address = "0.0.0.0", listen_port = 9091, n_threads = 10):
        super(MasterWorkerServer, self).__init__()
        
        self.master = master
        self.listen_address = listen_address
        self.listen_port = listen_port
        self.n_threads = n_threads
    
    def run(self):
        
        # Create Service handler
        handler = MasterWorkerServiceHandler(self.master)

        processor = MasterWorkerService.Processor(handler)

        transport = TSocket.TServerSocket(self.listen_address, self.listen_port)
        pfactory = TCompactProtocol.TCompactProtocolFactory()

        server = TNonblockingServer.TNonblockingServer(processor, transport, pfactory, pfactory, self.n_threads)
        
        # Start Kraken server
        server.serve()
        
class MasterClientServer(Thread):
    
    def __init__(self, master, listen_address = "0.0.0.0", listen_port = 9090, n_threads = 10):
        super(MasterClientServer, self).__init__()
        
        self.master = master
        self.listen_address = listen_address
        self.listen_port = listen_port
        self.n_threads = n_threads
        

    def run(self):
        # Create Service handler
        handler = MasterClientServiceHandler(self.master)

        processor = MasterClientService.Processor(handler)

        transport = TSocket.TServerSocket(self.listen_address, self.listen_port)
        pfactory = TCompactProtocol.TCompactProtocolFactory()

        server = TNonblockingServer.TNonblockingServer(processor, transport, pfactory, pfactory, self.n_threads)
        
        # Start Kraken server
        server.serve()

class MasterServer(object):
    
    def __init__(self, standalone = False):
        self.standalone = standalone
        self.master = Master() if not standalone else StandaloneMaster()
              
    def start(self):
        
        # Start master services
        self.master.start()
        
        _logger.info("Stating Kraken master client server.")
        
        self.mcserver = MasterClientServer(self.master)
        self.mcserver.setDaemon(True)
        self.mcserver.start()        
        _logger.info("Kraken master client server started, listening  at %s:%s", self.mcserver.listen_address, self.mcserver.listen_port)
        
        if (not self.standalone):
            _logger.info("Stating Kraken master worker server.")
            self.mwserver = MasterWorkerServer(self.master)
            self.mwserver.setDaemon(True)
            self.mwserver.start()        
            _logger.info("Kraken master worker server started, listening  at %s:%s", self.mwserver.listen_address, self.mwserver.listen_port)
        
        try:
            while True:
                if (not self.mcserver.isAlive()):
                    _logger.error("Unexpected Kraken master client server exit, stopping.")
                    break
                if (not self.standalone and not self.mwserver.isAlive()):
                    _logger.error("Unexpected Kraken worker client server exit, stopping.")
                    break
                # wait for 0.5 seconds
                time.sleep(0.5)
        except (KeyboardInterrupt, SystemExit):
            _logger.info("Received KeyboardInterrupt Signal.")
        except Exception as e:
            _logger.exception("Fatal server exception : %s, exiting", e)
        finally:
            _logger.info("Stopping Kraken server.")
            self.master.stop()
            _logger.info("Kraken server stopped.")