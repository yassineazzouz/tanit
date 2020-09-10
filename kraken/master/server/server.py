#!/usr/bin/env python

import time

from .handler import MasterClientServiceHandler, MasterWorkerServiceHandler
from ..core.master import Master
from ..standalone.master import StandaloneMaster
from ..thrift import MasterClientService, MasterWorkerService
from ..config.config import MasterConfig
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from threading import Thread

import logging as lg

_logger = lg.getLogger(__name__)

class MasterWorkerServer(Thread):
    
    def __init__(self, master):
        super(MasterWorkerServer, self).__init__()
        self.master = master
    
    def configure(self, config):
        self.listen_address = config.bind_address
        self.listen_port = config.worker_service_port
        self.n_threads = config.thrift_threads
        
    def run(self):
        
        # Create Service handler
        handler = MasterWorkerServiceHandler(self.master)

        server = TServer.TThreadedServer(
            MasterWorkerService.Processor(handler),
            TSocket.TServerSocket(self.listen_address, self.listen_port),
            TTransport.TBufferedTransportFactory(),
            TBinaryProtocol.TBinaryProtocolFactory(),
            daemon=True
        )
        
        # Start Kraken server
        server.serve()
        
class MasterClientServer(Thread):
    
    def __init__(self, master):
        super(MasterClientServer, self).__init__()
        self.master = master

    def configure(self, config):
        self.listen_address = config.bind_address
        self.listen_port = config.client_service_port
        self.n_threads = config.thrift_threads        

    def run(self):
        # Create Service handler
        handler = MasterClientServiceHandler(self.master)
        
        
        server = TServer.TThreadedServer(
            MasterClientService.Processor(handler),
            TSocket.TServerSocket(self.listen_address, self.listen_port),
            TTransport.TBufferedTransportFactory(),
            TBinaryProtocol.TBinaryProtocolFactory(),
            daemon=True
        )
        
        # Start Kraken server
        server.serve()

class MasterServer(object):
    
    def __init__(self, standalone = False):
        
        self.config = MasterConfig()
        self.config.load()
        
        self.standalone = standalone
        self.master = Master() if not standalone else StandaloneMaster()
        self.master.configure(self.config)
              
    def start(self):
        
        # Start master services
        self.master.start()
        
        _logger.info("Stating Kraken master client server.")
        
        self.mcserver = MasterClientServer(self.master)
        self.mcserver.configure(self.config)
        self.mcserver.setDaemon(True)
        self.mcserver.start()        
        _logger.info("Kraken master client server started, listening  at %s:%s", self.mcserver.listen_address, self.mcserver.listen_port)
        
        if (not self.standalone):
            _logger.info("Stating Kraken master worker server.")
            self.mwserver = MasterWorkerServer(self.master)
            self.mwserver.configure(self.config)
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