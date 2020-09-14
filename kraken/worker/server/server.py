#!/usr/bin/env python

from .handler import WorkerServiceHandler
from .worker import Worker
from ..thrift import WorkerService
from ..config.config import WorkerConfig
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from threading import Thread

import logging as lg

_logger = lg.getLogger(__name__)

class WorkerServer(object):
    
    def __init__(self):
        
        self.config = WorkerConfig()
        self.config.load()
        
        self.worker = Worker()
        self.worker.configure(self.config)
        

    def _run(self):
        # Create Service handler
        handler = WorkerServiceHandler(self.worker)

        server = TServer.TThreadedServer(
            WorkerService.Processor(handler),
            TSocket.TServerSocket(self.config.bind_address, self.config.bind_port),
            TTransport.TBufferedTransportFactory(),
            TBinaryProtocol.TBinaryProtocolFactory(),
            daemon=True
        )
        
        # Start Kraken server
        server.serve()
              
    def start(self):
        
        _logger.info("Stating Kraken worker server.")
        self.daemon = Thread(target=self._run, args=())
        self.daemon.setDaemon(True)
        self.daemon.start()     
        _logger.info("Kraken worker server started, listening  at %s:%s", self.config.bind_address, self.config.bind_port)
        
        # Start worker services
        try:
            self.worker.start()
        except Exception as e:
            _logger.exception("Failed to start Kraken worker services.")
            exit(1)
        
        try:
            while self.daemon.is_alive():
                # Try to join the child thread back to parent for 0.5 seconds
                self.daemon.join(0.5)
        except (KeyboardInterrupt, SystemExit):
            _logger.info("Received KeyboardInterrupt Signal.")
        except Exception as e:
            _logger.exception("Fatal server exception : %s, exiting", e)
        finally:
            _logger.info("Stopping Kraken worker server.")
            self.worker.stop()
            _logger.info("Kraken worker server stopped.") 