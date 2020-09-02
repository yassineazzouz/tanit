#!/usr/bin/env python

import socket
from .handler import WorkerServiceHandler
from .worker import Worker
from ..thrift import WorkerService
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from threading import Thread

import logging as lg

_logger = lg.getLogger(__name__)

class WorkerServer(object):
    
    def __init__(self, listen_address = "0.0.0.0", listen_port = 9093, n_threads = 10):
        super(WorkerServer, self).__init__()
        
        self.listen_address = listen_address
        self.listen_port = listen_port
        self.n_threads = n_threads
        
        self.worker = Worker(socket.gethostname(), listen_port)
        

    def _run(self):
        # Create Service handler
        handler = WorkerServiceHandler(self.worker)

        server = TServer.TThreadedServer(
            WorkerService.Processor(handler),
            TSocket.TServerSocket(self.listen_address, self.listen_port),
            TTransport.TBufferedTransportFactory(),
            TBinaryProtocol.TBinaryProtocolFactory()
        )
        
        # Start Kraken server
        server.serve()
              
    def start(self):
        
        _logger.info("Stating Kraken worker server.")
        self.daemon = Thread(target=self._run, args=())
        self.daemon.setDaemon(True)
        self.daemon.start()     
        _logger.info("Kraken worker server started, listening  at %s:%s", self.listen_address, self.listen_port)
        
        # Start worker services
        self.worker.start()
        
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
            _logger.info("Kraken worker server stopped.") 