#!/usr/bin/env python

listen_port = 9090
lister_address = "0.0.0.0"

from .handler import KrakenServiceHandler
from .master import Master
from ..core.worker import Worker
from ..thrift.KrakenService import Processor
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

import socket
from threading import Thread

import logging as lg

_logger = lg.getLogger(__name__)

class KrakenMasterServer(object):     

    def _run(self):
        # Create the master service
        self.master = Master()
        
        # Create Service handler
        handler = KrakenServiceHandler(self.master)

        # Create Kraken server
        server = TServer.TThreadedServer(
            Processor(handler),
            TSocket.TServerSocket(lister_address, listen_port),
            TTransport.TBufferedTransportFactory(),
            TBinaryProtocol.TBinaryProtocolFactory()
        )

        # Start Kraken master
        self.master.start()
        
        # temporary
        self.master.register_worker(Worker( "worker-%s-1" % socket.gethostname()))
        
        # Start Kraken server
        server.serve()
              
    def start(self):
        
        _logger.info("Stating Kraken master server.")
        self.daemon = Thread(target=self._run, args=())
        self.daemon.setDaemon(True)
        self.daemon.start()
        
        _logger.info("Kraken server started, listening  at %s:%s", lister_address, listen_port)
        try:
            while self.daemon.is_alive():
                # Try to join the child thread back to parent for 0.5 seconds
                self.daemon.join(0.5)
        except (KeyboardInterrupt, SystemExit):
            _logger.info("Received KeyboardInterrupt Signal.")
        except Exception as e:
            _logger.exception("Fatal server exception : %s, exiting", e)
        finally:
            _logger.info("Stopping Kraken server.")
            self.master.stop()
            _logger.info("Kraken server stopped.")