#!/usr/bin/env python

port = 9090

import time

from ..thrift.KrakenService import Processor
from .handler import KrakenServiceHandler
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from threading import Thread

import logging as lg

_logger = lg.getLogger(__name__)

class KrakenMasterServer(object):

    def _run(self):
        # set handler to our implementation
        handler = KrakenServiceHandler()

        processor = Processor(handler)
        transport = TSocket.TServerSocket("0.0.0.0", port)
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        # set server
        server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

        server.serve()
            
    def start(self):
        _logger.info("Stating kraken master server.")
        self.daemon = Thread(target=self._run, args=())
        self.daemon.setDaemon(True)
        self.daemon.start()
        
        _logger.info("Started kraken master server at port %s", port)
        self.daemon.join()