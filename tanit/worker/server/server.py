#!/usr/bin/env python

import logging as lg
from threading import Thread

from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket
from thrift.transport import TTransport

from ...thrift.worker.service import WorkerService
from ..config.config import WorkerConfig
from .handler import WorkerServiceHandler
from .worker import Worker

_logger = lg.getLogger(__name__)


class WorkerServer(object):
    def __init__(self, config=None):

        self.config = WorkerConfig(config)
        self.config.load()

        self.worker = Worker()
        self.worker.configure(self.config)

        self.stopped = False

    def stop(self):
        self.stopped = True

    def _run(self):
        # Create Service handler
        handler = WorkerServiceHandler(self.worker)

        server = TServer.TThreadedServer(
            WorkerService.Processor(handler),
            TSocket.TServerSocket(self.config.bind_address, self.config.bind_port),
            TTransport.TBufferedTransportFactory(),
            TBinaryProtocol.TBinaryProtocolFactory(),
            daemon=True,
        )

        # Start Tanit server
        server.serve()

    def start(self):

        self.stopped = False

        _logger.info("Stating Tanit worker server.")
        self.daemon = Thread(target=self._run, args=())
        self.daemon.setDaemon(True)
        self.daemon.start()
        _logger.info(
            "Tanit worker server started, listening  at %s:%s",
            self.config.bind_address,
            self.config.bind_port,
        )

        # Start worker services
        try:
            self.worker.start()
        except Exception:
            _logger.exception("Failed to start Tanit worker services.")
            exit(1)

        try:
            while self.daemon.is_alive():
                # Try to join the child thread back to parent for 0.5 seconds
                self.daemon.join(0.5)

                if self.stopped:
                    _logger.info("Tanit worker server stopped, exiting.")
                    break
        except (KeyboardInterrupt, SystemExit):
            _logger.info("Received KeyboardInterrupt Signal.")
        except Exception as e:
            _logger.exception("Fatal server exception : %s, exiting", e)
        finally:
            _logger.info("Stopping Tanit worker server.")
            self.worker.stop()
            _logger.info("Tanit worker server stopped.")
