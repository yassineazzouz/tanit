
import logging as lg
from threading import Thread

from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket
from thrift.transport import TTransport

from .handler import LocalFileSystemHandler
from .thrift.LocalFilesystem import Processor

_logger = lg.getLogger(__name__)


class LocalFileSystemService(object):
    bind_address = "0.0.0.0"
    bind_port = 8989

    def _run(self):
        # Create Service handler
        handler = LocalFileSystemHandler()

        server = TServer.TThreadedServer(
            Processor(handler),
            TSocket.TServerSocket(self.bind_address, self.bind_port),
            TTransport.TBufferedTransportFactory(),
            TBinaryProtocol.TBinaryProtocolFactory(),
            daemon=True,
        )

        server.serve()

    def start(self):

        _logger.info("Stating Kraken local file system service.")
        self.daemon = Thread(target=self._run, args=())
        self.daemon.setDaemon(True)
        self.daemon.start()
        _logger.info(
            "Kraken local file system service started, listening  at %s:%s",
            self.bind_address,
            self.bind_port,
        )
