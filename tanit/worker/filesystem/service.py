import logging as lg
from threading import Thread

from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket
from thrift.transport import TTransport

from ...thrift.worker.filesystem import LocalFilesystem
from .handler import LocalFileSystemHandler

_logger = lg.getLogger(__name__)


class LocalFileSystemService(object):
    def __init__(self, address="0.0.0.0", port=8989):
        self.bind_address = address
        self.bind_port = port

    def _run(self):
        # Create Service handler
        handler = LocalFileSystemHandler()

        server = TServer.TThreadedServer(
            LocalFilesystem.Processor(handler),
            TSocket.TServerSocket(self.bind_address, self.bind_port),
            TTransport.TBufferedTransportFactory(),
            TBinaryProtocol.TBinaryProtocolFactory(),
            daemon=True,
        )

        server.serve()

    def start(self):

        _logger.info("Stating Tanit local file system service.")
        self.daemon = Thread(target=self._run, args=())
        self.daemon.setDaemon(True)
        self.daemon.start()
        _logger.info(
            "Tanit local file system service started, listening  at %s:%s",
            self.bind_address,
            self.bind_port,
        )
