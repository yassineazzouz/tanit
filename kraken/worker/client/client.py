from thrift.protocol import TBinaryProtocol

# Thrift files
from thrift.transport import TSocket
from thrift.transport import TTransport

from ...common.model.worker import WorkerStatus
from ...thrift.worker.service import WorkerService
from ...thrift.worker.service.ttypes import FileSystem
from ...thrift.worker.service.ttypes import Task


class WorkerClient(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        # Init thrift connection and protocol handlers
        socket = TSocket.TSocket(host, port)
        self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)

        # Set client to our Example
        self.client = WorkerService.Client(protocol)

    def start(self):
        # Connect to server
        self.transport.open()

    def submit(self, tid, etype, params):
        self.client.submit(Task(tid, etype, params))

    def register_filesystem(self, name, parameters):
        self.client.register_filesystem(FileSystem(name, parameters))

    def worker_status(self):
        status = self.client.worker_status()
        return WorkerStatus(
            status.wid, status.running, status.pending, status.available
        )

    def stop(self):
        self.transport.close()
