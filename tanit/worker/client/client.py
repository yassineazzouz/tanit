import json
from thrift.protocol import TBinaryProtocol

from thrift.transport import TSocket
from thrift.transport import TTransport

from ...common.model.worker import WorkerStatus
from ...thrift.worker.service import WorkerService
from ...thrift.common.model.ttypes import Task as TTask
from ...thrift.common.model.ttypes import FileSystem as TFileSystem


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

    def submit(self, tid, operation, params):
        self.client.submit(TTask(tid, operation, params))

    def register_filesystem(self, filesystem):
        self.client.register_filesystem(
            TFileSystem(
                filesystem.name,
                filesystem.type,
                json.dumps(filesystem.parameters)
            )
        )

    def worker_status(self):
        status = self.client.worker_status()
        return WorkerStatus(
            status.wid, status.running, status.pending, status.available
        )

    def stop(self):
        self.transport.close()
