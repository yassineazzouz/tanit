import json
from thrift.protocol import TBinaryProtocol

from thrift.transport import TSocket
from thrift.transport import TTransport

from ...common.model.worker import WorkerStatus
from ...thrift.worker.service import WorkerService
from ...thrift.common.model.ttypes import Task as TTask
from ...thrift.common.model.ttypes import FileSystem as TFileSystem
from ...common.config.configuration import TanitConfiguration
from ...common.config.configuration_keys import Keys
from ...common.thrift.utils import connect


class WorkerClient(object):
    def __init__(self, host=None, port=None):
        configuration = TanitConfiguration.getInstance()
        self.worker_host = host or configuration.get(Keys.WORKER_HOSTNAME)
        self.rpc_port = port or configuration.get(Keys.WORKER_RPC_PORT)
        self.rpc_max_retries = configuration.get(Keys.RPC_CLIENT_MAX_RETRIES)
        self.rpc_retry_interval = configuration.get(Keys.RPC_CLIENT_RETRY_INTERVAL)

        self.transport = TTransport.TBufferedTransport(
            TSocket.TSocket(self.worker_host, self.rpc_port)
        )
        self.client = WorkerService.Client(
            TBinaryProtocol.TBinaryProtocol(self.transport)
        )

    def start(self):
        self.transport = connect(
            self.transport,
            self.rpc_max_retries,
            self.rpc_retry_interval / 1000
        )

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
