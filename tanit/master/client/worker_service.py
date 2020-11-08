import abc
import json
import logging as lg

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport

import six

from ...common.thrift.utils import connect
from ...common.model.worker import Worker
from ...thrift.master.service import MasterWorkerService
from ...thrift.common.model.ttypes import FileSystem as TFileSystem
from ...thrift.common.model.ttypes import Worker as TWorker

from ...common.config.configuration_keys import Keys
from ...common.config.configuration import TanitConfigurationException, TanitConfiguration

_logger = lg.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class WorkerServiceClientIFace(object):
    @abc.abstractmethod
    def start(self):
        raise NotImplementedError

    @abc.abstractmethod
    def register_worker(self, wid, address, port):
        raise NotImplementedError

    @abc.abstractmethod
    def unregister_worker(self, wid, address, port):
        raise NotImplementedError

    @abc.abstractmethod
    def register_heartbeat(self, wid, address, port):
        raise NotImplementedError

    @abc.abstractmethod
    def task_start(self, tid):
        raise NotImplementedError

    @abc.abstractmethod
    def task_success(self, tid):
        raise NotImplementedError

    @abc.abstractmethod
    def task_failure(self, tid):
        raise NotImplementedError

    @abc.abstractmethod
    def stop(self):
        raise NotImplementedError


class ThriftWorkerServiceClient(WorkerServiceClientIFace):
    """
    Worker service Thrift client.

    Used mainly by the master to to communicate with workers.
    """

    def __init__(self):
        self.configuration = TanitConfiguration.getInstance()

        master_host = self.configuration.get(Keys.MASTER_HOSTNAME)
        if master_host is None:
            raise TanitConfigurationException("Missing required configuration '%s'", Keys.MASTER_HOSTNAME)
        rpc_port = self.configuration.get(Keys.MASTER_RPC_SERVICE_PORT)
        if rpc_port is None:
            raise TanitConfigurationException("Missing required configuration '%s'", Keys.MASTER_RPC_SERVICE_PORT)

        self.transport = TTransport.TFramedTransport(
            TSocket.TSocket(master_host, rpc_port)
        )

        self.client = MasterWorkerService.Client(
            TBinaryProtocol.TBinaryProtocol(self.transport)
        )

    def start(self):
        connect(
            self.transport,
            self.configuration.get_int(Keys.RPC_CLIENT_MAX_RETRIES),
            self.configuration.get_int(Keys.RPC_CLIENT_RETRY_INTERVAL)/1000
        )

    def register_worker(self, wid, address, port):
        return self.client.register_worker(TWorker(wid, address, port))

    def unregister_worker(self, wid, address, port):
        return self.client.unregister_worker(TWorker(wid, address, port))

    def register_heartbeat(self, wid, address, port):
        self.client.register_heartbeat(TWorker(wid, address, port))

    def register_filesystem(self, filesystem):
        self.client.register_filesystem(
            TFileSystem(
                filesystem.name,
                filesystem.type,
                json.dumps(filesystem.parameters)
            )
        )

    def task_start(self, tid):
        self.client.task_start(tid)

    def task_success(self, tid):
        self.client.task_success(tid)

    def task_failure(self, tid):
        self.client.task_failure(tid)

    def stop(self):
        self.transport.close()


class LocalWorkerServiceClient(WorkerServiceClientIFace):
    """Used by local clients accessing the master worker services directly."""

    def __init__(self, master):
        self.master = master

    def start(self):
        # do nothing
        return

    def register_worker(self, wid, address, port):
        self.master.register_worker(Worker(self, wid, address, port))

    def unregister_worker(self, wid, address, port):
        self.master.unregister_worker(Worker(self, wid, address, port))

    def register_heartbeat(self, wid, address, port):
        self.master.register_heartbeat(Worker(self, wid, address, port))

    def register_filesystem(self, filesystem):
        self.master.register_filesystem(filesystem)

    def task_start(self, tid):
        self.master.task_start(tid)

    def task_success(self, tid):
        self.master.task_success(tid)

    def task_failure(self, tid):
        self.master.task_failure(tid)

    def stop(self):
        # do nothing
        return