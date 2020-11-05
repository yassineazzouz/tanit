import abc
import logging as lg
import time
import json

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport

import six

from ..core.worker.worker import WorkerStats
from ...filesystem.model import FileSystem
from ...filesystem.model import FileSystemMounts
from ...common.model.worker import Worker
from ...thrift.master.service import MasterUserService
from ...thrift.master.service import MasterWorkerService
from ...thrift.common.model.ttypes import FileSystem as TFileSystem
from ...thrift.common.model.ttypes import Worker as TWorker

_logger = lg.getLogger(__name__)

WORKER_SERVICE_CLIENT_NAME = "worker-service"
USER_SERVICE_CLIENT_NAME = "user-service"


def connect(master_host, master_port):
    # Create Transport
    socket = TSocket.TSocket(master_host, master_port)
    transport = TTransport.TBufferedTransport(socket)

    # Connect to server
    retries = 1
    last_error = None
    while retries < 30:
        try:
            transport.open()
            break
        except TTransport.TTransportException as e:
            _logger.error(
                "Could not connect to the master server. " + "retrying in 5 seconds ..."
            )
            last_error = e
        retries += 1
        time.sleep(5.0)

    if retries == 30:
        _logger.error(
            "Could not connect to the master server after 30 retries. " + "exiting ..."
        )
        raise last_error

    return transport


@six.add_metaclass(abc.ABCMeta)
class UserServiceClientIFace(object):
    @abc.abstractmethod
    def start(self):
        raise NotImplementedError

    @abc.abstractmethod
    def list_workers(self):
        raise NotImplementedError

    @abc.abstractmethod
    def deactivate_worker(self, wid):
        raise NotImplementedError

    @abc.abstractmethod
    def activate_worker(self, wid):
        raise NotImplementedError

    @abc.abstractmethod
    def register_filesystem(self, filesystem):
        raise NotImplementedError

    @abc.abstractmethod
    def mount_filesystem(self, name, path):
        raise NotImplementedError

    @abc.abstractmethod
    def umount_filesystem(self, mount_point):
        raise NotImplementedError

    @abc.abstractmethod
    def stop(self):
        raise NotImplementedError


class ThriftUserServiceClient(UserServiceClientIFace):
    def __init__(self, master_host, master_port):
        self.master_host = master_host
        self.master_port = master_port

    def start(self):
        self.transport = connect(self.master_host, self.master_port)
        self.client = MasterUserService.Client(
            TBinaryProtocol.TBinaryProtocol(self.transport)
        )

    def list_workers(self):
        wkr_list = []
        for wkr in self.client.list_workers():
            wkr_list.append(Worker(wkr.wid, wkr.address, wkr.port))
        return wkr_list

    def deactivate_worker(self, wid):
        self.client.deactivate_worker(wid)

    def activate_worker(self, wid):
        self.client.activate_worker(wid)

    def worker_stats(self, wid):
        stats = self.client.worker_stats(wid)
        return WorkerStats(
            wid=stats.wid,
            state=stats.state,
            last_heartbeat=stats.last_heartbeat,
            running_tasks=stats.running_tasks,
            pending_tasks=stats.pending_tasks,
            available_cores=stats.available_cores,
        )

    def register_filesystem(self, filesystem):
        self.client.register_filesystem(
            TFileSystem(
                filesystem.name,
                filesystem.type,
                json.dumps(filesystem.parameters)
            )
        )

    def mount_filesystem(self, name, mount_point, mount_path=""):
        if mount_path is None:
            mount_path = ""
        self.client.mount_filesystem(name, mount_point, mount_path)

    def umount_filesystem(self, mount_point):
        self.client.umount_filesystem(mount_point)

    def list_filesystems(self):
        filesystems = []
        for filesystem_mount in self.client.list_filesystems():
            filesystems.append(
                FileSystemMounts(
                    FileSystem(
                        filesystem_mount.filesystem.name, filesystem_mount.filesystem.type, {}
                    ),
                    filesystem_mount.mounts
                )
            )
        return filesystems

    def stop(self):
        self.transport.close()


class LocalUserServiceClient(UserServiceClientIFace):
    def __init__(self, master):
        self.master = master

    def start(self):
        # do nothing
        return

    def list_workers(self):
        return self.master.list_workers()

    def deactivate_worker(self, wid):
        self.master.deactivate_worker(wid)

    def activate_worker(self, wid):
        self.master.activate_worker(wid)

    def worker_stats(self, wid):
        return self.master.get_worker_stats(wid)

    def register_filesystem(self, filesystem):
        self.master.register_filesystem(filesystem)

    def mount_filesystem(self, name, mount_point, mount_path=""):
        if mount_path is None:
            mount_path = ""
        self.master.mount_filesystem(name, mount_point, mount_path)

    def umount_filesystem(self, mount_point):
        self.master.umount_filesystem(mount_point)

    def list_filesystems(self):
        return self.master.list_filesystems()

    def stop(self):
        # do nothing
        return


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

    def __init__(self, master_host, master_port):
        self.master_host = master_host
        self.master_port = master_port

    def start(self):
        self.transport = connect(self.master_host, self.master_port)
        self.client = MasterWorkerService.Client(
            TBinaryProtocol.TBinaryProtocol(self.transport)
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


class ClientType:
    USER_SERVICE = 1
    WORKER_SERVICE = 2

    _VALUES_TO_NAMES = {
        1: "USER_SERVICE",
        2: "WORKER_SERVICE",
    }

    _NAMES_TO_VALUES = {
        "USER_SERVICE": 1,
        "WORKER_SERVICE": 2,
    }


class LocalClientFactory(object):
    def __init__(self, master):
        self.master = master

    def create_client(self, client_type):
        if client_type == ClientType.WORKER_SERVICE:
            return LocalWorkerServiceClient(self.master)

        elif client_type == ClientType.USER_SERVICE:
            return LocalUserServiceClient(self.master)
        else:
            raise NoSuchClientException(
                "No such client [ %s ]", ClientType._VALUES_TO_NAMES[client_type]
            )


class ThriftClientFactory(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def create_client(self, client_type):
        if client_type == ClientType.WORKER_SERVICE:
            return ThriftWorkerServiceClient(self.host, self.port)

        elif client_type == ClientType.USER_SERVICE:
            return ThriftUserServiceClient(self.host, self.port)
        else:
            raise NoSuchClientException(
                "No such client [ %s ]", ClientType._VALUES_TO_NAMES[client_type]
            )


class NoSuchClientException(Exception):
    pass
