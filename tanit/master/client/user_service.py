import abc
import logging as lg
import json

from thrift.protocol import TBinaryProtocol
from thrift.protocol.TMultiplexedProtocol import TMultiplexedProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport

import six

from ..core.worker.worker import WorkerStats
from ...common.thrift.utils import connect
from ...filesystem.model import FileSystem
from ...filesystem.model import FileSystemMounts
from ...common.model.worker import Worker
from ...thrift.master.service import MasterUserService
from ...thrift.common.model.ttypes import FileSystem as TFileSystem
from ...common.config.configuration_keys import Keys
from ...common.config.configuration import TanitConfigurationException, TanitConfiguration

_logger = lg.getLogger(__name__)


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
    def __init__(self):
        self.configuration = TanitConfiguration.getInstance()
        master_host = self.configuration.get(Keys.MASTER_HOSTNAME)
        if master_host is None:
            raise TanitConfigurationException("Missing required configuration '%s'", Keys.MASTER_HOSTNAME)

        rpc_port = self.configuration.get_int(Keys.MASTER_RPC_PORT)
        if rpc_port is None:
            raise TanitConfigurationException("Missing required configuration '%s'", Keys.MASTER_RPC_PORT)

        self.transport = TTransport.TFramedTransport(
                TSocket.TSocket(master_host, rpc_port)
            )
        self.client = MasterUserService.Client(
            TMultiplexedProtocol(
                TBinaryProtocol.TBinaryProtocol(self.transport),
                "UserService"
            )
        )

    def start(self):
        connect(
            self.transport,
            self.configuration.get(Keys.RPC_CLIENT_MAX_RETRIES),
            self.configuration.get(Keys.RPC_CLIENT_RETRY_INTERVAL)/1000
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
    def __init__(self, master, configuration=None):
        self.master = master

    def configure(self, configuration):
        pass

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
