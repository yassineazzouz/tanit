import json
import ast
import logging as lg

from ...common.model.worker import Worker
from ...filesystem.model import FileSystem
from ...thrift.common.model.ttypes import Worker as TWorker
from ...thrift.common.model.ttypes import WorkerStats as TWorkerStats
from ...thrift.common.model.ttypes import FileSystemMount as TFileSystemMount
from ...thrift.common.model.ttypes import FileSystem as TFileSystem

_logger = lg.getLogger(__name__)


class UserServiceHandler(object):
    def __init__(self, master):
        self.master = master

    def list_workers(self):
        workers = []
        for wkr in self.master.list_workers():
            workers.append(TWorker(wkr.wid, wkr.address, wkr.port))
        return workers

    def deactivate_worker(self, wid):
        self.master.deactivate_worker(wid)

    def activate_worker(self, wid):
        self.master.activate_worker(wid)

    def worker_stats(self, wid):
        stats = self.master.get_worker_stats(wid)
        return TWorkerStats(
            wid=stats.wid,
            state=stats.state,
            last_heartbeat=stats.last_heartbeat,
            running_tasks=stats.running_tasks,
            pending_tasks=stats.pending_tasks,
            available_cores=stats.available_cores,
        )

    def list_filesystems(self):
        filesystems = []
        for filesystem_mount in self.master.list_filesystems():
            filesystems.append(
                TFileSystemMount(
                    TFileSystem(
                        filesystem_mount.filesystem.name, filesystem_mount.filesystem.type
                    ),
                    filesystem_mount.mounts
                )
            )
        return filesystems

    def register_filesystem(self, filesystem):
        name = filesystem.name
        tpe = filesystem.type
        try:
            parameters = json.loads(filesystem.parameters)
            if type(parameters) is str:
                parameters = ast.literal_eval(parameters)
        except Exception as e:
            _logger.error("Error parsing filesystem json parameters specification.")
            raise e
        self.master.register_filesystem(FileSystem(name, tpe, parameters))

    def mount_filesystem(self, name, mount_point, mount_path=""):
        self.master.mount_filesystem(name, mount_point, mount_path)

    def umount_filesystem(self, mount_point):
        self.master.umount_filesystem(mount_point)


class WorkerServiceHandler(object):
    def __init__(self, master):
        self.master = master

    def register_heartbeat(self, worker):
        self.master.register_heartbeat(Worker(worker.wid, worker.address, worker.port))

    def register_worker(self, worker):
        self.master.register_worker(Worker(worker.wid, worker.address, worker.port))

    def unregister_worker(self, worker):
        self.master.deactivate_worker(worker.wid)

    def register_filesystem(self, filesystem):
        name = filesystem.name
        tpe = filesystem.type
        try:
            parameters = json.loads(filesystem.parameters)
            if type(parameters) is str:
                parameters = ast.literal_eval(parameters)
        except Exception as e:
            _logger.error("Error parsing filesystem json parameters specification.")
            raise e
        self.master.register_filesystem(FileSystem(name, tpe, parameters))

    def mount_filesystem(self, name, mount_point, mount_path=""):
        self.master.mount_filesystem(name, mount_point, mount_path)

    def task_start(self, tid):
        self.master.task_start(tid)

    def task_success(self, tid):
        self.master.task_success(tid)

    def task_failure(self, tid):
        self.master.task_failure(tid)

    def send_heartbeat(self, worker):
        pass


class JobNotFoundException(Exception):
    """Raised when trying to submit a task to a stopped master."""

    pass
