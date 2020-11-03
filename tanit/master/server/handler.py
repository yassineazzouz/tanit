import logging as lg

from ...common.model.worker import Worker
from ...thrift.master.service import ttypes

_logger = lg.getLogger(__name__)


class UserServiceHandler(object):
    def __init__(self, master):
        self.master = master

    def list_workers(self):
        workers = []
        for wkr in self.master.list_workers():
            workers.append(ttypes.Worker(wkr.wid, wkr.address, wkr.port))
        return workers

    def deactivate_worker(self, wid):
        self.master.deactivate_worker(wid)

    def activate_worker(self, wid):
        self.master.activate_worker(wid)

    def worker_stats(self, wid):
        stats = self.master.get_worker_stats(wid)
        return ttypes.WorkerStats(
            wid=stats.wid,
            state=stats.state,
            last_heartbeat=stats.last_heartbeat,
            running_tasks=stats.running_tasks,
            pending_tasks=stats.pending_tasks,
            available_cores=stats.available_cores,
        )

    def register_filesystem(self, filesystem):
        self.master.register_filesystem(filesystem.name, filesystem.parameters)

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
        self.master.register_filesystem(filesystem.name, filesystem.parameters)

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
