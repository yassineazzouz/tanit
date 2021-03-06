import logging as lg

from ...common.model.worker import Worker
from ..dfs.distributed_filesystem import DistributedFileSystem
from .execution.execution_manager import ExecutionManager
from .worker.worker_decommissioner import WorkerDecommissioner
from .worker.worker_factory import WorkerFactory
from .worker.worker_manager import WorkerManager

_logger = lg.getLogger(__name__)


class Master(object):
    """
    The master object represent the service interface of the master server.

    Its role is to implement the master API in a technology independent way,
    so the service layer is completely dissociated from the service layer
    and the RPC technology used.
    """

    def __init__(self):
        # workers factory
        self.workers_factory = WorkerFactory(self)
        # workers manager
        self.workers_manager = WorkerManager(self.workers_factory)
        # execution manager
        self.execution_manager = ExecutionManager(self.workers_manager)
        # filesystems factory
        self.dfs = DistributedFileSystem(self.execution_manager)

        # decommissioner
        self.decommissioner = WorkerDecommissioner(
            self.execution_manager, self.workers_manager
        )

        self.started = False

    def submit_job(self, job):
        if not self.started:
            raise MasterStoppedException("Can not submit job, master server stopped.")
        return self.execution_manager.submit_job(job)

    def list_jobs(self):
        return self.execution_manager.list_jobs()

    def get_job(self, jid):
        return self.execution_manager.get_job(jid)

    def task_start(self, tid):
        self.execution_manager.task_start(tid)

    def task_success(self, tid):
        self.execution_manager.task_finish(tid)

    def task_failure(self, tid):
        self.execution_manager.task_failure(tid)

    def get_worker_stats(self, wid):
        return self.workers_manager.get_worker(wid).stats()

    def list_workers(self, state=None):
        _logger.info("Listing Workers.")
        wkr_list = []
        for wkr in self.workers_manager.list_workers(state):
            wkr_list.append(Worker(wkr.wid, wkr.address, wkr.port))
        return wkr_list

    def register_worker(self, worker):
        if not self.started:
            raise MasterStoppedException(
                "Can not register worker [ %s ] : master server stopped.", worker.wid
            )

        # register the worker as an executor in the workers manager
        self.workers_manager.register_worker(worker)
        # activate the worker to start receiving tasks
        self.workers_manager.activate_worker(worker.wid)
        # notify the worker about all filesystems
        remote_worker = self.workers_manager.get_worker(worker.wid)
        for filesystem in self.dfs.list_filesystems():
            remote_worker.register_filesystem(filesystem)

    def register_heartbeat(self, worker):
        _logger.debug("Received heart beat from Worker [ %s ].", worker.wid)
        self.workers_manager.register_heartbeat(worker)

    def deactivate_worker(self, wid):
        if not self.started:
            raise MasterStoppedException(
                "Can not register worker [ %s ] : master server stopped.", wid
            )

        # This will prevent any future tasks from being sent to the worker
        self.workers_manager.deactivate_worker(wid)

    def activate_worker(self, wid):
        if not self.started:
            raise MasterStoppedException(
                "Can not register worker [ %s ] : master server stopped.", wid
            )

        self.workers_manager.activate_worker(wid)

    def register_filesystem(self, filesystem):
        if not self.started:
            raise MasterStoppedException(
                "Can not register filesystem : master server stopped."
            )

        # register the worker as a filesystem
        self.dfs.register_filesystem(filesystem)
        # notify the workers about the new file system
        for worker in self.workers_manager.list_active_workers():
            worker.register_filesystem(filesystem)

    def mount_filesystem(self, name, mount_point, mount_path=""):
        if not self.started:
            raise MasterStoppedException(
                "Can not register filesystem [ %s ] : master server stopped.", name
            )

        _logger.info("Mounting filesystem [ %s ] path [ %s ] under [ %s ]." % (name, mount_path, mount_point))
        # register the worker as a filesystem
        self.dfs.mount_filesystem(name, mount_point, mount_path)
        _logger.info("Filesystem [ %s ] mounted.", name)

    def umount_filesystem(self, mount_point):
        if not self.started:
            raise MasterStoppedException(
                "Can not umount filesystem at [ %s ] : master server stopped.", mount_point
            )

        _logger.info("Unmounting filesystem at [ %s ]." % mount_point)
        self.dfs.umount_filesystem(mount_point)
        _logger.info("Filesystem under [ %s ] mounted.", mount_point)

    def list_filesystems(self):
        if not self.started:
            raise MasterStoppedException(
                "Can not list filesystems : master server stopped."
            )
        return self.dfs.list_filesystems_mounts()

    def start(self):
        _logger.info("Stating Tanit master services.")
        self.started = True
        self.workers_manager.start()
        self.execution_manager.start()
        self.decommissioner.start()
        _logger.info("Tanit master services started.")

    def stop(self):
        _logger.info("Stopping Tanit master services.")
        self.started = False
        self.decommissioner.stop()
        self.decommissioner.join()
        self.execution_manager.stop()
        self.workers_manager.stop()
        _logger.info("Tanit master services stopped.")


class MasterStoppedException(Exception):
    """Raised when trying to submit a task to a stopped master."""

    pass
