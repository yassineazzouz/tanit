import logging as lg

from ...common.model.worker import Worker
from ...filesystem.filesystem_factory import FileSystemFactory
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

    def __init__(self, config=None):
        # filesystems factory
        self.filesystems_factory = FileSystemFactory.getInstance()
        # workers factory
        self.workers_factory = WorkerFactory(self)
        # workers manager
        self.workers_manager = WorkerManager(self.workers_factory, config)
        # execution manager
        self.execution_manager = ExecutionManager(self.workers_manager, config)
        # decommissioner
        self.decommissioner = WorkerDecommissioner(
            self.execution_manager, self.workers_manager
        )

        self.configure(config)

        self.started = False

    def configure(self, config):
        pass

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

    def list_workers(self):
        _logger.info("Listing Workers.")
        wkr_list = []
        for wkr in self.workers_manager.list_live_workers():
            wkr_list.append(Worker(wkr.wid, wkr.address, wkr.port))
        return wkr_list

    def register_worker(self, worker):
        if not self.started:
            raise MasterStoppedException(
                "Can not register worker [ %s ] : master server stopped.", worker.wid
            )

        _logger.info("Registering new Worker [ %s ].", worker.wid)
        # register the worker as an executor in the workers manager
        self.workers_manager.register_worker(worker)
        _logger.info("Worker [ %s ] registered.", worker.wid)

    def register_filesystem(self, name, filesystem):
        if not self.started:
            raise MasterStoppedException(
                "Can not register filesystem [ %s ] : master server stopped.", name
            )

        _logger.info("Registering new filesystem [ %s ].", name)
        filesystem["name"] = name
        # register the worker as a filesystem
        self.filesystems_factory.register_filesystem(filesystem)
        # notify the workers about the new file system
        for worker in self.workers_manager.list_live_workers():
            worker.register_filesystem(name, filesystem)
        _logger.info("Filesystem [ %s ] registered.", name)

    def register_heartbeat(self, worker):
        _logger.debug("Received heart beat from Worker [ %s ].", worker.wid)
        self.workers_manager.register_heartbeat(worker)

    def unregister_worker(self, worker):
        if not self.started:
            raise MasterStoppedException(
                "Can not register worker [ %s ] : master server stopped.", worker.wid
            )

        # This will prevent any future tasks from being sent to the worker
        self.workers_manager.decommission_worker(worker.wid)

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
