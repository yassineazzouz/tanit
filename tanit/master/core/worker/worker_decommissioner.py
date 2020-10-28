import logging as lg
import time
from threading import Thread

from ..execution.execution_state import ExecutionState
from .worker import WorkerState

_logger = lg.getLogger(__name__)


class WorkerDecommissioner(Thread):
    def __init__(self, execution_manager, workers_manager):
        super(WorkerDecommissioner, self).__init__()
        self.execution_manager = execution_manager
        self.workers_manager = workers_manager
        self.stopped = False

    def run(self):
        while not self.stopped:
            for worker in self.workers_manager.list_workers():
                if worker.state == WorkerState.DEACTIVATING:
                    _logger.info(
                        "Decommissioning worker %s in state DEACTIVATING" % worker.wid
                    )
                    self._decommission_worker(worker)
                    worker.state = WorkerState.DEACTIVATED
                    _logger.info(
                        "Worker %s decommissioned, and marked as DEACTIVATED"
                        % worker.wid
                    )

    def stop(self):
        self.stopped = True

    def _decommission_worker(self, worker):
        worker_tasks = self.execution_manager.get_tasks(worker=worker.wid)

        while not self.stopped:
            # While the worker is up, wait for the tasks to finish
            # check if the worker is still alive
            try:
                worker.status()
            except Exception:
                break
            # check how many tasks are still alive
            worker_tasks = [
                task
                for task in worker_tasks
                if task.state not in [ExecutionState.FINISHED, ExecutionState.FAILED]
            ]
            if len(worker_tasks) == 0:
                break
            else:
                time.sleep(1.0)

        # check if all tasks actually
        if len(worker_tasks) == 0:
            _logger.info("Worker [ %s ] have no more active tasks.", worker.wid)
        else:
            _logger.info(
                "Worker [ %s ] still have %s active tasks.",
                worker.wid,
                len(worker_tasks),
            )
            # reschedule the tasks
            for task_exec in worker_tasks:
                self.execution_manager.task_reset(task_exec.tid)
