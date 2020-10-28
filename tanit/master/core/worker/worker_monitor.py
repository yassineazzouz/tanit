import logging as lg
from datetime import datetime
from threading import Thread

from .worker import WorkerState

_logger = lg.getLogger(__name__)


class WorkerMonitor(Thread):
    """Monitor workers heartbeats.

    The WorkerMonitor monitor for workers heartbeats and mark workers for
    decomissinning if the last hearbeat is older than `heartbeat_check_interval`.
    Note that only workers in `ACTIVE` state transition to `DEACTIVATING`,
    The stale workers state are transitionned as follow :
    `ACTIVE` -> `DEACTIVATING`
    `DEACTIVATING` -> `DEACTIVATING`
    `ALIVE`, `DEACTIVATED` -> `DEAD`
    """  # NOQA

    # in seconds
    heartbeat_check_interval = 30

    def __init__(self, worker_manager):
        super(WorkerMonitor, self).__init__()
        self.worker_manager = worker_manager
        self.stopped = False
        self.setDaemon(True)

    def stop(self):
        self.stopped = True

    def run(self):
        def _is_worker_alive(worker):
            if self.heartbeat_check_interval > 0:
                # only remote workers are monitored
                if worker.address is not None and worker.port is not None:
                    elapsed_time = datetime.now() - worker.last_hear_beat
                    if elapsed_time.total_seconds() > self.heartbeat_check_interval:
                        _logger.warning(
                            "Missing worker heartbeat from [ %s ] after %s seconds",  # NOQA
                            worker.wid,
                            self.heartbeat_check_interval,
                        )
                        return False
                    else:
                        return True
                else:
                    # worker is not remote, got to be alive
                    return True
            else:
                # no heartbeat check
                return True

        while not self.stopped:
            for worker in self.worker_manager.list_workers():
                if worker.state == WorkerState.DEAD:
                    pass
                elif worker.state == WorkerState.DEACTIVATING:
                    # nothing to do, the worker is already being decommissioned
                    pass
                else:
                    if not _is_worker_alive(worker):
                        if worker.state == WorkerState.ACTIVE:
                            _logger.info("Deactivating worker %s" % worker.wid)
                            worker.state = WorkerState.DEACTIVATING
                        elif worker.state in [
                            WorkerState.ALIVE,
                            WorkerState.DEACTIVATED,
                        ]:
                            _logger.info("Marking worker %s as DEAD." % worker.wid)
                            worker.state = WorkerState.DEAD
