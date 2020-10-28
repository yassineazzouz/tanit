import logging as lg
from datetime import datetime
from threading import RLock

from .worker import WorkerState
from .worker_monitor import WorkerMonitor

_logger = lg.getLogger(__name__)


class WorkerManager(object):
    """Monitor and Maintain workers states.

    The WorkerManager monitor the state of workers and maintain the list of active/dead workers.
    It is not really part of the execution pipeline, its role is just to keep tack of which
    machines are active and fetch their state.
    """  # NOQA

    def __init__(self, workers_factory, config=None):
        self.worker_factory = workers_factory
        self.workers = []

        self.lock = RLock()

        # monitor
        self.monitor = WorkerMonitor(self)

        self.configure(config)

    def configure(self, config):
        pass

    def start(self):
        _logger.info("Stating tanit worker manager.")
        self.monitor.start()
        _logger.info("Tanit worker manager started.")

    def stop(self):
        _logger.info("Stopping tanit worker manager.")
        self.monitor.stop()
        with self.lock:
            for wkr in self.list_workers():
                wkr.stop()
        _logger.info("Tanit worker manager stopped.")

    def disable_monitor(self):
        self.monitor.heartbeat_check_interval = -1

    def get_worker(self, wid):
        with self.lock:
            for wkr in self.workers:
                if wkr.wid == wid:
                    return wkr
            return None

    def list_workers(self, state=None):
        with self.lock:
            if state is None:
                return self.workers
            else:
                return [wkr for wkr in self.list_workers() if wkr.state == state]

    def list_active_workers(self):
        return self.list_workers(state=WorkerState.ACTIVE)

    def register_worker(self, worker):
        """Register a Worker.

        Register the worker in the worker manager.
        Note: This does not activate the worker.
        """
        _logger.info("Registering new Worker [ %s ].", worker.wid)
        with self.lock:
            remote_worker = self.get_worker(worker.wid)
            if remote_worker is not None:
                if remote_worker.wid == worker.wid:
                    _logger.warning("Worker [ %s ] is already registered.", worker.wid)
                elif (
                    remote_worker.address == worker.address
                    and remote_worker.port == worker.port
                    and worker.address is not None
                    and worker.port is not None
                ):
                    _logger.warning(
                        "Worker running on address [ %s ] and port [ %s ] "
                        + "is already registered.",
                        worker.address,
                        worker.port,
                    )
                elif remote_worker.address == worker.address:
                    _logger.warning(
                        "Another Worker [ %s ] is already running on [ %s ].",
                        worker.wid,
                        worker.address,
                    )
            else:
                remote_worker = self.worker_factory.create_worker(worker)
                remote_worker.start()
                self.workers.append(remote_worker)
        _logger.info("Worker [ %s ] registered.", worker.wid)

    def activate_worker(self, wid):
        """Activate a Worker.

        Transition worker to state `ACTIVE`.
        Worker in any state except `DEACTIVATING` can be activated
        """
        _logger.info("Activating Worker [ %s ]", wid)
        with self.lock:
            remote_worker = self.get_worker(wid)
            if remote_worker is not None:
                if remote_worker.state == WorkerState.DEACTIVATING:
                    _logger.error("Cannot activate worker in decommissioning state.")
                    raise IllegalWorkerStateException(
                        "Cannot activate worker in decommissioning state."
                    )
                elif remote_worker.state == WorkerState.DEAD:
                    _logger.warning(
                        "Worker %s is in %s state, forcing activation"
                        % (
                            remote_worker.wid,
                            WorkerState._VALUES_TO_NAMES[WorkerState.DEAD],
                        )
                    )
                _logger.info(
                    "Transitioning worker from state %s to %s"
                    % (
                        WorkerState._VALUES_TO_NAMES[remote_worker.state],
                        WorkerState._VALUES_TO_NAMES[WorkerState.ACTIVE],
                    )
                )
                remote_worker.state = WorkerState.ACTIVE
            else:
                raise NoSuchWorkerException("No such worker [ %s ]", wid)
        _logger.info("Worker [ %s ] Activated", wid)

    def deactivate_worker(self, wid):
        """Deactivate a Worker.

        Transition worker to state `DEACTIVATING`.
        This does not really decommission the worker but rather schedule it
        for decommissioning by the `WorkerDecommissioner`.
        Only workers in state `ACTIVE` transition to `DEACTIVATING` state.
        """
        _logger.info("Deactivating Worker [ %s ]", wid)
        with self.lock:
            remote_worker = self.get_worker(wid)
            if remote_worker is not None:
                if remote_worker.state == WorkerState.ACTIVE:
                    remote_worker.state = WorkerState.DEACTIVATING
                elif remote_worker.state in [
                    WorkerState.DEACTIVATING,
                    WorkerState.DEACTIVATED,
                ]:
                    _logger.info("Worker [ %s ] already decommissioned.", wid)
                elif remote_worker.state == WorkerState.ALIVE:
                    _logger.info(
                        "Worker %s in state %s , forcing state %s"
                        % (
                            remote_worker.wid,
                            WorkerState._VALUES_TO_NAMES[WorkerState.ALIVE],
                            WorkerState._VALUES_TO_NAMES[WorkerState.DEACTIVATED],
                        )
                    )
                    remote_worker.state = WorkerState.DEACTIVATED
                else:
                    _logger.error(
                        "Cannot transition worker %s from state %s to %s"
                        % (
                            remote_worker.wid,
                            WorkerState._VALUES_TO_NAMES[remote_worker.state],
                            WorkerState._VALUES_TO_NAMES[WorkerState.DEACTIVATING],
                        )
                    )
                    raise IllegalWorkerStateException(
                        "Cannot transition worker %s from state %s to %s"
                        % (
                            remote_worker.wid,
                            WorkerState._VALUES_TO_NAMES[remote_worker.state],
                            WorkerState._VALUES_TO_NAMES[WorkerState.DEACTIVATING],
                        )
                    )
            else:
                raise NoSuchWorkerException("No such worker [ %s ]", wid)
        _logger.info("Worker [ %s ] deactivated", wid)

    def register_heartbeat(self, worker):
        with self.lock:
            remote_worker = self.get_worker(worker.wid)
            if remote_worker is not None:
                remote_worker.last_hear_beat = datetime.now()
                if remote_worker.state == WorkerState.DEAD:
                    remote_worker.state = WorkerState.ALIVE
            else:
                # register the worker without activating
                _logger.info(
                    "Received heartbeat from unknown worker %s, registering it."
                    % worker.wid
                )
                self.register_worker(worker)


class IllegalWorkerStateException(Exception):
    pass


class NoSuchWorkerException(Exception):
    pass


class AlreadyRegisteredWorkerException(Exception):
    pass
