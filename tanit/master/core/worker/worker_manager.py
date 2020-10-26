import logging as lg
from datetime import datetime
from threading import RLock

from .worker_monitor import WorkerMonitor

_logger = lg.getLogger(__name__)


class WorkerManager(object):
    """
    The WorkerManager monitor the state of workers and maintain the list of active/dead workers.
    It is not really part of the execution pipeline, its role is just to keep tack of which
    machines are active and fetch their state.
    """  # NOQA

    def __init__(self, workers_factory, config=None):
        self.worker_factory = workers_factory

        self.live_workers = []
        self.decommissioning_workers = []
        self.dead_workers = []

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
            for wkr in self.live_workers:
                wkr.stop()
        _logger.info("Tanit worker manager stopped.")

    def disable_monitor(self):
        self.monitor.heartbeat_check_interval = -1

    def get_live_worker(self, wid):
        with self.lock:
            for wkr in self.live_workers:
                if wkr.wid == wid:
                    return wkr
            return None

    def list_live_workers(self):
        with self.lock:
            return self.live_workers

    def list_decommissioning_workers(self):
        with self.lock:
            return self.decommissioning_workers

    def register_worker(self, worker):
        with self.lock:
            for wkr in self.live_workers:
                if wkr.wid == worker.wid:
                    _logger.error("Worker [ %s ] is already registered.", worker.wid)
                    raise AlreadyRegisteredWorkerException(
                        "Worker [ %s ] is already registered.", worker.wid
                    )
                elif (
                    wkr.address == worker.address
                    and wkr.port == worker.port
                    and wkr.address is not None
                    and worker.port is not None
                ):
                    _logger.error(
                        "Worker running on address [ %s ] and port [ %s ] "
                        + "is already registered.",
                        worker.address,
                        worker.port,
                    )
                    raise AlreadyRegisteredWorkerException(
                        "Worker running on address [ %s ] and port [ %s ] "
                        + "is already registered.",
                        worker.address,
                        worker.port,
                    )
                elif wkr.address == worker.address:
                    _logger.warn(
                        "Another Worker [ %s ] is already running on [ %s ].",
                        worker.wid,
                        worker.address,
                    )

            for wkr in self.decommissioning_workers:
                if wkr.wid == worker.wid:
                    _logger.error(
                        "Worker [ %s ] is in decommissioning state, "
                        + "it can not be registered.",
                        worker.wid,
                    )
                    raise AlreadyRegisteredWorkerException(
                        "Worker [ %s ] is in decommissioning state, "
                        + "it can not be registered.",
                        worker.wid,
                    )

            remote_worker = self.worker_factory.create_worker(worker)
            remote_worker.start()
            self.live_workers.append(remote_worker)

    def register_heartbeat(self, worker):
        with self.lock:
            remote_worker = self.get_live_worker(worker.wid)
            if remote_worker is not None:
                remote_worker.last_hear_beat = datetime.now()
            else:
                raise NoSuchWorkerException("No such worker [ %s ]", worker.wid)

    def decommission_worker(self, wid):
        _logger.info("Decommisionning Worker [ %s ]", wid)
        with self.lock:
            remote_worker = self.get_live_worker(wid)
            if remote_worker is not None:
                self.live_workers.remove(remote_worker)
                self.decommissioning_workers.append(remote_worker)
            else:
                raise NoSuchWorkerException("No such worker [ %s ]", wid)
        _logger.info("Worker [ %s ] Decommisionned", wid)

    def on_worker_decommissioned(self, wid):
        _logger.info("Deleting Worker [ %s ]", wid)
        with self.lock:
            for wkr in self.decommissioning_workers:
                if wkr.wid == wid:
                    wkr.stop()
                    self.decommissioning_workers.remove(wkr)
                    self.dead_workers.append(wkr)
                    break
        _logger.info("Worker [ %s ] Deleted", wid)


class IllegalWorkerStateException(Exception):
    pass


class NoSuchWorkerException(Exception):
    pass


class AlreadyRegisteredWorkerException(Exception):
    pass
