import logging as lg
from datetime import datetime
from threading import Thread

_logger = lg.getLogger(__name__)


class WorkerMonitor(Thread):
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
        while not self.stopped:
            for worker in self.worker_manager.list_live_workers():
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
                            self.worker_manager.decommission_worker(worker.wid)
