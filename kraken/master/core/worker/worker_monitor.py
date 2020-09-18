
from threading import Thread
from datetime import datetime

import logging as lg
_logger = lg.getLogger(__name__)

class WorkerMonitor(Thread):
    
    # in seconds
    heartbeat_check_interval = 30
    
    def __init__(self, worker_manager):
        super(WorkerMonitor, self).__init__()
        self.worker_manager = worker_manager
        self.stopped = False
        
    def stop(self):
        self.stopped = True

    def run(self):
        while not self.stopped:
            for worker in self.worker_manager.list_live_workers():
                if (datetime.now() - worker.last_hear_beat).total_seconds() > self.heartbeat_check_interval:
                    _logger.warn("Missing worker hearbeat from [ %s ] after %s seconds", worker.wid, self.heartbeat_check_interval)
                    self.worker_manager.decommission_worker(worker.wid)
