import logging as lg
import time
from threading import Thread

_logger = lg.getLogger(__name__)


class SimpleScheduler(object):
    """Manages Job objects and the waiting between job executions."""

    def __init__(self, lqueue, cqueue, callback=None):
        self.cqueue = cqueue
        self.lqueue = lqueue
        self.callback = callback
        self.stopped = False

    def schedule(self, task):
        self.lqueue.put(task)

    def _run(self):
        while True:
            if not self.lqueue.empty():
                task_exec = self.lqueue.get()
                _logger.debug("Scheduling next task %s for execution.", task_exec.tid)
                if self.callback is not None:
                    self.callback(task_exec.tid)
                self.cqueue.put(task_exec)
                self.lqueue.task_done()
            else:
                _logger.debug("No new tasks to schedule, sleeping for %s seconds...", 2)
                time.sleep(2)
            if self.stopped and self.lqueue.empty():
                _logger.debug("No new tasks to schedule, terminating scheduler thread.")
                return

    def start(self):
        _logger.info("Stating tanit scheduler.")
        self.daemon = Thread(target=self._run, args=())
        self.daemon.setDaemon(True)
        self.daemon.start()
        _logger.info("Tanit scheduler started.")

    def stop(self):
        _logger.info("Stopping tanit scheduler.")
        self.stopped = True
        self.daemon.join()
        _logger.info("Tanit scheduler stopped.")
