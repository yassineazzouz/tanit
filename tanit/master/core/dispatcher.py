import abc
import logging as lg
import time
from threading import Thread

import six

_logger = lg.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class Dispatcher(object):
    """
    The Dispatcher distribute scheduled tasks between workers.

    Initially tasks are queued when submitted to the dispatcher
    then the dispatch thread continuously poll tasks and select
    the appropriate worker for execution.
    """

    def __init__(self, cqueue, workers_manager, callback=None):
        self.workers_manager = workers_manager
        self.callback = callback
        self.cqueue = cqueue
        self.stopped = False

    def _run(self):
        task_exec = None
        while True:
            if not self.cqueue.empty():
                worker = self.next_worker()
                if worker is None:
                    _logger.warn("Failed to dispatch tasks : no workers found !")
                    time.sleep(2)
                else:
                    task_exec = self.cqueue.get() if task_exec is None else task_exec
                    _logger.debug(
                        "Dispatching next task [ %s ] for execution.", task_exec.tid
                    )
                    if self.callback is not None:
                        self.callback(task_exec.tid, worker.wid)
                    try:
                        worker.submit(task_exec)
                        task_exec = None
                    except Exception:
                        _logger.exception(
                            "Exception submitting task [ %s ] to worker [ %s ]",  # NOQA
                            task_exec.tid,
                            worker.wid,
                        )

            else:
                _logger.debug("No new tasks to dispatch, sleeping for %s seconds...", 2)
                time.sleep(2)

            if self.stopped and self.cqueue.empty():
                _logger.debug(
                    "No new tasks to dispatch, terminating dispatcher thread."
                )
                return

    @abc.abstractmethod
    def next_worker(self):
        return

    def start(self):
        _logger.info("Stating tanit dispatcher.")
        self.daemon = Thread(target=self._run, args=())
        self.daemon.setDaemon(True)
        self.daemon.start()
        _logger.info("Tanit dispatcher started.")

    def stop(self):
        _logger.info("Stopping tanit dispatcher.")
        self.stopped = True
        self.daemon.join()
        _logger.info("tanit dispatcher Stopped.")


class FairDispatcher(Dispatcher):
    def next_worker(self):
        live_workers = self.workers_manager.list_live_workers()

        if len(live_workers) == 0:
            return None

        best_status = None
        best_worker = None

        for worker in live_workers:
            try:
                status = worker.status()
            except Exception:
                _logger.exception(
                    "Exception while fetching worker [ %s ] status", worker.wid
                )
                continue
            if best_status is None:
                best_status = status
                best_worker = worker
            elif status.pending < best_status.pending:
                best_worker = worker
                best_status = status
                continue
            elif status.pending == best_status.pending:
                if status.available > best_status.available:
                    best_worker = worker
                    best_status = status
                    continue
        return best_worker
