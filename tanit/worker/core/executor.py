import logging as lg
import time
from threading import Thread

from six.moves.queue import Empty

from ...master.client.client import ClientType

_logger = lg.getLogger(__name__)


class Executor(Thread):
    def __init__(self, eid, cqueue, factory):
        super(Executor, self).__init__()
        self.cqueue = cqueue
        self.eid = eid
        self.stopped = False
        self.idle = True

        """
        Apparently Thrift clients are not thread safe,
        so can not use without synchronization.
        synchronizing the client calls with locks will came with performance penalty
        the best approach seems to be, using a separate thrift client
        (connection) per thread.
        """
        self.master = factory.create_client(ClientType.WORKER_SERVICE)

        self.current_task = None

    def stop(self):
        self.stopped = True

    def run(self):
        # start the client
        self.master.start()
        self._run()
        self.master.stop()

    def _run(self):
        while True:
            self.idle = True
            try:
                task_exec = self.cqueue.get(timeout=0.5)
                self.idle = False
                self.master.task_start(str(task_exec.tid))
                self.current_task = task_exec

                task_exec.run()

                self.master.task_success(str(task_exec.tid))
                self.cqueue.task_done()
            except Empty:
                if self.stopped:
                    break
                time.sleep(0.5)
            except Exception as e:
                _logger.error("Error executing task [%s]", str(task_exec.tid))
                _logger.error(e, exc_info=True)
                self.master.task_failure(str(task_exec.tid))
                self.cqueue.task_done()

    def isIdle(self):
        return self.idle
