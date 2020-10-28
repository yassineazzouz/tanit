import abc
import logging as lg
from datetime import datetime

import six

from ....worker.client.client import WorkerClient

_logger = lg.getLogger(__name__)


class WorkerState:
    ALIVE = 1
    ACTIVE = 2
    DEACTIVATING = 3
    DEACTIVATED = 4
    DEAD = 5

    _VALUES_TO_NAMES = {
        1: "ALIVE",
        2: "ACTIVE",
        3: "DEACTIVATING",
        4: "DEACTIVATED",
        5: "DEAD",
    }

    _NAMES_TO_VALUES = {
        "ALIVE": 1,
        "ACTIVE": 2,
        "DEACTIVATING": 3,
        "DEACTIVATED": 4,
        "DEAD": 5,
    }


@six.add_metaclass(abc.ABCMeta)
class WorkerIFace(object):
    def __init__(self, wid, address, port):
        self.wid = wid
        self.address = address
        self.port = port
        self.last_hear_beat = datetime.now()

        self.stopped = True

        self.state = WorkerState.ALIVE

    def start(self):
        self.stopped = False

    def stop(self):
        self.stopped = True

    def activate(self):
        self.state = WorkerState.ACTIVE

    def decommissioning(self):
        self.state = WorkerState.DEACTIVATING

    def decommissioned(self):
        self.state = WorkerState.DEACTIVATED

    def dead(self):
        self.state = WorkerState.DEAD

    @abc.abstractmethod
    def submit(self, task):
        pass

    @abc.abstractmethod
    def register_filesystem(self, name, filesystem):
        pass

    @abc.abstractmethod
    def status(self):
        pass


class RemoteThriftWorker(WorkerIFace):
    def __init__(self, wid, address, port):
        super(RemoteThriftWorker, self).__init__(wid, address, port)
        self.client = WorkerClient(address, port)

    def start(self):
        super(RemoteThriftWorker, self).start()
        self.client.start()

    def stop(self):
        super(RemoteThriftWorker, self).stop()
        self.client.stop()

    def submit(self, task_exec):
        if not self.stopped:
            self.client.submit(task_exec.tid, task_exec.etype, task_exec.params)
        else:
            raise WorkerStoppedException(
                "Can not submit task [ %s ] to [ %s ] : worker stopped.",
                task_exec.tid,
                self.wid,
            )

    def register_filesystem(self, name, filesystem):
        if not self.stopped:
            self.client.register_filesystem(name, filesystem)
        else:
            raise WorkerStoppedException(
                "Can not register filesystem [ %s ] : worker stopped." % name
            )

    def status(self):
        return self.client.worker_status()


class WorkerStoppedException(Exception):
    """Raised when trying to submit a task to a stopped worker."""

    pass
