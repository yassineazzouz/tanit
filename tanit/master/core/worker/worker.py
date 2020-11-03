import abc
import logging as lg
from datetime import datetime

import six

from ....common.model.worker import WorkerStatus
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


class WorkerStats:
    def __init__(
        self, wid, state, last_heartbeat, running_tasks, pending_tasks, available_cores
    ):
        self.wid = wid
        self.state = state
        self.last_heartbeat = last_heartbeat
        self.running_tasks = running_tasks
        self.pending_tasks = pending_tasks
        self.available_cores = available_cores

    def __str__(self):
        return (
            "WorkerStats { "
            "id: %s, "
            "state: %s, "
            "last_heartbeat: %s, "
            "running_tasks: %s, "
            "pending_tasks: %s, "
            "available_cores: %s "
            "}"
            % (
                self.wid,
                self.state,
                self.last_heartbeat,
                self.running_tasks,
                self.pending_tasks,
                self.available_cores,
            )
        )


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
        """Return the status of this worker.

        This is used by the Dispatcher, to make scheduling decisions.
        Returns: an instance of
            `tanit.common.model.worker.WorkerStatus`
        """
        pass

    def stats(self):
        if self.state == WorkerState.DEAD:
            status = WorkerStatus(self.wid, 0, 0, 0)
        else:
            status = self.status()
        return WorkerStats(
            self.wid,
            WorkerState._VALUES_TO_NAMES[self.state],
            self.last_hear_beat.strftime("%d%m%Y%H%M%S"),
            status.running,
            status.pending,
            status.available,
        )


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
            self.client.submit(task_exec.tid, task_exec.operation, task_exec.params)
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
