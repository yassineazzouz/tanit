import logging as lg
from datetime import datetime
from threading import Lock

from .execution_state import ExecutionState

_logger = lg.getLogger(__name__)


class TaskExecution(object):
    """TaskExecution represent the execution flow of a task in the master."""

    def __init__(self, tid, operation, params, job):
        self.state = ExecutionState.SUBMITTED

        self.tid = tid
        self.operation = operation
        self.params = params
        self.job = job
        self.attempts = 1

    def on_schedule(self):
        if self.state not in [ExecutionState.SUBMITTED, ExecutionState.SCHEDULED]:
            raise IllegalStateTransitionException(
                "Can not transition from state %s to state %s",
                ExecutionState._VALUES_TO_NAMES[self.state],
                ExecutionState._VALUES_TO_NAMES[ExecutionState.SCHEDULED],
            )
        self.state = ExecutionState.SCHEDULED
        self.job.on_task_schedule(self.tid)

    def on_dispatch(self, worker=None):
        if self.state not in [ExecutionState.DISPATCHED, ExecutionState.SCHEDULED]:
            raise IllegalStateTransitionException(
                "Can not transition from state %s to state %s",
                ExecutionState._VALUES_TO_NAMES[self.state],
                ExecutionState._VALUES_TO_NAMES[ExecutionState.DISPATCHED],
            )
        self.state = ExecutionState.DISPATCHED
        self.worker = worker
        self.job.on_task_dispatch(self.tid)

    def on_start(self):
        if self.state not in [
            ExecutionState.RUNNING,
            ExecutionState.DISPATCHED,
            ExecutionState.FAILED,
        ]:  # NOQA
            raise IllegalStateTransitionException(
                "Can not transition from state %s to state %s",
                ExecutionState._VALUES_TO_NAMES[self.state],
                ExecutionState._VALUES_TO_NAMES[ExecutionState.RUNNING],
            )
        self.state = ExecutionState.RUNNING
        self.job.on_task_start(self.tid)

    def on_finish(self):
        if self.state not in [ExecutionState.RUNNING, ExecutionState.FINISHED]:
            raise IllegalStateTransitionException(
                "Can not transition from state %s to state %s",
                ExecutionState._VALUES_TO_NAMES[self.state],
                ExecutionState._VALUES_TO_NAMES[ExecutionState.RUNNING],
            )
        self.state = ExecutionState.FINISHED
        self.job.on_task_finish(self.tid)

    def on_fail(self):
        if self.state not in [ExecutionState.RUNNING, ExecutionState.FAILED]:
            raise IllegalStateTransitionException(
                "Can not transition from state %s to state %s",
                ExecutionState._VALUES_TO_NAMES[self.state],
                ExecutionState._VALUES_TO_NAMES[ExecutionState.RUNNING],
            )
        self.state = ExecutionState.FAILED
        self.job.on_task_fail(self.tid)

    def on_reset(self):
        self.job.on_task_reset(self.tid)

    def on_retry(self):
        self.attempts += 1
        self.job.on_task_reset(self.tid)

    def _reset(self):
        self.state = ExecutionState.SUBMITTED
        self.worker = None


class IllegalStateTransitionException(Exception):
    pass


class JobExecution(object):
    """
    JobExecution represent the execution flow of a job in the master.

    The job state diagram is as follow:
    ----------------------------------------------------------------------
    |   SUBMITTED --> SCHEDULED --> DISPATCHED --> RUNNING --> FINISHED  |
    |                                                |                   |
    |                                                |                   |
    |                                              FAILED                |
    ---------------------------------------------------------------------|
    """

    def __init__(self, jid):

        self.state = ExecutionState.SUBMITTED

        self.submission_time = datetime.now()
        self.schedule_time = None
        self.dispatch_time = None
        self.start_time = None
        self.finish_time = None
        self.execution_time_s = -1

        self.tasks = {}

        self.scheduled_tasks = 0
        self.dispatched_tasks = 0
        self.started_tasks = 0
        self.finished_tasks = 0
        self.failed_tasks = 0

        self.jlock = Lock()

        self.jid = jid

    def add_task(self, tid, operation, params):
        self.tasks[tid] = TaskExecution(tid, operation, params, self)

    def get_task(self, tid):
        if tid in self.tasks:
            return self.tasks[tid]
        else:
            return None

    def get_tasks(self):
        return list(self.tasks.values())

    def get_state(self):
        return self.state

    def on_task_schedule(self, tid):
        with self.jlock:
            self.scheduled_tasks += 1
            # Only the first task transition the state of the job
            if self.state == ExecutionState.SUBMITTED and self.scheduled_tasks == 1:
                _logger.info("Job [ %s ] execution scheduled.", self.jid)
                self.schedule_time = datetime.now()
                self.state = ExecutionState.SCHEDULED

    def on_task_dispatch(self, tid):
        with self.jlock:
            self.dispatched_tasks += 1
            # Only the first task transition the state of the job
            if self.state == ExecutionState.SCHEDULED and self.dispatched_tasks == 1:
                _logger.info("Job [ %s ] execution dispatched.", self.jid)
                self.dispatch_time = datetime.now()
                self.state = ExecutionState.DISPATCHED

    def on_task_start(self, tid):
        with self.jlock:
            self.started_tasks += 1
            # Only the first task transition the state of the job
            if self.state == ExecutionState.DISPATCHED and self.started_tasks == 1:
                _logger.info("Job [ %s ] execution started.", self.jid)
                self.start_time = datetime.now()
                self.state = ExecutionState.RUNNING

    def on_task_finish(self, tid):
        with self.jlock:
            self.finished_tasks += 1
            # All tasks need to finish (successfully)
            # to consider the job finished
            if self.finished_tasks == len(self.tasks):
                _logger.info("Job [ %s ] execution finished.", self.jid)
                self.state = ExecutionState.FINISHED
                self.finish_time = datetime.now()
                self.execution_time_s = int(
                    (self.finish_time - self.start_time).total_seconds()
                )

    def on_task_fail(self, tid):
        with self.jlock:
            self.failed_tasks += 1
            if self.state != ExecutionState.FAILED:
                self.state = ExecutionState.FAILED
                _logger.info("Job [ %s ] execution failed.", self.jid)

    def on_task_reset(self, tid):
        task = self.tasks[tid]
        with self.jlock:
            if task.state == ExecutionState.SCHEDULED:
                self.scheduled_tasks -= 1
            elif task.state == ExecutionState.DISPATCHED:
                self.scheduled_tasks -= 1
                self.dispatched_tasks -= 1
            elif task.state == ExecutionState.RUNNING:
                self.scheduled_tasks -= 1
                self.dispatched_tasks -= 1
                self.started_tasks -= 1
            elif task.state == ExecutionState.FINISHED:
                self.scheduled_tasks -= 1
                self.dispatched_tasks -= 1
                self.started_tasks -= 1
                self.finished_tasks -= 1
                if self.state == ExecutionState.FINISHED:
                    self.state = ExecutionState.RUNNING
            elif task.state == ExecutionState.FAILED:
                self.scheduled_tasks -= 1
                self.dispatched_tasks -= 1
                self.started_tasks -= 1
                if self.state == ExecutionState.FAILED and self.failed_tasks == 1:
                    self.state = ExecutionState.RUNNING
                self.failed_tasks -= 1

            task._reset()

    def is_finished(self):
        return self.state == ExecutionState.FINISHED

    def is_failed(self):
        return self.state == ExecutionState.FAILED