import logging as lg

from ...common.model.execution_type import ExecutionType
from ...common.model.job import Job
from ...common.model.worker import Worker
from ...thrift.master.service import ttypes
from ..core.execution.execution_state import ExecutionState

_logger = lg.getLogger(__name__)


class UserServiceHandler(object):
    def __init__(self, master):
        self.master = master

    def submit_job(self, job):
        if job.type == ttypes.JobType.COPY:
            etype = ExecutionType.COPY
        elif job.type == ttypes.JobType.UPLOAD:
            etype = ExecutionType.UPLOAD
        elif job.type == ttypes.JobType.MOCK:
            etype = ExecutionType.MOCK
        else:
            # should raise exception here
            pass

        try:
            job_exec = self.master.submit_job(Job(etype, job.params))
        except Exception as e:
            raise ttypes.JobInitializationException(str(e))
        return job_exec.jid

    def list_jobs(self):
        status = []
        for job_exec in self.master.list_jobs():
            status.append(
                ttypes.JobStatus(
                    job_exec.jid,
                    ttypes.JobState._NAMES_TO_VALUES[
                        ExecutionState._VALUES_TO_NAMES[job_exec.state]
                    ],
                    job_exec.submission_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "-"
                    if job_exec.start_time is None
                    else job_exec.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "-"
                    if job_exec.finish_time is None
                    else job_exec.finish_time.strftime("%Y-%m-%d %H:%M:%S"),
                    job_exec.execution_time_s,
                )
            )
        return status

    def job_status(self, jid):
        job_exec = self.master.get_job(jid)
        if job_exec is None:
            raise ttypes.JobNotFoundException("No such job [ %s ]" % jid)
        return ttypes.JobStatus(
            job_exec.jid,
            ttypes.JobState._NAMES_TO_VALUES[
                ExecutionState._VALUES_TO_NAMES[job_exec.state]
            ],
            job_exec.submission_time.strftime("%Y-%m-%d %H:%M:%S"),
            "-"
            if job_exec.start_time is None
            else job_exec.start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "-"
            if job_exec.finish_time is None
            else job_exec.finish_time.strftime("%Y-%m-%d %H:%M:%S"),
            job_exec.execution_time_s,
        )


class WorkerServiceHandler(object):
    def __init__(self, master):
        self.master = master

    def list_workers(self):
        workers = []
        for wkr in self.master.list_workers():
            workers.append(ttypes.Worker(wkr.wid, wkr.address, wkr.port))
        return workers

    def register_heartbeat(self, worker):
        self.master.register_heartbeat(Worker(worker.wid, worker.address, worker.port))

    def register_worker(self, worker):
        self.master.register_worker(Worker(worker.wid, worker.address, worker.port))

    def unregister_worker(self, worker):
        self.master.unregister_worker(Worker(worker.wid, worker.address, worker.port))

    def register_filesystem(self, filesystem):
        self.master.register_filesystem(filesystem.name, filesystem.parameters)

    def task_start(self, tid):
        self.master.task_start(tid)

    def task_success(self, tid):
        self.master.task_success(tid)

    def task_failure(self, tid):
        self.master.task_failure(tid)

    def send_heartbeat(self, worker):
        pass


class JobNotFoundException(Exception):
    """Raised when trying to submit a task to a stopped master."""

    pass
