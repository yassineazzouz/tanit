import logging as lg

from six.moves.queue import Queue

from ..dispatcher import FairDispatcher
from ..scheduler import SimpleScheduler
from .job_factory import JobFactory

_logger = lg.getLogger(__name__)


class ExecutionManager(object):
    """
    The ExecutionManager monitor the state of execution of jobs and tasks.

    It interacts with the different components involved in
    the execution pipeline and ensure the execution state is properly updated
    and reflect the real progress.
    """

    def __init__(self, workers_manager, config=None):
        # jobs list
        self.jobs = []
        # job factory
        self.jobs_factory = JobFactory()
        # Lister queue
        lqueue = Queue()
        # Call queue
        cqueue = Queue()
        # scheduler
        self.scheduler = SimpleScheduler(lqueue, cqueue, self.task_schedule)
        # dispatcher
        self.dispatcher = FairDispatcher(cqueue, workers_manager, self.task_dispatch)

        self.configure(config)

    def configure(self, config):
        # should be configurable
        self.max_task_retries = 3

    def start(self):
        _logger.info("Stating Tanit master services.")
        self.dispatcher.start()
        self.scheduler.start()
        _logger.info("Tanit master services started.")

    def stop(self):
        _logger.info("Stopping Tanit master services.")
        self.scheduler.stop()
        self.dispatcher.stop()
        _logger.info("Tanit master services stopped.")

    def submit_job(self, job):
        job_exec = self.jobs_factory.create_job(job)
        job_exec.setup()

        _logger.info("Submitting job [ %s ] for execution.", job_exec.jid)
        self.jobs.append(job_exec)
        for task_exec in self.get_tasks(jid=job_exec.jid):
            self.scheduler.schedule(task_exec)

        _logger.info(
            "Submitted %s tasks for execution in job [ %s ].",
            len(self.get_tasks(jid=job_exec.jid)),
            job_exec.jid,
        )
        return job_exec

    def cancel_job(self, conf):
        pass

    def list_jobs(self):
        return self.jobs

    def get_job(self, jid):
        for job_exec in self.jobs:
            if job_exec.jid == jid:
                return job_exec
        return None

    def get_tasks(self, jid=None, states=[], worker=None):
        target_jobs = []
        if jid is not None:
            job = self.get_job(jid)
            if job is not None:
                target_jobs.append(job)
            else:
                return []
        else:
            target_jobs = self.jobs

        target_tasks = []
        for job in target_jobs:
            for task in job.get_tasks():
                valid = True
                if len(states) != 0:
                    if task.state not in states:
                        valid = False
                if worker is not None:
                    if task.worker != worker:
                        valid = False
                if valid:
                    target_tasks.append(task)
        return target_tasks

    def get_task(self, tid):
        for job_exec in self.jobs:
            task = job_exec.get_task(tid)
            if task is not None:
                return task
        return None

    def task_schedule(self, tid):
        task = self.get_task(tid)
        if task is not None:
            task.on_schedule()
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)

    def task_dispatch(self, tid, worker=None):
        task = self.get_task(tid)
        if task is not None:
            task.on_dispatch(worker)
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)

    def task_start(self, tid):
        task = self.get_task(tid)
        if task is not None:
            task.on_start()
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)

    def task_finish(self, tid):
        task = self.get_task(tid)
        if task is not None:
            task.on_finish()
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)

    def task_failure(self, tid):
        task = self.get_task(tid)
        if task is not None:
            _logger.info("Received task failure for %s" % tid)
            if task.attempts == self.max_task_retries:
                _logger.info("Failing task %s after %s attempts" % (tid, task.attempts))
                task.on_fail()
            else:
                _logger.info(
                    "Retrying task %s, attempt %s out of %s"
                    % (tid, task.attempts, self.max_task_retries)
                )
                task.on_retry()
                self.scheduler.schedule(task)
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)

    def task_reset(self, tid):
        task = self.get_task(tid)
        if task is not None:
            task.on_reset()
            self.scheduler.schedule(task)
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)


class NoSuchJobException(Exception):
    pass


class NoSuchTaskException(Exception):
    pass
