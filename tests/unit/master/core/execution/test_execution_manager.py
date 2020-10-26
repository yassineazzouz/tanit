import pytest

from tanit.common.model.execution_type import ExecutionType
from tanit.common.model.job import Job
from tanit.common.model.worker import Worker
from tanit.master.core.execution.execution_manager import ExecutionManager
from tanit.master.core.execution.execution_state import ExecutionState
from tanit.master.core.worker.worker_manager import WorkerManager

from ....utils.tutils import wait_until
from ..worker.mock_worker import MockWorkerFactory


def mock_worker(wid, cores):
    worker = Worker(wid, None, None)
    worker.cores = cores  # hack
    return worker


@pytest.fixture
def execution_manager():
    workers_manager = WorkerManager(MockWorkerFactory(None))
    workers_manager.register_worker(mock_worker("worker 1", 10))

    execution_manager = ExecutionManager(workers_manager)

    execution_manager.start()
    yield execution_manager
    execution_manager.stop()


def _verify_state(obj, state):
    return obj.state == state


class TestExecutionManager:
    def test_job_submit(self, execution_manager):
        execution_manager.submit_job(Job(ExecutionType.MOCK, {"num_tasks": "2"}))
        assert True

    def test_task_finish(self, execution_manager):
        job_exec = execution_manager.submit_job(
            Job(ExecutionType.MOCK, {"num_tasks": "2"})
        )

        # verify the job is in running state
        assert wait_until(
            _verify_state,
            10,
            0.5,
            execution_manager.get_job(job_exec.jid),
            ExecutionState.DISPATCHED,
        )
        for task in execution_manager.get_job(job_exec.jid).get_tasks():
            assert wait_until(_verify_state, 10, 0.5, task, ExecutionState.DISPATCHED)
            execution_manager.task_start(task.tid)

        assert execution_manager.get_job(job_exec.jid).state == ExecutionState.RUNNING

        for task in execution_manager.get_job(job_exec.jid).get_tasks():
            assert task.state == ExecutionState.RUNNING
            execution_manager.task_finish(task.tid)

        assert execution_manager.get_job(job_exec.jid).state == ExecutionState.FINISHED

        for task in execution_manager.get_job(job_exec.jid).get_tasks():
            assert task.state == ExecutionState.FINISHED

    def test_task_fail(self, execution_manager):
        # do not retry tasks on failure
        execution_manager.max_task_retries = 1

        job_exec = execution_manager.submit_job(
            Job(ExecutionType.MOCK, {"num_tasks": "2"})
        )

        # verify the job is in running state
        assert wait_until(
            _verify_state,
            10,
            0.5,
            execution_manager.get_job(job_exec.jid),
            ExecutionState.DISPATCHED,
        )

        for task in execution_manager.get_job(job_exec.jid).get_tasks():
            assert wait_until(_verify_state, 10, 0.5, task, ExecutionState.DISPATCHED)
            execution_manager.task_start(task.tid)

        assert execution_manager.get_job(job_exec.jid).state == ExecutionState.RUNNING
        for task in execution_manager.get_job(job_exec.jid).get_tasks():
            assert task.state == ExecutionState.RUNNING

        execution_manager.task_failure(
            execution_manager.get_job(job_exec.jid).get_tasks()[0].tid
        )
        for task in execution_manager.get_job(job_exec.jid).get_tasks()[1:]:
            execution_manager.task_finish(task.tid)

        assert execution_manager.get_job(job_exec.jid).state == ExecutionState.FAILED
        return job_exec

    def test_task_reset(self, execution_manager):
        job_exec = self.test_task_fail(execution_manager)

        # the failed task
        task = execution_manager.get_job(job_exec.jid).get_tasks()[0]

        # reset the failed task
        execution_manager.task_reset(task.tid)
        assert execution_manager.get_job(job_exec.jid).state == ExecutionState.RUNNING

        # wait for the task to be dispatched
        assert wait_until(_verify_state, 10, 0.5, task, ExecutionState.DISPATCHED)

        execution_manager.task_start(task.tid)
        assert task.state == ExecutionState.RUNNING
        assert execution_manager.get_job(job_exec.jid).state == ExecutionState.RUNNING

        execution_manager.task_finish(task.tid)
        assert task.state == ExecutionState.FINISHED
        assert execution_manager.get_job(job_exec.jid).state == ExecutionState.FINISHED

    def test_task_lookup(self, execution_manager):
        job_exec = execution_manager.submit_job(
            Job(ExecutionType.MOCK, {"num_tasks": "2"})
        )

        # verify the job is in running state
        assert wait_until(
            _verify_state,
            10,
            0.5,
            execution_manager.get_job(job_exec.jid),
            ExecutionState.DISPATCHED,
        )
        for task in execution_manager.get_job(job_exec.jid).get_tasks():
            assert wait_until(_verify_state, 10, 0.5, task, ExecutionState.DISPATCHED)
            execution_manager.task_start(task.tid)

        assert (
            len(
                execution_manager.get_tasks(
                    jid=job_exec.jid, states=[ExecutionState.RUNNING]
                )
            )
            == 2
        )
