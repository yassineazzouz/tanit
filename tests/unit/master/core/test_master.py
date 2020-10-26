import pytest

from tanit.common.model.execution_type import ExecutionType
from tanit.common.model.job import Job
from tanit.common.model.worker import Worker
from tanit.master.core.execution.execution_job import JobInitializationException
from tanit.master.core.execution.execution_state import ExecutionState
from tanit.master.core.master import Master

from ...utils.tutils import wait_until


@pytest.fixture
def master():
    master = Master()
    master.start()
    yield master
    master.stop()


class TestMaster:
    def test_submit_job(self, master):
        master.register_worker(Worker("worker 1", None, None))
        job = master.submit_job(Job(ExecutionType.MOCK, {"num_tasks": "2"}))
        assert wait_until(
            lambda i: master.get_job(i).state == ExecutionState.FINISHED,
            20,
            0.5,
            job.jid,
        )

    def test_submit_bad_job_params(self, master):
        master.register_worker(Worker("worker 1", None, None))
        with pytest.raises(JobInitializationException):
            master.submit_job(Job(ExecutionType.MOCK, {"num_tasks": "not_an_int"}))

    def test_list_jobs(self, master):
        master.submit_job(Job(ExecutionType.MOCK, {"num_tasks": "2"}))
        master.register_worker(Worker("worker 1", None, None))
        assert len(master.list_jobs()) == 1

    def test_register_worker(self, master):
        master.register_worker(Worker("worker 1", None, None))
        assert len(master.list_workers()) == 1

    def test_register_heart_beat(self, master):
        master.register_worker(Worker("worker 1", None, None))
        master.register_heartbeat(Worker("worker 1", None, None))
        assert len(master.list_workers()) == 1

    def test_unregister_worker(self, master):
        master.register_worker(Worker("worker 1", None, None))
        assert len(master.list_workers()) == 1
        master.unregister_worker(Worker("worker 1", None, None))
        assert len(master.list_workers()) == 0
        master.register_worker(Worker("worker 2", None, None))
        assert len(master.list_workers()) == 1
