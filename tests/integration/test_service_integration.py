import pytest

from tanit.common.model.execution_type import ExecutionType
from tanit.common.model.job import Job
from tanit.thrift.master.service.ttypes import JobInitializationException

from ..unit.utils.tutils import wait_until


@pytest.mark.usefixtures("master_server", "worker_server")
class TestServiceIntegration:
    def test_server_up(self, user_client):
        assert len(user_client.list_jobs()) >= 0

    def test_register_worker(self, worker_client):
        assert len(worker_client.list_workers()) == 1

    def test_submit_job(self, user_client):
        jid = user_client.submit_job(Job(ExecutionType.MOCK, {"num_tasks": "10"}))
        # wait for the job to finish
        assert wait_until(
            lambda i: user_client.job_status(i).state == "FINISHED", 20, 0.5, jid
        )

    def test_submit_job_failure(self, user_client):
        with pytest.raises(JobInitializationException):
            user_client.submit_job(Job(ExecutionType.MOCK, {"num_tasks": "not_an_int"}))

    def test_inexistent_job(self, user_client):
        assert user_client.job_status("some_inexistent_job") is None

    def test_job_failure(self, user_client):
        jid = user_client.submit_job(
            Job(ExecutionType.MOCK, {"num_tasks": "10", "num_failures": "2"})
        )
        # wait for the job to finish
        assert wait_until(
            lambda i: user_client.job_status(i).state == "FAILED", 20, 0.5, jid
        )

    def test_list_jobs(self, user_client):
        assert len(user_client.list_jobs()) >= 1
