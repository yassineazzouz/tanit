from datetime import datetime

from tanit.common.model.job import JobStatus
from tanit.common.model.worker import Worker


class TestModel:
    def test_worker_eq(self):
        assert Worker("local-worker", "127.0.0.1", 8080) == Worker(
            "local-worker", "127.0.0.1", 8080
        )

        assert Worker("local-worker", "127.0.0.1", 8080) != Worker(
            "local-worker", "127.0.0.1", 8081
        )

        assert Worker("local-worker", "127.0.0.2", 8080) != Worker(
            "local-worker", "127.0.0.1", 8080
        )

        assert Worker("local-1-worker", "127.0.0.1", 8080) != Worker(
            "local-2-worker", "127.0.0.1", 8080
        )

    def test_worker_str(self):
        str(Worker("local-worker", "127.0.0.1", 8080))
        assert True

    def test_job_stats_str(self):
        str(
            JobStatus(
                "jid-1",
                "RUNNING",
                datetime.now().strftime("%d%m%Y%H%M%S"),
                datetime.now().strftime("%d%m%Y%H%M%S"),
                datetime.now().strftime("%d%m%Y%H%M%S"),
                50,
            )
        )
        assert True
