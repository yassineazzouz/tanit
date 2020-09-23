
import pytest

import time
from kraken.master.core.master import Master
from kraken.master.core.execution.execution_manager import ExecutionManager
from kraken.master.core.execution.execution_state import ExecutionState
from kraken.common.model.job import Job
from kraken.common.model.worker import Worker

from .execution.mock_job import MockJobFactory
from .worker.mock_worker import MockWorkerManager
from multiprocessing.pool import worker

class MockMaster(Master):

    def __init__(self):
        self.execution_manager = ExecutionManager(MockJobFactory(), MockWorkerManager())
        self.started = False
    
@pytest.fixture
def master(): 
        master = MockMaster()
        master.start()
        yield master
        master.stop()
   
def mock_worker(wid, cores):
        worker = Worker(wid, None, None)
        worker.cores = cores # hack
        return worker
    
class TestExecutionManager:

    def test_submit_job(self, master):
        master.submit_job(Job(
            jid = "job-1",
            src = "src",
            dest = "dest",
            src_path = "/tmp/src_path",
            dest_path = "/tmp/dest_path",
        ))
        master.register_worker(mock_worker("worker 1", 10))
        assert len(master.list_jobs()) == 1
        assert master.get_job("job-1") != None

    def test_register_worker(self, master):
        master.register_worker(mock_worker("worker 1", 10))
        assert len(master.list_workers()) == 1

    def test_register_heart_beat(self, master):
        master.register_worker(mock_worker("worker 1", 10))
        master.register_heartbeat(Worker("worker 1", None, None))
        assert len(master.list_workers()) == 1

    def test_unregister_worker(self, master):
        master.register_worker(mock_worker("worker 1", 10))
        assert len(master.list_workers()) == 1
        master.unregister_worker(Worker("worker 1", None, None))
        assert len(master.list_workers()) == 0
        master.register_worker(mock_worker("worker 2", 10))
        assert len(master.list_workers()) == 1