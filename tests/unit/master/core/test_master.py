
import pytest

from kraken.master.core.master import Master
from kraken.master.core.execution.execution_manager import ExecutionManager
from kraken.master.core.worker.worker_manager import WorkerManager
from kraken.master.core.worker.worker_decommissioner import WorkerDecommissioner
from kraken.common.model.execution_type import ExecutionType
from kraken.common.model.job import Job
from kraken.common.model.worker import Worker

from .worker.mock_worker import MockWorkerFactory

class MockMaster(Master):

    def __init__(self):

        # workers manager
        self.workers_manager = WorkerManager(MockWorkerFactory(None))
        self.workers_manager.disable_monitor()
        # execution manager
        self.execution_manager = ExecutionManager(self.workers_manager)
        # decommissioner
        self.decommissioner = WorkerDecommissioner(self.execution_manager, self.workers_manager)
        
        self.started = False

def mock_job(num_tasks):
    return Job(ExecutionType.MOCK, { "num_tasks" : str(num_tasks) })

def mock_worker(wid, cores):
        worker = Worker(wid, None, None)
        worker.cores = cores # hack
        return worker

@pytest.fixture
def master(): 
        master = MockMaster()
        master.start()
        yield master
        master.stop()
    
class TestMaster:

    def test_submit_job(self, master):
        master.submit_job(mock_job(2))
        master.register_worker(mock_worker("worker 1", 10))
        assert len(master.list_jobs()) == 1

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