
from kraken.master.config.config import MasterConfig 
from kraken.master.core.master import Master
from kraken.master.core.execution.execution_manager import ExecutionManager
from kraken.master.core.worker.worker_manager import WorkerManager
from kraken.master.core.worker.worker_decommissioner import WorkerDecommissioner

from .core.execution.mock_job import MockJobFactory

class MasterServer(object):
    
    def __init__(self, config = None):
        
        self.config = MasterConfig(config)
        self.config.load()
        
        self.standalone = False
        self.master = MockMaster()
        self.master.configure(self.config)
        
        self.stopped = False

class MockMaster(Master):

    def __init__(self):
        # workers manager
        self.workers_manager = WorkerManager()
        self.workers_manager.disable_monitor()
        # execution manager
        self.execution_manager = ExecutionManager(self.workers_manager, MockJobFactory())
        # decommissioner
        self.decommissioner = WorkerDecommissioner(self.execution_manager, self.workers_manager)
        
        self.started = False
        
