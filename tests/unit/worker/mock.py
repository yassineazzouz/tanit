
import time
import os
import logging as lg
from kraken.worker.server.worker import Worker, WorkerHearbeatReporter
from kraken.worker.core.executor import Executor
from kraken.worker.core.executor_pool import ExecutorPool
from kraken.worker.core.executor_factory import ExecutorFactory
from kraken.worker.config.config import WorkerConfig
from kraken.worker.server.server import WorkerServer
from kraken.master.client.client import ClientFactory

from ...resources import conf

config_dir = os.path.dirname(os.path.abspath(conf.__file__))

_logger = lg.getLogger(__name__)

class MockWorkerServer(WorkerServer):
    
    def __init__(self):
        
        self.config = WorkerConfig(path = config_dir)
        self.config.load()
        
        self.worker = MockWorker()
        self.worker.configure(self.config)

class MockWorker(Worker):
    
    def configure(self, config):
        self.address = config.worker_host
        self.port = config.worker_port
        
        self.wid = "kraken-worker-%s-%s" % (self.address, self.port)
        factory = ClientFactory(config.master_host, config.master_port)
        self.master = factory.create_client('worker-service')
        
        self.executor = ExecutorPool( self.wid,
                                      MockExecutorFactory(factory ,self.lqueue, config.executor_threads),
                                      self.lqueue,
                                      config.executor_threads)
        
        self.reporter = WorkerHearbeatReporter(self)
        self.reporter.setDaemon(True)

class MockExecutorFactory(ExecutorFactory):
        
    def create_executor(self, eid):
        return MockExecutor(eid, self.cqueue, self.client_factory)

class MockExecutor(Executor):
    
    def __init__(self, eid, cqueue, master):
        super(Executor, self).__init__(eid, cqueue, master)

    def _run(self, task):
        # This is just a mock execution
        time.sleep(2.0)
