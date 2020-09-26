
import pytest
import time
import os
from threading import Thread, RLock
from kraken.master.config.config import MasterConfig
from kraken.master.server.server import MasterServer
from kraken.master.client.client import ClientFactory
from kraken.common.model.execution_type import ExecutionType
from ...resources import conf
from ..core.tutils import wait_until

config_dir = os.path.dirname(os.path.abspath(conf.__file__))

glock = RLock()

@pytest.fixture
def user_client():
        config = MasterConfig(path = config_dir)
        factory = ClientFactory(config.client_service_host, config.client_service_port)
        client = factory.create_client('user-service')

        client.start()
        yield client
        client.stop()

@pytest.fixture  
def worker_client():
        config = MasterConfig(path = config_dir)
        factory = ClientFactory(config.worker_service_host, config.worker_service_port)
        client = factory.create_client('worker-service')

        client.start()
        yield client
        client.stop()

@pytest.fixture(scope="session")
def master_server():
          
    server = MasterServer(config = config_dir)
    server_daemon = Thread(target=server.start, args=())
    server_daemon.setDaemon(True)
    server_daemon.start()
    
    #wait for the server to start
    time.sleep(2.0)
        
    yield

    server.stop()
    server_daemon.join()

@pytest.mark.usefixtures("master_server")
class TestServer():
    
    def test_server_up(self, master_server, user_client):
        assert len(user_client.list_jobs()) >= 0

    def test_register_local_worker(self, worker_client):
        worker_client.register_worker("local-worker", None, None)
        assert len(worker_client.list_workers()) == 1

    def test_submit_job(self, user_client):
        jid = user_client.submit_job(ExecutionType.MOCK, {"num_tasks" : "10" })
        # wait for the job to finish
        assert wait_until( lambda i: user_client.job_status(i).state == "FINISHED", 20, 0.5, jid)

    def test_list_jobs(self, user_client):
        assert len(user_client.list_jobs()) >= 1

    def test_unregister_local_worker(self, worker_client):
        worker_client.unregister_worker("local-worker", None, None)
        assert len(worker_client.list_workers()) == 0