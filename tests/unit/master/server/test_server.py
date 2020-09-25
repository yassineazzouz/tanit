
import pytest
import time
import os
from threading import Thread
from kraken.master.config.config import MasterConfig
from kraken.master.server.server import MasterServer
from kraken.master.client.client import ClientFactory

from ...resources import conf

config_dir = os.path.dirname(os.path.abspath(conf.__file__))


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

@pytest.fixture(scope="class")
def server():
          
    server = MasterServer(config = config_dir)
    server_daemon = Thread(target=server.start, args=())
    server_daemon.setDaemon(True)
    server_daemon.start()
    
    #wait for the server to start
    time.sleep(2.0)
        
    yield server

    server.stop()
    server_daemon.join()

@pytest.mark.usefixtures("server")
class TestServer():
    
    def test_server_up(self, user_client):
        assert len(user_client.list_jobs()) == 0
        
    def test_register_worker(self, worker_client):
        worker_client.register_worker("local-worker", None, None)
        assert len(worker_client.list_workers()) == 1
        