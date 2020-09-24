
import pytest
import time
import os
from threading import Thread
from kraken.master.config.config import MasterConfig
from kraken.master.server.server import MasterServer
from kraken.master.client.client import ClientFactory

from ..resources import conf

config_dir = os.path.dirname(os.path.abspath(conf.__file__))

@pytest.fixture
def server():
    def get_user_client():
        config = MasterConfig(path = config_dir)
        factory = ClientFactory(config.client_service_host, config.client_service_port)
        return factory.create_client('user-service')

    server = MasterServer(config = config_dir)
    server_daemon = Thread(target=server.start, args=())
    server_daemon.setDaemon(True)
    server_daemon.start()
    
    #wait for the server to start
    time.sleep(2.0)
    client = get_user_client()
    client.start()
        
    yield client
        
    client.stop()
    server.stop()
    server_daemon.join()
    
class TestServer():
    
    def test_server_up(self,server):
        assert len(server.list_jobs()) == 0
        
