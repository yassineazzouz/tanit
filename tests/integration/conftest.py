import os
from threading import Thread

import pytest

from tanit.master.client.client import ClientType
from tanit.master.client.client import ThriftClientFactory
from tanit.master.config.config import MasterConfig
from tanit.master.server.server import MasterServer
from tanit.worker.server.server import WorkerServer

from ..resources import conf

config_dir = os.path.dirname(os.path.abspath(conf.__file__))


@pytest.fixture(scope="session")
def master_server():
    server = MasterServer(config=config_dir)
    server_daemon = Thread(target=server.start, args=())
    server_daemon.setDaemon(True)
    server_daemon.start()

    yield

    server.stop()
    server_daemon.join()


@pytest.fixture(scope="class")
def worker_server():
    server = WorkerServer(config=config_dir)
    server_daemon = Thread(target=server.start, args=())
    server_daemon.setDaemon(True)
    server_daemon.start()

    yield

    server.stop()
    server_daemon.join()


@pytest.fixture
def user_client():
    config = MasterConfig(path=config_dir)
    factory = ThriftClientFactory(
        config.client_service_host, config.client_service_port
    )
    client = factory.create_client(ClientType.USER_SERVICE)

    client.start()
    yield client
    client.stop()


@pytest.fixture
def worker_client():
    config = MasterConfig(path=config_dir)
    factory = ThriftClientFactory(
        config.worker_service_host, config.worker_service_port
    )
    client = factory.create_client(ClientType.WORKER_SERVICE)

    client.start()
    yield client
    client.stop()
