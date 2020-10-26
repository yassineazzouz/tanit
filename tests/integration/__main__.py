import contextlib
import os
import time
from threading import Thread

from tanit.master.client.client import ClientType
from tanit.master.client.client import ThriftClientFactory
from tanit.master.config.config import MasterConfig
from tanit.master.server.server import MasterServer
from tanit.worker.server.server import WorkerServer

from ..resources import conf

config_dir = os.path.dirname(os.path.abspath(conf.__file__))


@contextlib.contextmanager
def master_server():
    server = MasterServer(config=config_dir)
    server_daemon = Thread(target=server.start, args=())
    server_daemon.setDaemon(True)
    server_daemon.start()

    # wait for the server to start
    time.sleep(2.0)

    yield

    server.stop()
    server_daemon.join()


@contextlib.contextmanager
def worker_server():
    server = WorkerServer(config=config_dir)
    server_daemon = Thread(target=server.start, args=())
    server_daemon.setDaemon(True)
    server_daemon.start()

    # wait for the server to start
    time.sleep(2.0)

    yield

    server.stop()
    server_daemon.join()


@contextlib.contextmanager
def worker_client():
    config = MasterConfig(path=config_dir)
    factory = ThriftClientFactory(
        config.worker_service_host, config.worker_service_port
    )
    client = factory.create_client(ClientType.WORKER_SERVICE)

    client.start()
    yield client
    client.stop()


def main(argv=None):
    with master_server():
        with worker_server():
            with worker_client() as client:
                print(len(client.list_workers()))


if __name__ == "__main__":
    main()
