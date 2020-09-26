
import pytest
import time
import os
from threading import Thread
from kraken.common.model.execution_type import ExecutionType
from kraken.worker.server.server import WorkerServer
from ..master.server.test_server import master_server, user_client
from ..resources import conf
from ..master.core.tutils import wait_until

config_dir = os.path.dirname(os.path.abspath(conf.__file__))

@pytest.fixture(scope="class")
def worker_server():
    server = WorkerServer(config = config_dir)
    server_daemon = Thread(target=server.start, args=())
    server_daemon.setDaemon(True)
    server_daemon.start()
    
    #wait for the server to start
    time.sleep(2.0)
        
    yield
        
    server.stop()
    server_daemon.join()

@pytest.mark.usefixtures("master_server", "worker_server")
class TestIntegration():
    
    def test_server_up(self ,worker_server, user_client):
        assert len(user_client.list_jobs()) >= 0

    def test_submit_job(self, worker_server, user_client):
        jid = user_client.submit_job(ExecutionType.MOCK, {"num_tasks" : "10" })
        # wait for the job to finish
        assert wait_until( lambda i: user_client.job_status(i).state == "FINISHED", 20, 0.5, jid)

    def test_list_jobs(self, worker_server, user_client):
        assert len(user_client.list_jobs()) >= 1