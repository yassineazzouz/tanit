
import pytest

from Queue import Queue

from kraken.master.core.scheduler import SimpleScheduler
from kraken.master.core.execution.execution_job import JobExecution
from kraken.common.model.task import Task
from kraken.common.model.job import Job

from .execution.mock_job import MockJobFactory
from .tutils import wait_until

job_factory = MockJobFactory()

def mock_job(num_tasks):
    job = Job(
        jid = "job-1",
        src = "src",
        dest = "dest",
        src_path = "/tmp/src_path",
        dest_path = "/tmp/dest_path",
    )
    job.num_tasks = num_tasks
    return job

def mock_job_exec(num_tasks):
    job = job_factory.create_job(mock_job(num_tasks))
    job.setup()
    return job

def _verify_queue_size(cqueue, size):
    return cqueue.qsize() == size

@pytest.fixture
def simple_scheduler():
        simple_scheduler = SimpleScheduler(Queue(), Queue(), None)
        simple_scheduler.start()
        
        yield simple_scheduler
        
        simple_scheduler.stop()

class TestSimpleScheduler:

    def test_schedule(self, simple_scheduler):
    
        for task in mock_job_exec(2).get_tasks():
            simple_scheduler.lqueue.put(task)
            
        assert wait_until( _verify_queue_size, 10, 0.5, simple_scheduler.lqueue, 0)
        assert wait_until( _verify_queue_size, 10, 0.5, simple_scheduler.cqueue, 2)

    def test_scheduler_stop(self, simple_scheduler):
        simple_scheduler.stop()
        
        for task in mock_job_exec(2).get_tasks():
            simple_scheduler.lqueue.put(task)

        assert wait_until( _verify_queue_size, 10, 0.5, simple_scheduler.lqueue, 2)
        assert wait_until( _verify_queue_size, 10, 0.5, simple_scheduler.cqueue, 0)
        

    def test_scheduler_callback(self, simple_scheduler):
        
        def callback(tid):
            callback_received.append(tid)
        
        callback_received = []
        simple_scheduler.callback = callback
        
        for task in mock_job_exec(2).get_tasks():
            simple_scheduler.lqueue.put(task)
        
        assert wait_until(
            lambda list, size: len(list) == size, 10, 0.5, callback_received, 2)