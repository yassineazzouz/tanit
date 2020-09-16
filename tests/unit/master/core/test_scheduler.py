
from Queue import Queue

from kraken.master.core.scheduler import SimpleScheduler
from kraken.master.core.execution.execution_job import JobExecution
from kraken.common.model.task import Task
from kraken.common.model.job import Job

class MockExecutionManager(object):

    def task_schedule(self, tid):
        pass

def mock_job():
    job = JobExecution(
        Job(
            jid = "job-1",
            src = "src",
            dest = "dest",
            src_path = "/tmp/src_path",
            dest_path = "/tmp/dest_path",
        )
    )
    
    job.add_task(
        Task(
            tid = "task-1",
            src = "src",
            dest = "dest",
            src_path = "/tmp/src_path/1",
            dest_path = "/tmp/dest_path/1",
        )
    )

    job.add_task(
        Task(
            tid = "task-2",
            src = "src",
            dest = "dest",
            src_path = "/tmp/src_path/2",
            dest_path = "/tmp/dest_path/2",
        )
    )
    return job
    
def test_simple_scheduler():
    lqueue = Queue()
    cqueue = Queue()
    manager = MockExecutionManager()
    
    scheduler = SimpleScheduler(lqueue, cqueue, manager)
    scheduler.start()
    
    for task in mock_job().get_tasks():
        lqueue.put(task)
    
    scheduler.stop()
    assert lqueue.qsize() == 0
    assert cqueue.qsize() == 2