
from Queue import Queue

from kraken.master.core.scheduler import SimpleScheduler
from kraken.master.core.execution.execution_job import JobExecution
from kraken.master.core.execution.execution_manager import ExecutionManager
from kraken.master.core.execution.execution_state import ExecutionState
from kraken.common.model.task import Task
from kraken.common.model.job import Job

def mock_job(num_tasks):
    job = JobExecution(
        Job(
            jid = "job-1",
            src = "src",
            dest = "dest",
            src_path = "/tmp/src_path",
            dest_path = "/tmp/dest_path",
        )
    )
    
    for i in range(num_tasks):
        job.add_task(
            Task(
                tid = "task-%s" % i,
                src = "src",
                dest = "dest",
                src_path = "/tmp/src_path/%s" % i,
                dest_path = "/tmp/dest_path/%s" % i,
            )
        )
    return job

def test_simple_scheduler():
    lqueue = Queue()
    cqueue = Queue()
    manager = ExecutionManager()
    
    scheduler = SimpleScheduler(lqueue, cqueue, manager)
    scheduler.start()
    
    job =  mock_job(2)
    manager.register_job(job)
    
    for task in job.get_tasks():
        lqueue.put(task)
    
    scheduler.stop()
    assert lqueue.qsize() == 0
    assert cqueue.qsize() == 2

def test_scheduler_stop():
    lqueue = Queue()
    cqueue = Queue()
    manager = ExecutionManager()
    
    scheduler = SimpleScheduler(lqueue, cqueue, manager)
    scheduler.start()
    scheduler.stop()
    
    job =  mock_job(2)
    manager.register_job(job)
    
    for task in job.get_tasks():
        lqueue.put(task)

    assert lqueue.qsize() == 2
    assert cqueue.qsize() == 0

def test_scheduler_task_state():
    lqueue = Queue()
    cqueue = Queue()
    manager = ExecutionManager()
    
    scheduler = SimpleScheduler(lqueue, cqueue, manager)
    scheduler.start()
    
    job =  mock_job(2)
    manager.register_job(job)
    
    for task in job.get_tasks():
        lqueue.put(task)
    
    scheduler.stop()
    
    for job in manager.list_jobs():
        assert job.state == ExecutionState.SCHEDULED
        for task in job.get_tasks():
            assert task.state == ExecutionState.SCHEDULED