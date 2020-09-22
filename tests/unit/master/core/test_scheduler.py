
from Queue import Queue

from kraken.master.core.scheduler import SimpleScheduler
from kraken.master.core.execution.execution_job import JobExecution
from kraken.common.model.task import Task
from kraken.common.model.job import Job

def simple_job(num_tasks):
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

class TestSimpleScheduler:

    def test_schedule(self):

        simple_scheduler = SimpleScheduler(Queue(), Queue(), None)
        simple_scheduler.start()
    
        for task in simple_job(2).get_tasks():
            simple_scheduler.lqueue.put(task)
    
        simple_scheduler.stop()

        assert simple_scheduler.lqueue.qsize() == 0
        assert simple_scheduler.cqueue.qsize() == 2

    def test_scheduler_stop(self):
        simple_scheduler = SimpleScheduler(Queue(), Queue(), None)
        simple_scheduler.start()
        simple_scheduler.stop()
        
        for task in simple_job(2).get_tasks():
            simple_scheduler.lqueue.put(task)

        assert simple_scheduler.lqueue.qsize() == 2
        assert simple_scheduler.cqueue.qsize() == 0

    def test_scheduler_callback(self):
        
        callback_received = []
        
        def callback(tid):
            callback_received.append(tid)

        simple_scheduler = SimpleScheduler(Queue(), Queue(), callback)
        simple_scheduler.start()
        
        for task in simple_job(2).get_tasks():
            simple_scheduler.lqueue.put(task)
        
        simple_scheduler.stop()
        
        assert len(callback_received) == 2