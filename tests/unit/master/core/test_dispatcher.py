
import pytest

from Queue import Queue

from kraken.master.core.dispatcher import FairDispatcher
from kraken.master.core.execution.execution_job import JobExecution
from kraken.master.core.execution.execution_manager import ExecutionManager
from kraken.master.core.worker.worker_manager import WorkerManager
from kraken.master.core.worker.worker import WorkerIFace
from kraken.common.model.worker import WorkerStatus
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

class MockWorker(WorkerIFace):
    def __init__(self, wid, cores = 10):
        self.wid = wid
        self.tasks = []
        self.concurrency = cores
        
    def start(self):
        pass
        
    def stop(self):
        pass

    def submit(self, task):
        self.tasks.append(task)
        
    def status(self):
        return WorkerStatus(
                   self.wid,
                   len(self.tasks),
                   0 if len(self.tasks) < self.concurrency else len(self.tasks) - self.concurrency,
                   0 if self.concurrency < len(self.tasks) else self.concurrency - len(self.tasks)
                )

class MockWorkerManager(WorkerManager):
    
    def __init__(self):
        self.live_workers = []
    
    def register_worker(self, worker):
        self.live_workers.append(worker)

    def list_live_workers(self):
        return self.live_workers
    
    def all_tasks(self):
        tasks = []
        for worker in self.live_workers:
            tasks.extend(worker.tasks)
        return tasks
    
@pytest.fixture
def simple_dispatcher():
    cqueue = Queue()
    workers_manager = MockWorkerManager()
    workers_manager.register_worker(MockWorker("worker 1", 10))
    workers_manager.register_worker(MockWorker("worker 2", 10))
    
    return FairDispatcher( cqueue, workers_manager, None)

@pytest.fixture
def simple_dispatcher_2():
    cqueue = Queue()
    workers_manager = MockWorkerManager()
    workers_manager.register_worker(MockWorker("worker 1", 5))
    workers_manager.register_worker(MockWorker("worker 2", 12))
    
    return FairDispatcher( cqueue, workers_manager, None)

def test_simple_dipstacher(simple_dispatcher):
    simple_dispatcher.start()
     
    for task in simple_job(2).get_tasks():
        simple_dispatcher.cqueue.put(task)
    
    simple_dispatcher.stop()
    assert simple_dispatcher.cqueue.qsize() == 0
    assert len(simple_dispatcher.workers_manager.all_tasks()) == 2

def test_dipstacher_stop(simple_dispatcher):
    
    simple_dispatcher.start()
    simple_dispatcher.stop()
    
    for task in simple_job(2).get_tasks():
        simple_dispatcher.cqueue.put(task)
    
    assert simple_dispatcher.cqueue.qsize() == 2
    assert len(simple_dispatcher.workers_manager.all_tasks()) == 0

def test_dipstacher_fairness_1(simple_dispatcher):

    simple_dispatcher.start()
    
    for task in simple_job(4).get_tasks():
        simple_dispatcher.cqueue.put(task)
    
    simple_dispatcher.stop()
    
    assert simple_dispatcher.cqueue.qsize() == 0
    for worker in simple_dispatcher.workers_manager.list_live_workers():
        assert len(worker.tasks) == 2

def test_dipstacher_fairness_2(simple_dispatcher_2):
    '''
    The fair dispatcher dispatch tasks based on the load on workers, for instance:
    w1 --> 5 cores
    w2 --> 12 cores
    the dispatcher will dispatch tasks to the worker with less pending
    then the to the worker with more available cores so for (5,12) :
    (0,0) -> (0,1) -> (0,2) -> (0,3) -> (0.4) -> (0,5) -> (0,6) -> (0,7) -> (1,7)
    -> (1,8) -> (2,8) -> (2,9) -> (3,9) -> (3,10) -> (4,10) -> (4,11) -> (5,11)
    -> (5,12) -> (6,12) -> (6,13) -> (7,13)
    '''
    simple_dispatcher_2.start()
    
    for task in simple_job(20).get_tasks():
        simple_dispatcher_2.cqueue.put(task)
    
    simple_dispatcher_2.stop()
    
    assert simple_dispatcher_2.cqueue.qsize() == 0
    for worker in simple_dispatcher_2.workers_manager.list_live_workers():
        if (worker.wid == "worker 1"):
            assert len(worker.tasks) == 7
        elif (worker.wid == "worker 2"):
            assert len(worker.tasks) == 13

def test_dipstacher_callback():

    callback_received = []
    
    def callback(tid, worker):
        callback_received.append( {tid : worker } )

    cqueue = Queue()
    workers_manager = MockWorkerManager()
    workers_manager.register_worker(MockWorker("worker 1", 10))
    workers_manager.register_worker(MockWorker("worker 2", 10))
    
    simple_dispatcher = FairDispatcher( cqueue, workers_manager, callback)

    simple_dispatcher.start()
    
    for task in simple_job(2).get_tasks():
        simple_dispatcher.cqueue.put(task)
    
    simple_dispatcher.stop()
    
    assert len(callback_received) == 2