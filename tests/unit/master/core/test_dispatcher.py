import pytest
from six.moves.queue import Queue

from tanit.common.model.execution_type import ExecutionType
from tanit.common.model.job import Job
from tanit.common.model.worker import Worker
from tanit.master.core.dispatcher import FairDispatcher
from tanit.master.core.execution.job_factory import JobFactory
from tanit.master.core.worker.worker_manager import WorkerManager

from ...utils.tutils import wait_until
from .worker.mock_worker import MockWorkerFactory

job_factory = JobFactory()


def mock_job_exec(num_tasks):
    job = job_factory.create_job(Job(ExecutionType.MOCK, {"num_tasks": str(num_tasks)}))
    job.setup()
    return job


def mock_worker(wid, cores):
    worker = Worker(wid, None, None)
    worker.cores = cores  # hack
    return worker


@pytest.fixture
def simple_dispatcher():
    cqueue = Queue()
    workers_manager = WorkerManager(MockWorkerFactory(None))
    workers_manager.disable_monitor()
    dispatcher = FairDispatcher(cqueue, workers_manager, None)

    workers_manager.start()
    dispatcher.start()

    yield dispatcher

    dispatcher.stop()
    workers_manager.stop()


def _verify_queue_size(cqueue, size):
    return cqueue.qsize() == size


class TestSimpleDispatcher:
    def test_simple_dipstacher(self, simple_dispatcher):

        simple_dispatcher.workers_manager.register_worker(mock_worker("worker 1", 10))

        for task in mock_job_exec(2).get_tasks():
            simple_dispatcher.cqueue.put(task)

        assert wait_until(_verify_queue_size, 10, 0.5, simple_dispatcher.cqueue, 0)

    def test_dipstacher_stop(self, simple_dispatcher):

        simple_dispatcher.workers_manager.register_worker(mock_worker("worker 1", 10))

        simple_dispatcher.stop()

        for task in mock_job_exec(2).get_tasks():
            simple_dispatcher.cqueue.put(task)

        assert simple_dispatcher.cqueue.qsize() == 2

    def test_dipstacher_callback(self, simple_dispatcher):
        def callback(tid, worker):
            callback_received.append({tid: worker})

        callback_received = []

        simple_dispatcher.callback = callback
        simple_dispatcher.workers_manager.register_worker(mock_worker("worker 1", 10))

        for task in mock_job_exec(2).get_tasks():
            simple_dispatcher.cqueue.put(task)

        assert wait_until(lambda l, size: len(l) == size, 10, 0.5, callback_received, 2)


class TestFairDispatcher:
    def test_dipstacher_fairness_1(self, simple_dispatcher):

        simple_dispatcher.workers_manager.register_worker(mock_worker("worker 1", 10))
        simple_dispatcher.workers_manager.register_worker(mock_worker("worker 2", 10))

        for task in mock_job_exec(4).get_tasks():
            simple_dispatcher.cqueue.put(task)

        assert wait_until(_verify_queue_size, 10, 0.5, simple_dispatcher.cqueue, 0)

        for worker in simple_dispatcher.workers_manager.list_live_workers():
            assert len(worker.tasks) == 2

    def test_dispatcher_fairness_2(self, simple_dispatcher):
        """
        Tests fair dispatcher logic based on workers load.

        for instance:
           w1 --> 5 cores
           w2 --> 12 cores
        the dispatcher will dispatch tasks to the worker with less pending
        then the to the worker with more available cores so for (5,12) :
        (0,0) -> (0,1) -> (0,2) -> (0,3) -> (0.4) -> (0,5) -> (0,6)
        -> (0,7) -> (1,7) -> (1,8) -> (2,8) -> (2,9) -> (3,9) -> (3,10)
        -> (4,10) -> (4,11) -> (5,11) -> (5,12) -> (6,12) -> (6,13) -> (7,13)
        """
        simple_dispatcher.workers_manager.register_worker(mock_worker("worker 1", 5))
        simple_dispatcher.workers_manager.register_worker(mock_worker("worker 2", 12))

        for task in mock_job_exec(20).get_tasks():
            simple_dispatcher.cqueue.put(task)

        assert wait_until(_verify_queue_size, 10, 0.5, simple_dispatcher.cqueue, 0)

        for worker in simple_dispatcher.workers_manager.list_live_workers():
            if worker.wid == "worker 1":
                assert len(worker.tasks) == 7
            elif worker.wid == "worker 2":
                assert len(worker.tasks) == 13
