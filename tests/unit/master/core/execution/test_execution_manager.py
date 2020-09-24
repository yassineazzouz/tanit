
import pytest

import time
from kraken.master.core.execution.execution_manager import ExecutionManager
from kraken.master.core.execution.execution_state import ExecutionState
from kraken.master.core.worker.worker_manager import WorkerManager
from kraken.common.model.job import Job
from kraken.common.model.worker import Worker

from .mock_job import MockJobFactory, MockJobExecution
from ..worker.mock_worker import MockWorkerFactory
from ..tutils import wait_until

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

def mock_worker(wid, cores):
        worker = Worker(wid, None, None)
        worker.cores = cores # hack
        return worker

@pytest.fixture
def execution_manager(): 
        workers_manager = WorkerManager(MockWorkerFactory())
        workers_manager.register_worker(mock_worker("worker 1", 10))
        
        execution_manager = ExecutionManager(workers_manager, MockJobFactory())
        
        execution_manager.start()
        yield execution_manager
        execution_manager.stop()

def _verify_state(obj, state):
    return obj.state == state
   
class TestExecutionManager:
    

    def test_job_sumbit(self, execution_manager):
        execution_manager.submit_job(mock_job(2))
        assert True
    
    def test_task_finish(self, execution_manager):
        execution_manager.submit_job(mock_job(2))

        #verify the job is in running state
        assert wait_until( _verify_state , 10, 0.5, execution_manager.get_job("job-1"), ExecutionState.DISPATCHED)
        for task in execution_manager.get_job("job-1").get_tasks():
            assert wait_until( _verify_state, 10, 0.5, task, ExecutionState.DISPATCHED)
            execution_manager.task_start(task.task.tid)

        assert execution_manager.get_job("job-1").state == ExecutionState.RUNNING
        for task in execution_manager.get_job("job-1").get_tasks():
            assert task.state == ExecutionState.RUNNING
            execution_manager.task_finish(task.task.tid)
        
        assert execution_manager.get_job("job-1").state == ExecutionState.FINISHED
        for task in execution_manager.get_job("job-1").get_tasks():
            assert task.state == ExecutionState.FINISHED
    
    
    def test_task_fail(self, execution_manager):
        execution_manager.submit_job(mock_job(2))

        #verify the job is in running state
        assert wait_until( _verify_state , 10, 0.5, execution_manager.get_job("job-1"), ExecutionState.DISPATCHED)
        for task in execution_manager.get_job("job-1").get_tasks():
            assert wait_until( _verify_state, 10, 0.5, task, ExecutionState.DISPATCHED)
            execution_manager.task_start(task.task.tid)

        assert execution_manager.get_job("job-1").state == ExecutionState.RUNNING
        for task in execution_manager.get_job("job-1").get_tasks():
            assert task.state == ExecutionState.RUNNING
        
        execution_manager.task_failure(execution_manager.get_job("job-1").get_tasks()[0].task.tid)
        for task in execution_manager.get_job("job-1").get_tasks()[1:]:
            execution_manager.task_finish(task.task.tid)
        
        assert execution_manager.get_job("job-1").state == ExecutionState.FAILED

    def test_task_reset(self, execution_manager):
        self.test_task_fail(execution_manager)
        
        # the failed task
        task = execution_manager.get_job("job-1").get_tasks()[0]
        
        #reset the failed task
        execution_manager.task_reset(task.task.tid)
        assert execution_manager.get_job("job-1").state == ExecutionState.RUNNING
        
        #wait for the task to be dispatched
        assert wait_until( _verify_state, 10, 0.5, task, ExecutionState.DISPATCHED)
        
        execution_manager.task_start(task.task.tid)
        assert task.state == ExecutionState.RUNNING
        assert execution_manager.get_job("job-1").state == ExecutionState.RUNNING
        
        execution_manager.task_finish(task.task.tid)
        assert task.state == ExecutionState.FINISHED
        assert execution_manager.get_job("job-1").state == ExecutionState.FINISHED
        
    def test_task_lookup(self, execution_manager):
        execution_manager.submit_job(mock_job(2))
        
        #verify the job is in running state
        assert wait_until( _verify_state , 10, 0.5, execution_manager.get_job("job-1"), ExecutionState.DISPATCHED)
        for task in execution_manager.get_job("job-1").get_tasks():
            assert wait_until( _verify_state, 10, 0.5, task, ExecutionState.DISPATCHED)
            execution_manager.task_start(task.task.tid)
        
        assert len(execution_manager.get_tasks(jid="job-1", states = [ExecutionState.RUNNING])) == 2
        
        
        
        
        
    
