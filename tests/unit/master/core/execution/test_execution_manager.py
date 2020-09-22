
import pytest

import time
from kraken.master.core.execution.execution_manager import ExecutionManager
from kraken.master.core.execution.execution_state import ExecutionState
from kraken.common.model.job import Job

from .mock_job import MockJobFactory
from ..worker.mock_worker import MockWorkerManager, MockWorker
from ..tutils import wait_until

@pytest.fixture
def execution_manager(): 
        workers_manager = MockWorkerManager()
        workers_manager.register_worker(MockWorker("worker 1", 10))
        workers_manager.register_worker(MockWorker("worker 2", 10))
        
        execution_manager = ExecutionManager(MockJobFactory(), workers_manager)
        
        execution_manager.start()
        yield execution_manager  # provide the fixture value
        execution_manager.stop()

def _verify_state(obj, state):
    return obj.state == state
   
class TestExecutionManager:
    

    def test_simple_execution_flow(self, execution_manager):
        execution_manager.submit_job(Job(
            jid = "job-1",
            src = "src",
            dest = "dest",
            src_path = "/tmp/src_path",
            dest_path = "/tmp/dest_path",
        ))
        execution_manager.stop()
        assert True
    
    def test_task_finish(self, execution_manager):
        execution_manager.submit_job(Job(
            jid = "job-1",
            src = "src",
            dest = "dest",
            src_path = "/tmp/src_path",
            dest_path = "/tmp/dest_path",
        ))

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
        execution_manager.submit_job(Job(
            jid = "job-1",
            src = "src",
            dest = "dest",
            src_path = "/tmp/src_path",
            dest_path = "/tmp/dest_path",
        ))

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
        execution_manager.submit_job(Job(
            jid = "job-1",
            src = "src",
            dest = "dest",
            src_path = "/tmp/src_path",
            dest_path = "/tmp/dest_path",
        ))
        
        #verify the job is in running state
        assert wait_until( _verify_state , 10, 0.5, execution_manager.get_job("job-1"), ExecutionState.DISPATCHED)
        for task in execution_manager.get_job("job-1").get_tasks():
            assert wait_until( _verify_state, 10, 0.5, task, ExecutionState.DISPATCHED)
            execution_manager.task_start(task.task.tid)
        
        assert len(execution_manager.get_tasks(jid="job-1", states = [ExecutionState.RUNNING])) == 2
        
        
        
        
        
    
