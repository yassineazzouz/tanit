
import pytest

from kraken.master.core.execution.execution_state import ExecutionState
from kraken.master.core.execution.execution_job import *
from kraken.common.model.job import Job
from .mock_job import MockJobFactory


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

def mock_job_exec(num_tasks = 2): 
    factory = MockJobFactory()
    job = job_factory.create_job(mock_job(num_tasks))
    job.setup()
    return job

class TestExecutionState:
    
    def test_initial_state(self):
        job = mock_job_exec(2)
        assert job.state == ExecutionState.SUBMITTED
        for task in job.get_tasks():
            assert task.state == ExecutionState.SUBMITTED
    
    def test_schedule_state_transition(self):
        job = mock_job_exec(2)

        job.get_tasks()[0].on_schedule()
        assert job.state == ExecutionState.SCHEDULED
        
        for task in job.get_tasks()[1:] :
            task.on_schedule()
    
        assert job.state == ExecutionState.SCHEDULED
    
    def test_dispatch_state(self):
        job = mock_job_exec(2)
        
        job.get_tasks()[0].on_schedule()
        job.get_tasks()[0].on_dispatch()
        assert job.state == ExecutionState.DISPATCHED
        
        for task in job.get_tasks()[1:] :
            task.on_schedule()
        assert job.state == ExecutionState.DISPATCHED
        
        for task in job.get_tasks()[1:] :
            task.on_dispatch()
        assert job.state == ExecutionState.DISPATCHED        
        
    
    def test_running_state(self):
        job = mock_job_exec(2)
        
        job.get_tasks()[0].on_schedule()
        job.get_tasks()[0].on_dispatch()
        job.get_tasks()[0].on_start()
        assert job.state == ExecutionState.RUNNING
        
        for task in job.get_tasks()[1:] :
            task.on_schedule()
        assert job.state == ExecutionState.RUNNING
        
        for task in job.get_tasks()[1:] :
            task.on_dispatch()
        assert job.state == ExecutionState.RUNNING
    
        for task in job.get_tasks()[1:] :
            task.on_start()
        assert job.state == ExecutionState.RUNNING

    def test_running_state_2(self):
        job = mock_job_exec(2)
        
        job.get_tasks()[0].on_schedule()
        with pytest.raises(IllegalStateTransitionException):
            job.get_tasks()[0].on_start()
        
    def test_finish_state(self):
        job = mock_job_exec(2)
        
        job.get_tasks()[0].on_schedule()
        job.get_tasks()[0].on_dispatch()
        job.get_tasks()[0].on_start()
        job.get_tasks()[0].on_finish()
        assert job.state == ExecutionState.RUNNING
        
        for task in job.get_tasks()[1:] :
            task.on_schedule()
        assert job.state == ExecutionState.RUNNING
        
        for task in job.get_tasks()[1:] :
            task.on_dispatch()
        assert job.state == ExecutionState.RUNNING
    
        for task in job.get_tasks()[1:] :
            task.on_start()
        assert job.state == ExecutionState.RUNNING 

        for task in job.get_tasks()[1:] :
            task.on_finish()
        assert job.state == ExecutionState.FINISHED

    def test_finish_state_2(self):
        job = mock_job_exec(2)
        
        job.get_tasks()[0].on_schedule()
        job.get_tasks()[0].on_dispatch()
        with pytest.raises(IllegalStateTransitionException):
            job.get_tasks()[0].on_finish()
    
    def test_fail_state_1(self):
        job = mock_job_exec(2)
        job.get_tasks()[0].on_schedule()
        job.get_tasks()[0].on_dispatch()
        job.get_tasks()[0].on_start()
        job.get_tasks()[0].on_fail()
        assert job.state == ExecutionState.FAILED
        
        for task in job.get_tasks()[1:] :
            task.on_schedule()
            task.on_dispatch()
            task.on_start()
            task.on_finish()
        assert job.state == ExecutionState.FAILED
    
    def test_fail_state_2(self):
        
        job = mock_job_exec(2)
        job.get_tasks()[0].on_schedule()
        job.get_tasks()[0].on_dispatch()
        job.get_tasks()[0].on_start()
        job.get_tasks()[0].on_finish()
        assert job.state == ExecutionState.RUNNING
        
        for task in job.get_tasks()[1:] :
            task.on_schedule()
            task.on_dispatch()
            task.on_start()
            task.on_fail()
        assert job.state == ExecutionState.FAILED
    
    def test_state_reset(self):
        job = mock_job_exec(2)
        job.get_tasks()[0].on_schedule()
        job.get_tasks()[0].on_dispatch()
        job.get_tasks()[0].on_start()
        job.get_tasks()[0].on_fail()
        assert job.state == ExecutionState.FAILED
        
        for task in job.get_tasks()[1:] :
            task.on_schedule()
            task.on_dispatch()
            task.on_start()
            task.on_finish()
        assert job.state == ExecutionState.FAILED
        
        job.get_tasks()[0].on_reset()
        assert job.state == ExecutionState.RUNNING
        
        job.get_tasks()[0].on_schedule()
        job.get_tasks()[0].on_dispatch()
        job.get_tasks()[0].on_start()
        assert job.state == ExecutionState.RUNNING
        
        job.get_tasks()[0].on_finish()
        assert job.state == ExecutionState.FINISHED 
        