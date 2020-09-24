
from kraken.master.core.execution.execution_job import JobExecution
from kraken.master.core.execution.job_factory import JobFactory

from kraken.common.model.task import Task

class MockJobFactory(JobFactory):

    def create_job(self, conf):
        return MockJobExecution(conf)
    
class MockJobExecution(JobExecution):
    
    def __init__(self, job):
        super(MockJobExecution, self).__init__(job)
        try:
            self.num_tasks = job.num_tasks if job.num_tasks != None else 2
        except AttributeError:
            self.num_tasks = 2
        
    def setup(self):
        for i in range(self.num_tasks):
            self.add_task(
                Task(
                    tid = "task-%s" % i,
                    src = "src",
                    dest = "dest",
                    src_path = "/tmp/src_path/%s" % i,
                    dest_path = "/tmp/dest_path/%s" % i,
                )
            )
