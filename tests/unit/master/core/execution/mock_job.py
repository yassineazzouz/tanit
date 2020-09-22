
from kraken.master.core.execution.execution_job import JobExecution
from kraken.master.core.execution.job_factory import JobFactory

from kraken.common.model.task import Task

class MockJobFactory(JobFactory):
    def create_job(self, conf):
        return MockJobExecution(conf)
    
class MockJobExecution(JobExecution):
        
    def setup(self):
        for i in range(2):
            self.add_task(
                Task(
                    tid = "task-%s" % i,
                    src = "src",
                    dest = "dest",
                    src_path = "/tmp/src_path/%s" % i,
                    dest_path = "/tmp/dest_path/%s" % i,
                )
            )
