
import time
from threading import Thread
from ..execution.execution_state import ExecutionState
import logging as lg
_logger = lg.getLogger(__name__)

class WorkerDecommissioner(Thread):
    
    def __init__(self, execution_manager):
        super(WorkerDecommissioner, self).__init__()
        self.execution_manager = execution_manager
        self.workers_manager = execution_manager.workers_manager
        self.stopped = False
        
    def run(self):
        while not self.stopped:
            for worker in self.workers_manager.list_decommissioning_workers():
                self._decommission_worker(worker)
                self.workers_manager.on_worker_decommissioned(worker.wid)
    
    def stop(self):
        self.stopped = True

    def _decommission_worker(self, worker):
        worker_tasks = self.execution_manager.get_tasks(worker = worker.wid) 
          
        while(not self.stopped):
            # While the worker is up, wait for the tasks to finish
            # check if the worker is still alive
            try:
                worker.status()
            except:
                break
            # check how many tasks are still alive
            for task in worker_tasks:
                if (task.state in [ExecutionState.FINISHED, ExecutionState.FAILED]):
                    worker_tasks.pop(task)
            
            if (len(worker_tasks) == 0):
                break
            else:
                time.sleep(1.0)
    
        # check if all tasks actually
        if (len(worker_tasks) == 0):
            _logger.info("Worker [ %s ] have no more active tasks.", worker.wid)
        else:
            _logger.info("Worker [ %s ] still have %s active tasks.", worker.wid, len(worker_tasks))
            # reschedule the tasks
            for task_exec in worker_tasks:
                self.execution_manager.task_reset(task_exec.task.tid)
