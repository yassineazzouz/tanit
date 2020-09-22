
from kraken.master.core.worker.worker_manager import WorkerManager
from kraken.master.core.worker.worker import WorkerIFace
from kraken.common.model.worker import WorkerStatus

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
    
    def start(self):
        pass
    
    def stop(self):
        pass
    
    def register_worker(self, worker):
        self.live_workers.append(worker)

    def list_live_workers(self):
        return self.live_workers
    
    def all_tasks(self):
        tasks = []
        for worker in self.live_workers:
            tasks.extend(worker.tasks)
        return tasks