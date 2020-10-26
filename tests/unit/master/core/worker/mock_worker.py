from tanit.common.model.worker import WorkerStatus
from tanit.master.core.worker.worker import WorkerIFace
from tanit.master.core.worker.worker_factory import WorkerFactory


class MockWorker(WorkerIFace):
    def __init__(self, wid, cores=10):
        super(MockWorker, self).__init__(wid, None, None)

        self.tasks = []
        self.cores = cores

    def start(self):
        super(MockWorker, self).start()

    def stop(self):
        super(MockWorker, self).stop()

    def submit(self, task):
        self.tasks.append(task)

    def register_filesystem(self, name, filesystem):
        pass

    def status(self):
        return WorkerStatus(
            self.wid,
            len(self.tasks),
            0 if len(self.tasks) < self.cores else len(self.tasks) - self.cores,
            0 if self.cores < len(self.tasks) else self.cores - len(self.tasks),
        )


class MockWorkerFactory(WorkerFactory):
    def create_worker(self, worker):
        return MockWorker(worker.wid, worker.cores)
