from .executor import Executor


class ExecutorFactory(object):
    def __init__(self, client_factory, cqueue, concurrency):
        self.client_factory = client_factory
        self.cqueue = cqueue
        self.concurrency = concurrency

    def create_executor(self, eid):
        return Executor(eid, self.cqueue, self.client_factory)
