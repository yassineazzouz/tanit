from .executor import Executor


class ExecutorFactory(object):
    def __init__(self, client_factory, cqueue):
        self.client_factory = client_factory
        self.cqueue = cqueue

    def create_executor(self, eid):
        return Executor(eid, self.cqueue, self.client_factory)
