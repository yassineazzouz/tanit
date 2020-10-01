class Worker(object):
    def __init__(self, wid, address, port):
        self.wid = wid
        self.address = address
        self.port = port

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if self.wid == other.wid:
                if self.address == other.address:
                    if self.port == other.port:
                        return True
                    else:
                        return False
                else:
                    return False
            else:
                return False
        else:
            return False

    def __str__(self):
        return "Worker { id: %s, address: %s, port: %s}" % (
            self.wid,
            self.address,
            self.port,
        )


class WorkerStatus(object):
    def __init__(self, wid, running, pending, available):
        self.wid = wid
        self.running = running
        self.pending = pending
        self.available = available
