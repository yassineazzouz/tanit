#!/usr/bin/env python

from ..core.master import Master
from .worker import LocalWorker

import logging as lg

_logger = lg.getLogger(__name__)

class StandaloneMaster(Master):
    def __init__(self):
        super(StandaloneMaster, self).__init__()
        self.worker = LocalWorker(self)
        
    def start(self):
        super(StandaloneMaster, self).start()
        _logger.info("Starting new local Worker [ %s ].", self.worker.wid)
        self.dispatcher.register_worker(self.worker)
        _logger.info("Worker [ %s ] started.", self.worker.wid)
        
    def stop(self):
        super(StandaloneMaster, self).stop()
        _logger.info("Stopping new local Worker [ %s ].", self.worker.wid)
        self.worker.stop()
        _logger.info("Worker [ %s ] stopped.", self.worker.wid)
    
    def register_worker(self, worker):
        raise UnsupportedOperationException("Standalone master does not support registering external workers.")
    
class UnsupportedOperationException(Exception):
    pass