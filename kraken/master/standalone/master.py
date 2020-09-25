#!/usr/bin/env python

from ..core.master import Master
from ...common.model.worker import Worker

import logging as lg

_logger = lg.getLogger(__name__)

class StandaloneMaster(Master):
        
    def start(self):
        super(StandaloneMaster, self).start()
        _logger.info("Registering local Worker.")
        self.workers_manager.register_worker(Worker("local-worker", None, None))
        _logger.info("Local worker Registered.")
    
    def register_worker(self, worker):
        raise UnsupportedOperationException("Standalone master does not support registering external workers.")
    
class UnsupportedOperationException(Exception):
    pass