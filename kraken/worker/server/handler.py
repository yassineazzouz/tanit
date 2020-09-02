#!/usr/bin/env python

import logging as lg
from ...common.model.task import Task
from ..thrift.ttypes import WorkerStatus

_logger = lg.getLogger(__name__)

class WorkerServiceHandler(object):

    def __init__(self, worker):
        self.worker = worker

    def submit(self, task):
        self.worker.submit(
            Task( task.tid,
                  task.src,
                  task.dest,
                  task.src_path,
                  task.dest_path,
                  task.include_pattern,
                  task.min_size,
                  task.preserve,
                  task.force,
                  task.checksum,
                  task.files_only,
                  task.part_size,
                  task.buffer_size)
        )

    def worker_status(self):
        status = self.worker.get_stats()
        return WorkerStatus( 
                   status.wid,
                   status.running,
                   status.pending,
                   status.available)
