#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
from Queue import Queue
from threading import Thread

import logging as lg
_logger = lg.getLogger(__name__)

class SimpleScheduler(object):
    """
    Manages Job objects and the waiting between job executions
    """
    def __init__(self, dispatcher):
        self.dispatcher = dispatcher
        self.queue = Queue()
        self.stopped = False


    def _run(self):
        while True:
            if (not self.queue.empty()):
                task = self.queue.get()
                _logger.debug("Scheduling next task %s for execution.", task.tid)
                self.dispatcher.submit(task)
                self.queue.task_done()
            else:
                _logger.debug("No new tasks to schedule, sleeping for %s seconds...", 2)
                time.sleep(2)
            if (self.stopped and self.queue.empty()):
                _logger.debug("No new tasks to schedule, terminating scheduler thread.")
                return

    def submit(self, job):
        if (self.stopped):
            return
        
        for task in job.tasks:
            self.queue.put(task)
        _logger.info("Scheduling %s tasks for execution in job [ %s ].", len(job.tasks) ,job.jid)

    def start(self):
        _logger.info("Stating kraken scheduler.")
        self.daemon = Thread(target=self._run, args=())
        self.daemon.setDaemon(True)
        self.daemon.start()
        _logger.info("Kraken scheduler started.")

    def stop(self):
        _logger.info("Stopping kraken scheduler.")
        self.stopped = True
        self.daemon.join()