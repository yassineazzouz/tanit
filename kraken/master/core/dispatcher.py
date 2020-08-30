#!/usr/bin/env python
# The execution engine controls the execution environement
# encoding: utf-8

import time
from Queue import Queue
from multiprocessing.pool import ThreadPool
from threading import Thread

max_concurrency = 25

import logging as lg
_logger = lg.getLogger(__name__)

class ThreadPoolDispatcher(object):

    def __init__(self, engine):
        self.pool = ThreadPool(max_concurrency)
        self.queue = Queue()
        self.engine = engine
        self.running = []
        self.stopped = False

    def _run(self):
        while True:
            if (not self.queue.empty()):
                task = self.queue.get()
                _logger.debug("Dispatching next task [ %s ] for execution.", task.tid)
                self.run_task(task)
            else:
                _logger.debug("No new tasks to dispatch, sleeping for %s seconds...", 2)
                time.sleep(2)
            if (self.stopped and self.queue.empty()):
                _logger.debug("No new tasks to dispatch, terminating dispatcher thread.")
                return

    def submit(self, task):
        self.queue.put(task)

    def run_task(self, task):
        def _run(task):
            _logger.debug("Start Running task [ %s ]", task.tid)
            task.on_start()
            try:
                self.engine.run_task(task)
                _logger.debug("Finished Running task [ %s ]", task.tid)
                task.on_finish()
            except Exception:
                _logger.exception("Failed Running task [ {} ]".format(task.tid))
                task.on_fail()

        self.pool.map_async(_run, (task,))

    def start(self):
        _logger.info("Stating kraken dispatcher.")
        self.daemon = Thread(target=self._run, args=())
        self.daemon.setDaemon(True)
        self.daemon.start()
        _logger.info("Kraken dispatcher started.")

    def stop(self):
        _logger.info("Stopping kraken dispatcher.")
        self.stopped = True
        self.daemon.join()