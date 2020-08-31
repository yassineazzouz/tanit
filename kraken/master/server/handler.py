#!/usr/bin/env python

from Queue import Queue
from ..core.dispatcher import *
from ..core.engine import *
from ..core.scheduler import *
from ..core.worker import *
from ..core.job import *
from ..thrift.ttypes import JobStatus, JobState

import socket

import logging as lg

_logger = lg.getLogger(__name__)

class KrakenServiceHandler(object):

    def __init__(self):
        self.jobs = []

        self.engine = Engine()
        self.dispatcher  = FairDispatcher()
        self.dispatcher.add_worker(ThreadPoolWorker( "worker-%s-1" % socket.gethostname() ,self.engine))
        self.scheduler = SimpleScheduler(self.dispatcher)

        self.dispatcher.start()
        self.scheduler.start()

    def submit_job(self, conf):
        job = Job(conf)
        _logger.info("Received new job [ %s ].", job.jid)
        _logger.info("Configuring job [ %s ].", job.jid)
        self.engine.setup_job(job)
        _logger.info("Scheduling job [ %s ] for execution.", job.jid)
        self.scheduler.submit(job)
        self.jobs.append(job)

    def list_jobs(self):
        status = []
        for job in self.jobs:
            status.append(JobStatus(job.jid, JobState._NAMES_TO_VALUES[job.state], job.submission_time))
        return status

    def job_status(self, jid):
        for job in self.jobs:
            if (job.jid == jid):
                return JobStatus(job.jid, JobState._NAMES_TO_VALUES[job.state], job.submission_time)
