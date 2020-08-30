
import time
from hashlib import md5

import logging as lg
_logger = lg.getLogger(__name__)

class Task(object):
    def __init__(self, tid, job, src_path, dest_path):
        self.state = "SUBMITTED"
        self.tid = tid
        self.job = job
        self.src_path = src_path
        self.dest_path = dest_path

    def on_start(self):
        self.state = "RUNNING"
        self.job.on_task_start(self.tid)

    def on_finish(self):
        self.state = "FINISHED"
        self.job.on_task_finish(self.tid)

    def on_fail(self):
        self.state = "FAILED"
        self.job.on_task_fail(self.tid)

class Job(object):

    def __init__(self, conf):
        self.state = "SUBMITTED"
        self.submission_time = time.strftime("%Y-%m-%d %H:%M:%S")
        self.jid = "job-%s-%s" % (
            md5("{}-{}-{}-{}".format(conf.src, conf.src_path, conf.dest, conf.dest_path)).hexdigest(),
            time.strftime("%d%m%Y%H%M%S")
        )

        self.tasks = []
        self.started_tasks = 0
        self.finished_tasks = 0
        self.failed_tasks = 0

        self.buffer_size = int(conf.buffer_size)
        self.part_size = int(conf.part_size)
        self.include_pattern = conf.include_pattern
        self.min_size = int(conf.min_size)
        self.force = conf.force
        self.checksum = conf.checksum
        self.preserve = conf.preserve
        self.files_only = conf.files_only
        self.src_path = conf.src_path
        self.dest_path = conf.dest_path

        self.src_client = conf.src
        self.dest_client = conf.dest

    def on_task_start(self, tid):
        self.started_tasks += 1
        if (self.state == "SUBMITTED"):
            _logger.info("Job [ %s ] execution started.", self.jid)
        if (self.state != "FAILED"):
            self.state = "RUNNING"

    def on_task_finish(self, tid):
        self.finished_tasks += 1
        if (self.finished_tasks == len(self.tasks)):
            _logger.info("Job [ %s ] execution finished.", self.jid)
            self.state = "FINISHED"
            self.finish_time = time.strftime("%Y-%m-%d %H:%M:%S")

    def on_task_fail(self, tid):
        self.failed_tasks += 1
        if (self.state != "FAILED"):
            self.state = "FAILED"
            _logger.info("Job [ %s ] execution failed.", self.jid)
        
    def get_state(self):
        return self.state