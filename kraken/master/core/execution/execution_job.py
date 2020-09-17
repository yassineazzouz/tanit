#!/usr/bin/env python

import os
import os.path as osp
import logging as lg
from datetime import datetime
from threading import Lock
from .execution_state import ExecutionState

from ....common.utils import glob
from ....common.model.task import Task
from ....common.core.exception import KrakenError

_logger = lg.getLogger(__name__)

class TaskExecution(object):
    '''TaskExecution represent the execution flow of a task in the master'''
    
    def __init__(self, task, job):
        self.state = ExecutionState.SUBMITTED
        self.task = task
        self.job = job
        self.worker = None
        
    def on_schedule(self):
        self.state = ExecutionState.SCHEDULED
        self.job.on_task_schedule(self.task.tid)

    def on_dispatch(self, worker = None):
        self.state = ExecutionState.DISPATCHED
        self.worker = worker
        self.job.on_task_dispatch(self.task.tid)

    def on_start(self):
        self.state = ExecutionState.RUNNING
        self.job.on_task_start(self.task.tid)

    def on_finish(self):
        self.state = ExecutionState.FINISHED
        self.job.on_task_finish(self.task.tid)

    def on_fail(self):
        self.state = ExecutionState.FAILED
        self.job.on_task_fail(self.task.tid)
        
    def on_reset(self):
        self.job.on_task_reset(self.task.tid)

    def reset(self):
        self.state = ExecutionState.SUBMITTED
        self.worker = None

class JobExecution(object):
    '''
    JobExecution represent the execution flow of a job in the master
    The job state diagram is as follow:
    ----------------------------------------------------------------------
    |   SUBMITTED --> SCHEDULED --> DISPATCHED --> RUNNING --> FINISHED  |
    |                                                |                   |
    |                                                |                   |
    |                                              FAILED                |
    ---------------------------------------------------------------------|
    '''

    def __init__(self, job):
        
        self.state = ExecutionState.SUBMITTED
        
        self.submission_time = datetime.now()
        self.schedule_time = None
        self.dispatch_time = None
        self.start_time = None
        self.finish_time = None
        self.execution_time_s = -1

        self.tasks = {}
        
        self.scheduled_tasks = 0
        self.dispatched_tasks = 0
        self.started_tasks = 0
        self.finished_tasks = 0
        self.failed_tasks = 0

        self.job = job
        self.jlock = Lock()

    def add_task(self, task):
        self.tasks[task.tid] = TaskExecution(task, self)

    def get_task(self, tid):
        if tid in self.tasks:
            return self.tasks[tid]
        else:
            return None

    def get_tasks(self):
        return self.tasks.values()

    def get_state(self):
        return self.state

    def on_task_schedule(self, tid):
        with self.jlock:
            self.scheduled_tasks += 1
            # Only the first task transition the state of the job
            if (self.state == ExecutionState.SUBMITTED and self.scheduled_tasks == 1):
                _logger.info("Job [ %s ] execution scheduled.", self.job.jid)
                self.schedule_time = datetime.now()
                self.state = ExecutionState.SCHEDULED

    def on_task_dispatch(self, tid):
        with self.jlock:
            self.dispatched_tasks += 1
            # Only the first task transition the state of the job
            if (self.state == ExecutionState.SCHEDULED and self.dispatched_tasks == 1):
                _logger.info("Job [ %s ] execution dispatched.", self.job.jid)
                self.dispatch_time = datetime.now()
                self.state = ExecutionState.DISPATCHED

    def on_task_start(self, tid):
        with self.jlock:
            self.started_tasks += 1
            # Only the first task transition the state of the job
            if (self.state == ExecutionState.DISPATCHED and self.started_tasks == 1):
                _logger.info("Job [ %s ] execution started.", self.job.jid)
                self.start_time = datetime.now()
                self.state = ExecutionState.RUNNING

    def on_task_finish(self, tid):
        with self.jlock:
            self.finished_tasks += 1
            # All tasks need to finish (successfully) to consider the job finished
            if (self.finished_tasks == len(self.tasks)):
                _logger.info("Job [ %s ] execution finished.", self.job.jid)
                self.state = ExecutionState.FINISHED
                self.finish_time = datetime.now()
                self.execution_time_s = (self.finish_time - self.start_time).total_seconds()

    def on_task_fail(self, tid):
        with self.jlock:
            self.failed_tasks += 1
            if (self.state != ExecutionState.FAILED):
                self.state = ExecutionState.FAILED
                _logger.info("Job [ %s ] execution failed.", self.job.jid)

    def on_task_reset(self, tid):
        task = self.tasks[tid]
        with self.jlock:
            if (task.state == ExecutionState.SCHEDULED):
                self.scheduled_tasks -= 1
            elif (task.state == ExecutionState.DISPATCHED):
                self.scheduled_tasks -= 1
                self.dispatched_tasks -= 1
            elif (task.state == ExecutionState.RUNNING):
                self.scheduled_tasks -= 1
                self.dispatched_tasks -= 1
                self.started_tasks -= 1
            elif (task.state == ExecutionState.FINISHED):
                self.scheduled_tasks -= 1
                self.dispatched_tasks -= 1
                self.started_tasks -= 1
                self.finished_tasks -= 1
                if (self.state == ExecutionState.FINISHED):
                    self.state = ExecutionState.RUNNING
            elif (task.state == ExecutionState.FAILED):
                self.scheduled_tasks -= 1
                self.dispatched_tasks -= 1
                self.started_tasks -= 1
                if (self.state == ExecutionState.FAILED and self.failed_tasks == 1):
                    self.state = ExecutionState.RUNNING
                self.failed_tasks -= 1
            
            task.reset()

    def setup(self):
        
        from ....client.client_factory import ClientFactory
        
        job = self.job

        src = ClientFactory.getInstance().get_client(job.src)
        dst = ClientFactory.getInstance().get_client(job.dest)
        
        src_path = job.src_path
        dst_path = job.dest_path
        chunk_size = job.part_size
        overwrite = job.force
        
        if not chunk_size:
            raise ValueError('Copy chunk size must be positive.')

        # Normalise src and dst paths
        src_path = src.resolvepath(src_path)
        dst_path = dst.resolvepath(dst_path)

        # First, resolve the list of src files/directories to be copied
        copies = [ copy_file for copy_file in glob.glob(src, src_path) ]
     
        # need to develop a propper pattern based access function
        if len(copies) == 0:
            raise KrakenError('Cloud not resolve source path %s, either it does not exist or can not access it.', src_path)

        tuples = []
        for copy in copies:
            copy_tuple = dict()
            try:
                #filename = osp.basename(copy)
                #dst_base_path =  osp.join( dst_path, filename )
                status = dst.status(dst_path,strict=True)
                #statuses = [status for _, status in dst.list(dst_base_path)]
            except KrakenError as err:
                if 'File does not exist' in str(err):
                    # Remote path doesn't exist.
                    # check if parent exist
                    if (dst.status(osp.dirname(dst_path),strict=False) == None):
                        raise KrakenError('Parent directory of %r does not exist.', dst_path)
                    else:
                        # Remote path does not exist, and parent exist
                        # so we want the source to be renamed as destination
                        # so do not add the basename
                        dst_base_path =  dst_path
                        copy_tuple = dict({ 'src_path' : copy, 'dst_path' : dst_base_path})
            else:
                # Remote path exists.
                if status['type'] == 'FILE':
                    # Remote path exists and is a normal file.
                    if not overwrite:
                        raise KrakenError('Destination path %r already exists.', dst_path)
                    # the file is going to be deleted and the destination is going to be created with the same name
                    dst_base_path = dst_path
                else:
                    # Remote path exists and is a directory.
                    status = dst.status(osp.join( dst_path, osp.basename(copy) ),strict=False)
                    if (status == None):
                        # destination does not exist, great !
                        dst_base_path =  osp.join( dst_path, osp.basename(copy) )
                        pass
                    else:
                        # destination exists
                        dst_base_path = osp.join( dst_path, osp.basename(copy))
                        if not overwrite:
                            raise KrakenError('Destination path %r already exists.', dst_base_path)

                copy_tuple = dict({ 'src_path' : copy, 'dst_path' : dst_base_path})
            finally:
                tuples.append(copy_tuple)

        # This is a workaround for a Bug when copying files using a pattern
        # it may happen that files can have the same name:
        # ex : /home/user/test/*/*.py may result in duplicate files
        for i in range(0, len(tuples)):
            for x in range(i + 1, len(tuples)):
                if tuples[i]['dst_path'] == tuples[x]['dst_path']:
                    raise KrakenError('Conflicting files %r and %r : can\'t copy both files to %r'
                                % (tuples[i]['src_path'], tuples[x]['src_path'], tuples[i]['dst_path']) )


        for copy_tuple in tuples:
            # Then we figure out which files we need to copy, and where.
            src_paths = list(src.walk(copy_tuple['src_path']))
            if not src_paths:
                # This is a single file.
                src_fpaths = [copy_tuple['src_path']]
            else:
                src_fpaths = [
                  osp.join(dpath, fname)
                  for dpath, _, fnames in src_paths
                  for fname in fnames
                ]

            offset = len(copy_tuple['src_path'].rstrip(os.sep)) + len(os.sep)

            i = 1;
            for fpath in src_fpaths:
                tid = "{}-task-{}".format(job.jid, i)
                i+=1
                task = Task(tid,
                  job.src,
                  job.dest,
                  fpath,
                  osp.join(copy_tuple['dst_path'], fpath[offset:].replace(os.sep, '/')).rstrip(os.sep),
                  job.include_pattern,
                  job.min_size,
                  job.preserve,
                  job.force,
                  job.checksum,
                  job.files_only,
                  job.part_size,
                  job.buffer_size)

                self.add_task(task)