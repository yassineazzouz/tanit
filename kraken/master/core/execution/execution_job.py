#!/usr/bin/env python

import abc
import os
import os.path as osp
from datetime import datetime
from hashlib import md5
import logging as lg
from threading import Lock
from .execution_state import ExecutionState

from ....common.utils import glob
from ....common.utils.utils import str2bool
from ....common.core.exception import KrakenError
from ....common.model.execution_type import ExecutionType


_logger = lg.getLogger(__name__)

class TaskExecution(object):
    '''TaskExecution represent the execution flow of a task in the master'''
    
    def __init__(self, tid, etype, params, job):
        self.state = ExecutionState.SUBMITTED
        
        self.tid = tid
        self.etype = etype
        self.params = params
        self.job = job
        self.worker = None
        
    def on_schedule(self):
        if (self.state not in [ ExecutionState.SUBMITTED, ExecutionState.SCHEDULED ]):
            raise IllegalStateTransitionException("Can not transition from state %s to state %s",
                                                  ExecutionState._VALUES_TO_NAMES[self.state],
                                                  ExecutionState._VALUES_TO_NAMES[ExecutionState.SCHEDULED])
        self.state = ExecutionState.SCHEDULED
        self.job.on_task_schedule(self.tid)

    def on_dispatch(self, worker = None):
        if (self.state not in [ ExecutionState.DISPATCHED, ExecutionState.SCHEDULED ]):
            raise IllegalStateTransitionException("Can not transition from state %s to state %s",
                                                  ExecutionState._VALUES_TO_NAMES[self.state],
                                                  ExecutionState._VALUES_TO_NAMES[ExecutionState.DISPATCHED])
        self.state = ExecutionState.DISPATCHED
        self.worker = worker
        self.job.on_task_dispatch(self.tid)

    def on_start(self):
        if (self.state not in [ ExecutionState.RUNNING, ExecutionState.DISPATCHED, ExecutionState.FAILED ]):
            raise IllegalStateTransitionException("Can not transition from state %s to state %s",
                                                  ExecutionState._VALUES_TO_NAMES[self.state],
                                                  ExecutionState._VALUES_TO_NAMES[ExecutionState.RUNNING])
        self.state = ExecutionState.RUNNING
        self.job.on_task_start(self.tid)

    def on_finish(self):
        if (self.state not in [ ExecutionState.RUNNING, ExecutionState.FINISHED ]):
            raise IllegalStateTransitionException("Can not transition from state %s to state %s",
                                                  ExecutionState._VALUES_TO_NAMES[self.state],
                                                  ExecutionState._VALUES_TO_NAMES[ExecutionState.RUNNING])
        self.state = ExecutionState.FINISHED
        self.job.on_task_finish(self.tid)

    def on_fail(self):
        if (self.state not in [ ExecutionState.RUNNING, ExecutionState.FAILED ]):
            raise IllegalStateTransitionException("Can not transition from state %s to state %s",
                                                  ExecutionState._VALUES_TO_NAMES[self.state],
                                                  ExecutionState._VALUES_TO_NAMES[ExecutionState.RUNNING])
        self.state = ExecutionState.FAILED
        self.job.on_task_fail(self.tid)
        
    def on_reset(self):
        self.job.on_task_reset(self.tid)

    def reset(self):
        self.state = ExecutionState.SUBMITTED
        self.worker = None

class IllegalStateTransitionException(Exception):
    pass

class JobExecution(object):
    __metaclass__ = abc.ABCMeta
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

    def __init__(self, params):
        
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

        self.jlock = Lock()
        
        self.initialize(params)

    @abc.abstractmethod
    def initialize(self, params):
        return
    
    @abc.abstractmethod
    def setup(self):
        return
        
    def add_task(self, tid, params):
        self.tasks[tid] = TaskExecution(tid, self.etype, params, self)

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
                _logger.info("Job [ %s ] execution scheduled.", self.jid)
                self.schedule_time = datetime.now()
                self.state = ExecutionState.SCHEDULED

    def on_task_dispatch(self, tid):
        with self.jlock:
            self.dispatched_tasks += 1
            # Only the first task transition the state of the job
            if (self.state == ExecutionState.SCHEDULED and self.dispatched_tasks == 1):
                _logger.info("Job [ %s ] execution dispatched.", self.jid)
                self.dispatch_time = datetime.now()
                self.state = ExecutionState.DISPATCHED

    def on_task_start(self, tid):
        with self.jlock:
            self.started_tasks += 1
            # Only the first task transition the state of the job
            if (self.state == ExecutionState.DISPATCHED and self.started_tasks == 1):
                _logger.info("Job [ %s ] execution started.", self.jid)
                self.start_time = datetime.now()
                self.state = ExecutionState.RUNNING

    def on_task_finish(self, tid):
        with self.jlock:
            self.finished_tasks += 1
            # All tasks need to finish (successfully) to consider the job finished
            if (self.finished_tasks == len(self.tasks)):
                _logger.info("Job [ %s ] execution finished.", self.jid)
                self.state = ExecutionState.FINISHED
                self.finish_time = datetime.now()
                self.execution_time_s = (self.finish_time - self.start_time).total_seconds()

    def on_task_fail(self, tid):
        with self.jlock:
            self.failed_tasks += 1
            if (self.state != ExecutionState.FAILED):
                self.state = ExecutionState.FAILED
                _logger.info("Job [ %s ] execution failed.", self.jid)

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

class UploadJobExecution(JobExecution):
    # not implemented yet
    pass

class MockJobExecution(JobExecution):

    def initialize(self, params):
        self.num_tasks = int(params["num_tasks"]) if "num_tasks" in params else 2
        self.jid = "mock-job-%s" % str(datetime.now().strftime("%d%m%Y%H%M%S"))
        
        self.etype = ExecutionType.MOCK

    def setup(self):
        for i in range(self.num_tasks):
            self.add_task("{}-task-{}".format(self.jid, i), {})
    
class CopyJobExecution(JobExecution):
    
    def initialize(self, params):
        
        if "src" in params:
            self.src = params["src"]
        else:
            raise JobInitializationException("missing required copy job parameter 'src'")

        if "dst" in params:
            self.dst = params["dst"]
        else:
            raise JobInitializationException("missing required copy job parameter 'dst'")

        if "src_path" in params:
            self.src_path = params["src_path"]
        else:
            raise JobInitializationException("missing required copy job parameter 'src_path'")

        if "dest_path" in params:
            self.dest_path = params["dest_path"]
        else:
            raise JobInitializationException("missing required copy job parameter 'dest_path'")
        
        self.include_pattern = params["include_pattern"] if "include_pattern" in params else "*"
        self.min_size = int(params["min_size"]) if "min_size" in params else 0
        self.preserve = str2bool(params["preserve"]) if "preserve" in params else True
        self.force = str2bool(params["force"]) if "force" in params else True
        self.checksum = str2bool(params["checksum"]) if "checksum" in params else True
        self.files_only = str2bool(params["files_only"]) if "files_only" in params else True
        self.part_size = int(params["part_size"]) if "part_size" in params else 65536
        self.buffer_size = int(params["buffer_size"]) if "buffer_size" in params else 65536

        self.jid = "job-%s-%s" % (
            md5("{}-{}-{}-{}".format(self.src, self.src_path, self.dst, self.dest_path)).hexdigest(),
            str(datetime.now().strftime("%d%m%Y%H%M%S"))
        )
        
        self.etype = ExecutionType.COPY

    def setup(self):
        
        from ....client.client_factory import ClientFactory

        src = ClientFactory.getInstance().get_client(self.src)
        dst = ClientFactory.getInstance().get_client(self.dst)
        
        src_path = self.src_path
        dst_path = self.dest_path
        chunk_size = self.part_size
        overwrite = self.force
        
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
                self.add_task(
                    "{}-task-{}".format(self.jid, i),
                    params = {
                        "src" : str(self.src),
                        "dst" : str(self.dst),
                        "src_path" : str(fpath),
                        "dest_path" : str(osp.join(copy_tuple['dst_path'], fpath[offset:].replace(os.sep, '/')).rstrip(os.sep)),
                        "include_pattern" : str(self.include_pattern),
                        "min_size" : str(self.min_size),
                        "preserve" : str(self.preserve),
                        "force" : str(self.force),
                        "checksum" : str(self.checksum),
                        "files_only" : str(self.files_only),
                        "part_size" : str(self.part_size),
                        "buffer_size" : str(self.buffer_size)
                    }
                )
                i+=1

class JobInitializationException(Exception):
    pass