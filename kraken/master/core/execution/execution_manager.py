
import os
import os.path as osp
from datetime import datetime
from pywhdfs.config import WebHDFSConfig
from pywhdfs.utils import hglob
from pywhdfs.utils.utils import HdfsError

from .execution_state import ExecutionState
from .execution_job import JobExecution
from ....common.model.task import Task
from ....common.core.engine import Engine

import logging as lg

_logger = lg.getLogger(__name__)

class ExecutionManager(object):
    '''The ExecutionManager manage the state of execution of jobs and tasks'''
    
    def __init__(self):
        self.jobs = []
        
    def submit_job(self, job):
        
        job_exec = JobExecution(job)

        src = Engine.getInstance().get_client(job_exec.job.src)
        dst = Engine.getInstance().get_client(job_exec.job.dest)
        
        src_path = job_exec.job.src_path
        dst_path = job_exec.job.dest_path
        chunk_size = job_exec.job.part_size
        overwrite = job_exec.job.force
        
        if not chunk_size:
            raise ValueError('Copy chunk size must be positive.')

        # Normalise src and dst paths
        src_path = src.resolvepath(src_path)
        dst_path = dst.resolvepath(dst_path)

        # First, resolve the list of src files/directories to be copied
        copies = [ copy_file for copy_file in hglob.glob(src, src_path) ]
     
        # need to develop a propper pattern based access function
        if len(copies) == 0:
            raise HdfsError('Cloud not resolve source path %s, either it does not exist or can not access it.', src_path)

        tuples = []
        for copy in copies:
            copy_tuple = dict()
            try:
                #filename = osp.basename(copy)
                #dst_base_path =  osp.join( dst_path, filename )
                status = dst.status(dst_path,strict=True)
                #statuses = [status for _, status in dst.list(dst_base_path)]
            except HdfsError as err:
                if 'File does not exist' in str(err):
                    # Remote path doesn't exist.
                    # check if parent exist
                    try:
                        dst.status(osp.dirname(dst_path),strict=True)
                    except HdfsError, err:
                        raise HdfsError('Parent directory of %r does not exist.', dst_path)
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
                        raise HdfsError('Destination path %r already exists.', dst_path)
                    # the file is going to be deleted and the destination is going to be created with the same name
                    dst_base_path = dst_path
                else:
                    # Remote path exists and is a directory.
                    try:
                        status = dst.status(osp.join( dst_path, osp.basename(copy) ),strict=True)
                    except HdfsError as err:
                        if 'File does not exist' in str(err):
                            # destination does not exist, great !
                            dst_base_path =  osp.join( dst_path, osp.basename(copy) )
                            pass
                    else:
                        # destination exists
                        dst_base_path = osp.join( dst_path, osp.basename(copy))
                        if not overwrite:
                            raise HdfsError('Destination path %r already exists.', dst_base_path)

                copy_tuple = dict({ 'src_path' : copy, 'dst_path' : dst_base_path})
            finally:
                tuples.append(copy_tuple)

        # This is a workaround for a Bug when copying files using a pattern
        # it may happen that files can have the same name:
        # ex : /home/user/test/*/*.py may result in duplicate files
        for i in range(0, len(tuples)):
            for x in range(i + 1, len(tuples)):
                if tuples[i]['dst_path'] == tuples[x]['dst_path']:
                    raise HdfsError('Conflicting files %r and %r : can\'t copy both files to %r'
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
                tid = "{}-task-{}".format(job_exec.job.jid, i)
                i+=1
                task = Task(tid,
                  job_exec.job.src,
                  job_exec.job.dest,
                  fpath,
                  osp.join(copy_tuple['dst_path'], fpath[offset:].replace(os.sep, '/')).rstrip(os.sep),
                  job_exec.job.include_pattern,
                  job_exec.job.min_size,
                  job_exec.job.preserve,
                  job_exec.job.force,
                  job_exec.job.checksum,
                  job_exec.job.files_only,
                  job_exec.job.part_size,
                  job_exec.job.buffer_size)

                job_exec.add_task(task)

        self.jobs.append(job_exec)
    
    def list_jobs(self):
        return self.jobs
    
    def get_job(self, jid):
        for job_exec in self.jobs:
            if job_exec.job.jid == jid:
                return job_exec
        return None
    
    def get_tasks(self, jid = None, states = [], worker = None):
        target_jobs = []
        if jid != None:
            job = self.get_job(jid)
            if (job != None):
                target_jobs.append(job)
            else:
                return []
        else:
            target_jobs = self.jobs
        
        target_tasks = []
        for job in target_jobs:
            for task in job.get_tasks():
                valid = True
                if len(states) != 0:
                    if task.state not in states:
                        valid = False
                if worker != None:
                    if (task.worker != worker):
                        valid = False
                if valid:
                    target_tasks.append(task)
        return target_tasks
        
        

    def get_task(self, tid):
        for job_exec in self.jobs:
            task = job_exec.get_task(tid)
            if (task != None):
                return task
        return None

    def task_schedule(self, tid):
        task = self.get_task(tid)
        if (task != None):
            task.state = ExecutionState.SCHEDULED
            job = task.job
            with job.jlock:
                job.scheduled_tasks += 1
                # Only the first task transition the state of the job
                if (job.state == ExecutionState.SUBMITTED and job.scheduled_tasks == 1):
                    _logger.info("Job [ %s ] execution scheduled.", job.job.jid)
                    job.schedule_time = datetime.now()
                    job.state = ExecutionState.SCHEDULED
                
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)

    def task_dispatch(self, tid, worker = None):
        task = self.get_task(tid)
        if (task != None):
            task.state = ExecutionState.DISPATCHED
            task.worker = worker
            
            job = task.job
            with job.jlock:
                job.dispatched_tasks += 1
                # Only the first task transition the state of the job
                if (job.state == ExecutionState.SCHEDULED and job.dispatched_tasks == 1):
                    _logger.info("Job [ %s ] execution dispatched.", job.job.jid)
                    job.dispatch_time = datetime.now()
                    job.state = ExecutionState.DISPATCHED
            
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)
        
    def task_start(self, tid):
        task = self.get_task(tid)
        if (task != None):
            task.state = ExecutionState.RUNNING
            
            job = task.job
            with job.jlock:
                job.started_tasks += 1
                # Only the first task transition the state of the job
                if (job.state == ExecutionState.DISPATCHED and job.started_tasks == 1):
                    _logger.info("Job [ %s ] execution started.", job.job.jid)
                    job.start_time = datetime.now()
                    job.state = ExecutionState.RUNNING
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)
      
    def task_finish(self, tid):
        task = self.get_task(tid)
        if (task != None):
            task.state = ExecutionState.FINISHED
            
            job = task.job
            with job.jlock:
                job.finished_tasks += 1
                # All tasks need to finish (successfully) to consider the job finished
                if (job.finished_tasks == len(job.tasks)):
                    _logger.info("Job [ %s ] execution finished.", job.job.jid)
                    job.state = ExecutionState.FINISHED
                    job.finish_time = datetime.now()
                    job.execution_time_s = (job.finish_time - job.start_time).total_seconds()
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)
    
    def task_failure(self, tid):
        task = self.get_task(tid)
        if (task != None):
            task.state = ExecutionState.FAILED
            
            job = task.job
            with job.jlock:
                job.failed_tasks += 1
                if (job.state != ExecutionState.FAILED):
                    job.state = ExecutionState.FAILED
                    _logger.info("Job [ %s ] execution failed.", job.job.jid)
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)

    def task_reset(self, tid):
        task = self.get_task(tid)
        
        if (task != None):
            job = task.job
            with job.jlock:
                if (task.state == ExecutionState.SCHEDULED):
                    job.scheduled_tasks -= 1
                elif (task.state == ExecutionState.DISPATCHED):
                    job.scheduled_tasks -= 1
                    job.dispatched_tasks -= 1
                elif (task.state == ExecutionState.RUNNING):
                    job.scheduled_tasks -= 1
                    job.dispatched_tasks -= 1
                    job.started_tasks -= 1
                elif (task.state == ExecutionState.FINISHED):
                    job.scheduled_tasks -= 1
                    job.dispatched_tasks -= 1
                    job.started_tasks -= 1
                    job.finished_tasks -= 1
                    if (job.state == ExecutionState.FINISHED):
                        job.state = ExecutionState.RUNNING
                elif (task.state == ExecutionState.FAILED):
                    job.scheduled_tasks -= 1
                    job.dispatched_tasks -= 1
                    job.started_tasks -= 1
                    if (job.state == ExecutionState.FAILED and job.failed_tasks == 1):
                        job.state = ExecutionState.RUNNING
                    job.failed_tasks -= 1
    
                job.add_task(task.task)
        else:
            raise NoSuchTaskException("No such task [ %s ]", tid)

class NoSuchJobException(Exception):
    pass

class NoSuchTaskException(Exception):
    pass