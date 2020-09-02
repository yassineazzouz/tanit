
import time
import os
import os.path as osp
import logging as lg
from pywhdfs.config import WebHDFSConfig
from pywhdfs.utils import hglob
from pywhdfs.utils.utils import HdfsError

from ...common.model.task import Task
from ...common.core.engine import Engine

_logger = lg.getLogger(__name__)

class TaskExecution(object):
    '''TaskExecution represent the execution flow of a task in the master'''
    
    def __init__(self, task, job):
        self.state = "SUBMITTED"
        self.task = task
        self.job = job
    
    def on_start(self):
        self.state =     "RUNNING"
        self.job.on_task_start(self.task.tid)

    def on_finish(self):
        self.state = "FINISHED"
        self.job.on_task_finish(self.task.tid)

    def on_fail(self):
        self.state = "FAILED"
        self.job.on_task_fail(self.task.tid)

class JobExecution(object):
    '''JobExecution represent the execution flow of a job in the master'''

    def __init__(self, job):
        self.state = "SUBMITTED"
        self.submission_time = time.strftime("%Y-%m-%d %H:%M:%S")

        self.tasks = {}
        
        self.started_tasks = 0
        self.finished_tasks = 0
        self.failed_tasks = 0

        self.job = job


    def on_task_start(self, tid):
        self.started_tasks += 1
        if (self.state == "SUBMITTED"):
            _logger.info("Job [ %s ] execution started.", self.job.jid)
        if (self.state != "FAILED"):
            self.state = "RUNNING"

    def on_task_finish(self, tid):
        self.finished_tasks += 1
        if (self.finished_tasks == len(self.tasks)):
            _logger.info("Job [ %s ] execution finished.", self.job.jid)
            self.state = "FINISHED"
            self.finish_time = time.strftime("%Y-%m-%d %H:%M:%S")

    def on_task_fail(self, tid):
        self.failed_tasks += 1
        if (self.state != "FAILED"):
            self.state = "FAILED"
            _logger.info("Job [ %s ] execution failed.", self.job.jid)
        
    def get_state(self):
        return self.state
    
    def setup(self):

        src = Engine.getInstance().get_client(self.job.src)
        dst = Engine.getInstance().get_client(self.job.dest)
        
        src_path = self.job.src_path
        dst_path = self.job.dest_path
        chunk_size = self.job.part_size
        overwrite = self.job.force
        
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
                pstatus = dst.status(osp.dirname(dst_path),strict=True)
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

        fpath_tuples = []
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
            tid = "{}-task-{}".format(self.job.jid, i)
            i+=1
            task = Task(tid,
                self.job.src,
                self.job.dest,
                fpath,
                osp.join(copy_tuple['dst_path'], fpath[offset:].replace(os.sep, '/')).rstrip(os.sep),
                self.job.include_pattern,
                self.job.min_size,
                self.job.preserve,
                self.job.force,
                self.job.checksum,
                self.job.files_only,
                self.job.part_size,
                self.job.buffer_size)
            self.tasks[tid] = TaskExecution(task, self)