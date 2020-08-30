#!/usr/bin/env python
# encoding: utf-8

import time
import os
import logging as lg
import requests as rq
import os.path as osp
from pywhdfs.config import WebHDFSConfig
from pywhdfs.utils import hglob
from pywhdfs.utils.utils import HdfsError
from threading import Lock
from datetime import datetime

from job import Task, Job

_logger = lg.getLogger(__name__)

class Engine(object):
    
    
  def __init__(self):
      self.config = WebHDFSConfig(None)
      self.clients = {}


  def get_client(self, name):
      if name in self.clients:
          return self.clients[name]
      else:
          client = self.config.get_client(name)
          self.clients[name] = client
          return client

  def setup_job(self, job):

    src = self.get_client(job.src_client)
    dst = self.get_client(job.dest_client)
    
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

      tasks = []
      i = 1;
      for fpath in src_fpaths:
        tid = "{}-task-{}".format(job.jid, i)
        i+=1
        tasks.append(Task(tid, job, fpath, osp.join(copy_tuple['dst_path'], fpath[offset:].replace(os.sep, '/')).rstrip(os.sep)))
      job.tasks = tasks

  def run_task(self, task):

      # Can cache the clients in the engine
      src = self.get_client(task.job.src_client)
      dst = self.get_client(task.job.dest_client)
      
      checksum = task.job.checksum
      overwrite = task.job.force
      preserve = task.job.preserve
      buffer_size = task.job.buffer_size
      chunk_size = task.job.part_size
      
      _src_path = task.src_path
      _dst_path = task.dest_path
      _tmp_path = ""


      lock = Lock()
      stat_lock = Lock()
      
      _logger.info('Attempting Copy %r to %r.', _src_path, _dst_path)

      def _preserve(_src_path, _dst_path):
        # set the base path attributes

        srcstats = src.status(_src_path)
        _logger.debug("Preserving %r source attributes on %r" % (_src_path,_dst_path))
        dst.set_owner(_dst_path, owner=srcstats['owner'], group=srcstats['group'])
        dst.set_permission(_dst_path, permission=srcstats['permission'])
        dst.set_times(_dst_path, access_time=srcstats['accessTime'], modification_time=srcstats['modificationTime'])
        if srcstats['type'] == 'FILE':
          dst.set_replication(_dst_path, replication=int(srcstats['replication']))



      skip=False

      dst_st=dst.status(_dst_path,strict=False)

      if dst_st == None:
        # destination does not exist
        _tmp_path=_dst_path
      else:
        # destination exist
        if not overwrite:
          raise HdfsError('Destination file exist and Missing overwrite parameter.')
        _tmp_path = '%s.temp-%s' % (_dst_path, int(time.time()))

        if checksum == True:
          _src_path_checksum = src.checksum(_src_path)
          _dst_path_checksum = dst.checksum(_dst_path)
          if _src_path_checksum['algorithm'] != _dst_path_checksum['algorithm']:
            _logger.info('source and destination files does not seems to have the same block size or crc chunk size.')
          elif _src_path_checksum['bytes'] != _dst_path_checksum['bytes']:
            _logger.info('source destination files does not seems to have the same checksum value.')
          else:
            _logger.info('source %r and destination %r seems to be identical, skipping.', _src_path, _dst_path)
            skip=True
        else:
          _logger.info('no checksum check will be performed, forcing file copy source %r to destination %r.', _src_path, _dst_path)
          #skip=True

      if not skip:
        # Prevent race condition when creating directories
        with lock:
          if dst.status(osp.dirname(_tmp_path), strict=False) is None:
            _logger.debug('Parent directory %r does not exist, creating recursively.', osp.dirname(_tmp_path))
            curpath = ''
            root_dir = None
            for dirname in osp.dirname(_tmp_path).strip('/').split('/'):
              curpath = '/'.join([curpath, dirname])
              if dst.status(curpath, strict=False) is None:
                if root_dir is not None:
                  root_dir = curpath
                dst.makedirs(curpath)
                if preserve:
                  curr_src_path=osp.realpath( osp.join( _src_path,osp.relpath(curpath,_tmp_path)) )
                  _preserve(curr_src_path,curpath)
      
        _logger.info('Copying %r to %r.', _src_path, _tmp_path)

        kwargs = {}
        if preserve:
          srcstats = src.status(_src_path)
          kwargs['replication'] = int(srcstats['replication'])
          kwargs['blocksize'] = int(srcstats['blockSize'])

        with src.read(_src_path, chunk_size=chunk_size, progress=None, buffer_size=buffer_size) as _reader:
          dst.write(_tmp_path, _reader, buffersize=buffer_size, **kwargs)

        if _tmp_path != _dst_path:
          _logger.info( 'Copy of %r complete. Moving from %r to %r.', _src_path, _tmp_path, _dst_path )
          dst.delete(_dst_path)
          dst.rename(_tmp_path, _dst_path)
        else:
          _logger.info(
            'Copy of %r to %r complete.', _src_path, _dst_path
          )
      
        if preserve:
          _preserve(_src_path,_dst_path)

        return { 'status': 'copied', 'src_path': _src_path, 'dest_path' : _dst_path }
      else:
        _logger.info('Skipping copy %r to %r.', _src_path, _tmp_path)
        return { 'status': 'skipped', 'src_path': _src_path, 'dest_path' : _dst_path }
