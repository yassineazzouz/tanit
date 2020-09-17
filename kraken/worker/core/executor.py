#!/usr/bin/env python
# encoding: utf-8

import time
import Queue
import logging as lg
import os.path as osp
from threading import Thread, Lock
from ...client.client_factory import ClientFactory
from ...common.core.exception import KrakenError

_logger = lg.getLogger(__name__)

class Executor(Thread):
    
    def __init__(self, eid, cqueue, factory):
        super(Executor, self).__init__()
        self.cqueue = cqueue
        self.eid = eid
        self.stopped = False
        self.idle = True

        '''
        Apparently Thrift clients are not thread safe, so can not use without synchronization
        synchronizing the client calls with locks will came with performance penalty
        the best approach seems to be, using a separe thrift client (connection) per thread.
        '''
        self.master = factory.create_client('master-worker')
        
        self.current_task = None
        
    def stop(self):
        self.stopped = True
    
    def run(self):
        # start the client
        self.master.start()
        
        while True:
            self.idle = True
            try:
                task = self.cqueue.get(timeout=0.5)
                self.idle = False
                self.master.task_start(str(task.tid))
                self.current_task = task
                self._run(task)
                self.master.task_success(str(task.tid))
                self.cqueue.task_done()
            except Queue.Empty:
                if (self.stopped):
                    break
                time.sleep(0.5)
            except Exception as e:
                _logger.error("Error executing task [%s]", str(task.tid))
                _logger.error(e, exc_info=True)
                self.master.task_failure(str(task.tid))
                self.cqueue.task_done()
        
        self.master.stop()

    def isIdle(self):
        return self.idle

    def _run(self, task):

      # Can cache the clients in the engine
      src = ClientFactory.getInstance().get_client(task.src)
      dst = ClientFactory.getInstance().get_client(task.dest)
      
      checksum = task.checksum
      overwrite = task.force
      preserve = False
      buffer_size = task.buffer_size
      chunk_size = task.part_size
      
      _src_path = task.src_path
      _dst_path = task.dest_path
      _tmp_path = ""


      lock = Lock()
      stat_lock = Lock()

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
          raise KrakenError('Destination file exist and Missing overwrite parameter.')
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