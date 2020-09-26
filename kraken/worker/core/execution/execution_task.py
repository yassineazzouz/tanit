
import abc
import time
import logging as lg
import os.path as osp
from threading import Lock

from ....client.client_factory import ClientFactory
from ....common.core.exception import KrakenError
from ....common.utils.utils import str2bool

_logger = lg.getLogger(__name__)

class TaskExecution(object):
    __metaclass__ = abc.ABCMeta
    
    def __init__(self, tid, params):
        self.tid = tid
        self.params = params

        self.initialize(params)

    @abc.abstractmethod
    def initialize(self, params):
        return

    @abc.abstractmethod
    def run(self):
        return

class UploadTaskExecution(TaskExecution):
    # not implemented yet
    pass

class MockTaskExecution(TaskExecution):

    def initialize(self, params):
        return

    def run(self):
        time.sleep(2.0)
        return

class CopyTaskExecution(TaskExecution):
    
    def initialize(self, params):
        
        if "src" in params:
            self.src = params["src"]
        else:
            raise TaskInitializationException("missing required copy job parameter 'src'")

        if "dst" in params:
            self.dst = params["dst"]
        else:
            raise TaskInitializationException("missing required copy job parameter 'dst'")

        if "src_path" in params:
            self.src_path = params["src_path"]
        else:
            raise TaskInitializationException("missing required copy job parameter 'src_path'")

        if "dest_path" in params:
            self.dest_path = params["dest_path"]
        else:
            raise TaskInitializationException("missing required copy job parameter 'dest_path'")
        
        self.include_pattern = params["include_pattern"] if "include_pattern" in params else "*"
        self.min_size = int(params["min_size"]) if "min_size" in params else 0
        self.preserve = str2bool(params["preserve"]) if "preserve" in params else True
        self.force = str2bool(params["force"]) if "force" in params else True
        self.checksum = str2bool(params["checksum"]) if "checksum" in params else True
        self.files_only = str2bool(params["files_only"]) if "files_only" in params else True
        self.part_size = int(params["part_size"]) if "part_size" in params else 65536
        self.buffer_size = int(params["buffer_size"]) if "buffer_size" in params else 65536
        

    def run(self):

      # Can cache the clients in the engine
      src = ClientFactory.getInstance().get_client(self.src)
      dst = ClientFactory.getInstance().get_client(self.dst)
      
      checksum = self.checksum
      overwrite = self.force
      preserve = False
      buffer_size = self.buffer_size
      chunk_size = self.part_size
      
      _src_path = self.src_path
      _dst_path = self.dest_path
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
    

class TaskInitializationException(Exception):
    pass