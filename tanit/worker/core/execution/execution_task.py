import abc
import logging as lg
import os.path as osp
import time
from threading import Lock

import six

from ....common.core.exception import TanitError
from ....common.utils.utils import str2bool
from ....filesystem.filesystem_factory import FileSystemFactory

_logger = lg.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class TaskExecution(object):
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
        self.fail = str2bool(params["fail"]) if "fail" in params else False
        self.sleep = float(params["sleep"]) if "sleep" in params else 2.0

    def run(self):
        time.sleep(self.sleep)
        if self.fail:
            raise TaskFailedException("Task forced failure.")
        return


class CopyTaskExecution(TaskExecution):
    def initialize(self, params):

        if "src" in params:
            self.src = params["src"]
        else:
            raise TaskInitializationException(
                "missing required copy job parameter 'src'"
            )

        if "dst" in params:
            self.dst = params["dst"]
        else:
            raise TaskInitializationException(
                "missing required copy job parameter 'dst'"
            )

        if "src_path" in params:
            self.src_path = params["src_path"]
        else:
            raise TaskInitializationException(
                "missing required copy job parameter 'src_path'"
            )

        if "dest_path" in params:
            self.dest_path = params["dest_path"]
        else:
            raise TaskInitializationException(
                "missing required copy job parameter 'dest_path'"
            )

        self.force = str2bool(params["force"]) if "force" in params else False
        self.overwrite = (
            str2bool(params["overwrite"]) if "overwrite" in params else True
        )
        self.checksum = str2bool(params["checksum"]) if "checksum" in params else False
        self.chunk_size = int(params["chunk_size"]) if "chunk_size" in params else 65536
        self.buffer_size = (
            int(params["buffer_size"]) if "buffer_size" in params else 65536
        )

    def run(self):
        # Can cache the clients in the engine
        src = FileSystemFactory.getInstance().get_filesystem(self.src)
        dst = FileSystemFactory.getInstance().get_filesystem(self.dst)

        _src_path = self.src_path
        _dst_path = self.dest_path
        _tmp_path = ""

        lock = Lock()

        skip = False

        dst_st = dst.status(_dst_path, strict=False)

        if dst_st is None:
            # destination does not exist
            _tmp_path = _dst_path
        else:
            if not self.force:
                if self.checksum is True:
                    _src_path_checksum = src.checksum(_src_path)
                    _dst_path_checksum = dst.checksum(_dst_path)
                    if _src_path_checksum != _dst_path_checksum:
                        _logger.info(
                            "source and destination files does not seems "
                            + "to have the same checksum value, copying..."
                        )
                        skip = False
                    else:
                        _logger.info(
                            "source %r and destination %r seems to be identical, "
                            + "skipping.",
                            _src_path,
                            _dst_path,
                        )
                        skip = True
                else:
                    # By default check only the file sizes
                    if (
                        src.status(_src_path)["length"]
                        == dst.status(_dst_path)["length"]
                    ):
                        _logger.info(
                            "source %r and destination %r seems to have the same size, "
                            + "skipping.",
                            _src_path,
                            _dst_path,
                        )
                        skip = True
                    else:
                        _logger.info(
                            "source and destination files does not seems "
                            + "to have the same size, copying."
                        )
                        skip = False
            else:
                # force will always force the copy
                skip = False

            # destination exist
            if not self.overwrite and not skip:
                raise TanitError(
                    "Destination file exist and Missing overwrite parameter."
                )
            _tmp_path = "%s.temp-%s" % (_dst_path, int(time.time()))

        if not skip:
            # Prevent race condition when creating directories
            with lock:
                if dst.status(osp.dirname(_tmp_path), strict=False) is None:
                    _logger.debug(
                        "Parent directory %r does not exist, creating recursively.",
                        osp.dirname(_tmp_path),
                    )
                    curpath = ""
                    root_dir = None
                    for dirname in osp.dirname(_tmp_path).strip("/").split("/"):
                        curpath = "/".join([curpath, dirname])
                        if dst.status(curpath, strict=False) is None:
                            if root_dir is not None:
                                root_dir = curpath
                            dst.mkdir(curpath)

            _logger.info("Copying %r to %r.", _src_path, _tmp_path)

            kwargs = {}

            with src.read(
                _src_path, chunk_size=self.chunk_size, buffer_size=self.buffer_size
            ) as _reader:
                dst.write(_tmp_path, _reader, buffer_size=self.buffer_size, **kwargs)

            if _tmp_path != _dst_path:
                _logger.info(
                    "Copy of %r complete. Moving from %r to %r.",
                    _src_path,
                    _tmp_path,
                    _dst_path,
                )
                dst.delete(_dst_path)
                dst.rename(_tmp_path, _dst_path)
            else:
                _logger.info("Copy of %r to %r complete.", _src_path, _dst_path)

            return {"status": "copied", "src_path": _src_path, "dest_path": _dst_path}
        else:
            _logger.info("Skipping copy %r to %r.", _src_path, _dst_path)
            return {"status": "skipped", "src_path": _src_path, "dest_path": _dst_path}


class TaskInitializationException(Exception):
    pass


class TaskFailedException(Exception):
    pass
