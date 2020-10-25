import logging as lg
import os
import re
import time

import s3fs

from ...common.utils.glob import iglob
from ..filesystem import IFileSystem
from ..ioutils import FileSystemError

try:
    FileNotFoundError = FileNotFoundError
except NameError:
    FileNotFoundError = s3fs.core.FileNotFoundError

_logger = lg.getLogger(__name__)


# TODO : change S3FileSystem to boto3 instead of s3fs
class S3FileSystem(IFileSystem):
    def __init__(self, bucket_name, **params):
        self.s3 = s3fs.S3FileSystem(**params)
        self.bucket_name = bucket_name

    def resolve_path(self, path):
        if path == self.bucket_name:
            rpath = self.bucket_name + "/"
        elif not path.startswith(self.bucket_name + "/"):
            if path.startswith("/"):
                rpath = self.bucket_name + path
            else:
                rpath = self.bucket_name + "/" + path
        else:
            rpath = path
        return os.path.normpath(rpath)

    def format_path(self, path):
        if path.startswith(self.bucket_name + "/"):
            return re.sub("^%s" % self.bucket_name, "", path)
        else:
            return path

    def _status(self, path, strict=True):
        def _datetime2timestamp(dt):
            try:
                int(dt.timestamp() * 1000)
            except AttributeError:
                # python 2.7
                int(time.mktime(dt.timetuple()) * 1000)

        _logger.debug("Fetching status for %r.", path)
        rpath = self.resolve_path(path)
        if not self.s3.exists(rpath):
            if not strict:
                return None
            else:
                raise FileSystemError("%r does not exist.", rpath)
        else:
            if rpath == self.bucket_name:
                # this is equivalent to root '/'
                info = {"Key": self.bucket_name, "Size": 0, "StorageClass": "DIRECTORY"}
            else:
                try:
                    info = self.s3.info(rpath)
                except FileNotFoundError:
                    # ugly workaround
                    self.s3.ls(os.path.dirname(rpath), refresh=True)
                    info = self.s3.info(rpath)
            return {
                "fileId": str(info["Key"]),
                "length": str(info["Size"])
                if str(info["StorageClass"]).upper() == "STANDARD"
                else 0,
                "type": "FILE"
                if str(info["StorageClass"]).upper() == "STANDARD"
                else "DIRECTORY",
                "modificationTime": str(_datetime2timestamp(info["LastModified"]))
                if info["StorageClass"] == "STANDARD"
                else None,
            }

    def _list(self, path, status=False, glob=False):
        if not glob:
            files = list(
                filter(
                    None,
                    [
                        self.format_path(os.path.basename(f))
                        for f in self.s3.ls(path, refresh=True)
                    ],
                )
            )
            if status:
                return [(f, self.status(os.path.join(path, f))) for f in files]
            else:
                return files
        else:
            files = [self.format_path(i_file) for i_file in iglob(self, path)]
            if status:
                return [(f, self.status(f)) for f in files]
            else:
                return files

    def _delete(self, path, recursive=False):
        self.s3.rm(path, recursive=recursive)

    def _copy(self, src_path, dst_path):
        self.s3.copy(src_path, dst_path)

    def _set_owner(self, path, owner=None, group=None):
        raise NotImplementedError

    def _set_permission(self, path, permission):
        self.s3.chmod(path, permission)

    def _mkdir(self, path, permission=None):
        self.s3.mkdir(path, permission)

    def _open(self, path, mode, buffer_size=-1, encoding=None, **kwargs):
        # omit buffer_size, s3 lib use whole block buffering
        return self.s3.open(path, mode=mode, encoding=encoding)
