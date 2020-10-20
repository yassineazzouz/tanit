import logging as lg
import os

import s3fs

from ...common.utils.glob import iglob
from ..filesystem import IFileSystem
from ..ioutils import FileSystemError

_logger = lg.getLogger(__name__)


# TODO : change S3FileSystem to boto3 instead of s3fs
class S3FileSystem(IFileSystem):
    def __init__(self, bucket_name, **params):
        self.s3 = s3fs.S3FileSystem(**params)
        self.bucket_name = bucket_name

    def resolvepath(self, path):
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

    def status(self, path, strict=True):

        _logger.debug("Fetching status for %r.", path)
        rpath = self.resolvepath(path)
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
                "fileId": info["Key"],
                "length": info["Size"]
                if str(info["StorageClass"]).upper() == "STANDARD"
                else 0,
                "type": "FILE"
                if str(info["StorageClass"]).upper() == "STANDARD"
                else "DIRECTORY",
                "modificationTime": int(
                    self.s3.info("kraken-test/test/dummy.txt")[
                        "LastModified"
                    ].timestamp()
                    * 1000
                )
                if info["StorageClass"] == "STANDARD"
                else None,
            }

    def list(self, path, status=False, glob=False):
        _logger.debug("Listing %r.", path)
        rpath = self.resolvepath(path)
        if not glob:
            files = list(
                filter(
                    None, [os.path.basename(f) for f in self.s3.ls(rpath, refresh=True)]
                )
            )
            if status:
                return [(f, self.status(os.path.join(rpath, f))) for f in files]
            else:
                return files
        else:
            files = [i_file for i_file in iglob(self, path)]
            if status:
                return [(f, self.status(f)) for f in files]
            else:
                return files

    def content(self, path, strict=True):
        def _get_size(start_path="."):
            total_folders = 0
            total_files = 0
            total_size = 0

            for dirpath, dirnames, filenames in self.s3.walk(start_path):
                total_folders += len(dirnames)
                for f in filenames:
                    total_files += 1
                    total_size += int(self.s3.info(os.path.join(dirpath, f))["Size"])

            return {
                "length": total_size,
                "fileCount": total_files,
                "directoryCount": total_folders,
            }

        _logger.debug("Fetching content summary for %r.", path)
        rpath = self.resolvepath(path)
        if not self.s3.exists(rpath):
            if not strict:
                return None
            else:
                raise FileSystemError("%r does not exist.", rpath)
        else:
            return _get_size(rpath)

    def delete(self, path, recursive=False):
        rpath = self.resolvepath(path)
        if not self.s3.exists(rpath):
            raise FileSystemError("%r does not exist.", rpath)
        else:
            self.s3.rm(rpath, recursive=recursive)

    def rename(self, src_path, dst_path):
        rsrc_path = self.resolvepath(src_path)
        rdst_path = self.resolvepath(dst_path)
        if not self.s3.exists(rsrc_path):
            raise FileSystemError("%r does not exist.", rsrc_path)
        if self.s3.exists(rdst_path):
            raise FileSystemError("%r exists.", rdst_path)
        self.s3.mv(rsrc_path, rdst_path)

    def set_owner(self, path, owner=None, group=None):
        raise NotImplementedError

    def set_permission(self, path, permission):
        rpath = self.resolvepath(path)
        if not self.s3.exists(rpath):
            raise FileSystemError("%r does not exist.", rpath)
        else:
            self.s3.chmod(rpath, permission)

    def mkdir(self, path, permission=None):
        rpath = self.resolvepath(path)
        if not self.s3.exists(rpath):
            self.s3.mkdir(rpath, permission)

    def open(self, path, mode, buffer_size=-1, encoding=None, **kwargs):
        rpath = self.resolvepath(path)
        # omit buffer_size, s3 lib use whole block buffering
        return self.s3.open(rpath, mode=mode, encoding=encoding)
