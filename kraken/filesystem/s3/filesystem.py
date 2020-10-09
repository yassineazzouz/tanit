import codecs
import logging as lg
import os
import types
from contextlib import contextmanager

import s3fs

from ...common.utils.glob import iglob
from ..filesystem import IFileSystem
from ..ioutils import ChunkFileReader
from ..ioutils import DelimitedFileReader
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
                info = {'Key': self.bucket_name, 'Size': 0, 'StorageClass': 'DIRECTORY'}
            else:
                try:
                    info = self.s3.info(rpath)
                except s3fs.FileNotFoundError:
                    # ugly workaround
                    self.s3.ls(os.path.dirname(rpath), refresh=True)
                    info = self.s3.info(rpath)
            return {
                "fileId": info["Key"],
                "length": info["Size"] if str(info["StorageClass"]).upper() == "STANDARD" else 0,
                "type": "FILE" if str(info["StorageClass"]).upper() == "STANDARD" else "DIRECTORY",
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
                    total_size += self.s3.size(os.path.join(dirpath, f))

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
        self.s3.rename(rsrc_path, rdst_path)

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

    @contextmanager
    def read(
        self,
        path,
        offset=0,
        buffer_size=1024,
        encoding=None,
        chunk_size=None,
        delimiter=None,
        **kwargs
    ):

        if delimiter:
            if not encoding:
                raise ValueError("Delimiter splitting requires an encoding.")
            if chunk_size:
                raise ValueError("Delimiter splitting incompatible with chunk size.")

        rpath = self.resolvepath(path)
        if not self.s3.exists(rpath):
            raise FileSystemError("%r does not exist.", rpath)

        _logger.debug("Reading file %r.", path)
        file = self.s3.open(rpath, mode="rb")
        if offset > 0:
            file.seek(offset)

        try:
            if not chunk_size and not delimiter:
                # return a file like object
                yield codecs.getreader(encoding)(file) if encoding else file
            else:
                # return a generator function
                if delimiter:
                    reader = DelimitedFileReader(file, delimiter=delimiter)
                else:
                    reader = ChunkFileReader(file, chunk_size=chunk_size)
                yield codecs.getreader(encoding)(reader) if encoding else reader
        finally:
            file.close()
            _logger.debug("Closed response for reading file %r.", path)

    def write(
        self,
        path,
        data=None,
        overwrite=False,
        permission=None,
        buffer_size=1024,
        append=False,
        encoding=None,
        **kwargs
    ):

        rpath = self.resolvepath(path)
        if append:
            if overwrite:
                raise ValueError("Cannot both overwrite and append.")
            if permission:
                raise ValueError("Cannot change file properties while appending.")
            if self.s3.exists(rpath) and not self.s3.isfile(rpath):
                raise ValueError("Path %r is not a file.", rpath)
        else:
            if not overwrite:
                if self.s3.exists(rpath):
                    raise ValueError("Path %r exists, missing `append`.", rpath)
            else:
                if self.s3.exists(rpath) and not self.s3.isfile(rpath):
                    raise ValueError("Path %r is not a file.", rpath)

        _logger.debug("Writing to %r.", path)
        file = self.s3.open(rpath, mode="ab" if append else "wb", encoding=encoding)
        if data is None:
            return file
        else:
            with file:
                if isinstance(data, types.GeneratorType):
                    for chunk in data:
                        file.write(chunk)
                else:
                    file.write(data)
