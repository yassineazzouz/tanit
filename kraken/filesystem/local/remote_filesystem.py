import logging as lg
import os
import types
from contextlib import contextmanager

from ...worker.filesystem.client import LocalFileSystemClient
from ..filesystem import IFileSystem
from ..ioutils import ChunkFileReader
from ..ioutils import DelimitedFileReader
from ..ioutils import FileReader
from ..ioutils import FileSystemError

_logger = lg.getLogger(__name__)


class RemoteFileSystem(IFileSystem):
    def __init__(self, host, port):
        self.client = LocalFileSystemClient(host, port)
        self.client.start()

    def resolvepath(self, path):
        return os.path.normpath(os.path.abspath(path))

    def list(self, path, status=False, glob=False):
        return self.client.ls(path, status, glob)

    def status(self, path, strict=True):
        return self.client.status(path, strict)

    def content(self, path, strict=True):
        return self.client.content(path, strict)

    def delete(self, path, recursive=False):
        return self.client.rm(path, recursive)

    def rename(self, src_path, dst_path):
        return self.client.rename(src_path, dst_path)

    def set_owner(self, path, owner=None, group=None):
        return self.client.set_owner(path, owner, group)

    def set_permission(self, path, permission):
        return self.client.set_permission(path, permission)

    def mkdir(self, path, permission=None):
        return self.client.mkdir(path, permission)

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
        if self.client.status(rpath, strict=False) is None:
            raise FileSystemError("%r does not exist.", rpath)

        _logger.debug("Reading file %r.", path)
        file = self.client.open(
            rpath, mode="rb", buffer_size=buffer_size, encoding=encoding
        )

        if offset > 0:
            file.seek(offset)
        try:
            if not chunk_size and not delimiter:
                # return a file like object
                yield file
            else:
                # return a generator function
                if delimiter:
                    yield DelimitedFileReader(file, delimiter=delimiter)
                else:
                    yield ChunkFileReader(file, chunk_size=chunk_size)
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
        status = self.client.status(rpath, strict=False)
        if append:
            if overwrite:
                raise ValueError("Cannot both overwrite and append.")
            if permission:
                raise ValueError("Cannot change file properties while appending.")

            if status is not None and status["type"] != "FILE":
                raise ValueError("Path %r is not a file.", rpath)
        else:
            if not overwrite:
                if status is not None:
                    raise ValueError("Path %r exists, missing `append`.", rpath)
            else:
                if status is not None and status["type"] != "FILE":
                    raise ValueError("Path %r is not a file.", rpath)

        _logger.debug("Writing to %r.", path)
        file = self.client.open(
            rpath,
            mode="ab" if append else "wb",
            buffer_size=buffer_size,
            encoding=encoding,
        )
        if data is None:
            return file
        else:
            with file:
                if isinstance(data, types.GeneratorType) or isinstance(
                    data, FileReader
                ):
                    for chunk in data:
                        file.write(chunk)
                else:
                    file.write(data)
