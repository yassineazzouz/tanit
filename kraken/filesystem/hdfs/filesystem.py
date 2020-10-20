import logging as lg
from contextlib import contextmanager

from pywhdfs.client import create_client

from ..filesystem import IFileSystem

_logger = lg.getLogger(__name__)


class HDFSFileSystem(IFileSystem):
    def __init__(self, cluster, auth_mechanism, **params):
        self.cluster = cluster
        self.client = create_client(auth_mechanism, **params)

    def resolvepath(self, path):
        return self.client.resolvepath(path)

    def list(self, path, status=False, glob=False):
        return self.client.list(path, status, glob)

    def status(self, path, strict=True):
        s = self.client.status(path, strict)
        if s is None:
            return None
        else:
            return {
                "fileId": path,
                "length": s["length"],
                "type": str(s["type"]).upper(),
                "modificationTime": s["modificationTime"],
            }

    def content(self, path, strict=True):
        c = self.client.content(path, strict)
        if c is None:
            return None
        else:
            return {
                "length": c["length"],
                "fileCount": c["fileCount"],
                "directoryCount": c["directoryCount"],
            }

    def delete(self, path, recursive=False):
        return self.client.delete(path, recursive)

    def rename(self, src_path, dst_path):
        return self.client.rename(src_path, dst_path)

    def set_owner(self, path, owner=None, group=None):
        return self.client.set_owner(path, owner, group)

    def set_permission(self, path, permission):
        return self.client.set_permission(path, permission)

    def mkdir(self, path, permission=None):
        return self.client.makedirs(path, permission)

    def open(self, path, mode, buffer_size=-1, encoding=None, **kwargs):
        # HDFS library does not implement open method
        raise NotImplementedError

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
        with self.client.read(
            path,
            offset=offset,
            buffer_size=buffer_size,
            encoding=encoding,
            chunk_size=chunk_size,
            delimiter=delimiter,
            **kwargs
        ) as reader:
            yield reader

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
        return self.client.write(
            path,
            data=data,
            overwrite=overwrite,
            permission=permission,
            buffersize=buffer_size,
            append=append,
            encoding=encoding,
            **kwargs
        )
