import logging as lg
import os

from ...worker.filesystem.client import LocalFileSystemClient
from ..filesystem import IFileSystem

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

    def open(self, path, mode, buffer_size=-1, encoding=None, **kwargs):
        return self.client.open(
            path,
            mode=mode,
            buffer_size=buffer_size,
            encoding=encoding,
        )
