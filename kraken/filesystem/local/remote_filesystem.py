import logging as lg
import os

from ...worker.filesystem.client import LocalFileSystemClient
from ..filesystem import IFileSystem

_logger = lg.getLogger(__name__)


class RemoteFileSystem(IFileSystem):
    def __init__(self, host, port):
        self.client = LocalFileSystemClient(host, port)
        self.client.start()

    def resolve_path(self, path):
        return os.path.normpath(os.path.abspath(path))

    def _list(self, path, status=False, glob=False):
        return self.client.ls(path, status, glob)

    def _status(self, path, strict=True):
        return self.client.status(path, strict)

    def _content(self, path, strict=True):
        return self.client.content(path, strict)

    def _delete(self, path, recursive=False):
        return self.client.rm(path, recursive)

    def copy(self, src_path, dst_path, recursive=True):
        return self.client.copy(src_path, dst_path)

    def _copy(self, src_path, dst_path):
        return self.client.copy(src_path, dst_path)

    def rename(self, src_path, dst_path):
        return self.client.rename(src_path, dst_path)

    def _set_owner(self, path, owner=None, group=None):
        return self.client.set_owner(path, owner, group)

    def _set_permission(self, path, permission):
        return self.client.set_permission(path, permission)

    def _mkdir(self, path, permission=None):
        return self.client.mkdir(path, permission)

    def _open(self, path, mode, buffer_size=0, encoding=None, **kwargs):
        return self.client.open(
            path,
            mode=mode,
            buffer_size=buffer_size,
            encoding=encoding,
        )
