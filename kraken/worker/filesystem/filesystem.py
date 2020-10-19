import grp
import logging as lg
import os
import pwd
import shutil
import stat
import types
from contextlib import contextmanager

from ...common.utils.glob import iglob
from ...filesystem.filesystem import IFileSystem
from ...filesystem.ioutils import (
    FileSystemError, DelimitedFileReader, ChunkFileReader, FileReader
)

_logger = lg.getLogger(__name__)


class LocalFileSystem(IFileSystem):
    def status(self, path, strict=True):
        def _get_type(p):
            if os.path.isfile(p):
                return "FILE"
            elif os.path.isdir(p):
                return "DIRECTORY"
            elif os.path.islink(p):
                return "LINK"
            else:
                return ""

        _logger.debug("Fetching status for %r.", path)
        rpath = self.resolvepath(path)
        if not os.path.exists(rpath):
            if not strict:
                return None
            else:
                raise FileSystemError("%r does not exist.", rpath)
        else:
            path_stat = os.stat(rpath)
            return {
                "owner": str(pwd.getpwuid(path_stat.st_uid)[0]),
                "childrenNum": str(len(os.listdir(rpath)) if os.path.isdir(rpath) else 0),
                "accessTime": str(int(path_stat.st_atime * 1000)),
                "fileId": str(path_stat.st_ino),
                "permission": str(oct(stat.S_IMODE(path_stat.st_mode))),
                "length": str(path_stat.st_size if os.path.isfile(rpath) else 0),
                "type": str(_get_type(rpath)),
                "group": str(grp.getgrgid(path_stat.st_gid)[0]),
                "modificationTime": str(int(path_stat.st_mtime * 1000))
            }

    def content(self, path, strict=True):
        def _get_size(start_path="."):
            total_folders = 0
            total_files = 0
            total_size = 0

            for dirpath, dirnames, filenames in os.walk(start_path):
                total_folders += len(dirnames)
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    if not os.path.islink(fp):
                        total_files += 1
                        total_size += os.path.getsize(fp)

            return {
                "length": str(total_size),
                "fileCount": str(total_files),
                "directoryCount": str(total_folders)
            }

        _logger.debug("Fetching content summary for %r.", path)
        rpath = self.resolvepath(path)
        if not os.path.exists(rpath):
            if not strict:
                return None
            else:
                raise FileSystemError("%r does not exist.", rpath)
        else:
            return _get_size(rpath)

    def list(self, path, status=False, glob=False):
        _logger.debug("Listing %r.", path)
        rpath = self.resolvepath(path)
        if not glob:
            if not os.path.isdir(rpath):
                raise FileSystemError("%r is not a directory.", rpath)
            files = os.listdir(rpath)
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

    def delete(self, path, recursive=False):
        rpath = self.resolvepath(path)
        if not os.path.exists(rpath):
            raise FileSystemError("%r does not exist.", rpath)
        else:
            if os.path.isfile(rpath):
                os.remove(rpath)
            else:
                if len(os.listdir(rpath)) == 0:
                    os.rmdir(rpath)
                else:
                    if recursive:
                        shutil.rmtree(rpath)
                    else:
                        raise FileSystemError("Directory %r is not empty.", rpath)

    def rename(self, src_path, dst_path):
        rsrc_path = self.resolvepath(src_path)
        rdst_path = self.resolvepath(dst_path)
        if not os.path.exists(rsrc_path):
            raise FileSystemError("%r does not exist.", rsrc_path)
        if os.path.exists(rdst_path):
            raise FileSystemError("%r exists.", rdst_path)
        os.rename(rsrc_path, rdst_path)

    def set_owner(self, path, owner=None, group=None):
        rpath = self.resolvepath(path)
        if not os.path.exists(rpath):
            raise FileSystemError("%r does not exist.", rpath)
        else:
            uid = pwd.getpwnam(owner).pw_uid if owner is not None else -1
            gid = grp.getgrnam(group).gr_gid if group is not None else -1
            os.chown(rpath, uid, gid)

    def set_permission(self, path, permission):
        rpath = self.resolvepath(path)
        if not os.path.exists(rpath):
            raise FileSystemError("%r does not exist.", rpath)
        else:
            os.chmod(rpath, int(permission, 8))

    def mkdir(self, path, permission=None):
        rpath = os.path.abspath(path)
        if not os.path.exists(rpath):
            os.mkdir(rpath, int(permission, 8))

    def open(self, path, mode, buffer_size=-1, encoding=None):
        return open(path, mode=mode, buffering=buffer_size, encoding=encoding)

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
        if not self.status(rpath, strict=False) is None:
            raise FileSystemError("%r does not exist.", rpath)

        _logger.debug("Reading file %r.", path)
        file = self.open(rpath, mode="rb", buffer_size=buffer_size, encoding=encoding)

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
        status = self.status(rpath, strict=False)
        if append:
            if overwrite:
                raise ValueError("Cannot both overwrite and append.")
            if permission:
                raise ValueError("Cannot change file properties while appending.")

            if status is not None and status['type'] != 'FILE':
                raise ValueError("Path %r is not a file.", rpath)
        else:
            if not overwrite:
                if status is not None:
                    raise ValueError("Path %r exists, missing `append`.", rpath)
            else:
                if status is not None and status['type'] != 'FILE':
                    raise ValueError("Path %r is not a file.", rpath)

        _logger.debug("Writing to %r.", path)
        file = self.open(rpath, mode="ab" if append else "wb", buffer_size=buffer_size, encoding=encoding)
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

    def resolvepath(self, path):
        return os.path.normpath(os.path.abspath(path))
