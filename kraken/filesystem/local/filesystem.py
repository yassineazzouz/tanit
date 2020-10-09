import codecs
import grp
import logging as lg
import os
import pwd
import shutil
import stat
import types
from contextlib import contextmanager

from ...common.utils.glob import iglob
from ..filesystem import IFileSystem
from ..ioutils import ChunkFileReader
from ..ioutils import DelimitedFileReader
from ..ioutils import FileSystemError

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
                "owner": pwd.getpwuid(path_stat.st_uid)[0],
                "childrenNum": len(os.listdir(rpath)) if os.path.isdir(rpath) else 0,
                "accessTime": int(path_stat.st_atime * 1000),
                "fileId": path_stat.st_ino,
                "permission": oct(stat.S_IMODE(path_stat.st_mode)),
                "length": path_stat.st_size if os.path.isfile(rpath) else 0,
                "type": _get_type(rpath),
                "group": grp.getgrgid(path_stat.st_gid)[0],
                "modificationTime": int(path_stat.st_mtime * 1000),
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
                "length": total_size,
                "fileCount": total_files,
                "directoryCount": total_folders,
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

    @contextmanager
    def read(
        self,
        path,
        offset=0,
        buffer_size=1024,
        encoding=None,
        chunk_size=None,
        delimiter=None,
        **kwargs,
    ):

        if delimiter:
            if not encoding:
                raise ValueError("Delimiter splitting requires an encoding.")
            if chunk_size:
                raise ValueError("Delimiter splitting incompatible with chunk size.")

        _logger.debug("Reading file %r.", path)
        file = open(path, mode="rb", buffering=buffer_size)
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
        **kwargs,
    ):
        class FileWriter:
            def __init__(
                self,
                path,
                permission=None,
                buffer_size=1024,
                append=False,
                encoding=None,
            ):
                self.path = path
                self.permission = permission
                self.buffer_size = buffer_size
                self.encoding = encoding

                self.mode = "a" if append else "w"

            def write(self, _data):
                self.file.write(_data.encode(self.encoding) if self.encoding else _data)

            def __enter__(self):
                self.file = open(self.path, mode=self.mode, buffering=self.buffer_size)
                return self.file

            def __exit__(self, exc_type, exc_value, exc_traceback):
                self.file.close()

        if append:
            if overwrite:
                raise ValueError("Cannot both overwrite and append.")
            if permission:
                raise ValueError("Cannot change file properties while appending.")
            if os.path.exists(path) and not os.path.isfile(path):
                raise ValueError("Path %r is not a file.", path)
        else:
            if not overwrite:
                if os.path.exists(path):
                    raise ValueError("Path %r exists, missing `append`.", path)
            else:
                if os.path.exists(path) and not os.path.isfile(path):
                    raise ValueError("Path %r is not a file.", path)

        _logger.debug("Writing to %r.", path)
        writer = FileWriter(
            path,
            permission=permission,
            buffer_size=buffer_size,
            append=append,
            encoding=encoding,
        )
        if data is None:
            return writer
        else:
            with writer:
                if isinstance(data, types.GeneratorType):
                    for chunk in data:
                        writer.write(chunk)
                else:
                    writer.write(data)

    def resolvepath(self, path):
        return os.path.normpath(os.path.abspath(path))
