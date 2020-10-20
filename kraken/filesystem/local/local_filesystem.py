import grp
import logging as lg
import os
import pwd
import shutil
import stat

from ...common.utils.glob import iglob
from ..filesystem import IFileSystem
from ..ioutils import FileSystemError

_logger = lg.getLogger(__name__)


class LocalFileSystem(IFileSystem):
    def resolvepath(self, path):
        return os.path.normpath(os.path.abspath(path))

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
                "childrenNum": str(
                    len(os.listdir(rpath)) if os.path.isdir(rpath) else 0
                ),
                "accessTime": str(int(path_stat.st_atime * 1000)),
                "fileId": str(path_stat.st_ino),
                "permission": str(oct(stat.S_IMODE(path_stat.st_mode))),
                "length": str(path_stat.st_size if os.path.isfile(rpath) else 0),
                "type": str(_get_type(rpath)),
                "group": str(grp.getgrgid(path_stat.st_gid)[0]),
                "modificationTime": str(int(path_stat.st_mtime * 1000)),
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
                "directoryCount": str(total_folders),
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
        rpath = self.resolvepath(path)
        if not os.path.exists(rpath):
            os.mkdir(rpath)
            if permission:
                os.chmod(rpath, int(permission, 8))

    def open(self, path, mode, buffer_size=-1, encoding=None, **kwargs):
        return open(path, mode=mode, buffering=buffer_size, encoding=encoding)
