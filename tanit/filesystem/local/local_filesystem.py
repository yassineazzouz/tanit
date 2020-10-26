import grp
import io
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
    def resolve_path(self, path):
        return os.path.normpath(os.path.abspath(path))

    def _status(self, path, strict=True):
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
        rpath = self.resolve_path(path)
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

    def _list(self, path, status=False, glob=False):
        if not glob:
            files = os.listdir(path)
            if status:
                return [(f, self.status(os.path.join(path, f))) for f in files]
            else:
                return files
        else:
            files = [i_file for i_file in iglob(self, path)]
            if status:
                return [(f, self.status(f)) for f in files]
            else:
                return files

    def _delete(self, path, recursive=False):
        if os.path.isfile(path):
            os.remove(path)
        else:
            if len(os.listdir(path)) == 0:
                os.rmdir(path)
            else:
                if recursive:
                    shutil.rmtree(path)
                else:
                    raise FileSystemError("Directory %r is not empty.", path)

    def _copy(self, src_path, dst_path):
        shutil.copy(src_path, dst_path)

    def rename(self, src_path, dst_path):
        rsrc_path = self.resolve_path(src_path)
        rdst_path = self.resolve_path(dst_path)
        if not os.path.exists(rsrc_path):
            raise FileSystemError("%r does not exist.", rsrc_path)
        if os.path.exists(rdst_path):
            raise FileSystemError("%r exists.", rdst_path)
        os.rename(rsrc_path, rdst_path)

    def _set_owner(self, path, owner=None, group=None):
        uid = pwd.getpwnam(owner).pw_uid if owner is not None else -1
        gid = grp.getgrnam(group).gr_gid if group is not None else -1
        os.chown(path, uid, gid)

    def _set_permission(self, path, permission):
        os.chmod(path, int(permission, 8))

    def _mkdir(self, path, permission=None):
        os.makedirs(path)
        if permission:
            os.chmod(path, int(permission, 8))

    def _open(self, path, mode, buffer_size=0, encoding=None, **kwargs):
        file = open(path, mode=mode, buffering=buffer_size)
        if encoding:
            return io.TextIOWrapper(file, encoding=encoding)
        else:
            return file
