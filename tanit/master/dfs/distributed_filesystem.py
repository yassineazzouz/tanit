import logging as lg
import re
from datetime import datetime
from hashlib import md5
import time
import os

from ..core.execution.execution_job import JobExecution
from ...common.utils.glob import iglob
from ...filesystem.filesystem_manager import FilesystemManager
from ...filesystem.ioutils import FileSystemError
from ...filesystem.model import FileSystemType, FileSystemMounts, FileSystem

_logger = lg.getLogger(__name__)


class INode(object):
    def __init__(self, iname, itype, parent=None):
        self.iname = iname
        self.itype = itype
        self.parent = parent
        self.creation_time = datetime.now()
        self.mount = None
        self.mount_path = ""
        self.childs = {}

    def get_path(self):
        parent = self.parent
        path = self.iname
        while parent is not None:
            path = parent.iname + "/" + path
            parent = parent.parent
        return path


class DistributedFileSystem(object):

    def __init__(self, execution_manager):
        self.filesystem_manager = FilesystemManager.getInstance()
        self.filesystem_manager.configure()

        self.execution_manager = execution_manager
        self.root = INode("", "DIR")

    def list_filesystem(self):
        # register the worker as a filesystem
        self.filesystem_manager.list_filesystems()

    def register_filesystem(self, filesystem):
        # register the worker as a filesystem
        self.filesystem_manager.register_filesystem(filesystem)

    def mount_filesystem(self, name, mount_point, mount_path=""):
        def _mkmount(mount, mount_point, mount_path):
            parent = self.root
            for splt in mount_point.split("/"):
                if splt == "":
                    continue
                if splt in parent.childs:
                    if parent.childs[splt].itype == "MOUNT":
                        raise InvalidFileSystemMountError(
                            "Mount point %s already used." % mount_point
                        )
                    else:
                        parent = parent.childs[splt]
                else:
                    child = INode(splt, "DIR", parent)
                    parent.childs[splt] = child
                    parent = child

            parent.itype = "MOUNT"
            parent.mount = mount
            parent.mount_path = mount_path

        # verify the filesystem exist
        self.filesystem_manager.get_filesystem(name)
        _mkmount(name, mount_point, mount_path)

    def umount_filesystem(self, mount_point):
        inode = self.root
        for splt in mount_point.split("/"):
            if splt == "":
                continue
            if splt in inode.childs:
                inode = inode.childs[splt]
            else:
                raise FileSystemError("mount point %r does not exist.", mount_point)

        if inode.itype != "MOUNT":
            raise FileSystemError("%r is not a mount point.", mount_point)
        parent = inode.parent
        if parent is not None:
            parent.childs.pop(inode.iname)

    def list_filesystems(self):
        return self.filesystem_manager.list_filesystems()

    def list_filesystems_mounts(self):
        def list_mounts(inode=None):
            if inode is None:
                inode = self.root
            if inode.itype == "MOUNT":
                yield inode
            else:
                for dir in inode.childs:
                    for child in list_mounts(inode.childs[dir]):
                        yield child

        filesystems = {}
        for filesystem in self.filesystem_manager.list_filesystems():
            filesystems[filesystem.name] = FileSystemMounts(
                # when listing filesystems, never print the parameters
                FileSystem(filesystem.name, filesystem.type, {}), [],
        )
        for mount in list_mounts():
            filesystems[mount.mount].mounts.append({
                "mount point": mount.get_path(),
                "mount path": mount.mount_path
            })
        return filesystems.values()

    def status(self, path, strict=True):
        def _datetime2timestamp(dt):
            try:
                return int(dt.timestamp() * 1000)
            except AttributeError:
                # python 2.7
                return int(time.mktime(dt.timetuple()) * 1000)

        inode = self.root
        for splt in path.split("/"):
            if splt == "":
                continue
            if splt in inode.childs:
                if inode.childs[splt].itype == "MOUNT":
                    mount = inode.childs[splt]
                    filesystem = self.filesystem_manager.get_filesystem(inode.childs[splt].mount)
                    return filesystem.status(
                        re.sub("^%s" % mount.get_path(), mount.mount_path, path),
                        strict=strict
                    )
                else:
                    inode = inode.childs[splt]
            else:
                if strict:
                    raise FileSystemError("%r does not exist.", path)
                else:
                    return None

        return {
            "fileId": path,
            "length": 0,
            "type": "DIRECTORY",
            "modificationTime": str(_datetime2timestamp(inode.creation_time))
        }

    def exists(self, path):
        return self.status(path, strict=False) is not None

    def list(self, path, status=False, is_glob=False):
        if not is_glob:
            inode = self.root
            for splt in path.split("/"):
                if splt == "":
                    continue
                if splt in inode.childs:
                    if inode.childs[splt].itype == "MOUNT":
                        mount = inode.childs[splt]
                        filesystem = self.filesystem_manager.get_filesystem(inode.childs[splt].mount)
                        return filesystem.list(
                            re.sub("^%s" % mount.get_path(), mount.mount_path, path),
                            status=status
                        )
                    else:
                        inode = inode.childs[splt]
                else:
                    raise FileSystemError("%r does not exist.", path)
            if status:
                return [(child, self.status(child.get_path())) for child in inode.childs]
            else:
                return [child for child in inode.childs]
        else:
            files = [i_file for i_file in iglob(self, path)]
            if status:
                return [(f, self.status(f)) for f in files]
            else:
                return files

    def walk(self, path, depth=0, status=False):
        _logger.debug("Walking %r (depth %r).", path, depth)

        def _walk(dir_path, dir_status, depth):
            """Recursion helper."""
            infos = self.list(dir_path, status=True)
            dir_infos = [info for info in infos if info[1]["type"] == "DIRECTORY"]
            file_infos = [info for info in infos if info[1]["type"] == "FILE"]
            if status:
                yield ((dir_path, dir_status), dir_infos, file_infos)
            else:
                yield (
                    dir_path,
                    [name for name, _ in dir_infos],
                    [name for name, _ in file_infos],
                )
            if depth != 1:
                for name, s in dir_infos:
                    path = os.path.join(dir_path, name)
                    for infos in _walk(path, s, depth - 1):
                        yield infos

        s = self.status(path)
        if s["type"] == "DIRECTORY":
            for infos in _walk(path, s, depth):
                yield infos

    def content(self, path, strict=True):
        def _get_size(start_path="."):
            total_folders = 0
            total_files = 0
            total_size = 0

            for dirpath, dirnames, filenames in self.walk(start_path):
                total_folders += len(dirnames)
                for f in filenames:
                    total_files += 1
                    total_size += int(self.status(os.path.join(dirpath, f))["length"])

            return {
                "length": str(total_size),
                "fileCount": str(total_files),
                "directoryCount": str(total_folders),
            }

        _logger.debug("Fetching content summary for %r.", path)
        if not self.exists(path):
            if not strict:
                return None
            else:
                raise FileSystemError("%r does not exist.", path)
        else:
            return _get_size(path)

    def mkdir(self, path, permission=None):
        parent = self.root
        for splt in path.split("/"):
            if splt == "":
                continue
            if splt in parent.childs:
                if parent.childs[splt].itype == "MOUNT":
                    mount = parent.childs[splt]
                    filesystem = self.filesystem_manager.get_filesystem(parent.childs[splt].mount)
                    return filesystem.mkdir(
                        re.sub("^%s" % mount.get_path(), mount.mount_path, path),
                        permission
                    )
                else:
                    parent = parent.childs[splt]
            else:
                child = INode(splt, "DIR", parent)
                parent.childs[splt] = child
                parent = child

    def delete(self, path, recursive=False):
        def _delete(inode):
            if inode.itype == "MOUNT":
                if recursive:
                    filesystem = self.filesystem_manager.get_filesystem(inode.mount)
                    filesystem.delete(
                        inode.mount_path,
                        recursive=recursive
                    )
                else:
                    raise FileSystemError(
                        "Directory [%r] have a mount, use recursive to force delete." % path
                    )
            else:
                if len(inode.childs) == 0:
                    parent = inode.parent
                    parent.childs.pop(inode.iname)
                else:
                    # workaround 'dictionary changed size during iteration'
                    for child in list(inode.childs):
                        _delete(inode.childs[child])

        inode = self.root
        for splt in path.split("/"):
            if splt == "":
                continue
            if splt in inode.childs:
                if inode.childs[splt].itype == "MOUNT":
                    mount = inode.childs[splt]
                    filesystem = self.filesystem_manager.get_filesystem(inode.childs[splt].mount)
                    return filesystem.delete(
                        re.sub("^%s" % mount.get_path(), mount.mount_path, path),
                        recursive=recursive
                    )
                else:
                    inode = inode.childs[splt]
            else:
                raise FileSystemError("%r does not exist.", path)

        _delete(inode)

    def checksum(self, path, algorithm="md5"):
        inode = self.root
        for splt in path.split("/"):
            if splt == "":
                continue
            if splt in inode.childs:
                if inode.childs[splt].itype == "MOUNT":
                    mount = inode.childs[splt]
                    filesystem = self.filesystem_manager.get_filesystem(inode.childs[splt].mount)
                    return filesystem.checksum(
                        re.sub("^%s" % mount.get_path(), mount.mount_path, path),
                        algorithm
                    )
                else:
                    inode = inode.childs[splt]
            else:
                raise FileSystemError("%r does not exist.", path)
        raise FileSystemError("%r does not exist.", path)

    def get_path_mount(self, path):
        inode = self.root
        for splt in path.split("/"):
            if splt == "":
                continue
            if splt in inode.childs:
                if inode.childs[splt].itype == "MOUNT":
                    return inode.childs[splt]
                else:
                    inode = inode.childs[splt]
            else:
                return None
        if inode.itype == "MOUNT":
            return inode
        else:
            return None

    def copy(self, src_path, dst_path, overwrite=True, force=False, checksum=False):

        src_mount = self.get_path_mount(src_path)
        if src_mount is None:
            raise FileSystemError("Source path [%s] is not a mounted file system" % src_path)
        src_path = re.sub("^%s" % src_mount.get_path(), src_mount.mount_path, src_path)

        dst_mount = self.get_path_mount(dst_path)
        if dst_mount is None:
            raise FileSystemError("Destination path [%s] is not a mounted file system" % dst_path)
        dst_path = re.sub("^%s" % dst_mount.get_path(), dst_mount.mount_path, dst_path)

        src = self.filesystem_manager.get_filesystem(src_mount.mount)
        dst = self.filesystem_manager.get_filesystem(dst_mount.mount)

        # Normalise src and dst paths
        src_path = src.resolve_path(src_path)

        if str(dst_path).endswith("/"):
            dst_path = dst.resolve_path(dst_path)
            dst_name = ""
        else:
            r_path = dst.resolve_path(dst_path)
            dst_path = os.path.dirname(r_path)
            dst_name = os.path.basename(r_path)

        # First, resolve the list of src files/directories to be copied
        copies = [
            src.format_path(copy_file) for copy_file in list(iglob(src, src_path))
        ]

        # need to develop a proper pattern based access function
        if len(copies) == 0:
            raise FileSystemError(
                "Cloud not resolve source path %s : "
                + "either it does not exist or can not access it.",
                src_path,
            )

        tuples = []
        for copy in copies:
            copy_tuple = dict()

            # filename = osp.basename(copy)
            # dst_base_path =  osp.join( dst_path, filename )
            status = dst.status(dst_path, strict=False)
            # statuses = [status for _, status in dst.list(dst_base_path)]
            if status is None:
                # Remote path doesn't exist.
                raise FileSystemError("Base directory of %r does not exist.", dst_path)
            else:
                # Remote path exists.
                if status["type"] == "FILE":
                    # Remote path exists and is a normal file.
                    if not overwrite:
                        raise FileSystemError(
                            "Destination path %r already exists.", dst_path
                        )
                    # the file is going to be deleted and the destination
                    # is going to be created with the same name
                    dst_base_path = dst_path
                else:
                    # Remote path exists and is a directory.
                    dst_name = os.path.basename(copy) if dst_name == "" else dst_name
                    status = dst.status(os.path.join(dst_path, dst_name), strict=False)
                    if status is None:
                        # destination does not exist, great !
                        dst_base_path = os.path.join(dst_path, dst_name)
                        pass
                    else:
                        # destination exists
                        dst_base_path = os.path.join(dst_path, dst_name)
                        if not overwrite:
                            raise FileSystemError(
                                "Destination path %r already exists.", dst_base_path
                            )

                copy_tuple = dict({"src_path": copy, "dst_path": dst_base_path})
            tuples.append(copy_tuple)

        # This is a workaround for a Bug when copying files using a pattern
        # it may happen that files can have the same name:
        # ex : /home/user/test/*/*.py may result in duplicate files
        for i in range(0, len(tuples)):
            for x in range(i + 1, len(tuples)):
                if tuples[i]["dst_path"] == tuples[x]["dst_path"]:
                    raise FileSystemError(
                        "Conflicting files %r and %r : can't copy both files to %r"
                        % (  # NOQA
                            tuples[i]["src_path"],
                            tuples[x]["src_path"],
                            tuples[i]["dst_path"],
                        )
                    )

        for copy_tuple in tuples:
            # Then we figure out which files we need to copy, and where.
            src_paths = list(src.walk(copy_tuple["src_path"]))
            if not src_paths:
                # This is a single file.
                src_fpaths = [copy_tuple["src_path"]]
            else:
                src_fpaths = [
                    os.path.join(dpath, fname)
                    for dpath, _, fnames in src_paths
                    for fname in fnames
                ]

            offset = len(copy_tuple["src_path"].rstrip(os.sep)) + len(os.sep)

            # Initialize the execution
            execution = JobExecution("job-copy-%s-%s" % (
                md5(
                    "{}-{}-{}-{}".format(
                        src_mount, src_path, dst_mount, dst_path
                    ).encode("utf-8")
                ).hexdigest(),
                str(datetime.now().strftime("%d%m%Y%H%M%S")),
            ))
            i = 1
            for fpath in src_fpaths:
                execution.add_task(
                    "{}-task-{}".format(execution.jid, i),
                    operation="COPY",
                    params={
                        "src": str(src_mount.mount),
                        "dst": str(dst_mount.mount),
                        "src_path": str(fpath),
                        "dest_path": str(
                            os.path.join(
                                copy_tuple["dst_path"],
                                fpath[offset:].replace(os.sep, "/"),
                            ).rstrip(os.sep)
                        ),
                        "overwrite": str(overwrite),
                        "force": str(force),
                        "checksum": str(checksum),
                    },
                )
                i += 1

            # start the execution
            self.execution_manager.submit_job(execution)

            # wait for the job to finish
            while True:
                if execution.is_finished():
                    return
                elif execution.is_failed():
                    raise FileSystemError("Copy of %s to %s failed" % (src_path, dst_path))
                else:
                    time.sleep(1.2)

    def move(self, src_path, dst_path):

        src_mount = self.get_path_mount(src_path)
        if src_mount is None:
            raise FileSystemError("Source path [%s] is not a mounted file system" % src_path)
        real_src_path = re.sub("^%s" % src_mount.get_path(), src_mount.mount_path, src_path)

        dst_mount = self.get_path_mount(dst_path)
        if dst_mount is None:
            raise FileSystemError("Destination path [%s] is not a mounted file system" % dst_path)
        real_dst_path = re.sub("^%s" % dst_mount.get_path(), dst_mount.mount_path, dst_path)

        src = self.filesystem_manager.get_filesystem(src_mount.mount)
        dst = self.filesystem_manager.get_filesystem(dst_mount.mount)

        # Normalise src and dst paths
        real_src_path = src.resolve_path(real_src_path)

        if str(real_dst_path).endswith("/"):
            real_dst_path = os.path.join(dst.resolve_path(real_dst_path), os.path.basename(real_src_path))
        else:
            real_dst_path = dst.resolve_path(real_dst_path)

        if dst.status(real_dst_path, strict=False) is not None:
            raise FileSystemError("Destination directory %s exists." % real_dst_path)

        self.copy(src_path, dst_path, overwrite=False, force=False, checksum=False)
        src.delete(real_src_path, recursive=True)


class AlreadyMountedFileSystemError(FileSystemError):
    pass


class InvalidFileSystemMountError(FileSystemError):
    pass
