from ...filesystem.ioutils import FileSystemError
from ...thrift.master.dfs.ttypes import FileSystemException, FileContentOrNull, FileContent
from ...thrift.master.dfs.ttypes import FileStatus
from ...thrift.master.dfs.ttypes import FileStatusOrNull


class DistributedFileSystemHandler(object):
    def __init__(self, dfs):
        self.dfs = dfs

    def ls(self, path, status, glob):
        try:
            _list = self.dfs.list(path, status, glob)
        except Exception as e:
            raise FileSystemException(str(e))
        if not status:
            return [FileStatusOrNull(path=path) for path in _list]
        else:
            return [
                FileStatusOrNull(
                    path=st[0],
                    status=FileStatus(
                        fileId=str(st[1]["fileId"]),
                        length=str(st[1]["length"]),
                        type=str(st[1]["type"]),
                        modificationTime=str(st[1]["modificationTime"]),
                    ),
                )
                for st in _list
            ]

    def status(self, path, strict):
        try:
            status = self.dfs.status(path, False)
        except Exception as e:
            raise FileSystemException(str(e))
        if status is None:
            if strict:
                raise FileSystemException("File %s does not exist" % path)
            else:
                return FileStatusOrNull(path=path)
        else:
            return FileStatusOrNull(
                path=path,
                status=FileStatus(
                    fileId=str(status["fileId"]),
                    length=str(status["length"]),
                    type=str(status["type"]),
                    modificationTime=str(status["modificationTime"]),
                ),
            )

    def content(self, path, strict):
        try:
            content = self.dfs.content(path, False)
        except Exception as e:
            raise FileSystemException(str(e))
        if content is None:
            if strict:
                raise FileSystemException("File %s does not exist" % path)
            else:
                return FileContentOrNull(path=path)
        else:
            return FileContentOrNull(
                path=path,
                content=FileContent(
                    length=str(content["length"]),
                    fileCount=str(content["fileCount"]),
                    directoryCount=str(content["directoryCount"]),
                ),
            )

    def rm(self, path, recursive):
        try:
            self.dfs.delete(path, recursive)
        except FileSystemError:
            raise FileSystemException("File %s does not exist" % path)
        except Exception as e:
            raise FileSystemException(str(e))

    def mkdir(self, path, permission):
        try:
            self.dfs.mkdir(path, permission)
        except Exception as e:
            raise FileSystemException(str(e))

    def copy(self, src_path, dst_path, overwrite=True, force=False, checksum=False):
        try:
            self.dfs.copy(src_path, dst_path, overwrite, force, checksum)
        except Exception as e:
            raise FileSystemException(str(e))

    def move(self, src_path, dst_path):
        try:
            self.dfs.move(src_path, dst_path)
        except Exception as e:
            raise FileSystemException(str(e))

    def checksum(self, path, algorithm="md5"):
        try:
            return self.dfs.checksum(path, algorithm)
        except Exception as e:
            raise FileSystemException(str(e))