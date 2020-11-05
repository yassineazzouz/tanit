import logging as lg
from copy import deepcopy
from threading import RLock

from .gcp.filesystem import GCPFileSystem
from .hdfs.filesystem import HDFSFileSystem
from .ioutils import FileSystemError
from .local.local_filesystem import LocalFileSystem
from .local.remote_filesystem import RemoteFileSystem
from .s3.filesystem import S3FileSystem
from .model import FileSystemType

_logger = lg.getLogger(__name__)

_glock = RLock()


class FileSystemFactory(object):
    __instance = None

    @staticmethod
    def getInstance():
        with _glock:
            if FileSystemFactory.__instance is None:
                FileSystemFactory.__instance = FileSystemFactory()
        return FileSystemFactory.__instance

    def create_filesystem(self, filesystem):
        if filesystem.type == FileSystemType.LOCAL:
            if "address" in filesystem.parameters and "port" in filesystem.parameters:
                address = filesystem.parameters["address"]
                port = int(filesystem.parameters["port"])
                fs = RemoteFileSystem(address, port)
            else:
                fs = LocalFileSystem()
        elif filesystem.type == FileSystemType.HDFS:
            config = deepcopy(filesystem.parameters)
            auth_mechanism = config["auth_mechanism"]
            del config["auth_mechanism"]
            fs = HDFSFileSystem(auth_mechanism, **config)
        elif filesystem.type == FileSystemType.S3:
            config = deepcopy(filesystem.parameters)
            bucket = config["bucket"]
            del config["bucket"]
            fs = S3FileSystem(bucket, **config)
        elif filesystem.type == FileSystemType.GCS:
            config = deepcopy(filesystem.parameters)
            bucket = config["bucket"]
            del config["bucket"]
            if "token" in config:
                token = config["token"]
                del config["token"]
            else:
                token = None
            fs = GCPFileSystem(bucket, token, **config)
        else:
            raise NonSupportedFileSystemError(
                "Non supported filesystem type '%s'" % type
            )
        # This is an ugly workaround to initialize the client
        fs.list("/")
        return fs


class NonSupportedFileSystemError(FileSystemError):
    pass
