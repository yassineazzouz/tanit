import logging as lg
from threading import Lock

from .config import FileSystemsConfig
from .config import KrakenConfigurationException
from .gcp.filesystem import GCPFileSystem
from .hdfs.filesystem import HDFSFileSystem
from .ioutils import FileSystemError
from .local.filesystem import LocalFileSystem
from .s3.filesystem import S3FileSystem

_logger = lg.getLogger(__name__)

_glock = Lock()


class FileSystemFactory(object):
    __instance = None

    @staticmethod
    def getInstance():
        with _glock:
            """ Static access method. """
            if FileSystemFactory.__instance is None:
                FileSystemFactory()
        return FileSystemFactory.__instance

    def __init__(self):
        if FileSystemFactory.__instance is not None:
            raise Exception("Only one instance of Client Factory is allowed!")
        else:
            FileSystemFactory.__instance = self
            self._configure()

    def _configure(self):
        self.config = FileSystemsConfig(None)
        self.filesystems = {}

    def get_filesystem(self, name):
        with _glock:
            if name in self.filesystems:
                filesystem = self.filesystems[name]
            else:
                if name == "local":
                    filesystem = LocalFileSystem()
                else:
                    config = self.config.get_filesystem(name)
                    if "type" not in config:
                        raise KrakenConfigurationException(
                            "filesystem type missing for '%s'" % name
                        )
                    else:
                        fs_type = config["type"]
                        del config["type"]
                        if fs_type == "hdfs":
                            auth_mechanism = config["auth_mechanism"]
                            del config["auth_mechanism"]
                            filesystem = HDFSFileSystem(name, auth_mechanism, **config)
                        elif fs_type == "s3":
                            bucket = config["bucket"]
                            del config["bucket"]
                            filesystem = S3FileSystem(bucket, **config)
                        elif fs_type == "gcs":
                            project = config["project"]
                            del config["project"]
                            bucket = config["bucket"]
                            del config["bucket"]
                            token = config["token"]
                            del config["token"]
                            filesystem = GCPFileSystem(project, bucket, token, **config)
                        else:
                            raise NonSupportedFileSystemError(
                                "Non supported filesystem type '%s'" % type
                            )
                # This is an ugly workaround to initialize the client
                filesystem.list("/")
                self.filesystems[name] = filesystem
        return filesystem


class NonSupportedFileSystemError(FileSystemError):
    pass
