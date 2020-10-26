import logging as lg
from copy import deepcopy
from threading import RLock

from .config import FileSystemsConfig
from .gcp.filesystem import GCPFileSystem
from .hdfs.filesystem import HDFSFileSystem
from .ioutils import FileSystemError
from .local.local_filesystem import LocalFileSystem
from .local.remote_filesystem import RemoteFileSystem
from .s3.filesystem import S3FileSystem

_logger = lg.getLogger(__name__)

_glock = RLock()


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
        self._filesystems = {}
        _config = FileSystemsConfig(None).get_config()
        if _config is not None:
            for _fs_conf in _config["filesystems"]:
                self.register_filesystem(_fs_conf)
        else:
            _logger.warning("Empty filesystems configuration file.")

    def register_filesystem(self, conf):
        _conf = deepcopy(conf)
        with _glock:
            if "name" in _conf:
                name = _conf.pop("name")
                if name in self._filesystems:
                    # file system already exist, overwrite
                    del self._filesystems[name]
                self._filesystems[name] = _conf
                _logger.info("Registered filesystem '%s'" % name)
            else:
                raise InvalidFileSystemError(
                    "Missing filesystem name from %s" % str(_conf)
                )

    def get_filesystem(self, name):
        def _get_filesystem(name):
            if name in self._filesystems:
                return deepcopy(self._filesystems[name])
            else:
                # the name does not exist
                raise UnknownFileSystemError("Unknown Filesystem '%s'" % name)

        with _glock:
            config = _get_filesystem(name)
            if "type" not in config:
                raise InvalidFileSystemError("filesystem type missing for '%s'" % name)
            else:
                fs_type = config.pop("type")
                if fs_type == "local":
                    if "address" in config and "port" in config:
                        address = config["address"]
                        port = int(config["port"])
                        filesystem = RemoteFileSystem(address, port)
                    else:
                        filesystem = LocalFileSystem()
                elif fs_type == "hdfs":
                    auth_mechanism = config["auth_mechanism"]
                    del config["auth_mechanism"]
                    filesystem = HDFSFileSystem(name, auth_mechanism, **config)
                elif fs_type == "s3":
                    bucket = config["bucket"]
                    del config["bucket"]
                    filesystem = S3FileSystem(bucket, **config)
                elif fs_type == "gcs":
                    bucket = config["bucket"]
                    del config["bucket"]
                    if "token" in config:
                        token = config["token"]
                        del config["token"]
                    else:
                        token = None
                    filesystem = GCPFileSystem(bucket, token, **config)
                else:
                    raise NonSupportedFileSystemError(
                        "Non supported filesystem type '%s'" % type
                    )
            # This is an ugly workaround to initialize the client
            filesystem.list("/")
        return filesystem


class NonSupportedFileSystemError(FileSystemError):
    pass


class InvalidFileSystemError(FileSystemError):
    pass


class UnknownFileSystemError(FileSystemError):
    pass
