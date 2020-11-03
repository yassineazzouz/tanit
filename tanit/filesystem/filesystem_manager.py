
import logging as lg
from copy import deepcopy
from threading import RLock

from .config import FileSystemsConfig
from .filesystem_factory import FileSystemFactory
from .ioutils import FileSystemError

_logger = lg.getLogger(__name__)

_glock = RLock()


class FilesystemManager(object):
    __instance = None

    @staticmethod
    def getInstance():
        with _glock:
            """ Static access method. """
            if FilesystemManager.__instance is None:
                FilesystemManager()
        return FilesystemManager.__instance

    def __init__(self):
        if FilesystemManager.__instance is not None:
            raise Exception("Only one instance of Client Factory is allowed!")
        else:
            FilesystemManager.__instance = self
            self.factory = FileSystemFactory()
            self._configure()

    def _configure(self):
        self._filesystems = {}
        _config = FileSystemsConfig(None).get_config()
        if _config is not None:
            for _fs_conf in _config["filesystems"]:
                self.register_filesystem(_fs_conf)
        else:
            _logger.warning("Empty filesystems configuration file.")

    def register_filesystem(self, filesystem):
        _conf = deepcopy(filesystem)
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
        with _glock:
            if name in self._filesystems:
                return self.factory.create_filesystem(
                    deepcopy(self._filesystems[name])
                )
            else:
                # the name does not exist
                raise UnknownFileSystemError("Unknown Filesystem '%s'" % name)


class InvalidFileSystemError(FileSystemError):
    pass


class UnknownFileSystemError(FileSystemError):
    pass
