import logging as lg
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
            if FilesystemManager.__instance is None:
                FilesystemManager.__instance = FilesystemManager()
        return FilesystemManager.__instance

    def __init__(self):
        if FilesystemManager.__instance is not None:
            raise Exception("Only one instance of Client Factory is allowed!")

        self.factory = FileSystemFactory.getInstance()
        self._filesystems = {}

        FilesystemManager.__instance = self

    def configure(self):
        _config = FileSystemsConfig(None).get_config()
        if _config is not None:
            for _fs_conf in _config["filesystems"]:
                self.register_filesystem(_fs_conf)
        else:
            _logger.warning("Empty filesystems configuration file.")

    def register_filesystem(self, filesystem):
        with _glock:
            _logger.info("Registering new filesystem [ %s ].", filesystem.name)
            if filesystem.name in self._filesystems:
                # file system already exist, overwrite
                _logger.warning("A filesystem with name [%s] already exist, overwriting it." % filesystem.name)
            self._filesystems[filesystem.name] = filesystem
            _logger.info("Registered filesystem '%s'" % filesystem.name)

    def list_filesystems(self):
        with _glock:
            return [
                self._filesystems[name] for name in self._filesystems
            ]

    def get_filesystem(self, name):
        with _glock:
            if name in self._filesystems:
                return self.factory.create_filesystem(
                    self._filesystems[name]
                )
            else:
                # the name does not exist
                raise UnknownFileSystemError("Unknown Filesystem '%s'" % name)


class InvalidFileSystemError(FileSystemError):
    pass


class UnknownFileSystemError(FileSystemError):
    pass
