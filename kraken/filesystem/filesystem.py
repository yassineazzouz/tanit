import abc
import hashlib
import logging as lg
import posixpath as psp

import six

from .ioutils import FileSystemError

_logger = lg.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class IFileSystem:
    @abc.abstractmethod
    def list(self, path, status=False, glob=False):
        """Return names of files contained in a remote folder.

        :param path: Remote path to a directory. If `path` doesn't exist
          or points to a normal file, an :class:`FileSystemError` will be raised.
        :glob: Whether the path should be considered a glob expressions
        :param status: Also return each file's corresponding FileStatus.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def status(self, path, strict=True):
        """Get FileStatus_ for a file or folder on HDFS.

        :param path: path.
        :param strict: If `False`, return `None` rather than raise an exception if
          the path doesn't exist.
        """

    @abc.abstractmethod
    def content(self, path, strict=True):
        """Get ContentSummary_ for a file or folder.

        :param path: Remote path.
        :param strict: If `False`, return `None` rather than raise an exception if
          the path doesn't exist.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, path, recursive=False):
        """Remove a file or directory.

        :param path: path.
        :param recursive: Recursively delete files and directories. By default,
          this method will raise an :class:`FileSystemError` if trying to delete a
          non-empty directory.
        This function returns `True` if the deletion was successful and `False` if
        no file or directory previously existed at `path`.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def rename(self, src_path, dst_path):
        """Move a file or folder.

        :param src_path: Source path.
        :param dst_path: Destination path. If the path already exists and is
          a directory, the source will be moved into it. If the path exists and is
          a file, or if a parent destination directory is missing, this method will
          raise an :class:`FileSystemError`.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def set_owner(self, path, owner=None, group=None):
        """Change the owner of file.

        :param path: path.
        :param owner: Optional, new owner for file.
        :param group: Optional, new group for file.
        At least one of `owner` and `group` must be specified.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def set_permission(self, path, permission):
        """Change the permissions of file.

        :param path: path.
        :param permission: New octal permissions string of file.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def mkdir(self, path, permission=None):
        """Create a remote directory, recursively if necessary.

        :param path: Remote path. Intermediate directories will be created
          appropriately.
        :param permission: Octal permission to set on the newly created directory.
          These permissions will only be set on directories that do not already
          exist.
        """
        raise NotImplementedError

    def walk(self, path, depth=0, status=False):
        """Depth-first walk of remote filesystem.

        :param path: Starting path. If the path doesn't exist, an
          :class:`FileSystemError` will be raised. If it points to a file, the returned
          generator will be empty.
        :param depth: Maximum depth to explore. `0` for no limit.
        :param status: Also return each file or folder's corresponding FileStatus_.
        This method returns a generator yielding tuples `(path, dirs, files)`
        where `path` is the absolute path to the current directory, `dirs` is the
        list of directory names it contains, and `files` is the list of file names
        it contains.
        """
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
                    path = psp.join(dir_path, name)
                    for infos in _walk(path, s, depth - 1):
                        yield infos

        rpath = self.resolvepath(path)  # Cache resolution.
        s = self.status(rpath)
        if s["type"] == "DIRECTORY":
            for infos in _walk(rpath, s, depth):
                yield infos

    def checksum(self, path, algorithm="md5"):
        """Get checksum for a file.

        :param path: Remote path.
        :param algorithm: The checksum algorithm name.
        """
        if algorithm == "md5":
            hash_func = hashlib.md5
        elif algorithm == "sha1":
            hash_func = hashlib.sha1
        elif algorithm == "sha224":
            hash_func = hashlib.sha224
        elif algorithm == "sha256":
            hash_func = hashlib.sha256
        elif algorithm == "sha384":
            hash_func = hashlib.sha384
        elif algorithm == "sha512":
            hash_func = hashlib.sha512
        else:
            raise FileSystemError("Unknown checksum algorithm '%s'" % algorithm)

        checksum = hash_func()
        with open(path, "rb") as f:
            chunk = f.read(8192)
            while chunk:
                checksum.update(chunk)
                chunk = f.read(8192)
        return checksum.hexdigest()

    @abc.abstractmethod
    def read(
        self,
        path,
        offset=0,
        buffer_size=None,
        encoding=None,
        chunk_size=None,
        delimiter=None,
        **kwargs
    ):
        """Read a file from FileSystem.

        :param path: path.
        :param offset: Starting byte position.
        :param length: Number of bytes to be processed. `None` will read the entire
          file.
        :param buffer_size: Size of the buffer in bytes used for transferring the
          data.
        :param encoding: Encoding used to decode the request. By default the raw
          data is returned. This is mostly helpful in python 3, for example to
          deserialize JSON data (as the decoder expects unicode).
        :param chunk_size: If set to a positive number, the context manager will
          return a generator yielding every `chunk_size` bytes instead of a
          file-like object (unless `delimiter` is also set, see below).
        :param delimiter: If set, the context manager will return a generator
          yielding each time the delimiter is encountered. This parameter requires
          the `encoding` to be specified.
        if `chunk_size` and `delimiter` are None, a file like object is returned,
        otherwise return a generator.
        This method must be called using a `with` block:
        .. code-block:: python
          with client.read('foo') as reader:
            content = reader.read()
        This ensures that connections are always properly closed.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def write(
        self,
        path,
        data=None,
        overwrite=False,
        permission=None,
        buffer_size=None,
        append=False,
        encoding=None,
        **kwargs
    ):
        """Create a file.

        :param path: Path where to create file. The necessary directories will
          be created appropriately.
        :param data: Contents of file to write. Can be a string, a generator or a
          file object. The last two options will allow streaming upload (i.e.
          without having to load the entire contents into memory). If `None`, this
          method will return a file-like object and should be called using a `with`
          block (see below for examples).
        :param overwrite: Overwrite any existing file or directory.
        :param permission: Octal permission to set on the newly created file.
          Leading zeros may be omitted.
        :param buffer_size: Size of upload buffer.
        :param append: Append to a file rather than create a new one.
        :param encoding: Encoding used to serialize data written.
        Sample usages:
        .. code-block:: python
          from json import dump, dumps
          records = [
            {'name': 'foo', 'weight': 1},
            {'name': 'bar', 'weight': 2},
          ]
          # As a context manager:
          with client.write('data/records.jsonl', encoding='utf-8') as writer:
            dump(records, writer)
          # Or, passing in a generator directly:
          client.write('data/records.jsonl', data=dumps(records), encoding='utf-8')
        """
        raise NotImplementedError

    @abc.abstractmethod
    def resolvepath(self, path):
        """Return absolute, normalized path.

        :param path: Remote path.
        """
        raise NotImplementedError
