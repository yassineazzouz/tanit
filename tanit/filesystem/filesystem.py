import abc
import hashlib
import logging as lg
import os
import posixpath as psp
import types
from contextlib import contextmanager

import six

from .ioutils import ChunkFileReader
from .ioutils import DelimitedFileReader
from .ioutils import FileReader
from .ioutils import FileSystemError

_logger = lg.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class IFileSystem:
    def resolve_path(self, path):
        """Return absolute, normalized path.

        :param path: Remote path.
        """
        return path

    def format_path(self, path):
        """Return the path in the normalized display format.

        :param path: Remote path.
        """
        return path

    @abc.abstractmethod
    def _list(self, path, status=False, glob=False):
        """Return names of files contained in a remote folder.

        :param path: Remote path to a directory. If `path` doesn't exist
          or points to a normal file, an :class:`FileSystemError` will be raised.
        :glob: Whether the path should be considered a glob expressions
        :param status: Also return each file's corresponding FileStatus.
        """
        raise NotImplementedError

    def list(self, path, status=False, glob=False):
        """Return names of files contained in a remote folder.

        :param path: Remote path to a directory. If `path` doesn't exist
          or points to a normal file, an :class:`FileSystemError` will be raised.
        :glob: Whether the path should be considered a glob expressions
        :param status: Also return each file's corresponding FileStatus.
        """
        rpath = self.resolve_path(path)
        if not glob and not self.exists(rpath):
            raise FileSystemError("%r does not exist.", rpath)
        else:
            return self._list(rpath, status=status, glob=glob)

    @abc.abstractmethod
    def _status(self, path, strict=True):
        raise NotImplementedError

    def status(self, path, strict=True):
        """Get FileStatus_ for a file or folder on HDFS.

        :param path: path.
        :param strict: If `False`, return `None` rather than raise an exception if
          the path doesn't exist.
        """
        rpath = self.resolve_path(path)
        s = self._status(rpath, strict)
        if s is None:
            return None
        else:
            return {
                "fileId": rpath,
                "length": str(s["length"]),
                "type": str(s["type"]).upper(),
                "modificationTime": s["modificationTime"],
            }

    def exists(self, path):
        rpath = self.resolve_path(path)
        return self.status(rpath, strict=False) is not None

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
        rpath = self.resolve_path(path)
        if not self.exists(rpath):
            if not strict:
                return None
            else:
                raise FileSystemError("%r does not exist.", rpath)
        else:
            return _get_size(rpath)

    @abc.abstractmethod
    def _delete(self, path, recursive=True):
        raise NotImplementedError

    def delete(self, path, recursive=True):
        """Remove a file or directory.

        :param path: path.
        :param recursive: Recursively delete files and directories. By default,
          this method will raise an :class:`FileSystemError` if trying to delete a
          non-empty directory.
        This function returns `True` if the deletion was successful and `False` if
        no file or directory previously existed at `path`.
        """
        rpath = self.resolve_path(path)
        if not self.exists(rpath):
            raise FileSystemError("%r does not exist.", rpath)
        self._delete(rpath, recursive=recursive)

    @abc.abstractmethod
    def _copy(self, src_path, dst_path):
        raise NotImplementedError

    def copy(self, src_path, dst_path, recursive=True):
        """Copy a file.

        :param src_path: Source path.
        :param dst_path: Destination path. If the path already exists and is
          a directory, the source will be moved into it. If the path exists and is
          a file, or if a parent destination directory is missing, this method will
          raise an :class:`FileSystemError`.
        :param recursive: recursively copy.
        """
        rsrc_path = self.resolve_path(src_path)
        rdst_path = self.resolve_path(dst_path)
        src_status = self.status(rsrc_path, strict=False)
        dst_status = self.status(rdst_path, strict=False)
        if src_status is None:
            raise FileSystemError("%r does not exist.", rsrc_path)

        if src_status["type"] == "DIRECTORY":
            if not recursive:
                raise FileSystemError("'%s' is a directory." % src_path)
            if dst_status is None:
                self.mkdir(rdst_path)
            for rpath, _, files in self.walk(src_path):
                for f in files:
                    self._copy(os.path.join(rsrc_path, f), os.path.join(rdst_path, f))
        else:
            self._copy(rsrc_path, rdst_path)

    def rename(self, src_path, dst_path):
        """Rename a file or folder.

        Not all filesystems have a native rename implementation based
        on metadata only modification.
        :param src_path: Source path.
        :param dst_path: Destination path. If the path already exists and is
          a directory, the source will be moved into it. If the path exists and is
          a file, or if a parent destination directory is missing, this method will
          raise an :class:`FileSystemError`.
        """
        rsrc_path = self.resolve_path(src_path)
        rdst_path = self.resolve_path(dst_path)
        self.copy(rsrc_path, rdst_path, recursive=True)
        self.delete(rsrc_path)

    @abc.abstractmethod
    def _set_owner(self, path, owner=None, group=None):
        """Change the owner of file.

        :param path: path.
        :param owner: Optional, new owner for file.
        :param group: Optional, new group for file.
        At least one of `owner` and `group` must be specified.
        """
        raise NotImplementedError

    def set_owner(self, path, owner=None, group=None):
        rpath = self.resolve_path(path)
        if not self.exists(rpath):
            self._set_owner(rpath, owner=owner, group=group)
        else:
            raise FileSystemError("%r does not exist.", rpath)

    @abc.abstractmethod
    def _set_permission(self, path, permission):
        """Change the permissions of file.

        :param path: path.
        :param permission: New octal permissions string of file.
        """
        raise NotImplementedError

    def set_permission(self, path, permission=None):
        rpath = self.resolve_path(path)
        if not self.exists(rpath):
            self._set_permission(rpath, permission)
        else:
            raise FileSystemError("%r does not exist.", rpath)

    @abc.abstractmethod
    def _mkdir(self, path, permission=None):
        """Create a remote directory, recursively if necessary.

        :param path: Remote path. Intermediate directories will be created
          appropriately.
        :param permission: Octal permission to set on the newly created directory.
          These permissions will only be set on directories that do not already
          exist.
        """
        raise NotImplementedError

    def mkdir(self, path, permission=None):
        rpath = self.resolve_path(path)
        if not self.exists(rpath):
            self._mkdir(rpath, permission)
        else:
            raise FileSystemError("%r does not exist.", rpath)

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
                yield ((self.format_path(dir_path), dir_status), dir_infos, file_infos)
            else:
                yield (
                    self.format_path(dir_path),
                    [name for name, _ in dir_infos],
                    [name for name, _ in file_infos],
                )
            if depth != 1:
                for name, s in dir_infos:
                    path = psp.join(dir_path, name)
                    for infos in _walk(path, s, depth - 1):
                        yield infos

        rpath = self.resolve_path(path)  # Cache resolution.
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
        with self.read(path, chunk_size=8192) as reader:
            for chunk in reader:
                checksum.update(chunk)
        return checksum.hexdigest()

    @abc.abstractmethod
    def _open(self, path, mode, buffer_size=0, encoding=None, **kwargs):
        raise NotImplementedError

    def open(self, path, mode, buffer_size=1024, encoding=None, **kwargs):
        """Access a file from the Filesystem.

        Parameters
        ----------
        path: string
            Path of file on S3
        mode: string
            One of 'r', 'w', 'a', 'rb', 'wb', or 'ab'. These have the same meaning
            as they do for the built-in `open` function.
        buffer_size: int
            Size of data buffer when reading/writing data to the filesystem.
            The purpose of the buffer is to reduce the number of io.
        encoding : str
            The encoding to use if opening the file in text mode. The platform's
            default text encoding is used if not given.
        kwargs: dict-like
            Additional parameters used for file system specific usage.
        :return: A file object, idially an implementation of `BufferedIOBase`
        """
        rpath = self.resolve_path(path)
        return self._open(
            path=rpath, mode=mode, buffer_size=buffer_size, encoding=encoding, **kwargs
        )

    @contextmanager
    def read(
        self,
        path,
        offset=0,
        buffer_size=1024,
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
        if delimiter:
            if not encoding:
                raise ValueError("Delimiter splitting requires an encoding.")
            if chunk_size:
                raise ValueError("Delimiter splitting incompatible with chunk size.")

        rpath = self.resolve_path(path)
        if self.status(rpath, strict=False) is None:
            raise FileSystemError("%r does not exist.", rpath)

        _logger.debug("Reading file %r.", path)
        file = self.open(
            rpath, mode="rb", buffer_size=buffer_size, encoding=encoding, **kwargs
        )

        if offset > 0:
            file.seek(offset)
        try:
            if not chunk_size and not delimiter:
                # return a file like object
                yield file
            else:
                # return a generator function
                if delimiter:
                    yield DelimitedFileReader(file, delimiter=delimiter)
                else:
                    yield ChunkFileReader(file, chunk_size=chunk_size)
        finally:
            file.close()
            _logger.debug("Closed response for reading file %r.", path)

    def write(
        self,
        path,
        data=None,
        overwrite=False,
        permission=None,
        buffer_size=1024,
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
        :return: The file object returned by `open` if data is `None` else
        write the data to the file and return `void`.
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
        rpath = self.resolve_path(path)
        status = self.status(rpath, strict=False)
        if append:
            if overwrite:
                raise ValueError("Cannot both overwrite and append.")
            if permission:
                raise ValueError("Cannot change file properties while appending.")

            if status is not None and status["type"] != "FILE":
                raise ValueError("Path %r is not a file.", rpath)
        else:
            if not overwrite:
                if status is not None:
                    raise ValueError("Path %r exists, missing `append`.", rpath)
            else:
                if status is not None and status["type"] != "FILE":
                    raise ValueError("Path %r is not a file.", rpath)

        _logger.debug("Writing to %r.", path)
        file = self.open(
            rpath,
            mode="ab" if append else "wb",
            buffer_size=buffer_size,
            encoding=encoding,
            **kwargs
        )
        if data is None:
            return file
        else:
            with file:
                if isinstance(data, types.GeneratorType) or isinstance(
                    data, FileReader
                ):
                    for chunk in data:
                        file.write(chunk)
                else:
                    file.write(data)
