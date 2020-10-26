import io
import json
import logging as lg
import os
import re
import time

import google.auth
from google.cloud import storage
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials

from ...common.utils.glob import iglob
from ..filesystem import IFileSystem
from ..ioutils import FileSystemError

_logger = lg.getLogger(__name__)


class GCPFileSystem(IFileSystem):
    def __init__(self, bucket_name, token=None, **params):
        credentials = self.connect(token=token)
        self.client = storage.Client(credentials=credentials, **params)
        self.bucket = self.client.get_bucket(bucket_name)

    def connect(self, token):
        """Connect  using a concrete token.

        Parameters
        ----------
        token: str, dict or Credentials
            If a str, try to load as a Service file, or next as a JSON; if
            dict, try to interpret as credentials; if Credentials, use directly.
        """

        def _connect_service(service_file):
            # raises exception if file does not match expectation
            return service_account.Credentials.from_service_account_file(service_file)

        def _dict_to_credentials(token):
            try:
                credentials = service_account.Credentials.from_service_account_info(
                    token
                )
            except Exception:
                credentials = Credentials(
                    None,
                    refresh_token=token["refresh_token"],
                    client_secret=token["client_secret"],
                    client_id=token["client_id"],
                    token_uri="https://oauth2.googleapis.com/token",
                )
            return credentials

        if token is None:
            return None
        if isinstance(token, str):
            if not os.path.exists(token):
                raise FileNotFoundError(token)
            try:
                # is this a "service" token?
                return _connect_service(token)
            except Exception:
                token = json.load(open(token))
        if isinstance(token, dict):
            return _dict_to_credentials(token)
        if isinstance(token, google.auth.credentials.Credentials):
            return token
        else:
            raise ValueError("Token format not understood")

    def resolve_path(self, path):
        # Remove any leading and trailing slash
        rpath = os.path.normpath(re.sub("^/*", "", path))
        return "" if rpath == "." else rpath

    def format_path(self, path):
        if path.startswith(self.bucket.name + "/"):
            return re.sub("^%s" % self.bucket.name, "", path)
        else:
            if not path.startswith("/"):
                return "/" + path

    def _status(self, path, strict=True):
        def _datetime2timestamp(dt):
            try:
                int(dt.timestamp() * 1000)
            except AttributeError:
                # python 2.7
                int(time.mktime(dt.timetuple()) * 1000)

        if path in ["", "/"]:
            # consider empty or slash the root of the bucket
            return {
                "fileId": "/",
                "type": "DIRECTORY",
                "length": 0,
                "creationTime": None,
                "modificationTime": None,
            }
        # check if the path is a prefix
        if len(self._list_prefix(path)) != 0:
            return {
                "fileId": self.bucket.name + "/" + path,
                "type": "DIRECTORY",
                "length": 0,
                "creationTime": None,
                "modificationTime": None,
            }
        # check if the path is a real object
        blob = self.bucket.get_blob(path)
        if blob is not None:
            return {
                "fileId": blob.id,
                "type": "FILE",
                "length": blob.size,
                "creationTime": _datetime2timestamp(blob.time_created),
                "modificationTime": _datetime2timestamp(blob.updated),
            }
        # the path does not exist
        if not strict:
            return None
        else:
            raise FileSystemError("%r does not exist." % path)

    def _list_prefix(self, prefix):
        if prefix not in [".", ""]:
            # make sure the prefix have a tailing slash
            pfx = prefix + "/" if not str(prefix).endswith("/") else prefix
        else:
            pfx = ""
        iter = self.bucket.list_blobs(delimiter="/", prefix=pfx)
        files = [os.path.basename(blob.name) for blob in iter]
        # prefixes have a tailing slash, make sure to strip it
        files.extend(
            [os.path.basename(os.path.normpath(prefix)) for prefix in iter.prefixes]
        )
        return [file for file in files if file not in ["", ".", "/"]]

    def _list(self, path, status=False, glob=False):
        if not glob:
            files = self._list_prefix(path)
            if len(files) == 0:
                raise FileSystemError("%r is not a directory." % path)
            elif len(files) == 1 and files[0] == "":
                # special case when the directory exist and is empty
                # _list returns [''] return empty array instead
                return []
            else:
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
        for blob in self.bucket.list_blobs(prefix=path):
            blob.delete()

    def _copy(self, src_path, dst_path):
        src_blob = self.bucket.get_blob(src_path)
        self.bucket.copy_blob(
            src_blob, self.bucket, re.sub("^" + src_path, dst_path, src_blob.name)
        )

    def _set_owner(self, path, owner=None, group=None):
        raise NotImplementedError

    def _set_permission(self, path, permission):
        raise NotImplementedError

    def _mkdir(self, path, permission=None):
        blob = self.bucket.blob(path + "/")
        blob.upload_from_string("")

    def _open(
        self,
        path,
        mode="rb",
        buffer_size=1024,
        part_size=5 * 2 ** 20,
        acl="",
        encoding=None,
        **kwargs
    ):
        mode2 = mode if "b" in mode else (mode.replace("t", "") + "b")
        raw_file = GCSFile(
            self.bucket,
            path,
            mode2,
            buffer_size=buffer_size,
            part_size=part_size,
            acl=acl,
        )
        if "b" in mode:
            return raw_file
        return io.TextIOWrapper(raw_file, encoding=encoding)


class GCSFile(object):
    def __init__(
        self, bucket, path, mode="rb", buffer_size=1024, part_size=16 * 2 ** 20, acl=""
    ):
        self.mode = mode
        if mode not in {"rb", "wb", "ab"}:
            raise NotImplementedError(
                "File mode must be {'rb', 'wb', 'ab'}, " "not %s" % mode
            )
        self.path = path
        self.bucket = bucket
        self.part_size = part_size
        self.loc = 0  # The read/write location pointer
        self.closed = False
        self.acl = acl
        if self.writable():
            self.buffer = io.BytesIO()
            self.size = 0
            self.forced = False
            if "a" in mode:
                self.blob = self.bucket.get_blob(self.path)
                if self.blob is None:
                    raise FileSystemError(
                        "Can not append to non existent file %s" % self.path
                    )
            else:
                self.blob = self.bucket.blob(self.path)
                # create the actual blob
                self.blob.upload_from_string("")
        else:
            self.cache = b""
            self.buffer_size = buffer_size
            self.start = None  # The start position of the cache
            self.end = None  # The end position of the cache
            self.blob = self.bucket.get_blob(self.path)
            if self.blob is None:
                raise FileSystemError("Can not read non existent file %s" % self.path)
            self.size = self.blob.size

    def tell(self):
        """Tell Current file location."""
        return self.loc

    def seek(self, loc, whence=0):
        """Set current file location.

        Parameters
        ----------
        loc : int
            byte location
        whence : {0, 1, 2}
            from start of file, current location or end of file, resp.
        """
        if not self.readable():
            raise ValueError("Seek only available in read mode")
        if whence == 0:
            nloc = loc
        elif whence == 1:
            nloc = self.loc + loc
        elif whence == 2:
            nloc = self.size + loc
        else:
            raise ValueError("invalid whence (%s, should be 0, 1 or 2)" % whence)
        if nloc < 0:
            raise ValueError("Seek before start of file")
        self.loc = nloc
        return self.loc

    def readline(self, length=-1):
        """Read and return a line from the stream.

        If length is specified, at most size bytes will be read.
        """
        self._fetch(self.loc, self.loc + 1)
        while True:
            found = self.cache[self.loc - self.start :].find(b"\n") + 1  # NOOP
            if 0 < length < found:
                return self.read(length)
            if found:
                return self.read(found)
            if self.end > self.size:
                return self.read(length)
            self._fetch(self.start, self.end + self.buffer_size)

    def __next__(self):
        out = self.readline()
        if not out:
            raise StopIteration
        return out

    next = __next__

    def __iter__(self):
        return self

    def readlines(self):
        """Return all lines in a file as a list."""
        return list(self)

    def _fetch(self, start, end):
        part_end = max(end, start + self.buffer_size)
        if self.start is None and self.end is None:
            # First read
            self.start = start
            self.end = part_end
            _logger.debug("fetch %s --> %s" % (self.start, self.end))
            self.cache = self.blob.download_as_bytes(start=self.start, end=self.end)
        if start < self.start:
            if part_end < self.start:
                self.start, self.end = None, None
                return self._fetch(start, end)
            else:
                _logger.debug("fetch %s --> %s" % (start, self.start))
                pre = self.blob.download_as_bytes(start=start, end=self.start)
                self.start = start
                if part_end < self.end:
                    self.cache = pre + self.cache[self.start : part_end]
                    self.end = part_end
                else:
                    self.cache = pre + self.cache
        if end > self.end:
            if self.end > self.size:
                return
            if start > self.end:
                self.start, self.end = None, None
                return self._fetch(start, end)
            else:
                _logger.debug("fetch %s --> %s" % (self.end, part_end))
                new = self.blob.download_as_bytes(start=self.end, end=part_end)
                self.end = part_end
                if start <= self.start:
                    self.cache = self.cache + new
                else:
                    self.cache = self.cache[start:] + new
                    self.start = start

    def read(self, length=-1):
        """
        Return data from cache, or fetch pieces as necessary.

        Parameters
        ----------
        length : int (-1)
            Number of bytes to read; if <0, all remaining bytes.
        """
        if not self.readable():
            raise ValueError("File not in read mode")
        if length < 0:
            length = self.size
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        self._fetch(self.loc, self.loc + length)
        out = self.cache[self.loc - self.start : self.loc - self.start + length]
        self.loc += len(out)
        if len(out) > 0:
            return out
        else:
            return None

    def write(self, data):
        """Write data to a the GCS file.

        Writes the data to a temporary file and flush to remote storage on close()
        or when file size reach part_size.
        Parameters
        ----------
        data : bytes
            Set of bytes to be written.
        """
        if not self.writable():
            raise ValueError("File not in write mode")
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        out = self.buffer.write(data)
        self.loc += out
        if self.buffer.tell() >= self.part_size:
            # flush the buffer into a part
            self.flush()
        return out

    def flush(self, force=False):
        """Write buffered data to GCS.

        Uploads the current FILE, if it is larger than the part-size. If
        the buffer is smaller than the  part-size, this is a no-op.
        Parameters
        ----------
        force : bool
            When closing, write the last block even if it is smaller than
            blocks are allowed to be.
        """
        if self.writable() and not self.closed:
            if self.buffer.tell() < self.part_size and not force:
                # ignore if not enough data for a block and not closing
                return
            if self.buffer.tell() == 0:
                # no data in the buffer to write
                return
            if force and self.forced:
                raise ValueError("Force flush cannot be called more than once")
            if force:
                self.forced = True

            self.buffer.seek(0)
            part_blob = self.bucket.blob(self.path + ".part")
            part_blob.upload_from_file(self.buffer)

            self.blob.compose([self.blob, part_blob])
            part_blob.delete()

            self.buffer = io.BytesIO()

    def close(self):
        """Close file.

        If in write mode, key is only finalized upon close, and key will then
        be available to other processes.
        """
        if self.closed:
            return
        self.cache = None
        if self.writable():
            self.flush(force=True)
            self.buffer.seek(0)
            self.buffer.truncate(0)
        self.closed = True

    def readable(self):
        """Return whether the GCSFile was opened for reading."""
        return "r" in self.mode

    def seekable(self):
        """Return whether the GCSFile can be seeked (only in read mode)."""
        return self.readable()

    def writable(self):
        """Return whether the GCSFile was opened for writing."""
        return "w" in self.mode or "a" in self.mode

    def __del__(self):
        self.close()

    def __str__(self):
        return "<GCSFile %s/%s>" % (self.bucket, self.path)

    __repr__ = __str__

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
