import io
import logging as lg
import time

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport

from ...thrift.worker.filesystem import LocalFilesystem

_logger = lg.getLogger(__name__)


class LocalFileSystemClient(object):
    def __init__(self, host="127.0.0.1", port=8989):
        self.host = host
        self.port = port
        # Init thrift connection and protocol handlers
        socket = TSocket.TSocket(host, port)
        self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)

        # Set client to our Example
        self.client = LocalFilesystem.Client(protocol)

    def start(self):
        # Connect to server
        retries = 1
        last_error = None
        while retries < 10:
            try:
                self.transport.open()
                break
            except TTransport.TTransportException as e:
                _logger.error(
                    "Could not connect to any local system service. "
                    + "retrying after 5 seconds ..."
                )
                last_error = e
            retries += 1
            time.sleep(5.0)

        if retries == 10:
            _logger.error(
                "Could not connect to any local system service after 30 retries. "
                + "exiting ..."
            )
            self.stop()
            raise last_error

    def ls(self, path, status=False, glob=False):
        if status:
            return [
                (
                    st.path,
                    {
                        "fileId": st.status.fileId,
                        "length": st.status.length,
                        "type": st.status.type,
                        "modificationTime": st.status.modificationTime,
                    },
                )
                for st in self.client.ls(path, status, glob)
            ]
        else:
            return [status.path for status in self.client.ls(path, status, glob)]

    def status(self, path, strict=True):
        status = self.client.status(path, strict)
        if status.status:
            return {
                "fileId": status.status.fileId,
                "length": status.status.length,
                "type": status.status.type,
                "modificationTime": status.status.modificationTime,
            }
        else:
            return None

    def content(self, path, strict=True):
        content = self.client.content(path, strict)
        if content.content:
            return {
                "length": content.content.length,
                "fileCount": content.content.fileCount,
                "directoryCount": content.content.directoryCount,
            }
        else:
            return None

    def rm(self, path, recursive=False):
        self.client.rm(path, recursive)

    def rename(self, src_path, dst_path):
        self.client.rename(src_path, dst_path)

    def copy(self, src_path, dst_path):
        self.client.copy(src_path, dst_path)

    def set_owner(self, path, owner=None, group=None):
        self.client.set_owner(path, owner, group)

    def set_permission(self, path, permission):
        self.client.set_permission(path, permission)

    def mkdir(self, path, permission):
        self.client.mkdir(path, permission)

    def open(self, path, mode, buffer_size=1024, encoding=None):
        mode2 = mode if "b" in mode else (mode.replace("t", "") + "b")
        file = LocalFile(self.client, path, mode2)
        if file.readable() and file.writable():
            file = io.BufferedRandom(file, buffer_size)
        elif file.readable():
            file = io.BufferedReader(file, buffer_size)
        elif file.writable():
            file = io.BufferedWriter(file, buffer_size)
        if encoding:
            return io.TextIOWrapper(file, encoding=encoding)
        else:
            return file

    def stop(self):
        self.transport.close()


class LocalFile(object):
    def __init__(self, client, path, mode="rb"):
        if "b" not in mode:
            raise NotImplementedError("Only binary read/write modes are supported.")
        self.client = client
        self.loc = 0  # The read/write location pointer
        self.closed = False
        self.desc = self.client.open(path=path, mode=mode)

    def tell(self):
        return self.client.tell(self.desc)

    def seek(self, loc, whence=0):
        return self.client.seek(self.desc, loc)

    def read(self, size=-1):
        return self.client.read(self.desc, size)

    def readline(self):
        return self.client.readline(self.desc)

    def readlines(self, lines):
        return self.client.readlines(self.desc, lines)

    def write(self, data):
        return self.client.write(self.desc, data)

    def writelines(self, lines):
        return self.client.writelines(self.desc, lines)

    def seekable(self):
        return self.client.seekable(self.desc)

    def readable(self):
        return self.client.readable(self.desc)

    def writable(self):
        return self.client.writable(self.desc)

    def flush(self):
        self.client.flush(self.desc)

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.client.close(self.desc)

    def __str__(self):
        return "<RemoteFile %s:%s %s>" % (self.client.host, self.client.port, self.path)

    __repr__ = __str__

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # Implementations of BufferedIOBase stub methods

    def read1(self, length=-1):
        return self.read(length)

    def detach(self):
        raise io.UnsupportedOperation()

    def readinto(self, b):
        data = self.client.read(self.desc, len(b))
        b[: len(data)] = bytes(data)
        return len(data)

    def readinto1(self, b):
        return self.readinto(b)
