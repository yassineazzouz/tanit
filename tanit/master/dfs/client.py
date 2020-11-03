import time
import logging as lg

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport

from ...thrift.master.dfs import DistributedFilesystem

_logger = lg.getLogger(__name__)


class DistributedFileSystemClient(object):
    def __init__(self, host="127.0.0.1", port=9092):
        self.host = host
        self.port = port
        # Init thrift connection and protocol handlers
        socket = TSocket.TSocket(host, port)
        self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)

        # Set client to our Example
        self.client = DistributedFilesystem.Client(protocol)

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

    def stop(self):
        self.transport.close()

    def list(self, path, status=False, glob=False):
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

    def mkdir(self, path, permission=None):
        self.client.mkdir(path, permission)

    def rm(self, path, recursive=False):
        self.client.rm(path, recursive)

    def cp(self, src_path, dst_path, overwrite=True, force=False, checksum=False):
        self.client.copy(src_path, dst_path, overwrite, force, checksum)

    def move(self, src_path, dst_path):
        self.client.move(src_path, dst_path)

    def checksum(self, path, algorithm="md5"):
        return self.client.checksum(path, algorithm)
