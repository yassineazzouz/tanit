import logging as lg

from thrift.protocol import TBinaryProtocol
from thrift.protocol.TMultiplexedProtocol import TMultiplexedProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport

from ...common.thrift.utils import connect
from ...thrift.master.dfs import DistributedFilesystem
from ...common.config.configuration_keys import Keys
from ...common.config.configuration import TanitConfigurationException, TanitConfiguration

_logger = lg.getLogger(__name__)


class DistributedFileSystemClient(object):
    def __init__(self):
        configuration = TanitConfiguration.getInstance()
        self.master_host = configuration.get(Keys.MASTER_HOSTNAME)
        if self.master_host is None:
            raise TanitConfigurationException("Missing required configuration '%s'", Keys.MASTER_HOSTNAME)

        self.rpc_port = configuration.get(Keys.MASTER_RPC_PORT)
        if self.rpc_port is None:
            raise TanitConfigurationException("Missing required configuration '%s'", Keys.MASTER_RPC_PORT)

        self.rpc_max_retries = configuration.get(Keys.RPC_CLIENT_MAX_RETRIES)
        self.rpc_retry_interval = configuration.get(Keys.RPC_CLIENT_RETRY_INTERVAL)

    def start(self):
        self.transport = connect(
            TTransport.TFramedTransport(
                TSocket.TSocket(self.master_host, self.rpc_port)
            ),
            self.rpc_max_retries,
            self.rpc_retry_interval / 1000
        )
        # Set client to our Example
        self.client = DistributedFilesystem.Client(
            TMultiplexedProtocol(
                TBinaryProtocol.TBinaryProtocol(self.transport),
                "DFSService"
            )
        )

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
