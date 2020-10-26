import logging as lg

from ...common.config.config import Config
from ...common.config.config import TanitConfigurationException

_logger = lg.getLogger(__name__)


class MasterConfig(Config):
    default_bind_address = "0.0.0.0"
    default_thrift_threads = 25

    def __init__(self, path=None):
        super(MasterConfig, self).__init__(path)

    def load(self):

        if self.config.has_option("master", "bind_address"):
            self.bind_address = self.config.get("master", "bind_address")
        else:
            self.bind_address = MasterConfig.default_bind_address

        if self.config.has_option("master", "worker-service_address"):
            worker_service_address = self.config.get("master", "worker-service_address")
        else:
            raise TanitConfigurationException(
                "Missing master rpc worker service address from configuration"
            )

        if len(worker_service_address.split(":")) != 2:
            raise TanitConfigurationException(
                "Master rpc worker service address should be formatted as host:port."  # NOQA
            )

        self.worker_service_host = worker_service_address.split(":")[0]
        self.worker_service_port = int(worker_service_address.split(":")[1])

        if self.config.has_option("master", "client-service_address"):
            client_service_address = self.config.get("master", "client-service_address")
        else:
            raise TanitConfigurationException(
                "Missing master rpc client service address from configuration"
            )

        if len(client_service_address.split(":")) != 2:
            raise TanitConfigurationException(
                "Master rpc client service address should be formatted as host:port."  # NOQA
            )

        self.client_service_host = client_service_address.split(":")[0]
        self.client_service_port = int(client_service_address.split(":")[1])

        if self.config.has_option("worker", "thrift_threads"):
            self.thrift_threads = self.config.getint("worker", "thrift_threads")
        else:
            self.thrift_threads = MasterConfig.default_thrift_threads
