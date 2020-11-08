import logging as lg

from .user_service import LocalUserServiceClient, ThriftUserServiceClient
from .worker_service import LocalWorkerServiceClient, ThriftWorkerServiceClient

_logger = lg.getLogger(__name__)

WORKER_SERVICE_CLIENT_NAME = "worker-service"
USER_SERVICE_CLIENT_NAME = "user-service"


class ClientType:
    USER_SERVICE = 1
    WORKER_SERVICE = 2

    _VALUES_TO_NAMES = {
        1: "USER_SERVICE",
        2: "WORKER_SERVICE",
    }

    _NAMES_TO_VALUES = {
        "USER_SERVICE": 1,
        "WORKER_SERVICE": 2,
    }


class LocalClientFactory(object):
    def __init__(self, master):
        self.master = master

    def create_client(self, client_type):
        if client_type == ClientType.WORKER_SERVICE:
            return LocalWorkerServiceClient(self.master)

        elif client_type == ClientType.USER_SERVICE:
            return LocalUserServiceClient(self.master)
        else:
            raise NoSuchClientException(
                "No such client [ %s ]", ClientType._VALUES_TO_NAMES[client_type]
            )


class ThriftClientFactory(object):
    def create_client(self, client_type):
        if client_type == ClientType.WORKER_SERVICE:
            return ThriftWorkerServiceClient()

        elif client_type == ClientType.USER_SERVICE:
            return ThriftUserServiceClient()
        else:
            raise NoSuchClientException(
                "No such client [ %s ]", ClientType._VALUES_TO_NAMES[client_type]
            )


class NoSuchClientException(Exception):
    pass
