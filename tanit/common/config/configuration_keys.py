import socket
import multiprocessing


class ConfigurationKey(object):
    def __init__(self, section, name):
        self.section = section
        self.name = name

    def __str__(self):
        return "%s.%s" % (
            self.section,
            self.name
        )

    def __hash__(self):
        return hash(self.__str__())

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if self.section == other.section:
                if self.name == other.name:
                    return True
                else:
                    return False
            else:
                return False
        else:
            return False


class ConfigurationProperty(object):
    def __init__(self, key, description, default_value=None):
        self.key = key
        self.description = description
        self.default_value = default_value


class Keys(object):
    # Master configuration
    MASTER_HOSTNAME = ConfigurationKey("master", "hostname")
    MASTER_RPC_BIND_HOST = ConfigurationKey("master", "rpc.bind.host")
    MASTER_RPC_PORT = ConfigurationKey("master", "rpc.port")
    MASTER_RPC_SERVICE_BIND_HOST = ConfigurationKey("master", "rpc.service.bind.host")
    MASTER_RPC_SERVICE_PORT = ConfigurationKey("master", "rpc.service.port")
    MASTER_RPC_THREADS = ConfigurationKey("master", "rpc.threads")
    MASTER_EXECUTION_MAX_TASK_RETRIES = ConfigurationKey("master", "execution.max.task.retries")

    # Worker configuration
    WORKER_HOSTNAME = ConfigurationKey("worker", "hostname")
    WORKER_RPC_BIND_HOST = ConfigurationKey("worker", "rpc.bind.host")
    WORKER_RPC_PORT = ConfigurationKey("worker", "rpc.port")
    WORKER_RPC_THREADS = ConfigurationKey("worker", "rpc.threads")
    WORKER_EXECUTOR_THREADS = ConfigurationKey("worker", "executor.threads")
    WORKER_HEARTBEATS_INTERVAL = ConfigurationKey("worker", "heartbeats.interval")

    # RPC section (common configuration)
    RPC_CLIENT_MAX_RETRIES = ConfigurationKey("rpc", "client.connect.max.retries")
    RPC_CLIENT_RETRY_INTERVAL = ConfigurationKey("rpc", "client.connect.retry.interval")
    RPC_CLIENT_CONNECT_TIMEOUT = ConfigurationKey("rpc", "client.connect.timeout")


DEFAULT_PROPERTIES = [
    ConfigurationProperty(Keys.MASTER_HOSTNAME, "", ""),
    ConfigurationProperty(Keys.MASTER_RPC_PORT, "", "0.0.0.0"),
    ConfigurationProperty(Keys.MASTER_RPC_PORT, "", 9091),
    ConfigurationProperty(Keys.MASTER_RPC_SERVICE_BIND_HOST, "", "0.0.0.0"),
    ConfigurationProperty(Keys.MASTER_RPC_SERVICE_PORT, "", 9090),
    ConfigurationProperty(Keys.MASTER_RPC_THREADS, "", 25),
    ConfigurationProperty(Keys.MASTER_EXECUTION_MAX_TASK_RETRIES, "", 3),
    ConfigurationProperty(Keys.WORKER_HOSTNAME, "", socket.gethostname()),
    ConfigurationProperty(Keys.WORKER_RPC_BIND_HOST, "", "0.0.0.0"),
    ConfigurationProperty(Keys.WORKER_RPC_PORT, "", 9091),
    ConfigurationProperty(Keys.WORKER_RPC_THREADS, "", 25),
    ConfigurationProperty(Keys.WORKER_EXECUTOR_THREADS, "", multiprocessing.cpu_count()),
    ConfigurationProperty(Keys.WORKER_HEARTBEATS_INTERVAL, "", 3000),
    ConfigurationProperty(
        Keys.RPC_CLIENT_MAX_RETRIES,
        "Indicates the number of retries a client will make to establish a server connection.",
        20),
    ConfigurationProperty(
        Keys.RPC_CLIENT_RETRY_INTERVAL,
        "Indicates the number of milliseconds a client will wait for before retrying to establish a server connection.",
        2500),
    ConfigurationProperty(
        Keys.RPC_CLIENT_CONNECT_TIMEOUT,
        "Indicates the number of milliseconds a client will wait for the socket to establish a server connection.",
        20000
    )
]
