#!/usr/bin/env python

import logging as lg
import time
from threading import Thread

from thrift.TMultiplexedProcessor import TMultiplexedProcessor
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket
from thrift.transport import TTransport

from ...thrift.master.service import MasterUserService
from ...thrift.master.service import MasterWorkerService
from ...thrift.master.dfs import DistributedFilesystem
from ..core.master import Master
from ..standalone.master import StandaloneMaster
from ..dfs.handler import DistributedFileSystemHandler
from .handler import UserServiceHandler
from .handler import WorkerServiceHandler

from ...common.config.configuration import TanitConfiguration
from ...common.config.configuration_keys import Keys

_logger = lg.getLogger(__name__)


class UsersServer(Thread):
    def __init__(self, master):
        super(UsersServer, self).__init__()
        configuration = TanitConfiguration.getInstance()

        self.master = master
        self.listen_address = configuration.get(Keys.MASTER_RPC_BIND_HOST)
        self.listen_port = configuration.get_int(Keys.MASTER_RPC_PORT)
        self.n_threads = configuration.get_int(Keys.MASTER_RPC_THREADS)

    def run(self):

        processor = TMultiplexedProcessor()

        processor.registerProcessor(
            "DFSService",
            DistributedFilesystem.Processor(
                DistributedFileSystemHandler(self.master.dfs)
            )
        )

        processor.registerProcessor(
            "UserService",
            MasterUserService.Processor(
                UserServiceHandler(self.master)
            )
        )

        server = TServer.TThreadedServer(
            processor,
            TSocket.TServerSocket(self.listen_address, self.listen_port),
            TTransport.TFramedTransportFactory(),
            TBinaryProtocol.TBinaryProtocolFactory(),
            daemon=True,
        )

        # Start Tanit distributed filesystem
        server.serve()


class MasterWorkerServer(Thread):
    def __init__(self, master):
        super(MasterWorkerServer, self).__init__()
        configuration = TanitConfiguration.getInstance()

        self.master = master
        self.listen_address = configuration.get(Keys.MASTER_RPC_SERVICE_BIND_HOST)
        self.listen_port = configuration.get_int(Keys.MASTER_RPC_SERVICE_PORT)
        self.n_threads = configuration.get_int(Keys.MASTER_RPC_THREADS)

    def run(self):
        # Create Service handler
        handler = WorkerServiceHandler(self.master)

        server = TServer.TThreadedServer(
            MasterWorkerService.Processor(handler),
            TSocket.TServerSocket(self.listen_address, self.listen_port),
            TTransport.TFramedTransportFactory(),
            TBinaryProtocol.TBinaryProtocolFactory(),
            daemon=True,
        )

        # Start Tanit server
        server.serve()


class MasterServer(object):
    def __init__(self, standalone=False):

        self.standalone = standalone
        self.master = Master() if not standalone else StandaloneMaster()

        self.stopped = False

    def stop(self):
        self.stopped = True

    def start(self):
        # Start master services
        self.master.start()

        _logger.info("Stating Tanit rpc server.")

        rpc_server = UsersServer(self.master)
        rpc_server.setDaemon(True)
        rpc_server.start()
        _logger.info(
            "Tanit rpc server started, listening  at %s:%s",
            rpc_server.listen_address,
            rpc_server.listen_port,
        )

        if not self.standalone:
            _logger.info("Stating Tanit master worker server.")
            mwserver = MasterWorkerServer(self.master)
            mwserver.setDaemon(True)
            mwserver.start()
            _logger.info(
                "Tanit master worker server started, listening  at %s:%s",
                mwserver.listen_address,
                mwserver.listen_port,
            )

        try:
            while True:
                if not rpc_server.is_alive():
                    _logger.error(
                        "Unexpected Tanit rpc server exit, stopping."
                    )
                    break
                if not self.standalone and not mwserver.is_alive():
                    _logger.error(
                        "Unexpected Tanit worker client server exit, stopping."
                    )
                    break
                if self.stopped:
                    _logger.info("Tanit server stopped, exiting.")
                    break
                # wait for 0.5 seconds
                time.sleep(0.5)
        except (KeyboardInterrupt, SystemExit):
            _logger.info("Received KeyboardInterrupt Signal.")
        except Exception as e:
            _logger.exception("Fatal server exception : %s, exiting", e)
        finally:
            _logger.info("Stopping Tanit server.")
            self.master.stop()
            _logger.info("Tanit server stopped.")
