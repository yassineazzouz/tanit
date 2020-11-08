import time
import logging as lg
from thrift.transport import TTransport

_logger = lg.getLogger(__name__)


def connect(transport, max_retries=30, retry_interval=2.0):
    # Connect to server
    retries = 1
    last_error = None
    while retries < max_retries:
        try:
            transport.open()
            break
        except TTransport.TTransportException as e:
            _logger.error(
                "Could not connect to the master server. " + "retrying in 5 seconds ..."
            )
            last_error = e
        retries += 1
        time.sleep(retry_interval)

    if retries == max_retries:
        _logger.error(
            "Could not connect to the master server after 30 retries. " + "exiting ..."
        )
        raise last_error

    return transport